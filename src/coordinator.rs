use std::collections::HashMap;

use crate::client::ClientProcess;
use crate::metrics::{self, MetricsProcess};
use crate::persistence::{self, FileLog};
use crate::structure::{ConfirmationMessage, PublishJob, PublishMessage, QueueMessage, WriterRef};
use crate::topic_tree::TopicTree;
use lunatic::{
    host,
    process::{AbstractProcess, Message, ProcessMessage, ProcessRef, ProcessRequest, Request},
    supervisor::Supervisor,
};
use mqtt_packet_3_5::{
    ConfirmationPacket, Granted, MqttPacket, PacketType, PublishPacket, SubackPacket,
    SubscribePacket,
};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use uuid::Uuid;

// A reference to a client that joined the server.
struct Client {
    // username: String,
    pub client: ProcessRef<ClientProcess>,
    pub writer: WriterRef,
    pub should_persist: bool,
}

/// The `CoordinatorSup` is supervising one global instance of the `CoordinatorProcess`.
pub struct CoordinatorSup;
impl Supervisor for CoordinatorSup {
    type Arg = String;
    type Children = CoordinatorProcess;

    fn init(config: &mut lunatic::supervisor::SupervisorConfig<Self>, name: Self::Arg) {
        // Always register the `CoordinatorProcess` under the name passed to the supervisor.
        config.children_args(((), Some(name)))
    }
}

pub struct CoordinatorProcess {
    messages: Vec<QueueMessage>,
    clients: HashMap<String, Client>,
    topic_tree: TopicTree,
    wal: FileLog,
    waiting_qos1: HashMap<u16, bool>, // channels: HashMap<String, (ProcessRef<ChannelProcess>, usize)>,
    metrics: ProcessRef<MetricsProcess>,
    message_ids: HashMap<u16, Uuid>,
}

impl CoordinatorProcess {
    pub fn drop_message_by_uuid(messages: &mut Vec<QueueMessage>, message_uuid: Uuid) {
        messages.retain(|msg| match msg {
            QueueMessage::Publish(p) => p.message_uuid != message_uuid,
            QueueMessage::Confirmation(c) => c.message_uuid != message_uuid,
        });
    }

    pub fn drop_message_uuid(&mut self, message_uuid: Uuid) {
        CoordinatorProcess::drop_message_by_uuid(&mut self.messages, message_uuid)
    }

    pub fn drop_publish_message_id(&mut self, message_id: u16) {
        self.messages.retain(|msg| {
            if let QueueMessage::Publish(p) = msg {
                return p.packet.message_id != Some(message_id);
            }
            true
        });
    }

    // pub fn drop_publish_message_id(&mut self, message_id: u16) {
    //     self.messages.retain(|msg| {
    //         if let QueueMessage::Confirmation(p) = msg {
    //             return p.packet.message_id != message_id;
    //         }
    //         true
    //     });
    // }

    pub fn get_by_uuid(&self, message_uuid: Uuid) -> Option<&PublishMessage> {
        CoordinatorProcess::get_message_by_uuid(&self.messages, message_uuid)
    }

    pub fn get_by_uuid_mut(&mut self, message_uuid: Uuid) -> Option<&mut PublishMessage> {
        CoordinatorProcess::get_message_by_uuid_mut(&mut self.messages, message_uuid)
    }

    pub fn get_message_by_uuid(
        messages: &[QueueMessage],
        message_uuid: Uuid,
    ) -> Option<&PublishMessage> {
        for msg in messages.iter() {
            if let QueueMessage::Publish(p) = msg {
                if p.message_uuid == message_uuid {
                    return Some(p);
                }
            }
        }
        None
    }

    pub fn get_message_by_uuid_mut(
        messages: &mut [QueueMessage],
        message_uuid: Uuid,
    ) -> Option<&mut PublishMessage> {
        for msg in messages.iter_mut() {
            if let QueueMessage::Publish(p) = msg {
                if p.message_uuid == message_uuid {
                    return Some(p);
                }
            }
        }
        None
    }

    pub fn drop_inactive_subs(&mut self, queue_id: u128, inactive_subs: Vec<WriterRef>) {
        let queue = self.topic_tree.get_by_id(queue_id);
        queue.drop_inactive_subs(inactive_subs);
    }

    pub fn can_process_confirmation(
        waiting_qos1: &HashMap<u16, bool>,
        confirm: &mut ConfirmationMessage,
    ) -> bool {
        return !confirm.in_progress
            && !waiting_qos1.contains_key(&confirm.message_id)
            && confirm.send_to.process.is_some();
    }
}

impl AbstractProcess for CoordinatorProcess {
    type Arg = ();
    type State = Self;

    fn init(_: ProcessRef<Self>, _: Self::Arg) -> Self::State {
        // Coordinator shouldn't die when a client dies. This makes the link one-directional.
        unsafe { host::api::process::die_when_link_dies(0) };

        let mut topic_tree = TopicTree::default();
        let mut messages = Vec::new();
        let mut message_ids = HashMap::new();

        let prev_state = FileLog::read_file("persistence", "backup.log").unwrap();
        println!("[Coordinator] read prev_state {:?}", prev_state);

        for e in prev_state {
            match e {
                persistence::Entry::Publish(publish) => {
                    // save message_id -> uuid mapping
                    message_ids.insert(publish.packet.message_id.unwrap(), publish.uuid);
                    // ensure queue
                    let queue = topic_tree.get_by_name(publish.packet.topic.clone());
                    // save message to topic_tree
                    let client_id = publish.client_id;
                    messages.push(QueueMessage::Publish(PublishMessage {
                        message_uuid: publish.uuid,
                        packet: publish.packet,
                        queue_id: queue.id,
                        in_progress: false,
                        sent: false,
                        sender: WriterRef {
                            client_id,
                            process: None,
                            session_id: publish.session.uuid,
                            is_persistent_session: publish.session.is_persistent,
                        },
                        started_at: publish.received_at,
                    }));
                }
                persistence::Entry::Accepted(acc) => {
                    if let Some(publish) =
                        CoordinatorProcess::get_message_by_uuid_mut(&mut messages, acc.uuid)
                    {
                        // if qos 1 then we are done and can actually delete the message, but
                        // maybe still need to try and deliver the puback to the writer once
                        // he reconnects
                    }
                }
                persistence::Entry::Sent(entry) => {
                    if let Some(publish) =
                        CoordinatorProcess::get_message_by_uuid_mut(&mut messages, entry.uuid)
                    {
                        // write to log and mark message as sent
                        publish.sent = true;
                    }
                }
                persistence::Entry::Deleted(entry) => {}
                persistence::Entry::Completed(complete) => {
                    // delete message from messages
                    CoordinatorProcess::drop_message_by_uuid(&mut messages, complete.uuid);
                }
            }
        }

        CoordinatorProcess {
            topic_tree,
            messages,
            clients: HashMap::new(),
            wal: FileLog::new("persistence", "backup.log"),
            waiting_qos1: HashMap::new(),
            metrics: ProcessRef::<MetricsProcess>::lookup("metrics").unwrap(),
            message_ids,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Connect(pub ProcessRef<ClientProcess>, pub WriterRef, pub bool);
impl ProcessRequest<Connect> for CoordinatorProcess {
    type Response = bool;

    fn handle(
        state: &mut Self::State,
        Connect(client, writer, should_persist): Connect,
    ) -> Self::Response {
        // TODO: handle new connections and reconnections
        // Sometimes a client that has sent a high qos message will
        // disconnect and therefore a reference to the process
        // should be stored to the publish message
        state.clients.insert(
            writer.client_id.clone(),
            Client {
                client,
                writer: writer.clone(),
                should_persist,
            },
        );
        state.metrics.send(metrics::Connect);
        // update all messages to point to correct writer
        // after reconnect
        if !should_persist {}
        for msg in state.messages.iter_mut() {
            if let QueueMessage::Publish(publish) = msg {
                if publish.sender.client_id == writer.client_id {
                    publish.sender = writer.clone();
                }
            }
        }
        true
    }
}

#[derive(Serialize, Deserialize)]
pub struct Disconnect(pub String);
impl ProcessMessage<Disconnect> for CoordinatorProcess {
    fn handle(state: &mut Self::State, Disconnect(client_id): Disconnect) {
        state.clients.remove(&client_id);
        state.metrics.send(metrics::Disconnect);
    }
}

/// Publish message
#[derive(Serialize, Deserialize)]
pub struct Subscribe(pub SubscribePacket, pub WriterRef);
impl ProcessRequest<Subscribe> for CoordinatorProcess {
    type Response = bool;

    fn handle(
        state: &mut CoordinatorProcess,
        Subscribe(packet, writer): Subscribe,
    ) -> Self::Response {
        println!(
            "[Coordinator->Subscribe] Received Subscribe message {:?} {:?}",
            writer, packet
        );
        let mut suback = SubackPacket {
            granted: vec![],
            granted_reason_codes: vec![],
            message_id: packet.message_id,
            reason_code: Some(0),
            properties: None,
        };
        for sub in packet.subscriptions {
            state
                .topic_tree
                .add_subscriptions(sub.topic, writer.clone());
            suback.granted.push(Granted::QoS2);
            println!(
                "[Coordinator->Subscribe] Got these matching queues {:?}",
                state.topic_tree
            );
        }
        // safe to unwrap because the process is always present
        writer.process.unwrap().request(MqttPacket::Suback(suback))
    }
}

/// Publish message
#[derive(Serialize, Deserialize)]
pub struct Publish(pub PublishPacket, pub WriterRef, pub SystemTime);
impl ProcessRequest<Publish> for CoordinatorProcess {
    type Response = bool;

    fn handle(
        state: &mut CoordinatorProcess,
        Publish(packet, writer, started_at): Publish,
    ) -> Self::Response {
        let message_uuid = Uuid::new_v4();
        if packet.qos > 0 {
            state
                .message_ids
                .insert(packet.message_id.unwrap(), message_uuid);
            state
                .wal
                .append_publish(message_uuid, packet.clone(), &writer, started_at);
        }
        let queue = state.topic_tree.get_by_name(packet.topic.clone());
        println!(
            "[Coordinator->Publish] Adding publish message to message queue {} {:?}",
            packet.topic, queue
        );
        state.messages.push(QueueMessage::Publish(PublishMessage {
            message_uuid,
            packet,
            queue_id: queue.id,
            in_progress: false,
            sent: false,
            sender: writer,
            started_at,
        }));
        true
    }
}

/// ConfirmationPacket message
impl ProcessRequest<ConfirmationPacket> for CoordinatorProcess {
    type Response = bool;

    fn handle(state: &mut CoordinatorProcess, packet: ConfirmationPacket) -> Self::Response {
        // do qos 1 flow
        let message_id = packet.message_id;
        let message_uuid = *state.message_ids.get(&message_id).unwrap();
        if packet.cmd == PacketType::Puback {
            // let queue = state.topic_tree.get_by_name(packet.message_id);
            println!(
                "[Coordinator->Confirmation] Received PUBACK for {}. Releasing message {:?}",
                message_uuid, packet
            );
            state
                .wal
                .append_confirmation(message_uuid, packet.clone(), SystemTime::now());
            println!(
                "[Coordinator->Confirmation] Wrote to WAL, getting sender {} {:?}",
                message_uuid, state.messages
            );
            let publish_message = state.get_by_uuid(message_uuid);
            // handle case where multiple pubacks may be sent to broker
            // but the message qos flow was already handled and the message
            // has therefore been deleted
            if let (None, PacketType::Puback) = (publish_message, packet.cmd) {
                println!(
                    "[Coordinator->Confirmation] Matching publish message for puback not found {}",
                    message_id
                );
                return false;
            }

            // TODO: this will fail if message is qos 2 message and was not found
            // it needs to be ensured that this doesn't happen
            let publish_message = publish_message.unwrap();
            state
                .messages
                .push(QueueMessage::Confirmation(ConfirmationMessage {
                    message_id,
                    packet,
                    in_progress: false,
                    send_to: publish_message.sender.clone(),
                    started_at: publish_message.started_at,
                    message_uuid,
                }));
            println!(
                "[Coordinator->Confirmation] Added PUBACK for {}. Releasing message {:?}",
                message_id, state.messages
            );
        }
        // state.wal.append_confirmation(packet.clone());
        // let queue = state.topic_tree.get_by_name(packet.message_id);
        // println!(
        //     "[Coordinator->Confirmation] Dropping message {} {:?}",
        //     message_id, packet
        // );
        state.drop_publish_message_id(message_id);
        println!(
            "[Coordinator->Confirmation] Dropped PUBLISH for {}. Releasing message {:?}",
            message_id, state.messages
        );
        true
    }
}

/// Poll Message from queue
#[derive(Serialize, Deserialize)]
pub struct Poll;

#[derive(Serialize, Deserialize)]
pub enum PollResponse {
    None,
    Publish(PublishJob),
    Confirmation(ConfirmationMessage),
}
impl ProcessRequest<Poll> for CoordinatorProcess {
    type Response = PollResponse;

    fn handle(state: &mut CoordinatorProcess, _: Poll) -> Self::Response {
        for msg in state.messages.iter_mut() {
            match msg {
                QueueMessage::Publish(publish) => {
                    if !publish.in_progress && publish.sender.process.is_some() {
                        let queue = state.topic_tree.get_by_id(publish.queue_id);
                        if queue.subscribers.is_empty() {
                            continue;
                        }
                        publish.in_progress = true;
                        return PollResponse::Publish(PublishJob {
                            message: publish.clone(),
                            queue: queue.clone(),
                        });
                    }
                }
                QueueMessage::Confirmation(confirm) => {
                    println!(
                        "[Coordinator->Poll] Checking confirmation message {:?} | {:?}",
                        confirm,
                        state.waiting_qos1.contains_key(&confirm.message_id)
                    );
                    if CoordinatorProcess::can_process_confirmation(&state.waiting_qos1, confirm) {
                        confirm.in_progress = true;
                        // mark qos1 message as waiting to prevent sending puback multiple times
                        state.waiting_qos1.insert(confirm.message_id, true);
                        return PollResponse::Confirmation(confirm.clone());
                    }
                }
            }
        }
        PollResponse::None
    }
}

/// Release Message from queue
#[derive(Serialize, Deserialize)]
pub struct Release(
    pub Uuid,
    /// QoS
    pub u8,
    /// message_id of message with QoS > 0
    pub Option<u16>,
    pub Vec<WriterRef>,
);
impl ProcessRequest<Release> for CoordinatorProcess {
    type Response = bool;

    fn handle(
        state: &mut CoordinatorProcess,
        Release(id, qos, message_id, inactive_subs): Release,
    ) -> Self::Response {
        if qos == 1 {
            state.wal.append_deletion(id, SystemTime::now());
            state.wal.append_completion(id, SystemTime::now());
            if let Some(publish) = state.get_by_uuid(id) {
                state.drop_inactive_subs(publish.queue_id, inactive_subs);
            }
            state.waiting_qos1.remove(&message_id.unwrap());
        }
        println!("[Coordinator->Release] dropping message {}", id);
        state.drop_message_uuid(id);
        true
    }
}

#[derive(Serialize, Deserialize)]
pub enum RetryLater {
    Publish(Uuid, Vec<WriterRef>),
}
impl ProcessRequest<RetryLater> for CoordinatorProcess {
    type Response = bool;

    fn handle(state: &mut CoordinatorProcess, msg: RetryLater) -> Self::Response {
        if let RetryLater::Publish(uuid, inactive_subs) = msg {
            for msg in state.messages.iter_mut() {
                match msg {
                    QueueMessage::Publish(publish) => {
                        if publish.message_uuid == uuid {
                            publish.in_progress = false;
                            let queue = state.topic_tree.get_by_id(publish.queue_id);
                            queue.drop_inactive_subs(inactive_subs);
                            return true;
                        }
                    }
                    QueueMessage::Confirmation(confirm) => {
                        // println!(
                        //     "[Coordinator->Poll] Checking confirmation message {:?} | {:?}",
                        //     confirm,
                        //     state.waiting_qos1.contains_key(&confirm.message_id)
                        // );
                        // if !confirm.in_progress && !state.waiting_qos1.contains_key(&confirm.message_id)
                        // {
                        //     confirm.in_progress = true;
                        //     // mark qos1 message as waiting to prevent sending puback multiple times
                        //     state.waiting_qos1.insert(confirm.message_id, true);
                        //     return PollResponse::Confirmation(confirm.clone());
                        // }
                    }
                }
            }
        }
        false
    }
}

/// Mark Message sent from to client
#[derive(Serialize, Deserialize)]
pub struct Sent(pub Uuid, pub Vec<WriterRef>);
impl ProcessRequest<Sent> for CoordinatorProcess {
    type Response = bool;

    fn handle(state: &mut CoordinatorProcess, Sent(uuid, inactive_subs): Sent) -> Self::Response {
        state.wal.append_sent(uuid, SystemTime::now());
        if let Some(msg) = state.get_by_uuid_mut(uuid) {
            let queue_id = msg.queue_id;
            // write to log and mark message as sent
            msg.sent = true;
            state.drop_inactive_subs(queue_id, inactive_subs);
            return true;
        }
        eprintln!(
            "[Coordinator->Sent] failed to get message that was sent {} | {:?}",
            uuid, state.messages
        );
        false
    }
}
