use std::collections::HashMap;

use crate::client::{ClientProcess, WriterProcess};
use crate::persistence::FileLog;
use crate::structure::{ConfirmationMessage, PublishJob, PublishMessage, QueueMessage};
use crate::topic_tree::TopicTree;
use lunatic::{
    host,
    process::{AbstractProcess, ProcessMessage, ProcessRef, ProcessRequest, Request, StartProcess},
    supervisor::Supervisor,
};
use mqtt_packet_3_5::{
    ConfirmationPacket, Granted, MqttPacket, PacketType, PublishPacket, SubackPacket,
    SubscribePacket,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// A reference to a client that joined the server.
struct Client {
    // username: String,
    pub client: ProcessRef<ClientProcess>,
    pub writer: ProcessRef<WriterProcess>,
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
    clients: HashMap<u128, Client>,
    topic_tree: TopicTree,
    wal: FileLog,
    waiting_qos1: HashMap<u16, bool>, // channels: HashMap<String, (ProcessRef<ChannelProcess>, usize)>,
}

impl CoordinatorProcess {
    pub fn drop_message_uuid(&mut self, message_uuid: Uuid) {
        self.messages.retain(|msg| {
            if let QueueMessage::Publish(p) = msg {
                return p.message_id != message_uuid;
            }
            true
        });
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

    pub fn get_by_message_id(&mut self, message_id: u16) -> Option<&mut PublishMessage> {
        for msg in self.messages.iter_mut() {
            if let QueueMessage::Publish(p) = msg {
                if p.packet.message_id != Some(message_id) {
                    return Some(p);
                }
            }
        }
        None
    }
}

impl AbstractProcess for CoordinatorProcess {
    type Arg = ();
    type State = Self;

    fn init(_: ProcessRef<Self>, _: Self::Arg) -> Self::State {
        // Coordinator shouldn't die when a client dies. This makes the link one-directional.
        unsafe { host::api::process::die_when_link_dies(0) };

        CoordinatorProcess {
            topic_tree: TopicTree::default(),
            messages: Vec::new(),
            clients: HashMap::new(),
            wal: FileLog::new("persistence", "backup.log"),
            waiting_qos1: HashMap::new(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Connect(
    pub ProcessRef<ClientProcess>,
    pub ProcessRef<WriterProcess>,
    pub bool,
);
impl ProcessRequest<Connect> for CoordinatorProcess {
    type Response = bool;

    fn handle(
        state: &mut Self::State,
        Connect(client, writer, should_persist): Connect,
    ) -> Self::Response {
        state
            .clients
            .insert(writer.uuid(), Client { client, writer });

        true
    }
}

#[derive(Serialize, Deserialize)]
pub struct Disconnect(pub ProcessRef<ClientProcess>);
impl ProcessMessage<Disconnect> for CoordinatorProcess {
    fn handle(state: &mut Self::State, Disconnect(client): Disconnect) {
        state.clients.remove(&client.uuid());
    }
}

/// Publish message
#[derive(Serialize, Deserialize)]
pub struct Subscribe(pub SubscribePacket, pub ProcessRef<WriterProcess>);
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
        writer.request(MqttPacket::Suback(suback))
    }
}

/// Publish message
#[derive(Serialize, Deserialize)]
pub struct Publish(pub PublishPacket, pub ProcessRef<WriterProcess>);
impl ProcessRequest<Publish> for CoordinatorProcess {
    type Response = bool;

    fn handle(state: &mut CoordinatorProcess, Publish(packet, writer): Publish) -> Self::Response {
        if packet.qos > 0 {
            state.wal.append_publish(packet.clone());
        }
        let queue = state.topic_tree.get_by_name(packet.topic.clone());
        println!(
            "[Coordinator->Publish] Adding publish message to message queue {} {:?}",
            packet.topic, queue
        );
        state.messages.push(QueueMessage::Publish(PublishMessage {
            message_id: Uuid::new_v4(),
            packet,
            queue_id: queue.id,
            in_progress: false,
            sent: false,
            sender: writer,
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
        if packet.cmd == PacketType::Puback {
            // let queue = state.topic_tree.get_by_name(packet.message_id);
            println!(
                "[Coordinator->Confirmation] Received PUBACK for {}. Releasing message {:?}",
                message_id, packet
            );
            state.wal.append_confirmation(packet.clone());
            println!(
                "[Coordinator->Confirmation] Wrote to WAL, getting sender {} {:?}",
                message_id, state.messages
            );
            let orig_sender = state.get_by_message_id(message_id).unwrap().sender.clone();

            // state.messages.retain(|msg| {
            //     if let QueueMessage::Publish(p) = msg {
            //         return p.packet.message_id != Some(packet.message_id);
            //     }
            //     true
            // });
            state
                .messages
                .push(QueueMessage::Confirmation(ConfirmationMessage {
                    message_id,
                    packet,
                    in_progress: false,
                    send_to: orig_sender,
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
                    if !publish.in_progress {
                        let queue = state.topic_tree.get_by_id(publish.queue_id);
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
                    if !confirm.in_progress && !state.waiting_qos1.contains_key(&confirm.message_id)
                    {
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
pub struct Release(pub Uuid);
impl ProcessRequest<Release> for CoordinatorProcess {
    type Response = bool;

    fn handle(state: &mut CoordinatorProcess, Release(id): Release) -> Self::Response {
        state.drop_message_uuid(id);
        true
    }
}

/// Mark Message sent from to client
#[derive(Serialize, Deserialize)]
pub struct Sent(pub u16, pub u8);
impl ProcessRequest<Sent> for CoordinatorProcess {
    type Response = bool;

    fn handle(state: &mut CoordinatorProcess, Sent(id, qos): Sent) -> Self::Response {
        state.wal.append_sent(id, qos);
        if let Some(msg) = state.get_by_message_id(id) {
            // write to log and mark message as sent
            msg.sent = true;
            return true;
        }
        panic!(
            "[Coordinator->Sent] failed to get message that was sent {} | {:?}",
            id, state.messages
        );
    }
}
