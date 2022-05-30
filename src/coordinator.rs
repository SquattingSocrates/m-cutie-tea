use std::collections::HashMap;

use crate::client::ClientProcess;
use crate::message_store::MessageStore;
use crate::metrics::{self, MetricsProcess};
use crate::persistence::{self, FileLog};
use crate::structure::{
    Client, CompletionMessage, ConfirmationMessage, PublishContext, PublishJob, PublishMessage,
    QueueMessage, Receiver, ReleaseMessage, WriterRef,
};
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
    messages: MessageStore,
    clients: HashMap<String, Client>,
    metrics: ProcessRef<MetricsProcess>,
    topic_tree: TopicTree,
    wal: FileLog,
}

impl CoordinatorProcess {
    pub fn handle_pubrel(
        &mut self,
        pubrel: ConfirmationPacket,
        message_id: u16,
        message_uuid: Uuid,
    ) -> bool {
        println!(
            "[Coordinator->Confirmation] Received PUBREL for {}. Releasing message {:?}",
            message_uuid, pubrel
        );
        self.wal
            .append_confirmation(message_uuid, pubrel.clone(), SystemTime::now());
        println!(
                "[Coordinator->Confirmation] Received PUBREL for {}. Marking message to be released {:?}",
                message_id, self.messages
            );
        self.messages.mark_to_be_released(message_id, message_uuid);
        true
    }

    pub fn handle_pubrec(
        &mut self,
        _: ConfirmationPacket,
        message_id: u16,
        message_uuid: Uuid,
        subscriber: WriterRef,
    ) -> bool {
        // when we receive a PUBREC we can delete the message on the broker side
        self.wal.append_deletion(message_uuid, SystemTime::now());
        self.messages
            .delete_qos2_message(message_id, message_uuid, subscriber);
        println!(
                "[Coordinator->Confirmation] Received PUBREC from subscriber for {}. Deleting message {:?}",
                message_id, self.messages
            );
        true
    }

    pub fn handle_pubcomp(
        &mut self,
        _: ConfirmationPacket,
        message_id: u16,
        message_uuid: Uuid,
        subscriber: WriterRef,
    ) -> bool {
        // when we receive a PUBCOMP we can delete the message on the broker side
        self.wal.append_completion(message_uuid, SystemTime::now());
        // drop all remaining messages
        self.messages.drop_messages_by_uuid(message_uuid);
        println!(
                "[Coordinator->Confirmation] Received PUBCOMP from subscriber for {}. Completing message {:?}",
                message_id, self.messages
            );
        true
    }

    pub fn handle_puback(
        &mut self,
        puback: ConfirmationPacket,
        message_id: u16,
        message_uuid: Uuid,
        subscriber: WriterRef,
    ) -> bool {
        // let queue = state.topic_tree.get_by_name(packet.message_id);
        println!(
            "[Coordinator->Confirmation] Received PUBACK for {}. Releasing message {:?}",
            message_uuid, puback
        );
        self.wal
            .append_confirmation(message_uuid, puback.clone(), SystemTime::now());
        println!(
            "[Coordinator->Confirmation] Wrote to WAL, getting sender {} {:?}",
            message_uuid, self.messages
        );

        self.messages
            .insert_confirmation_message(message_id, message_uuid, puback)
    }

    pub fn drop_inactive_subs(&mut self, queue_id: u128, inactive_subs: Vec<WriterRef>) {
        let queue = self.topic_tree.get_by_id(queue_id);
        queue.drop_inactive_subs(inactive_subs);
    }
}

impl AbstractProcess for CoordinatorProcess {
    type Arg = ();
    type State = Self;

    fn init(_: ProcessRef<Self>, _: Self::Arg) -> Self::State {
        // Coordinator shouldn't die when a client dies. This makes the link one-directional.
        unsafe { host::api::process::die_when_link_dies(0) };

        let mut topic_tree = TopicTree::default();
        let mut message_queue = Vec::new();
        let mut messages = HashMap::new();
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
                    message_queue.push(QueueMessage::Publish(PublishMessage {
                        message_uuid: publish.uuid,
                        message_id: publish.packet.message_id,
                        // packet: publish.packet,
                        queue_id: queue.id,
                        in_progress: false,
                        sent: false,
                        // sender: WriterRef {
                        //     client_id,
                        //     process: None,
                        //     session_id: publish.session.uuid,
                        //     is_persistent_session: publish.session.is_persistent,
                        // },
                        // started_at: publish.received_at,
                        // receivers: vec![],
                    }));
                }
                persistence::Entry::Accepted(acc) => {
                    if let Some(publish) =
                        MessageStore::get_message_by_uuid_mut(&mut message_queue, acc.uuid)
                    {
                        // if qos 1 then we are done and can actually delete the message, but
                        // maybe still need to try and deliver the puback to the writer once
                        // he reconnects
                    }
                }
                persistence::Entry::Sent(entry) => {
                    if let Some(publish) =
                        MessageStore::get_message_by_uuid_mut(&mut message_queue, entry.uuid)
                    {
                        // write to log and mark message as sent
                        publish.sent = true;
                    }
                }
                persistence::Entry::Deleted(entry) => {}
                persistence::Entry::Completed(complete) => {
                    // delete message from messages
                    MessageStore::delete_messages_by_uuid(&mut message_queue, complete.uuid);
                }
            }
        }

        CoordinatorProcess {
            topic_tree,
            wal: FileLog::new("persistence", "backup.log"),
            messages: MessageStore::new(messages, message_queue, message_ids),
            clients: HashMap::new(),
            metrics: ProcessRef::<MetricsProcess>::lookup("metrics").unwrap(),
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
        state.messages.update_message_publisher_refs(&writer);
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
        println!("Getting process {:?}", writer.process);
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
        let message_uuid = state.messages.register_message_id(packet.message_id);
        if packet.qos > 0 {
            state
                .wal
                .append_publish(message_uuid, packet.clone(), &writer, started_at);
        }
        let queue = state.topic_tree.get_by_name(packet.topic.clone());
        println!(
            "[Coordinator->Publish] Adding publish message to message queue {} {:?}",
            packet.topic, queue
        );
        state
            .messages
            .insert_publish_message(message_uuid, packet, queue.id, writer, started_at);
        true
    }
}

/// ConfirmationPacket message
#[derive(Serialize, Deserialize)]
pub struct Confirm(pub ConfirmationPacket, pub WriterRef);

impl ProcessRequest<Confirm> for CoordinatorProcess {
    type Response = bool;

    fn handle(
        state: &mut CoordinatorProcess,
        Confirm(packet, subscriber): Confirm,
    ) -> Self::Response {
        // do qos 1 flow
        let message_id = packet.message_id;
        let message_uuid = state.messages.lookup_uuid(message_id);
        if packet.cmd == PacketType::Puback {
            return state.handle_puback(packet, message_id, message_uuid, subscriber);
        } else if packet.cmd == PacketType::Pubrel {
            // pubrel will never be sent by the subscriber, only the publisher
            return state.handle_pubrel(packet, message_id, message_uuid);
        } else if packet.cmd == PacketType::Pubrec {
            return state.handle_pubrec(packet, message_id, message_uuid, subscriber);
        } else if packet.cmd == PacketType::Pubcomp {
            return state.handle_pubcomp(packet, message_id, message_uuid, subscriber);
        }
        // state.wal.append_confirmation(packet.clone());
        // let queue = state.topic_tree.get_by_name(packet.message_id);
        // println!(
        //     "[Coordinator->Confirmation] Dropping message {} {:?}",
        //     message_id, packet
        // );
        state.messages.drop_publish_message_id(message_id);
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
    Publish(PublishJob, PublishContext),
    Confirmation(ConfirmationMessage, PublishContext),
    Complete(CompletionMessage, PublishContext),
    Release(ReleaseMessage, PublishContext),
}
impl ProcessRequest<Poll> for CoordinatorProcess {
    type Response = PollResponse;

    fn handle(state: &mut CoordinatorProcess, _: Poll) -> Self::Response {
        state.messages.poll(&mut state.topic_tree)
        //     Some(QueueMessage::Publish(message)) => {
        //         let queue_id = message.queue_id;
        //         PollResponse::Publish(PublishJob {
        //             message,
        //             queue: state.topic_tree.get_by_id(queue_id).clone(),
        //         })
        //     }
        //     Some(QueueMessage::Confirmation(confirmation)) => {
        //         PollResponse::Confirmation(confirmation)
        //     }
        //     Some(QueueMessage::Complete(complete)) => PollResponse::Complete(complete),
        //     None => PollResponse::None,
        // }
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
    /// vec of invalid subs that we have not been able to send messages to
    pub Vec<WriterRef>,
    /// vec of receivers that will be used for session state tracking
    /// as well as metrics
    pub Vec<Receiver>,
);
impl ProcessRequest<Release> for CoordinatorProcess {
    type Response = bool;

    fn handle(
        state: &mut CoordinatorProcess,
        Release(id, qos, message_id, inactive_subs, receivers): Release,
    ) -> Self::Response {
        println!(
            "[Coordinator->Release] Going to release message {:?} | {:?} | qos: {}",
            id, message_id, qos
        );
        if qos == 1 {
            state.wal.append_deletion(id, SystemTime::now());
            state.wal.append_completion(id, SystemTime::now());
            // state.waiting_qos1.remove(&message_id.unwrap());
        } else if qos == 2 {
            // deletion of qos 2 message happens elsewhere
            state.wal.append_completion(id, SystemTime::now());
            // state.waiting_qos2.remove(&message_id.unwrap());
        }
        if let Some(queue_id) = state.messages.get_queue_id(id) {
            state.drop_inactive_subs(queue_id, inactive_subs);
        }
        println!("[Coordinator->Release] dropping message {}", id);
        state.messages.drop_messages_by_uuid(id);
        true
    }
}

/// Complete QoS 2 message flow by sending Pubcomp
#[derive(Serialize, Deserialize)]
pub struct Complete(
    pub Uuid,
    /// message_id of message with QoS > 0
    pub u16,
);
impl ProcessRequest<Complete> for CoordinatorProcess {
    type Response = bool;

    fn handle(
        state: &mut CoordinatorProcess,
        Complete(message_uuid, message_id): Complete,
    ) -> Self::Response {
        println!(
            "[Coordinator->Complete] Going to complete qos 2 message {:?} | {:?}",
            message_uuid, message_id
        );
        if state
            .messages
            .insert_completion_message(message_id, message_uuid)
        {
            println!(
                "[Coordinator->Complete] Added PUBCOMP for {}. Completing message flow {:?}",
                message_id, state.messages
            );
            return true;
        }
        // state.wal.append_completion(message_uuid, SystemTime::now());
        // println!("[Coordinator->Complete] dropping message {}", id);
        // state.messages.drop_messages_by_uuid(id);
        // true
        false
    }
}

/// Complete QoS 2 message flow
#[derive(Serialize, Deserialize)]
pub struct Cleanup(
    pub Uuid,
    /// message_id of message with QoS > 0
    pub u16,
    /// qos of message
    pub u8,
);
impl ProcessRequest<Cleanup> for CoordinatorProcess {
    type Response = bool;

    fn handle(
        state: &mut CoordinatorProcess,
        Cleanup(message_uuid, message_id, qos): Cleanup,
    ) -> Self::Response {
        println!(
            "[Coordinator->Cleanup] Going to complete qos 2 message {:?} | {:?}",
            message_uuid, message_id
        );
        // state.wal.append_completion(message_uuid, SystemTime::now());
        // println!("[Coordinator->Release] dropping message {}", id);
        state
            .messages
            .cleanup_message(message_uuid, message_id, qos);
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
        state
            .messages
            .retry_message_later(msg, &mut state.topic_tree)
    }
}

/// Mark Message sent from to client
#[derive(Serialize, Deserialize)]
pub struct Sent(
    pub u16,
    pub Uuid,
    /// level of QoS
    pub u8,
    /// list of inactive subs
    pub Vec<WriterRef>,
    /// list of subs to which a message was sent
    pub Vec<Receiver>,
);
impl ProcessRequest<Sent> for CoordinatorProcess {
    type Response = bool;

    fn handle(
        state: &mut CoordinatorProcess,
        Sent(message_id, message_uuid, qos, inactive_subs, receivers): Sent,
    ) -> Self::Response {
        state.wal.append_sent(message_uuid, SystemTime::now());

        if let Some(queue_id) = state.messages.mark_sent(message_uuid, &receivers) {
            state.drop_inactive_subs(queue_id, inactive_subs);
        } else {
            eprintln!(
                "[Coordinator->Sent] failed to get message that was sent {} | {:?}",
                message_uuid, state.messages
            );
            return false;
        }
        if qos == 2 {
            state.messages.insert_confirmation_message(
                message_id,
                message_uuid,
                ConfirmationPacket {
                    cmd: PacketType::Pubrec,
                    message_id,
                    puback_reason_code: None,
                    pubcomp_reason_code: None,
                    properties: None,
                },
            );
        }
        true
    }
}
