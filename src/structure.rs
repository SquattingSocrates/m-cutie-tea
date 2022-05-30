use crate::client::{ClientProcess, WriterProcess};
use lunatic::process::ProcessRef;
use mqtt_packet_3_5::{ConfirmationPacket, PublishPacket};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub enum QueueResponse {
    Success,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Queue {
    pub id: u128,
    pub name: String,
    pub subscribers: Vec<WriterRef>,
}

impl Queue {
    pub fn drop_inactive_subs(&mut self, inactive_subs: Vec<WriterRef>) {
        self.subscribers.retain(|sub| !inactive_subs.contains(sub));
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum QueueMessage {
    Publish(PublishMessage),
    Confirmation(ConfirmationMessage),
    Complete(CompletionMessage),
    Release(ReleaseMessage),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ReleaseMessage {
    pub message_id: u16,
    pub message_uuid: Uuid,
    pub pubrec_received: bool,
    pub pubrel_received: bool,
}

/// A PublishMessage is what we have in the queue
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PublishMessage {
    pub message_uuid: Uuid,
    pub message_id: Option<u16>,
    pub queue_id: u128,
    pub in_progress: bool,
    pub sent: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PublishContext {
    pub packet: PublishPacket,
    /// will be initialized to empty vec if none
    /// and will contain all the subscribers to which a message was sent
    pub receivers: Vec<Receiver>,
    pub sender: WriterRef,
    pub started_at: SystemTime,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct WriterRef {
    pub process: Option<ProcessRef<WriterProcess>>,
    pub client_id: String,
    pub session_id: Uuid,
    pub is_persistent_session: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PublishJob {
    pub message: PublishMessage,
    pub queue: Queue,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ConfirmationMessage {
    pub message_uuid: Uuid,
    pub packet: ConfirmationPacket,
    pub message_id: u16,
    pub in_progress: bool,
    pub publisher: WriterRef,
    pub receivers: Vec<Receiver>,
    pub started_at: SystemTime,
    pub original_qos: u8,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CompletionMessage {
    pub message_uuid: Uuid,
    pub message_id: u16,
    pub in_progress: bool,
    pub publisher: WriterRef,
    pub receiver: Receiver,
    pub started_at: SystemTime,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Receiver {
    pub writer: WriterRef,
    pub received_qos: u8,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Qos2PublishContext {
    pub publisher: WriterRef,
    pub published_qos: u8,
    pub received_qos: u8,
    pub started_at: SystemTime,
    pub receivers: Vec<Receiver>,
}

// A reference to a client that joined the server.
pub struct Client {
    // username: String,
    pub client: ProcessRef<ClientProcess>,
    pub writer: WriterRef,
    pub should_persist: bool,
}
