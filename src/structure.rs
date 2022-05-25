use crate::client::WriterProcess;
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
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PublishMessage {
    pub message_uuid: Uuid,
    pub packet: PublishPacket,
    pub queue_id: u128,
    pub in_progress: bool,
    pub sent: bool,
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
    pub send_to: WriterRef,
    pub started_at: SystemTime,
}
