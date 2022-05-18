// use super::broker::Broker;
// use super::queue::queue::QueueState;
// use super::session::tcp_writer::TcpWriter;
use crate::client::WriterProcess;
use lunatic::{
    net::TcpStream,
    process::{ProcessRef, Request},
};
use mqtt_packet_3_5::{
    ConfirmationPacket, ConnackPacket, ConnectPacket, PacketType, PublishPacket, SubackPacket,
    SubscribePacket,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// pub type WriterProcess = ProcessRef<W>;
// pub type ReaderProcess = ProcessRef<()>;
// pub type BrokerProcess = ProcessRef<Broker>;
// pub type QueueProcess = ProcessRef<QueueState>;

// #[derive(Debug, Serialize, Deserialize)]
// pub enum QueueRequest {
//     /// WriterProcess is necessary for sending responses like Puback, Pubrel etc
//     Publish(PublishPacket, String, WriterProcess, u8),
//     /// Send Matching subscription index as well as the subscribe packet and
//     /// writer process
//     Subscribe(u8, u16, WriterProcess),
//     /// Send only client_id
//     Unsubscribe(WriterProcess),
//     /// request type for PUBACK, PUBREL, PUBREC and PUBCOMP
//     Confirmation(PacketType, u16, WriterProcess),
// }

#[derive(Debug, Serialize, Deserialize)]
pub enum QueueResponse {
    Success,
}

// pub struct WriterProcess {
//     pub is_receiving: bool,
//     stream: TcpStream,
//     connect_packet: ConnectPacket,
// }

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Queue {
    pub id: u128,
    pub name: String,
    pub subscribers: Vec<ProcessRef<WriterProcess>>,
}

pub struct QueueMessage {
    pub message_id: Uuid,
    pub packet: PublishPacket,
    pub queue_id: u128,
    pub in_progress: bool,
}

// #[derive(Debug, Serialize, Deserialize)]
// pub enum BrokerRequest {
//     /// Request Queue Process for publishing by topic name
//     GetQueue(String),
//     Subscribe(SubscribePacket, WriterProcess),
//     MoveToExistingSession(SessionConfig),
//     /// Receives client_id and registers new Process
//     RegisterSession(String, ReaderProcess),
//     DestroySession(String),
// }

// #[derive(Debug, Serialize, Deserialize)]
// pub enum BrokerResponse {
//     MatchingQueue(Queue),
//     Subscribed,
//     Registered,
//     ExistingSession(Option<ReaderProcess>),
//     Destroyed,
// }

// // #[derive(Debug, Serialize, Deserialize, Clone)]
// // pub struct Queue {
// //     pub name: String,
// //     pub process: QueueProcess,
// // }

// #[derive(Serialize, Deserialize, Debug)]
// pub enum ConnectionMessage {
//     Ping,
//     Disconnect,
//     Connect(u8),
//     Destroy,
// }

// #[derive(Serialize, Deserialize, Debug)]
// pub enum WriterMessage {
//     Connack(ConnackPacket),
//     Suback(SubackPacket),
//     Unsuback(u16),
//     Publish(u8, u16, Vec<u8>, QueueProcess),
//     Confirmation(ConfirmationPacket, QueueProcess),
//     Pong,
//     Die,
//     GetQueue(u16),
// }

// #[derive(Serialize, Deserialize, Debug)]
// pub enum WriterResponse {
//     Success,
//     Sent,
//     Failed,
//     MatchingQueue(QueueProcess),
// }

// #[derive(Serialize, Deserialize, Debug, Clone)]
// pub struct SessionConfig {
//     pub stream: net::TcpStream,
//     pub connect_packet: ConnectPacket,
// }

// #[derive(Serialize, Deserialize, Debug)]
// pub enum PostOfficeRequest {
//     Publish(u16, Vec<u8>),
// }

// #[derive(Debug, PartialEq)]
// pub enum MessageEvent {
//     Send,
//     /// Receive Pubrel from publisher
//     Pubrel,
//     /// Receive Pubrec from subscriber
//     Pubrec,
// }

// #[derive(Debug, PartialEq)]
// pub enum MessageState {
//     /// Ready is the initial state of all messages
//     Ready,
//     /// Sent is actually only relevant for QoS 2. It means the message
//     /// has been published and a PUBREC has been sent to the publisher
//     /// It can transition to Released IFF we receive a PUBREC from the subscriber
//     Sent,
//     /// Waiting means that a PUBREL from the Publisher arrived before the PUBREC
//     /// of the Subscriber
//     Waiting,
//     /// Essentially means that a PUBREC from the subscriber has been received
//     Released,
//     /// Received marks messages as deletable, whether it's set through an PUBACK
//     /// or a PUBREC from the subscriber
//     Received,
// }

// impl MessageState {
//     pub fn transition(&self, event: MessageEvent) -> MessageState {
//         match (self, &event) {
//             (MessageState::Ready, MessageEvent::Send) => MessageState::Sent,
//             (MessageState::Sent, MessageEvent::Pubrec) => MessageState::Received,
//             (MessageState::Sent, MessageEvent::Pubrel) => MessageState::Waiting,
//             (MessageState::Waiting, MessageEvent::Pubrec) => MessageState::Released,
//             (MessageState::Received, MessageEvent::Pubrel) => MessageState::Released,
//             _ => panic!(
//                 "Invalid combination of message state and event {:?} | {:?}",
//                 self, event
//             ),
//         }
//     }
// }
