use lunatic::{net, process::Process, Request};
use mqtt_packet_3_5::{
    ConfirmationPacket, ConnackPacket, ConnectPacket, PacketType, PublishPacket, SubackPacket,
    SubscribePacket,
};
use serde::{Deserialize, Serialize};

pub type WriterProcess = Process<Request<WriterMessage, WriterResponse>>;
pub type ReaderProcess = Process<()>;
pub type BrokerProcess = Process<Request<BrokerRequest, BrokerResponse>>;
pub type QueueProcess = Process<QueueRequest>;
pub type SessionProcess = Process<Request<SessionRequest, SessionResponse>>;

#[derive(Debug, Serialize, Deserialize)]
pub enum QueueRequest {
    /// WriterProcess is necessary for sending responses like Puback, Pubrel etc
    Publish(PublishPacket, String, SessionProcess, u8),
    /// Send Matching subscription index as well as the subscribe packet and
    /// writer process
    Subscribe(u8, u16, SessionProcess, WriterProcess),
    /// Send only client_id
    Unsubscribe(WriterProcess),
    /// request type for PUBACK, PUBREL, PUBREC and PUBCOMP
    Confirmation(PacketType, u16),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum BrokerRequest {
    /// Request Queue Process for publishing by topic name
    GetQueue(String),
    Subscribe(SubscribePacket, SessionProcess, WriterProcess),
    MoveToExistingSession(SessionConfig),
    /// Receives client_id and registers new Process
    RegisterSession(String, ReaderProcess),
    DestroySession(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum BrokerResponse {
    MatchingQueue(Queue),
    Subscribed,
    Registered,
    ExistingSession(Option<ReaderProcess>),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MessageSource {
    Client,
    Server,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SessionRequest {
    Publish(PublishPacket),
    Subscribe(SubscribePacket),
    Confirmation(ConfirmationPacket, MessageSource),
    PublishBroker(u8, u16, Vec<u8>, QueueProcess),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SessionResponse {
    Success,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Queue {
    pub name: String,
    pub process: Process<QueueRequest>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ConnectionMessage {
    Ping,
    Disconnect,
    Connect(u8),
    Destroy,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WriterMessage {
    Connack(ConnackPacket),
    Suback(SubackPacket),
    Unsuback(u16),
    Publish(Vec<u8>),
    Confirmation(ConfirmationPacket),
    Pong,
    Die,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WriterResponse {
    Success,
    Sent,
    Failed,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SessionConfig {
    pub stream: net::TcpStream,
    pub connect_packet: ConnectPacket,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum PostOfficeRequest {
    Publish(u16, Vec<u8>),
}

#[derive(Debug, PartialEq)]
pub enum MessageState {
    /// Ready is the initial state of all messages
    Ready,
    /// Sent is actually only relevant for QoS 2. It means the message
    /// has been published and a PUBREC has been sent to the publisher
    Sent,
    /// ToDelete marks messages as deletable, whether it's set through an PUBACK
    /// or a PUBREC from the subscriber
    ToDelete,
}
