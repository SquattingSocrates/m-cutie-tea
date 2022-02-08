use lunatic::{net, process::Process, Request};
use mqtt_packet_3_5::{
    ConnackPacket, ConnectPacket, PublishPacket, QoS, SubackPacket, SubscribePacket,
};
use serde::{Deserialize, Serialize};

pub type WriterProcess = Process<Request<WriterMessage, WriterResponse>>;
pub type ReaderProcess = Process<()>;
pub type BrokerProcess = Process<Request<BrokerRequest, BrokerResponse>>;
pub type QueueProcess = Process<QueueRequest>;

#[derive(Debug, Serialize, Deserialize)]
pub enum QueueRequest {
    /// WriterProcess is necessary for sending responses like Puback, Pubrel etc
    Publish(PublishPacket, String, WriterProcess, u8),
    /// Send Matching subscription index as well as the subscribe packet and
    /// writer process
    Subscribe(u8, u16, WriterProcess),
    /// Send only client_id
    Unsubscribe(WriterProcess),
    /// Release message because subscriber acknowledged message
    Puback(u16),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum BrokerRequest {
    /// Request Queue Process for publishing by topic name
    GetQueue(String),
    Subscribe(SubscribePacket, WriterProcess),
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
    Publish(u8, u16, Vec<u8>, QueueProcess),
    ReaderPuback(u16),
    ReaderPubrec(u16),
    ReaderPubcomp(u16),
    Puback(u16),
    Pubrec(u16),
    Pubcomp(u16),
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
