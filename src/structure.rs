use lunatic::{net, process::Process, Request};
use mqtt_packet_3_5::{ConnectPacket, MqttPacket, PublishPacket, SubscribePacket};
use serde::{Deserialize, Serialize};

pub type WriterProcess = Process<Request<WriterMessage, WriterResponse>>;
pub type ReaderProcess = Process<SessionRequest>;
pub type BrokerProcess = Process<Request<BrokerRequest, BrokerResponse>>;

#[derive(Debug, Serialize, Deserialize)]
pub enum QueueRequest {
    /// WriterProcess is necessary for sending responses like Puback, Pubrel etc
    Publish(PublishPacket, WriterProcess),
    /// Send Matching subscription index as well as the subscribe packet and
    /// writer process
    Subscribe(usize, SubscribePacket, WriterProcess),
    /// Send only client_id
    Unsubscribe(WriterProcess),
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
    ExistingSession(Option<Process<SessionRequest>>),
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
    Queue(MqttPacket),
    Connection(ConnectionMessage, Option<net::TcpStream>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WriterResponse {
    Published,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SessionConfig {
    pub stream: net::TcpStream,
    pub connect_packet: ConnectPacket,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum SessionRequest {
    Create(net::TcpStream, ConnectPacket),
    Destroy,
}
