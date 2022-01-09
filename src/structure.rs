use crate::mqtt::message::{
    ConnectPayload, ConnectVariableHeader, FixedHeader, PublishVariableHeader, SubscriptionRequest,
};
use lunatic::{net, process::Process};
use serde::{Deserialize, Serialize};

use crate::{Blob, MessageID, QoS, Topic};

#[derive(Debug, Serialize, Deserialize)]
pub enum QueueRequest {
    Publish(FixedHeader, PublishVariableHeader, Blob),
    Subscribe(Subscription),
    Unsubscribe(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum BrokerRequest {
    GetQueue(Topic),
    Subscribe(String, Vec<SubscriptionRequest>, Process<WriterMessage>),
    RegisterSession(String, Process<ConnectionConfig>),
    HasProcess(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum BrokerResponse {
    MatchingQueue(Queue),
    Subscribed,
    Registered,
    ExistingSession(Option<Process<ConnectionConfig>>),
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub client_id: String,
    // topic: String,
    // All channels that the client joined
    pub process: Process<WriterMessage>,
    pub qos: u8,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Queue {
    pub name: String,
    pub process: Process<QueueRequest>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WriterQueueResponse {
    Publish(MessageID, Topic, Blob, QoS),
    Subscribe(MessageID, Vec<SubscriptionRequest>),
    Unsubscribe(MessageID, Vec<Topic>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ConnectionMessage {
    Ping,
    Disconnect,
    Connect(u8),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WriterMessage {
    Queue(WriterQueueResponse),
    Connection(ConnectionMessage, Option<net::TcpStream>),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConnectionConfig {
    pub stream: net::TcpStream,
    pub variable_header: ConnectVariableHeader,
    pub payload: ConnectPayload,
}
