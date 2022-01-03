use crate::mqtt::message::{MqttMessage, SubscriptionRequest};
use lunatic::{net, process::Process, Mailbox, Request, Tag};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum QueueRequest {
    Publish(String, String, u32),
    Subscribe(Process<MqttMessage>, Vec<SubscriptionRequest>),
    Disconnect,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum QueueResponse {
    Puback(String),
    Suback,
    Publish(String, String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum BrokerRequest {
    GetQueues(String),
    Disconnect,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum BrokerResponse {
    MatchingQueues(Vec<Process<Request<QueueRequest, QueueResponse>>>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Subscription {
    pub link: Tag,
    // topic: String,
    // All channels that the client joined
    pub process: Process<MqttMessage>,
    pub qos: u8,
}

#[derive(Debug)]
pub struct QueueCtx {
    pub stream: net::TcpStream,
    pub mailbox: Mailbox<MqttMessage>,
    pub broker: Process<Request<QueueRequest, QueueResponse>>,
}

#[derive(Debug)]
pub struct Queue {
    pub name: String,
    pub subscribers: Vec<Subscription>,
}
