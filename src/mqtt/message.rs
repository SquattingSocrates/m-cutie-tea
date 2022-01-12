use super::flags::ControlPacketType;
use serde::{Deserialize, Serialize};

use crate::{Blob, MessageID, QoS};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum PublishSource {
    Client,
    Server,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct FixedHeader {
    pub control_packet: ControlPacketType,
    pub length: u32,
    pub dup: bool,
    pub qos: u8,
    pub retain: bool,
}

#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub struct ConnectFlags {
    pub user_name: bool,
    pub password: bool,
    pub will_retain: bool,
    pub will_qos: u8,
    pub will: bool,
    pub clean_session: bool,
}

impl ConnectFlags {
    pub fn new(byte: u8) -> ConnectFlags {
        ConnectFlags {
            user_name: (byte & (1 << 7)) != 0,
            password: (byte & (1 << 6)) != 0,
            will_retain: (byte & (1 << 5)) != 0,
            will_qos: byte & ((1 << 4) + (1 << 3)),
            will: (byte & (1 << 2)) != 0,
            clean_session: (byte & (1 << 1)) != 0,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ConnectVariableHeader {
    pub protocol_name: String,
    pub protocol_version: u32,
    pub connect_flags: ConnectFlags,
    pub keep_alive: u32,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PublishVariableHeader {
    pub topic_name: String,
    pub message_id: u32,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct MessageIDVariableHeader {
    pub message_id: u32,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum VariableHeader {
    Empty,
    Connect(ConnectVariableHeader),
    Publish(PublishVariableHeader),
    Subscribe(MessageIDVariableHeader),
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum MqttMessage {
    Connect(FixedHeader, ConnectVariableHeader, ConnectPayload),
    Connack(FixedHeader, MessageIDVariableHeader),
    Subscribe(
        FixedHeader,
        MessageIDVariableHeader,
        Vec<SubscriptionRequest>,
    ),
    Suback(FixedHeader, MessageIDVariableHeader, Vec<QoS>),
    Publish(FixedHeader, PublishVariableHeader, Blob),
    Puback(FixedHeader, MessageIDVariableHeader, MessageID),
    Pubrec(FixedHeader, MessageIDVariableHeader),
    Pubrel(FixedHeader, MessageIDVariableHeader),
    Pubcomp(FixedHeader, MessageIDVariableHeader),
    Unsubscribe(FixedHeader, MessageIDVariableHeader, Vec<String>),
    Unsuback(FixedHeader, MessageIDVariableHeader),
    Pingreq(FixedHeader),
    Pingresp(FixedHeader),
    Disconnect(FixedHeader),
    Unknown,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ConnectPayload {
    pub client_id: String,
    pub will_topic: Option<String>,
    pub will_message: Option<String>,
    pub user_name: Option<String>,
    pub password: Option<String>,
}

type DualByteNum = Vec<u8>;
type VariableNum = Vec<u8>;

impl MqttMessage {
    pub fn bytes_with_message_id(flag: u8, message_id: u32, payload: Option<Vec<u8>>) -> Vec<u8> {
        println!("Wrapping message {:?} {:?} {:?}", flag, message_id, payload);
        let flag = if flag <= 16 { flag << 4 } else { flag };
        let mut message_id = MqttMessage::encode_multibyte_num(message_id);
        let mut payload = payload.unwrap_or_default();
        let remaining_length = message_id.len() + payload.len();
        let mut length = MqttMessage::encode_variable_num(remaining_length as u32);
        println!(
            "Remaining length {} {} {:?}",
            message_id.len(),
            payload.len(),
            length
        );
        let mut v = Vec::<u8>::with_capacity(1 + length.len() + remaining_length);
        v.push(flag);
        v.append(&mut length);
        v.append(&mut message_id);
        v.append(&mut payload);
        v
    }

    pub fn encode_multibyte_num(message_id: u32) -> DualByteNum {
        // println!("SPLITTING MESSAGE_ID {}", message_id, message_id >> 8, message_id as u8);
        vec![(message_id >> 8) as u8, message_id as u8]
    }

    pub fn encode_variable_num(mut length: u32) -> VariableNum {
        let mut v = Vec::<u8>::with_capacity(4);
        while length > 0 {
            let mut next = length % 128;
            length /= 128;
            if length > 0 {
                next |= 0x80;
            }
            v.push(next as u8);
        }
        v
    }
}

pub type SubscribePayload = Vec<SubscriptionRequest>;

#[derive(Debug, PartialEq)]
pub enum MqttPayload {
    Empty,
    Connect(Vec<String>),
    Publish(String),
    Subscribe(Vec<SubscriptionRequest>),
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct SubscriptionRequest {
    pub topic: String,
    pub qos: u8,
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_byte() {
        let res = MqttMessage::encode_multibyte_num(78);
        assert_eq!(res[0], 0b0);
        assert_eq!(res[1], 0b01001110);
        assert_eq!(res.len(), 2, "Should have length of 2");
    }

    #[test]
    fn multi_byte() {
        let res = MqttMessage::encode_multibyte_num(7267);
        assert_eq!(res[0], 0b00011100);
        assert_eq!(res[1], 0b01100011);
    }
}
