use crate::structure::*;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, SystemTime};

pub struct QueuedMessage {
    pub publisher: SessionProcess,
    pub message: Vec<u8>,
    pub message_id: u16,
    pub state: MessageState,
    pub qos: u8,
}

pub struct MessageStore {
    /// messages that are ready to be sent
    ready_messages: VecDeque<QueuedMessage>,
    /// stores sent messages as well as timestamp to find out whether it
    /// can be deleted due to timeout
    sent_messages: HashMap<u16, (QueuedMessage, SystemTime)>,
}

///
/// Message state handling
/// sent_messages = []
/// queue = [msg1 = QoS1, msg2 = QoS2]
/// peek(msg1) -> send(msg1) -> push(sent_messages, (msg1, timestamp))
/// peek(msg2) -> send(msg2) -> push(sent_messages, (msg2, timestamp))
/// now we have no more messages to send
/// receive_puback(msg1) -> delete(sent_messages, msg1)
/// receive_pubrec(msg2) -> delete(sent_messages, msg2) -> send_pubrel_to_subscriber(msg2) -> send_pubcomp_to_publisher(msg2)
///

impl MessageStore {
    pub fn new(name: &str) -> MessageStore {
        // MessageStore {queue: QueueFile::open(name).unwrap()}
        MessageStore {
            ready_messages: VecDeque::new(),
            sent_messages: HashMap::new(),
        }
    }

    pub fn push(&mut self, publisher: SessionProcess, message: Vec<u8>, message_id: u16, qos: u8) {
        self.ready_messages.push_back(QueuedMessage {
            publisher,
            message,
            message_id,
            state: MessageState::Ready,
            qos,
        });
    }

    pub fn len(&self) -> usize {
        self.ready_messages.len()
    }

    pub fn peek(&self) -> Option<&QueuedMessage> {
        if let Some(msg) = self.ready_messages.get(0) {
            if msg.state == MessageState::Ready {
                return Some(msg);
            }
            return Some(msg);
        }
        None
    }

    pub fn is_empty(&self) -> bool {
        self.ready_messages.is_empty()
    }

    pub fn mark_sent_msg(&mut self, msg_id: u16) {
        println!("Releasing high qos message {}", self.ready_messages.len());
        if let Some(msg) = self.ready_messages.pop_front() {
            // msg.state = state;
            self.sent_messages
                .insert(msg.message_id, (msg, SystemTime::now()));
        }
        println!(
            "DONE Releasing high qos message {}",
            self.ready_messages.len()
        );
    }

    pub fn delete_msg(&mut self, msg_id: u16) {
        if let Some(msg) = self.sent_messages.remove(&msg_id) {
            println!("Deleted message with id {:?}", msg_id);
        } else {
            println!("Failed to delete message with id {:?}", msg_id);
        }
    }
}
