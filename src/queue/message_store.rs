use crate::structure::*;
use std::collections::{HashMap, VecDeque};

#[derive(Default)]
pub struct MessageStore {
    // queue: QueueFile
    queue: VecDeque<(WriterProcess, Vec<u8>, u16)>,
    pubacks: HashMap<u16, Vec<u8>>,
}

impl MessageStore {
    pub fn new(name: &str) -> MessageStore {
        // MessageStore {queue: QueueFile::open(name).unwrap()}
        MessageStore::default()
    }

    pub fn push(&mut self, publisher: WriterProcess, packet: Vec<u8>, message_id: u16) {
        self.queue.push_back((publisher, packet, message_id))
        // let mut encoded = Vec::with_capacity(8 + 2 + packet.len());
        // let mut mask =
        // self.queue.add(encoded).unwrap();
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn peek(&self) -> Option<&(WriterProcess, Vec<u8>, u16)> {
        self.queue.get(0)
    }

    pub fn poll(&mut self) -> Option<(WriterProcess, Vec<u8>, u16)> {
        self.queue.pop_front()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn release_qos1(&mut self, msg_id: u16) {
        println!("Releasing qos1 message {}", self.pubacks.len());
        self.pubacks.remove(&msg_id);
        println!("DONE Releasing qos1 message {}", self.pubacks.len());
    }

    pub fn wait_puback(&mut self, msg_id: u16, msg: Vec<u8>) {
        self.pubacks.insert(msg_id, msg);
    }
}
