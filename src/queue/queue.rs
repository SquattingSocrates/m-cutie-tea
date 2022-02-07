use lunatic::Mailbox;
use queue_file::QueueFile;
use std::collections::VecDeque;

use crate::structure::*;
use mqtt_packet_3_5::{Packet, PublishPacket, SubackPacket};

pub struct MessageStore {
    // queue: QueueFile
    queue: VecDeque<(WriterProcess, Vec<u8>, u16)>,
}

impl MessageStore {
    pub fn new(name: &str) -> MessageStore {
        // MessageStore {queue: QueueFile::open(name).unwrap()}
        MessageStore {
            queue: VecDeque::new(),
        }
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
}

pub struct Queue {
    name: String,
    buf: MessageStore,
    // buf: HashMap<u16, (WriterProcess, Vec<u8>)>,
    pub subscribers: VecDeque<(u8, WriterProcess)>,
    retained_msg: Option<PublishPacket>,
}

impl Queue {
    pub fn new(name: &str) -> Queue {
        Queue {
            name: name.to_string(),
            buf: MessageStore::new(""),
            // buf: HashMap::new(),
            retained_msg: None,
            subscribers: VecDeque::new(),
        }
    }

    pub fn handle_qos0(&mut self, packet: &PublishPacket, protocol_version: u8) {
        let encoded = packet.encode(protocol_version).unwrap();
        for (_, sub) in self.subscribers.iter() {
            if let Err(e) = sub.request(WriterMessage::Publish(encoded.clone())) {
                eprintln!("Failed to send QoS 0 packet to {:?}. Details: {:?}", sub, e);
            }
        }
    }

    pub fn handle_qos1(
        &mut self,
        packet: &PublishPacket,
        publisher: WriterProcess,
        protocol_version: u8,
    ) {
        let encoded = packet.encode(protocol_version).unwrap();
        // let mut wrote_once = false;
        let message_id = packet.message_id.unwrap();
        self.buf
            .push(publisher.clone(), encoded.clone(), message_id);

        self.send_messages();
    }

    fn send_messages(&mut self) {
        println!(
            "[Queue] subscribers before sending {:?} {:?}",
            self.subscribers.len(),
            self.buf.len()
        );
        // let mut to_remove = vec![];
        // for (message_id, (publisher, packet)) in self.buf.iter() {
        for _ in 0..self.buf.len() {
            // preempt if no subscribers present
            if self.subscribers.is_empty() {
                return;
            }
            println!("IN NON-EMPTY BUF");
            if let Some((publisher, packet, message_id)) = self.buf.peek() {
                let mut puback_sent = false;
                for _ in 0..self.subscribers.len() {
                    let (qos, sub) = self.subscribers.pop_front().unwrap();
                    // if *qos == 0 {
                    //     continue;
                    // }
                    // make sure we write puback once and continue trying to publish
                    println!("WRITING TO SUB");
                    if let Ok(WriterResponse::Sent) =
                        sub.request(WriterMessage::Publish(packet.to_vec()))
                    {
                        println!("Sent QoS 1 packet to {:?}", sub);
                        // write back subscriber
                        self.subscribers.push_back((qos, sub));
                        if let (false, Ok(_)) = (
                            puback_sent,
                            publisher.request(WriterMessage::Puback(*message_id)),
                        ) {
                            puback_sent = true;
                            // to_remove.push(sub);
                        }
                    } else {
                        // remove subscriber since it probably disconnected
                        eprintln!("\nNOT SENT {:?}\n", message_id);
                    }
                }
                // if no puback was sent but subscribers are not empty,
                // remove message
                if !puback_sent && !self.buf.is_empty() {
                    return;
                }
                if let None = self.buf.poll() {
                    eprintln!("[Queue {}] Failed to poll from message store", self.name)
                }
            }
        }
        println!(
            "[Queue] subscribers after sending {:?} {:?}",
            self.subscribers.len(),
            self.buf.len()
        );
        // self.subscribers.filter(|(qos, sub)| !to_remove.contains(&sub));
    }

    pub fn handle_qos2(
        &mut self,
        packet: &PublishPacket,
        publisher: WriterProcess,
        protocol_version: u8,
    ) {
    }
}

pub fn new_queue(name: String, mailbox: Mailbox<QueueRequest>) {
    let mut q = Queue::new(&name);
    loop {
        // q.send_messages();
        match mailbox.receive() {
            Ok(data) => {
                println!("[Queue {}] Received mqtt message {:?}", name, data);
                match data {
                    QueueRequest::Publish(mut packet, client_id, publisher, protocol_version) => {
                        // QoS 0 - fire and forget
                        println!("SUBSCRIBERS IN LIST: {}", q.subscribers.len());
                        let orig_qos = packet.qos;
                        // we want to send a QoS 0 publish to the subscribers
                        // because we don't want to receive
                        // packet.qos = 0;
                        match orig_qos {
                            0 => q.handle_qos0(&packet, protocol_version),
                            1 => q.handle_qos1(&packet, publisher, protocol_version),
                            2 => q.handle_qos2(&packet, publisher, protocol_version),
                            _ => eprintln!("Should never happen, QoS > 2"),
                        }
                        // for (sub, proc) in q.subscribers.iter() {
                        //     if packet.fixed.retain {
                        //         retained_msg = Some((packet.message_id, packet.payload.clone()));
                        //     }
                        //     proc.send(WriterMessage::Queue(MqttPacket::Publish(packet.clone())));
                        // }
                    }
                    QueueRequest::Subscribe(qos, message_id, writer) => {
                        q.subscribers.push_back((qos, writer.clone()));
                        // send retained message on new subscription
                        if let Err(e) = writer.request(WriterMessage::Suback(SubackPacket {
                            granted: vec![],
                            granted_reason_codes: vec![],
                            message_id: message_id,
                            reason_code: Some(0),
                            properties: None,
                        })) {
                            eprintln!("Failed to write suback: {:?}", e)
                        } else {
                            q.send_messages();
                        }
                    }
                    QueueRequest::Unsubscribe(unsub) => {
                        // q.subscribers
                        //     .retain(|(_, writer)| writer.id() != unsub.id());
                    }
                }
            }
            Err(e) => println!("[Queue {}] Error while receiving message {:?}", name, e),
        }
    }
}
