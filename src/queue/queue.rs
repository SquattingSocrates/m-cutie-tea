use super::message_store::MessageStore;
use lunatic::{process, Mailbox};
use std::collections::VecDeque;

use crate::structure::*;
use mqtt_packet_3_5::{Packet, PublishPacket, SubackPacket};

pub struct QueueState {
    name: String,
    buf: MessageStore,
    // buf: HashMap<u16, (WriterProcess, Vec<u8>)>,
    pub subscribers: VecDeque<(u8, WriterProcess)>,
    retained_msg: Option<PublishPacket>,
    process: QueueProcess,
}

impl QueueState {
    pub fn new(name: &str, process: QueueProcess) -> QueueState {
        QueueState {
            name: name.to_string(),
            buf: MessageStore::new(""),
            // buf: HashMap::new(),
            retained_msg: None,
            subscribers: VecDeque::new(),
            process,
        }
    }

    pub fn handle_qos0(&mut self, packet: &PublishPacket, protocol_version: u8) {
        let encoded = packet.encode(protocol_version).unwrap();
        for (_, sub) in self.subscribers.iter() {
            if let Err(e) = sub.request(WriterMessage::Publish(
                packet.qos,
                // send 0 if qos 0 because it's not going to be used anyway
                packet.message_id.unwrap_or(0),
                encoded.clone(),
                self.process.clone(),
            )) {
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
                    if let Ok(WriterResponse::Sent) = sub.request(WriterMessage::Publish(
                        1,
                        *message_id,
                        packet.to_vec(),
                        self.process.clone(),
                    )) {
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
                // if !puback_sent && !self.buf.is_empty() {
                //     return;
                // }
                if let Some((_, msg, msg_id)) = self.buf.poll() {
                    self.buf.wait_puback(msg_id, msg);
                } else {
                    eprintln!("[Queue {}] Failed to poll from message store", self.name);
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

    pub fn release_puback(&mut self, msg_id: u16) {
        self.buf.release_qos1(msg_id);
    }
}

pub fn new_queue(name: String, mailbox: Mailbox<QueueRequest>) {
    let this = process::this(&mailbox);
    let mut q = QueueState::new(&name, this);
    loop {
        // q.send_messages();
        match mailbox.receive() {
            Ok(data) => {
                println!("[Queue {}] Received mqtt message {:?}", name, data);
                match data {
                    QueueRequest::Publish(packet, client_id, publisher, protocol_version) => {
                        // QoS 0 - fire and forget
                        println!("SUBSCRIBERS IN LIST: {}", q.subscribers.len());
                        match packet.qos {
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
                    QueueRequest::Puback(msg_id) => {
                        q.release_puback(msg_id);
                    }
                }
            }
            Err(e) => println!("[Queue {}] Error while receiving message {:?}", name, e),
        }
    }
}
