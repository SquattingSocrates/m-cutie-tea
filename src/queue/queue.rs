use super::message_store::{MessageStore, QueuedMessage};
use lunatic::{process, Mailbox};
use std::collections::VecDeque;

use crate::structure::*;
use mqtt_packet_3_5::{ConfirmationPacket, Packet, PacketType, PublishPacket, SubackPacket};

pub struct QueueState {
    name: String,
    buf: MessageStore,
    // buf: HashMap<u16, (SessionProcess, Vec<u8>)>,
    pub subscribers: VecDeque<(u8, SessionProcess, WriterProcess)>,
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

    /// sends messages directly to writer process
    pub fn handle_qos0(&mut self, packet: &PublishPacket, protocol_version: u8) {
        let encoded = packet.encode(protocol_version).unwrap();
        for (_, _, writer) in self.subscribers.iter() {
            if let Err(e) = writer.request(WriterMessage::Publish(encoded.clone())) {
                eprintln!(
                    "Failed to send QoS 0 packet to {:?}. Details: {:?}",
                    writer, e
                );
            }
        }
    }

    pub fn enqueue_message(
        &mut self,
        packet: &PublishPacket,
        publisher: SessionProcess,
        protocol_version: u8,
    ) {
        let encoded = packet.encode(protocol_version).unwrap();
        // let mut wrote_once = false;
        let message_id = packet.message_id.unwrap();
        self.buf
            .push(publisher.clone(), encoded.clone(), message_id, packet.qos);

        self.send_messages();
    }

    fn send_messages(&mut self) {
        println!(
            "[Queue {}] subscribers before sending {:?} {:?}\n\n",
            self.name,
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
            if let Some(QueuedMessage {
                publisher,
                message,
                message_id,
                qos,
                ..
            }) = self.buf.peek()
            {
                let mut wrote_message = false;
                let qos = *qos;
                for _ in 0..self.subscribers.len() {
                    let (sub_qos, sub, _w) = self.subscribers.pop_front().unwrap();
                    // if *qos == 0 {
                    //     continue;
                    // }
                    // make sure we write puback once and continue trying to publish
                    println!(
                        "[Queue {}] WRITING TO SUB {:?} {:?}",
                        self.name, message_id, publisher
                    );
                    if let Ok(SessionResponse::Success) =
                        sub.request(SessionRequest::PublishBroker(
                            qos,
                            *message_id,
                            message.to_vec(),
                            self.process.clone(),
                        ))
                    {
                        println!(
                            "[Queue {}] Sent message {} with QoS {} packet to {:?}",
                            self.name, message_id, qos, sub
                        );
                        if !wrote_message {
                            let res = if qos == 1 {
                                publisher.request(SessionRequest::Confirmation(
                                    ConfirmationPacket::puback_v3(*message_id),
                                    MessageSource::Server,
                                ))
                            } else {
                                publisher.request(SessionRequest::Confirmation(
                                    ConfirmationPacket::pubrec_v3(*message_id),
                                    MessageSource::Server,
                                ))
                            };
                            if let Ok(_) = res {
                                wrote_message = true;
                            }
                        }
                        // push subscriber back into queue, otherwise it
                        // gets removed. Should probably delete a subscriber only
                        // if they are disconnected and have clean_session = true
                        self.subscribers.push_back((qos, sub, _w));
                    }
                }
                if wrote_message {
                    self.buf.mark_sent_msg(*message_id);
                }
                // eprintln!("[Queue {}] Failed to poll from message store", self.name);
            }
        }
        println!(
            "[Queue] subscribers after sending {:?} {:?}",
            self.subscribers.len(),
            self.buf.len()
        );
        // self.subscribers.filter(|(qos, sub)| !to_remove.contains(&sub));
    }

    pub fn register_confirmation(&mut self, packet_type: PacketType, msg_id: u16) {
        match packet_type {
            PacketType::Puback => self.buf.delete_msg(msg_id),
            PacketType::Pubrec => {
                println!("Received pubrec for msg {}", msg_id);
                self.buf.delete_msg(msg_id);
            }
            // PacketType::Pubrel => self.buf.set_pubrel(msg_id),
            PacketType::Pubcomp => println!("Received pubrec for msg {}", msg_id),
            t => println!("Received other PacketType {:?} | {}", t, msg_id),
        }
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
                            1 | 2 => q.enqueue_message(&packet, publisher, protocol_version),
                            _ => eprintln!("Should never happen, QoS > 2"),
                        }
                        // for (sub, proc) in q.subscribers.iter() {
                        //     if packet.fixed.retain {
                        //         retained_msg = Some((packet.message_id, packet.payload.clone()));
                        //     }
                        //     proc.send(WriterMessage::Queue(MqttPacket::Publish(packet.clone())));
                        // }
                    }
                    QueueRequest::Subscribe(qos, message_id, session, writer) => {
                        q.subscribers
                            .push_back((qos, session.clone(), writer.clone()));
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
                        q.subscribers
                            .retain(|(_, session, writer)| writer.id() != unsub.id());
                    }
                    QueueRequest::Confirmation(packet_type, msg_id) => {
                        q.register_confirmation(packet_type, msg_id)
                    }
                }
            }
            Err(e) => println!("[Queue {}] Error while receiving message {:?}", name, e),
        }
    }
}
