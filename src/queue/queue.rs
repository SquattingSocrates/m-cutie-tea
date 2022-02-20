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
            // println!("[Queue {}] Trying to send message to sub", self.name);
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
                let mut chosen_sub = None;
                for _ in 0..self.subscribers.len() {
                    let (sub_qos, sub, _w) = self.subscribers.pop_front().unwrap();
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
                            println!(
                                "[Queue {}] Going to send server confirmation {} {}",
                                self.name, qos, message_id
                            );
                            let res = if qos == 1 {
                                let conf = ConfirmationPacket::puback_v3(*message_id);
                                publisher.request(SessionRequest::ConfirmationServer(
                                    conf,
                                    self.process.clone(),
                                ))
                            } else {
                                let conf = ConfirmationPacket::pubrec_v3(*message_id);
                                publisher.request(SessionRequest::ConfirmationServer(
                                    conf,
                                    self.process.clone(),
                                ))
                            };
                            if let Ok(_) = res {
                                chosen_sub = Some(sub.clone());
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
                    self.buf.mark_sent_msg(*message_id, chosen_sub);
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

    pub fn register_confirmation(
        &mut self,
        packet_type: PacketType,
        msg_id: u16,
        publisher: SessionProcess,
    ) {
        println!(
            "[Queue {}] registering confirmation {:?} {:?}",
            self.name, packet_type, msg_id
        );
        match packet_type {
            PacketType::Puback => self.buf.delete_msg(msg_id),
            PacketType::Pubrec => {
                println!("[Queue {}] Received pubrec for msg {}", self.name, msg_id);
                // check whether a pubrel has been received from the publisher
                if let Some(sub) = self.buf.set_msg_state(msg_id, MessageEvent::Pubrec) {
                    sub.request(SessionRequest::ConfirmationServer(
                        ConfirmationPacket::pubrel_v3(msg_id),
                        self.process.clone(),
                    ));
                }
            }
            // pubrel comes from publisher and we need to send a pubrel
            // to the subscriber IF the subscriber sent us back a PUBREC
            PacketType::Pubrel => {
                println!("[Queue {}] Received pubrel for msg {}", self.name, msg_id);
                if let Some(sub) = self.buf.set_msg_state(msg_id, MessageEvent::Pubrel) {
                    sub.request(SessionRequest::ConfirmationServer(
                        ConfirmationPacket::pubrel_v3(msg_id),
                        self.process.clone(),
                    ));
                    publisher.request(SessionRequest::ConfirmationServer(
                        ConfirmationPacket::pubcomp_v3(msg_id),
                        self.process.clone(),
                    ));
                }
            }
            PacketType::Pubcomp => println!("[Queue] Received pubcomp for msg {}", msg_id),
            t => println!("[Queue] Received other PacketType {:?} | {}", t, msg_id),
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
                    QueueRequest::Confirmation(packet_type, msg_id, publisher) => {
                        q.register_confirmation(packet_type, msg_id, publisher)
                    }
                }
            }
            Err(e) => println!("[Queue {}] Error while receiving message {:?}", name, e),
        }
    }
}
