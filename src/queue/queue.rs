use lunatic::Mailbox;
use std::collections::VecDeque;

use crate::structure::*;
use mqtt_packet_3_5::{
    ConfirmationPacket, FixedHeader, MqttPacket, PacketType, PubackPubrecCode, PublishPacket, QoS,
};

pub struct Queue {
    name: String,
    buf: VecDeque<(WriterProcess, PublishPacket)>,
    pub subscribers: Vec<(QoS, WriterProcess)>,
    retained_msg: Option<PublishPacket>,
}

impl Queue {
    pub fn new(name: &str) -> Queue {
        Queue {
            name: name.to_string(),
            buf: VecDeque::new(),
            retained_msg: None,
            subscribers: Vec::new(),
        }
    }

    pub fn try_flush(&mut self) {
        println!("TRYING FLUSH {:?} {:?}", self.subscribers, self.buf.len());
        if self.buf.is_empty() || self.subscribers.is_empty() {
            return;
        }
        println!("GOT SUBSCRIBERS NOW {:?}", self.subscribers);
        for (publisher, packet) in self.buf.iter() {
            let packet_qos = packet.fixed.qos;
            let message_id = packet.message_id;
            let mut wrote_once = false;
            for (qos, sub) in self.subscribers.iter() {
                println!(
                    "GETTING MESSAGE TO SUBSCRIBERS {:?} {:?} {:?}",
                    sub,
                    packet_qos,
                    qos.to_byte()
                );
                // if fixed.retain {
                //     retained_msg =
                //         Some((fixed.clone(), variable.message_id, payload.to_string()));
                // }
                // handle qos 1 messages - send at least once
                if packet_qos > qos.to_byte() {
                    println!(
                        "Cannot send QoS {} to client who subscribed with QoS {:?}",
                        packet.fixed.qos, qos
                    );
                    continue;
                }
                match sub.request(WriterMessage::Queue(MqttPacket::Publish(packet.clone()))) {
                    Ok(d) => {
                        println!("\nWROTE MESSAGE TO WRITER\n");
                        // send puback only once
                        if !wrote_once && packet_qos == 1 {
                            // let publisher = publisher.clone().unwrap();
                            // don't wait for puback
                            publisher.request(WriterMessage::Queue(MqttPacket::Puback(
                                ConfirmationPacket {
                                    fixed: FixedHeader::for_type(PacketType::Puback),
                                    // since the packet has qos 1 we trust that it
                                    // would not get parsed without a message_id
                                    message_id: message_id.unwrap(),
                                    properties: None,
                                    length: 0,
                                    puback_reason_code: Some(PubackPubrecCode::Success),
                                    pubcomp_reason_code: None,
                                },
                            )));
                        }
                        wrote_once = true;
                    }
                    Err(e) => eprintln!("Failed to write to queue {:?}", e),
                };
            }
        }
        self.buf = VecDeque::new();
    }
}

pub fn new_queue(name: String, mailbox: Mailbox<QueueRequest>) {
    let mut q = Queue::new(&name);
    loop {
        q.try_flush();
        match mailbox.receive() {
            Ok(data) => {
                println!("[Queue {}] Received mqtt message {:?}", name, data);
                match &data {
                    QueueRequest::Publish(packet, publisher) => {
                        // QoS 0 - fire and forget
                        println!("SUBSCRIBERS IN LIST: {}", q.subscribers.len());
                        // for (sub, proc) in q.subscribers.iter() {
                        //     if packet.fixed.retain {
                        //         retained_msg = Some((packet.message_id, packet.payload.clone()));
                        //     }
                        //     proc.send(WriterMessage::Queue(MqttPacket::Publish(packet.clone())));
                        // }
                        q.buf.push_back((publisher.clone(), packet.clone()));
                    }
                    QueueRequest::Subscribe(matching, packet, writer) => {
                        let sub = &packet.subscriptions[*matching];
                        q.subscribers.push((sub.qos.clone(), writer.clone()));
                        // send retained message on new subscription
                        // if let Some((message_id, payload)) = &retained_msg {
                        //     writer.send(WriterMessage::Queue(MqttPacket::Publish(PublishPacket {
                        //         fixed: FixedHeader::for_type(PacketType::Publish),
                        //         message_id: *message_id,
                        //         length: 0,
                        //         properties: None,
                        //         payload: payload.clone(),
                        //         topic: name.clone(),
                        //     })))
                        // }
                    }
                    QueueRequest::Unsubscribe(unsub) => {
                        q.subscribers
                            .retain(|(_, writer)| writer.id() != unsub.id());
                    }
                }
            }
            Err(e) => println!("[Queue {}] Error while receiving message {:?}", name, e),
        }
    }
}
