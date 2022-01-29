use lunatic::Mailbox;

use crate::structure::*;
use mqtt_packet_3_5::{
    ConfirmationPacket, FixedHeader, MqttPacket, PacketType, PubackPubrecCode, PublishPacket,
    Subscription,
};

trait QueueMessage {
    fn handle_queue(&mut self);
}

pub fn new_queue(name: String, mailbox: Mailbox<QueueRequest>) {
    let mut subscribers = Vec::<(Subscription, WriterProcess)>::new();
    let mut retained_msg = None;

    loop {
        match mailbox.receive() {
            Ok(data) => {
                println!("[Queue {}] Received mqtt message {:?}", name, data);
                match &data {
                    QueueRequest::Publish(packet, publisher) => {
                        // QoS 0 - fire and forget
                        println!("SUBSCRIBERS IN LIST: {}", subscribers.len());
                        for (sub, proc) in subscribers.iter() {
                            if packet.fixed.retain {
                                retained_msg = Some((packet.message_id, packet.payload.clone()));
                            }
                            proc.send(WriterMessage::Queue(MqttPacket::Publish(packet.clone())));
                        }

                        if !subscribers.is_empty() && packet.fixed.qos == 1 {
                            let publisher = publisher.clone();
                            publisher.send(WriterMessage::Queue(MqttPacket::Puback(
                                ConfirmationPacket {
                                    fixed: FixedHeader::for_type(PacketType::Puback),
                                    length: 0,
                                    // since the packet has qos 1 we trust that it
                                    // would not get parsed without a message_id
                                    message_id: packet.message_id.unwrap(),
                                    puback_reason_code: Some(PubackPubrecCode::Success),
                                    pubcomp_reason_code: None,
                                    properties: None,
                                },
                            )));
                        }
                    }
                    QueueRequest::Subscribe(matching, packet, writer) => {
                        let sub = &packet.subscriptions[*matching];
                        subscribers.push((sub.clone(), writer.clone()));
                        // send retained message on new subscription
                        if let Some((message_id, payload)) = &retained_msg {
                            writer.send(WriterMessage::Queue(MqttPacket::Publish(PublishPacket {
                                fixed: FixedHeader::for_type(PacketType::Publish),
                                message_id: *message_id,
                                length: 0,
                                properties: None,
                                payload: payload.clone(),
                                topic: name.clone(),
                            })))
                        }
                    }
                    QueueRequest::Unsubscribe(unsub) => {
                        subscribers.retain(|(_, writer)| writer.id() != unsub.id());
                    }
                }
            }
            Err(e) => println!("[Queue {}] Error while receiving message {:?}", name, e),
        }
    }
}
