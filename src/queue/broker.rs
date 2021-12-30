use crate::mqtt::{
    flags::ControlPacketType,
    message::{FixedHeader, MqttMessage, PublishSource, PublishVariableHeader},
};
use crate::queue::structure::*;
use lunatic::{Mailbox, Message, Request, TransformMailbox};
use std::collections::HashMap;

pub fn broker_process(mailbox: Mailbox<Request<QueueRequest, QueueResponse>>) {
    let mailbox = mailbox.catch_link_panic();
    let mut subs = HashMap::<String, Queue>::new();
    loop {
        match mailbox.receive() {
            Message::Normal(message) => {
                let message = message.unwrap();
                let client = message.sender();
                match message.data() {
                    QueueRequest::Disconnect => {}
                    QueueRequest::Publish(topic, data, message_id) => {
                        let (topic, data) = (topic.to_string(), data.to_string());
                        let queue = subs.entry(topic.to_string()).or_insert(Queue {
                            name: topic.clone(),
                            subscribers: Vec::new(),
                        });
                        for sub in &queue.subscribers {
                            sub.process.send(MqttMessage::Publish(
                                FixedHeader {
                                    control_packet: ControlPacketType::PUBLISH,
                                    dup: false,
                                    qos: 1,
                                    length: 2,
                                    retain: false,
                                },
                                PublishVariableHeader {
                                    message_id: *message_id,
                                    topic_name: topic.to_string(),
                                },
                                data.to_string(),
                                PublishSource::Server,
                            ));
                        }
                        message.reply(QueueResponse::Puback(topic))
                    }
                    QueueRequest::Subscribe(process, topic) => {
                        for sub in topic {
                            let queue = subs.entry(sub.topic.clone()).or_insert(Queue {
                                name: sub.topic.clone(),
                                subscribers: Vec::new(),
                            });
                            queue.subscribers.push(Subscription {
                                qos: sub.qos,
                                link: client.link(),
                                process: process.clone(),
                            })
                        }
                        message.reply(QueueResponse::Suback)
                    }
                }
            }
            Message::Signal(tag) => eprintln!("Received signal from mailbox {:?}", tag),
        }
    }
}
