use super::topic_tree::*;
use crate::mqtt::{
    flags::ControlPacketType,
    message::{FixedHeader, MqttMessage, PublishSource, PublishVariableHeader},
};
use crate::queue::structure::*;
use lunatic::{Mailbox, Message, Request, TransformMailbox};

pub fn broker_process(mailbox: Mailbox<Request<QueueRequest, QueueResponse>>) {
    let mailbox = mailbox.catch_link_panic();
    let mut subs = TopicTree::new();
    loop {
        match mailbox.receive() {
            Message::Normal(message) => {
                let message = message.unwrap();
                let client = message.sender();
                match message.data() {
                    QueueRequest::Disconnect => {}
                    QueueRequest::Publish(topic, data, message_id) => {
                        let (topic, data) = (topic.to_string(), data.to_string());
                        for sub in &subs.get_subscribers(topic.to_string()) {
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
                            let _queue = subs.add_subscription(
                                sub.topic.clone(),
                                Subscription {
                                    qos: sub.qos,
                                    link: client.link(),
                                    process: process.clone(),
                                },
                            );
                        }
                        message.reply(QueueResponse::Suback)
                    }
                }
            }
            Message::Signal(tag) => eprintln!("Received signal from mailbox {:?}", tag),
        }
    }
}
