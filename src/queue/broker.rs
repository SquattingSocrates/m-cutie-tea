use crate::mqtt::{
    flags::ControlPacketType,
    message::{FixedHeader, MqttMessage, PublishSource, PublishVariableHeader},
};
use crate::queue::structure::{QueueRequest, QueueResponse, Subscription};
use lunatic::{Mailbox, Message, Request, TransformMailbox};

pub fn broker_process(mailbox: Mailbox<Request<QueueRequest, QueueResponse>>) {
    let mailbox = mailbox.catch_link_panic();
    let mut subs = Vec::<Subscription>::new();
    // let mut queues: HashMap<String, Vec<Subscription>> = HashMap::new();
    loop {
        match mailbox.receive() {
            Message::Normal(message) => {
                let message = message.unwrap();
                let client = message.sender();
                match message.data() {
                    QueueRequest::Disconnect => {}
                    QueueRequest::Publish(topic, data, message_id) => {
                        let (topic, data) = (topic.to_string(), data.to_string());
                        println!("PUBLISHING TO CLIENTS {} {}", topic, data);
                        for sub in subs.iter() {
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
                            subs.push(Subscription {
                                link: client.link(),
                                // topic: sub.topic,
                                qos: sub.qos,
                                process: process.clone(),
                            })
                        }
                        message.reply(QueueResponse::Suback("hello".to_string()))
                    }
                }
            }
            Message::Signal(tag) => eprintln!("Received signal from mailbox {:?}", tag),
        }
    }
}
