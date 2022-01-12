use lunatic::Mailbox;

use crate::structure::{QueueRequest, QueueResponse, Subscription, WriterMessage};

pub fn new_queue(name: String, mailbox: Mailbox<QueueRequest>) {
    let mut subscribers = Vec::<Subscription>::new();
    let mut retained_msg = None;

    loop {
        match mailbox.receive() {
            Ok(data) => {
                println!("[Queue {}] Received mqtt message {:?}", name, data);
                match &data {
                    QueueRequest::Publish(fixed, variable, payload, publisher) => {
                        // QoS 0 - fire and forget
                        println!("SUBSCRIBERS IN LIST: {}", subscribers.len());
                        for sub in subscribers.iter() {
                            if fixed.retain {
                                retained_msg = Some((variable.message_id, payload.to_string()));
                            }
                            sub.process
                                .send(WriterMessage::Queue(QueueResponse::Publish(
                                    variable.message_id,
                                    variable.topic_name.to_string(),
                                    payload.to_string(),
                                    fixed.qos,
                                )));
                        }

                        if fixed.qos == 1 {
                            let publisher = publisher.clone().unwrap();
                            publisher.send(WriterMessage::Queue(QueueResponse::Puback(
                                variable.message_id,
                            )));
                        }
                    }
                    QueueRequest::Subscribe(sub) => {
                        subscribers.push(sub.clone());
                        // send retained message on new subscription
                        if let Some((message_id, payload)) = &retained_msg {
                            sub.process
                                .send(WriterMessage::Queue(QueueResponse::Publish(
                                    *message_id,
                                    name.clone(),
                                    payload.to_string(),
                                    0,
                                )))
                        }
                    }
                    QueueRequest::Unsubscribe(id) => {
                        subscribers.retain(|sub| sub.client_id != *id);
                    }
                }
            }
            Err(e) => println!("[Queue {}] Error while receiving message {:?}", name, e),
        }
    }
}
