use lunatic::Mailbox;

use crate::structure::{QueueRequest, Subscription, WriterMessage, WriterQueueResponse};

pub fn new_queue(name: String, mailbox: Mailbox<QueueRequest>) {
    let mut subscribers = Vec::<Subscription>::new();
    let mut retained_msg = None;

    loop {
        match mailbox.receive() {
            Ok(data) => {
                println!("[Queue {}] Received mqtt message {:?}", name, data);
                match &data {
                    QueueRequest::Publish(fixed, variable, payload) => {
                        // QoS 0 - fire and forget
                        println!("SUBSCRIBERS IN LIST: {}", subscribers.len());
                        for sub in subscribers.iter() {
                            if fixed.retain {
                                retained_msg = Some(payload.clone());
                            }
                            sub.process
                                .send(WriterMessage::Queue(WriterQueueResponse::Publish(
                                    variable.message_id,
                                    variable.topic_name.to_string(),
                                    payload.to_string(),
                                    fixed.qos,
                                )))
                        }
                    }
                    QueueRequest::Subscribe(sub) => {
                        subscribers.push(sub.clone());
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
