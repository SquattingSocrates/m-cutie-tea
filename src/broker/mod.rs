mod topic_tree;

use self::topic_tree::TopicTree;
use std::collections::HashMap;

use crate::structure::*;
use lunatic::{process::Process, Mailbox, Message, Request, TransformMailbox};

pub fn broker_process(mailbox: Mailbox<Request<BrokerRequest, BrokerResponse>>) {
    let mailbox = mailbox.catch_link_panic();
    let mut subs = TopicTree::new();
    let mut clients = HashMap::<String, Process<SessionRequest>>::new();
    loop {
        match mailbox.receive() {
            Message::Normal(message) => {
                let message = message.unwrap();
                match message.data() {
                    BrokerRequest::Subscribe(client_id, sub_info, process) => {
                        for sub in sub_info {
                            let q = subs.ensure_topic_queue(sub.topic.clone());
                            q.process.send(QueueRequest::Subscribe(Subscription {
                                client_id: client_id.to_string(),
                                qos: sub.qos,
                                process: process.clone(),
                            }))
                        }
                        message.reply(BrokerResponse::Subscribed)
                    }
                    BrokerRequest::GetQueue(topic) => {
                        let q = subs.ensure_topic_queue(topic.to_string());
                        message.reply(BrokerResponse::MatchingQueue(q.clone()));
                    }
                    BrokerRequest::RegisterSession(client_id, process) => {
                        println!("REGISTERING SESSION {}", client_id);
                        if let Some(prev) = clients.insert(client_id.to_string(), process.clone()) {
                            prev.send(SessionRequest::Destroy);
                        }
                        message.reply(BrokerResponse::Registered);
                    }
                    BrokerRequest::HasProcess(client_id) => {
                        println!("CHECKING HAS PROCESS {} {:?}", client_id, clients);
                        if let Some(process) = clients.get(client_id) {
                            message.reply(BrokerResponse::ExistingSession(Some(process.clone())));
                        } else {
                            message.reply(BrokerResponse::ExistingSession(None));
                        }
                    }
                }
            }
            Message::Signal(tag) => eprintln!("Received signal from mailbox {:?}", tag),
        }
    }
}
