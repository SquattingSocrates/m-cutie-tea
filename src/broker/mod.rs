mod topic_tree;

use self::topic_tree::TopicTree;
use std::collections::HashMap;

use crate::structure::*;
use lunatic::{Mailbox, Message, Request, TransformMailbox};
use mqtt_packet_3_5::SubscribePacket;

struct Broker {
    subs: TopicTree,
    clients: HashMap<String, ReaderProcess>,
}

impl Default for Broker {
    fn default() -> Broker {
        Broker {
            subs: TopicTree::new(),
            clients: HashMap::new(),
        }
    }
}

impl Broker {
    pub fn register_session(&mut self, client_id: String, process: ReaderProcess) {
        println!("REGISTERING SESSION {}", client_id);
        if let Some(prev) = self.clients.insert(client_id.to_string(), process.clone()) {
            prev.send(SessionRequest::Destroy);
        }
    }

    pub fn maybe_move_to_existing(
        &mut self,
        SessionConfig {
            stream,
            connect_packet,
        }: SessionConfig,
    ) -> Option<ReaderProcess> {
        if let Some(process) = self.clients.get(&connect_packet.client_id) {
            process.send(SessionRequest::Create(stream.clone(), connect_packet));
            Some(process.clone())
        } else {
            None
        }
    }

    pub fn ensure_topic(&mut self, topic: &str) -> Queue {
        self.subs.ensure_topic_queue(topic)
    }

    pub fn subscribe_process(&mut self, packet: SubscribePacket, writer: WriterProcess) {
        for (idx, sub) in packet.subscriptions.iter().enumerate() {
            for q in self.subs.get_matching_queues(&sub.topic) {
                q.process
                    .send(QueueRequest::Subscribe(idx, packet.clone(), writer.clone()))
            }
        }
    }
}

pub fn broker_process(mailbox: Mailbox<Request<BrokerRequest, BrokerResponse>>) {
    let mailbox = mailbox.catch_link_panic();
    let mut state = Broker::default();
    loop {
        match mailbox.receive() {
            Message::Normal(message) => {
                let message = message.unwrap();
                let mut response = BrokerResponse::Subscribed;
                match message.data() {
                    BrokerRequest::Subscribe(packet, writer_process) => {
                        state.subscribe_process(packet.clone(), writer_process.clone());
                        response = BrokerResponse::Subscribed;
                    }
                    BrokerRequest::GetQueue(topic) => {
                        response = BrokerResponse::MatchingQueue(state.ensure_topic(topic));
                    }
                    BrokerRequest::RegisterSession(client_id, process) => {
                        state.register_session(client_id.clone(), process.clone());
                        response = BrokerResponse::Registered;
                    }
                    BrokerRequest::MoveToExistingSession(config) => {
                        println!(
                            "CHECKING HAS PROCESS {} {:?}",
                            config.connect_packet.client_id, config.connect_packet
                        );
                        response = BrokerResponse::ExistingSession(
                            state.maybe_move_to_existing(config.clone()),
                        );
                    }
                    BrokerRequest::DestroySession(s) => {}
                }
                message.reply(response);
            }
            Message::Signal(tag) => eprintln!("Received signal from mailbox {:?}", tag),
        }
    }
}
