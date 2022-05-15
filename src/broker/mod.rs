mod topic_tree;

use self::topic_tree::TopicTree;
use std::collections::HashMap;

use crate::structure::*;
use lunatic::{
    process::{AbstractProcess, Message, ProcessRef, ProcessRequest, Request},
    Mailbox,
};
use mqtt_packet_3_5::{QoS, SubscribePacket};

pub struct Broker {
    subs: TopicTree,
    clients: HashMap<String, ReaderProcess>,
}

impl Default for Broker {
    fn default() -> Broker {
        Broker {
            subs: TopicTree::default(),
            clients: HashMap::new(),
        }
    }
}

impl Broker {
    pub fn register_session(&mut self, client_id: String, process: ReaderProcess) {
        println!("REGISTERING SESSION {}", client_id);
        if let Some(prev) = self.clients.insert(client_id.to_string(), process.clone()) {
            // prev.send(SessionRequest::Destroy);
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
            // process.send(SessionRequest::Create(stream.clone(), connect_packet));
            Some(process.clone())
        } else {
            None
        }
    }

    pub fn ensure_topic(&mut self, topic: &str) -> Queue {
        self.subs.ensure_topic_queue(topic)
    }

    pub fn subscribe_process(&mut self, packet: SubscribePacket, writer: WriterProcess) {
        for sub in packet.subscriptions.iter() {
            for q in self.subs.get_matching_queues(&sub.topic) {
                q.process.request(BrokerRequest::Subscribe(
                    packet.qos,
                    packet.message_id,
                    writer.clone(),
                ));
            }
            // store subscription for future topics that match
            // self.subs.add_subscriber(
            //     packet.fixed.qos,
            //     packet.message_id,
            //     sub.topic.to_string(),
            //     writer.clone(),
            // );
        }
    }
}

impl AbstractProcess for Broker {
    type Arg = String;
    type State = Self;

    fn init(self_ref: ProcessRef<Self>, name: String) -> Self {
        Broker::default()
    }
}

impl ProcessRequest<BrokerRequest> for Broker {
    type Response = BrokerResponse;

    fn handle(state: &mut Self::State, message: BrokerRequest) -> BrokerResponse {
        match message {
            BrokerRequest::Subscribe(packet, writer_process) => {
                state.subscribe_process(packet.clone(), writer_process.clone());
                BrokerResponse::Subscribed
            }
            BrokerRequest::GetQueue(topic) => {
                BrokerResponse::MatchingQueue(state.ensure_topic(&topic))
            }
            BrokerRequest::RegisterSession(client_id, process) => {
                state.register_session(client_id.clone(), process.clone());
                BrokerResponse::Registered
            }
            BrokerRequest::MoveToExistingSession(config) => {
                println!(
                    "CHECKING HAS PROCESS {} {:?}",
                    config.connect_packet.client_id, config.connect_packet
                );
                BrokerResponse::ExistingSession(state.maybe_move_to_existing(config.clone()))
            }
            BrokerRequest::DestroySession(s) => BrokerResponse::Destroyed,
        }
    }
}

// pub fn broker_process(mailbox: Mailbox<Request<BrokerRequest, BrokerResponse>>) {
//     let mailbox = mailbox.catch_link_panic();
//     let mut state = Broker::default();
//     loop {
//         match mailbox.receive() {
//             Message::Normal(message) => {
//                 let message = message.unwrap();
//                 let mut response = BrokerResponse::Subscribed;
//                 match message.data() {
//                     BrokerRequest::Subscribe(packet, writer_process) => {
//                         state.subscribe_process(packet.clone(), writer_process.clone());
//                         response = BrokerResponse::Subscribed;
//                     }
//                     BrokerRequest::GetQueue(topic) => {
//                         response = BrokerResponse::MatchingQueue(state.ensure_topic(&topic));
//                     }
//                     BrokerRequest::RegisterSession(client_id, process) => {
//                         state.register_session(client_id.clone(), process.clone());
//                         response = BrokerResponse::Registered;
//                     }
//                     BrokerRequest::MoveToExistingSession(config) => {
//                         println!(
//                             "CHECKING HAS PROCESS {} {:?}",
//                             config.connect_packet.client_id, config.connect_packet
//                         );
//                         response = BrokerResponse::ExistingSession(
//                             state.maybe_move_to_existing(config.clone()),
//                         );
//                     }
//                     BrokerRequest::DestroySession(s) => {}
//                 }
//                 message.reply(response);
//             }
//             Message::Signal(tag) => eprintln!("Received signal from mailbox {:?}", tag),
//         }
//     }
// }
