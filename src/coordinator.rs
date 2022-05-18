use std::collections::HashMap;

use crate::client::{ClientProcess, WriterProcess};
use crate::structure::{Queue, QueueMessage};
use crate::topic_tree::TopicTree;
use lunatic::{
    host,
    process::{AbstractProcess, ProcessMessage, ProcessRef, ProcessRequest, Request, StartProcess},
    supervisor::Supervisor,
};
use mqtt_packet_3_5::{Granted, MqttPacket, PublishPacket, SubackPacket, SubscribePacket};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// A reference to a client that joined the server.
struct Client {
    // username: String,
    pub client: ProcessRef<ClientProcess>,
    pub writer: ProcessRef<WriterProcess>,
}

/// The `CoordinatorSup` is supervising one global instance of the `CoordinatorProcess`.
pub struct CoordinatorSup;
impl Supervisor for CoordinatorSup {
    type Arg = String;
    type Children = CoordinatorProcess;

    fn init(config: &mut lunatic::supervisor::SupervisorConfig<Self>, name: Self::Arg) {
        // Always register the `CoordinatorProcess` under the name passed to the supervisor.
        config.children_args(((), Some(name)))
    }
}

pub struct CoordinatorProcess {
    messages: Vec<QueueMessage>,
    clients: HashMap<u128, Client>,
    topic_tree: TopicTree,
    // channels: HashMap<String, (ProcessRef<ChannelProcess>, usize)>,
}
impl AbstractProcess for CoordinatorProcess {
    type Arg = ();
    type State = Self;

    fn init(_: ProcessRef<Self>, _: Self::Arg) -> Self::State {
        // Coordinator shouldn't die when a client dies. This makes the link one-directional.
        unsafe { host::api::process::die_when_link_dies(0) };

        CoordinatorProcess {
            topic_tree: TopicTree::default(),
            messages: Vec::new(),
            clients: HashMap::new(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Connect(
    pub ProcessRef<ClientProcess>,
    pub ProcessRef<WriterProcess>,
    pub bool,
);
impl ProcessRequest<Connect> for CoordinatorProcess {
    type Response = bool;

    fn handle(
        state: &mut Self::State,
        Connect(client, writer, should_persist): Connect,
    ) -> Self::Response {
        state
            .clients
            .insert(writer.uuid(), Client { client, writer });

        true
    }
}

#[derive(Serialize, Deserialize)]
pub struct Disconnect(pub ProcessRef<ClientProcess>);
impl ProcessMessage<Disconnect> for CoordinatorProcess {
    fn handle(state: &mut Self::State, Disconnect(client): Disconnect) {
        state.clients.remove(&client.uuid());
    }
}

/// Publish message
#[derive(Serialize, Deserialize)]
pub struct Subscribe(pub SubscribePacket, pub ProcessRef<WriterProcess>);
impl ProcessRequest<Subscribe> for CoordinatorProcess {
    type Response = bool;

    fn handle(
        state: &mut CoordinatorProcess,
        Subscribe(packet, writer): Subscribe,
    ) -> Self::Response {
        println!(
            "[Coordinator->Subscribe] Received Subscribe message {:?} {:?}",
            writer, packet
        );
        let mut suback = SubackPacket {
            granted: vec![],
            granted_reason_codes: vec![],
            message_id: packet.message_id,
            reason_code: Some(0),
            properties: None,
        };
        for sub in packet.subscriptions {
            state
                .topic_tree
                .add_subscriptions(sub.topic, writer.clone());
            suback.granted.push(Granted::QoS2);
            println!(
                "[Coordinator->Subscribe] Got these matching queues {:?}",
                state.topic_tree
            );
        }
        writer.request(MqttPacket::Suback(suback))
    }
}

/// Publish message
#[derive(Serialize, Deserialize)]
pub struct Publish(pub PublishPacket);
impl ProcessRequest<Publish> for CoordinatorProcess {
    type Response = bool;

    fn handle(state: &mut CoordinatorProcess, Publish(packet): Publish) -> Self::Response {
        let queue = state.topic_tree.get_by_name(packet.topic.clone());
        println!(
            "[Coordinator->Publish] Adding publish message to message queue {} {:?}",
            packet.topic, queue
        );
        state.messages.push(QueueMessage {
            message_id: Uuid::new_v4(),
            packet,
            queue_id: queue.id,
            in_progress: false,
        });
        true
    }
}

/// Poll Message from queue
#[derive(Serialize, Deserialize)]
pub struct Poll;

#[derive(Serialize, Deserialize)]
pub enum PollResponse {
    None,
    Publish(Uuid, PublishPacket, Queue),
}
impl ProcessRequest<Poll> for CoordinatorProcess {
    type Response = PollResponse;

    fn handle(state: &mut CoordinatorProcess, _: Poll) -> Self::Response {
        for msg in state.messages.iter_mut() {
            if !msg.in_progress {
                let queue = state.topic_tree.get_by_id(msg.queue_id);
                msg.in_progress = true;
                return PollResponse::Publish(msg.message_id, msg.packet.clone(), queue.clone());
            }
        }
        PollResponse::None
    }
}

/// Release Message from queue
#[derive(Serialize, Deserialize)]
pub struct Release(pub Uuid);
impl ProcessRequest<Release> for CoordinatorProcess {
    type Response = bool;

    fn handle(state: &mut CoordinatorProcess, Release(id): Release) -> Self::Response {
        state.messages.retain(|msg| msg.message_id != id);
        true
    }
}
