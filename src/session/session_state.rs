use crate::structure::*;
use lunatic::{process, Mailbox, Request};
use mqtt_packet_3_5::{
    ConfirmationPacket, ConnectPacket, MqttCode, PacketType, PublishPacket, SubackPacket,
    SubscribePacket, SubscriptionReasonCode,
};
use std::collections::HashMap;

struct SessionState {
    qos1: HashMap<u16, QueueProcess>,
    qos2: HashMap<u16, QueueProcess>,
    broker: BrokerProcess,
    pub_queues: HashMap<String, Queue>,
    writer_process: WriterProcess,
    protocol_version: u8,
    client_id: String,
    this: SessionProcess,
}

impl SessionState {
    pub fn new(
        broker: BrokerProcess,
        writer_process: WriterProcess,
        protocol_version: u8,
        client_id: String,
        this: SessionProcess,
    ) -> SessionState {
        SessionState {
            qos1: HashMap::new(),
            qos2: HashMap::new(),
            broker,
            pub_queues: HashMap::new(),
            writer_process,
            protocol_version,
            client_id,
            this,
        }
    }

    pub fn publish_to_queue(&mut self, packet: PublishPacket) {
        let protocol_version = self.protocol_version;
        let client_id = self.client_id.clone();
        let this = self.this.clone();
        let queue = self.ensure_queue(packet.clone());
        println!("Found queue to publish {:?}", queue);
        queue.process.send(QueueRequest::Publish(
            packet,
            client_id,
            this,
            protocol_version,
        ));
    }

    fn ensure_queue(&mut self, packet: PublishPacket) -> &Queue {
        self.pub_queues
            .entry(packet.topic.clone())
            .or_insert_with(|| {
                match self
                    .broker
                    .request(BrokerRequest::GetQueue(packet.topic))
                    .unwrap()
                {
                    BrokerResponse::MatchingQueue(q) => q,
                    x => {
                        eprintln!(
                            "[Reader {}] Broker responded with non-queue response: {:?}",
                            self.client_id, x
                        );
                        panic!("Broker messed up");
                    }
                }
            })
    }

    pub fn handle_client_confirmation(&mut self, confirmation: &ConfirmationPacket) {
        println!(
            "[Session {}] GOT CLIENT CONFIRMATION {:?}",
            self.client_id, confirmation
        );
        if let Some(proc) = self.qos2.get(&confirmation.message_id) {
            println!(
                "[Session {}] GOT CLIENT CONF PROCESS {:?}",
                self.client_id, proc
            );
            proc.send(QueueRequest::Confirmation(
                confirmation.cmd,
                confirmation.message_id,
            ))
        }
    }

    pub fn handle_server_confirmation(&mut self, confirmation: &ConfirmationPacket) {
        println!(
            "[Session {}] GOT SERVER CONFIRMATION {:?}",
            self.client_id, confirmation
        );
        if let Err(e) = self
            .writer_process
            .request(WriterMessage::Confirmation(confirmation.clone()))
        {
            eprintln!(
                "[Session {}] failed to write confirmation {:?}. Error: {:?}",
                self.client_id, confirmation, e
            );
        } else {
            println!(
                "[Session {}] Send confirmation to writer {:?}",
                self.client_id, confirmation
            );
        }
    }

    pub fn publish_to_client(&mut self, qos: u8, msg_id: u16, buf: &Vec<u8>, queue: QueueProcess) {
        if qos == 1 {
            self.qos1.insert(msg_id, queue);
        } else {
            self.qos2.insert(msg_id, queue);
        }
        if let Ok(WriterResponse::Sent) = self
            .writer_process
            .request(WriterMessage::Publish(buf.clone()))
        {
            println!(
                "[Session {}] Sent publish to client {}",
                self.client_id, msg_id
            );
        }
    }

    pub fn subscribe(&mut self, sub: &SubscribePacket) {
        let is_v5 = self.protocol_version == 5;
        match self.broker.request(BrokerRequest::Subscribe(
            sub.clone(),
            self.this.clone(),
            self.writer_process.clone(),
        )) {
            Ok(data) => {
                println!(
                    "[Session {}] Response from broker on subscribe {:?}",
                    self.client_id, data
                );
                let _ = self
                    .writer_process
                    .request(WriterMessage::Suback(SubackPacket {
                        message_id: sub.message_id,
                        properties: None,
                        reason_code: Some(0),
                        granted_reason_codes: if is_v5 {
                            sub.subscriptions
                                .iter()
                                .map(|x| {
                                    SubscriptionReasonCode::from_byte(x.qos.to_byte())
                                        .unwrap_or(SubscriptionReasonCode::UnspecifiedError)
                                })
                                .collect()
                        } else {
                            vec![]
                        },
                        granted: vec![],
                    }));
                println!("[Session {}] Sent suback to writer", self.client_id);
            }
            Err(e) => {
                eprintln!("Failed to subscribe: {}", e);
            }
        }
    }
}

pub fn session_process(
    (broker, writer, connect_packet): (BrokerProcess, WriterProcess, ConnectPacket),
    mailbox: Mailbox<Request<SessionRequest, SessionResponse>>,
) {
    let this = process::this(&mailbox);
    let mut state = SessionState::new(
        broker,
        writer,
        connect_packet.protocol_version,
        connect_packet.client_id.to_string(),
        this,
    );
    loop {
        match mailbox.receive() {
            Ok(msg) => {
                let mut response = SessionResponse::Success;
                println!("MSG.DATA() = {:?}", msg.data());
                match msg.data() {
                    SessionRequest::Publish(packet) => state.publish_to_queue(packet.clone()),
                    SessionRequest::Subscribe(packet) => {
                        state.subscribe(packet);
                    }
                    SessionRequest::Confirmation(packet, MessageSource::Client) => {
                        state.handle_client_confirmation(packet)
                    }
                    SessionRequest::Confirmation(packet, MessageSource::Server) => {
                        state.handle_server_confirmation(packet)
                    }
                    SessionRequest::PublishBroker(qos, msg_id, buf, queue) => {
                        state.publish_to_client(*qos, *msg_id, buf, queue.clone())
                    }
                }
                println!(
                    "[Session {}] Session SENDING REPLY {:?}",
                    state.client_id, response
                );
                msg.reply(response)
            }
            Err(e) => {
                eprintln!(
                    "[Session {}] Failed to receive mailbox {:?}",
                    state.client_id, e
                );
            }
        }
    }
}
