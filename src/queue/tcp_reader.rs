use lunatic::{
    net,
    process::{self, Process},
    Mailbox, ReceiveError,
};
use std::collections::HashMap;

use crate::structure::*;
use mqtt_packet_3_5::{
    ConnectPacket, FixedHeader, MqttCode, MqttPacket, PacketDecoder, PacketType, PingrespPacket,
    PublishPacket, SubackPacket, SubscribePacket, SubscriptionReasonCode,
};

struct ConnectionHelper {
    broker: BrokerProcess,
    pub_queues: HashMap<String, Queue>,
    writer_process: WriterProcess,
    connect_packet: ConnectPacket,
    pub reader: PacketDecoder<net::TcpStream>,
    pub is_receiving: bool,
}

impl ConnectionHelper {
    pub fn new(
        broker: BrokerProcess,
        writer_process: WriterProcess,
        stream: net::TcpStream,
        connect_packet: ConnectPacket,
    ) -> ConnectionHelper {
        ConnectionHelper {
            broker,
            pub_queues: HashMap::new(),
            reader: PacketDecoder::from_stream(stream),
            connect_packet,
            writer_process,
            is_receiving: true,
        }
    }

    pub fn publish(&mut self, packet: PublishPacket) {
        let protocol_version = self.connect_packet.protocol_version;
        let client_id = self.connect_packet.client_id.clone();
        let writer = self.writer_process.clone();
        let queue = self.ensure_queue(packet.clone());
        queue.process.send(QueueRequest::Publish(
            packet,
            client_id,
            writer,
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
                            self.connect_packet.client_id, x
                        );
                        panic!("Broker messed up");
                    }
                }
            })
    }

    // pub fn use_new_stream(&mut self, stream: net::TcpStream, packet: ConnectPacket) {
    //     self.reader = PacketDecoder::from_stream(stream.clone());
    //     self.is_receiving = true;
    //     self.connect_packet = packet;
    //     self.writer_process.request(WriterMessage::Connection(
    //         ConnectionMessage::Connect(0x0),
    //         Some(stream),
    //     ));
    // }

    pub fn read(&mut self) -> Result<MqttPacket, String> {
        self.reader
            .decode_packet(self.connect_packet.protocol_version)
    }

    pub fn subscribe(&mut self, sub: SubscribePacket) {
        let is_v5 = self.connect_packet.protocol_version == 5;
        match self.broker.request(BrokerRequest::Subscribe(
            sub.clone(),
            self.writer_process.clone(),
        )) {
            Ok(data) => {
                println!(
                    "[Reader {}] RESPONSE FROM BROKER ON SUBSCRIBE {:?}",
                    self.connect_packet.client_id, data
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
                println!(
                    "[Reader {}] JUST SENT TO WRITER",
                    self.connect_packet.client_id
                );
            }
            Err(e) => {
                eprintln!("Failed to subscribe: {}", e);
            }
        }
    }

    pub fn pong(&mut self) {
        match self.writer_process.request(WriterMessage::Pong) {
            Ok(_) => {}
            Err(e) => eprintln!(
                "Failed to send pong to writer for client {:?}. Error: {:?}",
                self.connect_packet.client_id, e
            ),
        }
    }

    pub fn disconnect(&mut self) {
        match self.writer_process.request(WriterMessage::Die) {
            Ok(_) => {}
            Err(e) => eprintln!(
                "[Reader {}] Failed to send disconnect to writer. Error: {:?}",
                self.connect_packet.client_id, e
            ),
        }
    }
}

pub fn handle_tcp(
    (client_id, writer_process, stream, broker, connect_packet): (
        String,
        WriterProcess,
        net::TcpStream,
        BrokerProcess,
        ConnectPacket,
    ),
    _: Mailbox<()>,
) {
    // controls whether data is read from tcp_stream
    let mut state = ConnectionHelper::new(broker, writer_process, stream, connect_packet);
    loop {
        match state.read() {
            Ok(v) => {
                println!(
                    "[Reader {}] READ PACKET {:?}",
                    state.connect_packet.client_id, v
                );
                match v {
                    // handle authentication and session creation
                    // MqttPacket::Connect(connect_packet) => {
                    //     state.handle_connect(connect_packet, this.clone())
                    // }
                    MqttPacket::Disconnect(_) => state.disconnect(),
                    MqttPacket::Pingreq => state.pong(),
                    // handle queue messages
                    MqttPacket::Publish(packet) => state.publish(packet),
                    MqttPacket::Subscribe(packet) => state.subscribe(packet),
                    msg => {
                        println!(
                            "[Reader {}] received message: {:?}",
                            state.connect_packet.client_id, msg
                        );
                    }
                }
            }
            Err(e) => {
                println!(
                    "[Reader {}] Error while decoding packet {:?}",
                    state.connect_packet.client_id, e
                );
                if e.contains("kind: UnexpectedEof") {
                    println!(
                        "[Reader {}] Exiting process...",
                        state.connect_packet.client_id
                    );
                    // state.close_writer();
                    return;
                }
            }
        }
    }
}
