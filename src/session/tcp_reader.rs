use crate::structure::*;
use lunatic::{net, Mailbox};
use mqtt_packet_3_5::{
    ConfirmationPacket, ConnectPacket, MqttPacket, PacketDecoder, PublishPacket, SubscribePacket,
};
use std::collections::HashMap;

struct ConnectionHelper {
    broker: BrokerProcess,
    pub_queues: HashMap<String, Queue>,
    confirmation_queues: HashMap<u16, QueueProcess>,
    connect_packet: ConnectPacket,
    pub reader: PacketDecoder<net::TcpStream>,
    writer_process: WriterProcess,
    protocol_version: u8,
    is_v5: bool,
    client_id: String,
}

impl ConnectionHelper {
    pub fn new(
        broker: BrokerProcess,
        writer_process: WriterProcess,
        stream: net::TcpStream,
        connect_packet: ConnectPacket,
    ) -> ConnectionHelper {
        let client_id = connect_packet.client_id.to_string();
        let protocol_version = connect_packet.protocol_version;
        ConnectionHelper {
            reader: PacketDecoder::from_stream(stream),
            connect_packet,
            writer_process,
            protocol_version,
            is_v5: protocol_version == 5,
            client_id,
            broker,
            pub_queues: HashMap::new(),
            confirmation_queues: HashMap::new(),
        }
    }

    pub fn publish(&mut self, packet: PublishPacket) {
        let protocol_version = self.protocol_version;
        let client_id = self.client_id.clone();
        let writer = self.writer_process.clone();
        let queue = self.ensure_pub_queue(packet.clone());
        println!("Found queue to publish {:?}", queue);
        queue.process.request(QueueRequest::Publish(
            packet,
            client_id,
            writer,
            protocol_version,
        ));
    }

    fn ensure_pub_queue(&mut self, packet: PublishPacket) -> &Queue {
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

    pub fn subscribe(&mut self, sub: SubscribePacket) {
        let is_v5 = self.protocol_version == 5;
        match self.broker.request(BrokerRequest::Subscribe(
            sub.clone(),
            self.writer_process.clone(),
        )) {
            Ok(data) => {
                // println!(
                //     "[Session {}] Response from broker on subscribe {:?}",
                //     self.client_id, data
                // );
            }
            Err(e) => {
                eprintln!("Failed to subscribe: {}", e);
            }
        }
    }

    pub fn read(&mut self) -> Result<MqttPacket, String> {
        self.reader
            .decode_packet(self.connect_packet.protocol_version)
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

    /// The Reader should only receive PUBACK from the subscriber
    ///
    pub fn confirmation(&mut self, packet: ConfirmationPacket) {
        let cmd = packet.cmd;
        let message_id = packet.message_id;
        let client_id = self.client_id.clone();
        // println!(
        //     "[Reader {}] SENDING CONFIRMATION TO SESSION {:?}",
        //     client_id, packet
        // );
        let writer = self.writer_process.clone();
        let queue = self.ensure_confirmation_queue(packet);
        if let Ok(QueueResponse::Success) =
            queue.request(QueueRequest::Confirmation(cmd, message_id, writer))
        {
            // println!(
            //     "[Reader {}] Sent confirmation to queue {:?}",
            //     client_id, queue
            // );
        }
    }

    fn ensure_confirmation_queue(&mut self, packet: ConfirmationPacket) -> &QueueProcess {
        self.confirmation_queues
            .entry(packet.message_id)
            .or_insert_with(|| {
                match self
                    .writer_process
                    .request(WriterMessage::GetQueue(packet.message_id))
                    .unwrap()
                {
                    WriterResponse::MatchingQueue(q) => q,
                    x => {
                        eprintln!(
                            "[Reader {}] Writer responded with non-queue response: {:?}",
                            self.client_id, x
                        );
                        panic!("Writer messed up");
                    }
                }
            })
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
    let mut state = ConnectionHelper::new(broker, writer_process, stream, connect_packet);
    loop {
        match state.read() {
            Ok(v) => {
                // println!(
                //     "[Reader {}] READ PACKET {:?}",
                //     state.connect_packet.client_id, v
                // );
                match v {
                    // handle authentication and session creation
                    // MqttPacket::Connect(connect_packet) => {
                    //     state.handle_connect(connect_packet, this.clone())
                    // }
                    MqttPacket::Disconnect(_) => state.disconnect(),
                    MqttPacket::Puback(packet)
                    | MqttPacket::Pubrec(packet)
                    | MqttPacket::Pubrel(packet)
                    | MqttPacket::Pubcomp(packet) => state.confirmation(packet),
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
                eprintln!(
                    "[Reader {}] Error while decoding packet {:?}",
                    state.connect_packet.client_id, e
                );
                if e.contains("kind: UnexpectedEof") {
                    eprintln!(
                        "[Reader {}] Exiting process...",
                        state.connect_packet.client_id
                    );
                    state.disconnect();
                    return;
                }
            }
        }
    }
}
