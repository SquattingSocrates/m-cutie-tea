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
        let writer = self.writer_process.clone();
        let queue = self.ensure_queue(packet.clone());
        queue.process.send(QueueRequest::Publish(packet, writer));
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
                        eprintln!("Broker responded with non-queue response: {:?}", x);
                        panic!("Broker messed up");
                    }
                }
            })
    }

    pub fn use_new_stream(&mut self, stream: net::TcpStream, packet: ConnectPacket) {
        self.reader = PacketDecoder::from_stream(stream.clone());
        self.is_receiving = true;
        self.connect_packet = packet;
        self.writer_process.request(WriterMessage::Connection(
            ConnectionMessage::Connect(0x0),
            Some(stream),
        ));
    }

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
                println!("RESPONSE FROM BROKER ON SUBSCRIBE {:?}", data);
                self.writer_process
                    .request(WriterMessage::Queue(MqttPacket::Suback(SubackPacket {
                        fixed: FixedHeader::for_type(PacketType::Suback),
                        length: 0,
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
                        granted_qos: if is_v5 {
                            vec![]
                        } else {
                            sub.subscriptions.iter().map(|x| x.qos.clone()).collect()
                        },
                    })));
                println!("JUST SENT TO WRITER");
            }
            Err(e) => {
                eprintln!("Failed to subscribe: {}", e);
            }
        }
    }

    pub fn wait_for_reconnect(&mut self, msg: Result<SessionRequest, ReceiveError>) -> bool {
        match msg {
            Ok(SessionRequest::Create(new_stream, connect_packet)) => {
                println!("RECEIVED RECONN IN MAILBOX");
                self.use_new_stream(new_stream, connect_packet);
                false
            }
            Ok(SessionRequest::Destroy) => {
                self.writer_process
                    .request(WriterMessage::Connection(ConnectionMessage::Destroy, None));
                true
            }
            Err(e) => {
                eprintln!("Error while receiving new stream {:?}", e);
                false
            }
        }
    }

    pub fn handle_connect(&mut self, packet: ConnectPacket, reader: Process<SessionRequest>) {
        let client_id = packet.client_id.clone();
        self.connect_packet = packet;
        if let BrokerResponse::Registered = self
            .broker
            .request(BrokerRequest::RegisterSession(client_id, reader))
            .unwrap()
        {
            self.writer_process.request(WriterMessage::Connection(
                ConnectionMessage::Connect(0),
                None,
            ));
        }
    }

    pub fn pong(&mut self) {
        self.writer_process
            .request(WriterMessage::Queue(MqttPacket::Pingresp(PingrespPacket {
                fixed: FixedHeader::for_type(PacketType::Pingresp),
            })));
    }

    pub fn disconnect(&mut self) {
        self.writer_process.request(WriterMessage::Connection(
            ConnectionMessage::Disconnect,
            None,
        ));
    }
}

pub fn handle_tcp(
    (mut client_id, writer_process, mut stream, broker, connect_packet): (
        String,
        WriterProcess,
        net::TcpStream,
        BrokerProcess,
        ConnectPacket,
    ),
    mailbox: Mailbox<SessionRequest>,
) {
    // controls whether data is read from tcp_stream
    let this = process::this(&mailbox);
    let mut state = ConnectionHelper::new(broker, writer_process, stream, connect_packet);
    loop {
        if !state.is_receiving {
            println!(
                "Waiting for mailbox in tcp_reader. is_receiving: {}",
                state.is_receiving
            );
            if let true = state.wait_for_reconnect(mailbox.receive()) {
                println!("Terminating process");
                return;
            }
            continue;
        }

        println!(
            "Waiting for fill_buf in tcp_reader. is_receiving: {}",
            state.is_receiving
        );
        match state.read() {
            Ok(v) => {
                println!("READ PACKET {:?}", v);
                match v {
                    // handle authentication and session creation
                    MqttPacket::Connect(connect_packet) => {
                        state.handle_connect(connect_packet, this.clone())
                    }
                    MqttPacket::Disconnect(_) => state.disconnect(),
                    MqttPacket::Pingreq(_) => state.pong(),
                    // handle queue messages
                    MqttPacket::Publish(packet) => state.publish(packet),
                    MqttPacket::Subscribe(packet) => state.subscribe(packet),
                    msg => {
                        println!("received message: {:?}", msg);
                    }
                }
            }
            Err(e) => {
                println!("Error while decoding packet {:?}", e);
                if e.contains("kind: UnexpectedEof") {
                    println!("Exiting process...");
                    // state.close_writer();
                    return;
                }
            }
        }
    }
}
