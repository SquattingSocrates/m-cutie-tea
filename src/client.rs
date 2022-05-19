use lunatic::process::{AbstractProcess, ProcessRef, ProcessRequest, Request, StartProcess};
use lunatic::{net::TcpStream, Mailbox, Process};
use mqtt_packet_3_5::{
    ConnackPacket, ConnectPacket, MqttPacket, Packet, PacketDecoder, PublishPacket,
};
use serde::{Deserialize, Serialize};
use std::io::Write;

use crate::coordinator::{self, CoordinatorProcess, Publish, Subscribe};

pub struct ClientProcess {
    this: ProcessRef<ClientProcess>,
    coordinator: ProcessRef<CoordinatorProcess>,
    writer: ProcessRef<WriterProcess>,
    connect_packet: ConnectPacket,
    // pub reader: PacketDecoder<TcpStream>,
    // protocol_version: u8,
    is_v5: bool,
    client_id: String,
}

impl AbstractProcess for ClientProcess {
    type Arg = TcpStream;
    type State = Self;

    fn init(this: ProcessRef<Self>, stream: Self::Arg) -> Self::State {
        // Look up the coordinator or fail if it doesn't exist.
        let coordinator = ProcessRef::<CoordinatorProcess>::lookup("coordinator").unwrap();
        // Link coordinator to child. The coordinator sets `die_when_link_dies` to `0` and will not fail if child fails.
        coordinator.link();

        let connect_packet = match PacketDecoder::from_stream(stream.clone()).decode_packet(3) {
            Ok(MqttPacket::Connect(packet)) => packet,
            x => {
                eprintln!("Unexpected value instead of connect_packet {:?}", x);
                panic!("Invalid connect packet");
            }
        };

        let writer = WriterProcess::start((stream.clone(), connect_packet.clone()), None);
        // Let the coordinator know that we joined.
        let client_info = coordinator.request(coordinator::Connect(
            this.clone(),
            writer.clone(),
            connect_packet.clean_session,
        ));

        Process::spawn_link(
            (
                this.clone(),
                stream.clone(),
                writer.clone(),
                connect_packet.clone(),
                coordinator.clone(),
            ),
            |(client, stream, writer, connect_packet, coordinator), _: Mailbox<()>| {
                let mut reader = PacketDecoder::from_stream(stream);

                loop {
                    match reader.decode_packet(connect_packet.protocol_version) {
                        Ok(message) => {
                            println!("Received packet {:?}", message);
                            match message {
                                MqttPacket::Subscribe(sub) => {
                                    coordinator.request(Subscribe(sub, writer.clone()));
                                }
                                MqttPacket::Publish(packet) => {
                                    coordinator.request(Publish(packet, writer.clone()));
                                }
                                MqttPacket::Pingreq => {
                                    if writer.request(MqttPacket::Pingresp) {
                                        println!("Sent pong");
                                    } else {
                                        eprintln!("Failed to send pong");
                                    }
                                }
                                MqttPacket::Puback(packet) | MqttPacket::Pubrel(packet) => {
                                    coordinator.request(packet);
                                }
                                other => println!("Received other packet {:?}", other),
                            }
                        }
                        Err(err) => panic!("A decoding error ocurred: {:?}", err),
                    };
                }
            },
        );

        let client_id = connect_packet.client_id.clone();
        let is_v5 = connect_packet.protocol_version == 5;

        // send connack response to client
        writer.request(MqttPacket::Connack(ConnackPacket {
            properties: None,
            reason_code: if is_v5 { Some(0) } else { None },
            return_code: if !is_v5 { Some(0) } else { None },
            session_present: false,
        }));

        ClientProcess {
            this: this.clone(),
            coordinator: coordinator.clone(),
            writer,
            connect_packet,
            is_v5,
            client_id,
        }
    }
}

// =====================================
// Writer process
// =====================================
pub struct WriterProcess {
    stream: TcpStream,
    connect_packet: ConnectPacket,
    client_id: String,
    is_v5: bool,
}

impl AbstractProcess for WriterProcess {
    type Arg = (TcpStream, ConnectPacket);
    type State = Self;

    fn init(this: ProcessRef<Self>, (stream, connect_packet): Self::Arg) -> Self::State {
        let client_id = connect_packet.client_id.clone();
        let is_v5 = connect_packet.protocol_version == 5;
        WriterProcess {
            stream,
            connect_packet,
            is_v5,
            client_id,
        }
    }
}

/// Write message
impl ProcessRequest<MqttPacket> for WriterProcess {
    type Response = bool;

    fn handle(state: &mut WriterProcess, packet: MqttPacket) -> Self::Response {
        println!(
            "[Writer {}] Received Mqtt Packet {:?}",
            state.client_id, packet
        );
        match packet.encode(state.connect_packet.protocol_version) {
            Err(encode_err) => {
                eprintln!("Failed to encode packet {}", encode_err);
                false
            }
            Ok(encoded) => {
                println!("[Writer {}] Successfully encoded message", state.client_id);
                if let Err(e) = state.stream.write_all(&encoded) {
                    eprintln!("Failed to write to stream {}", e);
                    return false;
                }
                println!("[Writer {}] Successfully wrote message", state.client_id);
                true
            }
        }
    }
}
