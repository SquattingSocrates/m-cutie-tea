use crate::structure::WriterRef;
use lunatic::process::{AbstractProcess, ProcessRef, StartProcess};
use lunatic::{abstract_process, Tag};
use lunatic::{net::TcpStream, Mailbox, Process};
use mqtt_packet_3_5::{ConnackPacket, ConnectPacket, MqttPacket, PacketDecoder};
use std::io::Write;
use std::time::SystemTime;
use uuid::Uuid;

use crate::coordinator::{CoordinatorProcess, CoordinatorProcessHandler};

pub struct ClientProcess {
    // this: ProcessRef<ClientProcess>,
    // coordinator: ProcessRef<CoordinatorProcess>,
    // writer: WriterRef,
    // connect_packet: ConnectPacket,
    // // pub reader: PacketDecoder<TcpStream>,
    // // protocol_version: u8,
    // is_v5: bool,
    // client_id: String,
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
                lunatic_log::error!("Unexpected value instead of connect_packet {:?}", x);
                panic!("Invalid connect packet");
            }
        };

        let writer = WriterProcess::start((stream.clone(), connect_packet.clone()), None);
        // Let the coordinator know that we joined.
        let writer_ref = WriterRef {
            process: Some(writer.clone()),
            client_id: connect_packet.client_id.clone(),
            session_id: Uuid::new_v4(),
            is_persistent_session: !connect_packet.clean_session,
        };
        let _ = coordinator.connect(
            this.clone(),
            writer_ref.clone(),
            connect_packet.clean_session,
        );

        Process::spawn_link(
            (
                this.clone(),
                stream,
                writer_ref.clone(),
                connect_packet.clone(),
                coordinator.clone(),
            ),
            |(_, stream, writer_ref, connect_packet, coordinator), _: Mailbox<()>| {
                let mut reader = PacketDecoder::from_stream(stream);
                let started_at = SystemTime::now();
                let writer = writer_ref.process.as_ref().unwrap();

                loop {
                    match reader.decode_packet(connect_packet.protocol_version) {
                        Ok(message) => {
                            lunatic_log::debug!("Received packet {:?}", message);
                            match message {
                                MqttPacket::Subscribe(sub) => {
                                    coordinator.subscribe(sub, writer_ref.clone());
                                }
                                MqttPacket::Publish(packet) => {
                                    coordinator.publish(packet, writer_ref.clone(), started_at);
                                }
                                MqttPacket::Pingreq => {
                                    if writer.write_packet(MqttPacket::Pingresp) {
                                        lunatic_log::debug!("Sent pong");
                                    } else {
                                        lunatic_log::error!("Failed to send pong");
                                    }
                                }
                                MqttPacket::Puback(packet)
                                | MqttPacket::Pubrel(packet)
                                | MqttPacket::Pubrec(packet) => {
                                    coordinator.confirm(packet, writer_ref.clone());
                                }
                                MqttPacket::Pubcomp(_packet) => {
                                    lunatic_log::debug!(
                                        "[Client {}] received pubcomp",
                                        writer_ref.client_id
                                    );
                                }
                                other => lunatic_log::debug!("Received other packet {:?}", other),
                            }
                        }
                        Err(err) => panic!("A decoding error ocurred: {:?}", err),
                    };
                }
            },
        );

        let is_v5 = connect_packet.protocol_version == 5;

        // send connack response to client
        writer.write_packet(MqttPacket::Connack(ConnackPacket {
            properties: None,
            reason_code: if is_v5 { Some(0) } else { None },
            return_code: if !is_v5 { Some(0) } else { None },
            session_present: false,
        }));

        ClientProcess {
            // this,
            // coordinator,
            // writer: writer_ref,
            // connect_packet,
            // is_v5,
            // client_id,
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
    // is_v5: bool,
}

#[abstract_process(visibility = pub)]
impl WriterProcess {
    #[init]
    fn init(_: ProcessRef<Self>, (stream, connect_packet): (TcpStream, ConnectPacket)) -> Self {
        let client_id = connect_packet.client_id.clone();
        // TODO: is_v5 will be needed for implementing v5 protocol
        // let is_v5 = connect_packet.protocol_version == 5;
        WriterProcess {
            stream,
            connect_packet,
            // is_v5,
            client_id,
        }
    }

    #[terminate]
    fn terminate(self) {
        lunatic_log::debug!("Shutdown process");
    }

    #[handle_link_trapped]
    fn handle_link_trapped(&self, _tag: Tag) {
        lunatic_log::debug!("Link trapped");
    }

    #[handle_request]
    fn write_packet(&mut self, packet: MqttPacket) -> bool {
        lunatic_log::debug!(
            "[Writer {}] Received Mqtt Packet {:?}",
            self.client_id,
            packet
        );
        match packet.encode(self.connect_packet.protocol_version) {
            Err(encode_err) => {
                lunatic_log::error!("Failed to encode packet {}", encode_err);
                false
            }
            Ok(encoded) => {
                lunatic_log::debug!("[Writer {}] Successfully encoded message", self.client_id);
                if let Err(e) = self.stream.write_all(&encoded) {
                    lunatic_log::error!("Failed to write to stream {}", e);
                    return false;
                }
                lunatic_log::info!("[Writer {}] Successfully wrote message", self.client_id);
                true
            }
        }
    }
}
