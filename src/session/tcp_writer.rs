use crate::structure::{QueueProcess, QueueRequest, WriterMessage, WriterResponse};
use lunatic::{net, Mailbox, Request};
use mqtt_packet_3_5::{
    ConfirmationPacket, ConnackPacket, ConnectPacket, FixedHeader, MqttPacket, Packet, PacketType,
    PublishPacket, UnsubackCode, UnsubackPacket,
};
use std::collections::HashMap;
use std::io::Write;

pub struct TcpWriter {
    pub is_receiving: bool,
    stream: net::TcpStream,
    qos_1_buf: Vec<PublishPacket>,
    qos_2_buf: Vec<PublishPacket>,
    connect_packet: ConnectPacket,
    qos_1_waiting: HashMap<u16, QueueProcess>,
    qos_2_waiting: HashMap<u16, QueueProcess>,
}

impl TcpWriter {
    pub fn new(stream: net::TcpStream, connect_packet: ConnectPacket) -> TcpWriter {
        TcpWriter {
            stream,
            connect_packet,
            is_receiving: true,
            qos_1_buf: vec![],
            qos_2_buf: vec![],
            qos_1_waiting: HashMap::new(),
            qos_2_waiting: HashMap::new(),
        }
    }

    fn write_buf(&mut self, buf: Vec<u8>) {
        match self.stream.write_all(&buf) {
            Ok(_) => println!("Wrote connection response to client"),
            Err(e) => {
                eprintln!("Failed to write connection response to stream {:?}", e)
            }
        };
    }

    pub fn write_packet(&mut self, packet: MqttPacket) {
        match packet.encode(self.connect_packet.protocol_version) {
            Ok(buf) => self.write_buf(buf),
            Err(e) => eprintln!("Failed to encode message {:?}", e),
        }
    }
}

/// This process has a one mailbox that it's listening to
/// but there can be multiple types of messages
pub fn write_mqtt(
    (stream, connect_packet): (net::TcpStream, ConnectPacket),
    mailbox: Mailbox<Request<WriterMessage, WriterResponse>>,
) {
    let mut state = TcpWriter::new(stream, connect_packet);
    loop {
        println!(
            "[Writer {}] WRITER IS RECEIVING {}",
            state.connect_packet.client_id, state.is_receiving
        );
        match mailbox.receive() {
            Ok(msg) => {
                let mut response = WriterResponse::Success;
                match msg.data() {
                    WriterMessage::Connack(packet) => {
                        state.write_packet(MqttPacket::Connack(packet.clone()));
                    }
                    WriterMessage::Suback(packet) => {
                        state.write_packet(MqttPacket::Suback(packet.clone()));
                    }
                    WriterMessage::Unsuback(message_id) => {
                        state.write_packet(MqttPacket::Unsuback(UnsubackPacket {
                            message_id: *message_id,
                            granted: vec![UnsubackCode::Success],
                            properties: None,
                        }));
                    }
                    WriterMessage::Publish(data) => {
                        match state.stream.write_all(data) {
                            Ok(_) => {
                                println!(
                                    "[Writer {}] Wrote publish response to client",
                                    state.connect_packet.client_id
                                );
                                response = WriterResponse::Sent;
                            }
                            Err(e) => {
                                eprintln!(
                                    "[Writer {}] Failed to write publish response to stream {:?}",
                                    state.connect_packet.client_id, e
                                );
                                response = WriterResponse::Failed;
                            }
                        };
                    }
                    WriterMessage::Confirmation(packet) => {
                        state.write_buf(
                            packet
                                .encode(state.connect_packet.protocol_version)
                                .unwrap(),
                        );
                    }
                    WriterMessage::Pong => {
                        state.write_packet(MqttPacket::Pingresp);
                    }
                    WriterMessage::Die => {
                        println!(
                            "[Writer {}] Killing TCP_Writer Process",
                            state.connect_packet.client_id
                        );
                        return;
                    }
                }
                println!(
                    "[Writer {}] WRITER SENDING REPLY {:?}",
                    state.connect_packet.client_id, response
                );
                msg.reply(response)
            }
            Err(e) => {
                eprintln!(
                    "[Writer {}] Failed to receive mailbox {:?}",
                    state.connect_packet.client_id, e
                );
            }
        }
    }
}
