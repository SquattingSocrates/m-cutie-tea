use crate::structure::{ConnectionMessage, WriterMessage, WriterResponse};
use lunatic::{net, Mailbox, Request};
use mqtt_packet_3_5::{
    ConnackPacket, ConnectPacket, FixedHeader, MqttPacket, PacketEncoder, PacketType, PublishPacket,
};
use std::io::Write;

pub struct TcpWriter {
    pub is_receiving: bool,
    stream: net::TcpStream,
    qos_1_buf: Vec<PublishPacket>,
    qos_2_buf: Vec<PublishPacket>,
    connect_packet: ConnectPacket,
}

impl TcpWriter {
    pub fn new(stream: net::TcpStream, connect_packet: ConnectPacket) -> TcpWriter {
        TcpWriter {
            stream,
            connect_packet,
            is_receiving: true,
            qos_1_buf: vec![],
            qos_2_buf: vec![],
        }
    }

    pub fn use_new_stream(&mut self, stream: Option<net::TcpStream>) {
        self.stream = stream.unwrap();
        let protocol_version = self.connect_packet.protocol_version;
        let connack = ConnackPacket {
            fixed: FixedHeader::for_type(PacketType::Connack),
            length: 0,
            properties: None,
            reason_code: if protocol_version == 5 { Some(0) } else { None },
            return_code: if protocol_version != 5 { Some(0) } else { None },
            session_present: false,
        };
        match PacketEncoder::encode_packet(MqttPacket::Connack(connack), protocol_version) {
            Ok(buf) => self.write_buf(buf),
            Err(e) => eprintln!("Failed to encode connack message {:?}", e),
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
        match PacketEncoder::encode_packet(packet, self.connect_packet.protocol_version) {
            Ok(buf) => self.write_buf(buf),
            Err(e) => eprintln!("Failed to encode message {:?}", e),
        }
    }
}

// This process has a one mailbox that it's listening to
// but there can be multiple types of messages
pub fn write_mqtt(
    (stream, connect_packet): (net::TcpStream, ConnectPacket),
    mailbox: Mailbox<Request<WriterMessage, WriterResponse>>,
) {
    let mut state = TcpWriter::new(stream, connect_packet);
    loop {
        println!("WRITER IS RECEIVING {}", state.is_receiving);
        match mailbox.receive() {
            Ok(msg) => {
                match msg.data() {
                    WriterMessage::Connection(msg, maybe_stream) => {
                        if let ConnectionMessage::Destroy = msg {
                            println!("Destroying old tcp_writer process");
                            return;
                        }
                        println!("Received WriterMessage::Connection {:?}", msg);
                        state.use_new_stream(maybe_stream.clone());
                    }
                    WriterMessage::Queue(packet) => {
                        println!("GOT QUEUE MESSAGE {:?}", packet);
                        state.write_packet(packet.clone());
                    }
                }
                msg.reply(WriterResponse::Published)
            }
            Err(e) => {
                eprintln!("Failed to receive mailbox {:?}", e);
            }
        }
    }
}
