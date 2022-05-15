use crate::structure::{QueueProcess, WriterMessage, WriterResponse};
use lunatic::{
    net,
    process::{AbstractProcess, ProcessRef, ProcessRequest, Request},
    Mailbox,
};
use mqtt_packet_3_5::{
    ConnectPacket, MqttPacket, Packet, PacketType, UnsubackCode, UnsubackPacket,
};
use std::collections::HashMap;
use std::io::Write;

pub struct TcpWriter {
    pub is_receiving: bool,
    stream: net::TcpStream,
    connect_packet: ConnectPacket,
    pub_queues: HashMap<u16, QueueProcess>,
}

impl TcpWriter {
    pub fn new(stream: net::TcpStream, connect_packet: ConnectPacket) -> TcpWriter {
        TcpWriter {
            stream,
            connect_packet,
            is_receiving: true,
            pub_queues: HashMap::new(),
        }
    }

    fn write_buf(&mut self, buf: Vec<u8>, cmd: PacketType) {
        match self.stream.write_all(&buf) {
            Ok(_) => println!("Wrote {:?} response to client", cmd),
            Err(e) => {
                eprintln!("Failed to write connection response to stream {:?}", e)
            }
        };
    }

    pub fn write_packet(&mut self, packet: MqttPacket) {
        match packet.encode(self.connect_packet.protocol_version) {
            Ok(buf) => self.write_buf(buf, PacketType::Pingreq),
            Err(e) => eprintln!("Failed to encode message {:?}", e),
        }
    }

    pub fn save_queue(&mut self, msg_id: u16, queue: &QueueProcess) {
        self.pub_queues.insert(msg_id, queue.clone());
    }

    pub fn get_queue(&mut self, msg_id: &u16) -> &QueueProcess {
        self.pub_queues.get(msg_id).unwrap()
    }
}

impl AbstractProcess for TcpWriter {
    type Arg = (net::TcpStream, ConnectPacket);
    type State = Self;

    fn init(_: ProcessRef<Self>, start: (net::TcpStream, ConnectPacket)) -> Self {
        TcpWriter::new(start.0, start.1)
    }
}

impl ProcessRequest<WriterMessage> for TcpWriter {
    type Response = WriterResponse;

    fn handle(state: &mut Self::State, msg: WriterMessage) -> WriterResponse {
        let mut response = WriterResponse::Success;
        match msg {
            WriterMessage::Connack(packet) => {
                state.write_packet(MqttPacket::Connack(packet.clone()));
            }
            WriterMessage::Suback(packet) => {
                state.write_packet(MqttPacket::Suback(packet.clone()));
            }
            WriterMessage::Unsuback(message_id) => {
                state.write_packet(MqttPacket::Unsuback(UnsubackPacket {
                    message_id,
                    granted: vec![UnsubackCode::Success],
                    properties: None,
                }));
            }
            WriterMessage::Publish(qos, msg_id, data, queue) => {
                match state.stream.write_all(&data) {
                    Ok(_) => {
                        // println!(
                        //     "[Writer {}] Wrote publish response to client",
                        //     state.connect_packet.client_id
                        // );
                        state.save_queue(msg_id, queue);
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
            WriterMessage::Confirmation(packet, queue) => {
                state.write_buf(
                    packet
                        .encode(state.connect_packet.protocol_version)
                        .unwrap(),
                    packet.cmd,
                );
            }
            WriterMessage::GetQueue(msg_id) => {
                response = WriterResponse::MatchingQueue(state.get_queue(msg_id).clone());
            }
            WriterMessage::Pong => {
                state.write_packet(MqttPacket::Pingresp);
            }
            WriterMessage::Die => {
                println!(
                    "[Writer {}] Killing TCP_Writer Process",
                    state.connect_packet.client_id
                );
                AbstractProcess::terminate(state);
            }
        }
        // println!(
        //     "[Writer {}] WRITER SENDING REPLY {:?}",
        //     state.connect_packet.client_id, response
        // );
        // msg.reply(response)
        response
    }
}

// This process has a one mailbox that it's listening to
// but there can be multiple types of messages
// pub fn write_mqtt(
//     (stream, connect_packet): (net::TcpStream, ConnectPacket),
//     mailbox: Mailbox<Request<WriterMessage, WriterResponse>>,
// ) {
//     let mut state = TcpWriter::new(stream, connect_packet);
//     loop {
//         // println!(
//         //     "[Writer {}] WRITER IS RECEIVING {}",
//         //     state.connect_packet.client_id, state.is_receiving
//         // );
//         match mailbox.receive() {
//             Ok(msg) => {
//                 let mut response = WriterResponse::Success;
//                 match msg.data() {
//                     WriterMessage::Connack(packet) => {
//                         state.write_packet(MqttPacket::Connack(packet.clone()));
//                     }
//                     WriterMessage::Suback(packet) => {
//                         state.write_packet(MqttPacket::Suback(packet.clone()));
//                     }
//                     WriterMessage::Unsuback(message_id) => {
//                         state.write_packet(MqttPacket::Unsuback(UnsubackPacket {
//                             message_id,
//                             granted: vec![UnsubackCode::Success],
//                             properties: None,
//                         }));
//                     }
//                     WriterMessage::Publish(qos, msg_id, data, queue) => {
//                         match state.stream.write_all(&data) {
//                             Ok(_) => {
//                                 // println!(
//                                 //     "[Writer {}] Wrote publish response to client",
//                                 //     state.connect_packet.client_id
//                                 // );
//                                 state.save_queue(msg_id, queue);
//                                 response = WriterResponse::Sent;
//                             }
//                             Err(e) => {
//                                 eprintln!(
//                                     "[Writer {}] Failed to write publish response to stream {:?}",
//                                     state.connect_packet.client_id, e
//                                 );
//                                 response = WriterResponse::Failed;
//                             }
//                         };
//                     }
//                     WriterMessage::Confirmation(packet, queue) => {
//                         state.write_buf(
//                             packet
//                                 .encode(state.connect_packet.protocol_version)
//                                 .unwrap(),
//                             packet.cmd,
//                         );
//                     }
//                     WriterMessage::GetQueue(msg_id) => {
//                         response = WriterResponse::MatchingQueue(state.get_queue(msg_id).clone());
//                     }
//                     WriterMessage::Pong => {
//                         state.write_packet(MqttPacket::Pingresp);
//                     }
//                     WriterMessage::Die => {
//                         println!(
//                             "[Writer {}] Killing TCP_Writer Process",
//                             state.connect_packet.client_id
//                         );
//                         return;
//                     }
//                 }
//                 // println!(
//                 //     "[Writer {}] WRITER SENDING REPLY {:?}",
//                 //     state.connect_packet.client_id, response
//                 // );
//                 msg.reply(response)
//             }
//             Err(e) => {
//                 eprintln!(
//                     "[Writer {}] Failed to receive mailbox {:?}",
//                     state.connect_packet.client_id, e
//                 );
//             }
//         }
//     }
// }
