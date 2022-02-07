pub mod queue;
pub mod tcp_reader;
pub mod tcp_writer;

use lunatic::{lookup, net, process, Mailbox, Request};

use crate::structure::{BrokerRequest, BrokerResponse, SessionConfig, WriterMessage};
use mqtt_packet_3_5::{ConnackPacket, MqttPacket, PacketDecoder};

pub fn connect_client(mut stream: net::TcpStream, mailbox: Mailbox<()>) {
    let broker = lookup::<Request<BrokerRequest, BrokerResponse>>("broker", "1.0.0");
    let broker = broker.unwrap().unwrap();
    let this = process::this(&mailbox);

    // first, wait for a connect message
    let mut reader = PacketDecoder::from_stream(&mut stream);
    let client_id: String;
    // let mut cursor = 0usize;
    let connect_packet = if let Ok(MqttPacket::Connect(packet)) = reader.decode_packet(5) {
        client_id = packet.client_id.clone();
        println!("CONNECT FLAGS {:?}", packet);
        if !packet.clean_session {
            let res = broker
                .request(BrokerRequest::MoveToExistingSession(SessionConfig {
                    stream: stream.clone(),
                    connect_packet: packet.clone(),
                }))
                .unwrap();
            println!("RESPONSE FROM BROKER INITIAL CONNECT {:?}", res);
            if let BrokerResponse::ExistingSession(Some(proc)) = res {
                println!("Transferred control to existing process {:?}", packet);
                return;
            }
        }
        packet
    } else {
        panic!("First request is not CONNECT");
    };

    println!("SETUP CONNECTION {}", client_id);

    // spawn a process that will send responses to tcp stream
    // let this = process::this(&mailbox);
    let writer_process = process::spawn_with(
        (stream.clone(), connect_packet.clone()),
        tcp_writer::write_mqtt,
    )
    .unwrap();

    // If the Server accepts a connection with Clean Start set to 1, the Server
    // MUST set Session Present to 0 in the CONNACK packet in addition to
    // setting a 0x00 (Success) Reason Code in the CONNACK packet
    let is_v5 = connect_packet.protocol_version == 5;
    writer_process.request(WriterMessage::Connack(ConnackPacket {
        properties: None,
        reason_code: if is_v5 { Some(0) } else { None },
        return_code: if !is_v5 { Some(0) } else { None },
        session_present: false,
    }));

    let _ = process::spawn_with(
        (
            client_id.clone(),
            writer_process.clone(),
            stream.clone(),
            broker.clone(),
            connect_packet,
        ),
        tcp_reader::handle_tcp,
    );

    // this should also destroy the other session if any
    if let BrokerResponse::Registered = broker
        .request(BrokerRequest::RegisterSession(
            client_id.clone(),
            this.clone(),
        ))
        .unwrap()
    {
        println!("Registered new process for client {}", client_id);
    }
}
