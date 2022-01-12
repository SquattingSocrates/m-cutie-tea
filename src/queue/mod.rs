pub mod queue;
pub mod tcp_reader;
pub mod tcp_writer;

use lunatic::{lookup, net, process, Mailbox, Request};
use std::io::prelude::*;
use std::io::BufReader;

use crate::mqtt::{message::MqttMessage, parser::ByteParser};
use crate::structure::{BrokerRequest, BrokerResponse, SessionConfig, SessionRequest};

pub fn connect_client(mut stream: net::TcpStream, mailbox: Mailbox<SessionRequest>) {
    let broker = lookup::<Request<BrokerRequest, BrokerResponse>>("broker", "1.0.0");
    let broker = broker.unwrap().unwrap();
    let this = process::this(&mailbox);

    // first, wait for a connect message
    let mut reader = BufReader::new(&mut stream);
    let data = reader.fill_buf().unwrap();
    let vc = data.to_vec();
    if vc.is_empty() {
        panic!("Received empty connect request");
    }
    println!("CONNECT {:?}", vc);
    let mut parser = ByteParser::new(vc);
    let client_id: String;
    // let mut cursor = 0usize;
    if let MqttMessage::Connect(_, variable, payload) = parser.parse_mqtt().unwrap() {
        client_id = payload.client_id.clone();
        println!("CONNECT FLAGS {:?}", variable.connect_flags);
        if !variable.connect_flags.clean_session {
            let res = broker
                .request(BrokerRequest::HasProcess(client_id.clone()))
                .unwrap();
            println!("RESPONSE FROM BROKER {:?}", res);
            if let BrokerResponse::ExistingSession(Some(proc)) = res {
                println!(
                    "Transferred control to existing process {:?} {:?}",
                    variable, payload
                );
                proc.send(SessionRequest::Create(SessionConfig {
                    stream: stream.clone(),
                    variable_header: variable,
                    payload: payload,
                }));

                return;
            }
        }
    } else {
        panic!("First request is not CONNECT");
    }

    println!("SETUP CONNECTION {}", client_id);

    // spawn a process that will send responses to tcp stream
    // let this = process::this(&mailbox);
    let writer_process = process::spawn_with(stream.clone(), tcp_writer::write_mqtt).unwrap();
    let _ = process::spawn_with(
        (
            client_id.clone(),
            writer_process.clone(),
            stream.clone(),
            broker.clone(),
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
