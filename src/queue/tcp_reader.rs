use super::tcp_writer;
use lunatic::{
    lookup, net,
    process::{self, Process},
    Mailbox, Request,
};
use std::collections::HashMap;
use std::io::prelude::*;
use std::io::BufReader;

use crate::mqtt::{message::MqttMessage, parser::ByteParser};
use crate::structure::{
    BrokerRequest, BrokerResponse, ConnectionMessage, Queue, QueueRequest, SessionConfig,
    SessionRequest, WriterMessage, WriterQueueResponse,
};

pub fn handle_tcp(mut stream: net::TcpStream, mailbox: Mailbox<SessionRequest>) {
    let broker = lookup::<Request<BrokerRequest, BrokerResponse>>("broker", "1.0.0");
    let broker = broker.unwrap().unwrap();

    // controls whether data is read from tcp_stream
    let mut is_receiving = true;

    // session_data
    let mut client_id = String::new();

    let this = process::this(&mailbox);
    let mut pub_queues: HashMap<String, Queue> = HashMap::new();

    // first, wait for a connect message
    let mut reader = BufReader::new(&mut stream);
    let data = reader.fill_buf().unwrap();
    let vc = data.to_vec();
    if vc.is_empty() {
        panic!("Received empty connect request");
    }
    println!("CONNECT {:?}", vc);
    let mut parser = ByteParser::new(vc);
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

    println!("SETUP CONNECTION {} {}", client_id, is_receiving);

    // spawn a process that will send responses to tcp stream
    // let this = process::this(&mailbox);
    let writer_process = process::spawn_with(stream.clone(), tcp_writer::write_mqtt).unwrap();

    // this should also destroy the other session if any
    if let BrokerResponse::Registered = broker
        .request(BrokerRequest::RegisterSession(
            client_id.clone(),
            this.clone(),
        ))
        .unwrap()
    {
        // send writer first CONNECT
        writer_process.send(WriterMessage::Connection(
            ConnectionMessage::Connect(0x0),
            Some(stream.clone()),
        ));
    }

    loop {
        if !is_receiving {
            println!(
                "Waiting for mailbox in tcp_reader. is_receiving: {}",
                is_receiving
            );
            match mailbox.receive() {
                Ok(new_stream) => match new_stream {
                    SessionRequest::Create(SessionConfig {
                        stream: new_stream, ..
                    }) => {
                        println!("RECEIVED RECONN IN MAILBOX");
                        stream = new_stream;
                        is_receiving = true;
                        writer_process.send(WriterMessage::Connection(
                            ConnectionMessage::Connect(0x0),
                            Some(stream.clone()),
                        ));
                        continue;
                    }
                    SessionRequest::Destroy => {
                        writer_process
                            .send(WriterMessage::Connection(ConnectionMessage::Destroy, None));
                        return;
                    }
                },
                Err(e) => {
                    eprintln!("Error while receiving new stream {:?}", e);
                    continue;
                }
            }
        }

        let mut reader = BufReader::new(&mut stream);
        println!(
            "Waiting for fill_buf in tcp_reader. is_receiving: {}",
            is_receiving
        );
        match reader.fill_buf() {
            Ok(v) => {
                let vc = v.to_vec();
                if vc.is_empty() {
                    // if vec is empty, the underlying stream is closed
                    is_receiving = false;
                    continue;
                }
                println!("RAW {:?}", vc);
                let mut parser = ByteParser::new(vc);
                // let mut cursor = 0usize;
                match parser.parse_mqtt() {
                    Ok(msg) => {
                        match &msg {
                            // handle authentication and session creation
                            MqttMessage::Connect(_, _, payload) => {
                                client_id = payload.client_id.clone();
                                if let BrokerResponse::Registered = broker
                                    .request(BrokerRequest::RegisterSession(
                                        client_id.clone(),
                                        this.clone(),
                                    ))
                                    .unwrap()
                                {
                                    writer_process.send(WriterMessage::Connection(
                                        ConnectionMessage::Connect(0),
                                        None,
                                    ))
                                }
                            }
                            MqttMessage::Disconnect(_) => writer_process.send(
                                WriterMessage::Connection(ConnectionMessage::Disconnect, None),
                            ),
                            MqttMessage::Pingreq(_) => writer_process
                                .send(WriterMessage::Connection(ConnectionMessage::Ping, None)),
                            // handle queue messages
                            MqttMessage::Publish(fixed, variable, payload) => {
                                let queue = pub_queues
                                    .entry(variable.topic_name.clone())
                                    .or_insert_with(|| {
                                        match broker
                                            .request(BrokerRequest::GetQueue(
                                                variable.topic_name.clone(),
                                            ))
                                            .unwrap()
                                        {
                                            BrokerResponse::MatchingQueue(q) => q,
                                            x => {
                                                eprintln!("Broker responded with non-queue response: {:?}", x);
                                                panic!("Broker messed up");
                                            }
                                        }
                                    });
                                queue.process.send(QueueRequest::Publish(
                                    fixed.clone(),
                                    variable.clone(),
                                    payload.clone(),
                                ));
                            }
                            MqttMessage::Subscribe(_, variable, subs) => {
                                match broker.request(BrokerRequest::Subscribe(
                                    client_id.clone(),
                                    subs.to_vec(),
                                    writer_process.clone(),
                                )) {
                                    Ok(data) => {
                                        println!("RESPONSE FROM BROKER ON SUBSCRIBE {:?}", data);
                                        writer_process.send(WriterMessage::Queue(
                                            WriterQueueResponse::Subscribe(
                                                variable.message_id,
                                                subs.to_vec(),
                                            ),
                                        ))
                                    }
                                    Err(e) => {
                                        eprintln!("Failed to subscribe {:?}", subs);
                                    }
                                }
                            }
                            _ => {
                                println!("received message: {:?}", msg);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to parse mqtt message: {:?}", e);
                        // stream.write_all(e.as_bytes()).unwrap();
                    }
                }
            }
            Err(e) => {
                println!("Error while filling buffer {:?}", e);
                is_receiving = false;
            }
        }
    }
}
