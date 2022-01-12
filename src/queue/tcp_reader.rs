use lunatic::{
    net,
    process::{self, Process},
    Mailbox, Request,
};
use std::collections::HashMap;
use std::io::prelude::*;
use std::io::BufReader;

use crate::mqtt::{message::MqttMessage, parser::ByteParser};
use crate::structure::{
    BrokerRequest, BrokerResponse, ConnectionMessage, Queue, QueueRequest, QueueResponse,
    SessionConfig, SessionRequest, WriterMessage,
};

pub fn handle_tcp(
    (mut client_id, writer_process, mut stream, broker): (
        String,
        Process<WriterMessage>,
        net::TcpStream,
        Process<Request<BrokerRequest, BrokerResponse>>,
    ),
    mailbox: Mailbox<SessionRequest>,
) {
    // controls whether data is read from tcp_stream
    let mut is_receiving = true;
    let this = process::this(&mailbox);
    let mut pub_queues: HashMap<String, Queue> = HashMap::new();
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
                                    if fixed.qos == 0 {
                                        None
                                    } else {
                                        Some(writer_process.clone())
                                    },
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
                                            QueueResponse::Subscribe(
                                                variable.message_id,
                                                subs.to_vec(),
                                            ),
                                        ))
                                    }
                                    Err(e) => {
                                        eprintln!("Failed to subscribe {:?}. Error: {}", subs, e);
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
