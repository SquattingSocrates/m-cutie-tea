use lunatic::{net,
    process::{self, Process},
    Mailbox,
    Request,
    Message,
    Tag,
    TransformMailbox,
    Config,
    Environment,
    lookup
};
use std::io::{self, BufReader, Write};
use std::io::prelude::*;
use std::time::SystemTime;
use std::collections::HashMap;
use num::integer::Integer;

use mqtt_broker::mqtt::{
    message::{
        MqttMessage,
        FixedHeader,
        VariableHeader,
        ConnectFlags,
        MqttPayload,
        SubscriptionRequest,
    },
    flags::ControlPacketType,
    parser::ByteParser,
};
use serde::{Deserialize, Serialize};

// static mut topics: HashMap<String, Vec<Mailbox>> = HashMap::new();

#[derive(Debug)]
pub struct ProcessCtx<'a> {
    pub tcp_stream: net::TcpStream,
    pub topics: &'a mut HashMap<String, Vec<Mailbox<Vec<u8>>>>
}


// static topics: HashMap<String, Vec<Mailbox<Vec<u8>>>> = HashMap::new(); 
// lazy_static! {
//     static ref TOPICS: HashMap<String, Vec<Mailbox<String>> = {
//         HashMap::new();
//     };
// }

fn main() {
    // let listener = net::TcpListener::bind("127.0.0.1:1883").unwrap();
    // println!("Started server at port 1883");
    // // let mut topics: 'static HashMap<String, Vec<Mailbox<Vec<u8>>>> = HashMap::new();
    // let queue_proc_mailbox = process::spawn(handle_queues).unwrap();
    // // let listeners = HashMap::new();
    // while let Ok((tcp_stream, _peer)) = listener.accept() {
    //     // Pass the TCP stream as a context to the new process. We can't use a closures that
    //     // capture parent variables because no memory is shared between processes.
    //     process::spawn_with(tcp_stream, handle).unwrap();
    // }


    let mut client_conf = Config::new(5_000_000, None);
    client_conf.allow_namespace("lunatic::");
    client_conf.allow_namespace("wasi_snapshot_preview1::fd_write");
    client_conf.allow_namespace("wasi_snapshot_preview1::clock_time_get");
    let mut client_env = Environment::new(client_conf).unwrap();
    let client_module = client_env.add_this_module().unwrap();

    // Create a coordinator and register it inside the environment
    let coordinator = process::spawn(queue_process).unwrap();
    client_env
        .register("coordinator", "1.0.0", coordinator)
        .unwrap();

    let port = 1883;
    println!("Started server on port {}", port);
    let address = format!("127.0.0.1:{}", port);
    let listener = net::TcpListener::bind(address).unwrap();
    while let Ok((stream, _)) = listener.accept() {
        println!("GOT SOMETHING ON STREAM");
        client_module
            .spawn_with(stream, client_process)
            .unwrap();
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum QueueRequest {
    Publish(String, String),
    Subscribe(Vec<SubscriptionRequest>),
    Disconnect
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum QueueResponse {
    Puback(String),
    Suback(String)
}

#[derive(Debug, Serialize, Deserialize)]
struct Client {
    // link: Tag,
    username: String,
    // All channels that the client joined
    // channels: HashSet<Process<ChannelMessage>>,
}

struct Subscription {
    link: Tag,
    topic: String,
    // All channels that the client joined
    process: Process<QueueResponse>,
    qos: u8
}

pub fn queue_process(mailbox: Mailbox<Request<QueueRequest, QueueResponse>>) {
    let mut queues: HashMap<String, Vec<Subscription>> = HashMap::new();

    let mailbox = mailbox.catch_link_panic();

    loop {
        let message = mailbox.receive();
        println!("QUEUE Received message");

        if let Message::Normal(message) = message {
            let message = message.unwrap();
            println!("Got a normal message {:?}", message.data());

            match message.data() {
                QueueRequest::Publish(topic, message) => {
                    let v = queues.entry(topic.to_string()).or_insert(Vec::with_capacity(100));
                    for sub in v {
                        sub.process.send(QueueResponse::Puback(topic.to_string()));
                    }
                }
                QueueRequest::Subscribe(subs) => {
                    let client = message.sender();
                    let client_link = client.link();
                    for sub in subs {
                        println!("Adding sub to list {:?}", sub);
                        let v = queues.entry(sub.topic.to_string()).or_insert(Vec::with_capacity(100));
                        v.push(Subscription {
                            link: client_link,
                            topic: sub.topic.to_string(),
                            process: client.clone(),
                            qos: sub.qos
                        });
                        client.send(QueueResponse::Suback(sub.topic.to_string()))
                        // channels.insert(channel_name.clone(), (channel.clone(), 1));
                        // v.push()
                    }
                    message.reply(QueueResponse::Suback(String::from("success")));
                }
                QueueRequest::Disconnect => {
                    println!("Received disconnect");
                }
            }
        }

        // if let Message::Normal(message) = message {
        //     let message = message.unwrap();
        //     let data = message.data();

        //     match data {
        //         QueueRequest::Publish => {
        //             let client = message.sender();
        //             let client_link = client.link();
        //             // Tags are always unique inside of a process.
        //             // let client_username = format!("user_{}", client_link.id());

        //             // clients.insert(
        //             //     client.id(),
        //             //     Client {
        //             //         link: client_link,
        //             //         username: client_username.clone(),
        //             //         channels: HashSet::new(),
        //             //     },
        //             // );

        //             message.reply(CoordinatorResponse::ServerJoined(Info {
        //                 username: client_username,
        //                 total_clients: clients.len(),
        //             }))
        //         }
        //     }
        // }
    }
}


pub fn client_process(mut stream: net::TcpStream, mailbox: Mailbox<String>) {
    // Username of the client
    // let mut username;
    // The number of all clients on the server.
    // let total_clients;

    // Look up the coordinator or fail if it doesn't exist.
    //
    // Notice that the the `Process<T>` type returned here can't be checked during compile time, as
    // it can be arbitrary. Sending a message of wrong type to the coordinator will fail during
    // runtime only, because the deserialization step will fail.
    println!("Started req proc");
    let coordinator = lookup("coordinator", "1.0.0");
    println!("GOT COORD {:?}", coordinator);
    let coordinator = coordinator.unwrap().unwrap();
    println!("LOOKED UP COORDINATOR");
    // Let the coordinator know that we joined.
    // if let QueueResponse::Puback(s) =
    //     coordinator.request(QueueRequest::Publish(String::from("general"), String::from("hello"))).unwrap()
    // {
    //     // Update username with an coordinator auto generated one.
    //     // username = client_info.username;
    //     // total_clients = client_info.total_clients;
    //     println!("puback: {}", s);
    // } else {
    //     eprintln!("PArsed something else");
    //     unreachable!("Received unexpected message");
    // }

    loop {
        let mut reader = BufReader::new(&mut stream);
        match reader.fill_buf() {
            Ok(v) => {
                let start = SystemTime::now();
                let vc = v.to_vec();
                if vc.len() == 0 {
                    continue
                }
                println!("READ THIS DATA {:?}", vc);
                let mut parser = ByteParser::new(vc);
                match parser.parse_mqtt() {
                    Ok(msg) => {
                        let since_the_epoch = SystemTime::now()
                            .duration_since(start)
                            .expect("Time went backwards");
                        println!("Time spent parsing -> {:?}", since_the_epoch);
                        println!("Parsed message {:?}", msg);
                        // let ret = handle_message(msg, mailbox);
                        match msg {
                            MqttMessage::Connect(fixed_header, variable_header, payload) => {
                                println!("Writing response {:?}", [ControlPacketType::CONNACK.bits(), 2u8, 0x0, 0x0]);
                                stream.write(&[ControlPacketType::CONNACK.bits() << 4, 0b00000010, 0x0, 0x0]).unwrap();
                            }
                            MqttMessage::Publish(fixed_header, variable_header, payload) => {
                                let m = format!("Successfully published {} to queue {} with response", variable_header.topic_name, payload);
                                if let QueueResponse::Puback(s) = coordinator.request(QueueRequest::Publish(variable_header.topic_name, payload)).unwrap() {
                                    println!("{} with response {}", m, s);
                                    stream.write(&[ControlPacketType::CONNACK.bits() << 4, 0b00000010, 0x0, 0x0]).unwrap();
                                }
                            }
                            MqttMessage::Subscribe(fixed_header, variable_header, payload) => {
                                let m = format!("Successfully subscribed to queue {:?}", payload);
                                println!("{}", m);
                                
                                // let res = coordinator.request(QueueRequest::Subscribe(payload)).unwrap();
                                // println!("Got this response from QUEUE REQUEST {:?}", res);
                                if let QueueResponse::Suback(s) = coordinator.request(QueueRequest::Subscribe(payload)).unwrap() {
                                    println!("{} with response {}", m, s);
                                    stream.write(&[
                                        (ControlPacketType::SUBACK.bits() << 4),
                                        0x3,
                                        (variable_header.message_id & 0xff) as u8,
                                        variable_header.message_id as u8,
                                        fixed_header.qos
                                    ]).unwrap();
                                }
                            }
                            MqttMessage::Pingreq(fixed_header) => {
                                println!("Got pingreq");
                                stream.write(&[(ControlPacketType::PINGRESP.bits() << 4), 0x0]).unwrap();
                            }
                            x => {
                                eprintln!("Unknown request type encountered {:?}", x);
                                stream.write(&[0x0]).unwrap();
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("SOMETHING FAILED {:?}", e);
                        stream.write(e.as_bytes()).unwrap();
                    }
                }
            }
            Err(e) => {
                println!("Error while filling buffer {:?}", e);
            }
        }
    }

    // This process is in charge of turning the raw tcp stream into higher level messages that are
    // sent to the client. It's linked to the client and if one of them fails the other will too.
    // let this = process::this(&mailbox);
    // let (_, mailbox) = process::spawn_link_unwrap_with(
    //     mailbox,
    //     (this, stream.clone()),
    //     |(client, mut stream), _: Mailbox<()>| {
            
    //     },
    // )
    // .unwrap();

    // Let the state process know that we left
    // coordinator
    //     .request(QueueRequest::Disconnect)
    //     .unwrap();
}

fn handle_queues(mailbox: Mailbox<String>) {
    loop {
        let msg = mailbox.receive().unwrap();
        println!("Received msg {:?}", msg);
    }
}
