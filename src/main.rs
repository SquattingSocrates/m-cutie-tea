use lunatic::{net, process, Mailbox};
use std::io::{self, BufReader, Write};
use std::io::prelude::*;
use std::time::SystemTime;
use std::collections::HashMap;
use num::integer::Integer;
use lazy_static::lazy_static;

use mqtt_broker::parser::{
    ControlPacketType,
    MqttMessage,
    ByteParser,
    FixedHeader,
    VariableHeader,
    ConnectFlags,
};

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
    let listener = net::TcpListener::bind("127.0.0.1:1883").unwrap();
    println!("Started server at port 1883");
    // let mut topics: 'static HashMap<String, Vec<Mailbox<Vec<u8>>>> = HashMap::new();
    let queue_proc_mailbox = process::spawn(handle_queues).unwrap();
    // let listeners = HashMap::new();
    while let Ok((tcp_stream, _peer)) = listener.accept() {
        // Pass the TCP stream as a context to the new process. We can't use a closures that
        // capture parent variables because no memory is shared between processes.
        process::spawn_with(tcp_stream, handle).unwrap();
    }
}

fn handle_queues(mailbox: Mailbox<String>) {
    loop {
        let msg = mailbox.receive().unwrap();
        println!("Received msg {:?}", msg);
    }
}

fn handle(mut tcp_stream: net::TcpStream, mailbox: Mailbox<()>) {
    loop {
        let mut reader = BufReader::new(&mut tcp_stream);
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
                        if msg.fixed_header.control_packet == ControlPacketType::CONNECT {
                            println!("Writing response {:?}", [ControlPacketType::CONNACK.bits(), 2u8, 0x0, 0x0]);
                            tcp_stream.write(&[ControlPacketType::CONNACK.bits() << 4, 0b00000010, 0x0, 0x0]).unwrap();
                        } else if msg.fixed_header.control_packet == ControlPacketType::PUBLISH {
                            
                        } else {
                            println!("OTHER TYPE UNHANDLED");
                        }
                    }
                    Err(e) => {
                        eprintln!("SOMETHING FAILED {:?}", e);
                        tcp_stream.write(e.as_bytes()).unwrap();
                    }
                }
            }
            Err(e) => {
                println!("Error while filling buffer {:?}", e);
            }
        }
    }
    // let start = SystemTime::now();
    // let mut parser = ByteParser::new(tcp_stream.clone().bytes());
    // match parser.parse_mqtt() {
    //     Ok(msg) => {
    //         let since_the_epoch = SystemTime::now()
    //             .duration_since(start)
    //             .expect("Time went backwards");
    //         println!("Time spent parsing -> {:?}", since_the_epoch);
    //         println!("Parsed message {:?}", msg);
    //         // let ret = handle_message(msg, mailbox);
    //         if msg.fixed_header.control_packet == ControlPacketType::CONNECT {
    //             println!("Writing response {:?}", [ControlPacketType::CONNACK.bits(), 2u8, 0x0, 0x0]);
    //             tcp_stream.write(&[ControlPacketType::CONNACK.bits() << 4, 0b00000010, 0x0, 0x0]).unwrap();
    //             // let mut s = Vec::new();
    //             let mut reader = BufReader::new(&mut tcp_stream);
    //             // tcp_stream.read(&mut s).expect("Failed to read next message");
    //             println!("READ NEW MESSAGE {:?}", reader.fill_buf());
    //         }
    //     }
    //     Err(e) => {
    //         eprintln!("SOMETHING FAILED {:?}", e);
    //         tcp_stream.write(e.as_bytes()).unwrap();
    //     }
    // }
}