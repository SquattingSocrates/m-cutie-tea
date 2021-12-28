use lunatic::{net, process::Process, Mailbox};
use std::io::prelude::*;
use std::io::{BufReader, Write};

use crate::mqtt::{message::MqttMessage, parser::ByteParser};

pub fn handle_tcp((client, mut stream): (Process<MqttMessage>, net::TcpStream), _: Mailbox<()>) {
    loop {
        let mut reader = BufReader::new(&mut stream);
        match reader.fill_buf() {
            Ok(v) => {
                let vc = v.to_vec();
                if vc.len() == 0 {
                    continue;
                }
                println!("RAW {:?}", vc);
                let mut parser = ByteParser::new(vc);
                match parser.parse_mqtt() {
                    Ok(msg) => {
                        client.send(msg);
                    }
                    Err(e) => {
                        eprintln!("Failed to parse mqtt message: {:?}", e);
                        stream.write(e.as_bytes()).unwrap();
                    }
                }
            }
            Err(e) => {
                println!("Error while filling buffer {:?}", e);
                panic!("Client closed connection {:?}", e);
            }
        }
    }
}
