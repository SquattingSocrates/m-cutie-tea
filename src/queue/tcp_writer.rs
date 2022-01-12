use crate::{
    mqtt::{flags::ControlPacketType, message::MqttMessage},
    structure::{ConnectionMessage, QueueResponse, WriterMessage},
};
use lunatic::{net, Mailbox};
use std::io::Write;

fn serialize_connection_message<'a>(msg: &ConnectionMessage) -> (bool, Vec<u8>) {
    match &msg {
        // handle disconnect by disabling writing to tcp stream
        ConnectionMessage::Disconnect => (false, vec![]),
        ConnectionMessage::Connect(code) => (
            true,
            vec![ControlPacketType::CONNACK.bits() << 4, 0x2, 0x0, *code],
        ),
        // TODO: ping should have data about keep-alive, not always true here
        ConnectionMessage::Ping => (true, vec![(ControlPacketType::PINGRESP.bits() << 4), 0x0]),
        _ => (false, vec![]),
    }
}

// This process has a one mailbox that it's listening to
// but there can be multiple types of messages
pub fn write_mqtt(mut stream: net::TcpStream, mailbox: Mailbox<WriterMessage>) {
    let mut is_receiving = true;
    loop {
        println!("WRITER IS RECEIVING {}", is_receiving);
        match mailbox.receive() {
            Ok(data) => match &data {
                WriterMessage::Connection(msg, maybe_stream) => {
                    if let ConnectionMessage::Destroy = msg {
                        println!("Destroying old tcp_writer process");
                        return;
                    }
                    println!("Received WriterMessage::Connection {:?}", msg);
                    let stuff = maybe_stream.is_some();
                    if stuff {
                        stream = maybe_stream.as_ref().unwrap().clone();
                    }
                    let (receiving, res) = serialize_connection_message(msg);
                    is_receiving = receiving;
                    match stream.write_all(&res) {
                        Ok(_) => println!("Wrote connection response to client"),
                        Err(e) => {
                            eprintln!("Failed to write connection response to stream {:?}", e)
                        }
                    };
                }
                WriterMessage::Queue(s) => match &s {
                    QueueResponse::Publish(message_id, _, blob, _) => {
                        // TODO: buffer messages with qos > 0
                        if !is_receiving {
                            continue;
                        }
                        // let flag = ControlPacketType::PUBLISH;
                        let res = MqttMessage::bytes_with_message_id(
                            ControlPacketType::PUBLISH.bits(),
                            *message_id,
                            Some(String::into_bytes(blob.to_string())),
                        );
                        match stream.write_all(&res) {
                            Ok(_) => println!("Wrote publish to client"),
                            Err(e) => {
                                eprintln!("Failed to write publish to stream {:?}", e)
                            }
                        };
                    }
                    QueueResponse::Subscribe(message_id, subs) => {
                        if !is_receiving {
                            continue;
                        }
                        let res = MqttMessage::bytes_with_message_id(
                            ControlPacketType::SUBACK.bits(),
                            *message_id,
                            Some(subs.iter().map(|x| x.qos).collect()),
                        );
                        match stream.write_all(&res) {
                            Ok(_) => println!("Wrote suback to client"),
                            Err(e) => {
                                eprintln!("Failed to write suback to stream {:?}", e)
                            }
                        };
                    }
                    QueueResponse::Unsubscribe(message_id, _) => {
                        if !is_receiving {
                            continue;
                        }
                        let message_id = MqttMessage::encode_multibyte_num(*message_id);
                        match stream.write_all(&[
                            (ControlPacketType::UNSUBACK.bits() << 4) + 0x2,
                            0x2,
                            message_id[0],
                            message_id[1],
                        ]) {
                            Ok(_) => println!("Wrote suback to client"),
                            Err(e) => {
                                eprintln!("Failed to write suback to stream {:?}", e)
                            }
                        };
                    }
                    QueueResponse::Puback(message_id) => {
                        // TODO: maybe handle publisher losing connection?
                        if !is_receiving {
                            continue;
                        }
                        let res = MqttMessage::bytes_with_message_id(
                            ControlPacketType::PUBACK.bits(),
                            *message_id,
                            None,
                        );
                        match stream.write_all(&res) {
                            Ok(_) => println!("Wrote publish to client"),
                            Err(e) => {
                                eprintln!("Failed to write publish to stream {:?}", e)
                            }
                        };
                    }
                },
            },
            Err(e) => {
                eprintln!("Failed to receive mailbox {:?}", e);
            }
        }
    }
}
