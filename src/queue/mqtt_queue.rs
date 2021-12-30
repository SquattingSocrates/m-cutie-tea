use lunatic::process;
use std::io::Write;

use super::structure::{QueueCtx, QueueRequest};
use crate::mqtt::message::{MqttMessage, PublishSource, SubscriptionRequest};

pub fn handle_mqtt_message(
    QueueCtx {
        mailbox,
        mut stream,
        broker,
    }: QueueCtx,
) {
    loop {
        match mailbox.receive() {
            Ok(mut data) => {
                println!("Received mqtt message {:?}", data);
                match &data {
                    // Since a Publish can come from either client or server we need to distinguish between the two
                    MqttMessage::Publish(_, variable, payload, PublishSource::Client) => {
                        match broker.request(QueueRequest::Publish(
                            variable.topic_name.to_string(),
                            payload.to_string(),
                            variable.message_id,
                        )) {
                            Ok(_) => {}
                            Err(e) => eprintln!(
                                "Failed to publish to topic {}. Error: {:?}",
                                variable.topic_name.to_string(),
                                e
                            ),
                        }
                    }
                    MqttMessage::Publish(_, variable, payload, PublishSource::Server) => {
                        println!("MqttMessage::Publish to client {:?} {}", variable, payload);
                        match stream.write(&MqttMessage::to_response(&mut data)) {
                            Ok(_) => println!("Wrote publish to client"),
                            Err(e) => eprintln!("Failed to write to stream {:?}", e),
                        };
                        continue;
                    }
                    MqttMessage::Subscribe(_, _, payload) => {
                        // let
                        let _res = broker.request(QueueRequest::Subscribe(
                            process::this(&mailbox),
                            payload
                                .iter()
                                .map(|x| SubscriptionRequest {
                                    topic: x.topic.to_string(),
                                    qos: x.qos,
                                })
                                .collect(),
                        ));
                    }
                    _ => println!("Received message with type {:?}", data),
                }
                match stream.write(&MqttMessage::to_response(&mut data)) {
                    Ok(_) => println!("Wrote back to stream"),
                    Err(e) => eprintln!("Failed to write to stream {:?}", e),
                }
            }
            Err(e) => println!("Error while receiving message {:?}", e),
        }
    }
}
