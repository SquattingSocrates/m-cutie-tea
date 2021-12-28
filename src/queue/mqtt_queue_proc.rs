use lunatic::{
    lookup, net,
    process::{self},
    Mailbox, Request,
};

use super::{
    mqtt_queue,
    structure::{QueueCtx, QueueRequest, QueueResponse},
};
use crate::mqtt::connection;
use crate::mqtt::message::MqttMessage;

pub fn handle_queue(stream: net::TcpStream, mailbox: Mailbox<MqttMessage>) {
    let broker = lookup::<Request<QueueRequest, QueueResponse>>("broker", "1.0.0");
    let broker = broker.unwrap().unwrap();

    // spawn a process that will send parsed mqtt messages
    // to this process
    let this = process::this(&mailbox);
    let (_, mailbox) =
        process::spawn_link_unwrap_with(mailbox, (this, stream.clone()), connection::handle_tcp)
            .unwrap();

    mqtt_queue::handle_mqtt_message(QueueCtx {
        mailbox,
        stream: stream.clone(),
        broker,
    });
}
