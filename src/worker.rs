use std::collections::{HashMap, HashSet};
use std::thread::sleep_ms;

use crate::coordinator::{CoordinatorProcess, Poll, PollResponse, Release, Sent};
use crate::structure::{ConfirmationMessage, PublishJob, QueueMessage};
use lunatic::{
    process::{AbstractProcess, Message, ProcessMessage, ProcessRef, Request},
    Mailbox, Process,
};
use mqtt_packet_3_5::{MqttPacket, PublishPacket, SubscribePacket};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

fn process_publish(coordinator: ProcessRef<CoordinatorProcess>, publish: PublishJob) {
    let message_id = publish.message.message_id;
    let packet = publish.message.packet;
    println!(
        "[Worker->Publish] Received Publish {}, {:?}",
        message_id, publish.queue
    );
    let mut message_sent = false;
    for sub in publish.queue.subscribers.iter() {
        println!(
            "[Worker->Publish] Sending Publish to client {}, {:?}",
            message_id, sub
        );
        let result = sub.request(MqttPacket::Publish(packet.clone()));
        message_sent = message_sent || result;
    }
    if !message_sent {
        eprintln!("Failed to send message {:?} | {:?}", packet, publish.queue);
        sleep_ms(1000);
        return;
    }
    println!("Successfully sent message");
    // A QoS > 0 message cannot be released just because it was sent
    if packet.qos > 0 && coordinator.request(Sent(packet.message_id.unwrap(), packet.qos)) {
        println!(
            "[Worker->Publish] Marked high QoS message as sent {:?}",
            packet.message_id
        );
    } else if coordinator.request(Release(message_id)) {
        println!(
            "[Worker->Publish] Successfully released message {}",
            message_id
        );
    } else {
        eprintln!("[Worker->Publish] Failed to release message {}", message_id);
    }
}

pub fn worker_process() {
    Process::spawn_link((), |_, _: Mailbox<()>| {
        // Look up the coordinator or fail if it doesn't exist.
        let coordinator = ProcessRef::<CoordinatorProcess>::lookup("coordinator").unwrap();
        loop {
            println!("Polling message from coordinator");
            match coordinator.request(Poll) {
                PollResponse::None => {
                    // println!("Worker got none");
                    sleep_ms(1000);
                }
                PollResponse::Publish(publish) => {
                    process_publish(coordinator.clone(), publish);
                }
                PollResponse::Confirmation(confirm) => {
                    println!("[Worker->Confirmation] received confirmation for message {} to process {:?}", confirm.message_id, confirm.packet);
                    confirm.send_to.request(MqttPacket::Puback(confirm.packet));
                }
            }
        }
    });
}
