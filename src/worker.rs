use std::collections::{HashMap, HashSet};
use std::thread::sleep_ms;

use crate::coordinator::{CoordinatorProcess, Poll, PollResponse, Release};
use lunatic::{
    process::{AbstractProcess, Message, ProcessMessage, ProcessRef, Request},
    Mailbox, Process,
};
use mqtt_packet_3_5::{MqttPacket, PublishPacket, SubscribePacket};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
                PollResponse::Publish(message_id, packet, queue) => {
                    println!(
                        "[Worker->Publish] Received Publish {}, {:?}",
                        message_id, queue
                    );
                    let mut message_sent = false;
                    for sub in queue.subscribers.iter() {
                        println!(
                            "[Worker->Publish] Sending Publish to client {}, {:?}",
                            message_id, sub
                        );
                        let result = sub.request(MqttPacket::Publish(packet.clone()));
                        message_sent = message_sent || result;
                    }
                    if !message_sent {
                        eprintln!("Failed to send message {:?} | {:?}", packet, queue);
                        sleep_ms(1000);
                        continue;
                    }
                    println!("Successfully sent message");
                    if coordinator.request(Release(message_id)) {
                        println!("Successfully released message {}", message_id);
                    } else {
                        eprintln!("Failed to release message {}", message_id);
                    }
                }
            }
        }
    });
}
