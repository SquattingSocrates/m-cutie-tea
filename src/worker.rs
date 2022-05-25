use std::time::Duration;

use crate::coordinator::{CoordinatorProcess, Poll, PollResponse, Release, RetryLater, Sent};
use crate::metrics::{self, MetricsProcess};
use crate::structure::{PublishJob, WriterRef};
use lunatic::{
    process::{Message, ProcessRef, Request},
    sleep, Mailbox, Process,
};
use mqtt_packet_3_5::MqttPacket;

fn process_publish(
    coordinator: ProcessRef<CoordinatorProcess>,
    metrics_process: ProcessRef<MetricsProcess>,
    publish: PublishJob,
) {
    let message_uuid = publish.message.message_uuid;
    let packet = publish.message.packet;
    println!(
        "[Worker->Publish] Received Publish {}, {:?}",
        message_uuid, publish.queue
    );
    let mut message_sent = false;
    let mut inactive_subs: Vec<WriterRef> = vec![];
    for sub in publish.queue.subscribers.iter() {
        println!(
            "[Worker->Publish] Sending Publish to client {}, {:?}",
            message_uuid, sub
        );
        // safe to unwrap because subscribers are required to pass a process with them
        let result = sub
            .process
            .as_ref()
            .unwrap()
            .request(MqttPacket::Publish(packet.clone()));
        message_sent = message_sent || result;
        // take note of all inactive subscribers and discard them
        if !result {
            inactive_subs.push(sub.clone());
        }
    }
    if !message_sent && packet.qos > 0 {
        eprintln!("Failed to send message {:?} | {:?}", packet, publish.queue);
        // unlock message in coordinator because apparently there are not active
        // subscribers and a message with qos > 0 is required to be delivered
        coordinator.request(RetryLater::Publish(message_uuid, inactive_subs));
        sleep(Duration::from_millis(1000));
        return;
    }
    println!("Successfully sent message");
    if let (Ok(duration), 0) = (publish.message.started_at.elapsed(), packet.qos) {
        metrics_process.send(metrics::DeliveryTime(0, duration.as_millis() as f64));
    }
    // A QoS > 0 message cannot be released just because it was sent
    if packet.qos > 0 {
        let was_sent = coordinator.request(Sent(message_uuid, inactive_subs.clone()));
        println!(
            "[Worker->Publish] Marked high QoS message as sent {:?} | Success: {}",
            packet.message_id, was_sent
        );
        return;
    }
    if coordinator.request(Release(message_uuid, 0, None, inactive_subs)) {
        println!(
            "[Worker->Publish] Successfully released message {}",
            message_uuid
        );
    } else {
        eprintln!(
            "[Worker->Publish] Failed to release message {}",
            message_uuid
        );
    }
}

pub fn worker_process() {
    Process::spawn_link((), |_, _: Mailbox<()>| {
        // Look up the coordinator or fail if it doesn't exist.
        let coordinator = ProcessRef::<CoordinatorProcess>::lookup("coordinator").unwrap();
        let metrics_process = ProcessRef::<MetricsProcess>::lookup("metrics").unwrap();
        loop {
            println!("Polling message from coordinator");
            match coordinator.request(Poll) {
                PollResponse::None => {
                    // println!("Worker got none");
                    sleep(Duration::from_millis(1000));
                }
                PollResponse::Publish(publish) => {
                    process_publish(coordinator.clone(), metrics_process.clone(), publish);
                }
                PollResponse::Confirmation(confirm) => {
                    println!("[Worker->Confirmation] received confirmation for message {} to process {:?}", confirm.message_id, confirm.packet);
                    // this is safe because the coordinator will never allow a None confirmation to
                    // be processed by a worker
                    if confirm
                        .send_to
                        .process
                        .unwrap()
                        .request(MqttPacket::Puback(confirm.packet))
                    {
                        if let Ok(duration) = confirm.started_at.elapsed() {
                            metrics_process
                                .send(metrics::DeliveryTime(0, duration.as_millis() as f64));
                        }
                        // send qos 1 because it's a puback
                        coordinator.request(Release(
                            confirm.message_uuid,
                            1,
                            Some(confirm.message_id),
                            vec![],
                        ));
                    }
                }
            }
        }
    });
}
