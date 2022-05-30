use std::time::Duration;

use crate::coordinator::{
    Cleanup, CoordinatorProcess, Poll, PollResponse, Release, RetryLater, Sent,
};
use crate::metrics::{self, MetricsProcess};
use crate::structure::{PublishContext, PublishJob, Receiver, WriterRef};
use lunatic::{
    process::{Message, ProcessRef, Request},
    sleep, Mailbox, Process,
};
use mqtt_packet_3_5::{ConfirmationPacket, MqttPacket, PacketType, PubcompPubrelCode};

fn process_publish(
    coordinator: ProcessRef<CoordinatorProcess>,
    metrics_process: ProcessRef<MetricsProcess>,
    publish: PublishJob,
    ctx: PublishContext,
) {
    let message_uuid = publish.message.message_uuid;
    let packet = ctx.packet;
    let message_qos = packet.qos;
    println!(
        "[Worker->Publish] Received Publish {}, {:?}",
        message_uuid, publish.queue
    );
    let mut message_sent = false;
    let mut inactive_subs: Vec<WriterRef> = vec![];
    let mut sent_to: Vec<Receiver> = vec![];
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
        } else {
            sent_to.push(Receiver {
                writer: sub.clone(),
                received_qos: packet.qos,
            });
            // short-circuit for qos 2 because it needs to be sent only once
            if message_qos == 2 {
                break;
            }
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
    println!("[Worker-Publish] Successfully sent message");
    if let (Ok(duration), 0) = (ctx.started_at.elapsed(), packet.qos) {
        metrics_process.send(metrics::DeliveryTime(0, duration.as_millis() as f64));
    }
    // A QoS > 0 message cannot be released just because it was sent
    if packet.qos > 0 {
        let was_sent = coordinator.request(Sent(
            packet.message_id.unwrap(),
            message_uuid,
            packet.qos,
            inactive_subs.clone(),
            sent_to,
        ));
        println!(
            "[Worker->Publish] Marked high QoS message as sent {:?} | Success: {}",
            packet.message_id, was_sent
        );
        return;
    }
    if coordinator.request(Release(message_uuid, 0, None, inactive_subs, sent_to)) {
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
                PollResponse::Publish(publish, ctx) => {
                    process_publish(coordinator.clone(), metrics_process.clone(), publish, ctx);
                }
                PollResponse::Confirmation(confirm, ctx) => {
                    println!("[Worker->Confirmation] received confirmation for message {} to process {:?}", confirm.message_id, confirm.packet);
                    // this is safe because the coordinator will never allow a None confirmation to
                    // be processed by a worker
                    let receiver = confirm.receivers.clone();
                    // wrap the packet correctly
                    let (qos, wrapped_packet) = if confirm.packet.cmd == PacketType::Puback {
                        (1, MqttPacket::Puback(confirm.packet))
                    } else {
                        (2, MqttPacket::Pubrec(confirm.packet))
                    };
                    if confirm.publisher.process.unwrap().request(wrapped_packet) {
                        if let Ok(duration) = confirm.started_at.elapsed() {
                            metrics_process
                                .send(metrics::DeliveryTime(qos, duration.as_millis() as f64));
                        }
                        // release if subscriber received a qos 1 message
                        // which could happen either if it's a qos 1 publish
                        // or if a qos 2 message was downgraded
                        coordinator.request(Release(
                            confirm.message_uuid,
                            qos,
                            Some(confirm.message_id),
                            vec![],
                            confirm.receivers,
                        ));
                    }
                }
                PollResponse::Complete(complete, ctx) => {
                    println!(
                        "[Worker->Complete] received completion for message {} to process {:?}",
                        complete.message_id, complete.publisher
                    );
                    // this is safe because the coordinator will never allow a None process to be processed
                    if complete
                        .publisher
                        .process
                        .unwrap()
                        .request(MqttPacket::Pubcomp(ConfirmationPacket {
                            cmd: PacketType::Pubcomp,
                            message_id: complete.message_id,
                            properties: None,
                            puback_reason_code: None,
                            pubcomp_reason_code: Some(PubcompPubrelCode::Success),
                        }))
                    {
                        if let Ok(duration) = complete.started_at.elapsed() {
                            metrics_process
                                .send(metrics::DeliveryTime(2, duration.as_millis() as f64));
                        }
                        // send qos 2 because a pubcomp has now been sent to the subscriber
                        coordinator.request(Release(
                            complete.message_uuid,
                            2,
                            Some(complete.message_id),
                            vec![],
                            vec![complete.receiver.clone()],
                        ));
                    }
                }
                PollResponse::Release(release, ctx) => {
                    println!(
                        "[Worker->Complete] received release(pubcomp) for message {} to process {:?}",
                        release.message_id, ctx.sender
                    );
                    // send pubrel to receiver
                    for rec in ctx.receivers.iter() {
                        if let Some(w) = &rec.writer.process {
                            if w.request(MqttPacket::Pubrel(ConfirmationPacket {
                                cmd: PacketType::Pubrel,
                                message_id: release.message_id,
                                properties: None,
                                puback_reason_code: None,
                                pubcomp_reason_code: Some(PubcompPubrelCode::Success),
                            })) {
                                break;
                            }
                        }
                    }
                    // this is safe because the coordinator will never allow a None process to be processed
                    if ctx.sender.process.unwrap().request(MqttPacket::Pubcomp(
                        ConfirmationPacket {
                            cmd: PacketType::Pubcomp,
                            message_id: release.message_id,
                            properties: None,
                            puback_reason_code: None,
                            pubcomp_reason_code: Some(PubcompPubrelCode::Success),
                        },
                    )) {
                        if let Ok(duration) = ctx.started_at.elapsed() {
                            metrics_process
                                .send(metrics::DeliveryTime(2, duration.as_millis() as f64));
                        }
                        // send qos 2 because a pubcomp has now been sent to the subscriber
                        coordinator.request(Cleanup(release.message_uuid, release.message_id, 2));
                    }
                }
            }
        }
    });
}
