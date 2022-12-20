use std::time::Duration;

use crate::client::WriterProcessHandler;
use crate::coordinator::{
    Cleanup, CoordinatorProcess, CoordinatorProcessHandler, PollResponse, Release, RetryLater, Sent,
};
use crate::metrics::{MetricsProcess, MetricsProcessHandler};
use crate::structure::{PublishContext, PublishJob, Receiver, WriterRef};
use lunatic::{process::ProcessRef, sleep, Mailbox, Process};
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
    lunatic_log::debug!(
        "[Worker->Publish] Received Publish {}, {:?}",
        message_uuid,
        publish.queue
    );
    let mut message_sent = false;
    let mut inactive_subs: Vec<WriterRef> = vec![];
    let mut sent_to: Vec<Receiver> = vec![];
    for sub in publish.queue.subscribers.iter() {
        lunatic_log::debug!(
            "[Worker->Publish] Sending Publish to client {}, {:?}",
            message_uuid,
            sub
        );
        // safe to unwrap because subscribers are required to pass a process with them
        let result = sub
            .process
            .as_ref()
            .unwrap()
            .write_packet(MqttPacket::Publish(packet.clone()));
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
        lunatic_log::error!("Failed to send message {:?} | {:?}", packet, publish.queue);
        // unlock message in coordinator because apparently there are not active
        // subscribers and a message with qos > 0 is required to be delivered
        coordinator.retry_message_later(RetryLater(message_uuid, inactive_subs));
        sleep(Duration::from_millis(1000));
        return;
    }
    lunatic_log::debug!("[Worker-Publish] Successfully sent message");
    if let (Ok(duration), 0) = (ctx.started_at.elapsed(), packet.qos) {
        metrics_process.track_delivery_time(0, duration.as_millis() as f64);
    }
    // A QoS > 0 message cannot be released just because it was sent
    if packet.qos > 0 {
        let was_sent = coordinator.mark_sent(Sent(
            packet.message_id.unwrap(),
            message_uuid,
            packet.qos,
            inactive_subs.clone(),
            sent_to,
        ));
        lunatic_log::debug!(
            "[Worker->Publish] Marked high QoS message as sent {:?} | Success: {}",
            packet.message_id,
            was_sent
        );
        return;
    }
    if coordinator.release_message(Release(message_uuid, 0, None, inactive_subs, sent_to)) {
        lunatic_log::debug!(
            "[Worker->Publish] Successfully released message {}",
            message_uuid
        );
    } else {
        lunatic_log::error!(
            "[Worker->Publish] Failed to release message {}",
            message_uuid
        );
    }
}

pub fn worker_process() {
    Process::spawn_link((), |_, _: Mailbox<()>| {
        // Look up the coordinator or fail if it doesn't exist.
        let coordinator = ProcessRef::<CoordinatorProcess>::lookup("coordinator").unwrap();
        let metrics_process = MetricsProcess::get_process();
        loop {
            lunatic_log::debug!("Polling message from coordinator");
            match coordinator.poll_job() {
                PollResponse::None => {
                    sleep(Duration::from_millis(1000));
                }
                PollResponse::Publish(publish, ctx) => {
                    process_publish(coordinator.clone(), metrics_process.clone(), publish, ctx);
                }
                PollResponse::Confirmation(confirm, _ctx) => {
                    lunatic_log::debug!("[Worker->Confirmation] received confirmation for message {} to process {:?}", confirm.message_id, confirm.packet);
                    // this is safe because the coordinator will never allow a None confirmation to
                    // be processed by a worker
                    // wrap the packet correctly
                    let (qos, wrapped_packet) = if confirm.packet.cmd == PacketType::Puback {
                        (1, MqttPacket::Puback(confirm.packet))
                    } else {
                        (2, MqttPacket::Pubrec(confirm.packet))
                    };
                    if confirm
                        .publisher
                        .process
                        .unwrap()
                        .write_packet(wrapped_packet)
                    {
                        if let Ok(duration) = confirm.started_at.elapsed() {
                            metrics_process.track_delivery_time(qos, duration.as_millis() as f64);
                        }
                        // release if subscriber received a qos 1 message
                        // which could happen either if it's a qos 1 publish
                        // or if a qos 2 message was downgraded
                        coordinator.release_message(Release(
                            confirm.message_uuid,
                            qos,
                            Some(confirm.message_id),
                            vec![],
                            confirm.receivers,
                        ));
                    }
                }
                PollResponse::Complete(complete, _ctx) => {
                    lunatic_log::debug!(
                        "[Worker->Complete] received completion for message {} to process {:?}",
                        complete.message_id,
                        complete.publisher
                    );
                    // this is safe because the coordinator will never allow a None process to be processed
                    if complete
                        .publisher
                        .process
                        .unwrap()
                        .write_packet(MqttPacket::Pubcomp(ConfirmationPacket {
                            cmd: PacketType::Pubcomp,
                            message_id: complete.message_id,
                            properties: None,
                            puback_reason_code: None,
                            pubcomp_reason_code: Some(PubcompPubrelCode::Success),
                        }))
                    {
                        if let Ok(duration) = complete.started_at.elapsed() {
                            metrics_process.track_delivery_time(2, duration.as_millis() as f64);
                        }
                        // send qos 2 because a pubcomp has now been sent to the subscriber
                        coordinator.release_message(Release(
                            complete.message_uuid,
                            2,
                            Some(complete.message_id),
                            vec![],
                            vec![complete.receiver.clone()],
                        ));
                    }
                }
                PollResponse::Release(release, ctx) => {
                    lunatic_log::debug!(
                        "[Worker->Complete] received release(pubcomp) for message {} to process {:?}",
                        release.message_id, ctx.sender
                    );
                    // send pubrel to receiver
                    for rec in ctx.receivers.iter() {
                        if let Some(w) = &rec.writer.process {
                            if w.write_packet(MqttPacket::Pubrel(ConfirmationPacket {
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
                    if ctx
                        .sender
                        .process
                        .unwrap()
                        .write_packet(MqttPacket::Pubcomp(ConfirmationPacket {
                            cmd: PacketType::Pubcomp,
                            message_id: release.message_id,
                            properties: None,
                            puback_reason_code: None,
                            pubcomp_reason_code: Some(PubcompPubrelCode::Success),
                        }))
                    {
                        if let Ok(duration) = ctx.started_at.elapsed() {
                            metrics_process.track_delivery_time(2, duration.as_millis() as f64);
                        }
                        // send qos 2 because a pubcomp has now been sent to the subscriber
                        coordinator.cleanup(Cleanup(release.message_uuid, release.message_id, 2));
                    }
                }
            }
        }
    });
}
