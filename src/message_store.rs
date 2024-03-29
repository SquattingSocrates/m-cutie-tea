use std::collections::HashMap;

use crate::coordinator::{PollResponse, RetryLater};
use crate::structure::{
    CompletionMessage, ConfirmationMessage, PublishContext, PublishJob, PublishMessage,
    QueueMessage, Receiver, ReleaseMessage, WriterRef,
};
use crate::topic_tree::TopicTree;
use mqtt_packet_3_5::{ConfirmationPacket, PublishPacket};
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Debug)]
pub struct MessageStore {
    message_queue: Vec<QueueMessage>,
    messages: HashMap<Uuid, PublishContext>,
    waiting_qos1: HashMap<u16, bool>, // channels: HashMap<String, (ProcessRef<ChannelProcess>, usize)>,
    waiting_qos2: HashMap<u16, bool>,
    waiting_release_qos2: HashMap<u16, bool>,
    message_ids: HashMap<u16, Uuid>,
    qos2_message_release: HashMap<Uuid, ReleaseMessage>,
}

impl MessageStore {
    /// create new message store
    pub fn new(
        messages: HashMap<Uuid, PublishContext>,
        message_queue: Vec<QueueMessage>,
        message_ids: HashMap<u16, Uuid>,
    ) -> MessageStore {
        MessageStore {
            messages,
            message_queue,
            waiting_qos1: HashMap::new(),
            waiting_qos2: HashMap::new(),
            waiting_release_qos2: HashMap::new(),
            message_ids,
            qos2_message_release: HashMap::new(),
        }
    }

    pub fn delete_messages_by_uuid(messages: &mut Vec<QueueMessage>, message_uuid: Uuid) {
        messages.retain(|msg| match msg {
            QueueMessage::Publish(p) => p.message_uuid != message_uuid,
            QueueMessage::Confirmation(c) => c.message_uuid != message_uuid,
            QueueMessage::Complete(complete) => complete.message_uuid != message_uuid,
            QueueMessage::Release(release) => release.message_uuid != message_uuid,
        });
    }

    pub fn drop_messages_by_uuid(&mut self, message_uuid: Uuid) {
        MessageStore::delete_messages_by_uuid(&mut self.message_queue, message_uuid)
    }

    pub fn cleanup_message(&mut self, message_uuid: Uuid, message_id: u16, qos: u8) {
        MessageStore::delete_messages_by_uuid(&mut self.message_queue, message_uuid);
        if qos == 1 {
            self.waiting_qos1.remove(&message_id);
        } else if qos == 2 {
            self.waiting_qos2.remove(&message_id);
            self.waiting_release_qos2.remove(&message_id);
        }
        self.message_ids.remove(&message_id);
        self.qos2_message_release.remove(&message_uuid);
        self.messages.remove(&message_uuid);
    }

    pub fn drop_publish_message_id(&mut self, message_id: u16) {
        self.message_queue.retain(|msg| {
            if let QueueMessage::Publish(p) = msg {
                return p.message_id != Some(message_id);
            }
            true
        });
    }

    pub fn drop_publish_message_uuid(&mut self, message_uuid: Uuid) {
        self.message_queue.retain(|msg| {
            if let QueueMessage::Publish(p) = msg {
                return p.message_uuid != message_uuid;
            }
            true
        });
    }

    pub fn get_by_uuid(&self, message_uuid: Uuid) -> Option<&PublishMessage> {
        MessageStore::get_message_by_uuid(&self.message_queue, message_uuid)
    }

    pub fn get_by_uuid_mut(
        &mut self,
        message_uuid: Uuid,
        receivers: &[Receiver],
    ) -> Option<&mut PublishMessage> {
        let msg = MessageStore::get_message_by_uuid_mut(&mut self.message_queue, message_uuid);
        let publish_context = self.messages.get_mut(&message_uuid).unwrap();
        publish_context.receivers = receivers.to_vec();
        // publish_context.queue;
        msg
    }

    pub fn mark_sent(&mut self, message_uuid: Uuid, receivers: &[Receiver]) -> Option<u128> {
        // ) -> Option<(u8, u128, u16, SystemTime, WriterRef)> {
        if let Some(msg) = self.get_by_uuid_mut(message_uuid, receivers) {
            let queue_id = msg.queue_id;
            // write to log and mark message as sent
            // let publish_context = self.messages.get(&message_uuid).unwrap();
            msg.sent = true;
            // publish_context.receivers = receivers.to_vec();
            return Some(
                // publish_context.packet.qos,
                queue_id,
                // message_id,
                // publish_context.started_at,
                // publish_context.sender.clone(),
            );
        }
        None
    }

    pub fn get_message_by_uuid(
        messages: &[QueueMessage],
        message_uuid: Uuid,
    ) -> Option<&PublishMessage> {
        for msg in messages.iter() {
            if let QueueMessage::Publish(p) = msg {
                if p.message_uuid == message_uuid {
                    return Some(p);
                }
            }
        }
        None
    }

    pub fn get_message_by_uuid_mut(
        messages: &mut [QueueMessage],
        message_uuid: Uuid,
    ) -> Option<&mut PublishMessage> {
        for msg in messages.iter_mut() {
            if let QueueMessage::Publish(p) = msg {
                if p.message_uuid == message_uuid {
                    return Some(p);
                }
            }
        }
        None
    }

    pub fn can_process_confirmation(
        waiting_qos1: &HashMap<u16, bool>,
        confirm: &mut ConfirmationMessage,
    ) -> bool {
        return !confirm.in_progress
            && !waiting_qos1.contains_key(&confirm.message_id)
            && confirm.publisher.process.is_some();
    }

    fn get_matching_message_context(
        &self,
        message_uuid: Uuid,
    ) -> Option<(u8, SystemTime, WriterRef, &[Receiver])> {
        // let publish_message = self.get_by_uuid(message_uuid);
        // // handle case where multiple pubacks may be sent to broker
        // // but the message qos flow was already handled and the message
        // // has therefore been deleted
        // if let None = publish_message {
        //     lunatic_log::debug!(
        //         "[Coordinator->Confirmation] Matching publish message for confirmation not found {}",
        //         message_uuid
        //     );
        //     return None;
        // }

        // let publish_message = publish_message.unwrap();
        let publish_context = self.messages.get(&message_uuid).unwrap();
        Some((
            publish_context.packet.qos,
            publish_context.started_at,
            publish_context.sender.clone(),
            &publish_context.receivers,
        ))
    }

    /// this function gets the time of creation, publisher and subscriber (since there is only one)
    /// references
    fn get_qos2_publish_context(
        &self,
        message_uuid: Uuid,
    ) -> Option<(SystemTime, WriterRef, &Receiver)> {
        // let publish_message = self.get_by_uuid(message_uuid);
        // if let None = publish_message {
        //     lunatic_log::debug!(
        //         "[Coordinator->Confirmation] Matching publish message for confirmation not found {}",
        //         message_uuid
        //     );
        //     return None;
        // }

        // let publish_message = publish_message.unwrap();
        let publish_context = self.messages.get(&message_uuid).unwrap();
        Some((
            publish_context.started_at,
            publish_context.sender.clone(),
            publish_context.receivers.get(0).unwrap(),
        ))
    }

    pub fn update_message_publisher_refs(&mut self, writer: &WriterRef) {
        for msg in self.message_queue.iter_mut() {
            if let QueueMessage::Publish(publish) = msg {
                let publish_context = self.messages.get_mut(&publish.message_uuid).unwrap();
                if publish_context.sender.client_id == writer.client_id {
                    publish_context.sender = writer.clone();
                }
            }
        }
    }

    /// helper function to insert confirmation message to
    /// be picked up by workers eventually
    pub fn insert_confirmation_message(
        &mut self,
        message_id: u16,
        message_uuid: Uuid,
        packet: ConfirmationPacket,
    ) -> bool {
        let cmd = packet.cmd;
        if let Some((original_qos, started_at, sender, receivers)) =
            self.get_matching_message_context(message_uuid)
        {
            self.message_queue
                .push(QueueMessage::Confirmation(ConfirmationMessage {
                    message_id,
                    packet,
                    in_progress: false,
                    publisher: sender.clone(),
                    started_at,
                    message_uuid,
                    original_qos,
                    receivers: receivers.to_vec(),
                }));
            lunatic_log::debug!(
                "[MessageStore] Added {:?} for {}. Releasing message {:?}",
                cmd,
                message_id,
                self.message_queue
            );
            return true;
        }
        false
    }

    pub fn delete_qos2_message(
        &mut self,
        message_id: u16,
        message_uuid: Uuid,
        _subscriber: WriterRef,
    ) {
        self.drop_publish_message_id(message_id);
        lunatic_log::debug!(
            "[MessageStore] Calling delete_qos2_message(pubrec) {:?}",
            message_uuid
        );
        self.upsert_release_message(message_id, message_uuid, true);
    }

    fn upsert_release_message(&mut self, message_id: u16, message_uuid: Uuid, is_pubrec: bool) {
        if let Some(msg) = self.qos2_message_release.get_mut(&message_uuid) {
            lunatic_log::debug!(
                "[MessageStore] Found matching release matching, needs updating. is_pubrec: {} | {:?}",
                is_pubrec, msg,
            );
            // if a message already exists we want to set the other one
            // to true, this way both end up being true
            msg.pubrec_received = true;
            msg.pubrel_received = true;
            self.message_queue.push(QueueMessage::Release(msg.clone()));
        } else {
            lunatic_log::debug!(
                "[MessageStore] Creating new ReleaseMessage {:?}",
                ReleaseMessage {
                    message_id,
                    message_uuid,
                    pubrec_received: is_pubrec,
                    pubrel_received: !is_pubrec,
                }
            );
            self.qos2_message_release.insert(
                message_uuid,
                ReleaseMessage {
                    message_id,
                    message_uuid,
                    pubrec_received: is_pubrec,
                    pubrel_received: !is_pubrec,
                },
            );
        }
    }

    /// mark message as to be released
    pub fn mark_to_be_released(&mut self, message_id: u16, message_uuid: Uuid) {
        lunatic_log::debug!(
            "[MessageStore] Calling mark_to_be_released {}",
            message_uuid
        );
        self.upsert_release_message(message_id, message_uuid, false);
    }

    /// check if message is good to be released
    // pub fn is_released(&self, message_id: u16) -> bool {
    //     if let Some(true) = self.qos2_message_released.get(&message_id) {
    //         return true;
    //     }
    //     false
    // }

    /// helper method to create new completion message that will be picked up
    /// by a worker eventually
    pub fn insert_completion_message(&mut self, message_id: u16, message_uuid: Uuid) -> bool {
        if let Some((started_at, sender, subscriber)) = self.get_qos2_publish_context(message_uuid)
        {
            self.message_queue
                .push(QueueMessage::Complete(CompletionMessage {
                    message_id,
                    in_progress: false,
                    publisher: sender.clone(),
                    receiver: subscriber.clone(),
                    started_at,
                    message_uuid,
                }));
            return true;
        }
        false
    }

    /// helper method to create new publish message that will be picked up
    /// by a worker eventually
    pub fn insert_publish_message(
        &mut self,
        message_uuid: Uuid,
        packet: PublishPacket,
        queue_id: u128,
        sender: WriterRef,
        started_at: SystemTime,
    ) {
        self.message_queue
            .push(QueueMessage::Publish(PublishMessage {
                message_uuid,
                message_id: packet.message_id,
                queue_id,
                in_progress: false,
                sent: false,
            }));

        self.messages.insert(
            message_uuid,
            PublishContext {
                packet,
                sender,
                started_at,
                receivers: vec![],
            },
        );
    }

    /// create a new internal message id and map it with the given message_id
    /// from the mqtt packet if any given
    pub fn register_message_id(&mut self, message_id: Option<u16>) -> Uuid {
        let uuid = Uuid::new_v4();
        if let Some(id) = message_id {
            self.message_ids.insert(id, uuid);
        }
        uuid
    }

    /// lookup uuid from a given message_id
    pub fn lookup_uuid(&self, message_id: u16) -> Uuid {
        *self.message_ids.get(&message_id).unwrap()
    }

    /// get queue_id for a given message_uuid
    pub fn get_queue_id(&self, uuid: Uuid) -> Option<u128> {
        if let Some(publish) = self.get_by_uuid(uuid) {
            return Some(publish.queue_id);
        }
        None
    }

    pub fn retry_message_later(
        &mut self,
        RetryLater(uuid, inactive_subs): RetryLater,
        topic_tree: &mut TopicTree,
    ) -> bool {
        for msg in self.message_queue.iter_mut() {
            match msg {
                QueueMessage::Publish(publish) => {
                    if publish.message_uuid == uuid {
                        publish.in_progress = false;
                        let queue = topic_tree.get_by_id(publish.queue_id);
                        queue.drop_inactive_subs(inactive_subs);
                        return true;
                    }
                }
                QueueMessage::Confirmation(confirm) => {
                    lunatic_log::debug!(
                        "[Coordinator->Poll] Checking confirmation message {:?} | {:?}",
                        confirm,
                        self.waiting_qos1.contains_key(&confirm.message_id)
                    );
                    // if !confirm.in_progress && !self.waiting_qos1.contains_key(&confirm.message_id)
                    // {
                    //     confirm.in_progress = true;
                    //     // mark qos1 message as waiting to prevent sending puback multiple times
                    //     self.waiting_qos1.insert(confirm.message_id, true);
                    //     return PollResponse::Confirmation(confirm.clone());
                    // }
                }
                // TODO: handle state of messages if complete and release have been encountered
                QueueMessage::Complete(_) => {}
                QueueMessage::Release(_) => {}
            }
        }

        true
    }

    /// main logic of the message "queue" which returns the next available message
    pub fn poll(&mut self, topic_tree: &mut TopicTree) -> PollResponse {
        for msg in self.message_queue.iter_mut() {
            match msg {
                QueueMessage::Publish(publish) => {
                    if !publish.in_progress {
                        let publish_context = self.messages.get(&publish.message_uuid).unwrap();
                        if publish_context.sender.process.is_none() {
                            continue;
                        }
                        let queue = topic_tree.get_by_id(publish.queue_id);
                        if queue.subscribers.is_empty() {
                            continue;
                        }
                        publish.in_progress = true;
                        if let None = publish_context.sender.process {
                            return PollResponse::None;
                        }
                        return PollResponse::Publish(
                            PublishJob {
                                message: publish.clone(),
                                queue: queue.clone(),
                            },
                            publish_context.clone(),
                        );
                        // return PollResponse::Publish(PublishJob {
                        //     message: publish.clone(),
                        //     queue: queue.clone(),
                        // });
                    }
                }
                QueueMessage::Confirmation(confirm) => {
                    lunatic_log::debug!(
                        "[Coordinator->Poll] Checking confirmation message {:?} | {:?}",
                        confirm,
                        self.waiting_qos1.contains_key(&confirm.message_id)
                    );
                    if MessageStore::can_process_confirmation(&self.waiting_qos1, confirm) {
                        confirm.in_progress = true;
                        // mark qos1 message as waiting to prevent sending puback multiple times
                        self.waiting_qos1.insert(confirm.message_id, true);
                        let publish_context = self.messages.get(&confirm.message_uuid).unwrap();
                        if let None = publish_context.sender.process {
                            return PollResponse::None;
                        }
                        return PollResponse::Confirmation(
                            confirm.clone(),
                            publish_context.clone(),
                        );
                        // return PollResponse::Confirmation(confirm.clone());
                    }
                }
                QueueMessage::Complete(complete) => {
                    lunatic_log::debug!(
                        "[Coordinator->Poll] Checking Complete message {:?} | {:?}",
                        complete.message_id,
                        self.waiting_qos2.contains_key(&complete.message_id)
                    );
                    if !self.waiting_qos2.contains_key(&complete.message_id) {
                        // mark qos1 message as waiting to prevent sending puback multiple times
                        self.waiting_qos2.insert(complete.message_id, true);
                        let publish_context = self.messages.get(&complete.message_uuid).unwrap();
                        if let None = publish_context.sender.process {
                            return PollResponse::None;
                        }
                        return PollResponse::Complete(complete.clone(), publish_context.clone());
                    }
                }
                QueueMessage::Release(release) => {
                    lunatic_log::debug!(
                        "[Coordinator->Poll] Checking Release message {:?} | {:?}",
                        release,
                        self.waiting_release_qos2.contains_key(&release.message_id)
                    );
                    if release.pubrel_received
                        && release.pubrec_received
                        && !self.waiting_release_qos2.contains_key(&release.message_id)
                    {
                        // mark qos1 message as waiting to prevent sending puback multiple times
                        self.waiting_release_qos2.insert(release.message_id, true);
                        let publish_context = self.messages.get(&release.message_uuid).unwrap();
                        if let None = publish_context.sender.process {
                            return PollResponse::None;
                        }
                        return PollResponse::Release(release.clone(), publish_context.clone());
                    }
                }
            }
        }
        PollResponse::None
    }
}
