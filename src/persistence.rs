use crate::structure::WriterRef;
use base64;
use mqtt_packet_3_5::{ConfirmationPacket, PublishPacket};
use ron;
use serde::{Deserialize, Serialize};
use std::fs::{DirBuilder, File};
use std::io::{BufRead, BufReader, Result as IoResult, Write};
use std::path::{Path, PathBuf};
use std::str;
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Debug)]
pub struct FileLog {
    // cwd: str,
    // file_name: str,
    full_path: PathBuf,
    file: File,
}

const NEWLINE: &[u8] = &[b'\n'];

/// Every Line is a new "state change" entry
/// Each line starts with one of the following keywords
/// that indicate the type of entry
const PUBLISH: u8 = 1;
const ACCEPTED: u8 = 2;
const SENT: u8 = 3;
const DELETED: u8 = 4;
const COMPLETE: u8 = 5;

// structures that will be stored per entry
/// PublishEntry is the structure used to write a log entry
/// for each new published message with QoS 1 or 2
/// The uuid will be set for the publish entry and then
/// used in all other entries to correlate with the correct message
#[derive(Debug, Serialize, Deserialize)]
pub struct PublishEntry {
    pub uuid: Uuid,
    pub received_at: SystemTime,
    pub packet: PublishPacket,
    pub client_id: String,
    pub session: SessionData,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionData {
    pub uuid: Uuid,
    pub is_persistent: bool,
}

/// AcceptedEntry is used to tell that a subscriber has received the
/// message and has sent a mqtt confirmation message
#[derive(Debug, Serialize, Deserialize)]
pub struct AcceptedEntry {
    pub uuid: Uuid,
    pub accepted_at: SystemTime,
    pub packet: ConfirmationPacket,
}

/// SentEntry tells us that the message has been published to a subscriber
#[derive(Debug, Serialize, Deserialize)]
pub struct SentEntry {
    pub uuid: Uuid,
    pub sent_at: SystemTime,
}

/// DeletedEntry tells us that an entry can be deleted from internal memory
/// because we're far enough in the QoS flow for that particular message
#[derive(Debug, Serialize, Deserialize)]
pub struct DeletedEntry {
    pub uuid: Uuid,
    pub deleted_at: SystemTime,
}

/// CompleteEntry is written once the whole QoS flow has been completed
#[derive(Debug, Serialize, Deserialize)]
pub struct CompleteEntry {
    pub uuid: Uuid,
    pub completed_at: SystemTime,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Entry {
    Publish(PublishEntry),
    Accepted(AcceptedEntry),
    Sent(SentEntry),
    Deleted(DeletedEntry),
    Completed(CompleteEntry),
}

/// Each line will start with 1 byte that indicates type of entry followed by a
/// base64 encoded string of a RON object
impl FileLog {
    pub fn new(cwd: &str, file_name: &str) -> FileLog {
        DirBuilder::new().recursive(true).create(cwd).unwrap();
        let full_path = Path::new(cwd).join(file_name);
        FileLog {
            // cwd,
            // file_name,
            full_path: full_path.to_path_buf(),
            file: match File::create(&full_path) {
                Err(why) => panic!("couldn't open {:?}: {}", cwd, why),
                // write 0 as initial cursor
                Ok(file) => file,
            },
        }
    }

    pub fn append_publish(
        &mut self,
        uuid: Uuid,
        packet: PublishPacket,
        writer: &WriterRef,
        received_at: SystemTime,
    ) {
        self.append(
            PUBLISH,
            ron::to_string(&PublishEntry {
                uuid,
                packet,
                received_at,
                client_id: writer.client_id.clone(),
                session: SessionData {
                    uuid: writer.session_id.clone(),
                    is_persistent: writer.is_persistent_session,
                },
            })
            .unwrap()
            .as_bytes(),
        );
    }

    pub fn append_confirmation(
        &mut self,
        uuid: Uuid,
        packet: ConfirmationPacket,
        accepted_at: SystemTime,
    ) {
        self.append(
            ACCEPTED,
            ron::to_string(&AcceptedEntry {
                uuid,
                packet,
                accepted_at,
            })
            .unwrap()
            .as_bytes(),
        );
    }

    /// Appends message that tells that the whole message cycle has been completed
    /// E.g. QoS 1
    ///
    pub fn append_completion(&mut self, uuid: Uuid, completed_at: SystemTime) {
        self.append(
            COMPLETE,
            ron::to_string(&CompleteEntry { uuid, completed_at })
                .unwrap()
                .as_bytes(),
        );
    }

    pub fn append_deletion(&mut self, uuid: Uuid, deleted_at: SystemTime) {
        self.append(
            DELETED,
            ron::to_string(&DeletedEntry { uuid, deleted_at })
                .unwrap()
                .as_bytes(),
        );
    }

    pub fn append_sent(&mut self, uuid: Uuid, sent_at: SystemTime) {
        self.append(
            SENT,
            ron::to_string(&SentEntry { uuid, sent_at })
                .unwrap()
                .as_bytes(),
        );
    }

    pub fn append(&mut self, header: u8, data: &[u8]) {
        // let x: MyStruct = ron::from_str("(boolean: true, float: 1.23)").unwrap();
        let encoded = base64::encode(data);
        let buf = [&[header], encoded.as_bytes(), NEWLINE].concat();
        match self.file.write_all(&buf) {
            Err(why) => panic!(
                "[FileLog {:?}] couldn't write to file: {}",
                self.full_path, why
            ),
            Ok(_) => lunatic_log::debug!(
                "[FileLog {:?}] Successfully appended log to file",
                self.full_path
            ),
        };
    }

    pub fn read_file(cwd: &str, file_name: &str) -> IoResult<Vec<Entry>> {
        let full_path = Path::new(cwd).join(file_name);
        match File::open(&full_path) {
            Ok(file) => {
                lunatic_log::debug!("[persistence] successfully opened file {:?}", full_path);
                let mut res = Vec::with_capacity(200);
                for entry in BufReader::new(file).lines().flatten() {
                    let bytes = entry.as_bytes();
                    let decoded = base64::decode(&entry[1..]).unwrap();
                    let decoded = str::from_utf8(&decoded).unwrap();
                    match bytes[0] {
                        PUBLISH => {
                            res.push(Entry::Publish(
                                ron::from_str::<PublishEntry>(decoded).unwrap(),
                            ));
                        }
                        ACCEPTED => {
                            res.push(Entry::Accepted(
                                ron::from_str::<AcceptedEntry>(decoded).unwrap(),
                            ));
                        }
                        SENT => {
                            res.push(Entry::Sent(ron::from_str::<SentEntry>(decoded).unwrap()));
                        }
                        DELETED => {
                            res.push(Entry::Deleted(
                                ron::from_str::<DeletedEntry>(decoded).unwrap(),
                            ));
                        }
                        COMPLETE => {
                            res.push(Entry::Completed(
                                ron::from_str::<CompleteEntry>(decoded).unwrap(),
                            ));
                        }
                        other => panic!(
                            "[persistence] Corrupted file entry. Begins with {:?}",
                            other
                        ),
                    }
                }
                Ok(res)
            }
            Err(e) => {
                lunatic_log::error!(
                    "[persistence] Failed to open file {:?} | {:?}",
                    full_path,
                    e
                );
                Ok(vec![])
            }
        }
    }
}
