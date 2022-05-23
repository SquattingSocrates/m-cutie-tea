use mqtt_packet_3_5::{ConfirmationPacket, MqttPacket, PacketType, PublishPacket};
use std::fs::{DirBuilder, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

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
const PUBLISH: &[u8] = "MQP".as_bytes();
const ACCEPTED: &[u8] = "MQA".as_bytes();
const SENT: &[u8] = "MQS".as_bytes();
const DELETED: &[u8] = "MQD".as_bytes();
const COMPLETE: &[u8] = "MQC".as_bytes();

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
                Ok(mut file) => match file.write_all("0".as_bytes()) {
                    Err(why) => panic!("couldn't read {:?}: {}", cwd, why),
                    Ok(_) => file,
                },
            },
        }
    }

    pub fn append_publish(&mut self, packet: PublishPacket, started_at: SystemTime) -> () {
        let encoded = MqttPacket::Publish(packet).encode(3).unwrap();
        self.append(PUBLISH, &encoded);
    }

    pub fn append_confirmation(&mut self, packet: ConfirmationPacket) -> () {
        let encoded = match packet.cmd {
            PacketType::Puback => MqttPacket::Puback(packet),
            PacketType::Pubrec => MqttPacket::Pubrec(packet),
            PacketType::Pubrel => MqttPacket::Pubrel(packet),
            PacketType::Pubcomp => MqttPacket::Pubcomp(packet),
            _ => panic!("[Persistence] Expected confirmation, received {:?}", packet),
        };
        self.append(ACCEPTED, &encoded.encode(3).unwrap());
    }

    /// Appends message that tells that the whole message cycle has been completed
    /// E.g. QoS 1
    ///
    pub fn append_completion(&mut self, qos: u8, message_id: u16) -> () {
        self.append(COMPLETE, &[qos, (message_id >> 8) as u8, message_id as u8]);
    }

    pub fn append_sent(&mut self, id: u16, qos: u8) -> () {
        self.append(SENT, &[(id >> 8) as u8, id as u8, qos]);
    }

    pub fn append(&mut self, header: &[u8], data: &[u8]) -> () {
        let buf = [header, data, NEWLINE].concat();
        match self.file.write_all(data) {
            Err(why) => panic!(
                "[FileLog {:?}] couldn't write to file: {}",
                self.full_path, why
            ),
            Ok(_) => println!(
                "[FileLog {:?}] Successfully appended log to file",
                self.full_path
            ),
        };
    }
}
