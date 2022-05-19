use mqtt_packet_3_5::{ConfirmationPacket, MqttPacket, PacketType, PublishPacket};
use serde::{Deserialize, Serialize};
use std::fs::{DirBuilder, File};
use std::io::prelude::*;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct FileLog {
    // cwd: str,
    // file_name: str,
    full_path: PathBuf,
    file: File,
}

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

    pub fn append_publish(&mut self, packet: PublishPacket) -> () {
        let encoded = MqttPacket::Publish(packet).encode(3).unwrap();
        self.append(&encoded);
    }

    pub fn append_confirmation(&mut self, packet: ConfirmationPacket) -> () {
        let encoded = match packet.cmd {
            PacketType::Puback => MqttPacket::Puback(packet),
            PacketType::Pubrec => MqttPacket::Pubrec(packet),
            PacketType::Pubrel => MqttPacket::Pubrel(packet),
            PacketType::Pubcomp => MqttPacket::Pubcomp(packet),
            _ => panic!("[Persistence] Expected confirmation, received {:?}", packet),
        };
        self.append(&encoded.encode(3).unwrap());
    }

    pub fn append_sent(&mut self, id: u16, qos: u8) -> () {
        self.append(&[
            b'>',
            b'>',
            b'S',
            b'E',
            b'N',
            b'T',
            (id >> 8) as u8,
            id as u8,
            qos,
        ]);
    }

    pub fn append(&mut self, data: &[u8]) -> () {
        if let Err(why) = self.file.write(&[b'\n']) {
            panic!(
                "[FileLog {:?}] couldn't write newline to file: {}",
                self.full_path, why
            );
        }
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
