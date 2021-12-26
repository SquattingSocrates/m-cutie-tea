use std::io;
use std::iter;
use lunatic::{net, process, Mailbox};
use bitflags::bitflags;

use std::time::SystemTime;

bitflags! {
    pub struct ControlPacketType: u8 {
        const RESERVED = 0b00000000;
        const CONNECT = 0b00000001;
        const CONNACK = 0b00000010;
        const PUBLISH = 0b00000011;
        const PUBACK = 0b00000100;
        const PUBREC = 0b00000101;
        const PUBREL = 0b00000110;
        const PUBCOMP = 0b00000111;
        const SUBSCRIBE = 0b00001000;
        const SUBACK = 0b00001001;
        const UNSUBSCRIBE = 0b00001010;
        const UNSUBACK = 0b00001011;
        const PINGREQ = 0b00001100;
        const PINGRESP = 0b00001101;
        const DISCONNECT = 0b00001110;
        const AUTH = 0b00001111;
    }
}    

#[derive(Debug, PartialEq)]
pub struct ConnectPayload {
    topic: String,
}

#[derive(Debug, PartialEq)]
pub enum MqttPayload {
    Empty,
    Connect(Vec<String>),
    Publish(Vec<u8>),
    Subsribe(String)
}

#[derive(Debug, PartialEq)]
pub struct ByteParser {
    // stream: iter::Peekable<io::Bytes<net::TcpStream>>,
    buf: Vec<u8>,
    cursor: usize
}

impl ByteParser {
    pub fn new(buf: Vec<u8>) -> ByteParser {
        ByteParser {buf, cursor: 0usize}
    }

    fn next(&mut self) -> Option<u8> {
        if self.cursor < self.buf.len() {
            let next = self.buf[self.cursor];
            self.cursor += 1;
            return Some(next)
        }
        None
    }

    fn peek(&mut self) -> Option<u8> {
        if self.cursor < self.buf.len() {
            return Some(self.buf[self.cursor])
        }
        None
    }

    fn take(&mut self, n: usize, fail_on_empty: bool) -> Result<&[u8], String> {
        let start = self.cursor;
        if start + n <= self.buf.len() {
            self.cursor += n;
            return Ok(&self.buf[start..start+n])
        }
        Err(format!("Trying to take {} out of bound. cursor: {} | buf.len: {}", n, self.cursor, self.buf.len()))
    }

    fn parse_string(&mut self) -> Result<String, String> {
        let str_size = self.take(2, true)?;
        println!("STR_SIZE {:?}", str_size);
        let str_size = ((str_size[0] as u32) << 8) + str_size[1] as u32;
        println!("Parsing payload with size {}", str_size);
        let s = self.take(str_size as usize, true)?;
        println!("PROTO {:?}", s);
        match String::from_utf8(Vec::from(s)) {
            Ok(s) => return Ok(s),
            Err(err) => {
                Err(format!("Failed to parse string {}", err))
            }
        }
    }

    pub fn take_while<F>(
        &mut self,
        cond: F,
        ) -> Result<&[u8], String>
        where
        F: Fn(u8) -> bool,
        {
        let mut curr = self.cursor;
        while curr < self.buf.len() {
            match self.peek() {
                Some(b) => {
                    if cond(b) {
                        curr += 1
                    } else {
                        break
                    }
                }
                None => break
            }
        }
        let res = &self.buf[self.cursor..curr];
        self.cursor = curr;
        Ok(res)
    }

    pub fn parse_fixed_header(&mut self) -> Result<FixedHeader, String> {
        let control_packet = self.take(1, true)?;
        let flags = control_packet[0] & 0b00001111;
        let control_packet = ControlPacketType::from_bits_truncate(control_packet[0] >> 4);
        let length = self.take_while(is_variable_length_int)?;
        println!("TOOK LENGTH {:?}", length);
        let mut length = if length.len() > 1 {
            length[..length.len()-1].iter().enumerate().fold(0, |acc, (i,x)| acc + (x << (i*8)) as u32) + length[length.len()-1] as u32
        } else if length.len() == 1 {
            length[0] as u32
        } else {
            0u32
        }; 
        length += self.take(1, true)?[0] as u32;
        println!("PARSED LEN {} | {}", length, self.buf.len());
        Ok(
            FixedHeader {
                control_packet,
                dup: flags & 0x8 > 0,
                qos: (flags & 0x6) >> 1,
                retain: flags & 0x1 > 0,
                length
            }
        )
    }

    pub fn parse_variable_header_connect(&mut self) -> Result<VariableHeader, String> {
        let protocol_name = self.parse_string()?;
        let protocol_version = self.take(1, true)?;
        let protocol_version = protocol_version[0] as u32;
        let connect_flags = self.take(1, true)?;
        let connect_flags = ConnectFlags::new(connect_flags[0]);
        let keep_alive = self.take(2, true)?;
        Ok(
            VariableHeader::Connect(VariableHeaderConnect {
                protocol_name,
                protocol_version,
                connect_flags,
                keep_alive: ((keep_alive[0] as u32) << 8) + (keep_alive[1] as u32),
            })
        )
    }

    pub fn parse_variable_header_publish(&mut self) -> Result<VariableHeader, String> {
        Ok(
            VariableHeader::Publish(VariableHeaderPublish {
                topic_name: self.parse_string()?
            })
        )
    }

    pub fn parse_variable_header(&mut self, control_packet: ControlPacketType) -> Result<VariableHeader, String> {
        if control_packet == ControlPacketType::CONNECT {
            return self.parse_variable_header_connect()
        } else if control_packet == ControlPacketType::PUBLISH {
            return self.parse_variable_header_publish()
        } {
            Ok(VariableHeader::Empty)
        } 
    }

    fn parse_payload(&mut self, control_packet: ControlPacketType) -> Result<MqttPayload, String> {
        if control_packet == ControlPacketType::CONNECT {
            let mut payload = Vec::<String>::new();
            while self.cursor < self.buf.len() {
                payload.push(self.parse_string()?);
            }
            return Ok(MqttPayload::Connect(payload))
        } else if control_packet == ControlPacketType::PUBLISH {
            let mut bytes = vec![0; self.buf.len() - self.cursor];
            bytes.clone_from_slice(&self.buf[self.cursor..]);
            self.cursor = self.buf.len();
            println!("GOT THESE BYTES {:?}", bytes);
            return Ok(MqttPayload::Publish(bytes))
        }
        Ok(MqttPayload::Empty)
    }

    pub fn parse_mqtt(&mut self) -> Result<MqttMessage, String> {
        let fixed_header = self.parse_fixed_header()?;
        let variable_header = self.parse_variable_header(fixed_header.control_packet)?;
        let payload = self.parse_payload(fixed_header.control_packet)?;
    
        Ok(
            MqttMessage {
                fixed_header,
                variable_header,
                payload
            }
        )
    }
}

fn is_variable_length_int(byte: u8) -> bool {
    byte & 0x40 == 0x40
}

#[derive(Debug, PartialEq)]
pub struct FixedHeader {
    pub control_packet: ControlPacketType,
    pub length: u32,
    pub dup: bool,
    pub qos: u8,
    pub retain: bool
}

#[derive(Debug, PartialEq)]
pub struct ConnectFlags {
    pub user_name: bool,
    pub password: bool,
    pub will_retain: bool,
    pub will_qos: u8,
    pub will: bool,
    pub clean_session: bool
}

impl ConnectFlags {
    pub fn new(byte: u8) -> ConnectFlags {
        ConnectFlags {
            user_name: (byte & (1 << 7)) != 0,
            password: (byte & (1 << 6)) != 0,
            will_retain: (byte & (1 << 5)) != 0,
            will_qos: byte & ((1 << 4) + (1 << 3)),
            will: (byte & (1 << 2)) != 0,
            clean_session: (byte & (1 << 1)) != 0,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct VariableHeaderConnect {
    pub protocol_name: String,
    pub protocol_version: u32,
    pub connect_flags: ConnectFlags,
    pub keep_alive: u32
}

#[derive(Debug, PartialEq)]
pub struct VariableHeaderPublish {
    pub topic_name: String
}

#[derive(Debug, PartialEq)]
pub struct VariableHeaderSubscribe {
    pub topics: Vec<String>
}

#[derive(Debug, PartialEq)]
pub enum VariableHeader {
    Empty,
    Connect(VariableHeaderConnect),
    Publish(VariableHeaderPublish),
    Subscribe(VariableHeaderSubscribe)
}

#[derive(Debug, PartialEq)]
pub struct MqttMessage {
    pub fixed_header: FixedHeader,
    pub variable_header: VariableHeader,
    pub payload: MqttPayload,
}
