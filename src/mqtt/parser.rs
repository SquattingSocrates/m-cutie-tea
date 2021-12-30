use super::message::*;
use super::ControlPacketType;

#[derive(Debug, PartialEq)]
pub struct ByteParser {
    // stream: iter::Peekable<io::Bytes<net::TcpStream>>,
    buf: Vec<u8>,
    cursor: usize,
}

impl ByteParser {
    pub fn new(buf: Vec<u8>) -> ByteParser {
        ByteParser {
            buf,
            cursor: 0usize,
        }
    }

    fn next(&mut self) -> Option<u8> {
        if self.cursor < self.buf.len() {
            let next = self.buf[self.cursor];
            self.cursor += 1;
            return Some(next);
        }
        None
    }

    fn peek(&mut self) -> Option<u8> {
        if self.cursor < self.buf.len() {
            return Some(self.buf[self.cursor]);
        }
        None
    }

    fn take(&mut self, n: usize) -> Result<&[u8], String> {
        let start = self.cursor;
        if start + n <= self.buf.len() {
            self.cursor += n;
            return Ok(&self.buf[start..start + n]);
        }
        Err(format!(
            "Trying to take {} out of bound. cursor: {} | buf.len: {}. buf: {:?}",
            n,
            self.cursor,
            self.buf.len(),
            self.buf
        ))
    }

    fn get_str_size(&mut self) -> Result<u32, String> {
        let str_size = self.take(2)?;
        let str_size = ((str_size[0] as u32) << 8) + str_size[1] as u32;
        Ok(str_size)
    }

    fn parse_string(&mut self) -> Result<String, String> {
        let str_size = self.get_str_size()?;
        if self.cursor + str_size as usize > self.buf.len() {
            return Err(format!("Trying to take more string than present in buffer, discarding as multi-message stream"));
        }
        let s = self.take(str_size as usize)?;
        match String::from_utf8(Vec::from(s)) {
            Ok(s) => return Ok(s),
            Err(err) => Err(format!("Failed to parse string {}", err)),
        }
    }

    fn parse_string_mandatory(&mut self) -> Result<String, String> {
        let str_size = self.get_str_size()?;
        let s = self.take(str_size as usize)?;
        match String::from_utf8(Vec::from(s)) {
            Ok(s) => return Ok(s),
            Err(err) => Err(format!("Failed to parse string {}", err)),
        }
    }

    pub fn take_while<F>(&mut self, cond: F) -> Result<&[u8], String>
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
                        break;
                    }
                }
                None => break,
            }
        }
        let res = &self.buf[self.cursor..curr];
        self.cursor = curr;
        Ok(res)
    }

    pub fn parse_fixed_header(&mut self) -> Result<FixedHeader, String> {
        let control_packet = self.take(1)?;
        let flags = control_packet[0] & 0b00001111;
        let control_packet = ControlPacketType::from_bits_truncate(control_packet[0] >> 4);
        let length = self.take_while(is_variable_length_int)?;
        let mut length = if length.len() > 1 {
            length[..length.len() - 1]
                .iter()
                .enumerate()
                .fold(0, |acc, (i, x)| acc + (x << (i * 8)) as u32)
                + length[length.len() - 1] as u32
        } else if length.len() == 1 {
            length[0] as u32
        } else {
            0u32
        };
        length += self.take(1)?[0] as u32;
        Ok(FixedHeader {
            control_packet,
            dup: flags >= 8,
            qos: (flags & 0x6) >> 1,
            retain: flags & 0x1 > 0,
            length,
        })
    }

    pub fn parse_variable_header_connect(&mut self) -> Result<ConnectVariableHeader, String> {
        let protocol_name = self.parse_string_mandatory()?;
        let protocol_version = self.take(1)?;
        let protocol_version = protocol_version[0] as u32;
        let connect_flags = self.take(1)?;
        let connect_flags = ConnectFlags::new(connect_flags[0]);
        let keep_alive = self.take(2)?;
        Ok(ConnectVariableHeader {
            protocol_name,
            protocol_version,
            connect_flags,
            keep_alive: ((keep_alive[0] as u32) << 8) + (keep_alive[1] as u32),
        })
    }

    pub fn parse_variable_header_publish(
        &mut self,
        qos: u8,
    ) -> Result<PublishVariableHeader, String> {
        let topic_name = self.parse_string_mandatory()?;
        let message_id = if qos > 0 { self.get_str_size()? } else { 0 };
        Ok(PublishVariableHeader {
            topic_name,
            message_id,
        })
    }

    pub fn parse_variable_header_subscribe(
        &mut self,
        qos: u8,
    ) -> Result<MessageIDVariableHeader, String> {
        Ok(MessageIDVariableHeader {
            message_id: if qos > 0 { self.get_str_size()? } else { 0 },
        })
    }

    pub fn parse_connect_message(
        &mut self,
        fixed_header: FixedHeader,
    ) -> Result<MqttMessage, String> {
        let variable_header = self.parse_variable_header_connect()?;
        let mut payload = Vec::<String>::new();
        while self.cursor < self.buf.len() {
            match self.parse_string() {
                Ok(topic) => {
                    payload.push(topic);
                }
                Err(err) => {
                    println!("{}", err);
                    break;
                }
            }
        }
        return Ok(MqttMessage::Connect(fixed_header, variable_header, payload));
    }

    pub fn parse_publish_message(
        &mut self,
        fixed_header: FixedHeader,
    ) -> Result<MqttMessage, String> {
        let variable_header = self.parse_variable_header_publish(fixed_header.qos)?;
        let mut bytes = vec![0; self.buf.len() - self.cursor];
        bytes.clone_from_slice(&self.buf[self.cursor..]);
        self.cursor = self.buf.len();
        let payload = match String::from_utf8(bytes) {
            Ok(s) => s,
            Err(e) => return Err(format!("Failed to push buf to string {:?}", e)),
        };

        return Ok(MqttMessage::Publish(
            fixed_header,
            variable_header,
            payload,
            PublishSource::Client,
        ));
    }

    pub fn parse_subscribe_message(
        &mut self,
        fixed_header: FixedHeader,
    ) -> Result<MqttMessage, String> {
        let variable_header = self.parse_variable_header_subscribe(fixed_header.qos)?;
        let mut payload = Vec::<SubscriptionRequest>::new();
        while self.cursor < self.buf.len() {
            match self.parse_string() {
                Ok(topic) => {
                    let qos = self.next().unwrap_or(0);
                    payload.push(SubscriptionRequest { topic, qos });
                }
                Err(err) => {
                    println!("{}", err);
                    break;
                }
            }
        }

        return Ok(MqttMessage::Subscribe(
            fixed_header,
            variable_header,
            payload,
        ));
    }

    pub fn parse_mqtt(&mut self) -> Result<MqttMessage, String> {
        let fixed_header = self.parse_fixed_header()?;

        println!("Parsing payload {:?}", fixed_header);
        if fixed_header.control_packet == ControlPacketType::CONNECT {
            return self.parse_connect_message(fixed_header);
        } else if fixed_header.control_packet == ControlPacketType::PUBLISH {
            return self.parse_publish_message(fixed_header);
        } else if fixed_header.control_packet == ControlPacketType::SUBSCRIBE {
            return self.parse_subscribe_message(fixed_header);
        } else if fixed_header.control_packet == ControlPacketType::PINGREQ {
            return Ok(MqttMessage::Pingreq(fixed_header));
        }

        Ok(MqttMessage::Unknown)
    }
}

fn is_variable_length_int(byte: u8) -> bool {
    byte & 0x40 == 0x40
}
