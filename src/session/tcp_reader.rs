use super::session_state;
use crate::structure::*;
use lunatic::{net, process, Mailbox};
use mqtt_packet_3_5::{
    ConfirmationPacket, ConnectPacket, MqttPacket, PacketDecoder, PublishPacket, SubscribePacket,
};

struct ConnectionHelper {
    session_process: SessionProcess,
    connect_packet: ConnectPacket,
    pub reader: PacketDecoder<net::TcpStream>,
    writer_process: WriterProcess,
}

impl ConnectionHelper {
    pub fn new(
        session_process: SessionProcess,
        writer_process: WriterProcess,
        stream: net::TcpStream,
        connect_packet: ConnectPacket,
    ) -> ConnectionHelper {
        ConnectionHelper {
            reader: PacketDecoder::from_stream(stream),
            connect_packet,
            writer_process,
            session_process,
        }
    }

    pub fn publish(&mut self, packet: PublishPacket) {
        if let Err(e) = self
            .session_process
            .request(SessionRequest::Publish(packet.clone()))
        {
            eprintln!("Publish failed {:?}. Packet: {:?}", e, packet)
        }
    }

    pub fn subscribe(&mut self, packet: SubscribePacket) {
        if let Err(e) = self
            .session_process
            .request(SessionRequest::Subscribe(packet.clone()))
        {
            eprintln!("Subscribe failed {:?}. Packet: {:?}", e, packet)
        }
    }

    pub fn read(&mut self) -> Result<MqttPacket, String> {
        self.reader
            .decode_packet(self.connect_packet.protocol_version)
    }

    pub fn pong(&mut self) {
        match self.writer_process.request(WriterMessage::Pong) {
            Ok(_) => {}
            Err(e) => eprintln!(
                "Failed to send pong to writer for client {:?}. Error: {:?}",
                self.connect_packet.client_id, e
            ),
        }
    }

    pub fn disconnect(&mut self) {
        match self.writer_process.request(WriterMessage::Die) {
            Ok(_) => {}
            Err(e) => eprintln!(
                "[Reader {}] Failed to send disconnect to writer. Error: {:?}",
                self.connect_packet.client_id, e
            ),
        }
    }

    pub fn confirmation(&mut self, packet: ConfirmationPacket) {
        let cmd = packet.cmd;
        println!(
            "[Reader {}] SENDING CONFIRMATION TO SESSION {:?}",
            self.connect_packet.client_id, packet
        );
        if let Err(e) = self
            .session_process
            .request(SessionRequest::ConfirmationClient(packet))
        {
            eprintln!("Failed to send {:?} to session {:?}", cmd, e);
        }
    }
}

pub fn handle_tcp(
    (client_id, writer_process, stream, broker, connect_packet): (
        String,
        WriterProcess,
        net::TcpStream,
        BrokerProcess,
        ConnectPacket,
    ),
    _: Mailbox<()>,
) {
    // controls whether data is read from tcp_stream
    let session_process = process::spawn_with(
        (broker, writer_process.clone(), connect_packet.clone()),
        session_state::session_process,
    )
    .unwrap();
    let mut state = ConnectionHelper::new(session_process, writer_process, stream, connect_packet);
    loop {
        match state.read() {
            Ok(v) => {
                println!(
                    "[Reader {}] READ PACKET {:?}",
                    state.connect_packet.client_id, v
                );
                match v {
                    // handle authentication and session creation
                    // MqttPacket::Connect(connect_packet) => {
                    //     state.handle_connect(connect_packet, this.clone())
                    // }
                    MqttPacket::Disconnect(_) => state.disconnect(),
                    MqttPacket::Puback(packet)
                    | MqttPacket::Pubrec(packet)
                    | MqttPacket::Pubcomp(packet)
                    | MqttPacket::Pubrel(packet) => state.confirmation(packet),
                    MqttPacket::Pingreq => state.pong(),
                    // handle queue messages
                    MqttPacket::Publish(packet) => state.publish(packet),
                    MqttPacket::Subscribe(packet) => state.subscribe(packet),
                    msg => {
                        println!(
                            "[Reader {}] received message: {:?}",
                            state.connect_packet.client_id, msg
                        );
                    }
                }
            }
            Err(e) => {
                println!(
                    "[Reader {}] Error while decoding packet {:?}",
                    state.connect_packet.client_id, e
                );
                if e.contains("kind: UnexpectedEof") {
                    println!(
                        "[Reader {}] Exiting process...",
                        state.connect_packet.client_id
                    );
                    state.disconnect();
                    return;
                }
            }
        }
    }
}
