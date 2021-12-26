mod headerflags {
    use bitflags::bitflags;

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
}