use super::packets::PacketId;
use std::fmt::{Debug, Display};

// ================================================
// ParseError
// ================================================

pub enum ParseErrorCode {
    Default,
    IncorrectPacketId,
    IncorrectMinimumHeaderSize,
    MismatchedPacketSize,
    UnavailableMasterAddress,
    IncorrectPayloadSizeAskIPAck,
    StreamReadingError,
}

pub struct ParseError {
    pub error_code: ParseErrorCode,
    pub packet_id: Option<PacketId>,
    pub packet_id_value: Option<u8>,
    pub header_size: Option<usize>,
    pub payload_size: Option<usize>,
    pub packet_size: Option<usize>,
}

impl std::fmt::Display for ParseErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ParseErrorCode::Default => "Default",
            ParseErrorCode::IncorrectPacketId => "IncorrectPacketId",
            ParseErrorCode::IncorrectMinimumHeaderSize => "IncorrectMinimumHeaderSize",
            ParseErrorCode::MismatchedPacketSize => "MismatchedPacketSize",
            ParseErrorCode::UnavailableMasterAddress => "UnavailableMasterAddress",
            ParseErrorCode::IncorrectPayloadSizeAskIPAck => "IncorrectPayloadSizeAskIPAck",
            ParseErrorCode::StreamReadingError => "StreamReading",
        };
        write!(f, "{}", s)
    }
}

impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParseError{{error_code: {}, packet_id: {:?}, packet_id_value: {:?}, header_size: {:?}, payload_size: {:?}, packet_size: {:?}}}",
            self.error_code, self.packet_id, self.packet_id_value, self.header_size, self.payload_size, self.packet_size
        )
    }
}

impl Debug for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = "Cannot parse byte stream";
        write!(f, "{}", msg)
    }
}

impl ParseError {
    fn create_instance() -> ParseError {
        ParseError {
            error_code: ParseErrorCode::Default,
            packet_id: None,
            packet_id_value: None,
            header_size: None,
            payload_size: None,
            packet_size: None,
        }
    }

    pub fn incorrect_packet_id(packet_id_value: u8) -> ParseError {
        let mut err = ParseError::create_instance();
        err.error_code = ParseErrorCode::IncorrectPacketId;
        err.packet_id_value = Some(packet_id_value);

        err
    }

    pub fn incorrect_min_header_size(header_size: usize) -> ParseError {
        let mut err = ParseError::create_instance();
        err.error_code = ParseErrorCode::IncorrectMinimumHeaderSize;
        err.header_size = Some(header_size);

        err
    }

    pub fn mismatched_packet_size(packet_id: PacketId, packet_size: usize, payload_size: usize) -> ParseError {
        let mut err = ParseError::create_instance();
        err.error_code = ParseErrorCode::MismatchedPacketSize;
        err.packet_id = Some(packet_id);
        err.packet_size = Some(packet_size);
        err.payload_size = Some(payload_size);

        err
    }

    pub fn unavailable_master_ip() -> ParseError {
        let mut err = ParseError::create_instance();
        err.error_code = ParseErrorCode::UnavailableMasterAddress;

        err
    }

    pub fn incorrect_payload_size_ask_ip_ack(payload_size: usize) -> ParseError {
        let mut err = ParseError::create_instance();
        err.error_code = ParseErrorCode::IncorrectPayloadSizeAskIPAck;

        err.payload_size = Some(payload_size);

        err
    }

    pub fn stream_reading_err() -> ParseError {
        let mut err = ParseError::create_instance();
        err.error_code = ParseErrorCode::StreamReadingError;
        err
    }
}

// ================================================
// NodeCreationError
// ================================================
pub enum NodeCreationErrorCode {
    Default,
    ReceiverThreadErr,
    ProcessorThreadErr,
    SenderThreadErr,
}

pub struct NodeCreationError {
    pub error_code: NodeCreationErrorCode,
}

impl Display for NodeCreationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO: HoangLe [Apr-28]: Modify this
        let msg = "Cannot parse byte stream";
        write!(f, "{}", msg)
    }
}

impl Debug for NodeCreationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO: HoangLe [Apr-28]: Modify this
        let msg = "Cannot parse byte stream";
        write!(f, "{}", msg)
    }
}
