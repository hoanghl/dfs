use crate::components::{entity::node_roles::Role, errors::ParseError, inmem_db::utils::*};
use std::{
    convert::From,
    fmt::{self},
    io::Read,
    net::SocketAddrV4,
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream},
    str::FromStr,
    sync::mpsc::{Receiver, Sender},
    time::Duration,
};

// ================================================
// Definition for enum and constants
// ================================================

const BYTE_SEP_CHARACTER: u8 = 124; // byte value of character '|'
const SIZE_HEADER: usize = 5;
const BUFF_LEN: usize = 1024;

#[derive(Clone)]
#[repr(u8)]
pub enum Action {
    Read = 0,
    Write = 1,
}

impl FromStr for Action {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "read" => {
                return Ok(Self::Read);
            }
            "write" => {
                return Ok(Self::Write);
            }
            _ => Err(format!("Cannot parse given string to Action. Got: {}", s)),
        }
    }
}

#[rustfmt::skip]
#[derive(Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketId {
    Default                 = 0,
    Heartbeat               = 1,
    HeartbeatAck            = 2,
    RequestSendReplica      = 3,
    SendReplica             = 4,
    SendReplicaAck          = 5,
    AskIp                   = 6,
    AskIpAck                = 7,
    RequestFromClient       = 8,
    ResponseNodeIp          = 9,
    ClientUpload            = 10,
    DataNodeSendData        = 11,
    ClientRequestAck        = 12,
    StateSync               = 13,
    StateSyncAck            = 14,
    Notify                  = 15,
    ClientUploadAck         = 16,
}

pub struct Packet {
    // General attributes
    pub packet_id: PacketId,
    pub addr_sender: Option<SocketAddr>,
    pub addr_rcv: Option<SocketAddr>,

    // Attributes dedicated for sending
    pub payload: Option<Vec<u8>>,

    // Attributes parsed from payload
    pub addr_master: Option<SocketAddr>,
    pub addr_data: Option<SocketAddr>,
    pub role: Option<Role>,
    pub node_id: Option<String>,
    pub flag_read_write: Option<Action>,
    pub filename: Option<String>,
    pub binary: Option<Vec<u8>>,
}

// ================================================
// Implementation
// ================================================

impl From<u8> for Action {
    fn from(value: u8) -> Self {
        match value {
            0 => Action::Read,
            1 => Action::Write,
            _ => panic!("Error as parsing to enum Action: value = {}", value),
        }
    }
}
impl From<Action> for u8 {
    fn from(value: Action) -> Self {
        match value {
            Action::Read => 0,
            Action::Write => 1,
        }
    }
}

impl From<u8> for PacketId {
    fn from(value: u8) -> Self {
        match value {
            0 => PacketId::Default,
            1 => PacketId::Heartbeat,
            2 => PacketId::HeartbeatAck,
            3 => PacketId::RequestSendReplica,
            4 => PacketId::SendReplica,
            5 => PacketId::SendReplicaAck,
            6 => PacketId::AskIp,
            7 => PacketId::AskIpAck,
            8 => PacketId::RequestFromClient,
            9 => PacketId::ResponseNodeIp,
            10 => PacketId::ClientUpload,
            11 => PacketId::DataNodeSendData,
            12 => PacketId::ClientRequestAck,
            13 => PacketId::StateSync,
            14 => PacketId::StateSyncAck,
            15 => PacketId::Notify,
            16 => PacketId::ClientUploadAck,
            _ => panic!("Error as parsing to enum PacketId: value = {}", value),
        }
    }
}

impl From<PacketId> for u8 {
    fn from(value: PacketId) -> Self {
        match value {
            PacketId::Default => 0,
            PacketId::Heartbeat => 1,
            PacketId::HeartbeatAck => 2,
            PacketId::RequestSendReplica => 3,
            PacketId::SendReplica => 4,
            PacketId::SendReplicaAck => 5,
            PacketId::AskIp => 6,
            PacketId::AskIpAck => 7,
            PacketId::RequestFromClient => 8,
            PacketId::ResponseNodeIp => 9,
            PacketId::ClientUpload => 10,
            PacketId::DataNodeSendData => 11,
            PacketId::ClientRequestAck => 12,
            PacketId::StateSync => 13,
            PacketId::StateSyncAck => 14,
            PacketId::Notify => 15,
            PacketId::ClientUploadAck => 16,
        }
    }
}

impl std::fmt::Display for PacketId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            PacketId::Default => "Default",
            PacketId::Heartbeat => "Heartbeat",
            PacketId::HeartbeatAck => "HeartbeatAck",
            PacketId::RequestSendReplica => "RequestSendReplica",
            PacketId::SendReplica => "SendReplica",
            PacketId::SendReplicaAck => "SendReplicaAck",
            PacketId::AskIp => "AskIp",
            PacketId::AskIpAck => "AskIpAck",
            PacketId::RequestFromClient => "RequestFromClient",
            PacketId::ResponseNodeIp => "ResponseNodeIp",
            PacketId::ClientUpload => "ClientUpload",
            PacketId::DataNodeSendData => "DataNodeSendData",
            PacketId::ClientRequestAck => "ClientRequestAck",
            PacketId::StateSync => "StateSync",
            PacketId::StateSyncAck => "StateSyncAck",
            PacketId::Notify => "Notify",
            PacketId::ClientUploadAck => "ClientUploadAck",
        };
        write!(f, "{}", s)
    }
}

impl std::fmt::Debug for PacketId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            PacketId::Default => "Default",
            PacketId::Heartbeat => "Heartbeat",
            PacketId::HeartbeatAck => "HeartbeatAck",
            PacketId::RequestSendReplica => "RequestSendReplica",
            PacketId::SendReplica => "SendReplica",
            PacketId::SendReplicaAck => "SendReplicaAck",
            PacketId::AskIp => "AskIp",
            PacketId::AskIpAck => "AskIpAck",
            PacketId::RequestFromClient => "RequestFromClient",
            PacketId::ResponseNodeIp => "ResponseNodeIp",
            PacketId::ClientUpload => "ClientUpload",
            PacketId::DataNodeSendData => "DataNodeSendData",
            PacketId::ClientRequestAck => "ClientRequestAck",
            PacketId::StateSync => "StateSync",
            PacketId::StateSyncAck => "StateSyncAck",
            PacketId::Notify => "Notify",
            PacketId::ClientUploadAck => "ClientUploadAck",
        };
        write!(f, "{}", s)
    }
}

impl Default for Packet {
    fn default() -> Self {
        Packet {
            packet_id: PacketId::Default,
            payload: None,
            addr_sender: None,
            addr_rcv: None,
            addr_master: None,
            addr_data: None,
            role: None,
            node_id: None,
            flag_read_write: None,
            filename: None,
            binary: None,
        }
    }
}

impl fmt::Display for Packet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let addr_sender = match self.addr_sender {
            Some(addr_sender) => format!("{}", addr_sender),
            None => String::from("None"),
        };
        write!(f, "Packet: packet_id: {}, addr_sender: {}", self.packet_id, addr_sender)
    }
}

impl fmt::Debug for Packet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let addr_sender = match self.addr_sender {
            Some(addr_sender) => format!("{}", addr_sender),
            None => String::from("None"),
        };
        write!(f, "Packet: packet_id: {}, addr_sender: {}", self.packet_id, addr_sender)
    }
}

impl Packet {
    /// Extract packet to byte array
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = vec![];

        // Add packet ID
        bytes.push(u8::from(self.packet_id));

        // Add payload size
        let mut payload_size = 0;
        if self.payload != None {
            payload_size = self.payload.as_ref().unwrap().len();
        }
        let bytes_payload_size = (payload_size as u32).to_be_bytes();
        bytes.extend_from_slice(&bytes_payload_size);

        // Add payload
        if self.payload != None {
            bytes.extend_from_slice(self.payload.as_ref().unwrap());
        }

        bytes
    }

    // ================================================
    // Functions to create packets
    // ================================================

    /// Create Packet from stream
    pub fn from_stream(stream: &mut TcpStream) -> Result<Packet, ParseError> {
        // log::debug!("Receive data from: {}", stream.peer_addr().unwrap());

        // ================================================
        // Read bytes from stream
        // ================================================
        let mut bytes = Vec::<u8>::new();
        let mut buff: [u8; BUFF_LEN] = [0; BUFF_LEN];
        loop {
            let n = stream.read(&mut buff).unwrap();
            if n == 0 {
                break;
            } else {
                bytes.extend_from_slice(&buff[0..n]);

                if n < BUFF_LEN {
                    break;
                }
            }
        }

        // ================================================
        // Parse header
        // ================================================
        if bytes.len() < SIZE_HEADER {
            return Err(ParseError::incorrect_min_header_size(bytes.len()));
        }

        let packet_id = PacketId::from(bytes[0]);
        // log::debug!("packet_id = {}", packet_id);

        let payload_size = u32::from_be_bytes(bytes[1..5].try_into().expect("Incorrect length")) as usize;
        // log::debug!("payload_size = {}", payload_size);

        if bytes.len() != SIZE_HEADER + payload_size {
            return Err(ParseError::mismatched_packet_size(packet_id, bytes.len(), payload_size));
        }

        // ================================================
        // Parse payload
        // ================================================
        let mut payload = Vec::<u8>::new();
        payload.extend_from_slice(&bytes[5..(5 + payload_size) as usize]);

        let mut packet = Packet {
            packet_id,
            addr_sender: Some(stream.peer_addr().unwrap()),
            ..Default::default()
        };

        match packet_id {
            PacketId::Heartbeat => {}
            PacketId::HeartbeatAck => match String::from_utf8(payload) {
                Ok(node_id) => packet.node_id = Some(node_id),
                Err(err) => {
                    log::error!("Parsing HEARTBEAT_ACK: Cannot parse node_id: {err}");
                }
            },
            PacketId::RequestSendReplica => {
                // TODO: HoangLe [May-02]: Implement this
            }
            PacketId::SendReplica => {
                // TODO: HoangLe [May-02]: Implement this
            }
            PacketId::SendReplicaAck => {
                // TODO: HoangLe [May-02]: Implement this
            }
            PacketId::AskIp => match payload_size {
                2 => {
                    // Parse port of thread:Receiver of sender
                    packet.addr_sender.as_mut().unwrap().set_port(u16::from_be_bytes(
                        payload
                            .as_slice()
                            .try_into()
                            .expect("Cannot parse 2 bytes in payload to port value"),
                    ));
                }
                _ => {
                    log::info!("Packet AskIP requires specifying port of thread:Receiver of sender");
                    return Err(ParseError::mismatched_packet_size(packet_id, bytes.len(), payload_size));
                }
            },
            PacketId::AskIpAck => match payload_size {
                0 => {
                    return Err(ParseError::unavailable_master_ip());
                }
                6 => {
                    let mut buff = Vec::with_capacity(payload_size);
                    if let Err(err) = stream.read_exact(&mut buff) {
                        log::error!("Err as reading bytes for payload: {}", err);
                        return Err(ParseError::stream_reading_err());
                    }
                    packet.payload = Some(buff);

                    // Parse addr's Master from payload
                    let ip_master = Ipv4Addr::new(payload[0], payload[1], payload[2], payload[3]);
                    let port_master =
                        u16::from_be_bytes(payload[4..6].try_into().expect("Cannot cast last 2 bytes to array"));
                    packet.addr_master = Some(SocketAddr::V4(SocketAddrV4::new(ip_master, port_master)));
                }
                _ => {
                    return Err(ParseError::incorrect_payload_size_ask_ip_ack(payload.len()));
                }
            },
            PacketId::RequestFromClient => {
                // Parse 'flag_read_write'
                match payload[0] {
                    0 | 1 => {
                        packet.flag_read_write = Some(Action::from(payload[0]));
                    }
                    _ => {
                        log::error!(
                            "Receiving RequestFromClient from: {:?}: Invalid 'flag_read_write': {}",
                            packet.addr_sender,
                            payload[0]
                        );
                        return Err(ParseError::stream_reading_err());
                    }
                }

                // Parse 'port'
                packet.addr_sender.as_mut().unwrap().set_port(u16::from_be_bytes(
                    payload[1..3]
                        .try_into()
                        .expect("Cannot parse 2 bytes in payload to port value"),
                ));

                // Parse 'filename'
                match String::from_utf8(payload[3..payload_size - 1].to_vec()) {
                    Ok(filename) => {
                        packet.filename = Some(filename);
                    }
                    Err(err) => {
                        log::error!(
                            "Receiving RequestFromClient from: {:?}: Cannot parse filename: {}",
                            packet.addr_sender,
                            err
                        );
                        return Err(ParseError::stream_reading_err());
                    }
                };
            }

            PacketId::ResponseNodeIp => match payload_size {
                0 => {
                    return Err(ParseError::unavailable_master_ip());
                }
                6 => {
                    let mut buff = Vec::with_capacity(payload_size);
                    if let Err(err) = stream.read_exact(&mut buff) {
                        log::error!("Err as reading bytes for payload: {}", err);
                        return Err(ParseError::stream_reading_err());
                    }
                    packet.payload = Some(buff);

                    // Parse Data node's address from payload
                    let ip = Ipv4Addr::new(payload[0], payload[1], payload[2], payload[3]);
                    let port = u16::from_be_bytes(payload[4..6].try_into().expect("Cannot cast last 2 bytes to array"));
                    packet.addr_data = Some(SocketAddr::V4(SocketAddrV4::new(ip, port)));
                }
                _ => {
                    return Err(ParseError::incorrect_payload_size_ask_ip_ack(payload.len()));
                }
            },

            PacketId::ClientUpload => {
                // Parse port info from payload
                packet.addr_sender.as_mut().unwrap().set_port(u16::from_be_bytes(
                    payload[0..2].try_into().expect("Cannot cast last 2 bytes to array"),
                ));

                // Loop over payload to find position of 2 consecutive character '|'
                let mut last_idx_sep_tok: Option<usize> = None;
                for (idx, byte) in payload.iter().enumerate() {
                    last_idx_sep_tok = match byte {
                        &BYTE_SEP_CHARACTER => match last_idx_sep_tok {
                            None => Some(idx),
                            Some(_) => {
                                // Found 2 consecutive separating characters '||'
                                break;
                            }
                        },
                        _ => None,
                    };
                }

                // Handle some corner cases
                if last_idx_sep_tok.is_none() {
                    log::error!("Reading ClientUpload: Not found 2 consecutive separating characters '||'");
                    return Err(ParseError::stream_reading_err());
                }
                if last_idx_sep_tok.unwrap() + 2 == payload_size {
                    log::error!("Reading ClientUpload: Found 2 consecutive separating characters '||' at the end of payload => No binary found");
                    return Err(ParseError::stream_reading_err());
                }

                // Parse field 'filename' and 'binary'
                packet.filename = match String::from_utf8(payload[2..last_idx_sep_tok.unwrap()].to_vec()) {
                    Ok(filename) => Some(filename),
                    Err(err) => {
                        log::error!("Reading ClientUpload: Got error as parsing filename: {}", err);
                        None
                    }
                };
                packet.binary = Some(payload[last_idx_sep_tok.unwrap() + 2..payload_size].to_vec());
            }

            PacketId::DataNodeSendData => {
                // TODO: HoangLe [May-02]: Implement this
            }
            PacketId::ClientRequestAck => {
                // TODO: HoangLe [May-02]: Implement this
            }
            PacketId::StateSync => {
                // TODO: HoangLe [May-02]: Implement this
            }
            PacketId::StateSyncAck => {
                // TODO: HoangLe [May-02]: Implement this
            }
            PacketId::Notify => match payload_size {
                3 => {
                    // Parse role of sender
                    packet.role = Some(Role::from(payload[0]));

                    // Parse port info from payload
                    packet.addr_sender.as_mut().unwrap().set_port(u16::from_be_bytes(
                        payload[1..3].try_into().expect("Cannot cast last 2 bytes to array"),
                    ));
                }
                _ => {
                    return Err(ParseError::mismatched_packet_size(packet_id, bytes.len(), payload_size));
                }
            },
            PacketId::ClientUploadAck => {}
            _ => return Err(ParseError::incorrect_packet_id(packet_id as u8)),
        }

        log::debug!("{}", packet);

        return Ok(packet);
    }

    pub fn create_heartbeat(addr_rcv: SocketAddr) -> Packet {
        Packet {
            packet_id: PacketId::Heartbeat,
            addr_rcv: Some(addr_rcv),
            ..Default::default()
        }
    }

    pub fn create_heartbeat_ack(addr_rcv: SocketAddr, addr_current: SocketAddr) -> Packet {
        let mut payload = Vec::<u8>::new();
        match addr_current {
            SocketAddr::V4(addr) => {
                payload.extend_from_slice(conv_addr2id(addr.ip(), addr.port()).as_bytes());
            }
            _ => {
                log::error!("Creating HEARTBEAT_ACK, but IP of current node isn't IPv4 format.");
            }
        }

        Packet {
            packet_id: PacketId::HeartbeatAck,
            addr_rcv: Some(addr_rcv),
            payload: Some(payload),
            ..Default::default()
        }
    }

    // pub fn create_RequestSendReplica() -> Packet {
    //     // TODO: HoangLe [Apr-28]: Implement this
    // }
    // pub fn create_SendReplica() -> Packet {
    //     // TODO: HoangLe [Apr-28]: Implement this
    // }
    // pub fn create_SendReplicaAck() -> Packet {
    //     // TODO: HoangLe [Apr-28]: Implement this
    // }

    pub fn create_ask_ip(addr_rcv: SocketAddr, port: u16) -> Packet {
        // Craft payload
        let mut payload = Vec::<u8>::new();
        payload.extend_from_slice(&port.to_be_bytes());

        Packet {
            packet_id: PacketId::AskIp,
            addr_rcv: Some(addr_rcv),
            payload: Some(payload),
            ..Default::default()
        }
    }

    pub fn create_ask_ip_ack(addr_rcv: SocketAddr, addr_master: Option<&SocketAddr>) -> Packet {
        let mut packet = Packet {
            packet_id: PacketId::AskIpAck,
            addr_rcv: Some(addr_rcv),
            ..Default::default()
        };
        match addr_master {
            Some(addr_master) => {
                if let IpAddr::V4(ip_master) = addr_master.ip() {
                    let mut payload = ip_master.octets().to_vec();
                    payload.extend_from_slice(&addr_master.port().to_be_bytes());
                    packet.payload = Some(payload);
                }
            }
            None => {}
        }

        packet
    }

    pub fn create_request_from_client(action: Action, port: u16, filename: &String, addr_rcv: SocketAddr) -> Packet {
        let mut packet = Packet {
            packet_id: PacketId::RequestFromClient,
            addr_rcv: Some(addr_rcv),
            ..Default::default()
        };

        // Craft payload
        let mut payload = Vec::<u8>::new();
        payload.push(action as u8);

        payload.extend_from_slice(&port.to_be_bytes());

        payload.extend_from_slice(filename.as_bytes());

        packet.payload = Some(payload);

        packet
    }

    pub fn create_response_node_ip(addr_rcv: SocketAddr, addr_node: SocketAddr) -> Packet {
        let mut packet = Packet {
            packet_id: PacketId::ResponseNodeIp,
            addr_rcv: Some(addr_rcv),
            ..Default::default()
        };

        // Craft payload: Contain IP and port of node holding data file
        if let IpAddr::V4(ip) = addr_node.ip() {
            let mut payload = ip.octets().to_vec();
            payload.extend_from_slice(&addr_node.port().to_be_bytes());

            packet.payload = Some(payload);
        }

        packet
    }

    pub fn create_client_upload(port: u16, addr_rcv: SocketAddr, filename: &String, binary: Vec<u8>) -> Packet {
        let mut packet = Packet {
            packet_id: PacketId::ClientUpload,
            addr_rcv: Some(addr_rcv),
            ..Default::default()
        };

        // Craft payload: port + filename + '|' + '|' + binary
        let mut payload = Vec::<u8>::new();

        payload.extend_from_slice(&port.to_be_bytes());

        payload.extend_from_slice(filename.as_bytes());

        payload.push(BYTE_SEP_CHARACTER);
        payload.push(BYTE_SEP_CHARACTER);

        payload.extend(binary.iter());

        packet.payload = Some(payload);

        packet
    }
    // pub fn create_DataNodeSendData() -> Packet {
    //     // TODO: HoangLe [Apr-28]: Implement this
    // }
    // pub fn create_ClientRequestAck() -> Packet {
    //     // TODO: HoangLe [Apr-28]: Implement this
    // }
    // pub fn create_StateSync() -> Packet {
    //     // TODO: HoangLe [Apr-28]: Implement this
    // }
    // pub fn create_StateSyncAck() -> Packet {
    //     // TODO: HoangLe [Apr-28]: Implement this
    // }

    pub fn create_notify(addr_rcv: SocketAddr, role: &Role, addr_current: SocketAddr) -> Packet {
        // Craft payload
        let mut payload = Vec::<u8>::new();
        payload.push(u8::try_from(role).expect("Cannot parse 'role' to u8 value."));

        let port = addr_current.port();
        payload.extend_from_slice(&port.to_be_bytes());

        Packet {
            packet_id: PacketId::Notify,
            addr_rcv: Some(addr_rcv),
            payload: Some(payload),
            ..Default::default()
        }
    }

    pub fn create_client_upload_ack(addr_rcv: SocketAddr) -> Packet {
        Packet {
            packet_id: PacketId::ClientUploadAck,
            addr_rcv: Some(addr_rcv),
            ..Default::default()
        }
    }
}

pub fn forward_packet(sndr_p2s: &Sender<Packet>, packet: Packet) {
    log::debug!("packet size: {}", packet.to_bytes().len());

    if let Err(err) = sndr_p2s.send(packet) {
        log::error!("Err as sending from thread:Processor -> thread:Sender: {}", err);
    };
}

pub fn wait_packet(rcvr_r2p: &Receiver<Packet>) -> Packet {
    loop {
        let received = rcvr_r2p.recv_timeout(Duration::from_secs(2));
        if let Ok(packet) = received {
            // log::info!("{}", err);
            // continue;
            return packet;
        }
        // let packet = received.unwrap();
    }
}

pub fn forward_packet(sender_processor2sender: &Sender<Packet>, packet: Packet) {
    if let Err(err) = sender_processor2sender.send(packet) {
        log::error!("Err as sending from thread:Processor -> thread:Sender: {}", err);
    };
}

pub fn wait_packet(receiver_receiver2processor: &Receiver<Packet>) -> Packet {
    loop {
        let received = receiver_receiver2processor.recv_timeout(Duration::from_secs(2));
        if let Ok(packet) = received {
            // log::info!("{}", err);
            // continue;
            return packet;
        }
        // let packet = received.unwrap();
    }
}
