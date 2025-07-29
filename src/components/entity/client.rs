use crate::components::{
    configs::Configs,
    entity::nodes::Node,
    errors::NodeCreationError,
    packets::{forward_packet, wait_packet, Action, Packet, PacketId},
};
use log;
use std::{
    fs::File,
    io::Read,
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4},
    process::exit,
    sync::mpsc::{Receiver, Sender},
};

// ================================================
// Definitions
// ================================================
pub struct Client<'conf> {
    configs: &'conf Configs,
}

// ================================================
// Implementations
// ================================================
impl<'conf> Client<'conf> {
    pub fn new(configs: &Configs) -> Client {
        Client { configs }
    }

    pub fn send_file(&self, rcvr_r2p: &Receiver<Packet>, sndr_p2s: &Sender<Packet>) {
        let addr_dns: SocketAddr = SocketAddr::new(IpAddr::V4(self.configs.ip_dns), self.configs.port_dns);
        let addr_current = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), self.configs.args.port));

        let mut packet: Packet;

        // 1. Read file
        log::debug!("1. Read file: {:?}", self.configs.args.path);

        let mut file = match File::open(self.configs.args.path.as_ref().unwrap()) {
            Ok(file) => file,
            Err(err) => {
                log::error!("Reading file: {:?}. Got err: {}", self.configs.args.path, err);
                exit(1);
            }
        };
        let mut binary = Vec::new();
        if let Err(err) = file.read_to_end(&mut binary) {
            log::error!("Reading file: {:?}. Got err: {}", self.configs.args.path, err);
            exit(1);
        }

        // 2. Ask DNS for Master's address
        log::debug!("Ask Master address from DNS: {}", addr_dns);

        forward_packet(sndr_p2s, Packet::create_ask_ip(addr_dns, addr_current.port()));
        packet = wait_packet(rcvr_r2p);
        if PacketId::AskIpAck != packet.packet_id {
            log::error!("Must received AskIpAck from DNS. Got: {}", packet.packet_id);
            exit(1);
        }
        let addr_master = match packet.addr_master {
            Some(addr) => addr,
            None => {
                log::error!("Received packet not contain addr_master");
                exit(1);
            }
        };

        // 3. Connect to Master to get addr of node to send data
        log::debug!("Connect to Master: {}", addr_master);

        forward_packet(
            sndr_p2s,
            Packet::create_request_from_client(
                Action::Write,
                self.configs.args.port,
                self.configs.args.name.as_ref().unwrap(),
                addr_master,
            ),
        );
        packet = wait_packet(rcvr_r2p);
        if PacketId::ResponseNodeIp != packet.packet_id {
            log::error!("Must received ResponseNodeIp from DNS. Got: {}", packet.packet_id);
            exit(1);
        }
        if packet.addr_data.is_none() {
            log::error!("Received packet not contained 'addr_data'");
            exit(1);
        };
        let addr_data = packet.addr_data.unwrap();

        // 4. Connect to Data node to send file
        log::debug!(
            "Connect to Data node to send file: {} - {:?}",
            addr_data,
            self.configs.args.path
        );

        forward_packet(
            sndr_p2s,
            Packet::create_client_upload(
                self.configs.args.port,
                addr_data,
                self.configs.args.name.as_ref().unwrap(),
                binary,
            ),
        );
        packet = wait_packet(rcvr_r2p);
        if PacketId::ClientUploadAck != packet.packet_id {
            log::error!("Supposed to received ClientRequestAck. Got: {}", packet.packet_id);
        }

        // 2. Ask DNS for Master's address
        log::debug!("Ask Master address from DNS: {}", addr_dns);

        forward_packet(
            sender_processor2sender,
            Packet::create_ask_ip(addr_dns, addr_current.port()),
        );
        packet = wait_packet(receiver_receiver2processor);
        if PacketId::AskIpAck != packet.packet_id {
            log::error!("Must received AskIpAck from DNS. Got: {}", packet.packet_id);
            exit(1);
        }
        let addr_master = match packet.addr_master {
            Some(addr) => addr,
            None => {
                log::error!("Received packet not contain addr_master");
                exit(1);
            }
        };

        // 3. Connect to Master to get addr of node to send data
        log::debug!("Connect to Master: {}", addr_master);

        forward_packet(
            sender_processor2sender,
            Packet::create_request_from_client(
                Action::Write,
                self.configs.args.port,
                self.configs.args.name.as_ref().unwrap(),
                addr_master,
            ),
        );
        packet = wait_packet(receiver_receiver2processor);
        if PacketId::ResponseNodeIp != packet.packet_id {
            log::error!("Must received ResponseNodeIp from DNS. Got: {}", packet.packet_id);
            exit(1);
        }
        if packet.addr_data.is_none() {
            log::error!("Received packet not contained 'addr_data'");
            exit(1);
        };
        let addr_data = packet.addr_data.unwrap();

        // 4. Connect to Data node to send file
        log::debug!(
            "Connect to Data node to send file: {} - {:?}",
            addr_data,
            self.configs.args.path
        );

        forward_packet(
            sender_processor2sender,
            Packet::create_client_upload(addr_data, self.configs.args.name.as_ref().unwrap(), binary),
        );
    }
}
