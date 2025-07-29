use crate::components::{
    configs::Configs,
    entity::{node_roles::Role, nodes::Node},
    errors::{NodeCreationError, NodeCreationErrorCode},
    file_utils::FileUtils,
    inmem_db::{entries::FileInfoEntry, manager::DBManager, utils::*},
    packets::{forward_packet, Action, Packet, PacketId},
};
use log;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4},
    process::exit,
    str::FromStr,
    sync::mpsc::{Receiver, Sender},
    time::{Duration, SystemTime},
};

pub struct Master<'a> {
    configs: &'a Configs,
}

impl<'a> Node for Master<'a> {
    fn trigger_processor(
        &mut self,
        rcvr_r2p: &Receiver<Packet>,
        sndr_p2s: &Sender<Packet>,
    ) -> Result<(), NodeCreationError> {
        let addr_dns: SocketAddr = SocketAddr::new(IpAddr::V4(self.configs.ip_dns), self.configs.port_dns);
        let addr_current = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), self.configs.args.port));

        // For data management
        let file_utils = match FileUtils::new(&self.configs) {
            Ok(file_utils) => file_utils,
            Err(err) => {
                log::error!("Err as creating file_utils: {}", err);
                return Err(NodeCreationError {
                    error_code: NodeCreationErrorCode::ProcessorThreadErr,
                });
            }
        };

        // For in-mem db manager

        let mut db_manager = match DBManager::new(self.configs) {
            Ok(manager) => manager,
            Err(_) => {
                return Err(NodeCreationError {
                    error_code: NodeCreationErrorCode::ProcessorThreadErr,
                });
            }
        };

        if let Err(_) = db_manager.initialize_db(Role::Master) {
            return Err(NodeCreationError {
                error_code: NodeCreationErrorCode::ProcessorThreadErr,
            });
        }

        // For counter: Use for heartbeat timer
        let mut last_ts: Option<SystemTime> = None;

        // ================================================
        // Execute 1st step of Initial procedure based on node's role
        // ================================================

        // Send its IP to DNS
        if let Err(err) = sndr_p2s.send(Packet::create_notify(
            addr_dns.clone(),
            &Role::Master,
            addr_current.clone(),
        )) {
            log::error!("Error as sending Notify: {}", err);
            return Err(NodeCreationError {
                error_code: NodeCreationErrorCode::ProcessorThreadErr,
            });
        }

        // ================================================
        // Start processing loop
        // ================================================

        loop {
            match rcvr_r2p.recv_timeout(Duration::from_secs(self.configs.timeout_chan_wait)) {
                Ok(packet) => {
                    log::debug!("Received: {}", packet);

                    match packet.packet_id {
                        PacketId::HeartbeatAck => {
                            if let Some(node_id) = packet.node_id {
                                match SocketAddrV4::from_str(node_id.as_str()) {
                                    Ok(addr) => {
                                        if let Err(err) =
                                            db_manager.upsert_node(addr.ip().clone(), addr.port(), Role::Data)
                                        {
                                            log::error!("Error as UPSERT: {}", err);
                                        }
                                    }
                                    Err(err) => {
                                        log::error!(
                                            "Cannot parse following node_id to SocketAddrV4: {} | Err: {}",
                                            node_id,
                                            err
                                        );
                                    }
                                }
                            }
                        }
                        PacketId::Notify => {
                            log::info!("Master receives NOTIFY from: {:?}", packet.addr_sender);

                            match packet.addr_sender {
                                Some(addr_sender) => {
                                    if let IpAddr::V4(ip) = addr_sender.ip() {
                                        let _ = db_manager.upsert_node(ip, addr_sender.port(), Role::Data);

                                        log::info!("Master added new Data node: {}", addr_sender);
                                    }
                                }
                                None => {
                                    log::error!("NOTIFY packet contains no sender' address");
                                }
                            }
                        }
                        PacketId::RequestFromClient => {
                            if packet.flag_read_write.is_none() {
                                log::error!("Received RequestFromClient: 'flag_read_write' not specified");
                                continue;
                            };
                            if packet.addr_sender.is_none() {
                                log::error!("Received RequestFromClient: Cannot determine 'packet.addr_sender'");
                                continue;
                            }

                            match packet.flag_read_write.unwrap() {
                                Action::Read => {
                                    // TODO: HoangLe [Jun-14]: Implement this
                                }
                                Action::Write => {
                                    let node_ids = match db_manager.get_nodes_replication(2) {
                                        Ok(node_ids) => node_ids,
                                        Err(err) => {
                                            log::error!("{}", err);
                                            continue;
                                        }
                                    };

                                    let node_rcv_data = node_ids[0];
                                    forward_packet(
                                        sndr_p2s,
                                        Packet::create_response_node_ip(packet.addr_sender.unwrap(), node_rcv_data),
                                    );

                                    // TODO: HoangLe [Jun-22]: Continue with Replication flow
                                }
                            }
                        }
                        PacketId::ClientUpload => {
                            let filename = packet.filename.unwrap();

                            // Store data
                            if let Err(err) = file_utils.save_file(&filename, packet.binary.as_ref().unwrap()) {
                                log::error!("Cannot create new file: {}: Err: {}", filename, err);
                                continue;
                            };

                            // Insert data
                            let ip = match addr_current.ip() {
                                IpAddr::V4(ip) => ip,
                                _ => {
                                    log::error!("Cannot parse addr_current to IpV4 format: {}", { addr_current });
                                    continue;
                                }
                            };

                            if let Err(err) = db_manager.upsert_file(FileInfoEntry::initialize(
                                filename,
                                true,
                                String::from(conv_addr2id(&ip, addr_current.port())),
                            )) {
                                log::error!("Error as upsert: {}", err);
                                exit(1);
                            }

                            // Response to client
                            forward_packet(
                                sndr_p2s,
                                Packet::create_client_upload_ack(packet.addr_sender.clone().unwrap()),
                            );
                        }
                        _ => {
                            log::error!("Unsupported packet type: {}", packet);
                            continue;
                        }
                    };
                }
                Err(_) => {
                    // Check timer and send Heartbeat
                    if last_ts.is_none() {
                        last_ts = Some(SystemTime::now());
                        continue;
                    }
                    match SystemTime::now().duration_since(last_ts.unwrap()) {
                        Ok(n) => {
                            if n.as_secs() >= self.configs.interval_heartbeat {
                                last_ts = Some(SystemTime::now());

                                // Send heartbeat
                                if let Ok(data_nodes) = db_manager.get_data_nodes() {
                                    for node in &data_nodes {
                                        match node.ip {
                                            None => {
                                                log::error!(
                                                    "Cannot retrieve ip from node with node_id = {}",
                                                    node.node_id
                                                );
                                                continue;
                                            }
                                            Some(ip) => {
                                                let addr = SocketAddr::V4(SocketAddrV4::new(ip, node.port));

                                                log::info!("Send HEARTBEAT to {}", addr);
                                                forward_packet(sndr_p2s, Packet::create_heartbeat(addr));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            log::error!("{}", err);
                        }
                    }
                }
            };
        }
    }
}

impl<'a> Master<'a> {
    /// Create new node
    pub fn new(configs: &Configs) -> Master {
        Master { configs }
    }
}
