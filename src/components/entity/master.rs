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
                                    let node_ids = match db_manager.get_nodes_replication(1) {
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
                                &filename,
                                true,
                                String::from(conv_addr2id(&ip, addr_current.port())),
                            )) {
                                log::error!("Error as upsert: {}", err);
                                exit(1);
                            }

                            // Send ACK to client
                            forward_packet(
                                sndr_p2s,
                                Packet::create_client_upload_ack(packet.addr_sender.clone().unwrap()),
                            );

                            // Notify Master (aka itself) node the writing process is completed
                            forward_packet(
                                sndr_p2s,
                                Packet::create_client_request_ack(Action::Write, &filename, addr_current),
                            );
                        }

                        PacketId::ClientRequestAck => {
                            // If write request: Master inserts info of node which is just receiving file from client
                            if let Action::Write = packet.flag_read_write.unwrap() {
                                let addr_sender = packet.addr_sender.as_ref().unwrap();

                                match addr_sender.ip() {
                                    IpAddr::V4(ip) => {
                                        if let Err(err) = db_manager.upsert_file(FileInfoEntry::initialize(
                                            packet.filename.as_ref().unwrap(),
                                            true,
                                            conv_addr2id(&ip, addr_sender.port()),
                                        )) {
                                            log::error!("Error as upsert: {}", err);
                                            exit(1);
                                        }
                                    }
                                    _ => {}
                                }
                            }

                            // Start Replication process

                            // [Replication step 1]: Select suitable node to store the file
                            let addr_deliver = match db_manager.get_nodes_replication(1) {
                                Ok(node_ids) => node_ids[0],
                                Err(err) => {
                                    log::error!("{}", err);
                                    continue;
                                }
                            };

                            // [Replication step 2]: Notify node A to send file to node B with RequestSendReplica
                            forward_packet(
                                sndr_p2s,
                                Packet::create_request_send_replica(
                                    packet.addr_sender.as_ref().unwrap().clone(),
                                    addr_deliver,
                                    packet.filename.as_ref().unwrap().clone(),
                                ),
                            );
                        }

                        PacketId::RequestSendReplica => {
                            // [Replication step 3]: Send nominated file to the deliver node
                            let filename = packet.filename.unwrap();
                            let binary = match file_utils.read_file(&filename) {
                                Ok(binary) => binary,
                                Err(err) => {
                                    log::error!("Err as reading file '{}': {}", &filename, err);
                                    continue;
                                }
                            };

                            forward_packet(
                                sndr_p2s,
                                Packet::create_send_replica(packet.addr_deliver.unwrap(), filename, binary),
                            );
                        }

                        PacketId::SendReplica => {
                            // [Replication step 3] (continue): At deliver node: receive file name and binary, store it
                            let filename = packet.filename.unwrap();
                            let binary = match file_utils.read_file(&filename) {
                                Ok(binary) => binary,
                                Err(err) => {
                                    log::error!("Err as reading file '{}': {}", &filename, err);
                                    continue;
                                }
                            };

                            // Store data
                            if let Err(err) = file_utils.save_file(&filename, &binary) {
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
                                &filename,
                                true,
                                String::from(conv_addr2id(&ip, addr_current.port())),
                            )) {
                                log::error!("Error as upsert: {}", err);
                                exit(1);
                            }

                            // [Replication step 4]: Send ACK to Master
                            forward_packet(
                                sndr_p2s,
                                Packet::create_send_replica_ack(addr_current.clone(), filename),
                            );
                        }

                        PacketId::SendReplicaAck => {
                            // [Replication step 5]: Master updates info DB which file is controlled by which node
                            let filename = packet.filename.unwrap();
                            let ip = match packet.addr_sender.unwrap().ip() {
                                IpAddr::V4(ip) => ip,
                                _ => {
                                    log::error!("Cannot parse addr_current to IpV4 format: {}", { addr_current });
                                    continue;
                                }
                            };
                            if let Err(err) = db_manager.upsert_file(FileInfoEntry::initialize(
                                &filename,
                                true,
                                String::from(conv_addr2id(&ip, addr_current.port())),
                            )) {
                                log::error!("Error as upsert: {}", err);
                                exit(1);
                            }
                        }

                        _ => {
                            log::error!("Unsupported packet type: {}", packet);
                            continue;
                        }
                    };
                }
                Err(_) => {
                    // Check timer and send Heartbeat
                    // FIXME: HoangLe [Jul-29]: Enable this after testing
                    // if last_ts.is_none() {
                    //     last_ts = Some(SystemTime::now());
                    //     continue;
                    // }
                    // match SystemTime::now().duration_since(last_ts.unwrap()) {
                    //     Ok(n) => {
                    //         if n.as_secs() >= self.configs.interval_heartbeat {
                    //             last_ts = Some(SystemTime::now());

                    //             // Send heartbeat
                    //             if let Ok(data_nodes) = db_manager.get_data_nodes() {
                    //                 for node in &data_nodes {
                    //                     match node.ip {
                    //                         None => {
                    //                             log::error!(
                    //                                 "Cannot retrieve ip from node with node_id = {}",
                    //                                 node.node_id
                    //                             );
                    //                             continue;
                    //                         }
                    //                         Some(ip) => {
                    //                             let addr = SocketAddr::V4(SocketAddrV4::new(ip, node.port));

                    //                             log::info!("Send HEARTBEAT to {}", addr);
                    //                             forward_packet(sndr_p2s, Packet::create_heartbeat(addr));
                    //                         }
                    //                     }
                    //                 }
                    //             }
                    //         }
                    //     }
                    //     Err(err) => {
                    //         log::error!("{}", err);
                    //     }
                    // }
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
