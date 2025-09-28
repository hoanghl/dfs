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
    sync::mpsc::{Receiver, Sender},
    time::Duration,
};

pub struct Data<'conf> {
    configs: &'conf Configs,
}

impl<'conf> Node for Data<'conf> {
    fn trigger_processor(
        &mut self,
        rcvr_r2p: &Receiver<Packet>,
        sndr_p2s: &Sender<Packet>,
    ) -> Result<(), NodeCreationError> {
        let mut addr_master: Option<SocketAddr> = None;

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
        if let Err(_) = db_manager.initialize_db(Role::Data) {
            return Err(NodeCreationError {
                error_code: NodeCreationErrorCode::ProcessorThreadErr,
            });
        }

        // ================================================
        // Execute 1st step of Initial procedure based on node's role
        // ================================================

        // Ask Master IP from DNS and notify to current master
        if let Err(err) = sndr_p2s.send(Packet::create_ask_ip(
            self.configs.addr_dns,
            self.configs.addr_local.port(),
        )) {
            log::error!("Error as sending AskIP: {}", err);
            return Err(NodeCreationError {
                error_code: NodeCreationErrorCode::ProcessorThreadErr,
            });
        }

        // ================================================
        // Start processing loop
        // ================================================
        loop {
            let packet = match rcvr_r2p.recv_timeout(Duration::from_secs(
                self.configs.timeout_chan_wait,
            )) {
                Ok(packet) => packet,
                Err(_) => continue,
            };

            match packet.packet_id {
                PacketId::Heartbeat => {
                    forward_packet(
                        sndr_p2s,
                        Packet::create_heartbeat_ack(
                            addr_master.clone().unwrap(),
                            self.configs.addr_local.clone(),
                        ),
                    );
                }
                PacketId::AskIpAck => match packet.addr_master {
                    None => {
                        log::error!(
                            "Received packet not contain address of Master"
                        );
                        continue;
                    }
                    Some(addr) => {
                        addr_master = Some(addr.clone());

                        forward_packet(
                            sndr_p2s,
                            Packet::create_notify(
                                addr,
                                &Role::Data,
                                self.configs.addr_local.clone(),
                            ),
                        );
                    }
                },
                PacketId::ClientUpload => {
                    let filename = packet.filename.unwrap();

                    // Store data
                    if let Err(err) = file_utils
                        .save_file(&filename, packet.binary.as_ref().unwrap())
                    {
                        log::error!(
                            "Cannot create new file: {}: Err: {}",
                            filename,
                            err
                        );
                        continue;
                    };

                    // Insert data
                    let ip = match self.configs.addr_local.ip() {
                        IpAddr::V4(ip) => ip,
                        _ => {
                            log::error!(
                                "Cannot parse self.configs.addr_local to IpV4 format: {}",
                                { self.configs.addr_local }
                            );
                            continue;
                        }
                    };

                    // FIXME: HoangLe [Sep-28]: Enable this
                    // if let Err(err) =
                    //     db_manager.upsert_file(FileInfoEntry::initialize(
                    //         &filename,
                    //         true,
                    //         String::from(conv_addr2id(
                    //             &ip,
                    //             self.configs.addr_local.port(),
                    //         )),
                    //     ))
                    // {
                    //     log::error!("Error as upsert: {}", err);
                    //     exit(1);
                    // }

                    // Send ACK to client
                    forward_packet(
                        sndr_p2s,
                        Packet::create_client_upload_ack(
                            packet.addr_sender.clone().unwrap(),
                        ),
                    );

                    // Notify Master (aka itself) node the writing process is completed
                    forward_packet(
                        sndr_p2s,
                        Packet::create_client_request_ack(
                            Action::Write,
                            self.configs.addr_local.port(),
                            &filename,
                            addr_master.unwrap().clone(),
                        ),
                    );
                }

                PacketId::RequestSendReplica => {
                    // [Replication step 3]: Send nominated file to the deliver node
                    let filename = packet.filename.unwrap();
                    let binary = match file_utils.read_file(&filename) {
                        Ok(binary) => binary,
                        Err(err) => {
                            log::error!(
                                "Err as reading file '{}': {}",
                                &filename,
                                err
                            );
                            continue;
                        }
                    };

                    forward_packet(
                        sndr_p2s,
                        Packet::create_send_replica(
                            packet.addr_deliver.unwrap(),
                            filename,
                            binary,
                        ),
                    );

                    log::debug!("Replication process: done step 3.1");
                }

                PacketId::SendReplica => {
                    // [Replication step 3] (continue): At deliver node: receive file name and binary, store it
                    let filename = packet.filename.unwrap();
                    let binary = packet.binary.unwrap();

                    // Store data
                    if let Err(err) = file_utils.save_file(&filename, &binary) {
                        log::error!(
                            "Cannot create new file: {}: Err: {}",
                            filename,
                            err
                        );
                        continue;
                    };

                    // Insert data
                    let ip = match self.configs.addr_local.ip() {
                        IpAddr::V4(ip) => ip,
                        _ => {
                            log::error!(
                                "Cannot parse self.configs.addr_local to IpV4 format: {}",
                                { self.configs.addr_local }
                            );
                            continue;
                        }
                    };
                    // FIXME: HoangLe [Sep-28]: Enable this
                    // if let Err(err) =
                    //     db_manager.upsert_file(FileInfoEntry::initialize(
                    //         &filename,
                    //         true,
                    //         String::from(conv_addr2id(
                    //             &ip,
                    //             self.configs.addr_local.port(),
                    //         )),
                    //     ))
                    // {
                    //     log::error!("Error as upsert: {}", err);
                    //     exit(1);
                    // }

                    log::debug!("Replication process: done step 3.2");

                    // [Replication step 4]: Send ACK to Master
                    forward_packet(
                        sndr_p2s,
                        Packet::create_send_replica_ack(
                            addr_master.unwrap().clone(),
                            filename,
                        ),
                    );

                    log::debug!("Replication process: done step 4");
                }

                PacketId::SendReplicaAck => {
                    // [Replication step 5]: Master updates info DB which file is controlled by which node
                    let filename = packet.filename.unwrap();
                    let ip = match packet.addr_sender.unwrap().ip() {
                        IpAddr::V4(ip) => ip,
                        _ => {
                            log::error!(
                                "Cannot parse self.configs.addr_local to IpV4 format: {}",
                                { self.configs.addr_local }
                            );
                            continue;
                        }
                    };
                    // FIXME: HoangLe [Sep-28]: Enable this
                    // if let Err(err) =
                    //     db_manager.upsert_file(FileInfoEntry::initialize(
                    //         &filename,
                    //         true,
                    //         String::from(conv_addr2id(
                    //             &ip,
                    //             self.configs.addr_local.port(),
                    //         )),
                    //     ))
                    // {
                    //     log::error!("Error as upsert: {}", err);
                    //     exit(1);
                    // }

                    log::debug!("Replication process: done step 5");
                }

                _ => {
                    log::error!("Unsupported packet type: {}", packet);
                    continue;
                }
            };
        }
    }
}

impl<'conf> Data<'conf> {
    /// Create new node
    pub fn new(configs: &'conf Configs) -> Data<'conf> {
        Data { configs }
    }
}
