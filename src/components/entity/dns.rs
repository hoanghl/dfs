use log;

use std::{
    net::SocketAddr,
    sync::mpsc::{Receiver, Sender},
    time::Duration,
};

use crate::components::{
    configs::Configs,
    entity::nodes::Node,
    errors::NodeCreationError,
    packets::{forward_packet, Packet, PacketId},
};

pub struct DNS<'a> {
    configs: &'a Configs,
}

impl<'a> Node for DNS<'a> {
    fn trigger_processor(
        &mut self,
        rcvr_r2p: &Receiver<Packet>,
        sndr_p2s: &Sender<Packet>,
    ) -> Result<(), NodeCreationError> {
        let mut addr_master: Option<SocketAddr> = None;

        // ================================================
        // Start processing loop
        // ================================================
        loop {
            let packet = match rcvr_r2p.recv_timeout(Duration::from_secs(self.configs.timeout_chan_wait)) {
                Ok(packet) => packet,
                Err(_) => continue,
            };

            log::debug!("Received: {}", packet);

            let addr_sender = match packet.addr_sender {
                None => {
                    log::error!("Attribute 'addr_sender' in packet not existed.");
                    continue;
                }
                Some(addr) => addr,
            };

            match packet.packet_id {
                PacketId::AskIp => {
                    // Data/Client --AskIp-> DNS
                    match addr_master {
                        None => {
                            forward_packet(sndr_p2s, Packet::create_ask_ip_ack(addr_sender, None));
                        }
                        Some(addr_master) => {
                            forward_packet(sndr_p2s, Packet::create_ask_ip_ack(addr_sender, Some(&addr_master)));
                        }
                    };
                }
                PacketId::Notify => {
                    // Master --Notify-> DNS

                    addr_master = packet.addr_sender;
                    log::info!("Address Master just notified: {}", &addr_master.as_ref().unwrap());
                }
                _ => {
                    log::error!("Unsupported packet type: {}", packet);
                    continue;
                }
            };
        }
    }
}

impl<'a> DNS<'a> {
    /// Create new node
    pub fn new(configs: &Configs) -> DNS {
        DNS { configs }
    }
}
