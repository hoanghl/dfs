use std::{
    net::Ipv4Addr,
    net::{IpAddr, SocketAddr},
};

use clap::Parser;

use crate::components::{entity::node_roles::Role, packets::Action};

#[derive(Parser)]
#[command(version, about, long_about = None)]

pub struct Configs {
    // Network-related params
    pub addr_dns: SocketAddr,
    pub addr_local: SocketAddr,

    // Operation-related params
    pub role: Role,
    pub dir_data: String,
    pub interval_heartbeat: u64,
    pub timeout_chan_wait: u64,

    // Client-specific arguments
    pub action: Option<Action>,
    pub name: Option<String>,
    pub path: Option<String>,
}

impl Configs {
    pub fn initialize(ip_dns: Ipv4Addr, port_dns: u16, port: u16) -> Configs {
        let ip_local = match local_ip_address::local_ip() {
            Ok(ip) => ip,
            Err(err) => {
                panic!("Err as getting local IP: {}", err);
            }
        };
        let addr_local = SocketAddr::new(ip_local, port);
        let addr_dns = SocketAddr::new(IpAddr::V4(ip_dns), port_dns);

        Configs {
            addr_dns,
            addr_local: addr_local,
            role: Role::Data,
            dir_data: "./data".to_string(),
            interval_heartbeat: 20,
            timeout_chan_wait: 1,
            action: None,
            name: None,
            path: None,
        }
    }
}
