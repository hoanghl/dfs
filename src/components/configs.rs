use std::net::Ipv4Addr;

use clap::Parser;

use crate::components::{entity::node_roles::Role, packets::Action};

#[derive(Parser)]
#[command(version, about, long_about = None)]

pub struct Configs {
    // Network-related params
    pub ip_dns: Ipv4Addr,
    pub port_dns: u16,

    pub port: u16,

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
    pub fn initialize(ip_dns: Ipv4Addr, port_dns: u16) -> Configs {
        Configs {
            ip_dns,
            port_dns,
            ..Default::default()
        }
    }
}

impl Default for Configs {
    fn default() -> Self {
        Configs {
            ip_dns: Ipv4Addr::new(0, 0, 0, 0),
            port_dns: 0,
            port: 7888,
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
