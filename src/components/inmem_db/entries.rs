use super::utils::*;
use crate::components::entity::node_roles::Role;
use chrono::{DateTime, Local};
use std::net::Ipv4Addr;

// ================================================
// Definitions for DB entry
// ================================================

pub struct FileInfoEntry {
    pub filename: String,
    pub is_local: bool,
    pub node_id: String,
    pub last_updated: Option<DateTime<Local>>,
}

pub struct NodeInfoEntry {
    pub node_id: String,
    pub ip: Option<Ipv4Addr>,
    pub port: u16,
    pub role: Role,
    pub last_updated: Option<DateTime<Local>>,
}

// ================================================
// Implementations
// ================================================

impl FileInfoEntry {
    pub fn initialize(filename: String, is_local: bool, node_id: String) -> FileInfoEntry {
        FileInfoEntry {
            filename,
            is_local,

            node_id,
            last_updated: None,
        }
    }
}

impl NodeInfoEntry {
    pub fn initialize(ip: Ipv4Addr, port: u16, role: Role) -> NodeInfoEntry {
        NodeInfoEntry {
            node_id: conv_addr2id(&ip, port),
            ip: Some(ip),
            role,
            port,
            last_updated: None,
        }
    }
}
