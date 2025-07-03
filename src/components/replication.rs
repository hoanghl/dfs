use log;
use rusqlite::Connection;
use std::collections::HashMap;

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4},
    process::exit,
    str::FromStr,
    sync::mpsc::{Receiver, Sender},
    time::{Duration, SystemTime},
};

use crate::components::{
    configs::Configs,
    db::{self, conv_addr2id, FileInfoDB, FileInfoEntry, NodeInfoDB},
    entity::{node_roles::Role, nodes::Node},
    errors::{NodeCreationError, NodeCreationErrorCode},
    file_utils::FileUtils,
    packets::{forward_packet, Action, Packet, PacketId},
};

use crate::components::db::NodeInfoEntry;

pub struct ReplicationTracker {
    pub tracker: HashMap<String, Vec<NodeInfoEntry>>,
}

impl ReplicationTracker {
    pub fn new() -> ReplicationTracker {
        ReplicationTracker {
            tracker: HashMap::<String, Vec<NodeInfoEntry>>::new(),
        }
    }

    pub fn add_track_entry(&mut self, filename: String, entry: NodeInfoEntry) {
        if let None = self.tracker.get::<String>(&filename) {
            self.tracker.insert(filename.clone(), Vec::<NodeInfoEntry>::new());
        }

        self.tracker.get_mut::<String>(&filename).as_mut().unwrap().push(entry);
    }

    pub fn remove_track(&mut self, filename: String) {
        self.tracker.remove::<String>(&filename);
    }
}

/// Get nodes' info for Replication process
pub fn get_nodes_replication(
    conn: &Connection,
    name_node_db: &'static str,
    name_file_db: &'static str,
    n: i32,
) -> Result<Vec<SocketAddr>> {
    let mut stmt = match conn.prepare(
        format!(
            "SELECT
                t1.node_id
            FROM {name_node_db} t1
            LEFT JOIN (
                SELECT
                    node as node_id
                    , COUNT(*) as count
                FROM {name_file_db}
                GROUP BY node
            ) t2 ON 1=1
                AND t2.node_id = t1.node_id
            ORDER BY count
            LIMIT ?1
            ;",
            name_node_db = name_node_db,
            name_file_db = name_file_db,
        )
        .as_str(),
    ) {
        Ok(stmt) => stmt,
        Err(err) => return Err(err),
    };

    let rows = stmt.query_map(params![n], |row| {
        log::debug!("get_nodes_replication: inside: {:?}", row.get::<usize, String>(0));
        let node_id = row.get::<usize, String>(0)?;
        match conv_id2addr(node_id) {
            Ok(addr) => Ok(addr),
            Err(_) => Err(rusqlite::Error::InvalidQuery),
        }
    })?;

    rows.collect()
}
