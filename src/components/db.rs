use std::{
    convert::From,
    net::{IpAddr, SocketAddr, SocketAddrV4},
};

use crate::components::entity::node_roles::Role;
use chrono::{DateTime, Local};
use rusqlite::{params, Connection, Result};
use std::{net::Ipv4Addr, str::FromStr};

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

// ================================================
// Definitions for DB instance
// ================================================

pub trait InMemDB<'a, T> {
    fn create_db(&mut self, conn: &'a Connection) -> Result<()>;
}

pub struct FileInfoDB<'a> {
    db_name: &'static str,
    conn: &'a Connection,
}

pub struct NodeInfoDB<'a> {
    db_name: &'static str,
    conn: &'a Connection,
}

// ================================================
// Implementations
// ================================================

impl<'a> InMemDB<'a, FileInfoEntry> for FileInfoDB<'a> {
    fn create_db(&mut self, conn: &'a Connection) -> Result<(), rusqlite::Error> {
        log::info!("Creating db {}", self.db_name);

        match conn.execute(
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
                filename        TEXT    PRIMARY KEY
                ,is_local       BOOLEAN NOT NULL
                ,node           TEXT    NOT NULL
                ,last_updated   TEXT    NOT NULL
            );",
                &self.db_name
            )
            .as_str(),
            [],
        ) {
            Ok(_) => Ok(()),
            Err(err) => Err(err),
        }
    }
}

impl<'a> FileInfoDB<'a> {
    pub fn intialize(db_name: &'static str, conn: &'a Connection) -> Result<FileInfoDB<'a>, rusqlite::Error> {
        let mut db = FileInfoDB { db_name, conn };

        match db.create_db(conn) {
            Ok(_) => Ok(db),
            Err(err) => Err(err),
        }
    }

    pub fn upsert(&self, info: &FileInfoEntry) -> Result<()> {
        log::debug!("Upsert..");

        let current = Local::now();

        match self.conn.execute(
            format!(
                "INSERT INTO {}
                (filename, is_local, node, last_updated)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(filename) DO UPDATE SET
                    is_local = ?2,
                    node = ?3,
                    last_updated = ?4
                ;",
                self.db_name
            )
            .as_str(),
            params![info.filename, info.is_local, info.node_id, current.to_rfc3339(),],
        ) {
            Ok(size) => log::debug!("Upserted: {}", size),
            Err(err) => log::error!("{}", err),
        };

        Ok(())
    }

    pub fn get_file_info(&self, filename: &String) -> Result<Vec<FileInfoEntry>> {
        let mut stmt = self
            .conn
            .prepare(format!("SELECT * FROM {} WHERE filename = ?1;", self.db_name).as_str())?;
        let rows = stmt.query_map([&filename], |row| {
            log::debug!("inside: {:?}", row.get::<usize, String>(3));

            Ok(FileInfoEntry {
                filename: row.get(0)?,
                is_local: row.get::<usize, i32>(1)? == 1,
                node_id: row.get(3)?,
                last_updated: Some(row.get::<usize, String>(4)?.parse().unwrap()),
            })
        })?;

        rows.collect()
    }
}

impl<'a> InMemDB<'a, NodeInfoEntry> for NodeInfoDB<'a> {
    fn create_db(&mut self, conn: &'a Connection) -> Result<(), rusqlite::Error> {
        log::info!("Creating db {}", self.db_name);

        match conn.execute(
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
                node_id         TEXT    PRIMARY KEY
                ,ip             TEXT    NOT NULL
                ,port           INTEGER NOT NULL
                ,role           INTEGER NOT NULL
                ,last_updated   TEXT    NOT NULL
            );",
                &self.db_name
            )
            .as_str(),
            [],
        ) {
            Ok(_) => Ok(()),
            Err(err) => Err(err),
        }
    }
}

impl<'a> NodeInfoDB<'a> {
    pub fn intialize(db_name: &'static str, conn: &'a Connection) -> Result<NodeInfoDB<'a>, rusqlite::Error> {
        let mut db = NodeInfoDB { db_name, conn };

        match db.create_db(conn) {
            Ok(_) => Ok(db),
            Err(err) => Err(err),
        }
    }

    pub fn upsert(&self, ip: Ipv4Addr, port: u16, role: Role) -> Result<()> {
        log::debug!("Upsert..");

        let current = Local::now();

        match self.conn.execute(
            format!(
                "INSERT INTO {}
                (node_id, ip, port, role, last_updated)
                VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(node_id) DO UPDATE SET
                    ip = ?2,
                    port = ?3,
                    role = ?4,
                    last_updated = ?5
                ;",
                self.db_name
            )
            .as_str(),
            params![
                SocketAddrV4::new(ip, port).to_string(),
                ip.to_string(),
                port,
                u8::from(&role),
                current.to_rfc3339(),
            ],
        ) {
            Ok(size) => log::info!("Upserted: {}", size),
            Err(err) => log::error!("{}", err),
        };

        Ok(())
    }

    pub fn get_node_info(&self, ip: Ipv4Addr, port: u16) -> Result<Vec<NodeInfoEntry>> {
        let node_id = conv_addr2id(&ip, port);

        let mut stmt = self
            .conn
            .prepare(format!("SELECT * FROM {} WHERE node_id = ?1;", self.db_name).as_str())?;
        let rows = stmt.query_map([&node_id], |row| {
            log::debug!("inside: {:?}", row.get::<usize, String>(3));

            let ip_str: String = row.get(1)?;
            let ip = match Ipv4Addr::from_str(ip_str.as_str()) {
                Ok(ip) => Some(ip),
                Err(e) => {
                    log::error!("Cannot parse following to Ipv4: {}: {}", ip_str, e);
                    None
                }
            };

            Ok(NodeInfoEntry {
                node_id: row.get(0)?,
                ip: ip,
                port: row.get::<usize, u16>(2)?,
                role: Role::from(row.get::<usize, u8>(3)?),
                last_updated: Some(row.get::<usize, String>(4)?.parse().unwrap()),
            })
        })?;

        rows.collect()
    }

    pub fn get_data_nodes(&self) -> Result<Vec<NodeInfoEntry>> {
        let mut stmt = self
            .conn
            .prepare(format!("SELECT * FROM {} WHERE role = ?1;", self.db_name).as_str())?;
        let rows = stmt.query_map([&u8::from(&Role::Data)], |row| {
            let ip_str: String = row.get(1)?;
            let ip = match Ipv4Addr::from_str(ip_str.as_str()) {
                Ok(ip) => Some(ip),
                Err(e) => {
                    log::error!("Cannot parse following to Ipv4: {}: {}", ip_str, e);
                    None
                }
            };

            Ok(NodeInfoEntry {
                node_id: row.get(0)?,
                ip: ip,
                port: row.get::<usize, u16>(2)?,
                role: Role::from(row.get::<usize, u8>(3)?),
                last_updated: Some(row.get::<usize, String>(4)?.parse().unwrap()),
            })
        })?;

        rows.collect()
    }
}

/// Convert address (including ip (in IpV4 format) and port) to node id which is later stored in in-memory DB
pub fn conv_addr2id(ip: &Ipv4Addr, port: u16) -> String {
    SocketAddrV4::new(ip.clone(), port).to_string()
}

pub fn conv_id2addr(node_id: String) -> Result<SocketAddr, ()> {
    let idx_colon = match node_id.find(':') {
        Some(idx) => idx,
        None => {
            log::debug!("Error as parsing node_id in string to IP and port: No colon found");
            return Err(());
        }
    };
    let ip = match Ipv4Addr::from_str(&node_id[0..idx_colon]) {
        Ok(ip) => ip,
        Err(err) => {
            log::debug!(
                "Error as parsing node_id in string to IP and port: err as parsing IP: {}",
                err
            );
            return Err(());
        }
    };
    let port = match u16::from_str(&node_id[idx_colon + 1..]) {
        Ok(port) => port,
        Err(err) => {
            log::debug!(
                "Error as parsing node_id in string to IP and port: Err as parsing port: {}",
                err
            );
            return Err(());
        }
    };

    Ok(SocketAddr::new(IpAddr::V4(ip), port))
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
