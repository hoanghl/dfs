use super::{
    entries::{FileInfoEntry, NodeInfoEntry},
    utils,
};
use crate::components::entity::node_roles::Role;
use chrono::Local;
use rusqlite::{params, Connection, Result};
use std::{
    convert::From,
    net::{Ipv4Addr, SocketAddrV4},
    str::FromStr,
};

// ================================================
// Definitions for DB instance
// ================================================

pub trait InMemDB<T> {
    fn create_db(&mut self, conn: &Connection) -> Result<()>;
}

pub struct FileInfoDB {
    db_name: &'static str,
}

pub struct NodeInfoDB {
    db_name: &'static str,
}

// ================================================
// Implementations
// ================================================

impl InMemDB<FileInfoEntry> for FileInfoDB {
    fn create_db(&mut self, conn: &Connection) -> Result<(), rusqlite::Error> {
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

impl FileInfoDB {
    pub fn intialize(db_name: &'static str, conn: &Connection) -> Result<FileInfoDB, rusqlite::Error> {
        let mut db = FileInfoDB { db_name };

        match db.create_db(conn) {
            Ok(_) => Ok(db),
            Err(err) => Err(err),
        }
    }

    pub fn upsert(&self, conn: &Connection, info: FileInfoEntry) -> Result<()> {
        log::debug!("Upsert..");

        let current = Local::now();

        match conn.execute(
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

    pub fn get_file_info(&self, conn: &Connection, filename: &String) -> Result<Vec<FileInfoEntry>> {
        let mut stmt = conn.prepare(format!("SELECT * FROM {} WHERE filename = ?1;", self.db_name).as_str())?;
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

impl InMemDB<NodeInfoEntry> for NodeInfoDB {
    fn create_db(&mut self, conn: &Connection) -> Result<(), rusqlite::Error> {
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

impl NodeInfoDB {
    pub fn intialize(db_name: &'static str, conn: &Connection) -> Result<NodeInfoDB, rusqlite::Error> {
        let mut db = NodeInfoDB { db_name };

        match db.create_db(conn) {
            Ok(_) => Ok(db),
            Err(err) => Err(err),
        }
    }

    pub fn upsert(&self, conn: &Connection, ip: Ipv4Addr, port: u16, role: Role) -> Result<()> {
        log::debug!("Upsert..");

        let current = Local::now();

        match conn.execute(
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

    pub fn get_node_info(&self, conn: &Connection, ip: Ipv4Addr, port: u16) -> Result<Vec<NodeInfoEntry>> {
        let node_id = utils::conv_addr2id(&ip, port);

        let mut stmt = conn.prepare(format!("SELECT * FROM {} WHERE node_id = ?1;", self.db_name).as_str())?;
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

    pub fn get_data_nodes(&self, conn: &Connection) -> Result<Vec<NodeInfoEntry>> {
        let mut stmt = conn.prepare(format!("SELECT * FROM {} WHERE role = ?1;", self.db_name).as_str())?;
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
