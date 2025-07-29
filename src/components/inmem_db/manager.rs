use super::{entries::*, errors::*, types::*, utils::*};
use crate::{components::entity::node_roles::Role, Configs};
use rusqlite::{params, Connection, Result};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

const NAME_DB_NODE: &'static str = "node_info";
const NAME_DB_FILE: &'static str = "file_info";

pub struct DBManager<'dbmngr> {
    conf: &'dbmngr Configs,

    conn: Connection,
    pub db_node: Option<NodeInfoDB>,
    pub db_file: Option<FileInfoDB>,
}

impl<'dbmngr> DBManager<'dbmngr> {
    pub fn new(conf: &'dbmngr Configs) -> Result<DBManager<'dbmngr>, DBManagerCreationError> {
        let conn = match Connection::open_in_memory() {
            Ok(conn) => conn,
            Err(err) => {
                log::error!("Error as initializing in-memory DB: {}", err);
                return Err(DBManagerCreationError {
                    error_code: DBManagerCreationErrorCode::Default,
                });
            }
        };

        Ok(DBManager {
            conf,
            conn,
            db_node: None,
            db_file: None,
        })
    }

    pub fn initialize_db(&mut self, role: Role) -> Result<(), DBManagerCreationError> {
        self.db_file = match FileInfoDB::intialize(NAME_DB_FILE, &self.conn) {
            Ok(db) => Some(db),
            Err(err) => {
                log::error!("Error as initializing in-memory DB: {}", err);
                return Err(DBManagerCreationError {
                    error_code: DBManagerCreationErrorCode::Default,
                });
            }
        };

        // Add itself to the list of nodes if Master
        self.db_node = match role {
            Role::Master => {
                let db_node = match NodeInfoDB::intialize(NAME_DB_NODE, &self.conn) {
                    Ok(db) => db,
                    Err(err) => {
                        log::error!("Error as initializing in-memory DB: {}", err);
                        return Err(DBManagerCreationError {
                            error_code: DBManagerCreationErrorCode::Default,
                        });
                    }
                };

                let addr_current = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), self.conf.args.port));
                match addr_current {
                    SocketAddr::V4(addr) => {
                        if let Err(err) =
                            db_node.upsert(&self.conn, addr.ip().clone(), addr_current.port(), Role::Master)
                        {
                            log::error!("Error as UPSERT: {}", err);

                            return Err(DBManagerCreationError {
                                error_code: DBManagerCreationErrorCode::Default,
                            });
                        }
                    }
                    _ => {
                        log::error!(
                        "Error as inserting socket address of itself to in-memory DB: socket address not in IpV4: {}",
                        addr_current
                    );
                        return Err(DBManagerCreationError {
                            error_code: DBManagerCreationErrorCode::Default,
                        });
                    }
                }
                Some(db_node)
            }
            _ => None,
        };

        Ok(())
    }

    /// Get nodes' info for Replication process
    pub fn get_nodes_replication(&mut self, n: i32) -> Result<Vec<SocketAddr>> {
        let mut stmt = match self.conn.prepare(
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
                name_node_db = NAME_DB_NODE,
                name_file_db = NAME_DB_FILE,
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

    pub fn upsert_node(&mut self, ip: Ipv4Addr, port: u16, role: Role) -> Result<(), rusqlite::Error> {
        self.db_node.as_mut().unwrap().upsert(&self.conn, ip, port, role)
    }

    pub fn upsert_file(&mut self, info: FileInfoEntry) -> Result<()> {
        self.db_file.as_mut().unwrap().upsert(&self.conn, info)
    }

    pub fn get_data_nodes(&mut self) -> Result<Vec<NodeInfoEntry>> {
        self.db_node.as_mut().unwrap().get_data_nodes(&self.conn)
    }
}
