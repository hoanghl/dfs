use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4},
    str::FromStr,
};

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
