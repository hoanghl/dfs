use env_logger::Builder;
use log::{self, LevelFilter};

use std::{
    io::Write,
    os::{
        android::net::SocketAddrExt,
        unix::net::{SocketAddr, UnixStream},
    },
};

use chrono::Local;

pub fn get_msg(log_type: log::Level, content: String) -> String {
    let dt = Local::now();
    let mut padded = log_type.to_string();
    if padded.len() < 5 {
        padded = padded + " ";
    }

    return dt.format("%Y%m%d%H%M%S%3f").to_string()
        + &padded
        + &content
        + "\0";
}

/**
 * Initialize log
 */
pub fn initialize_log_stream(socket_path: &'static str) {
    // 1. Set up Local socket
    let addr = match SocketAddr::from_abstract_name(socket_path.as_bytes()) {
        Ok(addr) => addr,
        Err(err) => {
            log::error!("RUST: Cannot create socket: {}", err);
            return;
        }
    };

    // 2. Set up log
    let mut builder = Builder::from_default_env();
    if let Err(err) = builder
        .format(move |_buf, record| {
            if let Ok(mut stream) = UnixStream::connect_addr(&addr) {
                let msg = get_msg(record.level(), format!("{}", record.args()));

                let _ = stream.write_all(msg.as_bytes());
            };

            Ok(())
        })
        .filter_level(LevelFilter::Debug)
        .try_init()
    {
        log::error!("Cannot initialize logger: {}", err)
    }
}
