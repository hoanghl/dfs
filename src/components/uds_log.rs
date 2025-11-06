use env_logger::Builder;
use log::{self, LevelFilter};

use std::{
    io::Write,
    os::{
        android::net::SocketAddrExt,
        unix::net::{SocketAddr, UnixStream},
    },
    sync::{Arc, Mutex},
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

    let stream = match UnixStream::connect_addr(&addr) {
        Ok(stream) => Arc::new(Mutex::new(stream)),
        Err(err) => {
            log::error!("RUST: {err}");
            return;
        }
    };

    // 2. Set up log
    let mut builder = Builder::from_default_env();
    builder
        .format(move |_buf, record| {
            let stream = Arc::clone(&stream);

            let msg = get_msg(record.level(), format!("{}", record.args()));

            let _ = stream.lock().unwrap().write_all(msg.as_bytes());

            Ok(())
        })
        .filter_level(LevelFilter::Debug)
        .init();
}
