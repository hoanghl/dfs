mod components;

use std::{env, net::Ipv4Addr};

// use android_logger::Config;
use jni::{
    objects::{JClass, JString},
    JNIEnv,
};
use log;
use serde::{Deserialize, Serialize};
use serde_json;

use crate::components::{
    configs::Configs,
    entity::{
        client::Client, data::Data, dns::DNS, master::Master, node_roles::Role,
        nodes::Node,
    },
};

#[cfg(target_os = "android")]
use crate::components::uds_log;

#[repr(C)]
#[derive(Serialize, Deserialize, Debug)]
pub struct ArgJNI {
    #[serde(rename = "ipDns")]
    ip_dns: Vec<u8>,

    #[serde(rename = "portDns")]
    port_dns: u16,

    #[serde(rename = "port")]
    port: u16,

    role: String,
}

#[unsafe(no_mangle)]

pub unsafe extern "C" fn Java_tommy_modules_dfs_DFSService_triggerDfs<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass,
    arg_str: JString,
) {
    // Set up logger
    // android_logger::init_once(
    //     Config::default()
    //         .with_tag("JNIRust")
    //         .with_max_level(log::LevelFilter::Debug),
    // );
    if cfg!(target_os = "android") {
        #[cfg(target_os = "android")]
        uds_log::initialize_log_stream("central.sock")
    } else {
        env_logger::init();
    }

    env::set_var("RUST_LOG", "debug");

    // Parse arguments from Kotlin
    let arg_str_conv = match env.get_string(&arg_str) {
        Ok(x) => String::from(x.to_str().expect("Cannot convert to String")),
        Err(err) => {
            log::error!("Err: {}", err);
            panic!();
        }
    };

    let args: ArgJNI = serde_json::from_str(&arg_str_conv).unwrap();

    // Convert parsed arguments to config
    let ip_dns = Ipv4Addr::new(
        args.ip_dns[0],
        args.ip_dns[1],
        args.ip_dns[2],
        args.ip_dns[3],
    );

    let mut configs = Configs::initialize(ip_dns, args.port_dns, args.port);
    configs.role = match args.role.parse::<Role>() {
        Ok(role) => role,
        Err(err) => {
            log::error!("Cannot parse 'role' argument: {}", err);
            panic!();
        }
    };

    // Start
    // let mut node = Data::new(&configs);
    // node.start(&configs.addr_local)

    match configs.role {
        Role::Master => {
            Master::new(&configs).start(&configs.addr_local);
        }
        Role::Data => {
            Data::new(&configs).start(&configs.addr_local);
        }
        Role::DNS => {
            DNS::new(&configs).start(&configs.addr_local);
        }
        Role::Client => {
            Client::new(&configs).start(&configs.addr_local);
        }
        _ => panic!("Invalid role argument"),
    };
}
