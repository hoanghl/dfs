mod components;

use components::{
    args::Args,
    configs::Configs,
    entity::{
        client::Client, data::Data, dns::DNS, master::Master, node_roles::Role,
        nodes::Node,
    },
};
use std::{env, net::Ipv4Addr};

fn main() {
    // ================================================
    // Intialize configs
    // ================================================
    // if env::var("RUST_LOG").is_err() {
    // }
    env::set_var("RUST_LOG", "debug");
    env_logger::init();

    let ip_dns = Ipv4Addr::new(192, 168, 0, 133);
    let port_dns: u16 = 7889;

    let args = Args::initialize();

    let port = match args.role {
        Role::DNS => port_dns,
        _ => args.port,
    };

    let mut configs = Configs::initialize(ip_dns, port_dns, port);
    configs.role = args.role;
    configs.dir_data = args.dir_data;
    configs.action = args.action;
    configs.name = args.name;
    configs.path = args.path;

    // ================================================
    // Establish server
    // ================================================
    match configs.role {
        Role::Master => Master::new(&configs).start(&configs.addr_local),
        Role::Data => Data::new(&configs).start(&configs.addr_local),
        Role::DNS => DNS::new(&configs).start(&configs.addr_local),
        Role::Client => Client::new(&configs).start(&configs.addr_local),
        _ => panic!("Invalid role argument"),
    };
}
