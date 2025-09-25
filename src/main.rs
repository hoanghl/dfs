use std::{env, net::Ipv4Addr};

use components::{
    configs::Configs,
    entity::{
        client::Client, data::Data, dns::DNS, master::Master, node_roles::Role,
        nodes::Node,
    },
};

use crate::components::args::Args;

mod components;

fn main() {
    // ================================================
    // Intialize configs
    // ================================================
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let ip_dns = Ipv4Addr::new(172, 20, 13, 0);
    let port_dns: u16 = 7889;

    let args = Args::initialize();

    let mut configs = Configs::initialize(ip_dns, port_dns);
    configs.role = args.role;
    configs.port = args.port;
    configs.dir_data = args.dir_data;
    configs.action = args.action;
    configs.name = args.name;
    configs.path = args.path;

    // ================================================
    // Establish server
    // ================================================
    match configs.role {
        Role::Master => {
            Master::new(&configs).start(configs.port);
        }
        Role::Data => {
            Data::new(&configs).start(configs.port);
        }
        Role::DNS => {
            DNS::new(&configs).start(configs.port_dns);
        }
        Role::Client => {
            Client::new(&configs).start(configs.port);
        }
        _ => panic!("Invalid role argument"),
    };
}
