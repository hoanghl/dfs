use components::{
    configs::Configs,
    entity::{client::Client, data::Data, dns::DNS, master::Master, node_roles::Role, nodes::Node},
};

mod components;

fn main() {
    // ================================================
    // Intialize configs
    // ================================================
    let configs = Configs::initialize();

    // ================================================
    // Establish server
    // ================================================
    match configs.args.role {
        Role::Master => {
            Master::new(&configs).start(configs.args.port);
        }
        Role::Data => {
            Data::new(&configs).start(configs.args.port);
        }
        Role::DNS => {
            DNS::new(&configs).start(configs.port_dns);
        }
        Role::Client => {
            Client::new(&configs).start(configs.args.port);
        }
        _ => panic!("Invalid role argument"),
    };
}
