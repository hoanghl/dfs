use std::{env, net::Ipv4Addr, process::exit, str::FromStr};

use clap::Parser;
use dotenv::dotenv;

use crate::components::{entity::node_roles::Role, packets::Action};

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    // ================================================
    // General arguments
    // ================================================

    // Role
    #[arg(short, long, value_parser = clap::value_parser!(Role))]
    pub role: Role,

    // Port of thread:receiver
    #[arg(short, long, default_value_t = 7888)]
    pub port: u16,

    // ================================================
    // Data/Master-specific arguments
    // ================================================
    #[arg(short, long, default_value = "./data")]
    pub dir_data: String,

    // ================================================
    // Client-specific arguments
    // ================================================

    // Action
    #[arg(long, value_parser = clap::value_parser!(Action))]
    pub action: Option<Action>,

    // File name
    #[arg(long)]
    pub name: Option<String>,

    // Path
    #[arg[long]]
    pub path: Option<String>,
}

pub struct Configs {
    pub ip_dns: Ipv4Addr,
    pub port_dns: u16,
    pub interval_heartbeat: u64,
    pub timeout_chan_wait: u64,

    pub args: Args,
}

impl Configs {
    pub fn initialize() -> Configs {
        // Read .env file and parse
        dotenv().ok();

        let ip_dns = match env::var("IP_DNS") {
            Ok(value) => Ipv4Addr::from_str(value.parse::<String>().unwrap().as_str())
                .expect("Cannot parse env 'IP_DNS' to correct IP address format"),
            Err(_) => panic!("env 'IP_DNS' not existed"),
        };
        let port_dns = match env::var("PORT_DNS") {
            Ok(value) => value.parse::<u16>().unwrap(),
            Err(_) => panic!("env 'PORT_DNS' not existed"),
        };
        let interval_heartbeat = match env::var("HEARTBEAT_INTERVAL_SECOND") {
            Ok(value) => value.parse::<u64>().unwrap(),
            Err(_) => panic!("env 'HEARTBEAT_INTERVAL_SECOND' not existed"),
        };
        let timeout_chan_wait = match env::var("TIMEOUT_CHANNEL_WAIT") {
            Ok(value) => value.parse::<u64>().unwrap(),
            Err(_) => 1,
        };

        // Set up logger
        if env::var("RUST_LOG").is_err() {
            env::set_var("RUST_LOG", "info");
        }
        env_logger::init();

        // Parse arguments
        let args = Args::parse();

        if let Role::Client = args.role {
            if args.action.is_none() {
                log::error!("Role: Client - Missing argument: 'action'");
                exit(1);
            }
            match args.name {
                None => {
                    log::error!("Role: Client - Missing argument: 'name'");
                    exit(1);
                }
                Some(_) => {
                    if args.path.is_none() {
                        log::error!("Role: Client - Missing argument: 'path'");
                        exit(1);
                    }
                }
            }
        }

        Configs {
            ip_dns: ip_dns,
            port_dns: port_dns,
            interval_heartbeat,
            timeout_chan_wait,
            args,
        }
    }
}
