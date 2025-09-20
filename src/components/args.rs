use clap::Parser;
use std::env;

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

impl Args {
    pub fn initialize() -> Args {
        Args::parse()
    }
}
