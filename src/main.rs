mod raft_node;
mod raft_service;

use raft_node::RaftNode;
use raft_service::RaftService;

use std::sync::Arc;
use tokio::sync::Mutex;

use clap::{arg, Parser, command, value_parser, ArgAction, Command};

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(
        long,
        short,
        value_name = "NETWORK",
        help = "an array of nodes",
        num_args = 0..,
    )]
    network: Vec<u32>,
    #[arg(long, short)]
    id: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let raft_node = Arc::new(Mutex::new(RaftNode::new(args.id)));
    let mut service = RaftService::new(args.id, raft_node);
    println!("{:?}", args);
    for node in args.network {
        println!("Adding node {} to network", node);
        service.add_network(node).await;
    }

    println!("Running service");
    service.run().await;
    Ok(())
}
