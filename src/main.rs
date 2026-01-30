mod error;
mod kv_service;
mod mem_storage;
mod message;
mod raft;
mod raft_manager;
mod raft_node;
mod raft_rpc;
mod raft_server;
mod storage;

use clap::{ArgAction, Command, Parser, arg, command, value_parser};

use crate::{kv_service::KVService, mem_storage::MemStorage};

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(
        long,
        short,
        value_name = "NETWORK",
        help = "an array of nodes",
        num_args = 0..,
    )]
    network: Vec<u64>,
    #[arg(long, short)]
    id: u64,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    // TODO actual storage struct here
    let store = MemStorage {};
    let mut kv_service = KVService::new(args.id, store);
    println!("{:?}", args);
    for node in args.network {
        println!("Adding node {} to network", node);
        kv_service.add_network(node);
    }

    println!("Running service");
    kv_service.run().await;
}
