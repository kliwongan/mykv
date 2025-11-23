mod raft_node;
mod raft_service;

use raft_node::RaftNode;
use raft_service::RaftService;

use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut id = 2222;
    let mut raft_node = Arc::new(Mutex::new(RaftNode::new(id)));
    let mut service = RaftService::new(id, raft_node);
    tokio::spawn(async move {
        service.run().await;
    });
    Ok(())
}
