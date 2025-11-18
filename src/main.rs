mod raft_node;
mod raft_service;
use raft_service::RaftService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut node = RaftService::new(2222);
    tokio::spawn(async move {
        node.run().await;
    });
    Ok(())
}
