mod raft;
use raft::RaftService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut node = RaftService::new(2222);
    tokio::spawn(async move {
        node.run().await;
    });
    Ok(())
}
