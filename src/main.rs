use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::timeout;

use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

mod raft;
use raft::RaftService;

async fn follower() {
    
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 2222));
    let listener = TcpListener::bind(&addr).await?;
    let mut node = RaftService::new(2222);

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    info!("Listening on: http://{}", addr);
    loop {
        info!("At loop start!");

        if node.isFollower() {
            let timeout_duration = Duration::from_millis(node.getTimeout());
            let result = timeout(timeout_duration, listener.accept()).await;
            match result {
                Err(_) => {
                    // if timeout is reached, become a candidate
                    info!("Becoming a candidate because timeout was reached");
                    node.becomeCandidate();
                    // continue loop or keep processing?
                }
                Ok(result) => {
                    if let Err(_) = result {
                        error!("Error in receiving RPC");
                        // error with receiving the RPC
                        continue;
                    } else {
                        let (mut stream, mut address) = result.unwrap();
                        tokio::spawn(async move {
                            info!("Serving request!");
                            let mut buffer = [0; 1024];
                            let _ = stream.read(&mut buffer).await;

                            let contents = "<h1>Hello, world!</h1>";
                            let content_length = contents.len();
                            let response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {content_length}\r\n\r\n{contents}"
                            );
                            let _ = stream.write_all(response.as_bytes()).await;
                        });
                    }
                }
            };
        } else if node.isCandidate() {
            // start an election
            
            // wait for votes

            // if majority is reached become leader


        } else {
            // node is leader

            // await client requests

            // if client requests to see something, return the connection within its log
            // else, 
        }
    }
}
