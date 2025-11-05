use std::net::SocketAddr;
use std::time::Duration;
use std::io::Error;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Handle, Runtime};
use tokio::time::timeout;

use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

mod raft;
use raft::RaftService;

fn handle_request(node: &mut RaftService, result: Result<(TcpStream, SocketAddr), Error>, handle: Handle) {
    let (mut stream, mut address) = result.unwrap();
    handle.spawn(async move {
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
        if node.is_follower() {
            info!("Node is a follower");
            let timeout_duration = Duration::from_millis(node.get_timeout());
            let result = timeout(timeout_duration, listener.accept()).await;
            match result {
                Err(_) => {
                    info!("Becoming a candidate because timeout was reached");
                    //node.become_candidate();
                    node.reset_timeout();
                    continue;
                }
                Ok(result) => {
                    if let Err(_) = result {
                        error!("Error in receiving RPC");
                        // error with receiving the RPC
                        continue;
                    } else {
                        let rt = Runtime::new().unwrap();
                        let handle = rt.handle();
                        handle_request(&mut node, result, handle.clone());
                    }
                }
            };
        } else if node.is_candidate() {
            // start an election
            info!("Node is a candidate");
            // wait for votes

            // if majority is reached become leader
            // else if another node rejects and returns a greater term, revert to follower

        } else {
            // node is leader

            // await client requests

            // if client requests to see something, return the connection within its log
            // else, 
        }
    }
}
