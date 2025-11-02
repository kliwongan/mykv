use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::timeout;

mod raft;
use raft::RaftService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 2222));
    let listener = TcpListener::bind(&addr).await?;
    let mut node = RaftService::new(2222);

    println!("Listening on: http://{}", addr);
    loop {
        let timeout_duration = Duration::from_millis(node.getTimeout());
        let result = timeout(timeout_duration, listener.accept()).await;
        match result {
            Err(_) => {
                // if timeout is reached, become a candidate
                node.becomeCandidate();
            }
            Ok(result) => {
                if let Err(_) = result {
                    // error with receiving the RPC
                } else {

                }
            }
        };

        // tokio::spawn(async move {
        //     let mut buffer = [0; 1024];
        //     let _ = stream.read(&mut buffer).await;

        //     let contents = "<h1>Hello, world!</h1>";
        //     let content_length = contents.len();
        //     let response = format!("HTTP/1.1 200 OK\r\nContent-Length: {content_length}\r\n\r\n{contents}");
        //     let _ = stream.write_all(response.as_bytes()).await;
        // });
    }
}
