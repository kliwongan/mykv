use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::RaftService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 2222));
    let listener = TcpListener::bind(&addr).await?;
    let node = RaftService::new(2222);

    println!("Listening on: http://{}", addr);

        // First, start the election timeout
        // await RPC queries on event queue
        // if no queries in event queue before election timeout ends, start election
        // else process the events in order ASAP
        
        loop { 
            let result = timeout(Duration::from_millis(self.timeout), rx).await;
            
            if let Err(_) = result {
                // become a candidate if no heartbeat detected from leader
                self.state = NodeState::Candidate;
                continue;
            } else {
                
            }
            
            match self.state {
                Err(err) => self.becomeLeader()

            };
        }

    loop {
        let (mut stream, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut buffer = [0; 1024];
            let _ = stream.read(&mut buffer).await;

            let contents = "<h1>Hello, world!</h1>";
            let content_length = contents.len();
            let response = format!("HTTP/1.1 200 OK\r\nContent-Length: {content_length}\r\n\r\n{contents}");
            let _ = stream.write_all(response.as_bytes()).await;
        });
    }
}
