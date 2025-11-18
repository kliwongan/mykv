use std::io::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::Mutex;
use tokio::time::timeout;

use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

use crate::raft_node::RaftNode;

pub struct RaftService {
    node: Arc<Mutex<RaftNode>>,
    id: u32,
}

impl RaftService {
    pub fn new(id: u32) -> RaftService {
        RaftService {
            node: Arc::new(Mutex::new(RaftNode::new(id))),
            id: id,
        }
    }

    pub async fn run(&mut self) {
        let addr = SocketAddr::from(([127, 0, 0, 1], self.id as u16));
        let listener = TcpListener::bind(&addr).await.unwrap();

        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::TRACE)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");

        info!("Listening on: http://{}", addr);
        loop {
            info!("At loop start!");
            let cur_node = Arc::clone(&self.node);
            let mut node_lock = cur_node.lock().await;
            if node_lock.is_follower() {
                info!("Node is a follower");
                let timeout_duration = Duration::from_millis(node_lock.get_timeout());
                let result = timeout(timeout_duration, listener.accept()).await;
                match result {
                    Err(_) => {
                        info!("Becoming a candidate because timeout was reached");
                        node_lock.become_candidate();
                        node_lock.reset_timeout();
                        drop(node_lock);
                        continue;
                    }
                    Ok(result) => {
                        if let Err(_) = result {
                            error!("Error in receiving RPC");
                            continue;
                        } else {
                            let rt = Runtime::new().unwrap();
                            let handle = rt.handle();
                            drop(node_lock);
                            handle_request(cur_node, result, handle.clone());
                        }
                    }
                };
            } else if node_lock.is_candidate() {
                info!("Node is a candidate");
                let timeout_duration = Duration::from_millis(node_lock.get_timeout());
                // let result = timeout(timeout_duration, listener.accept()).await;
                while !node_lock.check_majority() {
                    // send out RequestVote requests to nodes
                    
                    // for each response back, respond accordingly

                    // at the end of the loop, if the node isn't voted majority yet, reset election timeout
                    // and repeat
                    //RaftService::parse_response(node_lock.send_request_vote());
                }

                // else, the node is voted to become leader,
                node_lock.become_leader();
            } else {
                // node is leader

                // await client requests

                // if client requests to see something, return the connection within its log
                // else,
            }
        }
    }
}

fn handle_basic_http_request(result: Result<(TcpStream, SocketAddr), Error>, handle: Handle) {
    // dummy function for testing purposes
    let (mut stream, mut address) = result.unwrap();
    handle.spawn(async move {
        info!("Serving request!");
        let mut buffer = [0; 1024];
        let _ = stream.read(&mut buffer).await;

        let contents = "<h1>Hello, world!</h1>";
        let content_length = contents.len();
        let response =
            format!("HTTP/1.1 200 OK\r\nContent-Length: {content_length}\r\n\r\n{contents}");
        let _ = stream.write_all(response.as_bytes()).await;
    });
}

async fn handle_request(
    node: Arc<Mutex<RaftNode>>,
    result: Result<(TcpStream, SocketAddr), Error>,
    handle: Handle,
) {
    let (mut stream, mut address) = result.unwrap();
    handle.spawn(async move {
        let cur_node = Arc::clone(&node);
        let mut node_lock = cur_node.lock().await;
        info!("Deserializing request!");
        let mut buffer = [0; 1024];
        let _ = stream.read(&mut buffer).await;

        let message = String::from_utf8(buffer.to_vec()).unwrap();
        info!("{}", format!("{}: {}", "Message", &message));
        let response = node_lock.execute_from_message(&message);

        // now write the response back
        let _ = stream.write_all(response.as_bytes()).await;
    });
}
