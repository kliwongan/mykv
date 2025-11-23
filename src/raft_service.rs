use std::io::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio::task::JoinSet;

use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

use crate::raft_node::{RaftNode, NodeState};

pub struct RaftService {
    node: Arc<Mutex<RaftNode>>,
    id: u32,
}

impl RaftService {
    pub fn new(id: u32, node: Arc<Mutex<RaftNode>>) -> RaftService {
        RaftService {
            node: node.clone(),
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
            let mut node_lock = self.node.lock().await;
            let mut state = NodeState::Follower;
            {
                state = node_lock.getState().clone();

            }
            if state == NodeState::Follower {
                info!("Node is a follower");
                let timeout_duration = Duration::from_millis(node_lock.get_timeout());
                let result = timeout(timeout_duration, listener.accept()).await;
                match result {
                    Err(_) => {
                        info!("Becoming a candidate because timeout was reached");
                        node_lock.become_candidate();
                        node_lock.reset_timeout();
                        drop(node_lock);
                    }
                    Ok(result) => {
                        if let Err(_) = result {
                            error!("Error in receiving RPC");
                            continue;
                        } else {
                            let rt = Runtime::new().unwrap();
                            let handle = rt.handle();
                            drop(node_lock);
                            handle_request(Arc::clone(&self.node), result, handle.clone());
                        }
                    }
                };
            } else if state == NodeState::Candidate {
                info!("Node is a candidate");
                let timeout_duration = Duration::from_millis(node_lock.timeout);
                let mut set: JoinSet<()> = JoinSet::new();
                // let result = timeout(timeout_duration, listener.accept()).await;
                if !node_lock.check_majority() {
                    let now = SystemTime::now();
                    let network = node_lock.get_network().clone();
                    drop(node_lock);
                    for node in network {
                        // start a tokio task that
                        // opens a TCP connection with that node
                        // awaits its response
                        // responds accordingly
                        let new_node = self.node.clone();
                        set.spawn(async move {
                            let mut new_node_clone = new_node.clone();
                            let node_addr = SocketAddr::from(([127, 0, 0, 1], node as u16));
                            let stream = TcpStream::connect(&addr).await.unwrap();
                            let new_node_lock = new_node_clone.lock().await;
                            let data = new_node_lock.send_request_vote();
                            drop(new_node_lock);

                            // wait on the same stream for a response back
                            

                        });
                        // worry about deadlocks?
                    }
                    // at the end of the loop, if the node isn't voted majority yet, reset election timeout
                    // and repeat
                    //RaftService::parse_response(node_lock.send_request_vote());
                }
                // else, the node is voted to become leader,
                self.node.lock().await.become_leader();
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
