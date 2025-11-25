use std::io::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::timeout;

use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

use crate::raft_node::{NodeState, RaftNode};

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

    pub async fn add_network(&mut self, id: u32) {
        let mut node_lock = self.node.lock().await;
        node_lock.add_to_network(id);
    }

    pub async fn run(&mut self) {
        let addr = SocketAddr::from(([127, 0, 0, 1], self.id as u16));
        info!("Trying to bind TCPListener");
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
                if let Some(leader) = node_lock.getLeader() {
                    info!("Node is a follower, following {}", leader);
                } else {
                    info!("Node is a follower, following no one yet");
                }
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
                if !node_lock.check_majority() {
                    let now = SystemTime::now();
                    let mut set: JoinSet<Result<String, Error>> = JoinSet::new();
                    let network = node_lock.get_network().clone();
                    let data = node_lock.send_request_vote();

                    for node in network {
                        if node == self.id {
                            continue;
                        }

                        let message = data.clone();
                        set.spawn(async move {
                            let message = message.clone();
                            let node_addr = SocketAddr::from(([127, 0, 0, 1], node as u16));
                            let mut stream = TcpStream::connect(&node_addr).await?;
                            
                            info!("Sending request vote to {}", node);
                            stream.write_all(message.as_bytes()).await.unwrap();

                            // wait on the same stream for a response back
                            loop {
                                // Wait for the socket to be readable
                                stream.readable().await.unwrap();

                                // Creating the buffer **after** the `await` prevents it from
                                // being stored in the async task.
                                let mut buf = [0; 4096];

                                // Try to read data, this may still fail with `WouldBlock`
                                // if the readiness event is a false positive.
                                match stream.try_read(&mut buf) {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        let response = String::from_utf8(buf.to_vec()).unwrap();
                                        info!("read {} bytes from the current node {} with response {}", n, node, response);
                                        // parse into RaftMessage and send it to 
                                        return Ok(response);

                                    }
                                    Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => {
                                        continue;
                                    }
                                    Err(e) => {
                                        error!("{} (Error for node {})", e, node);
                                    }
                                }
                            }

                            Ok(String::from(""))
                        });
                    }
                    // at the end of the loop, if the node isn't voted majority yet, reset election timeout
                    // and repeat
                    let vote_result = set.join_all().await;
                    if (now.elapsed().unwrap() > timeout_duration) {
                        // increment term and start new election
                        node_lock.incrementTerm();
                        drop(node_lock);
                        continue;
                    }

                    // parse vote result
                    for message in vote_result {
                        if let Ok(msg) = message {
                            node_lock.execute_from_message(msg.as_str());
                        }
                    }
                }
                // else, the node is voted to become leader,
                self.node.lock().await.become_leader();
            } else {
                // send out heartbeat requests for now
                let network = node_lock.get_network().clone();
                let mut set: JoinSet<()> = JoinSet::new();
                let data = node_lock.send_append_entries(true);
                drop(node_lock);
                for node in network {
                    if node == self.id {
                        continue;
                    }

                    let message = data.clone();
                    set.spawn(async move {
                        let message = message.clone();
                        let node_addr = SocketAddr::from(([127, 0, 0, 1], node as u16));
                        let mut stream = TcpStream::connect(&node_addr).await.unwrap();

                        stream.write_all(message.as_bytes()).await.unwrap();
                    });
                }

                set.join_all().await;
                // other than sending heartbeat requests
                // we also have to deal with http requests from clients
                // keeping track of appending entries to followers,
                // persisting to disk, and snapshotting our goods
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
