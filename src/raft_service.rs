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
                        info!("Timeout is now {}", node_lock.get_timeout());
                        drop(node_lock);
                    }
                    Ok(result) => {
                        if let Err(_) = result {
                            error!("Error in receiving RPC");
                            continue;
                        } else {
                            drop(node_lock);
                            handle_request(Arc::clone(&self.node), result);
                        }
                    }
                };
            } else if state == NodeState::Candidate {
                info!("Node is a candidate");
                let timeout_duration = Duration::from_millis(node_lock.timeout);
                let mut check_majority = false;
                {
                    check_majority = node_lock.check_majority();
                }
                if !check_majority {
                    let now = SystemTime::now();
                    let mut set: JoinSet<Result<String, Error>> = JoinSet::new();
                    let network = node_lock.get_network().clone();
                    let data = node_lock.send_request_vote();

                    for node in network {
                        if node == self.id {
                            continue;
                        }

                        info!("Going through node {}", node);
                        let message = data.clone();
                        set.spawn(async move {
                            let message = message.clone();
                            let node_addr = SocketAddr::from(([127, 0, 0, 1], node as u16));
                            let mut stream: TcpStream;

                            loop {
                                let tcp = TcpStream::connect(&node_addr).await;
                                if let Ok(inner) = tcp {
                                    stream = inner;
                                    break;
                                } else {
                                    error!("Couldn't connect, trying again");
                                }
                            }
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
                                        break;
                                    }
                                }
                            }

                            Ok(String::from(""))
                        });
                    }
                    // at the end of the loop, if the node isn't voted majority yet, reset election timeout
                    // and repeat
                    //let vote_result = set.join_all().await;
                    info!("Determining vote result");
                    let vote_result = timeout(timeout_duration, set.join_all()).await;
                    // if (now.elapsed().unwrap() > timeout_duration) {
                    //     // increment term and start new election
                    //     node_lock.incrementTerm();
                    //     drop(node_lock);
                    //     continue;
                    // }
                    match vote_result {
                        Err(_) => {
                            info!("Timeout was reached, restarting vote");
                            node_lock.incrementTerm();
                            node_lock.reset_timeout();
                            info!("Timeout is now {}", node_lock.get_timeout());
                            drop(node_lock);
                            continue;
                        }
                        Ok(result) => {
                            info!("Parsing vote result");
                            for message in result {
                                if let Ok(msg) = message {
                                    node_lock.execute_from_message(msg.as_str());
                                }
                            }

                            if node_lock.check_majority() {
                                info!("Majority was reached");
                                node_lock.become_leader();
                            } else {
                                info!("Nothing much, resetting timeout and starting a new vote!");
                                node_lock.incrementTerm();
                                node_lock.reset_timeout();
                                info!("Timeout is now {}", node_lock.get_timeout());
                            }
                            drop(node_lock);
                        }
                    };
                } else {
                    // else, the node is voted to become leader,
                    info!("Became leader due to majority!");
                    node_lock.become_leader();
                    drop(node_lock);
                    continue;
                }
            } else {
                // send out heartbeat requests for now
                info!("Node is a leader!");
                let network = node_lock.get_network().clone();
                let mut set: JoinSet<()> = JoinSet::new();
                let data = node_lock.send_append_entries(true);
                info!("Dropping node lock");
                drop(node_lock);
                info!("Spawning heartbeat request tasks");
                for node in network {
                    if node == self.id {
                        continue;
                    }

                    let message = data.clone();
                    set.spawn(async move {
                        let message = message.clone();
                        let node_addr = SocketAddr::from(([127, 0, 0, 1], node as u16));
                        let mut stream: TcpStream;

                        loop {
                            let tcp = TcpStream::connect(&node_addr).await;
                            if let Ok(inner) = tcp {
                                stream = inner;
                                break;
                            } else {
                                error!("Couldn't connect, trying again");
                            }
                        }

                        info!("Sending heartbeat to {}", node);
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

fn handle_basic_http_request(result: Result<(TcpStream, SocketAddr), Error>) {
    // dummy function for testing purposes
    let (mut stream, mut address) = result.unwrap();
    tokio::spawn(async move {
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
) {
    let (mut stream, mut address) = result.unwrap();
    tokio::spawn(async move {
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
