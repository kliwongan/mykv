// The actual Raft
use crate::raft_node::{RaftNode};
use crate::raft_rpc::raftrpc::RaftMessage;
use crate::storage::Storage;
use crate::raft_server::raftrpc::raft_service_client::RaftServiceClient;

use core::panic;
use std::net::{SocketAddr, ToSocketAddrs};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, mpsc};
use tokio::time::timeout;
use tonic::transport::channel::Channel;
use tracing::warn;

const HEARTBEAT_DURATION: u64 = 100;

pub struct RaftServiceNode {
    addr: SocketAddr,
    client: RaftServiceClient<Channel>,
}

impl RaftServiceNode {
    pub async fn new<T: ToSocketAddrs>(addr: T) -> Self {
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        let client = RaftServiceClient::connect(format!("http://{}", addr)).await;
        if let Ok(client) = client {
            return RaftServiceNode { addr, client };
        } else {
            panic!("Failed to connect to peer at {}", format!("http://{}", addr));
        }
    }
}

pub struct Raft<T: Storage> {
    node: RaftNode<T>,
    pub network: HashMap<u64, RaftServiceNode>,
    pub rx: mpsc::Receiver<RaftMessage>,
    pub tx: mpsc::Sender<RaftMessage>,
    store: T,
}

impl<T: Storage> Raft<T> {
    pub async fn run(mut self) {
        let mut heartbeat = Duration::from_millis(HEARTBEAT_DURATION);
        let mut now = Instant::now();

        let mut clients = HashMap::new();
        let mut quit = false;
        loop {
            if quit {
                warn!("Quitting the Raft");
                break;
            }

            // Placeholders for now
            match timeout(heartbeat, self.rx.recv()).await {
                Ok(_) => {

                }
                Err(_) => {

                }
            }

            let elapsed = now.elapsed();
            now = Instant::now();
            if elapsed > heartbeat {
                heartbeat = Duration::from_millis(HEARTBEAT_DURATION);
                self.node.tick();
            } else {
                heartbeat -= elapsed;
            }

            self.on_ready(&mut clients).await;
        }
    }

    pub async fn on_ready(&mut self, clients: &mut HashMap<u64, oneshot::Sender<RaftMessage>>) {
        // TODO:
        
        //let mut ready = self.node.ready();

        if !ready.messages().is_empty() {
            self.send_messages
        }
    }
}