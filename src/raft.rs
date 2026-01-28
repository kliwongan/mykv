use crate::message::Message;
use crate::raft_manager::RaftManager;
use crate::raft_node::RaftConfig;
use crate::raft_rpc::raftrpc::RaftMessage;
use crate::raft_rpc::raftrpc::raft_service_client::RaftServiceClient;
use crate::storage::Storage;

use core::panic;
use prost::Message as PMessage;
use std::collections::HashMap;
use std::error::Error;
use std::net::{SocketAddr, ToSocketAddrs};
use std::ops::{Deref, DerefMut};
use std::time::{Duration, Instant};
use tokio::spawn;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tonic::Request;
use tonic::transport::channel::Channel;
use tracing::{info, warn};

const HEARTBEAT_DURATION: u64 = 100;
const MAX_RETRIES: usize = 5;
const MSG_RETRY_TIMEOUT: u64 = 100;

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
            panic!(
                "Failed to connect to peer at {}",
                format!("http://{}", addr)
            );
        }
    }
}

impl Deref for RaftServiceNode {
    type Target = RaftServiceClient<Channel>;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for RaftServiceNode {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

pub struct MessageSender {
    message: RaftMessage,
    client: RaftServiceClient<tonic::transport::channel::Channel>,
    client_id: u64,
    chan: mpsc::Sender<Message>,
}

impl MessageSender {
    async fn send_message(mut self) {
        let mut retries = 0usize;
        loop {
            let msg_request = Request::new(self.message.clone());
            match self.client.send_message(msg_request).await {
                Ok(_) => {
                    return;
                }
                Err(e) => {
                    if retries < MAX_RETRIES {
                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(MSG_RETRY_TIMEOUT)).await;
                    } else {
                        // send unreachable message back to the raft manager
                    }
                }
            }
        }
    }
}

pub struct Raft<T: Storage + 'static> {
    node: RaftManager<T>,
    pub network: HashMap<u64, RaftServiceNode>,
    pub rx: mpsc::Receiver<Message>,
    pub tx: mpsc::Sender<Message>,
    store: T,
}

impl<T: Storage> Raft<T> {
    pub fn new(store: T, rx: mpsc::Receiver<Message>, tx: mpsc::Sender<Message>) -> Self {
        let config = RaftConfig {
            id: 1,
            election_tick: 10,
            // Heartbeat tick is for how long the leader needs to send
            // a heartbeat to keep alive.
            heartbeat_tick: 3,
            // Just for log
            ..Default::default()
        };
        let node = RaftManager::new(config, store);
        let network = HashMap::<u64, RaftServiceNode>::new();
        Self {
            node,
            network,
            rx,
            tx,
            store,
        }
    }

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
                Ok(Some(Message::Raft(m))) => {
                    if let Ok(_a) = self.step(*m) {};
                }
                Err(_) => {}
                Ok(_) => {}
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

    pub fn step(&mut self, m: RaftMessage) -> Result<(), Box<dyn Error>> {
        self.node.step(m)
    }

    pub async fn on_ready(&mut self, clients: &mut HashMap<u64, oneshot::Sender<RaftMessage>>) {
        if !self.node.has_ready() {
            return;
        }

        let mut ready = self.node.ready();

        if !ready.messages.is_empty() {
            self.send_messages(ready.messages);
        }

        // TODO: Snapshot

        // TODO: if entries is not empty, append it to the store

        // todo: if hardstate changed, persist it to the store

        // send out persisted messages

        // send out light rd messages

        // handle committed entries and then advance apply
    }

    pub async fn add_node<S: ToSocketAddrs>(&mut self, addr: S, id: u64) {
        let new_node = RaftServiceNode::new(addr).await;
        self.network.insert(id, new_node);
    }

    pub fn get_node_mut(&mut self, id: &u64) -> Option<&mut RaftServiceNode> {
        self.network.get_mut(&id)
    }

    async fn send_messages(&mut self, msgs: Vec<RaftMessage>) {
        for msg in msgs {
            info!("Message ready to go");
            // Get peer that I want to send to
            if let Some(node) = self.get_node_mut(&msg.to) {
                // Send the message
                let message_sender = MessageSender {
                    client_id: msg.to,
                    client: node.clone(),
                    chan: self.tx.clone(),
                    message: msg,
                };
                tokio::spawn(message_sender.send_message());
            }
        }
    }
}
