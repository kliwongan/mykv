use crate::mem_storage::MemStorage;
use crate::message::Message;
use crate::raft_manager::RaftManager;
use crate::raft_node::RaftConfig;
use crate::raft_rpc::raftrpc::RaftMessage;
use crate::raft_rpc::raftrpc::raft_service_client::RaftServiceClient;
use crate::storage::Storage;

use core::panic;
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
use tracing::{error, info, warn};

const HEARTBEAT_DURATION: u64 = 100;
const MAX_RETRIES: usize = 5;
const MSG_RETRY_TIMEOUT: u64 = 25;
const MAX_RAFT_SERVICE_RETRIES: usize = 5;
const RAFT_SERVICE_TIMEOUT: u64 = 1000;

#[derive(Debug)]
pub struct RaftServiceNode {
    addr: SocketAddr,
    client: RaftServiceClient<Channel>,
}

impl RaftServiceNode {
    pub async fn new<T: ToSocketAddrs>(addr: T) -> Self {
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        let mut retries = 0;
        loop {
            let client = RaftServiceClient::connect(format!("http://{}", addr)).await;
            if let Ok(client) = client {
                return RaftServiceNode { addr, client };
            } else {
                if retries > MAX_RAFT_SERVICE_RETRIES {
                    break;
                }
                retries += 1;
                tokio::time::sleep(Duration::from_millis(RAFT_SERVICE_TIMEOUT)).await;
            }
        }
        // TODO: Instead of panicking do something productive
        panic!(
            "Failed to connect to peer at {}",
            format!("http://{}", addr)
        );
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
            info!("Attempting to send message");
            let msg_request = Request::new(self.message.clone());
            match self.client.send_message(msg_request).await {
                Ok(_) => {
                    info!("Message sent to {} successfully!", self.message.to);
                    return;
                }
                Err(e) => {
                    if retries < MAX_RETRIES {
                        retries += 1;
                        info!(
                            "Retrying to send the message to {} for the {}-th time",
                            self.message.to, &retries
                        );
                        tokio::time::sleep(Duration::from_millis(MSG_RETRY_TIMEOUT)).await;
                    } else {
                        break;
                        // send unreachable message back to the raft manager
                    }
                }
            }
        }
    }
}

pub struct Raft<T: Storage + 'static + Send> {
    node: RaftManager<MemStorage>,
    pub network: HashMap<u64, RaftServiceNode>,
    pub rx: mpsc::Receiver<Message>,
    pub tx: mpsc::Sender<Message>,
    // TODO: do we need a secondary store?
    store: T,
}

impl<T: Storage + 'static + Send> Raft<T> {
    pub fn new(id: u64, store: T, rx: mpsc::Receiver<Message>, tx: mpsc::Sender<Message>) -> Self {
        let config = RaftConfig {
            id,
            election_tick: 10,
            heartbeat_tick: 3,
            ..Default::default()
        };
        let mem_storage = MemStorage {};
        let mut node = RaftManager::new(config, mem_storage);
        let network = HashMap::<u64, RaftServiceNode>::new();
        //node.become_leader();
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
        let quit = false;
        loop {
            if quit {
                warn!("Quitting the Raft");
                break;
            }

            // Placeholders for now
            match timeout(heartbeat, self.rx.recv()).await {
                Ok(Some(Message::Raft(m))) => {
                    info!("Received a RaftMessage from the server, stepping");
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
                //info!("Ticking the Raft");
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
        //info!("Checking readiness");
        if !self.node.has_ready() {
            return;
        }

        let ready = self.node.ready();

        if !ready.messages.is_empty() {
            info!("Sending messages from the ready");
            self.send_messages(ready.messages).await;
        }

        // TODO: Snapshot

        // TODO: if entries is not empty, append it to the store

        // todo: if hardstate changed, persist it to the store

        // send out persisted messages

        // send out light rd messages

        // handle committed entries and then advance apply
    }

    pub async fn add_node<S: ToSocketAddrs>(&mut self, addr: S, id: u64, exclude_peer: bool) {
        // Do not add self to peer
        if !exclude_peer {
            let new_node = RaftServiceNode::new(addr).await;
            self.network.insert(id, new_node);
        }
        self.node.add_network(id);
    }

    pub fn get_node_mut(&mut self, id: &u64) -> Option<&mut RaftServiceNode> {
        self.network.get_mut(id)
    }

    async fn send_messages(&mut self, msgs: Vec<RaftMessage>) {
        for msg in msgs {
            info!("Message ready to send to {}", msg.to);
            // Get peer that I want to send to
            if let Some(node) = self.get_node_mut(&msg.to) {
                // Send the message
                let message_sender = MessageSender {
                    client_id: msg.to,
                    client: node.clone(),
                    chan: self.tx.clone(),
                    message: msg,
                };
                spawn(message_sender.send_message());
            } else {
                error!(
                    "Error when trying to get node {} from network, current network is {:?}",
                    &msg.to, &self.network
                );
            }
        }
    }
}
