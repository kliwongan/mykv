use std::net::{SocketAddr, ToSocketAddrs};
use tokio::sync::mpsc;
use tokio::try_join;
use tracing::info;

use crate::message::Message;
use crate::raft::Raft;
use crate::raft_server::RaftServer;
use crate::storage::Storage;

pub struct KVService<S: Storage + 'static + Send> {
    store: S,
    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
    addr: SocketAddr,
    id: u64,
    network: Vec<u64>,
}

impl<S: Storage + 'static + Send> KVService<S> {
    // TODO: Create store/addr here and not accept it as an argument
    pub fn new(id: u64, store: S) -> Self {
        let addr = format!("[::1]:{}", id);
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        let (tx, rx) = mpsc::channel::<Message>(100);
        Self {
            store,
            tx,
            rx,
            addr,
            id,
            network: Vec::new(),
        }
    }

    pub async fn run(self) {
        // To be run ONLY WHEN all setup is completed
        let addr = self.addr;
        let mut raft = Raft::new(self.id, self.store, self.rx, self.tx.clone());
        let server = RaftServer::new(self.tx, addr);

        let server_handle = tokio::spawn(server.run());

        for id in self.network {
            let addr = format!("[::1]:{}", id);
            raft.add_node(addr, id, id == self.id).await;
        }
        let node_handle = tokio::spawn(raft.run());

        let _ = try_join!(server_handle, node_handle);
    }

    pub fn add_network(&mut self, id: u64) {
        self.network.push(id);
    }
}
