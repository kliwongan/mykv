use std::net::{SocketAddr, ToSocketAddrs};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::message::Message;
use crate::raft::Raft;
use crate::raft_server::RaftServer;
use crate::storage::Storage;

pub struct KVService<S: Storage + 'static> {
    store: S,
    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
    addr: SocketAddr,
}

impl<S: Storage + 'static> KVService<S> {
    pub fn new<T: ToSocketAddrs>(addr: T, store: S) -> Self {
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        let (tx, rx) = mpsc::channel::<Message>(100);
        Self {
            store,
            tx,
            rx,
            addr,
        }
    }

    pub fn run(self) {
        let addr = self.addr.clone();
        let node = Raft::new();
    }
}
