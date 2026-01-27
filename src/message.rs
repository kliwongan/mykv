use crate::raft_rpc::raftrpc::RaftMessage;

use tokio::sync::mpsc::Sender;

pub enum Message {
    Propose {
        proposal: Vec<u8>,
        chan: Sender<RaftMessage>,
    },
    ConfigChange {
        // TODO: ConfChange
        change: u32,
        chan: Sender<RaftMessage>,
    },
    RequestId {
        addr: String,
        chan: Sender<RaftMessage>,
    },
    ReportUnreachable {
        node_id: u64,
    },
    Raft(Box<RaftMessage>),
}
