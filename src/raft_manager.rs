use crate::raft_rpc::raftrpc::{Entry, HardState, MessageType, RaftMessage};
use crate::raft_node::{NodeState};

#[derive(Default, Debug, PartialEq)]
pub struct SoftState {
    pub leader_id: u64,
    pub raft_state: NodeState,
}

#[derive(Default, Debug, PartialEq)]
pub struct ReadState {
    
}

#[derive(Default, Debug, PartialEq)]
pub struct Ready {
    pub number: u64,
    pub soft_state: Option<SoftState>,
    pub hard_state: Option<HardState>,
    pub read_states: Vec<ReadState>,
    pub entries: Vec<Entry>,
    // TODO: Snapshot
    //snapshot: Snapshot,
    //is_persisted_msg: bool,
    //light: LightReady,
    pub must_sync: bool,
}