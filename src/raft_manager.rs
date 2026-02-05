use crate::raft_node::{NodeState, RaftConfig, RaftNode, SoftState};
use crate::raft_rpc::raftrpc::{Entry, HardState, RaftMessage};
use crate::storage::Storage;
use std::mem;

// BIG TODO: Change error typing!!!
use core::error::Error;

#[derive(Default, Debug, PartialEq)]
pub struct ReadState {}

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
    // These are in the LightReady struct in raft-rs
    // but I though this made things easier
    pub commit_index: Option<u64>,
    pub committed_entries: Vec<Entry>,
    pub messages: Vec<RaftMessage>,
}

// Wrapper around RaftNode
pub struct RaftManager<T: Storage> {
    pub raft: RaftNode<T>,
    // TODO: remove pub when able to ASAP
    prev_ss: SoftState,
    prev_hs: HardState,
    max_number: u64,
    // TODO: ReadyRecord
    commit_since_index: u64,
}

impl<T: Storage> RaftManager<T> {
    pub fn new(config: RaftConfig, store: T) -> Self {
        assert_ne!(config.id, 0, "config.id must not be zero");
        let r = RaftNode::new(&config, store);
        let mut rm = RaftManager {
            raft: r,
            prev_hs: Default::default(),
            prev_ss: Default::default(),
            max_number: 0,
            commit_since_index: config.last_applied,
        };

        rm.prev_hs = rm.raft.get_hardstate();
        rm.prev_ss = rm.raft.get_softstate();
        rm
    }

    pub fn tick(&mut self) -> bool {
        self.raft.tick()
    }

    pub fn step(&mut self, m: RaftMessage) -> Result<(), Box<dyn Error>> {
        self.raft.step(m)
    }

    pub fn ready(&mut self) -> Ready {
        let raft = &mut self.raft;

        self.max_number += 1;
        let mut rd = Ready {
            number: self.max_number,
            ..Default::default()
        };

        // TODO: ready record

        if self.prev_ss.raft_state != NodeState::Leader && raft.state != NodeState::Leader {
            // TODO: Understand this logic
            // for record in every record, assert that record last entry and snapshot are not equal to None
        }

        let ss = raft.get_softstate();
        if ss != self.prev_ss {
            rd.soft_state = Some(ss);
        }

        let hs = raft.get_hardstate();
        if hs != self.prev_hs {
            if hs.vote != self.prev_hs.vote || hs.term != self.prev_hs.term {
                // If the term has been updated or we voted for someoene else,
                // probably better to fsync the entries?
                rd.must_sync = true;
            }
            rd.hard_state = Some(hs);
        }

        // TODO read states check

        // TODO snapshots check

        // TODO grab from proper log when implemented
        rd.entries = raft.log.clone();

        if !raft.msgs.is_empty() {
            rd.messages = mem::take(&mut raft.msgs);
        }

        // TODO: committed entries
        rd
    }

    pub fn has_ready(&self) -> bool {
        // If there is any work to be done,
        // grab the ready state and get to work
        // i.e. react to messages

        let raft = &self.raft;
        if !raft.msgs.is_empty() {
            return true;
        }

        if raft.get_softstate() != self.prev_ss {
            return true;
        }

        if raft.get_hardstate() != self.prev_hs {
            return true;
        }

        // TODO: read states, unstable entries, snapshots,
        // more entries
        false
    }

    pub fn become_leader(&mut self) {
        self.raft.become_leader();
    }

    pub fn add_network(&mut self, id: u64) {
        self.raft.add_to_network(id);
    }
}
