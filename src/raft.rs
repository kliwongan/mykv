use rand::Rng;
use std::cmp::min;
use std::collections::{HashMap, HashSet};

use serde::{Serialize, Deserialize};

// Based on the recommended values from the Raft paper
const MIN_ELECTION_DURATION: u64 = 150;
const MAX_ELECTION_DURATION: u64 = 300;

#[derive(Clone, PartialEq)]
enum NodeState {
    Leader,
    Follower,
    Candidate,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: i32 },
    Get { key: String },
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct LogEntry {
    command: Command,
    term: u32,
}

#[derive(Clone)]
struct NetworkConfig {
    nodes: HashSet<u32>,
}

#[derive(Clone)]
pub struct RaftService {
    _id: u32,

    // Persistent state
    currentTerm: u32,
    votedFor: Option<u32>,
    log: Vec<LogEntry>,

    // Volatile state
    commitIndex: u32,
    lastApplied: u32,

    // Leader state (volatile) only
    nextIndex: u32,
    matchIndex: u32,

    // State of the current node
    state: NodeState,

    // Election timeouts
    timeout: u64,

    // Implementation based state
    voteState: HashSet<u32>,
    network: NetworkConfig,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct RequestVote {
    term: u32,
    candidateId: u32,
    lastLogIndex: u32,
    lastLogTerm: u32,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct RequestVoteReply {
    term: u32,
    voteGranted: bool,
    _id: u32,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct AppendEntries {
    term: u32,
    leaderId: u32,
    prevLogIndex: u32,
    prevLogTerm: u32,
    entries: Vec<LogEntry>,
    leaderCommit: u32,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct AppendEntriesReply {
    term: u32,
    success: bool,
    conflictingEntryTerm: Option<u32>,
    conflictingFirstIndex: Option<u32>,
    _id: u32,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct InstallSnapshot {
    // TODO
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct InstallSnapshotReply {
    // TODO
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum RaftMessage {
    AE(AppendEntries),
    RV(RequestVote),
    IS(InstallSnapshot),
    AE_R(AppendEntriesReply),
    RV_R(RequestVoteReply),
    IS_R(InstallSnapshotReply),
    Nil,
}

impl RaftService {
    pub fn new(id: u32) -> Self {
        let mut rng = rand::rng();
        RaftService {
            _id: id,
            currentTerm: 1,
            votedFor: None,
            log: Vec::<LogEntry>::new(),
            commitIndex: 0,
            lastApplied: 0,
            nextIndex: 1,
            matchIndex: 1,
            state: NodeState::Follower,
            timeout: rng.random_range(MIN_ELECTION_DURATION..MAX_ELECTION_DURATION),
            voteState: HashSet::new(),
            network: NetworkConfig { nodes: HashSet::new() },
        }
    }

    pub fn append_entries(&mut self, args: AppendEntries) -> AppendEntriesReply {
        // TODO: need mutex guard for modifying state
        let mut result = AppendEntriesReply {
            term: self.currentTerm,
            success: false,
            conflictingEntryTerm: None,
            conflictingFirstIndex: None,
            _id: self._id,
        };

        if self.currentTerm != args.term {
            if self.currentTerm < args.term {
                // we need to step down if we are a leader or candidate
                self.state = NodeState::Follower;
                self.currentTerm = args.term;
            }
            return result;
        }

        // Check for conflicting entries
        // We must always check this, even for heartbeat RPCs
        if let Some(entry) = self.log.get((args.prevLogIndex - 1) as usize) {
            // existing entry in the same index but conflicts with current term
            if entry.term != args.prevLogTerm {
                // find first index and entry term of conflicting entries

                // TODO
                result.conflictingEntryTerm = Some(1);
                result.conflictingFirstIndex = Some(1);

                // Delete this entry and all following entries
                self.log
                    .truncate((self.log.len() as u32 - args.prevLogIndex + 1) as usize);
            }
            result.success = true;
        } else {
            return result;
        }

        // append all new entries not already in the log
        for entry in args.entries {
            self.log.push(entry);
        }

        if args.leaderCommit > self.commitIndex {
            self.commitIndex = min(args.leaderCommit, self.log.len() as u32);
        }

        return result;
    }

    pub fn request_vote(&mut self, args: RequestVote) -> RequestVoteReply {
        let mut result = RequestVoteReply {
            term: self.currentTerm,
            voteGranted: false,
            _id: self._id,
        };

        if self.currentTerm > args.term {
            return result;
        }

        let last_entry = self.log.get((args.lastLogIndex - 1) as usize);
        if self.votedFor.is_none()
            && !last_entry.is_none()
            && last_entry.unwrap().term == args.lastLogTerm
        {
            result.voteGranted = true;
            self.votedFor = Some(args.candidateId);
        }

        return result;
    }

    pub fn request_vote_reply(&mut self, args: RequestVoteReply) {
        if args.voteGranted {
            self.voteState.insert(args._id);
        } else if args.term >= self.currentTerm {
            self.state = NodeState::Follower;
        }
    }

    pub fn check_majority(&mut self) -> bool {
        let inter = self.voteState.intersection(&self.network.nodes);
        let cnt = inter.count();
        return cnt >= (self.network.nodes.len()).div_ceil(2);
    }

    pub fn execute_from_message(&mut self, message: &str) -> String {
        let message = RaftService::parse_request(&message);
        let inter: RaftMessage = match message {
            RaftMessage::AE(append_entries) => RaftMessage::AE_R(self.append_entries(append_entries)),
            RaftMessage::RV(request_vote) => RaftMessage::RV_R(self.request_vote(request_vote)),
            RaftMessage::IS(install_snapshot) => RaftMessage::IS_R(InstallSnapshotReply{}),
            RaftMessage::AE_R(append_entries_reply) => RaftMessage::Nil,
            RaftMessage::RV_R(request_vote_reply) => RaftMessage::Nil,
            RaftMessage::IS_R(install_snapshot_reply) => RaftMessage::Nil,
            _ => RaftMessage::Nil,
        };
        return RaftService::parse_response(inter);
    }

    pub fn parse_request(message: &str) -> RaftMessage {
        let deserialized: RaftMessage = serde_json::from_str(&message).unwrap();
        return deserialized;
    }

    pub fn parse_response(message: RaftMessage) -> String {
        let serialized = serde_json::to_string(&message).unwrap();
        return serialized;
    }

    pub fn get_timeout(&self) -> u64 {
        return self.timeout;
    }

    pub fn reset_timeout(&mut self) {
        let mut rng = rand::rng();
        self.timeout = rng.random_range(MIN_ELECTION_DURATION..MAX_ELECTION_DURATION);
    }

    // NodeState related methods

    pub fn become_leader(&mut self) {
        self.state = NodeState::Leader;
    }

    pub fn become_follower(&mut self) {
        self.state = NodeState::Follower;
    }

    pub fn become_candidate(&mut self) {
        self.state = NodeState::Candidate;
    }

    pub fn setTerm(&mut self) {
        self.currentTerm += 1;
    }

    pub fn getTerm(&self) -> u32 {
        return self.currentTerm;
    }

    pub fn is_leader(&self) -> bool {
        return self.state == NodeState::Leader;
    }

    pub fn is_follower(&self) -> bool {
        return self.state == NodeState::Follower;
    }

    pub fn is_candidate(&self) -> bool {
        return self.state == NodeState::Candidate;
    }

    pub fn add_to_network(&mut self, node: u32) {
        self.network.nodes.insert(node);
    }
}
