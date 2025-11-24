use rand::Rng;
use std::cmp::min;
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

// Based on the recommended values from the Raft paper
const MIN_ELECTION_DURATION: u64 = 150;
const MAX_ELECTION_DURATION: u64 = 300;

#[derive(Clone, PartialEq)]
pub enum NodeState {
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
pub struct RaftNode {
    pub _id: u32,

    // Persistent state
    pub currentTerm: u32,
    pub votedFor: Option<u32>,
    pub log: Vec<LogEntry>,

    // Volatile state
    pub commitIndex: u32,
    pub lastApplied: u32,

    // Leader state (volatile) only
    pub nextIndex: HashMap<u32, u32>,
    pub matchIndex: HashMap<u32, u32>,

    // State of the current node
    pub state: NodeState,

    // Election timeouts
    pub timeout: u64,

    // Implementation based state
    pub voteState: HashSet<u32>,
    pub network: NetworkConfig,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct RequestVote {
    term: u32,
    candidateId: u32,
    lastLogIndex: u32,
    lastLogTerm: u32,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct RequestVoteReply {
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
    heartbeat: bool,
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

impl RaftNode {
    pub fn new(id: u32) -> Self {
        let mut rng = rand::rng();
        RaftNode {
            _id: id,
            currentTerm: 1,
            votedFor: None,
            log: Vec::<LogEntry>::new(),
            commitIndex: 0,
            lastApplied: 0,
            nextIndex: HashMap::new(),
            matchIndex: HashMap::new(),
            state: NodeState::Follower,
            timeout: rng.random_range(MIN_ELECTION_DURATION..MAX_ELECTION_DURATION),
            voteState: HashSet::new(),
            network: NetworkConfig {
                nodes: HashSet::new(),
            },
        }
    }

    pub fn send_request_vote(&self) -> String {
        let message = RaftMessage::RV(RequestVote {
            term: self.currentTerm,
            candidateId: self._id,
            lastLogIndex: self.log.len() as u32 - 1,
            lastLogTerm: if self.log.len() as u32 - 1 >= 0 {
                self.log.get(self.log.len() - 1).unwrap().term
            } else {
                self.currentTerm
            },
        });
        RaftNode::parse_response(message)
    }

    pub fn request_vote_receiver(&mut self, args: RequestVote) -> RequestVoteReply {
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

    pub fn request_vote_sender_response(&mut self, args: RequestVoteReply) {
        if args.voteGranted {
            self.voteState.insert(args._id);
        } else if args.term >= self.currentTerm {
            self.state = NodeState::Follower;
        }
    }

    pub fn send_append_entries(&self, heartbeat: bool) -> String {
        let message = RaftMessage::AE(AppendEntries {
            term: self.currentTerm,
            leaderId: self._id,
            prevLogIndex: self.log.len() as u32 - 1,
            prevLogTerm: if self.log.len() as u32 - 1 >= 0 {
                self.log.get(self.log.len() - 1).unwrap().term
            } else {
                self.currentTerm
            },
            entries: self.log.clone(),
            leaderCommit: self.commitIndex,
            heartbeat: heartbeat,
        });
        RaftNode::parse_response(message)
    }

    pub fn append_entries_sender_response(
        &mut self,
        args: AppendEntriesReply,
    ) -> Option<AppendEntries> {
        if !args.success {
            self.nextIndex
                .insert(args._id, self.nextIndex.get(&args._id).unwrap() - 1);
            let mut nextIndex = self.nextIndex.get(&args._id).unwrap().clone();

            // minor AppendEntries optimization
            let conflictingFirstIndex = args.conflictingFirstIndex;
            let conflictingEntryTerm = args.conflictingEntryTerm;

            if conflictingEntryTerm.is_some() && conflictingFirstIndex.is_some() {
                let newIndex = conflictingFirstIndex.unwrap();
                nextIndex = newIndex - 1;
            }
            let prevLogTerm = if self.log.len() as u32 - 1 >= 0 {
                self.log.get(nextIndex as usize).unwrap().term
            } else {
                self.currentTerm
            };

            return Some(AppendEntries {
                term: self.currentTerm,
                leaderId: self._id,
                prevLogIndex: nextIndex,
                prevLogTerm: prevLogTerm,
                entries: self.log.clone(),
                leaderCommit: self.commitIndex,
                heartbeat: false,
            });
        }
        // Use voteState to maintain nodes that have already replicated our entries
        self.voteState.insert(args._id);
        None
    }

    pub fn append_entries_receiver(&mut self, args: AppendEntries) -> AppendEntriesReply {
        let mut result = AppendEntriesReply {
            term: self.currentTerm,
            success: false,
            conflictingEntryTerm: None,
            conflictingFirstIndex: None,
            _id: self._id,
        };

        if args.heartbeat {
            result.success = true;
            return result;
        }

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

    pub fn check_majority(&self) -> bool {
        let inter = self.voteState.intersection(&self.network.nodes);
        let cnt = inter.count();
        return cnt >= (self.network.nodes.len()).div_ceil(2);
    }

    pub fn execute_from_message(&mut self, message: &str) -> String {
        let message = RaftNode::parse_request(&message);
        let inter: RaftMessage = match message {
            RaftMessage::AE(append_entries) => {
                RaftMessage::AE_R(self.append_entries_receiver(append_entries))
            }
            RaftMessage::RV(request_vote) => {
                RaftMessage::RV_R(self.request_vote_receiver(request_vote))
            }
            RaftMessage::IS(install_snapshot) => RaftMessage::IS_R(InstallSnapshotReply {}),
            RaftMessage::RV_R(request_vote_reply) => {
                self.request_vote_sender_response(request_vote_reply);
                RaftMessage::Nil
            }
            RaftMessage::AE_R(append_entries_reply) => RaftMessage::Nil,
            RaftMessage::IS_R(install_snapshot_reply) => RaftMessage::Nil,
            _ => RaftMessage::Nil,
        };

        return RaftNode::parse_response(inter);
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

    pub fn become_leader(&mut self) {
        self.voteState.clear();
        self.state = NodeState::Leader;
    }

    pub fn become_follower(&mut self) {
        self.state = NodeState::Follower;
    }

    pub fn become_candidate(&mut self) {
        self.voteState.clear();
        self.incrementTerm();
        self.state = NodeState::Candidate;
        self.voteState.insert(self._id);
    }

    pub fn setTerm(&mut self, term: u32) {
        self.currentTerm = term;
    }

    pub fn getTerm(&self) -> u32 {
        return self.currentTerm;
    }

    pub fn incrementTerm(&mut self) {
        self.currentTerm = self.currentTerm + 1;
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

    pub fn getState(&self) -> &NodeState {
        return &self.state;
    }

    pub fn getLeader(&self) -> Option<u32> {
        return self.votedFor;
    }   

    pub fn add_to_network(&mut self, node: u32) {
        self.network.nodes.insert(node);
    }

    pub fn get_network(&self) -> &HashSet<u32> {
        &self.network.nodes
    }
}
