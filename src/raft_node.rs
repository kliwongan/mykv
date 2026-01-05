use crate::raft_rpc::raftrpc::{
    RaftMessage, Entry, MessageType,
};
use crate::storage::{Storage};
use rand::Rng;
use std::cmp::min;
use std::collections::{HashMap, HashSet};

// Based on the recommended values from the Raft paper
const MIN_ELECTION_DURATION: usize = 150;
const MAX_ELECTION_DURATION: usize = 300;

#[derive(Clone, PartialEq)]
pub enum NodeState {
    Leader,
    Follower,
    Candidate,
}

#[derive(Clone)]
struct NetworkConfig {
    nodes: HashSet<u32>,
}

pub struct RaftConfig {
    // config object for RaftNode
}

// Raft state machine + consensus/timing
pub struct RaftNode<T: Storage> {
    pub id: u32,

    // Persistent state
    pub current_term: u32,
    pub voted_for: Option<u32>,
    pub log: Vec<Entry>,

    // Volatile state
    pub commit_index: u32,
    pub last_applied: u32,

    // Timeout variables
    pub heartbeat_timeout: usize,
    pub election_timeout: usize,
    pub randomized_timeout: usize,

    // Internal time state variables
    pub election_elapsed: usize,
    pub heartbeat_elapsed: usize,

    // Leader state (volatile) only
    pub next_index: HashMap<u32, u32>,
    pub match_index: HashMap<u32, u32>,

    // State of the current node
    pub state: NodeState,

    // Implementation based state
    pub vote_state: HashSet<u32>,
    pub network: NetworkConfig,

    // Storage
    pub store: T,
}

impl<T: Storage> RaftNode<T> {
    pub fn new(id: u32, store: T) -> Self {
        let mut rng = rand::rng();
        RaftNode {
            id: id,
            current_term: 1,
            voted_for: None,
            log: Vec::<Entry>::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            state: NodeState::Follower,
            randomized_timeout: rng.random_range(MIN_ELECTION_DURATION..MAX_ELECTION_DURATION),
            vote_state: HashSet::new(),
            network: NetworkConfig {
                nodes: HashSet::new(),
            },
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            heartbeat_timeout: 0,
            election_timeout: 0,
            store: store,
        }
    }

    pub fn tick(&mut self) -> bool {
        match self.state {
            NodeState::Candidate | NodeState::Follower => self.tick_election(),
            NodeState::Leader => self.tick_heartbeat(),
        }
    }

    pub fn tick_election(&mut self) -> bool {
        self.election_elapsed += 1;
        if self.election_elapsed >= self.randomized_timeout {
            return false;
        }

        self.election_elapsed = 0;
        // create new message to send out elections and step into it
        // self.step(m)
        true
    }

    pub fn tick_heartbeat(&mut self) -> bool {
        self.election_elapsed += 1;
        self.heartbeat_elapsed += 1;

        let mut ready = false;
        if self.election_elapsed >= self.election_timeout {
            self.election_elapsed = 0;
            // get new message here and step through the msg
            // self.step()
            ready = true;
        }

        if self.state != NodeState::Leader {
            return ready;
        }

        if self.heartbeat_elapsed >= self.heartbeat_timeout {
            self.heartbeat_elapsed = 0;
             // get new message here and step through the msg
            // self.step()
            ready = true;
        }
        ready
    }

    pub fn step(&mut self, m: RaftMessage) {
        // we step through messages here
        
        // follower logic
        if self.term < m.log_term {
            // if requestvote, then we give it the vote
            // if we're not already following a leader
            if m.msg_type == MessageType::RequestVote {
                
            }
        } else {

        }

        // leader logic; send out message


    }

    fn new_message() -> RaftMessage {
        
    }

    pub fn send_request_vote(&self) -> RequestVote {
        RequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: if self.log.len() as u32 > 0 {
                self.log.len() as u32 - 1
            } else {
                0
            },
            last_log_term: if self.log.len() as u32 > 0 {
                self.log.get(self.log.len() - 1).unwrap().term
            } else {
                self.current_term
            },
        }
    }

    pub fn request_vote_receiver(&mut self, args: RequestVote) -> RequestVoteResponse {
        let mut result = RequestVoteResponse {
            term: self.current_term,
            vote_granted: false,
            id: self.id,
        };

        if self.current_term > args.term {
            return result;
        }

        let last_entry = self.log.get((args.last_log_index - 1) as usize);
        if self.voted_for.is_none()
            && !last_entry.is_none()
            && last_entry.unwrap().term == args.last_log_term
        {
            result.vote_granted = true;
            self.voted_for = Some(args.candidate_id);
        }

        return result;
    }

    pub fn request_vote_sender_response(&mut self, args: RequestVoteResponse) {
        if args.vote_granted {
            self.vote_state.insert(args.id);
        } else if args.term >= self.current_term {
            self.state = NodeState::Follower;
        }
    }

    pub fn send_append_entries(&self) -> AppendEntries {
        AppendEntries {
            term: self.current_term,
            leader_id: self.id,
            prev_log_index: if self.log.len() as u32 > 0 {
                self.log.len() as u32 - 1
            } else {
                0
            },
            prev_log_term: if self.log.len() as u32 > 0 {
                self.log.get(self.log.len() - 1).unwrap().term
            } else {
                self.current_term
            },
            entries: self.log.clone(),
            leader_commit: self.commit_index,
        }
    }

    pub fn append_entries_sender_response(
        &mut self,
        args: AppendEntriesResponse,
    ) -> Option<AppendEntries> {
        if !args.success {
            self.next_index
                .insert(args.id, self.next_index.get(&args.id).unwrap() - 1);
            let mut next_index = self.next_index.get(&args.id).unwrap().clone();

            // minor AppendEntries optimization
            let conflicting_first_index = args.conflicting_first_index;
            let conflicting_entry_term = args.conflicting_entry_term;

            if conflicting_first_index.is_some() && conflicting_entry_term.is_some() {
                let new_index = conflicting_first_index.unwrap();
                next_index = new_index - 1;
            }
            let prev_log_term = if self.log.len() as u32 - 1 > 0 {
                self.log.get(next_index as usize).unwrap().term
            } else {
                self.current_term
            };

            return Some(AppendEntries {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index: next_index,
                prev_log_term: prev_log_term,
                entries: self.log.clone(),
                leader_commit: self.commit_index,
            });
        }
        // Use vote_state to maintain nodes that have already replicated our entries
        self.vote_state.insert(args.id);
        None
    }

    pub fn append_entries_receiver(&mut self, args: AppendEntries) -> AppendEntriesResponse {
        let mut result = AppendEntriesResponse {
            term: self.current_term,
            success: false,
            conflicting_entry_term: None,
            conflicting_first_index: None,
            id: self.id,
        };

        if self.current_term != args.term {
            if self.current_term < args.term {
                // we need to step down if we are a leader or candidate
                self.state = NodeState::Follower;
                self.current_term = args.term;
            }
            return result;
        }

        // Check for conflicting entries
        // We must always check this, even for heartbeat RPCs
        if let Some(entry) = self.log.get((args.prev_log_index - 1) as usize) {
            // existing entry in the same index but conflicts with current term
            if entry.term != args.prev_log_term {
                // find first index and entry term of conflicting entries

                // TODO
                result.conflicting_entry_term = Some(1);
                result.conflicting_first_index = Some(1);

                // Delete this entry and all following entries
                self.log
                    .truncate((self.log.len() as u32 - args.prev_log_index + 1) as usize);
            }
            result.success = true;
        } else {
            return result;
        }

        // append all new entries not already in the log
        for entry in args.entries {
            self.log.push(entry);
        }

        if args.leader_commit > self.commit_index {
            self.commit_index = min(args.leader_commit, self.log.len() as u32);
        }

        return result;
    }

    pub fn check_majority(&self) -> bool {
        let inter = self.vote_state.intersection(&self.network.nodes);
        let cnt = inter.count();
        return cnt > (self.network.nodes.len()).div_ceil(2);
    }

    pub fn get_timeout(&self) -> u64 {
        return self.timeout;
    }

    pub fn reset_timeout(&mut self) {
        let mut rng = rand::rng();
        self.timeout = rng.random_range(MIN_ELECTION_DURATION..MAX_ELECTION_DURATION);
    }

    pub fn become_leader(&mut self) {
        self.vote_state.clear();
        self.state = NodeState::Leader;
    }

    pub fn become_follower(&mut self) {
        self.state = NodeState::Follower;
    }

    pub fn become_candidate(&mut self) {
        self.vote_state.clear();
        self.incrementTerm();
        self.state = NodeState::Candidate;
        self.vote_state.insert(self.id);
    }

    pub fn setTerm(&mut self, term: u32) {
        self.current_term = term;
    }

    pub fn getTerm(&self) -> u32 {
        return self.current_term;
    }

    pub fn incrementTerm(&mut self) {
        self.current_term = self.current_term + 1;
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
        return self.voted_for;
    }

    pub fn add_to_network(&mut self, node: u32) {
        self.network.nodes.insert(node);
    }

    pub fn get_network(&self) -> &HashSet<u32> {
        &self.network.nodes
    }
}
