use core::error::Error;
use rand::Rng;
use std::cmp::min;
use std::collections::{HashMap, HashSet};

use crate::raft_rpc::raftrpc::{Entry, MessageType, RaftMessage};
use crate::storage::Storage;

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
    nodes: HashSet<u64>,
}

pub struct RaftConfig {
    // config object for RaftNode
}

// Raft state machine + consensus/timing
pub struct RaftNode<T: Storage> {
    pub id: u64,

    // Persistent state
    pub term: u64,
    pub voted_for: Option<u64>,
    pub leader_id: u64,
    pub log: Vec<Entry>,

    // Volatile state
    pub commit_index: u64,
    pub last_applied: u64,

    // Timeout variables
    pub heartbeat_timeout: usize,
    pub election_timeout: usize,
    pub randomized_timeout: usize,

    // Internal time state variables
    pub election_elapsed: usize,
    pub heartbeat_elapsed: usize,

    // Leader state (volatile) only
    pub next_index: HashMap<u64, u64>,
    pub match_index: HashMap<u64, u64>,

    // State of the current node
    pub state: NodeState,

    // Implementation based state
    pub vote_state: HashSet<u64>,
    pub network: NetworkConfig,

    // Storage
    pub store: T,
}

impl<T: Storage> RaftNode<T> {
    pub fn new(id: u64, store: T) -> Self {
        let mut rng = rand::rng();
        RaftNode {
            id: id,
            term: 0,
            voted_for: None,
            leader_id: 0,
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

    pub fn step(&mut self, m: RaftMessage) -> Result<(), Box<dyn Error>>{
        // we step through messages here

        if self.term < m.log_term {
            // if requestvote received, then we give it the vote
            // as long as it doesn't hear from a leader within the minimum election timeout
            // due to leader completeness,
            if m.msg_type == MessageType::RequestVote as i32 {
                let not_avail =
                    self.voted_for.is_some() && self.election_elapsed < self.election_timeout;
                if not_avail {
                    // ignore the vote and log it
                    return Ok(());
                }
            }

            // we received a message from a higher term
            // log it here

            // what happens if we receive a message from a higher term here?
            // if the message is an appendentries, or heartbeat, simply follow

            // TODO: Snapshot
            if m.msg_type == (MessageType::Append as i32) || m.msg_type == (MessageType::Heartbeat as i32) {
                // this node is the leader to the best of our knowledge since it's sending the commands
                // if not then eventually we will find the right leader
                self.become_follower();
            } else {
                // becomes a follower with no idea who the leader is
                // since we can't guarantee this node is the leader
                self.become_follower();
            }
        } else if self.term > m.log_term {
            // if the current term is greater

            // if requestvote, reject
            // if append entries, we reject and say term is greater
            // if heartbeat, reject and tell your term

            // so basically barring some checkquorum and prevote shenanigans,
            // with checking logs, commit indexes, etc
            // we basically ignore, for now
        }

        // now we match by message type?
        // the cases that are handled here should be if self.term <= m.log_term
        match MessageType::try_from(m.msg_type) {
            Ok(MessageType::RequestVote) => {
                // if we already voted for the same node already,
                // or if we don't think there's a leader and we haven't voted yet
                let vote = self.voted_for.is_none() || self.voted_for == Some(m.from);
                //  if the above is met AND the log is up to date
                let log_up_to_date = true;
                // accept the vote
                if vote && log_up_to_date {
                } else {
                    // else reject
                }
            }
            _ => match self.state {
                NodeState::Leader => self.step_leader(m)?,
                NodeState::Candidate => self.step_candidate(m)?,
                NodeState::Follower => self.step_follower(m)?,
            },
        };
    }

    fn step_leader(&mut self, m: RaftMessage) -> Result<(), Box<dyn Error>> {
        match MessageType::try_from(m.msg_type) {
            Ok(MessageType::Beat) => {
                // send heartbeat out
            }
        };

        // separate match for response types
        match m.msg_type {
            _ => {}
        };
        Ok(())
    }

    fn step_candidate(&mut self, m: RaftMessage) -> Result<(), Box<dyn Error>> {
        match MessageType::try_from(m.msg_type) {
            Ok(MessageType::Propose) => {
                // reject proposal since there is no evident leader
            }
            Ok(MessageType::Heartbeat) => {
                // become a follower if we detect a leader
                // in the same or greater term
            }
            Ok(MessageType::Append) => {
                // append new things to log
                // and then become a follower
                // send append response
            }
            Ok(MessageType::RequestVoteResponse) => {
                // handle vote responses
                // note that fn step handles votes
            }
        };
        Ok(())
    }

    fn step_follower(&mut self, m: RaftMessage) -> Result<(), Box<dyn Error>> {
        match MessageType::try_from(m.msg_type) {
            // TODO: proposal forwarding to leader?
            Ok(MessageType::Propose) => {
                // TODO: proposal forwarding to leader?
            }
            Ok(MessageType::Heartbeat) => {
                self.election_elapsed = 0;
                // send back heartbeat response
            }
            Ok(MessageType::Append) => {
                self.election_elapsed = 0;
                // append new things to log
            }
        };
        Ok(())
    }

    fn new_message(mtype: MessageType, to: u64, from: Option<u64>) -> RaftMessage {
        let mut retval = RaftMessage {
            msg_type: 0,
            log_term: 0,
            index: 0,
            entries: Vec::new(),
            from: if let Some(t) = from { t } else { 0 },
            sender: to,
        };
        retval.set_msg_type(mtype);
        retval
    }

    pub fn send_request_vote(&self) -> RequestVote {
        RequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: if self.log.len() as u64 > 0 {
                self.log.len() as u64 - 1
            } else {
                0
            },
            last_log_term: if self.log.len() as u64 > 0 {
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
            prev_log_index: if self.log.len() as u64 > 0 {
                self.log.len() as u64 - 1
            } else {
                0
            },
            prev_log_term: if self.log.len() as u64 > 0 {
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
            let prev_log_term = if self.log.len() as u64 - 1 > 0 {
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
                    .truncate((self.log.len() as u64 - args.prev_log_index + 1) as usize);
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
            self.commit_index = min(args.leader_commit, self.log.len() as u64);
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

    pub fn setTerm(&mut self, term: u64) {
        self.current_term = term;
    }

    pub fn getTerm(&self) -> u64 {
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

    pub fn getLeader(&self) -> Option<u64> {
        return self.voted_for;
    }

    pub fn add_to_network(&mut self, node: u64) {
        self.network.nodes.insert(node);
    }

    pub fn get_network(&self) -> &HashSet<u64> {
        &self.network.nodes
    }
}
