use core::error::Error;
use rand::Rng;
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use tracing::{debug, error, info, trace};

use crate::error::Result;
use crate::raft_rpc::raftrpc::{Entry, HardState, MessageType, RaftMessage};
use crate::storage::Storage;

// Based on the recommended values from the Raft paper
const MIN_ELECTION_DURATION: usize = 1000 + 150;
const MAX_ELECTION_DURATION: usize = 1000 + 300;
const INVALID_ID: u64 = 0;

#[derive(Default, Clone, Copy, Debug, PartialEq)]
pub enum NodeState {
    #[default]
    Follower,
    Candidate,
    Leader,
}

#[derive(Default, Debug, PartialEq)]
pub struct SoftState {
    pub leader_id: u64,
    pub raft_state: NodeState,
}

#[derive(Default, Debug, PartialEq)]
pub struct RaftConfig {
    // config object for RaftNode
    pub id: u64,
    pub election_tick: usize,
    pub heartbeat_tick: usize,
    pub last_applied: u64,
}

#[derive(Clone, Debug)]
struct NetworkConfig {
    nodes: HashSet<u64>,
}

// Raft state machine + consensus/timing
// Inspired by tikv/raft-rs and etcd-io's implementation
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
    pub vote_state: HashMap<u64, bool>,
    pub network: HashSet<u64>,

    // Storage
    pub store: T,

    // Message state
    pub msgs: Vec<RaftMessage>,
}

impl<T: Storage> RaftNode<T> {
    pub fn new(config: &RaftConfig, store: T) -> Self {
        let mut rng = rand::rng();
        RaftNode {
            id: config.id,
            term: 0,
            voted_for: None,
            leader_id: INVALID_ID,
            log: Vec::<Entry>::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            state: NodeState::Follower,
            randomized_timeout: rng.random_range(config.election_tick..2 * config.election_tick),
            vote_state: HashMap::new(),
            network: HashSet::new(),
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            heartbeat_timeout: config.heartbeat_tick,
            election_timeout: config.election_tick,
            store,
            msgs: Vec::<RaftMessage>::new(),
        }
    }

    pub fn tick(&mut self) -> bool {
        match self.state {
            NodeState::Candidate | NodeState::Follower => self.tick_election(),
            NodeState::Leader => self.tick_heartbeat(),
        }
    }

    // For followers and candidates to tick the election timer
    pub fn tick_election(&mut self) -> bool {
        if self.state == NodeState::Follower {
            info!("Following {}", self.leader_id);
        }
        self.election_elapsed += 1;
        if self.election_elapsed < self.randomized_timeout {
            return false;
        }

        info!("Election timer timed out!");
        self.election_elapsed = 0;
        // create new message to send out elections and step into it
        // currently using Nil subtype to represent non-response
        let m = Self::new_message(MessageType::Nil, None, self.id);
        let _ = self.step(m);
        true
    }

    pub fn tick_heartbeat(&mut self) -> bool {
        // This function is run by leaders
        self.election_elapsed += 1;
        self.heartbeat_elapsed += 1;

        let mut ready = false;
        if self.election_elapsed >= self.election_timeout {
            self.election_elapsed = 0;
            // Currently don't have a check quorum function here
            // skip until it's needed?

            // get new message here and step through the msg

            //let m = Self::new_message(MessageType::Nil, None, self.id);
            //let _ = self.step(m);
            ready = true;
        }

        if self.state != NodeState::Leader {
            return ready;
        }

        if self.heartbeat_elapsed >= self.heartbeat_timeout {
            self.heartbeat_elapsed = 0;
            // Send out a heartbeat
            info!("Sending out heartbeat from {} to {}", self.id, INVALID_ID);
            let m = Self::new_message(MessageType::Beat, Some(self.id), INVALID_ID);
            let _ = self.step(m);
            ready = true;
        }
        ready
    }

    pub fn step(&mut self, m: RaftMessage) -> Result<()> {
        let mut message_log = format!(
            "Stepping through a message from {} to {}, with term {} of MessageType {:?}",
            m.from,
            m.to,
            m.log_term,
            MessageType::try_from(m.msg_type),
        );

        if m.log_term == 0 || m.from == INVALID_ID {
            // local message, ignore
        } else if self.term < m.log_term {
            // if requestvote received, then we give it the vote
            // as long as it doesn't hear from a leader within the minimum election timeout
            // due to leader completeness,
            if m.msg_type == MessageType::RequestVote as i32 {
                let not_avail =
                    self.voted_for.is_some() && self.election_elapsed < self.election_timeout;
                if not_avail {
                    // ignore the vote and log it
                    let log = format!(
                        "Ignoring the vote from {} since this node either voted for someone or has not exceeded the timeout limit",
                        m.from
                    );
                    info!(log);
                    return Ok(());
                }
            }

            // we received a message from a higher term
            // log it here
            info!(message_log);
            // what happens if we receive a message from a higher term here?
            // if the message is an appendentries, or heartbeat, simply follow

            // TODO: Snapshot
            if m.msg_type == (MessageType::Append as i32)
                || m.msg_type == (MessageType::Heartbeat as i32)
            {
                // this node is the leader to the best of our knowledge since it's sending the commands
                // if not then eventually we will find the right leader
                self.become_follower(m.from, m.log_term);
            } else {
                // becomes a follower with no idea who the leader is
                // since we can't guarantee this node is the leader
                self.become_follower(INVALID_ID, m.log_term);
            }
        } else if self.term > m.log_term {
            // if the current term is greater

            // if requestvote, reject
            // if append entries, we reject and say term is greater
            // if heartbeat, reject and tell your term

            // so basically barring some checkquorum and prevote shenanigans,
            // with checking logs, commit indexes, etc
            // we basically ignore, for now
            let mut m = Self::new_message(MessageType::AppendResponse, None, m.from);
            m.reject = true;
            self.send(m);
            message_log.push_str("\nRejecting the request since it is a lower term");
            info!(message_log);
        }

        // now we match by message type?
        // the cases that are handled here should be if self.term <= m.log_term
        match MessageType::try_from(m.msg_type) {
            Ok(MessageType::Nil) => {
                self.check_leadership(false);
            }
            Ok(MessageType::RequestVote) => {
                // if we already voted for the same node already, RequestVote
                // or if we don't think there's a leader and we haven't voted yet
                //info!("Received a RequestVote message from {}");
                let vote = self.voted_for == Some(m.from)
                    || (self.voted_for.is_none() && self.leader_id == INVALID_ID);
                //  if the above is met AND the log is up to date
                let other_log = m.entries.clone();
                let log_up_to_date = self.log_up_to_date(other_log);
                let mut to_send =
                    Self::new_message(MessageType::RequestVoteResponse, Some(self.id), m.from);
                to_send.reject = true;
                to_send.log_term = m.log_term;
                // accept the vote
                if vote && log_up_to_date {
                    info!("Accepting the vote from {}", m.from);
                    to_send.reject = false;
                    self.election_elapsed = 0;
                    self.voted_for = Some(m.from);
                    self.send(to_send);
                } else {
                    // else reject
                    info!("Rejecting the vote from {}", m.from);
                    to_send.reject = true;
                    self.send(to_send);
                }
            }
            _ => match self.state {
                NodeState::Leader => self.step_leader(m)?,
                NodeState::Candidate => self.step_candidate(m)?,
                NodeState::Follower => self.step_follower(m)?,
            },
        };

        Ok(())
    }

    fn step_leader(&mut self, m: RaftMessage) -> Result<()> {
        trace!("Step leader called");
        match MessageType::try_from(m.msg_type) {
            Ok(MessageType::Beat) => {
                // TODO turn into method later
                debug!("Sending out heartbeats");
                let lst = self.network.clone();
                debug!("Network is {:?}", &lst);
                for node in lst {
                    if node != self.id {
                        let mut msg = m.clone();
                        msg.set_msg_type(MessageType::Heartbeat);
                        msg.to = node;
                        info!("Sending heartbeat to {}", msg.to);
                        self.send(msg);
                    }
                }
            }
            _ => {
                // TODO
            }
        };

        // separate match for response types
        match MessageType::try_from(m.msg_type) {
            _ => {}
        };
        Ok(())
    }

    fn step_candidate(&mut self, m: RaftMessage) -> Result<()> {
        info!("Step candidate called");
        match MessageType::try_from(m.msg_type) {
            Ok(MessageType::Propose) => {
                // reject proposal since there is no evident leader
            }
            Ok(MessageType::Heartbeat) => {
                // become a follower if we detect a leader
                // in the same or greater term
                if self.term == m.log_term {
                    self.become_follower(m.from, m.log_term);
                }
            }
            Ok(MessageType::Append) => {
                // append new things to log
                // and then become a follower
                // send append response
                self.become_follower(m.from, m.log_term);
            }
            Ok(MessageType::RequestVoteResponse) => {
                info!("Received a RequestVoteResponse from {}", m.from);
                // handle vote responses
                // note that fn step handles votes
                if !m.reject {
                    info!("Recording the vote from {}", m.from);
                    self.vote_state.insert(m.from, true);
                }

                if self.check_majority() {
                    self.become_leader();
                }
            }
            _ => {}
        };
        Ok(())
    }

    fn step_follower(&mut self, m: RaftMessage) -> Result<()> {
        info!("Step follower called");
        match MessageType::try_from(m.msg_type) {
            // TODO: proposal forwarding to leader?
            Ok(MessageType::Propose) => {
                // TODO: proposal forwarding to leader?
            }
            Ok(MessageType::Heartbeat) => {
                self.election_elapsed = 0;
                self.leader_id = m.from;
                self.term = m.log_term;
                // send back heartbeat response
                // TODO: turn into method
                let msg = Self::new_message(MessageType::HeartbeatResponse, Some(self.id), m.from);
                self.send(msg);
            }
            Ok(MessageType::Append) => {
                self.election_elapsed = 0;
                // append new things to log
            }
            _ => {}
        };
        Ok(())
    }

    fn check_leadership(&mut self, leader_transfer: bool) {
        if self.state == NodeState::Leader {
            info!("Ignoring election timeout because we are the leader");
            return;
        }

        // check unapplied committed entries to find a config change
        // we need to apply configuration changes before we campaign
        // otherwise we must quit the action

        // check to see if we need to transfer leadership, or have prevote
        self.start_vote();
    }

    fn start_vote(&mut self) {
        info!("Starting a vote");
        self.become_candidate();

        info!("Checking if majority!");
        if self.check_majority() {
            // single node cluster, become leader and return
            info!("Single node cluster detected");
            self.become_leader();
            return;
        }

        // send out request vote messages
        let network = self.network.clone();
        for node in network {
            if node == self.id {
                continue;
            }

            let mut m = Self::new_message(MessageType::RequestVote, Some(self.id), node);
            m.log_term = self.term;
            m.index = self.last_applied;
            // TODO term vs log term distinction
            m.commit_index = Some(self.commit_index);
            // TODO: commit_term?
            self.send(m);
        }
    }

    fn new_message(mtype: MessageType, from: Option<u64>, to: u64) -> RaftMessage {
        let mut retval = RaftMessage {
            msg_type: 0,
            log_term: 0,
            index: 0,
            entries: Vec::new(),
            from: from.unwrap_or_default(),
            to,
            commit_index: None,
            commit_term: None,
            reject: false,
        };
        retval.set_msg_type(mtype);
        retval
    }

    pub fn send(&mut self, mut m: RaftMessage) {
        // TODO: Change to &self once I solve the problem

        // Send a message
        info!(
            "Pushing a message from {} to {} with type {:?}",
            m.from,
            m.to,
            MessageType::try_from(m.msg_type)
        );

        // includes any prevote stuff too if I get to do it
        if m.msg_type == MessageType::RequestVote as i32
            || m.msg_type == MessageType::RequestVoteResponse as i32
        {
            if m.log_term == 0 {
                error!(
                    "The term should be nonzero and set when sending {:?}",
                    MessageType::try_from(m.msg_type)
                );
                return;
            }
        } else {
            // I guess the term shouldn't be set here since we find out from the logs, but...
            // it's a bit unnecessary? leave in for now
            if m.log_term != 0 {
                error!(
                    "The term should not be set when sending {:?}",
                    MessageType::try_from(m.msg_type)
                );
                return;
            }

            // TODO: understand this logic
            if m.msg_type != MessageType::Propose as i32
                && m.msg_type != MessageType::ReadIndex as i32
            {
                m.log_term = self.term;
            }

            // set msg priority here for requestvote or prevote when I implement it
            // TODO
        }
        self.msgs.push(m);
    }

    pub fn log_up_to_date(&self, other_log: Vec<Entry>) -> bool {
        // Checks if the other log is at least as up to date as our own
        // if self.log.last().is_some() && other_log.last().is_some() {
        //     let log_last = self.log.last().unwrap();
        //     let other_last = other_log.last().unwrap();
        //     return log_last.commit_index > other_last.commit_index
        //         || log_last.term > other_last.term;
        // }
        true
    }

    pub fn log_reconcile(&self, other_log: Vec<Entry>) -> (u64, u64) {
        // Returns the last commit index and term where our log and the other log
        // is the same, aka reconcile the differences
        let mut next_index = min(self.log.len(), other_log.len());
        let mut next_index_i = next_index as i32;
        let mut term = self.term;
        while next_index_i >= 0 {
            let entry = &self.log[next_index];
            let other_entry = &other_log[next_index];
            if entry.term == other_entry.term && entry.commit_index == other_entry.commit_index {
                break;
            }
            next_index -= 1;
            next_index_i -= 1;
        }
        term = self.log[next_index].term;

        (next_index as u64, term)
    }

    pub fn check_majority(&self) -> bool {
        let mut cnt = 0;
        for (_, value) in self.vote_state.iter() {
            if *value {
                cnt += 1
            }
        }
        cnt >= (self.network.len()).div_ceil(2)
    }

    pub fn get_timeout(&self) -> usize {
        self.randomized_timeout
    }

    pub fn reset_timeout(&mut self) {
        let mut rng = rand::rng();
        self.randomized_timeout = rng.random_range(MIN_ELECTION_DURATION..MAX_ELECTION_DURATION);
    }

    fn reset(&mut self, term: u64) {
        if self.term != term {
            self.term = term;
            self.voted_for = None;
        }

        self.vote_state.clear();
        self.leader_id = INVALID_ID;
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;

        // basically abort everything
    }

    pub fn become_leader(&mut self) {
        self.vote_state.clear();
        self.state = NodeState::Leader;
        info!("Becoming a leader");
    }

    pub fn become_follower(&mut self, leader_id: u64, term: u64) {
        self.reset(term);
        self.leader_id = leader_id;
        self.state = NodeState::Follower;
        info!("Becoming a follower, following {}", leader_id);
    }

    pub fn become_candidate(&mut self) {
        let term = self.term + 1;
        self.reset(term);
        self.state = NodeState::Candidate;
        self.vote_state.insert(self.id, true);
        self.voted_for = Some(self.id);
        info!("Becoming a candidate");
        info!("Current term is now {}", self.term);
    }

    pub fn get_softstate(&self) -> SoftState {
        SoftState {
            leader_id: self.leader_id,
            raft_state: self.state,
        }
    }

    pub fn get_hardstate(&self) -> HardState {
        let mut hs = HardState::default();
        hs.term = self.term;
        hs.commit_index = self.commit_index;
        if let Some(vote) = self.voted_for {
            hs.vote = vote;
        }
        hs
    }

    pub fn set_term(&mut self, term: u64) {
        self.term = term;
    }

    pub fn get_term(&self) -> u64 {
        self.term
    }

    pub fn increment_term(&mut self) {
        self.term += 1;
    }

    pub fn is_leader(&self) -> bool {
        self.state == NodeState::Leader
    }

    pub fn is_follower(&self) -> bool {
        self.state == NodeState::Follower
    }

    pub fn is_candidate(&self) -> bool {
        self.state == NodeState::Candidate
    }

    pub fn get_state(&self) -> &NodeState {
        &self.state
    }

    pub fn get_leader(&self) -> Option<u64> {
        self.voted_for
    }

    pub fn add_to_network(&mut self, node: u64) {
        self.network.insert(node);
    }

    pub fn get_network(&self) -> &HashSet<u64> {
        &self.network
    }
}
