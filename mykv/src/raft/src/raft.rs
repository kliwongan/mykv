use std::cmp::min;
use futures::prelude::*;
use tokio::sync::mpsc;
use tokio::time::timeout;
use rand::Rng;


#[derive(Clone)]
enum NodeState {
    Leader,
    Follower,
    Candidate,
}

#[derive(Clone)]
enum Command {
    Set { key: String, value: i32 },
    Get { key : String },
}

#[derive(Clone)]
struct LogEntry {
    command: Command,
    term: u32,
}

#[derive(Clone)]
struct RaftService {
    // Persistent state
    currentTerm: u32,
    votedFor: Option<u32>,
    log: Vec<LogEntry>,

    // Volatile state
    commitIndex: u32,
    lastApplied: u32,

    // Leader state (volatile) only
    nextIndex: Vec<u32>,
    matchIndex: Vec<u32>,

    // State of the current node
    state: NodeState,

    // Election timeouts?
    timeout: u64
}

#[derive(Clone)]
struct RequestVote {
    term: u32,
    candidateId: u32,
    lastLogIndex: u32,
    lastLogTerm: u32,
}

#[derive(Clone)]
struct RequestVoteResult {
    term: u32,
    voteGranted: bool,
}

#[derive(Clone)]
struct AppendEntries {
    term: u32,
    leaderId: u32,
    prevLogIndex: u32,
    prevLogTerm: u32,
    entries: Vec<LogEntry>,
    leaderCommit: u32,
}

#[derive(Clone)]
struct AppendEntriesResult {
    term: u32,
    success: bool,
    conflictingEntryTerm: Option<u32>,
    conflictingFirstIndex: Option<u32>,
}

#[derive(Clone)]
struct InstallSnapshot {
    // TODO
}

#[derive(Clone)]
struct InstallSnapshotResult {
    // TODO
}

impl RaftService {
    fn new() -> Self {
        let mut rng = rand::rng();
        RaftService {
            currentTerm: 1,
            votedFor: None,
            log: Vec::new(),
            commitIndex: 0,
            lastApplied: 0,
            nextIndex: 1,
            matchIndex: 1,
            state: NodeState::Follower,
            timeout: rng.random_range(150..300),
        }
    }

    async fn append_entries(&mut self, args: AppendEntries) -> AppendEntriesResult {
        let mut result = AppendEntriesResult {
            term: self.currentTerm,
            success: false,
            conflictingEntryTerm: None,
            conflictingFirstIndex: None,
        };

        if self.currentTerm > args.term {
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
                self.log.truncate((self.log.len() as u32 - args.prevLogIndex + 1) as usize);
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

    async fn request_vote(&mut self, args: RequestVote) -> RequestVoteResult {
        let mut result = RequestVoteResult {
            term: self.currentTerm,
            voteGranted: false,
        };

        if self.currentTerm > args.term {
            return result;
        }

        let last_entry = self.log.get((args.lastLogIndex - 1) as usize);
        if self.votedFor.is_none() && !last_entry.is_none() && last_entry.unwrap().term == args.lastLogTerm {
            result.voteGranted = true;
            self.votedFor = Some(args.candidateId);
        }

        return result;
    }

    async fn run(&mut self) {
        // function that operates the node itself
        
        // First, start the election timeout
        // await RPC queries on event queue
        // if no queries in event queue before election timeout ends, start election
        // else process the events in order ASAP

        let (tx, mut rx) = mpsc::channel(100);
        
        loop { 
            if let Err(_) = timeout(Duration::from_millis(self.timeout), rx).await {
                // become a candidate if no heartbeat detected from leader
                
            }
            
            match self.state {
                
            }
        }

    }
}