use futures::prelude::*;

enum NodeState {
    Leader,
    Follower,
    Candidate,
}

enum Command {
    Set { key: str, value: i32 },
    Get { key : str },
}

struct LogEntry {
    command: Command,
    term: i32,
}

#[derive(Clone)]
struct RaftService {
    // Persistent state
    currentTerm: i32,
    votedFor: i32,
    log: Vec<LogEntry>,

    // Volatile state
    commitIndex: i32,
    lastApplied: i32,

    // Leader state (volatile) only
    nextIndex: Vec<i32>,
    matchIndex: Vec<i32>,

    // State of the current node
    state: NodeState,
}

struct RequestVote {
    term: i32,
    candidateId: i32,
    lastLogIndex: i32,
    lastLogTerm: i32,
}

struct RequestVoteResult {
    term: i32,
    voteGranted: bool,
}

struct AppendEntries {
    term: i32,
    leaderId: i32,
    prevLogIndex: i32,
    prevLogTerm: i32,
    entries: Vec<LogEntry>,
    leaderCommit: i32,
}

struct AppendEntriesResult {
    term: i32,
    success: bool,
}

struct InstallSnapshot {
    // TODO
}

trait RaftRPC {
    async fn append_entries(args: AppendEntries) -> AppendEntriesResult;
    async fn request_vote(args: RequestVote) -> RequestVoteResult;
}

impl RaftRPC for RaftService {
    async fn append_entries(args: AppendEntries) -> AppendEntriesResult {
        
    }
}