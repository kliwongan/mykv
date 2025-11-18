use rand::Rng;
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::io::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::Mutex;
use tokio::time::timeout;

use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

use serde::{Deserialize, Serialize};

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
    nextIndex: HashMap<u32, u32>,
    matchIndex: HashMap<u32, u32>,

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

    pub fn append_entries_receiver(&mut self, args: AppendEntries) -> AppendEntriesReply {
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
            });
        }
        // Use voteState to maintain nodes that have already replicated our entries
        self.voteState.insert(args._id);
        None
    }

    pub fn send_append_entries(&self) -> AppendEntries {
        AppendEntries {
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
        }
    }

    pub fn send_request_vote(&self) -> RequestVote {
        RequestVote {
            term: self.currentTerm,
            candidateId: self._id,
            lastLogIndex: self.log.len() as u32 - 1,
            lastLogTerm: if self.log.len() as u32 - 1 >= 0 {
                self.log.get(self.log.len() - 1).unwrap().term
            } else {
                self.currentTerm
            },
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
            RaftMessage::AE(append_entries) => {
                RaftMessage::AE_R(self.append_entries_receiver(append_entries))
            }
            RaftMessage::RV(request_vote) => {
                RaftMessage::RV_R(self.request_vote_receiver(request_vote))
            }
            RaftMessage::IS(install_snapshot) => RaftMessage::IS_R(InstallSnapshotReply {}),
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
        self.voteState.clear();
        self.state = NodeState::Leader;
    }

    pub fn become_follower(&mut self) {
        self.state = NodeState::Follower;
    }

    pub fn become_candidate(&mut self) {
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

    pub fn add_to_network(&mut self, node: u32) {
        self.network.nodes.insert(node);
    }

    pub async fn run(&mut self) {
        let addr = SocketAddr::from(([127, 0, 0, 1], 2222));
        let listener = TcpListener::bind(&addr).await.unwrap();
        let node = Arc::new(Mutex::new(RaftService::new(2222)));

        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::TRACE)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");

        info!("Listening on: http://{}", addr);
        loop {
            info!("At loop start!");
            let cur_node = Arc::clone(&node);
            let mut node_lock = cur_node.lock().await;
            if node_lock.is_follower() {
                info!("Node is a follower");
                let timeout_duration = Duration::from_millis(node_lock.get_timeout());
                let result = timeout(timeout_duration, listener.accept()).await;
                match result {
                    Err(_) => {
                        info!("Becoming a candidate because timeout was reached");
                        node_lock.become_candidate();
                        node_lock.reset_timeout();
                        drop(node_lock);
                        continue;
                    }
                    Ok(result) => {
                        if let Err(_) = result {
                            error!("Error in receiving RPC");
                            // error with receiving the RPC
                            continue;
                        } else {
                            let rt = Runtime::new().unwrap();
                            let handle = rt.handle();
                            drop(node_lock);
                            handle_request(cur_node, result, handle.clone());
                        }
                    }
                };
            } else if node_lock.is_candidate() {
                info!("Node is a candidate");
                let timeout_duration = Duration::from_millis(node_lock.get_timeout());
                // let result = timeout(timeout_duration, listener.accept()).await;
                while !node_lock.check_majority() {
                    //RaftService::parse_response(node_lock.send_request_vote());
                }
            } else {
                // node is leader

                // await client requests

                // if client requests to see something, return the connection within its log
                // else,
            }
        }
    }
}

fn handle_basic_http_request(
    result: Result<(TcpStream, SocketAddr), Error>,
    handle: Handle,
) {
    // dummy function for testing purposes
    let (mut stream, mut address) = result.unwrap();
    handle.spawn(async move {
        info!("Serving request!");
        let mut buffer = [0; 1024];
        let _ = stream.read(&mut buffer).await;

        let contents = "<h1>Hello, world!</h1>";
        let content_length = contents.len();
        let response =
            format!("HTTP/1.1 200 OK\r\nContent-Length: {content_length}\r\n\r\n{contents}");
        let _ = stream.write_all(response.as_bytes()).await;
    });
}

async fn handle_request(
    node: Arc<Mutex<RaftService>>,
    result: Result<(TcpStream, SocketAddr), Error>,
    handle: Handle,
) {
    let (mut stream, mut address) = result.unwrap();
    handle.spawn(async move {
        let cur_node = Arc::clone(&node);
        let mut node_lock = cur_node.lock().await;
        info!("Deserializing request!");
        let mut buffer = [0; 1024];
        let _ = stream.read(&mut buffer).await;

        let message = String::from_utf8(buffer.to_vec()).unwrap();
        info!("{}", format!("{}: {}", "Message", &message));
        let response = node_lock.execute_from_message(&message);

        // now write the response back
        let _ = stream.write_all(response.as_bytes()).await;
    });
}
