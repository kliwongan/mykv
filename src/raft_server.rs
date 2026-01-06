use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc::Sender;

use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

use tonic::transport::Server;

use crate::raft_node::{NodeState, RaftNode};

use prost::Message;
use tonic::{Request, Response, Status};

pub mod raftrpc {
    tonic::include_proto!("raft_service");
}

use raftrpc::log_entry::{Command, Get, Set};
use raftrpc::raft_service_client::RaftServiceClient;
use raftrpc::raft_service_server::{RaftService, RaftServiceServer};
use raftrpc::{AppendEntries, AppendEntriesResponse, LogEntry, RequestVote, RequestVoteResponse};

pub struct RaftServer {
    id: u32,
    // TODO: change this value
    rx: Sender<u32>,
}

impl RaftServer {
    pub fn new(id: u32, node: Arc<Mutex<RaftNode>>) -> RaftServer {
        RaftServer { node: node, id: id }
    }

    pub async fn run(&mut self) {
        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::TRACE)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
        //info!("Listening on: http://{}", addr);
    }
}

#[tonic::async_trait]
impl RaftService for RaftServer {
    async fn send_request_vote(
        &self,
        _request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Response<RequestVote>, tonic::Status> {
        let node = self.get_node();
        let node_lock = node.lock().await;
        Ok(Response::new(node_lock.send_request_vote()))
    }
    async fn request_vote_reply(
        &self,
        request: tonic::Request<RequestVote>,
    ) -> std::result::Result<tonic::Response<RequestVoteResponse>, tonic::Status> {
        let node = self.get_node();
        let mut node_lock = node.lock().await;
        Ok(Response::new(
            node_lock.request_vote_receiver(request.into_inner()),
        ))
    }
    async fn send_append_entries(
        &self,
        _request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Response<AppendEntries>, tonic::Status> {
        let node = self.get_node();
        let node_lock = node.lock().await;
        Ok(Response::new(node_lock.send_append_entries()))
    }
    async fn append_entries_reply(
        &self,
        request: tonic::Request<AppendEntries>,
    ) -> std::result::Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        let node = self.get_node();
        let mut node_lock = node.lock().await;
        Ok(Response::new(
            node_lock.append_entries_receiver(request.into_inner()),
        ))
    }
}
