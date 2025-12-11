use prost::Message;
use tonic::{Request, Response, Status};

pub mod raftrpc {
    tonic::include_proto!("raft_service");
}

use raftrpc::log_entry::{Command, Get, Set};
use raftrpc::raft_rpc_client::RaftRpcClient;
use raftrpc::raft_rpc_server::{RaftRpc, RaftRpcServer};
use raftrpc::{AppendEntries, AppendEntriesResponse, LogEntry, RequestVote, RequestVoteResponse};

use crate::raft_service::RaftService;

#[tonic::async_trait]
impl RaftRpc for RaftService {
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
