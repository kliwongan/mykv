use tonic;
use prost::{Message};

pub mod raftrpc {
    tonic::include_proto!("raft_service");
}

use raftrpc::raft_rpc_client::RaftRpcClient;
use raftrpc::raft_rpc_server::{RaftRpc, RaftRpcServer};
use raftrpc::{AppendEntries, AppendEntriesResponse, RequestVote, RequestVoteResponse};

use crate::raft_service::RaftService;

#[tonic::async_trait]
impl RaftRpc for RaftService {
    async fn send_request_vote(
        &self,
        request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Response<RequestVote>, tonic::Status> {
        return send_request_vote(&self);
    }
    async fn request_vote_reply(
        &self,
        request: tonic::Request<RequestVote>,
    ) -> std::result::Result<tonic::Response<RequestVoteResponse>, tonic::Status> {
        unimplemented!();
    }
    async fn send_append_entries(
        &self,
        request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Response<AppendEntries>, tonic::Status> {
        unimplemented!();
    }
    async fn append_entries_reply(
        &self,
        request: tonic::Request<AppendEntries>,
    ) -> std::result::Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        unimplemented!();
    }
}
