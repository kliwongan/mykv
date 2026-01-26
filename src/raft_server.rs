use std::net::{SocketAddr, ToSocketAddrs};
use tokio::sync::mpsc::Sender;

use tracing::{Level, info, warn};
use tracing_subscriber::FmtSubscriber;

use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub mod raftrpc {
    tonic::include_proto!("raft_service");
}
use raftrpc::raft_service_server::{RaftService, RaftServiceServer};
use raftrpc::{ConfChangeArgs, RaftMessage, RequestIdArgs};

pub struct RaftServer {
    addr: SocketAddr,
    rx: Sender<RaftMessage>,
}

impl RaftServer {
    pub fn new<T: ToSocketAddrs>(rx: Sender<RaftMessage>, addr: T) -> Self {
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        RaftServer { addr: addr, rx: rx }
    }

    pub async fn run(self) {
        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::TRACE)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
        let addr = self.addr;
        info!("Listening on: http://{:?}", addr);
        let service = RaftServiceServer::new(self);
        Server::builder()
            .add_service(service)
            .serve(addr)
            .await
            .expect("Error running the RaftServer");
        warn!("Server has stopped");
    }
}

#[tonic::async_trait]
impl RaftService for RaftServer {
    async fn send_message(
        &self,
        request: Request<RaftMessage>,
    ) -> Result<Response<RaftMessage>, Status> {
        let msg = request.into_inner();
        let sender = self.rx.clone();
        // TODO until I figure out serialization (serde vs bincode)
        unimplemented!();
    }
    async fn request_id(
        &self,
        request: Request<RequestIdArgs>,
    ) -> Result<Response<RaftMessage>, Status> {
        unimplemented!();
    }

    async fn change_conf(
        &self,
        request: Request<ConfChangeArgs>,
    ) -> Result<Response<RaftMessage>, Status> {
        unimplemented!();
    }
}
