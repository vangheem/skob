//! Raft RPC over HTTP. Peers are the advertise addresses stored in the membership.

use std::time::Duration;

use openraft::error::InstallSnapshotError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::BasicNode;
use reqwest::Client;

use crate::NodeId;
use crate::TypeConfig;

pub type RpcError<E = openraft::error::Infallible> = RPCError<NodeId, BasicNode, RaftError<NodeId, E>>;

#[derive(Clone)]
pub struct Network {
    client: Client,
}

impl Default for Network {
    fn default() -> Self {
        Self::new(Duration::from_millis(800))
    }
}

impl Network {
    pub fn new(timeout: Duration) -> Self {
        let client = Client::builder()
            .timeout(timeout)
            .no_proxy()
            .build()
            .unwrap_or_else(|_| Client::new());
        Self { client }
    }
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            target,
            addr: node.addr.clone(),
            client: self.client.clone(),
        }
    }
}

pub struct NetworkConnection {
    target: NodeId,
    addr: String,
    client: Client,
}

impl NetworkConnection {
    fn unreachable<E: std::error::Error>(&self, err: impl std::fmt::Display) -> RpcError<E> {
        let io = std::io::Error::other(format!("raft rpc to node {} at {}: {err}", self.target, self.addr));
        RPCError::Unreachable(Unreachable::new(&io))
    }

    async fn post<T, R, E>(&self, path: &str, body: &T) -> Result<R, RpcError<E>>
    where
        T: serde::Serialize,
        R: serde::de::DeserializeOwned,
        E: std::error::Error,
    {
        let url = format!("{}{path}", self.addr.trim_end_matches('/'));
        let response = self
            .client
            .post(url)
            .json(body)
            .send()
            .await
            .map_err(|err| self.unreachable(err))?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(self.unreachable(format!("status {status}: {text}")));
        }
        response.json().await.map_err(|err| self.unreachable(err))
    }
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RpcError> {
        self.post("/internal/raft/append", &req).await
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RpcError<InstallSnapshotError>> {
        self.post("/internal/raft/snapshot", &req).await
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RpcError> {
        self.post("/internal/raft/vote", &req).await
    }
}
