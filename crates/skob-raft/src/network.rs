//! Single-node Raft network.
//!
//! `RaftNetworkFactory::new_client` cannot return an error in openraft 0.9.25.
//! Unknown peers fail on the first RPC with `Unreachable`. A one-node group never
//! calls this factory.

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

use crate::NodeId;
use crate::TypeConfig;

pub type RpcError<E = openraft::error::Infallible> = RPCError<NodeId, BasicNode, RaftError<NodeId, E>>;

#[derive(Debug, Clone, Default)]
pub struct Network {}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            target,
            target_node: node.clone(),
        }
    }
}

pub struct NetworkConnection {
    target: NodeId,
    target_node: BasicNode,
}

impl NetworkConnection {
    fn unreachable<E: std::error::Error>(&self) -> RpcError<E> {
        let err = std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            format!(
                "single-node raft has no peer {} at {}",
                self.target, self.target_node.addr
            ),
        );
        RPCError::Unreachable(Unreachable::new(&err))
    }
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        _req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RpcError> {
        Err(self.unreachable())
    }

    async fn install_snapshot(
        &mut self,
        _req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RpcError<InstallSnapshotError>> {
        Err(self.unreachable())
    }

    async fn vote(&mut self, _req: VoteRequest<NodeId>, _option: RPCOption) -> Result<VoteResponse<NodeId>, RpcError> {
        Err(self.unreachable())
    }
}
