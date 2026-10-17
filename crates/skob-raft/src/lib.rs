//! Raft write-segment ownership.
//!
//! A writer accepts a mutation only when the applied Raft map says it owns that
//! segment. Object bytes are not replicated through the log.

mod log_store;
mod network;
mod store;

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use openraft::Config;
use serde_json::json;
use serde_json::Value;
use skob_core::segment_for;
use skob_core::ObjectId;
use skob_core::Result;
use skob_core::SegmentOwner;
use skob_core::SkobError;
use skob_core::CATALOG_SEGMENT;

pub use store::Request;
pub use store::Response;

pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Segment ownership. `D` carries `Rebalance`; `R` is the applied map.
    pub TypeConfig:
        D = Request,
        R = Response,
);

type SkobRaft = openraft::Raft<TypeConfig>;

/// Process settings for one writer in the ownership group.
#[derive(Clone, Debug)]
pub struct RaftConfig {
    pub node_id: u64,
    pub advertise_addr: String,
    pub data_dir: PathBuf,
    pub segment_count: u32,
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_max_ms: u64,
}

impl RaftConfig {
    pub fn new(
        node_id: u64,
        advertise_addr: impl Into<String>,
        data_dir: PathBuf,
        segment_count: u32,
    ) -> Self {
        Self {
            node_id,
            advertise_addr: advertise_addr.into(),
            data_dir,
            segment_count,
            heartbeat_ms: 500,
            election_min_ms: 1500,
            election_max_ms: 3000,
        }
    }

    /// Short heartbeats so multi-node tests can observe elections quickly.
    pub fn fast_elections(mut self) -> Self {
        self.heartbeat_ms = 40;
        self.election_min_ms = 120;
        self.election_max_ms = 240;
        self
    }
}

/// Live `openraft::Raft` handle plus the applied segment map.
pub struct SegmentCoordinator {
    node_id: u64,
    advertise_addr: String,
    segment_count: u32,
    raft: SkobRaft,
    store: Arc<store::StateMachineStore>,
}

impl SegmentCoordinator {
    /// Start Raft and listen for replication, without forming a cluster.
    ///
    /// Another writer must `add_voter` this node. The HTTP server has to be up
    /// before that call, because replication is an HTTP RPC to `advertise_addr`.
    pub async fn standby(config: RaftConfig) -> Result<Arc<Self>> {
        Self::start(config, false).await
    }

    /// Build a real `openraft::Raft`, initialize a one-node group, and assign
    /// every segment to this node through `client_write`.
    ///
    /// The log is in memory, so a fresh process (empty directory) initializes
    /// again. Ownership checks always read the map produced by Raft apply.
    pub async fn bootstrap(config: RaftConfig) -> Result<Arc<Self>> {
        let coord = Self::start(config, true).await?;
        coord.wait_leader().await?;
        coord.rebalance().await?;
        Ok(coord)
    }

    async fn start(config: RaftConfig, initialize: bool) -> Result<Arc<Self>> {
        if config.segment_count == 0 {
            return Err(SkobError::BadRequest("segment_count must be non-zero".into()));
        }
        std::fs::create_dir_all(&config.data_dir).map_err(|err| SkobError::raft(err))?;

        let raft_config = Config {
            cluster_name: "skob".into(),
            heartbeat_interval: config.heartbeat_ms,
            election_timeout_min: config.election_min_ms,
            election_timeout_max: config.election_max_ms,
            ..Config::default()
        };
        let raft_config = Arc::new(
            raft_config
                .validate()
                .map_err(|err| SkobError::raft(format!("invalid raft config: {err:?}")))?,
        );

        let log_store = log_store::LogStore::default();
        let store = Arc::new(store::StateMachineStore::default());
        let raft = SkobRaft::new(
            config.node_id,
            raft_config,
            network::Network::default(),
            log_store,
            store.clone(),
        )
        .await
        .map_err(|err| SkobError::raft(err.to_string()))?;

        if initialize {
            let initialized = raft
                .is_initialized()
                .await
                .map_err(|err| SkobError::raft(err.to_string()))?;
            if !initialized {
                let mut nodes = BTreeMap::new();
                nodes.insert(
                    config.node_id,
                    BasicNode {
                        addr: config.advertise_addr.clone(),
                    },
                );
                raft.initialize(nodes)
                    .await
                    .map_err(|err| SkobError::raft(err.to_string()))?;
            }
            let _ = raft.trigger().elect().await;
        }

        tracing::info!(
            node_id = config.node_id,
            segments = config.segment_count,
            initialize,
            "segment coordinator started"
        );
        Ok(Arc::new(Self {
            node_id: config.node_id,
            advertise_addr: config.advertise_addr,
            segment_count: config.segment_count,
            raft,
            store,
        }))
    }

    /// Add a learner that is already serving Raft RPC, then promote it to a voter.
    pub async fn add_voter(&self, node_id: u64, advertise_addr: impl Into<String>) -> Result<()> {
        self.ensure_leader().await?;
        let addr = advertise_addr.into();
        self.raft
            .add_learner(node_id, BasicNode { addr }, true)
            .await
            .map_err(|err| SkobError::raft(err.to_string()))?;
        let mut voters: Vec<u64> = self
            .raft
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .collect();
        if !voters.contains(&node_id) {
            voters.push(node_id);
        }
        voters.sort_unstable();
        voters.dedup();
        self.raft
            .change_membership(voters, false)
            .await
            .map_err(|err| SkobError::raft(err.to_string()))?;
        Ok(())
    }

    /// Assign every segment round-robin across the current voters.
    pub async fn rebalance(&self) -> Result<()> {
        self.ensure_leader().await?;
        let metrics = self.raft.metrics().borrow().clone();
        let voter_ids: std::collections::BTreeSet<u64> = metrics.membership_config.voter_ids().collect();
        let mut voters: Vec<(u64, String)> = metrics
            .membership_config
            .nodes()
            .filter(|(id, _)| voter_ids.contains(id))
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect();
        voters.sort_by_key(|(id, _)| *id);
        if voters.is_empty() {
            return Err(SkobError::raft("rebalance found no voters"));
        }
        let mut assignments = BTreeMap::new();
        for segment in 0..self.segment_count {
            let (node_id, advertise_addr) = &voters[segment as usize % voters.len()];
            assignments.insert(
                segment,
                SegmentOwner {
                    segment,
                    node_id: *node_id,
                    advertise_addr: advertise_addr.clone(),
                },
            );
        }
        self.raft
            .client_write(Request::Rebalance { assignments })
            .await
            .map_err(|err| SkobError::raft(err.to_string()))?;
        let applied = self.store.assignments().await;
        if applied.len() != self.segment_count as usize {
            return Err(SkobError::raft(format!(
                "rebalance applied {} segments, expected {}",
                applied.len(),
                self.segment_count
            )));
        }
        Ok(())
    }

    pub async fn wait_leader(&self) -> Result<()> {
        self.raft
            .wait(Some(Duration::from_secs(20)))
            .metrics(
                |metrics| metrics.current_leader == Some(self.node_id) && metrics.state.is_leader(),
                "wait until this node is leader",
            )
            .await
            .map_err(|err| SkobError::raft(format!("leader wait failed: {err:?}")))?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|err| SkobError::raft(err.to_string()))
    }

    pub async fn handle_append(&self, body: &[u8]) -> Result<Vec<u8>> {
        let req = serde_json::from_slice(body).map_err(|err| SkobError::raft(err))?;
        let resp = self
            .raft
            .append_entries(req)
            .await
            .map_err(|err| SkobError::raft(err.to_string()))?;
        serde_json::to_vec(&resp).map_err(|err| SkobError::raft(err))
    }

    pub async fn handle_vote(&self, body: &[u8]) -> Result<Vec<u8>> {
        let req = serde_json::from_slice(body).map_err(|err| SkobError::raft(err))?;
        let resp = self.raft.vote(req).await.map_err(|err| SkobError::raft(err.to_string()))?;
        serde_json::to_vec(&resp).map_err(|err| SkobError::raft(err))
    }

    pub async fn handle_snapshot(&self, body: &[u8]) -> Result<Vec<u8>> {
        let req = serde_json::from_slice(body).map_err(|err| SkobError::raft(err))?;
        let resp = self
            .raft
            .install_snapshot(req)
            .await
            .map_err(|err| SkobError::raft(err.to_string()))?;
        serde_json::to_vec(&resp).map_err(|err| SkobError::raft(err))
    }

    async fn ensure_leader(&self) -> Result<()> {
        if self.raft.metrics().borrow().state.is_leader() {
            return Ok(());
        }
        Err(SkobError::raft(format!("node {} is not the raft leader", self.node_id)))
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn advertise_addr(&self) -> &str {
        &self.advertise_addr
    }

    pub fn segment_count(&self) -> u32 {
        self.segment_count
    }

    pub fn segment_of(&self, object_id: &ObjectId) -> u32 {
        segment_for(object_id, self.segment_count)
    }

    /// Read the applied assignment. Missing segments are an error, never "self".
    pub async fn owner(&self, segment: u32) -> Result<SegmentOwner> {
        self.store.assignments().await.get(&segment).cloned().ok_or_else(|| {
            SkobError::raft(format!("segment {segment} has no owner in the applied raft map"))
        })
    }

    pub async fn ensure_owns_object(&self, object_id: &ObjectId) -> Result<SegmentOwner> {
        self.ensure_owns(self.segment_of(object_id)).await
    }

    pub async fn ensure_owns_catalog(&self) -> Result<SegmentOwner> {
        self.ensure_owns(CATALOG_SEGMENT).await
    }

    pub async fn metrics_json(&self) -> Value {
        let metrics = self.raft.metrics().borrow().clone();
        let mut assignments = serde_json::Map::new();
        for (segment, owner) in self.store.assignments().await {
            assignments.insert(
                segment.to_string(),
                serde_json::to_value(&owner).unwrap_or(Value::Null),
            );
        }
        let voters: Vec<u64> = metrics.membership_config.voter_ids().collect();
        json!({
            "leader_id": metrics.current_leader,
            "node_id": self.node_id,
            "voters": voters,
            "assignments": assignments,
        })
    }

    async fn ensure_owns(&self, segment: u32) -> Result<SegmentOwner> {
        let owner = self.owner(segment).await?;
        if owner.node_id != self.node_id {
            return Err(SkobError::NotOwner {
                segment,
                owner_id: owner.node_id,
                owner_addr: owner.advertise_addr,
            });
        }
        Ok(owner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use skob_core::segment_for;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bootstrap_assigns_segments_through_raft() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("skob-raft-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();

        let segment_count = 16;
        let coord = SegmentCoordinator::bootstrap(RaftConfig::new(
            1,
            "http://127.0.0.1:8080",
            dir.clone(),
            segment_count,
        ))
        .await
        .expect("bootstrap");

        coord.ensure_owns_catalog().await.expect("catalog owner");
        for raw in ["alpha", "beta-2", "obj_3"] {
            let id = ObjectId::parse(raw).unwrap();
            let owner = coord.ensure_owns_object(&id).await.expect("object owner");
            let segment = segment_for(&id, segment_count);
            assert_eq!(coord.segment_of(&id), segment);
            assert_eq!(owner.segment, segment);
            assert_eq!(owner.node_id, coord.node_id());
            assert_eq!(coord.owner(segment).await.unwrap().node_id, 1);
        }

        for segment in 0..segment_count {
            let owner = coord.owner(segment).await.unwrap();
            assert_eq!(owner.node_id, 1);
            assert_eq!(owner.segment, segment);
            assert_eq!(owner.advertise_addr, "http://127.0.0.1:8080");
        }

        let metrics = coord.metrics_json().await;
        assert_eq!(metrics["node_id"], 1);
        assert_eq!(metrics["leader_id"], 1);
        assert_eq!(metrics["assignments"]["0"]["node_id"], 1);

        let _ = std::fs::remove_dir_all(dir);
    }
}
