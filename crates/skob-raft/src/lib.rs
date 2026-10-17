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

/// Process settings for the single-node ownership group.
#[derive(Clone, Debug)]
pub struct RaftConfig {
    pub node_id: u64,
    pub advertise_addr: String,
    pub data_dir: PathBuf,
    pub segment_count: u32,
}

/// Live `openraft::Raft` handle plus the applied segment map.
pub struct SegmentCoordinator {
    node_id: u64,
    segment_count: u32,
    raft: SkobRaft,
    store: Arc<store::StateMachineStore>,
}

impl SegmentCoordinator {
    /// Build a real `openraft::Raft`, initialize a one-node group, and assign
    /// every segment to this node through `client_write`.
    ///
    /// The log is in memory, so a fresh process (empty directory) initializes
    /// again. Ownership checks always read the map produced by Raft apply.
    pub async fn bootstrap(config: RaftConfig) -> Result<Arc<Self>> {
        if config.segment_count == 0 {
            return Err(SkobError::BadRequest("segment_count must be non-zero".into()));
        }
        std::fs::create_dir_all(&config.data_dir).map_err(|err| SkobError::raft(err))?;

        let raft_config = Config {
            cluster_name: "skob".into(),
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
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

        // Kick an election so a single voter does not wait out a cold timeout.
        let _ = raft.trigger().elect().await;
        raft.wait(Some(Duration::from_secs(20)))
            .metrics(
                |metrics| metrics.current_leader == Some(config.node_id) && metrics.state.is_leader(),
                "wait until this node is leader",
            )
            .await
            .map_err(|err| SkobError::raft(format!("leader wait failed: {err:?}")))?;

        let already = store.assignments().await;
        let complete = (0..config.segment_count).all(|segment| {
            already
                .get(&segment)
                .is_some_and(|owner| owner.node_id == config.node_id && owner.segment == segment)
        });
        if !complete {
            let mut assignments = BTreeMap::new();
            for segment in 0..config.segment_count {
                assignments.insert(
                    segment,
                    SegmentOwner {
                        segment,
                        node_id: config.node_id,
                        advertise_addr: config.advertise_addr.clone(),
                    },
                );
            }
            raft.client_write(Request::Rebalance { assignments })
                .await
                .map_err(|err| SkobError::raft(err.to_string()))?;
        }

        let applied = store.assignments().await;
        if applied.len() != config.segment_count as usize {
            return Err(SkobError::raft(format!(
                "rebalance applied {} segments, expected {}",
                applied.len(),
                config.segment_count
            )));
        }

        tracing::info!(
            node_id = config.node_id,
            segments = config.segment_count,
            "segment coordinator bootstrapped"
        );
        Ok(Arc::new(Self {
            node_id: config.node_id,
            segment_count: config.segment_count,
            raft,
            store,
        }))
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
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
        json!({
            "leader_id": metrics.current_leader,
            "node_id": self.node_id,
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
        let coord = SegmentCoordinator::bootstrap(RaftConfig {
            node_id: 1,
            advertise_addr: "http://127.0.0.1:8080".into(),
            data_dir: dir.clone(),
            segment_count,
        })
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
