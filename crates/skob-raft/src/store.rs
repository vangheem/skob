//! Segment-ownership state machine.
//!
//! Storage shape is the openraft v0.9.25 `raft-kv-memstore` example. The map
//! `segment -> SegmentOwner` replaces the example key/value data.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
use openraft::BasicNode;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::RaftSnapshotBuilder;
use openraft::RaftTypeConfig;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use serde::Deserialize;
use serde::Serialize;
use skob_core::SegmentOwner;
use tokio::sync::RwLock;

use crate::NodeId;
use crate::TypeConfig;

/// Client write applied by the ownership state machine.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    /// Replace the applied segment map. Every segment in `0..segment_count` is listed.
    Rebalance { assignments: BTreeMap<u32, SegmentOwner> },
}

/// Applied assignment map after a write.
pub type Response = BTreeMap<u32, SegmentOwner>;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SnapshotAssignments {
    assignments: Vec<SegmentOwner>,
}

#[derive(Debug)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, BasicNode>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,
    pub assignments: BTreeMap<u32, SegmentOwner>,
}

#[derive(Debug, Default)]
pub struct StateMachineStore {
    pub state_machine: RwLock<StateMachineData>,
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

impl StateMachineStore {
    pub async fn assignments(&self) -> BTreeMap<u32, SegmentOwner> {
        self.state_machine.read().await.assignments.clone()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let state_machine = self.state_machine.read().await;
        let body = SnapshotAssignments {
            assignments: state_machine.assignments.values().cloned().collect(),
        };
        let data = serde_json::to_vec(&body).map_err(|err| StorageIOError::read_state_machine(&err))?;
        let last_applied_log = state_machine.last_applied_log;
        let last_membership = state_machine.last_membership.clone();
        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{snapshot_idx}")
        };
        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };
        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        *current_snapshot = Some(snapshot);
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>> {
        let state_machine = self.state_machine.read().await;
        Ok((state_machine.last_applied_log, state_machine.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut res = Vec::new();
        let mut sm = self.state_machine.write().await;
        for entry in entries {
            let log_id = entry.log_id.clone();
            tracing::debug!(%log_id, "replicate segment ownership");
            sm.last_applied_log = Some(log_id.clone());
            match entry.payload {
                EntryPayload::Blank => res.push(sm.assignments.clone()),
                EntryPayload::Normal(ref req) => match req {
                    Request::Rebalance { assignments } => {
                        sm.assignments = assignments.clone();
                        res.push(sm.assignments.clone());
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(log_id), mem.clone());
                    res.push(sm.assignments.clone());
                }
            }
        }
        Ok(res)
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };
        let decoded: SnapshotAssignments = serde_json::from_slice(&new_snapshot.data)
            .map_err(|err| StorageIOError::read_snapshot(Some(new_snapshot.meta.signature()), &err))?;
        let mut assignments = BTreeMap::new();
        for owner in decoded.assignments {
            assignments.insert(owner.segment, owner);
        }
        let updated = StateMachineData {
            last_applied_log: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            assignments,
        };
        let mut state_machine = self.state_machine.write().await;
        *state_machine = updated;
        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => Ok(Some(Snapshot {
                meta: snapshot.meta.clone(),
                snapshot: Box::new(Cursor::new(snapshot.data.clone())),
            })),
            None => Ok(None),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
