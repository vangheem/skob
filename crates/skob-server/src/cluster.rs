//! In-process multi-node cluster. Writers speak real Raft HTTP RPC. Readers only read.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use serde::Serialize;
use serde_json::json;
use serde_json::Value;
use skob_core::BlobStore;
use skob_core::MemoryBlobStore;
use skob_core::Result;
use skob_core::Role;
use skob_core::SkobError;
use skob_index::IndexManager;
use skob_raft::RaftConfig;
use skob_raft::SegmentCoordinator;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::router;
use crate::AppState;

pub struct Cluster {
    pub writers: Vec<Node>,
    pub readers: Vec<Node>,
    pub segment_count: u32,
    pub root: PathBuf,
    client: reqwest::Client,
}

pub struct Node {
    pub id: u64,
    pub addr: String,
    pub state: Arc<AppState>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchReport {
    pub writers: usize,
    pub readers: usize,
    pub objects: usize,
    pub owners: usize,
    pub direct_write_ops: f64,
    pub read_ops: f64,
    pub failover_ms: u128,
}

impl Cluster {
    /// Start `writers` Raft voters and `readers` read-only processes on a shared memory blob store.
    pub async fn start(writers: usize, readers: usize) -> Result<Self> {
        if writers == 0 {
            return Err(SkobError::BadRequest("cluster needs at least one writer".into()));
        }
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("skob-cluster-{nanos}"));
        std::fs::create_dir_all(&root).map_err(SkobError::storage)?;
        let blob: Arc<dyn BlobStore> = MemoryBlobStore::new();
        let segment_count = 8u32;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .no_proxy()
            .build()
            .map_err(SkobError::storage)?;

        let mut cluster = Self {
            writers: Vec::new(),
            readers: Vec::new(),
            segment_count,
            root,
            client,
        };

        let first = cluster
            .spawn_node(1, Role::Writer, blob.clone(), true)
            .await?;
        cluster.writers.push(first);
        cluster.wait_health(&cluster.writers[0].addr).await?;

        for id in 2..=writers as u64 {
            let node = cluster.spawn_node(id, Role::Writer, blob.clone(), false).await?;
            cluster.wait_health(&node.addr).await?;
            cluster
                .post_json(
                    &format!("{}/v1/raft/join", cluster.writers[0].addr),
                    json!({ "node_id": id, "advertise_addr": node.addr }),
                )
                .await?;
            cluster.writers.push(node);
        }
        cluster.wait_converged().await?;

        for offset in 0..readers {
            let id = 100 + offset as u64;
            let node = cluster.spawn_node(id, Role::Reader, blob.clone(), false).await?;
            cluster.wait_health(&node.addr).await?;
            cluster.readers.push(node);
        }
        Ok(cluster)
    }

    pub async fn shutdown(mut self) {
        let nodes = self.writers.drain(..).chain(self.readers.drain(..));
        for mut node in nodes {
            node.stop().await;
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }

    pub async fn stop_writer(&mut self, id: u64) -> Result<()> {
        let node = self
            .writers
            .iter_mut()
            .find(|node| node.id == id)
            .ok_or_else(|| SkobError::BadRequest(format!("writer {id} is not running")))?;
        node.stop().await;
        Ok(())
    }

    pub fn writer(&self, id: u64) -> Option<&Node> {
        self.writers.iter().find(|node| node.id == id && node.task.is_some())
    }

    pub async fn metrics(&self, addr: &str) -> Result<Value> {
        let response = self
            .client
            .get(format!("{addr}/v1/raft/metrics"))
            .send()
            .await
            .map_err(SkobError::raft)?;
        let status = response.status();
        let body = response.text().await.map_err(SkobError::raft)?;
        if !status.is_success() {
            return Err(SkobError::raft(format!("metrics {status}: {body}")));
        }
        serde_json::from_str(&body).map_err(SkobError::raft)
    }

    pub async fn leader_addr(&self) -> Result<String> {
        for writer in self.writers.iter().filter(|node| node.task.is_some()) {
            let Ok(metrics) = self.metrics(&writer.addr).await else {
                continue;
            };
            let Some(leader) = metrics["leader_id"].as_u64() else {
                continue;
            };
            if let Some(node) = self.writer(leader) {
                return Ok(node.addr.clone());
            }
        }
        Err(SkobError::raft("no live raft leader"))
    }

    pub async fn put_following(&self, path: &str, body: &Value) -> Result<Value> {
        let mut addr = self
            .writers
            .iter()
            .find(|node| node.task.is_some())
            .map(|node| node.addr.clone())
            .ok_or_else(|| SkobError::raft("no live writer"))?;
        for _ in 0..8 {
            let response = self
                .client
                .put(format!("{addr}{path}"))
                .header("content-type", "application/json")
                .json(body)
                .send()
                .await
                .map_err(SkobError::storage)?;
            let status = response.status();
            let payload: Value = response.json().await.map_err(SkobError::storage)?;
            if status.as_u16() == 409 && payload["error"] == "not_owner" {
                addr = payload["owner_addr"]
                    .as_str()
                    .ok_or_else(|| SkobError::raft("not_owner response missing owner_addr"))?
                    .to_string();
                continue;
            }
            if !status.is_success() {
                return Err(SkobError::BadRequest(format!("{status} {payload}")));
            }
            return Ok(payload);
        }
        Err(SkobError::raft("write redirected more than 8 times"))
    }

    pub async fn post_following(&self, path: &str, body: &Value) -> Result<Value> {
        let mut addr = self
            .writers
            .iter()
            .find(|node| node.task.is_some())
            .map(|node| node.addr.clone())
            .ok_or_else(|| SkobError::raft("no live writer"))?;
        for _ in 0..8 {
            let response = self
                .client
                .post(format!("{addr}{path}"))
                .header("content-type", "application/json")
                .json(body)
                .send()
                .await
                .map_err(SkobError::storage)?;
            let status = response.status();
            let payload: Value = response.json().await.map_err(SkobError::storage)?;
            if status.as_u16() == 409 && payload["error"] == "not_owner" {
                addr = payload["owner_addr"]
                    .as_str()
                    .ok_or_else(|| SkobError::raft("not_owner response missing owner_addr"))?
                    .to_string();
                continue;
            }
            if !status.is_success() {
                return Err(SkobError::BadRequest(format!("{status} {payload}")));
            }
            return Ok(payload);
        }
        Err(SkobError::raft("write redirected more than 8 times"))
    }

    pub async fn get_reader(&self, reader: usize, path: &str) -> Result<Value> {
        let node = self
            .readers
            .get(reader)
            .ok_or_else(|| SkobError::BadRequest(format!("reader {reader} missing")))?;
        let response = self
            .client
            .get(format!("{}{path}", node.addr))
            .send()
            .await
            .map_err(SkobError::storage)?;
        let status = response.status();
        let payload: Value = response.json().await.map_err(SkobError::storage)?;
        if !status.is_success() {
            return Err(SkobError::BadRequest(format!("{status} {payload}")));
        }
        Ok(payload)
    }

    pub async fn rebalance_leader(&self) -> Result<Value> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut last = String::from("no attempt");
        while Instant::now() < deadline {
            let leader = self.leader_addr().await?;
            match self.post_json(&format!("{leader}/v1/raft/rebalance"), json!({})).await {
                Ok(body) => return Ok(body),
                Err(err) => last = err.to_string(),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err(SkobError::raft(format!("rebalance failed: {last}")))
    }

    pub async fn owner_ids(&self) -> Result<BTreeSet<u64>> {
        let metrics = self.metrics(&self.writers.iter().find(|node| node.task.is_some()).unwrap().addr).await?;
        let mut owners = BTreeSet::new();
        if let Some(assignments) = metrics["assignments"].as_object() {
            for owner in assignments.values() {
                if let Some(id) = owner["node_id"].as_u64() {
                    owners.insert(id);
                }
            }
        }
        Ok(owners)
    }

    async fn spawn_node(&self, id: u64, role: Role, blob: Arc<dyn BlobStore>, bootstrap: bool) -> Result<Node> {
        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(SkobError::storage)?;
        let port = listener.local_addr().map_err(SkobError::storage)?.port();
        let addr = format!("http://127.0.0.1:{port}");
        let dir = self.root.join(format!("node-{id}"));
        std::fs::create_dir_all(dir.join("indexes")).map_err(SkobError::storage)?;
        let index = IndexManager::open(blob.clone(), dir.join("indexes"), role == Role::Reader)?;
        index.start();
        let raft = if role.serves_writes() {
            let config = RaftConfig::new(id, addr.clone(), dir.join("raft"), self.segment_count).fast_elections();
            Some(if bootstrap {
                SegmentCoordinator::bootstrap(config).await?
            } else {
                SegmentCoordinator::standby(config).await?
            })
        } else {
            None
        };
        let state = Arc::new(AppState {
            blob,
            index,
            raft,
            role,
        });
        let (stop, task) = spawn_server(listener, state.clone());
        Ok(Node {
            id,
            addr,
            state,
            stop: Some(stop),
            task: Some(task),
        })
    }

    async fn wait_health(&self, addr: &str) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if self
                .client
                .get(format!("{addr}/health"))
                .send()
                .await
                .ok()
                .is_some_and(|response| response.status().is_success())
            {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(SkobError::raft(format!("{addr} did not become healthy")));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_converged(&self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(20);
        let expected = self.writers.len();
        loop {
            let mut maps = Vec::new();
            let mut ready = true;
            for writer in &self.writers {
                match self.metrics(&writer.addr).await {
                    Ok(metrics) => {
                        let voters = metrics["voters"].as_array().map(|items| items.len()).unwrap_or(0);
                        if voters != expected {
                            ready = false;
                        }
                        maps.push(metrics["assignments"].to_string());
                    }
                    Err(_) => ready = false,
                }
            }
            if ready && maps.windows(2).all(|pair| pair[0] == pair[1]) && self.owner_ids().await?.len() >= expected.min(2) {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(SkobError::raft("writers did not converge on one assignment map"));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn post_json(&self, url: &str, body: Value) -> Result<Value> {
        let response = self.client.post(url).json(&body).send().await.map_err(SkobError::raft)?;
        let status = response.status();
        let payload: Value = response.json().await.map_err(SkobError::raft)?;
        if !status.is_success() {
            return Err(SkobError::raft(format!("{status} {payload}")));
        }
        Ok(payload)
    }
}

impl Node {
    async fn stop(&mut self) {
        if let Some(raft) = &self.state.raft {
            let _ = raft.shutdown().await;
        }
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
        }
    }
}

fn spawn_server(listener: TcpListener, state: Arc<AppState>) -> (oneshot::Sender<()>, JoinHandle<()>) {
    let (stop, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let _ = axum::serve(listener, router(state))
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .await;
    });
    (stop, task)
}

pub async fn benchmark(writers: usize, readers: usize, objects: usize) -> Result<BenchReport> {
    let cluster = Cluster::start(writers, readers).await?;
    cluster
        .post_following("/v1/projects", &json!({"id": "bench", "name": "Bench"}))
        .await?;
    cluster
        .post_following(
            "/v1/projects/bench/collections",
            &json!({"id": "docs", "name": "Docs"}),
        )
        .await?;
    let owners = cluster.owner_ids().await?.len();

    let started = Instant::now();
    for n in 0..objects {
        cluster
            .put_following(
                &format!("/v1/projects/bench/collections/docs/objects/obj{n}"),
                &json!({"n": n}),
            )
            .await?;
    }
    let write_secs = started.elapsed().as_secs_f64().max(0.000_001);

    let started = Instant::now();
    for n in 0..objects {
        let reader = n % readers.max(1);
        let got = cluster
            .get_reader(reader, &format!("/v1/projects/bench/collections/docs/objects/obj{n}"))
            .await?;
        if got["body"]["n"] != n {
            return Err(SkobError::BadRequest(format!("reader returned {got}")));
        }
    }
    let read_secs = started.elapsed().as_secs_f64().max(0.000_001);

    let leader = cluster.leader_addr().await?;
    let leader_id = cluster
        .writers
        .iter()
        .find(|node| node.addr == leader)
        .map(|node| node.id)
        .unwrap_or(1);
    let failover_started = Instant::now();
    // The cluster is moved into a local so we can stop the leader. Rebuild a small handle.
    let mut cluster = cluster;
    cluster.stop_writer(leader_id).await?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if cluster.leader_addr().await.is_ok() {
            break;
        }
        if Instant::now() > deadline {
            cluster.shutdown().await;
            return Err(SkobError::raft("no leader after failover"));
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    cluster.rebalance_leader().await?;
    let failover_ms = failover_started.elapsed().as_millis();
    cluster.shutdown().await;

    Ok(BenchReport {
        writers,
        readers,
        objects,
        owners,
        direct_write_ops: objects as f64 / write_secs,
        read_ops: objects as f64 / read_secs,
        failover_ms,
    })
}
