//! Tantivy-backed collection indexes. Local disk is a cache; blob storage is durability.

mod schema;

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Term, Value as TantivyValue};
use tantivy::{Index, IndexWriter, TantivyDocument};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use skob_core::{
    index_prefix, index_segment_prefix, index_status_key, index_tree_prefix, object_prefix,
    parse_object_key, to_json_bytes, validate_field_path, validate_index_name, BlobStore,
    CollectionId, IndexSpec, IndexState, IndexStatus, ObjectId, ProjectId, Result, SearchHit,
    SearchRequest, SkobError, StoredObject,
};

use schema::BuiltSchema;

const WRITER_MEMORY: usize = 20_000_000;

/// One Tantivy index per `(project, collection, index name)`.
pub struct IndexManager {
    store: Arc<dyn BlobStore>,
    local_root: PathBuf,
    read_only: bool,
    slots: tokio::sync::Mutex<HashMap<IndexKey, Arc<Slot>>>,
    jobs: Mutex<JobQueue>,
    job_notify: Notify,
    idle: Notify,
    worker: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct IndexKey {
    project: String,
    collection: String,
    name: String,
}

impl IndexKey {
    fn new(project: &ProjectId, collection: &CollectionId, name: &str) -> Self {
        Self {
            project: project.as_str().to_string(),
            collection: collection.as_str().to_string(),
            name: name.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Unloaded,
    Building,
    Ready,
    Dropping,
    Absent,
}

#[derive(Clone, Debug)]
enum PendingOp {
    Upsert(Value),
    Remove,
}

struct SlotInner {
    generation: u64,
    phase: Phase,
    pending: HashMap<String, PendingOp>,
    open: Option<OpenIndex>,
    /// Set by `forget_local` so an in-flight rebuild does not publish over a dropped cache.
    cancelled: bool,
}

impl Default for SlotInner {
    fn default() -> Self {
        Self {
            generation: 0,
            phase: Phase::Unloaded,
            pending: HashMap::new(),
            open: None,
            cancelled: false,
        }
    }
}

struct Slot {
    inner: tokio::sync::Mutex<SlotInner>,
}

impl Default for Slot {
    fn default() -> Self {
        Self {
            inner: tokio::sync::Mutex::new(SlotInner::default()),
        }
    }
}

struct OpenIndex {
    index: Index,
    writer: Option<IndexWriter>,
    schema: BuiltSchema,
    /// `IndexState.generation` this handle was opened for.
    generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JobKind {
    Rebuild,
    Drop,
}

struct Job {
    key: IndexKey,
    generation: u64,
    kind: JobKind,
}

struct JobQueue {
    queue: VecDeque<Job>,
    running: bool,
}

impl Default for JobQueue {
    fn default() -> Self {
        Self {
            queue: VecDeque::new(),
            running: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Manifest {
    /// Monotonic publish counter. Readers refresh when this is newer than the local copy.
    generation: u64,
    #[serde(default)]
    index_generation: u64,
    files: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalMeta {
    generation: u64,
}

impl IndexManager {
    pub fn open(
        store: Arc<dyn BlobStore>,
        local_root: PathBuf,
        read_only: bool,
    ) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&local_root).map_err(SkobError::index)?;
        Ok(Arc::new(Self {
            store,
            local_root,
            read_only,
            slots: tokio::sync::Mutex::new(HashMap::new()),
            jobs: Mutex::new(JobQueue::default()),
            job_notify: Notify::new(),
            idle: Notify::new(),
            worker: Mutex::new(None),
        }))
    }

    pub fn start(self: &Arc<Self>) {
        let mut worker = self.worker.lock().expect("index worker lock");
        if worker.is_some() {
            return;
        }
        {
            let mut jobs = self.jobs.lock().expect("index job lock");
            jobs.running = true;
        }
        let this = Arc::clone(self);
        *worker = Some(tokio::spawn(async move {
            if !this.read_only {
                if let Err(err) = this.resume_inflight().await {
                    tracing::warn!(error = %err, "failed to resume in-flight index jobs");
                }
            }
            this.worker_loop().await;
        }));
    }

    pub async fn upsert_index(
        &self,
        project: &ProjectId,
        collection: &CollectionId,
        spec: IndexSpec,
    ) -> Result<IndexState> {
        self.require_writable()?;
        validate_spec(&spec)?;
        let key = IndexKey::new(project, collection, &spec.name);
        let slot = self.slot(&key).await;
        let mut guard = slot.inner.lock().await;
        if guard.cancelled {
            guard.cancelled = false;
        }
        let previous = self.read_status(&key).await?;
        let generation = guard
            .generation
            .max(previous.as_ref().map(|state| state.generation).unwrap_or(0))
            .saturating_add(1);
        guard.generation = generation;
        guard.phase = Phase::Building;
        guard.open = None;
        guard.pending.clear();
        let state = IndexState {
            spec,
            status: IndexStatus::Building,
            generation,
            indexed: 0,
            error: None,
        };
        self.write_status(&key, &state).await?;
        drop(guard);
        self.enqueue(Job {
            key,
            generation,
            kind: JobKind::Rebuild,
        });
        Ok(state)
    }

    pub async fn remove_index(
        &self,
        project: &ProjectId,
        collection: &CollectionId,
        name: &str,
    ) -> Result<()> {
        self.require_writable()?;
        validate_index_name(name)?;
        let key = IndexKey::new(project, collection, name);
        let slot = self.slot(&key).await;
        let mut guard = slot.inner.lock().await;
        let Some(mut state) = self.read_status(&key).await? else {
            return Err(SkobError::NotFound {
                entity: "index",
                id: name.to_string(),
            });
        };
        let generation = guard.generation.max(state.generation).saturating_add(1);
        guard.generation = generation;
        guard.phase = Phase::Dropping;
        guard.open = None;
        guard.pending.clear();
        state.status = IndexStatus::Dropping;
        state.generation = generation;
        state.indexed = 0;
        state.error = None;
        self.write_status(&key, &state).await?;
        drop(guard);
        self.enqueue(Job {
            key,
            generation,
            kind: JobKind::Drop,
        });
        Ok(())
    }

    pub async fn get_index(
        &self,
        project: &ProjectId,
        collection: &CollectionId,
        name: &str,
    ) -> Result<Option<IndexState>> {
        let key = IndexKey::new(project, collection, name);
        self.read_status(&key).await
    }

    pub async fn list_indexes(
        &self,
        project: &ProjectId,
        collection: &CollectionId,
    ) -> Result<Vec<IndexState>> {
        let prefix = index_prefix(project, collection);
        let metas = self.list_all(&prefix).await?;
        let mut states = Vec::new();
        for meta in metas {
            let Some(rest) = meta.key.strip_prefix(&prefix) else {
                continue;
            };
            let Some(name) = rest.strip_suffix("/status.json") else {
                continue;
            };
            if name.is_empty() || name.contains('/') {
                continue;
            }
            if let Some(state) = self.get_index(project, collection, name).await? {
                states.push(state);
            }
        }
        states.sort_by(|left, right| left.spec.name.cmp(&right.spec.name));
        Ok(states)
    }

    pub async fn sync_object(
        &self,
        project: &ProjectId,
        collection: &CollectionId,
        object_id: &ObjectId,
        body: &Value,
    ) -> Result<()> {
        self.apply_mutation(
            project,
            collection,
            object_id,
            PendingOp::Upsert(body.clone()),
        )
        .await
    }

    pub async fn remove_object(
        &self,
        project: &ProjectId,
        collection: &CollectionId,
        object_id: &ObjectId,
    ) -> Result<()> {
        self.apply_mutation(project, collection, object_id, PendingOp::Remove)
            .await
    }

    pub async fn search(
        &self,
        project: &ProjectId,
        collection: &CollectionId,
        request: SearchRequest,
    ) -> Result<Vec<SearchHit>> {
        let request = request.normalized()?;
        if let Some(field) = &request.field {
            validate_field_path(field)?;
        }
        let state = self
            .get_index(project, collection, &request.index)
            .await?
            .ok_or_else(|| SkobError::NotFound {
                entity: "index",
                id: request.index.clone(),
            })?;
        if state.status != IndexStatus::Ready {
            return Err(SkobError::IndexNotReady {
                name: request.index.clone(),
                status: state.status.as_str().to_string(),
            });
        }
        let key = IndexKey::new(project, collection, &request.index);
        let slot = self.slot(&key).await;
        let mut guard = slot.inner.lock().await;
        if guard.cancelled || matches!(guard.phase, Phase::Building | Phase::Dropping | Phase::Absent)
        {
            return Err(SkobError::IndexNotReady {
                name: request.index.clone(),
                status: match guard.phase {
                    Phase::Dropping => IndexStatus::Dropping.as_str(),
                    Phase::Absent => "dropping",
                    _ => IndexStatus::Building.as_str(),
                }
                .to_string(),
            });
        }
        self.ensure_open(&mut guard, &key, &state, !self.read_only)
            .await?;
        let open = guard
            .open
            .as_ref()
            .ok_or_else(|| SkobError::index(format!("index `{}` is not open", request.index)))?;
        search_open(open, &request)
    }

    pub async fn forget_local(
        &self,
        project: &ProjectId,
        collection: Option<&CollectionId>,
    ) -> Result<()> {
        let project_name = project.as_str().to_string();
        let collection_name = collection.map(|id| id.as_str().to_string());
        let matches = |key: &IndexKey| {
            key.project == project_name
                && collection_name
                    .as_ref()
                    .map(|name| key.collection == *name)
                    .unwrap_or(true)
        };
        let doomed = {
            let slots = self.slots.lock().await;
            slots
                .iter()
                .filter(|(key, _)| matches(key))
                .map(|(_, slot)| Arc::clone(slot))
                .collect::<Vec<_>>()
        };
        for slot in doomed {
            let mut guard = slot.inner.lock().await;
            guard.open = None;
            guard.phase = Phase::Absent;
            guard.pending.clear();
            guard.cancelled = true;
            guard.generation = u64::MAX;
        }
        {
            let mut slots = self.slots.lock().await;
            slots.retain(|key, _| !matches(key));
        }
        let path = match collection {
            Some(id) => self.local_root.join(project.as_str()).join(id.as_str()),
            None => self.local_root.join(project.as_str()),
        };
        if path.exists() {
            std::fs::remove_dir_all(&path).map_err(SkobError::index)?;
        }
        Ok(())
    }

    /// Wait until the background worker has applied every queued rebuild/drop.
    pub async fn wait_idle(&self) -> Result<()> {
        loop {
            let notified = self.idle.notified();
            let idle = {
                let jobs = self.jobs.lock().expect("index job lock");
                jobs.queue.is_empty() && !jobs.running
            };
            if idle {
                return Ok(());
            }
            notified.await;
        }
    }

    fn require_writable(&self) -> Result<()> {
        if self.read_only {
            Err(SkobError::WrongRole("writes"))
        } else {
            Ok(())
        }
    }

    async fn slot(&self, key: &IndexKey) -> Arc<Slot> {
        let mut slots = self.slots.lock().await;
        slots
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Slot::default()))
            .clone()
    }

    fn enqueue(&self, job: Job) {
        {
            let mut jobs = self.jobs.lock().expect("index job lock");
            if let Some(existing) = jobs.queue.iter_mut().find(|queued| queued.key == job.key) {
                if job.generation >= existing.generation {
                    *existing = job;
                }
            } else {
                jobs.queue.push_back(job);
            }
        }
        self.job_notify.notify_one();
    }

    async fn worker_loop(self: Arc<Self>) {
        loop {
            let job = {
                let mut jobs = self.jobs.lock().expect("index job lock");
                if let Some(job) = jobs.queue.pop_front() {
                    jobs.running = true;
                    Some(job)
                } else {
                    jobs.running = false;
                    None
                }
            };
            if let Some(job) = job {
                self.execute(job).await;
                let mut jobs = self.jobs.lock().expect("index job lock");
                jobs.running = false;
                if jobs.queue.is_empty() {
                    drop(jobs);
                    self.idle.notify_waiters();
                }
            } else {
                self.idle.notify_waiters();
                let notified = self.job_notify.notified();
                let empty = self.jobs.lock().expect("index job lock").queue.is_empty();
                if empty {
                    notified.await;
                }
            }
        }
    }

    async fn execute(&self, job: Job) {
        let result = match job.kind {
            JobKind::Rebuild => self.rebuild(&job).await,
            JobKind::Drop => self.drop_index(&job).await,
        };
        if let Err(err) = result {
            tracing::warn!(index = %job.key.name, error = %err, "index job failed");
            if job.kind == JobKind::Rebuild {
                if let Err(mark_err) = self.mark_failed(&job, &err).await {
                    tracing::warn!(error = %mark_err, "failed to record index failure");
                }
            }
        }
    }

    async fn resume_inflight(&self) -> Result<()> {
        let metas = self.list_all("projects/").await?;
        for meta in metas {
            let Some((project, collection, name)) = parse_status_key(&meta.key) else {
                continue;
            };
            let key = IndexKey::new(&project, &collection, &name);
            let Some(state) = self.read_status(&key).await? else {
                continue;
            };
            let kind = match state.status {
                IndexStatus::Building => JobKind::Rebuild,
                IndexStatus::Dropping => JobKind::Drop,
                IndexStatus::Ready | IndexStatus::Failed => continue,
            };
            self.enqueue(Job {
                key,
                generation: state.generation,
                kind,
            });
        }
        Ok(())
    }

    async fn rebuild(&self, job: &Job) -> Result<()> {
        let slot = self.slot(&job.key).await;
        {
            let mut guard = slot.inner.lock().await;
            if !Self::claim_rebuild(&mut guard, job) {
                return Ok(());
            }
        }

        let Some(status) = self.read_status(&job.key).await? else {
            return Ok(());
        };
        if status.generation != job.generation || status.status == IndexStatus::Dropping {
            return Ok(());
        }
        let spec = status.spec;
        let built = BuiltSchema::create(&spec)?;
        let dir = self.index_dir(&job.key);
        if dir.exists() {
            std::fs::remove_dir_all(&dir).map_err(SkobError::index)?;
        }
        std::fs::create_dir_all(&dir).map_err(SkobError::index)?;
        let index = Index::create_in_dir(&dir, built.schema.clone()).map_err(SkobError::index)?;
        let writer = index
            .writer_with_num_threads::<TantivyDocument>(1, WRITER_MEMORY)
            .map_err(SkobError::index)?;
        writer.set_merge_policy(Box::new(tantivy::indexer::NoMergePolicy));
        let mut writer = Some(writer);

        let mut docs = self.scan_objects(&job.key, &built).await?;
        loop {
            let batch = {
                let mut guard = slot.inner.lock().await;
                if guard.cancelled || guard.generation != job.generation {
                    return Ok(());
                }
                std::mem::take(&mut guard.pending)
            };
            if batch.is_empty() {
                break;
            }
            apply_pending_to_map(&built, &mut docs, batch);
        }

        let mut indexed_ids: HashSet<String> = docs.keys().cloned().collect();
        {
            let writer = writer
                .as_mut()
                .ok_or_else(|| SkobError::index("index writer closed during rebuild"))?;
            for doc in docs.into_values() {
                writer.add_document(doc).map_err(SkobError::index)?;
            }
            writer.commit().map_err(SkobError::index)?;
        }
        self.upload_dir(&job.key, &dir, job.generation).await?;

        loop {
            let batch = {
                let mut guard = slot.inner.lock().await;
                if guard.cancelled || guard.generation != job.generation {
                    return Ok(());
                }
                if guard.pending.is_empty() {
                    let state = IndexState {
                        spec: spec.clone(),
                        status: IndexStatus::Ready,
                        generation: job.generation,
                        indexed: indexed_ids.len() as u64,
                        error: None,
                    };
                    self.write_status(&job.key, &state).await?;
                    guard.phase = Phase::Ready;
                    guard.open = Some(OpenIndex {
                        index: index.clone(),
                        writer: writer.take(),
                        schema: built.clone(),
                        generation: job.generation,
                    });
                    tracing::info!(
                        index = %job.key.name,
                        generation = job.generation,
                        indexed = indexed_ids.len(),
                        "index ready"
                    );
                    return Ok(());
                }
                std::mem::take(&mut guard.pending)
            };
            let writer = writer
                .as_mut()
                .ok_or_else(|| SkobError::index("index writer closed during rebuild"))?;
            for (id, op) in batch {
                writer.delete_term(Term::from_field_text(built.object_id, &id));
                match op {
                    PendingOp::Remove => {
                        indexed_ids.remove(&id);
                    }
                    PendingOp::Upsert(body) => {
                        writer
                            .add_document(built.document(&id, &body))
                            .map_err(SkobError::index)?;
                        indexed_ids.insert(id);
                    }
                }
            }
            writer.commit().map_err(SkobError::index)?;
            self.upload_dir(&job.key, &dir, job.generation).await?;
        }
    }

    fn claim_rebuild(guard: &mut SlotInner, job: &Job) -> bool {
        if guard.cancelled || guard.generation > job.generation {
            return false;
        }
        if guard.generation == job.generation
            && matches!(guard.phase, Phase::Ready | Phase::Absent | Phase::Dropping)
        {
            return false;
        }
        guard.generation = job.generation;
        guard.phase = Phase::Building;
        guard.open = None;
        guard.pending.clear();
        true
    }

    async fn drop_index(&self, job: &Job) -> Result<()> {
        let slot = self.slot(&job.key).await;
        let mut guard = slot.inner.lock().await;
        if guard.cancelled || guard.generation > job.generation {
            return Ok(());
        }
        if guard.generation == job.generation && guard.phase == Phase::Absent {
            return Ok(());
        }
        guard.generation = job.generation;
        guard.phase = Phase::Dropping;
        guard.open = None;
        guard.pending.clear();
        let dir = self.index_dir(&job.key);
        if dir.exists() {
            std::fs::remove_dir_all(&dir).map_err(SkobError::index)?;
        }
        let sidecar = self.sidecar_path(&job.key);
        if sidecar.exists() {
            std::fs::remove_file(&sidecar).map_err(SkobError::index)?;
        }
        if guard.generation != job.generation || guard.cancelled {
            return Ok(());
        }
        let (project, collection) = ids(&job.key)?;
        self.store
            .delete_prefix(&index_tree_prefix(&project, &collection, &job.key.name))
            .await?;
        guard.phase = Phase::Absent;
        guard.open = None;
        Ok(())
    }

    async fn mark_failed(&self, job: &Job, err: &SkobError) -> Result<()> {
        let slot = self.slot(&job.key).await;
        let mut guard = slot.inner.lock().await;
        if guard.cancelled || guard.generation != job.generation {
            return Ok(());
        }
        let Some(mut state) = self.read_status(&job.key).await? else {
            return Ok(());
        };
        if state.generation != job.generation {
            return Ok(());
        }
        state.status = IndexStatus::Failed;
        state.error = Some(err.to_string());
        self.write_status(&job.key, &state).await?;
        guard.phase = Phase::Unloaded;
        guard.open = None;
        Ok(())
    }

    async fn apply_mutation(
        &self,
        project: &ProjectId,
        collection: &CollectionId,
        object_id: &ObjectId,
        op: PendingOp,
    ) -> Result<()> {
        self.require_writable()?;
        let states = self.list_indexes(project, collection).await?;
        for state in states {
            if state.status == IndexStatus::Dropping {
                continue;
            }
            let key = IndexKey::new(project, collection, &state.spec.name);
            let slot = self.slot(&key).await;
            let mut guard = slot.inner.lock().await;
            if guard.cancelled {
                continue;
            }
            if guard.phase == Phase::Unloaded || guard.phase == Phase::Absent {
                guard.generation = state.generation;
                guard.phase = phase_of(state.status);
            } else if state.generation > guard.generation && guard.phase != Phase::Building {
                guard.generation = state.generation;
                guard.phase = phase_of(state.status);
            }
            match guard.phase {
                Phase::Building => {
                    guard
                        .pending
                        .insert(object_id.as_str().to_string(), op.clone());
                }
                Phase::Ready => {
                    self.ensure_open(&mut guard, &key, &state, true).await?;
                    self.apply_live(&mut guard, &key, object_id.as_str(), &op)
                        .await?;
                }
                Phase::Dropping | Phase::Absent | Phase::Unloaded => {}
            }
        }
        Ok(())
    }

    async fn apply_live(
        &self,
        guard: &mut SlotInner,
        key: &IndexKey,
        object_id: &str,
        op: &PendingOp,
    ) -> Result<()> {
        let open = guard
            .open
            .as_mut()
            .ok_or_else(|| SkobError::index(format!("index `{}` is not open", key.name)))?;
        let generation = open.generation;
        let dir = self.index_dir(key);
        {
            let writer = open.writer.as_mut().ok_or_else(|| {
                SkobError::index(format!("index `{}` has no writer", key.name))
            })?;
            writer.delete_term(Term::from_field_text(open.schema.object_id, object_id));
            if let PendingOp::Upsert(body) = op {
                writer
                    .add_document(open.schema.document(object_id, body))
                    .map_err(SkobError::index)?;
            }
            writer.commit().map_err(SkobError::index)?;
        }
        self.upload_dir(key, &dir, generation).await?;
        Ok(())
    }

    async fn ensure_open(
        &self,
        guard: &mut SlotInner,
        key: &IndexKey,
        state: &IndexState,
        writable: bool,
    ) -> Result<()> {
        if let Some(open) = &guard.open {
            if open.generation == state.generation && (!writable || open.writer.is_some()) {
                return Ok(());
            }
        }
        guard.open = None;
        self.refresh_local(key).await?;
        let dir = self.index_dir(key);
        let open = if writable {
            open_with_writer(&dir, &state.spec, state.generation)?
        } else {
            open_reader(&dir, &state.spec, state.generation)?
        };
        guard.generation = state.generation;
        guard.phase = Phase::Ready;
        guard.open = Some(open);
        Ok(())
    }

    async fn refresh_local(&self, key: &IndexKey) -> Result<()> {
        let remote = self.remote_manifest_generation(key).await?;
        let local = self.local_generation(key);
        let meta = self.index_dir(key).join("meta.json");
        if meta.exists() && remote <= local && local > 0 {
            return Ok(());
        }
        if remote == 0 {
            return Err(SkobError::index(format!(
                "index `{}` has no durable segments",
                key.name
            )));
        }
        self.download(key).await
    }

    async fn scan_objects(
        &self,
        key: &IndexKey,
        built: &BuiltSchema,
    ) -> Result<HashMap<String, TantivyDocument>> {
        let (project, collection) = ids(key)?;
        let metas = self.list_all(&object_prefix(&project, &collection)).await?;
        let mut docs = HashMap::new();
        for meta in metas {
            let Some((_, _, object_id)) = parse_object_key(&meta.key) else {
                continue;
            };
            let Some(bytes) = self.store.get(&meta.key).await? else {
                continue;
            };
            let stored: StoredObject = serde_json::from_slice(&bytes).map_err(|err| {
                SkobError::index(format!("object `{}` is not a StoredObject: {err}", meta.key))
            })?;
            let id = stored.id.as_str().to_string();
            let _ = object_id;
            docs.insert(id.clone(), built.document(&id, &stored.body));
        }
        Ok(docs)
    }

    async fn upload_dir(&self, key: &IndexKey, dir: &Path, index_generation: u64) -> Result<()> {
        let (project, collection) = ids(key)?;
        let files = list_index_files(dir)?;
        let prefix = index_segment_prefix(&project, &collection, &key.name);
        let mut relatives = Vec::with_capacity(files.len());
        for path in &files {
            let relative = relative_path(dir, path)?;
            let bytes = std::fs::read(path).map_err(SkobError::index)?;
            self.store
                .put(&format!("{prefix}{relative}"), Bytes::from(bytes), None)
                .await?;
            relatives.push(relative);
        }
        let remote = self.remote_manifest_generation(key).await?;
        let generation = remote.saturating_add(1).max(index_generation);
        let manifest = Manifest {
            generation,
            index_generation,
            files: relatives.clone(),
        };
        let manifest_key = format!(
            "{}manifest.json",
            index_tree_prefix(&project, &collection, &key.name)
        );
        self.store
            .put(
                &manifest_key,
                to_json_bytes(&manifest)?,
                Some("application/json"),
            )
            .await?;
        let keep: HashSet<String> = relatives
            .iter()
            .map(|relative| format!("{prefix}{relative}"))
            .collect();
        for meta in self.list_all(&prefix).await? {
            if !keep.contains(&meta.key) {
                self.store.delete(&meta.key).await?;
            }
        }
        self.write_sidecar(key, generation)?;
        Ok(())
    }

    async fn download(&self, key: &IndexKey) -> Result<()> {
        let (project, collection) = ids(key)?;
        let manifest_key = format!(
            "{}manifest.json",
            index_tree_prefix(&project, &collection, &key.name)
        );
        let Some(bytes) = self.store.get(&manifest_key).await? else {
            return Err(SkobError::index(format!(
                "index `{}` manifest is missing",
                key.name
            )));
        };
        let manifest: Manifest = serde_json::from_slice(&bytes).map_err(SkobError::index)?;
        let dir = self.index_dir(key);
        if dir.exists() {
            std::fs::remove_dir_all(&dir).map_err(SkobError::index)?;
        }
        std::fs::create_dir_all(&dir).map_err(SkobError::index)?;
        let prefix = index_segment_prefix(&project, &collection, &key.name);
        for relative in &manifest.files {
            let Some(body) = self.store.get(&format!("{prefix}{relative}")).await? else {
                return Err(SkobError::index(format!(
                    "index `{}` is missing segment `{relative}`",
                    key.name
                )));
            };
            let path = safe_join(&dir, relative)?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(SkobError::index)?;
            }
            std::fs::write(&path, &body).map_err(SkobError::index)?;
        }
        self.write_sidecar(key, manifest.generation)?;
        Ok(())
    }

    async fn remote_manifest_generation(&self, key: &IndexKey) -> Result<u64> {
        let (project, collection) = ids(key)?;
        let manifest_key = format!(
            "{}manifest.json",
            index_tree_prefix(&project, &collection, &key.name)
        );
        let Some(bytes) = self.store.get(&manifest_key).await? else {
            return Ok(0);
        };
        let manifest: Manifest = serde_json::from_slice(&bytes).map_err(SkobError::index)?;
        Ok(manifest.generation)
    }

    fn local_generation(&self, key: &IndexKey) -> u64 {
        let Ok(bytes) = std::fs::read(self.sidecar_path(key)) else {
            return 0;
        };
        serde_json::from_slice::<LocalMeta>(&bytes)
            .map(|meta| meta.generation)
            .unwrap_or(0)
    }

    fn write_sidecar(&self, key: &IndexKey, generation: u64) -> Result<()> {
        let path = self.sidecar_path(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(SkobError::index)?;
        }
        let bytes = serde_json::to_vec(&LocalMeta { generation }).map_err(SkobError::index)?;
        std::fs::write(path, bytes).map_err(SkobError::index)?;
        Ok(())
    }

    async fn read_status(&self, key: &IndexKey) -> Result<Option<IndexState>> {
        let (project, collection) = ids(key)?;
        let blob_key = index_status_key(&project, &collection, &key.name);
        let Some(bytes) = self.store.get(&blob_key).await? else {
            return Ok(None);
        };
        let state = serde_json::from_slice(&bytes)
            .map_err(|err| SkobError::index(format!("index status `{}`: {err}", key.name)))?;
        Ok(Some(state))
    }

    async fn write_status(&self, key: &IndexKey, state: &IndexState) -> Result<()> {
        let (project, collection) = ids(key)?;
        let blob_key = index_status_key(&project, &collection, &key.name);
        self.store
            .put(
                &blob_key,
                to_json_bytes(state)?,
                Some("application/json"),
            )
            .await?;
        Ok(())
    }

    async fn list_all(&self, prefix: &str) -> Result<Vec<skob_core::BlobMeta>> {
        let mut all = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.store.list(prefix, cursor.as_deref()).await?;
            let next = page.cursor.clone();
            if page.objects.is_empty() {
                break;
            }
            all.extend(page.objects);
            match next {
                Some(next) if cursor.as_deref() != Some(next.as_str()) => cursor = Some(next),
                _ => break,
            }
        }
        Ok(all)
    }

    fn index_dir(&self, key: &IndexKey) -> PathBuf {
        self.local_root
            .join(&key.project)
            .join(&key.collection)
            .join(&key.name)
    }

    fn sidecar_path(&self, key: &IndexKey) -> PathBuf {
        self.local_root
            .join(&key.project)
            .join(&key.collection)
            .join(format!("{}.skob.json", key.name))
    }
}

fn phase_of(status: IndexStatus) -> Phase {
    match status {
        IndexStatus::Building => Phase::Building,
        IndexStatus::Ready => Phase::Ready,
        IndexStatus::Dropping => Phase::Dropping,
        IndexStatus::Failed => Phase::Unloaded,
    }
}

fn validate_spec(spec: &IndexSpec) -> Result<()> {
    validate_index_name(&spec.name)?;
    if spec.fields.is_empty() {
        return Err(SkobError::BadRequest(
            "index must declare at least one field".into(),
        ));
    }
    for field in &spec.fields {
        validate_field_path(&field.path)?;
    }
    BuiltSchema::create(spec).map(|_| ())
}

fn ids(key: &IndexKey) -> Result<(ProjectId, CollectionId)> {
    Ok((
        ProjectId::parse(&key.project)?,
        CollectionId::parse(&key.collection)?,
    ))
}

fn parse_status_key(key: &str) -> Option<(ProjectId, CollectionId, String)> {
    let rest = key.strip_prefix("projects/")?;
    let (project, rest) = rest.split_once("/collections/")?;
    let (collection, rest) = rest.split_once("/indexes/")?;
    let name = rest.strip_suffix("/status.json")?;
    if name.is_empty() || name.contains('/') {
        return None;
    }
    Some((
        ProjectId::parse(project).ok()?,
        CollectionId::parse(collection).ok()?,
        name.to_string(),
    ))
}

fn apply_pending_to_map(
    built: &BuiltSchema,
    docs: &mut HashMap<String, TantivyDocument>,
    batch: HashMap<String, PendingOp>,
) {
    for (id, op) in batch {
        match op {
            PendingOp::Remove => {
                docs.remove(&id);
            }
            PendingOp::Upsert(body) => {
                docs.insert(id.clone(), built.document(&id, &body));
            }
        }
    }
}

fn open_with_writer(dir: &Path, spec: &IndexSpec, generation: u64) -> Result<OpenIndex> {
    let index = Index::open_in_dir(dir).map_err(SkobError::index)?;
    let schema = BuiltSchema::bind(&index, spec)?;
    let writer = index
        .writer_with_num_threads::<TantivyDocument>(1, WRITER_MEMORY)
        .map_err(SkobError::index)?;
    writer.set_merge_policy(Box::new(tantivy::indexer::NoMergePolicy));
    Ok(OpenIndex {
        index,
        writer: Some(writer),
        schema,
        generation,
    })
}

fn open_reader(dir: &Path, spec: &IndexSpec, generation: u64) -> Result<OpenIndex> {
    let index = Index::open_in_dir(dir).map_err(SkobError::index)?;
    let schema = BuiltSchema::bind(&index, spec)?;
    Ok(OpenIndex {
        index,
        writer: None,
        schema,
        generation,
    })
}

fn search_open(open: &OpenIndex, request: &SearchRequest) -> Result<Vec<SearchHit>> {
    let reader = open.index.reader().map_err(SkobError::index)?;
    let searcher = reader.searcher();
    let fields = open.schema.query_fields(request.field.as_deref())?;
    if fields.is_empty() {
        return Err(SkobError::BadRequest(
            "index has no searchable fields".into(),
        ));
    }
    let parser = QueryParser::for_index(&open.index, fields);
    let (query, errors) = parser.parse_query_lenient(&request.query);
    if request.field.is_some() && !errors.is_empty() {
        return Err(SkobError::BadRequest(errors[0].to_string()));
    }
    let limit = request.limit.max(1);
    let top_docs = searcher
        .search(&*query, &TopDocs::with_limit(limit).order_by_score())
        .map_err(SkobError::index)?;
    let mut hits = Vec::with_capacity(top_docs.len());
    for (score, address) in top_docs {
        let doc = searcher
            .doc::<TantivyDocument>(address)
            .map_err(SkobError::index)?;
        let Some(id) = doc
            .get_first(open.schema.object_id)
            .and_then(|value| TantivyValue::as_str(&value))
            .map(str::to_string)
        else {
            continue;
        };
        let object_id = ObjectId::parse(&id).map_err(|_| {
            SkobError::index(format!("stored object id `{id}` is not a valid object id"))
        })?;
        hits.push(SearchHit { object_id, score });
    }
    Ok(hits)
}

fn list_index_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    walk_files(dir, &mut files)?;
    Ok(files)
}

fn walk_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let entries = std::fs::read_dir(dir).map_err(SkobError::index)?;
    for entry in entries {
        let entry = entry.map_err(SkobError::index)?;
        let path = entry.path();
        if path.is_dir() {
            walk_files(&path, out)?;
            continue;
        }
        if is_payload_file(&path) {
            out.push(path);
        }
    }
    Ok(())
}

fn is_payload_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    !name.ends_with(".lock") && name != ".tantivy-writer.lock"
}

fn relative_path(root: &Path, path: &Path) -> Result<String> {
    let relative = path
        .strip_prefix(root)
        .map_err(|err| SkobError::index(err))?;
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            std::path::Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            _ => {
                return Err(SkobError::index(format!(
                    "index file `{}` is not a relative payload",
                    path.display()
                )))
            }
        }
    }
    Ok(parts.join("/"))
}

fn safe_join(root: &Path, relative: &str) -> Result<PathBuf> {
    if relative.is_empty()
        || relative
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(SkobError::index(format!(
            "invalid index file `{relative}`"
        )));
    }
    Ok(root.join(relative))
}
