use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use pplx_space_common_core::types::NodeManagedBy;

/// String values match the production `SandboxInfo.status` lexicon so existing
/// clients can deserialize the simulator's responses unchanged.
pub mod status {
    pub const CREATING: &str = "creating";
    pub const RUNNING: &str = "running";
    pub const PAUSED: &str = "paused";
    pub const SUSPENDED: &str = "suspended";
    pub const STOPPED: &str = "stopped";
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SandboxRecord {
    pub sandbox_id: String,
    pub template: String,
    pub runtime: String,
    pub user: String,
    pub status: String,
    pub created_at: String,
    pub content_hash: Option<String>,
    pub snapshots_enabled: bool,
    #[serde(default)]
    pub pool_template: Option<String>,
    #[serde(default)]
    pub process_env: BTreeMap<String, String>,
    #[serde(default)]
    pub route_generation: Option<u64>,
    #[serde(default)]
    pub execution_generation: Option<i64>,
    #[serde(default)]
    pub access_extended_until: Option<i64>,
    /// Encoded policy accepted by the simulator; it has no lifecycle scheduler.
    #[serde(default)]
    pub access_idle_policy: Vec<u8>,
    #[serde(default)]
    pub auto_lifecycle_authority: pplx_space_common_core::types::PolicyAuthority,
    #[serde(default)]
    pub route_substrate_kind: Option<NodeManagedBy>,
    /// Activation attempt the simulator last reported with the generations
    /// above. Replayed with them so a resume or wake receipt repeats the same
    /// attempt instead of minting a new one for an execution that did not
    /// change.
    #[serde(default)]
    pub activation_attempt_id: Option<String>,
}

impl SandboxRecord {
    pub fn new_creating(
        sandbox_id: String,
        template: String,
        runtime: String,
        user: String,
        content_hash: Option<String>,
        snapshots_enabled: bool,
    ) -> Self {
        Self {
            sandbox_id,
            template,
            runtime,
            user,
            status: status::CREATING.to_string(),
            created_at: chrono_like_now(),
            content_hash,
            snapshots_enabled,
            pool_template: None,
            process_env: BTreeMap::new(),
            route_generation: None,
            execution_generation: None,
            access_extended_until: None,
            access_idle_policy: Vec::new(),
            auto_lifecycle_authority: Default::default(),
            route_substrate_kind: None,
            activation_attempt_id: None,
        }
    }

    pub fn with_pool_template(mut self, pool_template: Option<String>) -> Self {
        self.pool_template = pool_template;
        self
    }

    pub fn with_route_identity_provenance(
        mut self,
        route_generation: u64,
        execution_generation: i64,
        substrate_kind: Option<NodeManagedBy>,
    ) -> Self {
        self.route_generation = Some(route_generation);
        self.execution_generation = Some(execution_generation);
        self.route_substrate_kind = substrate_kind;
        self
    }
}

/// The simulator persists suspended sandboxes as a JSON file in a shared
/// directory. The schema is intentionally narrow: any node that mounts the
/// same directory can pick up the snapshot and reconstruct the in-memory
/// record on restore.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub version: u32,
    pub record: SandboxRecord,
    pub written_by: String,
    pub written_at: String,
}

const SNAPSHOT_VERSION: u32 = 1;

/// Thread-safe handle over the in-memory sandbox map plus the shared snapshot
/// directory used to simulate suspend/restore across sim nodes.
#[derive(Clone)]
pub struct SimState {
    inner: Arc<Mutex<HashMap<String, SandboxRecord>>>,
    transition_after_state_read: Arc<Mutex<HashMap<String, String>>>,
    snapshot_dir: PathBuf,
    node_id: String,
    fs: crate::fs::SandboxFs,
    last_wait_ready_route_token: Arc<Mutex<Option<String>>>,
    processes: crate::processes::SimProcessTable,
}

#[derive(Debug, thiserror::Error)]
pub enum SimStateError {
    #[error("sandbox {0} not found")]
    NotFound(String),
    #[error("sandbox {0} already exists")]
    AlreadyExists(String),
    #[error("invalid sandbox id {0:?}: must be a single safe path component")]
    InvalidSandboxId(String),
    #[error("invalid transition for sandbox {sandbox_id}: {from:?} -> {to:?}")]
    InvalidTransition {
        sandbox_id: String,
        from: String,
        to: String,
    },
    #[error("snapshot for sandbox {sandbox_id} not found in {dir}")]
    SnapshotMissing { sandbox_id: String, dir: String },
    #[error("snapshot io error for {sandbox_id}: {source}")]
    SnapshotIo {
        sandbox_id: String,
        #[source]
        source: std::io::Error,
    },
    #[error("snapshot decode error for {sandbox_id}: {source}")]
    SnapshotDecode {
        sandbox_id: String,
        #[source]
        source: serde_json::Error,
    },
}

impl SimState {
    pub fn new(snapshot_dir: PathBuf, sandbox_fs_dir: PathBuf, node_id: String) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            transition_after_state_read: Default::default(),
            snapshot_dir,
            node_id,
            fs: crate::fs::SandboxFs::new(sandbox_fs_dir),
            last_wait_ready_route_token: Arc::new(Mutex::new(None)),
            processes: crate::processes::SimProcessTable::default(),
        }
    }

    pub(crate) fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Fake processes started through `SpacedStartProcess`.
    pub fn processes(&self) -> &crate::processes::SimProcessTable {
        &self.processes
    }

    pub fn snapshot_dir(&self) -> &Path {
        &self.snapshot_dir
    }

    pub fn fs(&self) -> &crate::fs::SandboxFs {
        &self.fs
    }

    pub async fn record_wait_ready_route_token(&self, route_token: Option<String>) {
        *self.last_wait_ready_route_token.lock().await = route_token;
    }

    pub async fn last_wait_ready_route_token(&self) -> Option<String> {
        self.last_wait_ready_route_token.lock().await.clone()
    }

    pub async fn insert_new(&self, record: SandboxRecord) -> Result<(), SimStateError> {
        // Validate the id BEFORE touching the in-memory map: otherwise a
        // caller-supplied id like `..` or `a/b` would land in the map and
        // only surface as an error when `ensure_sandbox_root` tries to
        // create the directory, leaving a poisoned record behind.
        validate_state_sandbox_id(&record.sandbox_id)?;
        {
            let mut map = self.inner.lock().await;
            if map.contains_key(&record.sandbox_id) {
                return Err(SimStateError::AlreadyExists(record.sandbox_id));
            }
            map.insert(record.sandbox_id.clone(), record.clone());
        }
        // Eagerly materialize the per-sandbox FS root so spaced file ops have
        // a directory to operate against. Failures bubble up so callers know
        // a malformed sandbox-fs-dir would silently strand later writes.
        self.fs
            .ensure_sandbox_root(&record.sandbox_id)
            .await
            .map_err(|err| SimStateError::SnapshotIo {
                sandbox_id: record.sandbox_id.clone(),
                source: std::io::Error::other(err.to_string()),
            })?;
        Ok(())
    }

    pub async fn get(&self, sandbox_id: &str) -> Result<SandboxRecord, SimStateError> {
        self.inner
            .lock()
            .await
            .get(sandbox_id)
            .cloned()
            .ok_or_else(|| SimStateError::NotFound(sandbox_id.to_string()))
    }

    #[cfg(test)]
    pub async fn list(&self) -> Vec<SandboxRecord> {
        self.inner.lock().await.values().cloned().collect()
    }

    /// Get the record for `sandbox_id`, inserting a default-running record if
    /// the simulator has never seen this id before. Used by the read-only
    /// surface (Spaced status/wait_ready/files) when the simulator runs
    /// as the per-sandbox pod under K8s substrate, where the api-server
    /// never calls `CreateSandbox` — the pod's mere existence on a pod IP is
    /// the substrate's "creation" event.
    pub async fn ensure_default_running(
        &self,
        sandbox_id: &str,
    ) -> Result<SandboxRecord, SimStateError> {
        {
            let map = self.inner.lock().await;
            if let Some(rec) = map.get(sandbox_id) {
                return Ok(rec.clone());
            }
        }
        let record = SandboxRecord {
            sandbox_id: sandbox_id.to_string(),
            template: "seed".into(),
            runtime: "fc".into(),
            user: "root".into(),
            status: status::RUNNING.to_string(),
            created_at: chrono_like_now(),
            content_hash: None,
            snapshots_enabled: false,
            pool_template: None,
            process_env: BTreeMap::new(),
            route_generation: None,
            execution_generation: None,
            access_extended_until: None,
            access_idle_policy: Vec::new(),
            auto_lifecycle_authority: Default::default(),
            route_substrate_kind: None,
            activation_attempt_id: None,
        };
        // insert_new races are tolerable: another concurrent call already
        // created the record, so fall through to a fresh get().
        match self.insert_new(record.clone()).await {
            Ok(()) => Ok(record),
            Err(SimStateError::AlreadyExists(_)) => self.get(sandbox_id).await,
            Err(err) => Err(err),
        }
    }

    /// Complete a transition after capturing the next State observation but
    /// before the caller can issue Wake, deterministically exercising stale reads.
    pub async fn transition_after_next_state_read(&self, sandbox_id: String, target: &str) {
        self.transition_after_state_read
            .lock()
            .await
            .insert(sandbox_id, target.to_owned());
    }

    pub async fn state_of(&self, ids: &[String]) -> (Vec<SandboxRecord>, Vec<String>) {
        let mut map = self.inner.lock().await;
        let mut transitions = self.transition_after_state_read.lock().await;
        let mut found = Vec::with_capacity(ids.len());
        let mut missing = Vec::new();
        for id in ids {
            match map.get(id) {
                Some(rec) => found.push(rec.clone()),
                None => missing.push(id.clone()),
            }
        }
        for id in ids {
            if let Some(target) = transitions.remove(id)
                && let Some(record) = map.get_mut(id)
            {
                record.status = target;
            }
        }
        (found, missing)
    }

    pub async fn set_status(
        &self,
        sandbox_id: &str,
        new_status: &str,
    ) -> Result<SandboxRecord, SimStateError> {
        let mut map = self.inner.lock().await;
        let rec = map
            .get_mut(sandbox_id)
            .ok_or_else(|| SimStateError::NotFound(sandbox_id.to_string()))?;
        rec.status = new_status.to_string();
        Ok(rec.clone())
    }

    /// Transition the sandbox from one of `allowed_from` into `to`. Returns the
    /// updated record. Used by lifecycle RPCs that legitimately reject some
    /// source states (e.g. pause from "stopped").
    pub async fn transition(
        &self,
        sandbox_id: &str,
        allowed_from: &[&str],
        to: &str,
    ) -> Result<SandboxRecord, SimStateError> {
        let mut map = self.inner.lock().await;
        let rec = map
            .get_mut(sandbox_id)
            .ok_or_else(|| SimStateError::NotFound(sandbox_id.to_string()))?;
        if !allowed_from.iter().any(|s| *s == rec.status) {
            return Err(SimStateError::InvalidTransition {
                sandbox_id: sandbox_id.to_string(),
                from: rec.status.clone(),
                to: to.to_string(),
            });
        }
        rec.status = to.to_string();
        Ok(rec.clone())
    }

    /// Remember the route identity the simulator last reported for a sandbox,
    /// so later resume/wake receipts repeat the persisted generations and
    /// activation attempt instead of regressing to the defaults (which the
    /// control plane refuses as stale) or minting a fresh attempt (which
    /// receipt ordering would take as a new execution).
    pub async fn set_route_identity_provenance(
        &self,
        sandbox_id: &str,
        route_generation: Option<u64>,
        execution_generation: Option<i64>,
        activation_attempt_id: Option<String>,
    ) -> Result<(), SimStateError> {
        let mut map = self.inner.lock().await;
        let rec = map
            .get_mut(sandbox_id)
            .ok_or_else(|| SimStateError::NotFound(sandbox_id.to_string()))?;
        if route_generation.is_some() {
            rec.route_generation = route_generation;
        }
        if execution_generation.is_some() {
            rec.execution_generation = execution_generation;
        }
        if activation_attempt_id.is_some() {
            rec.activation_attempt_id = activation_attempt_id;
        }
        Ok(())
    }

    pub async fn apply_access_deadline(
        &self,
        sandbox_id: &str,
        minimum: Option<i64>,
        reset_policy: Option<Vec<u8>>,
    ) -> Result<(), SimStateError> {
        let mut map = self.inner.lock().await;
        let record = map
            .get_mut(sandbox_id)
            .ok_or_else(|| SimStateError::NotFound(sandbox_id.to_string()))?;
        if let Some(policy) = reset_policy {
            record.access_idle_policy = policy;
            record.access_extended_until = None;
        }
        if let Some(minimum) = minimum {
            record.access_extended_until = Some(
                record
                    .access_extended_until
                    .map_or(minimum, |old| old.max(minimum)),
            );
        }
        Ok(())
    }

    pub async fn remove(&self, sandbox_id: &str) -> Result<SandboxRecord, SimStateError> {
        self.inner
            .lock()
            .await
            .remove(sandbox_id)
            .ok_or_else(|| SimStateError::NotFound(sandbox_id.to_string()))
    }

    pub async fn adopt_pool_sandbox(
        &self,
        template: &str,
        content_hash: Option<&str>,
        user: &str,
        process_env: BTreeMap<String, String>,
    ) -> Option<SandboxRecord> {
        let mut map = self.inner.lock().await;
        let selected_id = map
            .values()
            .filter(|record| {
                record.pool_template.as_deref() == Some(template)
                    && record.status == status::PAUSED
                    && content_hash.is_none_or(|hash| record.content_hash.as_deref() == Some(hash))
            })
            .map(|record| record.sandbox_id.clone())
            .min();

        let record = map.get_mut(&selected_id?)?;
        record.pool_template = None;
        record.user = user.to_string();
        record.process_env = process_env;
        Some(record.clone())
    }

    pub async fn update_metadata(
        &self,
        sandbox_id: &str,
        expected_current_owner: Option<&str>,
        owner: Option<&str>,
        process_env: Option<BTreeMap<String, String>>,
    ) -> Result<SandboxRecord, SimStateError> {
        let mut map = self.inner.lock().await;
        let rec = map
            .get_mut(sandbox_id)
            .ok_or_else(|| SimStateError::NotFound(sandbox_id.to_string()))?;
        if rec.status != status::PAUSED && rec.status != status::RUNNING {
            return Err(SimStateError::InvalidTransition {
                sandbox_id: sandbox_id.to_string(),
                from: rec.status.clone(),
                to: status::PAUSED.to_string(),
            });
        }
        if expected_current_owner.is_some_and(|owner| rec.user != owner) {
            return Err(SimStateError::InvalidTransition {
                sandbox_id: sandbox_id.to_string(),
                from: rec.user.clone(),
                to: expected_current_owner.unwrap_or_default().to_string(),
            });
        }
        if let Some(owner) = owner {
            rec.user = owner.to_string();
        }
        if let Some(process_env) = process_env {
            rec.process_env = process_env;
        }
        Ok(rec.clone())
    }

    /// Stop/Delete select exactly one identity, subject to its template predicate.
    pub async fn ids_matching_exact(
        &self,
        sandbox_id: &str,
        template_filter: Option<&str>,
    ) -> Vec<String> {
        let map = self.inner.lock().await;
        map.get(sandbox_id)
            .filter(|r| match template_filter {
                Some(t) if !t.is_empty() => r.template == t,
                _ => true,
            })
            .map(|r| r.sandbox_id.clone())
            .into_iter()
            .collect()
    }

    fn snapshot_path(&self, sandbox_id: &str) -> PathBuf {
        self.snapshot_dir.join(format!("{sandbox_id}.json"))
    }

    pub async fn suspend_to_shared_dir(
        &self,
        sandbox_id: &str,
    ) -> Result<SandboxRecord, SimStateError> {
        validate_state_sandbox_id(sandbox_id)?;
        let rec = self
            .transition(
                sandbox_id,
                &[status::RUNNING, status::PAUSED],
                status::SUSPENDED,
            )
            .await?;

        let snapshot = Snapshot {
            version: SNAPSHOT_VERSION,
            record: rec.clone(),
            written_by: self.node_id.clone(),
            written_at: chrono_like_now(),
        };

        tokio::fs::create_dir_all(&self.snapshot_dir)
            .await
            .map_err(|source| SimStateError::SnapshotIo {
                sandbox_id: sandbox_id.to_string(),
                source,
            })?;

        let path = self.snapshot_path(sandbox_id);
        let json = serde_json::to_vec_pretty(&snapshot).expect("snapshot serializable");
        tokio::fs::write(&path, json)
            .await
            .map_err(|source| SimStateError::SnapshotIo {
                sandbox_id: sandbox_id.to_string(),
                source,
            })?;

        Ok(rec)
    }

    /// Restore a previously suspended sandbox onto this node. The on-disk
    /// snapshot is read, the in-memory record is recreated (or refreshed) in
    /// `running` state, and the snapshot file is removed so a different sim
    /// node cannot also claim the same sandbox.
    pub async fn restore_from_shared_dir(
        &self,
        sandbox_id: &str,
    ) -> Result<SandboxRecord, SimStateError> {
        // Reject ids that could escape the shared snapshot dir BEFORE
        // building the path. Without this guard a caller-supplied id like
        // `../foo` would let `WakeSandbox` read and then remove
        // `<snapshot_dir>/../foo.json` and pull its contents back into the
        // in-memory map.
        validate_state_sandbox_id(sandbox_id)?;
        let path = self.snapshot_path(sandbox_id);
        let bytes = match tokio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(SimStateError::SnapshotMissing {
                    sandbox_id: sandbox_id.to_string(),
                    dir: self.snapshot_dir.display().to_string(),
                });
            }
            Err(source) => {
                return Err(SimStateError::SnapshotIo {
                    sandbox_id: sandbox_id.to_string(),
                    source,
                });
            }
        };
        let snapshot: Snapshot =
            serde_json::from_slice(&bytes).map_err(|source| SimStateError::SnapshotDecode {
                sandbox_id: sandbox_id.to_string(),
                source,
            })?;

        let mut rec = snapshot.record;
        rec.status = status::RUNNING.to_string();
        {
            let mut map = self.inner.lock().await;
            map.insert(rec.sandbox_id.clone(), rec.clone());
        }
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(SimStateError::SnapshotIo {
                    sandbox_id: sandbox_id.to_string(),
                    source,
                });
            }
        }
        Ok(rec)
    }
}

fn validate_state_sandbox_id(sandbox_id: &str) -> Result<(), SimStateError> {
    match crate::fs::validate_sandbox_id(sandbox_id) {
        Ok(()) => Ok(()),
        Err(crate::fs::FsError::UnsafeSandboxId(id)) => Err(SimStateError::InvalidSandboxId(id)),
        Err(err) => Err(SimStateError::InvalidSandboxId(err.to_string())),
    }
}

/// Lightweight RFC3339-ish timestamp without pulling in `chrono`. Good enough
/// for human-readable sandbox metadata in a local developer simulator.
pub(crate) fn chrono_like_now() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}", now.as_secs())
}

#[cfg(test)]
mod tests;
