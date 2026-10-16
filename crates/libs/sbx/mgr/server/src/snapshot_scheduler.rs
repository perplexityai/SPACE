//! Rolling btrfs snapshot scheduler.
//!
//! Manages per-sandbox snapshot timers. On each 1-minute tick, creates
//! tiered snapshots (1m, 10m, 1h) and prunes old ones according to
//! the retention policy. Also creates full (checkpoint) snapshots at
//! 10m, 1h, and 6h intervals.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::guest_filesystem_flush::{
    GuestSnapshotPreparationBudget, flush_guest_filesystems_via_spaced_host,
    prepare_guest_for_vm_snapshot_via_spaced_host,
};
use crate::node_state_sync::mark_sandbox_dirty;
use crate::runtime_reconciliation::{mark_quarantined_best_effort, observe_firecracker_vm_state};
use crate::state::SandboxState;
use pplx_space_common_core::types::{
    ArtifactKind, LocalArtifactId, SandboxArtifactMetadata, SandboxId, SandboxQuarantineReasonCode,
    SnapshotExecutionMetadata, SnapshotName,
};
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventType, emit_sandbox_event,
};
use pplx_space_node_mgr_client::proto::{
    CreateSnapshotRequest, PauseSandboxRequest, ResumeSandboxRequest, SyncGuestRequest,
};
use pplx_space_node_runtime_core::fc_api::FirecrackerVmState;

const TICK_INTERVAL: Duration = Duration::from_secs(60);

fn lock_db(ctx: &CheckpointContext) -> Result<crate::db::ConnectionGuard<'_>, String> {
    ctx.db
        .lock()
        .map_err(|error| format!("database mutex poisoned: {error}"))
}

/// `do_checkpoint`'s admission to the node-local SQLite `sandboxes.state`:
/// `started` or `paused` to `checkpointing`, under the connection guard that
/// read the state. Guard refusals and a missing row render
/// `SandboxStateWriteNotApplied`, and a from-state refusal reports the state
/// found; this module's
/// errors are strings, so it cannot use the service's status mapping.
fn begin_checkpoint_state(db: &crate::db::Connection, sandbox_id: &str) -> Result<(), String> {
    use crate::db::{SandboxStateWriteNotApplied, StateWriteOutcome, StateWriteRefusal};
    let edge = &crate::db::edges::CHECKPOINT_BEGIN;
    let not_applied = match crate::db::update_state_from(db, sandbox_id, edge)
        .map_err(|error| format!("db error: {error}"))?
    {
        StateWriteOutcome::Applied { .. } => return Ok(()),
        StateWriteOutcome::Refused {
            found,
            refusal: StateWriteRefusal::FromState,
        } => {
            return Err(format!(
                "sandbox state write was superseded: sandbox is {found}"
            ));
        }
        StateWriteOutcome::Refused {
            refusal: StateWriteRefusal::Guard(guard),
            ..
        } => SandboxStateWriteNotApplied::Guard(guard),
        StateWriteOutcome::Missing => SandboxStateWriteNotApplied::Missing,
    };
    Err(not_applied.to_string())
}

pub(crate) fn write_file_atomic(
    path: impl AsRef<std::path::Path>,
    contents: &[u8],
) -> std::io::Result<()> {
    let path = path.as_ref();
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "atomic write path has no parent",
        )
    })?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(contents)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    sync_parent_dir(parent)?;
    Ok(())
}

fn sync_parent_dir(parent: &std::path::Path) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        std::fs::File::open(parent)?.sync_all()?;
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = parent;
    }
    Ok(())
}

/// Write a sidecar `snapshot.json` next to snapshot data for recovery.
/// If the directory doesn't exist (e.g. disk snapshots managed by btrfs),
/// this is best-effort and failures are logged but not propagated.
fn write_snapshot_sidecar(
    snapshot_dir: &str,
    sandbox_id: &str,
    snapshot_name: &str,
    snapshot_type: &str,
    retention_tier: &str,
) {
    let meta = serde_json::json!({
        "sandbox_id": sandbox_id,
        "name": snapshot_name,
        "type": snapshot_type,
        "retention_tier": retention_tier,
        "timestamp": snapshot_timestamp(jiff::Timestamp::now()),
    });
    let path = format!("{snapshot_dir}/snapshot.json");
    if let Err(e) = std::fs::write(
        &path,
        serde_json::to_string_pretty(&meta).unwrap_or_default(),
    ) {
        tracing::warn!(
            sandbox_id,
            snapshot_name,
            path,
            error = %e,
            "failed to write scheduled snapshot sidecar"
        );
    }
}

fn write_snapshot_execution_sidecar(snapshot_dir: &str, sandbox_id: &str, snapshot_name: &str) {
    let sandbox_metadata_path =
        pplx_space_common_core::host::linux::paths::sandbox_metadata_path(sandbox_id);
    let bytes = match std::fs::read(&sandbox_metadata_path) {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::warn!(
                sandbox_id,
                snapshot_name,
                path = sandbox_metadata_path,
                %error,
                "failed to read sandbox execution metadata for scheduled snapshot"
            );
            return;
        }
    };
    let metadata = match serde_json::from_slice::<SandboxArtifactMetadata>(&bytes) {
        Ok(metadata) => metadata,
        Err(error) => {
            tracing::warn!(
                sandbox_id,
                snapshot_name,
                path = sandbox_metadata_path,
                %error,
                "failed to parse sandbox execution metadata for scheduled snapshot"
            );
            return;
        }
    };
    let execution = match SnapshotExecutionMetadata::new(metadata.execution) {
        Ok(execution) => execution.with_cpu_budget(metadata.cpu_budget),
        Err(error) => {
            tracing::warn!(
                sandbox_id,
                snapshot_name,
                %error,
                "sandbox execution metadata is not valid for scheduled snapshot"
            );
            return;
        }
    };
    let path = format!("{snapshot_dir}/runtime.json");
    let contents = match serde_json::to_vec_pretty(&execution) {
        Ok(contents) => contents,
        Err(error) => {
            tracing::warn!(
                sandbox_id,
                snapshot_name,
                %error,
                "failed to serialize scheduled snapshot execution metadata"
            );
            return;
        }
    };
    if let Err(error) = write_file_atomic(&path, &contents) {
        tracing::warn!(
            sandbox_id,
            snapshot_name,
            path,
            %error,
            "failed to write scheduled snapshot execution metadata"
        );
    }
}

/// Snapshot retention per tier. Each tier's count covers the gap to the next tier.
/// d-1m:  keep 5  → covers minutes 1-5 (until d-5m takes over)
/// d-5m:  keep 1  → covers minutes 5-10 (until f-10m takes over)
/// f-10m: keep 6  → covers 10min-1h (until f-1h takes over)
/// f-1h:  keep 6  → covers 1h-6h (until f-6h takes over)
/// f-6h:  keep 4  → covers 6h-24h
const KEEP_D_1M: usize = 5;
const KEEP_D_5M: usize = 1;
const KEEP_F_10M: usize = 6;
const KEEP_F_1H: usize = 6;
const KEEP_F_6H: usize = 4;

const CACHE_DIR: &str = pplx_space_common_core::host::linux::paths::SANDBOX_CACHE_DIR;

struct SandboxSnapshotState {
    tick_count: u64,
    pending_checkpoint: Option<CheckpointTier>,
    template: String,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum CheckpointTier {
    TenMinutes,
    OneHour,
    SixHours,
}

impl CheckpointTier {
    fn due(tick: u64) -> Option<Self> {
        if tick.is_multiple_of(360) {
            Some(Self::SixHours)
        } else if tick.is_multiple_of(60) {
            Some(Self::OneHour)
        } else if tick.is_multiple_of(10) {
            Some(Self::TenMinutes)
        } else {
            None
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::TenMinutes => "10m",
            Self::OneHour => "1h",
            Self::SixHours => "6h",
        }
    }
}

/// Shared context for checkpoint operations.
pub struct CheckpointContext {
    pub db: crate::db::SharedConnection,
    pub auto_lifecycle: crate::auto_lifecycle::AutoLifecycleCoordinator,
    pub health_lifecycle: crate::watchdog::HealthLifecycleEpochs,
    pub node_manager: crate::node_manager_transport::NodeManagerConnector,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointCompletion {
    Complete,
    CompleteWithLifecycleError(String),
    /// Capture was skipped: no snapshot files were written, so callers must
    /// not publish an artifact for this run — a published row would point at
    /// files that do not exist and poison the next restore.
    SkippedNoCapture(&'static str),
}

impl CheckpointCompletion {
    pub fn into_lifecycle_result(self) -> Result<(), String> {
        match self {
            Self::Complete | Self::SkippedNoCapture(_) => Ok(()),
            Self::CompleteWithLifecycleError(reason) => Err(reason),
        }
    }

    /// When set, nothing was captured and no artifact may be published.
    pub fn skip_reason(&self) -> Option<&'static str> {
        match self {
            Self::SkippedNoCapture(reason) => Some(reason),
            Self::Complete | Self::CompleteWithLifecycleError(_) => None,
        }
    }
}

pub struct SnapshotScheduler {
    sandboxes: Arc<Mutex<HashMap<String, SandboxSnapshotState>>>,
    _task: tokio::task::JoinHandle<()>,
}

impl SnapshotScheduler {
    #[cfg(test)]
    pub fn new(ctx: Arc<CheckpointContext>) -> Self {
        Self::new_with_ownership(
            ctx,
            Arc::new(crate::service::drain::DrainState::default()),
            tokio_util::sync::CancellationToken::new(),
            &tokio_util::task::TaskTracker::new(),
        )
    }

    pub(crate) fn new_with_ownership(
        ctx: Arc<CheckpointContext>,
        drain_state: Arc<crate::service::drain::DrainState>,
        shutdown: tokio_util::sync::CancellationToken,
        tracker: &tokio_util::task::TaskTracker,
    ) -> Self {
        let sandboxes: Arc<Mutex<HashMap<String, SandboxSnapshotState>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let sched_sandboxes = sandboxes.clone();
        let sched_ctx = ctx.clone();
        let guard = crate::service::drain::CriticalTaskGuard::new(
            "snapshot scheduler",
            shutdown.clone(),
            shutdown.clone(),
            Arc::clone(&drain_state),
        );
        let task = tracker.spawn(async move {
            let _guard = guard;
            scheduler_loop(sched_sandboxes, sched_ctx, drain_state, shutdown).await;
        });
        Self {
            sandboxes,
            _task: task,
        }
    }

    pub async fn add_sandbox(&self, sandbox_id: String, template: String) {
        let mut map = self.sandboxes.lock().await;
        map.entry(sandbox_id).or_insert(SandboxSnapshotState {
            tick_count: 0,
            pending_checkpoint: None,
            template,
        });
    }

    pub async fn remove_sandbox(&self, sandbox_id: &str) {
        let mut map = self.sandboxes.lock().await;
        map.remove(sandbox_id);
    }

    pub(crate) fn try_retirement<'a>(
        &'a self,
        sandbox_id: &str,
    ) -> Option<impl FnOnce() + use<'a>> {
        let mut sandboxes = self.sandboxes.try_lock().ok()?;
        let sandbox_id = sandbox_id.to_owned();
        Some(move || {
            sandboxes.remove(&sandbox_id);
        })
    }

    pub async fn is_active(&self, sandbox_id: &str) -> bool {
        let map = self.sandboxes.lock().await;
        map.contains_key(sandbox_id)
    }
}

async fn scheduler_loop(
    sandboxes: Arc<Mutex<HashMap<String, SandboxSnapshotState>>>,
    ctx: Arc<CheckpointContext>,
    drain_state: Arc<crate::service::drain::DrainState>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    let mut interval = tokio::time::interval(TICK_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            _ = interval.tick() => {},
        }

        // Snapshot current state to avoid holding the lock during RPCs
        let entries: Vec<(String, String)> = {
            let map = sandboxes.lock().await;
            map.iter()
                .map(|(id, s)| (id.clone(), s.template.clone()))
                .collect()
        };

        for (id, template) in entries {
            if shutdown.is_cancelled() {
                return;
            }
            let Ok(_writer) =
                crate::service::drain::acquire_state_writer(&drain_state, "ScheduledCheckpoint")
            else {
                // Dependency replacement may temporarily close admission;
                // leave work for the next tick without advancing its cadence.
                break;
            };
            if let Err(e) = run_scheduled_tick(&sandboxes, &id, async |tick, checkpoint| {
                process_tick(&id, &template, tick, checkpoint, &ctx).await
            })
            .await
            {
                tracing::warn!(
                    sandbox_id = %id,
                    error = %e,
                    "snapshot scheduler tick failed"
                );
            }
        }
    }
}

async fn run_scheduled_tick(
    sandboxes: &Mutex<HashMap<String, SandboxSnapshotState>>,
    sandbox_id: &str,
    operation: impl AsyncFnOnce(u64, Option<CheckpointTier>) -> Result<CheckpointCompletion, String>,
) -> Result<(), String> {
    let (tick, checkpoint) = {
        let mut map = sandboxes.lock().await;
        let Some(state) = map.get_mut(sandbox_id) else {
            return Ok(());
        };
        state.tick_count += 1;
        // One pending capture coalesces missed due tiers without replaying the
        // disk cadence. A larger tier takes precedence when it becomes due.
        state.pending_checkpoint = state
            .pending_checkpoint
            .max(CheckpointTier::due(state.tick_count));
        (state.tick_count, state.pending_checkpoint)
    };
    let completion = operation(tick, checkpoint).await?;
    {
        // Discharge known completion before reporting a follow-up lifecycle
        // error. Never mutate a replacement stop/start registration: only this
        // single scheduler advances ticks, and a fresh registration starts at 0.
        let mut map = sandboxes.lock().await;
        if let Some(state) = map.get_mut(sandbox_id)
            && state.tick_count == tick
        {
            state.pending_checkpoint = None;
        }
    }
    completion.into_lifecycle_result()
}

async fn process_tick(
    sandbox_id: &str,
    _template: &str,
    tick: u64,
    checkpoint_tier: Option<CheckpointTier>,
    ctx: &CheckpointContext,
) -> Result<CheckpointCompletion, String> {
    let mut node_client = ctx
        .node_manager
        .connect()
        .await
        .map_err(|e| e.to_string())?;

    let execution_fence: Option<pplx_space_node_mgr_client::proto::ExecutionFence> = {
        let db = lock_db(ctx)?;
        crate::db::node_execution_fence(&db, sandbox_id)
            .map_err(|error| error.to_string())?
            .map(Into::into)
    };

    // This flush belongs to the disk-only snapshots below. A full checkpoint
    // scheduled later on the same tick performs its own pre-memory preparation.
    flush_guest_filesystems_via_spaced_host(sandbox_id)
        .await
        .map_err(|error| error.to_string())?;

    node_client
        .sync_guest(SyncGuestRequest {
            sandbox_id: sandbox_id.to_string(),
        })
        .await
        .map_err(|e| format!("host rootfs sync failed: {e}"))?;

    let ts = snapshot_timestamp(jiff::Timestamp::now());

    // Helper: create btrfs disk snapshot + write sidecar
    async fn create_disk_snapshot(
        node_client: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
        sandbox_id: &str,
        snap_name: &str,
        tier: &str,
        execution_fence: Option<pplx_space_node_mgr_client::proto::ExecutionFence>,
    ) -> Result<(), String> {
        node_client
            .create_btrfs_snapshot(
                pplx_space_node_mgr_client::proto::CreateBtrfsSnapshotRequest {
                    sandbox_id: sandbox_id.to_string(),
                    snapshot_name: snap_name.to_string(),
                    writable: false,
                    execution_fence,
                },
            )
            .await
            .map_err(|e| format!("{tier} snapshot: {e}"))?;
        // Note: no sidecar for disk snapshots — btrfs subvolumes are read-only.
        // Metadata (type, tier) is encoded in the name and parsed by node-manager.
        Ok(())
    }

    // Disk snapshots publish under the artifact lock so backup selection never
    // races their creation or their retention deletion.
    {
        let (_wake_guard, _artifact_guard) = ctx
            .auto_lifecycle
            .artifact_publication_locks(sandbox_id, ArtifactKind::Disk)
            .await;
        // Prune once before creation so failed retention blocks further growth.
        // This tick may add one snapshot per due tier above its retention count;
        // the next successful prune removes that surplus before creating more.
        // Deletions from completed pages remain useful progress on a retry.
        prune_scheduled_snapshots(&mut node_client, ctx, sandbox_id).await?;
        // Always create a 1m disk snapshot.
        let disk_1m_name = format!("d-1m-{ts}");
        create_disk_snapshot(
            &mut node_client,
            sandbox_id,
            &disk_1m_name,
            "1m",
            execution_fence.clone(),
        )
        .await?;
        if let Err(error) =
            publish_named_artifact(ctx, sandbox_id, &disk_1m_name, ArtifactKind::Disk)
        {
            tracing::warn!(
                sandbox_id,
                snapshot_name = %disk_1m_name,
                error = %error,
                "created scheduled disk snapshot but failed to publish local artifact registry entry"
            );
        }

        // Every 5th tick: d-5m (filesystem state ~5 min ago).
        if tick.is_multiple_of(5) {
            let disk_5m_name = format!("d-5m-{ts}");
            create_disk_snapshot(
                &mut node_client,
                sandbox_id,
                &disk_5m_name,
                "5m",
                execution_fence.clone(),
            )
            .await?;
            if let Err(error) =
                publish_named_artifact(ctx, sandbox_id, &disk_5m_name, ArtifactKind::Disk)
            {
                tracing::warn!(
                    sandbox_id,
                    snapshot_name = %disk_5m_name,
                    error = %error,
                    "created scheduled disk snapshot but failed to publish local artifact registry entry"
                );
            }
        }
    }

    if let Some(tier) = checkpoint_tier {
        let tier = tier.as_str();
        let name = format!("f-{tier}-{ts}");
        return do_scheduled_checkpoint(ctx, sandbox_id, &name)
            .await
            .map_err(|error| format!("scheduled {tier} checkpoint failed: {error}"));
    }

    Ok(CheckpointCompletion::Complete)
}

fn publish_named_artifact(
    ctx: &CheckpointContext,
    sandbox_id: &str,
    snapshot_name: &str,
    artifact_kind: ArtifactKind,
) -> Result<(), String> {
    let snapshot_name = SnapshotName::parse(snapshot_name.to_string())
        .map_err(|error| format!("generated scheduled snapshot name is invalid: {error}"))?;
    let local_artifact_id = LocalArtifactId::new();
    let local_path = pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir(
        sandbox_id,
        snapshot_name.as_str(),
    );
    crate::local_artifacts::LocalArtifacts::new(ctx.db.clone())
        .publish_named_snapshot(
            sandbox_id,
            local_artifact_id,
            artifact_kind,
            &snapshot_name,
            &local_path,
        )
        .map_err(|error| format!("persist scheduled local artifact: {error}"))?;
    emit_sandbox_event(
        sandbox_id,
        SandboxEventType::LocalArtifactPublished,
        SandboxEventSource::SbxManager,
        SandboxEventDetails::default()
            .local_artifact_id(local_artifact_id.to_string())
            .artifact_kind(artifact_kind.as_str())
            .artifact_origin("snapshot")
            .snapshot_id(snapshot_name.to_string()),
    );
    Ok(())
}

async fn do_scheduled_checkpoint(
    ctx: &CheckpointContext,
    sandbox_id: &str,
    snapshot_name: &str,
) -> Result<CheckpointCompletion, String> {
    let (_wake_guard, _artifact_guard) = ctx
        .auto_lifecycle
        .artifact_publication_locks(sandbox_id, ArtifactKind::Full)
        .await;
    let completion = do_checkpoint(ctx, sandbox_id, snapshot_name).await?;
    // A skipped capture wrote nothing: publishing would create a Ready
    // artifact row (and event) pointing at files that do not exist.
    if let Some(reason) = completion.skip_reason() {
        tracing::info!(
            sandbox_id,
            snapshot_name,
            reason,
            "scheduled checkpoint skipped; nothing published"
        );
        return Ok(completion);
    }
    publish_named_artifact(ctx, sandbox_id, snapshot_name, ArtifactKind::Full)?;
    Ok(completion)
}

fn restore_checkpoint_state_best_effort(
    ctx: &CheckpointContext,
    sandbox_id: &str,
    restore_state: SandboxState,
) {
    match lock_db(ctx).and_then(|db| {
        crate::db::restore_checkpoint_state(&db, sandbox_id, restore_state)
            .map_err(|error| error.to_string())
    }) {
        Ok(true) => mark_sandbox_dirty(&ctx.db, sandbox_id),
        Ok(false) => tracing::warn!(
            sandbox_id,
            %restore_state,
            "checkpoint rollback state restoration was superseded"
        ),
        Err(error) => tracing::warn!(
            sandbox_id,
            %restore_state,
            %error,
            "failed to restore stable sandbox state after checkpoint failure"
        ),
    }
}

fn quarantine_checkpoint_resume_failure(ctx: &CheckpointContext, sandbox_id: &str, reason: &str) {
    mark_quarantined_best_effort(
        &ctx.db,
        sandbox_id,
        SandboxState::Checkpointing,
        SandboxQuarantineReasonCode::CheckpointResumeFailed,
        reason,
    );
}

fn finish_completed_checkpoint_after_resume_error(
    ctx: &CheckpointContext,
    sandbox_id: &str,
    reason: String,
    vm_state: Option<FirecrackerVmState>,
) -> Result<(), String> {
    let (stable_state, lifecycle_result) = match vm_state {
        Some(FirecrackerVmState::Running) => (SandboxState::Started, Ok(())),
        Some(FirecrackerVmState::Paused) => (SandboxState::Paused, Err(reason.clone())),
        _ => {
            quarantine_checkpoint_resume_failure(ctx, sandbox_id, &reason);
            return Err(reason);
        }
    };
    restore_checkpoint_state_best_effort(ctx, sandbox_id, stable_state);
    tracing::warn!(
        sandbox_id,
        observed_vm_state = %vm_state.map_or_else(|| "unknown".to_string(), |state| state.to_string()),
        %stable_state,
        error = %reason,
        "checkpoint resume response was lost; reconciled from Firecracker state"
    );
    lifecycle_result
}

async fn rollback_checkpoint_failure(
    ctx: &CheckpointContext,
    node_client: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
    sandbox_id: &str,
    execution_fence: Option<pplx_space_node_mgr_client::proto::ExecutionFence>,
    previous_state: SandboxState,
    checkpoint_error: String,
) -> String {
    if previous_state == SandboxState::Paused {
        restore_checkpoint_state_best_effort(ctx, sandbox_id, SandboxState::Paused);
        return checkpoint_error;
    }
    let sandbox_id_typed = match sandbox_id.parse::<SandboxId>() {
        Ok(sandbox_id) => sandbox_id,
        Err(error) => return format!("{checkpoint_error}; invalid sandbox ID: {error}"),
    };

    let mut resume_result = node_client
        .resume_sandbox(ResumeSandboxRequest {
            execution_fence,
            sandbox_id: sandbox_id.to_string(),
        })
        .await
        .map(|_| ())
        .map_err(|error| error.to_string());
    let observation = if resume_result.is_err() {
        let observation = observe_firecracker_vm_state(
            &sandbox_id_typed,
            "checkpoint rollback resume reconciliation",
            &ctx.node_manager,
        )
        .await;
        if let Some(probe_error) = observation.unobservable_reason()
            && let Err(resume_error) = &mut resume_result
        {
            resume_error.push_str("; ");
            resume_error.push_str(probe_error);
        }
        Some(observation)
    } else {
        None
    };
    finish_checkpoint_rollback(
        ctx,
        sandbox_id,
        previous_state,
        checkpoint_error,
        resume_result,
        observation.as_ref().and_then(|value| value.state()),
    )
}

fn finish_checkpoint_rollback(
    ctx: &CheckpointContext,
    sandbox_id: &str,
    previous_state: SandboxState,
    checkpoint_error: String,
    resume_result: Result<(), String>,
    observed_vm_state: Option<FirecrackerVmState>,
) -> String {
    debug_assert_eq!(previous_state, SandboxState::Started);
    match resume_result {
        Ok(_) => {
            restore_checkpoint_state_best_effort(ctx, sandbox_id, SandboxState::Started);
            checkpoint_error
        }
        Err(resume_error) => {
            let reason = format!(
                "{checkpoint_error}; resume after checkpoint rollback failed: {resume_error}"
            );
            match observed_vm_state {
                Some(FirecrackerVmState::Running) => {
                    restore_checkpoint_state_best_effort(ctx, sandbox_id, SandboxState::Started)
                }
                Some(FirecrackerVmState::Paused) => {
                    restore_checkpoint_state_best_effort(ctx, sandbox_id, SandboxState::Paused)
                }
                _ => quarantine_checkpoint_resume_failure(ctx, sandbox_id, &reason),
            }
            reason
        }
    }
}

fn finish_checkpoint_success(
    ctx: &CheckpointContext,
    sandbox_id: &str,
    previous_state: SandboxState,
) -> Result<(), String> {
    let restored = {
        let db = lock_db(ctx)?;
        crate::db::restore_checkpoint_state(&db, sandbox_id, previous_state)
            .map_err(|error| error.to_string())?
    };
    if !restored {
        tracing::warn!(
            sandbox_id,
            %previous_state,
            "checkpoint completed after lifecycle restoration was superseded; preserving the newer state and publishing the artifact"
        );
    }
    Ok(())
}

/// Perform a full checkpoint: pause vCPUs, snapshot VM state + memory, then resume.
/// On any error, try to resume the VM and restore its prior stable state. If
/// the resume outcome is uncertain, preserve the runtime behind quarantine.
pub async fn do_checkpoint(
    ctx: &CheckpointContext,
    sandbox_id: &str,
    snapshot_name: &str,
) -> Result<CheckpointCompletion, String> {
    let sandbox_id_typed = sandbox_id
        .parse::<SandboxId>()
        .map_err(|error| format!("invalid sandbox ID: {error}"))?;
    // 1. Read sandbox, check state, and transition to Checkpointing atomically
    let (previous_state, execution_fence) = {
        let db = lock_db(ctx)?;
        let sandbox =
            crate::db::get_sandbox(&db, sandbox_id).map_err(|e| format!("db error: {e}"))?;
        let sandbox = match sandbox {
            Some(s) => s,
            None => return Err(format!("sandbox {sandbox_id} not found in DB")),
        };
        if let Some(pool_template) = sandbox.pool_template.as_deref() {
            tracing::warn!(
                sandbox_id,
                pool_template,
                "disabling scheduled snapshots for pre-warm pool sandbox"
            );
            if let Err(error) = crate::db::update_snapshots_enabled(&db, sandbox_id, false) {
                tracing::warn!(
                    sandbox_id,
                    pool_template,
                    %error,
                    "failed to disable scheduled snapshots for pre-warm pool sandbox"
                );
            }
            return Ok(CheckpointCompletion::SkippedNoCapture(
                "pre-warm pool sandbox",
            ));
        }

        // A plaintext-flagged sandbox with committed COW state must not be
        // checkpointed — the capture reads only the origin rootfs file while
        // the guest's writes live in the COW (rationale in
        // `ensure_no_plaintext_pool_cow_for_snapshots`). Skip this tick
        // rather than disable: the state resolves once the claim stamps the
        // row or collapses the COW.
        if !sandbox.volume_encryption
            && crate::rootfs_encryption::committed_cow_state_exists(sandbox_id)
        {
            tracing::warn!(
                sandbox_id,
                "skipping checkpoint: plaintext-flagged sandbox with committed COW state"
            );
            return Ok(CheckpointCompletion::SkippedNoCapture(
                "plaintext-flagged sandbox with committed COW state",
            ));
        }

        // Only checkpoint if Started or Paused (skip silently for transient states)
        if sandbox.state != SandboxState::Started && sandbox.state != SandboxState::Paused {
            return Ok(CheckpointCompletion::SkippedNoCapture(
                "sandbox is not started or paused",
            ));
        }

        let previous_state = sandbox.state;

        // Transition to Checkpointing while still holding the lock
        begin_checkpoint_state(&db, sandbox_id)?;
        (previous_state, sandbox.node_execution_fence())
    };
    if previous_state == SandboxState::Started {
        ctx.health_lifecycle.record_edge(sandbox_id).await;
    }

    // 2. Connect to node-manager
    let mut node_client = match ctx.node_manager.connect().await {
        Ok(node_client) => node_client,
        Err(error) => {
            restore_checkpoint_state_best_effort(ctx, sandbox_id, previous_state);
            return Err(format!("cannot connect to node-manager: {error}"));
        }
    };

    // 3. Flush and disconnect guest bridge clients, then pause vCPUs.
    // A VM that was already paused cannot service the preparation RPC.
    if previous_state == SandboxState::Started
        && let Err(e) = prepare_guest_for_vm_snapshot_via_spaced_host(
            sandbox_id,
            GuestSnapshotPreparationBudget::Standard,
        )
        .await
    {
        restore_checkpoint_state_best_effort(ctx, sandbox_id, SandboxState::Started);
        return Err(format!(
            "guest snapshot preparation for checkpoint failed: {e}"
        ));
    }

    if previous_state == SandboxState::Started
        && let Err(e) = node_client
            .pause_sandbox(PauseSandboxRequest {
                execution_fence: execution_fence.clone(),
                sandbox_id: sandbox_id.to_string(),
                balloon_hint_timeout_ms: None,
            })
            .await
    {
        // A transport failure can hide a successfully applied pause. Resume is
        // idempotent, so reconcile to running before restoring Started.
        return Err(rollback_checkpoint_failure(
            ctx,
            &mut node_client,
            sandbox_id,
            execution_fence.clone(),
            previous_state,
            format!("pause for checkpoint failed: {e}"),
        )
        .await);
    }

    if let Err(e) = node_client
        .sync_guest(SyncGuestRequest {
            sandbox_id: sandbox_id.to_string(),
        })
        .await
    {
        let reason = format!("host rootfs sync for checkpoint failed: {e}");
        return Err(rollback_checkpoint_failure(
            ctx,
            &mut node_client,
            sandbox_id,
            execution_fence.clone(),
            previous_state,
            reason,
        )
        .await);
    }

    // 4. Create the full snapshot directory as a writable btrfs snapshot of instance/.
    // This captures disk state at the exact pause point. VM state files (vmstate, memory)
    // are then written into this same directory, making it a self-contained checkpoint.
    // Uses node-manager RPC (runs as root) for btrfs operations.
    let snapshot_dir = format!("{CACHE_DIR}/{sandbox_id}/snapshots/{snapshot_name}");
    if let Err(e) = node_client
        .create_btrfs_snapshot(
            pplx_space_node_mgr_client::proto::CreateBtrfsSnapshotRequest {
                sandbox_id: sandbox_id.to_string(),
                snapshot_name: snapshot_name.to_string(),
                writable: true,
                execution_fence: execution_fence.clone(),
            },
        )
        .await
    {
        let reason = if e.code() == tonic::Code::FailedPrecondition {
            "btrfs disk snapshot unavailable for checkpoint"
        } else {
            "btrfs disk snapshot for checkpoint failed"
        };
        tracing::warn!(
            sandbox_id,
            snapshot_name,
            status_code = ?e.code(),
            error = %e,
            reason,
            "checkpoint disk snapshot failed"
        );
        return Err(rollback_checkpoint_failure(
            ctx,
            &mut node_client,
            sandbox_id,
            execution_fence.clone(),
            previous_state,
            format!("{reason}: {e}"),
        )
        .await);
    }
    if let Err(e) = node_client
        .create_snapshot(CreateSnapshotRequest {
            execution_fence: execution_fence.clone(),
            sandbox_id: sandbox_id.to_string(),
            snapshot_path: "/vmstate".to_string(),
            mem_file_path: "/memory".to_string(),
            dest_dir: Some(snapshot_dir.clone()),
            snapshot_volume_mount_dir: None,
        })
        .await
    {
        return Err(rollback_checkpoint_failure(
            ctx,
            &mut node_client,
            sandbox_id,
            execution_fence.clone(),
            previous_state,
            format!("checkpoint snapshot failed: {e}"),
        )
        .await);
    }

    // 5. Resume vCPUs (skip if sandbox was already paused — caller manages resume)
    let (resume_state_reconciled, lifecycle_error) = if previous_state == SandboxState::Started {
        if let Err(e) = node_client
            .resume_sandbox(ResumeSandboxRequest {
                execution_fence: execution_fence.clone(),
                sandbox_id: sandbox_id.to_string(),
            })
            .await
        {
            let reason = format!("resume after checkpoint failed: {e}");
            let observation = observe_firecracker_vm_state(
                &sandbox_id_typed,
                "completed checkpoint resume reconciliation",
                &ctx.node_manager,
            )
            .await;
            let reason = match observation.unobservable_reason() {
                Some(probe_error) => format!("{reason}; {probe_error}"),
                None => reason,
            };
            let lifecycle_error = finish_completed_checkpoint_after_resume_error(
                ctx,
                sandbox_id,
                reason,
                observation.state(),
            )
            .err();
            (true, lifecycle_error)
        } else {
            (false, None)
        }
    } else {
        (false, None)
    };

    // 6. Restore state — back to Started if we resumed, back to Paused if we didn't
    if !resume_state_reconciled {
        finish_checkpoint_success(ctx, sandbox_id, previous_state)?;
    }

    // 7. Write sidecar metadata for recovery
    let retention_tier = if snapshot_name.starts_with("f-10m-") {
        "10m"
    } else if snapshot_name.starts_with("f-1h-") {
        "1h"
    } else if snapshot_name.starts_with("f-6h-") {
        "6h"
    } else {
        "" // manual checkpoint — no retention tier
    };
    write_snapshot_sidecar(
        &snapshot_dir,
        sandbox_id,
        snapshot_name,
        "full",
        retention_tier,
    );
    write_snapshot_execution_sidecar(&snapshot_dir, sandbox_id, snapshot_name);

    Ok(match lifecycle_error {
        Some(reason) => CheckpointCompletion::CompleteWithLifecycleError(reason),
        None => CheckpointCompletion::Complete,
    })
}

/// Keep only the newest entries seen for each scheduled tier. A partial walk
/// may delete an entry only after seeing enough newer entries to retain.
fn retention_candidate(
    retained: &mut [std::collections::BTreeSet<(String, String)>; 5],
    name: String,
) -> Option<String> {
    let (index, keep, timestamp) = [
        ("d-1m-", 0, KEEP_D_1M),
        ("1m-", 0, KEEP_D_1M),
        ("d-5m-", 1, KEEP_D_5M),
        ("5m-", 1, KEEP_D_5M),
        ("f-10m-", 2, KEEP_F_10M),
        ("f-1h-", 3, KEEP_F_1H),
        ("f-6h-", 4, KEEP_F_6H),
    ]
    .iter()
    .find_map(|(prefix, index, keep)| {
        name.strip_prefix(prefix)
            .map(|timestamp| (*index, *keep, timestamp.to_owned()))
    })?;
    let entries = retained.get_mut(index)?;
    entries.insert((timestamp, name));
    (entries.len() > keep)
        .then(|| entries.pop_first())
        .flatten()
        .map(|(_, name)| name)
}

async fn prune_scheduled_snapshots(
    node_client: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
    ctx: &CheckpointContext,
    sandbox_id: &str,
) -> Result<(), String> {
    let sandbox = sandbox_id
        .parse::<SandboxId>()
        .map_err(|error| error.to_string())?;
    let mut pages = pplx_space_node_mgr_client::SnapshotPages::new(sandbox, false);
    let mut retained = std::array::from_fn(|_| std::collections::BTreeSet::new());
    while let Some(snapshots) = pages
        .next(node_client)
        .await
        .map_err(|e| format!("list snapshots: {e}"))?
    {
        for snapshot in snapshots {
            let Some(name) = retention_candidate(&mut retained, snapshot.name) else {
                continue;
            };
            node_client
                .delete_btrfs_snapshot(
                    pplx_space_node_mgr_client::proto::DeleteBtrfsSnapshotRequest {
                        sandbox_id: sandbox_id.to_string(),
                        snapshot_name: name.clone(),
                    },
                )
                .await
                .map_err(|error| format!("prune {name}: {error}"))?;
            let db = lock_db(ctx)?;
            if let Err(error) = crate::db::delete_named_local_artifact(&db, sandbox_id, &name) {
                tracing::warn!(sandbox_id, snapshot_name = %name, %error,
                    "pruned snapshot but failed to remove local artifact registry entry");
            }
        }
    }
    Ok(())
}

pub(crate) fn snapshot_timestamp(now: jiff::Timestamp) -> String {
    // Snapshot names carry a UTC suffix regardless of the host's local zone.
    now.strftime("%Y%m%dT%H%M%SZ").to_string()
}

#[cfg(test)]
mod tests;
