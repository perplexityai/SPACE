use pplx_space_common_core::types::SandboxId;

use super::*;
use crate::controllers::db_contention_error;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeDrainPhase, NodeDrainState, NodeManagedBy, NodeRole};
use pplx_space_common_svc::sql::contention;
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_sandbox_portability::portable_for_restore_affinity;
use pplx_space_sandbox_store as sandbox_store;
use pplx_space_sandbox_store::fork_source_lifecycle::{
    active_fork_source_operation_exists, lock_fork_source_lifecycle,
};

const FINALIZE_SANDBOX_DB_DELETE_ATTEMPTS: usize = 4;
const FINALIZE_SANDBOX_DB_DELETE_BASE_DELAY_MS: u64 = 50;

/// Why a cluster delete proceeded to remove the Postgres row without a confirmed
/// node-local teardown. The cluster delete is intentionally best-effort about the
/// node RPC (a dead node must not make a sandbox undeletable), so when it fails
/// the node can be left with an orphaned local sandbox row + on-disk snapshot
/// that no cluster row points at — the genesis of the cold-eviction reupload
/// "cluster-api has no row" loop. Coarse, derived from the substrate error
/// variant; enough to break the rate down by cause in telemetry.
fn cluster_delete_miss_reason(err: &SubstrateError) -> &'static str {
    match err {
        // No assigned/reachable worker — e.g. `active_node_id` is NULL because the
        // sandbox is suspended/unplaced, or the node is unreachable. The node was
        // never contacted, so any local state definitely survives.
        SubstrateError::Unavailable { .. } => "node_unavailable",
        // The node was reached but the delete RPC failed / timed out / was
        // cancelled — local teardown is unconfirmed.
        SubstrateError::Failed { .. } => "node_delete_failed",
        SubstrateError::RetainedFilesystemOnlyCopy { .. } => "retained_filesystem_only_copy",
        SubstrateError::Timeout { .. } => "node_delete_timeout",
        SubstrateError::Cancelled { .. } => "node_delete_cancelled",
        // The node had no such sandbox — local state is already gone (not an
        // orphan); recorded for completeness so it doesn't inflate the orphan rate.
        SubstrateError::NotFound { .. } => "node_already_absent",
        // Its own bucket rather than `other`. Before contention was classified
        // this arrived as `Failed` and read as `node_delete_failed`, which was
        // never accurate — no delete RPC failed, a control-plane statement
        // lost a lock race — but `other` loses the signal entirely, and this
        // is one of the reasons the `lock_timeout` rollout needs to see.
        SubstrateError::LockContention { .. } => "lock_contention",
        _ => "other",
    }
}

fn cluster_delete_must_defer_control_plane(err: &SubstrateError) -> bool {
    matches!(
        err,
        SubstrateError::SuspendInFlight { .. } | SubstrateError::RetainedFilesystemOnlyCopy { .. }
    )
}

fn record_dormant_activation_if_needed(
    sandbox: &Sandbox,
    metric_identity: SandboxApiMetricIdentity,
    action: SandboxAction,
) {
    let (idle_since, age_source) = match sandbox.last_used_at {
        Some(last_used_at) => (
            last_used_at,
            pplx_space_metrics::sandbox::api::DormantActivationAgeSource::LastUsedAt,
        ),
        None => (
            sandbox.updated_at,
            pplx_space_metrics::sandbox::api::DormantActivationAgeSource::UpdatedAt,
        ),
    };

    pplx_space_metrics::sandbox::api::dormant_activation(
        pplx_space_metrics::sandbox::api::DormantActivation {
            identity: metric_identity,
            action: action.as_str(),
            from_lifecycle: sandbox.lifecycle.as_str(),
            idle_age_secs: idle_age_secs(time::OffsetDateTime::now_utc(), idle_since),
            age_source,
        },
    );
}

fn idle_age_secs(now: time::OffsetDateTime, idle_since: time::OffsetDateTime) -> u64 {
    now.unix_timestamp()
        .checked_sub(idle_since.unix_timestamp())
        .and_then(|secs| u64::try_from(secs).ok())
        .unwrap_or(0)
}

fn emit_api_sandbox_event(
    sandbox_id: SandboxId,
    event_type: Option<SandboxEventType>,
    action: SandboxAction,
    details: SandboxEventDetails,
) {
    if let Some(event_type) = event_type {
        SandboxEvent::new(sandbox_id, event_type, SandboxEventSource::Api)
            .emit_with(details.action(action.as_str()));
    }
}

/// The finalize path opens a transaction, takes the fork-source advisory
/// lock, then locks the sandbox row `FOR UPDATE` — every one of which can
/// abort with `55P03` once the application role carries a `lock_timeout`,
/// where it previously blocked until the peer transaction committed. Those
/// aborts are exactly what the retry loop is for: the wait was bounded, not
/// refused.
fn is_retryable_finalize_sandbox_db_delete_error(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut => true,
        _ => contention::is_lock_contention(err),
    }
}

/// Single source of truth for the 409 response body when the
/// in-flight-suspend gate refuses a DELETE. Used both at the
/// pre-flight check (before runtime teardown) and at the locked
/// check inside the finalize transaction.
fn in_flight_suspend_upload_conflict() -> ControllerError {
    ControllerError::conflict(
        "sandbox has a suspend-snapshot upload in flight; \
         retry the DELETE after the snapshot finalizes",
    )
}

fn fork_in_progress_conflict() -> ControllerError {
    ControllerError::conflict(
        "sandbox has a fork operation in progress; retry the DELETE after the fork completes",
    )
}

async fn begin_delete_against_active_fork(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<Option<pplx_space_common_core::types::SandboxLifecycle>, ControllerError> {
    let mut guard = pool.begin().await.map_err(|error| {
        // Classified like the advisory-lock acquisition on the next line,
        // which would otherwise be unreachable on a contended `begin`. A
        // `BEGIN` takes no locks, so in practice this fails on pool
        // acquisition — which `db_contention_error` deliberately keeps a 500 —
        // but exempting a statement because it "cannot" contend rests on a
        // property no reader of this line can see.
        let mapped = db_contention_error("failed to serialize sandbox deletion", &error);
        if !mapped.is_lock_contention() {
            tracing::error!(%sandbox_id, %error, "failed to begin sandbox delete guard");
        }
        mapped
    })?;
    lock_fork_source_lifecycle(&mut guard, sandbox_id)
        .await
        .map_err(|error| db_contention_error("failed to lock sandbox delete lifecycle", &error))?;
    let active_fork = active_fork_source_operation_exists(&mut guard, sandbox_id)
        .await
        .map_err(|error| {
            db_contention_error("fork precheck failed before sandbox DELETE", &error)
        })?;
    if active_fork {
        return Err(fork_in_progress_conflict());
    }
    let previous_lifecycle = sqlx::query_scalar::<_, String>(sql!(
        "SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE"
    ))
    .bind(sandbox_id)
    .fetch_optional(&mut *guard)
    .await
    .map_err(|error| db_contention_error("failed to read sandbox lifecycle before delete", &error))?
    .map(|lifecycle| {
        lifecycle
            .parse::<pplx_space_common_core::types::SandboxLifecycle>()
            .map_err(|error| {
                tracing::error!(%sandbox_id, %error, "sandbox has invalid lifecycle before delete");
                ControllerError::internal("sandbox has invalid lifecycle")
            })
    })
    .transpose()?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET lifecycle = 'deleting'
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&mut *guard)
    .await
    .map_err(|error| db_contention_error("failed to persist sandbox delete intent", &error))?;
    guard
        .commit()
        .await
        .map_err(|error| db_contention_error("failed to release sandbox delete guard", &error))?;
    Ok(previous_lifecycle)
}

async fn restore_lifecycle_after_failed_delete(
    pool: &PgPool,
    sandbox_id: SandboxId,
    lifecycle: pplx_space_common_core::types::SandboxLifecycle,
) -> Result<(), ControllerError> {
    let changed = sqlx::query(sql!(
        "UPDATE sandboxes
         SET lifecycle = $2
         WHERE id = $1 AND lifecycle = 'deleting'"
    ))
    .bind(sandbox_id)
    .bind(lifecycle.as_str())
    .execute(pool)
    .await
    // This compensating UPDATE takes a row lock on `sandboxes`, so it can lose
    // a race with any concurrent writer of that row. Its caller uses `?`, so a
    // failure here replaces the original delete error *and* leaves the row in
    // `lifecycle = 'deleting'` — which denies fork admission against the
    // sandbox and makes the "retry the DELETE" advice unachievable. Classified
    // so the caller is at least told to retry; the residual is that a failed
    // restore still leaves the row pinned until that retry lands.
    .map_err(|error| {
        db_contention_error("failed to restore sandbox after deferred deletion", &error)
    })?
    .rows_affected();
    if changed == 1 {
        Ok(())
    } else {
        Err(ControllerError::internal(
            "sandbox lifecycle changed while deletion was deferred",
        ))
    }
}

async fn restore_lifecycle_after_failed_finalize_delete(
    pool: &PgPool,
    sandbox_id: SandboxId,
    previous_lifecycle: Option<pplx_space_common_core::types::SandboxLifecycle>,
) -> Result<(), ControllerError> {
    let Some(previous_lifecycle) = previous_lifecycle else {
        return Ok(());
    };
    let changed = sqlx::query(sql!(
        "UPDATE sandboxes
         SET lifecycle = $2
         WHERE id = $1 AND lifecycle = 'deleting'"
    ))
    .bind(sandbox_id)
    .bind(previous_lifecycle.as_str())
    .execute(pool)
    .await
    // This compensating UPDATE takes a row lock on `sandboxes`, so it can lose
    // a race with any concurrent writer of that row. Its caller uses `?`, so a
    // failure here replaces the original delete error *and* leaves the row in
    // `lifecycle = 'deleting'` — which denies fork admission against the
    // sandbox and makes the "retry the DELETE" advice unachievable. Classified
    // so the caller is at least told to retry; the residual is that a failed
    // restore still leaves the row pinned until that retry lands.
    .map_err(|error| {
        db_contention_error(
            "failed to restore sandbox after deletion finalization",
            &error,
        )
    })?
    .rows_affected();
    if changed == 1 {
        return Ok(());
    }

    let existing_lifecycle = sandbox_store::get(pool, sandbox_id)
        .await
        .map_err(|error| {
            // Reached only when the compensating UPDATE above matched no rows,
            // so it decides that path's outcome and can block on the same row
            // the UPDATE just contended for. `SandboxStoreError` wraps one
            // transparent `sqlx::Error`, so the classification survives the
            // store boundary — without this the statement one line above
            // returns 503 and this one returns 500 for the same race.
            let error = sqlx::Error::from(error);
            db_contention_error(
                "failed to inspect sandbox after deletion finalization",
                &error,
            )
        })?
        .map(|record| record.lifecycle);
    if existing_lifecycle.is_none() {
        Ok(())
    } else {
        Err(ControllerError::internal(
            "sandbox lifecycle changed while deletion finalization failed",
        ))
    }
}

/// Retry wrapper around the user-facing finalize path. The serialization
/// failure / deadlock retry is for transient DB contention; the
/// `InFlightSuspendUpload` variant is **not** retryable — that error
/// means the snapshot upload is still happening and the user needs to
/// wait for it. Surfacing it after even one transient retry would be a
/// lie about how many times we actually consulted the DB state.
async fn finalize_sandbox_db_delete_user_initiated_resilient(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<(), FinalizeSandboxDbDeleteError> {
    let mut delay_ms = FINALIZE_SANDBOX_DB_DELETE_BASE_DELAY_MS;

    for attempt in 1..=FINALIZE_SANDBOX_DB_DELETE_ATTEMPTS {
        match finalize_sandbox_db_delete_user_initiated(pool, sandbox_id).await {
            Ok(()) => return Ok(()),
            Err(FinalizeSandboxDbDeleteError::Db(err))
                if attempt < FINALIZE_SANDBOX_DB_DELETE_ATTEMPTS
                    && is_retryable_finalize_sandbox_db_delete_error(&err) =>
            {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    attempt,
                    max_attempts = FINALIZE_SANDBOX_DB_DELETE_ATTEMPTS,
                    error = %err,
                    "sandbox DB delete finalization failed; retrying"
                );
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                delay_ms *= 2;
            }
            Err(err) => return Err(err),
        }
    }

    unreachable!("sandbox delete finalization loop must return before exhausting attempts")
}

fn post_wake_lifecycle_allows_started_update(lifecycle: SandboxLifecycle) -> bool {
    !matches!(
        lifecycle,
        SandboxLifecycle::Failed
            | SandboxLifecycle::Quarantined
            | SandboxLifecycle::Stopped
            | SandboxLifecycle::Stopping
            | SandboxLifecycle::Deleting
    )
}

fn wake_lifecycle_allows_started_update(lifecycle: &str) -> bool {
    match lifecycle
        .trim()
        .to_ascii_lowercase()
        .parse::<SandboxLifecycle>()
    {
        Ok(lifecycle) => post_wake_lifecycle_allows_started_update(lifecycle),
        Err(_) => !matches!(
            lifecycle.trim().to_ascii_lowercase().as_str(),
            "deleted" | "cleanup_required"
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WakeLifecycleCompletion {
    Completed,
    SupersededBySuspend,
    Lost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishedSuspendFence {
    Enforced,
    DirectRestoreBaseline,
}

impl PublishedSuspendFence {
    fn for_substrate(substrate: &crate::controllers::substrate::Substrate) -> Self {
        match substrate {
            crate::controllers::substrate::Substrate::Direct(_) => Self::DirectRestoreBaseline,
            #[cfg(feature = "kubernetes")]
            crate::controllers::substrate::Substrate::Kubernetes(_) => Self::Enforced,
        }
    }
}

/// Complete a wake only while no newer suspend owns the lifecycle.
///
/// A suspend claim can land after the node finishes waking but before this
/// control-plane write. In that case the node-side wake succeeded, so callers
/// may continue, but this stale completion must not replace `suspending` with
/// `started`. Kubernetes also fences a finalized `suspended` state. Direct
/// restores retain `suspended` as their owned baseline because that substrate
/// does not claim `restoring` before its node call. Other CAS losses retain the
/// existing error behavior.
async fn complete_wake_lifecycle(
    pool: &PgPool,
    sandbox_id: SandboxId,
    published_suspend_fence: PublishedSuspendFence,
) -> Result<WakeLifecycleCompletion, sqlx::Error> {
    let completed = sqlx::query_scalar::<_, bool>(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'started', placement = 'on_node'
          WHERE id = $1
            AND LOWER(TRIM(lifecycle)) NOT IN (
                'failed',
                'quarantined',
                'stopped',
                'stopping',
                'suspending',
                'deleting',
                'deleted',
                'cleanup_required'
            )
            AND (
                LOWER(TRIM(lifecycle)) <> 'suspended'
                OR $2 = FALSE
            )
        RETURNING TRUE"
    ))
    .bind(sandbox_id)
    .bind(published_suspend_fence == PublishedSuspendFence::Enforced)
    .fetch_optional(pool)
    .await?;
    if completed.is_some() {
        return Ok(WakeLifecycleCompletion::Completed);
    }

    let lifecycle = sandbox_store::get(pool, sandbox_id)
        .await?
        .map(|record| record.lifecycle);
    if lifecycle.as_deref().is_some_and(|lifecycle| {
        matches!(
            lifecycle.trim().to_ascii_lowercase().as_str(),
            "suspending" | "suspended"
        )
    }) {
        return Ok(WakeLifecycleCompletion::SupersededBySuspend);
    }
    Ok(WakeLifecycleCompletion::Lost)
}

/// Every sandbox action verb the controller cares about. Routes parse incoming
/// HTTP action strings into this enum once via `FromStr`; the controller then
/// dispatches on variants instead of stringly-typed `match action` arms.
///
/// Variants split into two groups:
/// - **HTTP-parseable lifecycle verbs** (`start`, `wake`, …, `enable_snapshots`,
///   `disable_snapshots`). These come from public routes via `FromStr`.
/// - **Internal access-path labels** (`Access`, `Stats`, `OpenPort`,
///   `Rollback`, `Spaced`, `Update`). These are constructed directly at the
///   call site (they're not user-visible action verbs); they exist so the
///   drain-guard 409 body and metrics name the operation the user actually
///   invoked rather than a misleading literal like `"wake"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxAction {
    Start,
    Wake,
    ImplicitWake(ExecutionActivityKind),
    Resume,
    Restore,
    Suspend,
    Stop,
    Kill,
    Pause,
    Checkpoint,
    EnableSnapshots,
    DisableSnapshots,
    Access,
    Stats,
    OpenPort,
    Rollback,
    Spaced,
    Update,
}

impl SandboxAction {
    fn wake_activity(self) -> Option<ExecutionActivityKind> {
        match self {
            Self::ImplicitWake(activity) => Some(activity),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Wake | Self::ImplicitWake(_) => "wake",
            Self::Resume => "resume",
            Self::Restore => "restore",
            Self::Suspend => "suspend",
            Self::Stop => "stop",
            Self::Kill => "kill",
            Self::Pause => "pause",
            Self::Checkpoint => "checkpoint",
            Self::EnableSnapshots => "enable_snapshots",
            Self::DisableSnapshots => "disable_snapshots",
            Self::Access => "access",
            Self::Stats => "stats",
            Self::OpenPort => "open_port",
            Self::Rollback => "rollback",
            Self::Spaced => "spaced",
            Self::Update => "update",
        }
    }

    /// Lifecycle verbs that would (re)place the sandbox onto its current
    /// `active_node_id` or otherwise race the node-drain reconciler. When the
    /// resident node is mid-drain these are rejected with a structured 409 so
    /// callers stop fighting the migration pipeline.
    ///
    /// Cleanup actions (`stop`, `kill`, `delete`) and `suspend` are allowed
    /// through because they either help the reconciler finish (suspend/kill)
    /// or do not touch placement. Note: `pause` and `checkpoint` are *not*
    /// allowed through — they are drain-gated separately inside `action()`
    /// because the k8s `runtime_client` would `ensure_pod_exists` for a
    /// recoverable-live sandbox whose pod IP is gone, racing the reconciler.
    pub fn affects_lifecycle(self) -> bool {
        matches!(
            self,
            Self::Start | Self::Wake | Self::ImplicitWake(_) | Self::Resume | Self::Restore
        )
    }

    /// Classify lifecycle actions that add an execution onto a node for
    /// cluster-wide activation admission. Cold create is handled separately:
    /// it is not a `SandboxAction` because it creates the sandbox row before
    /// the lifecycle action surface exists.
    fn activation_class(
        self,
        lifecycle: Option<SandboxLifecycle>,
    ) -> Option<crate::cluster_admission::ClusterActivationClass> {
        match self {
            // Start adds runtime placement only from a not-yet-running
            // lifecycle. Against an already-running/in-flight/terminal sandbox
            // (started/starting/creating/restoring/failed/deleting) it is a
            // no-op, a join of in-flight work, or a reject, so it must not spend
            // an activation token. No lifecycle context (the internal execute
            // path) charges conservatively as cold.
            Self::Start => match lifecycle {
                Some(SandboxLifecycle::Created) | Some(SandboxLifecycle::Stopped) | None => {
                    Some(crate::cluster_admission::ClusterActivationClass::Cold)
                }
                // `start` on a suspended sandbox loads a snapshot — the
                // substrate routes it through the restore machinery — so it's a
                // restore activation and must be charged as one.
                Some(SandboxLifecycle::Suspended) => {
                    Some(crate::cluster_admission::ClusterActivationClass::Restore)
                }
                Some(_) => None,
            },
            // Restore only adds runtime placement from a suspended snapshot.
            // ECW `resume` maps to `Restore`; against an already-active or
            // otherwise non-suspended sandbox it is a no-op reconnect that must
            // not spend a shared activation token. With no lifecycle context
            // (the internal execute path), charge conservatively.
            Self::Restore => match lifecycle {
                Some(SandboxLifecycle::Suspended) | None => {
                    Some(crate::cluster_admission::ClusterActivationClass::Restore)
                }
                Some(_) => None,
            },
            Self::Wake | Self::ImplicitWake(_) => match lifecycle? {
                SandboxLifecycle::Created | SandboxLifecycle::Stopped => {
                    Some(crate::cluster_admission::ClusterActivationClass::Cold)
                }
                SandboxLifecycle::Suspended => {
                    Some(crate::cluster_admission::ClusterActivationClass::Restore)
                }
                _ => None,
            },
            _ => None,
        }
    }

    pub fn pause_or_checkpoint(self) -> bool {
        matches!(self, Self::Pause | Self::Checkpoint)
    }

    pub fn is_snapshot_policy(self) -> bool {
        matches!(self, Self::EnableSnapshots | Self::DisableSnapshots)
    }

    pub fn is_stop_or_kill(self) -> bool {
        matches!(self, Self::Stop | Self::Kill)
    }

    pub fn requested_event(self) -> Option<SandboxEventType> {
        Some(match self {
            Self::Start => SandboxEventType::StartRequested,
            Self::Wake | Self::ImplicitWake(_) => SandboxEventType::WakeRequested,
            Self::Resume => SandboxEventType::ResumeRequested,
            Self::Restore => SandboxEventType::RestoreRequested,
            Self::Suspend => SandboxEventType::SuspendRequested,
            Self::Stop => SandboxEventType::StopRequested,
            Self::Kill => SandboxEventType::KillRequested,
            Self::Pause => SandboxEventType::PauseRequested,
            Self::Checkpoint => SandboxEventType::CheckpointRequested,
            Self::EnableSnapshots | Self::DisableSnapshots => {
                SandboxEventType::SnapshotPolicyUpdateRequested
            }
            Self::Access
            | Self::Stats
            | Self::OpenPort
            | Self::Rollback
            | Self::Spaced
            | Self::Update => return None,
        })
    }

    pub fn completed_event(self) -> Option<SandboxEventType> {
        Some(match self {
            Self::Start => SandboxEventType::StartCompleted,
            Self::Wake | Self::ImplicitWake(_) => SandboxEventType::WakeCompleted,
            Self::Resume => SandboxEventType::ResumeCompleted,
            Self::Restore => SandboxEventType::RestoreCompleted,
            Self::Suspend => SandboxEventType::SuspendCompleted,
            Self::Stop => SandboxEventType::StopCompleted,
            Self::Kill => SandboxEventType::KillCompleted,
            Self::Pause => SandboxEventType::PauseCompleted,
            Self::Checkpoint => SandboxEventType::CheckpointCompleted,
            Self::EnableSnapshots | Self::DisableSnapshots => {
                SandboxEventType::SnapshotPolicyUpdateCompleted
            }
            Self::Access
            | Self::Stats
            | Self::OpenPort
            | Self::Rollback
            | Self::Spaced
            | Self::Update => return None,
        })
    }

    pub fn failed_event(self) -> Option<SandboxEventType> {
        Some(match self {
            Self::Start => SandboxEventType::StartFailed,
            Self::Wake | Self::ImplicitWake(_) => SandboxEventType::WakeFailed,
            Self::Resume => SandboxEventType::ResumeFailed,
            Self::Restore => SandboxEventType::RestoreFailed,
            Self::Suspend => SandboxEventType::SuspendFailed,
            Self::Stop => SandboxEventType::StopFailed,
            Self::Kill => SandboxEventType::KillFailed,
            Self::Pause => SandboxEventType::PauseFailed,
            Self::Checkpoint => SandboxEventType::CheckpointFailed,
            Self::EnableSnapshots | Self::DisableSnapshots => {
                SandboxEventType::SnapshotPolicyUpdateFailed
            }
            Self::Access
            | Self::Stats
            | Self::OpenPort
            | Self::Rollback
            | Self::Spaced
            | Self::Update => return None,
        })
    }
}

impl std::fmt::Display for SandboxAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for SandboxAction {
    type Err = UnknownSandboxActionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "start" => Self::Start,
            "wake" => Self::Wake,
            "resume" => Self::Resume,
            "restore" => Self::Restore,
            "suspend" => Self::Suspend,
            "stop" => Self::Stop,
            "kill" => Self::Kill,
            "pause" => Self::Pause,
            "checkpoint" => Self::Checkpoint,
            "enable_snapshots" => Self::EnableSnapshots,
            "disable_snapshots" => Self::DisableSnapshots,
            _ => return Err(UnknownSandboxActionError(s.to_owned())),
        })
    }
}

impl TryFrom<pplx_space_public_api::SandboxLifecycleAction> for SandboxAction {
    type Error = UnknownSandboxActionError;

    fn try_from(
        action: pplx_space_public_api::SandboxLifecycleAction,
    ) -> Result<Self, Self::Error> {
        Ok(match action {
            pplx_space_public_api::SandboxLifecycleAction::Start => Self::Start,
            pplx_space_public_api::SandboxLifecycleAction::Stop => Self::Stop,
            pplx_space_public_api::SandboxLifecycleAction::Pause => Self::Pause,
            pplx_space_public_api::SandboxLifecycleAction::Resume => Self::Resume,
            pplx_space_public_api::SandboxLifecycleAction::Suspend => Self::Suspend,
            pplx_space_public_api::SandboxLifecycleAction::Restore => Self::Restore,
            pplx_space_public_api::SandboxLifecycleAction::Wake => Self::Wake,
            pplx_space_public_api::SandboxLifecycleAction::Kill => Self::Kill,
            pplx_space_public_api::SandboxLifecycleAction::Checkpoint => Self::Checkpoint,
            pplx_space_public_api::SandboxLifecycleAction::EnableSnapshots => Self::EnableSnapshots,
            pplx_space_public_api::SandboxLifecycleAction::DisableSnapshots => {
                Self::DisableSnapshots
            }
            pplx_space_public_api::SandboxLifecycleAction::Rollback => Self::Rollback,
            pplx_space_public_api::SandboxLifecycleAction::Delete => {
                return Err(UnknownSandboxActionError(action.as_str().to_owned()));
            }
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Unknown action: {0}")]
pub struct UnknownSandboxActionError(pub String);

fn cluster_admission_controller_error(
    error: crate::cluster_admission::ClusterAdmissionError,
) -> ControllerError {
    // Admission only errors when out of capacity (fail-open cases are handled
    // inside `admit_activation` and never reach here). Both map to a 429 +
    // Retry-After, but only the rate-limit case carries the
    // `ClusterActivationAdmission` cause that the async dispatcher treats as
    // indefinite backpressure. Empty-cluster omits the cause so the dispatcher
    // gives it a *bounded* retry (counted against the transient-error budget) —
    // a durable capacity outage must eventually terminate the create rather than
    // re-queue forever.
    use crate::cluster_admission::ClusterAdmissionError;
    match error {
        ClusterAdmissionError::RateLimited {
            message,
            retry_after,
        } => ControllerError::rate_limited(message)
            .with_retry_after_header(retry_after)
            .with_retry_after_cause(
                crate::controllers::ControllerRetryAfterCause::ClusterActivationAdmission,
            ),
        ClusterAdmissionError::EmptyCluster {
            message,
            retry_after,
        } => ControllerError::rate_limited(message).with_retry_after_header(retry_after),
    }
}

/// Classify the cluster-activation cost of *implicitly* waking a sandbox from
/// its currently-observed lifecycle. Covers the data-plane access path
/// (`access_runtime_client` / `access_sandbox_ip`) and the control-plane
/// `control_runtime_client` wake — the sites that spend a real node slot (and
/// often a Kubernetes pod) without going through an explicit lifecycle action.
///
/// Mirrors the explicit-action [`SandboxAction::activation_class`] granularity:
/// only dormant states that (re)occupy a node slot are charged — `suspended`
/// loads a snapshot ([`ClusterActivationClass::Restore`]), while `created` and
/// `stopped` cold-boot a fresh execution ([`ClusterActivationClass::Cold`]).
/// (The direct substrate's sbx-mgr implicit-wake path starts both `created` and
/// `stopped` on guest access, and the explicit `Start`/`Wake` path already
/// charges both as cold — so the implicit path must too.) `paused` keeps the VM
/// resident (cheap in-place resume, no new slot) and every live lifecycle is a
/// no-op reconnect — neither spends a shared activation token.
fn implicit_activation_class(
    lifecycle: SandboxLifecycle,
) -> Option<crate::cluster_admission::ClusterActivationClass> {
    // Delegate to the explicit-action mapping so the implicit access/control-wake
    // cost of a given dormant lifecycle can never drift from what an explicit
    // `Wake` of the same lifecycle charges — that divergence is the precise
    // inconsistency this limiter exists to close.
    SandboxAction::Wake.activation_class(Some(lifecycle))
}

/// Charge cluster activation admission for a sandbox about to be implicitly
/// woken from its observed lifecycle. A `None` handle (limiter not configured)
/// is a no-op, and an unparseable lifecycle fails open (no charge) — the
/// substrate remains the correctness gate. Fail-open on Redis errors is
/// inherited from [`crate::cluster_admission::ClusterSandboxAdmission::admit_activation`].
async fn charge_implicit_activation(
    admission: Option<&std::sync::Arc<crate::cluster_admission::ClusterSandboxAdmission>>,
    lifecycle: &str,
) -> Result<(), ControllerError> {
    let Some(admission) = admission else {
        return Ok(());
    };
    let Ok(lifecycle) = lifecycle.parse::<SandboxLifecycle>() else {
        return Ok(());
    };
    let Some(class) = implicit_activation_class(lifecycle) else {
        return Ok(());
    };
    admission
        .admit_activation(class)
        .await
        .map_err(cluster_admission_controller_error)
}

/// Access-path admission charge. The substrate honors the sandbox's auto-wake
/// policy and rejects a policy-disallowed implicit wake *before* doing any
/// work, so a disallowed access must not spend a token either.
pub(super) async fn charge_implicit_access_activation(
    admission: Option<&std::sync::Arc<crate::cluster_admission::ClusterSandboxAdmission>>,
    sandbox: &Sandbox,
    activity: ExecutionActivityKind,
) -> Result<(), ControllerError> {
    if !activity.allows_implicit_wake(&sandbox.auto_lifecycle.0) {
        return Ok(());
    }
    charge_implicit_activation(admission, &sandbox.lifecycle).await
}

/// Control-plane wake admission charge for `control_runtime_client`. Its
/// operator-style `wake_execution` bypasses auto-wake policy, so there is no
/// policy gate here — the lifecycle classification alone decides the charge.
pub(super) async fn charge_control_wake_activation(
    admission: Option<&std::sync::Arc<crate::cluster_admission::ClusterSandboxAdmission>>,
    sandbox: &Sandbox,
) -> Result<(), ControllerError> {
    charge_implicit_activation(admission, &sandbox.lifecycle).await
}

impl SandboxController {
    async fn resume_request_with_current_policy(
        &self,
        pool: &PgPool,
        id: SandboxId,
        client: &mut crate::controllers::substrate::RuntimeClient,
        activation: crate::controllers::substrate::ActivationOptions,
    ) -> Result<ResumeRequest, ControllerError> {
        client
            .require_activation_policy_support(activation.ready_timeout)
            .await?;
        let response = client
            .sandbox_state(pplx_space_sbx_mgr_client::proto::StateRequest {
                sandbox_ids: vec![id.to_string()],
            })
            .await?
            .into_inner();
        let generation = response
            .sandboxes
            .into_iter()
            .find(|row| row.sandbox_id == id.to_string())
            .ok_or_else(|| ControllerError::service_unavailable("resident execution is not known"))?
            .execution_generation;
        let policy = pplx_space_sandbox_store::auto_lifecycle_policy(pool, id)
            .await
            .map_err(|error| db_contention_error("failed to reload resume policy", &error.into()))?
            .ok_or_else(|| ControllerError::not_found("sandbox"))?;
        Ok(ResumeRequest {
            sandbox_id: id.to_string(),
            blocking: activation.blocking,
            ready_timeout_secs: Some(activation.ready_timeout.as_secs() as u32),
            activation_auto_lifecycle: Some(policy.into()),
            expected_execution_generation: generation,
        })
    }

    /// Non-recovering runtime client for read / in-place paths that must never
    /// recreate a pod (stats, list-snapshots, pause, checkpoint). A missing
    /// runtime is a retryable 503, not a silent, uncharged pod recreation.
    pub(super) async fn reachable_runtime_client_or_unavailable(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<RuntimeClient, ControllerError> {
        self.substrate
            .reachable_runtime_client(sandbox_id)
            .await?
            .ok_or_else(|| {
                ControllerError::service_unavailable("sandbox runtime is not currently reachable")
            })
    }

    pub(super) async fn admit_cluster_activation(
        &self,
        class: crate::cluster_admission::ClusterActivationClass,
    ) -> Result<(), ControllerError> {
        let Some(admission) = self.cluster_admission.as_ref() else {
            return Ok(());
        };
        admission
            .admit_activation(class)
            .await
            .map_err(cluster_admission_controller_error)
    }

    async fn admit_action_cluster_activation(
        &self,
        action: SandboxAction,
        lifecycle: Option<SandboxLifecycle>,
    ) -> Result<(), ControllerError> {
        let Some(class) = action.activation_class(lifecycle) else {
            return Ok(());
        };
        self.admit_cluster_activation(class).await
    }

    /// Deferred activation charge for lease-claiming wakes/restores. When
    /// the sandbox enters as `suspended` or `restoring`, admission is
    /// charged by the restore-lease claim-winner inside the substrate
    /// instead of at request entry: coalesced followers never spend a
    /// token, and a lease takeover always does.
    fn restore_claim_charge(
        &self,
        lifecycle: Option<SandboxLifecycle>,
    ) -> Option<crate::controllers::substrate::RecoveryCharge> {
        matches!(
            lifecycle,
            Some(SandboxLifecycle::Suspended) | Some(SandboxLifecycle::Restoring)
        )
        .then(|| {
            crate::controllers::substrate::RecoveryCharge::enforce(self.cluster_admission.as_ref())
        })
    }
}

/// Sandbox lifecycle status string returned from `action()` and written into
/// `sandboxes.lifecycle`. `None` from the return tuple means "preserve the
/// current lifecycle" (e.g. snapshot-policy and checkpoint updates).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxLifecycleStatus {
    Started,
    Paused,
    Stopped,
    Suspended,
    Unknown,
}

impl SandboxLifecycleStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
            Self::Suspended => "suspended",
            Self::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for SandboxLifecycleStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

async fn run_stop_or_kill(
    client: &mut RuntimeClient,
    action: SandboxAction,
    sandbox_id: SandboxId,
) -> Result<(), ControllerError> {
    let id_str = sandbox_id.to_string();
    match action {
        SandboxAction::Stop => {
            client
                .stop_sandbox(StopRequest {
                    sandbox_id: id_str,
                    template_filter: None,
                    blocking: true,
                    immediate_kill: false,
                })
                .await
                .map_err(ControllerError::from)?;
            Ok(())
        }
        SandboxAction::Kill => {
            client
                .kill_sandbox(KillRequest {
                    sandbox_id: id_str,
                    signal: 9,
                    blocking: true,
                })
                .await
                .map_err(ControllerError::from)?;
            Ok(())
        }
        _ => unreachable!("run_stop_or_kill caller guards Stop | Kill"),
    }
}

/// Fetch the `drain_phase` of the node that the next placement-affecting
/// call would land on. This must mirror `preferred_restore_node_name`'s
/// resolution priority, otherwise a guard placed in front of the substrate
/// could pass while the substrate then re-pins the pod to a draining node:
///
/// 1. Primary: `COALESCE(target_node_id, active_node_id)` — `target_node_id`
///    takes priority because `preferred_restore_node_name` reads
///    `target_node_id.or(active_node_id)`. Even when both columns are
///    populated and point at different nodes (e.g. an active node already
///    healthy after a partial migration but `target_node_id` still pinned
///    at the original, now-draining source), restore / wake will land on
///    `target_node_id`, so that's the node whose drain phase matters.
///    The kubernetes substrate also clears `active_node_id` in
///    `finish_suspended_execution` while leaving `target_node_id` pinned,
///    so the COALESCE direction matches both the divergence and the
///    post-suspend case.
/// 2. Fallback: the latest `sandbox_k8s_executions.k8s_node_name` →
///    `node_k8s` → `nodes` lookup. `preferred_restore_node_name` falls back
///    to this in **two** distinct cases: (a) both placement columns are
///    NULL (`forget_pod_tracking` cleared them but the execution row
///    remains), and (b) primary is set but the `node_k8s` mapping for that
///    node is missing (the substrate logs a warning and falls through).
///    The guard always consults both: if **either** the primary or the
///    fallback resolves to a draining node, the request is blocked. The
///    fallback rarely "over-blocks" because after a migration the latest
///    execution row matches the new placement.
///
/// Returns `Ok(Some(state))` when either the primary or fallback node is
/// in some drain phase (`draining` / `drained` / `terminating`), carrying that
/// node's `drain_mode` alongside it; the primary's phase wins when both
/// produce a non-NULL value (so a draining `target_node_id` is reported as the
/// source of the conflict). Returns `Ok(None)` when neither candidate resolves
/// to a draining node — direct-substrate sandboxes always fall through here
/// because they have no `sandbox_k8s_executions` rows and no drain phase in
/// practice.
///
/// The mode is resolved through [`NodeDrainMode::from_column_str`], so a NULL
/// or unrecognized `drain_mode` on a node that *is* draining reads as
/// [`NodeDrainMode::Gentle`] and leaves this gate open. That direction is
/// deliberate: a mode nobody set must not 409 every access and placement call
/// for the sandboxes on that node.
pub(super) async fn fetch_active_node_drain_state(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<Option<NodeDrainState>, sqlx::Error> {
    // Suspended-or-restoring sandboxes with a fresh ready S3 snapshot are
    // portable — restore can land them on any healthy node, so a drain phase
    // on their historical placement is no longer a reason to block
    // placement-affecting calls. Skip the rest of the gate in that case via
    // the shared portability predicate. The `lifecycle IN ('suspended',
    // 'restoring')` scope is load-bearing: for `started` / `paused` the
    // canonical state lives in the VM's live memory on the source node, not
    // in the snapshot, so a ready prior-cycle snapshot is stale relative to
    // the live VM and must not release the gate. `restoring` is included
    // because the gate is also called from inner paths after
    // `begin_restore_lifecycle` has flipped the row from `suspended →
    // restoring`; the snapshot is still canonical mid-restore.
    let portable_predicate = portable_for_restore_affinity("s");
    let portable_query = format!(
        "SELECT EXISTS (
             SELECT 1
               FROM sandboxes s
              WHERE s.id = $1
                AND {portable_predicate}
         )"
    );
    let portable: Option<bool> =
        sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(portable_query.as_str())))
            .bind(sandbox_id)
            .fetch_optional(pool)
            .await?;
    if matches!(portable, Some(true)) {
        return Ok(None);
    }

    // Primary lookup: drain phase + mode of the node that the next placement-
    // affecting call would land on. `target_node_id` wins over
    // `active_node_id` so this mirrors `preferred_restore_node_name`.
    // Yields `None` when the sandbox row is missing or both placement
    // columns are NULL, and `Some((None, _))` when the node exists but is not
    // draining — both cases fall through to the executions fallback below.
    let primary: Option<(Option<NodeDrainPhase>, Option<String>, bool)> = sqlx::query_as(sql!(
        "SELECT n.drain_phase,
                n.drain_mode,
                EXISTS (SELECT 1 FROM node_k8s nk WHERE nk.node_id = n.id) AS mapped
           FROM sandboxes s
           JOIN nodes n ON n.id = COALESCE(s.target_node_id, s.active_node_id)
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(pool)
    .await?;
    // `mapped` is whether the primary node has a `node_k8s` row. It decides
    // whether the executions fallback is even reachable: the substrate uses the
    // fallback only when the primary can't be resolved to a k8s node, so for a
    // mapped primary the fallback is not where placement lands and consulting it
    // would 409 on a node nothing is going to touch.
    let primary_mapped = primary.as_ref().is_some_and(|(_, _, mapped)| *mapped);
    let primary_state =
        primary.and_then(|(phase, mode, _)| NodeDrainState::from_columns(phase, mode.as_deref()));
    // Return early **only** when the primary actually blocks. A non-blocking
    // primary must still fall through to the executions fallback, because
    // `preferred_restore_node_name` uses that fallback when the primary has no
    // `node_k8s` mapping — so it, not the primary, is the node placement lands
    // on. Short-circuiting on a gentle primary would mask a forced or
    // terminating fallback and let wake/access race an evacuation there.
    if primary_state.is_some_and(NodeDrainState::blocks_placement) {
        return Ok(primary_state);
    }
    // A non-blocking *mapped* primary is the whole answer: placement resolves to
    // it, so a historical execution row naming some older drained node is not a
    // node this request can land on. Consulting the fallback here would turn a
    // stale row into a spurious 409 against a sandbox on a perfectly serviceable
    // gently-draining node. The fallback still matters when the primary is
    // absent or unmapped — that is when the substrate itself falls back, and
    // when a gentle primary could otherwise mask a forced fallback.
    if primary_mapped {
        return Ok(primary_state);
    }

    // Fallback: the latest `sandbox_k8s_executions.k8s_node_name`. The
    // unique index on (sandbox_id, generation) makes the LIMIT 1
    // deterministic per sandbox.
    let latest_k8s_name: Option<String> = sqlx::query_scalar(sql!(
        "SELECT ske.k8s_node_name
           FROM sandbox_k8s_executions ske
          WHERE ske.sandbox_id = $1
            AND ske.k8s_node_name IS NOT NULL
          ORDER BY ske.generation DESC
          LIMIT 1"
    ))
    .bind(sandbox_id)
    .fetch_optional(pool)
    .await?
    .flatten();
    let Some(k8s_node_name) = latest_k8s_name else {
        return Ok(primary_state);
    };

    // Resolve the latest execution's k8s node name to drain state via
    // `node_k8s`. `node_k8s.k8s_node_name` has no unique constraint, so one
    // physical k8s node can map to several SPACE `nodes` rows (worker + builder
    // roles, etc.), and those rows can differ in both phase and mode.
    //
    // `drain_phase IS NOT NULL` filters out the non-draining siblings so one of
    // them can't mask a draining one, and there is deliberately no `LIMIT`: with
    // one sibling gentle and another forced, an arbitrary pick would decide
    // whether the gate fires. Preferring a blocking sibling happens below, in
    // Rust, so the mode policy stays where `from_column_str` is rather than
    // being restated in an `ORDER BY`.
    let siblings: Vec<(NodeDrainPhase, Option<String>)> = sqlx::query_as(sql!(
        "SELECT n.drain_phase, n.drain_mode
           FROM node_k8s nk
           JOIN nodes n ON n.id = nk.node_id
          WHERE nk.k8s_node_name = $1
            AND n.drain_phase IS NOT NULL"
    ))
    .bind(k8s_node_name)
    .fetch_all(pool)
    .await?;
    let siblings: Vec<NodeDrainState> = siblings
        .into_iter()
        .filter_map(|(phase, mode)| NodeDrainState::from_columns(Some(phase), mode.as_deref()))
        .collect();
    // Precedence: any blocking candidate wins, whichever side it came from; else
    // the primary if it had a phase at all (so a caller can still see "draining,
    // gently"); else any sibling.
    Ok(siblings
        .iter()
        .copied()
        .find(|state| state.blocks_placement())
        .or(primary_state)
        .or_else(|| siblings.first().copied()))
}

/// Build the 409 returned when a drained-node guard fires. Centralised so
/// the explicit-action path and the implicit-wake path return identically
/// shaped errors.
pub(super) fn drain_block_conflict(
    action: SandboxAction,
    phase: NodeDrainPhase,
) -> ControllerError {
    ControllerError::conflict(format!(
        "sandbox cannot {action} while its resident node is {phase}; \
         the node-drain reconciler is migrating it off"
    ))
}

/// Drain gate for paths that will unconditionally place / re-place the sandbox
/// on its resolved node (`COALESCE(target_node_id, active_node_id)`),
/// regardless of lifecycle: the explicit-action gate in
/// `SandboxController::action()` and the k8s fallback in
/// `control_runtime_client`, where `wake_execution` fires whenever the
/// runtime is unreachable (including non-dormant lifecycles like `started`
/// or `restoring`).
///
/// Returns 409 when the resolved node is `drained` or `terminating`, or is
/// `draining` under a **forced** drain. A gentle drain does not block — see
/// [`NodeDrainState::blocks_placement`].
pub(super) async fn ensure_resolved_node_not_draining(
    pool: &PgPool,
    sandbox_id: SandboxId,
    action: SandboxAction,
) -> Result<(), ControllerError> {
    let drain_state = fetch_active_node_drain_state(pool, sandbox_id)
        .await
        .map_err(|err| {
            crate::controllers::db_contention_error("failed to load node drain state", &err)
        })?;
    if let Some(state) = drain_state
        && state.blocks_placement()
    {
        return Err(drain_block_conflict(action, state.phase));
    }
    Ok(())
}

/// Access-path drain gate. Returns 409 when the sandbox's resolved node
/// (`COALESCE(target_node_id, active_node_id)`) blocks placement, for any
/// lifecycle. Used by access paths (`access_sandbox_ip`,
/// `spaced_client(activity)`, `exec`, `apply_open_sandbox_port`) that flow
/// through the substrate's `access_runtime_client` / `access_sandbox_ip` —
/// both of which call `ensure_pod_exists` for any recoverable-live lifecycle
/// when the pod IP is gone, re-pinning the new pod to `target_node_id`. A
/// dormant-only gate would let a `started` sandbox whose runtime is briefly
/// unreachable race the reconciler. Under a **forced** drain the cost is that
/// exec/spaced/proxy traffic against a live sandbox on a draining node 409s
/// for the short window before the reconciler suspends + migrates it; users
/// see a clear "node is draining" error rather than a flaky stuck pod.
///
/// Under a **gentle** drain there is no migration window to protect, so this
/// does not fire while the node is `draining` — serving traffic normally is
/// the mode's entire purpose. It still fires once the node reaches `drained`
/// or `terminating`.
pub(super) async fn ensure_access_path_not_blocked_by_drain(
    pool: &PgPool,
    sandbox: &Sandbox,
    action: SandboxAction,
) -> Result<(), ControllerError> {
    ensure_resolved_node_not_draining(pool, sandbox.id, action).await
}

impl SandboxController {
    async fn delete_observed_sandbox(
        &self,
        pool: &PgPool,
        id: SandboxId,
    ) -> Result<(), ControllerError> {
        let observed = fetch_sandbox_observed_states(pool, &[id]).await?;
        let observed = observed
            .get(&id)
            .ok_or_else(|| ControllerError::not_found("Sandbox not found"))?;

        let target = NodeApiTarget {
            managed_by: observed
                .node_managed_by
                .as_deref()
                .and_then(|value| NodeManagedBy::parse(value).ok())
                .unwrap_or(NodeManagedBy::Space),
            role: observed
                .node_role
                .as_deref()
                .and_then(|value| NodeRole::parse(value).ok())
                .unwrap_or(NodeRole::Worker),
            direct_endpoint: observed.node_api_endpoint.as_deref(),
            substrate_node_id: observed
                .substrate_node_id
                .as_deref()
                .or(observed.node_name.as_deref()),
        };

        self.substrate
            .delete_observed_execution(id, &target)
            .await
            .map_err(ControllerError::from)?;

        tracing::info!(
            sandbox_id = %id,
            pool_template = observed.pool_template.as_deref().unwrap_or(""),
            node_id = ?observed.node_id,
            "deleted observed-only sandbox"
        );
        Ok(())
    }

    pub async fn delete(
        &self,
        pool: &PgPool,
        id: SandboxId,
        sandbox: Option<&Sandbox>,
    ) -> Result<(), ControllerError> {
        if sandbox.is_none() {
            return self.delete_observed_sandbox(pool, id).await;
        }

        if let Err(err) = self.substrate.delete_execution(id).await {
            if cluster_delete_must_defer_control_plane(&err) {
                return Err(ControllerError::from(err));
            }
            tracing::warn!(
                sandbox_id = %id,
                reason = cluster_delete_miss_reason(&err),
                error = %err,
                "cluster delete removing sandbox DB row without confirmed node-local teardown; \
                 node may retain an orphaned local sandbox row + snapshot (no cluster row)"
            );
        }
        let _ = pool;
        Ok(())
    }

    pub async fn delete_control_plane(
        &self,
        pool: &PgPool,
        id: SandboxId,
        sandbox: Option<&Sandbox>,
    ) -> Result<(), ControllerError> {
        let event_sandbox_id = id;
        let delete_started = std::time::Instant::now();
        SandboxEvent::new(
            event_sandbox_id,
            SandboxEventType::DeleteRequested,
            SandboxEventSource::Api,
        )
        .emit_with(
            SandboxEventDetails::default()
                .action("delete")
                .reason("api_delete")
                .trigger(SandboxEventTrigger::User),
        );

        // Pre-flight in-flight-upload gate. Run BEFORE `self.delete()`
        // so a 409 doesn't tear down the runtime as a side effect.
        // The authoritative locked check still happens inside
        // `finalize_sandbox_db_delete_user_initiated_resilient`; this
        // pre-check just catches the common case (suspend in flight
        // when DELETE arrives) without paying the cost of the
        // substrate delete. A `BeginArtifactBackup` arriving in the
        // narrow window between this pre-check and the locked
        // finalize is caught by the finalize's own check — the
        // execution-already-deleted edge case for that window is
        // self-healing (retry the DELETE once the upload finalizes).
        let phase_started = std::time::Instant::now();
        tracing::info!(sandbox_id = %id, phase = "snapshot_precheck", result = "started", "sandbox delete phase started");
        let snapshot_precheck = any_creating_snapshot_for_sandbox(pool, id).await;
        tracing::info!(
            sandbox_id = %id,
            phase = "snapshot_precheck",
            result = if snapshot_precheck.is_ok() { "completed" } else { "failed" },
            elapsed_ms = phase_started.elapsed().as_millis(),
            "sandbox delete phase completed"
        );
        if snapshot_precheck.map_err(|err| {
            SandboxEvent::new(
                event_sandbox_id,
                SandboxEventType::DeleteFailed,
                SandboxEventSource::Api,
            )
            .emit_with(
                SandboxEventDetails::default()
                    .action("delete")
                    .reason("snapshot_precheck_failed")
                    .error(err.to_string())
                    .trigger(SandboxEventTrigger::User),
            );
            // Classified: the pre-check is a plain `SELECT` on
            // `snapshots`, so under the role-level `lock_timeout` it aborts
            // with `55P03` whenever something holds a conflicting table
            // lock — in practice DDL, which takes `ACCESS EXCLUSIVE`. It
            // runs before `self.delete()` touches anything, so nothing
            // has been written when it fails.
            //
            // Durable lifecycle executors classify this controller error and
            // requeue transient failures. The E2B DELETE observer returns 204
            // only after the operation reaches authoritative finality.
            //
            // The `error!` is gated, not dropped, and it is this
            // path's only sink for a real failure: the durable executor
            // persists the error without rendering `ControllerError`.
            // `db_contention_error` already logs the contention case at
            // `warn`, so only the other case needs this.
            let mapped = db_contention_error("failed to check snapshot state", &err);
            if !mapped.is_lock_contention() {
                tracing::error!(
                    sandbox_id = %id,
                    error = %err,
                    "in-flight precheck failed before sandbox DELETE"
                );
            }
            mapped
        })? {
            SandboxEvent::new(
                event_sandbox_id,
                SandboxEventType::DeleteFailed,
                SandboxEventSource::Api,
            )
            .emit_with(
                SandboxEventDetails::default()
                    .action("delete")
                    .reason("in_flight_suspend_upload")
                    .trigger(SandboxEventTrigger::User),
            );
            tracing::info!(
                sandbox_id = %id,
                "rejecting sandbox DELETE pre-flight: \
                 suspend-snapshot upload in flight"
            );
            return Err(in_flight_suspend_upload_conflict());
        }
        // Publish delete intent while holding the same lifecycle lock as fork
        // admission. New forks are denied from this commit through runtime
        // teardown, closing the precheck/remote-delete race.
        let phase_started = std::time::Instant::now();
        tracing::info!(sandbox_id = %id, phase = "persist_delete_intent", result = "started", "sandbox delete phase started");
        let delete_intent = begin_delete_against_active_fork(pool, id).await;
        tracing::info!(sandbox_id = %id, phase = "persist_delete_intent", result = if delete_intent.is_ok() { "completed" } else { "failed" }, elapsed_ms = phase_started.elapsed().as_millis(), "sandbox delete phase completed");
        let previous_lifecycle = delete_intent?;
        let phase_started = std::time::Instant::now();
        tracing::info!(sandbox_id = %id, phase = "substrate_teardown", result = "started", "sandbox delete phase started");
        let delete_result = self.delete(pool, id, sandbox).await;
        tracing::info!(sandbox_id = %id, phase = "substrate_teardown", result = if delete_result.is_ok() { "completed" } else { "failed" }, elapsed_ms = phase_started.elapsed().as_millis(), "sandbox delete phase completed");
        if let Err(err) = delete_result {
            SandboxEvent::new(
                event_sandbox_id,
                SandboxEventType::DeleteFailed,
                SandboxEventSource::Api,
            )
            .emit_with(
                SandboxEventDetails::default()
                    .action("delete")
                    .reason("delete_execution_failed")
                    .error(err.to_string())
                    .trigger(SandboxEventTrigger::User),
            );
            if let Some(previous_lifecycle) = previous_lifecycle {
                restore_lifecycle_after_failed_delete(pool, id, previous_lifecycle).await?;
            }
            return Err(err);
        }
        let phase_started = std::time::Instant::now();
        tracing::info!(sandbox_id = %id, phase = "database_finalization", result = "started", "sandbox delete phase started");
        let finalization = finalize_sandbox_db_delete_user_initiated_resilient(pool, id).await;
        tracing::info!(sandbox_id = %id, phase = "database_finalization", result = if finalization.is_ok() { "completed" } else { "failed" }, elapsed_ms = phase_started.elapsed().as_millis(), "sandbox delete phase completed");
        if let Err(err) = finalization {
            restore_lifecycle_after_failed_finalize_delete(pool, id, previous_lifecycle).await?;
            return Err(match err {
                // Mid-upload suspend caught at the locked check
                // (a `BeginArtifactBackup` arrived between our
                // pre-flight and the finalize). The runtime is gone,
                // but the prior lifecycle is restored so the user can
                // retry DELETE after the upload finalizes.
                FinalizeSandboxDbDeleteError::InFlightSuspendUpload => {
                    SandboxEvent::new(
                        event_sandbox_id,
                        SandboxEventType::DeleteFailed,
                        SandboxEventSource::Api,
                    )
                    .emit_with(
                        SandboxEventDetails::default()
                            .action("delete")
                            .reason("in_flight_suspend_upload_locked")
                            .trigger(SandboxEventTrigger::User),
                    );
                    tracing::info!(
                        sandbox_id = %id,
                        "rejecting sandbox DELETE at locked check: \
                         suspend-snapshot upload landed mid-delete"
                    );
                    in_flight_suspend_upload_conflict()
                }
                FinalizeSandboxDbDeleteError::ForkInProgress => fork_in_progress_conflict(),
                FinalizeSandboxDbDeleteError::Db(err) => {
                    SandboxEvent::new(
                        event_sandbox_id,
                        SandboxEventType::DeleteFailed,
                        SandboxEventSource::Api,
                    )
                    .emit_with(
                        SandboxEventDetails::default()
                            .action("delete")
                            .reason("db_delete_failed")
                            .error(err.to_string())
                            .trigger(SandboxEventTrigger::User),
                    );
                    tracing::error!(
                        sandbox_id = %id,
                        error = %err,
                        "failed to finalize sandbox DB delete"
                    );
                    // Exhausting the retry budget does not make contention
                    // permanent — it means the peer held the row longer than
                    // four bounded attempts, and a later retry can still win.
                    // Classifying only the individual statements left this
                    // exit reporting the whole delete as a 500, so a caller
                    // that hit sustained contention was told to stop trying.
                    db_contention_error("failed to delete sandbox record", &err)
                }
            });
        }
        SandboxEvent::new(
            event_sandbox_id,
            SandboxEventType::DeleteCompleted,
            SandboxEventSource::Api,
        )
        .emit_with(
            SandboxEventDetails::default()
                .action("delete")
                .reason("api_delete")
                .trigger(SandboxEventTrigger::User),
        );
        tracing::info!(sandbox_id = %id, phase = "delete_control_plane", result = "completed", elapsed_ms = delete_started.elapsed().as_millis(), "sandbox delete reached authoritative finality");
        Ok(())
    }

    pub async fn action(
        &self,
        pool: &PgPool,
        id: SandboxId,
        sandbox: Option<&Sandbox>,
        metric_identity: SandboxApiMetricIdentity,
        action: SandboxAction,
        activation: Option<SandboxActivationRequest>,
    ) -> Result<serde_json::Value, ControllerError> {
        let typed_id = id;
        let Some(sandbox) = sandbox else {
            return self
                .untracked_action(id, metric_identity, action, activation)
                .await;
        };

        let reconnect_started_at = std::time::Instant::now();
        if action.affects_lifecycle()
            && let Err(err) = ensure_resolved_node_not_draining(pool, id, action).await
        {
            if matches!(action, SandboxAction::Wake | SandboxAction::ImplicitWake(_)) {
                record_reconnect_attempt(
                    metric_identity,
                    sandbox.lifecycle.as_str(),
                    ReconnectOutcome::Rejected,
                    ReconnectTerminalPhase::Api,
                    ReconnectReason::DrainBlocked,
                    reconnect_started_at,
                );
            }
            return Err(err);
        }

        // Validate activation options before admission and the requested event:
        // a malformed request (e.g. `ready_timeout_secs=0`) must be rejected
        // without spending a token or leaving an orphaned `*_requested` event.
        // Only Start/Restore/Resume carry these; for other actions `activation`
        // is `None`, so this resolves to defaults and never errors.
        let activation_options = resolve_activation_options(activation.as_ref(), true)?;

        // Gate cluster activation admission BEFORE emitting the `*_requested`
        // event. A rejected or backpressured activation must not leave an
        // orphaned requested event with no terminal outcome: the async
        // operation dispatcher re-drives the action on every requeue, so an
        // early requested-without-terminal would repeat indefinitely. Admission
        // is a rate-limit decision on the lifecycle observed at request entry,
        // not a consistency guard for races with other lifecycle writers. For
        // Wake the terminal-lifecycle check runs first — a Wake the guard
        // rejects must not spend a shared token or draw a 429 either.
        match action {
            SandboxAction::Start | SandboxAction::Restore => {
                let lifecycle = sandbox.lifecycle.parse::<SandboxLifecycle>().ok();
                // Restore-shaped starts defer admission to the restore-lease
                // claim-winner too. Direct has no lease and spends the same
                // policy at its substrate entry.
                if self.restore_claim_charge(lifecycle).is_none() {
                    self.admit_action_cluster_activation(action, lifecycle)
                        .await?;
                }
            }
            SandboxAction::Wake | SandboxAction::ImplicitWake(_) => {
                if !wake_lifecycle_allows_started_update(sandbox.lifecycle.as_str()) {
                    record_reconnect_attempt(
                        metric_identity,
                        sandbox.lifecycle.as_str(),
                        ReconnectOutcome::Rejected,
                        ReconnectTerminalPhase::Api,
                        ReconnectReason::TerminalLifecycle,
                        reconnect_started_at,
                    );
                    return Err(ControllerError::conflict(format!(
                        "sandbox cannot be woken while lifecycle is {}",
                        sandbox.lifecycle
                    )));
                }
                let lifecycle = sandbox
                    .lifecycle
                    .parse::<SandboxLifecycle>()
                    .map_err(|error| {
                        ControllerError::internal(format!("sandbox has invalid lifecycle: {error}"))
                    })?;
                // A restore-eligible wake (suspended/restoring) defers the
                // activation charge to the substrate RecoveryCharge path, whose
                // rejections surface as SubstrateError and are recorded there;
                // only the up-front admission charge is handled here. A
                // cluster-activation 429/backpressure here is the
                // control-plane-saturation signature the retry-storm alert
                // exists to catch, so record it on the reconnect series (the
                // error classifier maps the retry-after to exhausted/wait_budget)
                // before returning — otherwise a storm throttled by the cluster
                // limiter is invisible to the monitor.
                if self.restore_claim_charge(Some(lifecycle)).is_none()
                    && let Err(err) = self
                        .admit_action_cluster_activation(action, Some(lifecycle))
                        .await
                {
                    let (outcome, terminal_phase, reason) =
                        reconnect_controller_error_outcome(sandbox.lifecycle.as_str(), &err);
                    record_reconnect_attempt(
                        metric_identity,
                        sandbox.lifecycle.as_str(),
                        outcome,
                        terminal_phase,
                        reason,
                        reconnect_started_at,
                    );
                    return Err(err);
                }
            }
            _ => {}
        }

        let id_str = id.to_string();
        let body = serde_json::json!({"ok": true, "action": action.as_str(), "sandbox_id": id_str});
        emit_api_sandbox_event(
            typed_id,
            action.requested_event(),
            action,
            SandboxEventDetails::default()
                .lifecycle_state(sandbox.lifecycle.as_str())
                .placement(sandbox.placement.as_str()),
        );
        let result: Result<(Option<SandboxLifecycleStatus>, serde_json::Value), ControllerError> =
            match action {
                SandboxAction::Start => {
                    let activation = activation_options;
                    let execution = match self
                        .substrate
                        .start_execution(
                            metric_identity,
                            typed_id,
                            activation,
                            self.restore_claim_charge(
                                sandbox.lifecycle.parse::<SandboxLifecycle>().ok(),
                            )
                            .unwrap_or(crate::controllers::substrate::RecoveryCharge::Exempt),
                        )
                        .await
                    {
                        Ok(execution) => execution,
                        Err(err) => {
                            pplx_space_metrics::sandbox::api::lifecycle_error(
                                id,
                                metric_identity,
                                "substrate",
                            );
                            emit_api_sandbox_event(
                                typed_id,
                                action.failed_event(),
                                action,
                                SandboxEventDetails::default()
                                    .reason("substrate")
                                    .error(err.to_string()),
                            );
                            return Err(ControllerError::from(err));
                        }
                    };
                    if !execution.completion.should_finalize_lifecycle() {
                        return Ok(body);
                    }
                    maybe_update_sandbox_state(pool, id, "started", "on_node").await;
                    Ok((Some(SandboxLifecycleStatus::Started), body))
                }
                SandboxAction::Wake | SandboxAction::ImplicitWake(_) => {
                    let reconnect_initial_lifecycle = sandbox.lifecycle.as_str();
                    // Cancellation shield: once the wake is dispatched, the
                    // node finishes the restore whether or not the caller
                    // stays connected — but the `restoring -> started` CAS
                    // below lives in this request future and dies with it.
                    // Armed here, defused on every explicit exit; a Drop
                    // while armed is a cancelled request, and the guard
                    // spawns a bounded redrive that hydrates the execution
                    // and runs a routability-fenced completion CAS.
                    let mut completion_guard = wake_completion::WakeCompletionGuard::unarmed(
                        id,
                        pplx_space_metrics::sandbox::api::WakeCompletionAction::Wake,
                    );
                    if sandbox
                        .lifecycle
                        .parse::<SandboxLifecycle>()
                        .is_ok_and(|lc| wake_completion::shield_applies(&self.substrate, lc))
                    {
                        completion_guard.arm(wake_completion::wake_completion_redrive(
                            pool.clone(),
                            self.substrate.clone(),
                            id,
                            pplx_space_metrics::sandbox::api::WakeCompletionAction::Wake,
                            wake_completion::redrive_budget(),
                        ));
                    }
                    let execution = match self
                        .substrate
                        .wake_execution(
                            metric_identity,
                            sandbox,
                            DEFAULT_BLOCKING_WAIT_TIMEOUT,
                            action.wake_activity(),
                            self.restore_claim_charge(
                                sandbox.lifecycle.parse::<SandboxLifecycle>().ok(),
                            )
                            .unwrap_or(crate::controllers::substrate::RecoveryCharge::Exempt),
                        )
                        .await
                    {
                        Ok(execution) => execution,
                        Err(err) => {
                            // The handler survived to observe the error — the
                            // shield is for cancellation only; error paths keep
                            // their existing retry semantics.
                            completion_guard.defuse();
                            record_reconnect_substrate_error(
                                metric_identity,
                                reconnect_initial_lifecycle,
                                &err,
                                reconnect_started_at,
                            );
                            pplx_space_metrics::sandbox::api::lifecycle_error(
                                id,
                                metric_identity,
                                "substrate",
                            );
                            emit_api_sandbox_event(
                                typed_id,
                                action.failed_event(),
                                action,
                                SandboxEventDetails::default()
                                    .reason("substrate")
                                    .error(err.to_string()),
                            );
                            return Err(ControllerError::from(err));
                        }
                    };
                    if !execution.completion.should_finalize_lifecycle() {
                        completion_guard.defuse();
                        record_reconnect_attempt(
                            metric_identity,
                            reconnect_initial_lifecycle,
                            ReconnectOutcome::WakeSubmitted,
                            ReconnectTerminalPhase::Ready,
                            ReconnectReason::Ok,
                            reconnect_started_at,
                        );
                        return Ok(body);
                    }
                    // Same race as the explicit `Restore` arm below: a wake
                    // on a `suspended` sandbox goes through
                    // `prepare_wake_sandbox → begin_restore_lifecycle`, which
                    // flips the row to `restoring`. If the transient-lifecycle
                    // sweeper runs during a slow substrate wake and flips the
                    // row to `failed` (and tears down the rebuilt pod), the
                    // unconditional write this used to perform would resurrect
                    // the row as `started` with no backing execution. CAS-guard
                    // on teardown/terminal lifecycles — states the wake
                    // completion must never overwrite if a stop, delete, or
                    // newer suspend wins while substrate work is in flight.
                    let published_suspend_fence =
                        PublishedSuspendFence::for_substrate(&self.substrate);
                    let cas_result =
                        complete_wake_lifecycle(pool, id, published_suspend_fence).await;
                    // Explicit exit either way from here (Ok, cas-lost, or DB
                    // error surfaced to the caller): the handler is alive to
                    // decide, so cancellation recovery must not fire.
                    completion_guard.defuse();
                    let completion = cas_result.map_err(|err| {
                        db_contention_error("post-wake lifecycle update failed", &err)
                    })?;
                    if completion == WakeLifecycleCompletion::SupersededBySuspend {
                        record_reconnect_attempt(
                            metric_identity,
                            reconnect_initial_lifecycle,
                            ReconnectOutcome::WakeInProgress,
                            ReconnectTerminalPhase::Restore,
                            ReconnectReason::RestoreInFlight,
                            reconnect_started_at,
                        );
                        return Ok(body);
                    }
                    if completion == WakeLifecycleCompletion::Lost {
                        record_reconnect_attempt(
                            metric_identity,
                            reconnect_initial_lifecycle,
                            ReconnectOutcome::WakeFailed,
                            ReconnectTerminalPhase::Api,
                            ReconnectReason::PostWakeCasLost,
                            reconnect_started_at,
                        );
                        pplx_space_metrics::sandbox::api::lifecycle_error(
                            id,
                            metric_identity,
                            "post_wake_cas_lost",
                        );
                        emit_api_sandbox_event(
                            typed_id,
                            Some(SandboxEventType::LifecycleTransitionCasLost),
                            action,
                            SandboxEventDetails::default()
                                .reason("post_wake_cas_lost")
                                .target_state("started")
                                .rows_affected("0"),
                        );
                        return Err(ControllerError::internal(
                            "sandbox wake lost the lifecycle CAS — the transient-lifecycle \
                             sweeper or another supervisor flipped the row to a terminal \
                             state (likely 'failed') while the substrate wake was in flight. \
                             The substrate work this request started has been undone by the \
                             sweeper's cleanup; the sandbox needs to be recreated rather than \
                             retried, since wake_execution rejects terminal-lifecycle rows",
                        ));
                    }
                    record_reconnect_attempt(
                        metric_identity,
                        reconnect_initial_lifecycle,
                        ReconnectOutcome::WakeSubmitted,
                        ReconnectTerminalPhase::Ready,
                        ReconnectReason::Ok,
                        reconnect_started_at,
                    );
                    record_dormant_activation_if_needed(sandbox, metric_identity, action);
                    // Run port-exposure reconciliation here (was the
                    // post-result block's job for `Some(Started)`) and
                    // return `(None, body)` so the post-result block doesn't
                    // re-issue an `UPDATE sandboxes SET lifecycle =
                    // 'started'` on the row we just CAS'd.
                    self.reconcile_open_sandbox_port_exposures_best_effort(pool, id)
                        .await;
                    emit_api_sandbox_event(
                        typed_id,
                        action.completed_event(),
                        action,
                        SandboxEventDetails::default()
                            .with_to_state("started")
                            .placement("on_node"),
                    );
                    Ok((None, body))
                }
                SandboxAction::Restore => {
                    let activation = activation_options;
                    // Same cancellation shield as the Wake arm: the
                    // completion CAS below dies with a dropped request
                    // future while the node finishes the restore anyway.
                    let mut completion_guard = wake_completion::WakeCompletionGuard::unarmed(
                        id,
                        pplx_space_metrics::sandbox::api::WakeCompletionAction::Restore,
                    );
                    if sandbox
                        .lifecycle
                        .parse::<SandboxLifecycle>()
                        .is_ok_and(|lc| wake_completion::shield_applies(&self.substrate, lc))
                    {
                        completion_guard.arm(wake_completion::wake_completion_redrive(
                            pool.clone(),
                            self.substrate.clone(),
                            id,
                            pplx_space_metrics::sandbox::api::WakeCompletionAction::Restore,
                            wake_completion::redrive_budget(),
                        ));
                    }
                    let completion = match self
                        .substrate
                        .restore_execution(
                            metric_identity,
                            typed_id,
                            activation,
                            self.restore_claim_charge(
                                sandbox.lifecycle.parse::<SandboxLifecycle>().ok(),
                            )
                            .unwrap_or(crate::controllers::substrate::RecoveryCharge::Exempt),
                        )
                        .await
                    {
                        Ok(completion) => completion,
                        Err(err) => {
                            completion_guard.defuse();
                            pplx_space_metrics::sandbox::api::lifecycle_error(
                                id,
                                metric_identity,
                                "substrate",
                            );
                            emit_api_sandbox_event(
                                typed_id,
                                action.failed_event(),
                                action,
                                SandboxEventDetails::default()
                                    .reason("substrate")
                                    .error(err.to_string()),
                            );
                            return Err(ControllerError::from(err));
                        }
                    };
                    if !completion.should_finalize_lifecycle() {
                        completion_guard.defuse();
                        return Ok(body);
                    }
                    // Refuse to overwrite a terminal, teardown, or newer
                    // suspend lifecycle.
                    // We can't guard on `lifecycle = 'restoring'` alone
                    // because the two substrates enter restore from different
                    // lifecycles: the Kubernetes substrate's
                    // `begin_restore_lifecycle` flips
                    // `suspended -> restoring` before the substrate call, so
                    // the row sits in `restoring` during the slow part; the
                    // direct substrate's `restore_execution` doesn't touch
                    // lifecycle at all, so the row sits in `suspended` for the
                    // duration. The terminal-state exclusion is the only
                    // predicate that catches "sweeper landed" uniformly across
                    // substrates. On race-loss we surface a 5xx so the caller
                    // re-issues rather than silently observing a `started` row
                    // pointing at a deleted execution.
                    let published_suspend_fence =
                        PublishedSuspendFence::for_substrate(&self.substrate);
                    let cas_result =
                        complete_wake_lifecycle(pool, id, published_suspend_fence).await;
                    // Explicit exit either way from here — see the Wake arm.
                    completion_guard.defuse();
                    let completion = cas_result.map_err(|err| {
                        db_contention_error("post-restore lifecycle update failed", &err)
                    })?;
                    if completion == WakeLifecycleCompletion::SupersededBySuspend {
                        return Ok(body);
                    }
                    if completion == WakeLifecycleCompletion::Lost {
                        pplx_space_metrics::sandbox::api::lifecycle_error(
                            id,
                            metric_identity,
                            "post_restore_cas_lost",
                        );
                        emit_api_sandbox_event(
                            typed_id,
                            Some(SandboxEventType::LifecycleTransitionCasLost),
                            action,
                            SandboxEventDetails::default()
                                .reason("post_restore_cas_lost")
                                .target_state("started")
                                .rows_affected("0"),
                        );
                        return Err(ControllerError::internal(
                            "sandbox restore lost the lifecycle CAS — the transient-lifecycle \
                             sweeper or another supervisor flipped the row to a terminal state \
                             (likely 'failed') while the substrate restore was in flight. The \
                             substrate work this request started has been undone by the \
                             sweeper's cleanup; the sandbox needs to be recreated rather than \
                             retried, since restore_execution rejects terminal-lifecycle rows",
                        ));
                    }
                    record_dormant_activation_if_needed(sandbox, metric_identity, action);
                    // Same shape as the Wake arm above: run port-exposure
                    // reconciliation here and return `(None, body)` so the
                    // post-result block doesn't re-issue the `lifecycle =
                    // 'started'` write.
                    self.reconcile_open_sandbox_port_exposures_best_effort(pool, id)
                        .await;
                    emit_api_sandbox_event(
                        typed_id,
                        action.completed_event(),
                        action,
                        SandboxEventDetails::default()
                            .with_to_state("started")
                            .placement("on_node"),
                    );
                    Ok((None, body))
                }
                SandboxAction::Suspend => {
                    match self.substrate.suspend_execution(typed_id).await {
                        Ok(_) => {}
                        Err(err) => {
                            pplx_space_metrics::sandbox::api::lifecycle_error(
                                id,
                                metric_identity,
                                "substrate",
                            );
                            emit_api_sandbox_event(
                                typed_id,
                                action.failed_event(),
                                action,
                                SandboxEventDetails::default()
                                    .reason("substrate")
                                    .error(err.to_string()),
                            );
                            return Err(ControllerError::from(err));
                        }
                    }
                    Ok((Some(SandboxLifecycleStatus::Suspended), body))
                }
                // Snapshot-policy actions are DB-only — handle them before
                // acquiring a runtime client. On k8s, `runtime_client` would
                // `ensure_pod_exists` for a recoverable-live sandbox whose
                // pod IP is gone and re-pin the pod to `target_node_id` on
                // the draining node, racing the reconciler for no work the
                // snapshot-policy update actually needs.
                SandboxAction::EnableSnapshots | SandboxAction::DisableSnapshots => {
                    let desired = matches!(action, SandboxAction::EnableSnapshots);
                    let body = self
                        .set_snapshots_enabled_desired(pool, sandbox, desired)
                        .await?;
                    Ok((None, body))
                }
                // `stop` / `kill` need to behave differently depending on
                // whether the resolved node is draining:
                //
                // * **Healthy node** (no drain phase): use the regular
                //   recovering `runtime_client`. On k8s this is the only
                //   path that recovers a Pending / ContainerCreating pod
                //   so the cleanup RPC can actually run; otherwise an
                //   ordinary "stop a sandbox whose pod was evicted"
                //   request would 503 and leave the sandbox stuck
                //   `started` until an external reconciler intervenes.
                // * **Draining node**: use the non-recovering
                //   `reachable_runtime_client` so the cleanup path can't
                //   itself trigger `ensure_pod_exists` on the cordoned
                //   source node (racing the drain reconciler). When the
                //   runtime is unreachable we return 503 instead of
                //   optimistically marking the sandbox stopped — see the
                //   earlier `sandbox_ip` disambiguation attempt that
                //   conflated "pod gone" with "pod Pending / no IP yet"
                //   and falsely reported success on a still-running VM.
                //
                // This branch keys on the drain *phase* only, not the mode. A
                // gentle drain still cordons the node, so `ensure_pod_exists`
                // there would create a pod that can never be scheduled — the
                // non-recovering client is right in both modes. Unlike the
                // 409 gates, refusing to recover a pod here does not deny the
                // user their sandbox; it returns 503 for a cleanup whose pod
                // is already gone.
                SandboxAction::Stop | SandboxAction::Kill => {
                    let drain_state =
                        fetch_active_node_drain_state(pool, id)
                            .await
                            .map_err(|err| {
                                db_contention_error("failed to load node drain state", &err)
                            })?;
                    if drain_state.is_some() {
                        let maybe_client =
                            self.substrate.reachable_runtime_client(typed_id).await?;
                        let mut client = match maybe_client {
                            Some(client) => client,
                            None => {
                                return Err(ControllerError::service_unavailable(format!(
                                    "sandbox runtime is currently unreachable for {action}; \
                                     retry once the worker recovers or the drain reconciler \
                                     migrates the sandbox"
                                )));
                            }
                        };
                        run_stop_or_kill(&mut client, action, typed_id).await?;
                        Ok((Some(SandboxLifecycleStatus::Stopped), body))
                    } else {
                        let mut client = self
                            .substrate
                            .runtime_client(
                                typed_id,
                                crate::controllers::substrate::RecoveryCharge::Exempt,
                            )
                            .await?;
                        run_stop_or_kill(&mut client, action, typed_id).await?;
                        Ok((Some(SandboxLifecycleStatus::Stopped), body))
                    }
                }
                // `pause` and `checkpoint` preserve in-place state — they only
                // make sense against a live pod, so they use the non-recovering
                // `reachable_runtime_client`: a missing pod surfaces as 503
                // rather than pointlessly recreating a pod just to pause/snapshot
                // it (which would also be an uncharged activation and race the
                // drain reconciler). `resume` can legitimately recreate a lost
                // pod (restore machinery), which is a fresh activation, so it
                // charges recovery admission.
                SandboxAction::Pause | SandboxAction::Checkpoint | SandboxAction::Resume => {
                    if action.pause_or_checkpoint() {
                        ensure_resolved_node_not_draining(pool, id, action).await?;
                    }
                    match action {
                        SandboxAction::Pause => {
                            let mut client = self
                                .reachable_runtime_client_or_unavailable(typed_id)
                                .await?;
                            client
                                .pause_sandbox(PauseRequest {
                                    sandbox_id: id_str.clone(),
                                })
                                .await
                                .map(|_| (Some(SandboxLifecycleStatus::Paused), body))
                                .map_err(ControllerError::from)
                        }
                        SandboxAction::Resume => {
                            let activation = activation_options;
                            let mut client = self
                                .substrate
                                .runtime_client(
                                    typed_id,
                                    crate::controllers::substrate::RecoveryCharge::enforce(
                                        self.cluster_admission.as_ref(),
                                    ),
                                )
                                .await?;
                            let request = self
                                .resume_request_with_current_policy(
                                    pool,
                                    typed_id,
                                    &mut client,
                                    activation,
                                )
                                .await?;
                            client
                                .resume_sandbox(request)
                                .await
                                .map(|_| (Some(SandboxLifecycleStatus::Started), body))
                                .map_err(ControllerError::from)
                        }
                        SandboxAction::Checkpoint => {
                            let mut client = self
                                .reachable_runtime_client_or_unavailable(typed_id)
                                .await?;
                            client
                                .create_sandbox_snapshot(CreateSnapshotRequest {
                                    sandbox_id: id_str.clone(),
                                    r#type: SnapshotType::Full.into(),
                                    label: None,
                                    creation_identity: None,
                                    source: None,
                                })
                                .await
                                .map(|_| (None, body))
                                .map_err(ControllerError::from)
                        }
                        _ => unreachable!("outer match guards Pause | Checkpoint | Resume"),
                    }
                }
                SandboxAction::Access
                | SandboxAction::Stats
                | SandboxAction::OpenPort
                | SandboxAction::Rollback
                | SandboxAction::Spaced
                | SandboxAction::Update => {
                    return Err(ControllerError::bad_request(format!(
                        "Unknown action: {action}"
                    )));
                }
            };

        let (status, body) = match result {
            Ok(result) => result,
            Err(err) => {
                emit_api_sandbox_event(
                    typed_id,
                    action.failed_event(),
                    action,
                    SandboxEventDetails::default()
                        .reason("controller")
                        .error(err.to_string()),
                );
                return Err(err);
            }
        };
        if let Some(status) = status {
            maybe_update_sandbox_lifecycle(pool, id, status.as_str()).await;
            if status == SandboxLifecycleStatus::Started {
                record_dormant_activation_if_needed(sandbox, metric_identity, action);
                self.reconcile_open_sandbox_port_exposures_best_effort(pool, id)
                    .await;
            }
            emit_api_sandbox_event(
                typed_id,
                action.completed_event(),
                action,
                SandboxEventDetails::default().with_to_state(status.as_str()),
            );
        }
        Ok(body)
    }

    async fn untracked_action(
        &self,
        id: SandboxId,
        metric_identity: SandboxApiMetricIdentity,
        action: SandboxAction,
        activation: Option<SandboxActivationRequest>,
    ) -> Result<serde_json::Value, ControllerError> {
        let typed_id = id;
        let id_str = id.to_string();

        let status = match action {
            SandboxAction::Start => {
                let activation = resolve_activation_options(activation.as_ref(), true)?;
                self.admit_action_cluster_activation(action, None).await?;
                self.substrate
                    .start_execution(
                        metric_identity,
                        typed_id,
                        activation,
                        crate::controllers::substrate::RecoveryCharge::Exempt,
                    )
                    .await
                    .map_err(ControllerError::from)?;
                SandboxLifecycleStatus::Started
            }
            SandboxAction::Wake | SandboxAction::ImplicitWake(_) => {
                let mut client = self
                    .substrate
                    .runtime_client(
                        typed_id,
                        crate::controllers::substrate::RecoveryCharge::Exempt,
                    )
                    .await?;
                client
                    .wake_sandbox(WakeRequest {
                        implicit_wake_source: action
                            .wake_activity()
                            .map_or(0, ExecutionActivityKind::wake_source),
                        sandbox_id: id_str.clone(),
                        blocking: true,
                        start_context: None,
                        target_state: 0,
                    })
                    .await?;
                SandboxLifecycleStatus::Started
            }
            SandboxAction::Restore => {
                let activation = resolve_activation_options(activation.as_ref(), true)?;
                self.admit_action_cluster_activation(action, None).await?;
                self.substrate
                    .restore_execution(
                        metric_identity,
                        typed_id,
                        activation,
                        crate::controllers::substrate::RecoveryCharge::Exempt,
                    )
                    .await
                    .map_err(ControllerError::from)?;
                SandboxLifecycleStatus::Started
            }
            SandboxAction::Suspend => {
                self.substrate
                    .suspend_execution(typed_id)
                    .await
                    .map_err(ControllerError::from)?;
                SandboxLifecycleStatus::Suspended
            }
            SandboxAction::Stop => {
                let mut client = self
                    .substrate
                    .runtime_client(
                        typed_id,
                        crate::controllers::substrate::RecoveryCharge::Exempt,
                    )
                    .await?;
                client
                    .stop_sandbox(StopRequest {
                        sandbox_id: id_str.clone(),
                        template_filter: None,
                        blocking: true,
                        immediate_kill: false,
                    })
                    .await?;
                SandboxLifecycleStatus::Stopped
            }
            SandboxAction::Pause => {
                let mut client = self
                    .substrate
                    .runtime_client(
                        typed_id,
                        crate::controllers::substrate::RecoveryCharge::Exempt,
                    )
                    .await?;
                client
                    .pause_sandbox(PauseRequest {
                        sandbox_id: id_str.clone(),
                    })
                    .await?;
                SandboxLifecycleStatus::Paused
            }
            SandboxAction::Resume => {
                let mut client = self
                    .substrate
                    .runtime_client(
                        typed_id,
                        crate::controllers::substrate::RecoveryCharge::Exempt,
                    )
                    .await?;
                let activation = resolve_activation_options(activation.as_ref(), true)?;
                client
                    .resume_sandbox(ResumeRequest {
                        activation_auto_lifecycle: None,
                        expected_execution_generation: None,
                        sandbox_id: id_str.clone(),
                        blocking: activation.blocking,
                        ready_timeout_secs: Some(activation.ready_timeout.as_secs() as u32),
                    })
                    .await?;
                SandboxLifecycleStatus::Started
            }
            SandboxAction::Kill => {
                let mut client = self
                    .substrate
                    .runtime_client(
                        typed_id,
                        crate::controllers::substrate::RecoveryCharge::Exempt,
                    )
                    .await?;
                client
                    .kill_sandbox(KillRequest {
                        sandbox_id: id_str.clone(),
                        signal: 9,
                        blocking: true,
                    })
                    .await?;
                SandboxLifecycleStatus::Stopped
            }
            SandboxAction::Checkpoint => {
                let mut client = self
                    .substrate
                    .runtime_client(
                        typed_id,
                        crate::controllers::substrate::RecoveryCharge::Exempt,
                    )
                    .await?;
                client
                    .create_sandbox_snapshot(CreateSnapshotRequest {
                        sandbox_id: id_str.clone(),
                        r#type: SnapshotType::Full.into(),
                        label: None,
                        creation_identity: None,
                        source: None,
                    })
                    .await?;
                SandboxLifecycleStatus::Unknown
            }
            SandboxAction::EnableSnapshots
            | SandboxAction::DisableSnapshots
            | SandboxAction::Access
            | SandboxAction::Stats
            | SandboxAction::OpenPort
            | SandboxAction::Rollback
            | SandboxAction::Spaced
            | SandboxAction::Update => {
                return Err(ControllerError::bad_request(format!(
                    "Unknown action: {action}"
                )));
            }
        };

        Ok(serde_json::json!({
            "ok": true,
            "action": action.as_str(),
            "sandbox_id": id_str,
            "status": status.as_str(),
        }))
    }

    pub async fn stats(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        id: SandboxId,
    ) -> Result<pplx_space_sbx_mgr_client::proto::StatsResponse, ControllerError> {
        // `runtime_client` on the k8s substrate recovers a missing pod for
        // recoverable-live lifecycles, which would re-pin the pod to
        // `target_node_id` on a draining node. Gate before the call so a
        // stats request can't sneak past the drain guard.
        ensure_access_path_not_blocked_by_drain(pool, sandbox, SandboxAction::Stats).await?;
        // Reading stats must never recreate a pod — use the non-recovering client.
        let mut client = self.reachable_runtime_client_or_unavailable(id).await?;
        let stats = client
            .sandbox_stats(StatsRequest {
                sandbox_id: id.to_string(),
            })
            .await?
            .into_inner();

        let _ = sandbox;

        Ok(stats)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod drain_guard_tests;

// Postgres-backed tests for the DB helpers. These pin down the contract every
// caller of `ensure_resolved_node_not_draining` / `fetch_active_node_drain_phase`
// relies on: a drain-phase node referenced via *either* `active_node_id` or
// `target_node_id` must surface a 409 / `Some(phase)`. Plain `cargo test` skips
// these require Docker because the shared fixture starts a Postgres testcontainer.
#[cfg(test)]
mod drain_guard_postgres_tests;

#[cfg(test)]
mod wake_completion_postgres_tests;

// Coverage-gap acknowledgment: `SandboxController::control_runtime_client`'s
// ordering invariant (drain check must run before any substrate call that can
// implicit-recover a pod) is not unit-testable here because the `Substrate`
// enum dispatches to concrete `direct::DirectSubstrate` / kubernetes impls and
// isn't injectable from outside the controllers module. The fix is verifiable
// by reading the function body — the gate is the first statement. Building a
// substrate mock so this can be asserted is tracked separately.
