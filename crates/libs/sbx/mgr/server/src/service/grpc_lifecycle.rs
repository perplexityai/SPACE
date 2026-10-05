use pplx_space_common_core::types::{JobId, LocalArtifactId};

use super::activation_support::{
    GuestActivityKind, IdentityFreeDirectActivationAuthority, WakeNetworkContext, WakeStartConfig,
    grpc_timeout_budget, operation_activation_attempt_id_from_wire, sandbox_event_trigger,
    space_api_route_identity_for_runtime_activation,
    space_api_route_identity_from_wake_start_context, space_api_route_identity_to_wire,
    update_state_best_effort, wake_target_state_from_proto,
};
use super::cleanup::{
    AsyncCleanupRequest, CleanupAdmission, CleanupDispatchDisposition, SynchronousCleanupRequest,
};
use super::cleanup_support::{
    CleanupCompletion, CleanupLockTryResult, OwnedCleanupFenceGuard, require_state_write,
};
use super::create_support::SUSPEND_CHECKPOINT;
use super::network_runtime::*;
use super::recovery::SuspendSnapshotPublicationRecoveryOrigin;
use super::suspend_snapshot_publication::{
    SuspendSnapshotPublicationDecision, canonical_suspend_snapshot_dir,
    classify_suspend_snapshot_publication, publish_suspend_snapshot_dir,
    suspend_snapshot_staging_dir, suspend_snapshot_staging_parent_dir,
};
use super::tail_support::*;
use super::validation::optional_positive_or_default;
use super::*;
use crate::guest_filesystem_flush::{
    GuestSnapshotPreparationBudget, prepare_guest_for_vm_snapshot_via_spaced_host,
};
use pplx_space_common_core::types::{ArtifactKind, ArtifactOrigin, SandboxId};
use pplx_space_events::{
    SandboxEventBoolField, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger,
    SandboxEventType, emit_sandbox_event,
};

const PAUSE_RECONCILIATION_ATTEMPTS: usize = 3;
const PAUSE_RECONCILIATION_BACKOFF: Duration = Duration::from_millis(100);

pub(super) const SUSPEND_TRIGGER_METADATA: &str = "x-space-suspend-trigger";

pub(super) fn encrypted_snapshot_prepare_error_status(
    error: crate::snapshot_encryption::EncryptedSnapshotPrepareError,
) -> Status {
    let reason = format!("prepare encrypted snapshot volume failed: {error}");
    error.into_status(reason)
}

pub(super) fn teardown_backup_failure_requires_new_attempt(
    failure: Option<pplx_space_common_jobs::JobFailure>,
) -> bool {
    matches!(failure, Some(pplx_space_common_jobs::JobFailure::Retriable))
}

fn teardown_suspend_event_details(
    execution_generation: pplx_space_common_core::types::ExecutionGeneration,
    trigger: SandboxEventTrigger,
) -> SandboxEventDetails {
    SandboxEventDetails::default()
        .operation("generation_suspend")
        .trigger(trigger)
        .execution_generation(execution_generation.to_string())
}

pub(super) fn teardown_suspend_event_trigger(
    request: &Request<PrepareTeardownSuspendRequest>,
) -> Result<SandboxEventTrigger, Status> {
    match request
        .metadata()
        .get(SUSPEND_TRIGGER_METADATA)
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| Status::invalid_argument("invalid suspend trigger metadata"))?
    {
        None | Some("system") => Ok(SandboxEventTrigger::System),
        Some("user") => Ok(SandboxEventTrigger::User),
        Some("auto_lifecycle") => Ok(SandboxEventTrigger::AutoLifecycle),
        Some(_) => Err(Status::invalid_argument("unknown suspend trigger metadata")),
    }
}

fn teardown_suspend_prepare_response(
    outcome: TeardownSuspendOutcome,
    reason: Option<String>,
    metric_outcome: pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome,
    started_at: std::time::Instant,
) -> Response<PrepareTeardownSuspendResponse> {
    pplx_space_metrics::sandbox::worker::teardown_suspend_prepare(metric_outcome, started_at);
    let span = tracing::Span::current();
    span.record("grpc.response.outcome", outcome.as_str_name());
    if let Some(reason) = reason.as_deref() {
        span.record("grpc.response.reason", reason);
    }
    Response::new(PrepareTeardownSuspendResponse {
        outcome: outcome as i32,
        reason,
        published_artifact: None,
    })
}

fn durable_teardown_suspend_prepare_response(
    status: &crate::db::TeardownSuspendStatus,
    started_at: std::time::Instant,
) -> Result<Response<PrepareTeardownSuspendResponse>, Status> {
    let artifact = status.artifact.as_ref().ok_or_else(|| {
        Status::internal("durable teardown suspend is missing its published artifact")
    })?;
    let artifact_kind = match artifact.artifact_kind {
        pplx_space_common_core::types::ArtifactKind::Disk => crate::proto::ArtifactKind::Disk,
        pplx_space_common_core::types::ArtifactKind::Full => crate::proto::ArtifactKind::Full,
    };
    let mut response = teardown_suspend_prepare_response(
        TeardownSuspendOutcome::Durable,
        None,
        pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Durable,
        started_at,
    )
    .into_inner();
    response.published_artifact = Some(crate::proto::PublishedArtifact {
        local_artifact_id: artifact.local_artifact_id.to_string(),
        artifact_kind: artifact_kind as i32,
        origin: crate::proto::ArtifactOrigin::Suspend as i32,
        snapshot_name: None,
    });
    Ok(Response::new(response))
}

fn completed_suspend_retry_response(
    sandbox: &crate::db::SandboxRow,
    blocking: bool,
    requested_format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
) -> Result<Option<SuspendResponse>, Status> {
    if sandbox.state != SandboxState::Suspended
        || sandbox.current_local_artifact_kind != Some(ArtifactKind::Full)
        || sandbox.current_local_artifact_origin != Some(ArtifactOrigin::Suspend)
    {
        return Ok(None);
    }
    let Some(local_artifact_id) = sandbox.current_local_artifact_id else {
        return Ok(None);
    };
    if requested_format != pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Unspecified {
        let sandbox_dir = std::path::PathBuf::from(format!("{CACHE_DIR}/{}", sandbox.id));
        let snapshot_dir = canonical_suspend_snapshot_dir(&sandbox_dir);
        let recorded_format = reupload_format_from_sidecar(&snapshot_dir.to_string_lossy())?;
        ensure_completed_suspend_retry_format(requested_format, recorded_format)?;
    }
    Ok(Some(SuspendResponse {
        published_artifact: blocking.then(|| crate::proto::PublishedArtifact {
            local_artifact_id: local_artifact_id.to_string(),
            artifact_kind: crate::proto::ArtifactKind::Full as i32,
            origin: crate::proto::ArtifactOrigin::Suspend as i32,
            snapshot_name: None,
        }),
    }))
}

fn ensure_completed_suspend_retry_format(
    requested_format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
    recorded_format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
) -> Result<(), Status> {
    if requested_format != pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Unspecified
        && requested_format != recorded_format
    {
        return Err(Status::failed_precondition(format!(
            "cannot reuse completed suspend artifact encoded as {recorded_format:?}: request \
             explicitly requires {requested_format:?}",
        )));
    }
    Ok(())
}

fn touch_auto_lifecycle_source(value: i32) -> Result<AutoLifecycleActivitySource, Status> {
    match crate::proto::AutoLifecycleActivitySource::try_from(value)
        .unwrap_or(crate::proto::AutoLifecycleActivitySource::Unspecified)
    {
        crate::proto::AutoLifecycleActivitySource::Access => {
            Ok(AutoLifecycleActivitySource::Access)
        }
        crate::proto::AutoLifecycleActivitySource::Command => {
            Ok(AutoLifecycleActivitySource::Command)
        }
        crate::proto::AutoLifecycleActivitySource::Unspecified => {
            Err(Status::invalid_argument("source is required"))
        }
    }
}

fn touch_auto_lifecycle_observed_at_secs(
    observed_at: Option<&prost_types::Timestamp>,
) -> Result<u64, Status> {
    let Some(observed_at) = observed_at else {
        return Ok(now_secs());
    };
    if observed_at.seconds < 0 || observed_at.nanos < 0 || observed_at.nanos >= 1_000_000_000 {
        return Err(Status::invalid_argument(
            "observed_at must be a valid non-negative timestamp",
        ));
    }
    Ok(observed_at.seconds.cast_unsigned())
}

fn touch_auto_lifecycle_result_value(result: crate::proto::AutoLifecycleTouchResult) -> i32 {
    result as i32
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PauseReconciliationOutcome {
    Paused,
    Running,
    Unknown(String),
}

fn pause_reconciliation_outcome(
    observation: &crate::runtime_reconciliation::FirecrackerVmStateObservation,
) -> PauseReconciliationOutcome {
    match observation.state() {
        Some(FirecrackerVmState::Paused) => PauseReconciliationOutcome::Paused,
        Some(FirecrackerVmState::Running) => PauseReconciliationOutcome::Running,
        Some(FirecrackerVmState::NotStarted) => {
            PauseReconciliationOutcome::Unknown("Firecracker reported NotStarted".to_string())
        }
        None => PauseReconciliationOutcome::Unknown(
            observation
                .unobservable_reason()
                .unwrap_or("Firecracker state was unavailable")
                .to_string(),
        ),
    }
}

async fn reconcile_state_after_unconfirmed_pause(
    sandbox_id: &SandboxId,
    reason: &str,
) -> PauseReconciliationOutcome {
    let observation =
        observe_firecracker_vm_state(sandbox_id, "pause response reconciliation").await;
    let outcome = pause_reconciliation_outcome(&observation);
    tracing::warn!(
        sandbox_id = %sandbox_id,
        reason,
        reconciled_outcome = ?outcome,
        vm_state = %observation
            .state()
            .map_or_else(|| "unknown".to_string(), |state| state.to_string()),
        "reconciled sandbox state after unconfirmed pause"
    );
    outcome
}

async fn pause_with_reconciliation(
    node_client: &mut NodeManagerGrpcClient,
    execution_fence: Option<pplx_space_node_mgr_client::proto::ExecutionFence>,
    db: &Arc<Mutex<rusqlite::Connection>>,
    sandbox_id: &SandboxId,
    reason_context: &str,
    guest_preparation_budget: GuestSnapshotPreparationBudget,
) -> SandboxState {
    let sandbox_id_string = sandbox_id.to_string();
    let mut last_reason = None;
    for attempt in 0..PAUSE_RECONCILIATION_ATTEMPTS {
        match node_client
            .pause_sandbox(PauseSandboxRequest {
                execution_fence: execution_fence.clone(),
                sandbox_id: sandbox_id_string.clone(),
            })
            .await
        {
            Ok(_) => return SandboxState::Paused,
            Err(error) => {
                let reason = format!("{reason_context}: {error}");
                last_reason = Some(reason.clone());
                let outcome = reconcile_state_after_unconfirmed_pause(sandbox_id, &reason).await;
                let retry_requires_prepare =
                    matches!(&outcome, PauseReconciliationOutcome::Running);
                match outcome {
                    PauseReconciliationOutcome::Paused => return SandboxState::Paused,
                    PauseReconciliationOutcome::Running
                        if attempt + 1 == PAUSE_RECONCILIATION_ATTEMPTS =>
                    {
                        update_state_best_effort(db, &sandbox_id_string, SandboxState::Started);
                        return SandboxState::Started;
                    }
                    PauseReconciliationOutcome::Running => {}
                    PauseReconciliationOutcome::Unknown(detail) => {
                        last_reason = Some(format!("{reason}; {detail}"));
                    }
                }
                if attempt + 1 < PAUSE_RECONCILIATION_ATTEMPTS
                    && retry_requires_prepare
                    && let Err(error) = prepare_guest_for_vm_snapshot_via_spaced_host(
                        &sandbox_id_string,
                        guest_preparation_budget,
                    )
                    .await
                {
                    update_state_best_effort(db, &sandbox_id_string, SandboxState::Started);
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        %error,
                        "pause retry guest preparation failed"
                    );
                    return SandboxState::Started;
                }
            }
        }
        if attempt + 1 < PAUSE_RECONCILIATION_ATTEMPTS {
            tokio::time::sleep(PAUSE_RECONCILIATION_BACKOFF).await;
        }
    }

    let reason = last_reason.unwrap_or_else(|| reason_context.to_string());
    mark_quarantined_best_effort(
        db,
        &sandbox_id_string,
        SandboxState::Pausing,
        SandboxQuarantineReasonCode::PauseFailed,
        &reason,
    );
    SandboxState::Quarantined
}

/// Outcome of [`SandboxManagerService::submit_artifact_backup`].
/// `Enqueued` carries the durable-worker job id and the backup attempt id
/// owned by that job. The attempt may differ from the caller's candidate when
/// a deduplicated submission joins an existing job. `NoUploadConfig` is the
/// silent fall-through when neither the request override nor the env-derived
/// config is present (e.g. local dev without `SPACE_SNAPSHOT_BUCKET`).
#[derive(Debug)]
pub(super) enum SubmitSuspendSnapshotOutcome {
    Enqueued {
        job_id: JobId,
        backup_attempt_id: String,
    },
    NoUploadConfig,
}

/// Outcome of resolving a sandbox's org-level snapshot upload override
/// from the cluster-api. Callers distinguish "definitively no override"
/// (env-default applies) from "lookup failed transiently" (retryable),
/// because backup admission needs that distinction to avoid surfacing
/// a transient cluster-api blip as a terminal `FailedPrecondition` that
/// the drain reconciler would treat as mark-as-failed.
pub(super) enum OrgSnapshotConfigResolution {
    /// Override is present and parses to a valid upload config.
    Resolved(crate::snapshot_upload::SnapshotUploadConfig),
    /// Definitively no override: cluster-api isn't wired (local dev),
    /// the org left it unset, or the returned URI is empty. Callers
    /// fall back to the env-derived default with no logging.
    Absent,
    /// Lookup failed transiently or due to operator-fixable config
    /// (cluster-api unreachable, RPC errored, bearer/endpoint invalid,
    /// override URI didn't parse). Suspend collapses this to silent
    /// fall-through; reupload surfaces it as `Internal` so the
    /// reconciler retries instead of escalating to mark-as-failed.
    LookupFailed(String),
    /// `GetSandboxSnapshotStorageUri` returned `NotFound` — the
    /// sandbox has no row in the cluster yet. For suspend this is a
    /// benign create race (local `SandboxRow` exists before the cluster
    /// row is committed); for reupload it indicates state divergence,
    /// because the sandbox must already have been suspended at least
    /// once (and therefore registered with the cluster) before reupload
    /// can be invoked. Suspend collapses this to silent fall-through;
    /// reupload surfaces it as `FailedPrecondition` so the reconciler
    /// stops issuing reupload work that the durable worker would
    /// permanently fail at the `BeginArtifactBackup` step.
    ClusterRowMissing,
}

/// Precondition for durable artifact backup: the node must
/// have both cluster-api env vars wired so the durable worker can
/// register the uploaded snapshot. If wiring is missing, the RPC fails
/// loudly instead of returning a job id for an enqueue that the
/// downstream `register_with_cluster_api` step would mark
/// `JobError::permanent` (see
/// `crates/libs/sbx/mgr/server/src/snapshot_upload_jobs.rs`).
///
/// The status carries neither `REUPLOAD_ARTIFACT_MISSING_MARKER` nor
/// `REUPLOAD_SANDBOX_UNKNOWN_MARKER`, so the future cluster-controller
/// classifier will treat this as retryable rather than terminal — the
/// right behavior, since an operator wiring SPACE_INTERNAL_TOKEN fixes
/// the deployment without losing the sandbox.
///
/// Booleans (rather than `&str` values) keep this testable without
/// touching the process environment. Callers must apply the same
/// "non-empty" predicate the node-api client's `required_env` helper
/// uses — an env var present but set to the empty string is treated as
/// missing throughout the codebase.
pub(super) fn reupload_precondition_for_cluster_api_env(
    endpoint_set: bool,
    token_set: bool,
) -> Option<Status> {
    if endpoint_set && token_set {
        return None;
    }
    let missing: Vec<&str> = [
        (!endpoint_set).then_some("SPACE_INTERNAL_API_ENDPOINT"),
        (!token_set).then_some("SPACE_INTERNAL_TOKEN"),
    ]
    .into_iter()
    .flatten()
    .collect();
    Some(Status::failed_precondition(format!(
        "cannot reupload snapshot: this node is missing cluster-api wiring ({}); \
         even on a successful S3 upload, the durable worker cannot register the \
         snapshot location so the reconciler would never see a ready row land",
        missing.join(", "),
    )))
}

/// `SPACE_SUSPEND_DEFAULT_FORMAT=full` is the rollback lever that returns the
/// suspend-codec default from `BtrfsSend` to `Full` for callers that don't
/// supply an explicit `format`. Any other value keeps `BtrfsSend`. Resolved
/// per-request so ops can flip it without restarting workers.
fn env_default_is_full() -> bool {
    matches!(
        std::env::var("SPACE_SUSPEND_DEFAULT_FORMAT").as_deref(),
        Ok("full"),
    )
}

/// How soon after a completed wake an incoming suspend is flagged as a
/// possible wake/suspend oscillation.
const SUSPEND_AFTER_WAKE_WARN_THRESHOLD: std::time::Duration =
    std::time::Duration::from_millis(5_000);

/// Suspend-after-wake observation derived from the coordinator's
/// wake-completion record. `ms_since_wake` always lands on the
/// `suspend_requested` event when a prior wake is known;
/// `within_warn_threshold` additionally drives a `tracing::warn!` so the
/// oscillation signature is searchable without event-stream tooling.
pub(super) struct SuspendAfterWake {
    pub(super) ms_since_wake: u64,
    pub(super) within_warn_threshold: bool,
}

pub(super) fn classify_suspend_after_wake(
    since_wake: Option<std::time::Duration>,
    warn_threshold: std::time::Duration,
) -> Option<SuspendAfterWake> {
    let since_wake = since_wake?;
    Some(SuspendAfterWake {
        // Saturate rather than zero on overflow: a delta that large is by
        // definition not "shortly after a wake".
        ms_since_wake: u64::try_from(since_wake.as_millis()).unwrap_or(u64::MAX),
        within_warn_threshold: since_wake <= warn_threshold,
    })
}

const ENV_SUSPEND_WAKE_ARBITRATION: &str = "SPACE_SUSPEND_WAKE_ARBITRATION";

/// Whether a suspend that queued behind a concurrent wake is rejected as
/// superseded (the default) or allowed through. Resolved per-request so ops
/// can disable arbitration via a deployment env change without a daemonset
/// rollout. Default `true`: this is a kill switch, so unset/unrecognized
/// values fail toward the fixed behavior; only an explicit falsy value
/// disables it.
pub(super) fn suspend_wake_arbitration_enabled() -> bool {
    pplx_space_common_core::env::env_flag_var(ENV_SUSPEND_WAKE_ARBITRATION, true)
}

/// A suspend is superseded when its request arrived at this process before
/// the sandbox's most recent wake completed: the suspend queued behind the
/// wake on the wake lock, so the state it was issued against no longer
/// exists, and executing it would tear down a sandbox some caller just paid
/// a restore to bring up.
pub(super) fn suspend_superseded_by_wake(
    arrived_at: std::time::Instant,
    last_wake_completed_at: Option<std::time::Instant>,
) -> bool {
    last_wake_completed_at.is_some_and(|wake_completed_at| wake_completed_at > arrived_at)
}

fn emit_suspend_failed_event(
    sandbox_id: &str,
    base_details: &SandboxEventDetails,
    operation: &'static str,
    reason: &'static str,
    error: impl Into<String>,
) {
    emit_sandbox_event(
        sandbox_id,
        SandboxEventType::SuspendFailed,
        SandboxEventSource::SbxManager,
        base_details
            .clone()
            .operation(operation)
            .reason(reason)
            .error(error),
    );
}

/// "Set" means the env var is present *and* non-empty — matching the
/// node-api client's `required_env` helper, which `connect_from_env()`
/// uses to decide between `Ok(client)` and `MissingEndpoint`/
/// `MissingToken`. An env var present-but-empty (e.g. an unfilled Helm
/// template that rendered to `""`) must be classified as missing so
/// this precondition matches the downstream cluster-api connect
/// behavior; otherwise the RPC would happily enqueue a job that the
/// durable worker's `register_with_cluster_api` then marks
/// `JobError::permanent`.
fn cluster_api_env_var_set(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| !v.is_empty())
}

/// Sniff the local `snapshot.json` sidecar to recover the rootfs codec
/// the original suspend used. Reupload has no caller-supplied format
/// (the RPC predates any per-request opt-in), so we read it off the
/// snapshot we're re-uploading.
///
/// Three outcomes, only one of which is `Full`:
///
/// - **Sidecar missing** (`Ok(sniffed)`). Resolved from the rootfs
///   artifact actually on disk via
///   [`sidecarless_format_from_artifacts`]: pre-codec snapshots didn't
///   write a sidecar, but neither did the first btrfs-send suspends,
///   so "no sidecar" alone does not imply the historical full-rootfs
///   shape.
/// - **Sidecar present and parses** (`Ok(format)`). Use the codec the
///   original suspend recorded.
/// - **Sidecar present but unparseable** (`Err(Status::internal)`).
///   This is corruption (a truncated `std::fs::write` from a crash
///   mid-write is the realistic source) and **must not** silently
///   degrade to `Full`. Reuploading a btrfs-send snapshot under the
///   `Full` artifact set probes for `rootfs.ext4`, which the
///   `encode_rootfs_as_btrfs_send` step deleted from the snapshot
///   dir; the precondition loop would then return
///   `REUPLOAD_ARTIFACT_MISSING_MARKER` and the cluster controller's
///   reconciler treats that marker as terminal — marking the sandbox
///   `Failed`. Surfacing the parse error as `Status::internal`
///   instead keeps the reconciler in the retry path until either the
///   sidecar is rewritten by another suspend or operator action
///   intervenes.
///
/// Wire-format string compared here is the same constant the suspend
/// handler writes; don't fork the literal in either direction.
fn reupload_format_from_sidecar(
    snapshot_dir: &str,
) -> Result<pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat, Status> {
    let path = format!("{snapshot_dir}/snapshot.json");
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return sidecarless_format_from_artifacts(snapshot_dir);
        }
        Err(err) => {
            return Err(Status::internal(format!(
                "reupload: failed to read snapshot.json at {path}: {err}",
            )));
        }
    };
    let meta: crate::snapshot_upload::SnapshotMeta =
        serde_json::from_slice(&bytes).map_err(|err| {
            Status::internal(format!(
                "reupload: snapshot.json present but unparseable at {path}: {err}",
            ))
        })?;
    Ok(match meta.rootfs_diff_format.as_deref() {
        Some(crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1) => {
            pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::BtrfsSend
        }
        _ => pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Full,
    })
}

/// Resolve the rootfs codec of a sidecar-less snapshot from the rootfs
/// artifact on disk. The two codecs' artifacts are mutually exclusive
/// in a snapshot dir (`encode_rootfs_as_btrfs_send` deletes
/// `rootfs.ext4` when it writes `rootfs.btrfs-send`), so presence is an
/// unambiguous witness:
///
/// - `rootfs.ext4` present → `Full`. The historical shape wins even in
///   the (never-produced) both-present case: the `Full` artifact set is
///   satisfiable, so reupload proceeds.
/// - only `rootfs.btrfs-send` present → `BtrfsSend`. These are the
///   early btrfs-send suspends that predate local sidecar writing;
///   defaulting them to `Full` would probe `rootfs.ext4` and turn a
///   complete snapshot into the terminal
///   `REUPLOAD_ARTIFACT_MISSING_MARKER` (mark-as-failed to the drain
///   promoter, a forever-skip to the eviction legacy-repair lane).
/// - neither present → `Full`, the pre-codec fallback; the caller's
///   artifact-presence check then reports the genuinely-missing rootfs.
/// - a stat error → `Err(Status::internal)` (retryable), same rationale
///   as the unparseable-sidecar branch: transient IO must not pick a
///   codec whose artifact probe escalates to the terminal marker.
fn sidecarless_format_from_artifacts(
    snapshot_dir: &str,
) -> Result<pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat, Status> {
    let probe = |file_name: &str| -> Result<bool, Status> {
        let path = format!("{snapshot_dir}/{file_name}");
        std::path::Path::new(&path).try_exists().map_err(|err| {
            Status::internal(format!(
                "reupload: failed to stat rootfs artifact at {path}: {err}",
            ))
        })
    };
    if probe(crate::snapshot_upload::ROOTFS_FULL_FILENAME)? {
        return Ok(pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Full);
    }
    if probe(crate::snapshot_upload::ROOTFS_BTRFS_SEND_FILENAME)? {
        return Ok(pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::BtrfsSend);
    }
    Ok(pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Full)
}

fn artifact_backup_format(
    artifact: &crate::db::LocalArtifactRow,
) -> Result<pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat, Status> {
    match artifact.artifact_kind {
        pplx_space_common_core::types::ArtifactKind::Full => {
            reupload_format_from_sidecar(&artifact.local_path)
        }
        pplx_space_common_core::types::ArtifactKind::Disk => {
            sidecarless_format_from_artifacts(&artifact.local_path)
        }
    }
}

fn current_instance_backup_precondition(
    state: SandboxState,
    expected_kind: pplx_space_common_core::types::ArtifactKind,
) -> Result<(), Status> {
    use pplx_space_common_core::types::ArtifactKind;
    let current_kind = match state {
        SandboxState::Stopped => ArtifactKind::Disk,
        SandboxState::Suspended => ArtifactKind::Full,
        SandboxState::Restoring if expected_kind == ArtifactKind::Full => {
            return Err(Status::failed_precondition(format!(
                "cannot back up current instance: {}",
                pplx_space_common_core::node_drain_signals::REUPLOAD_SANDBOX_RESTORING_MARKER
            )));
        }
        SandboxState::Failed => {
            return Err(Status::failed_precondition(format!(
                "cannot back up current instance: {}",
                pplx_space_common_core::artifact_backup_signals::CURRENT_INSTANCE_BACKUP_SANDBOX_FAILED_MARKER
            )));
        }
        other => {
            return Err(Status::failed_precondition(format!(
                "cannot back up current instance: sandbox is {other}, must be stopped or suspended"
            )));
        }
    };
    if expected_kind == current_kind {
        return Ok(());
    }
    let message = if state == SandboxState::Suspended {
        format!(
            "requested {expected_kind} backup but {}",
            pplx_space_common_core::artifact_backup_signals::CURRENT_INSTANCE_BACKUP_PUBLISHES_FULL_MARKER
        )
    } else {
        format!(
            "requested {expected_kind} backup but current {state} instance publishes {current_kind}"
        )
    };
    Err(Status::failed_precondition(message))
}

fn artifact_backup_lookup_error(status: Status, full_current_instance: bool) -> Status {
    if full_current_instance && status.code() == tonic::Code::NotFound {
        Status::failed_precondition(format!(
            "cannot back up current instance: {}",
            pplx_space_common_core::node_drain_signals::REUPLOAD_SANDBOX_UNKNOWN_MARKER
        ))
    } else {
        status
    }
}

fn require_artifact_backup_files(
    local_path: &str,
    kind: pplx_space_common_core::types::ArtifactKind,
    format: crate::proto::SuspendSnapshotFormat,
    full_current_instance: bool,
) -> Result<(), Status> {
    for file in crate::snapshot_upload::artifact_files_for_kind(kind, format) {
        let path = format!("{local_path}/{}", file.local_name);
        if !std::path::Path::new(&path).try_exists().map_err(|error| {
            Status::internal(format!("failed to stat artifact at {path}: {error}"))
        })? {
            let reason = if full_current_instance {
                pplx_space_common_core::node_drain_signals::REUPLOAD_ARTIFACT_MISSING_MARKER
            } else {
                "local file is missing"
            };
            return Err(Status::failed_precondition(format!(
                "cannot back up artifact: {reason} at {path}"
            )));
        }
    }
    Ok(())
}

/// Map an [`OrgSnapshotConfigResolution`] to the upload-config the
/// artifact-backup admission should use, or to a tonic `Status` the
/// RPC should return immediately. The classification matters because
/// the drain reconciler's `FailedPrecondition`-marker classifier treats
/// reupload errors as terminal (mark-as-failed); a transient lookup
/// failure must therefore surface as `Internal` (retryable) instead.
///
/// - `Resolved(cfg)` → `Ok(Some(cfg))`.
/// - `Absent` + env fallback present → `Ok(None)` (handler proceeds with
///   the env default in `self.snapshot_upload_config`).
/// - `Absent` + no env fallback → `Err(FailedPrecondition)` (operator
///   must wire `SPACE_SNAPSHOT_BUCKET` or register the org override).
/// - `LookupFailed(_)` → `Err(Internal)` regardless of env fallback, so
///   the reconciler keeps retrying through a transient cluster-api blip.
pub(super) fn reupload_resolution_to_config(
    resolution: OrgSnapshotConfigResolution,
    env_fallback_present: bool,
) -> Result<Option<crate::snapshot_upload::SnapshotUploadConfig>, Status> {
    match resolution {
        OrgSnapshotConfigResolution::Resolved(cfg) => Ok(Some(cfg)),
        OrgSnapshotConfigResolution::Absent => {
            if env_fallback_present {
                Ok(None)
            } else {
                Err(Status::failed_precondition(
                    "cannot reupload snapshot: no snapshot upload destination configured \
                     (set SPACE_SNAPSHOT_BUCKET / SPACE_SNAPSHOT_REGION on this node, or \
                     register an org-level snapshot_base_object_storage_uri override)",
                ))
            }
        }
        OrgSnapshotConfigResolution::LookupFailed(reason) => Err(Status::internal(format!(
            "cannot reupload snapshot: org snapshot upload override lookup failed: \
             {reason}; the reconciler should retry once the cluster-api recovers",
        ))),
        OrgSnapshotConfigResolution::ClusterRowMissing => Err(Status::failed_precondition(
            "cannot reupload snapshot: cluster-api has no row for this sandbox \
             (GetSandboxSnapshotStorageUri returned NotFound). Reupload requires \
             a prior suspend to have registered the sandbox; the durable worker's \
             BeginArtifactBackup would otherwise also fail permanently. \
             Investigate state divergence between this node and the cluster.",
        )),
    }
}

fn suspend_snapshot_metadata(
    sandbox: &crate::db::SandboxRow,
    format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
) -> Result<crate::snapshot_upload::SnapshotMeta, Status> {
    let snapshot_execution = sandbox_snapshot_execution_metadata(&sandbox.id)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let execution = snapshot_execution.execution().clone();
    let rootfs_diff_format = match format {
        pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::BtrfsSend => {
            Some(crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1.to_string())
        }
        _ => None,
    };
    Ok(crate::snapshot_upload::SnapshotMeta {
        version: crate::snapshot_upload::SNAPSHOT_METADATA_VERSION,
        execution: Some(snapshot_execution),
        sandbox_id: sandbox.id.clone(),
        size_bytes: 0,
        created_at: jiff::Timestamp::now().to_string(),
        source: crate::snapshot_upload::SnapshotSource {
            runtime: execution.runtime().kind.alias().to_string(),
            runtime_version: None,
            state_format_version: None,
            template_id: sandbox.template.clone(),
            content_hash: sandbox.template_hash.clone(),
            rootfs_size_mib: sandbox.rootfs_size_mb,
            disk_io_max_operations_per_sec: Some(sandbox.disk_io_limits.max_operations_per_sec()),
            disk_io_max_bandwidth_mib_per_sec: Some(
                sandbox.disk_io_limits.max_bandwidth_mib_per_sec(),
            ),
            rootfs_version: None,
            kernel_version: None,
            kernel_digest: None,
        },
        sandbox: Some(crate::snapshot_upload::SnapshotSandbox {
            owner: sandbox.user.to_string(),
            snapshots_enabled: sandbox.snapshots_enabled,
            process_env: sandbox.process_env.clone(),
            auto_lifecycle: sandbox.auto_lifecycle.clone(),
        }),
        rootfs_diff_format,
        // Set by the suspend publication once it knows whether the sandbox's
        // live rootfs is encrypted (BYOK); plaintext suspends leave it false.
        encrypted: false,
        local_artifact_id: None,
        artifact_kind: None,
        artifact_origin: None,
        artifact_snapshot_name: None,
    })
}

struct LocalArtifactStagingGuard {
    path: std::path::PathBuf,
    armed: bool,
}

impl LocalArtifactStagingGuard {
    fn new(path: std::path::PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    async fn cleanup(mut self, node_client: &mut NodeManagerGrpcClient) {
        self.armed = false;
        // An encode RPC error is not proof that its host writer has exited.
        // Node-manager serializes deletion with that writer, including reap.
        if let Err(error) = node_client
            .delete_cache_artifact(
                pplx_space_node_mgr_client::proto::DeleteCacheArtifactRequest {
                    local_path: self.path.to_string_lossy().into_owned(),
                },
            )
            .await
        {
            tracing::warn!(path = %self.path.display(), %error,
                "failed to remove suspend staging; retained for inventory recovery or inspection");
        }
    }
}

impl Drop for LocalArtifactStagingGuard {
    fn drop(&mut self) {
        if self.armed {
            // Abandonment is not proof that the remote capture stopped writing.
            // Retain the evidence for recovery or operator inspection rather
            // than performing blocking deletion in Drop.
            tracing::warn!(path = %self.path.display(),
                "suspend publication abandoned; staging retained for inventory inspection");
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SuspendSnapshotPublicationFailurePersistence {
    RolledBackToPaused,
    Quarantined,
    StateChanged,
    PersistFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SuspendSnapshotStagingDisposition {
    Preserve,
    Cleanup,
}

pub(super) fn failed_encrypted_capture_may_release_volume(runtime_live: bool) -> bool {
    !runtime_live
}

/// Re-arms watchdog monitoring for a still-live sandbox if the suspend
/// publication is abandoned before its intentional teardown — a failure that
/// rolls the sandbox back to `paused` *or* a dropped handler future (client
/// disconnect / deadline mid-`create_snapshot`).
///
/// The encrypted capture unsubscribes the watchdog and the snapshot scheduler
/// *before* the fallible `create_snapshot` teardown, so the watchdog does not
/// fight the intentional runtime stop with sidecar-restart attempts. Anything
/// that abandons the suspend after that point would otherwise leave a live,
/// paused sandbox with unmonitored sidecars. This guard restores the watchdog
/// on drop when the runtime is still live. Re-arming reloads the current
/// SQLite row by sandbox ID instead of restoring this lifecycle operation's
/// older PID snapshot, so a concurrent sidecar replacement remains
/// authoritative.
///
/// It deliberately re-arms **only the watchdog, not the snapshot scheduler**:
/// the rolled-back state is `paused`, and a paused sandbox is watchdog-
/// monitored but not snapshot-scheduled (matching the `pause` RPC, which
/// removes the scheduler and keeps the watchdog). The scheduler is re-added by
/// the resume/restore/start path when the sandbox next goes live. Disarm on
/// the intentional-teardown path, where monitoring is meant to end.
struct SuspendMonitoringGuard {
    armed: bool,
    watchdog: Arc<crate::watchdog::SandboxWatchdog>,
    runtime_live: Arc<dyn Fn(&str) -> bool + Send + Sync>,
    sandbox_id: String,
}

impl SuspendMonitoringGuard {
    fn new(service: &SandboxManagerService, sandbox: &crate::db::SandboxRow) -> Self {
        Self {
            armed: true,
            watchdog: Arc::clone(&service.watchdog),
            runtime_live: Arc::clone(&service.sandbox_runtime_live),
            sandbox_id: sandbox.id.clone(),
        }
    }

    /// The suspend reached its intentional teardown (or otherwise owns the
    /// monitoring lifecycle from here); do not re-arm on drop.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SuspendMonitoringGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Drop can't await; re-add on the runtime if one is still available
        // (it is during task cancellation and normal returns; absent only
        // during runtime shutdown, where re-arming is moot).
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let watchdog = Arc::clone(&self.watchdog);
            let runtime_live = Arc::clone(&self.runtime_live);
            let sandbox_id = self.sandbox_id.clone();
            handle.spawn(async move {
                if runtime_live(&sandbox_id) {
                    watchdog.rearm_sandbox_from_db(&sandbox_id).await;
                }
            });
        }
    }
}

#[cfg(test)]
impl SandboxManagerService {
    /// Build and immediately drop an armed [`SuspendMonitoringGuard`], the way
    /// an abandoned suspend would, so tests can assert the drop-time re-arm.
    pub(super) fn drop_armed_suspend_monitoring_guard_for_test(
        &self,
        sandbox: &crate::db::SandboxRow,
    ) {
        drop(SuspendMonitoringGuard::new(self, sandbox));
    }

    /// Build a guard, disarm it (as the intentional-teardown path does), then
    /// drop it — must not re-arm.
    pub(super) fn drop_disarmed_suspend_monitoring_guard_for_test(
        &self,
        sandbox: &crate::db::SandboxRow,
    ) {
        let mut guard = SuspendMonitoringGuard::new(self, sandbox);
        guard.disarm();
        drop(guard);
    }

    pub(super) async fn submit_artifact_backup_for_test(
        &self,
        sandbox: &crate::db::SandboxRow,
        artifact_attempt: (
            LocalArtifactId,
            pplx_space_common_core::types::BackupAttemptId,
        ),
        snapshot_dir: &str,
        request_override: &crate::snapshot_upload::SnapshotUploadConfig,
        dedup_key: &str,
        restore_visible: bool,
    ) -> Result<SubmitSuspendSnapshotOutcome, String> {
        let (local_artifact_id, backup_attempt_id) = artifact_attempt;
        self.submit_artifact_backup(
            sandbox,
            ArtifactBackupSubmission {
                local_artifact_id,
                artifact_kind: pplx_space_common_core::types::ArtifactKind::Full,
                artifact_origin: pplx_space_common_core::types::ArtifactOrigin::Suspend,
                snapshot_name: None,
                backup_attempt_id,
                snapshot_dir,
                request_override: Some(request_override),
                format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Full,
                dedup_key: Some(dedup_key),
                handoff: if restore_visible {
                    ArtifactBackupHandoff::RestoreVisible
                } else {
                    ArtifactBackupHandoff::DurableWorker
                },
            },
        )
        .await
    }
}

impl SandboxManagerService {
    pub(super) fn record_suspend_snapshot_publication_failure_after_begin(
        &self,
        sandbox_id: &str,
        reason: &str,
    ) -> SuspendSnapshotPublicationFailurePersistence {
        let runtime_live = (self.sandbox_runtime_live)(sandbox_id);
        self.record_suspend_snapshot_publication_failure_after_begin_with_liveness(
            sandbox_id,
            reason,
            runtime_live,
        )
    }

    fn record_suspend_snapshot_publication_failure_after_begin_with_liveness(
        &self,
        sandbox_id: &str,
        reason: &str,
        runtime_live: bool,
    ) -> SuspendSnapshotPublicationFailurePersistence {
        if runtime_live {
            let rolled_back = {
                let Ok(db) = self.db_conn() else {
                    return SuspendSnapshotPublicationFailurePersistence::PersistFailed;
                };
                crate::db::rollback_suspending_to_paused(&db, sandbox_id)
            };
            match rolled_back {
                Ok(true) => {
                    tracing::warn!(
                        sandbox_id,
                        reason,
                        "rolled interrupted suspend publication back to paused"
                    );
                    mark_sandbox_dirty(&self.db, sandbox_id);
                    return SuspendSnapshotPublicationFailurePersistence::RolledBackToPaused;
                }
                Ok(false) => {
                    tracing::warn!(
                        sandbox_id,
                        reason,
                        "suspend publication failure raced with another state transition"
                    );
                    return SuspendSnapshotPublicationFailurePersistence::StateChanged;
                }
                Err(error) => {
                    tracing::warn!(
                        sandbox_id,
                        reason,
                        %error,
                        "failed to roll interrupted suspend publication back to paused"
                    );
                }
            }
        }

        let quarantined = {
            let Ok(db) = self.db_conn() else {
                return SuspendSnapshotPublicationFailurePersistence::PersistFailed;
            };
            crate::db::mark_suspending_quarantined(&db, sandbox_id, reason)
        };
        match quarantined {
            Ok(true) => {
                mark_sandbox_dirty(&self.db, sandbox_id);
                SuspendSnapshotPublicationFailurePersistence::Quarantined
            }
            Ok(false) => {
                tracing::warn!(
                    sandbox_id,
                    reason,
                    "suspend publication failure did not quarantine because state changed"
                );
                SuspendSnapshotPublicationFailurePersistence::StateChanged
            }
            Err(error) => {
                tracing::warn!(
                    sandbox_id,
                    reason,
                    %error,
                    "failed to persist suspend publication failure"
                );
                SuspendSnapshotPublicationFailurePersistence::PersistFailed
            }
        }
    }

    pub(super) fn record_suspend_snapshot_publication_failure_after_staging(
        &self,
        sandbox_id: &str,
        sandbox_dir: &std::path::Path,
        current_staging_dir: &std::path::Path,
        reason: &str,
    ) -> SuspendSnapshotStagingDisposition {
        let runtime_live = (self.sandbox_runtime_live)(sandbox_id);
        if runtime_live {
            self.record_suspend_snapshot_publication_failure_after_begin_with_liveness(
                sandbox_id, reason, true,
            );
            // The attempt's guard asks node-manager to remove its staging
            // after remote transfer ownership ends. State rollback alone is
            // not evidence that the encoder stopped writing.
            // A live rollback leaves the watchdog to be re-armed by the
            // caller's SuspendMonitoringGuard on return.
            return SuspendSnapshotStagingDisposition::Cleanup;
        }

        let row = {
            let Ok(db) = self.db_conn() else {
                return SuspendSnapshotStagingDisposition::Preserve;
            };
            match crate::db::get_sandbox(&db, sandbox_id) {
                Ok(Some(row)) if row.state == SandboxState::Suspending => row,
                Ok(Some(row)) => {
                    tracing::warn!(
                        sandbox_id,
                        reason,
                        state = %row.state,
                        "suspend publication failure raced with another state transition"
                    );
                    return SuspendSnapshotStagingDisposition::Cleanup;
                }
                Ok(None) => {
                    tracing::warn!(
                        sandbox_id,
                        reason,
                        "suspend publication failure raced with sandbox deletion"
                    );
                    return SuspendSnapshotStagingDisposition::Cleanup;
                }
                Err(error) => {
                    tracing::warn!(
                        sandbox_id,
                        reason,
                        %error,
                        "failed to load sandbox after suspend publication failure"
                    );
                    if current_staging_dir.exists() {
                        return SuspendSnapshotStagingDisposition::Preserve;
                    }
                    return SuspendSnapshotStagingDisposition::Cleanup;
                }
            }
        };

        let decision = match classify_suspend_snapshot_publication(
            sandbox_dir,
            row.current_local_artifact_id,
            false,
        ) {
            Ok(decision) => decision,
            Err(error) => {
                tracing::warn!(
                    sandbox_id,
                    reason,
                    %error,
                    "failed to classify interrupted suspend publication"
                );
                // `runtime_live` is false on this path (the live branch
                // returned above), so the rollback-to-paused arm of the
                // persistence handler is unreachable here.
                self.record_suspend_snapshot_publication_failure_after_begin_with_liveness(
                    sandbox_id,
                    reason,
                    runtime_live,
                );
                if current_staging_dir.exists() {
                    return SuspendSnapshotStagingDisposition::Preserve;
                }
                return SuspendSnapshotStagingDisposition::Cleanup;
            }
        };

        match decision {
            SuspendSnapshotPublicationDecision::RuntimeLiveWithoutUsableArtifact => unreachable!(
                "runtime-live suspend publication failures return before artifact classification"
            ),
            decision @ (SuspendSnapshotPublicationDecision::StagedComplete(_)
            | SuspendSnapshotPublicationDecision::CanonicalReady(_)) => {
                let preserve_current_staging = matches!(
                    &decision,
                    SuspendSnapshotPublicationDecision::StagedComplete(staged)
                        if staged.staging_dir == current_staging_dir
                );
                let teardown_generation = {
                    let Ok(db) = self.db_conn() else {
                        return SuspendSnapshotStagingDisposition::Preserve;
                    };
                    match crate::db::teardown_suspend_status(&db, sandbox_id) {
                        Ok(Some(status)) => status
                            .operation
                            .filter(|operation| operation.status.is_active())
                            .map(|operation| operation.execution_generation),
                        Ok(_) => None,
                        Err(error) => {
                            tracing::warn!(
                                sandbox_id,
                                %error,
                                "could not prove suspend durability mode after publish failure"
                            );
                            return SuspendSnapshotStagingDisposition::Preserve;
                        }
                    }
                };
                match self.apply_suspend_snapshot_publication_recovery_decision(
                    row,
                    sandbox_dir.to_path_buf(),
                    decision,
                    SuspendSnapshotPublicationRecoveryOrigin::LivePublishFailure,
                    teardown_generation,
                ) {
                    Ok(record) => {
                        if record.outcome.changed() {
                            mark_sandbox_dirty(&self.db, sandbox_id);
                        }
                        match record.outcome {
                            crate::db::StartupRecoveryOutcome::CleanupRequired { .. } => {
                                SuspendSnapshotStagingDisposition::Cleanup
                            }
                            crate::db::StartupRecoveryOutcome::Deferred => {
                                // The complete artifact still has a retry owner:
                                // periodic publication recovery scans suspending
                                // rows, not operator-attention quarantines.
                                if preserve_current_staging {
                                    SuspendSnapshotStagingDisposition::Preserve
                                } else {
                                    SuspendSnapshotStagingDisposition::Cleanup
                                }
                            }
                            crate::db::StartupRecoveryOutcome::State(_) => {
                                SuspendSnapshotStagingDisposition::Cleanup
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            sandbox_id,
                            reason,
                            %error,
                            "failed to finish complete suspend publication after live publish failure"
                        );
                        self.record_suspend_snapshot_publication_failure_after_begin_with_liveness(
                            sandbox_id, reason, false,
                        );
                        if preserve_current_staging && current_staging_dir.exists() {
                            SuspendSnapshotStagingDisposition::Preserve
                        } else {
                            SuspendSnapshotStagingDisposition::Cleanup
                        }
                    }
                }
            }
            SuspendSnapshotPublicationDecision::RuntimeGoneWithoutUsableArtifact => {
                self.record_suspend_snapshot_publication_failure_after_begin_with_liveness(
                    sandbox_id, reason, false,
                );
                SuspendSnapshotStagingDisposition::Cleanup
            }
        }
    }
}

pub(super) struct SuspendSnapshotPublicationContext {
    cleanup_fence: OwnedCleanupFenceGuard,
    sandbox: crate::db::SandboxRow,
    requested_format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
    suspend_format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
    create_codec: &'static str,
    local_artifact_id: LocalArtifactId,
    event_details: SandboxEventDetails,
    started_at: std::time::Instant,
    publish_node_state: bool,
    durability: SuspendDurability,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SuspendDurability {
    NodeLocal,
    RemoteBeforeTeardown {
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
    },
}

impl SuspendDurability {
    const fn is_remote_before_teardown(self) -> bool {
        matches!(self, Self::RemoteBeforeTeardown { .. })
    }

    const fn execution_generation(
        self,
    ) -> Option<pplx_space_common_core::types::ExecutionGeneration> {
        match self {
            Self::NodeLocal => None,
            Self::RemoteBeforeTeardown {
                execution_generation,
            } => Some(execution_generation),
        }
    }
}

pub(super) struct SuspendRequestContext {
    sandbox_id: String,
    blocking: bool,
    requested_format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
    suspend_format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
    create_codec: &'static str,
    publish_node_state: bool,
    event_trigger: Option<SandboxEventTrigger>,
    durability: SuspendDurability,
}

impl SuspendRequestContext {
    fn guest_preparation_budget(&self) -> GuestSnapshotPreparationBudget {
        if self.event_trigger == Some(SandboxEventTrigger::AutoLifecycle) {
            GuestSnapshotPreparationBudget::AutoLifecycle
        } else {
            GuestSnapshotPreparationBudget::Standard
        }
    }

    pub(super) fn new(
        sandbox_id: String,
        blocking: bool,
        format: i32,
        publish_node_state: bool,
        event_trigger: Option<SandboxEventTrigger>,
    ) -> Self {
        // Per-request rootfs codec. `Unspecified` (the default existing
        // API-substrate callers send) maps to `BtrfsSend`, or to `Full` when
        // `SPACE_SUSPEND_DEFAULT_FORMAT=full` is set on this worker (the
        // rollback lever). Explicit `BtrfsSend`/`Full` on the wire still
        // overrides the default.
        let requested_format =
            pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::try_from(format)
                .unwrap_or(pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Unspecified);
        let suspend_format = match requested_format {
            pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Unspecified => {
                if env_default_is_full() {
                    pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Full
                } else {
                    pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::BtrfsSend
                }
            }
            other => other,
        };
        // Codec tag for the `snapshot.create.*` metric family. Mirrors
        // the closed vocabulary in `pplx_space_metrics::snapshot`:
        // `full` and `btrfs_send_v1` (note underscore for the tag value
        // vs hyphen in the wire vocabulary).
        let create_codec = match suspend_format {
            pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::BtrfsSend => {
                pplx_space_metrics::snapshot::CODEC_BTRFS_SEND_V1
            }
            _ => pplx_space_metrics::snapshot::CODEC_FULL,
        };

        Self {
            sandbox_id,
            blocking,
            requested_format,
            suspend_format,
            create_codec,
            publish_node_state,
            event_trigger,
            durability: SuspendDurability::NodeLocal,
        }
    }

    pub(super) fn for_teardown(
        sandbox_id: String,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
    ) -> Self {
        let mut context = Self::new(
            sandbox_id,
            true,
            pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Unspecified as i32,
            true,
            Some(SandboxEventTrigger::System),
        );
        context.durability = SuspendDurability::RemoteBeforeTeardown {
            execution_generation,
        };
        context
    }
}

struct ArtifactBackupSubmission<'a> {
    local_artifact_id: LocalArtifactId,
    artifact_kind: pplx_space_common_core::types::ArtifactKind,
    artifact_origin: pplx_space_common_core::types::ArtifactOrigin,
    snapshot_name: Option<&'a str>,
    backup_attempt_id: pplx_space_common_core::types::BackupAttemptId,
    snapshot_dir: &'a str,
    request_override: Option<&'a crate::snapshot_upload::SnapshotUploadConfig>,
    format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
    dedup_key: Option<&'a str>,
    handoff: ArtifactBackupHandoff,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArtifactBackupHandoff {
    /// Persist the intent and let the durable worker perform remote work.
    DurableWorker,
    /// Stage immutable transfer handles and create the restore-visible cluster
    /// row before returning, then let the durable worker poll and finalize.
    RestoreVisible,
    /// Stage and register the exact artifact before runtime teardown.
    TeardownFence,
}

pub(super) const AUTO_LIFECYCLE_SUSPEND_BACKUP_FENCE_FAILED_METADATA: &str =
    "x-space-auto-lifecycle-suspend-backup-fence-failed";

pub(super) fn artifact_backup_fence_status(error: String) -> Status {
    let mut status = Status::internal(format!("artifact backup submission failed: {error}"));
    status.metadata_mut().insert(
        AUTO_LIFECYCLE_SUSPEND_BACKUP_FENCE_FAILED_METADATA,
        tonic::metadata::MetadataValue::from_static("true"),
    );
    status
}

struct TeardownSuspendBackup<'a> {
    execution_generation: pplx_space_common_core::types::ExecutionGeneration,
    artifact: &'a crate::db::LocalArtifactRow,
    format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
}

#[derive(Debug, Clone, Copy)]
enum SuspendedArtifactAdoption {
    TouchUpdatedAt,
    PreserveUpdatedAt,
}

impl SandboxManagerService {
    pub(super) fn require_node_local_backup_handoff(
        &self,
        sandbox_id: &str,
        local_artifact_id: LocalArtifactId,
        result: Result<SubmitSuspendSnapshotOutcome, Status>,
    ) -> Result<(), Status> {
        match result {
            Ok(SubmitSuspendSnapshotOutcome::Enqueued { job_id, .. }) => {
                tracing::info!(
                    sandbox_id,
                    local_artifact_id = %local_artifact_id,
                    job_id = %job_id,
                    "persisted suspend artifact backup before runtime teardown",
                );
                Ok(())
            }
            Ok(SubmitSuspendSnapshotOutcome::NoUploadConfig) => Ok(()),
            Err(status) => {
                let reason =
                    format!("persist suspend artifact backup before runtime teardown: {status}");
                self.record_suspend_snapshot_publication_failure_after_begin(sandbox_id, &reason);
                Err(status)
            }
        }
    }

    fn adopt_or_get_suspended_local_artifact(
        &self,
        sandbox_id: &str,
        local_artifact_id: LocalArtifactId,
        local_path: &str,
    ) -> Result<LocalArtifactId, Status> {
        self.adopt_or_get_suspended_local_artifact_with_mode(
            sandbox_id,
            local_artifact_id,
            local_path,
            SuspendedArtifactAdoption::TouchUpdatedAt,
        )
    }

    fn adopt_or_get_suspended_local_artifact_preserving_updated_at(
        &self,
        sandbox_id: &str,
        local_artifact_id: LocalArtifactId,
        local_path: &str,
    ) -> Result<LocalArtifactId, Status> {
        self.adopt_or_get_suspended_local_artifact_with_mode(
            sandbox_id,
            local_artifact_id,
            local_path,
            SuspendedArtifactAdoption::PreserveUpdatedAt,
        )
    }

    fn adopt_or_get_suspended_local_artifact_with_mode(
        &self,
        sandbox_id: &str,
        local_artifact_id: LocalArtifactId,
        local_path: &str,
        mode: SuspendedArtifactAdoption,
    ) -> Result<LocalArtifactId, Status> {
        let adopted = match mode {
            SuspendedArtifactAdoption::TouchUpdatedAt => {
                self.local_artifacts
                    .adopt_suspended(sandbox_id, local_artifact_id, local_path)
            }
            SuspendedArtifactAdoption::PreserveUpdatedAt => self
                .local_artifacts
                .adopt_suspended_preserving_updated_at(sandbox_id, local_artifact_id, local_path),
        }
        .map_err(|err| Status::internal(format!("db artifact adoption failed: {err}")))?;
        if adopted {
            return Ok(local_artifact_id);
        }

        let sandbox = {
            let db = self.db_conn()?;
            crate::db::get_sandbox(&db, sandbox_id)
                .map_err(|err| Status::internal(format!("db sandbox lookup failed: {err}")))?
                .ok_or_else(|| Status::not_found(format!("sandbox {sandbox_id} not found")))?
        };
        if sandbox.state != SandboxState::Suspended {
            return Err(Status::failed_precondition(format!(
                "cannot adopt suspended local artifact: sandbox is {}, must be suspended",
                sandbox.state
            )));
        }
        let current_id = sandbox.current_local_artifact_id.ok_or_else(|| {
            Status::failed_precondition(
                "current suspended instance has no published local artifact",
            )
        })?;
        if sandbox.current_local_artifact_kind
            != Some(pplx_space_common_core::types::ArtifactKind::Full)
            || sandbox.current_local_artifact_origin
                != Some(pplx_space_common_core::types::ArtifactOrigin::Suspend)
        {
            return Err(Status::failed_precondition(
                "current suspended instance artifact is not a full suspend artifact",
            ));
        }
        if self
            .local_artifacts
            .get(current_id)
            .map_err(|err| Status::internal(format!("db artifact lookup failed: {err}")))?
            .is_none()
        {
            return Err(Status::failed_precondition(format!(
                "local artifact {current_id} is not published"
            )));
        }
        Ok(current_id)
    }

    pub(super) async fn pause(
        &self,
        request: Request<PauseRequest>,
    ) -> Result<Response<PauseResponse>, Status> {
        let peer = self.authorize_peer(&request, "Pause", ALLOW_SANDBOX_OPERATOR_OR_TMPLMGR)?;
        let publish_node_state = !should_skip_node_publish(&request);
        let sandbox_id = &request.get_ref().sandbox_id;

        let sandbox = self.lookup_sandbox(sandbox_id)?;
        self.authorize_template_manager_sandbox(&peer, &sandbox, "Pause")?;
        let mut event_details =
            SandboxEventDetails::default().with_from_state(sandbox.state.to_string());
        if let Some(trigger) = sandbox_event_trigger(&request) {
            event_details = event_details.trigger(trigger);
        }
        emit_sandbox_event(
            &sandbox.id,
            SandboxEventType::PauseRequested,
            SandboxEventSource::SbxManager,
            event_details,
        );
        let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox.id);
        let _artifact_guard = artifact_lock.lock().await;
        let sandbox = self.lookup_sandbox(&sandbox.id)?;
        self.authorize_template_manager_sandbox(&peer, &sandbox, "Pause")?;
        sandbox
            .state
            .transition(SandboxState::Pausing)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;

        {
            let db = self.db_conn()?;
            require_state_write(
                crate::db::update_state(&db, &sandbox.id, SandboxState::Pausing),
                "begin pause",
            )?;
        }
        self.watchdog.record_lifecycle_edge(&sandbox.id).await;

        if let Err(e) = prepare_guest_for_vm_snapshot_via_spaced_host(
            &sandbox.id,
            GuestSnapshotPreparationBudget::Standard,
        )
        .await
        {
            let db = self.db_conn()?;
            if let Err(error) = require_state_write(
                crate::db::update_state(&db, &sandbox.id, SandboxState::Started),
                "roll back failed pause preparation",
            ) {
                tracing::warn!(sandbox_id = %sandbox.id, %error, "failed to roll back pause state");
            }
            return Err(Status::internal(format!(
                "guest snapshot preparation before pause failed: {e}"
            )));
        }

        let mut node_client = match connect_node().await {
            Ok(node_client) => node_client,
            Err(e) => {
                update_state_best_effort(&self.db, &sandbox.id, SandboxState::Started);
                return Err(Status::internal(format!(
                    "cannot connect to node-manager: {e}"
                )));
            }
        };

        let sandbox_id = sandbox
            .id
            .parse::<SandboxId>()
            .map_err(|error| Status::internal(format!("invalid sandbox ID: {error}")))?;
        let pause_state = pause_with_reconciliation(
            &mut node_client,
            sandbox.node_execution_fence(),
            &self.db,
            &sandbox_id,
            "pause failed",
            GuestSnapshotPreparationBudget::Standard,
        )
        .await;
        if pause_state != SandboxState::Paused {
            self.watchdog.record_lifecycle_edge(&sandbox.id).await;
            return Err(Status::internal(format!(
                "pause failed; sandbox reconciled to {pause_state}"
            )));
        }

        {
            let db = self.db_conn()?;
            require_state_write(
                crate::db::update_state(&db, &sandbox.id, SandboxState::Paused),
                "complete pause",
            )?;
        }

        // Pause snapshot scheduling
        self.snapshot_scheduler.remove_sandbox(&sandbox.id).await;
        if publish_node_state {
            mark_sandbox_dirty(&self.db, &sandbox.id);
        }

        emit_sandbox_event(
            &sandbox.id,
            SandboxEventType::PauseCompleted,
            SandboxEventSource::SbxManager,
            SandboxEventDetails::default().with_to_state(SandboxState::Paused.to_string()),
        );
        Ok(Response::new(PauseResponse {}))
    }

    pub(super) async fn resume(
        &self,
        request: Request<ResumeRequest>,
    ) -> Result<Response<ResumeResponse>, Status> {
        let peer = self.authorize_peer(&request, "Resume", ALLOW_SANDBOX_OPERATOR_OR_TMPLMGR)?;
        let req = request.get_ref();
        let policy = req
            .activation_auto_lifecycle
            .as_ref()
            .map(policy_from_proto)
            .transpose()?;
        let generation = req
            .expected_execution_generation
            .map(ExecutionGeneration::new)
            .transpose()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let resume_policy = match (policy.as_ref(), generation) {
            (Some(policy), generation) => {
                self.authorize_peer(
                    &request,
                    "Resume.activation_auto_lifecycle",
                    ALLOW_SANDBOX_START_CONTEXT,
                )?;
                Some((policy, generation))
            }
            (None, None) => None,
            _ => {
                return Err(Status::invalid_argument(
                    "resume execution generation requires a policy",
                ));
            }
        };
        let blocking = req.blocking;
        let sandbox_id = &req.sandbox_id;
        let ready_timeout_secs = optional_positive_or_default(
            "ready_timeout_secs",
            req.ready_timeout_secs,
            DEFAULT_RESUME_READY_TIMEOUT_SECS,
        )?;

        let sandbox = self.lookup_sandbox(sandbox_id)?;
        self.authorize_template_manager_sandbox(&peer, &sandbox, "Resume")?;
        let publish_node_state = !should_skip_node_publish(&request);
        let sandbox_id = sandbox.id.clone();
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox.id);
        let _guard = wake_lock.lock().await;
        let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox.id);
        let _guard = artifact_lock.lock().await;

        let sandbox = self.lookup_sandbox(&sandbox.id)?;
        self.authorize_template_manager_sandbox(&peer, &sandbox, "Resume")?;
        self.resume_after_wake_lock(&sandbox.id, resume_policy, blocking, ready_timeout_secs)
            .await?;
        if publish_node_state {
            mark_sandbox_dirty(&self.db, &sandbox_id);
        }
        let sandbox = self.lookup_sandbox(&sandbox_id)?;
        Ok(Response::new(ResumeResponse {
            route_identity: space_api_route_identity_to_wire(&sandbox.space_api_route_identity),
        }))
    }

    pub(super) async fn wake(
        &self,
        request: Request<WakeRequest>,
    ) -> Result<Response<WakeResponse>, Status> {
        let req = request.get_ref();
        let target_state = wake_target_state_from_proto(req.target_state)?;
        let activity =
            match crate::proto::AutoLifecycleWakeSource::try_from(req.implicit_wake_source) {
                Ok(crate::proto::AutoLifecycleWakeSource::Unspecified) => None,
                Ok(crate::proto::AutoLifecycleWakeSource::Access) => {
                    Some(GuestActivityKind::Access)
                }
                Ok(crate::proto::AutoLifecycleWakeSource::Command) => {
                    Some(GuestActivityKind::Command)
                }
                Err(_) => return Err(Status::invalid_argument("invalid implicit wake source")),
            };
        if activity.is_some() && (!req.blocking || target_state != SandboxState::Started) {
            return Err(Status::invalid_argument(
                "implicit wake requires blocking STARTED",
            ));
        }
        emit_sandbox_event(
            &req.sandbox_id,
            SandboxEventType::WakeRequested,
            SandboxEventSource::SbxManager,
            SandboxEventDetails::default()
                .target_state(target_state.to_string())
                .bool_field(
                    SandboxEventBoolField::HasNetnsPath,
                    req.start_context
                        .as_ref()
                        .and_then(|start_context| start_context.netns_path.as_deref())
                        .is_some_and(|path| !path.is_empty()),
                )
                .bool_field(
                    SandboxEventBoolField::RestoreStorageUriPresent,
                    req.start_context
                        .as_ref()
                        .and_then(|start_context| {
                            start_context.suspend_restore_storage_uri.as_ref()
                        })
                        .is_some(),
                ),
        );
        let mut start = if let Some(start_context) = &req.start_context {
            self.authorize_peer(&request, "Wake.start_context", ALLOW_SANDBOX_START_CONTEXT)?;
            let space_api_route_identity =
                space_api_route_identity_from_wake_start_context(start_context)?;
            let route_identity_provenance =
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::for_activation_identity(
                    &space_api_route_identity,
                );
            if start_context.expected_execution_generation.is_some()
                && start_context.activation_auto_lifecycle.is_none()
            {
                return Err(Status::invalid_argument(
                    "wake execution generation requires a policy",
                ));
            }
            WakeStartConfig {
                expected_execution_generation: start_context
                    .expected_execution_generation
                    .map(ExecutionGeneration::new)
                    .transpose()
                    .map_err(|error| Status::invalid_argument(error.to_string()))?,
                activation_auto_lifecycle: start_context
                    .activation_auto_lifecycle
                    .as_ref()
                    .map(policy_from_proto)
                    .transpose()?,
                operation_activation_attempt_id: operation_activation_attempt_id_from_wire(
                    start_context.operation_activation_attempt_id.as_deref(),
                    &space_api_route_identity,
                )?,
                vcpu: start_context.vcpu,
                memory_mib: start_context.memory_mib,
                network_context: WakeNetworkContext::for_wake_start_context(start_context),
                target_state,
                suspend_restore_storage_uri: start_context.suspend_restore_storage_uri.clone(),
                suspend_restore_source_node: start_context.suspend_restore_source_node.clone(),
                requested_space_api_route_identity: Some(space_api_route_identity.clone()),
                space_api_route_identity,
                route_identity_provenance,
                authoritative_route_identity: start_context.route_identity.is_some(),
                provisional_activation_attempt: false,
            }
        } else {
            self.authorize_peer(&request, "Wake", ALLOW_SANDBOX_OPERATOR)?;
            WakeStartConfig {
                target_state,
                ..WakeStartConfig::default()
            }
        };
        let normalized_route_identity =
            space_api_route_identity_for_runtime_activation(start.space_api_route_identity);
        start.space_api_route_identity = normalized_route_identity.identity;
        start.route_identity_provenance = normalized_route_identity.provenance;
        start.provisional_activation_attempt =
            normalized_route_identity.provisional_activation_attempt;
        let publish_node_state = !should_skip_node_publish(&request);
        let sandbox_id = req.sandbox_id.clone();

        // An implicit wake cannot stage an absent execution before its effective
        // local policy is known. Existing residents are checked under the wake
        // lock by ensure_active_with_start, including after lock contention.
        if activity.is_some() {
            self.lookup_sandbox(&sandbox_id)?;
        }

        // If the sandbox row is absent locally, stage from a checkpoint so
        // `wake_with_start_config` below sees a `Suspended` row to wake.
        // `blocking: false` here keeps the ready-wait and the user-facing
        // `WakeTransition` reporting on `wake_with_start_config`; a
        // blocking restore here would double-take the wake lock and the
        // outer call would report `Noop`.
        if let Err(status) = self.lookup_sandbox(&sandbox_id)
            && status.code() == tonic::Code::NotFound
        {
            // `target_state` propagates here too: with `blocking: false`,
            // `start_sandbox` still routes through `start_from_checkpoint`
            // and runs the resume RPC unless we tell it to stop at
            // paused. Without this propagation the staged-in restore
            // would transition through `Started` before
            // `wake_with_start_config` observes the row.
            let netns_path = match start.network_context.netns_path_for_launch() {
                Ok(netns_path) => netns_path,
                Err(error) => {
                    emit_sandbox_event(
                        &sandbox_id,
                        SandboxEventType::WakeFailed,
                        SandboxEventSource::SbxManager,
                        SandboxEventDetails::default()
                            .target_state(target_state.to_string())
                            .reason("stage_from_checkpoint")
                            .error(error.to_string()),
                    );
                    return Err(error);
                }
            };
            if let Err(error) = self
                .start_sandbox_with_authority(
                    StartRequest {
                        activation_auto_lifecycle: start
                            .activation_auto_lifecycle
                            .clone()
                            .map(Into::into),
                        sandbox_id: sandbox_id.clone(),
                        vcpu: start.vcpu,
                        memory_mib: start.memory_mib,
                        checkpoint: Some(SUSPEND_CHECKPOINT.to_string()),
                        blocking: false,
                        netns_path,
                        target_state: req.target_state,
                        suspend_restore_storage_uri: start.suspend_restore_storage_uri.clone(),
                        suspend_restore_source_node: start.suspend_restore_source_node.clone(),
                        route_identity: space_api_route_identity_to_wire(
                            &start.space_api_route_identity,
                        ),
                        operation_activation_attempt_id: start
                            .operation_activation_attempt_id
                            .map(|id| id.to_string()),
                        ..Default::default()
                    },
                    false,
                    IdentityFreeDirectActivationAuthority::Denied,
                )
                .await
            {
                emit_sandbox_event(
                    &sandbox_id,
                    SandboxEventType::WakeFailed,
                    SandboxEventSource::SbxManager,
                    SandboxEventDetails::default()
                        .target_state(target_state.to_string())
                        .reason("stage_from_checkpoint")
                        .error(error.to_string()),
                );
                return Err(error);
            }
        }

        // The cold-path artifact-dir permission repair runs in
        // `resume_after_wake_lock` (the shared resume chokepoint reached by this
        // Wake RPC, implicit activation, and the resume RPC), so it is not issued
        // here.
        let response = match self
            .wake_with_start_config(
                req.sandbox_id.clone(),
                grpc_timeout_budget(&request),
                start,
                req.blocking,
                activity,
            )
            .await
        {
            Ok(response) => response,
            Err(error) => {
                emit_sandbox_event(
                    &sandbox_id,
                    SandboxEventType::WakeFailed,
                    SandboxEventSource::SbxManager,
                    SandboxEventDetails::default()
                        .target_state(target_state.to_string())
                        .error(error.to_string()),
                );
                return Err(error);
            }
        };
        // The wake-completion record is written inside the wake-lock-holding
        // paths (`ensure_active_for_guest_activity_with_start` /
        // `start_wake_transition_without_ready_wait`), not here: by this
        // point the wake lock has been released, and a suspend parked on it
        // would acquire the lock and run its superseded-by-wake check before
        // a record written here became visible.
        emit_sandbox_event(
            &sandbox_id,
            SandboxEventType::WakeCompleted,
            SandboxEventSource::SbxManager,
            SandboxEventDetails::default().target_state(target_state.to_string()),
        );
        if publish_node_state {
            mark_sandbox_dirty(&self.db, &sandbox_id);
        }
        Ok(response)
    }

    pub(super) async fn set_auto_lifecycle_policy(
        &self,
        request: Request<SetAutoLifecyclePolicyRequest>,
    ) -> Result<Response<SetAutoLifecyclePolicyResponse>, Status> {
        let peer = self.authorize_peer(
            &request,
            "SetAutoLifecyclePolicy",
            ALLOW_SANDBOX_AUTO_LIFECYCLE_OPERATOR,
        )?;
        let publish_node_state = !should_skip_node_publish(&request);
        let req = request.get_ref();
        let sandbox_id = req.sandbox_id.clone();
        let requested_authority = crate::db::PolicyAuthority::try_from(req.authority)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        // `authority=local` auto-lifecycle is a node-local, cluster-immune
        // override (the cluster-over-local branch below is a no-op), so a
        // forwarded remote caller must not set it -- only an on-node operator
        // via the CLI. node-api and node-ctl only ever push cluster here.
        require_local_authority_peer(peer.principal(), requested_authority)?;
        let policy = req
            .policy
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("policy is required"))?;
        let policy = policy_from_proto(policy)?;
        let sandbox = self.lookup_sandbox(&req.sandbox_id)?;
        if requested_authority == crate::db::PolicyAuthority::Cluster
            && sandbox.auto_lifecycle_authority == crate::db::PolicyAuthority::Local
        {
            return Ok(Response::new(SetAutoLifecyclePolicyResponse {
                effective_policy: Some(policy_to_proto(&sandbox.auto_lifecycle)),
                authority: sandbox.auto_lifecycle_authority.as_proto_i32(),
            }));
        }
        let effective_policy = self
            .set_auto_lifecycle_policy_exact(&req.sandbox_id, policy, false, requested_authority)
            .await?;
        if publish_node_state {
            mark_sandbox_dirty(&self.db, &sandbox_id);
        }
        Ok(Response::new(SetAutoLifecyclePolicyResponse {
            effective_policy: Some(policy_to_proto(&effective_policy)),
            authority: requested_authority.as_proto_i32(),
        }))
    }

    pub(super) async fn touch_auto_lifecycle(
        &self,
        request: Request<TouchAutoLifecycleRequest>,
    ) -> Result<Response<TouchAutoLifecycleResponse>, Status> {
        self.authorize_peer(
            &request,
            "TouchAutoLifecycle",
            ALLOW_SANDBOX_AUTO_LIFECYCLE_OPERATOR,
        )?;
        let retry_context = pplx_space_common_grpc::GrpcRetryContext::none()
            .with_grpc_timeout_metadata(request.metadata());
        let publish_node_state = !should_skip_node_publish(&request);
        let req = request.get_ref();
        let source = touch_auto_lifecycle_source(req.source)?;
        let observed_at = touch_auto_lifecycle_observed_at_secs(req.observed_at.as_ref())?;

        let sandbox = self.lookup_sandbox(&req.sandbox_id)?;
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox.id);
        let _guard = wake_lock.lock().await;

        let sandbox = self.lookup_sandbox(&sandbox.id)?;
        if retry_context
            .remaining()
            .is_some_and(|remaining| remaining.is_zero())
        {
            return Err(Status::deadline_exceeded(
                "access activity request expired while waiting for lifecycle lock",
            ));
        }
        if let Some(update) = &req.access_idle_update {
            if source != AutoLifecycleActivitySource::Access {
                return Err(Status::invalid_argument(
                    "deadline updates require access source",
                ));
            }
            let expires = update
                .expires_at
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("deadline update requires expires_at"))?;
            let expires = touch_auto_lifecycle_observed_at_secs(Some(expires))?;
            if now_secs() >= expires {
                return Err(Status::deadline_exceeded("access deadline update expired"));
            }
            if sandbox
                .space_api_route_identity
                .execution_generation
                .map(|g| g.get())
                != Some(update.execution_generation)
            {
                return Err(Status::failed_precondition(
                    "access deadline execution changed",
                ));
            }
            if sandbox.state != SandboxState::Started {
                return Err(Status::failed_precondition(
                    "access deadline requires started execution",
                ));
            }
            let mut runtime = sandbox.auto_lifecycle_state.clone();
            let policy = match update
                .operation
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("deadline operation is required"))?
            {
                crate::proto::access_idle_update::Operation::ExtendUntil(until) => {
                    let minimum = touch_auto_lifecycle_observed_at_secs(Some(until))?;
                    apply_observed_access_touch(&mut runtime, &sandbox.auto_lifecycle, observed_at);
                    crate::auto_lifecycle::extend_access_deadline(
                        &mut runtime,
                        &sandbox.auto_lifecycle,
                        minimum,
                    )?;
                    sandbox.auto_lifecycle.clone()
                }
                crate::proto::access_idle_update::Operation::ResetPolicy(policy) => {
                    if sandbox.auto_lifecycle_authority == crate::db::PolicyAuthority::Local {
                        return Err(Status::failed_precondition(
                            "local lifecycle policy overrides cluster timeout",
                        ));
                    }
                    let policy = policy_from_proto(policy)?;
                    runtime = initial_runtime_state(&policy, now_secs());
                    policy
                }
            };
            let changed =
                runtime != sandbox.auto_lifecycle_state || policy != sandbox.auto_lifecycle;
            if changed {
                let db = self.db_conn()?;
                crate::db::update_auto_lifecycle_with_state(
                    &db,
                    &sandbox.id,
                    &policy,
                    &runtime,
                    sandbox.auto_lifecycle_authority,
                )
                .map_err(|e| Status::internal(format!("persist access deadline: {e}")))?;
                drop(db);
                if publish_node_state {
                    mark_sandbox_dirty(&self.db, &sandbox.id);
                }
            }
            let mut response = Response::new(TouchAutoLifecycleResponse {
                result: if changed {
                    crate::proto::AutoLifecycleTouchResult::Applied as i32
                } else {
                    crate::proto::AutoLifecycleTouchResult::AlreadyCurrent as i32
                },
            });
            response.metadata_mut().insert(
                pplx_space_sbx_mgr_client::ACCESS_IDLE_UPDATE_ACK_METADATA,
                tonic::metadata::MetadataValue::from_static("1"),
            );
            return Ok(response);
        }
        let mut runtime_state = sandbox.auto_lifecycle_state.clone();
        let mut touched = false;
        let result = if sandbox.state != SandboxState::Started {
            recompute_runtime_state(&mut runtime_state, &sandbox.auto_lifecycle);
            crate::proto::AutoLifecycleTouchResult::NotStarted
        } else if sandbox.auto_lifecycle.is_disabled() {
            recompute_runtime_state(&mut runtime_state, &sandbox.auto_lifecycle);
            crate::proto::AutoLifecycleTouchResult::Disabled
        } else {
            let initialized_runtime = runtime_state.conditions.is_empty();
            if initialized_runtime {
                let floor = runtime_state.access_extended_until;
                runtime_state = initial_runtime_state(&sandbox.auto_lifecycle, observed_at);
                runtime_state.access_extended_until = floor;
            }
            match source {
                AutoLifecycleActivitySource::Access => {
                    touched = apply_observed_access_touch(
                        &mut runtime_state,
                        &sandbox.auto_lifecycle,
                        observed_at,
                    );
                }
                AutoLifecycleActivitySource::Command => {
                    touched = apply_observed_command_and_access_touch(
                        &mut runtime_state,
                        &sandbox.auto_lifecycle,
                        observed_at,
                    );
                }
            }
            touched |= initialized_runtime && !runtime_state.conditions.is_empty();
            if touched {
                disarm_guest_activity_on_external_access(
                    &mut runtime_state,
                    &sandbox.auto_lifecycle,
                    observed_at,
                );
                clear_error(&mut runtime_state, &sandbox.auto_lifecycle);
                let db = self.db_conn()?;
                crate::db::update_auto_lifecycle_state(&db, &sandbox.id, &runtime_state).map_err(
                    |e| Status::internal(format!("db auto_lifecycle_state update failed: {e}")),
                )?;
                crate::proto::AutoLifecycleTouchResult::Applied
            } else {
                recompute_runtime_state(&mut runtime_state, &sandbox.auto_lifecycle);
                crate::proto::AutoLifecycleTouchResult::AlreadyCurrent
            }
        };

        if touched && publish_node_state {
            mark_sandbox_dirty(&self.db, &sandbox.id);
        }
        Ok(Response::new(TouchAutoLifecycleResponse {
            result: touch_auto_lifecycle_result_value(result),
        }))
    }

    pub(super) async fn get_network_policy(
        &self,
        request: Request<GetNetworkPolicyRequest>,
    ) -> Result<Response<GetNetworkPolicyResponse>, Status> {
        self.authorize_peer(&request, "GetNetworkPolicy", ALLOW_SANDBOX_OBSERVER)?;
        let req = request.get_ref();
        let sandbox_ids = request_sandbox_ids(&req.sandbox_ids);
        let mut response = GetNetworkPolicyResponse::default();

        for sandbox_id in sandbox_ids {
            let sandbox = self.lookup_sandbox(&sandbox_id)?;
            let effective_policy = self.effective_network_policy(&sandbox).await?;
            let detail = NetworkPolicyDetail {
                sandbox_id: sandbox.id.clone(),
                effective_policy: Some(network_policy_to_proto(&effective_policy.policy)),
                inherited: effective_policy.inherited,
                source_template: if effective_policy.inherited {
                    sandbox.template.clone()
                } else {
                    String::new()
                },
                effective_generation: effective_policy.generation,
                applied_generation: sandbox.applied_network_policy_generation,
                applied_source: sandbox.applied_network_policy_source.as_proto(),
                authority: effective_policy.authority.as_proto_i32(),
            };
            response.policies.push(detail);
        }

        Ok(Response::new(response))
    }

    pub(super) async fn set_network_policy(
        &self,
        request: Request<SetNetworkPolicyRequest>,
    ) -> Result<Response<SetNetworkPolicyResponse>, Status> {
        let peer = self.authorize_peer(
            &request,
            "SetNetworkPolicy",
            ALLOW_SANDBOX_NETWORK_POLICY_OPERATOR,
        )?;
        let req = request.get_ref();
        let sandbox = self.lookup_sandbox(&req.sandbox_id)?;
        // Serialize against the pre-wake network-policy gate. The resume path
        // holds this sandbox's wake lock across [gate confirm -> VM thaw] and
        // re-reads the desired policy fresh inside the gate. Taking the same
        // lock here forces a policy update to land either before the gate reads
        // it (the gate then confirms and thaws under the new policy) or after
        // the wake fully completes (a legitimate post-resume update the
        // reconciler converges). It can no longer commit inside the
        // confirm->thaw window and leave a live VM running under a stale gateway
        // policy. Lock order matches the wake path -- wake lock before the brief
        // db lock, and this handler never takes the artifact lock -- so it adds
        // no cycle.
        //
        // Key the lock on the canonical `sandbox.id` resolved by the lookup
        // above, not the raw `req.sandbox_id`: `lookup_sandbox` accepts a UUID
        // prefix or short label, and the resume path keys its wake lock on the
        // canonical id. Locking on the request spelling would take a different
        // map entry for the same sandbox and silently lose the mutual exclusion.
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox.id);
        let _guard = wake_lock.lock().await;
        // Re-read under the lock: an in-flight wake we queued behind may have
        // changed the sandbox state or policy while we waited to acquire it.
        let sandbox = self.lookup_sandbox(&sandbox.id)?;
        // Reject a synchronous refresh for a pool member up front, before any DB
        // mutation. Pool members (pre-adoption) intentionally run with no network
        // identity — the reconciler installs no_identity and the async enqueue
        // guard excludes pool_template-set sandboxes. A synchronous refresh would
        // bypass that and push a real policy to the gateway before adoption.
        // Validating here (rather than after the policy-override write) keeps a
        // rejected request from leaving behind mutated desired-policy state.
        if req.sync_to_gateway && Self::sandbox_is_pool_member(&sandbox) {
            return Err(Status::failed_precondition(
                "synchronous network policy update is not valid for a pool member; adopt the sandbox first",
            ));
        }
        let policy_override =
            network_policy_override_from_set_request(req.inherit_template, req.policy.as_ref())?;
        let publish_node_state = !should_skip_node_publish(&request);
        let requested_authority = crate::db::PolicyAuthority::try_from(req.authority)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        require_local_authority_peer(peer.principal(), requested_authority)?;
        let desired_generation = set_network_policy_desired_generation(
            &sandbox,
            &policy_override,
            req.policy_generation,
        );
        let event_details = || {
            SandboxEventDetails::default()
                .operation(if policy_override.is_some() {
                    "set_override"
                } else {
                    "inherit_template_default"
                })
                .network_policy_source(if policy_override.is_some() {
                    "override"
                } else {
                    "template"
                })
                .network_policy_authority(requested_authority.as_str())
                .network_policy_generation(desired_generation.to_string())
        };
        let outcome = {
            let db = self.db_conn()?;
            if requested_authority == crate::db::PolicyAuthority::Local && policy_override.is_none()
            {
                crate::db::clear_local_network_policy(
                    &db,
                    &sandbox.id,
                    Self::sandbox_has_live_net_gateway(&sandbox),
                    publish_node_state,
                )
            } else {
                crate::db::update_network_policy_override(
                    &db,
                    &sandbox.id,
                    policy_override.as_ref(),
                    desired_generation,
                    requested_authority,
                    Self::sandbox_has_live_net_gateway(&sandbox),
                    publish_node_state,
                )
            }
            .map_err(|e| Status::internal(format!("db network_policy update failed: {e}")))?
        };
        if outcome.decision == crate::db::NetworkPolicyOverrideUpdate::Conflict {
            pplx_space_metrics::reconcile::apply_conflict(FACET_NETWORK_POLICY);
            emit_sandbox_event(
                &sandbox.id,
                SandboxEventType::NetworkPolicyUpdateFailed,
                SandboxEventSource::SbxManager,
                event_details().error("network policy generation conflict"),
            );
            return Err(Status::failed_precondition(
                "network policy generation conflict",
            ));
        }
        match outcome.decision {
            crate::db::NetworkPolicyOverrideUpdate::Apply => {
                pplx_space_metrics::reconcile::apply_applied(FACET_NETWORK_POLICY, "apply");
            }
            crate::db::NetworkPolicyOverrideUpdate::Repair => {
                pplx_space_metrics::reconcile::apply_applied(FACET_NETWORK_POLICY, "repair");
            }
            crate::db::NetworkPolicyOverrideUpdate::Skip => {
                pplx_space_metrics::reconcile::apply_skipped(FACET_NETWORK_POLICY, "idempotent");
            }
            crate::db::NetworkPolicyOverrideUpdate::Conflict => unreachable!("handled above"),
        }

        let sandbox = self.lookup_sandbox(&sandbox.id)?;
        let effective_policy = self.effective_network_policy(&sandbox).await?;
        let mut refreshed_sandbox = None;
        if req.sync_to_gateway {
            // Pool-member rejection happened up front, before the DB mutation.
            if !Self::sandbox_has_live_net_gateway(&sandbox) {
                return Err(Status::unavailable(
                    "synchronous network policy update requires a live net-gateway",
                ));
            }
            let effective_policy_ref = effective_network_policy_ref(&sandbox, &effective_policy)?;
            match tokio::time::timeout(
                NETWORK_POLICY_SYNC_REFRESH_TIMEOUT,
                refresh_network_policy(
                    &self.db,
                    &sandbox.id,
                    Some(&effective_policy_ref),
                    "set-policy-sync",
                ),
            )
            .await
            {
                Ok(Ok(())) => {
                    // Record the applied state, mirroring the periodic reconciler
                    // (refresh + mark_network_policy_applied). Without this the
                    // applied_* columns stay stale: the response would misreport
                    // applied_generation/applied_source, and downstream
                    // idempotency/repair logic (keyed off applied_*) would later
                    // conclude the policy isn't applied and redundantly re-push.
                    self.mark_network_policy_applied(
                        &sandbox.id,
                        effective_policy.inherited,
                        effective_policy.generation,
                    )?;
                    // The durable outbox row written by the DB update (when
                    // queued_live_gateway) is left for the in-memory reconciler to
                    // drain rather than deleted here. Deleting it inline can't be
                    // done race-free: the row is keyed only by sandbox_id (second-
                    // resolution timestamps give no unique token), so a concurrent
                    // SetNetworkPolicy that re-enqueues between our read and delete
                    // would be clobbered. Handing it to the reconciler is safe: the
                    // reconciler reads the *latest* desired policy at drain time
                    // (correctly applying any concurrent newer update), finds the
                    // gateway already matches ours (idempotent), and deletes the row
                    // via its normal post-drain cleanup.
                    if outcome.queued_live_gateway {
                        self.enqueue_network_policy_reconcile(sandbox.id.clone());
                    }
                    refreshed_sandbox = Some(self.lookup_sandbox(&sandbox.id)?);
                }
                Ok(Err(error)) => {
                    // The desired policy is committed but the gateway push
                    // failed. Fall back to the async retry path so a caller that
                    // doesn't tear the sandbox down (or a slow rollback) still
                    // converges, then surface the failure so sync callers can
                    // roll back.
                    if outcome.queued_live_gateway {
                        self.enqueue_network_policy_reconcile(sandbox.id.clone());
                    }
                    return Err(Status::unavailable(format!(
                        "synchronous network policy refresh failed: {error}"
                    )));
                }
                Err(_) => {
                    if outcome.queued_live_gateway {
                        self.enqueue_network_policy_reconcile(sandbox.id.clone());
                    }
                    return Err(Status::unavailable(format!(
                        "synchronous network policy refresh timed out after {}ms",
                        NETWORK_POLICY_SYNC_REFRESH_TIMEOUT.as_millis()
                    )));
                }
            }
        } else if outcome.queued_live_gateway {
            self.enqueue_network_policy_reconcile(sandbox.id.clone());
        }
        let decision = match outcome.decision {
            crate::db::NetworkPolicyOverrideUpdate::Apply => "apply",
            crate::db::NetworkPolicyOverrideUpdate::Repair => "repair",
            crate::db::NetworkPolicyOverrideUpdate::Skip => "skip",
            crate::db::NetworkPolicyOverrideUpdate::Conflict => "conflict",
        };
        if outcome.decision != crate::db::NetworkPolicyOverrideUpdate::Skip {
            emit_sandbox_event(
                &sandbox.id,
                SandboxEventType::NetworkPolicyUpdateRequested,
                SandboxEventSource::SbxManager,
                event_details(),
            );

            let mut details = event_details().result(decision);
            if let Ok(effective_policy_ref) =
                effective_network_policy_ref(&sandbox, &effective_policy)
            {
                details = details.expected_network_policy_ref(effective_policy_ref.to_string());
            }
            emit_sandbox_event(
                &sandbox.id,
                SandboxEventType::NetworkPolicyUpdateCompleted,
                SandboxEventSource::SbxManager,
                details,
            );
        }
        let response_sandbox = refreshed_sandbox.as_ref().unwrap_or(&sandbox);

        #[allow(deprecated)]
        Ok(Response::new(SetNetworkPolicyResponse {
            effective_policy: Some(network_policy_to_proto(&effective_policy.policy)),
            inherited: effective_policy.inherited,
            source_template: if effective_policy.inherited {
                sandbox.template.clone()
            } else {
                String::new()
            },
            effective_generation: effective_policy.generation,
            applied_generation: response_sandbox.applied_network_policy_generation,
            applied_source: response_sandbox.applied_network_policy_source.as_proto(),
            apply_decision: apply_decision_to_proto(match outcome.decision {
                crate::db::NetworkPolicyOverrideUpdate::Apply => ApplyDecisionKind::Apply,
                crate::db::NetworkPolicyOverrideUpdate::Repair => ApplyDecisionKind::Repair,
                crate::db::NetworkPolicyOverrideUpdate::Skip => ApplyDecisionKind::Skip,
                crate::db::NetworkPolicyOverrideUpdate::Conflict => ApplyDecisionKind::Conflict,
            }),
            authority: effective_policy.authority.as_proto_i32(),
            // Only a confirmed live-gateway sync refreshes the sandbox row.
            applied_synchronously: refreshed_sandbox.is_some(),
        }))
    }

    pub(super) async fn template_network_policy_updated(
        &self,
        request: Request<TemplateNetworkPolicyUpdatedRequest>,
    ) -> Result<Response<TemplateNetworkPolicyUpdatedResponse>, Status> {
        self.authorize_peer(
            &request,
            "TemplateNetworkPolicyUpdated",
            ALLOW_SANDBOX_OPERATOR_OR_TMPLMGR,
        )?;
        let req = request.get_ref();
        let template = req.template.trim();
        if template.is_empty() {
            return Err(Status::invalid_argument("template is required"));
        }
        req.policy
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("policy is required"))
            .and_then(network_policy_from_proto)?;
        crate::db::PolicyAuthority::try_from(req.authority)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.template_network_policies.invalidate(template).await;

        let sandboxes = {
            let db = self.db_conn()?;
            crate::db::list_sandboxes(&db)
                .map_err(|error| {
                    Status::internal(format!(
                        "db list failed: {}",
                        crate::db::describe_error(&error)
                    ))
                })?
                .into_iter()
                .filter(|sandbox| {
                    sandbox.template == template
                        && !Self::sandbox_is_pool_member(sandbox)
                        && sandbox.network_policy_override.is_none()
                })
                .collect::<Vec<_>>()
        };

        let mut queued_gateways = 0;
        let mut repair_gateways = 0;
        for sandbox in &sandboxes {
            if !Self::sandbox_has_live_net_gateway(sandbox) {
                continue;
            }
            let queued = {
                let db = self.db_conn()?;
                crate::db::enqueue_network_policy_reconcile_if_live(&db, &sandbox.id).map_err(
                    |e| Status::internal(format!("db enqueue network_policy failed: {e}")),
                )?
            };
            if queued {
                self.enqueue_network_policy_reconcile(sandbox.id.clone());
                queued_gateways += 1;
                if sandbox.applied_network_policy_source
                    == crate::db::NetworkPolicyAppliedSource::Template
                    && sandbox.applied_network_policy_generation == req.generation
                {
                    repair_gateways += 1;
                }
            }
        }

        Ok(Response::new(TemplateNetworkPolicyUpdatedResponse {
            matched_sandboxes: sandboxes.len() as u32,
            notified_gateways: queued_gateways,
            apply_decision: apply_decision_to_proto(if queued_gateways > repair_gateways {
                ApplyDecisionKind::Apply
            } else if repair_gateways > 0 {
                ApplyDecisionKind::Repair
            } else {
                ApplyDecisionKind::Skip
            }),
        }))
    }

    pub(super) async fn prepare_teardown_suspend(
        &self,
        request: Request<PrepareTeardownSuspendRequest>,
    ) -> Result<Response<PrepareTeardownSuspendResponse>, Status> {
        let metric_started_at = std::time::Instant::now();
        self.authorize_peer(&request, "PrepareTeardownSuspend", ALLOW_SANDBOX_OPERATOR)?;
        let event_trigger = teardown_suspend_event_trigger(&request)?;
        let req = request.get_ref();
        let sandbox_id: SandboxId = req
            .sandbox_id
            .parse()
            .map_err(|error| Status::invalid_argument(format!("invalid sandbox_id: {error}")))?;
        // Runtime attachment establishes this node-local lease from the route
        // identity already passed down with the execution.
        let execution_generation =
            pplx_space_common_core::types::ExecutionGeneration::new(req.execution_generation)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let span = tracing::Span::current();
        span.record("sandbox_id", tracing::field::display(sandbox_id));
        span.record("execution_generation", execution_generation.get());
        if let Some(node_id) = self.local_node_id {
            span.record("current_node_id", tracing::field::display(node_id));
        }
        let action = TeardownSuspendAction::try_from(req.action)
            .ok()
            .filter(|action| *action != TeardownSuspendAction::Unspecified)
            .ok_or_else(|| Status::invalid_argument("teardown suspend action is required"))?;

        let sandbox_id_string = sandbox_id.to_string();
        let evidence = {
            let db = self.db_conn()?;
            crate::db::teardown_release_evidence(&db, &sandbox_id_string, execution_generation)
                .map_err(|error| Status::internal(format!("db error: {error}")))?
        };
        let Some(mut status) = evidence.suspend else {
            if evidence.deleted {
                return Ok(teardown_suspend_prepare_response(
                    TeardownSuspendOutcome::Deleted,
                    Some("a generation-fenced local delete covers this execution".to_string()),
                    pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Deleted,
                    metric_started_at,
                ));
            }
            return Ok(teardown_suspend_prepare_response(
                TeardownSuspendOutcome::Pending,
                Some("sandbox record is not present on this node".to_string()),
                pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Pending,
                metric_started_at,
            ));
        };

        if evidence.activation_rolled_back {
            return Ok(teardown_suspend_prepare_response(
                TeardownSuspendOutcome::Superseded,
                Some("a generation-fenced activation rollback retired this execution".to_string()),
                pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                metric_started_at,
            ));
        }

        let local_generation = match evidence.runtime_generation {
            Some(generation) => generation,
            None if status.state == SandboxState::Suspended => execution_generation,
            None => {
                return Ok(teardown_suspend_prepare_response(
                    TeardownSuspendOutcome::Pending,
                    Some("local runtime execution generation is not available".to_string()),
                    pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Pending,
                    metric_started_at,
                ));
            }
        };
        if local_generation != execution_generation {
            let (outcome, reason, metric_outcome) = if local_generation > execution_generation {
                (
                    TeardownSuspendOutcome::Superseded,
                    "a newer local execution owns generation suspend",
                    pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                )
            } else {
                (
                    TeardownSuspendOutcome::Pending,
                    "the requested execution does not own the local runtime",
                    pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Pending,
                )
            };
            return Ok(teardown_suspend_prepare_response(
                outcome,
                Some(reason.to_string()),
                metric_outcome,
                metric_started_at,
            ));
        }
        if status
            .operation
            .as_ref()
            .is_some_and(|operation| operation.execution_generation > execution_generation)
        {
            return Ok(teardown_suspend_prepare_response(
                TeardownSuspendOutcome::Superseded,
                Some("a newer local execution owns generation suspend".to_string()),
                pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                metric_started_at,
            ));
        }
        if action == TeardownSuspendAction::BeginOrJoin
            && matches!(status.state, SandboxState::Started | SandboxState::Paused)
        {
            let db = self.db_conn()?;
            match crate::db::begin_teardown_suspend_operation(
                &db,
                &sandbox_id.to_string(),
                execution_generation,
            ) {
                Ok(_) => {}
                Err(rusqlite::Error::InvalidQuery) => {
                    return Ok(teardown_suspend_prepare_response(
                        TeardownSuspendOutcome::Superseded,
                        Some("a newer local execution owns generation suspend".to_string()),
                        pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                        metric_started_at,
                    ));
                }
                Err(error) => return Err(Status::internal(format!("db error: {error}"))),
            }
            status = crate::db::teardown_suspend_status(&db, &sandbox_id.to_string())
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .ok_or_else(|| Status::not_found("sandbox record no longer exists"))?;
        }
        let has_exact_operation = status.operation.as_ref().is_some_and(|operation| {
            operation.execution_generation == execution_generation
                && operation.status.can_support_release_proof()
        });
        let can_bind_execution = status
            .operation
            .as_ref()
            .is_none_or(|operation| operation.execution_generation <= execution_generation);
        let mut bound_existing_suspend = false;
        if action == TeardownSuspendAction::BeginOrJoin
            && status.state == SandboxState::Suspended
            && !has_exact_operation
            && can_bind_execution
        {
            let db = self.db_conn()?;
            match crate::db::bind_suspended_teardown_operation(
                &db,
                &sandbox_id.to_string(),
                execution_generation,
            ) {
                Ok(_) => {}
                Err(rusqlite::Error::InvalidQuery) => {
                    return Ok(teardown_suspend_prepare_response(
                        TeardownSuspendOutcome::Superseded,
                        Some("a newer local execution owns generation suspend".to_string()),
                        pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                        metric_started_at,
                    ));
                }
                Err(error) => return Err(Status::internal(format!("db error: {error}"))),
            }
            status = crate::db::teardown_suspend_status(&db, &sandbox_id.to_string())
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .ok_or_else(|| Status::not_found("sandbox record no longer exists"))?;
            bound_existing_suspend = true;
        }
        if bound_existing_suspend {
            emit_sandbox_event(
                &sandbox_id.to_string(),
                SandboxEventType::SuspendRequested,
                SandboxEventSource::SbxManager,
                teardown_suspend_event_details(execution_generation, event_trigger)
                    .with_from_state(SandboxState::Suspended.to_string()),
            );
        }
        if status.is_durable_for(execution_generation) {
            return durable_teardown_suspend_prepare_response(&status, metric_started_at);
        }
        if status.is_ready_for(execution_generation) {
            self.complete_teardown_suspend_operation(
                &sandbox_id.to_string(),
                execution_generation,
                event_trigger,
            )?;
            status = {
                let db = self.db_conn()?;
                crate::db::teardown_suspend_status(&db, &sandbox_id_string)
                    .map_err(|error| Status::internal(format!("db error: {error}")))?
                    .ok_or_else(|| Status::not_found("sandbox record no longer exists"))?
            };
            if status.is_durable_for(execution_generation) {
                return durable_teardown_suspend_prepare_response(&status, metric_started_at);
            }
            return Err(Status::internal(
                "generation suspend completion did not bind its durable artifact",
            ));
        }
        let state = status.state;
        let resume_persisted = status.operation.as_ref().is_some_and(|operation| {
            operation.status == crate::db::TeardownSuspendOperationStatus::Running
                && operation.execution_generation == execution_generation
        });
        let begin_new = action == TeardownSuspendAction::BeginOrJoin
            && matches!(state, SandboxState::Started | SandboxState::Paused);
        if begin_new || resume_persisted {
            self.spawn_teardown_suspend_driver(sandbox_id, execution_generation, event_trigger);
            return Ok(teardown_suspend_prepare_response(
                TeardownSuspendOutcome::Pending,
                None,
                pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Pending,
                metric_started_at,
            ));
        }
        Ok(teardown_suspend_prepare_response(
            TeardownSuspendOutcome::Pending,
            Some(format!(
                "sandbox is {} without a durable teardown artifact",
                state
            )),
            pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Pending,
            metric_started_at,
        ))
    }

    pub(super) fn spawn_teardown_suspend_driver(
        &self,
        sandbox_id: SandboxId,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        event_trigger: SandboxEventTrigger,
    ) -> TeardownSuspendDriverAdmission {
        let dashmap::mapref::entry::Entry::Vacant(entry) =
            self.teardown_suspend_drivers.entry(sandbox_id)
        else {
            return TeardownSuspendDriverAdmission::Existing;
        };
        let permit = match Arc::clone(&self.teardown_suspend_driver_sem).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                pplx_space_metrics::sandbox::worker::teardown_suspend_driver_saturated();
                tracing::debug!(
                    %sandbox_id,
                        "generation suspend driver capacity is full; caller polling will retry admission"
                );
                return TeardownSuspendDriverAdmission::Saturated;
            }
        };
        entry.insert(permit);

        let driver_slot = TeardownSuspendDriverSlot {
            drivers: Arc::clone(&self.teardown_suspend_drivers),
            sandbox_id,
        };
        let service = self.clone();
        let shutdown_token = self.shutdown_token.clone();
        self.task_tracker.spawn(async move {
            let _driver_slot = driver_slot;
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    tracing::info!(
                        %sandbox_id,
                        "stopped generation suspend driver during service shutdown"
                    );
                }
                result = service.drive_teardown_suspend(
                    sandbox_id,
                    execution_generation,
                    event_trigger,
                ) => {
                    if let Err(error) = result {
                        tracing::warn!(
                            %sandbox_id,
                            code = %error.code(),
                            error = %error.message(),
                            "generation suspend did not become durable"
                        );
                    }
                }
            }
        });
        TeardownSuspendDriverAdmission::Started
    }

    async fn drive_teardown_suspend(
        &self,
        sandbox_id: SandboxId,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        event_trigger: SandboxEventTrigger,
    ) -> Result<(), Status> {
        let _state_writer = self.state_writer_lease("PrepareTeardownSuspendBackground")?;
        let sandbox_id_string = sandbox_id.to_string();
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox_id_string);
        let wake_guard = wake_lock.lock_owned().await;
        let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox_id_string);
        let artifact_guard = artifact_lock.lock_owned().await;
        let status = {
            let db = self.db_conn()?;
            crate::db::teardown_suspend_status(&db, &sandbox_id_string)
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .ok_or_else(|| Status::not_found("sandbox record no longer exists"))?
        };
        if status.is_durable_for(execution_generation) {
            return Ok(());
        }
        if status.is_ready_for(execution_generation) {
            self.complete_teardown_suspend_operation(
                &sandbox_id_string,
                execution_generation,
                event_trigger,
            )?;
            return Ok(());
        }
        self.ensure_teardown_suspend_owner(&sandbox_id_string, execution_generation)?;
        let active_generation = Some(execution_generation);
        let result: Result<(), Status> = match (status.state, status.artifact) {
            (SandboxState::Started | SandboxState::Paused, _) => {
                let context = SuspendRequestContext::for_teardown(
                    sandbox_id_string.clone(),
                    execution_generation,
                );
                let (_, publication) = self
                    .prepare_suspend_snapshot_publication_after_locks(
                        &context,
                        &wake_guard,
                        &artifact_guard,
                    )
                    .await?;
                self.complete_suspend_snapshot_publication(publication)
                    .await
            }
            (
                SandboxState::Suspending
                | SandboxState::CleanupRequired
                | SandboxState::CleanupFailed,
                Some(artifact),
            ) if active_generation == Some(execution_generation) => {
                let sandbox = self.lookup_sandbox(&sandbox_id_string)?;
                let format = reupload_format_from_sidecar(&artifact.local_path)?;
                self.ensure_teardown_suspend_backup(
                    &sandbox,
                    TeardownSuspendBackup {
                        execution_generation,
                        artifact: &artifact,
                        format,
                    },
                )
                .await?;
                self.ensure_teardown_suspend_owner(&sandbox_id_string, execution_generation)?;
                self.finalize_suspended_runtime(
                    &sandbox,
                    artifact.local_artifact_id,
                    &artifact.local_path,
                    None,
                    None,
                )
                .await?;
                mark_sandbox_dirty(&self.db, &sandbox_id_string);
                Ok(())
            }
            (SandboxState::Suspended, Some(artifact))
                if active_generation == Some(execution_generation) =>
            {
                let sandbox = self.lookup_sandbox(&sandbox_id_string)?;
                let format = reupload_format_from_sidecar(&artifact.local_path)?;
                self.ensure_teardown_suspend_backup(
                    &sandbox,
                    TeardownSuspendBackup {
                        execution_generation,
                        artifact: &artifact,
                        format,
                    },
                )
                .await
            }
            (state, _) => Err(Status::failed_precondition(format!(
                "cannot continue teardown suspend from {}",
                state
            ))),
        };
        result?;
        self.complete_teardown_suspend_operation(
            &sandbox_id_string,
            execution_generation,
            event_trigger,
        )?;
        Ok(())
    }

    fn ensure_teardown_suspend_owner(
        &self,
        sandbox_id: &str,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
    ) -> Result<(), Status> {
        let status = {
            let db = self
                .db
                .lock()
                .map_err(|_| Status::internal("database mutex poisoned"))?;
            crate::db::teardown_suspend_status(&db, sandbox_id)
                .map_err(|error| Status::internal(format!("db error: {error}")))?
        };
        let owns_runtime = status
            .and_then(|status| status.operation)
            .is_some_and(|operation| {
                operation.execution_generation == execution_generation
                    && operation.status == crate::db::TeardownSuspendOperationStatus::Running
            });
        if owns_runtime {
            Ok(())
        } else {
            Err(Status::aborted(
                "execution generation no longer owns sandbox",
            ))
        }
    }

    fn ensure_teardown_suspend_owner_for_durability(
        &self,
        sandbox_id: &str,
        durability: SuspendDurability,
    ) -> Result<(), Status> {
        if let Some(execution_generation) = durability.execution_generation() {
            self.ensure_teardown_suspend_owner(sandbox_id, execution_generation)?;
        }
        Ok(())
    }

    fn complete_teardown_suspend_operation(
        &self,
        sandbox_id: &str,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        event_trigger: SandboxEventTrigger,
    ) -> Result<(), Status> {
        let completed = {
            let db = self.db_conn()?;
            let status = crate::db::teardown_suspend_status(&db, sandbox_id)
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .ok_or_else(|| Status::not_found("sandbox record no longer exists"))?;
            let is_ready = status.is_ready_for(execution_generation);
            let artifact_id = status
                .artifact
                .filter(|_| is_ready)
                .map(|artifact| artifact.local_artifact_id)
                .ok_or_else(|| {
                    Status::failed_precondition(
                        "generation suspend artifact is not remotely durable",
                    )
                })?;
            let completed = crate::db::complete_teardown_suspend(
                &db,
                sandbox_id,
                execution_generation,
                Some(artifact_id),
            )
            .map_err(|error| Status::internal(format!("db error: {error}")))?;
            if !completed {
                crate::db::bind_completed_teardown_suspend_artifact(
                    &db,
                    sandbox_id,
                    execution_generation,
                    artifact_id,
                )
                .map_err(|error| Status::internal(format!("db error: {error}")))?;
            }
            completed
        };
        if completed {
            emit_sandbox_event(
                sandbox_id,
                SandboxEventType::SuspendCompleted,
                SandboxEventSource::SbxManager,
                teardown_suspend_event_details(execution_generation, event_trigger)
                    .result("durable")
                    .with_to_state(SandboxState::Suspended.to_string()),
            );
        }
        Ok(())
    }

    pub(super) async fn suspend(
        &self,
        request: Request<SuspendRequest>,
    ) -> Result<Response<SuspendResponse>, Status> {
        let arrived_at = std::time::Instant::now();
        self.authorize_peer(&request, "Suspend", ALLOW_SANDBOX_OPERATOR)?;
        let publish_node_state = !should_skip_node_publish(&request);
        let req = request.get_ref();
        crate::proto::SuspendSnapshotFormat::try_from(req.format).map_err(|_| {
            Status::invalid_argument(format!("unknown suspend format value {}", req.format))
        })?;
        let mut context = SuspendRequestContext::new(
            req.sandbox_id.clone(),
            req.blocking,
            req.format,
            publish_node_state,
            sandbox_event_trigger(&request),
        );

        let sandbox = self.lookup_sandbox(&context.sandbox_id)?;
        self.ensure_teardown_suspend_owner_for_durability(&sandbox.id, context.durability)?;
        self.ensure_not_pool_sandbox_for_snapshots(&sandbox, "suspend")?;
        self.ensure_no_plaintext_pool_cow_for_snapshots(&sandbox, "suspend")?;
        self.ensure_snapshot_restore_supported("suspend")?;
        context.sandbox_id = sandbox.id.clone();
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox.id);
        let wake_guard = wake_lock.lock_owned().await;
        // Suspend-vs-wake arbitration: if a wake completed after this request
        // arrived, the request queued behind that wake on the wake lock — the
        // caller decided to suspend a sandbox that has since been woken
        // (typically by a data-plane access restoring it). Executing it would
        // tear the sandbox down underneath the waker, which in the wild
        // oscillates the sandbox through suspend/restore cycles (each paying
        // a full suspend snapshot) while the data plane 502s. Reject; a
        // caller that still wants the sandbox suspended re-issues against the
        // now-running sandbox and wins cleanly.
        if suspend_wake_arbitration_enabled()
            && suspend_superseded_by_wake(
                arrived_at,
                self.auto_lifecycle.last_wake_completed_at(&sandbox.id),
            )
        {
            let mut details =
                SandboxEventDetails::default().with_from_state(sandbox.state.to_string());
            if let Some(trigger) = context.event_trigger {
                details = details.trigger(trigger);
            }
            if let Some(after_wake) = classify_suspend_after_wake(
                self.auto_lifecycle.since_last_wake_completed(&sandbox.id),
                SUSPEND_AFTER_WAKE_WARN_THRESHOLD,
            ) {
                details = details.ms_since_wake(after_wake.ms_since_wake.to_string());
            }
            emit_suspend_failed_event(
                &sandbox.id,
                &details,
                "arbitration",
                "superseded_by_concurrent_wake",
                "suspend queued behind a concurrent wake",
            );
            tracing::warn!(
                sandbox_id = %sandbox.id,
                "rejecting suspend that queued behind a concurrent wake"
            );
            return Err(Status::aborted(
                "suspend superseded by a concurrent wake: the sandbox was woken while this \
                 suspend request waited on the wake lock; re-issue the suspend if the sandbox \
                 should still be suspended",
            ));
        }
        let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox.id);
        let artifact_guard = artifact_lock.lock_owned().await;
        // A blocking suspend commits the full artifact before replying. If the
        // caller loses the response or its control-plane marker write fails,
        // retrying must return that exact committed artifact instead of trying
        // to suspend an already-suspended VM again.
        let sandbox = self.lookup_sandbox(&context.sandbox_id)?;
        if let Some(response) =
            completed_suspend_retry_response(&sandbox, context.blocking, context.requested_format)?
        {
            return Ok(Response::new(response));
        }
        let (blocking_local_artifact_id, publication) = self
            .prepare_suspend_snapshot_publication_after_locks(
                &context,
                &wake_guard,
                &artifact_guard,
            )
            .await?;
        if !context.blocking {
            let service = self.clone();
            let background_sandbox_id = publication.sandbox.id.clone();
            self.spawn_state_writer("SuspendPublication", async move {
                let _wake_guard = wake_guard;
                let _artifact_guard = artifact_guard;
                if let Err(error) = service
                    .complete_suspend_snapshot_publication(publication)
                    .await
                {
                    tracing::warn!(
                        sandbox_id = %background_sandbox_id,
                        code = %error.code(),
                        error = %error.message(),
                        "asynchronous suspend publication failed"
                    );
                }
            })?;
            return Ok(Response::new(SuspendResponse {
                published_artifact: None,
            }));
        }

        self.complete_suspend_snapshot_publication(publication)
            .await?;
        drop(artifact_guard);
        drop(wake_guard);

        Ok(Response::new(SuspendResponse {
            published_artifact: Some(crate::proto::PublishedArtifact {
                local_artifact_id: blocking_local_artifact_id,
                artifact_kind: crate::proto::ArtifactKind::Full as i32,
                origin: crate::proto::ArtifactOrigin::Suspend as i32,
                snapshot_name: None,
            }),
        }))
    }

    pub(super) async fn prepare_suspend_snapshot_publication_after_locks(
        &self,
        context: &SuspendRequestContext,
        _wake_guard: &crate::auto_lifecycle::LifecycleMutexGuard,
        _artifact_guard: &crate::auto_lifecycle::LifecycleMutexGuard,
    ) -> Result<(String, SuspendSnapshotPublicationContext), Status> {
        self.ensure_snapshot_restore_supported("suspend")?;
        // Cleanup takes its fence before any artifact lock. Never wait for it
        // while holding the wake/artifact locks: an admitted delete must win,
        // and retrying avoids an inverted lock-order cycle.
        let CleanupLockTryResult::Acquired(cleanup_fence) =
            self.try_cleanup_fence(&context.sandbox_id)
        else {
            return Err(Status::unavailable(
                "sandbox cleanup is active; retry suspend",
            ));
        };
        let sandbox = self.lookup_sandbox(&context.sandbox_id)?;
        self.ensure_not_pool_sandbox_for_snapshots(&sandbox, "suspend")?;
        self.ensure_no_plaintext_pool_cow_for_snapshots(&sandbox, "suspend")?;
        let local_artifact_id = LocalArtifactId::new();
        // Stamp encryption on every event this suspend emits: it selects the
        // suspend and restore code path and is immutable, so a timeline that
        // omits it cannot say which path ran.
        let mut suspend_event_details = SandboxEventDetails::default()
            .with_from_state(sandbox.state.to_string())
            .bool_field(
                SandboxEventBoolField::VolumeEncryption,
                sandbox.volume_encryption,
            );
        if let Some(trigger) = context.event_trigger {
            suspend_event_details = suspend_event_details.trigger(trigger);
        }
        if context.durability.is_remote_before_teardown() {
            suspend_event_details =
                suspend_event_details.operation(crate::db::PLANNED_TEARDOWN_SUSPEND_OPERATION);
        }
        if let Some(execution_generation) = context.durability.execution_generation() {
            suspend_event_details =
                suspend_event_details.execution_generation(execution_generation.to_string());
        }
        // Suspend-after-wake oscillation signal: a suspend landing moments
        // after a completed wake usually means an external caller is
        // fighting wake-on-access, re-suspending the sandbox over and over
        // and paying a full suspend snapshot per cycle. Signal only — never
        // delays or rejects the suspend.
        if let Some(after_wake) = classify_suspend_after_wake(
            self.auto_lifecycle.since_last_wake_completed(&sandbox.id),
            SUSPEND_AFTER_WAKE_WARN_THRESHOLD,
        ) {
            suspend_event_details =
                suspend_event_details.ms_since_wake(after_wake.ms_since_wake.to_string());
            if after_wake.within_warn_threshold {
                tracing::warn!(
                    sandbox_id = %sandbox.id,
                    ms_since_wake = after_wake.ms_since_wake,
                    "suspend requested shortly after a completed wake; \
                     possible wake/suspend oscillation"
                );
            }
        }
        emit_sandbox_event(
            &sandbox.id,
            SandboxEventType::SuspendRequested,
            SandboxEventSource::SbxManager,
            suspend_event_details.clone(),
        );

        let create_kind = "suspend";
        let create_started_at = std::time::Instant::now();
        tracing::info!(
            sandbox_id = %sandbox.id,
            blocking = context.blocking,
            "snapshot create started"
        );
        pplx_space_metrics::snapshot::create_started(
            create_kind,
            context.create_codec,
            sandbox.volume_encryption,
        );

        // Auto-pause if started
        if sandbox.state == SandboxState::Started {
            let pause_started_at = std::time::Instant::now();
            let mut node_client = connect_node().await.map_err(|e| {
                pplx_space_metrics::snapshot::create_failed(
                    create_kind,
                    context.create_codec,
                    sandbox.volume_encryption,
                    "pause",
                    "node_mgr_unreachable",
                    create_started_at,
                );
                let error = format!("cannot connect to node-manager: {e}");
                emit_suspend_failed_event(
                    &sandbox.id,
                    &suspend_event_details,
                    "pause",
                    "node_mgr_unreachable",
                    error.clone(),
                );
                Status::internal(error)
            })?;
            self.ensure_teardown_suspend_owner_for_durability(&sandbox.id, context.durability)?;
            {
                let db = self.db_conn()?;
                require_state_write(
                    crate::db::update_state(&db, &sandbox.id, SandboxState::Pausing),
                    "begin checkpoint pause",
                )?;
            }
            self.watchdog.record_lifecycle_edge(&sandbox.id).await;
            let guest_preparation_budget = context.guest_preparation_budget();
            if let Err(e) =
                prepare_guest_for_vm_snapshot_via_spaced_host(&sandbox.id, guest_preparation_budget)
                    .await
            {
                let db = self.db_conn()?;
                if let Err(error) = require_state_write(
                    crate::db::update_state(&db, &sandbox.id, SandboxState::Started),
                    "roll back checkpoint pause preparation",
                ) {
                    tracing::warn!(sandbox_id = %sandbox.id, %error, "failed to roll back checkpoint pause state");
                }
                pplx_space_metrics::snapshot::create_failed(
                    create_kind,
                    context.create_codec,
                    sandbox.volume_encryption,
                    "pause",
                    "guest_fs_flush_failed",
                    create_started_at,
                );
                emit_suspend_failed_event(
                    &sandbox.id,
                    &suspend_event_details,
                    "guest_filesystem_flush",
                    "guest_fs_flush_failed",
                    format!("guest filesystem flush before suspend failed: {e}"),
                );
                return Err(Status::internal(format!(
                    "guest snapshot preparation before suspend failed: {e}"
                )));
            }
            let sandbox_id = sandbox
                .id
                .parse::<SandboxId>()
                .map_err(|error| Status::internal(format!("invalid sandbox ID: {error}")))?;
            let pause_state = pause_with_reconciliation(
                &mut node_client,
                sandbox.node_execution_fence(),
                &self.db,
                &sandbox_id,
                "auto-pause failed",
                guest_preparation_budget,
            )
            .await;
            if pause_state != SandboxState::Paused {
                self.watchdog.record_lifecycle_edge(&sandbox.id).await;
                let reason = format!("auto-pause failed; sandbox reconciled to {pause_state}");
                pplx_space_metrics::snapshot::create_failed(
                    create_kind,
                    context.create_codec,
                    sandbox.volume_encryption,
                    "pause",
                    "fc_pause_failed",
                    create_started_at,
                );
                emit_suspend_failed_event(
                    &sandbox.id,
                    &suspend_event_details,
                    "pause",
                    "fc_pause_failed",
                    reason.clone(),
                );
                return Err(Status::internal(reason));
            }
            {
                let db = self.db_conn()?;
                require_state_write(
                    crate::db::update_state(&db, &sandbox.id, SandboxState::Paused),
                    "complete checkpoint pause",
                )?;
            }
            pplx_space_metrics::snapshot::create_step(
                create_kind,
                context.create_codec,
                sandbox.volume_encryption,
                "pause",
                pause_started_at,
            );
        } else if sandbox.state != SandboxState::Paused {
            pplx_space_metrics::snapshot::create_failed(
                create_kind,
                context.create_codec,
                sandbox.volume_encryption,
                "pause",
                "invalid_state",
                create_started_at,
            );
            // For the `failed` state the cluster-controller's node-drain
            // evacuation classifies this rejection on
            // `SUSPEND_SANDBOX_FAILED_MARKER` to give up and flip the DB row to
            // `failed` (so the node can finish draining) instead of retrying
            // suspend forever, so the failed branch must render that exact
            // phrase. Reference the shared constant rather than re-typing the
            // literal so a reword stays in lockstep with the consumer.
            let error = if sandbox.state == SandboxState::Failed {
                format!(
                    "cannot suspend: {}",
                    pplx_space_common_core::node_drain_signals::SUSPEND_SANDBOX_FAILED_MARKER
                )
            } else {
                format!(
                    "cannot suspend: sandbox is {}, must be started or paused",
                    sandbox.state
                )
            };
            emit_suspend_failed_event(
                &sandbox.id,
                &suspend_event_details,
                "pause",
                "invalid_state",
                error.clone(),
            );
            return Err(Status::failed_precondition(error));
        }

        // Now paused — create snapshot
        self.ensure_teardown_suspend_owner_for_durability(&sandbox.id, context.durability)?;
        {
            let db = self.db_conn()?;
            match context.durability {
                SuspendDurability::NodeLocal => {
                    let applied = crate::db::begin_suspend(&db, &sandbox.id)
                        .map_err(|e| Status::internal(format!("db error: {e}")))?;
                    if !applied {
                        return Err(Status::aborted("suspend admission superseded"));
                    }
                    Ok(())
                }
                SuspendDurability::RemoteBeforeTeardown {
                    execution_generation,
                } => crate::db::begin_teardown_suspend(&db, &sandbox.id, execution_generation)
                    .map(|_| ()),
            }
            .map_err(|e| Status::internal(format!("db error: {e}")))?;
        }

        let blocking_local_artifact_id = local_artifact_id.to_string();
        let publication = SuspendSnapshotPublicationContext {
            cleanup_fence,
            sandbox,
            requested_format: context.requested_format,
            suspend_format: context.suspend_format,
            create_codec: context.create_codec,
            local_artifact_id,
            event_details: suspend_event_details,
            started_at: create_started_at,
            publish_node_state: context.publish_node_state,
            durability: context.durability,
        };
        Ok((blocking_local_artifact_id, publication))
    }

    pub(super) async fn complete_suspend_snapshot_publication(
        &self,
        publication: SuspendSnapshotPublicationContext,
    ) -> Result<(), Status> {
        let SuspendSnapshotPublicationContext {
            cleanup_fence,
            sandbox,
            requested_format,
            suspend_format: mut published_format,
            mut create_codec,
            local_artifact_id,
            event_details: suspend_event_details,
            started_at: create_started_at,
            publish_node_state,
            durability,
        } = publication;
        let mut cleanup_fence = Some(cleanup_fence);
        // Backstop: if this publication is abandoned before its intentional
        // teardown while the runtime is still live — a failure that rolls back
        // to paused, or a dropped handler future mid-capture — re-arm the
        // watchdog on drop so the paused-but-live sandbox's sidecars keep being
        // monitored. Disarmed just before the deliberate monitoring removal on
        // the success path. Harmless no-op on paths that never unsubscribed.
        let mut monitoring_guard = SuspendMonitoringGuard::new(self, &sandbox);
        let create_kind = "suspend";
        let mut node_client = connect_node().await.map_err(|e| {
            pplx_space_metrics::snapshot::create_failed(
                create_kind,
                create_codec,
                sandbox.volume_encryption,
                "sync_guest",
                "node_mgr_unreachable",
                create_started_at,
            );
            let error = format!("cannot connect to node-manager: {e}");
            self.record_suspend_snapshot_publication_failure_after_begin(&sandbox.id, &error);
            emit_suspend_failed_event(
                &sandbox.id,
                &suspend_event_details,
                "sync_guest",
                "node_mgr_unreachable",
                error.clone(),
            );
            Status::internal(error)
        })?;

        let sync_started_at = std::time::Instant::now();
        if let Err(e) = node_client
            .sync_guest(SyncGuestRequest {
                sandbox_id: sandbox.id.clone(),
            })
            .await
        {
            let reason = format!("host rootfs sync failed: {e}");
            self.record_suspend_snapshot_publication_failure_after_begin(&sandbox.id, &reason);
            pplx_space_metrics::snapshot::create_failed(
                create_kind,
                create_codec,
                sandbox.volume_encryption,
                "sync_guest",
                "host_rootfs_sync_failed",
                create_started_at,
            );
            emit_suspend_failed_event(
                &sandbox.id,
                &suspend_event_details,
                "sync_guest",
                "host_rootfs_sync_failed",
                reason.clone(),
            );
            return Err(Status::internal(reason));
        }
        pplx_space_metrics::snapshot::create_step(
            create_kind,
            create_codec,
            sandbox.volume_encryption,
            "sync_guest",
            sync_started_at,
        );

        let sandbox_dir = std::path::PathBuf::from(format!("{CACHE_DIR}/{}", sandbox.id));
        let snapshot_dir = canonical_suspend_snapshot_dir(&sandbox_dir);
        let staging_parent = suspend_snapshot_staging_parent_dir(&sandbox_dir);
        let staging_dir = suspend_snapshot_staging_dir(&sandbox_dir, local_artifact_id);
        let record_artifact_stage_failure = |reason: &str| {
            pplx_space_metrics::snapshot::create_failed(
                create_kind,
                create_codec,
                sandbox.volume_encryption,
                "artifact_stage",
                "artifact_stage_failed",
                create_started_at,
            );
            emit_suspend_failed_event(
                &sandbox.id,
                &suspend_event_details,
                "artifact_stage",
                "artifact_stage_failed",
                reason.to_owned(),
            );
        };
        if let Err(e) = node_client
            .prepare_sandbox_artifact_dir(PrepareSandboxArtifactDirRequest {
                sandbox_id: sandbox.id.clone(),
            })
            .await
        {
            let reason = format!("prepare local artifact directory failed: {e}");
            self.record_suspend_snapshot_publication_failure_after_begin(&sandbox.id, &reason);
            record_artifact_stage_failure(&reason);
            return Err(Status::internal(reason));
        }
        std::fs::create_dir_all(&staging_parent).map_err(|err| {
            let reason = format!(
                "create local artifact staging parent {} failed: {err}",
                staging_parent.display()
            );
            self.record_suspend_snapshot_publication_failure_after_begin(&sandbox.id, &reason);
            record_artifact_stage_failure(&reason);
            Status::internal(reason)
        })?;
        std::fs::create_dir(&staging_dir).map_err(|err| {
            let reason = format!(
                "create local artifact staging directory {} failed: {err}",
                staging_dir.display()
            );
            self.record_suspend_snapshot_publication_failure_after_begin(&sandbox.id, &reason);
            record_artifact_stage_failure(&reason);
            Status::internal(reason)
        })?;
        let mut staging_guard = LocalArtifactStagingGuard::new(staging_dir.clone());
        let result = async {
            let staging_dir_string = staging_dir.to_string_lossy().into_owned();

            // BYOK: bring up the encrypted snapshot volume so the jailed Firecracker
            // writes vmstate+memory straight into it (compressed+encrypted at t0).
            // Keyed off the sandbox's immutable volume_encryption flag; `None` for
            // plaintext sandboxes — the plaintext producer below is then
            // byte-identical to before. A failure here is fail-closed.
            let enc_snapshot = if sandbox.volume_encryption {
                // A restored VM's encrypted memory file is the base into which
                // Firecracker 1.15 writes the next Diff snapshot. Never format
                // an empty replacement when that base is expected: a missing
                // mount/backing must fail this suspend instead of publishing a
                // sparse diff as though it were standalone.
                let fresh = crate::snapshot_encryption::capture_volume_is_fresh(&sandbox);
                match crate::snapshot_encryption::prepare_encrypted_snapshot_volume(
                    &sandbox.id,
                    sandbox.node_execution_fence(),
                    fresh,
                    None,
                )
                .await
                {
                    Ok(value) => Some(value),
                    Err(error) => {
                        let status = encrypted_snapshot_prepare_error_status(error);
                        let reason = status.message().to_string();
                        self.record_suspend_snapshot_publication_failure_after_begin(
                            &sandbox.id,
                            &reason,
                        );
                        record_artifact_stage_failure(&reason);
                        return Err(status);
                    }
                }
            } else {
                None
            };
            let encrypted = enc_snapshot.is_some();
            let enc_snapshot_mount_dir = enc_snapshot.as_ref().map(|e| e.mount_dir.clone());

            // The encrypted capture stops the runtime inside create_snapshot to
            // release Firecracker's mmap pin on the volume before it is sealed.
            // Stop watching first so the watchdog does not race that teardown with
            // sidecar-restart attempts. Idempotent: the removes near the end of
            // suspend are a no-op once already removed. Plaintext keeps its
            // existing ordering (removed only there). `monitoring_guard` re-arms
            // the watchdog on any live rollback (or dropped future) after this
            // point, so the unsubscribe here can never strand a live sandbox.
            if encrypted {
                self.watchdog.remove_sandbox(&sandbox.id).await;
                self.snapshot_scheduler.remove_sandbox(&sandbox.id).await;
            }

            let fc_create_started_at = std::time::Instant::now();
            self.ensure_teardown_suspend_owner_for_durability(&sandbox.id, durability)?;
            if let Err(e) = node_client
                .create_snapshot(CreateSnapshotRequest {
                    execution_fence: sandbox.node_execution_fence(),
                    sandbox_id: sandbox.id.clone(),
                    snapshot_path: "/vmstate".to_string(),
                    mem_file_path: "/memory".to_string(),
                    dest_dir: Some(staging_dir_string.clone()),
                    snapshot_volume_mount_dir: enc_snapshot_mount_dir,
                })
                .await
            {
                let reason = format!("snapshot failed: {e}");
                // An RPC failure is not proof that Firecracker stopped. While
                // it is still live, its mmap is the Diff snapshot's memory base:
                // unmounting or force-freeing that volume would leave the retry
                // unable to reuse the base safely. A stopped runtime cannot be
                // rolled back to paused, so only that path may release before
                // the failure is quarantined.
                let runtime_live = (self.sandbox_runtime_live)(&sandbox.id);
                if enc_snapshot.is_some()
                    && failed_encrypted_capture_may_release_volume(runtime_live)
                    && let Err(release_error) =
                        crate::snapshot_encryption::release_encrypted_snapshot_volume(
                            &sandbox.id,
                            sandbox.node_execution_fence(),
                            "",
                        )
                        .await
                {
                    tracing::warn!(
                        sandbox_id = %sandbox.id,
                        %release_error,
                        "failed to release encrypted snapshot volume after stopped capture failure"
                    );
                }
                self.record_suspend_snapshot_publication_failure_after_begin_with_liveness(
                    &sandbox.id,
                    &reason,
                    runtime_live,
                );
                pplx_space_metrics::snapshot::create_failed(
                    create_kind,
                    create_codec,
                    sandbox.volume_encryption,
                    "fc_create",
                    "fc_create_failed",
                    create_started_at,
                );
                emit_suspend_failed_event(
                    &sandbox.id,
                    &suspend_event_details,
                    "fc_create",
                    "fc_create_failed",
                    reason.clone(),
                );
                return Err(Status::internal(reason));
            }
            pplx_space_metrics::snapshot::create_step(
                create_kind,
                create_codec,
                sandbox.volume_encryption,
                "fc_create",
                fc_create_started_at,
            );

            // BYOK: Firecracker wrote vmstate+memory into the encrypted snapshot
            // volume. Seal it (unmount+close) and stage the two encrypted artifacts
            // — `snapshot.luks` (vmstate+memory) and the rootfs delta `cow.luks` —
            // into the publication dir. Both are already-encrypted LUKS2 containers,
            // carried to durable storage as opaque blobs; reflink keeps the copy
            // instant and sparse. The plaintext rootfs codec branch below is skipped
            // for encrypted snapshots (there is no plaintext `rootfs.ext4` to diff).
            if encrypted {
                // Seal the volume and stage both ciphertext artifacts into the
                // publication dir in one privileged step (node-mgr can read the
                // sandbox-private instance dir; sbx-mgr cannot).
                if let Err(error) = crate::snapshot_encryption::release_encrypted_snapshot_volume(
                    &sandbox.id,
                    sandbox.node_execution_fence(),
                    &staging_dir_string,
                )
                .await
                {
                    let reason = format!("seal/stage encrypted snapshot volume failed: {error}");
                    self.record_suspend_snapshot_publication_failure_after_begin(&sandbox.id, &reason);
                    record_artifact_stage_failure(&reason);
                    return Err(Status::internal(reason));
                }
            }

            // Codec branch: when the resolved format is `BtrfsSend`,
            // replace the just-written `rootfs.ext4` with a `btrfs send
            // -p <template> <instance.ro>` stream. Two failure shapes:
            //
            // - `FailedPrecondition` from the encoder means the sandbox
            //   is btrfs-send-*ineligible* (instance not a subvolume,
            //   template plain-dir, parent_uuid lineage broken,
            //   received_uuid mismatch). The encoder bails before any
            //   side-effect; FC's `rootfs.ext4` from `create_snapshot` is
            //   still intact on disk. If the caller didn't *explicitly*
            //   request btrfs-send (i.e. it was the env-var/template
            //   default that picked it), silently fall back to Full and
            //   continue — the user has no API surface today to opt out
            //   so we mustn't surface this as a hard error. Explicit
            //   `format=BtrfsSend` on the wire keeps the loud-fail
            //   behavior so callers who *care* still see the precondition
            //   miss.
            // - Any other code (`Internal`, `Unavailable`, …) is a real
            //   encode failure (subvolume snapshot failed, btrfs-send
            //   exited non-zero, etc.). Always loud-fail; no auto-Full
            //   fallback here because the staged snapshot shape may be
            //   partial. If the runtime is still live, the failure handler
            //   rolls the DB row back to `paused` so a later suspend can
            //   start with a fresh staging directory.
            if !encrypted
                && published_format
                    == pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::BtrfsSend
            {
                let btrfs_step_started_at = std::time::Instant::now();
                let encode_result = self
                    .encode_rootfs_as_btrfs_send(&sandbox, &staging_dir_string)
                    .await;
                match encode_result {
                    Ok(_) => {
                        pplx_space_metrics::snapshot::create_step(
                            create_kind,
                            create_codec,
                            sandbox.volume_encryption,
                            "btrfs_send",
                            btrfs_step_started_at,
                        );
                    }
                    Err(status)
                        if status.code() == tonic::Code::FailedPrecondition
                            && requested_format
                                != pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::BtrfsSend =>
                    {
                        // Reactive fallback: the caller didn't explicitly
                        // pick btrfs-send (env-var default did), the
                        // sandbox is ineligible, and `rootfs.ext4` from
                        // FC is still intact. Demote to Full for the rest
                        // of this RPC so the upload picks the Full
                        // artifact set and the sidecar carries no
                        // `rootfs_diff_format` marker.
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            template = %sandbox.template,
                            template_hash = sandbox.template_hash.as_deref().unwrap_or(""),
                            from_codec = pplx_space_metrics::snapshot::CODEC_BTRFS_SEND_V1,
                            to_codec = pplx_space_metrics::snapshot::CODEC_FULL,
                            reason = %status.message(),
                            "btrfs-send precondition not met; suspending with Full codec instead"
                        );
                        pplx_space_metrics::snapshot::create_failed(
                            create_kind,
                            create_codec,
                            sandbox.volume_encryption,
                            "btrfs_send",
                            "fallback_to_full",
                            btrfs_step_started_at,
                        );
                        create_codec = pplx_space_metrics::snapshot::CODEC_FULL;
                        published_format =
                            pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Full;
                    }
                    Err(status) => {
                        // Loud-fail path. `encode_rootfs_as_btrfs_send`
                        // Status messages name the failed check explicitly
                        // (template not subvolume, received_uuid mismatch,
                        // parent_uuid lineage broken, btrfs send exited
                        // with …); this log preserves that detail
                        // server-side.
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            template = %sandbox.template,
                            template_hash = sandbox.template_hash.as_deref().unwrap_or(""),
                            codec = pplx_space_metrics::snapshot::CODEC_BTRFS_SEND_V1,
                            code = %status.code(),
                            error = %status.message(),
                            "btrfs-send rootfs encode failed; suspend snapshot wire-shape \
                             will be unusable on this attempt"
                        );
                        // If the runtime is still present, leave the caller
                        // a paused sandbox it can retry with `format=full`.
                        let reason = format!("btrfs-send encode failed: {status}");
                        self.record_suspend_snapshot_publication_failure_after_begin(&sandbox.id, &reason);
                        pplx_space_metrics::snapshot::create_failed(
                            create_kind,
                            create_codec,
                            sandbox.volume_encryption,
                            "btrfs_send",
                            "encode_failed",
                            create_started_at,
                        );
                        emit_suspend_failed_event(
                            &sandbox.id,
                            &suspend_event_details,
                            "btrfs_send",
                            "encode_failed",
                            reason,
                        );
                        return Err(status);
                    }
                }
            }

            let publish_started_at = std::time::Instant::now();
            let mut metadata = match suspend_snapshot_metadata(&sandbox, published_format) {
                Ok(metadata) => metadata,
                Err(status) => {
                    let reason = format!("local artifact metadata construction failed: {status}");
                    self.record_suspend_snapshot_publication_failure_after_begin(&sandbox.id, &reason);
                    return Err(status);
                }
            };
            metadata.local_artifact_id = Some(local_artifact_id);
            metadata.artifact_kind = Some(pplx_space_common_core::types::ArtifactKind::Full);
            metadata.artifact_origin = Some(pplx_space_common_core::types::ArtifactOrigin::Suspend);
            metadata.encrypted = encrypted;
            let vm_artifacts = if encrypted {
                crate::snapshot_upload::encrypted_vm_artifacts()
            } else {
                crate::snapshot_upload::vm_artifacts_for_format(published_format)
            };
            if let Err(error) =
                crate::snapshot_upload::write_snapshot_metadata(&metadata, &staging_dir, &vm_artifacts)
            {
                let reason = format!("local artifact metadata creation failed: {error}");
                self.record_suspend_snapshot_publication_failure_after_begin(&sandbox.id, &reason);
                pplx_space_metrics::snapshot::create_failed(
                    create_kind,
                    create_codec,
                    sandbox.volume_encryption,
                    "publish",
                    "metadata_failed",
                    create_started_at,
                );
                emit_suspend_failed_event(
                    &sandbox.id,
                    &suspend_event_details,
                    "publish",
                    "metadata_failed",
                    reason.clone(),
                );
                return Err(Status::internal(reason));
            }
            if let Err(error) =
                publish_suspend_snapshot_dir(&staging_dir, &snapshot_dir, local_artifact_id)
            {
                let reason = format!("local artifact publication failed: {error}");
                // A live rollback here leaves `monitoring_guard` to re-arm the
                // watchdog on return.
                if self.record_suspend_snapshot_publication_failure_after_staging(
                    &sandbox.id,
                    &sandbox_dir,
                    &staging_dir,
                    &reason,
                ) == SuspendSnapshotStagingDisposition::Preserve
                {
                    staging_guard.disarm();
                }
                pplx_space_metrics::snapshot::create_failed(
                    create_kind,
                    create_codec,
                    sandbox.volume_encryption,
                    "publish",
                    "rename_failed",
                    create_started_at,
                );
                emit_suspend_failed_event(
                    &sandbox.id,
                    &suspend_event_details,
                    "publish",
                    "rename_failed",
                    reason.clone(),
                );
                return Err(Status::internal(reason));
            }
            staging_guard.disarm();
            pplx_space_metrics::snapshot::create_step(
                create_kind,
                create_codec,
                sandbox.volume_encryption,
                "publish",
                publish_started_at,
            );

            let snapshot_dir_string = snapshot_dir.to_string_lossy().into_owned();
            if let SuspendDurability::RemoteBeforeTeardown {
                execution_generation,
            } = durability
            {
                let stage_result = {
                    let db = self.db_conn()?;
                    crate::db::stage_teardown_suspend_artifact(
                        &db,
                        &sandbox.id,
                        execution_generation,
                        local_artifact_id,
                        &snapshot_dir_string,
                    )
                };
                if let Err(error) = stage_result {
                    let reason = format!("persist durable suspend artifact before teardown: {error}");
                    self.record_suspend_snapshot_publication_failure_after_begin(&sandbox.id, &reason);
                    return Err(Status::internal(reason));
                }

                let staged_artifact = crate::db::LocalArtifactRow {
                    local_artifact_id,
                    sandbox_id: sandbox.id.clone(),
                    artifact_kind: pplx_space_common_core::types::ArtifactKind::Full,
                    artifact_origin: pplx_space_common_core::types::ArtifactOrigin::Suspend,
                    snapshot_name: None,
                    local_path: snapshot_dir_string.clone(),
                };
                // Capture/publication are finished. Remote durability can take
                // hours; do not prevent Stop/Delete admission while waiting for it.
                // Finalization reacquires the fence and checks the original owner.
                drop(cleanup_fence.take());
                self.ensure_teardown_suspend_backup(
                    &sandbox,
                    TeardownSuspendBackup {
                        execution_generation,
                        artifact: &staged_artifact,
                        format: published_format,
                    },
                )
                .await?;
            }

            // Node-local auto-suspend must hand the exact published artifact to a
            // durable owner before runtime cleanup releases its rootfs. A missing
            // upload destination is an intentional local-only configuration; when
            // a destination exists, failure to persist the worker intent leaves
            // the runtime intact and makes this suspend retryable.
            let upload_queue_started_at = std::time::Instant::now();
            if durability == SuspendDurability::NodeLocal {
                let enqueue_result = self
                    .enqueue_suspend_artifact_backup(
                        &sandbox,
                        local_artifact_id,
                        &snapshot_dir_string,
                        published_format,
                        None,
                    )
                    .await;
                self.require_node_local_backup_handoff(&sandbox.id, local_artifact_id, enqueue_result)?;
            }
            pplx_space_metrics::snapshot::create_step(
                create_kind,
                create_codec,
                sandbox.volume_encryption,
                "upload_queue",
                upload_queue_started_at,
            );

            let cleanup_started_at = std::time::Instant::now();
            self.ensure_teardown_suspend_owner_for_durability(&sandbox.id, durability)?;
            if let Err(error) = self
                .finalize_suspended_runtime(
                    &sandbox,
                    local_artifact_id,
                    &snapshot_dir_string,
                    Some(&mut monitoring_guard),
                    cleanup_fence.take(),
                )
                .await
            {
                pplx_space_metrics::snapshot::create_failed(
                    create_kind,
                    create_codec,
                    sandbox.volume_encryption,
                    "cleanup",
                    "cleanup_failed",
                    create_started_at,
                );
                emit_suspend_failed_event(
                    &sandbox.id,
                    &suspend_event_details,
                    "cleanup",
                    "cleanup_failed",
                    format!("cleanup after suspend failed: {error}"),
                );
                return Err(error);
            }
            pplx_space_metrics::snapshot::create_step(
                create_kind,
                create_codec,
                sandbox.volume_encryption,
                "cleanup",
                cleanup_started_at,
            );

            if publish_node_state {
                mark_sandbox_dirty(&self.db, &sandbox.id);
            }

            pplx_space_metrics::snapshot::create_completed(
                create_kind,
                create_codec,
                sandbox.volume_encryption,
                create_started_at,
            );
            Ok(())
        }
        .await;
        if staging_guard.armed {
            match self.state_writer_lease("SuspendStagingCleanup") {
                Ok(writer) => {
                    let _writer = writer;
                    let _fence = cleanup_fence;
                    staging_guard.cleanup(&mut node_client).await;
                }
                Err(error) => {
                    tracing::warn!(%error, "suspend staging cleanup admission refused; retaining residue");
                }
            }
        }
        result
    }

    async fn finalize_suspended_runtime(
        &self,
        sandbox: &crate::db::SandboxRow,
        local_artifact_id: LocalArtifactId,
        snapshot_dir: &str,
        monitoring_guard: Option<&mut SuspendMonitoringGuard>,
        cleanup_fence: Option<OwnedCleanupFenceGuard>,
    ) -> Result<(), Status> {
        let fence = match cleanup_fence {
            Some(fence) => fence,
            None => match self.try_cleanup_fence(&sandbox.id) {
                CleanupLockTryResult::Acquired(fence) => fence,
                CleanupLockTryResult::Busy => {
                    return Err(Status::unavailable(
                        "sandbox cleanup is active; retry suspend completion",
                    ));
                }
            },
        };
        let current = self.lookup_sandbox(&sandbox.id)?;
        if current.uuid8 != sandbox.uuid8
            || current.space_api_route_identity.execution_generation
                != sandbox.space_api_route_identity.execution_generation
            || !CleanupCompletion::MarkSuspended.owns_row_intent(&current)
        {
            return Err(Status::aborted("suspend completion superseded"));
        }
        if let Some(guard) = monitoring_guard {
            guard.disarm();
        }
        self.watchdog.remove_sandbox(&sandbox.id).await;
        self.snapshot_scheduler.remove_sandbox(&sandbox.id).await;
        let (sandbox_id, _) = Self::cleanup_identity(&sandbox.id, Some(&sandbox.uuid8))?;
        let admission = CleanupAdmission::persisted(
            sandbox_id,
            sandbox.space_api_route_identity.execution_generation,
            None,
            fence,
        );
        self.run_cleanup_to_completion_using_admission(
            SynchronousCleanupRequest {
                sandbox_id: &sandbox.id,
                uuid8: sandbox.uuid8.as_str(),
                keep_filesystem: true,
                context: "cleanup after suspend",
                completion: CleanupCompletion::MarkSuspended,
                published_artifact: Some((local_artifact_id, snapshot_dir.to_string())),
                immediate_kill: Self::cleanup_immediate_kill(CleanupCompletion::MarkSuspended),
                guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
            },
            Some(admission),
            None,
        )
        .await?;
        emit_sandbox_event(
            &sandbox.id,
            SandboxEventType::LocalArtifactPublished,
            SandboxEventSource::SbxManager,
            SandboxEventDetails::default()
                .local_artifact_id(local_artifact_id.to_string())
                .artifact_kind("full")
                .artifact_origin("suspend"),
        );
        Ok(())
    }

    /// Record that a teardown suspend's durable backup is over, on the row
    /// the shim keeps polling.
    ///
    /// A permanent job failure ends the artifact's upload for good:
    /// [`teardown_backup_failure_requires_new_attempt`] refuses a fresh
    /// attempt for it, so every later poll rejoins the same terminal job.
    /// The suspend therefore can never publish, the row can never leave
    /// `suspending`, and nothing on this node distinguishes it from a suspend
    /// still uploading — a distinction the cluster-deleted eviction needs
    /// before it may reclaim anything. The marker supplies exactly that
    /// terminality proof and nothing else; whether the sandbox is *gone*
    /// stays the cluster's call.
    ///
    /// Best-effort: a failed stamp leaves the pre-existing stuck row, which is
    /// the behavior without this marker at all, and the next poll retries.
    fn record_teardown_suspend_backup_permanent_failure(
        &self,
        sandbox_id: &str,
        staged_artifact: LocalArtifactId,
    ) {
        let marked = self.db_conn().and_then(|db| {
            crate::db::mark_teardown_suspend_backup_permanently_failed(
                &db,
                sandbox_id,
                staged_artifact,
            )
            .map_err(|error| Status::internal(format!("db error: {error}")))
        });
        match marked {
            Ok(true) => {
                tracing::warn!(
                    sandbox_id,
                    %staged_artifact,
                    "generation suspend backup was permanently refused; marked the suspending \
                     row terminal so a cluster-deleted report can reclaim it"
                );
                mark_sandbox_dirty(&self.db, sandbox_id);
            }
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(
                    sandbox_id,
                    %staged_artifact,
                    %error,
                    "failed to mark a permanently refused generation suspend backup; the \
                     suspending row stays unreclaimable until a later attempt marks it"
                );
            }
        }
    }

    async fn ensure_teardown_suspend_backup(
        &self,
        sandbox: &crate::db::SandboxRow,
        backup: TeardownSuspendBackup<'_>,
    ) -> Result<(), Status> {
        let artifact_dedup_key = backup.artifact.local_artifact_id.to_string();
        let persisted_job = self.snapshot_upload_worker.find_job_by_dedup_key(
            crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
            &artifact_dedup_key,
        )?;
        let job_id = match persisted_job {
            Some(job)
                if !job.is_terminal()
                    || !teardown_backup_failure_requires_new_attempt(job.failure) =>
            {
                job.job_id
            }
            _ => match self
                .enqueue_suspend_artifact_backup(
                    sandbox,
                    backup.artifact.local_artifact_id,
                    &backup.artifact.local_path,
                    backup.format,
                    Some(&artifact_dedup_key),
                )
                .await?
            {
                SubmitSuspendSnapshotOutcome::Enqueued { job_id, .. } => job_id,
                SubmitSuspendSnapshotOutcome::NoUploadConfig => {
                    return Err(Status::failed_precondition(
                        "generation suspend requires a configured snapshot upload destination",
                    ));
                }
            },
        };
        let job = self
            .snapshot_upload_worker
            .wait_for_job(&job_id, self.snapshot_upload_worker.worst_case_runtime())
            .await?;
        if !job.is_terminal() || job.failure.is_some() {
            if matches!(
                job.failure,
                Some(pplx_space_common_jobs::JobFailure::Permanent { .. })
            ) {
                self.record_teardown_suspend_backup_permanent_failure(
                    &sandbox.id,
                    backup.artifact.local_artifact_id,
                );
            }
            return Err(Status::internal(
                job.error
                    .unwrap_or_else(|| "durable suspend backup failed".to_string()),
            ));
        }
        {
            let db = self.db_conn()?;
            crate::db::mark_local_artifact_backup_completed(
                &db,
                &backup.artifact.local_artifact_id.to_string(),
            )
            .map_err(|error| {
                Status::internal(format!("persist durable suspend completion: {error}"))
            })?;
        }

        let still_owned = {
            let db = self.db_conn()?;
            crate::db::teardown_suspend_status(&db, &sandbox.id)
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .is_some_and(|status| {
                    status.backup_completed
                        && status.operation.is_some_and(|operation| {
                            operation.status.is_active()
                                && operation.execution_generation == backup.execution_generation
                        })
                        && status.artifact.is_some_and(|artifact| {
                            artifact.local_artifact_id == backup.artifact.local_artifact_id
                        })
                })
        };
        if !still_owned {
            return Err(Status::aborted(
                "generation suspend no longer owns durable suspend artifact",
            ));
        }
        Ok(())
    }

    async fn existing_ready_artifact_backup_response(
        &self,
        artifact: &crate::db::LocalArtifactRow,
    ) -> Result<Option<EnqueueArtifactBackupResponse>, Status> {
        let mut client = match pplx_space_node_api_client::cluster::connect_from_env().await {
            Ok(client) => client,
            Err(error) => {
                tracing::warn!(
                    sandbox_id = %artifact.sandbox_id,
                    local_artifact_id = %artifact.local_artifact_id,
                    %error,
                    "failed to check existing artifact backup"
                );
                return Err(Status::unavailable(format!(
                    "cannot check existing artifact backup: {error}"
                )));
            }
        };
        let request = pplx_space_node_api_client::proto::GetArtifactBackupLocationRequest {
            sandbox_id: artifact.sandbox_id.clone(),
            local_artifact_id: artifact.local_artifact_id.to_string(),
            artifact_kind: match artifact.artifact_kind {
                pplx_space_common_core::types::ArtifactKind::Disk => {
                    crate::proto::ArtifactKind::Disk as i32
                }
                pplx_space_common_core::types::ArtifactKind::Full => {
                    crate::proto::ArtifactKind::Full as i32
                }
            },
        };

        match client.get_artifact_backup_location(request).await {
            Ok(response) => {
                if let Err(error) = self
                    .local_artifacts
                    .mark_backup_completed(artifact.local_artifact_id)
                {
                    tracing::warn!(
                        sandbox_id = %artifact.sandbox_id,
                        local_artifact_id = %artifact.local_artifact_id,
                        %error,
                        "failed to stamp local artifact backup-completed marker after cluster proof"
                    );
                }
                let response = response.into_inner();
                Ok(Some(EnqueueArtifactBackupResponse {
                    local_artifact_id: artifact.local_artifact_id.to_string(),
                    artifact_kind: response.artifact_kind,
                    backup_attempt_id: response.backup_attempt_id,
                }))
            }
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(status) => {
                tracing::warn!(
                    sandbox_id = %artifact.sandbox_id,
                    local_artifact_id = %artifact.local_artifact_id,
                    %status,
                    "failed to check existing artifact backup"
                );
                Err(status)
            }
        }
    }

    fn reusable_artifact_backup_attempt(
        status: pplx_space_common_jobs::JobStatus,
        has_failure: bool,
        payload: &str,
    ) -> Result<Option<String>, serde_json::Error> {
        // A completed job records a past upload, not present remote durability.
        // Ready backups were verified against the cluster before this lookup;
        // authoritative absence must allow a new attempt instead of joining a
        // terminal job forever.
        if has_failure
            || !matches!(
                status,
                pplx_space_common_jobs::JobStatus::Pending
                    | pplx_space_common_jobs::JobStatus::Running
            )
        {
            return Ok(None);
        }

        #[derive(serde::Deserialize)]
        struct PersistedArtifactBackupIdentity {
            snapshot_id: String,
        }

        serde_json::from_str::<PersistedArtifactBackupIdentity>(payload)
            .map(|identity| Some(identity.snapshot_id))
    }

    pub(super) async fn enqueue_artifact_backup(
        &self,
        request: Request<EnqueueArtifactBackupRequest>,
    ) -> Result<Response<EnqueueArtifactBackupResponse>, Status> {
        self.authorize_peer(&request, "EnqueueArtifactBackup", ALLOW_SANDBOX_OPERATOR)?;
        let req = request.into_inner();
        let sandbox_id = req.sandbox_id.trim();
        if sandbox_id.is_empty() {
            return Err(Status::invalid_argument("sandbox_id is required"));
        }
        let expected_kind = match crate::proto::ArtifactKind::try_from(req.expected_artifact_kind) {
            Ok(crate::proto::ArtifactKind::Disk) => {
                pplx_space_common_core::types::ArtifactKind::Disk
            }
            Ok(crate::proto::ArtifactKind::Full) => {
                pplx_space_common_core::types::ArtifactKind::Full
            }
            _ => {
                return Err(Status::invalid_argument(
                    "expected_artifact_kind is required",
                ));
            }
        };
        if let Some(status) = reupload_precondition_for_cluster_api_env(
            cluster_api_env_var_set(pplx_space_node_api_client::cluster::CLUSTER_API_ENDPOINT_ENV),
            cluster_api_env_var_set("SPACE_INTERNAL_TOKEN"),
        ) {
            return Err(status);
        }

        let full_current_instance = expected_kind
            == pplx_space_common_core::types::ArtifactKind::Full
            && matches!(
                req.source.as_ref(),
                Some(crate::proto::enqueue_artifact_backup_request::Source::CurrentInstance(_))
                    | None
            );
        let (sandbox, _guard) = self
            .lock_artifact_for_sandbox(sandbox_id)
            .await
            .map_err(|status| artifact_backup_lookup_error(status, full_current_instance))?;
        if full_current_instance {
            self.ensure_snapshot_restore_supported("enqueue_artifact_backup")?;
        }
        // Outer Some means the destination was already resolved before adopting
        // an unpinned suspend; its inner None selects the configured env default.
        let mut pre_adoption_upload_config = None;
        let (artifact, skip_ready_lookup) = match req.source {
            Some(crate::proto::enqueue_artifact_backup_request::Source::LocalArtifactId(value)) => {
                let id = value.parse::<LocalArtifactId>().map_err(|err| {
                    Status::invalid_argument(format!("invalid local_artifact_id: {err}"))
                })?;
                let artifact = self
                    .local_artifacts
                    .get(id)
                    .map_err(|err| Status::internal(format!("db artifact lookup failed: {err}")))?
                    .ok_or_else(|| {
                        Status::failed_precondition(format!("local artifact {id} is not published"))
                    })?;
                (artifact, false)
            }
            Some(crate::proto::enqueue_artifact_backup_request::Source::SnapshotName(value)) => {
                let name =
                    pplx_space_common_core::types::SnapshotName::parse(value).map_err(|err| {
                        Status::invalid_argument(format!("invalid snapshot_name: {err}"))
                    })?;
                let existing = self
                    .local_artifacts
                    .get_named(&sandbox.id, &name)
                    .map_err(|err| Status::internal(format!("db artifact lookup failed: {err}")))?;
                if let Some(existing) = existing {
                    (existing, false)
                } else {
                    let adopted_kind = if name.as_str().starts_with("d-") {
                        pplx_space_common_core::types::ArtifactKind::Disk
                    } else if name.as_str().starts_with("f-") {
                        pplx_space_common_core::types::ArtifactKind::Full
                    } else {
                        return Err(Status::failed_precondition(format!(
                            "snapshot {name} does not identify a disk or full artifact"
                        )));
                    };
                    if adopted_kind != expected_kind {
                        return Err(Status::failed_precondition(format!(
                            "requested {expected_kind} backup but snapshot {name} is {adopted_kind}"
                        )));
                    }
                    let local_path =
                        pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir(
                            &sandbox.id,
                            name.as_str(),
                        );
                    if !std::path::Path::new(&local_path).is_dir() {
                        return Err(Status::failed_precondition(format!(
                            "snapshot {name} has no published local artifact"
                        )));
                    }
                    let local_artifact_id = LocalArtifactId::new();
                    self.local_artifacts
                        .publish_named_snapshot(
                            &sandbox.id,
                            local_artifact_id,
                            adopted_kind,
                            &name,
                            &local_path,
                        )
                        .map_err(|err| {
                            Status::internal(format!("db artifact adoption failed: {err}"))
                        })?;
                    self.local_artifacts
                        .get(local_artifact_id)
                        .map_err(|err| {
                            Status::internal(format!("db artifact lookup failed: {err}"))
                        })?
                        .map(|artifact| (artifact, true))
                        .ok_or_else(|| Status::internal("adopted local artifact missing"))?
                }
            }
            Some(crate::proto::enqueue_artifact_backup_request::Source::CurrentInstance(_))
            | None => {
                current_instance_backup_precondition(sandbox.state, expected_kind)?;
                let mut skip_ready_lookup = false;
                let current_id = match (sandbox.state, sandbox.current_local_artifact_id) {
                    (SandboxState::Suspended, None) => {
                        let sandbox_dir =
                            std::path::PathBuf::from(format!("{CACHE_DIR}/{}", sandbox.id));
                        let local_path = canonical_suspend_snapshot_dir(&sandbox_dir)
                            .to_string_lossy()
                            .into_owned();
                        require_artifact_backup_files(
                            &local_path,
                            expected_kind,
                            reupload_format_from_sidecar(&local_path)?,
                            full_current_instance,
                        )?;
                        pre_adoption_upload_config = Some(reupload_resolution_to_config(
                            self.resolve_org_snapshot_upload_config(&sandbox.id).await,
                            self.snapshot_upload_config.is_some(),
                        )?);
                        let id = LocalArtifactId::new();
                        let current_id = self.adopt_or_get_suspended_local_artifact(
                            &sandbox.id,
                            id,
                            &local_path,
                        )?;
                        skip_ready_lookup = current_id == id;
                        current_id
                    }
                    (SandboxState::Stopped, None) => {
                        let id = LocalArtifactId::new();
                        let snapshot_name = format!("d-stop-{id}");
                        let mut node_client = connect_node().await.map_err(|error| {
                            Status::internal(format!("connect to node-manager: {error}"))
                        })?;
                        node_client
                            .create_btrfs_snapshot(
                                pplx_space_node_mgr_client::proto::CreateBtrfsSnapshotRequest {
                                    sandbox_id: sandbox.id.clone(),
                                    snapshot_name: snapshot_name.clone(),
                                    writable: false,
                                    execution_fence: sandbox.node_execution_fence(),
                                },
                            )
                            .await
                            .map_err(|error| {
                                Status::internal(format!("adopt stopped disk artifact: {error}"))
                            })?;
                        let local_path =
                            pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir(
                                &sandbox.id,
                                &snapshot_name,
                            );
                        let sandbox_id = sandbox.id.parse().map_err(|error| {
                            Status::internal(format!("persisted sandbox id is invalid: {error}"))
                        })?;
                        let publish_result = self.apply_route_identity_mutation(
                            sandbox_id,
                            "stopped_artifact_adoption",
                            || {
                                self.local_artifacts
                                    .publish_stopped(&sandbox.id, id, &local_path)
                                    .map(|()| ((), true))
                            },
                        );
                        if let Err(err) = publish_result {
                            if let Err(cleanup_error) = node_client
                                .delete_btrfs_snapshot(
                                    pplx_space_node_mgr_client::proto::DeleteBtrfsSnapshotRequest {
                                        sandbox_id: sandbox.id.clone(),
                                        snapshot_name: snapshot_name.clone(),
                                    },
                                )
                                .await
                            {
                                tracing::warn!(
                                    sandbox_id = %sandbox.id,
                                    snapshot_name,
                                    error = %cleanup_error,
                                    "failed to remove stopped artifact snapshot after adoption db failure"
                                );
                            }
                            return Err(Status::internal(format!(
                                "db artifact adoption failed: {err}"
                            )));
                        }
                        skip_ready_lookup = true;
                        id
                    }
                    (_, Some(id)) => id,
                    _ => {
                        return Err(Status::failed_precondition(
                            "current instance has no published local artifact",
                        ));
                    }
                };
                let artifact = self
                    .local_artifacts
                    .get(current_id)
                    .map_err(|err| Status::internal(format!("db artifact lookup failed: {err}")))?
                    .ok_or_else(|| {
                        Status::failed_precondition(format!(
                            "local artifact {current_id} is not published"
                        ))
                    })?;
                (artifact, skip_ready_lookup)
            }
        };
        if artifact.sandbox_id != sandbox.id {
            return Err(Status::failed_precondition(
                "local artifact does not belong to requested sandbox",
            ));
        }
        if artifact.artifact_kind != expected_kind {
            return Err(Status::failed_precondition(format!(
                "requested {expected_kind} backup but published artifact is {}",
                artifact.artifact_kind
            )));
        }
        let existing_ready_backup = if skip_ready_lookup {
            None
        } else {
            self.existing_ready_artifact_backup_response(&artifact)
                .await?
        };
        if let Some(response) = existing_ready_backup {
            tracing::info!(
                sandbox_id = %artifact.sandbox_id,
                local_artifact_id = %artifact.local_artifact_id,
                backup_attempt_id = %response.backup_attempt_id,
                "local artifact backup already ready; skipping duplicate enqueue"
            );
            return Ok(Response::new(response));
        }

        let artifact_dedup_key = artifact.local_artifact_id.to_string();
        if let Some(job) = self.snapshot_upload_worker.find_job_by_dedup_key(
            crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
            &artifact_dedup_key,
        )? {
            let job_status = job
                .status
                .parse::<pplx_space_common_jobs::JobStatus>()
                .map_err(|error| {
                    Status::internal(format!(
                        "persisted artifact backup status is invalid for {}: {error}",
                        artifact.local_artifact_id
                    ))
                })?;
            if let Some(backup_attempt_id) = Self::reusable_artifact_backup_attempt(
                job_status,
                job.failure.is_some(),
                &job.payload,
            )
            .map_err(|error| {
                Status::internal(format!(
                    "persisted artifact backup payload is invalid for {}: {error}",
                    artifact.local_artifact_id
                ))
            })? {
                self.establish_restore_visible_artifact_backup(&job.job_id)
                    .await
                    .map_err(|error| {
                        Status::internal(format!(
                            "establish restore-visible artifact backup: {error}"
                        ))
                    })?;
                tracing::info!(
                    sandbox_id = %artifact.sandbox_id,
                    local_artifact_id = %artifact.local_artifact_id,
                    %backup_attempt_id,
                    snapshot_upload_job_id = %job.job_id,
                    "local artifact backup already in progress; joining existing attempt"
                );
                return Ok(Response::new(EnqueueArtifactBackupResponse {
                    local_artifact_id: artifact.local_artifact_id.to_string(),
                    artifact_kind: match artifact.artifact_kind {
                        pplx_space_common_core::types::ArtifactKind::Disk => {
                            crate::proto::ArtifactKind::Disk as i32
                        }
                        pplx_space_common_core::types::ArtifactKind::Full => {
                            crate::proto::ArtifactKind::Full as i32
                        }
                    },
                    backup_attempt_id,
                }));
            }
        }

        let format = artifact_backup_format(&artifact)?;
        require_artifact_backup_files(
            &artifact.local_path,
            artifact.artifact_kind,
            format,
            full_current_instance,
        )?;

        let parsed_upload_config = match pre_adoption_upload_config {
            Some(config) => config,
            None => reupload_resolution_to_config(
                self.resolve_org_snapshot_upload_config(&sandbox.id).await,
                self.snapshot_upload_config.is_some(),
            )?,
        };
        let backup_attempt_id = pplx_space_common_core::types::BackupAttemptId::new();
        let backup_attempt_id = match self
            .submit_artifact_backup(
                &sandbox,
                ArtifactBackupSubmission {
                    local_artifact_id: artifact.local_artifact_id,
                    artifact_kind: artifact.artifact_kind,
                    artifact_origin: artifact.artifact_origin,
                    snapshot_name: artifact.snapshot_name.as_ref().map(|name| name.as_str()),
                    backup_attempt_id,
                    snapshot_dir: &artifact.local_path,
                    request_override: parsed_upload_config.as_ref(),
                    format,
                    dedup_key: Some(&artifact_dedup_key),
                    handoff: ArtifactBackupHandoff::RestoreVisible,
                },
            )
            .await
            .map_err(|err| Status::internal(format!("artifact backup enqueue failed: {err}")))?
        {
            SubmitSuspendSnapshotOutcome::Enqueued {
                backup_attempt_id, ..
            } => backup_attempt_id,
            SubmitSuspendSnapshotOutcome::NoUploadConfig => {
                return Err(Status::failed_precondition(
                    "cannot back up artifact: no snapshot upload destination configured",
                ));
            }
        };
        Ok(Response::new(EnqueueArtifactBackupResponse {
            local_artifact_id: artifact.local_artifact_id.to_string(),
            artifact_kind: match artifact.artifact_kind {
                pplx_space_common_core::types::ArtifactKind::Disk => {
                    crate::proto::ArtifactKind::Disk as i32
                }
                pplx_space_common_core::types::ArtifactKind::Full => {
                    crate::proto::ArtifactKind::Full as i32
                }
            },
            backup_attempt_id,
        }))
    }

    /// Queue a fresh backup for a legacy suspended sandbox so background
    /// eviction can later prove and remove its local cache presence.
    pub(crate) async fn reupload_legacy_sandbox_for_eviction(
        &self,
        sandbox_id: &str,
    ) -> Result<JobId, Status> {
        let sandbox_id = sandbox_id.trim();
        if sandbox_id.is_empty() {
            return Err(Status::invalid_argument("sandbox_id is required"));
        }
        self.enqueue_suspend_snapshot_reupload(
            sandbox_id,
            SuspendedArtifactAdoption::PreserveUpdatedAt,
        )
        .await
    }

    async fn lock_artifact_for_sandbox(
        &self,
        selector: &str,
    ) -> Result<
        (
            crate::db::SandboxRow,
            crate::auto_lifecycle::LifecycleMutexGuard,
        ),
        Status,
    > {
        let id = self.lookup_sandbox(selector)?.id;
        let guard = super::restart_admission::wait_for_activation_admission(
            &id,
            self.auto_lifecycle.artifact_lock(&id).lock_owned(),
        )
        .await?;
        // Re-resolve only the canonical identity: the selector may match a
        // different sandbox after waiting behind deletion or publication.
        let sandbox = self.lookup_sandbox(&id)?;
        Ok((sandbox, guard))
    }

    async fn enqueue_suspend_snapshot_reupload(
        &self,
        sandbox_id: &str,
        adoption: SuspendedArtifactAdoption,
    ) -> Result<JobId, Status> {
        // Cluster-api wiring is a hard precondition for reupload: the
        // durable worker's registration step uses the
        // same `connect_from_env` and treats MissingEndpoint/MissingToken
        // as `JobError::permanent`. Without this guard we'd return a job
        // id for an enqueue that can never close the loop (no `ready`
        // snapshot row would land), so the reconciler would never see
        // promotion progress.
        if let Some(status) = reupload_precondition_for_cluster_api_env(
            cluster_api_env_var_set("SPACE_INTERNAL_API_ENDPOINT"),
            cluster_api_env_var_set("SPACE_INTERNAL_TOKEN"),
        ) {
            return Err(status);
        }
        // Re-wrap `NotFound` from the local sqlite lookup as
        // `FailedPrecondition` carrying `REUPLOAD_SANDBOX_UNKNOWN_MARKER`.
        // This is a terminal condition for promotion: the sandbox isn't
        // on this node, so reupload can never succeed here. The cluster
        // controller's classifier keys off the marker (same pattern as
        // `REUPLOAD_ARTIFACT_MISSING_MARKER`) and flips the sandbox to
        // `failed`. We deliberately don't return raw `NotFound` because
        // the substrate adapter also surfaces endpoint-resolution
        // failures as `SubstrateError::NotFound`, which must stay
        // retryable.
        let (sandbox, _guard) =
            self.lock_artifact_for_sandbox(sandbox_id)
                .await
                .map_err(|status| {
                    if status.code() == tonic::Code::NotFound {
                        Status::failed_precondition(format!(
                    "cannot reupload snapshot: {marker} for sandbox_id={sandbox_id} \
                     — sbx-mgr has no record of this sandbox on this node",
                    marker =
                        pplx_space_common_core::node_drain_signals::REUPLOAD_SANDBOX_UNKNOWN_MARKER,
                ))
                    } else {
                        status
                    }
                })?;
        self.ensure_snapshot_restore_supported("reupload_suspend_snapshot")?;

        if sandbox.state != SandboxState::Suspended {
            // The cluster-controller's node-drain local-snapshot promotion
            // classifies ONLY the `restoring` rejection on
            // `REUPLOAD_SANDBOX_RESTORING_MARKER` to take its give-up path: a
            // permanently-refused reupload from a stuck on-node `restoring`
            // state would otherwise wedge the node's drain forever, and the
            // snapshot is canonical (the VM is loading from it, holds no newer
            // state). Render that exact phrase via the shared constant —
            // byte-identical to the historical message for compatibility with
            // older sandbox-managers. Every other non-suspended
            // state (started/paused — live state; stopped — possibly-modified
            // disk) gets a generic message carrying no marker, so the give-up
            // never abandons their potentially-newer state.
            let msg = if sandbox.state == SandboxState::Restoring {
                format!(
                    "cannot reupload snapshot: {}",
                    pplx_space_common_core::node_drain_signals::REUPLOAD_SANDBOX_RESTORING_MARKER,
                )
            } else {
                format!(
                    "cannot reupload snapshot: sandbox is {}, must be suspended",
                    sandbox.state
                )
            };
            return Err(Status::failed_precondition(msg));
        }

        let sandbox_dir = std::path::PathBuf::from(format!("{CACHE_DIR}/{}", sandbox.id));
        let snapshot_dir = canonical_suspend_snapshot_dir(&sandbox_dir)
            .to_string_lossy()
            .into_owned();
        // The reupload codec must match what the original suspend wrote.
        // Sniff the on-disk sidecar to learn it; absence of the file
        // (older snapshots that pre-date the sidecar) is treated as
        // Full, the historical shape. The codec choice changes only
        // which rootfs artifact filename we require on disk
        // (`rootfs.ext4` for Full, `rootfs.btrfs-send` for BtrfsSend).
        let reupload_format = reupload_format_from_sidecar(&snapshot_dir)?;
        let artifact_set = crate::snapshot_upload::vm_artifacts_for_format(reupload_format);
        // Check exactly the VM artifacts `submit_suspend_snapshot` reads
        // off disk. The `snapshot.json` metadata document it also uploads
        // is *regenerated* from these files at submit time, not required
        // to pre-exist, so it stays out of this precondition — but the VM
        // artifacts must be present, or the inner submit surfaces a
        // missing-file as `Status::internal` (without the terminal marker
        // the reconciler classifies on).
        for artifact in &artifact_set {
            let path = format!("{snapshot_dir}/{}", artifact.local_name);
            // `try_exists` distinguishes "stat says no" (Ok(false), genuine
            // missing artifact, terminal) from "stat itself failed" (Err,
            // transient permission/IO/mount issue). Only Ok(false) maps to
            // FailedPrecondition; stat errors surface as Internal so the
            // reconciler retries instead of escalating to mark-as-failed.
            match std::path::Path::new(&path).try_exists() {
                Ok(true) => {}
                Ok(false) => {
                    // The marker substring is the classifier signal the
                    // cluster controller's promote handler keys off (see
                    // `pplx_space_common_core::node_drain_signals`). Both
                    // sides must reference the constant — a reworded
                    // literal here would silently flip the consumer from
                    // "terminal, mark sandbox failed" to "retry forever".
                    return Err(Status::failed_precondition(format!(
                        "cannot reupload snapshot: {marker} at {path} \
                         — the local snapshot has already been pruned; the sandbox cannot \
                         be promoted from this node",
                        marker = pplx_space_common_core::node_drain_signals::REUPLOAD_ARTIFACT_MISSING_MARKER,
                    )));
                }
                Err(err) => {
                    return Err(Status::internal(format!(
                        "cannot reupload snapshot: failed to stat artifact at {path}: {err}",
                    )));
                }
            }
        }

        // Backup admission must distinguish a transient cluster-api lookup
        // failure (retryable — surface as `Internal` so the reconciler keeps
        // ticking) from a definitively-absent override (terminal —
        // `FailedPrecondition` if there's also no env fallback). Collapsing
        // both into `Option<_>` would let a transient blip masquerade as
        // a terminal "no destination configured" and trigger spurious
        // mark-as-failed by the reconciler's classifier.
        let resolution = self.resolve_org_snapshot_upload_config(&sandbox.id).await;
        let parsed_upload_config =
            reupload_resolution_to_config(resolution, self.snapshot_upload_config.is_some())?;

        let local_artifact_id = match sandbox.current_local_artifact_id {
            Some(local_artifact_id) => local_artifact_id,
            None => {
                let local_artifact_id = LocalArtifactId::new();
                match adoption {
                    SuspendedArtifactAdoption::TouchUpdatedAt => self
                        .adopt_or_get_suspended_local_artifact(
                            &sandbox.id,
                            local_artifact_id,
                            &snapshot_dir,
                        )?,
                    SuspendedArtifactAdoption::PreserveUpdatedAt => self
                        .adopt_or_get_suspended_local_artifact_preserving_updated_at(
                            &sandbox.id,
                            local_artifact_id,
                            &snapshot_dir,
                        )?,
                }
            }
        };
        let backup_attempt_id = pplx_space_common_core::types::BackupAttemptId::new();
        let outcome = self
            .submit_artifact_backup(
                &sandbox,
                ArtifactBackupSubmission {
                    local_artifact_id,
                    artifact_kind: pplx_space_common_core::types::ArtifactKind::Full,
                    artifact_origin: pplx_space_common_core::types::ArtifactOrigin::Suspend,
                    snapshot_name: None,
                    backup_attempt_id,
                    snapshot_dir: &snapshot_dir,
                    request_override: parsed_upload_config.as_ref(),
                    format: reupload_format,
                    dedup_key: None,
                    handoff: ArtifactBackupHandoff::DurableWorker,
                },
            )
            .await
            .map_err(|e| {
                tracing::error!(
                    sandbox_id = %sandbox.id,
                    error = %e,
                    "reupload_suspend_snapshot: durable worker submission failed",
                );
                Status::internal(format!("snapshot upload enqueue failed: {e}"))
            })?;
        let job_id = match outcome {
            SubmitSuspendSnapshotOutcome::Enqueued { job_id, .. } => job_id,
            SubmitSuspendSnapshotOutcome::NoUploadConfig => {
                // The earlier guard already rejected this case; preserve
                // the invariant defensively in case the request override
                // resolution races with config init.
                return Err(Status::failed_precondition(
                    "cannot reupload snapshot: no snapshot upload destination configured",
                ));
            }
        };
        Ok(job_id)
    }

    /// Resolve the org-level S3 override for this sandbox. Returns one
    /// of three classifications:
    ///
    /// - [`OrgSnapshotConfigResolution::Resolved`] — override present
    ///   and parses cleanly.
    /// - [`OrgSnapshotConfigResolution::Absent`] — no cluster-api wired
    ///   (local dev), sandbox row not yet visible (benign create
    ///   race), or org left the override unset / empty. Callers fall
    ///   back to the env-derived default.
    /// - [`OrgSnapshotConfigResolution::LookupFailed`] — cluster-api
    ///   unreachable, RPC errored with non-`NotFound`, connect failed
    ///   with bearer/endpoint/transport error, or the override URI
    ///   itself failed to parse. The suspend RPC collapses this to
    ///   silent fall-through (the local snapshot is authoritative);
    ///   backup admission surfaces it as `Internal` so the reconciler
    ///   retries rather than escalating to mark-as-failed.
    async fn resolve_org_snapshot_upload_config(
        &self,
        sandbox_id: &str,
    ) -> OrgSnapshotConfigResolution {
        use pplx_space_node_api_client::cluster::{ConnectClusterApiError, connect_from_env};

        let mut client = match connect_from_env().await {
            Ok(c) => c,
            Err(
                ConnectClusterApiError::MissingEndpoint | ConnectClusterApiError::MissingToken(_),
            ) => {
                // Node is not wired to a cluster API (e.g. local dev) —
                // definitively no override.
                return OrgSnapshotConfigResolution::Absent;
            }
            Err(e) => {
                // Transport errors and bearer/endpoint config bugs are
                // not "absent" — they're a lookup failure. Callers that
                // need to retry (reupload) treat this as `Internal`.
                return OrgSnapshotConfigResolution::LookupFailed(format!(
                    "cluster-api connect failed: {e}"
                ));
            }
        };

        let req = pplx_space_node_api_client::proto::GetSandboxSnapshotStorageUriRequest {
            sandbox_id: sandbox_id.to_string(),
        };
        let uri = match client.get_sandbox_snapshot_storage_uri(req).await {
            Ok(resp) => resp.into_inner().storage_uri,
            Err(status) if status.code() == tonic::Code::NotFound => {
                // For suspend this is a benign create race (the local
                // SandboxRow exists before the cluster row is committed).
                // For reupload it indicates state divergence — the call
                // site classifies. Returning the distinct
                // `ClusterRowMissing` variant keeps suspend's silent
                // fall-through while letting reupload reject up front.
                return OrgSnapshotConfigResolution::ClusterRowMissing;
            }
            Err(status) => {
                return OrgSnapshotConfigResolution::LookupFailed(format!(
                    "GetSandboxSnapshotStorageUri failed: code={:?} message={}",
                    status.code(),
                    status.message(),
                ));
            }
        };
        let uri = match uri {
            Some(u) if !u.is_empty() => u,
            _ => return OrgSnapshotConfigResolution::Absent,
        };

        match crate::snapshot_upload::SnapshotUploadConfig::from_uri(&uri) {
            Ok(cfg) => OrgSnapshotConfigResolution::Resolved(cfg),
            Err(e) => {
                // Malformed override is operator-fixable, not transient,
                // but it's also not "absent" — there is an override, it's
                // broken. Surfacing as `LookupFailed` keeps suspend's
                // silent fall-through behavior intact (the call site logs
                // and substitutes the env default) while letting reupload
                // return `Internal` so the reconciler doesn't mark the
                // sandbox failed on a config bug an operator can fix.
                OrgSnapshotConfigResolution::LookupFailed(format!(
                    "org snapshot_base_object_storage_uri did not parse: {e}"
                ))
            }
        }
    }

    /// Persist a published local artifact backup intent on the durable worker.
    /// The public enqueue and teardown handoffs additionally stage immutable
    /// vol-mgr transfer handles and establish the Cluster API `creating` row
    /// before returning; the compatibility reupload handoff returns after the
    /// local intent. In every case the worker polls and finalizes across
    /// process restarts.
    ///
    /// `request_override`, when `Some`, takes precedence over the env-derived
    /// default in [`SnapshotUploadConfig::from_env`].
    /// Encode the post-FC-freeze rootfs as a `btrfs send -p` stream
    /// by delegating to node-manager (the privileged daemon —
    /// sbxmgr is non-root in prod and lacks `CAP_SYS_ADMIN` for the
    /// btrfs ioctls). Strict preconditions hold inside nodemgr; any
    /// mismatch comes back as `FailedPrecondition` so the caller
    /// can retry with `format=full`. Preconditions checked
    /// nodemgr-side:
    ///
    /// 1. The local template directory at
    ///    `/space/cache/firecracker/templates/<name>/<content_hash>` is
    ///    a btrfs subvolume with `received_uuid` matching
    ///    `deterministic_template_received_uuid(name, content_hash)`.
    /// 2. The sandbox `instance/` directory is a btrfs subvolume whose
    ///    `parent_uuid` equals the local template's `uuid`. Set by
    ///    `node-mgr::launch_prepare` when the template was a subvolume
    ///    at sandbox-create time (the default for tmplmgr-installed
    ///    templates; non-subvolume legacy templates take the
    ///    create+reflink path and end up without a parent-UUID
    ///    lineage).
    ///
    /// Nodemgr then takes a read-only btrfs snapshot of the
    /// instance, runs `btrfs send -p <template> <instance.ro>`
    /// writing to `<snapshot_dir>/rootfs.btrfs-send`, removes the
    /// now-superseded `rootfs.ext4` so the upload set doesn't carry
    /// both, and tears down the `instance.ro` snapshot.
    ///
    /// The `sandbox.template_hash` precondition is checked here
    /// (sbxmgr-side) before the gRPC call so we surface the no-hash
    /// case without a round-trip; nodemgr's RPC would reject an
    /// empty `content_hash` field with the same FailedPrecondition.
    async fn encode_rootfs_as_btrfs_send(
        &self,
        sandbox: &crate::db::SandboxRow,
        snapshot_dir: &str,
    ) -> Result<u64, Status> {
        let content_hash = sandbox.template_hash.as_deref().ok_or_else(|| {
            Status::failed_precondition(
                "btrfs-send codec requires sandbox.template_hash to be set; \
                 retry with format=full",
            )
        })?;

        let stream_bytes = crate::btrfs_via_nodemgr::encode_rootfs_send(
            &sandbox.id,
            &sandbox.template,
            content_hash,
            snapshot_dir,
            sandbox
                .node_execution_fence()
                .as_ref()
                .ok_or_else(|| Status::failed_precondition("execution identity is missing"))?
                .try_into()?,
        )
        .await
        .map_err(|e| e.into_status())?;

        tracing::info!(
            sandbox_id = %sandbox.id,
            template = %sandbox.template,
            content_hash,
            stream_bytes,
            "btrfs-send codec: rootfs encoded as btrfs send -p stream (via nodemgr)"
        );
        Ok(stream_bytes)
    }

    async fn submit_artifact_backup(
        &self,
        sandbox: &crate::db::SandboxRow,
        submission: ArtifactBackupSubmission<'_>,
    ) -> Result<SubmitSuspendSnapshotOutcome, String> {
        let ArtifactBackupSubmission {
            local_artifact_id,
            artifact_kind,
            artifact_origin,
            snapshot_name,
            backup_attempt_id,
            snapshot_dir,
            request_override,
            format,
            dedup_key,
            handoff,
        } = submission;
        let config: &crate::snapshot_upload::SnapshotUploadConfig = match request_override {
            Some(c) => c,
            None => match self.snapshot_upload_config.as_ref() {
                Some(c) => c,
                None => return Ok(SubmitSuspendSnapshotOutcome::NoUploadConfig),
            },
        };

        // Encrypted-rootfs sandboxes are supported here: the publication staged
        // the encrypted `snapshot.luks` (vmstate+memory) and `cow.luks` (rootfs
        // delta), and `metadata.json` carries `encrypted: true` so the artifact
        // set below resolves to the encrypted shape. Nothing to refuse.
        let meta_path = std::path::Path::new(snapshot_dir)
            .join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME);
        let mut meta = match std::fs::read(&meta_path) {
            Ok(bytes) => serde_json::from_slice::<crate::snapshot_upload::SnapshotMeta>(&bytes)
                .map_err(|err| {
                    format!(
                        "published snapshot metadata is unparseable at {}: {err}",
                        meta_path.display()
                    )
                })?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                suspend_snapshot_metadata(sandbox, format).map_err(|error| error.to_string())?
            }
            Err(err) => {
                return Err(format!(
                    "read published snapshot metadata at {} failed: {err}",
                    meta_path.display()
                ));
            }
        };
        meta.local_artifact_id = Some(local_artifact_id);
        meta.artifact_kind = Some(artifact_kind);
        meta.artifact_origin = Some(artifact_origin);
        meta.artifact_snapshot_name = snapshot_name
            .map(pplx_space_common_core::types::SnapshotName::parse)
            .transpose()
            .map_err(|error| format!("invalid artifact snapshot name: {error}"))?;
        // Normalize legacy metadata before persisting the durable intent. The
        // worker may execute after this process exits and must not need to
        // rediscover capture-time execution identity from the replacement
        // host.
        meta.execution = Some(
            meta.execution_metadata(host_architecture().map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?,
        );

        // Every backup attempt gets a fresh UUIDv7 identity. It names the
        // per-attempt S3 prefix before any vol-mgr submission; the cluster row is
        // inserted in state `creating` against this same id and flipped to
        // `ready`/`failed` by the durable worker tail.
        let mut snapshot_id = backup_attempt_id.to_string();
        // Prepare the exact immutable artifact locally, but perform no remote
        // side effects here. The durable worker job below must exist before
        // vol-mgr submission, ClusterApi.BeginArtifactBackup, or runtime
        // teardown so cancellation at any later point has an owner that can
        // resume the complete pipeline.
        let mut jobs = crate::snapshot_upload::prepare_suspend_snapshot(
            &meta,
            &snapshot_id,
            std::path::Path::new(snapshot_dir),
            config,
            artifact_kind,
            format,
        )
        .map_err(|error| format!("prepare snapshot upload intent failed: {error}"))?;
        let mut storage_uri = jobs.storage_uri.clone();
        let local_artifact_id = local_artifact_id.to_string();

        let job_id = match self.snapshot_upload_worker.submit_json(
            crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
            &jobs,
            Some(&sandbox.id),
            dedup_key,
        ) {
            Ok(id) => id,
            Err(e) => {
                let reason = format!("snapshot_upload worker rejected the job: {e}");
                emit_sandbox_event(
                    &sandbox.id,
                    SandboxEventType::ArtifactBackupFailed,
                    SandboxEventSource::SbxManager,
                    SandboxEventDetails::default()
                        .snapshot_id(snapshot_id.clone())
                        .backup_attempt_id(snapshot_id.clone())
                        .local_artifact_id(local_artifact_id.to_string())
                        .artifact_kind(artifact_kind.as_str())
                        .artifact_origin(artifact_origin.as_str())
                        .object_uri(storage_uri.clone())
                        .reason("snapshot_upload_worker_submit")
                        .rootfs_diff_format(meta.rootfs_diff_format.as_deref().unwrap_or("full"))
                        .error(reason.clone()),
                );
                crate::snapshot_upload::cleanup_metadata_staging_dir(&jobs);
                return Err(reason);
            }
        };

        // `submit_json` may return an existing active job for a caller-supplied
        // dedup key. That job owns its immutable backup attempt: join its
        // payload instead of replacing it with this caller's newly prepared
        // snapshot ID and transfer set.
        let persisted_after_submit = self
            .snapshot_upload_worker
            .get_job(&job_id)
            .map_err(|error| format!("read snapshot-upload job {job_id}: {error}"))?
            .ok_or_else(|| format!("snapshot-upload job {job_id} disappeared after submit"))?
            .payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
            .map_err(|error| format!("decode snapshot-upload job {job_id}: {error}"))?;
        if persisted_after_submit.snapshot_id != jobs.snapshot_id {
            crate::snapshot_upload::cleanup_metadata_staging_dir(&jobs);
            jobs = persisted_after_submit;
            snapshot_id.clone_from(&jobs.snapshot_id);
            storage_uri.clone_from(&jobs.storage_uri);
        }
        tracing::info!(
            sandbox_id = %sandbox.id,
            snapshot_upload_job_id = %job_id,
            "local artifact backup intent persisted for durable processing"
        );

        if matches!(
            handoff,
            ArtifactBackupHandoff::RestoreVisible | ArtifactBackupHandoff::TeardownFence
        ) {
            self.establish_restore_visible_artifact_backup(&job_id)
                .await?;
        }
        if handoff == ArtifactBackupHandoff::TeardownFence {
            tracing::info!(
                sandbox_id = %sandbox.id,
                snapshot_upload_job_id = %job_id,
                "snapshot artifact staged in vol-mgr before runtime teardown"
            );
        }
        emit_sandbox_event(
            &sandbox.id,
            SandboxEventType::ArtifactBackupQueued,
            SandboxEventSource::SbxManager,
            SandboxEventDetails::default()
                .snapshot_id(snapshot_id.clone())
                .backup_attempt_id(snapshot_id.clone())
                .local_artifact_id(local_artifact_id.to_string())
                .artifact_kind(artifact_kind.as_str())
                .artifact_origin(artifact_origin.as_str())
                .object_uri(storage_uri)
                .snapshot_upload_job_id(job_id.to_string())
                .rootfs_diff_format(meta.rootfs_diff_format.as_deref().unwrap_or("full")),
        );
        Ok(SubmitSuspendSnapshotOutcome::Enqueued {
            job_id,
            backup_attempt_id: snapshot_id,
        })
    }

    async fn establish_restore_visible_artifact_backup(
        &self,
        job_id: &JobId,
    ) -> Result<crate::snapshot_upload::SnapshotUploadJobs, String> {
        let persisted = self
            .snapshot_upload_worker
            .get_job(job_id)
            .map_err(|error| format!("read snapshot-upload job {job_id}: {error}"))?
            .ok_or_else(|| format!("snapshot-upload job {job_id} disappeared"))?;
        if persisted.is_terminal() && persisted.failure.is_some() {
            return Err(format!(
                "snapshot-upload job {job_id} terminated before restore-visible handoff: {}",
                persisted.error.as_deref().unwrap_or("unknown error")
            ));
        }
        let mut payload = persisted
            .payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
            .map_err(|error| format!("decode snapshot-upload job {job_id}: {error}"))?;
        if crate::snapshot_upload_jobs::prepared_upload_requires_begin(&payload) {
            crate::snapshot_upload_jobs::begin_prepared_upload(&payload)
                .await
                .map_err(|error| format!("begin restore-visible snapshot backup: {error}"))?;
        }
        if payload.job_ids.is_empty() {
            match crate::snapshot_upload::submit_prepared_snapshot(&payload).await {
                Ok(submitted) => {
                    match crate::snapshot_upload_jobs::persist_submitted_transfer_ids(
                        &self.db, job_id, &submitted,
                    ) {
                        Ok(()) => payload = submitted,
                        Err(error) => {
                            // The durable worker may have persisted the same
                            // deterministic transfers or completed between our
                            // read and write. Join its payload instead of
                            // failing a valid handoff.
                            payload = self
                                .persisted_submitted_artifact_backup(job_id)
                                .map_err(|reload_error| {
                                    format!(
                                        "persist staged snapshot transfer IDs: {error}; {reload_error}"
                                    )
                                })?;
                        }
                    }
                }
                Err(error) => {
                    // A concurrent worker can move/delete the prepared
                    // metadata staging file after durably recording transfer
                    // IDs. Re-read before treating the local staging error as
                    // authoritative.
                    payload = self.persisted_submitted_artifact_backup(job_id).map_err(
                        |reload_error| {
                            format!("stage snapshot artifacts in vol-mgr: {error}; {reload_error}")
                        },
                    )?;
                }
            }
        }
        Ok(payload)
    }

    fn persisted_submitted_artifact_backup(
        &self,
        job_id: &JobId,
    ) -> Result<crate::snapshot_upload::SnapshotUploadJobs, String> {
        let job = self
            .snapshot_upload_worker
            .get_job(job_id)
            .map_err(|error| format!("re-read snapshot-upload job {job_id}: {error}"))?
            .ok_or_else(|| format!("snapshot-upload job {job_id} disappeared"))?;
        let payload = job
            .payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
            .map_err(|error| format!("decode snapshot-upload job {job_id}: {error}"))?;
        if payload.job_ids.is_empty() {
            return Err(format!(
                "snapshot-upload job {job_id} still has no durable transfer IDs"
            ));
        }
        Ok(payload)
    }

    async fn enqueue_suspend_artifact_backup(
        &self,
        sandbox: &crate::db::SandboxRow,
        local_artifact_id: LocalArtifactId,
        snapshot_dir: &str,
        format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
        dedup_key: Option<&str>,
    ) -> Result<SubmitSuspendSnapshotOutcome, Status> {
        let request_override = match self.resolve_org_snapshot_upload_config(&sandbox.id).await {
            OrgSnapshotConfigResolution::Resolved(config) => Some(config),
            OrgSnapshotConfigResolution::Absent
            | OrgSnapshotConfigResolution::ClusterRowMissing => None,
            OrgSnapshotConfigResolution::LookupFailed(reason) => {
                tracing::warn!(
                    sandbox_id = %sandbox.id,
                    reason,
                    "could not resolve org snapshot override; falling back to env-derived snapshot upload config",
                );
                None
            }
        };
        self.submit_artifact_backup(
            sandbox,
            ArtifactBackupSubmission {
                local_artifact_id,
                artifact_kind: pplx_space_common_core::types::ArtifactKind::Full,
                artifact_origin: pplx_space_common_core::types::ArtifactOrigin::Suspend,
                snapshot_name: None,
                backup_attempt_id: pplx_space_common_core::types::BackupAttemptId::new(),
                snapshot_dir,
                request_override: request_override.as_ref(),
                format,
                dedup_key,
                handoff: ArtifactBackupHandoff::TeardownFence,
            },
        )
        .await
        .map_err(artifact_backup_fence_status)
    }

    pub(super) async fn kill(
        &self,
        request: Request<KillRequest>,
    ) -> Result<Response<KillResponse>, Status> {
        self.authorize_peer(&request, "Kill", ALLOW_SANDBOX_OPERATOR)?;
        let _request_permit = if matches!(request.get_ref().signal, 9 | 15) {
            Some(self.cleanup_request_permit().await?)
        } else {
            None
        };
        let req = request.get_ref();
        let sandbox = self.lookup_sandbox(&req.sandbox_id)?;
        let id = &sandbox.id;
        let signal = req.signal;
        let blocking = req.blocking;

        if signal == 0 {
            return Err(Status::invalid_argument("signal must be non-zero"));
        }

        // Must be running or paused to receive a signal
        if !matches!(sandbox.state, SandboxState::Started | SandboxState::Paused) {
            return Err(Status::failed_precondition(format!(
                "cannot kill: sandbox is {}, not started or paused",
                sandbox.state
            )));
        }

        // Only fatal signals (SIGKILL=9, SIGTERM=15) trigger state transition and cleanup.
        // Non-fatal signals (e.g. SIGHUP, SIGUSR1) are just forwarded to the process.
        let is_fatal = signal == 9 || signal == 15;
        if !is_fatal {
            let mut node_client = connect_node()
                .await
                .map_err(|e| Status::internal(format!("cannot connect to node-manager: {e}")))?;
            node_client
                .stop_process(StopProcessRequest {
                    execution_fence: sandbox.node_execution_fence(),
                    sandbox_id: id.clone(),
                    signal,
                    pid: None,
                })
                .await
                .map_err(|e| Status::internal(format!("send signal failed: {e}")))?;
            return Ok(Response::new(KillResponse {}));
        }

        sandbox
            .state
            .transition(SandboxState::Stopping)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;

        let (sandbox_id, uuid8) = Self::cleanup_identity(id, Some(&sandbox.uuid8))?;
        let completion = CleanupCompletion::MarkStopped;
        let admission = self
            .admit_lifecycle_cleanup_for_observed_execution(
                sandbox_id,
                SandboxState::Stopping,
                true,
                completion,
                !blocking,
                Some(sandbox.space_api_route_identity.execution_generation),
            )
            .await
            .map_err(super::cleanup::CleanupAdmissionError::into_status)?;

        let execution_fence = sandbox.node_execution_fence();
        let dispatch_error = match self
            .dispatch_node_action_using_admission(sandbox_id, &admission, || async move {
                let mut node_client = connect_node().await.map_err(|error| {
                    Status::internal(format!("cannot connect to node-manager: {error}"))
                })?;
                node_client
                    .stop_process(StopProcessRequest {
                        execution_fence,
                        sandbox_id: id.clone(),
                        signal,
                        pid: None,
                    })
                    .await
                    .map(|_| ())
                    .map_err(|error| Status::internal(format!("send fatal signal failed: {error}")))
            })
            .await
        {
            Ok(CleanupDispatchDisposition::Dispatched) => None,
            Ok(CleanupDispatchDisposition::Superseded) => {
                return Err(Status::aborted(format!(
                    "fatal kill for sandbox {id} was superseded by a replacement execution"
                )));
            }
            Err(error) => {
                tracing::warn!(
                    sandbox_id = %id,
                    %error,
                    "fatal signal dispatch failed after cleanup admission; continuing cleanup"
                );
                Some(error)
            }
        };

        // Cleanup: stop sidecars, release resources, keep filesystem
        self.watchdog.remove_sandbox(id).await;
        self.snapshot_scheduler.remove_sandbox(id).await;

        if admission.is_unpersisted_for_sandbox(sandbox_id)? {
            let persistence_error = self
                .run_cleanup_after_persistence_failure(
                    SynchronousCleanupRequest {
                        sandbox_id: id,
                        uuid8: uuid8.as_str(),
                        keep_filesystem: true,
                        context: "cleanup after unpersisted kill",
                        completion,
                        published_artifact: None,
                        immediate_kill: Self::cleanup_immediate_kill(completion),
                        guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                    },
                    admission,
                )
                .await?;
            if let Some(error) = persistence_error {
                return Err(Status::internal(format!(
                    "runtime cleanup completed but stopping transition could not be persisted for sandbox {id}: {error}"
                )));
            }
            if let Some(error) = dispatch_error {
                return Err(error);
            }
            return Ok(Response::new(KillResponse {}));
        }
        if blocking {
            if let Err(error) = self
                .run_admitted_cleanup_to_completion(
                    SynchronousCleanupRequest {
                        sandbox_id: id,
                        uuid8: uuid8.as_str(),
                        keep_filesystem: true,
                        context: "cleanup after kill",
                        completion,
                        published_artifact: None,
                        immediate_kill: Self::cleanup_immediate_kill(completion),
                        guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                    },
                    admission,
                )
                .await
            {
                self.persist_and_retry_cleanup_failure(
                    sandbox_id,
                    uuid8,
                    true,
                    CleanupCompletion::MarkStopped,
                    error.message(),
                );
                return Err(error);
            }
        } else {
            self.schedule_admitted_async_cleanup(
                AsyncCleanupRequest {
                    sandbox_id,
                    uuid8,
                    keep_filesystem: true,
                    context: "cleanup after kill",
                    completion,
                    immediate_kill: Self::cleanup_immediate_kill(completion),
                    guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                    persist_intent: false,
                    cleanup_id: None,
                },
                admission,
            )
            .await;
        }

        if let Some(error) = dispatch_error {
            return Err(error);
        }
        Ok(Response::new(KillResponse {}))
    }
}

#[cfg(test)]
mod tests;
