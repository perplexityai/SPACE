use pplx_space_common_core::types::LocalArtifactId;

use super::suspend_snapshot_publication::{
    canonical_suspend_snapshot_dir, recovered_sandbox_row_fields,
};
use super::*;
use crate::service::start_support::start_stage;
use pplx_space_common_core::types::{
    ArtifactKind, ExactStorageUri, SandboxId, SnapshotId, StorageUri,
};
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventType, emit_sandbox_event,
};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug)]
pub(super) struct RestoreArtifactAsNewContext {
    pub(super) source_sandbox_id: SandboxId,
    pub(super) artifact_snapshot_id: SnapshotId,
    pub(super) target_owner: SandboxOwner,
    pub(super) artifact_kind: ArtifactKind,
    pub(super) target_disk_io_limits: Option<pplx_space_common_core::types::DiskIoLimits>,
}

impl RestoreArtifactAsNewContext {
    pub(super) fn from_proto(
        context: &crate::proto::RestoreArtifactAsNewContext,
        storage_uri: &StorageUri,
    ) -> Result<Self, Status> {
        let source_sandbox_id = context.source_sandbox_id.parse().map_err(|error| {
            Status::invalid_argument(format!(
                "invalid restore_artifact_as_new source_sandbox_id: {error}"
            ))
        })?;
        let artifact_snapshot_id = context.artifact_snapshot_id.parse().map_err(|error| {
            Status::invalid_argument(format!(
                "invalid restore_artifact_as_new artifact_snapshot_id: {error}"
            ))
        })?;
        let target_owner = context.target_owner.parse().map_err(|error| {
            Status::invalid_argument(format!(
                "invalid restore_artifact_as_new target_owner: {error}"
            ))
        })?;
        let artifact_kind = match crate::proto::ArtifactKind::try_from(context.artifact_kind) {
            Ok(crate::proto::ArtifactKind::Disk) => ArtifactKind::Disk,
            Ok(crate::proto::ArtifactKind::Full) => {
                return Err(Status::unimplemented(
                    "full artifact restore-as-new is reserved for rich runtime fork",
                ));
            }
            _ => {
                return Err(Status::invalid_argument(
                    "restore_artifact_as_new.artifact_kind must be DISK or FULL",
                ));
            }
        };
        let target_disk_io_limits = if context.disk_io_max_operations_per_sec.is_some()
            || context.disk_io_max_bandwidth_mib_per_sec.is_some()
        {
            Some(
                pplx_space_common_core::types::DiskIoLimits::try_from_optional(
                    context.disk_io_max_operations_per_sec,
                    context.disk_io_max_bandwidth_mib_per_sec,
                )
                .map_err(|error| Status::invalid_argument(error.to_string()))?,
            )
        } else {
            None
        };
        let mut uri_segments = storage_uri.key_prefix.rsplit('/');
        let uri_snapshot_id = uri_segments
            .next()
            .and_then(|value| value.parse::<SnapshotId>().ok());
        if uri_snapshot_id != Some(artifact_snapshot_id) {
            return Err(Status::invalid_argument(
                "artifact snapshot identity does not match its storage URI",
            ));
        }
        let uri_source_sandbox_id = uri_segments
            .next()
            .and_then(|value| value.parse::<SandboxId>().ok());
        if uri_source_sandbox_id != Some(source_sandbox_id) {
            return Err(Status::invalid_argument(
                "artifact source sandbox identity does not match its storage URI",
            ));
        }

        Ok(Self {
            source_sandbox_id,
            artifact_snapshot_id,
            target_owner,
            artifact_kind,
            target_disk_io_limits,
        })
    }
}

const QUARANTINE_RECOVERY_CLAIM_HEARTBEAT: std::time::Duration =
    std::time::Duration::from_secs(5 * 60);
// Exceeds the two sequential volume-manager wait budgets and still bounds a
// filesystem stall. The staging-lock wait is bounded separately and far more
// tightly, by `ORPHAN_STAGING_ADMISSION_WAIT`. `Duration` arithmetic is not const,
// so the budget is composed in seconds and converted once, here.
const QUARANTINE_RECOVERY_OPERATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(
    2 * crate::snapshot_restore::RESTORE_TRANSFER_WAIT_MAX_TOTAL.as_secs() + 2 * 60 * 60,
);

/// How long an orphan restore waits for the sandbox's staging lock before giving
/// the attempt up.
///
/// Neither extreme works. Waiting indefinitely parks the sole node-wide restore
/// permit behind one sandbox for as long as the holder keeps it, which is what the
/// fail-fast admission exists to prevent. Failing on the first collision converts a
/// momentary holder — a concurrent `Start`, a cleanup runner, a delete — into
/// `OrphanRestoreLocalContention`, which charges the attempt and reschedules through
/// `quarantine_recovery_backoff`: five sub-second collisions take an episode that
/// needed to wait milliseconds out to the 15-minute backoff cap.
///
/// Five seconds is long enough to absorb any hand-off that is not itself a
/// long-running operation, and short enough that parking the permit for it is
/// irrelevant against a restore measured in hours.
const ORPHAN_STAGING_ADMISSION_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// Drive one claimed recovery operation to completion under its claim.
///
/// Three terminal outcomes, in `biased` priority order: the operation finishes,
/// the operation deadline fires, or a heartbeat finds the claim is no longer
/// ours. Renewal is a short synchronous SQLite write, so it takes a closure
/// rather than a future.
/// A renewal that failed rather than reporting the claim lost.
///
/// A single failed renewal is not evidence the claim is gone. Aborting on the
/// first one throws away a multi-hour transfer at an arbitrary await point —
/// after the canonical subvolume has already been destroyed — for what is
/// usually a transient `SQLITE_BUSY` against a database several tasks share.
/// Renewal is retried across consecutive heartbeats, and only a lease that
/// actually lapses ends the operation, which the next fenced write catches
/// anyway.
///
/// Two, not three, and the arithmetic is the reason. The first `interval` tick is
/// consumed at t=0, so renewals land at 300s, 600s, 900s. With a budget of two the
/// abort is the second strike, at 600s — one full heartbeat inside the 900s lease.
/// At three it was the strike at 900s, coinciding exactly with expiry — and
/// `MissedTickBehavior::Delay` plus a blocking SQLite write against a shared mutex
/// means such a tick can land *after* expiry:
/// the operation would then keep running with no lease at all, which is the
/// overlap the `Ok(false)` arm below refuses to allow for even one heartbeat.
/// Both arms should reach the same conclusion about running unleased.
const QUARANTINE_RECOVERY_RENEWAL_FAILURES_BEFORE_ABORT: u32 = 2;

async fn drive_under_quarantine_recovery_claim<Operation, Renew, Provenance>(
    operation: Operation,
    sandbox_id: SandboxId,
    heartbeat_period: std::time::Duration,
    operation_timeout: std::time::Duration,
    shutdown_token: tokio_util::sync::CancellationToken,
    renew_claim: Renew,
) -> Result<Provenance, crate::snapshot_restore::RestoreError>
where
    Operation: Future<Output = Result<Provenance, crate::snapshot_restore::RestoreError>>,
    Renew: Fn() -> Result<bool, crate::snapshot_restore::RestoreError>,
{
    tokio::pin!(operation);
    let mut heartbeat = tokio::time::interval(heartbeat_period);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick of a fresh interval is immediate; consume it so the claim
    // acquired moments ago is not renewed straight away.
    heartbeat.tick().await;
    let operation_deadline = tokio::time::sleep(operation_timeout);
    tokio::pin!(operation_deadline);
    let mut consecutive_renewal_failures = 0_u32;

    loop {
        tokio::select! {
            biased;
            result = &mut operation => return result,
            // Without this arm the operation runs to its multi-hour deadline
            // through shutdown, so a SIGTERM ends as a SIGKILL mid-transfer.
            //
            // The caller then records a no-progress result, which releases the
            // lease and schedules the retry one backoff out — so the next process
            // resumes the episode on its next tick. Leaving the lease alone
            // instead, as an earlier revision did, made the episode wait out the
            // remainder of its 15-minute lease first.
            () = shutdown_token.cancelled() => {
                return Err(
                    crate::snapshot_restore::RestoreError::QuarantineRecoveryAbandonedAtShutdown {
                        sandbox_id,
                    },
                );
            }
            () = &mut operation_deadline => {
                return Err(crate::snapshot_restore::RestoreError::QuarantineRecoveryTimedOut {
                    sandbox_id,
                    timeout: operation_timeout,
                });
            }
            _ = heartbeat.tick() => {
                match renew_claim() {
                    Ok(true) => consecutive_renewal_failures = 0,
                    Ok(false) => {
                        // Abort immediately. A grace period here — letting the
                        // operation finish in case the lost claim was its own doing
                        // — was tried and reverted: it cannot help, and it hurts.
                        //
                        // It cannot help because a recovery that retires its own
                        // claim never yields afterwards. `finalize_restore` commits
                        // the publish synchronously under the database mutex and the
                        // future returns without a further await, so the heartbeat
                        // has no point at which to observe the post-publish state;
                        // the operation arm of this `select!` completes first.
                        //
                        // It hurts because the case that does reach here is a
                        // genuinely superseded operation, and a grace period lets it
                        // keep publishing canonical restore metadata for another
                        // heartbeat before the final CAS refuses it. Awaiting the
                        // operation inside this arm also parks the `select!`, so
                        // neither shutdown nor the operation deadline can interrupt
                        // it while it runs.
                        return Err(
                            crate::snapshot_restore::RestoreError::QuarantineRecoveryClaimSuperseded {
                                sandbox_id,
                            },
                        );
                    }
                    Err(error) => {
                        consecutive_renewal_failures =
                            consecutive_renewal_failures.saturating_add(1);
                        if consecutive_renewal_failures
                            >= QUARANTINE_RECOVERY_RENEWAL_FAILURES_BEFORE_ABORT
                        {
                            return Err(error);
                        }
                        tracing::warn!(
                            %sandbox_id,
                            consecutive_renewal_failures,
                            %error,
                            "quarantine recovery claim renewal failed; retrying on the next heartbeat"
                        );
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RestoreSourceSelection {
    RemoteLocalFilesMissing,
    RemoteUriSelected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SuspendRestoreDecision {
    /// Reuse the suspend snapshot bytes already materialized on this node
    /// (legacy in-place restore, or a same-node restore on the source node).
    UseLocal,
    /// Stage the snapshot from S3: either the URI threaded into the request, or
    /// the sandbox's latest ready backup resolved from cluster-api.
    StageRemote(RestoreSourceSelection),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SuspendRestorePlacement {
    Legacy,
    SourceNode,
    OtherNode,
    RemoteOnly,
}

#[derive(Debug, Clone)]
pub(super) struct CrossNodeRestoreProvenance {
    pub(super) storage_uri: String,
    pub(super) local_artifact_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CrossNodeRestoreStagingAdmission {
    /// Caller-cancellable, bounded admission before any staging side effects.
    Foreground,
    /// Give the staging lock at most this long, then report contention.
    ///
    /// The budget is per caller rather than a constant on the variant, because the
    /// two callers want different things and a shared value silently changed one of
    /// them: the orphan path wants a bounded wait so a momentary holder does not
    /// cost it an attempt and an exponential backoff, while the `Start` retry path
    /// already peeks the lock non-blockingly and answers `unavailable`, so it wants
    /// the instantaneous attempt it had before. `Duration::ZERO` is that attempt —
    /// `timeout` polls the acquisition once, which takes a free lock and gives up on
    /// a held one — and forgetting the distinction degrades to the stricter of the
    /// two rather than to a silent wait.
    WaitAtMost(std::time::Duration),
}

/// Reacquire cleanup-then-staging ownership for the canonical-root reclamation.
///
/// Cleanup first, then staging: that is the order cleanup itself takes them in, and
/// taking them the other way round here would invert it. Both block. A non-blocking
/// acquisition would end the attempt on any momentary lock holder, returning
/// `RestoreOperationBusy` with no retry of either fence, and it is reached only
/// after the staging transfer has completed — so what it would give up is the
/// attempt's whole progress toward the destructive boundary.
///
/// The caller owns the reasoning about how long that wait may last and what bounds
/// it; see the call site.
///
/// This cannot starve the node-wide orphan-restore permit: the block that calls this
/// runs only when `conflicting_canonical_source.is_some()`, which requires
/// `retrying_interrupted_restore`, which is set only on the `recovery.is_none()`
/// arm. The orphan quarantine-recovery path always passes `recovery: Some(..)` and
/// never reaches here.
async fn acquire_canonical_reclamation_fences(
    cleanup_owner: crate::service::cleanup_support::CleanupLockGuard,
    staging_lock: Arc<crate::auto_lifecycle::LifecycleMutex>,
) -> (
    crate::service::cleanup_support::OwnedCleanupFenceGuard,
    crate::auto_lifecycle::LifecycleMutexGuard,
) {
    let cleanup_fence = cleanup_owner.lock_owned().await;
    let staging_guard = staging_lock.lock_owned().await;
    (cleanup_fence, staging_guard)
}

/// Which local-row outcome a plain same-id cross-node restore converges on.
///
/// The quarantine-recovery and restore-as-new arms select themselves from
/// their own context fields, so this only distinguishes the two cases that
/// otherwise look identical on the wire: both are a `checkpoint = "suspend"`
/// restore of this sandbox's own bytes from an exact storage URI. What
/// separates them is the artifact being staged and the lifecycle the caller
/// asked to land in.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CrossNodeRestoreTarget {
    /// Stage a full suspend checkpoint and leave the row `Suspended` for the
    /// caller to resume. Every caller that predates the stopped evacuation.
    SuspendedCheckpoint,
    /// Stage an evacuated `stop`-origin disk artifact and leave the row
    /// `Stopped` with that artifact pinned as its current one. Requested by
    /// `WAKE_TARGET_STATE_STOPPED`; the sandbox never boots.
    ///
    /// Carries the artifact the caller says the URI resolves to. It travels
    /// with the target rather than being compared afterwards so the check can
    /// happen *before* the rootfs transfer and before the row is committed —
    /// a disagreement found after the publication has already written the pin
    /// costs a multi-gigabyte pull and leaves a row the boundary then refuses.
    StoppedDiskArtifact {
        expected: pplx_space_common_core::types::LocalArtifactId,
    },
}

pub(super) struct CrossNodeRestoreRequest<'a> {
    pub(super) sandbox_id: &'a str,
    pub(super) storage_uri: Option<&'a str>,
    pub(super) execution_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
    pub(super) activation_attempt_id:
        Option<pplx_space_common_core::types::SandboxActivationAttemptId>,
    pub(super) recovery: Option<QuarantineRecoveryContext<'a>>,
    pub(super) restore_artifact_as_new: Option<&'a RestoreArtifactAsNewContext>,
    pub(super) target: CrossNodeRestoreTarget,
    pub(super) staging_admission: CrossNodeRestoreStagingAdmission,
}

#[derive(Clone, Copy)]
pub(super) struct QuarantineRecoveryContext<'a> {
    plan: &'a crate::snapshot_restore::QuarantineRecoveryPlan,
    claim: &'a crate::db::ClaimedQuarantineRecoveryCandidate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RestoreAsNewMetadataDestination {
    Scoped,
    Canonical,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CanonicalRootReclaimDecision {
    Reclaim,
    AlreadyVacant,
}

fn canonical_root_reclaim_decision(
    initially_observed_source: &ExactStorageUri,
    current_source: Option<&ExactStorageUri>,
    canonical_root_exists: bool,
) -> Result<CanonicalRootReclaimDecision, crate::snapshot_restore::RestoreError> {
    match (current_source, canonical_root_exists) {
        (Some(current), true) if current == initially_observed_source => {
            Ok(CanonicalRootReclaimDecision::Reclaim)
        }
        (None, false) => Ok(CanonicalRootReclaimDecision::AlreadyVacant),
        _ => Err(crate::snapshot_restore::RestoreError::Other(
            "canonical restore root changed while replacement artifacts were staged; retry"
                .to_string(),
        )),
    }
}

fn ensure_staged_restore_source(
    expected_source: &ExactStorageUri,
    current_source: Option<&ExactStorageUri>,
) -> Result<(), crate::snapshot_restore::RestoreError> {
    if current_source == Some(expected_source) {
        return Ok(());
    }
    Err(crate::snapshot_restore::RestoreError::Other(
        "restore transfer scope changed while canonical-root reclamation was fenced; retry"
            .to_string(),
    ))
}

fn restore_as_new_metadata_destination(
    retrying_interrupted_restore: bool,
    canonical_dir_exists: bool,
) -> RestoreAsNewMetadataDestination {
    if retrying_interrupted_restore && canonical_dir_exists {
        RestoreAsNewMetadataDestination::Canonical
    } else {
        RestoreAsNewMetadataDestination::Scoped
    }
}

fn log_interrupted_restore_completion(canonical_id: &str, storage_uri: &str) {
    tracing::info!(
        sandbox_id = %canonical_id,
        storage_uri_len = storage_uri.len() as u64,
        "completed interrupted restore from its published artifacts"
    );
}

/// Records a staging failure, fenced on the recovery claim when there is one.
///
/// The claim has to reach this. Every caller runs after a transfer that can outlive
/// the claim that authorized it, so an unfenced write here lets a spent claimant
/// stamp `failure_reason` onto a row whose new owner is mid-restore — the state guard
/// alone does not stop it, because the new owner has put the row back in `restoring`.
fn persist_cross_node_staging_failure(
    conn: &rusqlite::Connection,
    sandbox_id: SandboxId,
    stage: &'static str,
    error: &crate::snapshot_restore::RestoreError,
    claim: Option<&crate::db::ClaimedQuarantineRecoveryCandidate>,
) -> Result<(), crate::snapshot_restore::RestoreError> {
    let failure_reason = format!("cross-node restore {stage} failed: {error}");
    let marked = match claim {
        Some(claimed) => {
            // The fenced statement targets the claim's own sandbox, so the two have to
            // be the same row. A `debug_assert!` was not enough: it is compiled out of
            // the release build, which is the only build where this would misattribute
            // a failure to the quarantined source and still report success. Refuse
            // instead, in every build.
            if sandbox_id != claimed.candidate.sandbox_id {
                return Err(
                    crate::snapshot_restore::RestoreError::QuarantineRecoveryClaimSuperseded {
                        sandbox_id,
                    },
                );
            }
            crate::db::mark_cross_node_restore_staging_failed_under_recovery_claim(
                conn,
                &failure_reason,
                claimed,
            )
        }
        None => crate::db::mark_cross_node_restore_staging_failed(
            conn,
            &sandbox_id.to_string(),
            &failure_reason,
        ),
    }
    .map_err(|source| crate::snapshot_restore::RestoreError::AdvanceRestorePhase { source })?;
    if !marked {
        return Err(
            crate::snapshot_restore::RestoreError::RestorePhaseTransitionLost { sandbox_id },
        );
    }
    Ok(())
}

fn select_interrupted_restore_source(
    incoming_source: Option<&str>,
    persisted_source: Option<ExactStorageUri>,
) -> Result<Option<ExactStorageUri>, crate::snapshot_restore::RestoreError> {
    incoming_source
        .map(ExactStorageUri::parse)
        .transpose()
        .map_err(
            |source| crate::snapshot_restore::RestoreError::InvalidRestoreSource {
                uri: incoming_source.unwrap_or_default().to_string(),
                source,
            },
        )
        .map(|incoming| incoming.or(persisted_source))
}

fn persist_selected_interrupted_restore_source(
    conn: &rusqlite::Connection,
    sandbox_id: SandboxId,
    persisted_source: &ExactStorageUri,
    selected_source: &ExactStorageUri,
) -> Result<(), crate::snapshot_restore::RestoreError> {
    if persisted_source == selected_source {
        return Ok(());
    }
    let replaced = crate::db::replace_retryable_remote_restore_source(
        conn,
        &sandbox_id.to_string(),
        persisted_source.as_wire(),
        selected_source.as_wire(),
    )
    .map_err(|source| crate::snapshot_restore::RestoreError::AdvanceRestorePhase { source })?;
    if !replaced {
        return Err(
            crate::snapshot_restore::RestoreError::RestorePhaseTransitionLost { sandbox_id },
        );
    }
    Ok(())
}

fn snapshot_id_from_storage_uri(storage_uri: &str) -> Option<String> {
    let parsed = StorageUri::parse(storage_uri).ok()?;
    parsed
        .key_prefix
        .rsplit('/')
        .next()
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn snapshot_metadata_sha256(path: &std::path::Path) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    Some(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
}

fn snapshot_restore_event_details(
    staged: &crate::snapshot_restore::RestoreMetadataStaged,
    details: SandboxEventDetails,
) -> SandboxEventDetails {
    let meta = &staged.meta;
    let mut details = super::create_support::mark_restore_storage_uri_present(
        details
            // The staged sidecar's own stamp: cross-node restore has no local row
            // to read, and without this the encrypted restore events omit the
            // field their matching metrics carry.
            .bool_field(
                pplx_space_events::SandboxEventBoolField::VolumeEncryption,
                meta.encrypted,
            )
            .operation("cross_node")
            .template_id(meta.source.template_id.as_str())
            .restore_codec(pplx_space_metrics::snapshot::codec_tag_from_diff_format(
                meta.rootfs_diff_format.as_deref(),
            )),
    );
    if let Some(content_hash) = meta.source.content_hash.as_deref() {
        details = details.template_content_hash(content_hash);
    }
    if let Some(snapshot_id) = snapshot_id_from_storage_uri(&staged.storage_uri) {
        details = details.restored_snapshot_id(snapshot_id);
    }
    if let Some(metadata_sha256) = snapshot_metadata_sha256(
        &staged
            .snapshot_dir
            .join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME),
    ) {
        details = details.snapshot_metadata_sha256(metadata_sha256);
    }
    if let Some(rootfs_version) = meta.source.rootfs_version.as_deref() {
        details = details.snapshot_rootfs_version(rootfs_version);
    }
    if let Some(kernel_version) = meta.source.kernel_version.as_deref() {
        details = details.snapshot_kernel_version(kernel_version);
    }
    if let Some(rootfs_diff_format) = meta.rootfs_diff_format.as_deref() {
        details = details.rootfs_diff_format(rootfs_diff_format);
    }
    if let Some(execution) = &meta.execution {
        details = details.execution_metadata(execution.execution());
    }
    details
}

pub(super) fn suspend_restore_placement(
    source_node: Option<&str>,
    current_node: Option<&str>,
    selected_storage_uri: bool,
) -> SuspendRestorePlacement {
    match source_node {
        Some(source_node) if Some(source_node) == current_node => {
            SuspendRestorePlacement::SourceNode
        }
        Some(_) => SuspendRestorePlacement::OtherNode,
        None if selected_storage_uri => SuspendRestorePlacement::RemoteOnly,
        None => SuspendRestorePlacement::Legacy,
    }
}

pub(super) fn decide_suspend_restore(
    placement: SuspendRestorePlacement,
    selected_storage_uri: bool,
    local_snapshot_missing: bool,
) -> SuspendRestoreDecision {
    match placement {
        // The source node (or a legacy in-place restore) still holds the local
        // suspend snapshot: restore from local bytes.
        SuspendRestorePlacement::SourceNode | SuspendRestorePlacement::Legacy
            if !local_snapshot_missing =>
        {
            SuspendRestoreDecision::UseLocal
        }
        // The planner threaded a specific remote backup URI: stage that URI.
        _ if selected_storage_uri => {
            SuspendRestoreDecision::StageRemote(RestoreSourceSelection::RemoteUriSelected)
        }
        // Local bytes are unusable on this node and no URI was threaded into the
        // request. Resolve the sandbox's latest ready backup from cluster-api at
        // stage time rather than failing here; a sandbox with no registered
        // ready snapshot fails downstream as SnapshotNotRegistered.
        _ => SuspendRestoreDecision::StageRemote(RestoreSourceSelection::RemoteLocalFilesMissing),
    }
}

impl RestoreSourceSelection {
    const fn reason_tag(self) -> &'static str {
        match self {
            Self::RemoteLocalFilesMissing => "local_files_missing",
            Self::RemoteUriSelected => "remote_uri_selected",
        }
    }
}

impl std::fmt::Display for RestoreSourceSelection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.reason_tag())
    }
}

fn validate_quarantine_recovery_artifact_identity(
    sandbox_id: &str,
    expected: Option<LocalArtifactId>,
    observed: Option<LocalArtifactId>,
) -> Result<Option<LocalArtifactId>, crate::snapshot_restore::RestoreError> {
    if expected == observed {
        return Ok(observed);
    }
    Err(crate::snapshot_restore::RestoreError::Other(format!(
        "recovery artifact identity changed for {sandbox_id}: expected {expected:?}, observed {observed:?}"
    )))
}

#[derive(Clone, Copy)]
struct RestoringRowInput<'a> {
    meta: &'a crate::snapshot_upload::SnapshotMeta,
    restore_source: &'a pplx_space_common_core::types::ExactStorageUri,
    disk_io_limits_override: Option<pplx_space_common_core::types::DiskIoLimits>,
}

impl<'a> RestoringRowInput<'a> {
    fn new(
        meta: &'a crate::snapshot_upload::SnapshotMeta,
        restore_source: &'a pplx_space_common_core::types::ExactStorageUri,
        disk_io_limits_override: Option<pplx_space_common_core::types::DiskIoLimits>,
    ) -> Self {
        Self {
            meta,
            restore_source,
            disk_io_limits_override,
        }
    }
}

/// Adopt the orphan's sandbox row and advance its recovery generation as one
/// transaction, with the claim checked first.
///
/// Both writes have to land together. A task whose claim was superseded — its
/// lease lapsed and another pass re-claimed the episode — would otherwise insert
/// a `restoring` sandbox row for the orphan and only then fail the generation
/// CAS, leaving that row behind for an operation that no longer owns the
/// episode. The two are one transaction now, so a refused CAS rolls the adoption
/// back with it and a stale task leaves no trace.
fn adopt_restoring_row_under_recovery_claim(
    conn: &rusqlite::Connection,
    sandbox_id: SandboxId,
    canonical_id: &str,
    row: RestoringRowInput<'_>,
    recovery: Option<&QuarantineRecoveryContext<'_>>,
    retrying_interrupted_restore: bool,
) -> Result<(), crate::snapshot_restore::RestoreError> {
    let RestoringRowInput {
        meta,
        restore_source,
        disk_io_limits_override,
    } = row;
    // The ordinary retry writes nothing: with no recovery context there is no
    // CAS, and a retry does not re-insert the row, so all that is left is an
    // identity comparison. Opening a transaction for it would take the node
    // database's write lock and commit empty, serializing every cross-node retry
    // behind a no-op.
    if retrying_interrupted_restore && recovery.is_none() {
        return if meta.sandbox_id == canonical_id {
            Ok(())
        } else {
            Err(
                crate::snapshot_restore::RestoreError::SnapshotIdentityMismatch {
                    expected: sandbox_id,
                    actual: meta.sandbox_id.clone(),
                },
            )
        };
    }
    let tx = crate::db::immediate_transaction(conn)
        .map_err(|source| crate::snapshot_restore::RestoreError::AdvanceRestorePhase { source })?;
    // Check recovery authority against the state observed before the upsert.
    // The generation CAS below sees the newly written Restoring state, so it
    // cannot independently prove the prior row was still eligible for recovery.
    if recovery.is_some() {
        use rusqlite::OptionalExtension;
        let existing_state: Option<String> = tx
            .query_row(
                pplx_space_common_core::sql!("SELECT state FROM sandboxes WHERE id = ?1"),
                rusqlite::params![canonical_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(
                |source| crate::snapshot_restore::RestoreError::AdvanceRestorePhase { source },
            )?;
        // An absent row is the orphan case this path exists for; the upsert
        // inserts it. Only an existing row in an unexpected state is a refusal.
        //
        // Parsed into `SandboxState` rather than compared as text: the set is
        // closed, and an unparseable value has to be refused too — matching raw
        // literals would silently treat a corrupt state as "not quarantined or
        // restoring" for the right reason and a garbage state as the same thing
        // for the wrong one, with no way to tell them apart in the log.
        if let Some(state) = existing_state
            && !state.parse::<SandboxState>().is_ok_and(|state| {
                matches!(state, SandboxState::Quarantined | SandboxState::Restoring)
            })
        {
            tracing::info!(
                %sandbox_id,
                state,
                "refusing quarantine recovery adoption: the sandbox left an \
                 activatable state after the claim was taken"
            );
            return Err(
                crate::snapshot_restore::RestoreError::QuarantineRecoveryClaimSuperseded {
                    sandbox_id,
                },
            );
        }
    }
    if retrying_interrupted_restore {
        if meta.sandbox_id != canonical_id {
            return Err(
                crate::snapshot_restore::RestoreError::SnapshotIdentityMismatch {
                    expected: sandbox_id,
                    actual: meta.sandbox_id.clone(),
                },
            );
        }
    } else {
        insert_restoring_sandbox_row_from_snapshot(
            &tx,
            canonical_id,
            meta,
            restore_source,
            disk_io_limits_override,
        )?;
    }
    // The row has to exist before this CAS: it updates `sandboxes` and requires
    // `state = 'restoring'`. Atomicity is what protects against a stale task, not
    // ordering — a refusal returns here, the transaction is dropped without a
    // commit, and the adoption above is rolled back with it. Before that, the row
    // insert committed on its own and outlived the failure.
    if let Some(recovery) = recovery
        && !crate::db::set_orphan_quarantine_recovery_generation(
            &tx,
            recovery.claim,
            recovery.plan.execution_generation,
        )
        .map_err(|source| crate::snapshot_restore::RestoreError::AdvanceRestorePhase { source })?
    {
        // A refusal here is *usually* a lost claim, and reporting it as a generic
        // lost phase transition hid the superseded case entirely — hence the
        // dedicated reason. But "only when the claim is no longer current" would
        // overstate it: this CAS also refuses when `current_local_artifact_id` is
        // set, and when the fence's `MustBeAbsent` cleanup guard finds a cleanup
        // episode. Both surface as `claim_superseded` too, so an operator reading
        // that label cannot tell a claim race from a cleanup owner arriving
        // mid-restore. Splitting them needs the refusing conjunct back from the
        // database layer, which is a wider change than this one.
        return Err(
            crate::snapshot_restore::RestoreError::QuarantineRecoveryClaimSuperseded { sandbox_id },
        );
    }
    tx.commit()
        .map_err(|source| crate::snapshot_restore::RestoreError::AdvanceRestorePhase { source })
}

/// Insert or adopt the `restoring` sandbox row for a cross-node restore.
///
/// `conn` must already be inside a transaction: the caller adopts the row and
/// advances the recovery generation together, so this uses the in-transaction
/// variant rather than opening and committing its own.
pub(super) fn insert_restoring_sandbox_row_from_snapshot(
    conn: &rusqlite::Connection,
    canonical_id: &str,
    meta: &crate::snapshot_upload::SnapshotMeta,
    restore_source: &pplx_space_common_core::types::ExactStorageUri,
    disk_io_limits_override: Option<pplx_space_common_core::types::DiskIoLimits>,
) -> Result<(), crate::snapshot_restore::RestoreError> {
    let sandbox_id = SandboxId::from_str(canonical_id).map_err(|source| {
        crate::snapshot_restore::RestoreError::InvalidSandboxId {
            value: canonical_id.to_string(),
            source,
        }
    })?;
    if meta.sandbox_id != canonical_id {
        return Err(
            crate::snapshot_restore::RestoreError::SnapshotIdentityMismatch {
                expected: sandbox_id,
                actual: meta.sandbox_id.clone(),
            },
        );
    }
    let uuid8 = SandboxRuntimeLabel::from_sandbox_id(sandbox_id);
    let mut recovered_row = recovered_sandbox_row_fields(meta).map_err(|source| {
        crate::snapshot_restore::RestoreError::InvalidRecoveredSandboxRow { source }
    })?;
    if let Some(disk_io_limits) = disk_io_limits_override {
        recovered_row.disk_io_limits = disk_io_limits;
    }
    let restore_operation =
        crate::restore_operation::RemoteRestoreOperation::metadata_staging(restore_source.clone());

    recovered_row
        .with_insert_params(
            canonical_id,
            &uuid8,
            SandboxState::Restoring,
            None,
            |params| {
                crate::db::restore_adopt_orphan_or_insert_sandbox_in_transaction(
                    conn,
                    params,
                    &restore_operation,
                )
            },
        )
        .map_err(|source| crate::snapshot_restore::RestoreError::PersistRestoringRow { source })
}

pub(super) fn transition_restored_sandbox_to_suspended(
    conn: &rusqlite::Connection,
    canonical_id: &str,
    meta: &crate::snapshot_upload::SnapshotMeta,
) -> Result<(), crate::snapshot_restore::RestoreError> {
    transition_restored_sandbox_to_suspended_parts(
        conn,
        canonical_id,
        meta.local_artifact_id,
        meta.artifact_kind,
        &format!("{CACHE_DIR}/{canonical_id}/snapshot"),
    )
}

fn transition_restored_sandbox_to_suspended_parts(
    conn: &rusqlite::Connection,
    canonical_id: &str,
    local_artifact_id: Option<LocalArtifactId>,
    artifact_kind: Option<pplx_space_common_core::types::ArtifactKind>,
    snapshot_dir: &str,
) -> Result<(), crate::snapshot_restore::RestoreError> {
    if let Some(local_artifact_id) = local_artifact_id {
        if artifact_kind != Some(pplx_space_common_core::types::ArtifactKind::Full) {
            return Err(crate::snapshot_restore::RestoreError::Other(
                "checkpoint restore backup metadata must identify a full artifact".to_string(),
            ));
        }
        crate::db::publish_restored_suspended_artifact(
            conn,
            canonical_id,
            local_artifact_id,
            snapshot_dir,
        )
        .map_err(|error| {
            crate::snapshot_restore::RestoreError::Other(format!(
                "persist restored local artifact identity: {error}"
            ))
        })?;
    } else {
        crate::db::publish_restored_suspended_without_artifact(conn, canonical_id).map_err(
            |e| {
                crate::snapshot_restore::RestoreError::Other(format!(
                    "transition restoring->suspended: {e}"
                ))
            },
        )?;
    }
    Ok(())
}

/// Whether an artifact's kind and origin make it a disk image a stop minted —
/// the only artifact an evacuated stopped sandbox may be materialized from or
/// left pinning. Shared by the staged-sidecar check and the local-row check so
/// the two can't disagree on what "evacuated disk" means.
pub(super) fn is_stop_origin_disk(
    kind: Option<pplx_space_common_core::types::ArtifactKind>,
    origin: Option<pplx_space_common_core::types::ArtifactOrigin>,
) -> bool {
    kind == Some(pplx_space_common_core::types::ArtifactKind::Disk)
        && origin == Some(pplx_space_common_core::types::ArtifactOrigin::Stop)
}

/// The artifact a same-id stopped materialization is allowed to pin.
///
/// Returns the pin rather than a bool so no caller can reach the `UPDATE`
/// without having proved the three things that make pinning it correct: the
/// metadata names a local artifact, that artifact is a disk image, and a stop
/// minted it. A `suspend`-origin full checkpoint, a user's named snapshot, and
/// a fork's immutable source all fail here — each would leave the sandbox
/// pinned to bytes that are not the disk it was evacuated with.
fn stopped_materialization_pin(
    meta: &crate::snapshot_upload::SnapshotMeta,
    expected: pplx_space_common_core::types::LocalArtifactId,
) -> Result<pplx_space_common_core::types::LocalArtifactId, crate::snapshot_restore::RestoreError> {
    let unsuitable =
        || crate::snapshot_restore::RestoreError::StoppedMaterializationArtifactUnsuitable {
            kind: meta.artifact_kind,
            origin: meta.artifact_origin,
            local_artifact_id: meta.local_artifact_id,
        };
    // An encrypted capture stages only ciphertext under `{id}/snapshot` and no
    // `instance/rootfs.ext4`, so the stopped boundary could never vouch for it;
    // refusing here keeps that from surfacing only after the row is committed.
    if !is_stop_origin_disk(meta.artifact_kind, meta.artifact_origin) || meta.encrypted {
        return Err(unsuitable());
    }
    let pin = meta.local_artifact_id.ok_or_else(unsuitable)?;
    // The caller named the artifact it believes this URI resolves to. Comparing
    // it here, not after the finalize, is what keeps a disagreement from
    // costing a multi-gigabyte transfer and a committed row.
    if pin != expected {
        return Err(
            crate::snapshot_restore::RestoreError::StoppedMaterializationArtifactMismatch {
                expected,
                staged: pin,
            },
        );
    }
    Ok(pin)
}

/// Finalize an evacuated stopped sandbox at its own identity.
///
/// The third arm alongside [`transition_restored_sandbox_to_suspended`] and
/// the restore-as-new publication. It is a separate arm rather than a relaxed
/// conditional on either because its postcondition is the opposite of
/// restore-as-new's: a fork stages the disk as an immutable source and drops
/// the pin twice over so it cannot become the child's checkpoint — once in the
/// staged sidecar (`rebind_artifact_snapshot_identity`) and again in the local
/// row (`publish_restored_disk_as_new`) — while an evacuation needs it — it is
/// the only name its durable copy has. `GetArtifactBackupLocation` selects a
/// ready remote copy for one exact `local_artifact_id`, so dropping the pin
/// leaves the bytes in object storage with nothing on this node able to ask
/// for them. Nothing is rebound here either: at the same id there is no new
/// owner to rebind to.
///
/// The recorded `local_path` is the `{id}/snapshots/d-stop-{artifact}` name a
/// locally stopped sandbox's pin uses (`artifact_backup.rs` builds it with
/// `sandbox_snapshot_dir`), not `{id}/snapshot`, the staging path the restore
/// wrote. Nothing exists at that name here — the bytes were staged, not
/// snapshotted — and nothing needs it to: starting a `Stopped` sandbox goes
/// through `template_source_for_sandbox_prepare`, which keys on
/// `{id}/instance/rootfs.ext4` — what `restore_pull_rootfs` stages — and never
/// reads `local_artifacts.local_path`. What the name buys is that the row never
/// shares a path with the suspend checkpoint, so whatever later stamps or reaps
/// it can't touch `{id}/snapshot`. The pin's job is identity — naming the
/// durable copy the cluster holds — not the boot.
pub(super) fn transition_restored_sandbox_to_stopped(
    conn: &rusqlite::Connection,
    canonical_id: &str,
    meta: &crate::snapshot_upload::SnapshotMeta,
    expected: pplx_space_common_core::types::LocalArtifactId,
) -> Result<(), crate::snapshot_restore::RestoreError> {
    let local_artifact_id = stopped_materialization_pin(meta, expected)?;
    crate::db::publish_restored_stopped_artifact(
        conn,
        canonical_id,
        local_artifact_id,
        &pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir(
            canonical_id,
            &format!("d-stop-{local_artifact_id}"),
        ),
    )
    .map_err(|error| {
        crate::snapshot_restore::RestoreError::Other(format!(
            "persist materialized stopped disk artifact identity: {error}"
        ))
    })
}

fn finalize_recovered_published_restore(
    conn: &rusqlite::Connection,
    canonical_id: &str,
    sandbox_dir: &std::path::Path,
    meta: &crate::snapshot_upload::SnapshotMeta,
    restore_artifact_as_new: bool,
    target: CrossNodeRestoreTarget,
) -> Result<(), crate::snapshot_restore::RestoreError> {
    if restore_artifact_as_new {
        publish_rebound_sandbox_artifact_metadata(meta, sandbox_dir)?;
        crate::db::publish_restored_disk_as_new(conn, canonical_id).map_err(|error| {
            crate::snapshot_restore::RestoreError::Other(format!(
                "transition restored disk artifact to stopped: {error}"
            ))
        })
    } else {
        transition_restored_sandbox(conn, canonical_id, meta, target)
    }
}

/// The plain same-id finalize, dispatched on what the request asked to land in.
///
/// Both call sites — the interrupted-restore recovery above and the finalize
/// closure in `run_cross_node_restore` — need exactly this choice, so it
/// lives once. A third target then has one place to be added rather than two
/// that must agree.
fn transition_restored_sandbox(
    conn: &rusqlite::Connection,
    canonical_id: &str,
    meta: &crate::snapshot_upload::SnapshotMeta,
    target: CrossNodeRestoreTarget,
) -> Result<(), crate::snapshot_restore::RestoreError> {
    match target {
        CrossNodeRestoreTarget::SuspendedCheckpoint => {
            transition_restored_sandbox_to_suspended(conn, canonical_id, meta)
        }
        CrossNodeRestoreTarget::StoppedDiskArtifact { expected } => {
            transition_restored_sandbox_to_stopped(conn, canonical_id, meta, expected)
        }
    }
}

async fn finalize_exchanged_restore_after_cleanup<T>(
    db: &Arc<Mutex<rusqlite::Connection>>,
    cleanup: impl std::future::Future<Output = Result<(), crate::snapshot_restore::RestoreError>>,
    finalize: impl FnOnce(&rusqlite::Connection) -> Result<T, crate::snapshot_restore::RestoreError>,
) -> Result<T, crate::snapshot_restore::RestoreError> {
    // Finalization retires the transfer scope. Keep that durable cleanup owner
    // until every fallible external and filesystem cleanup step has succeeded.
    cleanup.await?;
    let db = db
        .lock()
        .map_err(|_| crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned)?;
    finalize(&db)
}

fn claim_exchanged_restore_cleanup(
    db: &Arc<Mutex<rusqlite::Connection>>,
    sandbox_id: &str,
    existing: &crate::db::RestoreTransferScopeRow,
    execution_generation: pplx_space_common_core::types::ExecutionGeneration,
    activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
) -> Result<crate::db::RestoreTransferScopeRow, crate::snapshot_restore::RestoreError> {
    let db = db.lock().map_err(|_| {
        crate::snapshot_restore::RestoreError::Other("sandbox database mutex poisoned".to_string())
    })?;
    if crate::db::canonical_restore_reclaim_attempt_is_current(
        &db,
        sandbox_id,
        existing.transfer_scope_id,
        execution_generation,
        activation_attempt_id,
    )
    .map_err(|source| crate::snapshot_restore::RestoreError::AdvanceRestorePhase { source })?
    {
        return Ok(existing.clone());
    }
    if !crate::db::advance_exchanged_restore_cleanup_owner_if_newer(
        &db,
        sandbox_id,
        existing.transfer_scope_id,
        execution_generation,
        activation_attempt_id,
    )
    .map_err(|source| crate::snapshot_restore::RestoreError::AdvanceRestorePhase { source })?
    {
        return Err(crate::snapshot_restore::RestoreError::Other(format!(
            "restore activation attempt is not newer than the durable exchanged-root cleanup owner for {sandbox_id}; retry"
        )));
    }
    crate::db::restore_transfer_scope(&db, sandbox_id)
        .map_err(|source| crate::snapshot_restore::RestoreError::AdvanceRestorePhase { source })?
        .ok_or_else(|| {
            crate::snapshot_restore::RestoreError::Other(format!(
                "exchanged-root cleanup scope disappeared after ownership transfer for {sandbox_id}; retry"
            ))
        })
}

/// The encryption stamp of a staged suspend snapshot, read back from the
/// sidecar the cross-node staging already wrote.
///
/// The mirror of [`codec_from_local_sidecar`], and it exists for the same
/// reason: on the cross-node path the local row is absent, so a terminal
/// sample emitted after a mid-restore failure has no row to read the stamp
/// from. Failures during the rootfs pull, remote-metadata publication, or the
/// database phase all happen *after* staging wrote `snapshot.json`, so the
/// stamp is recoverable there — and without it those encrypted failures are
/// reported as plaintext and vanish from the BYOK failure panels.
///
/// Defaults to `false` when the sidecar is not there. Known residual: this
/// reads the canonical directory only, and cross-node restore keeps
/// `snapshot.json` in its hidden restore-staging scope until
/// `publish_remote_restore_metadata` runs — so a failure after the metadata
/// is pulled but before publication still reports `false`. Closing that needs
/// the staging scope (or the stamp itself) propagated out to the caller, which
/// is a signature change tracked separately.
pub(super) fn encrypted_from_staged_sidecar(sandbox_id: &str) -> bool {
    let sandbox_dir = std::path::PathBuf::from(format!("{CACHE_DIR}/{sandbox_id}"));
    let meta_path = canonical_suspend_snapshot_dir(&sandbox_dir).join("snapshot.json");
    let Ok(json) = std::fs::read_to_string(&meta_path) else {
        return false;
    };
    let Ok(meta) = serde_json::from_str::<crate::snapshot_upload::SnapshotMeta>(&json) else {
        return false;
    };
    meta.encrypted
}

pub(super) fn codec_from_local_sidecar(sandbox: &crate::db::SandboxRow) -> &'static str {
    let sandbox_dir = std::path::PathBuf::from(format!("{CACHE_DIR}/{}", sandbox.id));
    let meta_path = canonical_suspend_snapshot_dir(&sandbox_dir).join("snapshot.json");
    let Ok(json) = std::fs::read_to_string(&meta_path) else {
        return pplx_space_metrics::snapshot::CODEC_FULL;
    };
    let Ok(meta) = serde_json::from_str::<crate::snapshot_upload::SnapshotMeta>(&json) else {
        return pplx_space_metrics::snapshot::CODEC_FULL;
    };
    pplx_space_metrics::snapshot::codec_tag_from_diff_format(meta.rootfs_diff_format.as_deref())
}

impl SandboxManagerService {
    fn ensure_quarantine_recovery_claim_current(
        &self,
        claimed: Option<&crate::db::ClaimedQuarantineRecoveryCandidate>,
    ) -> Result<(), crate::snapshot_restore::RestoreError> {
        let Some(claimed) = claimed else {
            return Ok(());
        };
        let db = self
            .db
            .lock()
            .map_err(|_| crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned)?;
        if crate::db::quarantine_recovery_claim_is_current(&db, claimed).map_err(|source| {
            crate::snapshot_restore::RestoreError::ReadQuarantineRecoveryClaim { source }
        })? {
            return Ok(());
        }
        Err(
            crate::snapshot_restore::RestoreError::QuarantineRecoveryClaimSuperseded {
                sandbox_id: claimed.candidate.sandbox_id,
            },
        )
    }

    /// Runs one claimed orphan recovery end to end. The eight-hour deadline exceeds
    /// the two sequential three-hour volume-manager wait budgets and bounds a
    /// filesystem stall; the staging-lock wait has its own far tighter bound in
    /// `ORPHAN_STAGING_ADMISSION_WAIT`, so it is not what this deadline covers.
    pub(super) async fn run_quarantined_orphan_restore(
        &self,
        claimed: &crate::db::ClaimedQuarantineRecoveryCandidate,
        plan: &crate::snapshot_restore::QuarantineRecoveryPlan,
    ) -> Result<Option<CrossNodeRestoreProvenance>, crate::snapshot_restore::RestoreError> {
        let sandbox_id = claimed.candidate.sandbox_id.to_string();
        let restore = self.run_cross_node_restore(CrossNodeRestoreRequest {
            sandbox_id: &sandbox_id,
            storage_uri: Some(&plan.storage_uri),
            execution_generation: Some(plan.execution_generation),
            activation_attempt_id: None,
            recovery: Some(QuarantineRecoveryContext {
                plan,
                claim: claimed,
            }),
            restore_artifact_as_new: None,
            // Orphan recovery re-adopts a suspended sandbox's own full
            // checkpoint; it never carries a disk artifact.
            target: CrossNodeRestoreTarget::SuspendedCheckpoint,
            // A busy staging tree is per-sandbox contention, not node-wide I/O, so
            // this attempt gives up after a short bounded wait and releases the sole
            // orphan-restore permit rather than holding it for the operation
            // deadline. Bounded rather than instantaneous because a refusal charges
            // the attempt and escalates the backoff — see
            // `ORPHAN_STAGING_ADMISSION_WAIT`.
            staging_admission: CrossNodeRestoreStagingAdmission::WaitAtMost(
                ORPHAN_STAGING_ADMISSION_WAIT,
            ),
        });
        drive_under_quarantine_recovery_claim(
            restore,
            claimed.candidate.sandbox_id,
            QUARANTINE_RECOVERY_CLAIM_HEARTBEAT,
            QUARANTINE_RECOVERY_OPERATION_TIMEOUT,
            self.shutdown_token.clone(),
            || {
                let db = self.db.lock().map_err(|_| {
                    crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned
                })?;
                crate::db::renew_quarantine_recovery_claim(&db, claimed).map_err(|source| {
                    crate::snapshot_restore::RestoreError::RenewQuarantineRecoveryClaim { source }
                })
            },
        )
        .await
    }

    pub(super) fn create_restore_transfer_scope(
        &self,
        sandbox_id: &str,
        execution_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
        activation_attempt_id: Option<pplx_space_common_core::types::SandboxActivationAttemptId>,
    ) -> Result<pplx_space_common_core::types::TransferScopeId, crate::snapshot_restore::RestoreError>
    {
        let scope = pplx_space_common_core::types::TransferScopeId::new();
        let staging_dir =
            crate::snapshot_restore::remote_restore_metadata_staging_dir(sandbox_id, scope);
        let inserted = {
            let db = self.db.lock().map_err(|_| {
                crate::snapshot_restore::RestoreError::Other(
                    "sandbox database mutex poisoned".to_string(),
                )
            })?;
            crate::db::insert_restore_transfer_scope(
                &db,
                sandbox_id,
                scope,
                execution_generation,
                activation_attempt_id,
                &staging_dir.to_string_lossy(),
            )
            .map_err(|source| {
                crate::snapshot_restore::RestoreError::AdvanceRestorePhase { source }
            })?
        };
        if !inserted {
            return Err(crate::snapshot_restore::RestoreError::Other(format!(
                "restore transfer scope raced for {sandbox_id}; retry"
            )));
        }
        Ok(scope)
    }

    async fn claim_and_clean_retryable_restore_transfer_scope(
        &self,
        sandbox_id: &str,
        existing: &crate::db::RestoreTransferScopeRow,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
    ) -> Result<
        Option<pplx_space_common_core::types::TransferScopeId>,
        crate::snapshot_restore::RestoreError,
    > {
        // Pre-fencing releases wrote scopes without attempt ownership. A
        // complete authoritative attempt may claim that legacy scope after
        // the same row-locked runtime-free check as a fenced successor.
        let incoming_is_current_or_newer = match (
            existing.execution_generation,
            existing.activation_attempt_id,
        ) {
            (Some(existing_generation), Some(existing_attempt_id)) => {
                execution_generation.get() > existing_generation.get()
                    || (execution_generation == existing_generation
                        && activation_attempt_id >= existing_attempt_id)
            }
            (None, None) => true,
            _ => false,
        };
        if !incoming_is_current_or_newer {
            return Ok(None);
        }

        let claimed = {
            let db = self.db.lock().map_err(|_| {
                crate::snapshot_restore::RestoreError::Other(
                    "sandbox database mutex poisoned".to_string(),
                )
            })?;
            crate::db::claim_retryable_restore_transfer_scope_if_current_or_newer(
                &db,
                sandbox_id,
                existing.transfer_scope_id,
                execution_generation,
                activation_attempt_id,
            )
            .map_err(|source| {
                crate::snapshot_restore::RestoreError::AdvanceRestorePhase { source }
            })?
        };
        if !claimed {
            return Ok(None);
        }

        let claimed_scope = {
            let db = self
                .db_conn()
                .map_err(|_| crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned)?;
            crate::db::restore_transfer_scope(&db, sandbox_id)
                .map_err(
                    |source| crate::snapshot_restore::RestoreError::AdvanceRestorePhase { source },
                )?
                .ok_or_else(|| {
                    crate::snapshot_restore::RestoreError::Other(format!(
                        "claimed restore transfer scope disappeared for {sandbox_id}; retry"
                    ))
                })?
        };
        self.cancel_restore_transfer_scope_and_remove_staging(sandbox_id, &claimed_scope)
            .await
            .map_err(|error| {
                crate::snapshot_restore::RestoreError::Other(format!(
                    "remove superseded restore transfer scope staging for {sandbox_id}: {error}"
                ))
            })?;

        // Volume-manager cancellation is a writer barrier, not a permanent
        // scope closure. Reusing the retained scope avoids any crash window
        // in which the predecessor's physical cleanup reference is lost.
        Ok(Some(claimed_scope.transfer_scope_id))
    }

    pub(super) async fn run_cross_node_restore(
        &self,
        request: CrossNodeRestoreRequest<'_>,
    ) -> Result<Option<CrossNodeRestoreProvenance>, crate::snapshot_restore::RestoreError> {
        use crate::snapshot_restore::{
            RestoreError, canonical_restore_exchange_old_source_at,
            exchange_remote_restore_metadata, publish_remote_restore_metadata,
            reclaimable_published_remote_restore_source,
            reclaimable_published_remote_restore_source_at, recover_published_remote_restore,
            restore_pull_metadata_from_uri_as_new, restore_pull_metadata_from_uri_as_new_in_place,
            restore_pull_metadata_from_uri_scoped, restore_pull_metadata_scoped,
            restore_pull_rootfs, write_canonical_restore_exchange_receipt,
        };

        let CrossNodeRestoreRequest {
            sandbox_id,
            storage_uri,
            execution_generation,
            activation_attempt_id,
            recovery,
            restore_artifact_as_new,
            target,
            staging_admission,
        } = request;
        let sandbox_id =
            SandboxId::from_str(sandbox_id).map_err(|source| RestoreError::InvalidSandboxId {
                value: sandbox_id.to_string(),
                source,
            })?;
        let canonical_id = sandbox_id.to_string();

        let staging_lock = self.auto_lifecycle.staging_lock(&canonical_id);
        let mut _staging_guard = Some(match staging_admission {
            CrossNodeRestoreStagingAdmission::Foreground => start_stage(
                "restore.cross_node.staging_lock_wait",
                &canonical_id,
                super::restart_admission::wait_for_activation_admission(
                    &canonical_id,
                    Arc::clone(&staging_lock).lock_owned(),
                ),
            )
            .await
            .map_err(RestoreError::AdmissionRejected)?,
            CrossNodeRestoreStagingAdmission::WaitAtMost(budget) => {
                // A refusal here costs the episode an attempt and an exponential
                // backoff, so a caller that gives a budget is saying the refusal
                // must mean "someone holds this for a long time", not "someone held
                // it as this call arrived".
                match tokio::time::timeout(budget, Arc::clone(&staging_lock).lock_owned()).await {
                    Ok(guard) => guard,
                    Err(_elapsed) => {
                        return Err(RestoreError::RestoreOperationBusy { sandbox_id });
                    }
                }
            }
        });
        // Cleanup acquires cleanup_lock before this staging lock. Recovery must
        // never acquire cleanup_lock underneath it. The staging lock serializes
        // restore state and filesystem work; exact claim CASes additionally
        // fence recovery-specific mutations.
        self.ensure_quarantine_recovery_claim_current(recovery.map(|context| context.claim))?;
        let existing_row = {
            let db = self
                .db_conn()
                .map_err(|_| crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned)?;
            crate::db::get_sandbox(&db, &canonical_id)
                .map_err(|source| RestoreError::ReadSandboxRow { source })?
        };
        let mut recovering_orphan_quarantine = false;
        let mut retrying_interrupted_restore = false;
        let mut interrupted_restore_source = None;
        if let Some(row) = existing_row {
            if let Some(recovery) = recovery {
                if recovery.plan.reason_code
                    != pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing
                    || !matches!(
                        row.state,
                        SandboxState::Quarantined | SandboxState::Restoring
                    )
                {
                    return Ok(None);
                }
                let eligible = {
                    let db = self.db_conn().map_err(|_| {
                        crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned
                    })?;
                    crate::db::is_orphan_quarantine_recovery_state(&db, &canonical_id, row.state)
                        .map_err(|source| RestoreError::ReadSandboxRow { source })?
                };
                if !eligible {
                    return Ok(None);
                }
                recovering_orphan_quarantine = true;

                if (self.sandbox_runtime_live)(&canonical_id) {
                    return Err(RestoreError::Other(format!(
                        "refusing to remove orphan residue for live sandbox {canonical_id}"
                    )));
                }
                let canonical_dir =
                    pplx_space_common_core::host::linux::paths::sandbox_cache_dir(&canonical_id);
                if std::path::Path::new(&canonical_dir).exists() {
                    // This is intentionally whole-sandbox disposal, not partial
                    // restore cleanup. Keep teardown and recursive removal with
                    // the node-manager's lifecycle owner through completion.
                    let mut node = self.node_manager.connect().await.map_err(|error| {
                        RestoreError::Other(format!(
                            "connect node-manager for orphan cleanup {canonical_id}: {error}"
                        ))
                    })?;
                    super::cleanup_support::cleanup_sandbox_via_node(
                        &self.db,
                        &mut node,
                        &canonical_id,
                        false,
                        "remove canonical orphan residue",
                    )
                    .await
                    .map_err(|error| {
                        RestoreError::Other(format!(
                            "remove canonical orphan residue {canonical_dir}: {error}"
                        ))
                    })?;
                    emit_sandbox_event(
                        &canonical_id,
                        SandboxEventType::CleanupCompleted,
                        SandboxEventSource::SbxManager,
                        SandboxEventDetails::default()
                            .operation("quarantine_recovery")
                            .reason("unsafe_orphan_residue")
                            .result("removed"),
                    );
                }
            } else if crate::db::sandbox_is_retryable_interrupted_restore(&row) {
                retrying_interrupted_restore = true;
                interrupted_restore_source = row
                    .restore_operation
                    .as_ref()
                    .and_then(crate::restore_operation::RestoreOperation::remote_source)
                    .cloned();
            } else {
                return Ok(None);
            }
        };

        let expected_restore_source =
            select_interrupted_restore_source(storage_uri, interrupted_restore_source.clone())?;
        if retrying_interrupted_restore
            && let (Some(persisted_source), Some(selected_source)) = (
                interrupted_restore_source.as_ref(),
                expected_restore_source.as_ref(),
            )
        {
            let db = self
                .db_conn()
                .map_err(|_| crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned)?;
            persist_selected_interrupted_restore_source(
                &db,
                sandbox_id,
                persisted_source,
                selected_source,
            )?;
        }
        // Interrupted retries stage the same source selected for conflict
        // classification. Keep incoming-over-persisted precedence in the one
        // helper above so the retry caller cannot drift from this entry point.
        let storage_uri = if retrying_interrupted_restore {
            expected_restore_source
                .as_ref()
                .map(ExactStorageUri::as_wire)
        } else {
            storage_uri
        };
        let mut conflicting_canonical_source = None;
        if retrying_interrupted_restore
            && let Some(expected_restore_source) = expected_restore_source.as_ref()
        {
            let recovered = recover_published_remote_restore(
                sandbox_id,
                expected_restore_source,
                restore_artifact_as_new
                    .map(|context| (context.source_sandbox_id, context.artifact_kind)),
            )
            .await;
            let published = match recovered {
                Ok(published) => published,
                Err(RestoreError::CanonicalRootConflict { .. })
                    if restore_artifact_as_new.is_none()
                        && execution_generation.is_some()
                        && activation_attempt_id.is_some() =>
                {
                    conflicting_canonical_source = Some(
                        reclaimable_published_remote_restore_source(sandbox_id)
                            .await?
                            .ok_or_else(|| {
                                RestoreError::Other(format!(
                                    "canonical restore root for {sandbox_id} disappeared after conflict classification; retry"
                                ))
                            })?,
                    );
                    tracing::info!(
                        sandbox_id = %canonical_id,
                        "staging the authoritative restore source before fenced canonical-root reclamation"
                    );
                    None
                }
                Err(error @ RestoreError::CanonicalRootConflict { .. }) => {
                    let db = self.db_conn().map_err(|_| {
                        crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned
                    })?;
                    persist_cross_node_staging_failure(
                        &db,
                        sandbox_id,
                        "canonical-root classification",
                        &error,
                        recovery.map(|context| context.claim),
                    )?;
                    return Err(error);
                }
                Err(error) => return Err(error),
            };
            if let Some(published) = published {
                let exchanged_scope = {
                    let db = self.db_conn().map_err(|_| {
                        crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned
                    })?;
                    crate::db::restore_transfer_scope(&db, &canonical_id)
                        .map_err(|source| RestoreError::AdvanceRestorePhase { source })?
                        .filter(|scope| std::path::Path::new(&scope.staging_dir).exists())
                };
                let exchanged_scope_to_clean = if let Some(scope) = exchanged_scope {
                    let Some(execution_generation) = execution_generation else {
                        return Err(RestoreError::Other(format!(
                            "restore attempt ownership changed before exchanged canonical-root cleanup for {canonical_id}; retry"
                        )));
                    };
                    let Some(activation_attempt_id) = activation_attempt_id else {
                        return Err(RestoreError::Other(format!(
                            "restore attempt ownership changed before exchanged canonical-root cleanup for {canonical_id}; retry"
                        )));
                    };
                    if (self.sandbox_runtime_live)(&canonical_id) {
                        return Err(RestoreError::Other(format!(
                            "restore attempt cannot clean an exchanged canonical root for live sandbox {canonical_id}; retry"
                        )));
                    }
                    let scope = claim_exchanged_restore_cleanup(
                        &self.db,
                        &canonical_id,
                        &scope,
                        execution_generation,
                        activation_attempt_id,
                    )?;
                    let canonical_dir =
                        pplx_space_common_core::host::linux::paths::sandbox_cache_dir(
                            &canonical_id,
                        );
                    let old_source = canonical_restore_exchange_old_source_at(
                        std::path::Path::new(&canonical_dir),
                        sandbox_id,
                        scope.transfer_scope_id,
                        execution_generation,
                        activation_attempt_id,
                        expected_restore_source,
                    )?
                    .ok_or_else(|| {
                        RestoreError::Other(format!(
                            "canonical restore root for {canonical_id} has no durable exchange receipt; retry"
                        ))
                    })?;
                    let scoped_source = reclaimable_published_remote_restore_source_at(
                        std::path::Path::new(&scope.staging_dir),
                        sandbox_id,
                    )?;
                    ensure_staged_restore_source(&old_source, scoped_source.as_ref())?;
                    Some(scope)
                } else {
                    None
                };
                let canonical_dir =
                    pplx_space_common_core::host::linux::paths::sandbox_cache_dir(&canonical_id);
                if let Some(scope) = exchanged_scope_to_clean {
                    finalize_exchanged_restore_after_cleanup(
                        &self.db,
                        async {
                            self.cancel_restore_transfer_scope_and_remove_staging(
                                &canonical_id,
                                &scope,
                            )
                            .await
                            .map_err(|error| {
                                RestoreError::Other(format!(
                                    "remove exchanged canonical-root remnant for {canonical_id}: {error}"
                                ))
                            })
                        },
                        |db| {
                            finalize_recovered_published_restore(
                                db,
                                &canonical_id,
                                std::path::Path::new(&canonical_dir),
                                &published.meta,
                                restore_artifact_as_new.is_some(),
                                target,
                            )
                        },
                    )
                    .await?;
                } else {
                    let db = self.db_conn().map_err(|_| {
                        crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned
                    })?;
                    finalize_recovered_published_restore(
                        &db,
                        &canonical_id,
                        std::path::Path::new(&canonical_dir),
                        &published.meta,
                        restore_artifact_as_new.is_some(),
                        target,
                    )?;
                }
                log_interrupted_restore_completion(&canonical_id, &published.storage_uri);
                return Ok(Some(CrossNodeRestoreProvenance {
                    storage_uri: published.storage_uri,
                    local_artifact_id: published.meta.local_artifact_id.map(|id| id.to_string()),
                }));
            }
        }

        let transfer_scope_id = {
            let existing = {
                let db = self.db_conn().map_err(|_| {
                    crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned
                })?;
                crate::db::restore_transfer_scope(&db, &canonical_id)
                    .map_err(|source| RestoreError::AdvanceRestorePhase { source })?
            };
            if let Some(existing) = existing {
                if retrying_interrupted_restore {
                    let (Some(execution_generation), Some(activation_attempt_id)) =
                        (execution_generation, activation_attempt_id)
                    else {
                        return Err(RestoreError::Other(format!(
                            "retryable restore scope for {canonical_id} cannot be claimed without a complete activation attempt"
                        )));
                    };
                    self.claim_and_clean_retryable_restore_transfer_scope(
                        &canonical_id,
                        &existing,
                        execution_generation,
                        activation_attempt_id,
                    )
                    .await?
                    .ok_or_else(|| {
                        RestoreError::Other(format!(
                            "restore activation attempt is older than the durable scope owner for {canonical_id}; retry"
                        ))
                    })?
                } else if existing.execution_generation == execution_generation
                    && existing.activation_attempt_id == activation_attempt_id
                {
                    existing.transfer_scope_id
                } else {
                    let retired = if recovering_orphan_quarantine {
                        self.retire_orphan_quarantine_restore_transfer_scope(
                            &canonical_id,
                            existing,
                        )
                        .await
                    } else {
                        self.retire_rowless_restore_transfer_scope(&canonical_id, existing)
                            .await
                    }
                    .map_err(|error| {
                        RestoreError::Other(format!(
                            "retire superseded restore transfer scope for {canonical_id}: {error}"
                        ))
                    })?;
                    if !retired {
                        return Err(RestoreError::Other(format!(
                            "restore transfer scope ownership changed for {canonical_id}; retry"
                        )));
                    }
                    self.create_restore_transfer_scope(
                        &canonical_id,
                        execution_generation,
                        activation_attempt_id,
                    )?
                }
            } else {
                self.create_restore_transfer_scope(
                    &canonical_id,
                    execution_generation,
                    activation_attempt_id,
                )?
            }
        };

        let restore_as_new_destination = restore_artifact_as_new.map(|_| {
            let canonical_dir =
                pplx_space_common_core::host::linux::paths::sandbox_cache_dir(&canonical_id);
            restore_as_new_metadata_destination(
                retrying_interrupted_restore,
                std::path::Path::new(&canonical_dir).exists(),
            )
        });
        self.ensure_quarantine_recovery_claim_current(recovery.map(|context| context.claim))?;
        let stage_started_at = std::time::Instant::now();
        let mut staged = start_stage("restore.cross_node.pull_metadata", &canonical_id, async {
            match (
                storage_uri,
                restore_artifact_as_new,
                restore_as_new_destination,
            ) {
                (
                    Some(storage_uri),
                    Some(context),
                    Some(RestoreAsNewMetadataDestination::Scoped),
                ) => {
                    restore_pull_metadata_from_uri_as_new(
                        &canonical_id,
                        &context.source_sandbox_id.to_string(),
                        storage_uri,
                        transfer_scope_id,
                    )
                    .await
                }
                (
                    Some(storage_uri),
                    Some(context),
                    Some(RestoreAsNewMetadataDestination::Canonical),
                ) => {
                    restore_pull_metadata_from_uri_as_new_in_place(
                        &canonical_id,
                        &context.source_sandbox_id.to_string(),
                        storage_uri,
                        transfer_scope_id,
                    )
                    .await
                }
                (Some(storage_uri), None, None) => {
                    restore_pull_metadata_from_uri_scoped(
                        &canonical_id,
                        storage_uri,
                        transfer_scope_id,
                    )
                    .await
                }
                (None, Some(_), Some(_)) => Err(RestoreError::Other(
                    "artifact restore-as-new requires an exact storage URI".to_string(),
                )),
                (None, None, None) => {
                    restore_pull_metadata_scoped(&canonical_id, transfer_scope_id).await
                }
                _ => unreachable!("restore-as-new destination follows restore context"),
            }
        })
        .await?;
        if let Some(recovery) = recovery {
            validate_quarantine_recovery_artifact_identity(
                &canonical_id,
                recovery.plan.local_artifact_id,
                staged.meta.local_artifact_id,
            )?;
        }
        if let Some(restore_artifact_as_new) = restore_artifact_as_new {
            validate_restore_artifact_as_new_context(restore_artifact_as_new, &staged.storage_uri)?;
            validate_artifact_restore_as_new(&staged.meta, restore_artifact_as_new, &sandbox_id)?;
            rebind_artifact_snapshot_identity(
                &mut staged.meta,
                &sandbox_id.to_string(),
                &restore_artifact_as_new.target_owner,
            )?;
        }
        if let CrossNodeRestoreTarget::StoppedDiskArtifact { expected } = target {
            // The same check the finalize runs, hoisted ahead of the rootfs
            // transfer: the sidecar is already on disk here, and refusing a
            // wrongly-selected artifact now costs one metadata object instead
            // of a multi-gigabyte pull. Nothing is rebound and no identity is
            // reassigned — `restore_pull_metadata_from_source` already pinned
            // the staged metadata to this same sandbox id.
            stopped_materialization_pin(&staged.meta, expected)?;
        }
        let restore_source: pplx_space_common_core::types::ExactStorageUri = staged
            .storage_uri
            .parse()
            .map_err(|source| RestoreError::InvalidRestoreSource {
                uri: staged.storage_uri.clone(),
                source,
            })?;
        emit_sandbox_event(
            &canonical_id,
            SandboxEventType::SnapshotRestoreStarted,
            SandboxEventSource::SbxManager,
            snapshot_restore_event_details(
                &staged,
                SandboxEventDetails::default().reason("metadata_pulled"),
            ),
        );
        pplx_space_metrics::snapshot::restore_step(
            "cross_node",
            pplx_space_metrics::snapshot::codec_tag_from_diff_format(
                staged.meta.rootfs_diff_format.as_deref(),
            ),
            staged.meta.encrypted,
            "pull_metadata",
            stage_started_at,
        );

        if staged.meta.sandbox.is_none() {
            tracing::warn!(
                sandbox_id = %canonical_id,
                "snapshot.json missing `sandbox` block; restored row will use the default guest user, default auto-lifecycle, and empty process_env"
            );
        }

        let repopulate_started_at = std::time::Instant::now();
        self.apply_route_identity_mutation(sandbox_id, "cross_node_restore_row_adoption", || {
            let span = tracing::info_span!(
                "sbxmgr.start.stage",
                sandbox_id = %canonical_id,
                stage = "restore.cross_node.repopulate_row",
                otel.name = "restore.cross_node.repopulate_row",
            );
            let _entered = span.enter();
            let db = self
                .db_conn()
                .map_err(|_| crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned)?;
            adopt_restoring_row_under_recovery_claim(
                &db,
                sandbox_id,
                &canonical_id,
                RestoringRowInput::new(
                    &staged.meta,
                    &restore_source,
                    restore_artifact_as_new.and_then(|context| context.target_disk_io_limits),
                ),
                recovery.as_ref(),
                retrying_interrupted_restore,
            )?;
            Ok(((), true))
        })?;
        {
            let db = self
                .db_conn()
                .map_err(|_| crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned)?;
            if !crate::db::record_remote_restore_artifact_staging(&db, &canonical_id)
                .map_err(|source| RestoreError::AdvanceRestorePhase { source })?
            {
                return Err(RestoreError::RestorePhaseTransitionLost { sandbox_id });
            }
        }

        let meta_diff_format = staged.meta.rootfs_diff_format.as_deref();
        let codec = pplx_space_metrics::snapshot::codec_tag_from_diff_format(meta_diff_format);
        pplx_space_metrics::snapshot::restore_step(
            "cross_node",
            codec,
            staged.meta.encrypted,
            "repopulate_row",
            repopulate_started_at,
        );

        let phase2_started_at = std::time::Instant::now();
        let phase2_span = tracing::info_span!(
            "sbxmgr.start.stage",
            sandbox_id = %canonical_id,
            stage = "restore.cross_node.pull_rootfs",
            otel.name = "restore.cross_node.pull_rootfs",
            codec,
        );
        self.ensure_quarantine_recovery_claim_current(recovery.map(|context| context.claim))?;
        let execution_fence =
            super::cleanup_support::read_node_execution_fence(&self.db, &canonical_id)
                .map_err(|error| RestoreError::Other(error.to_string()))?
                .ok_or_else(|| {
                    RestoreError::Other("restore execution identity is missing".into())
                })?;
        let phase2_result = restore_pull_rootfs(
            &staged,
            execution_fence,
            &self.node_manager,
            self.template_resolver.as_ref(),
        )
        .instrument(phase2_span)
        .await;
        if let Err(ref e) = phase2_result {
            emit_sandbox_event(
                &canonical_id,
                SandboxEventType::SnapshotRestoreFailed,
                SandboxEventSource::SbxManager,
                snapshot_restore_event_details(
                    &staged,
                    SandboxEventDetails::default()
                        .reason("pull_rootfs")
                        .error_kind(e.kind_label())
                        .error(e.to_string()),
                ),
            );
            let failure_reason = format!("cross-node restore artifact staging failed: {e}");
            // The claim was current before the pull, but the pull is a transfer and
            // can outlive it. Under a recovery claim the write is fenced on that
            // claim in the same statement rather than re-checked before it, so a
            // superseded claimant cannot record its failure as an ordinary staging
            // deferral on an episode that now belongs to someone else. The event
            // above is emitted either way: the pull did fail, and dropping that
            // evidence because ownership moved would hide a real failure.
            let claim = recovery.map(|context| context.claim);
            let marked = {
                let db = self.db_conn().map_err(|_| {
                    crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned
                })?;
                match claim {
                    Some(claimed) => {
                        crate::db::mark_cross_node_restore_staging_failed_under_recovery_claim(
                            &db,
                            &failure_reason,
                            claimed,
                        )
                    }
                    None => crate::db::mark_cross_node_restore_staging_failed(
                        &db,
                        &canonical_id,
                        &failure_reason,
                    ),
                }
                .map_err(|error| {
                    RestoreError::Other(format!(
                        "persist cross-node restore staging failure: {error}"
                    ))
                })?
            };
            if !marked {
                // Reports which of the two the miss was. Diagnosis only — the fence
                // in the statement is what makes the refusal correct.
                self.ensure_quarantine_recovery_claim_current(claim)?;
                return Err(RestoreError::Other(format!(
                    "cross-node restore row left Restoring before staging failure could be persisted: {canonical_id}"
                )));
            }
            tracing::info!(
                sandbox_id = %canonical_id,
                error = %e,
                "cross-node restore staging failed; retaining restoring row for retry"
            );
        }
        phase2_result?;

        let mut canonical_reclaim_fence = None;
        let mut exchanged_canonical_scope = None;
        if let Some(initially_observed_source) = conflicting_canonical_source.as_ref() {
            if expected_restore_source.as_ref() != Some(&restore_source) {
                return Err(RestoreError::Other(format!(
                    "staged restore source changed for {canonical_id}; refusing canonical-root reclamation"
                )));
            }
            let execution_generation = execution_generation.ok_or_else(|| {
                RestoreError::Other("canonical reclamation requires execution generation".into())
            })?;
            let activation_attempt_id = activation_attempt_id.ok_or_else(|| {
                RestoreError::Other("canonical reclamation requires activation attempt".into())
            })?;

            // Cleanup takes cleanup_lock -> staging_lock. Release the staging
            // lock acquired for the long pull, then reacquire both in that
            // order for the short destructive boundary. Cleanup, a Start-RPC
            // restore, or a concurrent inventory pass can win the gap, but the exact
            // scope/attempt/source/presence checks below then fail closed before
            // touching the canonical root.
            // Both acquisitions below block, and the wait is on the per-sandbox
            // staging lock that another restore of the same sandbox can hold for as
            // long as its own pull takes. Neither `Start` call site into
            // `run_cross_node_restore` wraps it in a `tokio::time::timeout`; the
            // bound is the transport's, since sandbox-manager serves this through
            // `pplx_space_common_svc::grpc_server_builder` and tonic applies
            // `min(the caller's grpc-timeout, 120s)`, surfacing on elapse as
            // `CANCELLED "Timeout expired"`. Nothing here records a fence failure
            // when that fires, so the only evidence is on the calling side.
            //
            // That deadline is one budget shared with the transfer, not a second
            // bound on this wait: tonic arms a single sleep when the request
            // arrives, covering the whole handler. Since these fences come after the
            // transfer, this reclamation only ever runs on an attempt whose transfer
            // already fit inside the deadline, and the wait gets what is left.
            drop(_staging_guard.take());
            let cleanup_lock = self.cleanup_lock(&canonical_id);
            let cleanup_owner = crate::service::cleanup_support::CleanupLockGuard::new(
                Arc::clone(&self.cleanup_locks),
                canonical_id.clone(),
                cleanup_lock,
            );
            let (cleanup_fence, staging_guard) =
                acquire_canonical_reclamation_fences(cleanup_owner, Arc::clone(&staging_lock))
                    .await;
            _staging_guard = Some(staging_guard);

            let attempt_is_current = {
                let db = self.db_conn().map_err(|_| {
                    crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned
                })?;
                crate::db::canonical_restore_reclaim_attempt_is_current(
                    &db,
                    &canonical_id,
                    transfer_scope_id,
                    execution_generation,
                    activation_attempt_id,
                )
                .map_err(|source| RestoreError::AdvanceRestorePhase { source })?
            };
            if !attempt_is_current {
                return Err(RestoreError::Other(format!(
                    "restore attempt ownership changed before canonical-root reclamation for {canonical_id}; retry"
                )));
            }
            if (self.sandbox_runtime_live)(&canonical_id) {
                return Err(RestoreError::Other(format!(
                    "refusing canonical-root reclamation for live sandbox {canonical_id}"
                )));
            }

            let current_staged_source =
                reclaimable_published_remote_restore_source_at(&staged.sandbox_dir, sandbox_id)?;
            ensure_staged_restore_source(&restore_source, current_staged_source.as_ref())?;

            let canonical_dir =
                pplx_space_common_core::host::linux::paths::sandbox_cache_dir(&canonical_id);
            let current_source = reclaimable_published_remote_restore_source(sandbox_id).await?;
            match canonical_root_reclaim_decision(
                initially_observed_source,
                current_source.as_ref(),
                std::path::Path::new(&canonical_dir).exists(),
            )? {
                CanonicalRootReclaimDecision::Reclaim => {
                    write_canonical_restore_exchange_receipt(
                        &staged,
                        sandbox_id,
                        transfer_scope_id,
                        execution_generation,
                        activation_attempt_id,
                        initially_observed_source,
                        &restore_source,
                    )?;
                    exchange_remote_restore_metadata(&mut staged, sandbox_id)?;
                    exchanged_canonical_scope = Some({
                        let db = self.db_conn().map_err(|_| {
                            crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned
                        })?;
                        crate::db::restore_transfer_scope(&db, &canonical_id)
                            .map_err(|source| RestoreError::AdvanceRestorePhase { source })?
                            .ok_or_else(|| {
                                RestoreError::Other(format!(
                                    "restore transfer scope disappeared after canonical-root exchange for {canonical_id}; retry"
                                ))
                            })?
                    });
                }
                CanonicalRootReclaimDecision::AlreadyVacant => {}
            }
            canonical_reclaim_fence = Some(cleanup_fence);
        }

        let publish_result = if exchanged_canonical_scope.is_some() {
            Ok(())
        } else {
            (|| {
                if restore_artifact_as_new.is_some() {
                    publish_rebound_artifact_snapshot_metadata(&staged.meta, &staged.snapshot_dir)?;
                    publish_rebound_sandbox_artifact_metadata(&staged.meta, &staged.sandbox_dir)?;
                }
                publish_remote_restore_metadata(&mut staged, sandbox_id)
            })()
        };
        drop(canonical_reclaim_fence);
        if let Err(error) = publish_result {
            // Emitted before the durable write, and unconditionally. The write is fenced
            // and can miss for any of three reasons — a moved claim, a lapsed lease, a
            // cleanup owner — and on a miss it returns a lost-transition error that says
            // nothing about why publication failed. Recording the failure only when the
            // fence happens to match makes a full disk invisible.
            emit_sandbox_event(
                &canonical_id,
                SandboxEventType::SnapshotRestoreFailed,
                SandboxEventSource::SbxManager,
                SandboxEventDetails::default()
                    .reason("canonical_root_publication")
                    .error_kind(error.kind_label())
                    .error(error.to_string()),
            );
            let db = self
                .db_conn()
                .map_err(|_| crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned)?;
            persist_cross_node_staging_failure(
                &db,
                sandbox_id,
                "canonical-root publication",
                &error,
                recovery.map(|context| context.claim),
            )?;
            return Err(error);
        }

        pplx_space_metrics::snapshot::restore_step(
            "cross_node",
            codec,
            staged.meta.encrypted,
            "pull_rootfs",
            phase2_started_at,
        );
        tracing::info!(
            sandbox_id = %canonical_id,
            snapshot_dir = %staged.snapshot_dir.display(),
            storage_uri = %staged.storage_uri,
            "cross-node restore: snapshot artifacts staged from S3"
        );
        emit_sandbox_event(
            &canonical_id,
            SandboxEventType::SnapshotRestoreCompleted,
            SandboxEventSource::SbxManager,
            snapshot_restore_event_details(
                &staged,
                SandboxEventDetails::default().reason("artifacts_staged"),
            ),
        );

        let finalize_restore = |db: &rusqlite::Connection| {
            if let Some(recovery) = recovery {
                let local_artifact_id = validate_quarantine_recovery_artifact_identity(
                    &canonical_id,
                    recovery.plan.local_artifact_id,
                    staged.meta.local_artifact_id,
                )?;
                if !crate::db::publish_orphan_quarantine_recovered_artifact(
                    db,
                    recovery.claim,
                    local_artifact_id,
                    &staged.snapshot_dir.to_string_lossy(),
                    recovery.plan.execution_generation,
                    recovery.plan.recovery_id,
                )
                .map_err(|source| RestoreError::PersistOrphanQuarantineRecovery { source })?
                {
                    // Same class as the generation CAS above, and the typed
                    // rusqlite error is preserved rather than formatted away.
                    //
                    // This one refuses on strictly more than a lost claim: also on
                    // an execution-generation mismatch, on the sandbox no longer
                    // being `restoring`, and when its own episode DELETE matches a
                    // row count it did not expect. All of them arrive as
                    // `claim_superseded`, so the same caveat applies — the label
                    // names the likeliest cause, not the only one.
                    return Err(RestoreError::QuarantineRecoveryClaimSuperseded {
                        sandbox_id: recovery.claim.candidate.sandbox_id,
                    });
                }
            } else if restore_artifact_as_new.is_some() {
                crate::db::publish_restored_disk_as_new(db, &canonical_id).map_err(|error| {
                    RestoreError::Other(format!(
                        "transition restored disk artifact to stopped: {error}"
                    ))
                })?;
            } else {
                transition_restored_sandbox(db, &canonical_id, &staged.meta, target)?;
            }
            Ok(())
        };
        {
            let span = tracing::info_span!(
                "sbxmgr.start.stage",
                sandbox_id = %canonical_id,
                stage = "restore.cross_node.transition_suspended",
                otel.name = "restore.cross_node.transition_suspended",
                codec,
            );
            let _entered = span.enter();
            if let Some(scope) = exchanged_canonical_scope.as_ref() {
                finalize_exchanged_restore_after_cleanup(
                    &self.db,
                    async {
                        self.cancel_restore_transfer_scope_and_remove_staging(
                            &canonical_id,
                            scope,
                        )
                        .await
                        .map_err(|error| {
                            RestoreError::Other(format!(
                                "remove exchanged canonical-root remnant for {canonical_id}: {error}"
                            ))
                        })
                    },
                    finalize_restore,
                )
                .await?;
            } else {
                let db = self.db_conn().map_err(|_| {
                    crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned
                })?;
                finalize_restore(&db)?;
            }
        }
        if recovery.is_some() {
            mark_sandbox_dirty(&self.db, &canonical_id);
        }
        if exchanged_canonical_scope.is_some() {
            emit_sandbox_event(
                &canonical_id,
                SandboxEventType::CleanupCompleted,
                SandboxEventSource::SbxManager,
                SandboxEventDetails::default()
                    .operation("canonical_restore_reclamation")
                    .reason("superseded_restore_source")
                    .result("removed"),
            );
        }

        let provenance = CrossNodeRestoreProvenance {
            storage_uri: staged.storage_uri.clone(),
            local_artifact_id: staged.meta.local_artifact_id.map(|id| id.to_string()),
        };
        let meta = staged.meta;
        tracing::info!(
            sandbox_id = %canonical_id,
            template = %meta.source.template_id,
            runtime = %meta.source.runtime,
            "cross-node restore: repopulated local sandbox row from snapshot.json"
        );

        Ok(Some(provenance))
    }
}

fn validate_restore_artifact_as_new_context(
    context: &RestoreArtifactAsNewContext,
    storage_uri: &str,
) -> Result<(), crate::snapshot_restore::RestoreError> {
    let parsed_storage_uri = StorageUri::parse(storage_uri).ok();
    let mut storage_segments = parsed_storage_uri
        .as_ref()
        .map(|parsed| parsed.key_prefix.rsplit('/'));
    let storage_snapshot_id = storage_segments
        .as_mut()
        .and_then(Iterator::next)
        .and_then(|value| value.parse().ok());
    if storage_snapshot_id != Some(context.artifact_snapshot_id) {
        return Err(
            crate::snapshot_restore::RestoreError::ArtifactSnapshotUriMismatch {
                expected: context.artifact_snapshot_id,
                storage_uri: storage_uri.to_string(),
            },
        );
    }
    let storage_source_sandbox_id = storage_segments
        .as_mut()
        .and_then(Iterator::next)
        .and_then(|value| value.parse().ok());
    if storage_source_sandbox_id != Some(context.source_sandbox_id) {
        return Err(
            crate::snapshot_restore::RestoreError::ArtifactSourceUriMismatch {
                expected: context.source_sandbox_id,
                storage_uri: storage_uri.to_string(),
            },
        );
    }
    Ok(())
}

fn validate_artifact_restore_as_new(
    meta: &crate::snapshot_upload::SnapshotMeta,
    context: &RestoreArtifactAsNewContext,
    target_sandbox_id: &SandboxId,
) -> Result<(), crate::snapshot_restore::RestoreError> {
    let expected_kind = match context.artifact_kind {
        pplx_space_common_core::types::ArtifactKind::Disk => {
            pplx_space_common_core::types::ArtifactKind::Disk
        }
        pplx_space_common_core::types::ArtifactKind::Full => {
            return Err(crate::snapshot_restore::RestoreError::Other(
                "full artifact restore-as-new is not implemented".to_string(),
            ));
        }
    };
    if crate::snapshot_upload::artifact_kind_from_meta(meta) != expected_kind {
        return Err(crate::snapshot_restore::RestoreError::Other(format!(
            "restore artifact kind does not match requested {} artifact",
            expected_kind.as_str()
        )));
    }
    match meta.artifact_origin {
        Some(
            pplx_space_common_core::types::ArtifactOrigin::Snapshot
            | pplx_space_common_core::types::ArtifactOrigin::Fork,
        ) => {}
        _ => {
            return Err(crate::snapshot_restore::RestoreError::Other(
                "restore-as-new requires a snapshot or fork disk artifact".to_string(),
            ));
        }
    }
    crate::snapshot_restore::validate_restore_metadata_identity(
        meta,
        &target_sandbox_id.to_string(),
        &context.source_sandbox_id.to_string(),
    )?;
    if meta.encrypted {
        return Err(crate::snapshot_restore::RestoreError::Other(
            "artifact restore-as-new does not support encrypted artifacts".to_string(),
        ));
    }
    Ok(())
}

fn publish_rebound_artifact_snapshot_metadata(
    meta: &crate::snapshot_upload::SnapshotMeta,
    snapshot_dir: &std::path::Path,
) -> Result<(), crate::snapshot_restore::RestoreError> {
    let metadata_path = snapshot_dir.join("snapshot.json");
    let bytes = serde_json::to_vec_pretty(meta).map_err(|error| {
        crate::snapshot_restore::RestoreError::Other(format!(
            "serialize rebound artifact snapshot metadata: {error}"
        ))
    })?;
    crate::snapshot_scheduler::write_file_atomic(&metadata_path, &bytes).map_err(|error| {
        crate::snapshot_restore::RestoreError::Other(format!(
            "publish rebound artifact snapshot metadata: {error}"
        ))
    })
}

fn rebind_artifact_snapshot_identity(
    meta: &mut crate::snapshot_upload::SnapshotMeta,
    target_sandbox_id: &str,
    target_owner: &pplx_space_common_core::types::SandboxOwner,
) -> Result<(), crate::snapshot_restore::RestoreError> {
    meta.sandbox_id = target_sandbox_id.to_string();
    // The staged disk remains an immutable source artifact. It seeds a cold boot and must
    // not become the child's current suspend checkpoint.
    meta.local_artifact_id = None;
    meta.artifact_snapshot_name = None;
    rebind_artifact_snapshot_owner(meta.sandbox.as_mut(), target_owner)
}

fn publish_rebound_sandbox_artifact_metadata(
    meta: &crate::snapshot_upload::SnapshotMeta,
    sandbox_dir: &std::path::Path,
) -> Result<(), crate::snapshot_restore::RestoreError> {
    let sandbox_id = meta.sandbox_id.parse().map_err(|error| {
        crate::snapshot_restore::RestoreError::Other(format!(
            "invalid rebound sandbox identity: {error}"
        ))
    })?;
    let execution = meta
        .execution_metadata(super::tail_support::host_architecture().map_err(|error| {
            crate::snapshot_restore::RestoreError::Other(format!(
                "resolve host architecture for restored artifact: {error}"
            ))
        })?)
        .map_err(|error| {
            crate::snapshot_restore::RestoreError::Other(format!(
                "resolve restored artifact execution metadata: {error}"
            ))
        })?
        .into_execution();
    let template = meta.source.template_id.parse().map_err(|error| {
        crate::snapshot_restore::RestoreError::Other(format!(
            "invalid restored artifact template: {error}"
        ))
    })?;
    let content_hash = meta
        .source
        .content_hash
        .as_deref()
        .map(str::parse)
        .transpose()
        .map_err(|error| {
            crate::snapshot_restore::RestoreError::Other(format!(
                "invalid restored artifact content hash: {error}"
            ))
        })?;
    let sandbox_metadata = pplx_space_common_core::types::SandboxArtifactMetadata::new(
        sandbox_id,
        execution,
        template,
        content_hash,
    );
    let metadata_path = sandbox_dir.join("metadata.json");
    let bytes = serde_json::to_vec_pretty(&sandbox_metadata).map_err(|error| {
        crate::snapshot_restore::RestoreError::Other(format!(
            "serialize rebound sandbox artifact metadata: {error}"
        ))
    })?;
    crate::snapshot_scheduler::write_file_atomic(&metadata_path, &bytes).map_err(|error| {
        crate::snapshot_restore::RestoreError::Other(format!(
            "publish rebound sandbox artifact metadata: {error}"
        ))
    })
}

fn rebind_artifact_snapshot_owner(
    sandbox: Option<&mut crate::snapshot_upload::SnapshotSandbox>,
    target_owner: &pplx_space_common_core::types::SandboxOwner,
) -> Result<(), crate::snapshot_restore::RestoreError> {
    let sandbox =
        sandbox.ok_or(crate::snapshot_restore::RestoreError::MissingSandboxOwnershipMetadata)?;
    sandbox.owner = target_owner.to_string();
    Ok(())
}

#[cfg(test)]
mod tests;
