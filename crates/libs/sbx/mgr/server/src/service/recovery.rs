//! Startup and periodic repair of interrupted lifecycle operations.

use pplx_space_common_core::types::LocalArtifactId;

use super::cache_cleanup::{ReconcileSource, cleanup_orphaned_dirs, ensure_sandbox_cache_root_in};
use super::cleanup_support::{CleanupCompletion, CleanupLockTryResult, CleanupRecoveryTarget};
use super::create_support::*;
#[cfg(test)]
use super::suspend_snapshot_publication::recovered_sandbox_row_fields;
use super::suspend_snapshot_publication::{
    self, SuspendSnapshotPublicationDecision, canonical_suspend_snapshot_dir,
};
use super::*;
use futures_util::{StreamExt, stream};
use orphan_dispatch::AdmittedRecoveryKind;
use pplx_space_common_core::types::{
    SandboxId, SandboxIdParseError, SandboxQuarantineNextAction, SandboxQuarantineSource,
};
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
    emit_sandbox_event,
};
use pplx_space_metrics::sandbox::worker::{
    QuarantineRecoveryAttemptOutcome, QuarantineRecoveryOutcome,
};

const SUSPEND_RECOVERY_KIND: &str = "suspend";

/// Candidates admitted per inventory pass.
///
/// Each admission costs one bounded plan lookup against cluster-api plus a claim
/// write, both on the serial inventory reconciler. An unbounded cohort would
/// stretch a single tick by its own size. Discovery advances a finite rowid
/// sweep, examining at most 128 rows and returning at most eight candidates.
/// Deferred candidates cannot keep later rows out of subsequent batches.
///
/// A control-plane outage can still cost the plan-lookup timeout per admitted
/// candidate: 8 × 15s = 120s, twice the 60s tick. The multi-hour orphan restore
/// runs on a tracked task rather than in this pass, so it does not extend the tick.
/// A dedicated one-per-node permit bounds how much transfer and filesystem work
/// that dispatch can put in flight at once.
///
/// An episode whose recovery stopped at `QUARANTINE_RECOVERY_PLAN_REJECTION_BOUND`
/// is never a candidate, so it takes no admission from one that can recover.
///
/// A capacity-deferred orphan is not charged an attempt. Per-pass outcomes
/// describe only the returned batch. Backlog gauges are refreshed only when
/// the whole sweep completes. Partial scans publish no census; failures
/// invalidate the last sample.
const QUARANTINE_RECOVERY_ADMISSIONS_PER_PASS: usize = 8;

/// Node-wide concurrent orphan restores. One, because that is what the inline
/// pass effectively enforced and a snapshot restore is transfer plus filesystem
/// work, not a request. Raising it raises node I/O contention, so it is a constant
/// rather than a knob until there is a measurement to raise it against.
pub(super) const QUARANTINE_RECOVERY_ORPHAN_RESTORE_PERMITS: usize = 1;

/// Publish quarantine-recovery backlog observations from a completed sweep.
///
/// `None` means the scan failed. Such a pass publishes only
/// `sample_valid = 0`: replacing the last backlog value with a plausible one
/// read from an incomplete scan is what lets a stalled node look healthy.
fn publish_quarantine_recovery_backlog(scan: Option<&crate::db::QuarantineRecoveryScan>) {
    let Some(scan) = scan else {
        pplx_space_metrics::sandbox::worker::quarantine_recovery_backlog_sample_valid(false);
        return;
    };
    pplx_space_metrics::sandbox::worker::quarantine_recovery_backlog(
        scan.pending_count,
        scan.pending_oldest_age,
    );
    pplx_space_metrics::sandbox::worker::quarantine_recovery_candidates_malformed(
        scan.malformed_count,
    );
}

/// A pass that never acquired the claim has no episode row to charge, so the
/// outcome is reported without a durable retry write.
fn emit_quarantine_recovery_claim_not_acquired(
    reason: crate::db::QuarantineRecoveryNoProgressReason,
) {
    pplx_space_metrics::sandbox::worker::quarantine_recovery_no_progress(
        reason,
        QuarantineRecoveryAttemptOutcome::ClaimNotAcquired,
    );
}

/// Classify a failed orphan restore so the two terminal outcomes this path can
/// reach are distinguishable from an ordinary deferral on a dashboard.
fn quarantine_recovery_no_progress_reason(
    error: &crate::snapshot_restore::RestoreError,
) -> crate::db::QuarantineRecoveryNoProgressReason {
    // Exhaustive on purpose. This was a four-arm match over a catch-all, so 24 of
    // the 28 restore failures reported `orphan_restore_deferred` — a transfer that
    // needs retrying — including node-local database outages and evidence that
    // disagreed with itself. An operator reading the metric was sent to the
    // snapshot's storage for a broken disk. Listing every variant means a new one
    // cannot be added without deciding where it points.
    match error {
        // Terminal outcomes of the recovery machinery itself.
        crate::snapshot_restore::RestoreError::QuarantineRecoveryTimedOut { .. } => {
            crate::db::QuarantineRecoveryNoProgressReason::OrphanRestoreTimedOut
        }
        crate::snapshot_restore::RestoreError::QuarantineRecoveryClaimSuperseded { .. } => {
            crate::db::QuarantineRecoveryNoProgressReason::ClaimSuperseded
        }
        crate::snapshot_restore::RestoreError::QuarantineRecoveryAbandonedAtShutdown { .. } => {
            crate::db::QuarantineRecoveryNoProgressReason::AbandonedAtShutdown
        }
        crate::snapshot_restore::RestoreError::RenewQuarantineRecoveryClaim { .. } => {
            crate::db::QuarantineRecoveryNoProgressReason::ClaimRenewalFailed
        }
        // A lost phase CAS is the same class as a changed adoption fence: someone
        // else moved the row out from under this operation.
        crate::snapshot_restore::RestoreError::RestorePhaseTransitionLost { .. } => {
            crate::db::QuarantineRecoveryNoProgressReason::LocalAdoptionFenceChanged
        }
        // This node's database refused a read or a write.
        crate::snapshot_restore::RestoreError::ReadSandboxRow { .. }
        | crate::snapshot_restore::RestoreError::PersistRestoringRow { .. }
        | crate::snapshot_restore::RestoreError::AdvanceRestorePhase { .. }
        | crate::snapshot_restore::RestoreError::ReadQuarantineRecoveryClaim { .. }
        | crate::snapshot_restore::RestoreError::PersistOrphanQuarantineRecovery { .. }
        | crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned => {
            crate::db::QuarantineRecoveryNoProgressReason::NodeDatabaseFailure
        }
        // This node's filesystem refused an operation.
        crate::snapshot_restore::RestoreError::CreateDirectory { .. }
        | crate::snapshot_restore::RestoreError::RemoveFile { .. }
        | crate::snapshot_restore::RestoreError::RemoveDirectory { .. }
        | crate::snapshot_restore::RestoreError::RenamePath { .. } => {
            crate::db::QuarantineRecoveryNoProgressReason::NodeFilesystemFailure
        }
        // The restore's own evidence disagreed with itself.
        crate::snapshot_restore::RestoreError::SnapshotIdentityMismatch { .. }
        | crate::snapshot_restore::RestoreError::InvalidSandboxId { .. }
        | crate::snapshot_restore::RestoreError::InvalidRestoreSource { .. }
        | crate::snapshot_restore::RestoreError::ArtifactSnapshotUriMismatch { .. }
        | crate::snapshot_restore::RestoreError::ArtifactSourceUriMismatch { .. }
        | crate::snapshot_restore::RestoreError::InvalidRecoveredSandboxRow { .. }
        | crate::snapshot_restore::RestoreError::RootfsPolicy(..)
        | crate::snapshot_restore::RestoreError::MissingSandboxOwnershipMetadata => {
            crate::db::QuarantineRecoveryNoProgressReason::RestoreEvidenceMismatch
        }
        // The control plane declined to serve it.
        crate::snapshot_restore::RestoreError::ClusterApiUnconfigured(..)
        | crate::snapshot_restore::RestoreError::SnapshotNotRegistered(..)
        | crate::snapshot_restore::RestoreError::AdmissionRejected(..)
        | crate::snapshot_restore::RestoreError::QuarantineRecoveryPlanRpc { .. } => {
            crate::db::QuarantineRecoveryNoProgressReason::ControlPlaneRefused
        }
        crate::snapshot_restore::RestoreError::RestoreOperationBusy { .. } => {
            crate::db::QuarantineRecoveryNoProgressReason::OrphanRestoreLocalContention
        }
        // Contention over the canonical root, a parent template that is not yet
        // available, a node-manager receive that failed, and the genuinely
        // unclassified bag: all are the same instruction to an operator, which is
        // to wait for the retry.
        crate::snapshot_restore::RestoreError::CanonicalRootConflict { .. }
        | crate::snapshot_restore::RestoreError::ParentTemplateLookup { .. }
        | crate::snapshot_restore::RestoreError::BtrfsReceive { .. }
        | crate::snapshot_restore::RestoreError::Other(..) => {
            crate::db::QuarantineRecoveryNoProgressReason::OrphanRestoreDeferred
        }
    }
}

const SUSPEND_RECOVERY_STAGED_PUBLISH_COMPLETED: &str = "staged_publish_completed";
const SUSPEND_RECOVERY_CANONICAL_PINNED: &str = "canonical_pinned";
const SUSPEND_RECOVERY_PUBLISH_RETRY_FAILED: &str = "publish_retry_failed";
const SUSPEND_RECOVERY_ROLLED_BACK_TO_PAUSED: &str = "rolled_back_to_paused";
const SUSPEND_RECOVERY_QUARANTINED_NO_ARTIFACT_RUNTIME_GONE: &str =
    "quarantined_no_artifact_runtime_gone";
const SUSPEND_RECOVERY_DEFERRED: &str = "deferred";
pub(super) const STARTUP_RECOVERY_CONCURRENCY: usize = 8;

#[derive(Debug, thiserror::Error)]
pub enum StartupRecoveryError {
    #[error(transparent)]
    Inventory(#[from] super::cache_cleanup::InventoryError),
    #[error("startup recovery task failed: {0}")]
    WorkerTask(#[source] tokio::task::JoinError),
    #[error(transparent)]
    StateRecovery(#[from] crate::db::RecoverStartupStatesError),
    #[error("sandbox database mutex is poisoned")]
    DatabaseMutexPoisoned,
    #[error("startup recovery database operation failed: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("failed to inspect interrupted suspend snapshot publication: {0}")]
    SuspendSnapshot(#[from] suspend_snapshot_publication::SuspendSnapshotPublicationError),
    #[error(transparent)]
    StateWrite(#[from] crate::db::SandboxStateWriteNotApplied),
    #[error("failed to quarantine sandbox during startup recovery: {0}")]
    MarkQuarantined(#[from] crate::db::MarkQuarantinedError),
    #[error("startup recovery found an invalid sandbox ID: {0}")]
    InvalidSandboxId(#[from] SandboxIdParseError),
}

#[derive(Clone, Copy, Debug)]
pub(super) enum SuspendSnapshotPublicationRecoveryOrigin {
    StartupRecovery,
    PeriodicRecovery,
    LivePublishFailure,
}

impl SuspendSnapshotPublicationRecoveryOrigin {
    fn event_reason(self) -> &'static str {
        match self {
            Self::StartupRecovery => "startup_recovery",
            Self::PeriodicRecovery => "periodic_recovery",
            Self::LivePublishFailure => "live_publish_failure_recovery",
        }
    }

    fn runtime_live_event_reason(self) -> &'static str {
        match self {
            Self::StartupRecovery => "startup_recovery_runtime_live",
            Self::PeriodicRecovery => "periodic_recovery_runtime_live",
            Self::LivePublishFailure => "live_publish_failure_runtime_live",
        }
    }
}

/// The points in a recovery pass where another actor can interleave.
///
/// Every one of these is a place the pass either awaits or has released the
/// database lock, so a concurrent claimant, a cleanup owner, or an operator can
/// change the episode underneath it. They are named rather than left implicit
/// because a window nobody named is a window nobody tested: the claim-to-adoption
/// gap was unreachable from any test seam until it appeared as a review finding.
///
/// Adding a step to the pass that awaits or drops the lock means adding a variant
/// here — `every_named_pass_window_has_a_hook_in_the_pass` fails until the pass
/// hooks it, and `the_interleaving_matrix_fixture_adopts_when_nobody_interferes`
/// fails until a pass actually reaches it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RecoveryPassWindow {
    /// After the candidate scan, before the first candidate is touched.
    Scanned,
    /// After the plan lookup's RPC, before the claim is taken.
    PlanResolved,
    /// After the claim is written, before the cleanup fence is tried.
    Claimed,
    /// After the cleanup fence is held, before the artifact is inspected.
    CleanupFenceHeld,
    /// After the blocking artifact inspection, before the adoption transaction.
    ArtifactInspected,
    /// After the adoption transaction returns, before its outcome is written. The
    /// adoption is fenced itself, but the write that records its result is a second
    /// statement, so this is the gap between the two.
    AdoptionAttempted,
    /// After the orphan restore returns, before its outcome is written. The restore
    /// runs on the detached task rather than in the pass, and is the longest step of
    /// the whole recovery by orders of magnitude, so this is the window a claim is
    /// most likely to have been lost in by the time it is reached.
    RestoreCompleted,
    /// After a detached orphan restore is dispatched, before the pass claims its
    /// next candidate. This is the statement that makes the pass concurrent with
    /// itself: from here the task holds a claim, the node-wide permit, and the
    /// sandbox's staging lock while the pass keeps going. It is not an `await`, so
    /// nothing about its shape marks it as a fork point.
    OrphanDispatched,
}

/// What a recovery pass accepts as its interleaving probe.
///
/// A monomorphized bound, not a `dyn` object: the shipped probe is an empty
/// closure, so every one of the probe sites inlines to nothing. `Clone`, `Send`,
/// and `'static` are exactly what the detached restore task needs to own its own
/// copy — the pass keeps using the probe after it dispatches, so the task cannot
/// borrow it.
pub(crate) trait RecoveryPassProbe: Fn(RecoveryPassWindow) + Clone + Send + 'static {}

impl<P> RecoveryPassProbe for P where P: Fn(RecoveryPassWindow) + Clone + Send + 'static {}

impl RecoveryPassWindow {
    #[cfg(test)]
    pub(crate) const ALL: [Self; 8] = [
        Self::Scanned,
        Self::PlanResolved,
        Self::Claimed,
        Self::CleanupFenceHeld,
        Self::ArtifactInspected,
        Self::AdoptionAttempted,
        Self::RestoreCompleted,
        Self::OrphanDispatched,
    ];
}

/// A no-op probe. Production passes this, so the instrumented pass and the shipped
/// pass are the same code path rather than two that are meant to agree.
pub(crate) fn no_pass_probe() -> impl RecoveryPassProbe {
    |_| {}
}

impl SandboxManagerService {
    /// Reconcile startup inventory and cleanup, then restore watchdog and snapshot registrations.
    pub async fn recover_runtime_registrations(&self) -> Result<(), StartupRecoveryError> {
        self.retire_rowless_restore_transfer_scopes().await;
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            service.reconcile_sandbox_inventory(ReconcileSource::Startup)?;
            cleanup_orphaned_dirs(ReconcileSource::Startup);
            Ok::<_, StartupRecoveryError>(())
        })
        .await
        .map_err(StartupRecoveryError::WorkerTask)??;
        let service = self.clone();
        let cleanup_targets = tokio::task::spawn_blocking(move || {
            Self::recover_required_cleanup_targets(
                &service.db,
                super::recovery::CleanupRecoveryAuthority::Startup,
            )
        })
        .await
        .map_err(StartupRecoveryError::WorkerTask)?;
        let cleanup_targets = match cleanup_targets {
            Ok(targets) => targets,
            Err(error) => {
                if self.db.is_poisoned() {
                    self.fail_poisoned_database();
                }
                tracing::error!(%error, "startup cleanup discovery failed");
                return Err(error);
            }
        };
        self.schedule_cleanup_recovery_targets(cleanup_targets);

        self.watchdog.recover_all().await;

        // Recover snapshot scheduling for started sandboxes with snapshots_enabled
        let started = {
            let db = self.startup_recovery_db()?;
            crate::db::list_sandboxes_for_recovery(&db)?
        };
        for row in started
            .into_iter()
            .filter(|row| row.state == SandboxState::Started)
        {
            if row.pool_template.is_some() && row.snapshots_enabled {
                tracing::warn!(
                    sandbox_id = %row.id,
                    pool_template = row.pool_template.as_deref().unwrap_or_default(),
                    "disabling snapshots for pre-warm pool sandbox during recovery"
                );
                let db = self.startup_recovery_db()?;
                crate::db::update_snapshots_enabled(&db, &row.id, false)?;
                continue;
            }
            if row.snapshots_enabled {
                self.snapshot_scheduler
                    .add_sandbox(row.id, row.template)
                    .await;
            }
        }
        Ok(())
    }

    pub async fn recover_interrupted_startup_states(
        &self,
    ) -> Result<Vec<crate::db::StartupRecoveryRecord>, StartupRecoveryError> {
        let mut records = self.recover_interrupted_pauses().await?;
        records.extend(self.recover_interrupted_resident_operations().await?);
        // One listing feeds both the inspection of interrupted publications
        // and the prelaunch classification that consumes its answers.
        let rows = {
            let db = self.startup_recovery_db()?;
            crate::db::list_sandboxes_for_recovery(&db)?
        };
        let verdicts = self
            .suspending_retained_source_verdicts(Path::new(CACHE_DIR), &rows)
            .await;
        records.extend(self.recover_prelaunch_rows(Path::new(CACHE_DIR), &verdicts, rows)?);
        let db_records = {
            let db = self.startup_recovery_db()?;
            crate::db::recover_interrupted_startup_states(&db)?
        };
        records.extend(
            db_records
                .into_iter()
                .filter(|record| record.outcome.changed()),
        );

        let changed_ids = records
            .iter()
            .filter(|record| record.outcome.changed())
            .map(|record| record.id.clone())
            .collect::<Vec<_>>();
        if !changed_ids.is_empty() {
            mark_sandboxes_dirty(&self.db, changed_ids);
        }

        Ok(records)
    }

    async fn recover_interrupted_pauses(
        &self,
    ) -> Result<Vec<crate::db::StartupRecoveryRecord>, StartupRecoveryError> {
        let rows = {
            let db = self.startup_recovery_db()?;
            crate::db::list_sandboxes_for_recovery(&db)?
        };
        let failed = AtomicBool::new(false);
        stream::iter(
            rows.into_iter()
                .filter(|row| row.state == SandboxState::Pausing),
        )
        .take_while(|_| std::future::ready(!failed.load(Ordering::Relaxed)))
        .map(|row| {
            let failed = &failed;
            async move {
                let result = self
                    .recover_interrupted_pause_with_probe(row, |sandbox_id| async move {
                        crate::runtime_reconciliation::observe_firecracker_vm_state(
                            &sandbox_id,
                            "startup pause recovery",
                            &self.node_manager,
                        )
                        .await
                    })
                    .await;
                if result.is_err() {
                    failed.store(true, Ordering::Relaxed);
                }
                result
            }
        })
        .buffer_unordered(STARTUP_RECOVERY_CONCURRENCY)
        // Finish admitted peers before propagating a persistence failure.
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect()
    }

    pub(super) async fn recover_interrupted_pause_with_probe<Probe, ProbeFuture>(
        &self,
        row: crate::db::SandboxRow,
        probe: Probe,
    ) -> Result<crate::db::StartupRecoveryRecord, StartupRecoveryError>
    where
        Probe: FnMut(SandboxId) -> ProbeFuture,
        ProbeFuture: std::future::Future<
                Output = crate::runtime_reconciliation::FirecrackerVmStateObservation,
            >,
    {
        if !crate::db::sandbox_has_runtime_fields(&row) {
            return self
                .quarantine_interrupted_pause(row, "sandbox row has no retained runtime identity")
                .await;
        }
        let Ok(sandbox_id) = row.id.parse::<SandboxId>() else {
            return self
                .quarantine_interrupted_pause(row, "invalid sandbox ID; operator repair required")
                .await;
        };
        use crate::runtime_reconciliation::observe_stable_resident_state;
        use pplx_space_node_runtime_core::fc_api::FirecrackerVmState;
        let target = match observe_stable_resident_state(sandbox_id, probe).await {
            Ok(FirecrackerVmState::Running) => crate::db::PauseFailedRecoveryTarget::Started,
            Ok(FirecrackerVmState::Paused) => crate::db::PauseFailedRecoveryTarget::Paused,
            Ok(FirecrackerVmState::NotStarted) => {
                return self
                    .quarantine_interrupted_pause(row, "Firecracker reported NotStarted")
                    .await;
            }
            Err(error) => {
                return self
                    .quarantine_interrupted_pause(row, &error.to_string())
                    .await;
            }
        };
        let changed = {
            let db = self.startup_recovery_db()?;
            crate::db::recover_interrupted_pause_state(&db, &row.id, target)?
        };
        if changed {
            self.watchdog.record_lifecycle_edge(&row.id).await;
            if target == crate::db::PauseFailedRecoveryTarget::Paused {
                self.snapshot_scheduler.remove_sandbox(&row.id).await;
            }
            mark_sandbox_dirty(&self.db, &row.id);
            let event_type = match target {
                crate::db::PauseFailedRecoveryTarget::Started => SandboxEventType::PauseFailed,
                crate::db::PauseFailedRecoveryTarget::Paused => SandboxEventType::PauseCompleted,
            };
            emit_sandbox_event(
                &row.id,
                event_type,
                SandboxEventSource::SbxManager,
                SandboxEventDetails::default()
                    .reason("startup_recovery_runtime_state_observed")
                    .operation("recover_interrupted_pause_state")
                    .trigger(SandboxEventTrigger::System)
                    .with_from_state(SandboxState::Pausing.as_str())
                    .with_to_state(target.sandbox_state().as_str()),
            );
        }
        Ok(crate::db::StartupRecoveryRecord {
            id: row.id,
            previous_state: row.state,
            outcome: if changed {
                crate::db::StartupRecoveryOutcome::State(target.sandbox_state())
            } else {
                crate::db::StartupRecoveryOutcome::Deferred
            },
        })
    }

    async fn quarantine_interrupted_pause(
        &self,
        row: crate::db::SandboxRow,
        ambiguity: &str,
    ) -> Result<crate::db::StartupRecoveryRecord, StartupRecoveryError> {
        let reason = format!(
            "startup recovery could not establish a definitive interrupted pause outcome: {ambiguity}"
        );
        let changed = {
            let db = self.startup_recovery_db()?;
            crate::db::mark_quarantined_from_state(
                &db,
                &row.id,
                SandboxState::Pausing,
                SandboxQuarantineSource::SbxManager,
                SandboxQuarantineReasonCode::PauseFailed,
                &reason,
                SandboxQuarantineNextAction::RequiresOperatorAttention,
            )?
        };
        if changed {
            self.watchdog.record_lifecycle_edge(&row.id).await;
            mark_sandbox_dirty(&self.db, &row.id);
        }
        Ok(crate::db::StartupRecoveryRecord {
            id: row.id,
            previous_state: row.state,
            outcome: if changed {
                crate::db::StartupRecoveryOutcome::State(SandboxState::Quarantined)
            } else {
                crate::db::StartupRecoveryOutcome::Deferred
            },
        })
    }

    /// Node-manager's answers, then the blocking validation that consumes them.
    /// The caller holds the cleanup fence across both, so no cleanup owner can
    /// replace the artifact in between. Each node-manager call has its own
    /// deadline; the blocking validation is not cancellable.
    async fn inspect_canonical_artifact_for_adoption(
        &self,
        cache_dir: std::path::PathBuf,
        sandbox_id: SandboxId,
    ) -> Result<
        Result<
            suspend_snapshot_publication::ValidatedSuspendSnapshotArtifact,
            suspend_snapshot_publication::CanonicalSuspendSnapshotRecoveryError,
        >,
        tokio::task::JoinError,
    > {
        // Only the canonical artifact is validated here; staging directories
        // are the sweep's, so their sources are not discovered or inspected.
        let sandbox_dir = cache_dir.join(sandbox_id.to_string());
        let sources = match tokio::task::spawn_blocking(move || {
            suspend_snapshot_publication::retained_sources_in_canonical_snapshot(&sandbox_dir)
        })
        .await
        {
            Ok(sources) => sources,
            Err(error) => {
                tracing::warn!(%error, "retained source discovery task failed");
                Vec::new()
            }
        };
        let verdicts = self
            .inspect_retained_sources(sources, &mut Default::default())
            .await;
        tokio::task::spawn_blocking(move || {
            suspend_snapshot_publication::validate_canonical_suspend_snapshot_for_recovery(
                &cache_dir, sandbox_id, &verdicts,
            )
        })
        .await
    }

    /// Startup classifies interrupted publications synchronously, so inspect
    /// their retained sources first. No other owner runs before startup
    /// recovery finishes; classification still rechecks each identity.
    async fn suspending_retained_source_verdicts(
        &self,
        cache_dir: &Path,
        rows: &[crate::db::SandboxRow],
    ) -> crate::retained_source::RetainedSourceVerdicts {
        let sandbox_dirs: Vec<_> = rows
            .iter()
            .filter(|row| row.state == SandboxState::Suspending)
            .map(|row| cache_dir.join(&row.id))
            .collect();
        if sandbox_dirs.is_empty() {
            return Default::default();
        }
        self.inspect_retained_sources_in(sandbox_dirs, &mut Default::default())
            .await
    }

    /// Node-manager verdicts for the retained sources that suspend artifacts
    /// in `sandbox_dirs` name. Quarantine adoption and cleanup completion hold
    /// their artifact fences across this and the validation that consumes it.
    /// Periodic batches take fences only during validation, so an answer can
    /// be one budget old by then. It still applies only to the same artifact
    /// directory and source identity, and the cleanup owners that delete
    /// bundles hold the fences validation then waits for.
    pub(super) async fn inspect_retained_sources_in(
        &self,
        sandbox_dirs: Vec<std::path::PathBuf>,
        schedule: &mut crate::retained_source::InspectionSchedule,
    ) -> crate::retained_source::RetainedSourceVerdicts {
        let sources = match tokio::task::spawn_blocking(move || {
            sandbox_dirs
                .iter()
                .flat_map(|sandbox_dir| {
                    suspend_snapshot_publication::retained_sources_in_sandbox_dir(sandbox_dir)
                })
                .collect::<Vec<_>>()
        })
        .await
        {
            Ok(sources) => sources,
            Err(error) => {
                tracing::warn!(%error, "retained source discovery task failed");
                Vec::new()
            }
        };
        self.inspect_retained_sources(sources, schedule).await
    }

    async fn inspect_retained_sources(
        &self,
        sources: Vec<crate::retained_source::RetainedSource>,
        schedule: &mut crate::retained_source::InspectionSchedule,
    ) -> crate::retained_source::RetainedSourceVerdicts {
        crate::retained_source::inspect_in_background(
            self.retained_source_inspector.as_ref(),
            sources,
            crate::retained_source::BACKGROUND_INSPECTION_BUDGET,
            schedule,
        )
        .await
    }

    /// Prelaunch recovery over a fresh listing, for tests of one state.
    #[cfg(test)]
    pub(super) fn recover_prelaunch_startup_states_in(
        &self,
        cache_dir: &Path,
        verdicts: &crate::retained_source::RetainedSourceVerdicts,
    ) -> Result<Vec<crate::db::StartupRecoveryRecord>, StartupRecoveryError> {
        let rows = {
            let db = self.startup_recovery_db()?;
            crate::db::list_sandboxes_for_recovery(&db)?
        };
        self.recover_prelaunch_rows(cache_dir, verdicts, rows)
    }

    fn recover_prelaunch_rows(
        &self,
        cache_dir: &Path,
        verdicts: &crate::retained_source::RetainedSourceVerdicts,
        rows: Vec<crate::db::SandboxRow>,
    ) -> Result<Vec<crate::db::StartupRecoveryRecord>, StartupRecoveryError> {
        let mut records = Vec::new();
        for row in rows {
            match row.state {
                SandboxState::Creating => {
                    records.push(self.recover_interrupted_create(cache_dir, row)?);
                }
                SandboxState::Starting if !crate::db::sandbox_has_runtime_fields(&row) => {
                    records.push(self.recover_unlaunched_start(cache_dir, row)?);
                }
                SandboxState::Suspending => {
                    let id = row.id.clone();
                    let previous_state = row.state;
                    match self.recover_interrupted_suspend_snapshot_publication(
                        cache_dir,
                        row,
                        SuspendSnapshotPublicationRecoveryOrigin::StartupRecovery,
                        verdicts,
                    ) {
                        Ok(record) => records.push(record),
                        Err(error) => {
                            tracing::warn!(
                                sandbox_id = %id,
                                %error,
                                "deferred interrupted suspend publication recovery"
                            );
                            pplx_space_metrics::snapshot::create_recovery(
                                SUSPEND_RECOVERY_KIND,
                                SUSPEND_RECOVERY_DEFERRED,
                            );
                            records.push(crate::db::StartupRecoveryRecord {
                                id,
                                previous_state,
                                outcome: crate::db::StartupRecoveryOutcome::Deferred,
                            });
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(records)
    }

    /// One periodic pass over a discovered batch of rows: inspect the retained
    /// sources of its unowned publications, then recover exactly that batch.
    /// Answers are not carried to another batch. `hold` is released after the
    /// blocking recovery returns.
    pub(super) async fn recover_deferred_suspend_publication_batch(
        &self,
        cache_dir: &Path,
        rows: Vec<crate::db::SandboxRow>,
        shutdown: &CancellationToken,
        schedule: &mut crate::retained_source::InspectionSchedule,
        hold: impl Send + 'static,
    ) -> crate::retained_source::RetainedSourceVerdicts {
        let verdicts = self
            .inspect_retained_sources_in(
                rows.iter()
                    .filter(|row| unowned_publication(row))
                    .map(|row| cache_dir.join(&row.id))
                    .collect(),
                schedule,
            )
            .await;
        let (service, cache_dir, shutdown) =
            (self.clone(), cache_dir.to_path_buf(), shutdown.clone());
        let recovered = tokio::task::spawn_blocking(move || {
            let _hold = hold;
            if let Err(error) = service
                .recover_deferred_suspend_publications_in(&cache_dir, rows, &shutdown, &verdicts)
            {
                tracing::warn!(%error, "periodic suspend publication recovery failed; will retry");
            }
            verdicts
        })
        .await;
        recovered.unwrap_or_else(|error| {
            tracing::warn!(%error, "periodic suspend publication recovery task failed");
            Default::default()
        })
    }

    /// Retry only unowned publications. Startup's broader recovery is unsafe
    /// here: Creating/Starting and teardown-owned captures can still be live.
    pub(super) fn recover_deferred_suspend_publications_in(
        &self,
        cache_dir: &Path,
        rows: Vec<crate::db::SandboxRow>,
        shutdown: &CancellationToken,
        verdicts: &crate::retained_source::RetainedSourceVerdicts,
    ) -> Result<(), StartupRecoveryError> {
        for row in rows.into_iter().filter(unowned_publication) {
            if shutdown.is_cancelled() {
                break;
            }
            let wake = self.auto_lifecycle.wake_lock(&row.id);
            let Ok(_wake) = wake.try_lock() else { continue };
            let artifact = self.auto_lifecycle.artifact_lock(&row.id);
            let Ok(_artifact) = artifact.try_lock() else {
                continue;
            };
            let CleanupLockTryResult::Acquired(_cleanup) = self.try_cleanup_fence(&row.id) else {
                continue;
            };
            let id = row.id.clone();
            // Discovery has advanced past the whole batch. Isolate the entire
            // candidate, including associated evidence, or a retained bad row
            // can hide its siblings on every sweep.
            let recovered = (|| -> Result<(), StartupRecoveryError> {
                {
                    let db = self.startup_recovery_db()?;
                    if !crate::db::startup_resident_witness_current(&db, &row)?
                        || crate::db::teardown_suspend_status(&db, &row.id)?
                            .and_then(|status| status.operation)
                            .is_some_and(|operation| operation.status.is_active())
                    {
                        return Ok(());
                    }
                }
                self.recover_interrupted_suspend_snapshot_publication(
                    cache_dir,
                    row,
                    SuspendSnapshotPublicationRecoveryOrigin::PeriodicRecovery,
                    verdicts,
                )?;
                Ok(())
            })();
            if let Err(error) = recovered {
                tracing::warn!(sandbox_id = %id, %error, "suspend publication remains deferred; periodic recovery will retry");
            }
        }
        Ok(())
    }

    fn recover_interrupted_suspend_snapshot_publication(
        &self,
        cache_dir: &Path,
        row: crate::db::SandboxRow,
        origin: SuspendSnapshotPublicationRecoveryOrigin,
        verdicts: &crate::retained_source::RetainedSourceVerdicts,
    ) -> Result<crate::db::StartupRecoveryRecord, StartupRecoveryError> {
        let teardown_status = {
            let db = self.startup_recovery_db()?;
            crate::db::teardown_suspend_status(&db, &row.id)?
        };
        let teardown_generation = teardown_status
            .as_ref()
            .and_then(|status| status.operation.as_ref())
            .filter(|operation| operation.status.is_active())
            .map(|operation| operation.execution_generation);
        if let Some(status) = teardown_status
            && teardown_generation.is_some()
            && status.artifact.is_some()
        {
            pplx_space_metrics::snapshot::create_recovery(
                SUSPEND_RECOVERY_KIND,
                SUSPEND_RECOVERY_DEFERRED,
            );
            return Ok(crate::db::StartupRecoveryRecord {
                id: row.id,
                previous_state: row.state,
                outcome: crate::db::StartupRecoveryOutcome::Deferred,
            });
        }
        let sandbox_dir = cache_dir.join(&row.id);
        let runtime_live = (self.sandbox_runtime_live)(&row.id);
        let decision = suspend_snapshot_publication::classify_suspend_snapshot_publication(
            &sandbox_dir,
            row.current_local_artifact_id,
            runtime_live,
            verdicts,
        )?;
        self.apply_suspend_snapshot_publication_recovery_decision(
            row,
            sandbox_dir,
            decision,
            origin,
            teardown_generation,
            verdicts,
        )
    }

    pub(super) fn apply_suspend_snapshot_publication_recovery_decision(
        &self,
        row: crate::db::SandboxRow,
        sandbox_dir: std::path::PathBuf,
        decision: SuspendSnapshotPublicationDecision,
        origin: SuspendSnapshotPublicationRecoveryOrigin,
        teardown_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
        verdicts: &crate::retained_source::RetainedSourceVerdicts,
    ) -> Result<crate::db::StartupRecoveryRecord, StartupRecoveryError> {
        match decision {
            SuspendSnapshotPublicationDecision::CanonicalReady(artifact) => {
                if let Some(execution_generation) = teardown_generation {
                    return self.stage_recovered_teardown_artifact(
                        row,
                        execution_generation,
                        artifact.local_artifact_id,
                        artifact.path,
                    );
                }
                self.pin_recovered_suspend_artifact_for_cleanup(
                    row,
                    artifact.local_artifact_id,
                    artifact.path,
                    SUSPEND_RECOVERY_CANONICAL_PINNED,
                    origin,
                )
            }
            SuspendSnapshotPublicationDecision::StagedComplete(staged) => {
                let snapshot_dir = canonical_suspend_snapshot_dir(&sandbox_dir);
                let published = suspend_snapshot_publication::publish_suspend_snapshot_dir(
                    &staged.staging_dir,
                    &snapshot_dir,
                    staged.local_artifact_id,
                    verdicts,
                );
                let error = match published {
                    Ok(garbage) => {
                        for path in garbage {
                            drop(staging_cleanup::reclaim_garbage_now(
                                row.id.clone(),
                                path,
                                self.node_manager.clone(),
                            ));
                        }
                        None
                    }
                    Err(error) => Some(error),
                };
                if let Some(error) = error {
                    tracing::warn!(
                        sandbox_id = %row.id,
                        local_artifact_id = %staged.local_artifact_id,
                        staging_dir = %staged.staging_dir.display(),
                        snapshot_dir = %snapshot_dir.display(),
                        %error,
                        "deferred interrupted suspend publication recovery after publish retry failed"
                    );
                    pplx_space_metrics::snapshot::create_recovery(
                        SUSPEND_RECOVERY_KIND,
                        SUSPEND_RECOVERY_PUBLISH_RETRY_FAILED,
                    );
                    emit_sandbox_event(
                        &row.id,
                        SandboxEventType::LocalArtifactPublishFailed,
                        SandboxEventSource::SbxManager,
                        SandboxEventDetails::default()
                            .trigger(SandboxEventTrigger::System)
                            .reason(origin.event_reason())
                            .error(error.to_string())
                            .logged_by_caller()
                            .with_from_state(row.state.to_string())
                            .local_artifact_id(staged.local_artifact_id.to_string())
                            .artifact_kind("full")
                            .artifact_origin("suspend")
                            .local_path(snapshot_dir.to_string_lossy()),
                    );
                    pplx_space_metrics::snapshot::create_recovery(
                        SUSPEND_RECOVERY_KIND,
                        SUSPEND_RECOVERY_DEFERRED,
                    );
                    return Ok(crate::db::StartupRecoveryRecord {
                        id: row.id,
                        previous_state: row.state,
                        outcome: crate::db::StartupRecoveryOutcome::Deferred,
                    });
                }
                if let Some(execution_generation) = teardown_generation {
                    self.stage_recovered_teardown_artifact(
                        row,
                        execution_generation,
                        staged.local_artifact_id,
                        snapshot_dir,
                    )
                } else {
                    self.pin_recovered_suspend_artifact_for_cleanup(
                        row,
                        staged.local_artifact_id,
                        snapshot_dir,
                        SUSPEND_RECOVERY_STAGED_PUBLISH_COMPLETED,
                        origin,
                    )
                }
            }
            SuspendSnapshotPublicationDecision::RuntimeLiveWithoutUsableArtifact => {
                let db = self.startup_recovery_db()?;
                if crate::db::rollback_suspending_to_paused(&db, &row.id)? {
                    drop(db);
                    // Staging is rediscovered independently of this transition
                    // by the inventory loop, including after restart or a failed
                    // deletion. A rollback is not evidence that a writer exited.
                    pplx_space_metrics::snapshot::create_recovery(
                        SUSPEND_RECOVERY_KIND,
                        SUSPEND_RECOVERY_ROLLED_BACK_TO_PAUSED,
                    );
                    emit_sandbox_event(
                        &row.id,
                        SandboxEventType::SuspendFailed,
                        SandboxEventSource::SbxManager,
                        SandboxEventDetails::default()
                            .trigger(SandboxEventTrigger::System)
                            .reason(origin.runtime_live_event_reason())
                            .with_from_state(row.state.to_string())
                            .with_to_state(SandboxState::Paused.to_string()),
                    );
                    return Ok(crate::db::StartupRecoveryRecord {
                        id: row.id,
                        previous_state: row.state,
                        outcome: crate::db::StartupRecoveryOutcome::State(SandboxState::Paused),
                    });
                }
                pplx_space_metrics::snapshot::create_recovery(
                    SUSPEND_RECOVERY_KIND,
                    SUSPEND_RECOVERY_DEFERRED,
                );
                Ok(crate::db::StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: crate::db::StartupRecoveryOutcome::Deferred,
                })
            }
            SuspendSnapshotPublicationDecision::RuntimeGoneWithoutUsableArtifact => {
                let db = self.startup_recovery_db()?;
                let quarantined = crate::db::mark_suspending_quarantined(
                    &db,
                    &row.id,
                    crate::db::SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE,
                )?;
                drop(db);
                if !quarantined {
                    pplx_space_metrics::snapshot::create_recovery(
                        SUSPEND_RECOVERY_KIND,
                        SUSPEND_RECOVERY_DEFERRED,
                    );
                    return Ok(crate::db::StartupRecoveryRecord {
                        id: row.id,
                        previous_state: row.state,
                        outcome: crate::db::StartupRecoveryOutcome::Deferred,
                    });
                }
                pplx_space_metrics::snapshot::create_recovery(
                    SUSPEND_RECOVERY_KIND,
                    SUSPEND_RECOVERY_QUARANTINED_NO_ARTIFACT_RUNTIME_GONE,
                );
                emit_sandbox_event(
                    &row.id,
                    SandboxEventType::SuspendFailed,
                    SandboxEventSource::SbxManager,
                    SandboxEventDetails::default()
                        .trigger(SandboxEventTrigger::System)
                        .reason(crate::db::SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE)
                        .with_from_state(row.state.to_string())
                        .with_to_state(SandboxState::Quarantined.to_string()),
                );
                Ok(crate::db::StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: crate::db::StartupRecoveryOutcome::State(SandboxState::Quarantined),
                })
            }
        }
    }

    fn stage_recovered_teardown_artifact(
        &self,
        row: crate::db::SandboxRow,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        local_artifact_id: LocalArtifactId,
        snapshot_dir: std::path::PathBuf,
    ) -> Result<crate::db::StartupRecoveryRecord, StartupRecoveryError> {
        let db = self.startup_recovery_db()?;
        crate::db::stage_teardown_suspend_artifact(
            &db,
            &row.id,
            execution_generation,
            local_artifact_id,
            &snapshot_dir.to_string_lossy(),
        )?;
        pplx_space_metrics::snapshot::create_recovery(
            SUSPEND_RECOVERY_KIND,
            SUSPEND_RECOVERY_DEFERRED,
        );
        Ok(crate::db::StartupRecoveryRecord {
            id: row.id,
            previous_state: row.state,
            outcome: crate::db::StartupRecoveryOutcome::Deferred,
        })
    }

    fn pin_recovered_suspend_artifact_for_cleanup(
        &self,
        row: crate::db::SandboxRow,
        local_artifact_id: LocalArtifactId,
        local_path: std::path::PathBuf,
        recovery_outcome: &'static str,
        origin: SuspendSnapshotPublicationRecoveryOrigin,
    ) -> Result<crate::db::StartupRecoveryRecord, StartupRecoveryError> {
        let local_path = local_path.to_string_lossy().into_owned();
        let db = self.startup_recovery_db()?;
        crate::db::pin_suspended_artifact_for_cleanup(
            &db,
            &row.id,
            local_artifact_id,
            &local_path,
        )?;
        pplx_space_metrics::snapshot::create_recovery(SUSPEND_RECOVERY_KIND, recovery_outcome);
        emit_sandbox_event(
            &row.id,
            SandboxEventType::LocalArtifactPublished,
            SandboxEventSource::SbxManager,
            SandboxEventDetails::default()
                .trigger(SandboxEventTrigger::System)
                .reason(origin.event_reason())
                .with_from_state(row.state.to_string())
                .with_to_state(SandboxState::CleanupRequired.to_string())
                .local_artifact_id(local_artifact_id.to_string())
                .artifact_kind("full")
                .artifact_origin("suspend")
                .local_path(local_path),
        );
        Ok(crate::db::StartupRecoveryRecord {
            id: row.id,
            previous_state: row.state,
            outcome: crate::db::StartupRecoveryOutcome::CleanupRequired {
                keep_filesystem: true,
                completion: crate::db::CleanupCompletion::MarkSuspended,
            },
        })
    }

    fn recover_interrupted_create(
        &self,
        cache_dir: &Path,
        row: crate::db::SandboxRow,
    ) -> Result<crate::db::StartupRecoveryRecord, StartupRecoveryError> {
        {
            let db = self.startup_recovery_db()?;
            if crate::db::has_cleanup_episode(&db, &row.id)? {
                // Startup cleanup owns interrupted creates, including partial
                // unpublished clones. Promoting the row would retire its
                // armed deletion episode before resources are cleaned.
                return Ok(crate::db::StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: crate::db::StartupRecoveryOutcome::Deferred,
                });
            }
        }
        if let Err(error) = ensure_sandbox_cache_root_in(cache_dir, &row.id) {
            tracing::warn!(
                sandbox_id = %row.id,
                %error,
                "deferred startup recovery for interrupted create; cache root setup failed"
            );
            return Ok(crate::db::StartupRecoveryRecord {
                id: row.id,
                previous_state: row.state,
                outcome: crate::db::StartupRecoveryOutcome::Deferred,
            });
        }

        {
            let db = self.startup_recovery_db()?;
            crate::db::update_state(&db, &row.id, SandboxState::Created)?.require_applied()?;
        }
        Ok(crate::db::StartupRecoveryRecord {
            id: row.id,
            previous_state: row.state,
            outcome: crate::db::StartupRecoveryOutcome::State(SandboxState::Created),
        })
    }

    fn recover_unlaunched_start(
        &self,
        cache_dir: &Path,
        row: crate::db::SandboxRow,
    ) -> Result<crate::db::StartupRecoveryRecord, StartupRecoveryError> {
        let has_episode = {
            let db = self.startup_recovery_db()?;
            crate::db::has_cleanup_episode(&db, &row.id)?
        };
        if has_episode {
            // Even before runtime allocation, the episode owns rollback and
            // its receipt. Do not promote the row or fail a generic state write.
            return Ok(crate::db::StartupRecoveryRecord {
                id: row.id,
                previous_state: row.state,
                outcome: crate::db::StartupRecoveryOutcome::Deferred,
            });
        }
        let target_state = if sandbox_rootfs_exists_in(cache_dir, &row.id) {
            SandboxState::Stopped
        } else {
            if let Err(error) = ensure_sandbox_cache_root_in(cache_dir, &row.id) {
                tracing::warn!(
                    sandbox_id = %row.id,
                    %error,
                    "deferred startup recovery for interrupted start; cache root setup failed"
                );
                return Ok(crate::db::StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: crate::db::StartupRecoveryOutcome::Deferred,
                });
            }
            SandboxState::Created
        };

        {
            let db = self.startup_recovery_db()?;
            crate::db::update_state(&db, &row.id, target_state)?.require_applied()?;
        }
        Ok(crate::db::StartupRecoveryRecord {
            id: row.id,
            previous_state: row.state,
            outcome: crate::db::StartupRecoveryOutcome::State(target_state),
        })
    }

    pub(super) fn startup_recovery_db(
        &self,
    ) -> Result<MutexGuard<'_, rusqlite::Connection>, StartupRecoveryError> {
        self.db_conn()
            .map_err(|_| StartupRecoveryError::DatabaseMutexPoisoned)
    }

    pub(super) fn schedule_cleanup_recovery_targets(&self, targets: Vec<CleanupRecoveryTarget>) {
        for target in targets {
            self.schedule_recovered_cleanup(target);
        }
    }

    pub(super) async fn adopt_quarantine_recovery_episodes(
        &self,
        cache_dir: &Path,
        shutdown_token: &CancellationToken,
        scan_cursor: &mut crate::db::PeriodicQuarantineRecoveryScan,
    ) {
        self.adopt_quarantine_recovery_episodes_with_shutdown(
            scan_cursor,
            cache_dir,
            shutdown_token,
            |sandbox_id| {
                Box::pin(async move {
                    crate::snapshot_restore::quarantine_recovery_plan(&sandbox_id).await
                })
            },
            no_pass_probe(),
        )
        .await;
    }

    /// The pass with no shutdown token, for tests that are not about shutdown.
    #[cfg(test)]
    async fn adopt_quarantine_recovery_episodes_with<Resolve>(
        &self,
        cache_dir: &Path,
        resolve_plan: Resolve,
    ) where
        Resolve: Fn(
            String,
        ) -> std::pin::Pin<
            Box<
                dyn Future<
                        Output = Result<
                            crate::snapshot_restore::QuarantineRecoveryPlanResolution,
                            crate::snapshot_restore::RestoreError,
                        >,
                    > + Send,
            >,
        >,
    {
        self.adopt_quarantine_recovery_episodes_with_probe(
            cache_dir,
            resolve_plan,
            no_pass_probe(),
        )
        .await;
    }

    /// The pass with a probe at every named interleaving window, for the tests that
    /// drive a concurrent actor into one of them.
    ///
    /// Runs the production dispatch shape — the pass returns while the restore is
    /// still going — and then waits for what it dispatched, so a test can assert on
    /// a durable outcome without the pass itself joining the task.
    #[cfg(test)]
    async fn adopt_quarantine_recovery_episodes_with_probe<Resolve, Probe>(
        &self,
        cache_dir: &Path,
        resolve_plan: Resolve,
        probe: Probe,
    ) where
        Resolve: Fn(
            String,
        ) -> std::pin::Pin<
            Box<
                dyn Future<
                        Output = Result<
                            crate::snapshot_restore::QuarantineRecoveryPlanResolution,
                            crate::snapshot_restore::RestoreError,
                        >,
                    > + Send,
            >,
        >,
        Probe: RecoveryPassProbe,
    {
        // Measured before the pass, not against the full permit count: a test may
        // deliberately hold capacity for the whole pass to exercise the deferral,
        // and waiting for a permit it still owns would hang instead of asserting.
        let capacity_before = self.quarantine_recovery_restore_sem.available_permits();
        self.adopt_quarantine_recovery_episodes_with_shutdown(
            &mut crate::db::PeriodicQuarantineRecoveryScan::default(),
            cache_dir,
            &CancellationToken::new(),
            resolve_plan,
            probe,
        )
        .await;
        tests::wait_for_orphan_restore_capacity(self, capacity_before).await;
    }

    /// The pass itself, with the plan lookup supplied by the caller.
    ///
    /// The lookup is the one step that needs a control-plane connection and it
    /// sits in the middle of the loop, so without a seam here the whole
    /// orchestration — claim outcomes, plan dispositions, adoption branches, the
    /// no-progress classifier — is reachable only from a live cluster.
    async fn adopt_quarantine_recovery_episodes_with_shutdown<Resolve, Probe>(
        &self,
        scan_cursor: &mut crate::db::PeriodicQuarantineRecoveryScan,
        cache_dir: &Path,
        shutdown_token: &CancellationToken,
        resolve_plan: Resolve,
        probe: Probe,
    ) where
        Resolve: Fn(
            String,
        ) -> std::pin::Pin<
            Box<
                dyn Future<
                        Output = Result<
                            crate::snapshot_restore::QuarantineRecoveryPlanResolution,
                            crate::snapshot_restore::RestoreError,
                        >,
                    > + Send,
            >,
        >,
        Probe: RecoveryPassProbe,
    {
        let mut cursor = std::mem::take(scan_cursor);
        let scan = self
            .storage(move |db| {
                let batch = cursor
                    .next_batch(db)
                    .map_err(|error| Status::internal(format!("quarantine discovery: {error}")))?;
                Ok((cursor, batch))
            })
            .await;
        probe(RecoveryPassWindow::Scanned);
        let scan = match scan {
            Ok((cursor, scan)) => {
                *scan_cursor = cursor;
                scan
            }
            Err(error) => {
                tracing::warn!(%error, "failed to scan quarantine recovery episodes");
                publish_quarantine_recovery_backlog(None);
                return;
            }
        };
        if let Some(summary) = scan.completed_sweep.as_ref() {
            publish_quarantine_recovery_backlog(Some(summary));
        }
        pplx_space_metrics::sandbox::worker::quarantine_recovery_backlog_sample_valid(
            scan_cursor.has_recent_completed_sweep(),
        );

        let due = scan.candidates.len();
        let mut admitted = 0_usize;
        let mut capacity_deferred = 0_usize;
        let mut capacity_closed = 0_usize;
        // Set on the first refusal, so at most one orphan per pass is refused and
        // every orphan the pass reaches after that is counted without a further
        // attempt. A successful acquisition does not set it, and the permit is
        // released by every `continue` between here and the match that consumes it,
        // so a pass can reach the semaphore once per admitted candidate rather than
        // once overall — sizing a pass's control-plane load off "one lookup" would
        // undercount it. Observed on the Kind bench: two orphans whose plan lookup
        // was ineligible both took the single permit inside one pass, 3ms apart, with
        // no refusal between them. Carries *why* it was refused: a closed semaphore is a
        // different condition from a busy one, and counting the orphans behind it as
        // "deferred" would rebuild the exact conflation the separate gauge exists to
        // remove — 99 deferred beside 1 closed, on a node where nothing was busy at
        // all.
        let mut capacity_refusal: Option<tokio::sync::TryAcquireError> = None;
        for candidate in scan.candidates {
            if admitted == QUARANTINE_RECOVERY_ADMISSIONS_PER_PASS {
                break;
            }
            // Checked per candidate, not once per pass. The driver's shutdown arm
            // bounds the operation it wraps; it does nothing about the seven
            // candidates queued behind it, each of which would still spend up to
            // the plan-lookup timeout and then charge an attempt. Eight of those
            // outlast the service's drain budget, so the process is killed
            // mid-pass and whichever episode was claimed but not yet released
            // waits out its whole lease plus a backoff before anyone can retry it.
            if shutdown_token.is_cancelled() {
                tracing::info!(
                    admitted,
                    "abandoning the remaining quarantine recovery candidates at shutdown"
                );
                break;
            }
            let sandbox_id = candidate.sandbox_id;
            // Which branch this episode takes was decided by the eligibility gate
            // that read its evidence, and is carried on the candidate. Deriving it
            // here instead — from `candidate`'s copy of a mutable `sandboxes`
            // column — would put the choice in a second place, and the two would
            // only agree for as long as nobody changed one of them. What that costs
            // is specific now that the restore branch owns a node-wide permit: a
            // reason code `run_cross_node_restore_inner` refuses would take the
            // permit, spend a plan lookup and a claim, get `Ok(None)`, and repeat
            // every backoff interval forever while reporting
            // `orphan_restore_deferred`. One decision, carried, cannot drift.
            //
            // The restore branch reserves that node-wide I/O capacity before the
            // plan RPC and the claim, so another sandbox's long restore cannot charge
            // this episode an attempt. A capacity skip is not an admission, so keep
            // walking the ordered cohort; otherwise eight old orphans would hide
            // later artifact recoveries for the duration of the in-flight restore.
            let admitted_kind = match candidate.recovery {
                crate::db::QuarantineRecoveryBranch::AdoptLocalArtifact(adoptable) => {
                    AdmittedRecoveryKind::Artifact(adoptable)
                }
                crate::db::QuarantineRecoveryBranch::RestoreOrphanedStaging => {
                    // Tried at most once per pass. The scan is ordered by
                    // `entered_at`, so the first orphan to reach this line is the
                    // oldest due one; retrying per candidate would hand capacity
                    // released mid-pass to whichever orphan happened to be walked
                    // next, which is a younger one than the ones already refused.
                    // An orphan that loses that race can lose it every pass, so
                    // freed capacity waits for the next tick and the oldest gets
                    // first refusal again — at most 60s of idle capacity against an
                    // operation measured in hours.
                    //
                    // Still `continue` rather than `break`: artifact adoptions need
                    // no permit, and stopping the walk would let a queue of orphans
                    // hide them.
                    if let Some(refusal) = &capacity_refusal {
                        match refusal {
                            tokio::sync::TryAcquireError::NoPermits => {
                                capacity_deferred = capacity_deferred.saturating_add(1);
                            }
                            // Counted under the closed condition, not under busy
                            // capacity. The gauge does not read this tally — it reads
                            // `Semaphore::is_closed()` directly, because a pass can
                            // finish without walking an orphan. This count feeds the
                            // pass-summary log: the `capacity_closed` field, and the
                            // `remaining` subtraction that keeps a closed refusal from
                            // reading as a candidate the pass silently skipped.
                            tokio::sync::TryAcquireError::Closed => {
                                capacity_closed = capacity_closed.saturating_add(1);
                            }
                        }
                        continue;
                    }
                    match Arc::clone(&self.quarantine_recovery_restore_sem).try_acquire_owned() {
                        Ok(permit) => AdmittedRecoveryKind::Orphan(permit),
                        Err(tokio::sync::TryAcquireError::NoPermits) => {
                            capacity_refusal = Some(tokio::sync::TryAcquireError::NoPermits);
                            capacity_deferred = capacity_deferred.saturating_add(1);
                            tracing::debug!(
                                %sandbox_id,
                                local_quarantine_id = %candidate.local_quarantine_id,
                                "quarantine recovery orphan restore deferred by \
                                 node-wide capacity"
                            );
                            continue;
                        }
                        Err(tokio::sync::TryAcquireError::Closed) => {
                            capacity_refusal = Some(tokio::sync::TryAcquireError::Closed);
                            // Nothing closes this semaphore today. If something ever
                            // does, reporting it as `orphan_restore_capacity_deferred`
                            // would be indistinguishable from a node that is merely
                            // busy, so it gets its own reason — emitted at the end of
                            // the pass off the semaphore's own state, because a pass
                            // that never walks an orphan must report it too. Every
                            // pass that completes its candidate scan, that is; one
                            // whose scan fails returns before the loop.
                            //
                            // `continue`, not `break`: artifact adoptions need no
                            // permit, and abandoning the pass here would turn a
                            // closed orphan-restore semaphore into a total halt of
                            // recovery on the node.
                            capacity_closed = capacity_closed.saturating_add(1);
                            continue;
                        }
                    }
                }
            };
            admitted = admitted.saturating_add(1);
            // The plan lookup is a read-only RPC and runs before the claim, so a
            // slow control plane cannot burn the lease the claim installs and
            // leave every downstream exact-claim CAS to fail.
            let plan_resolution = resolve_plan(sandbox_id.to_string()).await;
            probe(RecoveryPassWindow::PlanResolved);
            // Re-checked on the far side of that RPC. The check above cannot cover
            // the lookup itself, so a token cancelled while it was in flight would
            // still charge an attempt and take a lease this process is about to
            // stop renewing — leaving the episode undue for a lease plus a backoff
            // for a pass that will never finish it. The lookup writes nothing
            // durable, so abandoning its result costs only the lookup.
            if shutdown_token.is_cancelled() {
                tracing::info!(
                    %sandbox_id,
                    admitted,
                    "abandoning a quarantine recovery candidate whose plan lookup \
                     outlived the shutdown signal"
                );
                break;
            }
            // The answer's effect on the run of plan rejections is written by the
            // claim itself, so an accepted plan ends the run even if the work it
            // dispatches never records an outcome.
            let plan_run = match &plan_resolution {
                Ok(crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(_)) => {
                    crate::db::PlanRejectionRunEffect::Ends
                }
                Ok(crate::snapshot_restore::QuarantineRecoveryPlanResolution::NoProgress(
                    reason,
                )) => crate::db::PlanRejectionRunEffect::of(*reason),
                Err(_) => crate::db::PlanRejectionRunEffect::of(
                    crate::db::QuarantineRecoveryNoProgressReason::PlanLookupFailed,
                ),
            };
            let claimed = {
                let Ok(db) = self.db_conn() else {
                    return;
                };
                match crate::db::claim_quarantine_recovery_candidate(&db, &candidate, plan_run) {
                    Ok(Some(claimed)) => claimed,
                    Ok(None) => {
                        // The acquire CAS pins nine conjuncts, so a miss means any
                        // one of them moved between the scan and the claim — the
                        // episode was replaced, its due time advanced, a cleanup
                        // owner appeared, the sandbox left an activatable state.
                        // Blaming "another owner" named one cause out of many.
                        tracing::debug!(
                            %sandbox_id,
                            local_quarantine_id = %candidate.local_quarantine_id,
                            "quarantine recovery candidate stopped being admissible before the claim"
                        );
                        emit_quarantine_recovery_claim_not_acquired(
                            crate::db::QuarantineRecoveryNoProgressReason::ClaimPreconditionsChanged,
                        );
                        continue;
                    }
                    Err(error) => {
                        tracing::warn!(
                            %sandbox_id,
                            local_quarantine_id = %candidate.local_quarantine_id,
                            error = %crate::db::describe_error(&error),
                            "failed to claim quarantine recovery candidate; abandoning this pass"
                        );
                        emit_quarantine_recovery_claim_not_acquired(
                            crate::db::QuarantineRecoveryNoProgressReason::ClaimAcquisitionFailed,
                        );
                        // No claim means no row to charge, so there is no durable
                        // backoff to write. A database that refused this write will
                        // refuse the rest, so abandon the pass rather than retrying
                        // the same failure once per remaining candidate; the
                        // reconciler's own interval then paces the retry.
                        break;
                    }
                }
            };
            probe(RecoveryPassWindow::Claimed);

            let plan = match plan_resolution {
                Ok(crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(plan))
                    if plan.reason_code == claimed.candidate.reason_code =>
                {
                    plan
                }
                Ok(crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(plan)) => {
                    tracing::info!(
                        %sandbox_id,
                        local_quarantine_id = %claimed.candidate.local_quarantine_id,
                        local_reason = %claimed.candidate.reason_code,
                        central_reason = %plan.reason_code,
                        "quarantine recovery plan no longer matches the local episode"
                    );
                    self.record_quarantine_recovery_no_progress(
                        &claimed,
                        crate::db::QuarantineRecoveryNoProgressReason::LocalReasonMismatch,
                    )
                    .await;
                    continue;
                }
                Ok(crate::snapshot_restore::QuarantineRecoveryPlanResolution::NoProgress(
                    reason,
                )) => {
                    self.record_quarantine_recovery_no_progress(&claimed, reason)
                        .await;
                    continue;
                }
                Err(error) => {
                    // Same disclosure as the deferral path below: a plan whose
                    // storage URI fails to parse reports it inside the error.
                    tracing::warn!(
                        %sandbox_id,
                        local_quarantine_id = %claimed.candidate.local_quarantine_id,
                        error_kind = %error.kind_label(),
                        error_detail = ?error.redacted_detail(),
                        "failed to resolve quarantine recovery plan"
                    );
                    self.record_quarantine_recovery_no_progress(
                        &claimed,
                        crate::db::QuarantineRecoveryNoProgressReason::PlanLookupFailed,
                    )
                    .await;
                    continue;
                }
            };
            let adoptable = match admitted_kind {
                AdmittedRecoveryKind::Artifact(adoptable) => adoptable,
                AdmittedRecoveryKind::Orphan(permit) => {
                    self.dispatch_quarantined_orphan_restore(claimed, plan, probe.clone(), permit);
                    probe(RecoveryPassWindow::OrphanDispatched);
                    continue;
                }
            };
            let local_artifact_id = adoptable.local_artifact_id();
            if Some(local_artifact_id) != plan.local_artifact_id {
                tracing::debug!(
                    %sandbox_id,
                    local_quarantine_id = %claimed.candidate.local_quarantine_id,
                    %local_artifact_id,
                    planned_artifact_id = ?plan.local_artifact_id,
                    "quarantine recovery retained because artifact identity changed"
                );
                self.record_quarantine_recovery_no_progress(
                    &claimed,
                    crate::db::QuarantineRecoveryNoProgressReason::PlanArtifactMismatch,
                )
                .await;
                continue;
            }
            // Checked here rather than before the plan RPC, so a busy fence costs
            // one RPC and two writes. Acquiring earlier would hold a cleanup
            // owner off for the length of that RPC, and a non-acquiring peek
            // would skip without charging an attempt, pinning the retry rate flat
            // for as long as cleanup holds the sandbox. The release below charges
            // the attempt and backs off, so a long-held fence costs this once per
            // backoff interval — growing to the cap — not once per tick.
            let CleanupLockTryResult::Acquired(operation_fence) =
                self.try_cleanup_fence(&sandbox_id.to_string())
            else {
                tracing::debug!(
                    %sandbox_id,
                    local_quarantine_id = %claimed.candidate.local_quarantine_id,
                    "quarantine recovery adoption deferred to the current cleanup owner"
                );
                self.record_quarantine_recovery_no_progress(
                    &claimed,
                    crate::db::QuarantineRecoveryNoProgressReason::CleanupFenceBusy,
                )
                .await;
                continue;
            };
            probe(RecoveryPassWindow::CleanupFenceHeld);
            // Deliberately not timed out either. `spawn_blocking` cannot be
            // cancelled, so returning early here meant dropping
            // `operation_fence` while the inspection was still walking the
            // snapshot directory — releasing the cleanup lock under a live
            // reader and letting a cleanup owner delete the tree beneath it.
            let inspected = self
                .inspect_canonical_artifact_for_adoption(cache_dir.to_path_buf(), sandbox_id)
                .await;
            probe(RecoveryPassWindow::ArtifactInspected);
            let artifact = match inspected {
                Ok(Ok(artifact)) if artifact.local_artifact_id == local_artifact_id => artifact,
                Ok(Ok(_)) => {
                    drop(operation_fence);
                    self.record_quarantine_recovery_no_progress(
                        &claimed,
                        crate::db::QuarantineRecoveryNoProgressReason::LocalArtifactMismatch,
                    )
                    .await;
                    continue;
                }
                Ok(Err(error)) => {
                    drop(operation_fence);
                    tracing::debug!(
                        %sandbox_id,
                        local_quarantine_id = %claimed.candidate.local_quarantine_id,
                        %error,
                        "quarantine recovery retained after artifact inspection"
                    );
                    self.record_quarantine_recovery_no_progress(
                            &claimed,
                            crate::db::QuarantineRecoveryNoProgressReason::LocalArtifactInspectionFailed,
                        )
                        .await;
                    continue;
                }
                Err(error) => {
                    drop(operation_fence);
                    tracing::warn!(
                        %sandbox_id,
                        local_quarantine_id = %claimed.candidate.local_quarantine_id,
                        %error,
                        "quarantine recovery inspection task failed"
                    );
                    self.record_quarantine_recovery_no_progress(
                            &claimed,
                            crate::db::QuarantineRecoveryNoProgressReason::LocalArtifactInspectionFailed,
                        )
                        .await;
                    continue;
                }
            };

            let service = self.clone();
            let task_claim = claimed.clone();
            let writer = super::drain::inherited_state_writer(&self.drain_state);
            let recovery_id = plan.recovery_id;
            let execution_generation = plan.execution_generation;
            // Deliberately not timed out. The blocking closure holds the
            // database mutex, so a timeout arm's first act — recording a
            // no-progress result — blocks on that same mutex until the
            // adoption finishes anyway: the deadline bounded nothing. Worse,
            // `spawn_blocking` is not cancellable, so an adoption that
            // committed after the deadline fired would have its success
            // reported as a stale claim, skipping `mark_sandbox_dirty` and
            // losing the publication. The claim lease is the bound here: if
            // the adoption outlives it, the exact-claim CAS inside refuses
            // and the pass retries under a fresh claim.
            let adopted = tokio::task::spawn_blocking(move || {
                let _writer = writer;
                let _operation_fence = operation_fence;
                let db = service.db_conn()?;
                crate::db::adopt_quarantine_cleanup_episode(
                    &db,
                    &task_claim,
                    adoptable,
                    execution_generation,
                    recovery_id,
                    &artifact.execution,
                    &artifact.snapshot_dir.to_string_lossy(),
                )
                .map_err(|error| Status::internal(crate::db::describe_error(&error)))
            })
            .await;
            probe(RecoveryPassWindow::AdoptionAttempted);
            match adopted {
                Ok(Ok(crate::db::QuarantineAdoptionOutcome::Adopted)) => {
                    tracing::info!(
                        %sandbox_id,
                        local_quarantine_id = %claimed.candidate.local_quarantine_id,
                        %recovery_id,
                        snapshot_id = %plan.snapshot_id,
                        reason_code = %claimed.candidate.reason_code,
                        "adopted quarantine into durable cleanup"
                    );
                    pplx_space_metrics::sandbox::worker::quarantine_recovery_succeeded(
                        QuarantineRecoveryOutcome::CleanupAdopted,
                    );
                    mark_sandbox_dirty(&self.db, &sandbox_id.to_string());
                }
                Ok(Ok(crate::db::QuarantineAdoptionOutcome::FenceChanged)) => {
                    // The adoption CAS missed, and the usual reason is that a
                    // cleanup owner arrived after the claim. Recording that
                    // through the no-progress write cannot work — that statement
                    // requires the cleanup episode to be absent, so it misses too
                    // and leaves the lease in place until it lapses, keeping the
                    // episode out of the candidate set for a lease plus a backoff
                    // rather than a backoff.
                    let reason =
                        crate::db::QuarantineRecoveryNoProgressReason::LocalAdoptionFenceChanged;
                    self.record_quarantine_recovery_no_progress(&claimed, reason)
                        .await;
                }
                Ok(Err(error)) => {
                    tracing::warn!(
                        %sandbox_id,
                        local_quarantine_id = %claimed.candidate.local_quarantine_id,
                        %error,
                        "quarantine recovery adoption failed against the database"
                    );
                    self.record_quarantine_recovery_no_progress(
                        &claimed,
                        crate::db::QuarantineRecoveryNoProgressReason::LocalAdoptionFailed,
                    )
                    .await;
                }
                Err(error) => {
                    tracing::warn!(
                        %sandbox_id,
                        local_quarantine_id = %claimed.candidate.local_quarantine_id,
                        %error,
                        "quarantine recovery adoption task panicked or was cancelled"
                    );
                    self.record_quarantine_recovery_no_progress(
                        &claimed,
                        crate::db::QuarantineRecoveryNoProgressReason::LocalAdoptionFailed,
                    )
                    .await;
                }
            }
        }
        // Gauges on their own names, once per pass. Capacity is a property of the
        // node, not an attempt that made no progress, and a due cohort can be
        // hundreds deep behind one multi-hour restore — folding that into the
        // attempt counter would add the cohort size to it every tick, against a
        // prior per-pass ceiling of the admission cap, and distort every rate built
        // on it. A gauge reports the depth without accumulating.
        let capacity_closed_now = self.quarantine_recovery_restore_sem.is_closed();
        pplx_space_metrics::sandbox::worker::quarantine_recovery_orphan_capacity(
            capacity_deferred,
            capacity_closed_now,
        );
        // The closed reading comes off the semaphore above, not off the loop's
        // counter: a pass whose admission budget goes to artifact adoptions before
        // it walks a single orphan never observes the closed semaphore, and that is
        // exactly the node with other work queued ahead of it. The log carries no
        // sandbox id — the condition is node-wide, and naming whichever orphan
        // happened to be walked first sends an operator to the wrong place.
        if capacity_closed_now {
            tracing::error!(
                "node-wide orphan restore capacity is closed; no orphan restore can \
                 be admitted on this node"
            );
        }
        // One line per pass, not one per condition: a pass can both pace and defer,
        // and two summaries carrying the same numbers under different messages make
        // the reader diff message strings to learn which condition held. The
        // `remaining`, `capacity_deferred`, and `capacity_closed` fields say it
        // instead.
        let remaining = due
            .saturating_sub(admitted)
            .saturating_sub(capacity_deferred)
            .saturating_sub(capacity_closed);
        if capacity_deferred > 0 || capacity_closed > 0 || remaining > 0 {
            tracing::info!(
                due,
                admitted,
                capacity_deferred,
                capacity_closed,
                remaining,
                "quarantine recovery pass left candidates for a later pass"
            );
        }
    }

    async fn record_quarantine_recovery_no_progress(
        &self,
        claimed: &crate::db::ClaimedQuarantineRecoveryCandidate,
        reason: crate::db::QuarantineRecoveryNoProgressReason,
    ) {
        let sandbox_id = claimed.candidate.sandbox_id;
        let episode_age_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(claimed.candidate.entered_at);
        let outcome = {
            let Ok(db) = self.db_conn() else {
                return;
            };
            crate::db::record_quarantine_recovery_no_progress(&db, claimed, reason)
        };
        match outcome {
            Ok(outcome) => {
                // A stale claim here is a fenced write that missed because another
                // owner took the episode, which is the same loss the cleanup-
                // contention path reports. Emitting on one and not the other left a
                // timeline where the contention loss was visible and this one was
                // not, so an operator reading the events saw the episode simply stop.
                // Keyed off the database outcome rather than the metrics tag, so a
                // future tag rename cannot change which events exist.
                if matches!(
                    outcome,
                    crate::db::QuarantineRecoveryNoProgressOutcome::StaleClaim
                ) {
                    emit_sandbox_event(
                        &sandbox_id.to_string(),
                        SandboxEventType::LifecycleTransitionCasLost,
                        SandboxEventSource::SbxManager,
                        SandboxEventDetails::default()
                            .trigger(SandboxEventTrigger::AutoLifecycle)
                            .operation("quarantine_recovery_no_progress")
                            .reason(reason.as_str())
                            .rows_affected("0"),
                    );
                }
                pplx_space_metrics::sandbox::worker::quarantine_recovery_no_progress(
                    reason,
                    match outcome {
                        crate::db::QuarantineRecoveryNoProgressOutcome::StaleClaim => {
                            QuarantineRecoveryAttemptOutcome::StaleClaim
                        }
                        crate::db::QuarantineRecoveryNoProgressOutcome::RetryScheduled {
                            ..
                        } => QuarantineRecoveryAttemptOutcome::RetryScheduled,
                        crate::db::QuarantineRecoveryNoProgressOutcome::AutomaticRecoveryStopped {
                            ..
                        } => QuarantineRecoveryAttemptOutcome::AutomaticRecoveryStopped,
                    },
                );
                // Once per arming: a stopped episode is never claimed again, so
                // this cannot repeat until a re-arm grants another attempt.
                if let crate::db::QuarantineRecoveryNoProgressOutcome::AutomaticRecoveryStopped {
                    attempt_count,
                    consecutive_plan_rejections,
                } = outcome
                {
                    tracing::warn!(
                        %sandbox_id,
                        local_quarantine_id = %claimed.candidate.local_quarantine_id,
                        reason_code = %claimed.candidate.reason_code,
                        episode_age_secs,
                        %reason,
                        attempt_count,
                        consecutive_plan_rejections,
                        "stopped automatic quarantine recovery after consecutive plan \
                         rejections; the hold needs operator attention"
                    );
                    return;
                }
                tracing::info!(
                    %sandbox_id,
                    local_quarantine_id = %claimed.candidate.local_quarantine_id,
                    claim_token = %claimed.claim_token,
                    episode_age_secs,
                    %reason,
                    ?outcome,
                    "recorded bounded quarantine recovery no-progress result"
                );
            }
            Err(error) => {
                // Emit even here. The loop's health signal is this counter, and
                // going silent exactly when node-local SQLite is failing hides the
                // outage behind an absence — the pass looks idle rather than
                // broken. The claim was held, so `claim_not_acquired` would say the
                // opposite of what happened; the write is what failed.
                pplx_space_metrics::sandbox::worker::quarantine_recovery_no_progress(
                    reason,
                    QuarantineRecoveryAttemptOutcome::RetryWriteFailed,
                );
                tracing::warn!(
                %sandbox_id,
                local_quarantine_id = %claimed.candidate.local_quarantine_id,
                claim_token = %claimed.claim_token,
                episode_age_secs,
                %reason,
                %error,
                "failed to record quarantine recovery no-progress result"
                );
            }
        }
    }

    /// Re-arm every stopped quarantine recovery, allowing one more plan rejection
    /// before it stops again. Called once at startup; see
    /// `crate::db::rearm_stopped_quarantine_recoveries`.
    pub fn rearm_stopped_quarantine_recoveries(&self) -> Result<usize, StartupRecoveryError> {
        let db = self.startup_recovery_db()?;
        Ok(crate::db::rearm_stopped_quarantine_recoveries(&db)?)
    }

    fn schedule_recovered_cleanup(&self, target: CleanupRecoveryTarget) {
        self.schedule_async_cleanup(super::cleanup::AsyncCleanupRequest {
            sandbox_id: target.sandbox_id,
            uuid8: target.uuid8,
            keep_filesystem: target.keep_filesystem,
            context: "cleanup recovery failed",
            completion: target.completion,
            immediate_kill: Self::cleanup_immediate_kill(target.completion),
            guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
            persist_intent: false,
            cleanup_id: target.cleanup_id,
        });
    }

    pub(super) fn recover_required_cleanup_targets(
        db: &Arc<Mutex<rusqlite::Connection>>,
        authority: CleanupRecoveryAuthority,
    ) -> Result<Vec<CleanupRecoveryTarget>, StartupRecoveryError> {
        let rows = {
            let db = db
                .lock()
                .map_err(|_| StartupRecoveryError::DatabaseMutexPoisoned)?;
            crate::db::list_sandboxes_for_recovery(&db)?
        };

        Self::recover_required_cleanup_targets_from_rows(db, rows, authority)
    }

    fn recover_required_cleanup_targets_for_ids(
        db: &Arc<Mutex<rusqlite::Connection>>,
        sandbox_ids: Vec<SandboxId>,
    ) -> Result<Vec<CleanupRecoveryTarget>, StartupRecoveryError> {
        let rows = {
            let db = db
                .lock()
                .map_err(|_| StartupRecoveryError::DatabaseMutexPoisoned)?;
            sandbox_ids
                .into_iter()
                .filter_map(|sandbox_id| {
                    let sandbox_id = sandbox_id.to_string();
                    match crate::db::get_sandbox(&db, &sandbox_id) {
                        Ok(row) => row,
                        Err(error) => {
                            tracing::warn!(
                                %sandbox_id,
                                %error,
                                "failed to load sandbox row for cleanup recovery"
                            );
                            None
                        }
                    }
                })
                .collect()
        };

        Self::recover_required_cleanup_targets_from_rows(
            db,
            rows,
            CleanupRecoveryAuthority::RequestedOnly,
        )
    }

    pub(super) fn recover_requested_cleanup_targets_for_ids(
        &self,
        sandbox_ids: Vec<SandboxId>,
    ) -> Vec<CleanupRecoveryTarget> {
        let mut targets = Vec::new();
        for sandbox_id in sandbox_ids {
            let sandbox_id_string = sandbox_id.to_string();
            let CleanupLockTryResult::Acquired(_fence) = self.try_cleanup_fence(&sandbox_id_string)
            else {
                tracing::debug!(
                    %sandbox_id,
                    "requested cleanup recovery deferred to the current lifecycle owner"
                );
                continue;
            };
            match Self::recover_required_cleanup_targets_for_ids(&self.db, vec![sandbox_id]) {
                Ok(recovered) => targets.extend(recovered),
                Err(error) => {
                    if self.db.is_poisoned() {
                        self.fail_poisoned_database();
                    }
                    tracing::error!(%error, "cleanup recovery discovery failed");
                    break;
                }
            }
        }
        targets
    }

    #[cfg(test)]
    pub(super) fn recover_requested_cleanup_targets(&self) -> Vec<CleanupRecoveryTarget> {
        let sandbox_ids = {
            let db = self.db.lock().expect("mutex poisoned");
            match crate::db::list_requested_cleanup_recovery_candidate_ids(&db) {
                Ok(sandbox_ids) => sandbox_ids,
                Err(error) => {
                    let error_detail = crate::db::describe_error(&error);
                    tracing::warn!(
                        error = %error_detail,
                        "failed to list sandboxes for requested cleanup recovery"
                    );
                    Vec::new()
                }
            }
        };
        self.recover_requested_cleanup_targets_for_ids(sandbox_ids)
    }

    fn recover_required_cleanup_targets_from_rows(
        db: &Arc<Mutex<rusqlite::Connection>>,
        rows: Vec<crate::db::SandboxRow>,
        authority: CleanupRecoveryAuthority,
    ) -> Result<Vec<CleanupRecoveryTarget>, StartupRecoveryError> {
        let mut targets = Vec::new();
        for row in rows {
            if row.state == SandboxState::Failed {
                continue;
            }
            let cleanup_episode = {
                let db = db
                    .lock()
                    .map_err(|_| StartupRecoveryError::DatabaseMutexPoisoned)?;
                match crate::db::cleanup_episode(&db, &row.id) {
                    Ok(episode) => episode,
                    Err(error) => {
                        tracing::warn!(
                            sandbox_id = %row.id,
                            %error,
                            "failed to load durable cleanup episode"
                        );
                        continue;
                    }
                }
            };
            if let Some(episode) = cleanup_episode {
                if episode.completion == CleanupCompletion::DeletePoolClone
                    && row.state == SandboxState::Quarantined
                {
                    continue; // The operator hold retains disposal authority across restart.
                }
                if episode.requested_at.is_none() {
                    if authority == CleanupRecoveryAuthority::RequestedOnly {
                        tracing::debug!(
                            sandbox_id = %row.id,
                            cleanup_id = %episode.id,
                            "leaving armed activation cleanup with its live owner"
                        );
                        continue;
                    }
                    if row.state != episode.activation_state {
                        let retired = {
                            let db = db
                                .lock()
                                .map_err(|_| StartupRecoveryError::DatabaseMutexPoisoned)?;
                            crate::db::retire_stale_armed_cleanup_episode(
                                &db, &row.id, episode.id, row.state,
                            )
                        };
                        if let Err(error) = retired {
                            tracing::warn!(
                                sandbox_id = %row.id,
                                cleanup_id = %episode.id,
                                %error,
                                "failed to retire stale armed cleanup episode"
                            );
                        }
                        continue;
                    }
                    let reason_code = match episode.activation_state {
                        SandboxState::Creating => SandboxQuarantineReasonCode::CreateInterrupted,
                        SandboxState::Starting => {
                            SandboxQuarantineReasonCode::ColdActivationInterrupted
                        }
                        SandboxState::Restoring => {
                            SandboxQuarantineReasonCode::RestoreActivationInterrupted
                        }
                        _ => continue,
                    };
                    let requested = {
                        let db = db
                            .lock()
                            .map_err(|_| StartupRecoveryError::DatabaseMutexPoisoned)?;
                        crate::db::request_cleanup_episode(
                            &db,
                            &row.id,
                            episode.id,
                            reason_code,
                            "cleanup recovery resumed an interrupted activation cleanup",
                        )
                    };
                    match requested {
                        Ok(crate::db::CleanupEpisodeRequestOutcome::Requested) => {
                            pplx_space_metrics::sandbox::worker::activation_cleanup(
                                pplx_space_metrics::sandbox::worker::ActivationCleanupOutcome::Requested,
                            );
                        }
                        Ok(crate::db::CleanupEpisodeRequestOutcome::AlreadyRequested) => {}
                        Ok(crate::db::CleanupEpisodeRequestOutcome::Superseded) => continue,
                        Err(error) => {
                            tracing::warn!(
                                sandbox_id = %row.id,
                                cleanup_id = %episode.id,
                                %error,
                                "failed to request interrupted activation cleanup"
                            );
                            continue;
                        }
                    }
                }
                let sandbox_id = match SandboxId::from_str(&row.id) {
                    Ok(sandbox_id) => sandbox_id,
                    Err(error) => {
                        tracing::warn!(sandbox_id = %row.id, %error, "invalid cleanup sandbox id");
                        continue;
                    }
                };
                targets.push(CleanupRecoveryTarget {
                    sandbox_id,
                    uuid8: row.uuid8,
                    keep_filesystem: episode.keep_filesystem,
                    completion: episode.completion,
                    cleanup_id: Some(episode.id),
                });
                continue;
            }
            let cleanup_intent = match authority {
                CleanupRecoveryAuthority::Startup => {
                    crate::db::cleanup_intent_for_startup_recovery(&row)
                }
                CleanupRecoveryAuthority::RequestedOnly => {
                    // State alone is authoritative for teardown states entered
                    // under the cleanup fence and the settled legacy cleanup
                    // owner. A live suspend, pause, resume, checkpoint, start,
                    // or restore does not hold that fence and must not be
                    // adopted by the periodic reconciler.
                    match (row.cleanup_keep_filesystem, row.cleanup_completion) {
                        (Some(keep_filesystem), Some(completion))
                            if completion.owns_row_intent(&row) =>
                        {
                            Some((keep_filesystem, completion))
                        }
                        (None, None) if row.state == SandboxState::Stopping => {
                            Some((true, CleanupCompletion::MarkStopped))
                        }
                        (None, None) if row.state == SandboxState::Deleting => {
                            Some((false, CleanupCompletion::DeleteRow))
                        }
                        (None, None) if row.state == SandboxState::CleanupRequired => {
                            Some((true, CleanupCompletion::Quarantine))
                        }
                        (None, None)
                            if matches!(row.state, SandboxState::CleanupFailed)
                                && crate::db::sandbox_has_runtime_fields(&row) =>
                        {
                            Some((true, CleanupCompletion::Quarantine))
                        }
                        _ => None,
                    }
                }
            };
            let Some((keep_filesystem, completion)) = cleanup_intent else {
                continue;
            };
            let must_persist_intent = !completion.owns_row_intent(&row);
            if must_persist_intent {
                let db = db
                    .lock()
                    .map_err(|_| StartupRecoveryError::DatabaseMutexPoisoned)?;
                let reason = row
                    .failure_reason
                    .as_deref()
                    .unwrap_or("cleanup reconciliation required");
                let persisted = match authority {
                    CleanupRecoveryAuthority::Startup => crate::db::mark_cleanup_required(
                        &db,
                        &row.id,
                        keep_filesystem,
                        completion,
                        reason,
                    )
                    .map(|()| true),
                    CleanupRecoveryAuthority::RequestedOnly => {
                        crate::db::mark_cleanup_required_if_no_intent_in_state(
                            &db,
                            &row.id,
                            row.state,
                            keep_filesystem,
                            completion,
                            reason,
                        )
                    }
                };
                match persisted {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(error) => {
                        tracing::warn!(
                            sandbox_id = %row.id,
                            %error,
                            "failed to persist cleanup recovery requirement"
                        );
                        continue;
                    }
                };
            }

            let sandbox_id = match SandboxId::from_str(&row.id) {
                Ok(sandbox_id) => sandbox_id,
                Err(error) => {
                    tracing::warn!(
                        sandbox_id = %row.id,
                        %error,
                        "skipping cleanup recovery for invalid sandbox id"
                    );
                    continue;
                }
            };

            targets.push(CleanupRecoveryTarget {
                sandbox_id,
                uuid8: row.uuid8,
                keep_filesystem,
                completion,
                cleanup_id: None,
            });
        }
        Ok(targets)
    }

    /// Spawn a background task that keeps SQLite aligned with node-local disk/runtime state.
    /// Lifecycle and inventory discovery advance in bounded batches. Ownership
    /// survives each admitted filesystem operation; batching is not an I/O
    /// deadline and does not permit abandoning a blocking mutation during drain.
    pub fn start_inventory_reconciler(
        &self,
        shutdown_token: CancellationToken,
        task_tracker: &TaskTracker,
    ) {
        self.ensure_cleanup_executor();
        let service = self.clone();
        let guard = self.critical_task_guard("inventory reconciler", &shutdown_token);
        task_tracker.spawn(async move {
            let _guard = guard;
            let mut recovery_cursor = crate::db::PeriodicLifecycleRecoveryScan::default();
            let mut staging_scan = staging_cleanup::StagingCleanupScan::default();
            let mut node_client = service.node_manager.lazy();
            let mut inventory_scan = super::cache_cleanup::PeriodicInventoryScan::default();
            let mut quarantine_scan = crate::db::PeriodicQuarantineRecoveryScan::default();
            let mut transfer_scan = crate::db::inventory::RestoreTransferScopeScan::default();
            let mut retained_pending = crate::retained_source::PendingRetainedSources::default();
            let mut retained_schedule = crate::retained_source::InspectionSchedule::default();
            loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    tracing::info!("sandbox inventory reconciler shutting down");
                    break;
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {}
            }
            let Ok(state_writer) = service.state_writer_lease("PeriodicLifecycleRecovery") else {
                // Dependency replacement and capacity pressure are transient;
                // the next tick owns the retry, not process shutdown.
                continue;
            };
            super::drain::with_state_writer(
                state_writer.clone(),
                service.retire_rowless_restore_transfer_scope_batch(&mut transfer_scan, &shutdown_token),
            ).await;
            if shutdown_token.is_cancelled() { break; }
            match service.storage(move |db| {
                crate::db::reconcile_terminal_teardown_suspend_jobs(db)
                    .map_err(|error| Status::internal(format!("teardown job reconciliation: {error}")))
            }).await {
                Ok(failed) if failed > 0 => tracing::info!(failed, "reconciled terminal teardown suspend jobs"),
                Ok(_) => {},
                Err(error) => tracing::warn!(%error, "teardown suspend job reconciliation failed; will retry"),
            }
            let mut cursor = std::mem::take(&mut recovery_cursor);
            let candidates = match service.storage(move |db| {
                let rows = crate::db::periodic_lifecycle_recovery_batch(db, &mut cursor)
                    .map_err(|error| Status::internal(format!("lifecycle discovery: {error}")))?;
                Ok((cursor, rows))
            }).await {
                Ok((cursor, rows)) => { recovery_cursor = cursor; rows },
                Err(error) => {
                    tracing::warn!(%error, "periodic lifecycle discovery failed; will retry");
                    continue;
                }
            };
            let (publications, residents) = candidates.into_iter()
                .partition(|row| row.state == SandboxState::Suspending);
            service.recover_periodic_resident_operations(residents, &shutdown_token).await;
            // Each batch is inspected and then validated as one unit, so
            // synchronous validation never waits on node-manager and every
            // answer reaches the rows it was asked for.
            let publication_verdicts = service
                .recover_deferred_suspend_publication_batch(
                    Path::new(CACHE_DIR), publications, &shutdown_token, &mut retained_schedule, state_writer.clone(),
                )
                .await;
            let reconcile_service = service.clone();
            let reconcile_shutdown = shutdown_token.clone();
            let blocking_writer = state_writer.clone();
            let discovered = tokio::task::spawn_blocking(move || {
                let _state_writer = blocking_writer;
                let batch = (!reconcile_shutdown.is_cancelled()).then(|| {
                    reconcile_service.discover_periodic_inventory(&mut inventory_scan, &reconcile_shutdown)
                });
                (inventory_scan, batch)
            })
            .await;
            let batch = match discovered {
                Ok((scan, batch)) => {
                    inventory_scan = scan;
                    match batch {
                        Some(Ok(batch)) => Some(batch),
                        Some(Err(error)) => {
                            tracing::warn!(%error, "periodic inventory discovery failed; will retry");
                            None
                        }
                        None => None,
                    }
                }
                Err(error) => {
                    inventory_scan = super::cache_cleanup::PeriodicInventoryScan::default();
                    tracing::warn!(%error, "sandbox inventory discovery task failed");
                    None
                }
            };
            let inventory_verdicts = match &batch {
                Some(batch) => {
                    service
                        .inspect_retained_sources_in(batch.orphan_candidates.clone(), &mut retained_schedule)
                        .await
                }
                None => Default::default(),
            };
            let reconcile_service = service.clone();
            let reconcile_shutdown = shutdown_token.clone();
            let blocking_writer = state_writer.clone();
            let inventory_result = tokio::task::spawn_blocking(move || {
                let _state_writer = blocking_writer;
                let mut paths = Vec::new();
                if !reconcile_shutdown.is_cancelled() {
                    if let Some(batch) = batch
                        && let Err(error) = reconcile_service.reconcile_periodic_inventory_batch(batch, &inventory_verdicts)
                    {
                        tracing::warn!(%error, "periodic inventory reconciliation failed; will retry");
                    }
                    if !reconcile_shutdown.is_cancelled() {
                        match reconcile_service.abandoned_suspend_staging_batch(Path::new(CACHE_DIR), &mut staging_scan) {
                            Ok(batch) => paths = batch,
                            Err(error) => tracing::warn!(%error, "suspend staging discovery failed; will retry"),
                        }
                    }
                }
                (staging_scan, paths, inventory_verdicts)
            })
            .await;
            match inventory_result {
                Ok((staging, paths, inventory_verdicts)) => {
                    retained_pending.observe(
                        &[&publication_verdicts, &inventory_verdicts],
                        std::time::Instant::now(),
                    );
                    staging_scan = staging;
                    staging_cleanup::delete_batch(&mut node_client, paths, &shutdown_token).await;
                }
                Err(error) => {
                    staging_scan = staging_cleanup::StagingCleanupScan::default();
                    tracing::warn!(%error, "sandbox inventory reconciler blocking task failed");
                }
            }
            if shutdown_token.is_cancelled() { break; }
            super::drain::with_state_writer(
                state_writer,
                service.adopt_quarantine_recovery_episodes(Path::new(CACHE_DIR), &shutdown_token, &mut quarantine_scan),
            ).await;
            service.cleanup_executor.notify();
        }
        });
    }
}

fn unowned_publication(row: &crate::db::SandboxRow) -> bool {
    row.state == SandboxState::Suspending && row.current_local_artifact_id.is_none()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CleanupRecoveryAuthority {
    Startup,
    RequestedOnly,
}

mod orphan_dispatch;
pub(in crate::service) mod staging_cleanup;

#[cfg(test)]
mod tests;
