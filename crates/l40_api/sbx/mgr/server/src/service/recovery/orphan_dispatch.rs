//! Dispatch, supervision, and outcome persistence for one orphan quarantine
//! restore.
//!
//! Self-contained by design: the pass hands in a claimed candidate, a plan, and the
//! node-wide permit, and nothing here reads the pass's state. That is what lets it
//! sit apart from `recovery.rs`, which also holds the pass itself, the `Start` RPC
//! handler, the no-progress classifier, and the inventory reconciler.

use super::{
    RecoveryPassProbe, RecoveryPassWindow, SandboxManagerService,
    quarantine_recovery_no_progress_reason,
};
use pplx_space_common_core::types::SandboxId;
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_metrics::sandbox::worker::{
    QuarantineRecoveryAttemptOutcome, QuarantineRecoveryOutcome,
};

/// Which of the pass's two recoveries a claimed candidate was admitted for, and
/// the resource that admission reserved.
///
/// Deliberately not `QuarantineRecoveryBranch`, which it mirrors: that type says
/// what an episode's *evidence* admits, this one says what this pass *reserved*
/// for it. Collapsing them would make "an orphan restore dispatched without a
/// permit" representable, and the permit is the only thing bounding concurrent
/// transfer and filesystem work on the node.
///
/// The orphan variant owns that permit from before the plan lookup until the
/// detached task persists its outcome, so an early `continue` cannot strand node
/// capacity.
pub(super) enum AdmittedRecoveryKind {
    Artifact(crate::db::AdoptableArtifact),
    Orphan(tokio::sync::OwnedSemaphorePermit),
}

impl SandboxManagerService {
    /// Dispatch one claimed orphan restore without holding up the inventory pass.
    ///
    /// Two tracked tasks, not one. The restore owns the claim, the permit, and the
    /// outcome write; the supervisor only observes a panic or cancellation that
    /// prevented that write, which is why it can stay clear of a database mutex the
    /// panic may have poisoned.
    ///
    /// `TaskTracker` keeps shutdown accounting explicit. The permit is acquired by
    /// the caller before plan lookup and held through outcome persistence, so at
    /// most one legacy orphan restore performs transfer or filesystem work on a
    /// node and no pre-spawn exit can strand node-wide capacity. The single permit
    /// is also a safety fence after lease loss: a fresh claimant cannot begin
    /// concurrent transfer or filesystem work against the same staging tree while
    /// the superseded task is still unwinding.
    pub(super) fn dispatch_quarantined_orphan_restore<Probe: RecoveryPassProbe>(
        &self,
        claimed: crate::db::ClaimedQuarantineRecoveryCandidate,
        plan: crate::snapshot_restore::QuarantineRecoveryPlan,
        probe: Probe,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        let sandbox_id = claimed.candidate.sandbox_id;
        let local_quarantine_id = claimed.candidate.local_quarantine_id;
        let service = self.clone();
        let restore = self.task_tracker.spawn(async move {
            let _permit = permit;
            let result = service
                .run_quarantined_orphan_restore(&claimed, &plan)
                .await;
            probe(RecoveryPassWindow::RestoreCompleted);
            service
                .finish_quarantined_orphan_restore(&claimed, &plan, result)
                .await;
        });
        drop(self.supervise_quarantined_orphan_restore_task(
            sandbox_id,
            local_quarantine_id,
            restore,
        ));
    }

    /// Observe a detached restore's task-level failure without pretending that
    /// its durable retry write succeeded.
    ///
    /// The restore task owns the claim and the permit; both are released by
    /// unwinding, and the claim lease is what makes the episode eligible again.
    /// This supervisor performs no *database* work, because a poisoned database
    /// mutex may be the panic's cause — so no retry state is written and no
    /// `last_error` is recorded here.
    ///
    /// It does emit a sandbox event, which is not database work: without one, an
    /// episode whose restore panicked simply stops on the timeline for the length
    /// of its lease. `record_quarantine_recovery_no_progress` closes the same kind
    /// of silence for a lost CAS; the type here is the one the cross-node restore
    /// already uses for a failed restore of this sandbox.
    ///
    /// Writing nothing costs the episode its remaining lease, not its pacing.
    /// `claim_quarantine_recovery_candidate` already charged the attempt and set
    /// `next_action_after` to the lease expiry plus this attempt's backoff, so a task
    /// that panics deterministically still escalates toward the cap. What it gives up
    /// is the head start a recorded outcome buys: the episode returns one lease *plus*
    /// one backoff out rather than one backoff out, which is the same cost
    /// `stale_claim` already carries. `outcome:background_task_failed` is what makes
    /// the panic itself visible.
    pub(super) fn supervise_quarantined_orphan_restore_task(
        &self,
        sandbox_id: SandboxId,
        local_quarantine_id: uuid::Uuid,
        task: tokio::task::JoinHandle<()>,
    ) -> tokio::task::JoinHandle<()> {
        self.task_tracker.spawn(async move {
            let Err(error) = task.await else {
                return;
            };
            pplx_space_metrics::sandbox::worker::quarantine_recovery_no_progress(
                crate::db::QuarantineRecoveryNoProgressReason::OrphanRestoreTaskFailed,
                QuarantineRecoveryAttemptOutcome::BackgroundTaskFailed,
            );
            // `SnapshotRestoreFailed`, matching what the cross-node restore's own
            // failure path emits for this operation. `RestoreFailed` is the
            // user-visible lifecycle restore, and folding a background
            // quarantine-recovery task panic into it would inflate that count.
            // The typed id goes straight in: `emit_sandbox_event` takes a string
            // and parses it back, dropping the event if it fails to.
            SandboxEvent::new(
                sandbox_id,
                SandboxEventType::SnapshotRestoreFailed,
                SandboxEventSource::SbxManager,
            )
            .emit_with(
                SandboxEventDetails::default()
                    .trigger(SandboxEventTrigger::AutoLifecycle)
                    .operation("quarantine_recovery_orphan_restore")
                    .reason(
                        crate::db::QuarantineRecoveryNoProgressReason::OrphanRestoreTaskFailed
                            .as_str(),
                    )
                    .result(if error.is_panic() {
                        "task_panicked"
                    } else {
                        "task_cancelled"
                    })
                    .logged_by_caller(),
            );
            tracing::error!(
                %sandbox_id,
                %local_quarantine_id,
                task_panicked = error.is_panic(),
                task_cancelled = error.is_cancelled(),
                "detached orphan quarantine recovery task failed"
            );
        })
    }

    pub(super) async fn finish_quarantined_orphan_restore(
        &self,
        claimed: &crate::db::ClaimedQuarantineRecoveryCandidate,
        plan: &crate::snapshot_restore::QuarantineRecoveryPlan,
        result: Result<
            Option<super::recovery_cross_node::CrossNodeRestoreProvenance>,
            crate::snapshot_restore::RestoreError,
        >,
    ) {
        let sandbox_id = claimed.candidate.sandbox_id;
        match result {
            Ok(Some(_)) => {
                tracing::info!(
                    %sandbox_id,
                    local_quarantine_id = %claimed.candidate.local_quarantine_id,
                    recovery_id = %plan.recovery_id,
                    snapshot_id = %plan.snapshot_id,
                    "recovered orphaned restore staging to suspended"
                );
                pplx_space_metrics::sandbox::worker::quarantine_recovery_succeeded(
                    QuarantineRecoveryOutcome::OrphanRestored,
                );
            }
            Ok(None) => {
                self.record_quarantine_recovery_no_progress(
                    claimed,
                    crate::db::QuarantineRecoveryNoProgressReason::OrphanRestoreDeferred,
                )
                .await;
            }
            Err(error) => {
                let reason = quarantine_recovery_no_progress_reason(&error);
                // Not `%error`: several `RestoreError` variants render the raw
                // storage URI, which is a control-plane connection string and can
                // carry endpoint credentials. `reason` collapses most of them into
                // one bucket, so the bounded label carries the detail instead.
                tracing::warn!(
                    %sandbox_id,
                    local_quarantine_id = %claimed.candidate.local_quarantine_id,
                    error_kind = %error.kind_label(),
                    error_detail = ?error.redacted_detail(),
                    %reason,
                    "orphaned restore staging recovery deferred"
                );
                self.record_quarantine_recovery_no_progress(claimed, reason)
                    .await;
            }
        }
    }
}
