//! Quarantine entry and recovery.

use super::*;

// The no-progress reason vocabulary is a durable noun, not a tag vocabulary:
// its rendered form is persisted into `sandbox_quarantine_state.last_error`. It
// therefore lives in common-core next to the other quarantine vocabularies, so
// renaming a metric tag cannot change what rows persist.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuarantineRecoveryAttemptOutcome {
    RetryScheduled,
    StaleClaim,
    /// The pass never held the claim, so no durable retry state was written.
    ClaimNotAcquired,
    /// The claim was held, but the node-local database refused the write, so the
    /// retry state is whatever the claim left behind. Distinct from
    /// `ClaimNotAcquired`, which reports never having held the claim at all.
    RetryWriteFailed,
    /// A detached task panicked or was cancelled before it could classify and
    /// persist its result. The claim lease remains the liveness backstop.
    BackgroundTaskFailed,
    /// The result completed the run of consecutive plan rejections that stops
    /// automatic recovery, so no retry was scheduled: the episode is left to an
    /// operator until it is re-armed.
    AutomaticRecoveryStopped,
}

impl QuarantineRecoveryAttemptOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RetryScheduled => "retry_scheduled",
            Self::StaleClaim => "stale_claim",
            Self::ClaimNotAcquired => "claim_not_acquired",
            Self::RetryWriteFailed => "retry_write_failed",
            Self::BackgroundTaskFailed => "background_task_failed",
            Self::AutomaticRecoveryStopped => "automatic_recovery_stopped",
        }
    }
}

impl fmt::Display for QuarantineRecoveryAttemptOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// What a completed legacy quarantine recovery actually delivered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuarantineRecoveryOutcome {
    CleanupAdopted,
    OrphanRestored,
}

impl QuarantineRecoveryOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CleanupAdopted => "cleanup_adopted",
            Self::OrphanRestored => "orphan_restored",
        }
    }
}

impl fmt::Display for QuarantineRecoveryOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A sandbox entered `quarantined`, counted once per entry after the
/// transition commits, beside its `sandbox_quarantined` event. `reason` is
/// the quarantine's reason code; `operation` names the sandbox-manager path
/// that quarantined it, the event's own `operation` (`mark_quarantined`,
/// `insert_quarantined_sandbox`, `startup_resident_recovery`,
/// `complete_cleanup_episode`, `complete_row_intent_cleanup`,
/// `quarantine_cleanup_episode`). A re-confirmed quarantine is not an entry.
pub fn quarantine_entered(reason: SandboxQuarantineReasonCode, operation: &'static str) {
    counter(
        "sandbox.worker.quarantine.entered",
        1,
        &[tag("reason", reason.as_str()), tag("operation", operation)],
    );
}

pub fn quarantine_recovery_no_progress(
    reason: QuarantineRecoveryNoProgressReason,
    outcome: QuarantineRecoveryAttemptOutcome,
) {
    counter(
        "sandbox.worker.quarantine_recovery.no_progress",
        1,
        &[tag("reason", reason), tag("outcome", outcome)],
    );
}

/// Node-wide orphan-restore capacity as one recovery pass found it.
///
/// Gauges, not `no_progress` counters, and on their own names. A capacity skip is
/// a pre-claim observation about the node, not an attempt that made no progress:
/// folding it into the attempt counter meant a node with a large due cohort behind
/// one multi-hour restore could add its whole cohort size to that counter every
/// tick, against a prior per-pass ceiling of the admission cap. A gauge does not
/// accumulate, so the deferred depth stays readable without swamping the metric
/// every attempt-rate query is built on.
///
/// `deferred` is the number of orphans this bounded batch reached and skipped for
/// lack of capacity, not the depth of the queue. A batch filled with artifact
/// adoptions reports zero even when later pages hold orphans. Neither this value
/// nor the batch summary's `remaining` describes the entire queue.
/// `closed` is whether the semaphore is closed, which is permanent and needs an
/// operator; it is read off the semaphore rather than counted from the walk, so a
/// pass that never reaches an orphan still reports it.
pub fn quarantine_recovery_orphan_capacity(deferred: usize, closed: bool) {
    gauge(
        "sandbox.worker.quarantine_recovery.orphan_capacity_deferred",
        deferred as f64,
        &[],
    );
    gauge(
        "sandbox.worker.quarantine_recovery.orphan_capacity_closed",
        f64::from(u8::from(closed)),
        &[],
    );
}

/// One legacy quarantine recovery that advanced the sandbox. Pairs with
/// `quarantine_recovery.no_progress` as the denominator a no-progress rate
/// needs: without it, a node whose every claim loses the CAS is
/// indistinguishable from a node with nothing quarantined.
pub fn quarantine_recovery_succeeded(outcome: QuarantineRecoveryOutcome) {
    counter(
        "sandbox.worker.quarantine_recovery.succeeded",
        1,
        &[tag("outcome", outcome)],
    );
}

pub fn quarantine_recovery_candidates_malformed(count: u64) {
    gauge(
        "sandbox.worker.quarantine_recovery.candidate_malformed",
        count as f64,
        &[],
    );
}

pub fn quarantine_recovery_backlog_sample_valid(valid: bool) {
    gauge(
        "sandbox.worker.quarantine_recovery.sample_valid",
        if valid { 1.0 } else { 0.0 },
        &[],
    );
}

pub fn quarantine_recovery_backlog(pending_count: u64, pending_oldest_age: std::time::Duration) {
    gauge(
        "sandbox.worker.quarantine_recovery.pending.count",
        pending_count as f64,
        &[],
    );
    gauge(
        "sandbox.worker.quarantine_recovery.pending.oldest_age_secs",
        pending_oldest_age.as_secs() as f64,
        &[],
    );
}
