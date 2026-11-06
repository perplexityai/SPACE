//! Cleanup: the cleanup queue, activation cleanup and local artifact metadata.
//! Runtime stops and leaked cgroups are node-manager's (its
//! `metrics::sandbox_worker`).

use super::*;

/// Number of `schedule_async_cleanup` spawns currently holding a permit
/// from the node-wide cleanup semaphore. Bounded by the sandbox manager's
/// cleanup concurrency cap (16). A sustained reading
/// at the cap means the cleanup queue is saturating — see
/// `sandbox.worker.cleanup.queue_depth` for the backlog.
pub fn cleanup_active(value: usize) {
    gauge("sandbox.worker.cleanup.active", value as f64, &[]);
}

/// Requests waiting for cleanup admission before target allocation.
pub fn cleanup_queue_depth(value: usize) {
    gauge("sandbox.worker.cleanup.queue_depth", value as f64, &[]);
}

/// A cleanup attempt persisted for retry after a dependency or completion failure.
pub fn cleanup_retry() {
    counter("sandbox.worker.cleanup.retries", 1, &[]);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivationCleanupOutcome {
    Requested,
    Completed,
    AttemptFailed,
    UnsafeEvidenceQuarantined,
    Superseded,
}

impl ActivationCleanupOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Completed => "completed",
            Self::AttemptFailed => "attempt_failed",
            Self::UnsafeEvidenceQuarantined => "unsafe_evidence_quarantined",
            Self::Superseded => "superseded",
        }
    }
}

impl fmt::Display for ActivationCleanupOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn activation_cleanup(outcome: ActivationCleanupOutcome) {
    counter(
        "sandbox.worker.activation_cleanup",
        1,
        &[tag("outcome", outcome.as_str())],
    );
}

pub fn local_artifact_metadata_pruned(count: u64) {
    if count == 0 {
        return;
    }
    counter("sandbox.worker.local_artifact_metadata.pruned", count, &[]);
}
