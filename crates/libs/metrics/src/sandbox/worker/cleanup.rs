//! Cleanup: the cleanup queue, activation cleanup, runtime stops, cgroups and local artifact
//! metadata.

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

/// One cleanup in which node-manager observed the runtime supervisor fail to
/// stop a sandbox (the `runtime supervisor failed to stop sandbox during
/// cleanup` path). In practice this is dominated by cgroup-removal `EBUSY`
/// (`Device or resource busy`): the cgroup still held a live task — typically a
/// Firecracker vCPU thread finishing VM/memory teardown — or a child cgroup
/// when the removal budget elapsed. A transient failure self-heals via the
/// per-sandbox cleanup retry; cross-reference `cleanup.leaked_cgroups` and
/// `cleanup.cgroups_reclaimed` for the permanent-leak picture, and the
/// supervisor ERROR log for the precise cgroup path.
pub fn runtime_stop_failed() {
    counter("sandbox.worker.cleanup.runtime_stop_failed", 1, &[]);
}

/// Untracked, unpopulated sandbox cgroup directories observed by the node-local
/// reclaim sweep in its latest tick (the leak backlog it found and acted on).
/// Sustained nonzero means cgroups are leaking faster than a single sweep
/// clears, or reclaim is failing — cross-reference `cgroups_reclaimed`.
pub fn leaked_cgroups(value: usize) {
    gauge("sandbox.worker.cleanup.leaked_cgroups", value as f64, &[]);
}

/// Orphaned sandbox cgroups removed by the node-local reclaim sweep. These are
/// directories whose sandbox lease/record is gone (so no per-sandbox cleanup
/// would ever retry them) and which were unpopulated when reclaimed.
pub fn cgroups_reclaimed(count: u64) {
    if count == 0 {
        return;
    }
    counter("sandbox.worker.cleanup.cgroups_reclaimed", count, &[]);
}

pub fn local_artifact_metadata_pruned(count: u64) {
    if count == 0 {
        return;
    }
    counter("sandbox.worker.local_artifact_metadata.pruned", count, &[]);
}
