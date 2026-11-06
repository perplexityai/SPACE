//! Sandbox worker cleanup metrics node-manager records: runtime stops and
//! the cgroups a stopped sandbox leaves behind.

use pplx_space_metrics_emit::{counter, gauge};

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
