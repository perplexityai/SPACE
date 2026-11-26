//! Cluster-wide unscheduled sandbox pod backlog, as cold-create admission reads it.

use std::time::Duration;

/// Unscheduled sandbox pods (`spec.nodeName` unset, neither deleting nor
/// terminal) waiting on `kube-scheduler` in the sandbox namespace, read from
/// the substrate's pod reflector with no apiserver or database call.
///
/// Shared token buckets are open-loop with respect to scheduler throughput:
/// when `kube-scheduler` bind rate degrades under control-plane load, cold
/// creates keep materializing `Pending` pods until queue wait crosses the
/// create readiness budget and synchronous timeout deletes amplify apiserver
/// and binding churn. Reading the live backlog from the reflector lets cold
/// creates shed before `POST`ing another pod.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnscheduledPodBacklog {
    /// The sandbox-pod watch is healthy and delivering events; carries the
    /// exact count of unscheduled sandbox pods in the reflector index.
    Fresh(u64),
    /// The sandbox-pod watch returned an error, ended, or is mid-re-list.
    /// `last_count` is the last complete snapshot before the watch stalled, and
    /// `stale_for` is how long the watch has gone without a live event or
    /// completed re-list.
    Stale {
        last_count: u64,
        stale_for: Duration,
    },
    /// The backlog can't be read at all, so admission must fail open.
    Unknown(UnscheduledPodBacklogUnknown),
}

/// Why [`UnscheduledPodBacklog::Unknown`] was returned. Each reason has its own
/// metric value so an unconfigured substrate is distinguishable from a cold
/// reflector cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnscheduledPodBacklogUnknown {
    /// The substrate runs no Kubernetes pods (Direct).
    Unsupported,
    /// The sandbox-pod reflector hasn't finished its initial `LIST`.
    PodCacheCold,
}
