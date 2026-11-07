//! The environment placement gate's shadow mode, in every service that places
//! sandboxes (`space-api` and `space-cluster-controller`).
//!
//! Emitted only while `SPACE_ENVIRONMENT_PLACEMENT_GATE` is `shadow`: each
//! increment is the candidate nodes one placement statement returned that the
//! gate, on, would have left out. Zero across a soak is the signal that turning it
//! on refuses nothing today's placement uses.

use pplx_space_metrics_emit::{counter, metric_vocabulary, tag};

metric_vocabulary!(
    /// The placement statement that returned the candidate.
    EnvironmentPlacementPath {
        /// A create from a template the node holds, through a fresh snapshot.
        CachedCreate => "cached_create",
        /// The create fallback that tolerates a missing or stale snapshot.
        LastKnownCreate => "last_known_create",
        /// A create that pulls the template onto the node.
        PullCreate => "pull_create",
        /// A warm-pool member to adopt.
        PoolAdoption => "pool_adoption",
        /// A restore on the node holding the sandbox's local state.
        RestoreSource => "restore_source",
        /// A restore from durable storage on another node.
        RestoreRemote => "restore_remote",
    }
);

/// `count` candidates one placement statement returned that the gate would
/// refuse for `reason`, the refusal's wire value
/// (`pplx_space_node_status_store::EnvironmentRefusal::as_str`).
pub fn would_refuse(path: EnvironmentPlacementPath, reason: &'static str, count: usize) {
    counter(
        "environment_placement.would_refuse",
        u64::try_from(count).unwrap_or(u64::MAX),
        &[tag("path", path.as_str()), tag("reason", reason)],
    );
}
