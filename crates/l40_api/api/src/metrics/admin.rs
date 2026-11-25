//! The admin metrics the API server records.

use pplx_space_metrics_emit::{counter, metric_vocabulary, tag};

pub fn audit_persist_failed(method: &str, mode: &str) {
    counter(
        "admin.audit.persist_failed",
        1,
        &[tag("method", method), tag("mode", mode)],
    );
}

metric_vocabulary! {
    /// What the `:terminate` occupancy guard decided under the v1 drain engine,
    /// on the counts `admin_occupancy::terminate_occupancy` returned.
    NodeTerminateGuardOutcome {
        /// Both counts were zero, so the call went on to the phase write.
        Allowed => "allowed",
        /// A resident sandbox was counted, with or without drain blockers.
        RefusedActive => "refused_active",
        /// The guard refused without counting a resident sandbox: only
        /// drain-gate blockers were counted.
        RefusedDrainBlockers => "refused_drain_blockers",
    }
}

/// One `:terminate` under the v1 drain engine that reached the occupancy guard.
/// `allowed` means the guard let the call through to the `terminating` phase
/// write, not that the write committed or the instance was terminated: the
/// terminating tick recounts the blocking residents first and returns the node
/// to `drained` if one attached after the write. The cluster controller's
/// `drained_node_terminator` also moves drained nodes to `terminating`, and
/// never counts here. "Admin node terminate guard" in `docs/observability.md`
/// describes each outcome; keep it current.
pub fn node_terminate_guard(node_id: &str, outcome: NodeTerminateGuardOutcome) {
    counter(
        "admin.node_terminate.guard",
        1,
        &[tag("node", node_id), tag("outcome", outcome)],
    );
}
