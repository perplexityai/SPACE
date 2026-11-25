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
    /// What the v1 `:terminate` occupancy guard decided, on the counts
    /// `admin_occupancy::terminate_occupancy` returned.
    NodeTerminateGuardOutcome {
        /// Both counts were zero, so the call went on to the phase write.
        Allowed => "allowed",
        /// A resident sandbox was counted, with or without drain blockers.
        RefusedActive => "refused_active",
        /// Only drain-gate blockers were counted.
        RefusedDrainBlockers => "refused_drain_blockers",
    }
}

/// One v1 `:terminate` that reached the occupancy guard. `allowed` means the
/// guard let the call through, not that the phase write then committed. A
/// node refused on every call stays `drained`, and an allowed one is
/// terminated with whatever the count missed. "Admin node terminate guard" in
/// `docs/observability.md` describes each outcome; keep it current.
pub fn node_terminate_guard(node_id: &str, outcome: NodeTerminateGuardOutcome) {
    counter(
        "admin.node_terminate.guard",
        1,
        &[tag("node_id", node_id), tag("outcome", outcome)],
    );
}
