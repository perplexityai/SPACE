//! Sandbox desired-state deletion reports, which internal-api records.

use pplx_space_metrics_emit::{counter, distribution, tag};

/// Per desired-state request: how many requested sandbox ids had no
/// cluster row. A distribution so both the fleet-wide backlog drain and
/// per-node outliers stay observable while cluster-deleted eviction runs.
pub fn sandbox_desired_state_deleted_reported(node: &str, count: u64) {
    distribution(
        "reconcile.sandbox_desired_state.deleted_reported",
        count as f64,
        &[tag("node", node)],
    );
}

/// The deleted partition exceeded the plausibility threshold and was
/// suppressed instead of returned. Any occurrence warrants immediate
/// investigation of cluster database staleness or misrouting.
pub fn sandbox_desired_state_deleted_suppressed() {
    counter("reconcile.sandbox_desired_state.deleted_suppressed", 1, &[]);
}
