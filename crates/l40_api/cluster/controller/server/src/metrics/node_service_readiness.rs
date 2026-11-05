//! Readiness of the node-local SPACE service pods (node-ctl, volume-manager,
//! …) as published on Kubernetes Nodes for sandbox placement.
//!
//! The cluster-singleton writer emits the fleet-level view; the control plane
//! emits only the fail-open counter, so a wedged writer is visible from the
//! reader side too.

use pplx_space_metrics_emit::{counter, gauge, tag, timing_ms};

/// Nodes in the sandbox placement set whose node-local service pods are not
/// all `Ready` — the population currently deprioritized for new placements.
/// Expected to spike to roughly the DaemonSet `maxUnavailable` share during a
/// worker rollout and return to zero after.
pub fn not_ready_nodes(count: usize) {
    gauge("node_service_readiness.not_ready_nodes", count as f64, &[]);
}

/// Nodes in the sandbox placement set the writer evaluated this tick. The
/// denominator for `not_ready_nodes`.
pub fn evaluated_nodes(count: usize) {
    gauge("node_service_readiness.evaluated_nodes", count as f64, &[]);
}

/// One node label reconcile outcome.
pub fn label_write(outcome: LabelWriteOutcome) {
    label_writes(outcome, 1);
}

/// `count` node label reconciles that shared an outcome. Emitted as one packet
/// rather than one per node: the steady state is "every node unchanged", and a
/// few hundred nodes on a seconds-scale tick would otherwise be a needless
/// per-node statsd stream for a single series.
pub fn label_writes(outcome: LabelWriteOutcome, count: u64) {
    if count == 0 {
        return;
    }
    counter(
        "node_service_readiness.label_write",
        count,
        &[tag("outcome", outcome.as_str())],
    );
}

/// Terminal result of reconciling one node's `node-services-ready` label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelWriteOutcome {
    /// The label was patched to a new value.
    Applied,
    /// The label already carried the desired value; no apiserver call.
    Unchanged,
    /// Live writes are disabled; the change was logged, not applied.
    DryRun,
    /// The apiserver rejected the patch.
    Error,
}

impl LabelWriteOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Unchanged => "unchanged",
            Self::DryRun => "dry_run",
            Self::Error => "error",
        }
    }
}

impl std::fmt::Display for LabelWriteOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Nodes whose published label disagreed with the verdict computed this tick —
/// the writer's outstanding work. In steady state this is the size of the
/// current DaemonSet wave and returns to zero after it; sustained elevation
/// means the writer is not keeping up with the transition rate, so the labels
/// placement reads are trailing the rollout they describe.
pub fn pending_label_changes(count: usize) {
    gauge(
        "node_service_readiness.pending_label_changes",
        count as f64,
        &[],
    );
}

/// Wall-clock of one reconcile tick's action phase. Approaching the configured
/// tick period means patches are being deferred and the signal is lagging.
pub fn tick_duration_ms(millis: u64) {
    timing_ms(
        "node_service_readiness.tick_duration_ms",
        millis as f64,
        &[],
    );
}

/// `1` while the *writer* is failing open: more of the fleet reads not-ready
/// than the watched DaemonSets' rollout budgets can explain, so `Ready` is
/// published everywhere and stale `false` labels are actively cleared. Alert on
/// this — it means the signal is not describing a rollout, and placement is
/// running unbiased until it clears.
pub fn fail_open(active: bool) {
    gauge(
        "node_service_readiness.fail_open",
        if active { 1.0 } else { 0.0 },
        &[],
    );
}

/// A planned patch was dropped because the node's verdict changed between the
/// plan and the write (the rollout moved under the tick). Healthy in small
/// numbers during a wave — it is the writer declining to publish a value it
/// already knows is stale.
pub fn stale_verdict_skipped() {
    counter("node_service_readiness.stale_verdict_skipped", 1, &[]);
}
