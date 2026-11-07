//! The node-service readiness metric the API server records.

use pplx_space_metrics_emit::counter;

/// A placement decision ignored the `node-services-ready` labels because too
/// much of the eligible fleet read as degraded to be a DaemonSet rollout.
/// Non-zero means the signal is not being applied — treat as a writer outage,
/// not as a fleet outage.
pub fn placement_signal_suppressed() {
    counter("node_service_readiness.placement_signal_suppressed", 1, &[]);
}
