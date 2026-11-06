//! node-api's authentication policy metrics.

use pplx_space_metrics_emit::{counter, tag};

/// Classification applied before node-api forwards SpacedWaitReady to the
/// local sandbox-manager.
pub fn node_api_wait_ready_policy(policy: &str, route_token: &str) {
    counter(
        "auth.node_api.wait_ready_policy",
        1,
        &[tag("policy", policy), tag("route_token", route_token)],
    );
}
