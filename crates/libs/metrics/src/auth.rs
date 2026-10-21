//! Service-to-service authentication metrics: per-decision counters and
//! TokenReview cache/limiter events. Emitted by the credmgr JWT identity
//! path (and any other service that adopts projected-token TokenReview auth).
//!
//! UDS peer-process metrics tag the checked socket's service as `service` on
//! native DogStatsD and `service.role` on OTLP, separately from process identity.

use crate::emit::{counter, tag};

/// One authentication decision on a service identity surface.
///
/// - `mode`: `"k8s"` (projected-token TokenReview) or `"static"` (the
///   shared-secret fallback during rollout).
/// - `surface`: the listener/identity surface (e.g. `"credmgr-tcp"`).
/// - `result`: `"allow"` or `"deny"`.
/// - `reason`: machine-readable cause (`"ok"`, `"service_account_not_allowed"`,
///   `"token_review_rejected"`, …).
pub fn decision(mode: &str, surface: &str, result: &str, reason: &str) {
    counter(
        "auth.decision",
        1,
        &[
            tag("mode", mode),
            tag("surface", surface),
            tag("result", result),
            tag("reason", reason),
        ],
    );
}

/// One authentication decision on the node-api identity surface. Same shape as
/// [`decision`] but emitted under a dedicated metric for node-api callers.
pub fn node_api_decision(mode: &str, surface: &str, result: &str, reason: &str) {
    counter(
        "auth.node_api.decision",
        1,
        &[
            tag("mode", mode),
            tag("surface", surface),
            tag("result", result),
            tag("reason", reason),
        ],
    );
}

/// Classification applied before node-api forwards SpacedWaitReady to the
/// local sandbox-manager.
pub fn node_api_wait_ready_policy(policy: &str, route_token: &str) {
    counter(
        "auth.node_api.wait_ready_policy",
        1,
        &[tag("policy", policy), tag("route_token", route_token)],
    );
}

/// One TokenReview cache or limiter event (`"hit_authenticated"`, `"miss"`,
/// `"rate_limited"`, `"limiter_wait"`, `"evict_capacity"`, …).
pub fn token_review_cache(result: &str) {
    counter("auth.token_review.cache", 1, &[tag("result", result)]);
}

/// One evaluation of the opt-in UDS peer-process checks (binary identity +
/// capabilities), emitted per RPC that reaches the armed layer.
///
/// - `service`: the emitting service (e.g. `"space-credential-manager"`).
/// - `result`: `"allow"` or `"deny"` — the RPC verdict. In log-only mode a
///   failing check still allows, so `result:allow, reason!=ok` is exactly
///   the would-deny population a log-only bake reads.
/// - `reason`: `"ok"`, `"uid_drift"`, `"binary_not_allowed"`,
///   `"forbidden_capabilities"`, `"pid_unavailable"`, `"proc_unreadable"`.
/// - `principal`: the matched peer account's principal.
/// - `mode`: `"log-only"` or `"enforce"` (`off` never evaluates).
pub fn uds_peer_process_check(
    service: &str,
    result: &str,
    reason: &str,
    principal: &str,
    mode: &str,
) {
    counter(
        "auth.uds_peer_process_check",
        1,
        &[
            tag("service", service),
            tag("result", result),
            tag("reason", reason),
            tag("principal", principal),
            tag("mode", mode),
        ],
    );
}

/// A UDS peer's backing executable was unlinked while it kept running
/// (in-place upgrade, or tampering). Observed during the peer-process
/// checks, independent of the verdict.
pub fn uds_peer_exe_deleted(service: &str, principal: &str) {
    counter(
        "auth.uds_peer_exe_deleted",
        1,
        &[tag("service", service), tag("principal", principal)],
    );
}

/// Per-hop scope protocol support; a receipt does not certify authorization.
pub fn template_lookup_compatibility(result: &str) {
    counter(
        "auth.template_lookup.compatibility",
        1,
        &[tag("result", result)],
    );
}
