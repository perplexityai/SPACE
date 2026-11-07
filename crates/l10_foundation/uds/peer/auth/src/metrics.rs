//! UDS peer-process check metrics.
//!
//! They tag the checked socket's service as `service` on native DogStatsD and
//! `service.role` on OTLP, separately from process identity.

use pplx_space_metrics_emit::{counter, tag};

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
