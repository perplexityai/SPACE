//! The spaced guest as sbx-mgr sees it: the activation environment, guest
//! flushes on stop, observed guest specs, and guest memory usage. The bridge's RPC results,
//! latency and guest readiness are spaced-host-server's (its `metrics`), and
//! the guest readiness wait is in `pplx-space-sandbox-metrics`'
//! `gateway_readiness`.

use super::*;

pub fn activation_env_attempt(phase: &str, first_attempt: bool, succeeded: bool) {
    let phase = match phase {
        "create" => "create",
        "start" => "start",
        "restore" => "restore",
        "resume" => "resume",
        _ => "other",
    };
    counter(
        "sandbox.worker.activation_env.attempt",
        1,
        &[
            tag("phase", phase),
            tag("attempt", if first_attempt { "first" } else { "retry" }),
            tag("result", if succeeded { "ok" } else { "error" }),
        ],
    );
}

/// Outcome of the guest root-filesystem flush an ordinary stop attempts before
/// terminating the VM: `flushed` or `failed`.
pub fn stop_guest_flush(outcome: &'static str) {
    counter(
        "sandbox.worker.stop.guest_flush",
        1,
        &[tag("outcome", outcome)],
    );
}

pub fn guest_observed_specs(template: &str, mem_total_bytes: Option<u64>, vcpu_count: Option<u32>) {
    let tags = &[tag("template", template)];
    if let Some(value) = mem_total_bytes {
        distribution("sandbox.guest.mem_total_bytes", value as f64, tags);
    }
    if let Some(value) = vcpu_count {
        distribution("sandbox.guest.vcpu_count", f64::from(value), tags);
    }
}

/// One guest memory reading of a running sandbox, in bytes.
///
/// Reports `MemTotal - MemAvailable` tagged by template; deployment
/// dimensions come from the sink. The watchdog emits one sample per accepted
/// probe of a started sandbox. Paused sandboxes, including ready warm-pool
/// slots, are not pinged and record nothing.
/// Do not attach sandbox IDs or other per-sandbox identifiers.
pub fn guest_memory_used(template: &str, used_bytes: u64) {
    distribution(
        "sandbox.guest.mem_used_bytes",
        used_bytes as f64,
        &[tag("template", template)],
    );
}
