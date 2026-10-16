//! The spaced guest bridge: RPC results, latency and timeouts, guest readiness, the activation
//! environment, guest flushes on stop, and observed guest specs.

use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpacedBridgeRpcResult {
    Ok,
    Timeout,
    Cancelled,
    Unknown,
    InvalidArgument,
    DeadlineExceeded,
    NotFound,
    AlreadyExists,
    PermissionDenied,
    ResourceExhausted,
    FailedPrecondition,
    Aborted,
    OutOfRange,
    Unimplemented,
    Internal,
    Unavailable,
    DataLoss,
    Unauthenticated,
}

impl SpacedBridgeRpcResult {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::Unknown => "unknown",
            Self::InvalidArgument => "invalid_argument",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::NotFound => "not_found",
            Self::AlreadyExists => "already_exists",
            Self::PermissionDenied => "permission_denied",
            Self::ResourceExhausted => "resource_exhausted",
            Self::FailedPrecondition => "failed_precondition",
            Self::Aborted => "aborted",
            Self::OutOfRange => "out_of_range",
            Self::Unimplemented => "unimplemented",
            Self::Internal => "internal",
            Self::Unavailable => "unavailable",
            Self::DataLoss => "data_loss",
            Self::Unauthenticated => "unauthenticated",
        }
    }
}

impl fmt::Display for SpacedBridgeRpcResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn spaced_guest_ready(guest_version: &str) {
    counter(
        "sandbox.worker.spaced_guest.ready",
        1,
        &[tag("guest_version", guest_version)],
    );
}

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

/// Manager-observed guest readiness, including connection setup and RPC retries.
/// Shares node/build attribution and cancellation handling with gateway waits.
pub fn spaced_guest_readiness_wait(reason: &str) -> crate::gateway::readiness::Wait {
    let reason = match reason {
        "start" => "start",
        "resume" => "resume",
        _ => "other",
    };
    crate::gateway::readiness::Wait::new(
        "sandbox.worker.spaced_guest.readiness_wait.duration_ms",
        [tag("reason", reason)],
    )
}

pub fn spaced_bridge_rpc_latency(
    bridge: &str,
    operation: &str,
    result: SpacedBridgeRpcResult,
    started_at: Instant,
) {
    timing_ms(
        "sandbox.worker.spaced_bridge.rpc_latency_ms",
        elapsed_ms(started_at),
        &[
            tag("bridge", bridge),
            tag("operation", operation),
            tag("result", result.as_str()),
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

pub fn spaced_bridge_rpc_timeout(bridge: &str, operation: &str) {
    counter(
        "sandbox.worker.spaced_bridge.rpc_timeout",
        1,
        &[tag("bridge", bridge), tag("operation", operation)],
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
