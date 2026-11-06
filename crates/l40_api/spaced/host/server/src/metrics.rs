//! Metrics of the spaced host bridge: its guest's readiness and its RPCs.

use std::fmt;
use std::time::Instant;

use pplx_space_metrics_emit::{counter, elapsed_ms, tag, timing_ms};

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

pub fn spaced_bridge_rpc_timeout(bridge: &str, operation: &str) {
    counter(
        "sandbox.worker.spaced_bridge.rpc_timeout",
        1,
        &[tag("bridge", bridge), tag("operation", operation)],
    );
}
