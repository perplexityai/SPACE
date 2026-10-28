//! Ingress gauges report worker-wide protected-stream count and oldest live age,
//! sampled by lifecycle scheduling. Admission wait latency and lifecycle deferrals
//! describe contention independently of request preparation latency.
//!
//! `sandbox.worker.activation_env.attempt` counts completed environment-setup
//! attempts (including local deadlines and fence failures) for nonempty maps.
//! Tags are phase, attempt=first|retry, and result=ok|error. First-attempt errors
//! divided by all first attempts measure initial reliability independently of
//! recovery. Dropped futures have no completed outcome; empty maps are skipped.
//!
//! `sandbox.worker.teardown_suspend.backup_refusal_replayed` counts teardown
//! drives that rejoined a refused backup upload of their own generation's
//! capture instead of starting a new attempt, tagged by `reason`. A teardown
//! that keeps replaying keeps its pod terminating, and wakes of its sandbox
//! wait on that pod; the drive's log line names the sandbox.

use std::{fmt, str::FromStr, time::Instant};

use pplx_space_common_core::types::{
    AutoLifecycleIdleAction, ExecutionReleaseDetachReason, ExecutionReleaseIntent,
    QuarantineRecoveryNoProgressReason, SandboxQuarantineReasonCode,
};

use crate::emit::{counter, distribution, elapsed_ms, gauge, tag, timing_ms};

mod auto_lifecycle;
mod cleanup;
mod guest_bridge;
mod health;
mod ingress;
mod inventory;
mod net_gateway;
mod quarantine_recovery;
mod route_authorization;
mod teardown_suspend;

pub use auto_lifecycle::*;
pub use cleanup::*;
pub use guest_bridge::*;
pub use health::*;
pub use ingress::*;
pub use inventory::*;
pub use net_gateway::*;
pub use quarantine_recovery::*;
pub use route_authorization::*;
pub use teardown_suspend::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionReleaseDecision {
    Accepted,
    Detached(ExecutionReleaseDetachReason),
}

impl ExecutionReleaseDecision {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Detached(_) => "detached",
        }
    }

    const fn reason_str(self) -> &'static str {
        match self {
            Self::Accepted => "not_applicable",
            Self::Detached(reason) => reason.as_str(),
        }
    }
}

impl fmt::Display for ExecutionReleaseDecision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A generation-fenced runtime execution release reached an ownership
/// decision. Errors before a decision are excluded from this counter.
pub fn execution_release_decision(
    intent: ExecutionReleaseIntent,
    decision: ExecutionReleaseDecision,
) {
    counter(
        "sandbox.worker.execution_release.decision",
        1,
        &[
            tag("intent", intent.as_str()),
            tag("disposition", decision.as_str()),
            tag("reason", decision.reason_str()),
        ],
    );
}

#[cfg(test)]
mod tests;
