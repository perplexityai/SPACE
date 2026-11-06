//! node-ctl's reconcile metrics: queue claims, desired-state application and
//! auto-lifecycle delete delivery. The facet-keyed queue and run metrics every
//! reconciling service shares are in `pplx-space-reconcile-metrics`.

use std::fmt;
use std::str::FromStr;

use pplx_space_metrics_emit::{counter, tag};

pub fn claimed(count: u64) {
    counter("reconcile.queue.claimed", count, &[]);
}

/// Outcome vocabulary for [`cluster_deleted_evict`]. Closed so a typo'd
/// literal cannot mint a new metric series silently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterDeletedEvictResult {
    Evicted,
    Deferred,
    Failed,
}

impl ClusterDeletedEvictResult {
    pub fn as_str(self) -> &'static str {
        match self {
            ClusterDeletedEvictResult::Evicted => "evicted",
            ClusterDeletedEvictResult::Deferred => "deferred",
            ClusterDeletedEvictResult::Failed => "failed",
        }
    }
}

impl std::fmt::Display for ClusterDeletedEvictResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One per cluster-deleted eviction attempt from node-ctl's sandbox
/// desired-state facet sync. `result:evicted|deferred|failed`.
pub fn cluster_deleted_evict(result: ClusterDeletedEvictResult) {
    counter(
        "reconcile.cluster_deleted_evict",
        1,
        &[tag("result", result.as_str())],
    );
}

/// Disposition of a failed sandbox desired-state apply sub-op (port exposure,
/// loopback-bridge open, snapshot, auto-lifecycle) from node-ctl. Closed so a
/// typo'd literal cannot mint a new metric series silently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DesiredStateApplyResult {
    /// Transient failure (transport, `Unavailable`, `Internal`, timeouts,
    /// rollout-skew `Unimplemented`): an element error was recorded so the
    /// queue re-drives the ref.
    Retryable,
    /// Terminal failure (the guest is unreachable because the sandbox is
    /// failed/suspending/suspended/stopped, or the local allocation/row is
    /// gone): the ref completes without an element error. The owning lifecycle
    /// or cluster reconciler repairs state; periodic desired-state sync
    /// re-applies after the failure becomes actionable.
    Terminal,
}

impl DesiredStateApplyResult {
    pub fn as_str(self) -> &'static str {
        match self {
            DesiredStateApplyResult::Retryable => "retryable",
            DesiredStateApplyResult::Terminal => "terminal",
        }
    }
}

impl std::fmt::Display for DesiredStateApplyResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One per failed sandbox desired-state apply sub-op from node-ctl's sandbox
/// desired-state facet sync. `result:retryable|terminal`. Terminal completions
/// are the signal that a poison ref was retired instead of retried to the
/// backoff cap.
pub fn desired_state_apply(result: DesiredStateApplyResult) {
    counter(
        "reconcile.desired_state_apply",
        1,
        &[tag("result", result.as_str())],
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoLifecycleDeleteDeliveryOperation {
    List,
    Acknowledge,
}

impl AutoLifecycleDeleteDeliveryOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::List => "list",
            Self::Acknowledge => "acknowledge",
        }
    }
}

impl fmt::Display for AutoLifecycleDeleteDeliveryOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown auto-lifecycle delete delivery operation {0:?}")]
pub struct AutoLifecycleDeleteDeliveryOperationParseError(String);

impl FromStr for AutoLifecycleDeleteDeliveryOperation {
    type Err = AutoLifecycleDeleteDeliveryOperationParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "list" => Ok(Self::List),
            "acknowledge" => Ok(Self::Acknowledge),
            other => Err(AutoLifecycleDeleteDeliveryOperationParseError(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoLifecycleDeleteDeliveryOutcome {
    Succeeded,
    Failed,
    Unimplemented,
}

impl AutoLifecycleDeleteDeliveryOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Unimplemented => "unimplemented",
        }
    }
}

impl fmt::Display for AutoLifecycleDeleteDeliveryOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown auto-lifecycle delete delivery outcome {0:?}")]
pub struct AutoLifecycleDeleteDeliveryOutcomeParseError(String);

impl FromStr for AutoLifecycleDeleteDeliveryOutcome {
    type Err = AutoLifecycleDeleteDeliveryOutcomeParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "unimplemented" => Ok(Self::Unimplemented),
            other => Err(AutoLifecycleDeleteDeliveryOutcomeParseError(
                other.to_string(),
            )),
        }
    }
}

pub fn auto_lifecycle_delete_delivery(
    operation: AutoLifecycleDeleteDeliveryOperation,
    outcome: AutoLifecycleDeleteDeliveryOutcome,
) {
    counter(
        "reconcile.auto_lifecycle_delete_delivery",
        1,
        &[tag("operation", operation), tag("outcome", outcome)],
    );
}
