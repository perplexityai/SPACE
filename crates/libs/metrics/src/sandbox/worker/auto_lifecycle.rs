//! The auto-lifecycle scheduler and its delete admission and outbox.

use super::*;

/// Whether all auto-lifecycle transition slots are occupied at a scheduler tick.
pub fn auto_lifecycle_scheduler_saturated(saturated: bool) {
    gauge(
        "sandbox.worker.auto_lifecycle.scheduler.saturated",
        u8::from(saturated) as f64,
        &[],
    );
}

/// Seconds between a lifecycle stage becoming due and its authoritative
/// post-lock execution beginning.
pub fn auto_lifecycle_transition_lateness(value_secs: u64, action: AutoLifecycleIdleAction) {
    distribution(
        "sandbox.worker.auto_lifecycle.scheduler.lateness_secs",
        value_secs as f64,
        &[tag("action", action.as_str())],
    );
}

/// An auto-lifecycle transition exhausted its retry budget and parked; the
/// sandbox stays running until fresh activity re-arms its conditions.
pub fn auto_lifecycle_retries_exhausted(action: AutoLifecycleIdleAction) {
    counter(
        "sandbox.worker.auto_lifecycle.retries_exhausted",
        1,
        &[tag("action", action.as_str())],
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AutoLifecycleDeleteOutboxState {
    Pending,
    Completed,
}

impl AutoLifecycleDeleteOutboxState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Completed => "completed",
        }
    }
}

impl fmt::Display for AutoLifecycleDeleteOutboxState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown auto-lifecycle delete outbox state {0:?}")]
pub struct AutoLifecycleDeleteOutboxStateParseError(pub(super) String);

impl FromStr for AutoLifecycleDeleteOutboxState {
    type Err = AutoLifecycleDeleteOutboxStateParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "completed" => Ok(Self::Completed),
            other => Err(AutoLifecycleDeleteOutboxStateParseError(other.to_string())),
        }
    }
}

pub fn auto_lifecycle_delete_outbox_depth(value: u64, state: AutoLifecycleDeleteOutboxState) {
    gauge(
        "sandbox.worker.auto_lifecycle.delete_outbox.depth",
        value as f64,
        &[tag("state", state)],
    );
}

pub fn auto_lifecycle_delete_outbox_oldest_age_secs(
    value: u64,
    state: AutoLifecycleDeleteOutboxState,
) {
    gauge(
        "sandbox.worker.auto_lifecycle.delete_outbox.oldest_age_secs",
        value as f64,
        &[tag("state", state)],
    );
}

pub fn auto_lifecycle_delete_outbox_max_attempt_count(value: u64) {
    gauge(
        "sandbox.worker.auto_lifecycle.delete_outbox.max_attempt_count",
        value as f64,
        &[],
    );
}
