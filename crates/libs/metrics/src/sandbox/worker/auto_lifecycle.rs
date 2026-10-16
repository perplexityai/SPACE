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
pub enum AutoLifecycleDeleteAdmissionDisposition {
    ReceiptClaimed,
    Receiptless,
    Blocked,
    Failed,
}

impl AutoLifecycleDeleteAdmissionDisposition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReceiptClaimed => "receipt_claimed",
            Self::Receiptless => "receiptless",
            Self::Blocked => "blocked",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for AutoLifecycleDeleteAdmissionDisposition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown auto-lifecycle delete admission disposition {0:?}")]
pub struct AutoLifecycleDeleteAdmissionDispositionParseError(pub(super) String);

impl FromStr for AutoLifecycleDeleteAdmissionDisposition {
    type Err = AutoLifecycleDeleteAdmissionDispositionParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "receipt_claimed" => Ok(Self::ReceiptClaimed),
            "receiptless" => Ok(Self::Receiptless),
            "blocked" => Ok(Self::Blocked),
            "failed" => Ok(Self::Failed),
            other => Err(AutoLifecycleDeleteAdmissionDispositionParseError(
                other.to_string(),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AutoLifecycleDeleteAdmissionReason {
    ClusterFenced,
    LocalAuthority,
    DirectUnsupported,
    NodeRoleUnavailable,
    NodeRoleNotWorker,
    PartialCurrentRoute,
    ConflictingRouteIdentity,
    MissingRouteIdentity,
    MissingSubstrate,
    UnknownSubstrate,
    UnknownProvenance,
    UnfencedRoute,
    PolicyChanged,
    AuthorityChanged,
    ReceiptConflict,
}

impl AutoLifecycleDeleteAdmissionReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClusterFenced => "cluster_fenced",
            Self::LocalAuthority => "local_authority",
            Self::DirectUnsupported => "direct_unsupported",
            Self::NodeRoleUnavailable => "node_role_unavailable",
            Self::NodeRoleNotWorker => "node_role_not_worker",
            Self::PartialCurrentRoute => "partial_current_route",
            Self::ConflictingRouteIdentity => "conflicting_route_identity",
            Self::MissingRouteIdentity => "missing_route_identity",
            Self::MissingSubstrate => "missing_substrate",
            Self::UnknownSubstrate => "unknown_substrate",
            Self::UnknownProvenance => "unknown_provenance",
            Self::UnfencedRoute => "unfenced_route",
            Self::PolicyChanged => "policy_changed",
            Self::AuthorityChanged => "authority_changed",
            Self::ReceiptConflict => "receipt_conflict",
        }
    }
}

impl fmt::Display for AutoLifecycleDeleteAdmissionReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown auto-lifecycle delete admission reason {0:?}")]
pub struct AutoLifecycleDeleteAdmissionReasonParseError(pub(super) String);

impl FromStr for AutoLifecycleDeleteAdmissionReason {
    type Err = AutoLifecycleDeleteAdmissionReasonParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "cluster_fenced" => Ok(Self::ClusterFenced),
            "local_authority" => Ok(Self::LocalAuthority),
            "direct_unsupported" => Ok(Self::DirectUnsupported),
            "node_role_unavailable" => Ok(Self::NodeRoleUnavailable),
            "node_role_not_worker" => Ok(Self::NodeRoleNotWorker),
            "partial_current_route" => Ok(Self::PartialCurrentRoute),
            "conflicting_route_identity" => Ok(Self::ConflictingRouteIdentity),
            "missing_route_identity" => Ok(Self::MissingRouteIdentity),
            "missing_substrate" => Ok(Self::MissingSubstrate),
            "unknown_substrate" => Ok(Self::UnknownSubstrate),
            "unknown_provenance" => Ok(Self::UnknownProvenance),
            "unfenced_route" => Ok(Self::UnfencedRoute),
            "policy_changed" => Ok(Self::PolicyChanged),
            "authority_changed" => Ok(Self::AuthorityChanged),
            "receipt_conflict" => Ok(Self::ReceiptConflict),
            other => Err(AutoLifecycleDeleteAdmissionReasonParseError(
                other.to_string(),
            )),
        }
    }
}

pub fn auto_lifecycle_delete_admission(
    disposition: AutoLifecycleDeleteAdmissionDisposition,
    reason: AutoLifecycleDeleteAdmissionReason,
) {
    counter(
        "sandbox.worker.auto_lifecycle.delete_admission",
        1,
        &[tag("disposition", disposition), tag("reason", reason)],
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

pub fn auto_lifecycle_delete_blocked(
    count: u64,
    oldest_age_secs: u64,
    reason: AutoLifecycleDeleteAdmissionReason,
) {
    gauge(
        "sandbox.worker.auto_lifecycle.delete_blocked.count",
        count as f64,
        &[tag("reason", reason)],
    );
    gauge(
        "sandbox.worker.auto_lifecycle.delete_blocked.oldest_age_secs",
        oldest_age_secs as f64,
        &[tag("reason", reason)],
    );
}
