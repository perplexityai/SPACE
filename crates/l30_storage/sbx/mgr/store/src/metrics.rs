//! Metrics this store records, quarantine entries and inventory list budgets,
//! and the tag vocabulary it computes: the reason of an auto-lifecycle delete
//! admission, which sbx-mgr counts from this store's verdicts.

use std::{fmt, str::FromStr};

use pplx_space_common_core::types::SandboxQuarantineReasonCode;
use pplx_space_metrics_emit::{counter, tag};

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

/// A sandbox entered `quarantined`, counted once per entry after the
/// transition commits, beside its `sandbox_quarantined` event. `reason` is
/// the quarantine's reason code; `operation` names the sandbox-manager path
/// that quarantined it, the event's own `operation` (`mark_quarantined`,
/// `insert_quarantined_sandbox`, `startup_resident_recovery`,
/// `complete_cleanup_episode`, `complete_row_intent_cleanup`,
/// `quarantine_cleanup_episode`).
pub fn quarantine_entered(reason: SandboxQuarantineReasonCode, operation: &'static str) {
    counter(
        "sandbox.worker.quarantine.entered",
        1,
        &[tag("reason", reason.as_str()), tag("operation", operation)],
    );
}

/// A complete unary sandbox List exceeded a strict inventory budget and was
/// served anyway. `rpc` and `limit` are bounded static labels.
pub fn unary_list_budget_exceeded(rpc: &'static str, limit: &'static str) {
    counter(
        "sandbox.worker.inventory.unary_list_budget_exceeded",
        1,
        &[tag("rpc", rpc), tag("limit", limit)],
    );
}
