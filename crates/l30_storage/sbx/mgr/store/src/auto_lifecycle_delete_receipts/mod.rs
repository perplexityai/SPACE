use std::fmt;
use std::str::FromStr;

use pplx_space_common_core::types::{
    AutoLifecycleDeleteReceiptId, ExecutionGeneration, NodeManagedBy, SandboxActivationAttemptId,
    SandboxId, SpaceApiRouteIdentity,
};
use pplx_space_common_svc::types::AutoLifecyclePolicyFingerprint;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteIdentityProvenance {
    Unfenced,
    ClusterFenced,
}

impl RouteIdentityProvenance {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Unfenced => "unfenced",
            Self::ClusterFenced => "cluster_fenced",
        }
    }

    pub fn for_activation_identity(identity: &SpaceApiRouteIdentity) -> Self {
        if identity.effective_substrate_kind() == Some(NodeManagedBy::Kubernetes)
            && identity.pod_uid.is_some()
            && identity.activation_attempt_id.is_some()
        {
            Self::ClusterFenced
        } else {
            Self::Unfenced
        }
    }

    /// Classify a route assembled from runtime-owned pod identity and
    /// control-plane-owned activation identity.
    ///
    /// The incoming wake is authoritative for generation and activation
    /// attempt, but not for substrate or pod UID. Requiring the former from
    /// `incoming` and the latter from `current` prevents both promotion of a
    /// locally synthesized attempt and provenance that describes a different
    /// tuple from the one persisted by the merge.
    pub fn for_authoritative_adoption(
        current: &SpaceApiRouteIdentity,
        incoming: &SpaceApiRouteIdentity,
    ) -> Self {
        if current.effective_substrate_kind() == Some(NodeManagedBy::Kubernetes)
            && current.pod_uid.is_some()
            && incoming.execution_generation.is_some()
            && incoming.activation_attempt_id.is_some()
        {
            Self::ClusterFenced
        } else {
            Self::Unfenced
        }
    }
}

impl fmt::Display for RouteIdentityProvenance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unknown route identity provenance {0:?}")]
pub struct RouteIdentityProvenanceParseError(String);

impl FromStr for RouteIdentityProvenance {
    type Err = RouteIdentityProvenanceParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "unfenced" => Ok(Self::Unfenced),
            "cluster_fenced" => Ok(Self::ClusterFenced),
            other => Err(RouteIdentityProvenanceParseError(other.to_string())),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AutoLifecycleDeleteBlockReason {
    NodeRoleUnavailable,
    NodeRoleNotWorker,
    PartialCurrentRoute,
    ConflictingRouteIdentity,
    MissingRouteIdentity,
    MissingSubstrate,
    UnknownSubstrate,
    UnknownProvenance,
    UnfencedRoute,
}

impl AutoLifecycleDeleteBlockReason {
    pub const ALL: [Self; 9] = [
        Self::NodeRoleUnavailable,
        Self::NodeRoleNotWorker,
        Self::PartialCurrentRoute,
        Self::ConflictingRouteIdentity,
        Self::MissingRouteIdentity,
        Self::MissingSubstrate,
        Self::UnknownSubstrate,
        Self::UnknownProvenance,
        Self::UnfencedRoute,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NodeRoleUnavailable => "node_role_unavailable",
            Self::NodeRoleNotWorker => "node_role_not_worker",
            Self::PartialCurrentRoute => "partial_current_route",
            Self::ConflictingRouteIdentity => "conflicting_route_identity",
            Self::MissingRouteIdentity => "missing_route_identity",
            Self::MissingSubstrate => "missing_substrate",
            Self::UnknownSubstrate => "unknown_substrate",
            Self::UnknownProvenance => "unknown_provenance",
            Self::UnfencedRoute => "unfenced_route",
        }
    }

    pub const fn metric_reason(
        self,
    ) -> pplx_space_metrics::sandbox::worker::AutoLifecycleDeleteAdmissionReason {
        use pplx_space_metrics::sandbox::worker::AutoLifecycleDeleteAdmissionReason as Reason;
        match self {
            Self::NodeRoleUnavailable => Reason::NodeRoleUnavailable,
            Self::NodeRoleNotWorker => Reason::NodeRoleNotWorker,
            Self::PartialCurrentRoute => Reason::PartialCurrentRoute,
            Self::ConflictingRouteIdentity => Reason::ConflictingRouteIdentity,
            Self::MissingRouteIdentity => Reason::MissingRouteIdentity,
            Self::MissingSubstrate => Reason::MissingSubstrate,
            Self::UnknownSubstrate => Reason::UnknownSubstrate,
            Self::UnknownProvenance => Reason::UnknownProvenance,
            Self::UnfencedRoute => Reason::UnfencedRoute,
        }
    }

    pub(crate) const fn is_route_evidence(self) -> bool {
        matches!(
            self,
            Self::PartialCurrentRoute
                | Self::ConflictingRouteIdentity
                | Self::MissingRouteIdentity
                | Self::MissingSubstrate
                | Self::UnknownSubstrate
                | Self::UnknownProvenance
                | Self::UnfencedRoute
        )
    }
}

impl fmt::Display for AutoLifecycleDeleteBlockReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for AutoLifecycleDeleteBlockReason {
    type Err = AutoLifecycleDeleteBlockReasonParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "node_role_unavailable" => Ok(Self::NodeRoleUnavailable),
            "node_role_not_worker" => Ok(Self::NodeRoleNotWorker),
            "partial_current_route" => Ok(Self::PartialCurrentRoute),
            "conflicting_route_identity" => Ok(Self::ConflictingRouteIdentity),
            "missing_route_identity" => Ok(Self::MissingRouteIdentity),
            "missing_substrate" => Ok(Self::MissingSubstrate),
            "unknown_substrate" => Ok(Self::UnknownSubstrate),
            "unknown_provenance" => Ok(Self::UnknownProvenance),
            "unfenced_route" => Ok(Self::UnfencedRoute),
            other => Err(AutoLifecycleDeleteBlockReasonParseError(other.to_string())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unknown auto-lifecycle delete block reason {0:?}")]
pub struct AutoLifecycleDeleteBlockReasonParseError(String);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AutoLifecycleDeleteOutboxState {
    Pending,
    Completed,
}

impl AutoLifecycleDeleteOutboxState {
    pub(crate) const fn as_str(self) -> &'static str {
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

#[derive(Debug, thiserror::Error)]
#[error("unknown auto-lifecycle delete outbox state {0:?}")]
pub(crate) struct AutoLifecycleDeleteOutboxStateParseError(String);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteEvidence {
    pub execution_generation: ExecutionGeneration,
    pub activation_attempt_id: SandboxActivationAttemptId,
    pub substrate: NodeManagedBy,
    pub provenance: RouteIdentityProvenance,
}

impl RouteEvidence {
    pub fn from_identity(
        identity: &SpaceApiRouteIdentity,
        provenance: RouteIdentityProvenance,
    ) -> Option<Self> {
        Some(Self {
            execution_generation: identity.execution_generation?,
            activation_attempt_id: identity.activation_attempt_id?,
            substrate: identity.effective_substrate_kind()?,
            provenance,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AutoLifecycleDeleteExpectation {
    pub policy_fingerprint: AutoLifecyclePolicyFingerprint,
    pub authority: pplx_space_common_core::types::PolicyAuthority,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AutoLifecycleDeleteReceipt {
    pub receipt_id: AutoLifecycleDeleteReceiptId,
    pub sandbox_id: SandboxId,
    pub execution_generation: ExecutionGeneration,
    pub route_activation_id: SandboxActivationAttemptId,
    pub policy_fingerprint: AutoLifecyclePolicyFingerprint,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AutoLifecycleDeleteOutboxStats {
    pub pending_count: u64,
    pub completed_count: u64,
    pub max_attempt_count: u64,
    pub oldest_pending_age_secs: u64,
    pub oldest_completed_age_secs: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AutoLifecycleDeleteBlockedStats {
    pub reason: AutoLifecycleDeleteBlockReason,
    pub count: u64,
    pub oldest_age_secs: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AutoLifecycleDeleteAdmission {
    Ordinary(AutoLifecycleDeleteOrdinaryReason),
    Receipt(RouteEvidence),
    Blocked {
        reason: AutoLifecycleDeleteBlockReason,
        changed: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AutoLifecycleDeleteOrdinaryReason {
    LocalAuthority,
    DirectUnsupported,
}

impl AutoLifecycleDeleteOrdinaryReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalAuthority => "local_authority",
            Self::DirectUnsupported => "direct_unsupported",
        }
    }

    pub const fn metric_reason(
        self,
    ) -> pplx_space_metrics::sandbox::worker::AutoLifecycleDeleteAdmissionReason {
        use pplx_space_metrics::sandbox::worker::AutoLifecycleDeleteAdmissionReason as Reason;
        match self {
            Self::LocalAuthority => Reason::LocalAuthority,
            Self::DirectUnsupported => Reason::DirectUnsupported,
        }
    }
}
