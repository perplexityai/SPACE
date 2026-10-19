//! Route authorization: Space API route tokens, endpoint attestation, route streams and route
//! identity adoption.

use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpaceApiRouteTokenAuthorizationResult {
    Ok,
    MissingToken,
    InvalidToken,
    Expired,
    VerifierUnavailable,
    SandboxMismatch,
    PurposeMismatch,
    InvalidSandboxId,
    IdentityCheckDisabled,
    LocalIdentityIncomplete,
    RouteGenerationMismatch,
    NodeMismatch,
    SubstrateMismatch,
    ExecutionGenerationMismatch,
    ActivationAttemptMismatch,
    PodUidMismatch,
    ServicePortMismatch,
}

impl SpaceApiRouteTokenAuthorizationResult {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::MissingToken => "missing_token",
            Self::InvalidToken => "invalid_token",
            Self::Expired => "expired",
            Self::VerifierUnavailable => "verifier_unavailable",
            Self::SandboxMismatch => "sandbox_mismatch",
            Self::PurposeMismatch => "purpose_mismatch",
            Self::InvalidSandboxId => "invalid_sandbox_id",
            Self::IdentityCheckDisabled => "identity_check_disabled",
            Self::LocalIdentityIncomplete => "local_identity_incomplete",
            Self::RouteGenerationMismatch => "route_generation_mismatch",
            Self::NodeMismatch => "node_mismatch",
            Self::SubstrateMismatch => "substrate_mismatch",
            Self::ExecutionGenerationMismatch => "execution_generation_mismatch",
            Self::ActivationAttemptMismatch => "activation_attempt_mismatch",
            Self::PodUidMismatch => "pod_uid_mismatch",
            Self::ServicePortMismatch => "service_port_mismatch",
        }
    }

    pub(super) const fn disposition(self) -> SpaceApiRouteTokenAuthorizationDisposition {
        match self {
            Self::Ok => SpaceApiRouteTokenAuthorizationDisposition::Allowed,
            Self::IdentityCheckDisabled => {
                SpaceApiRouteTokenAuthorizationDisposition::AllowedUnchecked
            }
            Self::RouteGenerationMismatch
            | Self::NodeMismatch
            | Self::SubstrateMismatch
            | Self::ExecutionGenerationMismatch
            | Self::ActivationAttemptMismatch
            | Self::PodUidMismatch => {
                SpaceApiRouteTokenAuthorizationDisposition::RefreshableRejected
            }
            Self::MissingToken
            | Self::InvalidToken
            | Self::Expired
            | Self::VerifierUnavailable
            | Self::SandboxMismatch
            | Self::PurposeMismatch
            | Self::InvalidSandboxId
            | Self::LocalIdentityIncomplete
            | Self::ServicePortMismatch => {
                SpaceApiRouteTokenAuthorizationDisposition::TerminalRejected
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SpaceApiRouteTokenAuthorizationDisposition {
    Allowed,
    AllowedUnchecked,
    TerminalRejected,
    RefreshableRejected,
}

impl SpaceApiRouteTokenAuthorizationDisposition {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::AllowedUnchecked => "allowed_unchecked",
            Self::TerminalRejected => "terminal_rejected",
            Self::RefreshableRejected => "refreshable_rejected",
        }
    }
}

impl fmt::Display for SpaceApiRouteTokenAuthorizationResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn space_api_route_token_authorization(
    operation: &str,
    result: SpaceApiRouteTokenAuthorizationResult,
) {
    counter(
        "sandbox.worker.space_api_route_token.authorization",
        1,
        &[
            tag("operation", operation),
            tag("result", result.as_str()),
            tag("disposition", result.disposition().as_str()),
        ],
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpacedEndpointAttestationResult {
    Ok,
    DeferredIdentityFreePoolDonor,
    LocalBindingRejected,
    UdsPeerRejected,
    Unavailable,
}

impl SpacedEndpointAttestationResult {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::DeferredIdentityFreePoolDonor => "deferred_identity_free_pool_donor",
            Self::LocalBindingRejected => "local_binding_rejected",
            Self::UdsPeerRejected => "uds_peer_rejected",
            Self::Unavailable => "unavailable",
        }
    }
}

pub fn spaced_endpoint_attestation(result: SpacedEndpointAttestationResult) {
    counter(
        "sandbox.worker.spaced_endpoint.attestation",
        1,
        &[tag("result", result.as_str())],
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteStreamCloseReason {
    IdentityChanged,
    LifecycleTeardown,
}

impl RouteStreamCloseReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IdentityChanged => "identity_changed",
            Self::LifecycleTeardown => "lifecycle_teardown",
        }
    }
}

pub fn route_stream_closed(reason: RouteStreamCloseReason) {
    counter(
        "sandbox.worker.route_stream.closed",
        1,
        &[tag("reason", reason.as_str())],
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteIdentityAdoptionOutcome {
    Adopted,
    Inert,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteIdentityAdoptionReason {
    Absent,
    Unchanged,
    Unpublishable,
    ProvisionalNotAdopted,
    ControlPlaneMetadata,
    ControlPlaneActivation,
    StaleOrConflict,
    PendingReceiptOwnsExecution,
    LifecycleOwnsExecution,
    NotFound,
    Internal,
}

impl RouteIdentityAdoptionReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Unchanged => "unchanged",
            Self::Unpublishable => "unpublishable",
            Self::ProvisionalNotAdopted => "provisional_not_adopted",
            Self::ControlPlaneMetadata => "control_plane_metadata",
            Self::ControlPlaneActivation => "control_plane_activation",
            Self::StaleOrConflict => "stale_or_conflict",
            Self::PendingReceiptOwnsExecution => "pending_receipt_owns_execution",
            Self::LifecycleOwnsExecution => "lifecycle_owns_execution",
            Self::NotFound => "not_found",
            Self::Internal => "internal",
        }
    }
}

impl std::fmt::Display for RouteIdentityAdoptionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for RouteIdentityAdoptionReason {
    type Err = RouteIdentityAdoptionReasonParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "absent" => Ok(Self::Absent),
            "unchanged" => Ok(Self::Unchanged),
            "unpublishable" => Ok(Self::Unpublishable),
            "provisional_not_adopted" => Ok(Self::ProvisionalNotAdopted),
            "control_plane_metadata" => Ok(Self::ControlPlaneMetadata),
            "control_plane_activation" => Ok(Self::ControlPlaneActivation),
            "stale_or_conflict" => Ok(Self::StaleOrConflict),
            "pending_receipt_owns_execution" => Ok(Self::PendingReceiptOwnsExecution),
            "lifecycle_owns_execution" => Ok(Self::LifecycleOwnsExecution),
            "not_found" => Ok(Self::NotFound),
            "internal" => Ok(Self::Internal),
            _ => Err(RouteIdentityAdoptionReasonParseError),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown route identity adoption reason")]
pub struct RouteIdentityAdoptionReasonParseError;

impl RouteIdentityAdoptionOutcome {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Adopted => "adopted",
            Self::Inert => "inert",
            Self::Rejected => "rejected",
        }
    }
}

pub fn route_identity_adoption(
    outcome: RouteIdentityAdoptionOutcome,
    reason: RouteIdentityAdoptionReason,
) {
    counter(
        "sandbox.worker.route_identity.adoption",
        1,
        &[
            tag("outcome", outcome.as_str()),
            tag("reason", reason.as_str()),
        ],
    );
}
