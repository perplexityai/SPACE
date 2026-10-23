//! SPACE API route identity and capabilities: the gRPC metadata vocabulary
//! for route tokens, bridge caller policy, wait-ready purpose and
//! route-identity rejections, the scheme-neutral route capability
//! (`capability`), and Ed25519-signed, incarnation-bound route tokens
//! (`ed25519`).
//!
//! It is its own crate so services can mint and verify route capabilities
//! without depending on the rest of `pplx-space-common-svc`.

pub mod capability;
pub mod ed25519;

use std::fmt;
use std::str::FromStr;

use pplx_space_common_core::types::{SandboxRouteIdentity, SpaceApiRouteIdentity};
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::{Request, Status};

pub use crate::capability::{
    SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY, SandboxRouteCapability, SpaceApiRouteProtocol,
    SpaceApiRouteProtocolParseError, SpaceApiRoutePurpose, SpaceApiRoutePurposeParseError,
};
pub const SPACE_API_ROUTE_GENERATION_FENCE_ENABLED_ENV: &str =
    "SPACE_API_ROUTE_GENERATION_FENCE_ENABLED";
pub const SPACE_API_BRIDGE_CALLER_POLICY_METADATA_KEY: &str = "x-space-api-bridge-caller-policy";
pub const WAIT_READY_CALL_PURPOSE_METADATA_KEY: &str = "x-space-wait-ready-call-purpose";

/// Node-api's authenticated classification for a sandbox-manager bridge RPC.
///
/// This is an internal hop marker, not caller authority. Sandbox-manager only
/// honors it when the authenticated UDS peer is node-api.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpaceApiBridgeCallerPolicy {
    RouteTokenRequired,
    TrustedLifecycle,
}

/// Purpose declared by an authenticated node-api caller for SpacedWaitReady.
///
/// The marker is only a request for the lifecycle exemption. Node-api also
/// requires authenticated lifecycle authority before deriving a trusted
/// bridge policy. Missing or unknown values remain on the public bridge path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitReadyCallPurpose {
    PublicBridge,
    LifecycleProbe,
}

impl WaitReadyCallPurpose {
    const LIFECYCLE_PROBE_VALUE: &'static str = "lifecycle_probe";
}

pub fn wait_ready_call_purpose(metadata: &MetadataMap) -> WaitReadyCallPurpose {
    match metadata
        .get(WAIT_READY_CALL_PURPOSE_METADATA_KEY)
        .and_then(|value| value.to_str().ok())
    {
        Some(WaitReadyCallPurpose::LIFECYCLE_PROBE_VALUE) => WaitReadyCallPurpose::LifecycleProbe,
        _ => WaitReadyCallPurpose::PublicBridge,
    }
}

pub fn take_wait_ready_call_purpose(metadata: &mut MetadataMap) -> WaitReadyCallPurpose {
    let purpose = wait_ready_call_purpose(metadata);
    metadata.remove(WAIT_READY_CALL_PURPOSE_METADATA_KEY);
    purpose
}

pub fn wait_ready_lifecycle_request<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        WAIT_READY_CALL_PURPOSE_METADATA_KEY,
        MetadataValue::from_static(WaitReadyCallPurpose::LIFECYCLE_PROBE_VALUE),
    );
    request
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpaceApiRouteCapabilityPresence {
    Absent,
    Present,
}

impl SpaceApiRouteCapabilityPresence {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Present => "present",
        }
    }
}

impl SpaceApiBridgeCallerPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RouteTokenRequired => "route_token_required",
            Self::TrustedLifecycle => "trusted_lifecycle",
        }
    }
}

impl fmt::Display for SpaceApiBridgeCallerPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for SpaceApiBridgeCallerPolicy {
    type Err = SpaceApiBridgeCallerPolicyParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "route_token_required" => Ok(Self::RouteTokenRequired),
            "trusted_lifecycle" => Ok(Self::TrustedLifecycle),
            _ => Err(SpaceApiBridgeCallerPolicyParseError),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown SPACE API bridge caller policy")]
pub struct SpaceApiBridgeCallerPolicyParseError;

pub fn set_space_api_bridge_caller_policy(
    metadata: &mut MetadataMap,
    policy: SpaceApiBridgeCallerPolicy,
) {
    metadata.insert(
        SPACE_API_BRIDGE_CALLER_POLICY_METADATA_KEY,
        MetadataValue::from_static(policy.as_str()),
    );
}

pub fn space_api_bridge_caller_policy(
    metadata: &MetadataMap,
) -> Result<Option<SpaceApiBridgeCallerPolicy>, Status> {
    metadata
        .get(SPACE_API_BRIDGE_CALLER_POLICY_METADATA_KEY)
        .map(|value| {
            value
                .to_str()
                .map_err(|_| Status::permission_denied("invalid bridge caller policy"))?
                .parse()
                .map_err(|_| Status::permission_denied("invalid bridge caller policy"))
        })
        .transpose()
}

pub fn space_api_route_capability_presence(
    metadata: &MetadataMap,
) -> SpaceApiRouteCapabilityPresence {
    if metadata.contains_key(SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY) {
        SpaceApiRouteCapabilityPresence::Present
    } else {
        SpaceApiRouteCapabilityPresence::Absent
    }
}

/// One route-identity dimension compared after capability verification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpaceApiRouteIdentityDimension {
    SandboxId,
    RouteGeneration,
    CurrentNodeId,
    SubstrateKind,
    ExecutionGeneration,
    ActivationAttemptId,
    PodUid,
}

impl SpaceApiRouteIdentityDimension {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SandboxId => "sandbox_id",
            Self::RouteGeneration => "route_generation",
            Self::CurrentNodeId => "current_node_id",
            Self::SubstrateKind => "substrate_kind",
            Self::ExecutionGeneration => "execution_generation",
            Self::ActivationAttemptId => "activation_attempt_id",
            Self::PodUid => "pod_uid",
        }
    }

    /// Whether this dimension identifies one incarnation of a route rather than
    /// the sandbox the route belongs to.
    ///
    /// Incarnation dimensions turn over while a sandbox keeps its identity: a
    /// replacement runtime receives a new route activation ID, a restore bumps
    /// the execution generation, and an evacuation moves the current node and
    /// pod. On Kubernetes, an ordinary lifecycle-operation lease does not
    /// rotate the route activation ID when the routed runtime is unchanged;
    /// Direct continues to use its activation attempt as an incarnation
    /// dimension. A token minted before an actual incarnation change is stale
    /// but was legitimately issued to its holder. The sandbox ID is the one
    /// dimension that does not turn over, so a mismatch there means the token
    /// was minted for a different sandbox.
    pub const fn rotates_per_incarnation(self) -> bool {
        match self {
            Self::SandboxId => false,
            Self::RouteGeneration
            | Self::CurrentNodeId
            | Self::SubstrateKind
            | Self::ExecutionGeneration
            | Self::ActivationAttemptId
            | Self::PodUid => true,
        }
    }
}

impl fmt::Display for SpaceApiRouteIdentityDimension {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown route identity dimension")]
pub struct SpaceApiRouteIdentityDimensionParseError;

impl FromStr for SpaceApiRouteIdentityDimension {
    type Err = SpaceApiRouteIdentityDimensionParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "sandbox_id" => Ok(Self::SandboxId),
            "route_generation" => Ok(Self::RouteGeneration),
            "current_node_id" => Ok(Self::CurrentNodeId),
            "substrate_kind" => Ok(Self::SubstrateKind),
            "execution_generation" => Ok(Self::ExecutionGeneration),
            "activation_attempt_id" => Ok(Self::ActivationAttemptId),
            "pod_uid" => Ok(Self::PodUid),
            _ => Err(SpaceApiRouteIdentityDimensionParseError),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SpaceApiRouteIdentityMatchError {
    #[error("route identity dimension {0} does not match")]
    Mismatch(SpaceApiRouteIdentityDimension),
}

impl SpaceApiRouteIdentityMatchError {
    /// How the caller that presented the rejected token should react.
    pub const fn remediation(&self) -> RouteIdentityRemediation {
        match self {
            Self::Mismatch(dimension) if dimension.rotates_per_incarnation() => {
                RouteIdentityRemediation::RefreshRoute
            }
            // A sandbox-ID mismatch is a token minted for someone else, and the
            // remaining variants describe identities that cannot be completed by
            // asking again: both sides would produce the same verdict.
            Self::Mismatch(_) => RouteIdentityRemediation::Deny,
        }
    }
}

/// gRPC metadata key carrying a [`RouteIdentityRemediation`] on a rejection.
pub const SPACE_API_ROUTE_IDENTITY_REMEDIATION_METADATA_KEY: &str =
    "x-space-api-route-identity-remediation";

/// gRPC metadata key carrying the rejected [`SpaceApiRouteIdentityDimension`].
///
/// Diagnostic only: callers decide what to do from the remediation, and label
/// their recovery telemetry with the dimension.
pub const SPACE_API_ROUTE_IDENTITY_DIMENSION_METADATA_KEY: &str =
    "x-space-api-route-identity-dimension";

/// How a caller should react to a rejected SPACE API route identity.
///
/// Endpoints reject a route token whenever its embedded identity disagrees with
/// the identity the endpoint is currently serving. Only some of those
/// disagreements are recoverable, and the caller cannot tell which from the
/// status code alone — every one of them is `PermissionDenied`. Carrying this
/// verdict on the rejection keeps the decision with the side that computed the
/// mismatch, so callers never have to re-derive it by inspecting error text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteIdentityRemediation {
    /// The token names a superseded incarnation of a route the caller is still
    /// entitled to. Re-resolving the route mints a token for the current
    /// incarnation, so the operation can be retried once.
    RefreshRoute,
    /// The rejection is not a staleness artifact. Re-resolving would return the
    /// same verdict, so the request must fail.
    Deny,
}

impl RouteIdentityRemediation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RefreshRoute => "refresh_route",
            Self::Deny => "deny",
        }
    }
}

impl fmt::Display for RouteIdentityRemediation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown route identity remediation")]
pub struct RouteIdentityRemediationParseError;

impl FromStr for RouteIdentityRemediation {
    type Err = RouteIdentityRemediationParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "refresh_route" => Ok(Self::RefreshRoute),
            "deny" => Ok(Self::Deny),
            _ => Err(RouteIdentityRemediationParseError),
        }
    }
}

/// Attach a route-identity rejection verdict to an outgoing `PermissionDenied`.
pub fn set_route_identity_rejection_metadata(
    status: &mut tonic::Status,
    remediation: RouteIdentityRemediation,
    dimension: Option<SpaceApiRouteIdentityDimension>,
) {
    let metadata = status.metadata_mut();
    metadata.insert(
        SPACE_API_ROUTE_IDENTITY_REMEDIATION_METADATA_KEY,
        MetadataValue::from_static(match remediation {
            RouteIdentityRemediation::RefreshRoute => "refresh_route",
            RouteIdentityRemediation::Deny => "deny",
        }),
    );
    if let Some(dimension) = dimension {
        metadata.insert(
            SPACE_API_ROUTE_IDENTITY_DIMENSION_METADATA_KEY,
            MetadataValue::from_static(dimension.as_str()),
        );
    }
}

/// Read the remediation verdict a rejecting endpoint attached to a status.
///
/// Absent or unparseable metadata yields `None`, which callers must treat as
/// "not known to be recoverable" rather than as a retry signal.
pub fn route_identity_remediation_from_status(
    status: &tonic::Status,
) -> Option<RouteIdentityRemediation> {
    status
        .metadata()
        .get(SPACE_API_ROUTE_IDENTITY_REMEDIATION_METADATA_KEY)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

/// Read the rejected identity dimension a rejecting endpoint attached.
///
/// Parsed into the enum here, at the wire boundary, so callers label telemetry
/// from a closed set instead of forwarding whatever string arrived.
pub fn route_identity_dimension_from_status(
    status: &tonic::Status,
) -> Option<SpaceApiRouteIdentityDimension> {
    status
        .metadata()
        .get(SPACE_API_ROUTE_IDENTITY_DIMENSION_METADATA_KEY)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

/// Compare two complete route identities and name the first differing dimension.
///
/// This is the single comparator behind every strict identity check, so the
/// verification paths cannot drift on which dimension they blame for a
/// rejection, and therefore cannot drift on whether a caller is told the
/// rejection is refreshable.
pub fn match_complete_route_identity(
    claims: &SandboxRouteIdentity,
    local: &SandboxRouteIdentity,
) -> Result<(), SpaceApiRouteIdentityMatchError> {
    if claims == local {
        return Ok(());
    }
    Err(first_route_identity_mismatch(
        &SpaceApiRouteIdentity::from(claims.clone()),
        &SpaceApiRouteIdentity::from(local.clone()),
    )
    .expect("unequal complete route identities have a differing dimension"))
}

/// Reject any populated dimension of a local identity that contradicts a
/// complete identity.
///
/// Missing local dimensions remain compatible, but a third identity that
/// omits a dimension cannot mask disagreement between these two identities.
pub fn match_present_route_identity_dimensions(
    local: &SpaceApiRouteIdentity,
    expected: &SandboxRouteIdentity,
) -> Result<(), SpaceApiRouteIdentityMatchError> {
    first_present_route_identity_mismatch(local, expected).map_or(Ok(()), Err)
}

fn first_route_identity_mismatch(
    claims: &SpaceApiRouteIdentity,
    local: &SpaceApiRouteIdentity,
) -> Option<SpaceApiRouteIdentityMatchError> {
    [
        (
            claims.sandbox_id != local.sandbox_id,
            SpaceApiRouteIdentityDimension::SandboxId,
        ),
        (
            claims.route_generation != local.route_generation,
            SpaceApiRouteIdentityDimension::RouteGeneration,
        ),
        (
            claims.current_node_id != local.current_node_id,
            SpaceApiRouteIdentityDimension::CurrentNodeId,
        ),
        (
            claims.substrate_kind != local.substrate_kind,
            SpaceApiRouteIdentityDimension::SubstrateKind,
        ),
        (
            claims.execution_generation != local.execution_generation,
            SpaceApiRouteIdentityDimension::ExecutionGeneration,
        ),
        (
            claims.activation_attempt_id != local.activation_attempt_id,
            SpaceApiRouteIdentityDimension::ActivationAttemptId,
        ),
        (
            claims.pod_uid != local.pod_uid,
            SpaceApiRouteIdentityDimension::PodUid,
        ),
    ]
    .into_iter()
    .find_map(|(differs, dimension)| {
        differs.then_some(SpaceApiRouteIdentityMatchError::Mismatch(dimension))
    })
}

fn first_present_route_identity_mismatch(
    local: &SpaceApiRouteIdentity,
    expected: &SandboxRouteIdentity,
) -> Option<SpaceApiRouteIdentityMatchError> {
    [
        (
            present_route_dimension_differs(
                local.sandbox_id.as_ref(),
                Some(&expected.sandbox_id()),
            ),
            SpaceApiRouteIdentityDimension::SandboxId,
        ),
        (
            present_route_dimension_differs(
                local.route_generation.as_ref(),
                Some(&expected.route_generation()),
            ),
            SpaceApiRouteIdentityDimension::RouteGeneration,
        ),
        (
            present_route_dimension_differs(
                local.current_node_id.as_ref(),
                Some(&expected.current_node_id()),
            ),
            SpaceApiRouteIdentityDimension::CurrentNodeId,
        ),
        (
            present_route_dimension_differs(
                local.substrate_kind.as_ref(),
                Some(&expected.substrate_kind()),
            ),
            SpaceApiRouteIdentityDimension::SubstrateKind,
        ),
        (
            present_route_dimension_differs(
                local.execution_generation.as_ref(),
                Some(&expected.execution_generation()),
            ),
            SpaceApiRouteIdentityDimension::ExecutionGeneration,
        ),
        (
            present_route_dimension_differs(
                local.activation_attempt_id.as_ref(),
                Some(&expected.activation_attempt_id()),
            ),
            SpaceApiRouteIdentityDimension::ActivationAttemptId,
        ),
        (
            present_route_dimension_differs(local.pod_uid.as_ref(), expected.pod_uid()),
            SpaceApiRouteIdentityDimension::PodUid,
        ),
    ]
    .into_iter()
    .find_map(|(differs, dimension)| {
        differs.then_some(SpaceApiRouteIdentityMatchError::Mismatch(dimension))
    })
}

fn present_route_dimension_differs<T: Eq>(local: Option<&T>, expected: Option<&T>) -> bool {
    local.is_some_and(|local| Some(local) != expected)
}

fn env_enabled_by_default(name: &str) -> bool {
    match std::env::var(name) {
        Ok(value) => !matches!(
            value.trim(),
            "0" | "false" | "FALSE" | "no" | "NO" | "off" | "OFF"
        ),
        Err(_) => true,
    }
}

/// Kill switch for the runtime activation generation fence in
/// sandbox-manager. Enabled by default; set
/// `SPACE_API_ROUTE_GENERATION_FENCE_ENABLED=false` to stop rejecting
/// activations whose generation is not newer than the retained floor
/// (fail-open) while keeping identity minting and claim bookkeeping intact.
pub fn space_api_route_generation_fence_enabled() -> bool {
    env_enabled_by_default(SPACE_API_ROUTE_GENERATION_FENCE_ENABLED_ENV)
}

pub fn space_api_route_token_metadata(metadata: &MetadataMap) -> MetadataMap {
    crate::capability::route_token_metadata(metadata)
}

#[cfg(test)]
mod tests;
