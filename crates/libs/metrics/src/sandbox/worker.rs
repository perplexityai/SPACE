//! Ingress gauges report worker-wide protected-stream count and oldest live age,
//! sampled by lifecycle scheduling. Admission wait latency and lifecycle deferrals
//! describe contention independently of request preparation latency.
//!
//! `sandbox.worker.activation_env.attempt` counts completed environment-setup
//! attempts (including local deadlines and fence failures) for nonempty maps.
//! Tags are phase, attempt=first|retry, and result=ok|error. First-attempt errors
//! divided by all first attempts measure initial reliability independently of
//! recovery. Dropped futures have no completed outcome; empty maps are skipped.
use std::{fmt, str::FromStr, time::Instant};

use pplx_space_common_core::types::{
    AutoLifecycleIdleAction, ExecutionReleaseDetachReason, ExecutionReleaseIntent,
    QuarantineRecoveryNoProgressReason,
};

use crate::emit::{counter, distribution, elapsed_ms, gauge, tag, timing_ms};

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
    fn as_str(self) -> &'static str {
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
    fn as_str(self) -> &'static str {
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

    const fn disposition(self) -> SpaceApiRouteTokenAuthorizationDisposition {
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
enum SpaceApiRouteTokenAuthorizationDisposition {
    Allowed,
    AllowedUnchecked,
    TerminalRejected,
    RefreshableRejected,
}

impl SpaceApiRouteTokenAuthorizationDisposition {
    const fn as_str(self) -> &'static str {
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
pub enum ServicePortIngressRejectionReason {
    Capacity,
    StartTimeout,
}

impl ServicePortIngressRejectionReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Capacity => "capacity",
            Self::StartTimeout => "start_timeout",
        }
    }
}

pub fn service_port_ingress_rejected(reason: ServicePortIngressRejectionReason) {
    counter(
        "sandbox.worker.service_port_ingress.rejected",
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
    const fn as_str(self) -> &'static str {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteVerificationBundleReloadResult {
    Updated,
    Unchanged,
    Absent,
    Missing,
    Invalid,
    Stale,
    Conflict,
}

impl RouteVerificationBundleReloadResult {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Updated => "updated",
            Self::Unchanged => "unchanged",
            Self::Absent => "absent",
            Self::Missing => "missing",
            Self::Invalid => "invalid",
            Self::Stale => "stale",
            Self::Conflict => "conflict",
        }
    }
}

impl fmt::Display for RouteVerificationBundleReloadResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn route_verification_bundle_reload(result: RouteVerificationBundleReloadResult) {
    counter(
        "sandbox.worker.route_verification_bundle.reload",
        1,
        &[tag("result", result.as_str())],
    );
}

pub fn route_verification_bundle_generation(generation: u64) {
    gauge(
        "sandbox.worker.route_verification_bundle.generation",
        generation as f64,
        &[],
    );
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NetGatewayRouteVerificationStatus {
    Disabled,
    Matched,
    Probing,
    Converging,
    Mismatched,
    Unsupported,
    Unknown,
}

impl NetGatewayRouteVerificationStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Matched => "matched",
            Self::Probing => "probing",
            Self::Converging => "converging",
            Self::Mismatched => "mismatched",
            Self::Unsupported => "unsupported",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for NetGatewayRouteVerificationStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn net_gateway_route_verification(value: usize, status: NetGatewayRouteVerificationStatus) {
    gauge(
        "sandbox.worker.net_gateway.route_verification",
        value as f64,
        &[tag("status", status.as_str())],
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetGatewayRouteSecurityPushOperation {
    Identity,
    VerificationKeys,
}

impl NetGatewayRouteSecurityPushOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Identity => "identity",
            Self::VerificationKeys => "verification_keys",
        }
    }
}

impl fmt::Display for NetGatewayRouteSecurityPushOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetGatewayRouteSecurityPushOutcome {
    Applied,
    /// There is no local route identity to push at all.
    Absent,
    Diverged,
    Unsupported,
    Failed,
}

impl NetGatewayRouteSecurityPushOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Absent => "absent",
            Self::Diverged => "diverged",
            Self::Unsupported => "unsupported",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for NetGatewayRouteSecurityPushOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn net_gateway_route_security_push(
    operation: NetGatewayRouteSecurityPushOperation,
    outcome: NetGatewayRouteSecurityPushOutcome,
) {
    counter(
        "sandbox.worker.net_gateway.route_security_push",
        1,
        &[
            tag("operation", operation.as_str()),
            tag("outcome", outcome.as_str()),
        ],
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TeardownSuspendPrepareOutcome {
    Pending,
    Durable,
    Superseded,
    Deleted,
}

impl TeardownSuspendPrepareOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Durable => "durable",
            Self::Superseded => "superseded",
            Self::Deleted => "deleted",
        }
    }
}

impl fmt::Display for TeardownSuspendPrepareOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn teardown_suspend_prepare(outcome: TeardownSuspendPrepareOutcome, started_at: Instant) {
    let tags = [tag("outcome", outcome.as_str())];
    counter("sandbox.worker.teardown_suspend.prepare", 1, &tags);
    timing_ms(
        "sandbox.worker.teardown_suspend.prepare.latency_ms",
        elapsed_ms(started_at),
        &tags,
    );
}

/// Configured node-wide capacity for planned-teardown suspend drivers.
pub fn teardown_suspend_driver_capacity(value: usize) {
    gauge(
        "sandbox.worker.teardown_suspend.driver_capacity",
        value as f64,
        &[],
    );
}

/// A planned-teardown poll could not start its durable-suspend driver because
/// all node-wide driver permits were occupied.
pub fn teardown_suspend_driver_saturated() {
    counter("sandbox.worker.teardown_suspend.driver_saturated", 1, &[]);
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

/// Number of `schedule_async_cleanup` spawns currently holding a permit
/// from the node-wide cleanup semaphore. Bounded by the sandbox manager's
/// cleanup concurrency cap (16). A sustained reading
/// at the cap means the cleanup queue is saturating — see
/// `sandbox.worker.cleanup.queue_depth` for the backlog.
pub fn cleanup_active(value: usize) {
    gauge("sandbox.worker.cleanup.active", value as f64, &[]);
}

/// Requests waiting for cleanup admission before target allocation.
pub fn cleanup_queue_depth(value: usize) {
    gauge("sandbox.worker.cleanup.queue_depth", value as f64, &[]);
}

/// A cleanup attempt persisted for retry after a dependency or completion failure.
pub fn cleanup_retry() {
    counter("sandbox.worker.cleanup.retries", 1, &[]);
}

/// Current node-local durable sandbox rows in one lifecycle bucket. The caller
/// supplies a value decoded through the sandbox-manager's bounded state enum;
/// do not attach sandbox, pod, node, or execution identifiers.
pub fn lifecycle_count(value: u64, state: &str) {
    gauge(
        "sandbox.worker.lifecycle.count",
        value as f64,
        &[tag("state", state)],
    );
}

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
pub struct AutoLifecycleDeleteAdmissionDispositionParseError(String);

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
pub struct AutoLifecycleDeleteAdmissionReasonParseError(String);

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
pub struct AutoLifecycleDeleteOutboxStateParseError(String);

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivationCleanupOutcome {
    Requested,
    Completed,
    AttemptFailed,
    UnsafeEvidenceQuarantined,
    Superseded,
}

impl ActivationCleanupOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Completed => "completed",
            Self::AttemptFailed => "attempt_failed",
            Self::UnsafeEvidenceQuarantined => "unsafe_evidence_quarantined",
            Self::Superseded => "superseded",
        }
    }
}

impl fmt::Display for ActivationCleanupOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn activation_cleanup(outcome: ActivationCleanupOutcome) {
    counter(
        "sandbox.worker.activation_cleanup",
        1,
        &[tag("outcome", outcome.as_str())],
    );
}

// The no-progress reason vocabulary is a durable noun, not a tag vocabulary:
// its rendered form is persisted into `sandbox_quarantine_state.last_error`. It
// therefore lives in common-core next to the other quarantine vocabularies, so
// renaming a metric tag cannot change what rows persist.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuarantineRecoveryAttemptOutcome {
    RetryScheduled,
    StaleClaim,
    /// The pass never held the claim, so no durable retry state was written.
    ClaimNotAcquired,
    /// The claim was held, but the node-local database refused the write, so the
    /// retry state is whatever the claim left behind. Distinct from
    /// `ClaimNotAcquired`, which reports never having held the claim at all.
    RetryWriteFailed,
    /// A detached task panicked or was cancelled before it could classify and
    /// persist its result. The claim lease remains the liveness backstop.
    BackgroundTaskFailed,
}

impl QuarantineRecoveryAttemptOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RetryScheduled => "retry_scheduled",
            Self::StaleClaim => "stale_claim",
            Self::ClaimNotAcquired => "claim_not_acquired",
            Self::RetryWriteFailed => "retry_write_failed",
            Self::BackgroundTaskFailed => "background_task_failed",
        }
    }
}

impl fmt::Display for QuarantineRecoveryAttemptOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// What a completed legacy quarantine recovery actually delivered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuarantineRecoveryOutcome {
    CleanupAdopted,
    OrphanRestored,
}

impl QuarantineRecoveryOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CleanupAdopted => "cleanup_adopted",
            Self::OrphanRestored => "orphan_restored",
        }
    }
}

impl fmt::Display for QuarantineRecoveryOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub fn quarantine_recovery_no_progress(
    reason: QuarantineRecoveryNoProgressReason,
    outcome: QuarantineRecoveryAttemptOutcome,
) {
    counter(
        "sandbox.worker.quarantine_recovery.no_progress",
        1,
        &[tag("reason", reason), tag("outcome", outcome)],
    );
}

/// Node-wide orphan-restore capacity as one recovery pass found it.
///
/// Gauges, not `no_progress` counters, and on their own names. A capacity skip is
/// a pre-claim observation about the node, not an attempt that made no progress:
/// folding it into the attempt counter meant a node with a large due cohort behind
/// one multi-hour restore could add its whole cohort size to that counter every
/// tick, against a prior per-pass ceiling of the admission cap. A gauge does not
/// accumulate, so the deferred depth stays readable without swamping the metric
/// every attempt-rate query is built on.
///
/// `deferred` is the number of orphans this bounded batch reached and skipped for
/// lack of capacity, not the depth of the queue. A batch filled with artifact
/// adoptions reports zero even when later pages hold orphans. Neither this value
/// nor the batch summary's `remaining` describes the entire queue.
/// `closed` is whether the semaphore is closed, which is permanent and needs an
/// operator; it is read off the semaphore rather than counted from the walk, so a
/// pass that never reaches an orphan still reports it.
pub fn quarantine_recovery_orphan_capacity(deferred: usize, closed: bool) {
    gauge(
        "sandbox.worker.quarantine_recovery.orphan_capacity_deferred",
        deferred as f64,
        &[],
    );
    gauge(
        "sandbox.worker.quarantine_recovery.orphan_capacity_closed",
        f64::from(u8::from(closed)),
        &[],
    );
}

/// One legacy quarantine recovery that advanced the sandbox. Pairs with
/// `quarantine_recovery.no_progress` as the denominator a no-progress rate
/// needs: without it, a node whose every claim loses the CAS is
/// indistinguishable from a node with nothing quarantined.
pub fn quarantine_recovery_succeeded(outcome: QuarantineRecoveryOutcome) {
    counter(
        "sandbox.worker.quarantine_recovery.succeeded",
        1,
        &[tag("outcome", outcome)],
    );
}

pub fn quarantine_recovery_candidates_malformed(count: u64) {
    gauge(
        "sandbox.worker.quarantine_recovery.candidate_malformed",
        count as f64,
        &[],
    );
}

pub fn quarantine_recovery_backlog_sample_valid(valid: bool) {
    gauge(
        "sandbox.worker.quarantine_recovery.sample_valid",
        if valid { 1.0 } else { 0.0 },
        &[],
    );
}

pub fn quarantine_recovery_backlog(pending_count: u64, pending_oldest_age: std::time::Duration) {
    gauge(
        "sandbox.worker.quarantine_recovery.pending.count",
        pending_count as f64,
        &[],
    );
    gauge(
        "sandbox.worker.quarantine_recovery.pending.oldest_age_secs",
        pending_oldest_age.as_secs() as f64,
        &[],
    );
}

/// One cleanup in which node-manager observed the runtime supervisor fail to
/// stop a sandbox (the `runtime supervisor failed to stop sandbox during
/// cleanup` path). In practice this is dominated by cgroup-removal `EBUSY`
/// (`Device or resource busy`): the cgroup still held a live task — typically a
/// Firecracker vCPU thread finishing VM/memory teardown — or a child cgroup
/// when the removal budget elapsed. A transient failure self-heals via the
/// per-sandbox cleanup retry; cross-reference `cleanup.leaked_cgroups` and
/// `cleanup.cgroups_reclaimed` for the permanent-leak picture, and the
/// supervisor ERROR log for the precise cgroup path.
pub fn runtime_stop_failed() {
    counter("sandbox.worker.cleanup.runtime_stop_failed", 1, &[]);
}

/// Untracked, unpopulated sandbox cgroup directories observed by the node-local
/// reclaim sweep in its latest tick (the leak backlog it found and acted on).
/// Sustained nonzero means cgroups are leaking faster than a single sweep
/// clears, or reclaim is failing — cross-reference `cgroups_reclaimed`.
pub fn leaked_cgroups(value: usize) {
    gauge("sandbox.worker.cleanup.leaked_cgroups", value as f64, &[]);
}

/// Orphaned sandbox cgroups removed by the node-local reclaim sweep. These are
/// directories whose sandbox lease/record is gone (so no per-sandbox cleanup
/// would ever retry them) and which were unpopulated when reclaimed.
pub fn cgroups_reclaimed(count: u64) {
    if count == 0 {
        return;
    }
    counter("sandbox.worker.cleanup.cgroups_reclaimed", count, &[]);
}

pub fn local_artifact_metadata_pruned(count: u64) {
    if count == 0 {
        return;
    }
    counter("sandbox.worker.local_artifact_metadata.pruned", count, &[]);
}

/// Disposition of an inventory-reconcile orphan row materialization — which
/// arm fired when the reconcile found sandbox evidence (disk or runtime)
/// with no sbx-manager metadata row. Shared by the
/// `sandbox.worker.orphan_row_materialized` counter tag and the
/// materialization warn log field so metric trends and per-sandbox log
/// forensics use the same vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrphanRowDisposition {
    /// A complete canonical suspend artifact reconstructed the missing row in
    /// the healthy suspended state.
    RecoveredSuspended,
    /// Valid cross-node restore metadata reconstructed a retryable Restoring
    /// row while one or more durable-backed artifacts were still absent.
    RecoveredRestoring,
    /// Cache dir with no live VM — preserved behind a quarantine row because
    /// its sbx-manager metadata row is missing.
    QuarantinedRow,
    /// A live runtime was found; the row was queued for cleanup.
    CleanupQuarantine,
    /// Artifact evidence only; the row exists just to drive artifact deletion.
    DeleteRow,
}

impl OrphanRowDisposition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RecoveredSuspended => "recovered_suspended",
            Self::RecoveredRestoring => "recovered_restoring",
            Self::QuarantinedRow => "quarantined_row",
            Self::CleanupQuarantine => "cleanup_quarantine",
            Self::DeleteRow => "delete_row",
        }
    }
}

impl fmt::Display for OrphanRowDisposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OrphanRowDispositionParseError {
    #[error("unknown orphan-row disposition {0:?}")]
    Unknown(String),
}

impl FromStr for OrphanRowDisposition {
    type Err = OrphanRowDispositionParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "recovered_suspended" => Ok(Self::RecoveredSuspended),
            "recovered_restoring" => Ok(Self::RecoveredRestoring),
            "quarantined_row" => Ok(Self::QuarantinedRow),
            "cleanup_quarantine" => Ok(Self::CleanupQuarantine),
            "delete_row" => Ok(Self::DeleteRow),
            other => Err(OrphanRowDispositionParseError::Unknown(other.to_string())),
        }
    }
}

/// The inventory reconcile materialized a DB row for sandbox evidence
/// (disk or runtime) that had no sbx-manager metadata row. See
/// [`OrphanRowDisposition`] for what each disposition means.
pub fn orphan_row_materialized(source: &str, disposition: OrphanRowDisposition) {
    counter(
        "sandbox.worker.orphan_row_materialized",
        1,
        &[
            tag("source", source),
            tag("disposition", disposition.as_str()),
        ],
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InventoryReconcileOperation {
    StaleRowRemoval,
    OrphanMaterialization,
}

impl InventoryReconcileOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StaleRowRemoval => "stale_row_removal",
            Self::OrphanMaterialization => "orphan_materialization",
        }
    }
}

impl fmt::Display for InventoryReconcileOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InventoryReconcileOutcome {
    Applied,
    Deferred,
    Noop,
    Failed,
}

impl InventoryReconcileOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Deferred => "deferred",
            Self::Noop => "noop",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for InventoryReconcileOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InventoryReconcileReason {
    Removed,
    Materialized,
    LiveLockBusy,
    RestoreStagingBusy,
    ImportStaging,
    DurableCleanupEpisode,
    RowCleanupIntent,
    LifecycleOwned,
    RestoreTransferScope,
    FilesystemChanged,
    DatabaseRowAppeared,
    WitnessChanged,
    AlreadyMissing,
    DatabaseError,
    MaterializationError,
    MalformedCleanupIntent,
    FilesystemInspectionFailed,
}

impl InventoryReconcileReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Removed => "removed",
            Self::Materialized => "materialized",
            Self::LiveLockBusy => "live_lock_busy",
            Self::RestoreStagingBusy => "restore_staging_busy",
            Self::ImportStaging => "import_staging",
            Self::DurableCleanupEpisode => "durable_cleanup_episode",
            Self::RowCleanupIntent => "row_cleanup_intent",
            Self::LifecycleOwned => "lifecycle_owned",
            Self::RestoreTransferScope => "restore_transfer_scope",
            Self::FilesystemChanged => "filesystem_changed",
            Self::DatabaseRowAppeared => "database_row_appeared",
            Self::WitnessChanged => "witness_changed",
            Self::AlreadyMissing => "already_missing",
            Self::DatabaseError => "database_error",
            Self::MaterializationError => "materialization_error",
            Self::MalformedCleanupIntent => "malformed_cleanup_intent",
            Self::FilesystemInspectionFailed => "filesystem_inspection_failed",
        }
    }
}

impl fmt::Display for InventoryReconcileReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn inventory_reconcile(
    operation: InventoryReconcileOperation,
    outcome: InventoryReconcileOutcome,
    reason: InventoryReconcileReason,
) {
    counter(
        "sandbox.worker.inventory_reconcile",
        1,
        &[
            tag("operation", operation),
            tag("outcome", outcome),
            tag("reason", reason),
        ],
    );
}

/// Count of local sandboxes by guest health category and node-local lifecycle.
/// Do not attach sandbox IDs or other per-sandbox identifiers to this gauge.
pub fn guest_health(value: usize, lifecycle: &str, health: &str) {
    gauge(
        "sandbox.guest.health",
        value as f64,
        &[tag("lifecycle", lifecycle), tag("health", health)],
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxHealthComponent {
    Firecracker,
    SpacedHost,
    SpacedGuest,
    NetGateway,
}

impl SandboxHealthComponent {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Firecracker => "firecracker",
            Self::SpacedHost => "spaced_host",
            Self::SpacedGuest => "spaced_guest",
            Self::NetGateway => "net_gateway",
        }
    }
}

impl fmt::Display for SandboxHealthComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxHealthProbeOutcome {
    Success,
    Timeout,
    Unavailable,
    UnexpectedState,
    NotApplicable,
}

impl SandboxHealthProbeOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Timeout => "timeout",
            Self::Unavailable => "unavailable",
            Self::UnexpectedState => "unexpected_state",
            Self::NotApplicable => "not_applicable",
        }
    }
}

impl fmt::Display for SandboxHealthProbeOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxHealthStatus {
    Unspecified,
    Initializing,
    Healthy,
    Degraded,
}

impl SandboxHealthStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Initializing => "initializing",
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
        }
    }
}

impl fmt::Display for SandboxHealthStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn sandbox_health_probe(
    component: SandboxHealthComponent,
    outcome: SandboxHealthProbeOutcome,
    latency_micros: u64,
) {
    let tags = [
        tag("component", component.as_str()),
        tag("outcome", outcome.as_str()),
    ];
    counter("sandbox.health.probe", 1, &tags);
    timing_ms(
        "sandbox.health.probe.duration_ms",
        latency_micros as f64 / 1_000.0,
        &tags,
    );
}

pub fn sandbox_health_status(value: usize, lifecycle: &str, status: SandboxHealthStatus) {
    gauge(
        "sandbox.health.status",
        value as f64,
        &[tag("lifecycle", lifecycle), tag("status", status.as_str())],
    );
}

pub fn sandbox_health_transition(status: SandboxHealthStatus) {
    counter(
        "sandbox.health.transition",
        1,
        &[tag("status", status.as_str())],
    );
}

/// `gateway_sha` value for a gateway running a binary that reports no sha, and
/// for one that has not answered a stats probe yet.
pub const NET_GATEWAY_SHA_UNKNOWN: &str = "unknown";

/// Placeholder for template metadata absent from the local sandbox row.
pub const NET_GATEWAY_INVENTORY_UNKNOWN: &str = "unknown";

/// Live net-gateway sidecars by gateway build and sandbox lifecycle.
pub fn net_gateway_live(value: usize, gateway_sha: &str, lifecycle: &str) {
    gauge(
        "sandbox.worker.net_gateway.live",
        value as f64,
        &[tag("gateway_sha", gateway_sha), tag("lifecycle", lifecycle)],
    );
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NetGatewayTlsInterceptionMode {
    Off,
    On,
    Unknown,
}

impl NetGatewayTlsInterceptionMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::On => "on",
            Self::Unknown => "unknown",
        }
    }
}

/// Live net-gateway sidecars by accepted TLS mode, gateway build, pinned
/// template content identity, and sandbox lifecycle.
pub fn net_gateway_inventory(
    value: usize,
    tls_interception_mode: NetGatewayTlsInterceptionMode,
    gateway_sha: &str,
    template_content_hash: &str,
    lifecycle: &str,
) {
    gauge(
        "sandbox.worker.net_gateway.inventory",
        value as f64,
        &[
            tag("tls_interception_mode", tls_interception_mode.as_str()),
            tag("gateway_sha", gateway_sha),
            tag("template_content_hash", template_content_hash),
            tag("lifecycle", lifecycle),
        ],
    );
}

/// Terminal outcome of one deliberate operator-triggered net-gateway cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetGatewayCycleResult {
    Ok,
    Error,
}

impl NetGatewayCycleResult {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }
}

impl fmt::Display for NetGatewayCycleResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One deliberate operator-triggered net-gateway cycle (kill + relaunch of the
/// gateway sidecar) reaching a terminal outcome.
pub fn net_gateway_cycle(result: NetGatewayCycleResult) {
    counter(
        "sandbox.worker.net_gateway.cycles",
        1,
        &[tag("result", result.as_str())],
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

#[cfg(test)]
mod tests;

/// Worker-wide current ingress ownership, sampled by the lifecycle scheduler.
pub fn ingress_protection(count: usize, oldest_age_seconds: f64) {
    gauge(
        "sandbox.worker.ingress.protected_streams",
        count as f64,
        &[],
    );
    gauge(
        "sandbox.worker.ingress.oldest_protected_stream_age_seconds",
        oldest_age_seconds,
        &[],
    );
}
pub fn ingress_admission_wait(start: Instant) {
    timing_ms(
        "sandbox.worker.ingress.admission_wait_ms",
        elapsed_ms(start),
        &[],
    );
}
pub fn ingress_lifecycle_deferred() {
    counter("sandbox.worker.ingress.lifecycle_deferred", 1, &[]);
}
