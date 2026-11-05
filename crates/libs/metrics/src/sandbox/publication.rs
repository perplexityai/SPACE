use std::fmt;

use crate::emit::{counter, tag, timing_ms};

pub fn state_convergence_duration_ms(state: &str, duration_ms: f64) {
    let state = match state {
        "creating" | "created" | "starting" | "started" | "stopping" | "stopped" | "deleting"
        | "pausing" | "paused" | "resuming" | "checkpointing" | "suspending" | "suspended"
        | "restoring" | "cleanup_required" | "cleanup_failed" | "quarantined" | "failed" => state,
        _ => "unknown",
    };
    timing_ms(
        "sandbox.state_convergence.duration_ms",
        duration_ms.max(0.0),
        &[tag("state", state)],
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatePublicationComponent {
    Worker,
    Api,
}

impl StatePublicationComponent {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::Api => "api",
        }
    }
}

impl fmt::Display for StatePublicationComponent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatePublicationField {
    CoreRow,
    QuarantineRecoveryObservation,
    QuarantineReasonCode,
    TransitionReceiptId,
    TransitionReceiptKind,
    TransitionReceiptTargetState,
    TransitionReceiptExecutionGeneration,
    TransitionReceiptReasonCode,
    TransitionReceiptActivationAttemptId,
    StoredLifecycle,
}

impl StatePublicationField {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CoreRow => "core_row",
            Self::QuarantineRecoveryObservation => "quarantine_recovery_observation",
            Self::QuarantineReasonCode => "quarantine_reason_code",
            Self::TransitionReceiptId => "transition_receipt_id",
            Self::TransitionReceiptKind => "transition_receipt_kind",
            Self::TransitionReceiptTargetState => "transition_receipt_target_state",
            Self::TransitionReceiptExecutionGeneration => "transition_receipt_execution_generation",
            Self::TransitionReceiptReasonCode => "transition_receipt_reason_code",
            Self::TransitionReceiptActivationAttemptId => {
                "transition_receipt_activation_attempt_id"
            }
            Self::StoredLifecycle => "stored_lifecycle",
        }
    }
}

impl fmt::Display for StatePublicationField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatePublicationDisposition {
    OmitSandbox,
    OmitQuarantineRecoveryObservation,
    StateOnly,
    OmitTransitionReceipt,
    FallbackTransitionReason,
    FallbackQuarantined,
    SkipPauseFailedRecovery,
}

impl StatePublicationDisposition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OmitSandbox => "omit_sandbox",
            Self::OmitQuarantineRecoveryObservation => "omit_quarantine_recovery_observation",
            Self::StateOnly => "state_only",
            Self::OmitTransitionReceipt => "omit_transition_receipt",
            Self::FallbackTransitionReason => "fallback_transition_reason",
            Self::FallbackQuarantined => "fallback_quarantined",
            Self::SkipPauseFailedRecovery => "skip_pause_failed_recovery",
        }
    }
}

impl fmt::Display for StatePublicationDisposition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub fn degraded(
    component: StatePublicationComponent,
    field: StatePublicationField,
    disposition: StatePublicationDisposition,
) {
    counter(
        "sandbox.state_publication.degraded",
        1,
        &[
            tag("component", component.as_str()),
            tag("field", field.as_str()),
            tag("disposition", disposition.as_str()),
        ],
    );
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AutoLifecycleDeleteCompletionDisposition {
    Applied,
    AcknowledgedNoop,
    Deferred,
    Unacknowledgeable,
}

impl AutoLifecycleDeleteCompletionDisposition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::AcknowledgedNoop => "acknowledged_noop",
            Self::Deferred => "deferred",
            Self::Unacknowledgeable => "unacknowledgeable",
        }
    }
}

impl fmt::Display for AutoLifecycleDeleteCompletionDisposition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AutoLifecycleDeleteCompletionReason {
    Applied,
    SandboxAbsent,
    StaleGeneration,
    MissingLineage,
    AheadOfLineage,
    MissingRouteIdentity,
    RouteIncarnationMismatch,
    DifferentExecutionNode,
    PolicyMismatch,
    PolicyUnreadable,
    PolicyDoesNotAuthorizeDelete,
    DeletedTombstone,
    CleanupRequired,
    UnknownLifecycle,
    TransientLifecycle,
    DifferentActiveNode,
    AwaitingOwnershipEvidence,
    OwnershipEvidenceExpired,
    OwnershipAbsent,
    QuarantineHold,
    CreatingSnapshot,
    MalformedReceipt,
    UnsupportedFingerprintVersion,
    DuplicateSandboxReceipt,
    LockContended,
    FinalizeContended,
    BatchLimit,
    RequestLimit,
}

impl AutoLifecycleDeleteCompletionReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::SandboxAbsent => "sandbox_absent",
            Self::StaleGeneration => "stale_generation",
            Self::MissingLineage => "missing_lineage",
            Self::AheadOfLineage => "ahead_of_lineage",
            Self::MissingRouteIdentity => "missing_route_identity",
            Self::RouteIncarnationMismatch => "route_incarnation_mismatch",
            Self::DifferentExecutionNode => "different_execution_node",
            Self::PolicyMismatch => "policy_mismatch",
            Self::PolicyUnreadable => "policy_unreadable",
            Self::PolicyDoesNotAuthorizeDelete => "policy_does_not_authorize_delete",
            Self::DeletedTombstone => "deleted_tombstone",
            Self::CleanupRequired => "cleanup_required",
            Self::UnknownLifecycle => "unknown_lifecycle",
            Self::TransientLifecycle => "transient_lifecycle",
            Self::DifferentActiveNode => "different_active_node",
            Self::AwaitingOwnershipEvidence => "awaiting_ownership_evidence",
            Self::OwnershipEvidenceExpired => "ownership_evidence_expired",
            Self::OwnershipAbsent => "ownership_absent",
            Self::QuarantineHold => "quarantine_hold",
            Self::CreatingSnapshot => "creating_snapshot",
            Self::MalformedReceipt => "malformed_receipt",
            Self::UnsupportedFingerprintVersion => "unsupported_fingerprint_version",
            Self::DuplicateSandboxReceipt => "duplicate_sandbox_receipt",
            Self::LockContended => "lock_contended",
            Self::FinalizeContended => "finalize_contended",
            Self::BatchLimit => "batch_limit",
            Self::RequestLimit => "request_limit",
        }
    }
}

impl fmt::Display for AutoLifecycleDeleteCompletionReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub fn auto_lifecycle_delete_completion(
    disposition: AutoLifecycleDeleteCompletionDisposition,
    reason: AutoLifecycleDeleteCompletionReason,
    value: u64,
) {
    counter(
        "sandbox.auto_lifecycle.delete_completion",
        value,
        &[
            tag("disposition", disposition.as_str()),
            tag("reason", reason.as_str()),
        ],
    );
}
