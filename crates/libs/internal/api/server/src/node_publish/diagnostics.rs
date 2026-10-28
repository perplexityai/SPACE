use pplx_space_common_core::types::{
    AutoLifecycleDeleteReceiptId, NodeId, SandboxId, SandboxQuarantineVocabularyParseError,
};
use pplx_space_common_svc::types::AUTO_LIFECYCLE_DELETE_COMPLETION_REQUEST_LIMIT;
use pplx_space_metrics::sandbox::publication::{
    AutoLifecycleDeleteCompletionDisposition, AutoLifecycleDeleteCompletionReason,
    StatePublicationComponent as Component, StatePublicationDisposition as Disposition,
    StatePublicationField as Field, degraded,
};
use tonic::Status;

use super::validate::delete_completions::CompletionValidationError;
use super::validate::receipts::{PublishedTransitionReceiptError, RollbackReasonFallback};
use super::validate::sandbox_observation::InvalidSpacedVersion;

#[derive(Debug)]
pub(crate) struct InvalidCensusIdentity {
    pub(crate) raw_id: String,
    pub(crate) error: Status,
}

impl InvalidCensusIdentity {
    pub(crate) fn emit(&self, origin: &crate::OriginNode) {
        tracing::warn!(
            target: "pplx_space_internal_api_server::publish",
            node_id = %origin.display_id,
            sandbox_id = %self.raw_id,
            error = %self.error,
            "preserving existing node associations because a complete inventory contains an invalid sandbox identity"
        );
    }
}

#[derive(Debug)]
pub(crate) enum DeleteCompletionDiagnostic {
    Oversized {
        received: usize,
        dropped: usize,
    },
    Malformed {
        receipt_id: String,
        error: CompletionValidationError,
        disposition: AutoLifecycleDeleteCompletionDisposition,
    },
    Duplicate {
        sandbox_id: SandboxId,
        receipt_id: AutoLifecycleDeleteReceiptId,
    },
}

impl DeleteCompletionDiagnostic {
    pub(crate) fn emit(&self, origin_node_id: NodeId) {
        const TARGET: &str = "pplx_space_internal_api_server::auto_lifecycle_delete_completions";
        match self {
            Self::Oversized { received, dropped } => tracing::warn!(
                target: TARGET,
                node_id = %origin_node_id,
                received,
                limit = AUTO_LIFECYCLE_DELETE_COMPLETION_REQUEST_LIMIT,
                dropped,
                "truncating oversized auto-lifecycle delete completion batch"
            ),
            Self::Malformed {
                receipt_id,
                error,
                disposition,
            } => tracing::warn!(
                target: TARGET,
                %receipt_id,
                %error,
                %disposition,
                "ignoring auto-lifecycle delete completion this receiver cannot act on"
            ),
            Self::Duplicate {
                sandbox_id,
                receipt_id,
            } => tracing::debug!(
                target: TARGET,
                %sandbox_id,
                %receipt_id,
                reason = %AutoLifecycleDeleteCompletionReason::DuplicateSandboxReceipt,
                "deferring auto-lifecycle delete completion"
            ),
        }
    }
}

// Keep the existing diagnostic target while parsing moves to a pure boundary.
const LOG_TARGET: &str = "pplx_space_internal_api_server::published_state";

/// Non-fatal evidence retained by validation for the caller to report.
/// Invalid wire labels are diagnostic context, never transition authority.
#[derive(Debug)]
pub(crate) enum SandboxDiagnostic {
    OmitSandbox {
        raw_id: String,
        error: Status,
    },
    FallbackTransitionReason {
        raw_id: String,
        fallback: RollbackReasonFallback,
    },
    OmitTransitionReceipt {
        sandbox_id: SandboxId,
        error: PublishedTransitionReceiptError,
    },
    OmitQuarantineRecovery {
        sandbox_id: SandboxId,
        error: Status,
    },
    UnknownQuarantineReason {
        sandbox_id: SandboxId,
        label: String,
        error: SandboxQuarantineVocabularyParseError,
    },
    InvalidSpacedVersion {
        sandbox_id: SandboxId,
        warning: InvalidSpacedVersion,
    },
    StateOnly {
        sandbox_id: SandboxId,
        error: Status,
    },
}

impl SandboxDiagnostic {
    pub(crate) fn emit(&self, node_id: NodeId) {
        match self {
            Self::OmitSandbox { raw_id, error } => {
                let disposition = Disposition::OmitSandbox;
                degraded(Component::Api, Field::CoreRow, disposition);
                tracing::warn!(
                    target: LOG_TARGET,
                    %node_id,
                    sandbox_id = %raw_id,
                    %error,
                    %disposition,
                    "omitting malformed sandbox identity from node publication"
                );
            }
            Self::FallbackTransitionReason { raw_id, fallback } => {
                let disposition = Disposition::FallbackTransitionReason;
                degraded(
                    Component::Api,
                    Field::TransitionReceiptReasonCode,
                    disposition,
                );
                tracing::warn!(
                    target: LOG_TARGET,
                    sandbox_id = %raw_id,
                    reason_code = fallback.raw_reason,
                    error = %fallback.error,
                    %disposition,
                    fallback_reason_code = %fallback.reason,
                    "using target-specific fallback for a forward-compatible activation rollback reason"
                );
            }
            Self::OmitTransitionReceipt { sandbox_id, error } => {
                let disposition = Disposition::OmitTransitionReceipt;
                degraded(Component::Api, error.field, disposition);
                tracing::warn!(
                    target: LOG_TARGET,
                    %node_id,
                    %sandbox_id,
                    field = %error.field,
                    %disposition,
                    %error,
                    "ignoring malformed or forward-incompatible transition receipt without rejecting sandbox state"
                );
            }
            Self::OmitQuarantineRecovery { sandbox_id, error } => {
                let disposition = Disposition::OmitQuarantineRecoveryObservation;
                degraded(
                    Component::Api,
                    Field::QuarantineRecoveryObservation,
                    disposition,
                );
                tracing::warn!(
                    target: LOG_TARGET,
                    %node_id,
                    %sandbox_id,
                    %error,
                    %disposition,
                    "ignoring malformed quarantine recovery observation without rejecting sandbox state"
                );
            }
            Self::UnknownQuarantineReason {
                sandbox_id,
                label,
                error,
            } => tracing::warn!(
                target: LOG_TARGET,
                %node_id,
                %sandbox_id,
                quarantine_reason_code = %label,
                %error,
                "downgrading unknown node quarantine reason without rejecting state publication"
            ),
            Self::InvalidSpacedVersion {
                sandbox_id,
                warning,
            } => tracing::warn!(
                target: LOG_TARGET,
                %node_id,
                %sandbox_id,
                spaced_version = %warning.label,
                error = %warning.error,
                "dropping invalid sandbox spaced version from node publish"
            ),
            Self::StateOnly { sandbox_id, error } => {
                let disposition = Disposition::StateOnly;
                degraded(Component::Api, Field::CoreRow, disposition);
                tracing::warn!(
                    target: LOG_TARGET,
                    %node_id,
                    %sandbox_id,
                    %error,
                    %disposition,
                    "publishing sandbox lifecycle without malformed optional fields"
                );
            }
        }
    }
}
