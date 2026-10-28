use pplx_space_common_core::types::{NodeId, SandboxId, SandboxQuarantineVocabularyParseError};
use pplx_space_metrics::sandbox::publication::{
    StatePublicationComponent as Component, StatePublicationDisposition as Disposition,
    StatePublicationField as Field, degraded,
};
use tonic::Status;

use super::validate::receipts::{PublishedTransitionReceiptError, RollbackReasonFallback};
use super::validate::sandbox_observation::InvalidSpacedVersion;

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
