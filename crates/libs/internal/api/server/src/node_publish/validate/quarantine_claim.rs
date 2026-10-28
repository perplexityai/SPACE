//! Node-authored hold evidence, not a decision about the current database row.

use pplx_space_common_core::types::{
    SandboxId, SandboxQuarantineReasonCode, SandboxQuarantineSource,
    SandboxQuarantineVocabularyParseError,
};

use crate::metadata::optional_trimmed;
use crate::proto::PublishedSandboxState;

#[derive(Debug)]
enum ReportedReason {
    Absent,
    Recognized(SandboxQuarantineReasonCode),
    Unrecognized {
        label: String,
        error: SandboxQuarantineVocabularyParseError,
    },
}

/// A reported quarantine still requires the store's ownership/lifecycle guard.
/// Durable episode and history identifiers are not node claims and are created
/// only when the storage path materializes its SQL input.
#[derive(Debug)]
pub(crate) struct QuarantineClaim {
    sandbox_id: SandboxId,
    failure_reason: Option<String>,
    reason: ReportedReason,
}

impl QuarantineClaim {
    pub(crate) fn from_proto(sandbox_id: SandboxId, state: &PublishedSandboxState) -> Option<Self> {
        if !state.present
            || state.state_degraded
            || !optional_trimmed(state.status.as_deref())
                .is_some_and(|status| status.eq_ignore_ascii_case("quarantined"))
        {
            return None;
        }
        let reason = match optional_trimmed(state.quarantine_reason_code.as_deref()) {
            Some(label) => match label.parse::<SandboxQuarantineReasonCode>() {
                Ok(reason) => ReportedReason::Recognized(reason),
                Err(error) => ReportedReason::Unrecognized { label, error },
            },
            None => ReportedReason::Absent,
        };
        Some(Self {
            sandbox_id,
            failure_reason: optional_trimmed(state.failure_reason.as_deref()),
            reason,
        })
    }

    pub(crate) fn sandbox_id(&self) -> SandboxId {
        self.sandbox_id
    }

    pub(crate) fn failure_reason(&self) -> Option<&str> {
        self.failure_reason.as_deref()
    }

    pub(crate) fn source(&self) -> SandboxQuarantineSource {
        match self.reason {
            ReportedReason::Recognized(_) => SandboxQuarantineSource::SbxManager,
            ReportedReason::Absent | ReportedReason::Unrecognized { .. } => {
                SandboxQuarantineSource::NodeStatePublish
            }
        }
    }

    pub(crate) fn reason_code(&self) -> SandboxQuarantineReasonCode {
        match self.reason {
            ReportedReason::Recognized(reason) => reason,
            ReportedReason::Absent | ReportedReason::Unrecognized { .. } => {
                SandboxQuarantineReasonCode::NodePublishedQuarantined
            }
        }
    }

    pub(crate) fn unrecognized_reason(
        &self,
    ) -> Option<(&str, &SandboxQuarantineVocabularyParseError)> {
        match &self.reason {
            ReportedReason::Unrecognized { label, error } => Some((label, error)),
            ReportedReason::Absent | ReportedReason::Recognized(_) => None,
        }
    }
}

#[cfg(test)]
mod tests;
