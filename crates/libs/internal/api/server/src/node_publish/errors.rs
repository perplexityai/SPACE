use tonic::Status;

use super::store::ApplyPublishError;
use crate::execution_observation::PrepareExecutionObservationError;
use crate::published_state::ApplySandboxClaimsError;
use crate::sandbox_quarantine_store::{
    ApplyActivationRollbacksError, RecordQuarantineRecoveryError, RecoverPauseFailedError,
};

// Keep transport mapping at the RPC boundary. Legacy operations retain their
// existing status unchanged until they acquire operation-specific error types.
impl From<ApplyPublishError> for Status {
    fn from(error: ApplyPublishError) -> Self {
        match error {
            ApplyPublishError::Legacy(error) => error,
            ApplyPublishError::Sandboxes(error) => error.into(),
            ApplyPublishError::ExecutionPreparation(
                error @ PrepareExecutionObservationError::MissingNode,
            ) => Self::not_found(error.to_string()),
            error @ (ApplyPublishError::Begin(_)
            | ApplyPublishError::Commit(_)
            | ApplyPublishError::PendingBackupDecision(_)
            | ApplyPublishError::PendingBackupReconciliation(_)
            | ApplyPublishError::ExecutionPreparation(_)
            | ApplyPublishError::ExecutionInventory(_)
            | ApplyPublishError::SandboxCensus(_)
            | ApplyPublishError::NodeStatus(_)
            | ApplyPublishError::DeleteCompletions(_)) => Self::internal(error.to_string()),
        }
    }
}

impl From<ApplySandboxClaimsError> for Status {
    fn from(error: ApplySandboxClaimsError) -> Self {
        match error {
            ApplySandboxClaimsError::Legacy(error) => error,
            error @ (ApplySandboxClaimsError::ActivationRollback(
                ApplyActivationRollbacksError::LostOwnership(_),
            )
            | ApplySandboxClaimsError::PauseRecovery(
                RecoverPauseFailedError::LostOwnership(_),
            )
            | ApplySandboxClaimsError::RecoveryHistory(
                RecordQuarantineRecoveryError::HistoryCas { .. }
                | RecordQuarantineRecoveryError::RetirementCas { .. },
            )) => Self::aborted(error.to_string()),
            error => Self::internal(error.to_string()),
        }
    }
}

#[cfg(test)]
mod tests;
