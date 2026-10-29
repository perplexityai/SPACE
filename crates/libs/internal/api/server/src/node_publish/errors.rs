use tonic::Status;

use super::store::ApplyPublishError;
use super::store::ready_suspend::ProjectReadySuspendError;
use crate::execution_observation::PrepareExecutionObservationError;
use crate::execution_observation::PublishExecutionInventoryError;
use crate::published_state::{ApplySandboxClaimsError, ApplyTemplateClaimsError};
use crate::sandbox_quarantine_store::{
    ApplyActivationRollbacksError, RecordQuarantineRecoveryError, RecoverPauseFailedError,
};

// Keep transport mapping at the RPC boundary; store operations retain their
// typed causes throughout the publication transaction.
impl From<ApplyPublishError> for Status {
    fn from(error: ApplyPublishError) -> Self {
        match error {
            ApplyPublishError::Observation(error) => error.into(),
            ApplyPublishError::InvalidTemplate(error)
            | ApplyPublishError::ExecutionInventory(
                PublishExecutionInventoryError::InvalidInventory(error),
            ) => error,
            ApplyPublishError::Sandboxes(error) => error.into(),
            ApplyPublishError::Templates(error) => error.into(),
            ApplyPublishError::ExecutionPreparation(
                error @ PrepareExecutionObservationError::MissingNode,
            ) => Self::not_found(error.to_string()),
            error @ (ApplyPublishError::Begin(_)
            | ApplyPublishError::Commit(_)
            | ApplyPublishError::TemplateCensus(_)
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

impl From<ProjectReadySuspendError> for Status {
    fn from(error: ProjectReadySuspendError) -> Self {
        Self::internal(error.to_string())
    }
}

impl From<ApplyTemplateClaimsError> for Status {
    fn from(error: ApplyTemplateClaimsError) -> Self {
        Self::internal(error.to_string())
    }
}

#[cfg(test)]
mod tests;
