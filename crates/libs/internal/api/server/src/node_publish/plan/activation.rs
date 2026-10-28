use pplx_space_common_core::types::{
    ExecutionGeneration, SandboxActivationAttemptId, SandboxCleanupEpisodeId, SandboxId,
    SandboxLifecycle, SandboxQuarantineReasonCode, SandboxTransitionReceipt,
    SandboxTransitionTargetState,
};

use crate::node_publish::validate::sandbox_observation::ValidatedSandboxObservation;

/// The node's completion evidence, not a decision to release the stored attempt.
#[derive(Debug)]
pub(crate) struct ActivationCompletionClaim {
    pub(crate) sandbox_id: SandboxId,
    pub(crate) status: Option<SandboxLifecycle>,
    pub(crate) target_state: SandboxTransitionTargetState,
    pub(crate) execution_generation: ExecutionGeneration,
    pub(crate) activation_attempt_id: SandboxActivationAttemptId,
}

impl ActivationCompletionClaim {
    pub(super) fn new(
        observation: &ValidatedSandboxObservation,
        receipt: &SandboxTransitionReceipt<SandboxQuarantineReasonCode>,
    ) -> Option<Self> {
        Some(Self {
            sandbox_id: observation.sandbox_id(),
            status: observation.reported_lifecycle(),
            target_state: receipt.target_state(),
            execution_generation: receipt.execution_generation(),
            activation_attempt_id: receipt.activation_attempt_id()?,
        })
    }
}

/// Rollback evidence retains the receipt identity and diagnostic used by history.
#[derive(Debug)]
pub(crate) struct ActivationRollbackClaim {
    pub(crate) sandbox_id: SandboxId,
    pub(crate) status: Option<SandboxLifecycle>,
    pub(crate) receipt_id: SandboxCleanupEpisodeId,
    pub(crate) target_state: SandboxTransitionTargetState,
    pub(crate) execution_generation: ExecutionGeneration,
    pub(crate) activation_attempt_id: SandboxActivationAttemptId,
    pub(crate) reason_code: SandboxQuarantineReasonCode,
    pub(crate) failure_reason: Option<String>,
}

impl ActivationRollbackClaim {
    pub(super) fn new(
        observation: &ValidatedSandboxObservation,
        receipt: &SandboxTransitionReceipt<SandboxQuarantineReasonCode>,
    ) -> Option<Self> {
        Some(Self {
            sandbox_id: observation.sandbox_id(),
            status: observation.reported_lifecycle(),
            receipt_id: receipt.id(),
            target_state: receipt.target_state(),
            execution_generation: receipt.execution_generation(),
            activation_attempt_id: receipt.activation_attempt_id()?,
            reason_code: *receipt.reason_code()?,
            failure_reason: observation.failure_reason().map(str::to_owned),
        })
    }
}
