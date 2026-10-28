use pplx_space_common_core::types::{
    ExecutionGeneration, SandboxActivationAttemptId, SandboxCleanupEpisodeId, SandboxId,
    SandboxLifecycle, SandboxQuarantineReasonCode, SandboxTransitionReceipt,
    SandboxTransitionTargetState,
};

use crate::node_publish::validate::sandbox_observation::ValidatedSandboxObservation;

/// The node's completion evidence, not a decision to release the stored attempt.
#[derive(Debug, serde::Serialize)]
#[serde(tag = "transition_receipt_kind", rename = "activation_completed")]
pub(crate) struct ActivationCompletionClaim {
    sandbox_id: SandboxId,
    status: Option<SandboxLifecycle>,
    #[serde(rename = "transition_receipt_target_state")]
    target_state: SandboxTransitionTargetState,
    #[serde(rename = "transition_receipt_execution_generation")]
    execution_generation: ExecutionGeneration,
    #[serde(rename = "transition_receipt_activation_attempt_id")]
    activation_attempt_id: SandboxActivationAttemptId,
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
#[derive(Debug, serde::Serialize)]
#[serde(tag = "transition_receipt_kind", rename = "activation_rollback")]
pub(crate) struct ActivationRollbackClaim {
    sandbox_id: SandboxId,
    status: Option<SandboxLifecycle>,
    #[serde(rename = "transition_receipt_id")]
    receipt_id: SandboxCleanupEpisodeId,
    #[serde(rename = "transition_receipt_target_state")]
    target_state: SandboxTransitionTargetState,
    #[serde(rename = "transition_receipt_execution_generation")]
    execution_generation: ExecutionGeneration,
    #[serde(rename = "transition_receipt_activation_attempt_id")]
    activation_attempt_id: SandboxActivationAttemptId,
    #[serde(
        rename = "transition_receipt_reason_code",
        serialize_with = "serialize_reason"
    )]
    reason_code: SandboxQuarantineReasonCode,
    failure_reason: Option<String>,
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

fn serialize_reason<S: serde::Serializer>(
    reason: &SandboxQuarantineReasonCode,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(reason.as_str())
}
