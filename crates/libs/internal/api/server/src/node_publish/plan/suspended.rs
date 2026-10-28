use pplx_space_common_core::types::{
    ExecutionGeneration, PodUid, SandboxId, SandboxLifecycle, SandboxQuarantineReasonCode,
    SandboxTransitionReceipt,
};
use serde::ser::SerializeStruct;

use crate::node_publish::validate::{
    sandbox_identity::PublishedLocalArtifact, sandbox_observation::ValidatedSandboxObservation,
};

/// Suspended runtime evidence; durable artifacts and current ownership are
/// checked only by the store while it holds the publication transaction.
#[derive(Debug)]
pub(crate) struct SuspendedClaim {
    sandbox_id: SandboxId,
    receipt: Option<SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
    artifact: Option<PublishedLocalArtifact>,
    execution_generation: Option<ExecutionGeneration>,
    pod_uid: Option<PodUid>,
}

impl SuspendedClaim {
    pub(super) fn new(
        observation: &ValidatedSandboxObservation,
        receipt: Option<SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
    ) -> Self {
        let identity = observation.identity();
        Self {
            sandbox_id: observation.sandbox_id(),
            receipt,
            artifact: identity.current_local_artifact().cloned(),
            execution_generation: identity.execution_generation(),
            pod_uid: identity.pod_uid().cloned(),
        }
    }
}

impl serde::Serialize for SuspendedClaim {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let receipt = self.receipt.as_ref();
        let artifact = self.artifact.as_ref();
        let mut row = serializer.serialize_struct("SuspendedClaim", 13)?;
        row.serialize_field("sandbox_id", &self.sandbox_id)?;
        row.serialize_field("status", &SandboxLifecycle::Suspended)?;
        row.serialize_field("transition_receipt_id", &receipt.map(|value| value.id()))?;
        row.serialize_field(
            "transition_receipt_kind",
            &receipt.map(|value| value.kind()),
        )?;
        row.serialize_field(
            "transition_receipt_target_state",
            &receipt.map(|value| value.target_state()),
        )?;
        row.serialize_field(
            "transition_receipt_execution_generation",
            &receipt.map(|value| value.execution_generation()),
        )?;
        row.serialize_field(
            "transition_receipt_reason_code",
            &receipt.and_then(|value| value.reason_code().map(|reason| reason.as_str())),
        )?;
        row.serialize_field(
            "transition_receipt_recovery_policy",
            &receipt
                .and_then(|value| value.reason_code())
                .and_then(|reason| reason.automatic_recovery_policy())
                .map(|policy| policy.as_str()),
        )?;
        row.serialize_field(
            "current_local_artifact_id",
            &artifact.map(|value| value.id()),
        )?;
        row.serialize_field(
            "current_local_artifact_kind",
            &artifact.map(|value| value.kind().as_str()),
        )?;
        row.serialize_field(
            "current_local_artifact_origin",
            &artifact.map(|value| value.origin().as_str()),
        )?;
        row.serialize_field("execution_generation", &self.execution_generation)?;
        row.serialize_field("pod_uid", &self.pod_uid)?;
        row.end()
    }
}
