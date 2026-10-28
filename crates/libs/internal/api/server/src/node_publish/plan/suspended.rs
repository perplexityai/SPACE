use pplx_space_common_core::types::{
    ExecutionGeneration, PodUid, SandboxId, SandboxQuarantineReasonCode, SandboxTransitionReceipt,
};

use crate::node_publish::validate::{
    sandbox_identity::PublishedLocalArtifact, sandbox_observation::ValidatedSandboxObservation,
};

/// Suspended runtime evidence; durable artifacts and current ownership are
/// checked only by the store while it holds the publication transaction.
#[derive(Debug)]
pub(crate) struct SuspendedClaim {
    pub(crate) sandbox_id: SandboxId,
    pub(crate) receipt: Option<SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
    pub(crate) artifact: Option<PublishedLocalArtifact>,
    pub(crate) execution_generation: Option<ExecutionGeneration>,
    pub(crate) pod_uid: Option<PodUid>,
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
