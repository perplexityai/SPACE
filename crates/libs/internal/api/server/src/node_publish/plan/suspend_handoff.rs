use pplx_space_common_core::types::{ExecutionGeneration, PodUid, SandboxId};

use crate::node_publish::validate::{
    sandbox_identity::PublishedLocalArtifact, sandbox_observation::ValidatedSandboxObservation,
    sandbox_status::PublishedSandboxStatus,
};

/// Runtime evidence that can replace or clear a matching execution's handoff.
/// Missing status or execution identity cannot satisfy the store's guards.
#[derive(Debug)]
pub(crate) struct SuspendHandoffClaim {
    pub(crate) sandbox_id: SandboxId,
    pub(crate) status: PublishedSandboxStatus,
    pub(crate) execution_generation: ExecutionGeneration,
    pub(crate) pod_uid: PodUid,
    pub(crate) artifact: Option<PublishedLocalArtifact>,
}

impl SuspendHandoffClaim {
    pub(super) fn from_observation(observation: &ValidatedSandboxObservation) -> Option<Self> {
        let identity = observation.identity();
        Some(Self {
            sandbox_id: observation.sandbox_id(),
            status: observation.status()?.clone(),
            execution_generation: identity.execution_generation()?,
            pod_uid: identity.pod_uid()?.clone(),
            artifact: identity.current_local_artifact().cloned(),
        })
    }
}
