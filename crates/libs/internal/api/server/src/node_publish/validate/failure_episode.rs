use pplx_space_common_core::types::{
    ExecutionGeneration, PodUid, SandboxActivationAttemptId, SandboxId,
};

use super::sandbox_status::PublishedSandboxStatus;
use crate::metadata::optional_trimmed;
use crate::proto::PublishedSandboxState;

/// Optional identity evidence for the store's guarded failure-episode writes.
/// An invalid optional identity is omitted independently, never inferred from
/// another field and never promoted into authority over the database row.
#[derive(Debug)]
pub(crate) struct FailureEpisodeClaim {
    pub(in crate::node_publish) sandbox_id: SandboxId,
    pub(in crate::node_publish) status: Option<PublishedSandboxStatus>,
    pub(in crate::node_publish) state_degraded: bool,
    pub(in crate::node_publish) execution_generation: Option<ExecutionGeneration>,
    pub(in crate::node_publish) pod_uid: Option<PodUid>,
    pub(in crate::node_publish) activation_attempt_id: Option<SandboxActivationAttemptId>,
}

impl FailureEpisodeClaim {
    pub(crate) fn from_proto(
        sandbox_id: SandboxId,
        state: &PublishedSandboxState,
        state_degraded: bool,
    ) -> Self {
        Self {
            sandbox_id,
            status: PublishedSandboxStatus::from_proto(state.status.as_deref()),
            state_degraded,
            execution_generation: state
                .execution_generation
                .and_then(|generation| ExecutionGeneration::new(generation).ok()),
            pod_uid: optional_trimmed(state.pod_uid.as_deref())
                .and_then(|pod_uid| PodUid::parse(pod_uid).ok()),
            activation_attempt_id: optional_trimmed(state.activation_attempt_id.as_deref())
                .and_then(|attempt| attempt.parse::<SandboxActivationAttemptId>().ok()),
        }
    }
}

#[cfg(test)]
mod tests;
