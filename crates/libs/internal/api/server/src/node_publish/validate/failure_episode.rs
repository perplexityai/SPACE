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
    sandbox_id: SandboxId,
    status: Option<PublishedSandboxStatus>,
    state_degraded: bool,
    execution_generation: Option<ExecutionGeneration>,
    pod_uid: Option<PodUid>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
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

    pub(crate) fn legacy_row(&self) -> serde_json::Value {
        serde_json::json!({
            "sandbox_id": self.sandbox_id,
            "status": self.status.as_ref().map(PublishedSandboxStatus::as_str),
            "state_degraded": self.state_degraded,
            "execution_generation": self.execution_generation.map(ExecutionGeneration::get),
            "pod_uid": self.pod_uid.as_ref().map(ToString::to_string),
            "activation_attempt_id": self.activation_attempt_id.as_ref().map(ToString::to_string),
        })
    }
}

#[cfg(test)]
mod tests;
