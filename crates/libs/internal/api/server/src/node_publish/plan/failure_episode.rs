use pplx_space_common_core::types::{
    ExecutionGeneration, PodUid, SandboxActivationAttemptId, SandboxId, SandboxLifecycle,
};

use crate::node_publish::validate::failure_episode::FailureEpisodeClaim;

/// A non-degraded failed report supplies an execution identity, not ownership.
#[derive(Debug)]
pub(crate) struct FailureEpisodeObservation {
    pub(crate) sandbox_id: SandboxId,
    pub(crate) execution_generation: ExecutionGeneration,
    pub(crate) pod_uid: Option<PodUid>,
    pub(crate) activation_attempt_id: Option<SandboxActivationAttemptId>,
}

/// A report omits usable failure evidence; only its current owner may clear it.
#[derive(Debug)]
pub(crate) struct ClearFailureEpisodeClaim {
    pub(crate) sandbox_id: SandboxId,
}

#[derive(Debug, Default)]
pub(crate) struct FailureEpisodePlan {
    pub(crate) observations: Vec<FailureEpisodeObservation>,
    pub(crate) clears: Vec<ClearFailureEpisodeClaim>,
}

impl From<Vec<FailureEpisodeClaim>> for FailureEpisodePlan {
    fn from(claims: Vec<FailureEpisodeClaim>) -> Self {
        let mut plan = Self::default();
        for claim in claims {
            let failed = claim.status.as_ref().and_then(|status| status.lifecycle())
                == Some(SandboxLifecycle::Failed);
            if !failed || (!claim.state_degraded && claim.execution_generation.is_none()) {
                plan.clears.push(ClearFailureEpisodeClaim {
                    sandbox_id: claim.sandbox_id,
                });
            } else if !claim.state_degraded
                && let Some(execution_generation) = claim.execution_generation
            {
                plan.observations.push(FailureEpisodeObservation {
                    sandbox_id: claim.sandbox_id,
                    execution_generation,
                    pod_uid: claim.pod_uid,
                    activation_attempt_id: claim.activation_attempt_id,
                });
            }
        }
        plan
    }
}
