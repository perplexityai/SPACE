use pplx_space_common_core::types::{PolicyAuthority, SandboxId};
use pplx_space_net_common::{NetworkPolicyV1, proto::NetworkPolicySource};

use crate::node_publish::validate::sandbox_observation::ValidatedSandboxObservation;

#[derive(Debug)]
pub(crate) struct PolicyAuthorityClaim {
    pub(crate) sandbox_id: SandboxId,
    pub(crate) authority: Option<PolicyAuthority>,
}

#[derive(Debug)]
pub(crate) struct NetworkPolicyObservation {
    pub(crate) sandbox_id: SandboxId,
    pub(crate) generation: i64,
    pub(crate) policy: Option<NetworkPolicyV1>,
    pub(crate) source: NetworkPolicySource,
    pub(crate) authority: Option<PolicyAuthority>,
}

#[derive(Debug)]
pub(crate) struct ClearPolicyFailureClaim {
    pub(crate) sandbox_id: SandboxId,
}

#[derive(Debug, Default)]
pub(crate) struct NetworkPolicyPlan {
    pub(crate) authorities: Vec<PolicyAuthorityClaim>,
    pub(crate) observations: Vec<NetworkPolicyObservation>,
    pub(crate) clear_failures: Vec<ClearPolicyFailureClaim>,
}

impl NetworkPolicyPlan {
    /// Only a supplied generation permits policy projection or failure clearing.
    /// Assignment and ownership remain database guards, not node claims.
    pub(crate) fn observe(&mut self, observation: &mut ValidatedSandboxObservation) {
        let Some(generation) = observation.network_policy_generation else {
            return;
        };
        let sandbox_id = observation.sandbox_id();
        let authority = observation.network_policy_authority;
        self.authorities.push(PolicyAuthorityClaim {
            sandbox_id,
            authority,
        });
        self.observations.push(NetworkPolicyObservation {
            sandbox_id,
            generation,
            policy: observation.network_policy.take(),
            source: observation.network_policy_source,
            authority,
        });
        self.clear_failures
            .push(ClearPolicyFailureClaim { sandbox_id });
    }
}
