use pplx_space_common_core::types::{
    ExecutionGeneration, NodeId, NodeManagedBy, PodUid, RouteGeneration,
    SandboxActivationAttemptId, SandboxId, SandboxRouteIdentity,
    SandboxRouteIdentityInvariantError,
};
use tonic::Status;

pub(crate) struct SandboxRouteIdentityParts {
    pub sandbox_id: SandboxId,
    pub route_generation: RouteGeneration,
    pub current_node_id: NodeId,
    pub substrate_kind: Option<NodeManagedBy>,
    pub execution_generation: Option<ExecutionGeneration>,
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub pod_uid: Option<PodUid>,
}

/// Assemble the complete identity required by asymmetric route capabilities.
///
pub(crate) fn route_identity_for_signing(
    parts: &SandboxRouteIdentityParts,
) -> Result<SandboxRouteIdentity, Status> {
    let (Some(substrate_kind), Some(execution_generation), Some(activation_attempt_id)) = (
        parts.substrate_kind,
        parts.execution_generation,
        parts.activation_attempt_id,
    ) else {
        return Err(Status::failed_precondition(
            "route capability requires complete authoritative route identity",
        ));
    };
    SandboxRouteIdentity::try_new(
        parts.sandbox_id,
        parts.route_generation,
        parts.current_node_id,
        substrate_kind,
        execution_generation,
        activation_attempt_id,
        parts.pod_uid.clone(),
    )
    .map_err(|error| match error {
        SandboxRouteIdentityInvariantError::MissingPodUid => {
            Status::failed_precondition("Kubernetes route capability requires a pod UID")
        }
        error => Status::failed_precondition(error.to_string()),
    })
}

#[cfg(test)]
mod tests;
