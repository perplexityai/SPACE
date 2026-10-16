use pplx_space_common_core::types::{
    ExecutionGeneration, NodeManagedBy, PodUid, SandboxActivationAttemptId, SpaceApiRouteIdentity,
};

use super::*;

#[test]
fn route_evidence_uses_the_effective_kubernetes_substrate() {
    let activation_attempt_id = SandboxActivationAttemptId::new();
    let identity = SpaceApiRouteIdentity {
        execution_generation: Some(ExecutionGeneration::new(12).unwrap()),
        pod_uid: Some(PodUid::parse("pod-uid-12").unwrap()),
        activation_attempt_id: Some(activation_attempt_id),
        ..SpaceApiRouteIdentity::default()
    };

    assert_eq!(
        RouteEvidence::from_identity(&identity, RouteIdentityProvenance::ClusterFenced),
        Some(RouteEvidence {
            execution_generation: ExecutionGeneration::new(12).unwrap(),
            activation_attempt_id,
            substrate: NodeManagedBy::Kubernetes,
            provenance: RouteIdentityProvenance::ClusterFenced,
        })
    );
}
