use super::*;

#[test]
fn missing_kubernetes_pod_uid_is_rejected_for_signing() {
    let parts = SandboxRouteIdentityParts {
        sandbox_id: SandboxId::new(),
        route_generation: RouteGeneration::new(7).unwrap(),
        current_node_id: NodeId::new(),
        substrate_kind: Some(NodeManagedBy::Kubernetes),
        execution_generation: Some(ExecutionGeneration::new(11).unwrap()),
        activation_attempt_id: Some(SandboxActivationAttemptId::new()),
        pod_uid: None,
    };

    let error = route_identity_for_signing(&parts).unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
}
