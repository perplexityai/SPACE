use pplx_space_common_svc::core::types::{K8sNodeUid, NodeRole};

use super::*;
use crate::auth::{ClientService, K8sIdentity};
use crate::credential_injection_bundle::BundleMaterializationError;
use crate::credential_provider::ResolutionError;

#[test]
fn node_agent_identity_maps_to_the_worker_node_incarnation() {
    let caller = ResolvedCaller::K8sJwt(
        K8sIdentity::from_token_review(
            ClientService::NodeCredentialAgent,
            "credential-agent-a",
            "credential-agent-a-uid",
            "worker-a",
            "worker-a-uid",
        )
        .unwrap(),
    );
    let expected_node_id =
        pplx_space_common_svc::types::k8s::space_k8s_node_id("worker-a", NodeRole::Worker.as_str());

    assert_eq!(
        resolution_caller(&caller).unwrap(),
        ResolutionCaller::new(expected_node_id, K8sNodeUid::parse("worker-a-uid").unwrap())
    );
}

#[test]
fn non_agent_kubernetes_identity_cannot_resolve_for_a_node() {
    let caller = ResolvedCaller::K8sJwt(
        K8sIdentity::from_token_review(
            ClientService::SandboxManager,
            "sandbox-manager-a",
            "sandbox-manager-a-uid",
            "worker-a",
            "worker-a-uid",
        )
        .unwrap(),
    );

    assert_eq!(
        resolution_caller(&caller).unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
}

#[test]
fn domain_failures_keep_the_security_and_retry_status_contract() {
    for (error, expected) in [
        (
            BundleResolutionError::CandidateUnavailableOrUnauthorized,
            tonic::Code::PermissionDenied,
        ),
        (BundleResolutionError::StaleCandidate, tonic::Code::Aborted),
        (
            BundleResolutionError::Materialize(BundleMaterializationError::InvalidRules(
                "malformed",
            )),
            tonic::Code::Internal,
        ),
        (
            BundleResolutionError::Materialize(BundleMaterializationError::Provider(
                ResolutionError::ProviderUnavailable,
            )),
            tonic::Code::Unavailable,
        ),
    ] {
        assert_eq!(bundle_resolution_error_to_status(error).code(), expected);
    }
}
