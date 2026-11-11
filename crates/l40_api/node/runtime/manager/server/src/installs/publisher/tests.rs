use super::*;

#[test]
fn an_artifact_travels_by_what_it_is() {
    let release = node_artifact(&NodeArtifactRef::Release(
        RuntimeIdentity {
            kind: pplx_space_common_core::types::RuntimeKind::Firecracker,
            version: pplx_space_common_core::types::RuntimeVersion::parse("1.16.2").unwrap(),
        },
        Digest::sha256([7; 32]),
    ));
    assert!(matches!(
        release.artifact,
        Some(node::node_artifact::Artifact::RuntimeRelease(ref release))
            if release.version == "1.16.2"
                && release.runtime_kind
                    == pplx_space_execution_common::proto::RuntimeKind::Firecracker as i32
    ));
    let kernel = node_artifact(&NodeArtifactRef::Kernel(
        "6.1.158".parse().unwrap(),
        Digest::sha256([7; 32]),
    ));
    assert!(matches!(
        kernel.artifact,
        Some(node::node_artifact::Artifact::Component(ref kernel)) if kernel.version == "6.1.158"
    ));
}

#[test]
fn only_an_answer_node_manager_would_repeat_is_a_refusal() {
    for code in [
        tonic::Code::InvalidArgument,
        tonic::Code::FailedPrecondition,
        tonic::Code::PermissionDenied,
    ] {
        assert!(
            classify(tonic::Status::new(code, "no")).is_permanent(),
            "{code:?}"
        );
    }
    for code in [
        tonic::Code::Unavailable,
        tonic::Code::Internal,
        tonic::Code::DeadlineExceeded,
        // A busy store, and an older node-manager the rollout replaces.
        tonic::Code::ResourceExhausted,
        tonic::Code::Unimplemented,
    ] {
        assert!(
            !classify(tonic::Status::new(code, "later")).is_permanent(),
            "{code:?}"
        );
    }
}
