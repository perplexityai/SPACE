use super::*;
use pplx_space_common_core::types::SandboxLifecycle;

#[test]
fn failure_episode_omits_bad_optional_identity_without_losing_other_evidence() {
    let sandbox_id = SandboxId::from_uuid(uuid::Uuid::from_u128(1));
    let attempt = "00000000-0000-0000-0000-000000000002";
    for (generation, pod, activation, expected) in [
        (Some(7), Some(" pod-a "), Some(attempt), (true, true, true)),
        (Some(0), Some(" pod-a "), Some(attempt), (false, true, true)),
        (Some(7), Some(" \t"), Some(attempt), (true, false, true)),
        (
            Some(7),
            Some(" pod-a "),
            Some("not-a-uuid"),
            (true, true, false),
        ),
        (None, None, None, (false, false, false)),
    ] {
        let state = PublishedSandboxState {
            status: Some(" FaIlEd ".to_owned()),
            execution_generation: generation,
            pod_uid: pod.map(str::to_owned),
            activation_attempt_id: activation.map(str::to_owned),
            ..Default::default()
        };
        let claim = FailureEpisodeClaim::from_proto(sandbox_id, &state, true);
        assert_eq!(
            (
                claim.execution_generation.is_some(),
                claim.pod_uid.is_some(),
                claim.activation_attempt_id.is_some(),
            ),
            expected,
        );
        assert_eq!(claim.sandbox_id, sandbox_id);
        assert!(claim.state_degraded);
        let status = claim.status.as_ref().unwrap();
        assert_eq!(status.lifecycle(), Some(SandboxLifecycle::Failed));
        assert_eq!(status.as_str(), "FaIlEd");
        if let Some(generation) = claim.execution_generation {
            assert_eq!(generation.get(), 7);
        }
        if let Some(pod_uid) = claim.pod_uid {
            assert_eq!(pod_uid.as_str(), "pod-a");
        }
    }
}

#[test]
fn unknown_status_remains_opaque_and_empty_status_remains_absent() {
    for raw in [None, Some(""), Some(" \t")] {
        assert!(PublishedSandboxStatus::from_proto(raw).is_none());
    }
    let status = PublishedSandboxStatus::from_proto(Some(" future-state ")).unwrap();
    assert_eq!(status.as_str(), "future-state");
    assert_eq!(status.lifecycle(), None);
}
