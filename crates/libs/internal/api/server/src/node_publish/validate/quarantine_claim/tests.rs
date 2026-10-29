use super::*;

#[test]
fn quarantine_claim_preserves_status_and_reason_provenance() {
    let sandbox_id = SandboxId::from_uuid(uuid::Uuid::from_u128(1));
    for (status, reason, expected) in [
        (None, None, None),
        (Some(" \t"), None, None),
        (Some("started"), Some("pause_failed"), None),
        (Some("future-state"), Some("pause_failed"), None),
        (
            Some(" QuArAnTiNeD "),
            None,
            Some((
                SandboxQuarantineSource::NodeStatePublish,
                SandboxQuarantineReasonCode::NodePublishedQuarantined,
            )),
        ),
        (
            Some("quarantined"),
            Some(" \t"),
            Some((
                SandboxQuarantineSource::NodeStatePublish,
                SandboxQuarantineReasonCode::NodePublishedQuarantined,
            )),
        ),
        (
            Some("quarantined"),
            Some(" pause_failed "),
            Some((
                SandboxQuarantineSource::SbxManager,
                SandboxQuarantineReasonCode::PauseFailed,
            )),
        ),
        (
            Some("quarantined"),
            Some("reason_added_by_newer_node"),
            Some((
                SandboxQuarantineSource::NodeStatePublish,
                SandboxQuarantineReasonCode::NodePublishedQuarantined,
            )),
        ),
    ] {
        let mut state = PublishedSandboxState {
            present: true,
            status: status.map(str::to_owned),
            quarantine_reason_code: reason.map(str::to_owned),
            failure_reason: Some(" retained local evidence ".to_owned()),
            ..Default::default()
        };
        let claim = QuarantineClaim::from_proto(sandbox_id, &state);
        assert_eq!(
            claim
                .as_ref()
                .map(|claim| (claim.source(), claim.reason_code())),
            expected,
        );
        if let Some(claim) = claim {
            assert_eq!(claim.sandbox_id(), sandbox_id);
            assert_eq!(claim.failure_reason(), Some("retained local evidence"));
        }
        state.state_degraded = true;
        assert!(QuarantineClaim::from_proto(sandbox_id, &state).is_none());
        state.state_degraded = false;
        state.present = false;
        assert!(QuarantineClaim::from_proto(sandbox_id, &state).is_none());
    }
}
