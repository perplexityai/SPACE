use super::*;
use pplx_space_sbx_mgr_client::proto::QuarantineRecoveryObservation;
use tonic::Code;

fn state(observation: QuarantineRecoveryObservation) -> PublishedSandboxState {
    PublishedSandboxState {
        quarantine_recovery: Some(observation),
        ..Default::default()
    }
}

fn valid_observation() -> QuarantineRecoveryObservation {
    QuarantineRecoveryObservation {
        schema_version: 1,
        local_quarantine_id: "01990000-0000-7000-8000-000000000001".into(),
        observed_owner: QuarantineRecoveryObservedOwner::LegacyRecovery as i32,
        attempt_count: 3,
        ..Default::default()
    }
}

#[test]
fn absence_and_collection_degradation_are_not_invalid_observations() {
    assert!(matches!(
        quarantine_recovery_projection(&PublishedSandboxState::default()).unwrap(),
        QuarantineRecoveryProjectionState::Absent
    ));
    assert!(matches!(
        quarantine_recovery_projection(&state(QuarantineRecoveryObservation {
            degraded: true,
            ..Default::default()
        }))
        .unwrap(),
        QuarantineRecoveryProjectionState::Degraded
    ));
}

#[test]
fn invalid_recovery_fields_keep_their_existing_rejections() {
    for (observation, message) in [
        (
            QuarantineRecoveryObservation {
                schema_version: 0,
                ..valid_observation()
            },
            "unsupported quarantine recovery observation schema version 0",
        ),
        (
            QuarantineRecoveryObservation {
                local_quarantine_id: " ".into(),
                ..valid_observation()
            },
            "local_quarantine_id is required",
        ),
        (
            QuarantineRecoveryObservation {
                next_attempt_at: Some(prost_types::Timestamp {
                    seconds: -1,
                    nanos: 0,
                }),
                ..valid_observation()
            },
            "quarantine_next_attempt_at is negative",
        ),
    ] {
        let error = quarantine_recovery_projection(&state(observation))
            .err()
            .unwrap();
        assert_eq!(error.code(), Code::InvalidArgument);
        assert_eq!(error.message(), message);
    }
    let error = quarantine_recovery_projection(&state(QuarantineRecoveryObservation {
        local_quarantine_id: "invalid".into(),
        ..valid_observation()
    }))
    .err()
    .unwrap();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().starts_with("invalid local_quarantine_id: "));
}
