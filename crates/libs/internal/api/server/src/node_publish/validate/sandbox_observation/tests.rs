use super::*;

#[test]
fn invalid_optional_version_is_omitted_without_changing_valid_observation_fields() {
    let sandbox_id = SandboxId::from_uuid(uuid::Uuid::from_u128(1));
    let state = PublishedSandboxState {
        status: Some(" StArTeD ".to_owned()),
        runtime: Some(" future-runtime ".to_owned()),
        owner: Some(" sample-owner ".to_owned()),
        spaced_version: Some("not-semver".to_owned()),
        network_tx_bytes: Some(0),
        last_access_at: Some(prost_types::Timestamp {
            seconds: 0,
            nanos: 7,
        }),
        ..Default::default()
    };
    let observation = ValidatedSandboxObservation::parse(sandbox_id, &state, None)
        .observation
        .unwrap();
    let row = observation.legacy_row(None, None, false).unwrap();
    assert_eq!(row["status"], "StArTeD");
    assert_eq!(row["runtime"], "future-runtime");
    assert_eq!(row["sandbox_owner"], "sample-owner");
    assert_eq!(row["sandbox_user"], "sample-owner");
    assert_eq!(row["network_tx_bytes"], 0);
    assert_eq!(row["network_rx_bytes"], serde_json::Value::Null);
    assert_eq!(row["last_access_at"], "1970-01-01T00:00:00.000000007Z");
    assert_eq!(row["spaced_version"], serde_json::Value::Null);
}

#[test]
fn observation_validation_preserves_optional_field_rejection_precedence() {
    let sandbox_id = SandboxId::from_uuid(uuid::Uuid::from_u128(1));
    let mut state = PublishedSandboxState {
        pid: Some(u32::MAX),
        last_used_at: Some(prost_types::Timestamp {
            seconds: 0,
            nanos: -1,
        }),
        network_tx_bytes: Some(u64::MAX),
        ..Default::default()
    };
    for (step, expected) in [
        "pid exceeds i32::MAX: number too large to fit in target type",
        "last_used_at has invalid nanos",
        "network_tx_bytes exceeds i64::MAX: number too large to fit in target type",
    ]
    .into_iter()
    .enumerate()
    {
        let error = ValidatedSandboxObservation::parse(sandbox_id, &state, None)
            .observation
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(error.message(), expected);
        match step {
            0 => state.pid = None,
            1 => state.last_used_at = None,
            _ => {}
        }
    }
}
