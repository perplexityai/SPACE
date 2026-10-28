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
    assert_eq!(
        observation.status().map(PublishedSandboxStatus::as_str),
        Some("StArTeD")
    );
    assert_eq!(
        observation
            .runtime
            .as_ref()
            .map(PublishedRuntimeLabel::as_str),
        Some("future-runtime")
    );
    assert_eq!(observation.owner.as_deref(), Some("sample-owner"));
    assert_eq!(observation.network_tx_bytes, Some(0));
    assert_eq!(observation.network_rx_bytes, None);
    assert_eq!(
        observation
            .last_access_at()
            .map(PublishedActivityTimestamp::as_str),
        Some("1970-01-01T00:00:00.000000007Z")
    );
    assert_eq!(observation.spaced_version, None);
}

#[test]
fn observation_validation_preserves_optional_field_rejection_precedence() {
    let sandbox_id = SandboxId::from_uuid(uuid::Uuid::from_u128(1));
    let mut state = PublishedSandboxState {
        status: Some("started".to_owned()),
        // Valid for the time type, but not representable as an RFC3339 year.
        state_changed_at: Some(prost_types::Timestamp {
            seconds: -62_167_219_201,
            nanos: 0,
        }),
        pid: Some(u32::MAX),
        last_used_at: Some(prost_types::Timestamp {
            seconds: 0,
            nanos: -1,
        }),
        network_tx_bytes: Some(u64::MAX),
        ..Default::default()
    };
    for (step, (code, expected)) in [
        (
            tonic::Code::InvalidArgument,
            "pid exceeds i32::MAX: number too large to fit in target type",
        ),
        (tonic::Code::InvalidArgument, "last_used_at has invalid nanos"),
        (
            tonic::Code::InvalidArgument,
            "network_tx_bytes exceeds i64::MAX: number too large to fit in target type",
        ),
        (
            tonic::Code::Internal,
            "format state_changed_at: The year component cannot be formatted into the requested format.",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let error = ValidatedSandboxObservation::parse(sandbox_id, &state, None)
            .observation
            .unwrap_err();
        assert_eq!(error.code(), code);
        assert_eq!(error.message(), expected);
        match step {
            0 => state.pid = None,
            1 => state.last_used_at = None,
            2 => state.network_tx_bytes = None,
            _ => {}
        }
    }
}
