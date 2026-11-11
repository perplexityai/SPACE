use super::*;

#[test]
fn optional_reason_rejects_empty_string() {
    let err = validate_optional_reason(Some(String::new())).unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[test]
fn optional_reason_passes_through_none_and_nonempty() {
    assert_eq!(validate_optional_reason(None).unwrap(), None);
    assert_eq!(
        validate_optional_reason(Some("boom".into())).unwrap(),
        Some("boom".into())
    );
}

#[test]
fn timeout_requires_presence() {
    let err = validate_timeout(None).unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[test]
fn timeout_rejects_negative_and_zero() {
    assert_eq!(
        validate_timeout(Some(prost_types::Duration {
            seconds: -1,
            nanos: 0,
        }))
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        validate_timeout(Some(prost_types::Duration {
            seconds: 0,
            nanos: 0,
        }))
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn timeout_accepts_positive_duration() {
    let d = validate_timeout(Some(prost_types::Duration {
        seconds: 1,
        nanos: 500_000_000,
    }))
    .unwrap();
    assert_eq!(d.as_duration(), Duration::from_millis(1_500));
}

#[test]
fn timeout_rejects_values_that_overflow_the_runtime_clock() {
    let error = validate_timeout(Some(prost_types::Duration {
        seconds: i64::MAX,
        nanos: 0,
    }))
    .expect_err("unrepresentable timeout should be rejected");

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}

#[test]
fn optional_duration_rejects_zero() {
    let error = validate_optional_duration(
        "hold_for",
        Some(prost_types::Duration {
            seconds: 0,
            nanos: 0,
        }),
    )
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(error.message(), "hold_for must be greater than 0");
}

#[test]
fn optional_duration_accepts_positive_duration() {
    let duration = validate_optional_duration(
        "hold_for",
        Some(prost_types::Duration {
            seconds: 30,
            nanos: 0,
        }),
    )
    .unwrap()
    .expect("duration present");
    assert_eq!(duration, Duration::from_secs(30));
}

#[test]
fn duration_requires_presence() {
    let error = validate_duration("hold_for", None).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(error.message(), "hold_for is required");
}
