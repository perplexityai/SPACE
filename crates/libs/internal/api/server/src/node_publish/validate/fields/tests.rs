use super::*;
use tonic::Code;

fn assert_invalid(status: Status, message: &str) {
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), message);
}

#[test]
fn numeric_fields_keep_the_existing_overflow_contract() {
    assert_eq!(
        u64_to_i64(u64::try_from(i64::MAX).unwrap(), "bytes").unwrap(),
        i64::MAX
    );
    assert_eq!(
        u32_to_i32(u32::try_from(i32::MAX).unwrap(), "pid").unwrap(),
        i32::MAX
    );
    assert_invalid(
        u64_to_i64(u64::MAX, "bytes").unwrap_err(),
        "bytes exceeds i64::MAX: number too large to fit in target type",
    );
    assert_invalid(
        u32_to_i32(u32::MAX, "pid").unwrap_err(),
        "pid exceeds i32::MAX: number too large to fit in target type",
    );
    assert_eq!(non_zero_u64_to_i64(0, "generation").unwrap(), None);
    assert_eq!(non_zero_u64_to_i64(1, "generation").unwrap(), Some(1));
    non_zero_u64_to_i64(u64::MAX, "generation").unwrap_err();
    assert_eq!(
        optional_u64_to_i64_when(false, Some(u64::MAX), "bytes").unwrap(),
        None
    );
    assert_eq!(
        optional_u32_to_i32_when(false, Some(u32::MAX), "pid").unwrap(),
        None
    );
    optional_u64_to_i64_when(true, Some(u64::MAX), "bytes").unwrap_err();
    optional_u32_to_i32_when(true, Some(u32::MAX), "pid").unwrap_err();
}

#[test]
fn timestamps_preserve_precision_and_reject_invalid_wire_values() {
    let valid = prost_types::Timestamp {
        seconds: 0,
        nanos: 123_456_789,
    };
    assert_eq!(
        proto_timestamp_to_rfc3339(&valid, "activity").unwrap(),
        "1970-01-01T00:00:00.123456789Z"
    );
    for nanos in [-1, 1_000_000_000] {
        assert_invalid(
            proto_timestamp(&prost_types::Timestamp { nanos, ..valid }, "activity").unwrap_err(),
            "activity has invalid nanos",
        );
    }
    let out_of_range = proto_timestamp(
        &prost_types::Timestamp {
            seconds: i64::MAX,
            nanos: 0,
        },
        "activity",
    )
    .unwrap_err();
    assert_eq!(out_of_range.code(), Code::InvalidArgument);
    assert!(
        out_of_range
            .message()
            .starts_with("activity is out of range: ")
    );
}

#[test]
fn execution_omission_and_invalid_identity_remain_distinct() {
    assert!(execution_projection(None).unwrap().is_none());
    assert_invalid(
        snapshot_execution_metadata(None).unwrap_err(),
        "execution metadata is required",
    );
    let empty = pplx_space_execution_common::proto::ExecutionMetadata::default();
    let invalid = execution_projection(Some(&empty)).err().unwrap();
    assert_invalid(invalid, "execution metadata is missing runtime identity");
    assert_invalid(
        snapshot_execution_metadata(Some(&empty)).unwrap_err(),
        "execution metadata is missing runtime identity",
    );
}

#[test]
fn artifact_origins_preserve_rejection_of_unknown_wire_values() {
    for unsupported in [0, i32::MAX] {
        assert_invalid(
            published_artifact_origin(unsupported).unwrap_err(),
            "current_local_artifact_origin must be STOP, SUSPEND, SNAPSHOT, or FORK",
        );
    }
}
