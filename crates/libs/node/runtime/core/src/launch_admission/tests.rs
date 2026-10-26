use super::*;

#[test]
fn only_tagged_admission_rejections_change_the_existing_error_contract() {
    use tonic::Code;
    for (error, expected) in [
        (
            Status::failed_precondition("store unavailable"),
            Code::Internal,
        ),
        (Status::unavailable("connection dropped"), Code::Internal),
        (
            Status::invalid_argument("legacy request refused"),
            Code::Internal,
        ),
        (Status::internal("spawn failed"), Code::Internal),
        (
            admission_refusal(Status::failed_precondition("source mismatch")),
            Code::FailedPrecondition,
        ),
        (
            admission_refusal(Status::invalid_argument("invalid protected layout")),
            Code::InvalidArgument,
        ),
        (
            admission_refusal(Status::internal("unexpected failure")),
            Code::Internal,
        ),
    ] {
        assert_eq!(supervisor_launch_status(error).code(), expected);
    }
}

#[test]
fn implemented_runtime_errors_never_authorize_legacy_replay() {
    use tonic::Code;
    for code in [
        Code::Unimplemented,
        Code::InvalidArgument,
        Code::FailedPrecondition,
        Code::Unavailable,
        Code::DeadlineExceeded,
        Code::Internal,
        Code::Cancelled,
    ] {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("test-evidence", "preserved".parse().unwrap());
        let status = runtime_operation_status(Status::with_details_and_metadata(
            code,
            "operation failed",
            b"opaque-details".as_slice().into(),
            metadata,
        ));
        assert_eq!(status.details(), b"opaque-details");
        assert_eq!(status.metadata().get("test-evidence").unwrap(), "preserved");
        assert_eq!(status.message(), "operation failed");
        assert_eq!(
            status.code(),
            if code == Code::Unimplemented {
                Code::FailedPrecondition
            } else {
                code
            },
        );
    }
}
