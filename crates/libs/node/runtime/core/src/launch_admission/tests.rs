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
