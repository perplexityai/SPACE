use super::*;

#[test]
fn encrypted_rootfs_rpc_error_preserves_status_code_and_metadata() {
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert("x-test-reason", "invalid-key".parse().unwrap());
    let error = EncryptedRootfsPrepareError::Rpc(tonic::Status::with_metadata(
        tonic::Code::FailedPrecondition,
        "federated service grant is required",
        metadata,
    ));

    let status = error.into_status("encrypted rootfs preparation failed".into());

    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        status.metadata().get("x-test-reason").unwrap(),
        "invalid-key"
    );
}

/// The degrade decision is the guard against booting the bare rootfs file
/// over a live composite: only failures that prove node-manager built
/// nothing (never reached, or answered — its unwind ran) may degrade. Every
/// transport-ambiguous code must fail the start, because the lost reply may
/// hide a prepare that succeeded node-side.
#[test]
fn degrade_is_allowed_only_when_no_partial_device_state_is_possible() {
    let rpc = |code: tonic::Code| PoolRootfsPrepareError::Rpc(tonic::Status::new(code, "x"));

    for definitive in [
        tonic::Code::InvalidArgument,
        tonic::Code::FailedPrecondition,
        tonic::Code::ResourceExhausted,
        tonic::Code::Unimplemented,
        tonic::Code::PermissionDenied,
        tonic::Code::Unauthenticated,
        tonic::Code::NotFound,
    ] {
        assert!(
            rpc(definitive).degrade_to_rootfs_file_is_safe(),
            "{definitive:?} is a definitive node answer; the file is safe"
        );
    }
    // Internal is a device failure whose unwind is best-effort (and a
    // panicked task has no unwind at all), so devices may survive it.
    for ambiguous in [
        tonic::Code::Internal,
        tonic::Code::Unavailable,
        tonic::Code::DeadlineExceeded,
        tonic::Code::Cancelled,
        tonic::Code::Unknown,
        tonic::Code::Aborted,
        tonic::Code::DataLoss,
    ] {
        assert!(
            !rpc(ambiguous).degrade_to_rootfs_file_is_safe(),
            "{ambiguous:?} may hide a node-side success; must not degrade"
        );
    }
}
