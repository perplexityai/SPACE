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
