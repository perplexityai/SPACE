use super::*;

#[test]
fn issued_dek_plaintext_field_is_zeroizing() {
    // Compile-time pin on the type contract: the plaintext DEK field
    // must be `Zeroizing<Vec<u8>>` so its bytes are scrubbed from
    // heap on drop. A plain `Vec<u8>` would just release the
    // allocation back to the allocator without overwriting it,
    // leaving fresh key material visible to any process-memory
    // disclosure on the host. This test fails to compile if the
    // field type drifts back to a non-zeroizing container.
    fn assert_zeroizing(_: &Zeroizing<Vec<u8>>) {}
    let dek = IssuedDek {
        plaintext_dek: Zeroizing::new(vec![0u8; 32]),
        wrapped_dek: vec![1u8; 64],
    };
    assert_zeroizing(&dek.plaintext_dek);
}

#[test]
fn upload_scope_includes_read_for_durable_head_verification() {
    assert_eq!(StorageAccess::Read.scope(), STORAGE_READ_SCOPE);
    assert_eq!(
        StorageAccess::ReadWrite.scope(),
        format!("{STORAGE_READ_SCOPE} {STORAGE_WRITE_SCOPE}")
    );
}
