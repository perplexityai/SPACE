use super::*;

#[test]
fn rootfs_entitlement_rejects_zero_and_over_limit_without_rounding() {
    for bytes in [1, 1024 * 1024, MAX_SANDBOX_ROOTFS_BYTES] {
        assert_eq!(validate_rootfs_bytes(bytes).unwrap().as_bytes(), bytes);
    }
    assert_eq!(validate_rootfs_bytes(0), Err(RootfsSizeError::Zero));
    for bytes in [MAX_SANDBOX_ROOTFS_BYTES + 1, u64::MAX] {
        assert!(matches!(
            validate_rootfs_bytes(bytes),
            Err(RootfsSizeError::TooLarge { .. })
        ));
    }
    assert_eq!(
        validate_rootfs_mib(128 * 1024).unwrap().as_bytes(),
        128 * 1024 * 1024 * 1024
    );
    validate_rootfs_mib(128 * 1024 + 1).unwrap_err();
    validate_rootfs_mib(u32::MAX).unwrap_err();
}
