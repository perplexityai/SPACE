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

const FILES: [DiskFile; 3] = [DiskFile::Absent, DiskFile::Empty, DiskFile::Present];

#[test]
fn a_rootfs_disk_needs_a_non_empty_rootfs_whatever_the_cow() {
    for cow in FILES {
        assert_eq!(
            restorable_disk(DiskForm::Rootfs, DiskFile::Present, cow),
            Ok(())
        );
        assert_eq!(
            restorable_disk(DiskForm::Rootfs, DiskFile::Empty, cow),
            Err(IncompleteDisk::EmptyRootfs)
        );
        assert_eq!(
            restorable_disk(DiskForm::Rootfs, DiskFile::Absent, cow),
            Err(IncompleteDisk::NoRootfs)
        );
    }
}

#[test]
fn a_cow_only_disk_needs_a_non_empty_cow_and_no_empty_rootfs() {
    let form = DiskForm::RootfsOrEncryptedCow;
    assert_eq!(
        restorable_disk(form, DiskFile::Absent, DiskFile::Present),
        Ok(())
    );
    for cow in [DiskFile::Absent, DiskFile::Empty] {
        assert_eq!(
            restorable_disk(form, DiskFile::Absent, cow),
            Err(IncompleteDisk::NoRootfs)
        );
    }
    for cow in FILES {
        assert_eq!(
            restorable_disk(form, DiskFile::Empty, cow),
            Err(IncompleteDisk::EmptyRootfs)
        );
        assert_eq!(restorable_disk(form, DiskFile::Present, cow), Ok(()));
    }
}

#[test]
fn a_bootable_encrypted_disk_has_its_cow_and_a_named_origin() {
    for rootfs in FILES {
        for origin_named in [false, true] {
            assert_eq!(
                bootable_encrypted_disk(rootfs, DiskFile::Absent, origin_named),
                Err(IncompleteDisk::NoCow)
            );
            assert_eq!(
                bootable_encrypted_disk(rootfs, DiskFile::Empty, origin_named),
                Err(IncompleteDisk::EmptyCow)
            );
        }
    }
    assert_eq!(
        bootable_encrypted_disk(DiskFile::Absent, DiskFile::Present, false),
        Err(IncompleteDisk::NoOrigin)
    );
    assert_eq!(
        bootable_encrypted_disk(DiskFile::Absent, DiskFile::Present, true),
        Ok(())
    );
    assert_eq!(
        bootable_encrypted_disk(DiskFile::Empty, DiskFile::Present, true),
        Err(IncompleteDisk::EmptyRootfs)
    );
    for origin_named in [false, true] {
        assert_eq!(
            bootable_encrypted_disk(DiskFile::Present, DiskFile::Present, origin_named),
            Ok(())
        );
    }
}

/// Admission never admits a disk its rollback refuses.
#[test]
fn every_bootable_encrypted_disk_is_restorable() {
    for rootfs in FILES {
        for cow in FILES {
            for origin_named in [false, true] {
                if bootable_encrypted_disk(rootfs, cow, origin_named).is_ok() {
                    assert_eq!(
                        restorable_disk(DiskForm::RootfsOrEncryptedCow, rootfs, cow),
                        Ok(()),
                        "{rootfs:?} {cow:?} {origin_named}"
                    );
                }
            }
        }
    }
}
