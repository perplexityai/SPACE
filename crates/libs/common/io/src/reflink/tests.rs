#[test]
fn extent_copy_preserves_surrounding_bytes_and_rejects_out_of_bounds_ranges() {
    use std::io::ErrorKind;
    use std::os::unix::fs::FileExt;

    let source = tempfile::tempfile().unwrap();
    let destination = tempfile::tempfile().unwrap();
    let source_bytes = b"header:chunk-content:trailer";
    source.write_all_at(source_bytes, 0).unwrap();
    destination.write_all_at(&[0xaa; 32], 0).unwrap();

    // Unaligned extents exercise the byte-copy fallback even on btrfs.
    assert!(!super::copy_range_on_descriptors(&source, 7, &destination, 9, 13).unwrap());
    let mut expected = [0xaa; 32];
    expected[9..22].copy_from_slice(b"chunk-content");
    let mut observed = [0; 32];
    destination.read_exact_at(&mut observed, 0).unwrap();
    assert_eq!(observed, expected);

    for (source_offset, destination_offset, length) in [
        (0, 0, 0),
        (u64::MAX, 0, 1),
        (0, u64::MAX, 1),
        (25, 0, 8),
        (0, 31, 2),
    ] {
        let error = super::copy_range_on_descriptors(
            &source,
            source_offset,
            &destination,
            destination_offset,
            length,
        )
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        destination.read_exact_at(&mut observed, 0).unwrap();
        assert_eq!(observed, expected);
    }
    let mut original = vec![0; source_bytes.len()];
    source.read_exact_at(&mut original, 0).unwrap();
    assert_eq!(&original, source_bytes);
}

#[cfg(target_os = "linux")]
#[test]
fn nocow_preparation_clones_shared_extents_and_rejects_aliases() {
    use super::*;
    use rustix::fs::{IFlags, fstatfs, ioctl_getflags, ioctl_setflags};
    use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt};

    let Some(root) = std::env::var_os("SPACE_NOCOW_TEST_BTRFS_DIR") else {
        assert!(
            std::env::var_os("CI").is_none()
                && std::env::var_os("SPACE_CI_REQUIRE_TEST_DEPS").is_none(),
            "CI must provide the disposable NoCoW btrfs fixture"
        );
        eprintln!("not opted in: set SPACE_NOCOW_TEST_BTRFS_DIR to a disposable btrfs directory");
        return;
    };
    let temporary = tempfile::tempdir_in(root).unwrap();
    let root = temporary.path();
    let src = root.join("source");
    let source = File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&src)
        .unwrap();
    assert_eq!(fstatfs(&source).unwrap().f_type, libc::BTRFS_SUPER_MAGIC);
    ioctl_setflags(&source, IFlags::NOCOW).unwrap();
    source.write_all_at(&[7; 8192], 0).unwrap();
    for (name, flags) in [
        ("cow", IFlags::empty()),
        ("nocow", IFlags::NOCOW),
        ("compressed", IFlags::COMPRESSED),
        ("uncompressed", IFlags::from_bits_retain(0x400)),
    ] {
        let dst = root.join(name);
        let destination = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&dst)
            .unwrap();
        ioctl_setflags(&destination, flags).unwrap();
        destination.write_all_at(b"old output", 0).unwrap();
        // The descriptor helper must never convert an existing populated file.
        assert!(preserve_nocow(&source, &destination).is_err());
        assert!(copy_file(&src, &dst).unwrap());
        let destination = File::options().read(true).write(true).open(&dst).unwrap();
        assert!(
            ioctl_getflags(&destination)
                .unwrap()
                .contains(IFlags::NOCOW)
        );
        // copy_file returned true only after a successful kernel reflink.
        destination.write_all_at(b"changed", 0).unwrap();
        assert_eq!(std::fs::read(&src).unwrap(), vec![7; 8192]);
    }
    copy_file(&src, &src).unwrap_err();
    let alias = root.join("alias");
    std::fs::hard_link(&src, &alias).unwrap();
    copy_file(&src, &alias).unwrap_err();
    std::fs::remove_file(&alias).unwrap();
    std::os::unix::fs::symlink(&src, &alias).unwrap();
    copy_file(&src, &alias).unwrap_err();
    assert_eq!(std::fs::read(&src).unwrap(), vec![7; 8192]);
    std::fs::set_permissions(&src, std::fs::Permissions::from_mode(0o444)).unwrap();
    let readonly = root.join("readonly-copy");
    assert!(copy_file(&src, &readonly).unwrap());
    assert_eq!(
        std::fs::metadata(&readonly).unwrap().permissions().mode() & 0o777,
        0o444
    );
    assert_eq!(std::fs::read(&readonly).unwrap(), vec![7; 8192]);

    let other_fs = tempfile::tempdir().unwrap();
    if other_fs.path().metadata().unwrap().dev() != source.metadata().unwrap().dev() {
        let destination = other_fs.path().join("previous-output");
        std::fs::write(&destination, b"previous").unwrap();
        reflink_file(&src, &destination).unwrap_err();
        assert_eq!(std::fs::read(&destination).unwrap(), b"previous");
        assert_eq!(std::fs::read_dir(other_fs.path()).unwrap().count(), 1);
        assert!(!copy_file(&src, &destination).unwrap());
        assert_eq!(std::fs::read(&destination).unwrap(), vec![7; 8192]);
    }
}
