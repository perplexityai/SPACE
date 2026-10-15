use super::*;

#[test]
fn rejects_unknown_backing_policy() {
    for (raw, expected) in [
        ("inherit", Some(RootfsBackingPolicy::Inherit)),
        ("nocow", Some(RootfsBackingPolicy::NoCow)),
        ("", None),
        ("no-cow", None),
        ("NOCOW", None),
    ] {
        assert_eq!(raw.parse::<RootfsBackingPolicy>().ok(), expected);
    }
}

#[test]
fn explicit_nocow_rejects_an_unsupported_cache_filesystem() {
    let directory = tempfile::tempdir().unwrap();
    RootfsBackingPolicy::Inherit
        .validate_cache(&directory.path().join("not-created"))
        .unwrap();
    #[cfg(target_os = "linux")]
    let supports_nocow = rustix::fs::fstatfs(File::open(directory.path()).unwrap())
        .unwrap()
        .f_type
        == 0x9123_683e;
    #[cfg(not(target_os = "linux"))]
    let supports_nocow = false;
    let result = RootfsBackingPolicy::NoCow.validate_cache(directory.path());
    if supports_nocow {
        result.unwrap();
    } else {
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Unsupported);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn materialization_preserves_bytes_sparsity_and_clone_isolation() {
    use rustix::fs::{IFlags, ioctl_getflags, ioctl_setflags};
    use sha2::{Digest, Sha256};
    use std::io::Write;
    use std::os::unix::fs::{FileExt, MetadataExt};

    let Some(root) = std::env::var_os("SPACE_NOCOW_TEST_BTRFS_DIR") else {
        assert!(
            std::env::var_os("CI").is_none()
                && std::env::var_os("SPACE_CI_REQUIRE_TEST_DEPS").is_none(),
            "CI must provide the disposable NoCoW btrfs fixture"
        );
        eprintln!(
            "not opted in: SPACE_NOCOW_TEST_BTRFS_DIR must name a disposable btrfs directory"
        );
        return;
    };
    let tmp = tempfile::tempdir_in(root).unwrap();
    RootfsBackingPolicy::NoCow
        .validate_cache(tmp.path())
        .unwrap();
    let mut bytes = vec![0; 16 * 1024 * 1024];
    bytes[1024..1032].copy_from_slice(b"original");
    let expected_hash = format!("sha256:{}", hex::encode(Sha256::digest(&bytes)));
    let compressed = tmp.path().join("rootfs.zst");
    let mut encoder =
        zstd::stream::write::Encoder::new(File::create(&compressed).unwrap(), 1).unwrap();
    encoder.write_all(&bytes).unwrap();
    encoder.finish().unwrap();

    for (name, inherited_flags) in [
        ("plain", IFlags::empty()),
        ("nocow", IFlags::NOCOW),
        ("compress", IFlags::COMPRESSED),
        ("nocompress", IFlags::from_bits_retain(0x400)),
    ] {
        let directory = tmp.path().join(name);
        std::fs::create_dir(&directory).unwrap();
        ioctl_setflags(File::open(&directory).unwrap(), inherited_flags).unwrap();
        for compressed_input in [false, true] {
            let output = directory.join(if compressed_input { "decoded" } else { "raw" });
            if compressed_input {
                crate::rootfs_zstd::decompress_rootfs(
                    &compressed,
                    &output,
                    Some(&expected_hash),
                    RootfsBackingPolicy::NoCow,
                )
                .unwrap();
            } else {
                if inherited_flags.contains(IFlags::NOCOW) {
                    let mut writer = pplx_space_common_io::sparse::SparseFileWriter::new(
                        File::create(&output).unwrap(),
                        64 * 1024,
                    )
                    .unwrap();
                    writer.write_all(&bytes).unwrap();
                    writer.finish().unwrap();
                } else {
                    std::fs::write(&output, &bytes).unwrap();
                }
                RootfsBackingPolicy::NoCow.prepare_raw(&output).unwrap();
            }
            let source = File::open(&output).unwrap();
            assert!(ioctl_getflags(&source).unwrap().contains(IFlags::NOCOW));
            assert_eq!(std::fs::read(&output).unwrap(), bytes);
            assert!(source.metadata().unwrap().blocks() * 512 < 1024 * 1024);
            // Re-adoption must retain the existing inode and shared extents,
            // rather than rewriting an image that already has the policy.
            RootfsBackingPolicy::NoCow.prepare_raw(&output).unwrap();
            assert_eq!(
                source.metadata().unwrap().ino(),
                std::fs::metadata(&output).unwrap().ino()
            );
            // A reflink requires compatible checksum policy. It must share
            // successfully and isolate a subsequent write from the parent.
            let clone_path = directory.join(if compressed_input {
                "decoded-clone"
            } else {
                "raw-clone"
            });
            let clone = File::options()
                .read(true)
                .write(true)
                .create_new(true)
                .open(clone_path)
                .unwrap();
            RootfsBackingPolicy::NoCow.prepare_empty(&clone).unwrap();
            rustix::fs::ioctl_ficlone(&clone, &source).unwrap();
            clone.write_all_at(b"modified", 1024).unwrap();
            let mut actual = [0; 8];
            source.read_exact_at(&mut actual, 1024).unwrap();
            assert_eq!(&actual, b"original");
            assert_eq!(
                RootfsBackingPolicy::NoCow
                    .prepare_empty(&source)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
    }
}
