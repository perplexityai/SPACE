use super::*;

const MIB: u64 = 1024 * 1024;

#[test]
fn rejects_size_smaller_than_current_rootfs() {
    let temp = tempfile::tempdir().unwrap();
    let rootfs = temp.path().join("rootfs.ext4");
    std::fs::File::create(&rootfs)
        .unwrap()
        .set_len(2 * MIB)
        .unwrap();

    let error = expand_rootfs(&rootfs, 1).unwrap_err();

    assert!(matches!(
        error,
        ExpandedRootfsError::TooSmall {
            rootfs_size_mb: 1,
            current_mb: 2
        }
    ));
}

#[test]
fn expands_from_metadata_when_disk_size_is_set() {
    let temp = tempfile::tempdir().unwrap();
    let rootfs = temp.path().join("rootfs.ext4");
    std::fs::File::create(&rootfs)
        .unwrap()
        .set_len(MIB)
        .unwrap();
    std::fs::write(temp.path().join("metadata.json"), r#"{"disk_size_mb":1}"#).unwrap();

    let expanded = expand_rootfs_from_metadata(temp.path()).unwrap();

    assert_eq!(
        expanded.as_ref().map(|(p, _)| p.as_path()),
        Some(rootfs.as_path())
    );
}

#[test]
fn verify_reports_missing_sized_rootfs() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("metadata.json"), r#"{"disk_size_mb":1}"#).unwrap();

    let error = verify_rootfs_from_metadata(temp.path()).unwrap_err();

    assert!(matches!(error, ExpandedRootfsError::Missing { .. }));
}

#[test]
fn invalid_metadata_is_an_expansion_error() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("metadata.json"), b"{").unwrap();

    let error = verify_rootfs_from_metadata(temp.path()).unwrap_err();

    assert!(matches!(error, ExpandedRootfsError::InvalidMetadata { .. }));
    assert_eq!(error.reason(), "invalid_metadata");
}

#[test]
fn verify_rejects_unexpanded_rootfs_when_disk_size_is_set() {
    let temp = tempfile::tempdir().unwrap();
    let rootfs = temp.path().join("rootfs.ext4");
    std::fs::File::create(&rootfs)
        .unwrap()
        .set_len(MIB)
        .unwrap();
    // metadata declares runtime size 4 MiB; manifest declares rootfs 1 MiB.
    std::fs::write(temp.path().join("metadata.json"), r#"{"disk_size_mb":4}"#).unwrap();
    std::fs::write(
        temp.path().join("manifest.json"),
        format!(r#"{{"files":{{"rootfs.ext4":{{"size":{}}}}}}}"#, MIB),
    )
    .unwrap();

    let error = verify_rootfs_from_metadata(temp.path()).unwrap_err();
    assert!(matches!(
        error,
        ExpandedRootfsError::WrongSize {
            expected_bytes: 4194304,
            actual_bytes: 1048576,
            ..
        }
    ));
}

#[test]
fn verify_accepts_expanded_rootfs_matching_disk_size_mb() {
    let temp = tempfile::tempdir().unwrap();
    let rootfs = temp.path().join("rootfs.ext4");
    std::fs::File::create(&rootfs)
        .unwrap()
        .set_len(4 * MIB)
        .unwrap();
    std::fs::write(temp.path().join("metadata.json"), r#"{"disk_size_mb":4}"#).unwrap();
    std::fs::write(
        temp.path().join("manifest.json"),
        format!(r#"{{"files":{{"rootfs.ext4":{{"size":{}}}}}}}"#, MIB),
    )
    .unwrap();

    verify_rootfs_from_metadata(temp.path()).unwrap();
}

#[test]
fn verify_rejects_rootfs_that_matches_neither_shape() {
    let temp = tempfile::tempdir().unwrap();
    let rootfs = temp.path().join("rootfs.ext4");
    std::fs::File::create(&rootfs)
        .unwrap()
        .set_len(2 * MIB)
        .unwrap();
    std::fs::write(temp.path().join("metadata.json"), r#"{"disk_size_mb":4}"#).unwrap();
    std::fs::write(
        temp.path().join("manifest.json"),
        format!(r#"{{"files":{{"rootfs.ext4":{{"size":{}}}}}}}"#, MIB),
    )
    .unwrap();

    let error = verify_rootfs_from_metadata(temp.path()).unwrap_err();
    assert!(matches!(error, ExpandedRootfsError::WrongSize { .. }));
}

#[test]
fn unsupported_document_versions_refuse_before_disk_growth() {
    for version in [4, 999] {
        for (metadata, manifest) in [
            (
                format!(r#"{{"schema_version":{version},"disk_size_mb":2}}"#),
                "{}".to_owned(),
            ),
            (
                r#"{"disk_size_mb":2}"#.to_owned(),
                format!(r#"{{"schema_version":{version}}}"#),
            ),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let disk = directory.path().join("rootfs.ext4");
            let original = vec![0x5a; MIB as usize];
            std::fs::write(&disk, &original).unwrap();
            std::fs::write(directory.path().join("metadata.json"), metadata).unwrap();
            std::fs::write(directory.path().join("manifest.json"), manifest).unwrap();
            let error = expand_rootfs_from_metadata(directory.path()).unwrap_err();
            assert_eq!(error.reason(), "invalid_metadata");
            assert_eq!(std::fs::read(&disk).unwrap(), original);
        }
    }
}
