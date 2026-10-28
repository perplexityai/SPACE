use super::*;

#[test]
fn manifests_match_the_operation_and_registered_artifact_kind() {
    use SnapshotArtifact::*;
    let entry = |artifact: SnapshotArtifact| SnapshotArtifactDigest {
        artifact: artifact.file_name().into(),
        digest: format!("sha256:{}", "12".repeat(32)),
        size_bytes: 4,
    };
    // Copy source, copied disk, disk backup, full backup.
    for (artifacts, expected) in [
        (vec![], [true, true, true, true]),
        (vec![Metadata], [false; 4]),
        (vec![Metadata, Rootfs], [true, true, true, false]),
        (vec![Metadata, RootfsBtrfsSend], [true, true, true, false]),
        (vec![Metadata, Rootfs, Memory], [false; 4]),
        (
            vec![Metadata, Rootfs, Memory, VmState],
            [true, false, false, true],
        ),
        (
            vec![Metadata, RootfsBtrfsSend, Memory, VmState],
            [true, false, false, true],
        ),
        (vec![Metadata, Rootfs, RootfsBtrfsSend], [false; 4]),
        (vec![Metadata, Metadata], [false; 4]),
        (
            vec![Metadata, EncryptedSnapshotVolume, EncryptedRootfsCow],
            [false, false, true, true],
        ),
        (vec![Metadata, EncryptedSnapshotVolume], [false; 4]),
        (
            vec![
                Metadata,
                EncryptedSnapshotVolume,
                EncryptedRootfsCow,
                Rootfs,
            ],
            [false; 4],
        ),
    ] {
        let entries: Vec<_> = artifacts.into_iter().map(entry).collect();
        let results = [
            parse_copy_source(&entries),
            parse_copied_disk(&entries),
            parse_backup(&entries, ArtifactKind::Disk),
            parse_backup(&entries, ArtifactKind::Full),
        ];
        for (result, valid) in results.into_iter().zip(expected) {
            assert_eq!(result.is_ok(), valid, "{entries:?}: {result:?}");
        }
    }
    for digest in ["bad".into(), format!("sha256:{}", "AB".repeat(32))] {
        let mut invalid = entry(Rootfs);
        invalid.digest = digest;
        parse_copy_source(&[entry(Metadata), invalid]).unwrap_err();
    }
    let error = parse_copied_disk(&[entry(Metadata), entry(Memory)]).unwrap_err();
    assert!(
        matches!(error, ManifestError::Incomplete { actual, .. } if actual == ["memfile", "metadata.json"])
    );
}
