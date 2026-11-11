use super::*;

/// A written record reads back for its own artifact and not for another; a
/// directory without one reads as no record, and a corrupt one fails rather
/// than reading as none.
#[test]
fn a_capture_record_reads_back_only_for_its_artifact() {
    let dir = tempfile::tempdir().unwrap();
    let artifact = LocalArtifactId::new();
    assert_eq!(read(dir.path(), artifact).unwrap(), None);

    let generation = ExecutionGeneration::new(7).unwrap();
    write(
        dir.path(),
        artifact,
        generation,
        RecordWriter::CapturingTeardown,
    )
    .unwrap();
    assert_eq!(read(dir.path(), artifact).unwrap(), Some(generation));
    assert_eq!(
        read(dir.path(), LocalArtifactId::new()).unwrap(),
        None,
        "another artifact in the directory isn't the capture the record names"
    );

    std::fs::write(dir.path().join(CAPTURE_RECORD_FILENAME), b"{").unwrap();
    assert_eq!(
        read(dir.path(), artifact).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
}

/// Every record fences, but only the capturing teardown's own orders
/// captures: not recovery's, nor one written before writers were told apart,
/// which reads like recovery's. Recovery keeps the teardown's record when it
/// names the same capture and generation, and replaces anything else.
#[test]
fn only_the_capturing_teardowns_record_orders_captures() {
    let dir = tempfile::tempdir().unwrap();
    let artifact = LocalArtifactId::new();
    let generation = ExecutionGeneration::new(7).unwrap();

    write(
        dir.path(),
        artifact,
        generation,
        RecordWriter::CapturingTeardown,
    )
    .unwrap();
    assert_eq!(read(dir.path(), artifact).unwrap(), Some(generation));
    assert_eq!(
        read_capture_order(dir.path(), artifact).unwrap(),
        Some(generation)
    );
    assert_eq!(
        read_capture_order(dir.path(), LocalArtifactId::new()).unwrap(),
        None
    );

    // Recovery naming the same generation keeps the teardown's record.
    write(dir.path(), artifact, generation, RecordWriter::Recovery).unwrap();
    assert_eq!(
        read_capture_order(dir.path(), artifact).unwrap(),
        Some(generation)
    );
    // Recovery naming another generation replaces it with one that fences
    // under that generation and orders nothing.
    let active = ExecutionGeneration::new(8).unwrap();
    write(dir.path(), artifact, active, RecordWriter::Recovery).unwrap();
    assert_eq!(read(dir.path(), artifact).unwrap(), Some(active));
    assert_eq!(read_capture_order(dir.path(), artifact).unwrap(), None);
    // Nor does recovery keep the teardown's record of another capture.
    let other = LocalArtifactId::new();
    write(
        dir.path(),
        other,
        generation,
        RecordWriter::CapturingTeardown,
    )
    .unwrap();
    write(dir.path(), artifact, generation, RecordWriter::Recovery).unwrap();
    assert_eq!(read(dir.path(), artifact).unwrap(), Some(generation));
    assert_eq!(read_capture_order(dir.path(), artifact).unwrap(), None);
    // A record that can't be read is replaced, as before.
    std::fs::write(dir.path().join(CAPTURE_RECORD_FILENAME), b"{").unwrap();
    write(dir.path(), artifact, generation, RecordWriter::Recovery).unwrap();
    assert_eq!(read(dir.path(), artifact).unwrap(), Some(generation));

    std::fs::write(
        dir.path().join(CAPTURE_RECORD_FILENAME),
        serde_json::to_vec(&serde_json::json!({
            "local_artifact_id": artifact,
            "execution_generation": generation,
        }))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(read(dir.path(), artifact).unwrap(), Some(generation));
    assert_eq!(read_capture_order(dir.path(), artifact).unwrap(), None);
}

/// The record is node-local: no upload, of any artifact kind or rootfs
/// codec, plaintext or encrypted, carries it to the object store, so no
/// restore or fork elsewhere inherits an execution that isn't its own.
#[test]
fn no_upload_carries_the_capture_record() {
    use crate::snapshot_upload::{encrypted_upload_artifacts, upload_artifacts_for_kind};
    use pplx_space_common_core::types::ArtifactKind;
    use pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat;
    let mut uploads = vec![
        encrypted_upload_artifacts(ArtifactKind::Full),
        encrypted_upload_artifacts(ArtifactKind::Disk),
    ];
    for kind in [ArtifactKind::Disk, ArtifactKind::Full] {
        for format in [
            SuspendSnapshotFormat::Unspecified,
            SuspendSnapshotFormat::Full,
            SuspendSnapshotFormat::BtrfsSend,
        ] {
            uploads.push(upload_artifacts_for_kind(kind, format));
        }
    }
    for artifacts in uploads {
        assert!(
            artifacts
                .iter()
                .all(|artifact| artifact.local_name != CAPTURE_RECORD_FILENAME),
            "{:?}",
            artifacts
                .iter()
                .map(|artifact| artifact.local_name)
                .collect::<Vec<_>>()
        );
    }
}
