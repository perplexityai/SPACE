use super::*;
#[test]
fn proof_covers_consumed_bytes_and_requires_eof() {
    let sink = DigestSink::default();
    let mut reader = DigestReader::new(io::Cursor::new(b"captured-state"), Some(sink.clone()));
    assert_eq!(reader.read(&mut []).unwrap(), 0);
    sink.completed().expect_err("incomplete input has no proof");
    let mut first = [0; 3];
    reader.read_exact(&mut first).unwrap();
    sink.completed().expect_err("incomplete input has no proof");
    let mut rest = Vec::new();
    reader.read_to_end(&mut rest).unwrap();
    let (digest, bytes, _) = sink.completed().unwrap();
    assert_eq!(bytes, 14);
    assert_eq!(
        digest,
        Digest::sha256(Sha256::digest(b"captured-state").into())
    );
}

#[test]
fn metadata_proof_binds_stable_fields_but_ignores_regenerated_values() {
    let base = serde_json::json!({"version":2,"sandbox_id":"sandbox","local_artifact_id":"artifact","artifact_kind":"full","artifact_origin":"suspend","execution":{"runtime":"fc"},"source":{"template_id":"seed","content_hash":"original"},"created_at":"old","size_bytes":1});
    let read = |value: &serde_json::Value| {
        let sink = DigestSink::for_artifact(SnapshotArtifact::Metadata);
        let mut reader = DigestReader::new(
            io::Cursor::new(serde_json::to_vec(value).unwrap()),
            Some(sink.clone()),
        );
        io::copy(&mut reader, &mut io::sink()).unwrap();
        sink.completed().unwrap().2
    };
    let original = read(&base).expect("valid version 2 metadata");
    let mut regenerated = base.clone();
    regenerated["created_at"] = "later".into();
    regenerated["size_bytes"] = 123.into();
    regenerated["rootfs_send_source_id"] = "node-local-handle".into();
    assert_eq!(read(&regenerated), Some(original));
    regenerated["source"]["content_hash"] = "different".into();
    assert_ne!(read(&regenerated), Some(original));
    assert_eq!(
        read(&serde_json::json!({"version":1})),
        None,
        "legacy metadata remains unknown"
    );
}
