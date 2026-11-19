use super::*;

#[tokio::test]
async fn receipt_pins_its_directory_and_rejects_other_identities_or_symlinks() {
    let root = tempfile::tempdir().unwrap();
    let sandbox = SandboxId::new();
    let artifact = LocalArtifactId::new();
    let path = root
        .path()
        .join(sandbox.to_string())
        .join("snapshots")
        .join(format!("{STAGING_PREFIX}{artifact}"));
    std::fs::create_dir_all(&path).unwrap();
    let payload = serde_json::to_vec(
        &serde_json::json!({"sandbox_id": sandbox.to_string(), "local_artifact_id": artifact}),
    )
    .unwrap();
    std::fs::write(path.join(INTENT), &payload).unwrap();
    assert!(CaptureReceipt::open_under(root.path(), SandboxId::new(), &path).is_err());
    let mut receipt = CaptureReceipt::open_under(root.path(), sandbox, &path).unwrap();
    assert!(!path.join(COMMITTED).exists());
    let moved = path.with_file_name("retired");
    std::fs::rename(&path, &moved).unwrap();
    std::fs::create_dir(&path).unwrap();
    receipt.begin_capture();
    receipt.commit().await.unwrap();
    receipt
        .settle_error()
        .await
        .expect_err("a later error must not overwrite an already renamed completion");
    assert!(!moved.join(FAILED).exists());
    assert_eq!(std::fs::read(moved.join(COMMITTED)).unwrap(), payload);
    assert!(!moved.join(INTENT).exists());
    assert!(!path.join(COMMITTED).exists());
    std::os::unix::fs::symlink(moved.join(COMMITTED), path.join(INTENT)).unwrap();
    assert!(CaptureReceipt::open_under(root.path(), sandbox, &path).is_err());
    std::fs::remove_file(path.join(INTENT)).unwrap();
    std::fs::write(
        path.join(INTENT),
        br#"{"sandbox_id":"wrong","local_artifact_id":"00000000-0000-0000-0000-000000000001"}"#,
    )
    .unwrap();
    assert!(CaptureReceipt::open_under(root.path(), sandbox, &path).is_err());
}

#[tokio::test]
async fn producer_admission_is_exclusive_and_explicit_settlement_records_refusal() {
    let root = tempfile::tempdir().unwrap();
    let sandbox = SandboxId::new();
    let artifact = LocalArtifactId::new();
    let path = root
        .path()
        .join(sandbox.to_string())
        .join("snapshots")
        .join(format!("{STAGING_PREFIX}{artifact}"));
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(
        path.join(INTENT),
        serde_json::to_vec(&serde_json::json!({
            "sandbox_id": sandbox.to_string(), "local_artifact_id": artifact
        }))
        .unwrap(),
    )
    .unwrap();
    let receipt = CaptureReceipt::open_under(root.path(), sandbox, &path).unwrap();
    assert!(
        CaptureReceipt::open_under(root.path(), sandbox, &path).is_err(),
        "a duplicate must not publish refusal for another producer"
    );
    receipt.settle_error().await.unwrap();
    assert!(path.join(".encrypted-capture.json.refused").exists());
    assert!(!path.join(INTENT).exists());
}

#[test]
fn failed_capture_cannot_be_retried_or_reclassified_as_refused() {
    let root = tempfile::tempdir().unwrap();
    let sandbox = SandboxId::new();
    let artifact = LocalArtifactId::new();
    let path = root
        .path()
        .join(sandbox.to_string())
        .join("snapshots")
        .join(format!("{STAGING_PREFIX}{artifact}"));
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(
        path.join(INTENT),
        serde_json::to_vec(&serde_json::json!({
            "sandbox_id": sandbox.to_string(), "local_artifact_id": artifact
        }))
        .unwrap(),
    )
    .unwrap();
    let mut receipt = CaptureReceipt::open_under(root.path(), sandbox, &path).unwrap();
    receipt.begin_capture();
    drop(receipt);
    assert!(CaptureReceipt::open_under(root.path(), sandbox, &path).is_err());
    assert!(path.join(INTENT).exists());
    assert!(!path.join(REFUSED).exists());
    assert!(!path.join(COMMITTED).exists());
    assert!(!path.join(FAILED).exists());
}

#[test]
fn abandoning_a_receipt_does_not_publish_refusal_from_drop() {
    let root = tempfile::tempdir().unwrap();
    let sandbox = SandboxId::new();
    let artifact = LocalArtifactId::new();
    let path = root
        .path()
        .join(sandbox.to_string())
        .join("snapshots")
        .join(format!("{STAGING_PREFIX}{artifact}"));
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(
        path.join(INTENT),
        serde_json::to_vec(&serde_json::json!({
            "sandbox_id": sandbox.to_string(), "local_artifact_id": artifact,
        }))
        .unwrap(),
    )
    .unwrap();
    drop(CaptureReceipt::open_under(root.path(), sandbox, &path).unwrap());
    assert!(
        path.join(INTENT).exists(),
        "abandoned work retains uncertainty until its owner settles it"
    );
    assert!(!path.join(REFUSED).exists());
}
