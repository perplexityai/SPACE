use super::*;

fn rec(id: &str) -> SandboxRecord {
    SandboxRecord::new_creating(
        id.to_string(),
        "seed".to_string(),
        "fc".to_string(),
        "root".to_string(),
        Some("hash-1".to_string()),
        false,
    )
}

#[tokio::test]
async fn insert_then_get() {
    let dir = tempfile::tempdir().unwrap();
    let state = SimState::new(
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
        "node-a".into(),
    );
    state.insert_new(rec("sbx-1")).await.unwrap();
    let got = state.get("sbx-1").await.unwrap();
    assert_eq!(got.sandbox_id, "sbx-1");
    assert_eq!(got.status, status::CREATING);
}

#[tokio::test]
async fn unsafe_sandbox_id_rejected_without_poisoning_state() {
    let dir = tempfile::tempdir().unwrap();
    let state = SimState::new(
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
        "node-a".into(),
    );
    let bad = rec("../escape");
    let err = state.insert_new(bad).await.unwrap_err();
    assert!(matches!(err, SimStateError::InvalidSandboxId(_)));
    // The map must be empty: the validation rejection must precede the
    // map insert so a re-attempt with a valid id still succeeds.
    assert!(state.list().await.is_empty());
    state.insert_new(rec("ok-id")).await.unwrap();
}

#[tokio::test]
async fn duplicate_insert_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let state = SimState::new(
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
        "node-a".into(),
    );
    state.insert_new(rec("sbx-1")).await.unwrap();
    let err = state.insert_new(rec("sbx-1")).await.unwrap_err();
    assert!(matches!(err, SimStateError::AlreadyExists(_)));
}

#[tokio::test]
async fn transition_enforces_source_states() {
    let dir = tempfile::tempdir().unwrap();
    let state = SimState::new(
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
        "node-a".into(),
    );
    state.insert_new(rec("sbx-1")).await.unwrap();
    // creating -> running ok
    let updated = state
        .transition("sbx-1", &[status::CREATING], status::RUNNING)
        .await
        .unwrap();
    assert_eq!(updated.status, status::RUNNING);
    // running -> running rejected by source list
    let err = state
        .transition("sbx-1", &[status::CREATING], status::RUNNING)
        .await
        .unwrap_err();
    assert!(matches!(err, SimStateError::InvalidTransition { .. }));
}

#[tokio::test]
async fn suspend_writes_file_and_restore_consumes_it() {
    let dir = tempfile::tempdir().unwrap();
    let node_a = SimState::new(
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
        "node-a".into(),
    );
    let node_b = SimState::new(
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
        "node-b".into(),
    );

    node_a.insert_new(rec("sbx-1")).await.unwrap();
    node_a
        .transition("sbx-1", &[status::CREATING], status::RUNNING)
        .await
        .unwrap();
    node_a.suspend_to_shared_dir("sbx-1").await.unwrap();

    let path = dir.path().join("sbx-1.json");
    assert!(path.exists(), "suspend should write snapshot file");

    let restored = node_b.restore_from_shared_dir("sbx-1").await.unwrap();
    assert_eq!(restored.status, status::RUNNING);
    assert_eq!(restored.sandbox_id, "sbx-1");
    assert!(!path.exists(), "restore should remove the snapshot file");
}

#[tokio::test]
async fn restore_missing_snapshot_errors() {
    let dir = tempfile::tempdir().unwrap();
    let state = SimState::new(
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
        "node-a".into(),
    );
    let err = state
        .restore_from_shared_dir("never-created")
        .await
        .unwrap_err();
    assert!(matches!(err, SimStateError::SnapshotMissing { .. }));
}

#[tokio::test]
async fn exact_cleanup_identity_and_template_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let state = SimState::new(
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
        "node-a".into(),
    );
    let mut r1 = rec("aa-1");
    r1.template = "seed".into();
    let mut r2 = rec("aa-2");
    r2.template = "code-runtime".into();
    let mut r3 = rec("bb-1");
    r3.template = "seed".into();
    state.insert_new(r1).await.unwrap();
    state.insert_new(r2).await.unwrap();
    state.insert_new(r3).await.unwrap();

    for (id, template, expected) in [
        ("aa", None, None),
        ("all", None, None),
        ("", Some("seed"), None),
        ("aa-1", Some("code-runtime"), None),
        ("aa-1", None, Some("aa-1")),
        ("aa-2", Some("code-runtime"), Some("aa-2")),
    ] {
        assert_eq!(
            state.ids_matching_exact(id, template).await,
            expected.into_iter().map(str::to_owned).collect::<Vec<_>>(),
        );
    }
}
