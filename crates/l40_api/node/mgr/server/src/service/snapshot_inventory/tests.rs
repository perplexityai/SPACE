use super::*;

#[test]
fn pages_resume_past_the_work_bound_without_truncation_or_cross_sandbox_reuse() {
    let root = tempfile::tempdir().unwrap();
    let snapshots = root.path().join("snapshots");
    std::fs::create_dir(&snapshots).unwrap();
    for i in 0..PAGE_ENTRIES + 3 {
        std::fs::create_dir(snapshots.join(format!("d-1m-{i:04}"))).unwrap();
    }
    let inventory = SnapshotInventory::default();
    let sandbox = SandboxId::new();
    let (mut paths, token, _) = inventory.page(sandbox, root.path(), "").unwrap();
    assert!(paths.len() <= PAGE_ENTRIES);
    assert!(!token.is_empty());
    assert_eq!(
        inventory
            .page(SandboxId::new(), root.path(), &token)
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    inventory
        .0
        .lock()
        .unwrap()
        .get_mut(&token.parse().unwrap())
        .unwrap()
        .started -= Duration::from_secs(31);
    let (rest, end, elapsed) = inventory.page(sandbox, root.path(), &token).unwrap();
    assert!(
        elapsed >= Duration::from_secs(31),
        "pagination must not reset the sizing budget"
    );
    paths.extend(rest);
    assert!(end.is_empty());
    assert_eq!(paths.len(), PAGE_ENTRIES + 3);
    paths.sort();
    paths.dedup();
    assert_eq!(paths.len(), PAGE_ENTRIES + 3);
    assert_eq!(
        inventory
            .page(sandbox, root.path(), &token)
            .unwrap_err()
            .code(),
        tonic::Code::Aborted
    );
    assert!(inventory.0.lock().unwrap().is_empty());
}
