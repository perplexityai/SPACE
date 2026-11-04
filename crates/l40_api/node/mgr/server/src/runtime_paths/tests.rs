use super::*;

#[test]
fn create_snapshot_accepts_runtime_scoped_destinations() {
    let sandbox_id = "sbx_123";
    let sandbox_snapshot_dir =
        Path::new(pplx_space_common_core::host::linux::paths::SANDBOX_CACHE_DIR)
            .join(sandbox_id)
            .join("snapshot");

    let paths = RuntimePaths::firecracker()
        .create_snapshot_paths(
            sandbox_id,
            &SnapshotFiles::parse("/vmstate".into(), "/memory".into()).unwrap(),
            Some(&NodeSpacePath::parse(sandbox_snapshot_dir.clone()).unwrap()),
        )
        .unwrap();

    assert_eq!(paths.vmstate_name, "vmstate");
    assert_eq!(paths.memory_name, "memory");
    assert_eq!(
        paths.dest_dir.as_deref(),
        Some(sandbox_snapshot_dir.as_path())
    );

    let template_staging_dir =
        Path::new(pplx_space_common_core::host::linux::paths::SANDBOX_CACHE_DIR)
            .join(sandbox_id)
            .join("template-snapshot-staging")
            .join("base");
    let paths = RuntimePaths::firecracker()
        .create_snapshot_paths(
            sandbox_id,
            &SnapshotFiles::parse("/vmstate".into(), "/memory".into()).unwrap(),
            Some(&NodeSpacePath::parse(template_staging_dir.clone()).unwrap()),
        )
        .unwrap();

    assert_eq!(
        paths.dest_dir.as_deref(),
        Some(template_staging_dir.as_path())
    );
}

#[test]
fn create_snapshot_rejects_destinations_outside_the_sandbox_roots() {
    let files = SnapshotFiles::parse("/vmstate".into(), "/memory".into()).unwrap();
    for path in [
        "/tmp/snapshot",
        pplx_space_common_core::host::linux::paths::TEMPLATE_DIR,
    ] {
        RuntimePaths::firecracker()
            .create_snapshot_paths(
                "sbx_123",
                &files,
                Some(&NodeSpacePath::parse(path).unwrap()),
            )
            .unwrap_err();
    }
}
