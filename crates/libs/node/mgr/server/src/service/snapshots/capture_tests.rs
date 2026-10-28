use super::*;

#[test]
#[ignore = "requires root and an explicitly supplied disposable btrfs fixture directory"]
fn encrypted_capture_publishes_only_the_copy_permissions() {
    let Some(root) = std::env::var_os("SPACE_NODEMGR_TEST_BTRFS_DIR") else {
        return;
    };
    assert!(nix::unistd::geteuid().is_root());
    let fixture = tempfile::tempdir_in(root).unwrap();
    let root = fixture.path().canonicalize().unwrap();
    let parent = super::super::trusted_paths::open_directory(&root).unwrap();
    crate::btrfs::create(&parent, "instance".as_ref()).unwrap();
    let instance = root.join("instance");
    let cow = instance.join("cow.luks");
    std::fs::write(&cow, b"sealed disk state").unwrap();
    std::fs::set_permissions(&cow, std::fs::Permissions::from_mode(0o600)).unwrap();
    let snapshots = root.join("snapshots");
    capture_instance_snapshot(&instance, &snapshots, "capture".as_ref(), false, true).unwrap();
    let source_mode = std::fs::metadata(&cow).unwrap().mode() & 0o777;
    let captured = snapshots.join("capture");
    let capture_mode = std::fs::metadata(captured.join("cow.luks")).unwrap().mode() & 0o777;
    let capture = super::super::trusted_paths::open_directory(&captured).unwrap();
    let readonly = crate::btrfs::is_readonly(&capture).unwrap();
    std::fs::write(&cow, b"later source writes").unwrap();
    let original_bytes = std::fs::read(captured.join("cow.luks")).unwrap();
    let replay = capture_instance_snapshot(&instance, &snapshots, "capture".as_ref(), false, true);
    let snapshot_parent = super::super::trusted_paths::open_directory(&snapshots).unwrap();
    super::super::artifact_fs::remove_at(&snapshot_parent, "capture".as_ref()).unwrap();
    super::super::artifact_fs::remove_at(&parent, "instance".as_ref()).unwrap();
    assert_eq!(
        source_mode, 0o600,
        "capture loosened the live private backing"
    );
    assert_eq!(capture_mode, 0o660);
    assert!(readonly);
    assert_eq!(original_bytes, b"sealed disk state");
    assert!(
        replay.is_err(),
        "capture replaced an already published artifact"
    );
}
