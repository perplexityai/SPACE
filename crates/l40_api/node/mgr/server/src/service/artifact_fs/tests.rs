use super::*;
use std::os::unix::fs::symlink;

#[test]
fn copying_and_deletion_never_follow_cache_links() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let cache = root.join("cache");
    let outside = root.join("outside");
    std::fs::create_dir(&cache).unwrap();
    std::fs::create_dir(&outside).unwrap();
    let secret = outside.join("secret");
    std::fs::write(&secret, "do not copy or remove").unwrap();
    symlink(&outside, cache.join("link")).unwrap();
    let destination = trusted_paths::open_directory(&cache).unwrap();
    copy(&cache.join("link/secret"), &destination, "copy".as_ref()).unwrap_err();
    remove(&cache.join("link/secret")).unwrap_err();
    symlink(&secret, cache.join("secret")).unwrap();
    copy(&cache.join("secret"), &destination, "copy".as_ref()).unwrap_err();
    write_atomic(&cache.join("secret"), |mut output| {
        use std::io::Write;
        output.write_all(b"replacement").map_err(error)
    })
    .unwrap();
    assert_eq!(std::fs::read(cache.join("secret")).unwrap(), b"replacement");
    assert!(remove(&cache).unwrap());
    assert_eq!(
        std::fs::read_to_string(&secret).unwrap(),
        "do not copy or remove"
    );
}

#[test]
fn copy_uses_pinned_parent_after_directory_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let destination = root.join("destination");
    std::fs::create_dir(&destination).unwrap();
    let pinned = trusted_paths::open_directory(&destination).unwrap();
    std::fs::rename(&destination, root.join("original")).unwrap();
    std::fs::create_dir(&destination).unwrap();
    let source = root.join("source");
    std::fs::write(&source, "contents").unwrap();
    copy(&source, &pinned, "copied".as_ref()).unwrap();
    assert_eq!(
        std::fs::read_to_string(root.join("original/copied")).unwrap(),
        "contents"
    );
    assert!(!destination.join("copied").exists());
    write_atomic(&destination.join("metadata"), |mut output| {
        use std::io::Write;
        std::fs::rename(&destination, root.join("writer-pinned")).unwrap();
        symlink(root.join("original"), &destination).unwrap();
        output.write_all(b"metadata").map_err(error)
    })
    .unwrap();
    assert_eq!(
        std::fs::read(root.join("writer-pinned/metadata")).unwrap(),
        b"metadata"
    );
    assert!(!root.join("original/metadata").exists());
}

#[test]
fn failed_rollback_preserves_instance_and_retry_commits_complete_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let parent = trusted_paths::open_directory(&root).unwrap();
    std::fs::create_dir(root.join("instance")).unwrap();
    std::fs::write(root.join("instance/rootfs.ext4"), "current data").unwrap();
    std::fs::create_dir(root.join("snapshot")).unwrap();
    let source = directory(&parent, "snapshot".as_ref()).unwrap();
    let gid = nix::unistd::getegid().as_raw();
    rollback(&parent, &source, gid).unwrap_err();
    assert_eq!(
        std::fs::read_to_string(root.join("instance/rootfs.ext4")).unwrap(),
        "current data"
    );
    std::fs::write(root.join("snapshot/rootfs.ext4"), "restored data").unwrap();
    // Fail part-way through staging, not only on the initial existence check.
    symlink("rootfs.ext4", root.join("snapshot/poison")).unwrap();
    rollback(&parent, &source, gid).unwrap_err();
    assert_eq!(
        std::fs::read_to_string(root.join("instance/rootfs.ext4")).unwrap(),
        "current data"
    );
    std::fs::remove_file(root.join("snapshot/poison")).unwrap();
    rollback(&parent, &source, gid).unwrap();
    assert_eq!(
        std::fs::read_to_string(root.join("instance/rootfs.ext4")).unwrap(),
        "restored data"
    );
    assert!(!root.join(".instance-rollback").exists());
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires root and an explicitly supplied disposable btrfs fixture directory"]
fn btrfs_pinned_operations_survive_path_replacement_and_nested_cleanup() {
    // CI includes ignored tests; host mutation still requires explicit opt-in.
    let Some(root) = std::env::var_os("SPACE_NODEMGR_TEST_BTRFS_DIR") else {
        eprintln!("not opted in: set SPACE_NODEMGR_TEST_BTRFS_DIR to a disposable btrfs directory");
        return;
    };
    let root = std::path::PathBuf::from(root);
    assert!(nix::unistd::geteuid().is_root());
    let temp = tempfile::tempdir_in(root).unwrap();
    let root = temp.path().canonicalize().unwrap();
    let parent = trusted_paths::open_directory(&root).unwrap();
    btrfs::create(&parent, "template".as_ref()).unwrap();
    let template = directory(&parent, "template".as_ref()).unwrap();
    std::fs::write(root.join("template/rootfs.ext4"), "base").unwrap();
    let received = uuid::Uuid::now_v7();
    btrfs::stamp(&template, received).unwrap();
    btrfs::set_readonly(&template, true).unwrap();
    assert_eq!(
        btrfs::info(&template).unwrap().1.received_uuid,
        Some(received)
    );
    btrfs::snapshot(&template, &parent, "instance".as_ref(), false).unwrap();
    let instance = directory(&parent, "instance".as_ref()).unwrap();
    std::fs::write(root.join("instance/rootfs.ext4"), "changed").unwrap();
    btrfs::snapshot(&instance, &parent, "instance.ro".as_ref(), true).unwrap();
    let snapshot = directory(&parent, "instance.ro".as_ref()).unwrap();
    let stream = std::fs::File::create(root.join("stream")).unwrap();
    std::fs::rename(root.join("instance.ro"), root.join("renamed.ro")).unwrap();
    symlink("template", root.join("instance.ro")).unwrap();
    btrfs::send(&template, &snapshot, &stream).unwrap();
    assert!(stream.metadata().unwrap().len() > 16);
    rollback(&parent, &snapshot, nix::unistd::getegid().as_raw()).unwrap();
    assert_eq!(
        std::fs::read_to_string(root.join("instance/rootfs.ext4")).unwrap(),
        "changed"
    );
    btrfs::set_readonly(&template, false).unwrap();
    assert_eq!(btrfs::info(&template).unwrap().1.received_uuid, None);
    btrfs::create(&template, "nested".as_ref()).unwrap();
    remove_at(&parent, "template".as_ref()).unwrap();
    remove_at(&parent, "instance".as_ref()).unwrap();
    remove_at(&parent, "renamed.ro".as_ref()).unwrap();
    remove_at(&parent, "instance.ro".as_ref()).unwrap();
    assert!(!root.join("template").exists());

    // Restore memory can outlive the artifact name through its jail bind.
    // A later suspend must export that inode even after retention unlinks it.
    std::fs::write(root.join("restore-memory"), b"restored memory").unwrap();
    let source = trusted_paths::open_regular_file(&parent, "restore-memory".as_ref()).unwrap();
    let target = trusted_paths::open_bind_target(&parent, "memory".as_ref()).unwrap();
    trusted_paths::bind_pinned_files(&source, &target).unwrap();
    std::fs::remove_file(root.join("restore-memory")).unwrap();
    let copied = copy(&root.join("memory"), &parent, "exported".as_ref());
    nix::mount::umount(root.join("memory").as_path()).unwrap();
    copied.unwrap();
    assert_eq!(
        std::fs::read(root.join("exported")).unwrap(),
        b"restored memory"
    );
}
