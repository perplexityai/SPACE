use super::*;
use std::os::unix::fs::{FileExt, symlink};

#[cfg(target_os = "linux")]
#[test]
fn staging_preserves_nocow_sparse_files_and_clone_isolation() {
    use rustix::fs::{IFlags, fstatfs, ioctl_getflags, ioctl_setflags};

    let Some(root) = std::env::var_os("SPACE_NOCOW_TEST_BTRFS_DIR") else {
        assert!(
            std::env::var_os("CI").is_none()
                && std::env::var_os("SPACE_CI_REQUIRE_TEST_DEPS").is_none(),
            "CI must provide the disposable NoCoW btrfs fixture"
        );
        eprintln!("not opted in: set SPACE_NOCOW_TEST_BTRFS_DIR to a disposable btrfs directory");
        return;
    };
    let tmp = tempfile::tempdir_in(root).unwrap();
    let directory = File::open(tmp.path()).unwrap();
    assert_eq!(fstatfs(&directory).unwrap().f_type, libc::BTRFS_SUPER_MAGIC);
    let source_path = tmp.path().join("source");
    let source = File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&source_path)
        .unwrap();
    ioctl_setflags(&source, ioctl_getflags(&source).unwrap() | IFlags::NOCOW).unwrap();
    source.set_len(16 * 1024 * 1024).unwrap();
    source.write_all_at(b"original", 1024 * 1024).unwrap();
    source.sync_all().unwrap();

    for (name, policy) in [
        ("default", IFlags::empty()),
        ("nocow", IFlags::NOCOW),
        ("compressed", IFlags::COMPRESSED),
        ("uncompressed", IFlags::from_bits_retain(0x0000_0400)),
    ] {
        let path = tmp.path().join(name);
        std::fs::create_dir(&path).unwrap();
        let destination = File::open(&path).unwrap();
        ioctl_setflags(&destination, policy).unwrap();
        let copied = stage_open_file(&source, &destination, "copy".as_ref()).unwrap();
        assert!(ioctl_getflags(&copied).unwrap().contains(IFlags::NOCOW));
        assert_eq!(
            copied.metadata().unwrap().len(),
            source.metadata().unwrap().len()
        );
        assert!(copied.metadata().unwrap().blocks() * 512 < 1024 * 1024);
        copied.write_all_at(b"modified", 1024 * 1024).unwrap();
        copied.sync_all().unwrap();
        let mut bytes = [0; 8];
        source.read_exact_at(&mut bytes, 1024 * 1024).unwrap();
        assert_eq!(&bytes, b"original");
        File::open(path.join("copy"))
            .unwrap()
            .read_exact_at(&mut bytes, 1024 * 1024)
            .unwrap();
        assert_eq!(&bytes, b"modified");
    }
}

#[test]
fn descriptor_staging_keeps_pins_and_preserves_output_on_failure() {
    let (_tmp, root) = setup();
    let src_path = root.join("snapshot/vmstate");
    let dst_path = root.join("instance/vmstate");
    std::fs::write(&src_path, b"snapshot").unwrap();
    std::fs::write(&dst_path, b"previous").unwrap();
    let source = File::open(&src_path).unwrap();
    let destination = File::open(root.join("instance")).unwrap();
    // A real failed read occurs after creation/truncation of the staging file.
    let unreadable = File::options().write(true).open(&src_path).unwrap();
    stage_open_file(&unreadable, &destination, "vmstate".as_ref()).unwrap_err();
    assert_eq!(std::fs::read(&dst_path).unwrap(), b"previous");
    assert_eq!(std::fs::read_dir(root.join("instance")).unwrap().count(), 1);

    let outside = root.join("outside");
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(outside.join("vmstate"), b"private").unwrap();
    std::fs::rename(&src_path, root.join("snapshot/original")).unwrap();
    symlink(outside.join("vmstate"), &src_path).unwrap();
    std::fs::rename(root.join("instance"), root.join("pinned-instance")).unwrap();
    symlink(&outside, root.join("instance")).unwrap();
    let copied = stage_open_file(&source, &destination, "vmstate".as_ref()).unwrap();
    assert_eq!(
        std::fs::read(root.join("pinned-instance/vmstate")).unwrap(),
        b"snapshot"
    );
    assert_eq!(std::fs::read(outside.join("vmstate")).unwrap(), b"private");
    std::fs::remove_file(root.join("snapshot/original")).unwrap();
    assert_eq!(source.metadata().unwrap().nlink(), 0);
    stage_open_file(&source, &destination, "unlinked".as_ref()).unwrap();
    assert_eq!(
        std::fs::read(root.join("pinned-instance/unlinked")).unwrap(),
        b"snapshot"
    );
    assert_eq!(
        copied.metadata().unwrap().ino(),
        std::fs::metadata(root.join("pinned-instance/vmstate"))
            .unwrap()
            .ino()
    );

    // Destination replacement after publication must not redirect the returned
    // descriptor used for privileged ownership changes.
    std::fs::rename(
        root.join("pinned-instance/vmstate"),
        root.join("pinned-instance/copied"),
    )
    .unwrap();
    symlink(
        outside.join("vmstate"),
        root.join("pinned-instance/vmstate"),
    )
    .unwrap();
    assert_eq!(
        copied.metadata().unwrap().ino(),
        std::fs::metadata(root.join("pinned-instance/copied"))
            .unwrap()
            .ino()
    );
}

fn setup() -> (tempfile::TempDir, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("sandbox");
    std::fs::create_dir_all(root.join("instance")).unwrap();
    std::fs::create_dir_all(root.join("snapshot")).unwrap();
    (tmp, root)
}

fn assert_escape(result: Result<File, ContainedCopyError>, what: &str) {
    match result {
        Err(ContainedCopyError::Escape { .. }) => {}
        other => panic!("{what} must be rejected as containment escape, got {other:?}"),
    }
}

#[test]
fn copies_content_and_overwrites_existing_destination() {
    let (_tmp, root) = setup();
    let src = root.join("snapshot").join("snapshot.luks");
    let dst = root.join("instance").join("snapshot.luks");
    std::fs::write(&src, b"sealed-bytes").unwrap();
    std::fs::write(&dst, b"stale-and-longer-content").unwrap();

    stage_file_contained(&root, &src, &dst).unwrap();
    assert_eq!(std::fs::read(&dst).unwrap(), b"sealed-bytes");
}

#[test]
fn preserves_length_of_sparse_source() {
    let (_tmp, root) = setup();
    let src = root.join("instance").join("sparse.luks");
    let dst = root.join("snapshot").join("sparse.luks");
    // 1 MiB leading hole, then a data tail.
    let file = std::fs::File::create(&src).unwrap();
    file.set_len(1024 * 1024).unwrap();
    file.write_all_at(b"tail-data", 1024 * 1024).unwrap();
    drop(file);

    stage_file_contained(&root, &src, &dst).unwrap();
    let out = std::fs::read(&dst).unwrap();
    assert_eq!(out.len(), 1024 * 1024 + 9);
    assert!(out[..1024 * 1024].iter().all(|b| *b == 0));
    assert_eq!(&out[1024 * 1024..], b"tail-data");
}

#[test]
fn src_equal_dst_preserves_content_instead_of_truncating() {
    // The removed `cp` refused a same-file copy; the caller controls both
    // validated paths, so setting them equal must not truncate-destroy the
    // (root-only) source before reading it.
    let (_tmp, root) = setup();
    let path = root.join("instance").join("snapshot.luks");
    std::fs::write(&path, b"sealed-artifact-bytes").unwrap();

    stage_file_contained(&root, &path, &path).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"sealed-artifact-bytes");
}

#[test]
fn failed_copy_leaves_no_partial_at_destination() {
    // A destination that already holds a good backing must be untouched if
    // staging fails (here: source is a directory, so the open fails after
    // the temp is created) — no partial/truncated canonical file survives.
    let (_tmp, root) = setup();
    let dst = root.join("instance").join("snapshot.luks");
    std::fs::write(&dst, b"previous-good-backing").unwrap();
    std::fs::create_dir(root.join("snapshot").join("dir.luks")).unwrap();

    let result = stage_file_contained(&root, &root.join("snapshot").join("dir.luks"), &dst);
    assert!(result.is_err(), "copying a directory source must fail");
    assert_eq!(
        std::fs::read(&dst).unwrap(),
        b"previous-good-backing",
        "the prior destination backing must be preserved on failure"
    );
    // No staging temp left behind in the destination directory.
    let leftover: Vec<_> = std::fs::read_dir(root.join("instance"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with(".stage-tmp."))
        .collect();
    assert!(
        leftover.is_empty(),
        "staging temp must be cleaned up on failure"
    );
}

#[test]
fn rejects_symlink_source_final_component() {
    let (tmp, root) = setup();
    let outside = tmp.path().join("outside-secret");
    std::fs::write(&outside, b"host-secret").unwrap();
    let src = root.join("snapshot").join("snapshot.luks");
    symlink(&outside, &src).unwrap();

    assert_escape(
        stage_file_contained(&root, &src, &root.join("instance").join("snapshot.luks")),
        "symlinked source file",
    );
}

#[test]
fn rejects_symlinked_intermediate_directory_in_source() {
    // Models the caller renaming a node-derived directory (`instance/`)
    // into a symlink pointing outside the sandbox.
    let (tmp, root) = setup();
    let outside = tmp.path().join("outside-dir");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("cow.luks"), b"host-file").unwrap();
    std::fs::remove_dir(root.join("instance")).unwrap();
    symlink(&outside, root.join("instance")).unwrap();

    assert_escape(
        stage_file_contained(
            &root,
            &root.join("instance").join("cow.luks"),
            &root.join("snapshot").join("cow.luks"),
        ),
        "symlinked intermediate source directory",
    );
}

#[test]
fn rejects_symlinked_stage_dir_destination() {
    // A symlinked staging *directory* must not let the copy land outside
    // the sandbox (arbitrary host write as root).
    let (tmp, root) = setup();
    let outside = tmp.path().join("outside-stage");
    std::fs::create_dir_all(&outside).unwrap();
    let src = root.join("instance").join("snapshot.luks");
    std::fs::write(&src, b"sealed-bytes").unwrap();
    std::fs::remove_dir(root.join("snapshot")).unwrap();
    symlink(&outside, root.join("snapshot")).unwrap();

    assert_escape(
        stage_file_contained(&root, &src, &root.join("snapshot").join("snapshot.luks")),
        "symlinked destination staging dir",
    );
    assert!(
        !outside.join("snapshot.luks").exists(),
        "no bytes may land outside the root"
    );
}

#[test]
fn fifo_at_destination_name_is_replaced_without_blocking() {
    // A FIFO planted at the destination *name* (parent dir is legit) must
    // not wedge root's write: we never open that name, we rename over it.
    let (_tmp, root) = setup();
    let src = root.join("instance").join("snapshot.luks");
    std::fs::write(&src, b"sealed-bytes").unwrap();
    let dst = root.join("snapshot").join("snapshot.luks");
    nix::unistd::mkfifo(&dst, Mode::from_bits_truncate(0o644)).unwrap();

    stage_file_contained(&root, &src, &dst).unwrap();
    let meta = std::fs::symlink_metadata(&dst).unwrap();
    assert!(
        meta.file_type().is_file(),
        "FIFO must be replaced by a regular file"
    );
    assert_eq!(std::fs::read(&dst).unwrap(), b"sealed-bytes");
}

#[test]
fn rejects_hard_linked_source() {
    // A hard link the caller planted inside the sandbox can alias an inode
    // whose other name is outside; refuse any multiply-linked source.
    let (tmp, root) = setup();
    let outside = tmp.path().join("host-target");
    std::fs::write(&outside, b"host-secret").unwrap();
    let src = root.join("instance").join("snapshot.luks");
    std::fs::hard_link(&outside, &src).unwrap();

    assert_escape(
        stage_file_contained(&root, &src, &root.join("snapshot").join("out.luks")),
        "hard-linked source",
    );
}

#[test]
fn rejects_paths_not_beneath_the_root() {
    let (tmp, root) = setup();
    let src = root.join("instance").join("snapshot.luks");
    std::fs::write(&src, b"sealed-bytes").unwrap();

    assert_escape(
        stage_file_contained(&root, &src, &tmp.path().join("outside.luks")),
        "destination outside the root",
    );
    assert_escape(
        stage_file_contained(&root, &tmp.path().join("outside.luks"), &src),
        "source outside the root",
    );
    assert_escape(
        stage_file_contained(&root, &root, &src),
        "the root itself as source",
    );
    assert_escape(
        stage_file_contained(
            &root,
            &root.join("instance").join("..").join("secret"),
            &src,
        ),
        "parent-dir traversal",
    );
}

#[test]
fn rejects_non_regular_source_without_blocking() {
    let (_tmp, root) = setup();
    let fifo = root.join("instance").join("pipe.luks");
    nix::unistd::mkfifo(&fifo, Mode::from_bits_truncate(0o644)).unwrap();

    assert_escape(
        stage_file_contained(&root, &fifo, &root.join("snapshot").join("out.luks")),
        "FIFO source",
    );
}
