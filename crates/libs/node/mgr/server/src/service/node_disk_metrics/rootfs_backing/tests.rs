use super::*;

#[test]
fn inventory_includes_each_generation_and_retained_rootfs_without_following_links() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap();
    let templates = root.join("templates");
    let sandboxes = root.join("sandboxes");
    let sandbox = sandboxes.join("018f6d8d-0000-7000-8000-000000000001");
    for directory in [
        templates.join("example/sha256:aaa"),
        templates.join("example/sha256:bbb"),
        templates.join(".staging/sha256:ccc"),
        templates.join("example/sha256:aaa.importing.fixture"),
        templates.join("example/sha256:bbb.replaced.fixture"),
        sandbox.join("instance"),
        // Checkpoints count even while memory/vmstate are incomplete.
        sandbox.join("snapshot"),
        sandbox.join("snapshots/f-manual-example"),
    ] {
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("rootfs.ext4"), b"fixture").unwrap();
    }
    let unsafe_entry = templates.join("example/sha256:ddd");
    std::fs::create_dir(&unsafe_entry).unwrap();
    std::os::unix::fs::symlink(
        sandbox.join("instance/rootfs.ext4"),
        unsafe_entry.join("rootfs.ext4"),
    )
    .unwrap();

    let inventory = collect(&sandboxes, &templates).unwrap();
    for (kind, expected) in [
        (Kind::Template, 3),
        (Kind::Instance, 1),
        (Kind::SuspendCheckpoint, 1),
        (Kind::DiskSnapshot, 1),
    ] {
        let count: u64 = inventory
            .counts
            .iter()
            .filter(|((category, _), _)| *category == kind)
            .map(|(_, count)| count)
            .sum();
        assert_eq!(count, expected);
    }
    assert_eq!(
        inventory.counts.get(&(Kind::Template, Backing::Unknown)),
        Some(&1)
    );
    std::fs::remove_dir_all(&sandboxes).unwrap();
    assert!(
        collect(&sandboxes, &templates).is_err(),
        "missing root is not an empty inventory"
    );
    std::fs::create_dir(&sandboxes).unwrap();
    std::fs::remove_dir_all(&templates).unwrap();
    std::fs::create_dir(&templates).unwrap();
    assert!(collect(&sandboxes, &templates).unwrap().counts.is_empty());
}

#[cfg(target_os = "linux")]
#[test]
fn backing_inspection_reads_actual_btrfs_inode_policy() {
    use rustix::fs::{IFlags, fstatfs, ioctl_setflags};

    let Some(root) = std::env::var_os("SPACE_NOCOW_TEST_BTRFS_DIR") else {
        assert!(
            std::env::var_os("CI").is_none()
                && std::env::var_os("SPACE_CI_REQUIRE_TEST_DEPS").is_none(),
            "CI must provide the disposable NoCoW btrfs fixture"
        );
        eprintln!("not opted in: set SPACE_NOCOW_TEST_BTRFS_DIR to a disposable btrfs directory");
        return;
    };
    let temporary = tempfile::tempdir_in(root).unwrap();
    let root = temporary.path().canonicalize().unwrap();
    for (name, flags, expected) in [
        ("cow", IFlags::empty(), Backing::Cow),
        ("nocow", IFlags::NOCOW, Backing::NoCow),
    ] {
        let path = root.join(name);
        std::fs::create_dir(&path).unwrap();
        let file = File::create(path.join("rootfs.ext4")).unwrap();
        assert_eq!(fstatfs(&file).unwrap().f_type, libc::BTRFS_SUPER_MAGIC);
        ioctl_setflags(&file, flags).unwrap();
        std::io::Write::write_all(&mut &file, b"populated image").unwrap();
        assert_eq!(inspect(&path).unwrap(), expected);
    }
}
