use super::*;

#[test]
fn snapshot_rollback_requires_complete_regular_inputs() {
    for name in ["rootfs.ext4", "vmstate", "memory"] {
        for invalid in ["missing", "empty", "directory", "symlink"] {
            let root = tempfile::tempdir().unwrap();
            for other in ["rootfs.ext4", "vmstate", "memory"] {
                if other != name {
                    std::fs::write(root.path().join(other), b"complete").unwrap();
                }
            }
            let path = root.path().join(name);
            match invalid {
                "missing" => {}
                "empty" => std::fs::write(&path, b"").unwrap(),
                "directory" => std::fs::create_dir(&path).unwrap(),
                "symlink" => {
                    let target = root.path().join("elsewhere");
                    std::fs::write(&target, b"complete").unwrap();
                    std::os::unix::fs::symlink(target, &path).unwrap();
                }
                _ => unreachable!(),
            }
            let validate = if name == "rootfs.ext4" {
                validate_snapshot_rootfs
            } else {
                validate_snapshot_memory
            };
            assert!(validate(root.path()).is_err(), "{name}: {invalid}");
            if invalid == "directory" {
                std::fs::remove_dir(&path).unwrap();
            } else if invalid != "missing" {
                std::fs::remove_file(&path).unwrap();
            }
            std::fs::write(path, b"complete").unwrap();
            validate(root.path()).unwrap();
        }
    }
}
