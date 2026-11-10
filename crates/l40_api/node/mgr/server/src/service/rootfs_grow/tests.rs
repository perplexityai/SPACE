use super::*;
use serde_json::json;

#[test]
fn root_drive_is_the_single_root_device() {
    let config = json!({"drives": [
        {"drive_id": "data", "is_root_device": false, "path_on_host": "/data.ext4"},
        {"drive_id": "rootfs", "is_root_device": true, "path_on_host": "/rootfs.ext4"},
    ]});
    assert_eq!(
        root_drive(&config).unwrap(),
        RootDrive {
            id: "rootfs".to_owned(),
            path_on_host: "/rootfs.ext4".to_owned(),
        }
    );
}

#[test]
fn root_drive_rejects_missing_or_ambiguous_roots() {
    for config in [
        json!({}),
        json!({"drives": []}),
        json!({"drives": [{"drive_id": "data", "is_root_device": false}]}),
        json!({"drives": [
            {"drive_id": "a", "is_root_device": true, "path_on_host": "/rootfs.ext4"},
            {"drive_id": "b", "is_root_device": true, "path_on_host": "/rootfs.ext4"},
        ]}),
        json!({"drives": [{"drive_id": "rootfs", "is_root_device": true}]}),
    ] {
        assert_eq!(
            root_drive(&config).unwrap_err().code(),
            tonic::Code::FailedPrecondition,
            "{config}"
        );
    }
}

#[test]
fn grow_target_only_grows_within_the_platform_maximum() {
    let current = 20_480 * MIB;
    assert_eq!(grow_target(30_720, current).unwrap(), 30_720 * MIB);
    // Same size is a retry of a grow whose final notification failed.
    assert_eq!(grow_target(20_480, current).unwrap(), current);
    for (size_mib, why) in [(10_240, "shrink"), (0, "zero"), (u32::MAX, "over maximum")] {
        assert_eq!(
            grow_target(size_mib, current).unwrap_err().code(),
            tonic::Code::InvalidArgument,
            "{why}"
        );
    }
}

#[test]
fn only_the_inode_firecracker_serves_is_pinned() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    std::fs::create_dir_all(root.join("instance")).unwrap();
    std::fs::create_dir_all(root.join("jail")).unwrap();
    let instance = root.join("instance/rootfs.ext4");
    std::fs::write(&instance, vec![0_u8; 4096]).unwrap();

    // Plaintext launch binds the instance file itself into the jail.
    let pinned = pin_served_rootfs(&instance, &instance).unwrap();
    assert_eq!(pinned.current_bytes, 4096);
    pinned.backing.file.set_len(8192).unwrap();
    assert_eq!(std::fs::metadata(&instance).unwrap().len(), 8192);

    // A different file, or a link to the instance, is not proof of serving.
    let other = root.join("jail/rootfs.ext4");
    std::fs::write(&other, vec![0_u8; 4096]).unwrap();
    assert_eq!(
        pin_served_rootfs(&instance, &other).err().unwrap().code(),
        tonic::Code::FailedPrecondition
    );
    std::fs::remove_file(&other).unwrap();
    std::os::unix::fs::symlink(&instance, &other).unwrap();
    assert_eq!(
        pin_served_rootfs(&instance, &other).err().unwrap().code(),
        tonic::Code::FailedPrecondition
    );
}
