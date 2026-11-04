use super::*;
use std::os::unix::fs::{PermissionsExt, symlink};

#[test]
fn privileged_sources_are_bound_to_the_sandbox_and_operation() {
    let id = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    validate_rootfs_device(id, &format!("/dev/mapper/sbxroot-{id}")).unwrap();
    validate_snapshot_volume(
        id,
        &format!("{}/{id}/snapshot-vol", super::super::CACHE_DIR),
    )
    .unwrap();
    for alias in [id.to_uppercase(), id.replace('-', "")] {
        assert!(validate_rootfs_device(&alias, &format!("/dev/mapper/sbxroot-{id}")).is_err());
        assert!(
            validate_snapshot_volume(
                &alias,
                &format!("{}/{id}/snapshot-vol", super::super::CACHE_DIR),
            )
            .is_err()
        );
    }
    for path in [
        "/dev/sda",
        "/dev/mapper/sbxroot-other",
        "/etc",
        "/space/cache/../etc",
        " /dev/mapper/sbxroot-x",
    ] {
        assert!(validate_rootfs_device(id, path).is_err(), "{path}");
        assert!(validate_snapshot_volume(id, path).is_err(), "{path}");
    }
}

#[test]
fn export_never_follows_links_or_changes_an_outside_inode() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let victim = root.join("victim");
    std::fs::write(&victim, b"outside").unwrap();
    std::fs::set_permissions(&victim, std::fs::Permissions::from_mode(0o600)).unwrap();
    for hard in [false, true] {
        let tree = root.join(if hard { "hard" } else { "symbolic" });
        std::fs::create_dir(&tree).unwrap();
        if hard {
            std::fs::hard_link(&victim, tree.join("escape")).unwrap();
        } else {
            symlink(&victim, tree.join("escape")).unwrap();
        }
        assert!(export_permissions(&tree, None).is_err());
        assert_eq!(std::fs::metadata(&victim).unwrap().mode() & 0o777, 0o600);
    }
    symlink(&root, root.join("alias")).unwrap();
    open_directory(&root.join("alias/symbolic")).unwrap_err();
    create_directory(&root.join("alias/new-child")).unwrap_err();
    assert!(!root.join("new-child").exists());
    super::super::cleanup_helpers::chmod_path_blocking(
        root.join("alias/victim").to_str().unwrap(),
        0o777,
    )
    .unwrap_err();
    assert_eq!(std::fs::metadata(&victim).unwrap().mode() & 0o777, 0o600);
    open_rootfs_device(&victim).unwrap_err();
}

#[test]
fn directory_handle_survives_path_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let path = root.join("volume");
    std::fs::create_dir(&path).unwrap();
    let pinned = open_directory(&path).unwrap();
    std::fs::rename(&path, root.join("old")).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert_eq!(
        pinned.metadata().unwrap().ino(),
        std::fs::metadata(root.join("old")).unwrap().ino()
    );
    assert_ne!(
        pinned.metadata().unwrap().ino(),
        std::fs::metadata(&path).unwrap().ino()
    );
    set_permissions(&pinned, None, 0o710).unwrap();
    assert_eq!(
        std::fs::metadata(root.join("old")).unwrap().mode() & 0o777,
        0o710
    );
    assert_ne!(std::fs::metadata(&path).unwrap().mode() & 0o777, 0o710);
}

#[test]
fn installation_symlink_is_followed_but_descendant_links_are_not() {
    let temp = tempfile::tempdir().unwrap();
    let installed = temp.path().join("installed");
    let alias = temp.path().join("space");
    std::fs::create_dir(&installed).unwrap();
    std::fs::set_permissions(&installed, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::create_dir(installed.join("cache")).unwrap();
    std::fs::set_permissions(
        installed.join("cache"),
        std::fs::Permissions::from_mode(0o775),
    )
    .unwrap();
    symlink(&installed, &alias).unwrap();
    let owner = std::fs::symlink_metadata(&alias).unwrap().uid();
    let opened = open_beneath_installation(&alias, Path::new("cache"), owner).unwrap();
    assert_eq!(
        opened.metadata().unwrap().ino(),
        std::fs::metadata(installed.join("cache")).unwrap().ino()
    );
    symlink(&installed, installed.join("escape")).unwrap();
    for relative in ["escape/cache", "../installed", "/cache"] {
        open_beneath_installation(&alias, Path::new(relative), owner).unwrap_err();
    }
    open_beneath_installation(&alias, Path::new("cache"), owner + 1).unwrap_err();
    std::fs::set_permissions(&installed, std::fs::Permissions::from_mode(0o777)).unwrap();
    open_beneath_installation(&alias, Path::new("cache"), owner).unwrap_err();
}
#[test]
fn bind_target_creation_rejects_links_and_keeps_existing_contents() {
    let temp = tempfile::tempdir().unwrap();
    let directory = std::fs::File::open(temp.path()).unwrap();
    let victim = temp.path().join("victim");
    std::fs::write(&victim, b"private").unwrap();
    for hard in [false, true] {
        let target = temp.path().join("memory");
        if hard {
            std::fs::hard_link(&victim, &target).unwrap();
        } else {
            std::os::unix::fs::symlink(&victim, &target).unwrap();
        }
        super::open_bind_target(&directory, "memory".as_ref()).unwrap_err();
        assert_eq!(std::fs::read(&victim).unwrap(), b"private");
        std::fs::remove_file(target).unwrap();
    }
    let target = super::open_bind_target(&directory, "memory".as_ref()).unwrap();
    std::fs::write(temp.path().join("memory"), b"existing").unwrap();
    super::open_bind_target(&directory, "memory".as_ref()).unwrap();
    assert_eq!(
        std::fs::read(temp.path().join("memory")).unwrap(),
        b"existing"
    );
    std::fs::rename(temp.path().join("memory"), temp.path().join("pinned")).unwrap();
    std::os::unix::fs::symlink(&victim, temp.path().join("memory")).unwrap();
    assert_eq!(
        target.metadata().unwrap().ino(),
        std::fs::metadata(temp.path().join("pinned")).unwrap().ino()
    );
}
