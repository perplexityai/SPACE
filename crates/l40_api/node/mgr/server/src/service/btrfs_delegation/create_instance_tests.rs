use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};

use super::*;

/// The test process stands in for both root and sandbox-manager, so the
/// ownership change can run without privilege. The hand-over group is a
/// supplementary group the process belongs to when it has one, so a skipped
/// `fchown` leaves a group the test can tell apart.
fn test_owners() -> InstanceOwners {
    let uid = nix::unistd::geteuid().as_raw();
    let egid = nix::unistd::getegid().as_raw();
    let gid = nix::unistd::getgroups()
        .ok()
        .and_then(|groups| {
            groups
                .into_iter()
                .map(|group| group.as_raw())
                .find(|group| *group != egid)
        })
        .unwrap_or(egid);
    InstanceOwners {
        creator: uid,
        sandbox_manager: (uid, gid),
    }
}

fn on_btrfs(path: &Path) -> bool {
    artifact_fs::btrfs::is_on_btrfs(&std::fs::File::open(path).unwrap()).unwrap()
}

fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().mode() & 0o7777
}

fn set_mode(path: &Path, mode: u32) {
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

/// A restore that finds no `instance/` gets an empty one it can write its
/// rootfs into: sandbox-manager's, mode 0755, and reported as a subvolume
/// exactly when the cache filesystem is btrfs.
#[test]
fn an_absent_instance_is_created_and_handed_to_sandbox_manager() {
    let root = tempfile::tempdir().unwrap();
    let owners = test_owners();

    let subvolume = create_sandbox_instance_subvolume(root.path(), owners).unwrap();

    assert_eq!(subvolume, on_btrfs(root.path()));
    let instance = root.path().join("instance");
    let metadata = fs::symlink_metadata(&instance).unwrap();
    assert!(metadata.is_dir());
    assert_eq!((metadata.uid(), metadata.gid()), owners.sandbox_manager);
    assert_eq!(mode(&instance), 0o755);
    assert_eq!(fs::read_dir(&instance).unwrap().count(), 0);
}

/// A create that died after making `instance/` but before handing it over
/// leaves it its creator's and empty. The retry hands that one over rather
/// than refusing it, which would fail every retry of the restore.
#[test]
fn an_instance_a_dead_create_left_unowned_is_handed_over() {
    let root = tempfile::tempdir().unwrap();
    let owners = test_owners();
    let parent = std::fs::File::open(root.path()).unwrap();
    artifact_fs::btrfs::create(&parent, std::ffi::OsStr::new("instance")).unwrap();
    let instance = root.path().join("instance");
    set_mode(&instance, 0o700);

    let subvolume = create_sandbox_instance_subvolume(root.path(), owners).unwrap();

    assert_eq!(subvolume, on_btrfs(root.path()));
    let metadata = fs::symlink_metadata(&instance).unwrap();
    assert_eq!((metadata.uid(), metadata.gid()), owners.sandbox_manager);
    assert_eq!(mode(&instance), 0o755);
}

/// What a restore finds already in place stays as it is. A directory with
/// something in it, or one its creator didn't make, could hold a rootfs, so
/// it's neither replaced nor handed over, and it isn't reported as a
/// subvolume.
#[test]
fn a_directory_already_in_place_is_kept_as_it_is() {
    let owners = test_owners();
    let egid = nix::unistd::getegid().as_raw();
    let other_creator = InstanceOwners {
        creator: owners.creator.wrapping_add(1),
        ..owners
    };
    for (name, populated, owners) in [
        ("holding a rootfs", true, owners),
        ("made by another user", false, other_creator),
    ] {
        let root = tempfile::tempdir().unwrap();
        let instance = root.path().join("instance");
        fs::create_dir(&instance).unwrap();
        if populated {
            fs::write(instance.join("rootfs.ext4"), b"bytes").unwrap();
        }
        set_mode(&instance, 0o700);

        let subvolume = create_sandbox_instance_subvolume(root.path(), owners).unwrap();

        assert!(!subvolume, "{name}");
        assert_eq!(mode(&instance), 0o700, "{name}");
        assert_eq!(
            fs::symlink_metadata(&instance).unwrap().gid(),
            egid,
            "{name}"
        );
        let entries: Vec<_> = fs::read_dir(&instance)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        if populated {
            assert_eq!(entries, ["rootfs.ext4"], "{name}");
            assert_eq!(
                fs::read(instance.join("rootfs.ext4")).unwrap(),
                b"bytes",
                "{name}"
            );
        } else {
            assert!(entries.is_empty(), "{name}");
        }
    }
}

/// Something at `instance/` that isn't a directory (a file, or a symlink
/// that would lead somewhere else) is refused, not replaced or followed, and
/// left where it is.
#[test]
fn a_path_that_is_not_a_directory_is_refused_and_left_alone() {
    let owners = test_owners();

    let root = tempfile::tempdir().unwrap();
    let instance = root.path().join("instance");
    fs::write(&instance, b"not a directory").unwrap();
    let error = create_sandbox_instance_subvolume(root.path(), owners).unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("not a directory"), "{error}");
    assert_eq!(fs::read(&instance).unwrap(), b"not a directory");

    let root = tempfile::tempdir().unwrap();
    let elsewhere = root.path().join("elsewhere");
    fs::create_dir(&elsewhere).unwrap();
    set_mode(&elsewhere, 0o700);
    let instance = root.path().join("instance");
    std::os::unix::fs::symlink(&elsewhere, &instance).unwrap();
    let error = create_sandbox_instance_subvolume(root.path(), owners).unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("not a directory"), "{error}");
    assert!(fs::symlink_metadata(&instance).unwrap().is_symlink());
    assert_eq!(mode(&elsewhere), 0o700);
    assert_eq!(
        fs::symlink_metadata(&elsewhere).unwrap().gid(),
        nix::unistd::getegid().as_raw()
    );
}

/// The hand-over re-checks the directory it reopened by name. One that isn't
/// the fresh instance its creator just made is refused rather than handed to
/// sandbox-manager: here it's owned by a user other than the one node-manager
/// creates as, the shape both a swapped-in directory and a node-manager not
/// running as that user leave.
#[test]
fn a_created_instance_that_is_not_its_creators_is_not_handed_over() {
    let root = tempfile::tempdir().unwrap();
    let test = test_owners();
    let owners = InstanceOwners {
        creator: test.creator.wrapping_add(1),
        ..test
    };

    let error = create_sandbox_instance_subvolume(root.path(), owners).unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(
        error
            .message()
            .contains("changed between its creation and its ownership change"),
        "{error}"
    );
    let instance = root.path().join("instance");
    assert_eq!(
        fs::symlink_metadata(&instance).unwrap().gid(),
        nix::unistd::getegid().as_raw()
    );
    assert_ne!(mode(&instance), 0o755);
}

/// A sandbox root that doesn't exist fails the call and creates nothing.
#[test]
fn a_missing_sandbox_root_fails_without_creating_anything() {
    let root = tempfile::tempdir().unwrap();
    let missing = root.path().join("absent");

    let error = create_sandbox_instance_subvolume(&missing, test_owners()).unwrap_err();

    assert_eq!(error.code(), tonic::Code::NotFound, "{error}");
    assert!(!missing.exists());
}

/// The decision table behind the filesystem checks: a restore that writes its
/// own rootfs gets an empty subvolume, what's already there is kept, as the
/// plain `mkdir` this replaces kept it, except an instance a dead create left
/// unowned, which is finished; only a path that isn't a directory refuses.
#[test]
fn the_instance_is_created_finished_or_kept_and_only_a_non_directory_refuses() {
    for (existing, expected) in [
        (ExistingInstance::Absent, InstanceAction::Create),
        (ExistingInstance::Unowned, InstanceAction::HandOver),
        (ExistingInstance::Subvolume, InstanceAction::Keep),
        (ExistingInstance::PlainDirectory, InstanceAction::Keep),
    ] {
        assert_eq!(instance_action(existing).unwrap(), expected, "{existing:?}");
    }
    assert_eq!(
        instance_action(ExistingInstance::Other).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
}
