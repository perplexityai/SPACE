use super::*;
use pplx_space_common_core::host::linux::identities::gids;
use std::{
    fs::Permissions,
    os::unix::fs::{PermissionsExt, symlink},
};

#[test]
fn registration_uses_the_sidecar_directory_contract() {
    assert_eq!(Uid::effective(), Uid::from_raw(0), "requires root fixture");
    let scratch = tempfile::tempdir().unwrap();
    let run = scratch.path().join("sandbox");
    std::fs::create_dir(&run).unwrap();
    for owner in [0, 10001] {
        nix::unistd::chown(
            &run,
            Some(Uid::from_raw(owner)),
            Some(Gid::from_raw(gids::SPACE_SIDECARCLIENTS)),
        )
        .unwrap();
        std::fs::set_permissions(&run, std::fs::Permissions::from_mode(0o2770)).unwrap();
        // PrepareSandbox uses root ownership; sidecar launch uses the slot UID.
        open_registration_directory(&run, 10001).unwrap();
    }
    for (owner, group, mode) in [
        (10002, gids::SPACE_SIDECARCLIENTS, 0o2770),
        (0, 10002, 0o2770),
        (0, gids::SPACE_SIDECARCLIENTS, 0o2777),
    ] {
        nix::unistd::chown(&run, Some(Uid::from_raw(owner)), Some(Gid::from_raw(group))).unwrap();
        std::fs::set_permissions(&run, std::fs::Permissions::from_mode(mode)).unwrap();
        open_registration_directory(&run, 10001).unwrap_err();
    }
    std::fs::remove_dir(&run).unwrap();
    symlink(scratch.path(), &run).unwrap();
    open_registration_directory(&run, 10001).unwrap_err();
}

#[test]
fn output_never_writes_a_preexisting_or_replaced_inode() {
    assert_eq!(Uid::effective(), Uid::from_raw(0), "requires root fixture");
    let scratch = tempfile::tempdir().unwrap();
    let original = scratch.path().join("source");
    std::fs::write(&original, b"authenticated source").unwrap();
    std::fs::set_permissions(&original, Permissions::from_mode(0o600)).unwrap();
    let jail = scratch.path().join("root");
    std::fs::create_dir(&jail).unwrap();
    nix::unistd::chown(
        &jail,
        Some(Uid::from_raw(10001)),
        Some(Gid::from_raw(10001)),
    )
    .unwrap();
    let output = jail.join("console.log");
    for replacement in ["hardlink", "symlink", "fifo", "tenant-file"] {
        match replacement {
            "hardlink" => std::fs::hard_link(&original, &output).unwrap(),
            "symlink" => symlink(&original, &output).unwrap(),
            "fifo" => nix::unistd::mkfifo(&output, Mode::from_bits_truncate(0o600)).unwrap(),
            "tenant-file" => {
                std::fs::write(&output, b"untrusted").unwrap();
                nix::unistd::chown(
                    &output,
                    Some(Uid::from_raw(10001)),
                    Some(Gid::from_raw(10001)),
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        let mut log = open_runtime_output(&output).unwrap();
        let metadata = log.metadata().unwrap();
        assert_eq!(metadata.uid(), 0);
        assert_eq!(metadata.mode() & 0o777, 0o600);
        assert_ne!(metadata.ino(), std::fs::metadata(&original).unwrap().ino());
        // Replace the published name too: subsequent writes still use our inode.
        let moved = jail.join("moved.log");
        std::fs::rename(&output, &moved).unwrap();
        std::fs::hard_link(&original, &output).unwrap();
        log.write_all(b"console").unwrap();
        assert_eq!(std::fs::read(&original).unwrap(), b"authenticated source");
        assert_eq!(std::fs::read(&moved).unwrap(), b"console");
        std::fs::remove_file(&output).unwrap();
        std::fs::remove_file(moved).unwrap();
    }
}

#[test]
fn stdin_uses_the_pinned_private_device_directory() {
    use std::io::Read;
    assert_eq!(Uid::effective(), Uid::from_raw(0), "requires root fixture");
    let scratch = tempfile::tempdir().unwrap();
    let path = scratch.path().join("dev");
    std::fs::create_dir(&path).unwrap();
    mknod(
        &path.join("null"),
        SFlag::S_IFCHR,
        Mode::from_bits_truncate(0o600),
        makedev(1, 3),
    )
    .unwrap();
    let pinned = File::open(&path).unwrap();
    std::fs::rename(&path, scratch.path().join("moved")).unwrap();
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("null"), b"replacement").unwrap();
    let mut null = open_private_null(&pinned).unwrap();
    assert_eq!(null.metadata().unwrap().rdev(), makedev(1, 3));
    assert_eq!(null.read(&mut [0; 1]).unwrap(), 0);
}
