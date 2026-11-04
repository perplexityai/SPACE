use super::*;
use crate::cache_ownership::RETAINED_SOURCE_MODE;
use nix::fcntl::AtFlags;
use std::os::unix::fs::{PermissionsExt, symlink};

/// Root rewrites a retired uid distinct from the new owner, and only root can
/// build the private shelter: a root-owned bundle in the cache group.
#[test]
fn owned_walk_prunes_snapshots_and_never_mutates_link_targets() {
    if !pplx_space_test_env::root_or_skip("chown between distinct uids") {
        return;
    }
    let old_uid = 100_042;
    assert_owned_walk(OwnedWalk {
        old_uid,
        cache_gid: pplx_space_common_core::host::linux::identities::gids::SPACE_CACHE_RW,
        // Keep the bundle's owner distinct from the retired UID so
        // verification must descend to find that UID, rather than return at
        // the boundary.
        other_uid: old_uid + 1,
        private_shelter: true,
    });
}

/// The unprivileged path: every file already belongs to this process's uid
/// and group, which is all an ordinary user can chown to.
#[test]
fn owned_walk_over_the_current_uid_prunes_snapshots_and_never_mutates_link_targets() {
    let uid = nix::unistd::getuid().as_raw();
    assert_owned_walk(OwnedWalk {
        old_uid: uid,
        cache_gid: nix::unistd::getgid().as_raw(),
        other_uid: uid,
        private_shelter: false,
    });
}

struct OwnedWalk {
    old_uid: u32,
    cache_gid: u32,
    other_uid: u32,
    private_shelter: bool,
}

fn assert_owned_walk(
    OwnedWalk {
        old_uid,
        cache_gid,
        other_uid,
        private_shelter,
    }: OwnedWalk,
) {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let root = base.join("cache");
    std::fs::create_dir_all(root.join("snapshots/kept")).unwrap();
    let file = root.join("data");
    let kept = root.join("snapshots/kept/data");
    let outside = base.join("outside");
    for path in [&file, &kept, &outside] {
        std::fs::write(path, b"content").unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let link = root.join("rootfs.btrfs-source");
    symlink(&outside, &link).unwrap();
    let fifo = root.join("fifo");
    nix::unistd::mkfifo(&fifo, Mode::from_bits_truncate(0o600)).unwrap();
    let special = vec![fifo];
    // Darwin cannot pin filesystem sockets without opening them. Production
    // Linux uses O_PATH; non-Linux retains fail-closed behavior for sockets.
    #[cfg(target_os = "linux")]
    let socket = root.join("socket");
    #[cfg(target_os = "linux")]
    let _listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    #[cfg(target_os = "linux")]
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
    #[cfg(target_os = "linux")]
    let special = {
        let mut special = special;
        special.push(socket);
        special
    };
    let uid = nix::unistd::getuid().as_raw();
    let gid = nix::unistd::getgid().as_raw();
    let base_fd = File::open(&base).unwrap();
    for path in [
        &root,
        &root.join("snapshots"),
        &root.join("snapshots/kept"),
        &file,
        &kept,
        &link,
    ]
    .into_iter()
    .chain(special.iter())
    {
        nix::unistd::fchownat(
            &base_fd,
            path.strip_prefix(&base).unwrap(),
            Some(Uid::from_raw(old_uid)),
            Some(Gid::from_raw(gid)),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .unwrap();
    }
    assert_eq!(
        crate::cache_ownership::first_path_owned_by_uid(&link, old_uid).unwrap(),
        Some(link.clone())
    );
    rewrite(&root, true, Some(old_uid), uid, gid).unwrap();
    assert_eq!(std::fs::symlink_metadata(&link).unwrap().uid(), uid);
    assert_eq!(std::fs::metadata(&file).unwrap().uid(), uid);
    assert_eq!(std::fs::metadata(&file).unwrap().mode() & 0o7777, 0o660);
    for path in &special {
        let metadata = std::fs::symlink_metadata(path).unwrap();
        assert_eq!(metadata.uid(), uid);
        assert_eq!(metadata.gid(), gid);
        assert_eq!(metadata.mode() & 0o7777, 0o660);
    }
    assert_eq!(
        std::fs::metadata(root.join("snapshots")).unwrap().mode() & 0o7777,
        0o2770
    );
    for path in [&kept, &outside] {
        assert_eq!(std::fs::metadata(path).unwrap().mode() & 0o7777, 0o600);
    }
    rewrite(&root.join("snapshots/kept"), false, None, uid, gid).unwrap();
    assert_eq!(std::fs::metadata(&kept).unwrap().mode() & 0o7777, 0o660);
    // A private-looking name alone cannot shelter sandbox-owned files from
    // retirement: writable, wrong-owner, and wrong-group directories are ordinary cache.
    for (name, owner, group, mode, private) in [
        ("writable", uid, cache_gid, 0o770, false),
        (
            "wrong-owner",
            other_uid,
            cache_gid,
            RETAINED_SOURCE_MODE,
            false,
        ),
        ("wrong-group", uid, gid, RETAINED_SOURCE_MODE, false),
    ]
    .into_iter()
    .chain(private_shelter.then_some(("private", 0, cache_gid, RETAINED_SOURCE_MODE, true)))
    {
        let bundle = root.join(name).join("rootfs.btrfs-source");
        std::fs::create_dir_all(&bundle).unwrap();
        nix::unistd::chown(
            &bundle,
            Some(Uid::from_raw(owner)),
            Some(Gid::from_raw(group)),
        )
        .unwrap();
        std::fs::set_permissions(&bundle, std::fs::Permissions::from_mode(mode)).unwrap();
        let data = bundle.join("data");
        std::fs::write(&data, b"retired slot").unwrap();
        nix::unistd::chown(
            &data,
            Some(Uid::from_raw(old_uid)),
            Some(Gid::from_raw(gid)),
        )
        .unwrap();
        assert_eq!(
            crate::cache_ownership::first_path_owned_by_uid(&bundle, old_uid)
                .unwrap()
                .is_none(),
            private,
            "{name}"
        );
        rewrite(bundle.parent().unwrap(), false, Some(old_uid), uid, gid).unwrap();
        let metadata = std::fs::metadata(&data).unwrap();
        assert_eq!(
            metadata.uid(),
            if private { old_uid } else { uid },
            "{name}"
        );
        if private {
            assert_eq!(std::fs::metadata(&bundle).unwrap().mode() & 0o7777, mode);
            assert_eq!(
                crate::cache_ownership::first_path_owned_by_uid(&bundle, old_uid).unwrap(),
                None
            );
        } else {
            assert_eq!(metadata.mode() & 0o7777, 0o660, "{name}");
        }
    }
    std::fs::hard_link(&outside, root.join("hardlink")).unwrap();
    rewrite(&root, true, None, uid, gid).unwrap_err();
    assert_eq!(std::fs::metadata(&outside).unwrap().mode() & 0o7777, 0o600);
}
