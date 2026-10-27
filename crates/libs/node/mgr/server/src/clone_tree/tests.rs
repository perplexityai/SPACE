use super::*;
use std::os::unix::fs::{PermissionsExt, symlink};

struct Fixture {
    _temp: tempfile::TempDir,
    cache: std::path::PathBuf,
    run: std::path::PathBuf,
    outside: std::path::PathBuf,
    identity: CloneDirectoryIdentity,
}
impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        let run = temp.path().join("run");
        let outside = temp.path().join("outside");
        for path in [
            cache.join("donor/instance"),
            cache.join("donor/snapshot"),
            cache.join("target"),
            run.clone(),
            outside.clone(),
        ] {
            std::fs::create_dir_all(path).unwrap();
        }
        std::fs::set_permissions(cache.join("target"), std::fs::Permissions::from_mode(0o700))
            .unwrap();
        std::fs::write(cache.join("donor/instance/rootfs.ext4"), b"rootfs").unwrap();
        std::fs::write(cache.join("donor/snapshot/vmstate"), b"vmstate").unwrap();
        std::fs::write(cache.join("donor/snapshot/memory"), b"memory").unwrap();
        std::fs::write(outside.join("sentinel"), b"untouched").unwrap();
        let metadata = std::fs::metadata(cache.join("target")).unwrap();
        let identity = CloneDirectoryIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        Self {
            _temp: temp,
            cache,
            run,
            outside,
            identity,
        }
    }
    fn open(&self) -> io::Result<CloneTree> {
        CloneTree::open(&self.cache, &self.run, "donor", "target", &self.identity)
    }
    fn outside_untouched(&self) {
        assert_eq!(
            std::fs::read(self.outside.join("sentinel")).unwrap(),
            b"untouched"
        );
        assert_eq!(std::fs::read_dir(&self.outside).unwrap().count(), 1);
    }
}

// The native tests replace only the btrfs ioctl; all real containment opens,
// copies, permission updates and publication checks execute unchanged.
fn snapshot_fixture(source: &File, target: &File) -> io::Result<()> {
    let instance = create_directory(target, "instance")?;
    copy_file(
        &regular_file(source, "rootfs.ext4")?,
        &instance,
        "rootfs.ext4",
        nix::unistd::getgid().as_raw(),
    )
}
fn copy(tree: CloneTree) -> io::Result<()> {
    let gid = nix::unistd::getgid().as_raw();
    tree.copy_with_snapshot(gid, gid, snapshot_fixture)
}

#[test]
fn clone_rejects_replaced_roots_links_special_files_and_preexisting_outputs() {
    for case in [
        "root_link",
        "root_replaced",
        "donor_link",
        "snapshot_link",
        "file_link",
        "hardlink",
        "fifo",
        "output_link",
    ] {
        let mut fixture = Fixture::new();
        match case {
            "root_link" => {
                std::fs::remove_dir(fixture.cache.join("target")).unwrap();
                symlink(&fixture.outside, fixture.cache.join("target")).unwrap();
                // Even a caller supplying the symlink target's inode cannot
                // authorize traversing a symlink at the sandbox boundary.
                let meta = std::fs::metadata(&fixture.outside).unwrap();
                fixture.identity = CloneDirectoryIdentity {
                    device: meta.dev(),
                    inode: meta.ino(),
                };
            }
            "root_replaced" => {
                std::fs::rename(fixture.cache.join("target"), fixture.cache.join("old")).unwrap();
                std::fs::create_dir(fixture.cache.join("target")).unwrap();
            }
            "donor_link" | "snapshot_link" => {
                let entry = if case == "donor_link" {
                    "donor"
                } else {
                    "donor/snapshot"
                };
                std::fs::rename(fixture.cache.join(entry), fixture.cache.join("old")).unwrap();
                symlink(&fixture.outside, fixture.cache.join(entry)).unwrap();
            }
            "file_link" | "hardlink" | "fifo" => {
                let memory = fixture.cache.join("donor/snapshot/memory");
                std::fs::remove_file(&memory).unwrap();
                match case {
                    "file_link" => symlink(fixture.outside.join("sentinel"), memory).unwrap(),
                    "hardlink" => {
                        std::fs::hard_link(fixture.outside.join("sentinel"), memory).unwrap()
                    }
                    _ => nix::unistd::mkfifo(&memory, Mode::from_bits_truncate(0o600)).unwrap(),
                }
            }
            "output_link" => {
                symlink(&fixture.outside, fixture.cache.join("target/snapshot")).unwrap()
            }
            _ => unreachable!(),
        }
        let result = fixture.open().and_then(copy);
        result.expect_err(case);
        fixture.outside_untouched();
    }
}

#[test]
fn clone_keeps_descriptors_pinned_across_directory_replacement() {
    for replace_target in [false, true] {
        let fixture = Fixture::new();
        let tree = fixture.open().unwrap();
        let entry = if replace_target {
            "target"
        } else {
            "donor/snapshot"
        };
        std::fs::rename(fixture.cache.join(entry), fixture.cache.join("moved")).unwrap();
        symlink(&fixture.outside, fixture.cache.join(entry)).unwrap();
        let result = copy(tree);
        if replace_target {
            result.expect_err("must not publish a replacement destination");
        } else {
            result.expect("copy must keep reading the original pinned donor files");
            assert_eq!(
                std::fs::read(fixture.cache.join("target/snapshot/vmstate")).unwrap(),
                b"vmstate"
            );
            assert_eq!(
                std::fs::read(fixture.cache.join("target/snapshot/memory")).unwrap(),
                b"memory"
            );
        }
        fixture.outside_untouched();
    }
}

#[test]
fn rejected_clone_disposal_preserves_replacement_victims_and_is_idempotent() {
    for case in [
        "root_link",
        "root_replaced",
        "descendant_link",
        "partial_copy",
        "root_replaced_during_disposal",
        "runtime_appeared",
        "shared_complete",
        "shared_graft",
        "unbound",
    ] {
        let fixture = Fixture::new();
        std::fs::create_dir(fixture.outside.join("instance")).unwrap();
        std::fs::write(fixture.outside.join("instance/rootfs.ext4"), b"victim data").unwrap();
        match case {
            "root_link" | "root_replaced" => {
                std::fs::rename(fixture.cache.join("target"), fixture.cache.join("original"))
                    .unwrap();
                if case == "root_link" {
                    symlink(&fixture.outside, fixture.cache.join("target")).unwrap();
                } else {
                    std::fs::rename(&fixture.outside, fixture.cache.join("target")).unwrap();
                }
            }
            "descendant_link" => {
                symlink(&fixture.outside, fixture.cache.join("target/snapshot")).unwrap()
            }
            "partial_copy" | "root_replaced_during_disposal" | "runtime_appeared" => {
                let tree = fixture.open().unwrap();
                let gid = nix::unistd::getgid().as_raw();
                tree.copy_with_snapshot(gid, gid, |source, target| {
                    snapshot_fixture(source, target)?;
                    Err(io::Error::other("injected failure after partial clone"))
                })
                .unwrap_err();
            }
            "shared_complete" | "shared_graft" => {
                copy(fixture.open().unwrap()).unwrap();
                if case == "shared_graft" {
                    // The real clone opens group access before its RPC reply.
                    // A lost reply can schedule disposal after a cache writer
                    // has moved an unrelated real tree beneath that same root.
                    std::fs::rename(
                        &fixture.outside,
                        fixture.cache.join("target/snapshots/foreign"),
                    )
                    .unwrap();
                }
            }
            "unbound" => {}
            _ => unreachable!(),
        }
        if case != "partial_copy" && case != "unbound" {
            fixture
                .open()
                .and_then(copy)
                .expect_err("clone must reject tampering");
        }
        if case == "runtime_appeared" {
            std::fs::create_dir(fixture.run.join("target")).unwrap();
            std::fs::write(fixture.run.join("target/runtime"), b"unexpected runtime").unwrap();
        }
        let expected = (case != "unbound").then_some(&fixture.identity);
        let result = crate::clone_cleanup::dispose_with_hook(
            &fixture.cache,
            &fixture.run,
            "target",
            expected,
            || {
                if case == "root_replaced_during_disposal" {
                    std::fs::rename(fixture.cache.join("target"), fixture.cache.join("original"))
                        .unwrap();
                    symlink(&fixture.outside, fixture.cache.join("target")).unwrap();
                }
            },
        );
        if matches!(
            case,
            "root_link"
                | "root_replaced"
                | "unbound"
                | "root_replaced_during_disposal"
                | "runtime_appeared"
                | "shared_complete"
                | "shared_graft"
        ) {
            result.expect_err("missing or replaced identity requires a hold");
            assert!(fixture.cache.join("target").exists());
            if case == "runtime_appeared" {
                assert_eq!(
                    std::fs::read(fixture.cache.join("target/instance/rootfs.ext4")).unwrap(),
                    b"rootfs"
                );
            }
        } else {
            result.unwrap();
            crate::clone_cleanup::dispose(&fixture.cache, &fixture.run, "target", expected)
                .unwrap();
            assert!(!fixture.cache.join("target").exists());
        }
        let victim = if case == "shared_graft" {
            fixture.cache.join("target/snapshots/foreign")
        } else if case == "root_replaced" {
            fixture.cache.join("target")
        } else {
            fixture.outside.clone()
        };
        assert_eq!(
            std::fs::read(victim.join("instance/rootfs.ext4")).unwrap(),
            b"victim data",
            "{case}"
        );
        assert_eq!(
            std::fs::read(victim.join("sentinel")).unwrap(),
            b"untouched",
            "{case}"
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires a disposable root container to run a distinct cache-service UID"]
fn private_clone_blocks_foreign_uid_grafts_and_shared_clone_is_held() {
    use std::os::unix::process::CommandExt;
    assert_eq!(
        nix::unistd::geteuid().as_raw(),
        0,
        "run in a disposable root container"
    );
    let fixture = Fixture::new();
    for path in [
        fixture._temp.path(),
        fixture.cache.as_path(),
        fixture.outside.as_path(),
    ] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o2775)).unwrap();
    }
    let imported = fixture.cache.join("target/foreign");
    let graft = || {
        std::process::Command::new("/bin/mv")
            .arg(&fixture.outside)
            .arg(&imported)
            .uid(65534)
            .gid(nix::unistd::getgid().as_raw())
            .output()
            .unwrap()
    };
    // Different UID, same shared-cache group: the kernel denies traversal of
    // the private root. The peer can neither graft children nor chmod the root.
    assert!(!graft().status.success());
    fixture.outside_untouched();
    copy(fixture.open().unwrap()).unwrap();
    let result = graft();
    assert!(
        result.status.success(),
        "shared-cache graft failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(matches!(
        crate::clone_cleanup::dispose(
            &fixture.cache,
            &fixture.run,
            "target",
            Some(&fixture.identity)
        ),
        Err(crate::clone_cleanup::CloneDisposalError::SharedRoot)
    ));
    assert_eq!(
        std::fs::read(imported.join("sentinel")).unwrap(),
        b"untouched"
    );
}

/// A clone keeps the producer record restore selects its consumer from;
/// without it, the clone's memory would read as the initial environment's.
#[test]
fn clone_copies_the_donors_snapshot_producer_when_it_has_one() {
    for recorded in [true, false] {
        let fixture = Fixture::new();
        if recorded {
            std::fs::write(
                fixture.cache.join("donor/snapshot/runtime.json"),
                b"{\"producer\":\"donor\"}",
            )
            .unwrap();
        }
        copy(fixture.open().unwrap()).unwrap();
        let copied = fixture.cache.join("target/snapshot/runtime.json");
        assert_eq!(copied.exists(), recorded);
        if recorded {
            assert_eq!(std::fs::read(&copied).unwrap(), b"{\"producer\":\"donor\"}");
        }
        assert_eq!(
            std::fs::read(fixture.cache.join("target/snapshot/vmstate")).unwrap(),
            b"vmstate"
        );
        fixture.outside_untouched();
    }
}

/// The producer record is held to the same containment as the snapshot it
/// describes: a link in its place fails the clone.
#[test]
fn clone_refuses_a_linked_producer_record() {
    let fixture = Fixture::new();
    symlink(
        fixture.outside.join("sentinel"),
        fixture.cache.join("donor/snapshot/runtime.json"),
    )
    .unwrap();
    assert!(fixture.open().is_err());
    fixture.outside_untouched();
}
