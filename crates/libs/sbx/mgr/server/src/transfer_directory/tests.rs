use super::*;

#[test]
fn invalid_transfer_paths_cannot_create_directories() {
    let root = tempfile::tempdir().unwrap();
    for relative in ["", ".", "../outside", "operation/../../outside", "/outside"] {
        let error = prepare_under(root.path(), Path::new(relative)).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput, "{relative}");
    }
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    assert_eq!(
        prepare(root.path()).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires root to switch between two real service UIDs"]
fn distinct_service_uids_can_exchange_downloads() {
    use std::process::Command;
    use std::time::Duration;

    use nix::unistd::{Uid, chown};
    use pplx_space_common_core::host::linux::identities::users;

    const STAGE: &str = "SPACE_TEST_TRANSFER_STAGE";
    const ROOT: &str = "SPACE_TEST_TRANSFER_ROOT";
    let relative = Path::new("cache/operation/attempt");
    if let Ok(stage) = std::env::var(STAGE) {
        let root = std::path::PathBuf::from(std::env::var_os(ROOT).unwrap());
        let path = root.join(relative);
        match stage.as_str() {
            "prepare" => {
                assert_eq!(geteuid().as_raw(), users::SPACE_SBXMGR.uid);
                let umask = std::env::var("SPACE_TEST_TRANSFER_UMASK").unwrap();
                nix::sys::stat::umask(Mode::from_bits_truncate(
                    u32::from_str_radix(&umask, 8).unwrap(),
                ));
                let original_owner = std::fs::metadata(&path).ok().map(|metadata| metadata.uid());
                prepare_under(&root, relative).unwrap();
                let metadata = std::fs::metadata(&path).unwrap();
                assert_eq!(
                    metadata.uid(),
                    original_owner.unwrap_or(users::SPACE_SBXMGR.uid)
                );
                assert_eq!(metadata.gid(), groups::SPACE_CACHE_RW.gid);
                assert_ne!(
                    metadata.mode() & 0o2000,
                    0,
                    "downloads must inherit the shared group"
                );
                let link = root.join("cache/link");
                std::os::unix::fs::symlink(&path, &link).unwrap();
                for linked_path in [Path::new("cache/link"), Path::new("cache/link/child")] {
                    assert!(
                        prepare_under(&root, linked_path).is_err(),
                        "do not follow directory links"
                    );
                }
                assert!(
                    !path.join("child").exists(),
                    "do not create through an ancestor link"
                );
                std::fs::remove_file(link).unwrap();
            }
            "missing-group" => {
                assert_eq!(
                    prepare_under(&root, relative).unwrap_err().kind(),
                    std::io::ErrorKind::PermissionDenied
                );
                assert_eq!(
                    std::fs::metadata(&path).unwrap().mode() & 0o2000,
                    0,
                    "the kernel removes setgid without membership"
                );
            }
            "prefix-link" => {
                let target = root.join("cache-target");
                let before = std::fs::metadata(&target).unwrap();
                assert!(prepare_under(&root, relative).is_err());
                assert!(!target.join("operation").exists());
                let after = std::fs::metadata(&target).unwrap();
                assert_eq!(
                    (after.uid(), after.gid(), after.mode()),
                    (before.uid(), before.gid(), before.mode())
                );
            }
            "denied" => {
                let before = std::fs::metadata(&path).unwrap();
                assert_eq!(
                    prepare_under(&root, relative).unwrap_err().kind(),
                    std::io::ErrorKind::PermissionDenied
                );
                let after = std::fs::metadata(&path).unwrap();
                assert_eq!(
                    (after.uid(), after.gid(), after.mode()),
                    (before.uid(), before.gid(), before.mode())
                );
            }
            "download" => {
                assert_eq!(geteuid().as_raw(), users::SPACE_VOLMGR.uid);
                // Atomic pull promotion and its provenance both need directory write access.
                for name in ["rootfs", "rootfs.provenance"] {
                    let temp = path.join(format!("{name}.partial"));
                    std::fs::write(&temp, b"downloaded").unwrap();
                    std::fs::set_permissions(&temp, std::fs::Permissions::from_mode(0o640))
                        .unwrap();
                    assert_eq!(
                        std::fs::metadata(&temp).unwrap().gid(),
                        groups::SPACE_CACHE_RW.gid
                    );
                    std::fs::rename(&temp, path.join(name)).unwrap();
                }
                // An already-correct directory owned by a sibling needs no ownership change.
                prepare_under(&root, relative).unwrap();
            }
            "consume" => {
                assert_eq!(geteuid().as_raw(), users::SPACE_SBXMGR.uid);
                for name in ["rootfs", "rootfs.provenance"] {
                    assert_eq!(std::fs::read(path.join(name)).unwrap(), b"downloaded");
                    std::fs::remove_file(path.join(name)).unwrap();
                }
            }
            _ => panic!("unexpected test stage"),
        }
        println!("verified transfer stage {stage}");
        return;
    }
    assert!(geteuid().is_root(), "run this test with a root runner");
    for (name, parent_mode, existing, first_stage) in [
        ("inherited", 0o2770, None, "prepare"),
        ("plain", 0o0775, None, "prepare"),
        ("restrictive-umask", 0o0775, None, "prepare"),
        ("installation-link", 0o0775, None, "prepare"),
        ("prefix-link", 0o0775, None, "prefix-link"),
        (
            "wrong-group",
            0o2770,
            Some((users::SPACE_SBXMGR.uid, groups::SPACE_SBXMGR.gid, 0o2775)),
            "prepare",
        ),
        (
            "root-ready",
            0o2770,
            Some((0, groups::SPACE_CACHE_RW.gid, 0o2775)),
            "prepare",
        ),
        (
            "root-wrong-group",
            0o2770,
            Some((0, groups::SPACE_SBXMGR.gid, 0o2775)),
            "denied",
        ),
        (
            "root-missing-mode",
            0o2770,
            Some((0, groups::SPACE_CACHE_RW.gid, 0o2755)),
            "denied",
        ),
        (
            "missing-membership",
            0o0775,
            Some((users::SPACE_SBXMGR.uid, groups::SPACE_CACHE_RW.gid, 0o0755)),
            "missing-group",
        ),
        (
            "missing-membership-wrong-group",
            0o0775,
            Some((users::SPACE_SBXMGR.uid, groups::SPACE_SBXMGR.gid, 0o0755)),
            "denied",
        ),
        (
            "untrusted-root",
            0o0775,
            Some((users::SPACE_SBXMGR.uid, groups::SPACE_CACHE_RW.gid, 0o2775)),
            "denied",
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        let cache = root.path().join("cache");
        std::fs::create_dir(&cache).unwrap();
        chown(
            &cache,
            None,
            Some(Gid::from_raw(groups::SPACE_CACHE_RW.gid)),
        )
        .unwrap();
        std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(parent_mode)).unwrap();
        if let Some((uid, gid, mode)) = existing {
            let path = root.path().join(relative);
            std::fs::create_dir_all(&path).unwrap();
            chown(&path, Some(Uid::from_raw(uid)), Some(Gid::from_raw(gid))).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        }
        let installation = if name == "installation-link" {
            let link = root.path().join("alias");
            std::os::unix::fs::symlink(root.path(), &link).unwrap();
            link
        } else {
            root.path().to_owned()
        };
        if name == "untrusted-root" {
            chown(
                root.path(),
                Some(Uid::from_raw(users::SPACE_SBXMGR.uid)),
                None,
            )
            .unwrap();
        }
        if name == "prefix-link" {
            let target = root.path().join("cache-target");
            std::fs::rename(&cache, &target).unwrap();
            std::os::unix::fs::symlink(target, &cache).unwrap();
        }
        let stages = if first_stage == "prepare" {
            vec![
                ("prepare", users::SPACE_SBXMGR.uid),
                ("download", users::SPACE_VOLMGR.uid),
                ("consume", users::SPACE_SBXMGR.uid),
            ]
        } else {
            vec![(first_stage, users::SPACE_SBXMGR.uid)]
        };
        for (stage, uid) in stages {
            let (primary_gid, supplementary) = if uid == users::SPACE_SBXMGR.uid {
                (
                    groups::SPACE_SBXMGR.gid,
                    format!(
                        "{},{}",
                        groups::SPACE_SBXCLIENTS.gid,
                        groups::SPACE_CACHE_RW.gid
                    ),
                )
            } else {
                (
                    groups::SPACE_VOLMGR.gid,
                    format!(
                        "{},{},{}",
                        groups::SPACE_SBXCLIENTS.gid,
                        groups::SPACE_CACHE_RW.gid,
                        groups::SPACE_VOLCREDCLIENTS.gid
                    ),
                )
            };
            let supplementary = if name.starts_with("missing-membership") {
                groups::SPACE_SBXCLIENTS.gid.to_string()
            } else {
                supplementary
            };
            let mut command = Command::new("setpriv");
            command
                .arg(format!("--reuid={uid}"))
                .arg(format!("--regid={primary_gid}"))
                .arg(format!("--groups={supplementary}"))
                .arg("--")
                .arg(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "transfer_directory::tests::distinct_service_uids_can_exchange_downloads",
                    "--include-ignored",
                    "--nocapture",
                ])
                .env(STAGE, stage)
                .env(
                    "SPACE_TEST_TRANSFER_UMASK",
                    if name == "restrictive-umask" {
                        "077"
                    } else {
                        "022"
                    },
                )
                .env(ROOT, &installation);
            let (_, output) =
                pplx_space_test_env::run_command(&mut command, Duration::from_secs(30)).unwrap();
            assert!(
                output.status.success(),
                "{name}/{stage}: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                String::from_utf8_lossy(&output.stdout)
                    .contains(&format!("verified transfer stage {stage}"))
            );
        }
    }
}
