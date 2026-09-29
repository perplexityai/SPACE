use super::*;

#[test]
fn pull_directory_preserves_setgid_and_allows_sibling_writes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("snapshot");
    std::fs::create_dir(&path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o2755)).unwrap();
    prepare(&path).unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o7777,
        0o2775
    );
    let link = root.path().join("link");
    std::os::unix::fs::symlink(&path, &link).unwrap();
    assert!(prepare(&link).is_err(), "do not chmod a symlink target");
}

#[test]
#[ignore = "requires root to switch between two real service UIDs"]
fn distinct_service_uids_can_exchange_downloads() {
    use std::os::unix::fs::MetadataExt;
    use std::process::Command;
    use std::time::Duration;

    use nix::unistd::{Gid, chown, geteuid};
    use pplx_space_common_core::host::linux::identities::{groups, users};

    const STAGE: &str = "SPACE_TEST_TRANSFER_STAGE";
    const ROOT: &str = "SPACE_TEST_TRANSFER_ROOT";
    if let Ok(stage) = std::env::var(STAGE) {
        let root = std::path::PathBuf::from(std::env::var_os(ROOT).unwrap());
        let path = root.join("operation/attempt");
        match stage.as_str() {
            "prepare" => {
                assert_eq!(geteuid().as_raw(), users::SPACE_SBXMGR.uid);
                nix::sys::stat::umask(nix::sys::stat::Mode::from_bits_truncate(0o022));
                prepare(&path).unwrap();
                let metadata = std::fs::metadata(&path).unwrap();
                assert_eq!(metadata.gid(), groups::SPACE_CACHE_RW.gid);
                assert_ne!(metadata.mode() & 0o2000, 0, "preserve setgid inheritance");
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
                // A retry must accept an already-writable directory owned by the sibling UID.
                prepare(&path).unwrap();
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
    let root = tempfile::tempdir().unwrap();
    chown(
        root.path(),
        None,
        Some(Gid::from_raw(groups::SPACE_CACHE_RW.gid)),
    )
    .unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o2770)).unwrap();
    for (stage, uid) in [
        ("prepare", users::SPACE_SBXMGR.uid),
        ("download", users::SPACE_VOLMGR.uid),
        ("consume", users::SPACE_SBXMGR.uid),
    ] {
        let primary_gid = if uid == users::SPACE_SBXMGR.uid {
            groups::SPACE_SBXMGR.gid
        } else {
            groups::SPACE_VOLMGR.gid
        };
        let mut command = Command::new("setpriv");
        command
            .arg(format!("--reuid={uid}"))
            .arg(format!("--regid={primary_gid}"))
            .arg(format!("--groups={}", groups::SPACE_CACHE_RW.gid))
            .arg("--")
            .arg(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "transfer_directory::tests::distinct_service_uids_can_exchange_downloads",
                "--include-ignored",
                "--nocapture",
            ])
            .env(STAGE, stage)
            .env(ROOT, root.path());
        let (_, output) =
            pplx_space_test_env::run_command(&mut command, Duration::from_secs(30)).unwrap();
        assert!(
            output.status.success(),
            "{stage}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .contains(&format!("verified transfer stage {stage}"))
        );
    }
}
