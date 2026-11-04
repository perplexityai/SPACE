use super::*;
use std::collections::BTreeSet;
use std::os::unix::fs::{PermissionsExt, chown, symlink};
use std::process::Command;
use std::time::Duration;

use nix::unistd::{Gid, Uid, chroot, setgid, setgroups, setuid};

#[test]
#[ignore = "requires root to isolate the filesystem and assume real service identities"]
fn preflight_checks_real_permissions_without_replacing_live_socket() {
    const CHILD: &str = "SPACE_TEST_SOCKET_PREFLIGHT_ROOT";
    const CASE: &str = "SPACE_TEST_SOCKET_PREFLIGHT_CASE";
    if let Some(root) = std::env::var_os(CHILD) {
        let case = std::env::var(CASE).unwrap();
        chroot(Path::new(&root)).unwrap();
        std::env::set_current_dir("/").unwrap();
        let groups = if case == "missing-group" {
            vec![]
        } else {
            vec![Gid::from_raw(965), Gid::from_raw(966)]
        };
        setgroups(&groups).unwrap();
        setgid(Gid::from_raw(960)).unwrap();
        setuid(Uid::from_raw(950)).unwrap();
        exercise(&case).expect("preflight fixture");
        println!("preflight assertions completed: {case}");
        return;
    }

    assert!(
        nix::unistd::geteuid().is_root(),
        "run this test as root; it uses an isolated filesystem and real service IDs"
    );
    for case in [
        "valid",
        "missing-group",
        "wrong-group-mapping",
        "unsafe-installation-link",
    ] {
        let root = tempfile::tempdir().unwrap();
        let directory = |path: &str, uid, gid, mode| {
            let path = root.path().join(path);
            fs::create_dir_all(&path).unwrap();
            chown(&path, Some(uid), Some(gid)).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
        };
        directory("", 0, 0, 0o755);
        directory("etc", 0, 0, 0o755);
        let gid = if case == "wrong-group-mapping" {
            966
        } else {
            965
        };
        fs::write(
            root.path().join("etc/group"),
            format!("space-volcredclients:x:{gid}:\n"),
        )
        .unwrap();
        fs::write(root.path().join("etc/nsswitch.conf"), "group: files\n").unwrap();
        for (path, uid, gid, mode) in [
            ("run/volume", 950, 965, 0o750),
            ("run/bad-mode", 950, 965, 0o770),
            ("run/bad-owner", 964, 965, 0o750),
            ("run/bad-group", 950, 952, 0o750),
            ("install/run/volume", 950, 965, 0o750),
            ("writable/run/volume", 950, 965, 0o750),
            ("service-owned/run/volume", 950, 965, 0o750),
        ] {
            directory(path, uid, gid, mode);
        }
        directory("writable", 0, 0, 0o777);
        directory("service-owned", 950, 0, 0o755);
        symlink("/run/volume", root.path().join("run/redirect")).unwrap();
        symlink("/install", root.path().join("redirected")).unwrap();
        directory("untrusted", 0, 0, 0o777);
        symlink("/install", root.path().join("untrusted/redirect")).unwrap();
        let installation = if case == "unsafe-installation-link" {
            "/untrusted/redirect"
        } else {
            "/install"
        };
        symlink(installation, root.path().join("space")).unwrap();
        fs::write(root.path().join("run/volume/regular"), "preserve").unwrap();
        symlink("regular", root.path().join("run/volume/link")).unwrap();

        let mut command = Command::new(std::env::current_exe().unwrap());
        command.args(["--exact", "run::volume_socket::tests::preflight_checks_real_permissions_without_replacing_live_socket", "--include-ignored", "--nocapture"])
            .env(CHILD, root.path()).env(CASE, case);
        let (_, output) =
            pplx_space_test_env::run_command(&mut command, Duration::from_secs(20)).unwrap();
        assert!(
            output.status.success(),
            "{case}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .contains(&format!("preflight assertions completed: {case}"))
        );
    }
}

fn exercise(case: &str) -> io::Result<()> {
    let preflight = |path: &Path| super::preflight(path, Path::new("/run/standard.sock"));
    let path = Path::new(if case == "unsafe-installation-link" {
        "/space/run/volume/credmgr.sock"
    } else {
        "/run/volume/credmgr.sock"
    });
    let listener = UnixListener::bind(path)?;
    let inode = fs::symlink_metadata(path)?.ino();
    let entries = || -> io::Result<BTreeSet<_>> {
        fs::read_dir(path.parent().expect("socket parent"))?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect()
    };
    let before = entries()?;
    let result = preflight(path);
    if case == "valid" {
        result?;
        let redirected = Path::new("/space/run/volume/credmgr.sock");
        let resolved = Path::new("/install/run/volume/credmgr.sock");
        assert!(
            super::preflight(redirected, resolved).is_err(),
            "accepted aliased standard and volume sockets"
        );
        assert!(super::preflight(path, Path::new("/run/volume/../volume/credmgr.sock")).is_err());
        super::preflight(path, resolved)?;
        super::preflight(redirected, Path::new("/install/run/volume/other.sock"))?;
        super::preflight(path, Path::new("/missing/credmgr.sock"))?;
        std::env::set_current_dir("/install/run/volume")?;
        assert!(super::preflight(redirected, Path::new("credmgr.sock")).is_err());
        std::env::set_current_dir("/")?;
        preflight(Path::new("/space/run/volume/absent.sock"))?;
        assert!(!Path::new("/space/run/volume/absent.sock").exists());
        for invalid in [
            "/run/missing/socket",
            "/run/bad-mode/socket",
            "/run/bad-owner/socket",
            "/run/bad-group/socket",
            "/run/redirect/socket",
            "/redirected/run/volume/socket",
            "/writable/run/volume/socket",
            "/service-owned/run/volume/socket",
            "/run/volume/regular",
            "/run/volume/link",
            "/run/volume/../volume/socket",
        ] {
            assert!(preflight(Path::new(invalid)).is_err(), "accepted {invalid}");
        }
        let too_long = format!("/run/volume/{}", "x".repeat(108));
        assert!(preflight(Path::new(&too_long)).is_err());
    } else {
        let error = result.expect_err(case);
        if case == "missing-group" {
            assert!(matches!(
                error.get_ref().and_then(|source| source.downcast_ref::<pplx_space_common_svc::bootstrap::ApplyUnixSocketPermissionsError>()),
                Some(pplx_space_common_svc::bootstrap::ApplyUnixSocketPermissionsError::SetGroup { .. })
            ), "the real chgrp must fail: {error:?}");
        }
    }
    assert_eq!(
        fs::symlink_metadata(path)?.ino(),
        inode,
        "preflight replaced the live listener"
    );
    assert_eq!(entries()?, before, "preflight left probe files behind");
    assert_eq!(fs::read_to_string("/run/volume/regular")?, "preserve");
    // Connecting anew proves that the original listener remains reachable.
    let connection = std::os::unix::net::UnixStream::connect(path)?;
    listener.accept()?;
    drop(connection);
    Ok(())
}
