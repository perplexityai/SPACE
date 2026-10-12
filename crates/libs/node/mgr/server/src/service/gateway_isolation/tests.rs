use super::*;

#[cfg(target_os = "linux")]
#[test]
fn isolated_gateway_cannot_access_vm_resources() {
    use std::os::unix::fs::{PermissionsExt, chown};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::os::unix::process::CommandExt;
    use std::process::Command;

    const CHILD_ROOT: &str = "SPACE_TEST_GATEWAY_ISOLATION_ROOT";
    if let Some(root) = std::env::var_os(CHILD_ROOT) {
        let root = PathBuf::from(root);
        if std::env::var_os("SPACE_TEST_GATEWAY_MANAGER").is_some() {
            UnixStream::connect(root.join("vm/netgtw-control.sock")).unwrap();
            assert_eq!(
                std::fs::read(root.join("cache/memory")).unwrap(),
                b"private snapshot"
            );
            return;
        }
        for socket in ["vm/v.sock", "vm/firecracker.sock", "other/control.sock"] {
            assert_eq!(
                UnixStream::connect(root.join(socket)).unwrap_err().kind(),
                std::io::ErrorKind::PermissionDenied
            );
        }
        assert_eq!(
            std::fs::read(root.join("cache/memory")).unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            std::fs::write(root.join("vm/fc.pid"), b"bad")
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::PermissionDenied
        );
        let pid: i32 = std::env::var("SPACE_TEST_VM_PID").unwrap().parse().unwrap();
        let handle = launch::ProcessHandle::open(u32::try_from(pid).unwrap()).unwrap();
        handle
            .program_matches("/bin/sleep")
            .expect_err("unreadable live executable must not be classified as absent");
        assert_eq!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).unwrap_err(),
            nix::errno::Errno::EPERM
        );
        let listener = UnixListener::bind(root.join("gateways/own/control.sock")).unwrap();
        UnixStream::connect(root.join("gateways/own/control.sock")).unwrap();
        drop(listener);
        return;
    }
    if !pplx_space_test_env::root_or_skip("run probes as the gateway and sandbox uids") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let owner = 10_042;
    let isolated = gateway::gateway_uid(LinuxUid::new(owner)).unwrap().get();
    for (name, uid, gid, mode) in [
        ("vm", owner, gids::SPACE_SBXMGR, 0o750),
        ("cache", owner, gids::SPACE_CACHE_RW, 0o2775),
        ("other", isolated + 1, isolated + 1, 0o700),
    ] {
        let path = root.path().join(name);
        std::fs::create_dir(&path).unwrap();
        chown(&path, Some(uid), Some(gid)).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }
    ensure_sandbox_artifact_dir_permissions(root.path().join("cache")).unwrap();
    std::fs::write(root.path().join("cache/memory"), b"private snapshot").unwrap();
    let listeners = ["v.sock", "firecracker.sock"].map(|name| {
        let path = root.path().join("vm").join(name);
        let listener = UnixListener::bind(&path).unwrap();
        chown(&path, Some(owner), Some(gids::SPACE_SBXMGR)).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o770)).unwrap();
        listener
    });
    let alias = root.path().join("vm/netgtw-control.sock");
    let target = root.path().join("gateways/own/control.sock");
    let legacy = UnixListener::bind(&alias).unwrap();
    prepare_gateway_socket_at(&alias, &target, isolated, true)
        .expect_err("a live legacy listener must survive incomplete process inventory");
    assert!(!alias.is_symlink());
    UnixStream::connect(&alias).unwrap();
    drop(legacy);
    prepare_gateway_socket_at(&alias, &target, isolated, true).unwrap();
    prepare_gateway_socket_at(&alias, &target, isolated, true).unwrap();
    assert_eq!(std::fs::read_link(&alias).unwrap(), target);

    fn drop_identity(command: &mut Command, uid: u32, gid: u32, groups: Vec<nix::unistd::Gid>) {
        // SAFETY: only credential-changing syscalls run between fork and exec.
        unsafe {
            command.pre_exec(move || {
                nix::unistd::setgroups(&groups)?;
                nix::unistd::setgid(nix::unistd::Gid::from_raw(gid))?;
                nix::unistd::setuid(nix::unistd::Uid::from_raw(uid))?;
                Ok(())
            });
        }
    }
    let mut vm_command = Command::new("sleep");
    vm_command.arg("30");
    drop_identity(&mut vm_command, owner, owner, vec![]);
    let mut vm = vm_command.spawn().unwrap();
    let handle = launch::ProcessHandle::open(vm.id()).unwrap();
    assert_eq!(handle.credentials_match(owner, owner).unwrap(), Some(true));
    assert_eq!(
        existing_sidecar_uid(
            &handle,
            paths::NET_GATEWAY_SVC_BIN,
            isolated,
            isolated,
            owner
        )
        .unwrap(),
        Some(owner),
        "adoption keeps a legacy gateway's identity when isolation is enabled"
    );
    existing_sidecar_uid(
        &handle,
        paths::SPACED_HOST_SVC_BIN,
        isolated,
        isolated,
        owner,
    )
    .unwrap_err();
    assert_eq!(
        handle.credentials_match(isolated, isolated).unwrap(),
        Some(false)
    );
    let mut probe = Command::new(std::env::current_exe().unwrap());
    probe
        .args([
            "--exact",
            "service::gateway_isolation::tests::isolated_gateway_cannot_access_vm_resources",
            "--nocapture",
        ])
        .env(CHILD_ROOT, root.path())
        .env("SPACE_TEST_VM_PID", vm.id().to_string());
    drop_identity(&mut probe, isolated, isolated, vec![]);
    let result = probe.output();
    vm.kill().unwrap();
    vm.wait().unwrap();
    let result = result.unwrap();
    assert!(
        result.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    // Existing clients still dial the original path after the gateway binds.
    std::fs::remove_file(&target).unwrap();
    let listener = UnixListener::bind(&target).unwrap();
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o770)).unwrap();
    let mut manager = Command::new(std::env::current_exe().unwrap());
    manager
        .args([
            "--exact",
            "service::gateway_isolation::tests::isolated_gateway_cannot_access_vm_resources",
            "--nocapture",
        ])
        .env(CHILD_ROOT, root.path())
        .env("SPACE_TEST_GATEWAY_MANAGER", "1");
    drop_identity(
        &mut manager,
        2001,
        gids::SPACE_SBXMGR,
        vec![
            nix::unistd::Gid::from_raw(gids::SPACE_SIDECARCLIENTS),
            nix::unistd::Gid::from_raw(gids::SPACE_CACHE_RW),
        ],
    );
    let result = manager.output().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    prepare_gateway_socket_at(&alias, &target, owner, false)
        .expect_err("rollback must not unlink a live isolated listener");
    assert!(alias.is_symlink());
    drop(listener);
    drop(listeners);
    prepare_gateway_socket_at(&alias, &target, owner, false).unwrap();
    assert!(!alias.is_symlink());
}

#[cfg(target_os = "linux")]
#[test]
fn exited_gateway_does_not_abort_recovery() {
    let mut child = std::process::Command::new("sleep")
        .arg("30")
        .spawn()
        .unwrap();
    let handle = launch::ProcessHandle::open(child.id()).unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    let identity =
        existing_sidecar_uid(&handle, paths::NET_GATEWAY_SVC_BIN, 10_042, 10_042, 10_042)
            .expect("a process that disappears during inspection is absent, not foreign");
    assert_eq!(identity, None);
    assert_eq!(handle.program_matches("/bin/sleep").unwrap(), None);
}

#[test]
fn launch_identity_is_bound_to_role_and_allocated_sandbox() {
    let owner = 10_042;
    let isolated = gateway::gateway_uid(LinuxUid::new(owner)).unwrap().get();
    for (program, uid, gid, allowed) in [
        (paths::NET_GATEWAY_SVC_BIN, owner, owner, true),
        (paths::NET_GATEWAY_SVC_BIN, isolated, isolated, true),
        (paths::NET_GATEWAY_SVC_BIN, isolated, owner, false),
        (
            paths::NET_GATEWAY_SVC_BIN,
            isolated + 1,
            isolated + 1,
            false,
        ),
        (paths::NET_GATEWAY_SVC_BIN, 0, 0, false),
        (paths::SPACED_HOST_SVC_BIN, owner, owner, true),
        (paths::SPACED_HOST_SVC_BIN, isolated, isolated, false),
    ] {
        assert_eq!(
            sidecar_launch_uid(program, uid, gid, owner, "sandbox").is_ok(),
            allowed,
            "program={program} uid={uid} gid={gid}"
        );
    }
}
