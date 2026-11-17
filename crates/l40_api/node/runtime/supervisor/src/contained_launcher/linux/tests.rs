use super::*;
use pplx_space_host_layout::identities::gids;
use std::{
    fs::Permissions,
    os::unix::fs::{PermissionsExt, symlink},
};

#[test]
fn runner_drops_privileges_without_procfs() {
    if !pplx_space_test_env::root_or_skip("chroot and drop contained runner privileges") {
        return;
    }
    const CHILD_ROOT: &str = "SPACE_TEST_CONTAINED_RUNNER_ROOT";
    if let Some(root) = std::env::var_os(CHILD_ROOT) {
        // The runner has already discarded host procfs and must release the
        // child before the child can mount its replacement.
        nix::unistd::chroot(Path::new(&root)).unwrap();
        nix::unistd::chdir("/").unwrap();
        assert!(!Path::new("/proc/self/oom_score_adj").exists());
        drop_privileges(10001, 10001).unwrap();
        assert_eq!(Uid::effective(), Uid::from_raw(10001));
        assert_eq!(Gid::effective(), Gid::from_raw(10001));
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "contained_launcher::linux::tests::runner_drops_privileges_without_procfs",
            "--nocapture",
        ])
        .env(CHILD_ROOT, root.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

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

#[test]
fn contained_profiles_limit_devices_and_control_to_the_runtime() {
    assert_eq!(Uid::effective(), Uid::from_raw(0), "requires root fixture");
    const CHILD_ROOT: &str = "SPACE_TEST_CONTAINED_DEVICE_ROOT";
    const CHILD_RUNTIME: &str = "SPACE_TEST_CONTAINED_DEVICE_RUNTIME";
    if let Some(root) = std::env::var_os(CHILD_ROOT) {
        // Device creation uses a pinned cwd after pivot_root. Isolate that cwd
        // change in a child, not in the multithreaded test process.
        std::env::set_current_dir(root).unwrap();
        nix::sys::stat::umask(Mode::from_bits_truncate(0o077));
        let runtime = std::env::var(CHILD_RUNTIME).unwrap().parse().unwrap();
        prepare_devices(runtime, 10001, 10001).unwrap();
        let mut arguments = vec!["--version".to_owned()];
        let control = prepare_runtime_control(runtime, &mut arguments).unwrap();
        for name in ["null", "urandom"] {
            let metadata = std::fs::metadata(name).unwrap();
            assert_eq!((metadata.uid(), metadata.gid()), (10001, 10001));
            assert_eq!(metadata.mode() & 0o777, 0o600);
        }
        match runtime {
            RuntimeKind::CloudHypervisor => {
                assert_eq!(std::fs::metadata("kvm").unwrap().rdev(), makedev(10, 232));
                assert_eq!(
                    std::fs::metadata("net/tun").unwrap().rdev(),
                    makedev(10, 200)
                );
                let listener = control.unwrap();
                let _client = UnixStream::connect("control.sock").unwrap();
                listener.accept().unwrap();
                assert!(
                    !nix::fcntl::FdFlag::from_bits_truncate(
                        nix::fcntl::fcntl(&listener, nix::fcntl::FcntlArg::F_GETFD).unwrap()
                    )
                    .contains(nix::fcntl::FdFlag::FD_CLOEXEC)
                );
                let metadata = std::fs::metadata("control.sock").unwrap();
                assert_eq!(metadata.uid(), 0);
                assert_eq!(metadata.mode() & 0o077, 0);
            }
            RuntimeKind::Wasmtime => {
                // Userspace isolation must not confer access to hardware
                // virtualization, host network devices or a native VMM API.
                let names: std::collections::BTreeSet<_> = std::fs::read_dir(".")
                    .unwrap()
                    .map(|entry| entry.unwrap().file_name())
                    .collect();
                assert_eq!(
                    names,
                    ["null", "urandom"].map(std::ffi::OsString::from).into()
                );
                assert!(control.is_none());
                assert_eq!(arguments, ["--version"]);
            }
            RuntimeKind::Firecracker | RuntimeKind::Gvisor => {
                unreachable!("these runtimes do not use standalone containment")
            }
        }
        return;
    }

    for runtime in [RuntimeKind::CloudHypervisor, RuntimeKind::Wasmtime] {
        let root = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "contained_launcher::linux::tests::contained_profiles_limit_devices_and_control_to_the_runtime",
                "--nocapture",
            ])
            .env(CHILD_ROOT, root.path())
            .env(CHILD_RUNTIME, runtime.as_str())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{runtime}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn userspace_roots_cannot_be_replaced_by_a_sandbox_identity() {
    assert_eq!(Uid::effective(), Uid::from_raw(0), "requires root fixture");
    let root = tempfile::tempdir().unwrap();
    for (owner, mode, wasi, cloud_hypervisor) in [
        (0, 0o755, true, false),
        (0, 0o775, false, false),
        (0, 0o757, false, false),
        (10001, 0o700, false, true),
        (10002, 0o755, false, false),
    ] {
        nix::unistd::chown(
            root.path(),
            Some(Uid::from_raw(owner)),
            Some(Gid::from_raw(10001)),
        )
        .unwrap();
        std::fs::set_permissions(root.path(), Permissions::from_mode(mode)).unwrap();
        let metadata = root.path().metadata().unwrap();
        for (runtime, accepted) in [
            (RuntimeKind::Wasmtime, wasi),
            (RuntimeKind::CloudHypervisor, cloud_hypervisor),
        ] {
            assert_eq!(
                validate_root_metadata(runtime, 10001, &metadata).is_ok(),
                accepted,
                "{runtime}, owner={owner}, mode={mode:o}"
            );
        }
    }
}
