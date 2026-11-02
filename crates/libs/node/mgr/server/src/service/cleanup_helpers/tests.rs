use super::{
    CLEANUP_QUIESCENCE_TIMEOUT, stderr_has_missing_path, stderr_indicates_not_subvolume,
    wait_for_cleanup_quiescence_with,
};
use std::cell::{Cell, RefCell};
use std::time::Duration;

#[test]
fn incomplete_device_setup_is_discovered_without_a_cow_marker() {
    let root = tempfile::tempdir().unwrap();
    let sys_block = root.path().join("sys-block");
    let mapper_dir = root.path().join("mapper");
    std::fs::create_dir(&sys_block).unwrap();
    let id = "018f03b0-1832-7000-8000-000000000001";
    let real_cache = root.path().join("real-cache");
    std::fs::create_dir(&real_cache).unwrap();
    let real_cache = real_cache.canonicalize().unwrap();
    let cache = root.path().join("cache");
    std::os::unix::fs::symlink(&real_cache, &cache).unwrap();
    let probe = |id| super::host_device_stack_exists(&sys_block, &mapper_dir, &cache, id);
    assert!(!probe(id).unwrap());
    let loop_dir = sys_block.join("loop7/loop");
    std::fs::create_dir_all(&loop_dir).unwrap();
    std::fs::write(
        loop_dir.join("backing_file"),
        format!("/unrelated/{id}/instance/rootfs.ext4\n"),
    )
    .unwrap();
    assert!(
        !probe(id).unwrap(),
        "a UUID component does not prove ownership"
    );
    for spelling in [&cache, &real_cache] {
        for suffix in ["", " (deleted)"] {
            std::fs::write(
                loop_dir.join("backing_file"),
                format!("{}/{id}/instance/rootfs.ext4{suffix}\n", spelling.display()),
            )
            .unwrap();
            assert!(probe(id).unwrap());
        }
    }
    assert!(!probe("another").unwrap());
    std::fs::remove_file(loop_dir.join("backing_file")).unwrap();
    std::fs::create_dir(&mapper_dir).unwrap();
    for (name, owned) in [
        (format!("sbxroot-{id}"), true),
        (format!("sbxroot-{id}-cow"), true),
        (format!("sbxroot-{id}-fork-source"), true),
        (format!("sbxroot-{id}-fork-target"), true),
        (format!("sbxvol-{id}-snapshot"), true),
        (format!("sbxroot-{id}-unrelated"), false),
        ("sbxroot-another".into(), false),
        ("control".into(), false),
    ] {
        let node = mapper_dir.join(&name);
        std::os::unix::fs::symlink("missing-dm-device", &node).unwrap();
        assert_eq!(probe(id).unwrap(), owned, "{name}");
        std::fs::remove_file(node).unwrap();
    }
    std::fs::remove_dir(&mapper_dir).unwrap();
    std::fs::write(&mapper_dir, b"not a directory").unwrap();
    let error = probe(id).expect_err("unreadable mapper inventory is not absence");
    assert_names_path(&error, &mapper_dir);
    let unreadable = root.path().join("unreadable");
    let error = super::host_device_stack_exists(&unreadable, &mapper_dir, &cache, id).unwrap_err();
    assert_names_path(&error, &unreadable);
}

#[test]
fn device_scan_rescans_attributes_of_devices_being_removed() {
    #[derive(Clone, Copy, PartialEq)]
    enum BetweenPasses {
        Stays,
        Disappears,
        /// Another device takes the same name, as when one is created and
        /// removed in a loop.
        Replaced,
    }
    struct Case {
        name: &'static str,
        device: &'static str,
        value: String,
        /// Reads answering `errno` before the attribute reads `value`.
        failures: usize,
        errno: i32,
        between_passes: BetweenPasses,
        owned_mapper_node: bool,
        /// Another device starts failing after the first pass and keeps
        /// failing, as during a mass delete.
        later_removal: bool,
        /// `None`: the scan fails, naming the attribute.
        expected: Option<bool>,
        expected_pauses: usize,
    }
    let id = "018f03b0-1832-7000-8000-000000000001";
    let unrelated = || Case {
        name: "unrelated dm device removed mid-scan",
        device: "dm-3",
        value: "sbxroot-another".into(),
        failures: 1,
        errno: libc::EINVAL,
        between_passes: BetweenPasses::Disappears,
        owned_mapper_node: false,
        later_removal: false,
        expected: Some(false),
        expected_pauses: 1,
    };
    let cases = [
        Case {
            name: "unrelated loop device detached mid-scan",
            device: "loop4",
            value: "/unrelated/backing.img".into(),
            errno: libc::ENODEV,
            ..unrelated()
        },
        Case {
            name: "no device being removed",
            failures: 0,
            expected_pauses: 0,
            ..unrelated()
        },
        Case {
            name: "another removal starting after the first pass",
            later_removal: true,
            ..unrelated()
        },
        Case {
            name: "name reused by successive removals",
            failures: usize::MAX,
            between_passes: BetweenPasses::Replaced,
            ..unrelated()
        },
        Case {
            name: "same device failing on every pass",
            failures: usize::MAX,
            between_passes: BetweenPasses::Stays,
            expected: None,
            expected_pauses: super::SYS_BLOCK_RESCAN_DELAYS.len(),
            ..unrelated()
        },
        Case {
            name: "own dm device that answers on a rescan",
            value: format!("sbxroot-{id}-cow"),
            errno: libc::ENODEV,
            between_passes: BetweenPasses::Stays,
            expected: Some(true),
            ..unrelated()
        },
        Case {
            name: "own mapper node beside an unrelated removal",
            owned_mapper_node: true,
            expected: Some(true),
            ..unrelated()
        },
        unrelated(),
    ];
    for case in cases {
        let root = tempfile::tempdir().unwrap();
        let sys_block = root.path().join("sys-block");
        let mapper_dir = root.path().join("mapper");
        std::fs::create_dir_all(&mapper_dir).unwrap();
        if case.owned_mapper_node {
            std::os::unix::fs::symlink("../dm-0", mapper_dir.join(format!("sbxroot-{id}")))
                .unwrap();
        }
        let device = sys_block.join(case.device);
        let probe = if case.device.starts_with("loop") {
            "loop/backing_file"
        } else {
            "dm/name"
        };
        let plant = |device: &std::path::Path| {
            let attribute = device.join(probe);
            std::fs::create_dir_all(attribute.parent().unwrap()).unwrap();
            std::fs::write(attribute, format!("{}\n", case.value)).unwrap();
        };
        plant(&device);
        let attribute = device.join(probe);
        let later = sys_block.join("dm-9");
        let reads = Cell::new(0);
        let pauses = RefCell::new(Vec::new());
        let result = super::host_device_stack_exists_with(
            &sys_block,
            &mapper_dir,
            &root.path().join("cache"),
            id,
            |path| {
                if path == attribute && reads.replace(reads.get() + 1) < case.failures {
                    return Err(std::io::Error::from_raw_os_error(case.errno));
                }
                if path.starts_with(&later) {
                    return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
                }
                std::fs::read_to_string(path)
            },
            |delay| {
                pauses.borrow_mut().push(delay);
                if case.later_removal {
                    plant(&later);
                }
                match case.between_passes {
                    BetweenPasses::Stays => {}
                    BetweenPasses::Disappears => {
                        if device.exists() {
                            std::fs::remove_dir_all(&device).unwrap();
                        }
                    }
                    BetweenPasses::Replaced => {
                        // Build the successor before removing the device so
                        // that it cannot reuse the device's inode.
                        let successor = sys_block.join("successor");
                        plant(&successor);
                        std::fs::remove_dir_all(&device).unwrap();
                        std::fs::rename(&successor, &device).unwrap();
                    }
                }
            },
        );
        match case.expected {
            Some(expected) => assert_eq!(result.unwrap(), expected, "{}", case.name),
            None => {
                assert_names_path(&result.unwrap_err(), &attribute);
                assert_eq!(
                    reads.get(),
                    super::SYS_BLOCK_RESCAN_DELAYS.len() + 1,
                    "{}",
                    case.name
                );
            }
        }
        assert_eq!(
            pauses.into_inner(),
            super::SYS_BLOCK_RESCAN_DELAYS[..case.expected_pauses],
            "{}",
            case.name
        );
    }
}

/// Device inventory errors must name the path that failed, which is usually
/// another device's, not the sandbox being cleaned.
fn assert_names_path(error: &std::io::Error, path: &std::path::Path) {
    let message = error.to_string();
    assert!(message.contains(&path.display().to_string()), "{message}");
}

#[tokio::test(start_paused = true)]
async fn cleanup_wait_observes_cgroup_tail_after_tracked_pids_exit() {
    let started = tokio::time::Instant::now();
    let tracked_observations = Cell::new(0);
    let cgroup_observations = Cell::new(0);

    let live = wait_for_cleanup_quiescence_with(
        CLEANUP_QUIESCENCE_TIMEOUT,
        || {
            tracked_observations.set(tracked_observations.get() + 1);
            Vec::new()
        },
        || {
            cgroup_observations.set(cgroup_observations.get() + 1);
            if started.elapsed() < Duration::from_millis(2_200) {
                vec!["cgroup still populated (non-leader task present)".to_string()]
            } else {
                Vec::new()
            }
        },
    )
    .await;

    assert!(live.is_empty());
    assert!(started.elapsed() >= Duration::from_millis(2_200));
    assert!(started.elapsed() < CLEANUP_QUIESCENCE_TIMEOUT);
    assert!(tracked_observations.get() > 1);
    assert_eq!(tracked_observations.get(), cgroup_observations.get());
}

#[tokio::test(start_paused = true)]
async fn cleanup_wait_bounds_persistent_cgroup_population() {
    let started = tokio::time::Instant::now();
    let expected = vec!["cgroup still populated (non-leader task present)".to_string()];

    let live =
        wait_for_cleanup_quiescence_with(CLEANUP_QUIESCENCE_TIMEOUT, Vec::new, || expected.clone())
            .await;

    assert_eq!(live, expected);
    assert!(started.elapsed() >= CLEANUP_QUIESCENCE_TIMEOUT);
}

#[test]
fn stderr_not_subvolume_matches_btrfs_getflags_failure() {
    // Plain dir on btrfs (EINVAL) and non-btrfs filesystem (ENOTTY) — both
    // emit the same prefix with a varying errno tail.
    assert!(stderr_indicates_not_subvolume(
        "ERROR: Could not get subvolume flags: Invalid argument"
    ));
    assert!(stderr_indicates_not_subvolume(
        "ERROR: Could not get subvolume flags: Inappropriate ioctl for device"
    ));
    // Case-insensitive across btrfs-progs spelling variants.
    assert!(stderr_indicates_not_subvolume(
        "ERROR: Not a Btrfs subvolume"
    ));
    assert!(stderr_indicates_not_subvolume(
        "ERROR: not a btrfs subvolume"
    ));
    assert!(stderr_indicates_not_subvolume(
        "ERROR: not a subvolume: /space/cache/x/snapshots/imported"
    ));
}

#[test]
fn stderr_not_subvolume_ignores_transient_failures() {
    // A real subvolume never returns the getflags message; transient/IO
    // failures surface differently and must propagate, not be skipped.
    assert!(!stderr_indicates_not_subvolume(
        "ERROR: could not open: Input/output error"
    ));
    assert!(!stderr_indicates_not_subvolume(
        "ERROR: cannot access '/space/cache/x': No such file or directory"
    ));
}

#[test]
fn stderr_missing_path_matches_multiline_find_output() {
    let stderr = "\
find: '/tmp/cache': Permission denied
chmod: cannot access '/tmp/cache/instance': No such file or directory
find: '/tmp/cache/other': Permission denied";

    assert!(stderr_has_missing_path(stderr));
}

#[test]
fn stderr_missing_path_ignores_non_enoent_output() {
    let stderr = "\
find: '/tmp/cache': Permission denied
chmod: changing permissions of '/tmp/cache/instance': Operation not permitted";

    assert!(!stderr_has_missing_path(stderr));
}

/// A warm-pool member's composite must be torn down on delete like any other.
/// It rides a plaintext COW, so a marker keyed on the encrypted COW name alone
/// skips the teardown and strands the mapper plus both loop devices.
#[test]
fn rootfs_device_stack_is_detected_for_both_plaintext_and_encrypted_cows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let instance = dir.path().join("instance");
    std::fs::create_dir_all(&instance).expect("create instance dir");

    // Booted straight off the rootfs file: no COW, so nothing to release.
    std::fs::write(instance.join("rootfs.ext4"), b"").expect("write rootfs");
    assert!(
        !super::device_teardown_needed(&instance).unwrap(),
        "a sandbox with no COW has no device stack to tear down"
    );

    // Warm-pool member, pre-claim.
    let plain = instance.join(crate::rootfs_dmcrypt::POOL_COW_FILE);
    std::fs::write(&plain, b"").expect("write plaintext cow");
    assert!(
        super::device_teardown_needed(&instance).unwrap(),
        "a plaintext COW is a dm-snapshot composite and must be torn down"
    );
    std::fs::remove_file(&plain).expect("remove plaintext cow");

    // Encrypted sandbox, or a claimed member after the re-home.
    std::fs::write(
        instance.join(super::super::encrypted_volumes::ROOTFS_COW_FILE),
        b"",
    )
    .expect("write encrypted cow");
    assert!(
        super::device_teardown_needed(&instance).unwrap(),
        "an encrypted COW must be torn down"
    );
}

#[test]
fn failed_device_teardown_preserves_artifacts_for_retry() {
    use crate::host_command::Command;
    use std::os::unix::process::ExitStatusExt;
    use std::process::{ExitStatus, Output};

    for (backing, failed_command) in ["cow.plain", "cow.luks", "snapshot.luks"]
        .into_iter()
        .flat_map(|backing| ["ls", "mknodes", "mounts"].map(|command| (backing, command)))
    {
        let sandbox_id = "018f03b0-1832-7000-8000-000000000001";
        let mapper = format!("sbxroot-{sandbox_id}");
        let root = tempfile::tempdir().unwrap();
        let instance = root.path().join("instance");
        let run = root.path().join("run");
        let gateway = root.path().join("gateway");
        let jail = root.path().join("jail");
        let mapper_node = root.path().join(&mapper);
        std::fs::write(&mapper_node, b"stale device node").unwrap();
        let kernel_device_live = Cell::new(true);
        let failures_remaining = Cell::new(2);
        let mut output = |command: &mut Command, _: Duration, _: &[u8]| {
            let args: Vec<_> = command
                .get_args()
                .map(|arg| arg.to_str().unwrap())
                .collect();
            let program = command.get_program().to_str().unwrap();
            if program == "dmsetup" && args == [failed_command] && failures_remaining.get() > 0 {
                failures_remaining.set(failures_remaining.get() - 1);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    failed_command,
                ));
            }
            let stdout = match (program, args.as_slice()) {
                ("dmsetup", ["ls"]) if kernel_device_live.get() => {
                    format!("{mapper}\t(253:7)\n").into_bytes()
                }
                ("dmsetup", ["ls"]) | ("losetup", ["-a"]) => Vec::new(),
                ("dmsetup", ["status", name]) if *name == mapper => {
                    if failed_command == "mounts" {
                        b"0 1024 crypt aes-xts-plain64\n".to_vec()
                    } else {
                        b"0 1024 snapshot 0/1024 8\n".to_vec()
                    }
                }
                ("dmsetup", ["remove", "--retry", name]) | ("cryptsetup", ["close", name])
                    if *name == mapper =>
                {
                    kernel_device_live.set(false);
                    Vec::new()
                }
                ("dmsetup", ["mknodes"]) => {
                    assert!(!kernel_device_live.get());
                    if mapper_node.exists() {
                        std::fs::remove_file(&mapper_node).unwrap();
                    }
                    Vec::new()
                }
                _ => panic!("unexpected teardown command: {command:?}"),
            };
            Ok(Output {
                status: ExitStatus::from_raw(0),
                stdout,
                stderr: Vec::new(),
            })
        };
        for path in [&instance, &gateway, &run, &jail] {
            std::fs::create_dir(path).unwrap();
        }
        std::fs::write(instance.join(backing), b"backing").unwrap();
        std::fs::write(run.join("fc.pid"), b"identity").unwrap();
        let mut mounts = |_: &str| -> std::io::Result<Vec<String>> {
            // A missing mapper node is not proof that the kernel device or its
            // mounts are gone. Cleanup must retain the error but still close.
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "mounts"))
        };
        for _ in 0..if failed_command == "mounts" { 1 } else { 2 } {
            let error = super::cleanup_runtime_artifacts_at(
                gateway.to_str().unwrap(),
                run.to_str().unwrap(),
                jail.to_str().unwrap(),
                &instance,
                |_| Ok(false),
                || super::teardown_encrypted_volumes_with(sandbox_id, &mut output, &mut mounts),
            )
            .unwrap_err();
            assert!(error.to_string().contains(failed_command));
            let super::JailCleanupError::DeviceTeardown(super::RuntimeArtifactCleanupError(errors)) =
                &error
            else {
                panic!("device teardown failures must retain their error list: {error:?}");
            };
            assert!(!errors.is_empty());
            assert!(run.join("fc.pid").exists());
            assert!(gateway.exists());
            assert!(jail.exists());
            assert!(instance.join(backing).exists());
            assert_eq!(mapper_node.exists(), failed_command != "mounts");
            assert_eq!(kernel_device_live.get(), failed_command == "ls");
        }
        super::cleanup_runtime_artifacts_at(
            gateway.to_str().unwrap(),
            run.to_str().unwrap(),
            jail.to_str().unwrap(),
            &instance,
            |_| Ok(false),
            || super::teardown_encrypted_volumes_with(sandbox_id, &mut output, &mut mounts),
        )
        .unwrap();
        assert!(!mapper_node.exists());
        assert!(!kernel_device_live.get());
        assert!(!run.exists());
        assert!(!gateway.exists());
        assert!(!jail.exists());
    }
}

#[test]
fn teardown_drains_snapshot_binds_before_device_release_or_recursive_removal() {
    for fail_at in [Some(2), None] {
        let directory = tempfile::tempdir().unwrap();
        let jail = directory.path().join("jail");
        let snapshot = jail.join("root/encsnap/memory");
        let instance = directory.path().join("instance");
        let run = directory.path().join("run");
        let gateway = directory.path().join("gateway");
        for path in [snapshot.parent().unwrap(), &instance, &run, &gateway] {
            std::fs::create_dir_all(path).unwrap();
        }
        std::fs::write(&snapshot, b"retained snapshot").unwrap();
        std::fs::write(instance.join("snapshot.luks"), b"backing").unwrap();
        let layers = Cell::new(2_usize);
        let mut calls = 0;
        let released = Cell::new(false);
        let result = super::cleanup_runtime_artifacts_at(
            gateway.to_str().unwrap(),
            run.to_str().unwrap(),
            jail.to_str().unwrap(),
            &instance,
            |path| {
                if !path.ends_with("encsnap") {
                    return Ok(false);
                }
                calls += 1;
                if fail_at == Some(calls) {
                    return Err("injected lower-layer unmount failure".into());
                }
                let remaining = layers.get();
                layers.set(remaining.saturating_sub(1));
                Ok(remaining != 0)
            },
            || {
                assert_eq!(
                    layers.get(),
                    0,
                    "device release requires every bind drained"
                );
                assert_eq!(std::fs::read(&snapshot).unwrap(), b"retained snapshot");
                released.set(true);
                (Vec::new(), Vec::new())
            },
        );
        if fail_at.is_some() {
            assert!(result.is_err());
            assert!(!released.get());
            assert_eq!(std::fs::read(&snapshot).unwrap(), b"retained snapshot");
            assert!(instance.join("snapshot.luks").exists());
            assert!(run.exists());
            assert!(gateway.exists());
        } else {
            result.unwrap();
            assert!(released.get());
            assert_eq!(layers.get(), 0);
            assert!(!jail.exists());
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn teardown_preserves_backing_files_below_stacked_directory_binds() {
    if !pplx_space_test_env::root_or_skip("test jail cleanup in a private mount namespace") {
        return;
    }
    std::thread::spawn(|| {
        use nix::mount::{MsFlags, mount};
        nix::sched::unshare(nix::sched::CloneFlags::CLONE_NEWNS).unwrap();
        mount(
            None::<&str>,
            "/",
            None::<&str>,
            MsFlags::MS_REC | MsFlags::MS_PRIVATE,
            None::<&str>,
        )
        .unwrap();
        for relative in ["encsnap", "state/encsnap"] {
            let directory = tempfile::tempdir().unwrap();
            let jail = directory.path().join("jail");
            let target = jail.join("root").join(relative);
            let instance = directory.path().join("instance");
            let run = directory.path().join("run");
            let gateway = directory.path().join("gateway");
            let lower = directory.path().join("lower-volume");
            let upper = directory.path().join("upper-volume");
            for path in [&target, &instance, &run, &gateway, &lower, &upper] {
                std::fs::create_dir_all(path).unwrap();
            }
            for volume in [&lower, &upper] {
                std::fs::write(volume.join("memory"), b"snapshot backing data").unwrap();
                mount(
                    Some(volume.as_path()),
                    &target,
                    None::<&str>,
                    MsFlags::MS_BIND,
                    None::<&str>,
                )
                .unwrap();
            }
            super::cleanup_runtime_artifacts_at(
                gateway.to_str().unwrap(),
                run.to_str().unwrap(),
                jail.to_str().unwrap(),
                &instance,
                |path| super::unmount_if_present(path),
                || (Vec::new(), Vec::new()),
            )
            .unwrap();
            for volume in [&lower, &upper] {
                assert_eq!(
                    std::fs::read(volume.join("memory")).unwrap(),
                    b"snapshot backing data"
                );
            }
            assert!(!jail.exists());
        }
    })
    .join()
    .unwrap();
}

#[test]
fn stale_jail_removal_failure_refuses_reuse() {
    let root = tempfile::tempdir().unwrap();
    let stale = root.path().join("jail-root");
    std::fs::write(&stale, b"must not be reused").unwrap();
    let error = super::cleanup_jail_root(stale.to_str().unwrap(), |_| Ok(false), || Ok(Vec::new()))
        .unwrap_err();
    assert!(matches!(error, super::JailCleanupError::Remove(_)));
    assert_eq!(std::fs::read(&stale).unwrap(), b"must not be reused");
    std::fs::remove_file(&stale).unwrap();
    std::fs::create_dir(&stale).unwrap();
    std::fs::write(stale.join("firecracker"), b"stale executable").unwrap();
    for _ in 0..2 {
        super::cleanup_jail_root(stale.to_str().unwrap(), |_| Ok(false), || Ok(Vec::new()))
            .unwrap();
        assert!(!stale.try_exists().unwrap());
    }
}

#[test]
fn stale_mount_cleanup_drains_stacks_and_bounds_failures() {
    for (layers, fail_at, expected_calls, succeeds) in [
        (0, None, 1, true),
        (1, None, 2, true),
        (3, None, 4, true),
        (31, None, 32, true),
        (32, None, 32, false),
        (33, None, 32, false),
        (usize::MAX, None, 32, false),
        (3, Some(2), 2, false),
    ] {
        let mut calls = 0;
        let mut remaining = layers;
        let result =
            super::drain_stale_mounts(std::path::Path::new("/synthetic/jail/rootfs"), |_| {
                calls += 1;
                if fail_at == Some(calls) {
                    return Err("injected unmount failure".into());
                }
                let mounted = remaining != 0;
                remaining = remaining.saturating_sub(1);
                Ok(mounted)
            });
        assert_eq!(result.is_ok(), succeeds);
        assert_eq!(calls, expected_calls);
        if let Err(error) = result {
            assert_eq!(tonic::Status::from(error).code(), tonic::Code::Internal);
        }
        if fail_at.is_none() && layers != usize::MAX {
            let retry =
                super::drain_stale_mounts(std::path::Path::new("/synthetic/jail/rootfs"), |_| {
                    let mounted = remaining != 0;
                    remaining = remaining.saturating_sub(1);
                    Ok(mounted)
                });
            retry.expect("retry must finish draining a finite mount stack");
            assert_eq!(remaining, 0);
        }
    }
}

#[test]
fn encrypted_bind_failure_preserves_stale_jail_contents() {
    let root = tempfile::tempdir().unwrap();
    let jail = root.path().join("jail");
    std::fs::create_dir_all(jail.join("encsnap")).unwrap();
    let snapshot = jail.join("encsnap/memory");
    std::fs::write(&snapshot, b"snapshot retained").unwrap();
    let mut visited = Vec::new();
    let error = super::cleanup_jail_root(
        jail.to_str().unwrap(),
        |path| {
            let name = path.file_name().unwrap().to_str().unwrap().to_owned();
            visited.push(name.clone());
            if name == "encsnap" {
                Err("injected unmount failure".into())
            } else {
                Ok(false)
            }
        },
        || Ok(Vec::new()),
    )
    .unwrap_err();
    assert!(matches!(error, super::JailCleanupError::Unmount { .. }));
    assert_eq!(visited, ["rootfs.ext4", "memory", "encsnap"]);
    assert_eq!(std::fs::read(snapshot).unwrap(), b"snapshot retained");
    let mut encrypted_layers = 2;
    super::cleanup_jail_root(
        jail.to_str().unwrap(),
        |path| {
            if path.ends_with("encsnap") && encrypted_layers > 0 {
                encrypted_layers -= 1;
                Ok(true)
            } else {
                Ok(false)
            }
        },
        || Ok(Vec::new()),
    )
    .unwrap();
    assert_eq!(encrypted_layers, 0);
    assert!(!jail.exists());
}

#[test]
fn jail_cleanup_removes_planted_symlinks_without_following_them() {
    enum Target {
        Loop,
        Missing,
        Outside,
    }
    for name in super::JAIL_BIND_NAMES {
        for target in [Target::Loop, Target::Missing, Target::Outside] {
            let temp = tempfile::tempdir().unwrap();
            let jail = temp.path().join("jail");
            let instance = temp.path().join("instance");
            let outside = temp.path().join("outside");
            let link = jail.join("root").join(name);
            std::fs::create_dir_all(link.parent().unwrap()).unwrap();
            std::fs::create_dir_all(&instance).unwrap();
            std::fs::create_dir_all(&outside).unwrap();
            std::fs::write(outside.join("memory"), b"retained backing data").unwrap();
            let destination = match target {
                Target::Loop => std::path::PathBuf::from(link.file_name().unwrap()),
                Target::Missing => temp.path().join("absent"),
                Target::Outside => outside.clone(),
            };
            std::os::unix::fs::symlink(destination, &link).unwrap();
            super::cleanup_runtime_artifacts_at(
                temp.path().join("gateway").to_str().unwrap(),
                temp.path().join("run").to_str().unwrap(),
                jail.to_str().unwrap(),
                &instance,
                |path| {
                    // No real mounts: exercise link inspection, including children
                    // under a planted parent link, without requiring mount authority.
                    if path.starts_with(&link) {
                        super::unmount_if_present(path)
                    } else {
                        Ok(false)
                    }
                },
                || panic!("no encrypted device exists in this fixture"),
            )
            .unwrap();
            assert!(!jail.exists());
            assert_eq!(
                std::fs::read(outside.join("memory")).unwrap(),
                b"retained backing data"
            );
        }
    }
}
