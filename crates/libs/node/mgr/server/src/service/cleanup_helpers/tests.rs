use super::{
    CLEANUP_QUIESCENCE_TIMEOUT, stderr_has_missing_path, stderr_indicates_not_subvolume,
    wait_for_cleanup_quiescence_with,
};
use std::cell::Cell;
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
    assert!(
        probe(id).is_err(),
        "unreadable mapper inventory is not absence"
    );
    super::host_device_stack_exists(&root.path().join("unreadable"), &mapper_dir, &cache, id)
        .unwrap_err();
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
                || super::teardown_encrypted_volumes_with(sandbox_id, &mut output, &mut mounts),
            )
            .unwrap_err();
            assert!(error.to_string().contains(failed_command));
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
