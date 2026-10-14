use super::*;
use uuid::Uuid;

fn sid() -> SandboxId {
    SandboxId::from_uuid(Uuid::parse_str("019d84bd-280d-7331-99c6-b63377f3a3a2").unwrap())
}

#[test]
fn mapper_name_is_derived_and_distinct_from_rootfs() {
    let s = sid();
    assert_eq!(
        mapper_name(&s, "snapshot"),
        "sbxvol-019d84bd-280d-7331-99c6-b63377f3a3a2-snapshot"
    );
    // Distinct from the rootfs mapper (sbxroot-<id>) and within the dm
    // 127-char name limit even with a volume name appended.
    assert!(mapper_name(&s, "snapshot").starts_with("sbxvol-"));
    assert!(mapper_name(&s, "snapshot").len() < 128);
}

#[tokio::test]
async fn fresh_and_restore_source_are_mutually_exclusive() {
    // A fresh capture recreates its backing; pairing it with a restore
    // source would silently discard the downloaded snapshot. Rejected
    // before any device work.
    let err = prepare(PrepareParams {
        sandbox_id: sid(),
        volume_name: "snapshot".to_string(),
        backing_path: PathBuf::from("/nonexistent/instance/snapshot.luks"),
        restore_source: Some(PathBuf::from("/nonexistent/snapshot/snapshot.luks")),
        mount_dir: PathBuf::from("/nonexistent/snapshot-vol"),
        size_bytes: 1,
        dek: b"dek",
        fresh: true,
    })
    .await
    .expect_err("fresh + restore_source must be rejected");
    assert!(matches!(err, DmCryptError::InvalidParams(_)), "{err:?}");
}

#[test]
fn backing_size_floors_at_btrfs_minimum() {
    // A tiny request is floored to the btrfs minimum.
    assert_eq!(volume_backing_size_bytes(1), MIN_VOLUME_BYTES);
    // A large request gets headroom on top.
    let big = 4 * 1024 * 1024 * 1024;
    assert_eq!(volume_backing_size_bytes(big), big + VOLUME_HEADROOM_BYTES);
}

/// Fix 1: the unwind enumerates the device's mountpoints from mountinfo —
/// canonical dir AND jail bind, nothing hardcoded — latest-mounted first, and
/// matches by device identity. btrfs reports an anonymous `0:N` filesystem
/// device in mountinfo, so `/dev/dm-N` must be matched to dmsetup's minor.
#[test]
fn unwind_enumerates_device_mounts_latest_first_by_identity() {
    let mapper = "sbxvol-01a0a702-518c-7211-a938-72bfa87751b0-snapshot";
    let dev = format!("/dev/mapper/{mapper}");
    let mountinfo = "\
        801 25 0:851 / /space/cache/sandboxes/01a0a702/snapshot-vol rw - btrfs /dev/dm-5 rw,compress-force=zstd\n\
        802 25 259:1 / /space/cache rw - btrfs /dev/nvme0n1p1 rw\n\
        803 25 0:851 / /space/cache/firecracker/jails/firecracker/01a0a702/root/encsnap rw - btrfs /dev/dm-5 rw,compress-force=zstd\n";
    let expected = vec![
        "/space/cache/firecracker/jails/firecracker/01a0a702/root/encsnap".to_string(),
        "/space/cache/sandboxes/01a0a702/snapshot-vol".to_string(),
    ];
    // btrfs may report an anonymous superblock device rather than the block
    // mapper's major:minor, including on hosts without /dev/dm-N nodes.
    for info in [mountinfo.to_owned(), mountinfo.replace("0:851", "253:5")] {
        assert_eq!(
            mountpoints_for_mapper_identity(&info, mapper, MapperIdentity::Present((253, 5)))
                .unwrap(),
            expected
        );
    }
    // dm-N spelling defeats the path fallback alone...
    assert!(mountpoints_of_device(mountinfo, &dev, None).is_empty());
    // ...which still works when the source was recorded as the mapper path.
    let by_path = mountinfo.replace("/dev/dm-5", &dev);
    assert_eq!(
        mountpoints_for_mapper_identity(&by_path, mapper, MapperIdentity::Absent).unwrap(),
        expected
    );
    // An unresolved identity is unknown, not proof that there are no mapper
    // mounts: teardown must stop before close/force-free.
    assert!(matches!(
        mountpoints_for_mapper_identity(
            mountinfo,
            mapper,
            MapperIdentity::Error(DmCryptError::Command {
                cmd: "dmsetup info".into(),
                stderr: "timed out".into(),
            })
        ),
        Err(DmCryptError::Command { .. })
    ));
}

/// Fix 2: a mapping is debris when its loop's backing file is deleted or is
/// no longer the current backing inode; a healthy mapping is left alone.
#[test]
fn mapper_probe_classifies_absent_vs_error() {
    // "does not exist" is a POSITIVE absence (name freed / renamed away);
    // anything else is an unresolved probe that must never destroy.
    assert!(matches!(
        classify_mapper_probe(Ok((253, 5))),
        MapperIdentity::Present((253, 5))
    ));
    assert!(matches!(
        classify_mapper_probe(Err(DmCryptError::Command {
            cmd: "dmsetup info".into(),
            stderr: "Device sbxvol-x-snapshot does not exist.".into(),
        })),
        MapperIdentity::Absent
    ));
    assert!(matches!(
        classify_mapper_probe(Err(DmCryptError::Command {
            cmd: "dmsetup info".into(),
            stderr: "dmsetup: connection timed out".into(),
        })),
        MapperIdentity::Error(_)
    ));
}

/// The idempotent-arm decision. Accept requires POSITIVE proof (present mapper
/// AND the mount sits on it); a foreign or gone-name mount rebuilds; a vanished
/// mount device (TOCTOU) or a probe error is Retry — never accept an
/// unverified mount (Firecracker would write plaintext into an unmounted dir),
/// never destroy on an unproven probe.
#[test]
fn idempotent_arm_accepts_only_a_positively_verified_mount() {
    // Present + matching + mounted → accept.
    assert_eq!(
        idempotent_arm(&MapperIdentity::Present((253, 5)), Some((253, 5))),
        IdempotentArm::Accept
    );
    // Present + positively foreign → rebuild.
    assert_eq!(
        idempotent_arm(&MapperIdentity::Present((253, 5)), Some((253, 9))),
        IdempotentArm::Rebuild
    );
    // An anonymous filesystem ID is not a different block device. Without
    // backing-device evidence, retry instead of destroying the mounted volume.
    assert_eq!(
        idempotent_arm(&MapperIdentity::Present((253, 5)), Some((0, 42))),
        IdempotentArm::Retry
    );
    // Present but the mount's device vanished/unresolved (TOCTOU): retry, do
    // NOT accept an unverified mount.
    assert_eq!(
        idempotent_arm(&MapperIdentity::Present((253, 5)), None),
        IdempotentArm::Retry
    );
    // Absent mapper name (force-freed away) plus a positively present mount:
    // the surviving mount is stale and must be detached before rebuilding.
    let present_mount = Some((253, 5));
    assert_eq!(
        idempotent_arm(&MapperIdentity::Absent, present_mount),
        IdempotentArm::Rebuild
    );
    assert_eq!(
        idempotent_arm(&MapperIdentity::Absent, None),
        IdempotentArm::Rebuild
    );
    // Probe error: unknown → retry, never destroy.
    assert_eq!(
        idempotent_arm(
            &MapperIdentity::Error(DmCryptError::Command {
                cmd: "dmsetup info".into(),
                stderr: "timed out".into(),
            }),
            Some((253, 9))
        ),
        IdempotentArm::Retry
    );
}

#[test]
fn mount_device_numbers_reads_the_mountinfo_majmin() {
    let info = "\
        801 25 253:5 / /space/cache/sandboxes/01a0a702/snapshot-vol rw - btrfs /dev/dm-5 rw\n\
        802 25 259:1 / /space/cache rw - btrfs /dev/nvme0n1p1 rw\n";
    assert_eq!(
        mount_device_numbers(info, "/space/cache/sandboxes/01a0a702/snapshot-vol"),
        Some((253, 5))
    );
    assert_eq!(
        mount_device_numbers(info, "/space/cache/sandboxes/other"),
        None
    );
    assert!(mountpoint_is_present(
        info,
        Path::new("/space/cache/sandboxes/01a0a702/snapshot-vol")
    ));
    assert!(!mountpoint_is_present(
        info,
        Path::new("/space/cache/sandboxes/other")
    ));
}

#[test]
fn mount_identity_judges_the_visible_topmost_mount() {
    let info = "\
        801 25 253:5 / /space/cache/sandboxes/01a0a702/snapshot-vol rw - btrfs /dev/dm-5 rw\n\
        802 25 259:1 / /space/cache/sandboxes/01a0a702/snapshot-vol rw - ext4 /dev/nvme0n1p1 rw\n";
    assert_eq!(
        mount_device_numbers(info, "/space/cache/sandboxes/01a0a702/snapshot-vol"),
        Some((259, 1)),
        "the last mountinfo entry is the visible stacked mount"
    );
}

/// A busy close is retried on the backoff schedule and succeeds once the
/// holder lets go; a persistent holder exhausts the schedule; any other
/// failure is returned without retrying. Holders are recorded at the first
/// and the last busy refusal only, and every close that was refused at least
/// once counts its outcome.
#[tokio::test(start_paused = true)]
async fn close_retries_only_busy_refusals_and_records_first_and_last() {
    #[derive(Clone, Copy)]
    enum Refusal {
        Busy,
        Other,
    }
    fn refusal(refusal: Refusal) -> DmCryptError {
        let (cmd, stderr) = (
            "cryptsetup close x".to_string(),
            "Device x is still in use.".into(),
        );
        match refusal {
            Refusal::Busy => DmCryptError::Busy { cmd, stderr },
            // A busy-sounding message without the busy exit status is not
            // retried: the classification is structural.
            Refusal::Other => DmCryptError::Command { cmd, stderr },
        }
    }
    let delays = CLOSE_BUSY_RETRY_DELAYS;
    let all_attempts = delays.len() + 1;

    struct Case {
        refusals: Vec<Option<Refusal>>,
        attempts: Option<usize>,
        recorded: Vec<(usize, bool)>,
        outcome: Option<&'static str>,
        elapsed: std::time::Duration,
    }
    use pplx_space_metrics::byok;
    let cases = [
        Case {
            refusals: vec![None],
            attempts: Some(1),
            recorded: vec![],
            outcome: None,
            elapsed: std::time::Duration::ZERO,
        },
        Case {
            refusals: vec![Some(Refusal::Busy), Some(Refusal::Busy), None],
            attempts: Some(3),
            recorded: vec![(1, false)],
            outcome: Some(byok::CLOSE_BUSY_RECOVERED),
            elapsed: delays[0] + delays[1],
        },
        Case {
            refusals: vec![Some(Refusal::Busy); all_attempts + 3],
            attempts: None,
            recorded: vec![(1, false), (all_attempts, true)],
            outcome: Some(byok::CLOSE_BUSY_EXHAUSTED),
            elapsed: delays.iter().sum(),
        },
        Case {
            refusals: vec![Some(Refusal::Busy), Some(Refusal::Other)],
            attempts: None,
            recorded: vec![(1, false)],
            outcome: Some(byok::CLOSE_BUSY_FAILED),
            elapsed: delays[0],
        },
        Case {
            refusals: vec![Some(Refusal::Other)],
            attempts: None,
            recorded: vec![],
            outcome: None,
            elapsed: std::time::Duration::ZERO,
        },
    ];

    for case in cases {
        let calls = std::cell::Cell::new(0usize);
        let recorded = std::cell::RefCell::new(Vec::new());
        let started = tokio::time::Instant::now();
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let result = close_with_busy_retry(
            || {
                let outcome = case.refusals[calls.get()].map(refusal);
                calls.set(calls.get() + 1);
                async move { outcome.map_or(Ok(()), Err) }
            },
            &delays,
            |attempt, final_attempt, _error| {
                recorded.borrow_mut().push((attempt, final_attempt));
                async {}
            },
        )
        .await;
        let counters = capture.finish();
        assert_eq!(result.ok(), case.attempts);
        assert_eq!(recorded.into_inner(), case.recorded);
        assert_eq!(started.elapsed(), case.elapsed);
        let outcomes: Vec<_> = counters
            .iter()
            .filter(|counter| counter.name == "byok.volume_close.busy")
            .map(|counter| counter.tags.clone())
            .collect();
        let expected: Vec<Vec<String>> = case
            .outcome
            .map(|outcome| vec![format!("outcome:{outcome}")])
            .into_iter()
            .collect();
        assert_eq!(outcomes, expected);
    }
}

/// The logged reason is the last stderr line, not libdevmapper's repeated
/// retry lines.
#[test]
fn busy_reason_is_the_last_stderr_line() {
    let error = DmCryptError::Busy {
        cmd: "cryptsetup close x".into(),
        stderr: "device-mapper: remove ioctl on x  failed: Device or resource busy\n\
                 device-mapper: remove ioctl on x  failed: Device or resource busy\n\
                 Device x is still in use.\n"
            .into(),
    };
    assert_eq!(
        last_stderr_line(&error),
        "`cryptsetup close x`: Device x is still in use."
    );
}

/// Kills and reaps a holder process when the test no longer needs it,
/// including on a failed assertion.
#[cfg(target_os = "linux")]
struct Holder(std::process::Child);

#[cfg(target_os = "linux")]
impl Drop for Holder {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Spawn `sh -c 'wait for $1; exec 7<$1; echo ready; sleep'` in a new mount
/// namespace with `propagation`; `write` opens fd 7 read-write (`7<>`).
/// Returns the holder, its pid, and its stdout for [`wait_ready`].
#[cfg(target_os = "linux")]
fn spawn_holder(
    propagation: &str,
    file: &Path,
    write: bool,
) -> (Holder, u32, std::process::ChildStdout) {
    let script = format!(
        "while [ ! -e \"$1\" ]; do sleep 0.02; done; exec 7{}\"$1\"; echo ready; exec sleep 600",
        if write { "<>" } else { "<" }
    );
    let mut child = std::process::Command::new("unshare")
        .args(["--mount", "--propagation", propagation, "--"])
        .args(["sh", "-c", &script, "sh"])
        .arg(file)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let pid = child.id();
    (Holder(child), pid, stdout)
}

/// Block until a [`spawn_holder`] process has its descriptor open.
#[cfg(target_os = "linux")]
fn wait_ready(stdout: std::process::ChildStdout) {
    use std::io::BufRead;

    let mut ready = String::new();
    std::io::BufReader::new(stdout)
        .read_line(&mut ready)
        .unwrap();
    assert_eq!(ready.trim(), "ready");
}

/// Real dm-crypt + btrfs busy closes, from each kind of holder:
///
/// - A process in a private mount namespace holding a file open, as a
///   `btrfs receive` child could before its namespace became a slave: its
///   mount copy survives the lazy unmount. The report must name the
///   namespace and the descriptor.
/// - A descriptor opened through node-manager's own mount, which the lazy
///   unmount detaches; matched by its recorded mount ID.
/// - A descriptor opened through a copy that reached a slave namespace by
///   propagation, as in Firecracker's jail. The unmount detaches that copy
///   too, so its mount ID is unknown and only `st_dev` can match it.
///
/// The close must succeed on retry once all of them exit.
/// - A raw open of the dm device: the filesystem is torn down by the
///   unmount, so the report must say so and match nothing by its stale
///   superblock number. With the schedule exhausted the mapping is
///   force-freed, and a reopen of the same backing must explain the dm UUID
///   the renamed mapping still holds, then succeed once the holder lets go.
#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires root, loop/dm-crypt and an explicitly supplied disposable btrfs fixture directory"]
async fn busy_close_reports_its_holders_and_recovers() {
    use std::sync::Mutex;

    // CI includes ignored tests; host mutation still requires explicit opt-in.
    let Some(root) = std::env::var_os("SPACE_NODEMGR_TEST_BTRFS_DIR") else {
        eprintln!("not opted in: set SPACE_NODEMGR_TEST_BTRFS_DIR to a disposable btrfs directory");
        return;
    };
    assert!(nix::unistd::geteuid().is_root());
    let temp = tempfile::tempdir_in(root).unwrap();
    let root = temp.path().canonicalize().unwrap();
    // A shared parent, like node-manager's Bidirectional /space/cache, so the
    // volume mount propagates into a slave namespace.
    struct Unbind(PathBuf);
    impl Drop for Unbind {
        fn drop(&mut self) {
            let _ = nix::mount::umount2(&self.0, nix::mount::MntFlags::MNT_DETACH);
        }
    }
    {
        use nix::mount::{MsFlags, mount};
        mount(
            Some(&root),
            &root,
            None::<&str>,
            MsFlags::MS_BIND,
            None::<&str>,
        )
        .unwrap();
    }
    let _unbind = Unbind(root.clone());
    nix::mount::mount(
        None::<&str>,
        &root,
        None::<&str>,
        nix::mount::MsFlags::MS_SHARED,
        None::<&str>,
    )
    .unwrap();
    let sandbox_id = SandboxId::new();
    let mapper = mapper_name(&sandbox_id, "snapshot");
    let backing_path = root.join("snapshot.luks");
    let mount_dir = root.join("snapshot-vol");
    let dek = zeroize::Zeroizing::new(vec![42; 32]);
    let params = |fresh| PrepareParams {
        sandbox_id,
        volume_name: "snapshot".to_string(),
        backing_path: backing_path.clone(),
        restore_source: None,
        mount_dir: mount_dir.clone(),
        size_bytes: 64 * 1024 * 1024,
        dek: &dek,
        fresh,
    };
    let reports = Mutex::new(Vec::new());
    let record = |report: &busy_holders::BusyHolderReport| {
        reports.lock().unwrap().push((
            report.filesystem_alive,
            report.namespaces.clone(),
            report.files.clone(),
        ));
    };

    let memory = mount_dir.join("memory");
    // The slave namespace exists before the volume is mounted, and opens
    // `memory` through the copy it receives by propagation.
    let (slave, slave_pid, slave_ready) = spawn_holder("slave", &memory, false);
    prepare(params(true)).await.unwrap();
    std::fs::write(&memory, b"vmstate").unwrap();
    wait_ready(slave_ready);
    let (private, private_pid, private_ready) = spawn_holder("private", &memory, false);
    wait_ready(private_ready);
    let holders = Mutex::new(vec![slave, private]);
    let own_file = Mutex::new(Some(std::fs::File::open(&memory).unwrap()));
    // Record, then let go, so the next attempt can succeed.
    let observe = |report: &busy_holders::BusyHolderReport| {
        record(report);
        holders.lock().unwrap().clear();
        own_file.lock().unwrap().take();
    };
    release_with(
        &sandbox_id,
        "snapshot",
        &mount_dir,
        &CloseRetry {
            delays: &[std::time::Duration::from_millis(100)],
            observe: &observe,
        },
    )
    .await
    .expect("the close succeeds once the namespace holder exits");
    {
        let reports = std::mem::take(&mut *reports.lock().unwrap());
        assert_eq!(reports.len(), 1, "recorded only at the first refusal");
        let (alive, namespaces, files) = &reports[0];
        assert_eq!(*alive, Some(true));
        assert_eq!(namespaces.len(), 1, "{namespaces:?}");
        assert!(!namespaces[0].is_node_manager);
        assert!(
            namespaces[0]
                .processes
                .iter()
                .any(|process| process.starts_with(&format!("{private_pid} "))),
            "{namespaces:?}"
        );
        assert!(
            files
                .iter()
                .any(|file| file.pid == private_pid && file.via == "fd/7"),
            "{files:?}"
        );
        assert!(
            files
                .iter()
                .any(|file| file.pid == slave_pid && file.via == "fd/7"),
            "the propagated copy's holder: {files:?}"
        );
        assert!(
            files
                .iter()
                .any(|file| file.pid == std::process::id() && file.path.ends_with("/memory")),
            "{files:?}"
        );
    }

    // Reopen the same backing once a force-freed mapping's holder is gone:
    // until then the renamed mapping keeps the dm UUID.
    async fn reopen<F, Fut>(prepare_again: F)
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<PreparedEncryptedVolume, DmCryptError>>,
    {
        let mut reopened = prepare_again().await;
        for _ in 0..50 {
            if reopened.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            reopened = prepare_again().await;
        }
        reopened.expect("reopen succeeds after the holder exits");
    }

    // A raw read-write open of the dm device, which outlives the filesystem.
    // Every close is busy, but the superblock and the dm device are made
    // read-only, so the release is sealed and the mapping force-freed. The
    // loop device stays writable: a read-only flag would outlive its detach.
    prepare(params(false)).await.unwrap();
    assert_eq!(std::fs::read(&memory).unwrap(), b"vmstate");
    let raw = std::fs::File::options()
        .read(true)
        .write(true)
        .open(format!("/dev/mapper/{mapper}"))
        .unwrap();
    release_with(
        &sandbox_id,
        "snapshot",
        &mount_dir,
        &CloseRetry {
            delays: &[],
            observe: &record,
        },
    )
    .await
    .expect("a holder that can no longer write leaves the volume sealed");
    {
        let reports = std::mem::take(&mut *reports.lock().unwrap());
        assert_eq!(reports.len(), 1, "a single attempt is first and last");
        let (alive, namespaces, files) = &reports[0];
        assert_eq!(*alive, Some(false));
        assert!(namespaces.is_empty(), "{namespaces:?}");
        let own = std::process::id();
        assert!(
            files
                .iter()
                .any(|file| file.pid == own && file.via.starts_with("fd/")),
            "{files:?}"
        );
        assert!(
            files.iter().all(|file| file.via.starts_with("fd/")),
            "a dead filesystem has no mappings, cwd or root: {files:?}"
        );
    }
    let sealed = std::fs::read(&backing_path).unwrap();
    {
        use std::os::unix::fs::FileExt;
        let write = raw.write_at(b"tampered", 0).unwrap_err();
        assert_eq!(write.raw_os_error(), Some(libc::EPERM), "{write}");
    }
    assert!(
        std::fs::read(&backing_path).unwrap() == sealed,
        "the sealed backing is unchanged"
    );
    let error = prepare(params(false))
        .await
        .expect_err("the renamed mapping still holds the dm UUID");
    assert!(
        error
            .to_string()
            .contains("still holds this volume's dm UUID"),
        "{error}"
    );
    drop(raw);
    reopen(|| prepare(params(false))).await;
    assert_eq!(std::fs::read(&memory).unwrap(), b"vmstate");
    let loops = std::fs::read_dir("/sys/block")
        .unwrap()
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("loop"))
        .filter(|entry| {
            std::fs::read_to_string(entry.path().join("ro"))
                .unwrap_or_default()
                .trim()
                == "1"
        })
        .map(|entry| entry.file_name())
        .collect::<Vec<_>>();
    assert!(
        loops.is_empty(),
        "the seal left read-only loop devices: {loops:?}"
    );

    // A reader in a private namespace keeps its copy of the mount, as a
    // `btrfs receive` child used to. The read-only superblock reaches that
    // copy: the release is sealed, and the holder can no longer write.
    let (reader, reader_pid, reader_ready) = spawn_holder("private", &memory, false);
    wait_ready(reader_ready);
    release_with(
        &sandbox_id,
        "snapshot",
        &mount_dir,
        &CloseRetry {
            delays: &[],
            observe: &record,
        },
    )
    .await
    .expect("a read-only copy leaves the volume sealed");
    {
        let reports = std::mem::take(&mut *reports.lock().unwrap());
        let (alive, namespaces, _) = &reports[0];
        assert_eq!(*alive, Some(true));
        assert!(
            namespaces.iter().any(|namespace| namespace
                .processes
                .iter()
                .any(|process| process.starts_with(&format!("{reader_pid} ")))),
            "{namespaces:?}"
        );
    }
    let write = std::process::Command::new("nsenter")
        .args(["-t", &reader_pid.to_string(), "-m", "--"])
        .args(["sh", "-c", "echo tampered >> \"$1\"", "sh"])
        .arg(&memory)
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(!write.success(), "the private copy is read-only");
    drop(reader);
    reopen(|| prepare(params(false))).await;
    assert_eq!(std::fs::read(&memory).unwrap(), b"vmstate");

    // A file open for writing refuses the read-only remount, so a busy close
    // fails the release exactly as before: the volume was not sealed.
    let (writer, _, writer_ready) = spawn_holder("private", &memory, true);
    wait_ready(writer_ready);
    release_with(
        &sandbox_id,
        "snapshot",
        &mount_dir,
        &CloseRetry {
            delays: &[],
            observe: &record,
        },
    )
    .await
    .expect_err("a writer blocks the read-only seal");
    reports.lock().unwrap().clear();
    drop(writer);
    reopen(|| prepare(params(false))).await;
    assert_eq!(std::fs::read(&memory).unwrap(), b"vmstate");
    release(&sandbox_id, "snapshot", &mount_dir).await.unwrap();
    let names = run(
        "dmsetup",
        &["info", "--columns", "--noheadings", "-o", "name"],
    )
    .await
    .unwrap();
    assert!(
        !String::from_utf8_lossy(&names).contains(&mapper),
        "no mapping of this volume, renamed or not, survives"
    );
}

/// The read-only check reads the superblock options, which a plain
/// `remount,ro` changes, and judges the visible (last) mount at the point.
#[test]
fn superblock_read_only_reads_the_superblock_options_of_the_visible_mount() {
    let point = "/space/cache/sandboxes/a/snapshot-vol";
    let line = |mount_options: &str, super_options: &str| {
        format!("900 880 0:56 / {point} {mount_options} - btrfs /dev/mapper/x {super_options}\n")
    };
    let read_only = line("rw,relatime", "ro,ssd,space_cache=v2,subvolid=5,subvol=/");
    let writable = line("ro,relatime", "rw,ssd,space_cache=v2,subvolid=5,subvol=/");
    assert!(superblock_read_only(&read_only, point));
    assert!(
        !superblock_read_only(&writable, point),
        "a read-only bind over a writable superblock is not sealed"
    );
    assert!(!superblock_read_only(
        &format!("{read_only}{writable}"),
        point
    ));
    assert!(superblock_read_only(
        &format!("{writable}{read_only}"),
        point
    ));
    assert!(!superblock_read_only(&read_only, "/elsewhere"));
    // An option that merely starts with "ro" is not the read-only flag.
    assert!(!superblock_read_only(
        &line("rw", "rw,rootcontext=x"),
        point
    ));
}
