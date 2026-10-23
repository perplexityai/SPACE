use super::*;
use uuid::Uuid;

fn sid() -> SandboxId {
    SandboxId::from_uuid(Uuid::parse_str("019d84bd-280d-7331-99c6-b63377f3a3a2").unwrap())
}

#[test]
fn luks_format_forces_cheap_kdf() {
    // The passphrase is a full-entropy DEK, so the memory-hard argon2id
    // default must never be used: it would spike ~700 MiB and OOM the
    // memory-limited node-manager. Pin the cheap pbkdf2 KDF here so a
    // refactor can't silently fall back to the default.
    let args = luks_format_args("/space/cache/.../instance/cow.luks");
    assert!(
        args.windows(2).any(|w| w == ["--pbkdf", "pbkdf2"]),
        "must force the non-memory-hard pbkdf2 KDF: {args:?}"
    );
    assert!(
        args.windows(2)
            .any(|w| w == ["--pbkdf-force-iterations", "1000"]),
        "must pin the minimum pbkdf2 iteration count: {args:?}"
    );
    assert!(
        !args.iter().any(|a| a.contains("argon2")),
        "must not select any argon2 (memory-hard) KDF: {args:?}"
    );
    // Still a luks2 format taking the key on stdin, backing path last.
    assert!(args.contains(&"luksFormat") && args.contains(&"luks2"));
    assert!(args.contains(&"--key-file=-"));
    assert_eq!(args.last(), Some(&"/space/cache/.../instance/cow.luks"));
}

#[test]
fn device_names_are_derived_and_distinct() {
    let s = sid();
    assert_eq!(
        composite_name(&s),
        "sbxroot-019d84bd-280d-7331-99c6-b63377f3a3a2"
    );
    assert_eq!(
        cow_mapper_name(&s),
        "sbxroot-019d84bd-280d-7331-99c6-b63377f3a3a2-cow"
    );
    // dm names must stay within the kernel's 127-char limit.
    assert!(cow_mapper_name(&s).len() < 128);
}

#[test]
fn cow_size_covers_full_rewrite_plus_header() {
    // 10 GiB rootfs: COW must exceed rootfs (worst-case full rewrite) and
    // add headroom for the LUKS header + dm-snapshot metadata.
    let rootfs = 10 * 1024 * 1024 * 1024_u64;
    let cow = cow_backing_size_bytes(rootfs);
    assert!(cow > rootfs, "COW must exceed rootfs for a full rewrite");
    assert!(
        cow - rootfs >= COW_HEADROOM_BYTES,
        "COW must add at least the fixed headroom"
    );
}

#[tokio::test]
async fn oversized_encrypted_rootfs_is_rejected_before_kernel_or_cow_work() {
    let directory = tempfile::tempdir().unwrap();
    let origin = directory.path().canonicalize().unwrap().join("origin.ext4");
    let cow = directory.path().join("cow.luks");
    std::fs::File::create(&origin)
        .unwrap()
        .set_len(128 * 1024 * 1024 * 1024 + 1)
        .unwrap();
    let error = prepare(PrepareParams {
        sandbox_id: sid(),
        origin_path: origin,
        cow_path: cow.clone(),
        restore_cow_source: None,
        rootfs_size_bytes: 0,
        dek: b"test-dek",
    })
    .await
    .unwrap_err();
    assert!(matches!(error, DmCryptError::InvalidParams(_)), "{error}");
    assert!(!cow.exists());
}

#[test]
fn snapshot_table_is_well_formed() {
    let t = snapshot_table(262144, "/dev/loop5", "/dev/mapper/sbxroot-x-cow");
    assert_eq!(
        t,
        "0 262144 snapshot /dev/loop5 /dev/mapper/sbxroot-x-cow P 8"
    );
}

/// Stop→start regression: an existing `cow.plain` whose store the kernel has
/// activated must read as initialized (open, don't zero), while every
/// never-activated shape — zeros, garbage, short — must read as fresh.
/// Re-zeroing an activated store orphans every remapped chunk and silently
/// reverts the guest to the template.
#[test]
fn snapshot_store_magic_separates_open_from_initialize() {
    // The kernel writes SNAP_MAGIC 0x70416e53 little-endian: "SnAp" on disk.
    assert!(has_persistent_snapshot_store(b"SnAp\x01\x00\x00\x00"));
    assert!(has_persistent_snapshot_store(&0x70416e53u32.to_le_bytes()));

    // A fresh truncate reads as zeros; a recycled file as garbage; a crash
    // between truncate and activation as a short/empty file. All fresh.
    assert!(!has_persistent_snapshot_store(&[0u8; 4096]));
    assert!(!has_persistent_snapshot_store(b"LUKS\xba\xbe"));
    assert!(!has_persistent_snapshot_store(b"Sn"));
    assert!(!has_persistent_snapshot_store(b""));
}

/// The freeze/recovery invariant: every command from suspend through resume
/// runs on a bounded runner that KILLS a wedged child — otherwise one wedged
/// invocation holds the per-sandbox device lock and leaves guest I/O frozen
/// indefinitely. Verify this adapter preserves the command deadline/error;
/// host_command's test owns the direct-child kill-and-reap contract.
#[tokio::test]
async fn bounded_runner_kills_and_reports_a_wedged_command() {
    let started = std::time::Instant::now();
    let err = run_bounded("sleep", &["30"], std::time::Duration::from_millis(200))
        .await
        .expect_err("a command past its budget must fail");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "must return at the budget, not after the wedged command finishes"
    );
    assert!(
        matches!(err, DmCryptError::Io { ref source, .. } if source.kind() == std::io::ErrorKind::TimedOut),
        "the error must name the timeout: {err}"
    );
}

/// Only `cryptsetup close`'s busy exit status classifies as busy: neither a
/// busy-sounding message nor the same status from another tool does.
#[test]
fn only_cryptsetup_close_exit_five_is_busy() {
    use std::os::unix::process::ExitStatusExt;
    fn failing(
        code: i32,
        stderr: &'static str,
    ) -> impl FnOnce(&mut Command, std::time::Duration, &[u8]) -> std::io::Result<std::process::Output>
    {
        move |_, _, _| {
            Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(code << 8),
                stdout: Vec::new(),
                stderr: stderr.into(),
            })
        }
    }
    const IN_USE: &str = "Device x is still in use.";
    assert!(matches!(
        ReleaseDevice::Crypt("x").blocking_with(failing(5, IN_USE)),
        Err(DmCryptError::Busy { .. })
    ));
    assert!(matches!(
        ReleaseDevice::Crypt("x").blocking_with(failing(1, IN_USE)),
        Err(DmCryptError::Command { .. })
    ));
    assert!(matches!(
        ReleaseDevice::Loop("/dev/loop0").blocking_with(failing(5, "busy")),
        Err(DmCryptError::Command { .. })
    ));
}

/// A live Firecracker writes the guest rootfs through the composite with a
/// buffered descriptor it keeps open, so its last close never flushes the
/// device before the COW backing is staged. After [`flush_composite`], a copy
/// of `cow.luks` taken while that writer is still open carries the write: the
/// copy opens as another composite over the same origin and reads it back.
#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires root, loop/dm-crypt and an explicitly supplied disposable btrfs fixture directory"]
async fn a_cow_copy_after_flush_holds_a_live_writers_buffered_writes() {
    use std::os::unix::fs::FileExt;

    // CI includes ignored tests; host mutation still requires explicit opt-in.
    let Some(root) = std::env::var_os("SPACE_NODEMGR_TEST_BTRFS_DIR") else {
        eprintln!("not opted in: set SPACE_NODEMGR_TEST_BTRFS_DIR to a disposable btrfs directory");
        return;
    };
    assert!(nix::unistd::geteuid().is_root());
    let temp = tempfile::tempdir_in(root).unwrap();
    let root = temp.path().canonicalize().unwrap();
    let dek = zeroize::Zeroizing::new(vec![42; 32]);
    let (live, copy) = (SandboxId::new(), SandboxId::new());
    let dir = |name: &str| {
        let dir = root.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    };
    let (live_dir, copy_dir) = (dir("live"), dir("copy"));
    let origin = vec![0_u8; 16 * 1024 * 1024];
    std::fs::write(live_dir.join("rootfs.ext4"), &origin).unwrap();
    std::fs::write(copy_dir.join("rootfs.ext4"), &origin).unwrap();
    const OFFSET: u64 = 3 * 4096;
    const MARKER: &[u8] = b"guest write still in the page cache";

    let prepared = prepare(PrepareParams {
        sandbox_id: live,
        origin_path: live_dir.join("rootfs.ext4"),
        cow_path: live_dir.join("cow.luks"),
        restore_cow_source: None,
        rootfs_size_bytes: 0,
        dek: &dek,
    })
    .await
    .unwrap();
    let writer = std::fs::File::options()
        .write(true)
        .open(&prepared.mapper_path)
        .unwrap();
    writer.write_at(MARKER, OFFSET).unwrap();

    flush_composite(&live).await.unwrap();
    // Opened in place below, as a same-node restore opens the kept COW.
    std::fs::copy(live_dir.join("cow.luks"), copy_dir.join("cow.luks")).unwrap();
    drop(writer);

    let reopened = prepare(PrepareParams {
        sandbox_id: copy,
        origin_path: copy_dir.join("rootfs.ext4"),
        cow_path: copy_dir.join("cow.luks"),
        restore_cow_source: None,
        rootfs_size_bytes: 0,
        dek: &dek,
    })
    .await
    .unwrap();
    let mut read = vec![0_u8; MARKER.len()];
    std::fs::File::open(&reopened.mapper_path)
        .unwrap()
        .read_exact_at(&mut read, OFFSET)
        .unwrap();
    // The origin loops are attached read-only, and a detached loop device
    // keeps that flag until it is attached read-write again. Reattach each to
    // a scratch file and detach it, so other tests on this host see none.
    let origin_loops = [&live_dir, &copy_dir].map(|dir| {
        let found = std::process::Command::new("losetup")
            .arg("-j")
            .arg(dir.join("rootfs.ext4"))
            .output()
            .unwrap();
        String::from_utf8_lossy(&found.stdout)
            .lines()
            .filter_map(|line| line.split(':').next().map(str::to_owned))
            .collect::<Vec<_>>()
    });
    release(&copy, &copy_dir.join("rootfs.ext4")).await.unwrap();
    release(&live, &live_dir.join("rootfs.ext4")).await.unwrap();
    let scratch = root.join("scratch.img");
    std::fs::File::create(&scratch)
        .unwrap()
        .set_len(1024 * 1024)
        .unwrap();
    for device in origin_loops.iter().flatten() {
        // Autoclear detaches the origin asynchronously after release. Detach
        // only a device this test then attached; another process may own it.
        let mut attached = false;
        for _ in 0..50 {
            attached = std::process::Command::new("losetup")
                .arg(device)
                .arg(&scratch)
                .status()
                .unwrap()
                .success();
            if attached {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if attached {
            let _ = std::process::Command::new("losetup")
                .args(["-d", device])
                .status();
        }
    }
    assert_eq!(read, MARKER, "the staged COW must carry the buffered write");
}
