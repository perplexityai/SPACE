use super::*;
#[cfg(target_os = "linux")]
use crate::encrypted_volume::MapperIdentity;

const MIB: u64 = 1024 * 1024;

#[test]
fn compacts_a_volume_that_doubled_its_baseline_by_a_useful_margin() {
    use BaselineState::{Absent, Bytes};
    use Decision::{Compact, Keep, RecordBaseline};
    for (used, baseline, fresh, expected) in [
        (900 * MIB, Absent, true, RecordBaseline),
        (4096 * MIB, Bytes(MIB), true, RecordBaseline),
        (2048 * MIB, Bytes(1024 * MIB), false, Compact),
        (2047 * MIB, Bytes(1024 * MIB), false, Keep),
        (400 * MIB, Bytes(150 * MIB), false, Keep),
        (300 * MIB, Bytes(0), false, Compact),
        (256 * MIB, Absent, false, Compact),
        (255 * MIB, Absent, false, RecordBaseline),
    ] {
        assert_eq!(
            decide(used, baseline, fresh),
            expected,
            "{used} bytes against {baseline:?}, fresh {fresh}"
        );
    }
}

#[tokio::test]
async fn the_baseline_record_round_trips_and_refuses_what_it_did_not_write() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        read_baseline(dir.path()).await.unwrap(),
        BaselineState::Absent
    );
    record_baseline(dir.path(), 42 * MIB).await.unwrap();
    assert_eq!(
        read_baseline(dir.path()).await.unwrap(),
        BaselineState::Bytes(42 * MIB)
    );
    record_baseline(dir.path(), 7).await.unwrap();
    assert_eq!(
        read_baseline(dir.path()).await.unwrap(),
        BaselineState::Bytes(7),
        "a rewrite truncates the old record"
    );

    std::fs::write(dir.path().join(BASELINE_FILE), b"lots").unwrap();
    assert_eq!(
        read_baseline(dir.path()).await.unwrap(),
        BaselineState::Absent
    );

    let target = dir.path().join("elsewhere");
    std::fs::write(&target, b"1").unwrap();
    std::fs::remove_file(dir.path().join(BASELINE_FILE)).unwrap();
    std::os::unix::fs::symlink(&target, dir.path().join(BASELINE_FILE)).unwrap();
    read_baseline(dir.path()).await.unwrap_err();
    record_baseline(dir.path(), 9).await.unwrap_err();
    assert_eq!(
        std::fs::read(&target).unwrap(),
        b"1",
        "the link is not followed"
    );
}

#[test]
fn the_sibling_sits_beside_the_volume_under_a_derived_name() {
    let sibling = sibling(
        "snapshot",
        Path::new("/cache/sbx/instance/snapshot.luks"),
        Path::new("/cache/sbx/snapshot-vol"),
    );
    assert_eq!(sibling.volume_name, "snapshot-compact");
    assert_eq!(
        sibling.backing_path,
        Path::new("/cache/sbx/instance/snapshot-compact.luks")
    );
    assert_eq!(
        sibling.mount_dir,
        Path::new("/cache/sbx/snapshot-compact-vol")
    );
    let sandbox_id = SandboxId::new();
    assert!(
        mapper_name(&sandbox_id, &sibling.volume_name)
            .starts_with(&format!("sbxvol-{sandbox_id}-")),
        "node cleanup sweeps the sibling with the sandbox's other volumes"
    );
}

#[test]
fn copy_keeps_bytes_length_and_mode() {
    use std::os::unix::fs::PermissionsExt;

    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let memory = source.path().join("memory");
    let mut bytes = vec![0u8; 3 * COPY_CHUNK_BYTES + 17];
    for (index, byte) in bytes.iter_mut().enumerate().skip(COPY_CHUNK_BYTES) {
        *byte = (index % 251) as u8;
    }
    std::fs::write(&memory, &bytes).unwrap();
    std::fs::set_permissions(&memory, std::fs::Permissions::from_mode(0o640)).unwrap();
    // A trailing hole the copy must restore through the length alone.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&memory)
        .unwrap();
    file.set_len(bytes.len() as u64 + 5 * MIB).unwrap();
    bytes.resize(bytes.len() + 5 * MIB as usize, 0);
    std::fs::write(source.path().join("vmstate"), b"state").unwrap();

    copy_contents(
        source.path(),
        target.path(),
        Instant::now() + Duration::from_secs(60),
    )
    .unwrap();

    assert_eq!(std::fs::read(target.path().join("memory")).unwrap(), bytes);
    assert_eq!(
        std::fs::read(target.path().join("vmstate")).unwrap(),
        b"state"
    );
    let mode = std::fs::metadata(target.path().join("memory"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o7777, 0o640);
}

#[cfg(target_os = "linux")]
#[test]
fn copy_leaves_holes_unallocated() {
    use std::os::unix::fs::{FileExt, MetadataExt};

    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let memory = std::fs::File::create(source.path().join("memory")).unwrap();
    memory.set_len(256 * MIB).unwrap();
    memory.write_all_at(&[7; 4096], 128 * MIB).unwrap();
    memory.sync_all().unwrap();
    let source_blocks = memory.metadata().unwrap().blocks();
    if source_blocks * 512 >= 256 * MIB {
        eprintln!("the temporary filesystem does not keep holes; nothing to check");
        return;
    }

    copy_contents(
        source.path(),
        target.path(),
        Instant::now() + Duration::from_secs(60),
    )
    .unwrap();

    let copy = std::fs::metadata(target.path().join("memory")).unwrap();
    assert_eq!(copy.len(), 256 * MIB);
    assert!(
        copy.blocks() * 512 < 16 * MIB,
        "allocated {} bytes",
        copy.blocks() * 512
    );
}

#[test]
fn copy_refuses_anything_but_regular_files() {
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("memory"), b"memory").unwrap();
    std::os::unix::fs::symlink("/etc/hostname", source.path().join("vmstate")).unwrap();
    let error = copy_contents(
        source.path(),
        target.path(),
        Instant::now() + Duration::from_secs(60),
    )
    .unwrap_err();
    // O_NOFOLLOW refuses the link itself.
    assert_eq!(error.raw_os_error(), Some(nix::libc::ELOOP), "{error}");

    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    std::fs::create_dir(source.path().join("nested")).unwrap();
    let error = copy_contents(
        source.path(),
        target.path(),
        Instant::now() + Duration::from_secs(60),
    )
    .unwrap_err();
    assert!(error.to_string().contains("unexpected entry"), "{error}");
}

#[test]
fn copy_stops_at_its_budget() {
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("memory"), vec![1; 4096]).unwrap();
    let error = copy_contents(source.path(), target.path(), Instant::now()).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
}

/// Deterministic page contents shaped like guest memory: zero, incompressible
/// and repetitive pages.
#[cfg(target_os = "linux")]
fn page(seed: &mut u64) -> Vec<u8> {
    let mut next = || {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        *seed
    };
    match next() % 4 {
        0 | 1 => vec![0; 4096],
        2 => (0..4096 / 8).flat_map(|_| next().to_le_bytes()).collect(),
        _ => b"guest page cache "
            .iter()
            .copied()
            .cycle()
            .take(4096)
            .collect(),
    }
}

#[cfg(target_os = "linux")]
fn fixture_root() -> Option<tempfile::TempDir> {
    // CI includes ignored tests; host mutation still requires explicit opt-in.
    let Some(root) = std::env::var_os("SPACE_NODEMGR_TEST_BTRFS_DIR") else {
        eprintln!("not opted in: set SPACE_NODEMGR_TEST_BTRFS_DIR to a disposable btrfs directory");
        return None;
    };
    assert!(nix::unistd::geteuid().is_root());
    Some(tempfile::tempdir_in(root).unwrap())
}

/// Tears a privileged test's devices down even when an assertion fails.
#[cfg(target_os = "linux")]
struct Teardown {
    sandbox_id: SandboxId,
    root: PathBuf,
}

#[cfg(target_os = "linux")]
impl Drop for Teardown {
    fn drop(&mut self) {
        for volume in ["snapshot", "snapshot-compact"] {
            let _ = nix::mount::umount2(
                &self.root.join(format!("{volume}-vol")),
                nix::mount::MntFlags::MNT_DETACH,
            );
            let _ = std::process::Command::new("cryptsetup")
                .args(["close", &mapper_name(&self.sandbox_id, volume)])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
        }
    }
}

#[cfg(target_os = "linux")]
fn assert_same_contents(left: &Path, right: &Path) {
    use std::io::Read;

    let (mut left, mut right) = (
        std::fs::File::open(left).unwrap(),
        std::fs::File::open(right).unwrap(),
    );
    assert_eq!(
        left.metadata().unwrap().len(),
        right.metadata().unwrap().len()
    );
    let (mut a, mut b) = (vec![0; MIB as usize], vec![0; MIB as usize]);
    loop {
        let read = left.read(&mut a).unwrap();
        right.read_exact(&mut b[..read]).unwrap();
        assert!(a[..read] == b[..read], "memory bytes differ");
        if read == 0 {
            break;
        }
    }
}

/// A volume whose memory file was rewritten in place over several suspends,
/// as Firecracker's Diff snapshots do, compacts into a copy that is a fraction
/// of its size and opens under the same key with byte-identical files. The
/// volume itself is left as it was.
#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires root, loop/dm-crypt and an explicitly supplied disposable btrfs fixture directory"]
async fn a_rewritten_volume_compacts_into_a_smaller_copy_with_its_bytes() {
    use std::os::unix::fs::FileExt;

    let Some(temp) = fixture_root() else { return };
    let root = temp.path().canonicalize().unwrap();
    let sandbox_id = SandboxId::new();
    let _teardown = Teardown {
        sandbox_id,
        root: root.clone(),
    };
    let backing_path = root.join("snapshot.luks");
    let mount_dir = root.join("snapshot-vol");
    let reference = root.join("memory.reference");
    let dek = zeroize::Zeroizing::new(vec![42; 32]);
    let pages = (512 * MIB / 4096) as usize;
    let open = |fresh| {
        prepare(PrepareParams {
            sandbox_id,
            volume_name: "snapshot".to_string(),
            backing_path: backing_path.clone(),
            restore_source: None,
            mount_dir: mount_dir.clone(),
            size_bytes: 2048 * MIB,
            dek: &dek,
            fresh,
        })
    };

    let mut seed = 0x9e37_79b9_7f4a_7c15;
    open(true).await.unwrap();
    {
        let memory = std::fs::File::create(mount_dir.join("memory")).unwrap();
        let mirror = std::fs::File::create(&reference).unwrap();
        for index in 0..pages {
            let page = page(&mut seed);
            memory.write_all_at(&page, index as u64 * 4096).unwrap();
            mirror.write_all_at(&page, index as u64 * 4096).unwrap();
        }
        memory.sync_all().unwrap();
        std::fs::write(mount_dir.join("vmstate"), b"vmstate").unwrap();
    }
    // The first publication of a fresh volume records its baseline.
    let first = used_bytes(&mount_dir).await.unwrap();
    let recorded = read_baseline(&mount_dir).await.unwrap();
    assert_eq!(decide(first, recorded, true), Decision::RecordBaseline);
    record_baseline(&mount_dir, first).await.unwrap();
    release(&sandbox_id, "snapshot", &backing_path, &mount_dir)
        .await
        .unwrap();
    for _ in 0..8 {
        open(false).await.unwrap();
        let memory = std::fs::OpenOptions::new()
            .write(true)
            .open(mount_dir.join("memory"))
            .unwrap();
        let mirror = std::fs::OpenOptions::new()
            .write(true)
            .open(&reference)
            .unwrap();
        for _ in 0..pages * 3 / 10 {
            let index = (seed % pages as u64) * 4096;
            let page = page(&mut seed);
            memory.write_all_at(&page, index).unwrap();
            mirror.write_all_at(&page, index).unwrap();
        }
        memory.sync_all().unwrap();
        drop(memory);
        release(&sandbox_id, "snapshot", &backing_path, &mount_dir)
            .await
            .unwrap();
    }

    open(false).await.unwrap();
    assert!(
        is_volume_mounted(&sandbox_id, "snapshot", &mount_dir)
            .await
            .unwrap()
    );
    let before = used_bytes(&mount_dir).await.unwrap();
    let baseline = read_baseline(&mount_dir).await.unwrap();
    assert_eq!(baseline, BaselineState::Bytes(first));
    assert_eq!(
        decide(before, baseline, false),
        Decision::Compact,
        "used {before} against a first publication of {first}"
    );
    let before_allocated = allocated_bytes(&backing_path).await.unwrap();
    // Staging holds the volume read-only while it is copied.
    crate::encrypted_volume::make_read_only(&sandbox_id, "snapshot", &mount_dir)
        .await
        .unwrap();
    let copy = build_compacted_copy(CompactParams {
        sandbox_id,
        volume_name: "snapshot",
        backing_path: &backing_path,
        mount_dir: &mount_dir,
        dek: &dek,
    })
    .await
    .unwrap();
    let copy_allocated = allocated_bytes(&copy.backing_path).await.unwrap();
    eprintln!(
        "first publication used {first}; before compacting used {before}, \
         allocated {before_allocated}; copy used {}, allocated {copy_allocated}",
        copy.used
    );
    assert!(
        copy.used * 2 <= before,
        "copy used {} of {before}",
        copy.used
    );
    assert!(copy_allocated * 2 <= before_allocated);
    assert!(
        matches!(
            mapper_identity(&mapper_name(&sandbox_id, "snapshot-compact")).await,
            MapperIdentity::Absent
        ),
        "the copy is sealed"
    );
    assert_eq!(
        allocated_bytes(&backing_path).await.unwrap(),
        before_allocated,
        "the live volume is left as it was"
    );
    assert_same_contents(&mount_dir.join("memory"), &reference);
    release(&sandbox_id, "snapshot", &backing_path, &mount_dir)
        .await
        .unwrap();

    // A restore opens the published copy as the volume.
    std::fs::rename(&copy.backing_path, &backing_path).unwrap();
    discard(&sandbox_id, "snapshot", &backing_path, &mount_dir).await;
    assert!(!root.join("snapshot-compact.luks").exists());
    assert!(!root.join("snapshot-compact-vol").exists());
    open(false).await.unwrap();
    assert_same_contents(&mount_dir.join("memory"), &reference);
    assert_eq!(
        std::fs::read(mount_dir.join("vmstate")).unwrap(),
        b"vmstate"
    );
    assert_eq!(
        read_baseline(&mount_dir).await.unwrap(),
        BaselineState::Bytes(copy.used)
    );
    release(&sandbox_id, "snapshot", &backing_path, &mount_dir)
        .await
        .unwrap();
}

/// A copy that can't be built leaves no sibling behind and the volume as it
/// was.
#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires root, loop/dm-crypt and an explicitly supplied disposable btrfs fixture directory"]
async fn a_failed_copy_leaves_no_sibling_and_the_volume_unchanged() {
    let Some(temp) = fixture_root() else { return };
    let root = temp.path().canonicalize().unwrap();
    let sandbox_id = SandboxId::new();
    let _teardown = Teardown {
        sandbox_id,
        root: root.clone(),
    };
    let backing_path = root.join("snapshot.luks");
    let mount_dir = root.join("snapshot-vol");
    let dek = zeroize::Zeroizing::new(vec![42; 32]);
    prepare(PrepareParams {
        sandbox_id,
        volume_name: "snapshot".to_string(),
        backing_path: backing_path.clone(),
        restore_source: None,
        mount_dir: mount_dir.clone(),
        size_bytes: 256 * MIB,
        dek: &dek,
        fresh: true,
    })
    .await
    .unwrap();
    std::fs::write(mount_dir.join("memory"), b"memory").unwrap();
    std::fs::create_dir(mount_dir.join("planted")).unwrap();

    build_compacted_copy(CompactParams {
        sandbox_id,
        volume_name: "snapshot",
        backing_path: &backing_path,
        mount_dir: &mount_dir,
        dek: &dek,
    })
    .await
    .unwrap_err();
    assert!(!root.join("snapshot-compact.luks").exists());
    assert!(!root.join("snapshot-compact-vol").exists());
    assert!(matches!(
        mapper_identity(&mapper_name(&sandbox_id, "snapshot-compact")).await,
        MapperIdentity::Absent
    ));
    assert_eq!(std::fs::read(mount_dir.join("memory")).unwrap(), b"memory");
    std::fs::remove_dir(mount_dir.join("planted")).unwrap();
    release(&sandbox_id, "snapshot", &backing_path, &mount_dir)
        .await
        .unwrap();
}
