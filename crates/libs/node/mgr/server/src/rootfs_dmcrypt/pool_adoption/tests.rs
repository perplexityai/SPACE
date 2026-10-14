use super::*;
use std::os::unix::fs::FileExt;

struct DeviceCleanup(Option<SandboxId>);

impl Drop for DeviceCleanup {
    fn drop(&mut self) {
        let Some(id) = self.0 else { return };
        for device in [
            ReleaseDevice::Snapshot(&composite_name(&id)),
            ReleaseDevice::Snapshot(&format!("{}-seed", composite_name(&id))),
            ReleaseDevice::Crypt(&cow_mapper_name(&id)),
        ] {
            if let Err(error) = device.blocking_with(crate::host_command::output) {
                eprintln!("pool test device cleanup failed: {error}");
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires root, loop/dm-crypt and an explicitly supplied disposable btrfs fixture directory"]
async fn open_pool_disk_survives_encryption_and_reopen() {
    let Some(root) = std::env::var_os("SPACE_NODEMGR_TEST_BTRFS_DIR") else {
        eprintln!("not opted in: set SPACE_NODEMGR_TEST_BTRFS_DIR to a disposable btrfs directory");
        return;
    };
    assert!(nix::unistd::geteuid().is_root());
    let temp = tempfile::tempdir_in(root).unwrap();
    let root = temp.path().canonicalize().unwrap();
    let sandbox_id = SandboxId::new();
    let mut cleanup = DeviceCleanup(Some(sandbox_id));
    let origin = root.join("rootfs.ext4");
    let encrypted = root.join("cow.luks");
    let origin_file = Backing::create(&origin).unwrap();
    origin_file.resize(64 * 1024 * 1024).unwrap();
    for offset in [32, 40] {
        origin_file
            .file
            .write_all_at(&[127; 4096], offset * 1024 * 1024)
            .unwrap();
    }
    retain_pool_origin(origin.clone()).await.unwrap();
    let disk = std::fs::File::options()
        .read(true)
        .write(true)
        .open(&origin)
        .unwrap();
    // Boot-time changes span several encrypted snapshot metadata areas.
    for index in 0..1024_u64 {
        disk.write_all_at(&[index as u8; 4096], index * 8192)
            .unwrap();
    }
    #[cfg(target_os = "linux")]
    rustix::fs::fallocate(
        &disk,
        rustix::fs::FallocateFlags::PUNCH_HOLE | rustix::fs::FallocateFlags::KEEP_SIZE,
        32 * 1024 * 1024,
        4096,
    )
    .unwrap();
    disk.write_all_at(&[0; 4096], 40 * 1024 * 1024).unwrap();
    disk.sync_all().unwrap();
    let dek = [42_u8; 32];
    let composite = composite_name(&sandbox_id);
    let prepared = prepare_pool_drive_switch(sandbox_id, origin.clone())
        .await
        .unwrap();
    let original_table = run("dmsetup", &["table", &composite]).await.unwrap();
    let (_, minor) = String::from_utf8_lossy(&original_table)
        .split_whitespace()
        .nth(3)
        .and_then(parse_device_numbers)
        .unwrap();
    let warm = root.join(POOL_ROOTFS_FILE);
    let warm_backing = Backing::require(Backing::open(&warm, false).unwrap()).unwrap();
    let original_device = LoopDevice::matching(
        &PathBuf::from(format!("/dev/loop{minor}")),
        &warm_backing.file,
    )
    .unwrap()
    .unwrap();
    drop(disk);
    let disk = std::fs::File::options()
        .read(true)
        .write(true)
        .open(&prepared.mapper_path)
        .unwrap();
    let started = std::time::Instant::now();
    let queued_path = prepared.mapper_path.clone();
    let mut queued = Some(tokio::task::spawn_blocking(move || {
        let mut options = std::fs::File::options();
        options.read(true).write(true);
        #[cfg(target_os = "linux")]
        options.custom_flags(libc::O_DIRECT);
        let pending = options.open(queued_path).unwrap();
        let buffer = vec![85; 8192];
        let start = buffer.as_ptr().align_offset(4096);
        pending
            .write_all_at(&buffer[start..start + 4096], 8192)
            .unwrap();
        pending.sync_all().unwrap();
    }));
    for attempt in 0..3 {
        if attempt == 1 {
            // Reconstruct a crash after the encrypted backing was committed
            // but before the suspended linear target was replaced.
            suspend_device(&composite).await.unwrap();
            run(
                "dmsetup",
                &[
                    "reload",
                    &composite,
                    "--table",
                    &String::from_utf8_lossy(&original_table),
                ],
            )
            .await
            .unwrap();
            run("dmsetup", &["resume", &composite]).await.unwrap();
            suspend_device(&composite).await.unwrap();
        }
        prepare_pool_drive_switch(sandbox_id, origin.clone())
            .await
            .unwrap();
        let adopted = encrypt_pool_rootfs(
            sandbox_id,
            origin.clone(),
            warm.clone(),
            encrypted.clone(),
            &dek,
        )
        .await
        .unwrap();
        if let Some(queued) = queued.take() {
            queued.await.unwrap();
        }
        assert_eq!(adopted.mapper_path, prepared.mapper_path);
        run(
            "blockdev",
            &["--flushbufs", &prepared.mapper_path.to_string_lossy()],
        )
        .await
        .unwrap();
        for index in 0..1024_u64 {
            let mut actual = [0; 4096];
            disk.read_exact_at(&mut actual, index * 8192).unwrap();
            assert!(
                actual == [if index == 1 { 85 } else { index as u8 }; 4096],
                "lost pre-adoption block {index}"
            );
        }
        for offset in [32, 40] {
            let mut actual = [1; 4096];
            disk.read_exact_at(&mut actual, offset * 1024 * 1024)
                .unwrap();
            assert_eq!(actual, [0; 4096], "lost hole or zero overwrite");
        }
    }
    eprintln!(
        "pool rootfs encryption and retry completed in {:?}",
        started.elapsed()
    );
    disk.write_all_at(b"customer-write-after-encryption", 16 * 1024 * 1024)
        .unwrap();
    disk.sync_all().unwrap();
    let table = run("dmsetup", &["table", &composite_name(&sandbox_id)])
        .await
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&table)
            .split_whitespace()
            .nth(4)
            .and_then(parse_device_numbers),
        Some(
            crypt_mapper_numbers(&cow_mapper_name(&sandbox_id))
                .await
                .unwrap()
        )
    );
    encrypt_pool_rootfs(
        sandbox_id,
        origin.clone(),
        warm.clone(),
        encrypted.clone(),
        &[43; 32],
    )
    .await
    .expect_err("an incorrect DEK must not acknowledge adoption");
    drop(disk);
    release(&sandbox_id, &origin).await.unwrap();

    // Restore must depend only on the pristine template and encrypted COW.
    drop(original_device);
    drop(warm_backing);
    drop(origin_file);
    std::fs::remove_file(&warm).unwrap();
    std::fs::remove_file(&origin).unwrap();
    let restored_origin = Backing::create(&origin).unwrap();
    restored_origin.resize(64 * 1024 * 1024).unwrap();
    for offset in [32, 40] {
        restored_origin
            .file
            .write_all_at(&[127; 4096], offset * 1024 * 1024)
            .unwrap();
    }
    restored_origin.file.sync_all().unwrap();

    let reopened = prepare(PrepareParams {
        sandbox_id,
        origin_path: origin.clone(),
        cow_path: encrypted,
        restore_cow_source: None,
        rootfs_size_bytes: 0,
        dek: &dek,
    })
    .await
    .unwrap();
    let disk = std::fs::File::open(reopened.mapper_path).unwrap();
    let mut actual = [0; b"customer-write-after-encryption".len()];
    disk.read_exact_at(&mut actual, 16 * 1024 * 1024).unwrap();
    assert_eq!(&actual, b"customer-write-after-encryption");
    for index in 0..1024_u64 {
        let mut actual = [0; 4096];
        disk.read_exact_at(&mut actual, index * 8192).unwrap();
        assert!(
            actual == [if index == 1 { 85 } else { index as u8 }; 4096],
            "lost reopened block {index}"
        );
    }
    drop(disk);
    release(&sandbox_id, &origin).await.unwrap();
    cleanup.0 = None;
}
