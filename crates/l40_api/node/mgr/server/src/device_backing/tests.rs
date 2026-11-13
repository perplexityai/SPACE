use super::*;
use std::os::unix::fs::{FileExt, symlink};

#[test]
fn backing_rejects_links_and_retains_the_opened_inode() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    std::fs::create_dir(root.join("instance")).unwrap();
    let path = root.join("instance/cow");
    #[cfg(target_os = "linux")]
    {
        let pending = Backing::create_unpublished(&path).unwrap();
        pending.file.write_all_at(b"partial setup", 0).unwrap();
        drop(pending);
        assert!(!path.exists(), "abandoned setup must remain absent");
    }
    let secret = root.join("secret");
    std::fs::write(&secret, b"retained").unwrap();
    for hard in [false, true] {
        if hard {
            std::fs::hard_link(&secret, &path).unwrap();
        } else {
            symlink(&secret, &path).unwrap();
        }
        assert!(Backing::open(&path, true).is_err());
        assert!(Backing::create(&path).is_err());
        std::fs::remove_file(&path).unwrap();
    }
    let backing = Backing::create(&path).unwrap();
    std::fs::rename(root.join("instance"), root.join("original")).unwrap();
    symlink(&root, root.join("instance")).unwrap();
    assert!(Backing::open(&path, true).is_err());
    backing.file.write_all_at(b"pinned", 0).unwrap();
    assert_eq!(std::fs::read(root.join("original/cow")).unwrap(), b"pinned");
    assert_eq!(std::fs::read(&secret).unwrap(), b"retained");
    #[cfg(target_os = "linux")]
    {
        let pending = Backing::create_unpublished(&root.join("original/cow")).unwrap();
        pending.file.write_all_at(b"initialized", 0).unwrap();
        std::fs::rename(root.join("original"), root.join("moved")).unwrap();
        std::fs::create_dir(root.join("original")).unwrap();
        assert_eq!(
            pending
                .publish(std::ffi::OsStr::new("cow"))
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists,
        );
        assert_eq!(std::fs::read(root.join("moved/cow")).unwrap(), b"pinned");
        pending.publish(std::ffi::OsStr::new("published")).unwrap();
        assert_eq!(
            std::fs::read(root.join("moved/published")).unwrap(),
            b"initialized"
        );
        assert!(
            std::fs::read_dir(root.join("original"))
                .unwrap()
                .next()
                .is_none()
        );
    }
}

#[cfg(target_os = "linux")]
fn attached_loops(backing: &Backing) -> Vec<LoopDevice> {
    std::fs::read_dir("/sys/block")
        .unwrap()
        .map(Result::unwrap)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("loop"))
        .filter_map(|entry| {
            match LoopDevice::matching(&Path::new("/dev").join(entry.file_name()), &backing.file) {
                Ok(device) => device,
                Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                Err(error) => panic!("read loop identity: {error}"),
            }
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn assert_loop_mode(device: &LoopDevice, expected: LoopIoMode) {
    let dio = Path::new("/sys/block")
        .join(device.path().file_name().unwrap())
        .join("loop/dio");
    assert_eq!(
        std::fs::read_to_string(dio).unwrap().trim(),
        match expected {
            LoopIoMode::Buffered => "0",
            LoopIoMode::Direct => "1",
        }
    );
    assert_eq!(device.io_mode(), expected);
}

#[cfg(target_os = "linux")]
struct MountedFilesystem(PathBuf);

#[cfg(target_os = "linux")]
impl Drop for MountedFilesystem {
    fn drop(&mut self) {
        if let Err(error) = nix::mount::umount(&self.0) {
            let detached = nix::mount::umount2(&self.0, nix::mount::MntFlags::MNT_DETACH);
            if std::thread::panicking() {
                eprintln!("unmount disposable fixture: {error}; lazy detach: {detached:?}");
            } else {
                panic!("unmount disposable fixture: {error}; lazy detach: {detached:?}");
            }
        }
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires root, loop/dm-crypt and an explicitly supplied disposable btrfs fixture directory"]
async fn pinned_devices_survive_replacement_and_encrypted_reopen() {
    use crate::{encrypted_volume, rootfs_dmcrypt};
    use pplx_space_common_core::types::SandboxId;
    // CI includes ignored tests; host mutation still requires explicit opt-in.
    let Some(root) = std::env::var_os("SPACE_NODEMGR_TEST_BTRFS_DIR") else {
        eprintln!("not opted in: set SPACE_NODEMGR_TEST_BTRFS_DIR to a disposable btrfs directory");
        return;
    };
    let root = PathBuf::from(root);
    assert!(nix::unistd::geteuid().is_root());
    let temp = tempfile::tempdir_in(root).unwrap();
    let root = temp.path().canonicalize().unwrap();
    let path = root.join("pinned");
    let backing = Backing::create(&path).unwrap();
    backing.resize(8 * 1024 * 1024).unwrap();
    backing.file.write_all_at(b"original", 0).unwrap();
    std::fs::rename(&path, root.join("original")).unwrap();
    std::fs::write(&path, b"replacement").unwrap();
    let replacement = Backing::open(&path, false).unwrap().unwrap();
    let mut bytes = [0; 8];
    for mode in [LoopIoMode::Buffered, LoopIoMode::Direct] {
        let device = LoopDevice::attach(&backing, true, mode).unwrap();
        assert_loop_mode(&device, mode);
        assert!(matches_loop(device.path(), &backing.file).unwrap());
        assert!(!matches_loop(device.path(), &replacement.file).unwrap());
        File::open(device.path())
            .unwrap()
            .read_exact_at(&mut bytes, 0)
            .unwrap();
        assert_eq!(&bytes, b"original");
        drop(device);
        assert!(attached_loops(&backing).is_empty());
    }

    // A 512-byte loop cannot use direct I/O through a 4 KiB-sector backing
    // device. Unlike filesystem-name assumptions, this forces the unsupported
    // configuration while leaving the production loop's block size unchanged.
    {
        use std::os::fd::AsRawFd;
        let block_size = |device: &LoopDevice| {
            std::fs::read_to_string(
                Path::new("/sys/block")
                    .join(device.path().file_name().unwrap())
                    .join("queue/logical_block_size"),
            )
            .unwrap()
            .trim()
            .parse::<u32>()
            .unwrap()
        };
        let image = Backing::create(&root.join("sector-4k.img")).unwrap();
        image.resize(64 * 1024 * 1024).unwrap();
        let parent = LoopDevice::attach(&image, false, LoopIoMode::Buffered).unwrap();
        assert_eq!(
            // SAFETY: SET_BLOCK_SIZE takes an integer and this task-owned loop
            // fd stays live until its disposable filesystem is unmounted.
            unsafe { libc::ioctl(parent._device.as_raw_fd(), 0x4c09, 4096 as libc::c_ulong) },
            0,
            "{}",
            io::Error::last_os_error()
        );
        assert_eq!(block_size(&parent), 4096);
        let formatted = std::process::Command::new("mkfs.ext4")
            .args(["-q", "-F", "-b", "4096"])
            .arg(parent.path())
            .output()
            .unwrap();
        assert!(
            formatted.status.success(),
            "{}",
            String::from_utf8_lossy(&formatted.stderr)
        );
        let mountpoint = root.join("sector-4k");
        std::fs::create_dir(&mountpoint).unwrap();
        nix::mount::mount(
            Some(parent.path()),
            &mountpoint,
            Some("ext4"),
            nix::mount::MsFlags::MS_NOSUID | nix::mount::MsFlags::MS_NODEV,
            None::<&str>,
        )
        .unwrap();
        let mounted = MountedFilesystem(mountpoint.clone());
        {
            let backing = Backing::create(&mountpoint.join("buffered")).unwrap();
            backing.file.set_len(1024 * 1024).unwrap();
            let device = LoopDevice::attach(&backing, false, LoopIoMode::Direct).unwrap();
            assert_eq!(block_size(&device), 512);
            assert_loop_mode(&device, LoopIoMode::Buffered);
            let file = File::options().write(true).open(device.path()).unwrap();
            file.write_all_at(b"buffered", 4096).unwrap();
            file.sync_all().unwrap();
            backing.file.read_exact_at(&mut bytes, 4096).unwrap();
            assert_eq!(&bytes, b"buffered");
            drop(file);
            drop(device);
            assert!(attached_loops(&backing).is_empty());
        }
        drop(mounted);
        drop(parent);
        assert!(attached_loops(&image).is_empty());
    }

    let sandbox_id = SandboxId::new();
    let origin = root.join("rootfs.ext4");
    let origin_file = Backing::create(&origin).unwrap();
    origin_file.resize(16 * 1024 * 1024).unwrap();
    let cow_dir = root.join("cow-dir");
    std::fs::create_dir(&cow_dir).unwrap();
    let cow = cow_dir.join("cow.luks");
    let dek = zeroize::Zeroizing::new(vec![42; 32]);
    // A refused fresh setup must leave no canonical COW: existence selects
    // reopen, not initialize, on every later prepare. Use a tiny task-owned
    // filesystem rather than filling the host cache to exercise real low-space
    // admission, then retry the same name on the underlying btrfs filesystem.
    {
        nix::mount::mount(
            Some("tmpfs"),
            &cow_dir,
            Some("tmpfs"),
            nix::mount::MsFlags::MS_NOSUID | nix::mount::MsFlags::MS_NODEV,
            Some("size=64m"),
        )
        .unwrap();
        let mounted = MountedFilesystem(cow_dir.clone());
        let result = rootfs_dmcrypt::prepare(rootfs_dmcrypt::PrepareParams {
            sandbox_id,
            origin_path: origin.clone(),
            cow_path: cow.clone(),
            restore_cow_source: None,
            rootfs_size_bytes: 0,
            dek: &dek,
        })
        .await;
        let cow_exists = cow.try_exists().unwrap();
        drop(mounted);
        assert!(
            matches!(result, Err(DmCryptError::InsufficientDisk(_))),
            "{result:?}"
        );
        assert!(!cow_exists, "low-space rejection stranded the fresh COW");
    }
    for restore in [false, true] {
        let prepared = rootfs_dmcrypt::prepare(rootfs_dmcrypt::PrepareParams {
            sandbox_id,
            origin_path: origin.clone(),
            cow_path: cow.clone(),
            restore_cow_source: None,
            rootfs_size_bytes: 0,
            dek: &dek,
        })
        .await
        .unwrap();
        for path in [&origin, &cow] {
            let backing = Backing::open(path, false).unwrap().unwrap();
            let devices = attached_loops(&backing);
            assert_eq!(devices.len(), 1);
            assert_loop_mode(&devices[0], LoopIoMode::Buffered);
        }
        let composite = File::options()
            .read(true)
            .write(true)
            .open(&prepared.mapper_path)
            .unwrap();
        if restore {
            composite.read_exact_at(&mut bytes, 4096).unwrap();
            assert_eq!(&bytes, b"survives");
        } else {
            composite.write_all_at(b"survives", 4096).unwrap();
            composite.sync_all().unwrap();
        }
        drop(composite);
        // Re-entry validates the composite's actual origin without replacing it.
        rootfs_dmcrypt::prepare(rootfs_dmcrypt::PrepareParams {
            sandbox_id,
            origin_path: origin.clone(),
            cow_path: cow.clone(),
            restore_cow_source: None,
            rootfs_size_bytes: 0,
            dek: &dek,
        })
        .await
        .unwrap();
        rootfs_dmcrypt::release(&sandbox_id, &origin).await.unwrap();
    }
    let invalid = root.join("invalid.luks");
    std::fs::write(&invalid, b"unformatted retained data").unwrap();
    rootfs_dmcrypt::prepare(rootfs_dmcrypt::PrepareParams {
        sandbox_id,
        origin_path: origin.clone(),
        cow_path: invalid.clone(),
        restore_cow_source: None,
        rootfs_size_bytes: 0,
        dek: &dek,
    })
    .await
    .unwrap_err();
    assert_eq!(
        std::fs::read(&invalid).unwrap(),
        b"unformatted retained data"
    );

    for (name, mode) in [
        ("test", LoopIoMode::Buffered),
        ("snapshot", LoopIoMode::Direct),
    ] {
        let volume_backing = root.join(format!("{name}.luks"));
        let mount = root.join(format!("{name}-vol"));
        let params = |fresh| encrypted_volume::PrepareParams {
            sandbox_id,
            volume_name: name.into(),
            backing_path: volume_backing.clone(),
            restore_source: None,
            mount_dir: mount.clone(),
            size_bytes: 1,
            dek: &dek,
            fresh,
        };
        let memory = vec![42; 8193];
        for fresh in [true, false] {
            encrypted_volume::prepare(params(fresh)).await.unwrap();
            let backing = Backing::open(&volume_backing, false).unwrap().unwrap();
            let devices = attached_loops(&backing);
            assert_eq!(devices.len(), 1);
            assert_loop_mode(&devices[0], mode);
            if fresh {
                std::fs::write(mount.join("memory"), &memory).unwrap();
                File::open(mount.join("memory"))
                    .unwrap()
                    .sync_all()
                    .unwrap();
            } else {
                assert_eq!(std::fs::read(mount.join("memory")).unwrap(), memory);
            }
            if fresh && mode == LoopIoMode::Direct {
                use std::os::fd::AsRawFd;
                // Model a pre-upgrade buffered loop. Mounted re-entry and
                // recovery of an existing mapper must not switch live I/O.
                assert_eq!(
                    // SAFETY: the test owns this loop and submits no concurrent I/O.
                    unsafe {
                        libc::ioctl(devices[0]._device.as_raw_fd(), 0x4c08, 0 as libc::c_ulong)
                    },
                    0
                );
                for mounted in [true, false] {
                    if !mounted {
                        nix::mount::umount(&mount).unwrap();
                    }
                    encrypted_volume::prepare(params(false)).await.unwrap();
                    let reused = attached_loops(&backing);
                    assert_eq!(reused.len(), 1);
                    assert_eq!(reused[0].path(), devices[0].path());
                    assert_loop_mode(&reused[0], LoopIoMode::Buffered);
                    assert_eq!(std::fs::read(mount.join("memory")).unwrap(), memory);
                }
            }
            drop(devices);
            encrypted_volume::release(&sandbox_id, name, &volume_backing, &mount)
                .await
                .unwrap();
            assert!(attached_loops(&backing).is_empty());
            if fresh {
                let mut rejected = params(false);
                rejected.dek = b"wrong key";
                encrypted_volume::prepare(rejected).await.unwrap_err();
                assert!(attached_loops(&backing).is_empty());
                assert!(!mount.join("memory").exists());
            }
        }
    }
}
