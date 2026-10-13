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
    let device = LoopDevice::attach(&backing, true).unwrap();
    assert!(matches_loop(device.path(), &backing.file).unwrap());
    let replacement = Backing::open(&path, false).unwrap().unwrap();
    assert!(!matches_loop(device.path(), &replacement.file).unwrap());
    let mut bytes = [0; 8];
    File::open(device.path())
        .unwrap()
        .read_exact_at(&mut bytes, 0)
        .unwrap();
    assert_eq!(&bytes, b"original");
    drop(device);

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
        struct SmallFilesystem(PathBuf);
        impl Drop for SmallFilesystem {
            fn drop(&mut self) {
                nix::mount::umount(&self.0).expect("unmount disposable low-space fixture");
            }
        }
        nix::mount::mount(
            Some("tmpfs"),
            &cow_dir,
            Some("tmpfs"),
            nix::mount::MsFlags::MS_NOSUID | nix::mount::MsFlags::MS_NODEV,
            Some("size=64m"),
        )
        .unwrap();
        let mounted = SmallFilesystem(cow_dir.clone());
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

    let volume_backing = root.join("snapshot.luks");
    let mount = root.join("snapshot-vol");
    for fresh in [true, false] {
        encrypted_volume::prepare(encrypted_volume::PrepareParams {
            sandbox_id,
            volume_name: "test".into(),
            backing_path: volume_backing.clone(),
            restore_source: None,
            mount_dir: mount.clone(),
            size_bytes: 1,
            dek: &dek,
            fresh,
        })
        .await
        .unwrap();
        if fresh {
            std::fs::write(mount.join("memory"), b"encrypted snapshot").unwrap();
        } else {
            assert_eq!(
                std::fs::read(mount.join("memory")).unwrap(),
                b"encrypted snapshot"
            );
        }
        // On reopen, a holder outside the unmount keeps the lazily detached
        // filesystem alive past `cryptsetup close`'s own remove retries; the
        // seal must wait for it rather than fail.
        let holder = (!fresh).then(|| {
            let held = File::open(mount.join("memory")).unwrap();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_secs(7));
                drop(held);
            })
        });
        encrypted_volume::release(&sandbox_id, "test", &mount)
            .await
            .unwrap();
        if let Some(holder) = holder {
            holder.join().unwrap();
        }
    }
}
