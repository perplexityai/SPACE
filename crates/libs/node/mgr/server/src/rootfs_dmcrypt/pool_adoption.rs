//! Claim-time conversion of an ordinary rootfs, followed by a Firecracker drive switch.

use super::*;
#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;

mod delta;

pub(crate) const POOL_ORIGIN_FILE: &str = "pool-origin.ext4";
pub(crate) const POOL_ROOTFS_FILE: &str = "pool-rootfs.ext4";

/// Preserve the pre-boot origin with a reflink; the VM still boots the ordinary file.
pub(crate) async fn retain_pool_origin(origin: PathBuf) -> Result<PreparedRootfs, DmCryptError> {
    let path = origin.clone();
    device_backing::blocking(move || {
        for name in [POOL_ROOTFS_FILE, "cow.luks", "cow.plain"] {
            if Backing::open(&path.with_file_name(name), false)?.is_some() {
                return Err(std::io::Error::other(
                    "pool drive already carries adoption state",
                ));
            }
        }
        let baseline = path.with_file_name(POOL_ORIGIN_FILE);
        if Backing::open(&baseline, false)?.is_none() {
            let source = Backing::require(Backing::open(&path, false)?)?;
            let target = Backing::create_unpublished(&baseline)?;
            #[cfg(target_os = "linux")]
            rustix::fs::ioctl_ficlone(&target.file, &source.file)?;
            #[cfg(not(target_os = "linux"))]
            crate::contained_copy::copy_on_descriptors(
                &source.file,
                &target.file,
                source.file.metadata()?.len(),
            )?;
            target.publish(std::ffi::OsStr::new(POOL_ORIGIN_FILE))?;
        }
        Ok(())
    })
    .await?;
    Ok(PreparedRootfs {
        mapper_path: origin,
    })
}

fn retain_warm_rootfs(origin: &Path) -> std::io::Result<PathBuf> {
    let warm = origin.with_file_name(POOL_ROOTFS_FILE);
    let baseline = origin.with_file_name(POOL_ORIGIN_FILE);
    let parent = crate::service::trusted_paths::open_directory(
        origin.parent().ok_or(std::io::ErrorKind::InvalidInput)?,
    )
    .map_err(std::io::Error::other)?;
    if Backing::open(&warm, false)?.is_none() {
        Backing::require(Backing::open(&baseline, false)?)?;
        Backing::require(Backing::open(origin, false)?)?;
        nix::fcntl::renameat(&parent, "rootfs.ext4", &parent, POOL_ROOTFS_FILE)?;
        parent.sync_all()?;
    }
    if Backing::open(origin, false)?.is_none() {
        Backing::require(Backing::open(&baseline, false)?)?;
        nix::fcntl::renameat(&parent, POOL_ORIGIN_FILE, &parent, "rootfs.ext4")?;
        parent.sync_all()?;
    }
    Ok(warm)
}

/// The suspended linear device holds any requests dispatched after Firecracker
/// switches drives. Its old file engine can drain before the warm file is read.
pub(crate) async fn prepare_pool_drive_switch(
    sandbox_id: SandboxId,
    origin_path: PathBuf,
) -> Result<PreparedRootfs, DmCryptError> {
    let path = origin_path.clone();
    let warm = device_backing::blocking(move || retain_warm_rootfs(&path)).await?;
    let composite = composite_name(&sandbox_id);
    let mapper_path = PathBuf::from(format!("/dev/mapper/{composite}"));
    if dm_exists(&composite).await? {
        let table = run("dmsetup", &["table", &composite]).await?;
        let table = String::from_utf8_lossy(&table);
        let backing = match table.split_whitespace().nth(2) {
            Some("linear") => warm,
            Some("snapshot") => origin_path,
            _ => {
                return Err(DmCryptError::InvalidParams(
                    "unexpected pool drive target".into(),
                ));
            }
        };
        let backing =
            device_backing::blocking(move || Backing::require(Backing::open(&backing, false)?))
                .await?;
        reuse_live_composite(&composite, backing, mapper_path.clone()).await?;
    } else {
        let device = device_backing::blocking(move || {
            let backing = Backing::require(Backing::open(&warm, true)?)?;
            LoopDevice::attach(&backing, false)
        })
        .await?;
        let sectors = blockdev_sectors(device.path()).await?;
        let table = format!("0 {sectors} linear {} 0", device.path().display());
        run("dmsetup", &["create", &composite, "--table", &table]).await?;
        run("dmsetup", &["mknodes", &composite]).await?;
    }
    suspend_device(&composite).await?;
    Ok(PreparedRootfs { mapper_path })
}

pub(crate) async fn encrypt_pool_rootfs(
    sandbox_id: SandboxId,
    origin_path: PathBuf,
    warm_path: PathBuf,
    encrypted_path: PathBuf,
    dek: &[u8],
) -> Result<PreparedRootfs, DmCryptError> {
    let composite = composite_name(&sandbox_id);
    let cow = cow_mapper_name(&sandbox_id);
    let seed = format!("{composite}-seed");
    let mapper_path = PathBuf::from(format!("/dev/mapper/{composite}"));
    let cow_mapper = format!("/dev/mapper/{cow}");
    let origin =
        device_backing::blocking(move || Backing::require(Backing::open(&origin_path, false)?))
            .await?;
    let origin_device = device_backing::blocking(move || LoopDevice::attach(&origin, true)).await?;
    let sectors = blockdev_sectors(origin_device.path()).await?;
    let encrypted = {
        let path = encrypted_path.clone();
        device_backing::blocking(move || Backing::open(&path, true)).await?
    };
    if let Some(encrypted) = encrypted {
        let device = if dm_exists(&cow).await? {
            let status = run("cryptsetup", &["status", &cow]).await?;
            let path = String::from_utf8_lossy(&status)
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find_map(|(key, value)| {
                    (key.trim() == "device").then(|| PathBuf::from(value.trim()))
                })
                .ok_or_else(|| DmCryptError::InvalidParams("encrypted COW has no device".into()))?;
            device_backing::blocking(move || {
                LoopDevice::matching(&path, &encrypted.file)?
                    .ok_or_else(|| std::io::Error::other("encrypted COW backing changed"))
            })
            .await?
        } else {
            let device =
                device_backing::blocking(move || LoopDevice::attach(&encrypted, false)).await?;
            run_with_stdin(
                "cryptsetup",
                &[
                    "open",
                    "--key-file=-",
                    &device.path().to_string_lossy(),
                    &cow,
                ],
                dek,
            )
            .await?;
            device
        };
        run_with_stdin(
            "cryptsetup",
            &[
                "open",
                "--test-passphrase",
                "--key-file=-",
                &device.path().to_string_lossy(),
            ],
            dek,
        )
        .await?;
    } else {
        // No published COW means Firecracker is still held on the linear target.
        // Any interrupted seed is disposable and has never accepted guest I/O.
        let table = run("dmsetup", &["table", &composite]).await?;
        if String::from_utf8_lossy(&table).split_whitespace().nth(2) != Some("linear") {
            return Err(DmCryptError::InvalidParams(
                "uncommitted pool drive is not linear".into(),
            ));
        }
        if dm_exists(&seed).await? {
            ReleaseDevice::Snapshot(&seed).run().await?;
        }
        if dm_exists(&cow).await? {
            ReleaseDevice::Crypt(&cow).run().await?;
        }
        let path = encrypted_path.clone();
        let (backing, device) = device_backing::blocking(move || {
            let backing = Backing::create_unpublished(&path)?;
            backing.resize(cow_backing_size_bytes(sectors * SECTOR_BYTES))?;
            let device = LoopDevice::attach(&backing, false)?;
            Ok((backing, device))
        })
        .await?;
        let device_path = device.path().to_string_lossy();
        run_with_stdin("cryptsetup", &luks_format_args(&device_path), dek).await?;
        run_with_stdin(
            "cryptsetup",
            &["open", "--key-file=-", &device_path, &cow],
            dek,
        )
        .await?;
        run(
            "dd",
            &[
                "if=/dev/zero",
                &format!("of={cow_mapper}"),
                "bs=1M",
                "count=4",
                "oflag=direct",
                "conv=fsync",
                "status=none",
            ],
        )
        .await?;
        let table = snapshot_table(
            sectors,
            &origin_device.path().to_string_lossy(),
            &cow_mapper,
        );
        run("dmsetup", &["create", &seed, "--table", &table]).await?;
        run("dmsetup", &["mknodes", &seed]).await?;
        let seed_path = format!("/dev/mapper/{seed}");
        let origin_loop = origin_device.path().to_owned();
        let copied = device_backing::blocking(move || {
            let source = Backing::require(Backing::open(&warm_path, false)?)?;
            let origin = Backing::require(Backing::open(
                &warm_path.with_file_name("rootfs.ext4"),
                false,
            )?)?;
            if !device_backing::matches_loop(&origin_loop, &origin.file)? {
                return Err(std::io::Error::other("pool origin changed during adoption"));
            }
            let mut options = std::fs::File::options();
            options.read(true).write(true);
            #[cfg(target_os = "linux")]
            options.custom_flags(libc::O_DIRECT);
            let target = options.open(seed_path)?;
            delta::copy_changed_blocks(&source.file, &origin.file, &target)
        })
        .await?;
        suspend_device(&seed).await?;
        ReleaseDevice::Snapshot(&seed).run().await?;
        let name = encrypted_path
            .file_name()
            .ok_or_else(|| DmCryptError::InvalidParams("missing COW name".into()))?
            .to_owned();
        device_backing::blocking(move || backing.publish(&name)).await?;
        tracing::info!(%sandbox_id, copied_bytes = copied, "seeded encrypted rootfs from warm file");
    }
    let table = snapshot_table(
        sectors,
        &origin_device.path().to_string_lossy(),
        &cow_mapper,
    );
    run_bounded(
        "dmsetup",
        &["reload", &composite, "--table", &table],
        FREEZE_COMMAND_BUDGET,
    )
    .await?;
    run_bounded("dmsetup", &["resume", &composite], FREEZE_COMMAND_BUDGET).await?;
    Ok(PreparedRootfs { mapper_path })
}

async fn suspend_device(composite: &str) -> Result<(), DmCryptError> {
    let state = run(
        "dmsetup",
        &["info", "-c", "--noheadings", "-o", "suspended", composite],
    )
    .await?;
    match String::from_utf8_lossy(&state).trim() {
        "Active" => run_bounded("dmsetup", &["suspend", composite], FREEZE_COMMAND_BUDGET)
            .await
            .map(drop),
        "Suspended" => Ok(()),
        _ => Err(DmCryptError::InvalidParams(
            "unknown rootfs suspend state".into(),
        )),
    }
}

#[cfg(test)]
mod tests;
