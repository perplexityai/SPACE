//! Resolve btrfs backing identity without trusting mount-source path aliases.

use std::{io, path::Path};

/// Kernel sysfs links identify the actual mounted devices even after a mapper
/// rename. Snapshot volumes are single-device; partial/multi-device evidence
/// cannot authorize accepting or rebuilding one.
fn single_device(devices: &Path) -> io::Result<(u32, u32)> {
    let mut entries = std::fs::read_dir(devices)?;
    let device = entries
        .next()
        .transpose()?
        .ok_or_else(|| io::Error::other("mounted btrfs device is missing"))?;
    if entries.next().transpose()?.is_some() {
        return Err(io::Error::other("expected a single-device snapshot volume"));
    }
    let raw = std::fs::read_to_string(device.path().join("dev"))?;
    let (major, minor) = raw
        .trim()
        .split_once(':')
        .ok_or_else(|| io::Error::other("invalid btrfs backing device number"))?;
    match (major.parse(), minor.parse()) {
        (Ok(major), Ok(minor)) if major != 0 => Ok((major, minor)),
        _ => Err(io::Error::other("invalid btrfs backing device number")),
    }
}

#[cfg(target_os = "linux")]
pub(super) fn btrfs_device(dir: &Path, expected_mount_id: u64) -> io::Result<(u32, u32)> {
    let (num_devices, fsid) = btrfs_fs_info(dir, expected_mount_id)?;
    if num_devices != 1 {
        return Err(io::Error::other("expected a single-device snapshot volume"));
    }
    single_device(&btrfs_sysfs_dir(fsid).join("devices"))
}

/// The btrfs filesystem UUID of the mount at `dir`.
#[cfg(target_os = "linux")]
pub(super) fn btrfs_fsid(dir: &Path, expected_mount_id: u64) -> io::Result<uuid::Uuid> {
    btrfs_fs_info(dir, expected_mount_id).map(|(_, fsid)| fsid)
}

/// `/sys/fs/btrfs/<fsid>`: present while any mount, in any namespace, or any
/// open file still holds the filesystem's superblock, and removed when the
/// superblock is torn down.
#[cfg(target_os = "linux")]
pub(super) fn btrfs_sysfs_dir(fsid: uuid::Uuid) -> std::path::PathBuf {
    Path::new("/sys/fs/btrfs").join(fsid.to_string())
}

/// Device count and fsid from `BTRFS_IOC_FS_INFO` on the mount at `dir`,
/// provided that mount is still `expected_mount_id`.
#[cfg(target_os = "linux")]
fn btrfs_fs_info(dir: &Path, expected_mount_id: u64) -> io::Result<(u64, uuid::Uuid)> {
    use std::os::fd::AsRawFd;

    let mount = crate::service::trusted_paths::open_directory(dir).map_err(io::Error::other)?;
    // btrfs inode st_dev can name a subvolume's anonymous device, different
    // from mountinfo's superblock number. Mount ID identifies the opened mount
    // itself and remains pinned by this fd throughout the filesystem ioctl.
    let fdinfo = std::fs::read_to_string(format!("/proc/self/fdinfo/{}", mount.as_raw_fd()))?;
    let opened_mount_id = super::busy_holders::parse_fdinfo_mount_id(&fdinfo);
    if opened_mount_id != Some(expected_mount_id) {
        return Err(io::Error::other(
            "mount changed during backing-device lookup",
        ));
    }

    // BTRFS_IOC_FS_INFO's 1024-byte UAPI; only the stable first 32 bytes are
    // consumed. Zero the remaining fields, including optional request flags.
    #[repr(C)]
    struct FsInfo {
        max_id: u64,
        num_devices: u64,
        fsid: [u8; 16],
        rest: [u8; 992],
    }
    let mut info = FsInfo {
        max_id: 0,
        num_devices: 0,
        fsid: [0; 16],
        rest: [0; 992],
    };
    // SAFETY: the descriptor pins the filesystem and info matches the kernel's
    // writable 1024-byte ABI. No pointer or buffer outlives this call.
    if unsafe { libc::ioctl(mount.as_raw_fd(), 0x8400_941fu32 as _, &mut info) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok((info.num_devices, uuid::Uuid::from_bytes(info.fsid)))
}

#[cfg(test)]
mod tests;
