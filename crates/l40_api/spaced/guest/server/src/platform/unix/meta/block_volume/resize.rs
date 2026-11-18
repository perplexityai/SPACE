//! In-place ext4 growth only. Device enlargement and ownership stay on the host.

use super::{DEVICE, data_device, verify_ext4_identity, verify_permissions};
use pplx_space_execution_spec::{BlockVolumeSizeGiB, GuestBlockMountPath, GuestDataDrive};
use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
    GrowDataDriveRequest, GrowDataDriveResponse,
};
use std::{
    fs::File,
    os::fd::AsRawFd,
    os::unix::fs::{FileExt, FileTypeExt, MetadataExt, OpenOptionsExt},
};
use tonic::Status;

nix::ioctl_read!(block_device_size, 0x12, 114, u64);
nix::ioctl_write_ptr!(resize_ext4, b'f', 16, u64);

fn refused(error: impl std::fmt::Display) -> Status {
    Status::failed_precondition(format!("guest data-drive growth refused: {error}"))
}

#[derive(Debug)]
struct GrowthGeometry {
    current_bytes: u64,
    target_blocks: u64,
}

fn growth_geometry(header: &[u8; 1024], target_bytes: u64) -> Result<GrowthGeometry, Status> {
    let field = |offset| {
        header
            .get(offset..offset + 4)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .ok_or_else(|| refused("invalid ext4 superblock field"))
    };
    let shift = field(0x18)?;
    if shift > 6 {
        return Err(refused("invalid ext4 block size"));
    }
    let block_size = 1024_u64 << shift;
    let high = field(0x150)?;
    if high != 0 && field(0x60)? & 0x80 == 0 {
        return Err(refused("invalid ext4 high block count"));
    }
    let blocks = u64::from(field(0x04)?) | (u64::from(high) << 32);
    let current_bytes = blocks
        .checked_mul(block_size)
        .filter(|bytes| *bytes != 0 && *bytes <= target_bytes)
        .ok_or_else(|| refused("invalid filesystem size or requested shrink"))?;
    if !target_bytes.is_multiple_of(block_size) {
        return Err(refused("target is not aligned to filesystem blocks"));
    }
    Ok(GrowthGeometry {
        current_bytes,
        target_blocks: target_bytes / block_size,
    })
}

fn read_superblock(device: &File) -> Result<[u8; 1024], Status> {
    let mut header = [0; 1024];
    device.read_exact_at(&mut header, 1024).map_err(refused)?;
    Ok(header)
}

pub(crate) fn grow_data_drive(
    request: GrowDataDriveRequest,
) -> Result<GrowDataDriveResponse, Status> {
    if request.topology_version != GuestDataDrive::TOPOLOGY_VERSION {
        return Err(refused("unsupported data-drive topology"));
    }
    let size = BlockVolumeSizeGiB::try_from(request.size_gib).map_err(refused)?;
    if size.get() != 256 {
        return Err(refused(
            "only the 128 to 256 GiB growth target is supported",
        ));
    }
    let mount = GuestBlockMountPath::try_from(request.mount).map_err(refused)?;
    let expected = uuid::Uuid::parse_str(&request.filesystem_uuid).map_err(refused)?;
    let metadata = data_device()?.ok_or_else(|| refused("data device missing"))?;
    let device = File::options()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(DEVICE)
        .map_err(refused)?;
    let pinned = device.metadata().map_err(refused)?;
    if !pinned.file_type().is_block_device() || pinned.rdev() != metadata.rdev() {
        return Err(refused("data device changed during open"));
    }
    let directory = File::options()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(mount.as_str())
        .map_err(refused)?;
    if directory.metadata().map_err(refused)?.dev() != pinned.rdev() {
        return Err(refused("mount is not backed by the data device"));
    }
    verify_permissions(&directory, false)?;
    let mut device_size = 0_u64;
    // SAFETY: the pinned descriptor is a block device and the ioctl writes one u64.
    unsafe { block_device_size(device.as_raw_fd(), &mut device_size) }.map_err(refused)?;
    let target_bytes = u64::from(size.get()) * 1024 * 1024 * 1024;
    if device_size != target_bytes {
        return Err(refused(
            "guest device has not reached the requested capacity",
        ));
    }
    let header = read_superblock(&device)?;
    verify_ext4_identity(header[..0x78].try_into().map_err(refused)?, expected)?;
    let geometry = growth_geometry(&header, target_bytes)?;
    if geometry.current_bytes != target_bytes {
        // EXT4_IOC_RESIZE_FS performs online growth inside the guest kernel.
        // No host parses or mounts the guest filesystem, and no process or
        // existing descriptor is restarted. The shared flush slot survives
        // RPC cancellation until this syscall and verification finish.
        // SAFETY: directory pins the writable filesystem and target_blocks is
        // a live u64 for the duration of the kernel's copy_from_user.
        unsafe { resize_ext4(directory.as_raw_fd(), &geometry.target_blocks) }.map_err(refused)?;
    }
    nix::unistd::syncfs(&directory).map_err(refused)?;
    let observed = growth_geometry(&read_superblock(&device)?, target_bytes)?;
    if observed.current_bytes != target_bytes {
        return Err(refused("filesystem has not reached the requested capacity"));
    }
    Ok(GrowDataDriveResponse {
        device_size_bytes: device_size,
        filesystem_size_bytes: observed.current_bytes,
    })
}

#[cfg(test)]
mod tests;
