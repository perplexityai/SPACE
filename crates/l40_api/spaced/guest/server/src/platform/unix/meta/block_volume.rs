//! Guest-only filesystem operations. Neither this module nor its RPC can
//! identify, attach, format, or expose a host block device.

#[cfg(target_os = "linux")]
mod resize;
#[cfg(target_os = "linux")]
pub(super) use resize::grow_data_drive;
#[cfg(not(target_os = "linux"))]
pub(super) fn grow_data_drive(
    _: pplx_space_spaced_guest_protocol::proto::bridge_guest::GrowDataDriveRequest,
) -> Result<pplx_space_spaced_guest_protocol::proto::bridge_guest::GrowDataDriveResponse, Status> {
    Err(Status::failed_precondition(
        "data-drive growth requires a Linux guest",
    ))
}

use pplx_space_spaced_guest_protocol::proto::bridge_guest::MountDataDriveRequest;
use tonic::Status;

#[cfg(target_os = "linux")]
use {
    nix::mount::{MsFlags, mount},
    pplx_space_execution_spec::{GuestBlockMountPath, GuestDataDrive},
    std::{
        fs::File,
        os::fd::AsRawFd,
        os::unix::{
            ffi::OsStringExt,
            fs::{FileExt, FileTypeExt, MetadataExt, PermissionsExt},
        },
        path::PathBuf,
    },
};

#[cfg(target_os = "linux")]
const DEVICE: &str = "/dev/vdb";

#[cfg(target_os = "linux")]
nix::ioctl_readwrite!(freeze_filesystem, b'X', 119, libc::c_int);

#[cfg(target_os = "linux")]
fn data_device() -> Result<Option<std::fs::Metadata>, Status> {
    let metadata = match std::fs::symlink_metadata(DEVICE) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(Status::internal(format!(
                "stat guest data device failed: {error}"
            )));
        }
    };
    if !metadata.file_type().is_block_device() {
        return Err(Status::failed_precondition(
            "guest data device is not a block device",
        ));
    }
    Ok(Some(metadata))
}

#[cfg(target_os = "linux")]
pub(super) fn mount_data_drive(request: MountDataDriveRequest) -> Result<(), Status> {
    if request.topology_version != GuestDataDrive::TOPOLOGY_VERSION {
        return Err(Status::failed_precondition(
            "unsupported guest data drive topology",
        ));
    }
    let path = GuestBlockMountPath::try_from(request.mount)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let device =
        data_device()?.ok_or_else(|| Status::failed_precondition("guest data drive missing"))?;
    verify_filesystem_uuid(&request.filesystem_uuid, request.read_only)?;
    std::fs::create_dir_all(path.as_str()).map_err(|error| {
        Status::internal(format!("create guest data mountpoint failed: {error}"))
    })?;
    let canonical = std::fs::canonicalize(path.as_str()).map_err(|error| {
        Status::internal(format!("resolve guest data mountpoint failed: {error}"))
    })?;
    if canonical != std::path::Path::new(path.as_str()) {
        return Err(Status::failed_precondition(
            "guest data mountpoint traverses a symlink",
        ));
    }
    let directory = File::open(&canonical)
        .map_err(|error| Status::internal(format!("open guest data mountpoint failed: {error}")))?;
    let current = directory
        .metadata()
        .map_err(|error| Status::internal(format!("stat guest data mountpoint failed: {error}")))?;
    if current.dev() == device.rdev() {
        return verify_permissions(&directory, request.read_only);
    }
    let mut entries = std::fs::read_dir(&canonical).map_err(|error| {
        Status::internal(format!("inspect guest data mountpoint failed: {error}"))
    })?;
    match entries.next() {
        None => {}
        Some(Ok(_)) => {
            return Err(Status::failed_precondition(
                "guest data mountpoint is not empty",
            ));
        }
        Some(Err(error)) => {
            return Err(Status::internal(format!(
                "read guest data mountpoint failed: {error}"
            )));
        }
    }
    let mut flags = MsFlags::MS_NODEV | MsFlags::MS_NOSUID;
    if request.read_only {
        flags |= MsFlags::MS_RDONLY;
    }
    // Keep filesystem initialization entirely outside the guest. In particular
    // an ext4 mount failure is never a reason to run mkfs.
    mount(
        Some(DEVICE),
        canonical.as_path(),
        Some("ext4"),
        flags,
        request.read_only.then_some("noload"),
    )
    .map_err(|error| Status::internal(format!("mount guest data filesystem failed: {error}")))?;
    let mounted = File::open(&canonical).map_err(|error| {
        Status::internal(format!(
            "open mounted guest data filesystem failed: {error}"
        ))
    })?;
    if mounted
        .metadata()
        .map_err(|error| Status::internal(error.to_string()))?
        .dev()
        != device.rdev()
    {
        return Err(Status::failed_precondition(
            "mounted guest data device mismatch",
        ));
    }
    verify_permissions(&mounted, request.read_only)?;
    if !request.read_only {
        // A fresh ext4 root belongs to root. The sandbox's ordinary user must
        // be able to use its disk without any host filesystem traversal.
        mounted
            .set_permissions(std::fs::Permissions::from_mode(0o777))
            .map_err(|error| {
                Status::internal(format!("initialize data directory permissions: {error}"))
            })?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn verify_filesystem_uuid(expected: &str, read_only: bool) -> Result<(), Status> {
    let expected = uuid::Uuid::parse_str(expected)
        .map_err(|_| Status::invalid_argument("a valid ext4 filesystem UUID is required"))?;
    let device = File::open(DEVICE)
        .map_err(|error| Status::internal(format!("open guest data device failed: {error}")))?;
    // ext4 superblock starts at 1024; s_magic is at 0x38, s_uuid at 0x68.
    // Firecracker's drive_id is not the guest serial, so use the persisted
    // filesystem identity in addition to the stable device topology.
    let mut superblock = [0; 0x78];
    device
        .read_exact_at(&mut superblock, 1024)
        .map_err(|error| Status::internal(format!("read guest data superblock failed: {error}")))?;
    verify_ext4_identity(&superblock, expected)?;
    if read_only {
        verify_read_only_clean(&superblock)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn verify_read_only_clean(superblock: &[u8; 0x78]) -> Result<(), Status> {
    // noload is safe only after a clean journal checkpoint. Never hide an
    // unclean shutdown by skipping recovery on a genuinely dirty journal.
    let incompat = u32::from_le_bytes([
        superblock[0x60],
        superblock[0x61],
        superblock[0x62],
        superblock[0x63],
    ]);
    let state = u16::from_le_bytes([superblock[0x3a], superblock[0x3b]]);
    if incompat & 0x4 != 0 || state & 0x2 != 0 {
        return Err(Status::failed_precondition(
            "read-only data mount requires a clean journal; recover with a read-write owner first",
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn verify_ext4_identity(superblock: &[u8; 0x78], expected: uuid::Uuid) -> Result<(), Status> {
    if superblock[0x38..0x3a] != [0x53, 0xef] || superblock[0x68..0x78] != *expected.as_bytes() {
        return Err(Status::failed_precondition(
            "guest data filesystem identity mismatch",
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn verify_permissions(directory: &File, read_only: bool) -> Result<(), Status> {
    let flags = nix::sys::statvfs::fstatvfs(directory)
        .map_err(|error| Status::internal(format!("stat guest data mount flags failed: {error}")))?
        .flags();
    if flags.contains(nix::sys::statvfs::FsFlags::ST_RDONLY) != read_only {
        return Err(Status::failed_precondition(
            "guest data mount permissions mismatch",
        ));
    }
    Ok(())
}

/// Enumerate the mounted device each time rather than retaining only an
/// in-process registry: a guest agent restart must not silently stop flushing
/// the data disk. Pin and verify every opened filesystem before syncfs.
#[cfg(target_os = "linux")]
pub(super) fn flush_data_drive(quiesce_for_stop: bool) -> Result<(), Status> {
    let Some(device) = data_device()? else {
        return Ok(());
    };
    let info = std::fs::read_to_string("/proc/self/mountinfo")
        .map_err(|error| Status::internal(format!("read guest mount table failed: {error}")))?;
    let identity = format!(
        "{}:{}",
        nix::sys::stat::major(device.rdev()),
        nix::sys::stat::minor(device.rdev())
    );
    let paths = data_mount_paths(&info, &identity)?;
    if paths.is_empty() {
        return Err(Status::failed_precondition(
            "guest data device is not mounted",
        ));
    }
    for path in paths {
        let filesystem = File::open(path).map_err(|error| {
            Status::internal(format!(
                "open guest data filesystem for flush failed: {error}"
            ))
        })?;
        if filesystem
            .metadata()
            .map_err(|error| Status::internal(error.to_string()))?
            .dev()
            != device.rdev()
        {
            return Err(Status::failed_precondition(
                "guest data mount changed during flush",
            ));
        }
        if quiesce_for_stop {
            // FIFREEZE blocks new writers and checkpoints ext4's journal,
            // including clearing needs_recovery. syncfs alone does neither.
            // Do not thaw: this execution is terminating, not being captured.
            // SAFETY: this is a pinned filesystem FD; FIFREEZE ignores the
            // integer argument. EBUSY makes a repeated stop attempt idempotent.
            match unsafe { freeze_filesystem(filesystem.as_raw_fd(), &mut 0) } {
                Ok(_) | Err(nix::errno::Errno::EBUSY) => {}
                Err(error) => {
                    return Err(Status::internal(format!(
                        "freeze guest data filesystem before stop failed: {error}"
                    )));
                }
            }
        } else {
            nix::unistd::syncfs(&filesystem).map_err(|error| {
                Status::internal(format!("syncfs guest data filesystem failed: {error}"))
            })?;
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn data_mount_paths(info: &str, identity: &str) -> Result<Vec<PathBuf>, Status> {
    let mut paths = Vec::new();
    for line in info.lines() {
        let fields: Vec<_> = line.split_ascii_whitespace().collect();
        let [_, _, device, _, mountpoint, _, _, ..] = fields.as_slice() else {
            return Err(Status::internal("malformed guest mount table"));
        };
        if *device != identity {
            continue;
        }
        let mut bytes = mountpoint.bytes();
        let mut decoded = Vec::with_capacity(mountpoint.len());
        while let Some(byte) = bytes.next() {
            if byte == b'\\' {
                let mut escape = [0; 3];
                for digit in &mut escape {
                    *digit = bytes
                        .next()
                        .ok_or_else(|| Status::internal("truncated guest mount escape"))?;
                }
                decoded.push(match &escape {
                    b"040" => b' ',
                    b"011" => b'\t',
                    b"012" => b'\n',
                    b"134" => b'\\',
                    _ => return Err(Status::internal("invalid guest mount escape")),
                });
            } else {
                decoded.push(byte);
            }
        }
        let path = PathBuf::from(std::ffi::OsString::from_vec(decoded));
        if !path.is_absolute() {
            return Err(Status::internal("relative guest mountpoint"));
        }
        paths.push(path);
    }
    Ok(paths)
}

#[cfg(not(target_os = "linux"))]
pub(super) fn mount_data_drive(_: MountDataDriveRequest) -> Result<(), Status> {
    Err(Status::failed_precondition(
        "block-volume mounts require Linux",
    ))
}

#[cfg(test)]
#[cfg(target_os = "linux")]
mod tests {
    use super::*;

    #[test]
    fn data_device_requires_the_recorded_ext4_identity_not_just_a_block_device_path() {
        let expected = uuid::Uuid::from_u128(0x12345678);
        let mut superblock = [0; 0x78];
        superblock[0x38..0x3a].copy_from_slice(&[0x53, 0xef]);
        superblock[0x68..0x78].copy_from_slice(expected.as_bytes());
        verify_ext4_identity(&superblock, expected).unwrap();
        assert_eq!(
            verify_ext4_identity(&superblock, uuid::Uuid::from_u128(0x12345679))
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        superblock[0x38] = 0;
        assert_eq!(
            verify_ext4_identity(&superblock, expected)
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[test]
    fn read_only_mount_refuses_journal_recovery_and_filesystem_errors() {
        let mut superblock = [0; 0x78];
        verify_read_only_clean(&superblock).unwrap();
        // Other incompatibility bits do not imply journal recovery.
        superblock[0x60..0x64].copy_from_slice(&0x40_u32.to_le_bytes());
        verify_read_only_clean(&superblock).unwrap();
        for (incompat, state) in [(0x44_u32, 0_u16), (0x40, 2)] {
            superblock[0x60..0x64].copy_from_slice(&incompat.to_le_bytes());
            superblock[0x3a..0x3c].copy_from_slice(&state.to_le_bytes());
            assert_eq!(
                verify_read_only_clean(&superblock).unwrap_err().code(),
                tonic::Code::FailedPrecondition
            );
        }
    }

    #[test]
    fn flush_discovers_every_mount_of_the_data_device_and_decodes_kernel_escapes() {
        let table = "1 0 8:0 / / rw - ext4 /dev/vda rw\n2 1 8:16 / /data\\040set rw - ext4 /dev/vdb rw\n3 1 8:16 / /mnt/data\\134backup rw - ext4 /dev/vdb rw\n";
        assert_eq!(
            data_mount_paths(table, "8:16").unwrap(),
            vec![
                PathBuf::from("/data set"),
                PathBuf::from("/mnt/data\\backup"),
            ]
        );
        assert!(data_mount_paths(table, "8:32").unwrap().is_empty());
        for invalid in [
            "bad",
            "2 1 8:16 / /bad\\999 rw - ext4 /dev/vdb rw",
            "2 1 8:16 / /bad\\04 rw - ext4 /dev/vdb rw",
        ] {
            assert_eq!(
                data_mount_paths(invalid, "8:16").unwrap_err().code(),
                tonic::Code::Internal
            );
        }
    }
}
