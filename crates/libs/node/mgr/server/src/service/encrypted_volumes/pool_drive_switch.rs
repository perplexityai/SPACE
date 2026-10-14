use std::fs::File;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};

use pplx_space_common_core::host::linux::paths;
use pplx_space_common_core::types::SandboxId;
use tonic::Status;

use super::super::{JAIL_DIR, RUN_DIR, runtime, trusted_paths};

pub(super) async fn switch(id: &SandboxId, device: &Path) -> Result<(), Status> {
    let id = *id;
    let device = device.to_owned();
    let binding = device.clone();
    let needs_switch = tokio::task::spawn_blocking(move || bind_drive(id, &binding))
        .await
        .map_err(|e| Status::internal(format!("bind pool drive task: {e}")))??;
    if needs_switch {
        let socket = format!("{JAIL_DIR}/firecracker/{id}/root/firecracker.sock");
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            crate::fc_api::FcApiClient::new(socket).update_drive_path("rootfs", "/rootfs.ext4"),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("pool drive switch timed out"))?
        .map_err(|e| Status::unavailable(format!("pool drive switch failed: {e}")))?;
    }
    if tokio::task::spawn_blocking(move || bind_drive(id, &device))
        .await
        .map_err(|e| Status::internal(format!("verify pool drive task: {e}")))??
    {
        return Err(Status::failed_precondition(
            "Firecracker did not open the encrypted rootfs device",
        ));
    }
    Ok(())
}

fn bind_drive(id: SandboxId, device: &Path) -> Result<bool, Status> {
    let io = |e| Status::internal(format!("bind pool rootfs: {e}"));
    let pid = runtime::registered_firecracker_pid(Path::new(RUN_DIR), &id.to_string())?;
    let warm_path = PathBuf::from(paths::sandbox_instance_dir(&id.to_string()))
        .join(crate::rootfs_dmcrypt::POOL_ROOTFS_FILE);
    let warm = crate::device_backing::Backing::require(
        crate::device_backing::Backing::open(&warm_path, false).map_err(io)?,
    )
    .map_err(io)?;
    let warm_meta = warm.file.metadata().map_err(io)?;
    let source = trusted_paths::open_rootfs_device(device)?;
    let source_meta = source.metadata().map_err(io)?;
    let mut has_file = false;
    let mut has_device = false;
    for entry in std::fs::read_dir(format!("/proc/{}/fd", pid.get())).map_err(io)? {
        let path = entry.map_err(io)?.path();
        let metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(io(e)),
        };
        has_device |=
            metadata.file_type().is_block_device() && metadata.rdev() == source_meta.rdev();
        has_file |= metadata.is_file()
            && metadata.dev() == warm_meta.dev()
            && metadata.ino() == warm_meta.ino();
    }
    if !has_file && !has_device {
        return Err(Status::failed_precondition(
            "Firecracker does not own this pool drive",
        ));
    }
    let permissions = File::open(format!("/proc/self/fd/{}", source.as_raw_fd())).map_err(io)?;
    trusted_paths::reown(&permissions, warm_meta.uid())?;
    let jail = PathBuf::from(format!("{JAIL_DIR}/firecracker/{id}/root"));
    let directory = trusted_paths::open_directory(&jail)?;
    let target = jail.join("rootfs.ext4");
    let metadata = std::fs::symlink_metadata(&target).map_err(io)?;
    if metadata.file_type().is_block_device() {
        if metadata.rdev() != source_meta.rdev() {
            return Err(Status::failed_precondition(
                "pool jail contains another rootfs device",
            ));
        }
    } else {
        let previous =
            trusted_paths::open_regular_file(&directory, std::ffi::OsStr::new("rootfs.ext4"))?;
        let metadata = previous.metadata().map_err(io)?;
        if metadata.dev() == warm_meta.dev() && metadata.ino() == warm_meta.ino() {
            #[cfg(target_os = "linux")]
            nix::mount::umount2(&target, nix::mount::MntFlags::MNT_DETACH)
                .map_err(|e| io(std::io::Error::from(e)))?;
        }
        let target =
            trusted_paths::open_bind_target(&directory, std::ffi::OsStr::new("rootfs.ext4"))?;
        trusted_paths::bind_pinned_files(&source, &target)?;
    }
    // A retry must not PATCH an already switched drive: draining its pending
    // I/O would wait forever on the deliberately suspended adoption device.
    Ok(!has_device)
}
