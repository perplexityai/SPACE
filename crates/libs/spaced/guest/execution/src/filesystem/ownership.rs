use std::os::unix::fs::OpenOptionsExt;

use tonic::Status;

use super::map_io_error;

pub(super) async fn chown_directory_no_follow(
    path: impl Into<std::path::PathBuf>,
    uid: u32,
    gid: u32,
) -> Result<(), Status> {
    let path = path.into();
    tokio::task::spawn_blocking(move || {
        // Pin the directory before changing ownership. A concurrent rename or
        // symlink swap cannot redirect fchown to a different inode.
        let directory = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(path)
            .map_err(|error| match error.raw_os_error() {
                Some(libc::ELOOP | libc::ENOTDIR) => Status::failed_precondition(
                    "ownership target must be a directory, not a symlink",
                ),
                _ => map_io_error(error),
            })?;
        chown_open_file(&directory, uid, gid)
    })
    .await
    .map_err(|error| Status::internal(format!("chown task panicked: {error}")))?
}

pub(super) fn chown_open_file(
    file: &impl std::os::fd::AsFd,
    uid: u32,
    gid: u32,
) -> Result<(), Status> {
    std::os::unix::fs::fchown(file, Some(uid), Some(gid)).map_err(map_io_error)
}

#[cfg(test)]
mod tests;
