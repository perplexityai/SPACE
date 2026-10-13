//! Filesystem authority is an open directory or file, never a validated path
//! which a cache peer can replace before a privileged operation.
use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::path::Path;

use nix::fcntl::{OFlag, openat};
use nix::sys::stat::Mode;
use nix::unistd::{UnlinkatFlags, unlinkat};
use tonic::Status;

use super::trusted_paths;

pub(super) const DIRECTORY: OFlag = OFlag::O_RDONLY
    .union(OFlag::O_DIRECTORY)
    .union(OFlag::O_NOFOLLOW)
    .union(OFlag::O_CLOEXEC);

pub(super) fn error(error: impl std::fmt::Display) -> Status {
    Status::failed_precondition(format!("artifact filesystem operation: {error}"))
}

pub(super) fn parent(path: &Path) -> Result<(File, &OsStr), Status> {
    let parent = path.parent().ok_or_else(|| error("missing parent"))?;
    let name = path.file_name().ok_or_else(|| error("missing name"))?;
    Ok((trusted_paths::open_directory(parent)?, name))
}

pub(super) fn directory(parent: &File, name: &OsStr) -> io::Result<File> {
    openat(parent, name, DIRECTORY, Mode::empty())
        .map(File::from)
        .map_err(Into::into)
}

pub(super) fn copy(source: &Path, destination: &File, name: &OsStr) -> Result<File, Status> {
    let (parent, source_name) = parent(source)?;
    let source = trusted_paths::open_regular_file(&parent, source_name)?;
    crate::contained_copy::stage_open_file(&source, destination, name).map_err(error)
}

/// Publish only a complete new inode. A replaced destination name cannot
/// redirect a write, and later permission changes use the returned descriptor.
pub(super) fn write_atomic(
    path: &Path,
    write: impl FnOnce(&File) -> Result<(), Status>,
) -> Result<File, Status> {
    let (parent, name) = parent(path)?;
    write_atomic_at(&parent, name, write)
}

pub(super) fn write_atomic_at(
    parent: &File,
    name: &OsStr,
    write: impl FnOnce(&File) -> Result<(), Status>,
) -> Result<File, Status> {
    let temporary = format!(".artifact-{}.tmp", uuid::Uuid::now_v7());
    let output = File::from(
        openat(
            parent,
            temporary.as_str(),
            OFlag::O_RDWR | OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
            Mode::from_bits_truncate(0o600),
        )
        .map_err(error)?,
    );
    let result = (|| {
        write(&output)?;
        output.sync_all().map_err(error)?;
        nix::fcntl::renameat(parent, temporary.as_str(), parent, name).map_err(error)?;
        parent.sync_all().map_err(error)
    })();
    if let Err(error) = result {
        let _ = unlinkat(parent, temporary.as_str(), UnlinkatFlags::NoRemoveDir);
        return Err(error);
    }
    Ok(output)
}

pub(super) fn delete(path: &Path) -> Result<(), Status> {
    remove(path).map(|_| ())
}

pub(super) fn unlink(path: &Path) -> Result<(), Status> {
    let (parent, name) = parent(path)?;
    unlinkat(&parent, name, UnlinkatFlags::NoRemoveDir).map_err(error)
}

pub(super) fn remove(path: &Path) -> Result<bool, Status> {
    // Missing ancestors are an idempotent absence, but symlinks and all other
    // failures must not become a pathname-based recursive fallback.
    let (parent, name) = match parent(path) {
        Ok(pair) => pair,
        Err(error) if error.code() == tonic::Code::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    remove_at(&parent, name)
}

pub(super) fn remove_at(parent: &File, name: &OsStr) -> Result<bool, Status> {
    crate::clone_cleanup::remove_at(parent, name).map_err(error)
}

pub(super) fn entries(directory: &File) -> Result<Vec<std::ffi::OsString>, Status> {
    use std::os::unix::ffi::OsStrExt;
    let mut entries =
        nix::dir::Dir::openat(directory, ".", DIRECTORY, Mode::empty()).map_err(error)?;
    let mut names = Vec::new();
    for entry in entries.iter() {
        let entry = entry.map_err(error)?;
        let name = entry.file_name().to_bytes();
        if matches!(name, b"." | b"..") {
            continue;
        }
        if names.len() >= 100_000 {
            return Err(Status::resource_exhausted(
                "artifact enumeration limit exceeded",
            ));
        }
        names.push(OsStr::from_bytes(name).to_owned());
    }
    Ok(names)
}

pub(super) fn remove_empty_parent(path: &Path) -> Result<(), Status> {
    let (parent, name) = parent(path)?;
    // rmdir is an atomic emptiness check. Never promote a failed rmdir into
    // recursive subvolume destruction after a racy userspace emptiness probe.
    unlinkat(&parent, name, UnlinkatFlags::RemoveDir).map_err(error)
}

pub(super) fn snapshot(source: &Path, destination: &Path, readonly: bool) -> Result<(), Status> {
    let source = trusted_paths::open_directory(source)?;
    let (parent, name) = parent(destination)?;
    btrfs::snapshot(&source, &parent, name, readonly).map_err(error)
}

pub(super) fn info(
    path: &Path,
) -> Result<pplx_space_common_core::host::btrfs::SubvolumeInfo, Status> {
    btrfs::info(&trusted_paths::open_directory(path)?)
        .map(|(_, info)| info)
        .map_err(error)
}

pub(super) fn set_readonly(path: &Path, readonly: bool) -> Result<(), Status> {
    btrfs::set_readonly(&trusted_paths::open_directory(path)?, readonly).map_err(error)
}

pub(super) fn is_subvolume(path: &Path) -> Result<bool, Status> {
    btrfs::is_subvolume(&trusted_paths::open_directory(path)?).map_err(error)
}

/// A retry may discard the staging sibling: before exchange it is an
/// incomplete replacement; afterwards it is the superseded instance. The
/// canonical instance is never removed before a complete replacement exists.
pub(super) fn rollback(parent: &File, source: &File, gid: u32) -> Result<(), Status> {
    // Full memory-only snapshots cannot replace an instance's disk. Preserve
    // the completeness check for subvolumes as well as ordinary directories.
    let rootfs = trusted_paths::open_regular_file(source, "rootfs.ext4".as_ref())?;
    if rootfs.metadata().map_err(error)?.len() == 0 {
        return Err(Status::failed_precondition(
            "cannot restore disk state: rootfs.ext4 is empty; re-export with include_rootfs=true",
        ));
    }
    let staging = OsStr::new(".instance-rollback");
    remove_at(parent, staging)?;
    if btrfs::is_subvolume(source).map_err(error)? {
        btrfs::snapshot(source, parent, staging, false).map_err(error)?;
    } else {
        // Validate required input before creating anything, then copy every
        // disk artifact through no-follow descriptors into the private stage.
        btrfs::create(parent, staging).map_err(error)?;
        let stage = directory(parent, staging).map_err(error)?;
        for name in entries(source)? {
            if matches!(name.to_str(), Some("vmstate" | "memory" | "snapshot.json")) {
                continue;
            }
            let input = trusted_paths::open_regular_file(source, &name)?;
            crate::contained_copy::stage_open_file(&input, &stage, &name).map_err(error)?;
        }
        stage.sync_all().map_err(error)?;
    }
    let stage = directory(parent, staging).map_err(error)?;
    let rootfs = trusted_paths::open_regular_file(&stage, "rootfs.ext4".as_ref())?;
    if rootfs.metadata().map_err(error)?.len() == 0 {
        return Err(Status::failed_precondition("staged rootfs.ext4 is empty"));
    }
    // Keep normal cache permissions after publication, including setgid.
    trusted_paths::set_permissions(&stage, Some(gid), 0o2770)?;
    match directory(parent, "instance".as_ref()) {
        Ok(_) => exchange(parent, staging, "instance".as_ref()).map_err(error)?,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            nix::fcntl::renameat(parent, staging, parent, "instance").map_err(error)?;
        }
        Err(e) => return Err(error(e)),
    }
    parent.sync_all().map_err(error)?;
    if let Err(error) = remove_at(parent, staging) {
        tracing::warn!(%error, "rollback committed; superseded instance retained for cleanup/retry");
    }
    Ok(())
}

fn exchange(parent: &File, from: &OsStr, to: &OsStr) -> io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        rustix::fs::renameat_with(parent, from, parent, to, rustix::fs::RenameFlags::EXCHANGE)
            .map_err(Into::into)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (parent, from, to);
        Err(io::ErrorKind::Unsupported.into())
    }
}

pub(super) use crate::btrfs;
pub(super) mod receive;

#[cfg(test)]
mod tests;
