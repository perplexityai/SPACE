//! Overwriting a file by replacing it, so a failed or interrupted overwrite
//! leaves the previous content in place and no reader sees a partial file.
//!
//! Replacing must not change anything about the destination but its content,
//! or let anyone do what overwriting would not. So an overwrite replaces only
//! a regular file with one link and no access ACL, owned by the writer, that
//! the writer can open for writing, in a directory where the writer can create
//! the temporary file. Anything else, and every append, is written in place as
//! before.
//!
//! The temporary file's writes complete before the rename, which publishes
//! it atomically. Neither the file nor the rename is synced; see
//! [`Replacement::commit`].

use std::io;
use std::path::{Path, PathBuf};

use tonic::Status;

use super::map_io_error;

/// Where a write's bytes go until it completes.
pub(super) struct WriteTarget {
    pub(super) file: tokio::fs::File,
    /// Set when the bytes go to a temporary file that replaces the destination
    /// once the write completes.
    pub(super) replacement: Option<Replacement>,
}

/// A temporary file next to a write's destination. Until [`Self::commit`]
/// renames it over the destination, [`Self::discard`] or dropping it removes
/// it.
pub(super) struct Replacement {
    temporary: PathBuf,
    destination: PathBuf,
    /// The existing destination, opened for writing: the proof that it may be
    /// overwritten, and the target of the in-place fallback.
    existing: Option<std::fs::File>,
    /// The temporary file the upload writes, kept open so that the fallback
    /// copies the uploaded inode. Another user who may write the directory
    /// can replace the temporary name, but not this descriptor.
    upload: std::fs::File,
    /// Committed or discarded, so there is nothing left to remove.
    settled: bool,
}

impl Drop for Replacement {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let temporary = std::mem::take(&mut self.temporary);
        let remove = move || {
            if let Err(error) = std::fs::remove_file(&temporary) {
                tracing::debug!(%error, "failed to remove an abandoned write's temporary file");
            }
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => drop(runtime.spawn_blocking(remove)),
            Err(_) => remove(),
        }
    }
}

impl Replacement {
    /// Remove the temporary file before returning. Dropping only schedules
    /// the removal, which a caller that lists the directory could outrun.
    pub(super) async fn discard(mut self) {
        self.settled = true;
        let temporary = std::mem::take(&mut self.temporary);
        let removed = tokio::task::spawn_blocking(move || std::fs::remove_file(&temporary)).await;
        if let Ok(Err(error)) = removed {
            tracing::debug!(%error, "failed to remove an interrupted write's temporary file");
        }
    }

    pub(super) async fn commit(mut self) -> Result<(), Status> {
        let temporary = self.temporary.clone();
        let destination = self.destination.clone();
        let existing = self
            .existing
            .as_ref()
            .map(std::fs::File::try_clone)
            .transpose()
            .map_err(map_io_error)?;
        let upload = self.upload.try_clone().map_err(map_io_error)?;
        let replaced = tokio::task::spawn_blocking(move || {
            match std::fs::rename(&temporary, &destination) {
                // Neither the temporary file nor the rename is synced: the
                // rename publishes the complete file atomically, and the
                // filesystem persists both in its own time. An ordinary stop
                // attempts to flush the guest's root filesystem first; a
                // guest crash, a forced stop, a failed flush or a host
                // failure can lose the replacement, leaving the
                // destination's previous content (or no new file). To make
                // a replacement persistent at acknowledgement instead, at
                // several times the disk requests per small upload, sync the
                // temporary file before the rename (`finalize_written_file`),
                // keep the destination's directory open from
                // `open_replacement`, and:
                //     Ok(()) => directory.sync_all(),
                Ok(()) => Ok(()),
                // A bind-mounted destination cannot be renamed over. Copy into
                // it in place, as the write would have done.
                Err(error) if matches!(error.raw_os_error(), Some(libc::EBUSY | libc::EXDEV)) => {
                    match existing {
                        Some(existing) => copy_upload_in_place(&upload, existing, &temporary),
                        None => Err(error),
                    }
                }
                Err(error) => Err(error),
            }
        })
        .await
        .map_err(|error| Status::internal(format!("file replace task failed: {error}")))?;
        replaced.map_err(map_io_error)?;
        // Renamed or copied, the temporary file no longer exists.
        self.settled = true;
        Ok(())
    }
}

/// Copy the uploaded bytes from `upload`, the temporary file's own
/// descriptor, into `target`, then remove the temporary name if it still
/// names that file. The name is never opened: it may now name another file.
fn copy_upload_in_place(
    upload: &std::fs::File,
    mut target: std::fs::File,
    temporary: &Path,
) -> io::Result<()> {
    use std::io::Seek;
    use std::os::unix::fs::MetadataExt;
    // The upload may have left O_DIRECT on the file description it shares
    // with this descriptor, which would constrain the copy's reads. Its
    // writer is gone, so the shared offset is this copy's to rewind.
    super::upload::set_direct_io(upload, false)?;
    let mut source = upload;
    source.rewind()?;
    target.set_len(0)?;
    target.rewind()?;
    // Between files this copies in the kernel (copy_file_range), with no
    // buffer to allocate after the destination is already truncated.
    io::copy(&mut source, &mut target)?;
    target.sync_all()?;
    let uploaded = upload.metadata()?;
    match std::fs::symlink_metadata(temporary) {
        Ok(named) if (named.dev(), named.ino()) == (uploaded.dev(), uploaded.ino()) => {
            std::fs::remove_file(temporary)
        }
        Ok(_) | Err(_) => {
            tracing::warn!(
                path = %temporary.display(),
                "a write's temporary name no longer names its upload; leaving it"
            );
            Ok(())
        }
    }
}

/// Open the destination of a write: a temporary file to replace it with on
/// completion where that is allowed, otherwise the destination itself.
pub(super) async fn open_write_target(
    destination: PathBuf,
    append: bool,
    owner_uid: u32,
) -> Result<WriteTarget, Status> {
    if !append {
        let target = destination.clone();
        let replacement = tokio::task::spawn_blocking(move || open_replacement(target, owner_uid))
            .await
            .map_err(|error| Status::internal(format!("file open task failed: {error}")))?
            .map_err(map_io_error)?;
        if let Some((file, replacement)) = replacement {
            return Ok(WriteTarget {
                file: tokio::fs::File::from_std(file),
                replacement: Some(replacement),
            });
        }
    }
    let mut options = tokio::fs::OpenOptions::new();
    options.create(true).write(true);
    options.custom_flags(libc::O_NOFOLLOW);
    if append {
        options.append(true);
    } else {
        options.truncate(true);
    }
    let file = options.open(&destination).await.map_err(map_io_error)?;
    Ok(WriteTarget {
        file,
        replacement: None,
    })
}

/// Create the temporary file for an overwrite, or `None` to write in place.
/// Whatever the in-place open would refuse, it refuses with the caller's
/// context. Any other failure, a full disk among them, fails the write:
/// writing in place then would leave a partial file, the outcome replacing
/// exists to prevent.
fn open_replacement(
    destination: PathBuf,
    owner_uid: u32,
) -> io::Result<Option<(std::fs::File, Replacement)>> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    let metadata = match std::fs::symlink_metadata(&destination) {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(_) => return Ok(None),
    };
    if let Some(metadata) = &metadata
        && (!metadata.file_type().is_file() || metadata.nlink() != 1 || metadata.uid() != owner_uid)
    {
        return Ok(None);
    }
    // Opening for writing is the check overwriting would make, ACLs included.
    let existing = match &metadata {
        None => None,
        Some(metadata) => {
            let Ok(file) = std::fs::OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&destination)
            else {
                return Ok(None);
            };
            let opened = file.metadata()?;
            if (opened.dev(), opened.ino()) != (metadata.dev(), metadata.ino()) {
                // Replaced since it was inspected; do what an overwrite does.
                return Ok(None);
            }
            // A replacement carries the mode but not an ACL; an unreadable
            // ACL is treated as present.
            if has_access_acl(&file).unwrap_or(true) {
                return Ok(None);
            }
            Some(file)
        }
    };
    let (Some(file_name), Some(parent)) = (destination.file_name(), destination.parent()) else {
        return Ok(None);
    };
    let mut name = std::ffi::OsString::from(".");
    name.push(file_name);
    name.push(format!(".spaced-{}", uuid::Uuid::now_v7().simple()));
    let temporary = parent.join(name);
    // A new destination is created as the in-place write would create it.
    // A replacement for an existing one starts owner-only, so nobody gains
    // access the destination denies before it takes the destination's group
    // and mode: a descriptor opened meanwhile would outlive any later chmod.
    let creation_mode = if metadata.is_some() { 0o600 } else { 0o666 };
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(creation_mode)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temporary)
    {
        Ok(file) => file,
        Err(error) if directory_refuses_temporary_file(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    // The guard owns the temporary file before anything else can fail, so
    // that every failure from here on removes it.
    let replacement = Replacement {
        temporary,
        destination,
        existing,
        upload: file,
        settled: false,
    };
    let file = replacement.upload.try_clone()?;
    if let Some(metadata) = metadata {
        // Group first, ACL next and mode last, so that the mode is never in
        // force with another group or an ACL, and a group change cannot clear
        // the set-id bits it carries.
        let keeps_metadata = (|| {
            if file.metadata()?.gid() != metadata.gid() {
                std::os::unix::fs::fchown(&file, None, Some(metadata.gid()))?;
            }
            // The directory's default ACL can give the temporary file an
            // access ACL the destination does not have (the destination has
            // none, or it is written in place). With the destination's mode
            // applied, its named entries would take effect.
            remove_access_acl(&file)?;
            file.set_permissions(std::fs::Permissions::from_mode(metadata.mode() & 0o7777))
        })();
        match keeps_metadata {
            Ok(()) => {}
            // Dropping the replacement removes the temporary file.
            Err(error) if directory_refuses_temporary_file(&error) => return Ok(None),
            Err(error) => return Err(error),
        }
    }
    Ok(Some((file, replacement)))
}

/// Whether `file` has an access ACL. Its mode's group bits are then the ACL's
/// mask, so a replacement carrying only the mode would give the owning group
/// what the ACL gave named users and groups, whatever it gave the group.
#[cfg(target_os = "linux")]
fn has_access_acl(file: &std::fs::File) -> io::Result<bool> {
    let mut size_query: [u8; 0] = [];
    match rustix::fs::fgetxattr(file, "system.posix_acl_access", &mut size_query[..]) {
        Ok(_) | Err(rustix::io::Errno::RANGE) => Ok(true),
        Err(rustix::io::Errno::NODATA | rustix::io::Errno::NOTSUP) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

#[cfg(not(target_os = "linux"))]
fn has_access_acl(_file: &std::fs::File) -> io::Result<bool> {
    Ok(false)
}

#[cfg(target_os = "linux")]
fn remove_access_acl(file: &std::fs::File) -> io::Result<()> {
    match rustix::fs::fremovexattr(file, "system.posix_acl_access") {
        Ok(()) | Err(rustix::io::Errno::NODATA | rustix::io::Errno::NOTSUP) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(not(target_os = "linux"))]
fn remove_access_acl(_file: &std::fs::File) -> io::Result<()> {
    Ok(())
}

fn directory_refuses_temporary_file(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::EACCES | libc::EPERM | libc::EROFS | libc::ENAMETOOLONG)
    )
}

#[cfg(test)]
mod tests;
