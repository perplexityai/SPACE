use std::ffi::OsStr;
use std::fs::File;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path};

use nix::fcntl::{OFlag, open, openat};
use nix::sys::stat::{Mode, mkdirat};
use nix::unistd::{Gid, fchown, geteuid};
use pplx_space_host_layout::{identities::groups, paths};

/// Sandbox-manager creates the destination; volume-manager writes the pull.
/// Establish shared access even when the cache does not inherit its group.
pub(crate) fn prepare(path: &Path) -> std::io::Result<()> {
    let cache = Path::new(paths::SANDBOX_CACHE_DIR);
    if path == cache || !path.starts_with(cache) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "transfer directory is outside the sandbox cache",
        ));
    }
    let root = Path::new("/space");
    let relative = path
        .strip_prefix(root)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    prepare_under(root, relative)
}

/// [`prepare`] for `relative` below the installation root `root`.
pub(crate) fn prepare_under(root: &Path, relative: &Path) -> std::io::Result<()> {
    if relative.components().next().is_none()
        || relative
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "expected a directory below the cache root",
        ));
    }
    let flags = OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
    // /space may be an administrator-owned devbox symlink. Descendants,
    // including the cache prefix, must not redirect the permission grant.
    let entry = std::fs::symlink_metadata(root)?;
    if entry.uid() != 0 || (!entry.is_symlink() && entry.mode() & 0o022 != 0) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "installation root is not administrator-controlled",
        ));
    }
    let mut directory = File::from(open(root, flags & !OFlag::O_NOFOLLOW, Mode::empty())?);
    let anchor = directory.metadata()?;
    if anchor.uid() != 0 || anchor.mode() & 0o022 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "installation root is writable by other users",
        ));
    }
    for part in relative.components() {
        let name = part.as_os_str();
        let created = match mkdirat(&directory, name, Mode::from_bits_truncate(0o775)) {
            Ok(()) => true,
            Err(nix::errno::Errno::EEXIST) => false,
            Err(error) => return Err(error.into()),
        };
        directory = File::from(openat(&directory, name, flags, Mode::empty())?);
        // A restrictive umask must not make newly created parents untraversable.
        if created {
            share_directory(&directory, shared_cache_group(), 0o2050)?;
        }
    }
    share_directory(&directory, shared_cache_group(), 0o2070)
}

/// The group through which sandbox-manager and volume-manager share cache
/// transfers.
pub(crate) fn shared_cache_group() -> Gid {
    Gid::from_raw(groups::SPACE_CACHE_RW.gid)
}

/// Create directory `name` in `parent` for members of `group` to read
/// through, whatever the umask and whether `parent` passes its group on.
/// Production passes [`shared_cache_group`].
///
/// `parent` must be the administrator-controlled cache directory: it is opened
/// following symlinks, unlike the anchored walk in [`prepare`]. Only `name`
/// is opened without following one.
pub(crate) fn prepare_readable_child(
    parent: &Path,
    name: &OsStr,
    group: Gid,
) -> std::io::Result<()> {
    let mut parts = Path::new(name).components();
    if !matches!(
        (parts.next(), parts.next()),
        (Some(Component::Normal(_)), None)
    ) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "expected one directory name",
        ));
    }
    let flags = OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC;
    let parent = File::from(open(parent, flags, Mode::empty())?);
    match mkdirat(&parent, name, Mode::from_bits_truncate(0o750)) {
        Ok(()) | Err(nix::errno::Errno::EEXIST) => {}
        Err(error) => return Err(error.into()),
    }
    let directory = File::from(openat(
        &parent,
        name,
        flags | OFlag::O_NOFOLLOW,
        Mode::empty(),
    )?);
    share_directory(&directory, group, 0o2050)
}

fn share_directory(directory: &File, group: Gid, required_mode: u32) -> std::io::Result<()> {
    let metadata = directory.metadata()?;
    let mode = metadata.permissions().mode();
    let shared_gid = group.as_raw();
    // Correct root- or sibling-owned directories need no chmod/chown on retry.
    if metadata.gid() == shared_gid && mode & required_mode == required_mode {
        return Ok(());
    }
    if metadata.uid() != geteuid().as_raw() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "transfer directory requires repair by its owner",
        ));
    }
    if metadata.gid() != shared_gid {
        fchown(directory, None, Some(group))?;
    }
    directory.set_permissions(std::fs::Permissions::from_mode(
        (mode & 0o7777) | required_mode,
    ))?;
    // chmod may silently drop setgid when group membership is missing.
    let actual = directory.metadata()?;
    if actual.gid() != shared_gid || actual.mode() & required_mode != required_mode {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "transfer directory lacks shared group access",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
