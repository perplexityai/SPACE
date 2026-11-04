//! Prepared artifacts live below a shared cache. Only the installation root
//! may resolve symlinks; cache and sandbox-relative lookups are pinned.
use std::fs::File;
use std::io::{self, Read as _, Write as _};
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use nix::fcntl::{OFlag, openat, renameat};
use nix::sys::stat::{Mode, mkdirat};
use nix::unistd::{UnlinkatFlags, unlinkat};

const DIRECTORY: OFlag = OFlag::O_RDONLY
    .union(OFlag::O_DIRECTORY)
    .union(OFlag::O_NOFOLLOW)
    .union(OFlag::O_CLOEXEC);

pub(super) fn directory(path: &Path, create: bool) -> io::Result<File> {
    let sandbox = path
        .parent()
        .ok_or_else(|| io::Error::other("missing sandbox directory"))?;
    let cache = sandbox
        .parent()
        .ok_or_else(|| io::Error::other("missing cache directory"))?;
    let name = sandbox
        .file_name()
        .ok_or_else(|| io::Error::other("missing sandbox name"))?;
    if path.file_name() != Some(std::ffi::OsStr::new("instance")) {
        return Err(io::Error::other("expected prepared instance directory"));
    }
    // /space may be an administrator-installed symlink. Everything beneath it,
    // including the shared cache prefix, must be walked without following links.
    let cache = super::super::trusted_paths::open_directory(cache).map_err(io::Error::other)?;
    if create {
        match mkdirat(&cache, name, Mode::from_bits_truncate(0o755)) {
            Ok(()) | Err(nix::errno::Errno::EEXIST) => {}
            Err(error) => return Err(error.into()),
        }
    }
    let sandbox = File::from(openat(&cache, name, DIRECTORY, Mode::empty())?);
    match openat(&sandbox, "instance", DIRECTORY, Mode::empty()) {
        Ok(fd) => Ok(File::from(fd)),
        Err(nix::errno::Errno::ENOENT) if create => {
            create_instance(&sandbox)?;
            Ok(File::from(openat(
                &sandbox,
                "instance",
                DIRECTORY,
                Mode::empty(),
            )?))
        }
        Err(error) => Err(error.into()),
    }
}

fn create_instance(parent: &File) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    if nix::sys::statfs::fstatfs(parent)?.filesystem_type() == nix::sys::statfs::BTRFS_SUPER_MAGIC {
        return crate::btrfs::create_subvolume(parent, "instance".as_ref());
    }

    mkdirat(parent, "instance", Mode::from_bits_truncate(0o755))?;
    Ok(())
}

pub(super) fn regular(directory: &File, name: &std::ffi::OsStr) -> io::Result<File> {
    let file = File::from(openat(
        directory,
        name,
        OFlag::O_RDONLY | OFlag::O_NONBLOCK | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )?);
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "prepared artifact must be a single-link regular file",
        ));
    }
    Ok(file)
}

pub(super) fn read_marker(directory: &File, name: &str) -> io::Result<Option<String>> {
    let file = match regular(directory, name.as_ref()) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    // A u32 plus whitespace is at most 11 bytes. Bound reads even when a cache
    // peer replaces or grows the marker after its initial metadata check.
    let mut value = String::new();
    file.take(12).read_to_string(&mut value)?;
    if value.len() > 11 {
        return Err(io::Error::other(
            "prepared rootfs marker exceeds size limit",
        ));
    }
    Ok(Some(value))
}

pub(super) fn write_marker(directory: &File, name: &str, size: u32) -> io::Result<()> {
    let temporary_name = format!(".{name}.{}.tmp", uuid::Uuid::now_v7());
    let mut file = File::from(openat(
        directory,
        temporary_name.as_str(),
        OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o644),
    )?);
    let result = (|| {
        write!(file, "{size}")?;
        file.sync_all()?;
        rename(directory, &temporary_name, name)
    })();
    if result.is_err()
        && let Err(error) = remove(directory, temporary_name.as_ref())
    {
        tracing::warn!(%error, "cannot remove temporary prepared rootfs marker");
    }
    result
}

pub(super) fn rename(directory: &File, from: &str, to: &str) -> io::Result<()> {
    renameat(directory, from, directory, to)?;
    directory.sync_all()
}

pub(super) fn remove(directory: &File, name: &std::ffi::OsStr) -> io::Result<()> {
    // Never recursively delete a directory planted where a file belongs. Cache
    // peers could graft a different sandbox's directory here. Retain the disk
    // reservation and require repair for malformed directory-shaped artifacts.
    match unlinkat(directory, name, UnlinkatFlags::NoRemoveDir) {
        Ok(()) => directory.sync_all(),
        Err(nix::errno::Errno::ENOENT) => Ok(()),
        Err(error) => Err(error.into()),
    }
}
