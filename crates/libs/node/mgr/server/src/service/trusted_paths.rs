//! Pin privileged inputs before mutating or mounting them. Path validation alone
//! does not protect a subsequent pathname lookup from replacement.
use std::fs::File;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Component, Path, PathBuf};

use nix::fcntl::{OFlag, open, openat};
use nix::sys::stat::{Mode, fchmod};
use nix::unistd::{Gid, Uid, fchown};
use pplx_space_common_core::types::SandboxId;
use tonic::Status;

const DIRECTORY: OFlag = OFlag::O_RDONLY
    .union(OFlag::O_DIRECTORY)
    .union(OFlag::O_NOFOLLOW)
    .union(OFlag::O_CLOEXEC);

fn path_error(error: impl std::fmt::Display) -> Status {
    Status::failed_precondition(format!("unsafe or unavailable privileged path: {error}"))
}

fn require_path(actual: &str, expected: PathBuf) -> Result<(), Status> {
    // Compare the spelling too: Path equality normalizes some degenerate forms.
    if actual != expected.to_string_lossy() {
        return Err(Status::invalid_argument(
            "path does not belong to this sandbox",
        ));
    }
    Ok(())
}

pub(super) fn canonical_sandbox_id(raw: &str) -> Result<SandboxId, Status> {
    let id: SandboxId = raw.parse().map_err(path_error)?;
    if raw != id.to_string() {
        return Err(Status::invalid_argument(
            "sandbox_id must use canonical UUID spelling",
        ));
    }
    Ok(id)
}

pub(super) fn validate_rootfs_device(id: &str, path: &str) -> Result<(), Status> {
    let id = canonical_sandbox_id(id)?;
    require_path(path, format!("/dev/mapper/sbxroot-{id}").into())
}

pub(super) fn validate_snapshot_volume(id: &str, path: &str) -> Result<(), Status> {
    let id = canonical_sandbox_id(id)?;
    require_path(
        path,
        Path::new(super::CACHE_DIR)
            .join(id.to_string())
            .join("snapshot-vol"),
    )
}

pub(crate) fn open_directory(path: &Path) -> Result<File, Status> {
    if !path.is_absolute() {
        return Err(Status::invalid_argument(
            "privileged directory must be absolute",
        ));
    }
    if let Ok(relative) = path.strip_prefix("/space") {
        return open_beneath_installation(Path::new("/space"), relative, 0);
    }
    walk_directory(
        File::from(open(Path::new("/"), DIRECTORY, Mode::empty()).map_err(path_error)?),
        path.strip_prefix("/").map_err(path_error)?,
    )
}

fn open_beneath_installation(root: &Path, relative: &Path, owner: u32) -> Result<File, Status> {
    // /space is installed by the host administrator. Devboxes deliberately
    // point it into the administrator's persistent directory. Trust that fixed
    // root-owned entry, not caller-supplied symlinks beneath it. Cache children
    // are intentionally group-writable and are walked with O_NOFOLLOW.
    let entry = std::fs::symlink_metadata(root).map_err(path_error)?;
    if entry.uid() != owner || (!entry.is_symlink() && entry.mode() & 0o022 != 0) {
        return Err(path_error("installation root is not administrator-owned"));
    }
    let directory =
        File::from(open(root, DIRECTORY & !OFlag::O_NOFOLLOW, Mode::empty()).map_err(path_error)?);
    let metadata = directory.metadata().map_err(path_error)?;
    if metadata.uid() != owner || metadata.mode() & 0o022 != 0 {
        return Err(path_error(
            "installation root is writable by group or others",
        ));
    }
    walk_directory(directory, relative)
}

fn walk_directory(mut directory: File, relative: &Path) -> Result<File, Status> {
    for component in relative.components() {
        match component {
            Component::Normal(name) => {
                directory = File::from(
                    openat(&directory, Path::new(name), DIRECTORY, Mode::empty()).map_err(
                        |error| {
                            if error == nix::errno::Errno::ENOENT {
                                Status::not_found("privileged directory component is absent")
                            } else {
                                path_error(error)
                            }
                        },
                    )?,
                );
            }
            _ => {
                return Err(Status::invalid_argument(
                    "invalid privileged path component",
                ));
            }
        }
    }
    Ok(directory)
}

pub(crate) fn create_directory(path: &Path) -> Result<File, Status> {
    if path == Path::new("/") || path == Path::new("/space") {
        return open_directory(path);
    }
    let parent = create_directory(path.parent().ok_or_else(|| path_error("missing parent"))?)?;
    let name = path
        .file_name()
        .ok_or_else(|| path_error("missing directory name"))?;
    match nix::sys::stat::mkdirat(&parent, name, Mode::from_bits_truncate(0o700)) {
        Ok(()) | Err(nix::errno::Errno::EEXIST) => {}
        Err(error) => return Err(path_error(error)),
    }
    openat(&parent, name, DIRECTORY, Mode::empty())
        .map(File::from)
        .map_err(path_error)
}

pub(super) fn permission_inode(path: &Path) -> Result<File, Status> {
    let parent = open_directory(path.parent().ok_or_else(|| path_error("missing parent"))?)?;
    let name = path
        .file_name()
        .ok_or_else(|| path_error("missing file name"))?;
    let file = File::from(
        openat(
            &parent,
            name,
            OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC,
            Mode::empty(),
        )
        .map_err(path_error)?,
    );
    let metadata = file.metadata().map_err(path_error)?;
    if !metadata.is_dir() && (!metadata.is_file() || metadata.nlink() != 1) {
        return Err(path_error(
            "permission target must be a directory or single-link regular file",
        ));
    }
    Ok(file)
}

pub(super) fn open_regular_file(directory: &File, name: &std::ffi::OsStr) -> Result<File, Status> {
    open_regular_file_with_flags(directory, name, OFlag::O_RDONLY)
}

pub(super) fn open_bind_target(directory: &File, name: &std::ffi::OsStr) -> Result<File, Status> {
    // Do not truncate on retries. Validate the opened inode before mounting;
    // pathname checks followed by File::create can follow a replacement link.
    open_regular_file_with_flags(directory, name, OFlag::O_RDONLY | OFlag::O_CREAT)
}

pub(super) fn open_bind_directory(
    directory: &File,
    name: &std::ffi::OsStr,
) -> Result<File, Status> {
    // The parent may be writable by the sandbox uid. Create and open the
    // mountpoint relative to the pinned parent so a planted link cannot
    // redirect a root-owned mount outside it.
    require_file_name(name)?;
    match nix::sys::stat::mkdirat(directory, name, Mode::from_bits_truncate(0o700)) {
        Ok(()) | Err(nix::errno::Errno::EEXIST) => {}
        Err(error) => return Err(path_error(error)),
    }
    openat(directory, name, DIRECTORY, Mode::empty())
        .map(File::from)
        .map_err(path_error)
}

fn require_file_name(name: &std::ffi::OsStr) -> Result<(), Status> {
    if Path::new(name).components().count() != 1
        || !matches!(
            Path::new(name).components().next(),
            Some(Component::Normal(_))
        )
    {
        return Err(path_error("expected a file name"));
    }
    Ok(())
}

fn open_regular_file_with_flags(
    directory: &File,
    name: &std::ffi::OsStr,
    flags: OFlag,
) -> Result<File, Status> {
    require_file_name(name)?;
    let file = File::from(
        openat(
            directory,
            Path::new(name),
            flags | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC,
            Mode::from_bits_truncate(0o600),
        )
        .map_err(path_error)?,
    );
    let metadata = file.metadata().map_err(path_error)?;
    // A restore-memory bind can retain an inode after its artifact name is
    // retired. Zero links are safe: no other pathname can alias that inode.
    if !metadata.is_file() || metadata.nlink() > 1 {
        return Err(path_error(
            "snapshot input must be regular and not multiply linked",
        ));
    }
    Ok(file)
}

pub(super) fn reown(file: &File, uid: u32) -> Result<(), Status> {
    fchown(file, Some(Uid::from_raw(uid)), Some(Gid::from_raw(uid))).map_err(path_error)
}

pub(super) fn open_rootfs_device(path: &Path) -> Result<File, Status> {
    // /dev/mapper aliases are intentionally symlinks maintained by root/udev.
    // Pin the resolved device; do not re-resolve the alias at mount time.
    #[cfg(target_os = "linux")]
    let flags = OFlag::O_PATH | OFlag::O_CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    let flags = OFlag::O_RDONLY | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC;
    let file = File::from(open(path, flags, Mode::empty()).map_err(path_error)?);
    if !file
        .metadata()
        .map_err(path_error)?
        .file_type()
        .is_block_device()
    {
        return Err(Status::failed_precondition(
            "rootfs source is not a block device",
        ));
    }
    Ok(file)
}

pub(super) fn bind_pinned(source: &File, target: &Path) -> Result<(), Status> {
    super::snapshots::bind_mount_file(
        &PathBuf::from(format!("/proc/self/fd/{}", source.as_raw_fd())),
        target,
    )
}

pub(super) fn bind_pinned_files(source: &File, target: &File) -> Result<(), Status> {
    bind_pinned(
        source,
        &PathBuf::from(format!("/proc/self/fd/{}", target.as_raw_fd())),
    )
}

pub(super) fn export_permissions(path: &Path, gid: Option<u32>) -> Result<(), Status> {
    let directory = open_directory(path)?;
    export_directory(&directory, gid, 0, &mut 100_000)
}

fn export_directory(
    directory: &File,
    gid: Option<u32>,
    depth: usize,
    remaining: &mut usize,
) -> Result<(), Status> {
    if depth > 64 {
        return Err(Status::resource_exhausted(
            "snapshot export tree is too deep",
        ));
    }
    let mut entries =
        nix::dir::Dir::openat(directory, ".", DIRECTORY, Mode::empty()).map_err(path_error)?;
    for entry in entries.iter() {
        let entry = entry.map_err(path_error)?;
        let name = entry.file_name();
        if matches!(name.to_bytes(), b"." | b"..") {
            continue;
        }
        *remaining = remaining
            .checked_sub(1)
            .ok_or_else(|| Status::resource_exhausted("snapshot export tree is too large"))?;
        let file = File::from(
            openat(
                directory,
                name,
                OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC,
                Mode::empty(),
            )
            .map_err(path_error)?,
        );
        let metadata = file.metadata().map_err(path_error)?;
        if metadata.is_dir() {
            export_directory(&file, gid, depth + 1, remaining)?;
        } else if metadata.is_file() && metadata.nlink() == 1 {
            set_permissions(&file, gid, 0o660)?;
        } else {
            return Err(Status::failed_precondition(
                "snapshot export contains a special file or hard link",
            ));
        }
    }
    set_permissions(directory, gid, 0o2770)
}

pub(super) fn set_permissions(file: &File, gid: Option<u32>, mode: u32) -> Result<(), Status> {
    if let Some(gid) = gid {
        fchown(file, None, Some(Gid::from_raw(gid))).map_err(path_error)?;
    }
    fchmod(file, Mode::from_bits_truncate(mode as _)).map_err(path_error)
}

#[cfg(test)]
mod tests;
