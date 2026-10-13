//! Permission mutations run in the owning blocking task, never in descendants
//! of a killable `find` process. Directory descriptors pin every traversal.
use std::fs::File;
use std::io;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
#[cfg(target_os = "linux")]
use std::os::unix::fs::PermissionsExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::Path;

#[cfg(target_os = "linux")]
use nix::fcntl::AtFlags;
use nix::fcntl::{OFlag, openat};
use nix::sys::stat::Mode;
use nix::unistd::{Gid, Uid};
#[cfg(not(target_os = "linux"))]
use nix::{sys::stat::fchmod, unistd::fchown};

const DIRECTORY: OFlag = OFlag::O_RDONLY
    .union(OFlag::O_DIRECTORY)
    .union(OFlag::O_NOFOLLOW)
    .union(OFlag::O_CLOEXEC);

pub(super) fn rewrite(
    root: &Path,
    prune_snapshots: bool,
    old_uid: Option<u32>,
    uid: u32,
    gid: u32,
) -> io::Result<()> {
    let parent = super::trusted_paths::open_directory(
        root.parent()
            .ok_or_else(|| io::Error::other("ownership root has no parent"))?,
    )
    .map_err(io::Error::other)?;
    let name = root
        .file_name()
        .ok_or_else(|| io::Error::other("ownership root has no name"))?;
    let root = open_inode(&parent, name)?;
    let metadata = root.metadata()?;
    if crate::cache_ownership::is_private_send_source(name, &metadata) {
        return Ok(());
    }
    if metadata.is_dir() {
        Policy { old_uid, uid, gid }.directory(&root, prune_snapshots, false, 0)
    } else {
        permissions(&root, old_uid, uid, gid)
    }
}

// Pin metadata without opening a device, connecting to a socket, waiting for a
// FIFO peer, or following a symlink. Classify the pinned inode, not its name.
fn open_inode(parent: &File, name: &(impl nix::NixPath + ?Sized)) -> io::Result<File> {
    #[cfg(target_os = "linux")]
    let flags = OFlag::O_PATH | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    let flags = OFlag::from_bits_retain(libc::O_EVTONLY | libc::O_SYMLINK)
        | OFlag::O_NONBLOCK
        | OFlag::O_CLOEXEC;
    Ok(File::from(openat(parent, name, flags, Mode::empty())?))
}

struct Policy {
    old_uid: Option<u32>,
    uid: u32,
    gid: u32,
}

impl Policy {
    fn directory(
        &self,
        root: &File,
        prune_snapshots: bool,
        skip_children: bool,
        depth: usize,
    ) -> io::Result<()> {
        if depth > 64 {
            return Err(io::Error::other("cache ownership tree exceeds depth limit"));
        }
        if !skip_children {
            let mut entries = nix::dir::Dir::openat(root, ".", DIRECTORY, Mode::empty())?;
            for entry in entries.iter() {
                let entry = entry?;
                let name = entry.file_name();
                if matches!(name.to_bytes(), b"." | b"..") {
                    continue;
                }
                let child = open_inode(root, name)?;
                let metadata = child.metadata()?;
                if crate::cache_ownership::is_private_send_source(
                    std::ffi::OsStr::from_bytes(name.to_bytes()),
                    &metadata,
                ) {
                    continue;
                }
                if metadata.is_dir() {
                    self.directory(
                        &child,
                        false,
                        prune_snapshots && name.to_bytes() == b"snapshots",
                        depth + 1,
                    )?;
                } else {
                    permissions(&child, self.old_uid, self.uid, self.gid)?;
                }
            }
        }
        permissions(root, self.old_uid, self.uid, self.gid)
    }
}

fn permissions(file: &File, old_uid: Option<u32>, uid: u32, gid: u32) -> io::Result<()> {
    let metadata = file.metadata()?;
    let kind = metadata.file_type();
    if !kind.is_dir()
        && (metadata.nlink() != 1
            || !(kind.is_file() || kind.is_symlink() || kind.is_fifo() || kind.is_socket()))
    {
        return Err(io::Error::other(
            "unsupported or multiply linked cache inode",
        ));
    }
    // A previous attempt can have committed ownership before chmod failed.
    // Include the neutral owner pair so a UID-scoped retry finishes that inode.
    if old_uid.is_some_and(|old| metadata.uid() != old && metadata.gid() != old)
        && !(metadata.uid() == uid && metadata.gid() == gid)
    {
        return Ok(());
    }
    let executable = metadata.is_dir() || metadata.mode() & 0o111 != 0;
    // Match ug+rwX,o-rwx, with setgid directories for UID retirement. Strip
    // setuid/setgid from regular files when privileged ownership is assigned.
    let mode = if metadata.is_dir() {
        0o2770
    } else {
        (metadata.mode() & 0o770) | 0o660 | if executable { 0o110 } else { 0 }
    };
    #[cfg(target_os = "linux")]
    nix::unistd::fchownat(
        file,
        "",
        Some(Uid::from_raw(uid)),
        Some(Gid::from_raw(gid)),
        AtFlags::AT_EMPTY_PATH | AtFlags::AT_SYMLINK_NOFOLLOW,
    )?;
    #[cfg(not(target_os = "linux"))]
    fchown(file, Some(Uid::from_raw(uid)), Some(Gid::from_raw(gid)))?;
    if kind.is_symlink() {
        return Ok(());
    }
    // chown can clear setgid, including on directories.
    // O_PATH cannot be passed to fchmod on older Linux kernels. procfs resolves
    // this live descriptor to the pinned non-symlink inode, without opening it
    // for I/O or looking up the mutable cache name again.
    #[cfg(target_os = "linux")]
    std::fs::set_permissions(
        format!("/proc/self/fd/{}", file.as_raw_fd()),
        std::fs::Permissions::from_mode(mode),
    )?;
    #[cfg(not(target_os = "linux"))]
    fchmod(file, Mode::from_bits_truncate(mode as libc::mode_t))?;
    Ok(())
}

#[cfg(test)]
mod tests;
