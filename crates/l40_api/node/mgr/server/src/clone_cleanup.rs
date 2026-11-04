//! Disposal of unpublished clones uses the same resource identity as creation.
//! Destruction requires both the original inode and a still-private root.
//! Sharing a root revokes that authority: a cache peer could graft another
//! sandbox's real directories beneath it without changing the root inode.
use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::{Duration, Instant};

use nix::fcntl::{OFlag, open, openat};
use nix::sys::stat::Mode;
use nix::unistd::{UnlinkatFlags, unlinkat};

use crate::proto::CloneDirectoryIdentity;

const DIRECTORY: OFlag = OFlag::O_RDONLY
    .union(OFlag::O_DIRECTORY)
    .union(OFlag::O_NOFOLLOW)
    .union(OFlag::O_CLOEXEC);

#[derive(Debug, thiserror::Error)]
pub(crate) enum CloneDisposalError {
    #[error("prepared clone root identity is missing or changed")]
    RootIdentity,
    #[error("prepared clone root is no longer private; operator repair required")]
    SharedRoot,
    #[error("prepared clone has unexpected runtime files")]
    UnexpectedRuntime,
    #[cfg(target_os = "linux")]
    #[error("refusing to delete the filesystem root")]
    FilesystemRoot,
    #[error("prepared clone disposal traversal budget exhausted")]
    BudgetExhausted,
    #[error("prepared clone disposal filesystem operation failed: {0}")]
    Io(#[from] io::Error),
}

impl From<nix::errno::Errno> for CloneDisposalError {
    fn from(error: nix::errno::Errno) -> Self {
        Self::Io(error.into())
    }
}

pub(crate) fn dispose(
    cache: &Path,
    run: &Path,
    id: &str,
    expected: Option<&CloneDirectoryIdentity>,
) -> Result<(), CloneDisposalError> {
    dispose_with_hook(cache, run, id, expected, || {})
}

pub(crate) fn dispose_with_hook(
    cache: &Path,
    run: &Path,
    id: &str,
    expected: Option<&CloneDirectoryIdentity>,
    after_pin: impl FnOnce(),
) -> Result<(), CloneDisposalError> {
    require_empty_run_directory(run, id)?;
    let cache = File::from(open(cache, DIRECTORY, Mode::empty())?);
    let root = match openat(&cache, id, DIRECTORY, Mode::empty()) {
        Ok(fd) => File::from(fd),
        Err(nix::errno::Errno::ENOENT) => {
            // A retry after filesystem cleanup but before SQLite completion.
            return remove_run_directory(run, id);
        }
        Err(nix::errno::Errno::ELOOP | nix::errno::Errno::ENOTDIR) => {
            return Err(CloneDisposalError::RootIdentity);
        }
        Err(error) => return Err(error.into()),
    };
    let actual = root.metadata()?;
    if !expected
        .is_some_and(|expected| (expected.device, expected.inode) == (actual.dev(), actual.ino()))
    {
        return Err(CloneDisposalError::RootIdentity);
    }
    // Preparation creates mode 0700 owned by sandbox-manager; cloning never
    // changes its owner. Cache peers cannot chmod that inode. The node lifecycle
    // gate excludes cloning's permission release while disposal runs. Do not
    // chmod an exposed root back to private: foreign children may already exist.
    // Even read/traverse access exposes writable descendants after cloning.
    if actual.mode() & 0o777 != 0o700 {
        return Err(CloneDisposalError::SharedRoot);
    }
    after_pin();
    let mut budget = Budget {
        remaining: 100_000,
        deadline: Instant::now() + Duration::from_secs(60),
    };
    remove_contents(&root, &mut budget, 0)?;
    // Every content mutation used the pinned root. The final directory unlink
    // cannot traverse a replacement, and must not report success for one.
    let current = File::from(openat(&cache, id, DIRECTORY, Mode::empty())?).metadata()?;
    if (current.dev(), current.ino()) != (actual.dev(), actual.ino()) {
        return Err(CloneDisposalError::RootIdentity);
    }
    unlinkat(&cache, id, UnlinkatFlags::RemoveDir)?;
    remove_run_directory(run, id)
}

fn require_empty_run_directory(run: &Path, id: &str) -> Result<(), CloneDisposalError> {
    let run = File::from(open(run, DIRECTORY, Mode::empty())?);
    let mut entries = match nix::dir::Dir::openat(&run, id, DIRECTORY, Mode::empty()) {
        Ok(entries) => entries,
        Err(nix::errno::Errno::ENOENT) => return Ok(()),
        Err(nix::errno::Errno::ELOOP | nix::errno::Errno::ENOTDIR) => {
            return Err(CloneDisposalError::UnexpectedRuntime);
        }
        Err(error) => return Err(error.into()),
    };
    for entry in entries.iter() {
        if !matches!(entry?.file_name().to_bytes(), b"." | b"..") {
            return Err(CloneDisposalError::UnexpectedRuntime);
        }
    }
    Ok(())
}

fn remove_run_directory(run: &Path, id: &str) -> Result<(), CloneDisposalError> {
    let run = File::from(open(run, DIRECTORY, Mode::empty())?);
    // Cloning creates only an empty run directory, never a runtime. Do not
    // recursively remove an unexpected execution or follow a replacement.
    match unlinkat(&run, id, UnlinkatFlags::RemoveDir) {
        Ok(()) | Err(nix::errno::Errno::ENOENT) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

struct Budget {
    remaining: usize,
    deadline: Instant,
}

/// Bounded descriptor-relative disposal shared by published cache cleanup.
/// Unlinking a symlink is safe; no symlink is traversed, including parents.
pub(crate) fn remove_at(parent: &File, name: &OsStr) -> Result<bool, CloneDisposalError> {
    let directory = match openat(parent, name, DIRECTORY, Mode::empty()) {
        Ok(fd) => File::from(fd),
        Err(nix::errno::Errno::ENOENT) => return Ok(false),
        Err(nix::errno::Errno::ENOTDIR | nix::errno::Errno::ELOOP) => {
            unlinkat(parent, name, UnlinkatFlags::NoRemoveDir)?;
            return Ok(true);
        }
        Err(error) => return Err(error.into()),
    };
    let mut budget = Budget {
        remaining: 100_000,
        deadline: Instant::now() + Duration::from_secs(60),
    };
    if !delete_subvolume_tree(&directory, &mut budget, 0)? {
        let original = directory.metadata()?;
        remove_contents(&directory, &mut budget, 0)?;
        let current = File::from(openat(parent, name, DIRECTORY, Mode::empty())?).metadata()?;
        if (current.dev(), current.ino()) != (original.dev(), original.ino()) {
            return Err(CloneDisposalError::RootIdentity);
        }
        unlinkat(parent, name, UnlinkatFlags::RemoveDir)?;
    }
    Ok(true)
}

fn remove_contents(
    root: &File,
    budget: &mut Budget,
    depth: usize,
) -> Result<(), CloneDisposalError> {
    walk_contents(root, budget, depth, false)
}

fn delete_subvolume_tree(
    directory: &File,
    budget: &mut Budget,
    depth: usize,
) -> Result<bool, CloneDisposalError> {
    match delete_subvolume(directory) {
        Err(CloneDisposalError::Io(error)) if error.raw_os_error() == Some(libc::ENOTEMPTY) => {
            // A subvolume can contain other subvolumes, even when the outer
            // root is read-only. Remove child subvolumes by ID without writing
            // ordinary files in that read-only tree, then retry the root.
            walk_contents(directory, budget, depth, true)?;
            delete_subvolume(directory)
        }
        result => result,
    }
}

fn walk_contents(
    root: &File,
    budget: &mut Budget,
    depth: usize,
    subvolumes_only: bool,
) -> Result<(), CloneDisposalError> {
    if depth > 64 || Instant::now() >= budget.deadline {
        return Err(CloneDisposalError::BudgetExhausted);
    }
    // Open an independent directory stream: dup shares the directory offset.
    let mut entries = nix::dir::Dir::openat(root, ".", DIRECTORY, Mode::empty())?;
    for entry in entries.iter() {
        let entry = entry?;
        let name = entry.file_name();
        if name.to_bytes() == b"." || name.to_bytes() == b".." {
            continue;
        }
        budget.remaining = budget
            .remaining
            .checked_sub(1)
            .ok_or(CloneDisposalError::BudgetExhausted)?;
        if Instant::now() >= budget.deadline {
            return Err(CloneDisposalError::BudgetExhausted);
        }
        match openat(root, name, DIRECTORY, Mode::empty()) {
            Ok(fd) => {
                let child = File::from(fd);
                if !delete_subvolume_tree(&child, budget, depth + 1)? {
                    walk_contents(&child, budget, depth + 1, subvolumes_only)?;
                    if !subvolumes_only {
                        unlinkat(root, name, UnlinkatFlags::RemoveDir)?;
                    }
                }
            }
            Err(nix::errno::Errno::ENOTDIR | nix::errno::Errno::ELOOP) => {
                // unlinkat does not follow a symlink or mutate a hardlink's target.
                if !subvolumes_only {
                    unlinkat(root, name, UnlinkatFlags::NoRemoveDir)?;
                }
            }
            Err(nix::errno::Errno::ENOENT) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn delete_subvolume(_directory: &File) -> Result<bool, CloneDisposalError> {
    Ok(false)
}

#[cfg(target_os = "linux")]
fn delete_subvolume(directory: &File) -> Result<bool, CloneDisposalError> {
    use std::os::fd::AsRawFd;
    // Only btrfs subvolume roots have inode 256. Ordinary directories continue
    // through descriptor-relative unlink; no btrfs error falls back to rm -rf.
    if directory.metadata()?.ino() != 256 {
        return Ok(false);
    }
    #[repr(C)]
    struct Lookup {
        tree: u64,
        object: u64,
        name: [u8; 4080],
    }
    let mut lookup = Lookup {
        tree: 0,
        object: 256,
        name: [0; 4080],
    };
    // SAFETY: Linux btrfs UAPI INO_LOOKUP has this 4096-byte layout; the fd and
    // writable output buffer remain live for the entire synchronous ioctl.
    let result = unsafe { libc::ioctl(directory.as_raw_fd(), 0xd000_9412u32 as _, &mut lookup) };
    if result == -1 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ENOTTY) {
            return Ok(false);
        }
        return Err(error.into());
    }
    if lookup.tree <= 5 {
        return Err(CloneDisposalError::FilesystemRoot);
    }
    #[repr(C)]
    struct Destroy {
        fd: i64,
        transid: u64,
        flags: u64,
        unused: [u64; 4],
        subvolid: u64,
        padding: [u8; 4032],
    }
    let args = Destroy {
        fd: 0,
        transid: 0,
        flags: 1 << 4,
        unused: [0; 4],
        subvolid: lookup.tree,
        padding: [0; 4032],
    };
    // SNAP_DESTROY_V2 with SUBVOL_SPEC_BY_ID addresses the pinned subvolume,
    // including read-only snapshots, without re-resolving its mutable name.
    // SAFETY: the 4096-byte structure matches Linux UAPI and has no pointers.
    if unsafe { libc::ioctl(directory.as_raw_fd(), 0x5000_943f as _, &args) } == -1 {
        return Err(io::Error::last_os_error().into());
    }
    Ok(true)
}

#[cfg(test)]
mod tests;
