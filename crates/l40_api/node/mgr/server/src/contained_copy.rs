//! Symlink-rejecting, descriptor-relative, atomic file staging beneath a
//! sandbox root.
//!
//! The BYOK delegation RPCs let the unprivileged sandbox-manager hand
//! node-manager (root) paths to copy — restore sources staged by the snapshot
//! pull, the suspend publication staging dir. The RPC layer's lexical
//! containment check cannot stop a compromised caller from planting a symlink,
//! hard link, or FIFO *below* the sandbox dir (it can write the sandbox cache
//! dir and rename entries such as `instance/`), turning a root-privileged copy
//! into an arbitrary host read, an arbitrary host write, or an indefinite
//! blocking open.
//!
//! [`stage_file_contained`] closes those holes:
//!
//! - **Parents resolved descriptor-relative, symlinks rejected.** Both
//!   endpoints' parent directories are walked component-by-component from a
//!   descriptor on the sandbox root, every step opened with `O_NOFOLLOW` (the
//!   userspace equivalent of `openat2`'s `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS`,
//!   portable to the macOS dev build). A swapped `instance/` (or any) directory
//!   entry that became a symlink is refused.
//! - **Source pinned and checked.** The source is opened `O_NOFOLLOW`
//!   (rejects a symlinked final component) with `O_NONBLOCK` (a planted FIFO
//!   fails the regular-file check instead of blocking root's open forever),
//!   must be a regular file, and must not have multiple hard links (a hard
//!   link to a host file outside the sandbox is refused). A pinned, unlinked
//!   inode is safe: no remaining directory entry aliases it.
//! - **Destination written atomically, never opened by its caller-named
//!   path.** The copy lands in a freshly `O_CREAT | O_EXCL` temp file created
//!   in the resolved destination directory and is `renameat`-swapped into
//!   place. So an interrupted copy never leaves a partial file at the
//!   canonical path (a poisoned backing a later restore would open), a FIFO or
//!   symlink sitting at the destination name cannot wedge or redirect the
//!   write (that name is never opened for writing; rename replaces it), and a
//!   `src == dst` request degrades to a content-preserving rewrite instead of
//!   the truncate-before-read that would destroy the source.
//!
//! Data moves via `FICLONE` (instant, sparse-preserving reflink on btrfs) with
//! a hole-preserving `SEEK_DATA`/`SEEK_HOLE` byte copy as the non-CoW-filesystem
//! fallback — the backing files are sparse LUKS2 containers, so a hole-blind
//! copy would materialize gigabytes of zeros.

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;
use std::os::fd::OwnedFd;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use nix::fcntl::{OFlag, open, openat};
use nix::sys::stat::Mode;

/// Per-process counter making staging temp file names unique.
static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, thiserror::Error)]
pub enum ContainedCopyError {
    /// The path lexically escapes the root, contains a non-normal component,
    /// hits a symlink during resolution, or names a file that isn't a
    /// regular file without multiple links. These are containment violations from the
    /// (untrusted) caller, not I/O luck.
    #[error("{path} escapes containment under {root}: {detail}")]
    Escape {
        root: PathBuf,
        path: PathBuf,
        detail: String,
    },
    #[error("{op} {path}: {source}")]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

impl ContainedCopyError {
    fn escape(root: &Path, path: &Path, detail: impl Into<String>) -> Self {
        Self::Escape {
            root: root.to_path_buf(),
            path: path.to_path_buf(),
            detail: detail.into(),
        }
    }

    fn io(op: &'static str, path: &Path, source: io::Error) -> Self {
        Self::Io {
            op,
            path: path.to_path_buf(),
            source,
        }
    }
}

/// Copy the regular file at `src` to `dst`, both resolved strictly beneath
/// `root` with symlinks rejected at every component and hard-linked sources
/// refused. The write is atomic: it lands in a temp file in `dst`'s directory
/// and is renamed into place, so a partial copy never appears at `dst` and a
/// special file planted at the `dst` name cannot redirect or wedge it. A fresh
/// destination inherits the source's permission bits (masked to `0o666`,
/// further reduced by the umask), matching what the previous `cp` produced.
/// Return the copied inode's descriptor for caller-owned publication permissions.
/// Blocking — call from `spawn_blocking`.
pub fn stage_file_contained(
    root: &Path,
    src: &Path,
    dst: &Path,
) -> Result<File, ContainedCopyError> {
    // Source: pin the parent, then open the final component O_NOFOLLOW so a
    // symlinked source file is refused. O_NONBLOCK so a planted FIFO fails the
    // regular-file check below instead of blocking the open forever.
    let (src_dir, src_name) = resolve_parent(root, src)?;
    let src_fd = openat(
        &src_dir,
        src_name.as_os_str(),
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(|errno| walk_error(root, src, src, &src_dir, &src_name, errno))?;
    let src_file = File::from(src_fd);
    let src_meta = src_file
        .metadata()
        .map_err(|e| ContainedCopyError::io("stat", src, e))?;
    if !src_meta.is_file() {
        return Err(ContainedCopyError::escape(
            root,
            src,
            "source is not a regular file",
        ));
    }
    // A hard link the caller planted inside the sandbox can point at an inode
    // whose other name lives outside — the symlink walk can't see that, so
    // refuse any multiply-linked source. Sealed LUKS artifacts are single-link.
    if src_meta.nlink() > 1 {
        return Err(ContainedCopyError::escape(
            root,
            src,
            format!("source has {} hard links; refusing", src_meta.nlink()),
        ));
    }

    // Destination: pin the parent dir; the final name is never opened for
    // writing. Write into an O_EXCL temp and rename it over the name.
    let (dst_dir, dst_name) = resolve_parent(root, dst)?;
    stage_open_file(&src_file, &File::from(dst_dir), &dst_name)
        .map_err(|error| ContainedCopyError::io("stage", dst, error))
}

/// Stage from a pinned source into a pinned destination directory. The final
/// name is replaced, never opened for writing; retries therefore cannot
/// truncate an existing output or follow a destination link. Return the copied
/// inode's descriptor so subsequent ownership changes cannot race a lookup.
pub(crate) fn stage_open_file(
    src_file: &File,
    dst_dir: &File,
    dst_name: &OsStr,
) -> io::Result<File> {
    let mut components = Path::new(dst_name).components();
    if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected one destination file name",
        ));
    }
    let src_meta = src_file.metadata()?;
    // The pinned source may have been unlinked (e.g. restore memory held by a
    // jail bind). Reject aliases, not an inode with no remaining names.
    if !src_meta.is_file() || src_meta.nlink() > 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "source must be regular and not multiply linked",
        ));
    }
    let create_mode = Mode::from_bits_truncate((src_meta.mode() & 0o666) as libc::mode_t);
    let tmp_name = temp_name();
    let tmp_fd = openat(
        dst_dir,
        tmp_name.as_os_str(),
        OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        create_mode,
    )
    .map_err(io::Error::from)?;
    let tmp_file = File::from(tmp_fd);

    // Copy → fsync → atomic rename; on any failure remove the temp so a
    // partial never survives.
    let staged = (|| -> io::Result<()> {
        copy_on_descriptors(src_file, &tmp_file, src_meta.len())?;
        tmp_file.sync_all()?;
        renameat(dst_dir, &tmp_name, dst_dir, dst_name)
    })();
    if let Err(error) = staged {
        if let Err(cleanup_error) = unlinkat(dst_dir, &tmp_name) {
            tracing::warn!(%cleanup_error, temp = ?tmp_name, "failed to remove incomplete staging file");
        }
        return Err(error);
    }
    Ok(tmp_file)
}

/// A staging temp name in the destination directory. Hidden and namespaced by
/// pid + a monotonic counter so concurrent stages don't collide; `O_EXCL`
/// creation is the ultimate guard.
fn temp_name() -> OsString {
    let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
    OsString::from(format!(".stage-tmp.{}.{seq}", std::process::id()))
}

/// Resolve `path` (absolute, beneath `root`) to `(parent_dir_fd, final_name)`,
/// walking every parent component from a descriptor on `root` with
/// `O_NOFOLLOW`. Symlinks anywhere in the parent chain — including
/// node-derived segments like `instance/`, whose directory *entry* the caller
/// can rename — are rejected. Symlinks *above* `root` (e.g. the `/space` bind
/// on dev hosts) are fine: `root` itself is service-derived, not caller input.
/// The final component is returned unopened, for the caller to open
/// `O_NOFOLLOW` (source) or write via a temp + rename (destination).
fn resolve_parent(root: &Path, path: &Path) -> Result<(OwnedFd, OsString), ContainedCopyError> {
    let rel = path
        .strip_prefix(root)
        .map_err(|_| ContainedCopyError::escape(root, path, "not beneath the root"))?;
    let mut names: Vec<&OsStr> = Vec::new();
    for component in rel.components() {
        match component {
            Component::Normal(name) => names.push(name),
            other => {
                return Err(ContainedCopyError::escape(
                    root,
                    path,
                    format!("non-normal path component {other:?}"),
                ));
            }
        }
    }
    let Some((last, dirs)) = names.split_last() else {
        return Err(ContainedCopyError::escape(
            root,
            path,
            "path must name a file beneath the root, not the root itself",
        ));
    };

    let dir_flags = OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
    let mut dir = open(root, dir_flags, Mode::empty())
        .map_err(|errno| ContainedCopyError::io("open", root, errno.into()))?;
    let mut walked = root.to_path_buf();
    for name in dirs {
        walked.push(name);
        dir = openat(&dir, *name, dir_flags, Mode::empty())
            .map_err(|errno| walk_error(root, path, &walked, &dir, name, errno))?;
    }
    Ok((dir, last.to_os_string()))
}

/// Classify a failed `O_NOFOLLOW` open. A symlink surfaces as `ELOOP` (Linux)
/// or, for `O_DIRECTORY` opens on macOS, as `ENOTDIR` — so besides the errno,
/// ask `AT_SYMLINK_NOFOLLOW` stat whether the failing component is a symlink.
/// The classification only labels the error (escape vs I/O); the open itself
/// already refused to follow the link either way.
fn walk_error(
    root: &Path,
    path: &Path,
    at: &Path,
    dir: &OwnedFd,
    name: &OsStr,
    errno: nix::errno::Errno,
) -> ContainedCopyError {
    use nix::errno::Errno;
    use nix::sys::stat::SFlag;
    let is_symlink = matches!(errno, Errno::ELOOP | Errno::EMLINK)
        || nix::sys::stat::fstatat(dir, name, nix::fcntl::AtFlags::AT_SYMLINK_NOFOLLOW).is_ok_and(
            |st| SFlag::from_bits_truncate(st.st_mode) & SFlag::S_IFMT == SFlag::S_IFLNK,
        );
    if is_symlink {
        ContainedCopyError::escape(
            root,
            path,
            format!("symlink component rejected at {}", at.display()),
        )
    } else {
        ContainedCopyError::io("open", at, errno.into())
    }
}

/// Rename between names relative to pinned directory descriptors.
fn renameat(old_dir: &File, old_name: &OsStr, new_dir: &File, new_name: &OsStr) -> io::Result<()> {
    nix::fcntl::renameat(old_dir, old_name, new_dir, new_name).map_err(Into::into)
}

fn unlinkat(dir: &File, name: &OsStr) -> io::Result<()> {
    nix::unistd::unlinkat(dir, name, nix::unistd::UnlinkatFlags::NoRemoveDir).map_err(Into::into)
}

/// Copy `len` bytes between two pinned descriptors: whole-file reflink where
/// the filesystem supports it, hole-preserving byte copy otherwise. The
/// destination is a freshly created empty temp file.
pub(crate) fn copy_on_descriptors(src: &File, dst: &File, len: u64) -> io::Result<()> {
    pplx_space_common_io::reflink::copy_on_descriptors(src, dst, len).map(|_| ())
}

#[cfg(test)]
mod tests;
