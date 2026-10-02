//! The node's store of runtime releases and guest kernels, and the only way
//! anything is published into it or removed from it.
//!
//! The store is root-owned: `<releases>/v<version>/bin/…` and
//! `<kernels>/<version>/vmlinux.bin`, plus the `default` aliases that
//! select what boots. The runtime manager, which downloads and unpacks
//! releases and kernels unprivileged, stages them in its own directory; the
//! store copies the staged bytes into a root-owned temporary directory,
//! hashing them as it goes, and publishes them only if every byte matches the
//! catalog's pins. It never renames a staged file into place: the staging
//! account owns those inodes and could still hold them open for writing.
//!
//! Staged files are opened one path component at a time without following
//! links, so a staged tree cannot point the store at other files. A
//! publication lands with one rename that never replaces anything; a
//! removal renames the directory away before deleting it. Both are
//! idempotent, so a caller that lost the answer can ask again. Every change
//! to a root holds an exclusive lock on it, across processes, so a
//! publication, a removal, and leftover cleanup never interleave. Each
//! publication and removal works in a directory of its own, named
//! `.incoming-*` or `.removing-*`; listings skip dot entries, and
//! [`ArtifactStore::clear_leftovers`] removes what an interrupted one left.
//!
//! The runtime manager stages each artifact kind in its own namespace under
//! the staging root, `<runtime>/releases/<staged>/` and
//! `<runtime>/kernels/<staged>/` (see [`release_staging`] and
//! [`kernel_staging`]), so other artifact kinds can share the root later.
//!
//! Removal refuses what existing artifacts depend on: the releases and
//! kernels pinned as the initial ones, whatever a root's `default` (or a
//! kernel root's `legacy-default`) alias selects, and a root's last release.

use std::io::{Read as _, Write as _};
use std::os::fd::OwnedFd;
use std::os::unix::fs::{
    DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _,
};
use std::path::{Path, PathBuf};

use pplx_space_common_core::types::{
    ArtifactVersion, Digest, INITIAL_FIRECRACKER_KERNEL_VERSION,
    INITIAL_FIRECRACKER_RUNTIME_VERSION, RuntimeIdentity, RuntimeKind, RuntimeVersion,
};
use rustix::fs::{Mode, OFlags};
use sha2::{Digest as _, Sha256};

use crate::catalog::{PinnedKernel, PinnedRelease};
use crate::runtime_inventory::diagnostic::{DEFAULT_MARKER, LEGACY_DEFAULT_MARKER, ReleaseLayout};

/// A kernel's image within its directory.
pub const KERNEL_IMAGE: &str = "vmlinux.bin";

const INCOMING_PREFIX: &str = ".incoming-";
const REMOVING_PREFIX: &str = ".removing-";
const COPY_BUFFER_BYTES: usize = 64 * 1024;

/// The staging namespace of `runtime`'s releases: `firecracker/releases`.
pub fn release_staging(runtime: RuntimeKind) -> String {
    format!("{runtime}/releases")
}

/// The staging namespace of the guest kernels for `runtime`:
/// `firecracker/kernels`.
pub fn kernel_staging(runtime: RuntimeKind) -> String {
    format!("{runtime}/kernels")
}

/// A name no other publication or removal in this or another process uses.
fn unique_suffix() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    let sequence = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("{}-{nanos}-{sequence}", std::process::id())
}

/// Hold an exclusive lock on `root` until the returned descriptor is
/// dropped. The lock is the directory's own `flock`, so it also excludes
/// another node-manager process, as during a handoff. Blocking.
fn lock_root(root: &Path) -> Result<OwnedFd, std::io::Error> {
    let directory = rustix::fs::open(
        root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    rustix::fs::flock(&directory, rustix::fs::FlockOperation::LockExclusive)?;
    Ok(directory)
}

/// The directory a release occupies in its root: `v<version>`.
pub fn release_directory(release: &RuntimeIdentity) -> String {
    format!("v{}", release.version)
}

/// Whether `release` is one existing templates and snapshots were made with,
/// which stays on a node whoever installed it.
pub fn is_protected_release(release: &RuntimeIdentity) -> bool {
    release.kind == RuntimeKind::Firecracker
        && release.version.as_str() == INITIAL_FIRECRACKER_RUNTIME_VERSION
}

/// Whether kernel `version` is the one existing templates and snapshots
/// boot, which stays on a node whoever installed it.
pub fn is_protected_kernel(version: &ArtifactVersion) -> bool {
    version.as_str() == INITIAL_FIRECRACKER_KERNEL_VERSION
}

/// The name of a staging directory the runtime manager prepared: one path
/// component of letters, digits, and hyphens, such as a job id.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StagedName(String);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("invalid staging directory name {0:?}; expected letters, digits, and hyphens")]
pub struct StagedNameParseError(String);

impl StagedName {
    pub fn parse(value: impl Into<String>) -> Result<Self, StagedNameParseError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-');
        if valid {
            Ok(Self(value))
        } else {
            Err(StagedNameParseError(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for StagedName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// What a publication found.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublishOutcome {
    /// The staged bytes were published.
    Published,
    /// The pinned bytes were already in the store; nothing was written.
    AlreadyPresent,
}

/// A release or kernel in the store, after a publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Publication {
    pub outcome: PublishOutcome,
    /// The release's or kernel's directory.
    pub path: PathBuf,
    /// The release's digest over its binaries, or the kernel image's
    /// SHA-256.
    pub digest: Digest,
}

/// What a removal found.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveOutcome {
    Removed,
    /// Nothing was there.
    Absent,
}

/// Why a publication failed. Nothing is left in the store either way.
#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("{} is {problem}; the store root must be root-owned and writable only by root", path.display())]
    UnsafeRoot {
        path: PathBuf,
        problem: &'static str,
    },
    #[error("open staged {}", path.display())]
    Staged {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("staged {} is not a regular file", path.display())]
    NotRegularFile { path: PathBuf },
    /// The staged path is a link or passes through one; links are never
    /// followed, so the same request is refused again.
    #[error("staged {} is or passes through a link", path.display())]
    Linked { path: PathBuf },
    #[error("staged {} is over the {limit}-byte limit", path.display())]
    TooLarge { path: PathBuf, limit: u64 },
    #[error("staged {} has SHA-256 {found}, not the pinned {pinned}", path.display())]
    Mismatch {
        path: PathBuf,
        found: Digest,
        pinned: Digest,
    },
    #[error("staged {} is {found} bytes, not the pinned {pinned}", path.display())]
    WrongSize {
        path: PathBuf,
        found: u64,
        pinned: u64,
    },
    #[error("{} is already in the store with other bytes; left in place", path.display())]
    TargetDiffers { path: PathBuf },
    /// Publishing beside a root without a working selection could change
    /// what new sandboxes boot: the execution inventory falls back to the
    /// newest release when a root has no `default`.
    #[error(
        "{} does not name a release in its root; set or repair it before publishing another",
        alias.display()
    )]
    NoSelection { alias: PathBuf },
    #[error("{action} {}", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl PublishError {
    /// Whether the same request can succeed later without anyone changing
    /// the staged bytes, the pins, or the store: only an I/O failure can.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Io { .. } | Self::Staged { .. })
    }
}

/// Why a removal was refused or failed.
#[derive(Debug, thiserror::Error)]
pub enum RemoveError {
    #[error(
        "{} is the one existing templates and snapshots were made with; it stays",
        subject
    )]
    Protected { subject: String },
    #[error("{} selects {subject}; switch the alias away before removing it", alias.display())]
    Selected { subject: String, alias: PathBuf },
    #[error("{subject} is the last release in {}", root.display())]
    Last { subject: String, root: PathBuf },
    #[error("{} is {problem}; the store root must be root-owned and writable only by root", path.display())]
    UnsafeRoot {
        path: PathBuf,
        problem: &'static str,
    },
    #[error("{action} {}", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl RemoveError {
    /// Whether the same request can succeed later without anyone changing
    /// the store: only an I/O failure can.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Io { .. })
    }
}

/// The node's release and kernel roots, and where publications come from.
#[derive(Clone, Debug)]
pub struct ArtifactStore {
    releases_root: PathBuf,
    kernels_root: PathBuf,
    staging_root: PathBuf,
    /// The account the store and everything in it must belong to: root.
    owner: u32,
    /// The largest staged file a publication copies.
    max_file_bytes: u64,
}

impl ArtifactStore {
    /// A store owned by root.
    pub fn new(
        releases_root: impl Into<PathBuf>,
        kernels_root: impl Into<PathBuf>,
        staging_root: impl Into<PathBuf>,
        max_file_bytes: u64,
    ) -> Self {
        Self {
            releases_root: releases_root.into(),
            kernels_root: kernels_root.into(),
            staging_root: staging_root.into(),
            owner: 0,
            max_file_bytes,
        }
    }

    /// Trust another account as the store's owner, for tests.
    pub fn owned_by(mut self, owner: u32) -> Self {
        self.owner = owner;
        self
    }

    pub fn releases_root(&self) -> &Path {
        &self.releases_root
    }

    pub fn kernels_root(&self) -> &Path {
        &self.kernels_root
    }

    /// Publish the release staged as
    /// `<staging>/<runtime>/releases/<staged>/v<version>/` if its binaries
    /// match `pin`. Blocking.
    pub fn publish_release(
        &self,
        pin: &PinnedRelease,
        staged: &StagedName,
    ) -> Result<Publication, PublishError> {
        let directory = release_directory(&pin.release);
        check_root(&self.releases_root, self.owner)
            .map_err(|(path, problem)| PublishError::UnsafeRoot { path, problem })?;
        let _locked = lock_root(&self.releases_root)
            .map_err(|source| io_publish("lock", &self.releases_root, source))?;
        check_selection(pin.release.kind, &self.releases_root)?;
        let files: Vec<PlannedFile> = pin
            .binaries
            .iter()
            .map(|binary| PlannedFile {
                path: binary.path,
                sha256: binary.sha256,
                size: None,
                mode: 0o755,
            })
            .collect();
        self.publish(
            &self.releases_root,
            &directory,
            &release_staging(pin.release.kind),
            staged,
            &files,
            pin.digest(),
        )
    }

    /// Publish the kernel staged as
    /// `<staging>/<runtime>/kernels/<staged>/<version>/vmlinux.bin` if it
    /// matches `pin`. Blocking.
    pub fn publish_kernel(
        &self,
        pin: &PinnedKernel,
        staged: &StagedName,
    ) -> Result<Publication, PublishError> {
        let files = [PlannedFile {
            path: KERNEL_IMAGE,
            sha256: pin.sha256,
            size: Some(pin.size),
            mode: 0o644,
        }];
        check_root(&self.kernels_root, self.owner)
            .map_err(|(path, problem)| PublishError::UnsafeRoot { path, problem })?;
        let _locked = lock_root(&self.kernels_root)
            .map_err(|source| io_publish("lock", &self.kernels_root, source))?;
        self.publish(
            &self.kernels_root,
            pin.version.as_str(),
            &kernel_staging(pin.runtime),
            staged,
            &files,
            pin.sha256,
        )
    }

    /// Remove `release` from the store. Blocking.
    pub fn remove_release(&self, release: &RuntimeIdentity) -> Result<RemoveOutcome, RemoveError> {
        let subject = release.to_string();
        if is_protected_release(release) {
            return Err(RemoveError::Protected { subject });
        }
        let root = &self.releases_root;
        check_root(root, self.owner)
            .map_err(|(path, problem)| RemoveError::UnsafeRoot { path, problem })?;
        let _locked = lock_root(root).map_err(|source| io("lock", root, source))?;
        let target = root.join(release_directory(release));
        if !exists(&target).map_err(|source| io("inspect", &target, source))? {
            return Ok(RemoveOutcome::Absent);
        }
        refuse_if_selected(root, &[DEFAULT_MARKER], &target, &subject)?;
        if !has_another_release(release.kind, root, &target)? {
            return Err(RemoveError::Last {
                subject,
                root: root.clone(),
            });
        }
        remove_directory(root, &target)?;
        Ok(RemoveOutcome::Removed)
    }

    /// Remove kernel `version` from the store. Blocking.
    pub fn remove_kernel(&self, version: &ArtifactVersion) -> Result<RemoveOutcome, RemoveError> {
        let subject = format!("kernel {version}");
        if is_protected_kernel(version) {
            return Err(RemoveError::Protected { subject });
        }
        let root = &self.kernels_root;
        check_root(root, self.owner)
            .map_err(|(path, problem)| RemoveError::UnsafeRoot { path, problem })?;
        let _locked = lock_root(root).map_err(|source| io("lock", root, source))?;
        let target = root.join(version.as_str());
        if !exists(&target).map_err(|source| io("inspect", &target, source))? {
            return Ok(RemoveOutcome::Absent);
        }
        refuse_if_selected(
            root,
            &[DEFAULT_MARKER, LEGACY_DEFAULT_MARKER],
            &target,
            &subject,
        )?;
        remove_directory(root, &target)?;
        Ok(RemoveOutcome::Removed)
    }

    /// Make the staging root a directory only `uid` can use, owned by
    /// `uid:gid`, mode 0700. Its parent is shared with other services that
    /// can create entries in it, so anything else at the path, a link, a
    /// file, or a directory of another account's, is removed, and the
    /// directory is created by this process before it is handed over: no
    /// other account can have put anything in it. A staging root that is
    /// already right is left as it is, contents and all, since a runtime
    /// manager may be staging in it. Nothing is created where the parent does
    /// not exist. Returns whether it was created. Blocking.
    pub fn ensure_staging_root(&self, uid: u32, gid: u32) -> Result<bool, std::io::Error> {
        let root = &self.staging_root;
        if !root.parent().is_some_and(Path::is_dir) {
            return Ok(false);
        }
        match std::fs::symlink_metadata(root) {
            Ok(metadata)
                if metadata.is_dir() && metadata.uid() == uid && metadata.mode() & 0o077 == 0 =>
            {
                return Ok(false);
            }
            Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(root)?,
            Ok(_) => std::fs::remove_file(root)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        // Fails if another account recreated the path meanwhile, rather
        // than trusting what it made.
        std::fs::DirBuilder::new().mode(0o700).create(root)?;
        std::os::unix::fs::lchown(root, Some(uid), Some(gid))?;
        Ok(true)
    }

    /// Remove what interrupted publications and removals left in both roots.
    /// Returns how many entries were removed. Blocking.
    pub fn clear_leftovers(&self) -> Result<usize, std::io::Error> {
        let mut removed = 0;
        for root in [&self.releases_root, &self.kernels_root] {
            let _locked = match lock_root(root) {
                Ok(locked) => locked,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            for entry in std::fs::read_dir(root)? {
                let entry = entry?;
                let leftover = entry.file_name().to_str().is_some_and(|name| {
                    name.starts_with(INCOMING_PREFIX) || name.starts_with(REMOVING_PREFIX)
                });
                if leftover && entry.file_type()?.is_dir() {
                    std::fs::remove_dir_all(entry.path())?;
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }

    /// Publish into `root`, whose lock the caller holds.
    fn publish(
        &self,
        root: &Path,
        directory: &str,
        namespace: &str,
        staged: &StagedName,
        files: &[PlannedFile],
        digest: Digest,
    ) -> Result<Publication, PublishError> {
        check_root(root, self.owner)
            .map_err(|(path, problem)| PublishError::UnsafeRoot { path, problem })?;
        let target = root.join(directory);
        if exists(&target).map_err(|source| io_publish("inspect", &target, source))? {
            return self.already_present(&target, files, digest);
        }

        // This publication's own directory: created here, so nothing else's
        // bytes are in it, and never one another publication made.
        let incoming = root.join(format!("{INCOMING_PREFIX}{directory}-{}", unique_suffix()));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&incoming)
            .map_err(|source| io_publish("create", &incoming, source))?;
        let copied = self.copy_staged(namespace, staged, directory, &incoming, files);
        if let Err(error) = copied {
            let _ = std::fs::remove_dir_all(&incoming);
            return Err(error);
        }
        std::fs::set_permissions(&incoming, std::fs::Permissions::from_mode(0o755))
            .map_err(|source| io_publish("chmod", &incoming, source))?;
        sync_dir(&incoming).map_err(|source| io_publish("sync", &incoming, source))?;
        match rename_noreplace(&incoming, &target) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                // Published meanwhile: what is there decides.
                let _ = std::fs::remove_dir_all(&incoming);
                return self.already_present(&target, files, digest);
            }
            Err(source) => {
                let _ = std::fs::remove_dir_all(&incoming);
                return Err(io_publish("publish", &target, source));
            }
        }
        sync_dir(root).map_err(|source| io_publish("sync", root, source))?;
        tracing::info!(path = %target.display(), %digest, "published into the node's store");
        Ok(Publication {
            outcome: PublishOutcome::Published,
            path: target,
            digest,
        })
    }

    /// Copy each planned file from staging into `incoming`, checking it
    /// against its pin, and make every written file and directory durable.
    fn copy_staged(
        &self,
        namespace: &str,
        staged: &StagedName,
        directory: &str,
        incoming: &Path,
        files: &[PlannedFile],
    ) -> Result<(), PublishError> {
        let mut directories = Vec::new();
        for file in files {
            let destination = incoming.join(file.path);
            if let Some(parent) = destination.parent()
                && parent != incoming
            {
                std::fs::DirBuilder::new()
                    .recursive(true)
                    .mode(0o755)
                    .create(parent)
                    .map_err(|source| io_publish("create", parent, source))?;
                directories.push(parent.to_path_buf());
            }
            let mut components: Vec<&str> = namespace.split('/').collect();
            components.extend([staged.as_str(), directory]);
            components.extend(file.path.split('/'));
            let shown = self.staging_root.join(components.join("/"));
            let mut source = self.open_staged(&components, &shown)?;
            let mut out = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&destination)
                .map_err(|source| io_publish("create", &destination, source))?;
            let (sha256, size) = self.copy_hashing(&mut source, &mut out, &shown, &destination)?;
            if sha256 != file.sha256 {
                return Err(PublishError::Mismatch {
                    path: shown,
                    found: sha256,
                    pinned: file.sha256,
                });
            }
            if let Some(pinned) = file.size
                && pinned != size
            {
                return Err(PublishError::WrongSize {
                    path: shown,
                    found: size,
                    pinned,
                });
            }
            out.set_permissions(std::fs::Permissions::from_mode(file.mode))
                .and_then(|()| out.sync_all())
                .map_err(|source| io_publish("sync", &destination, source))?;
        }
        directories.sort_by(|left, right| {
            right
                .components()
                .count()
                .cmp(&left.components().count())
                .then_with(|| left.cmp(right))
        });
        directories.dedup();
        for directory in &directories {
            sync_dir(directory).map_err(|source| io_publish("sync", directory, source))?;
        }
        Ok(())
    }

    /// Open the staged file `components` names under the staging root, one
    /// component at a time, following no links.
    fn open_staged(
        &self,
        components: &[&str],
        shown: &Path,
    ) -> Result<std::fs::File, PublishError> {
        let staged = |source: std::io::Error| PublishError::Staged {
            path: shown.to_path_buf(),
            source,
        };
        // A link where a directory or the file should be fails the
        // no-follow open with ELOOP, or ENOTDIR for a directory component.
        let opened = |errno: rustix::io::Errno| match errno {
            rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR => PublishError::Linked {
                path: shown.to_path_buf(),
            },
            errno => staged(errno.into()),
        };
        let directory_flags =
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let mut directory: OwnedFd =
            rustix::fs::open(&self.staging_root, directory_flags, Mode::empty())
                .map_err(|errno| staged(errno.into()))?;
        let Some((name, parents)) = components.split_last() else {
            return Err(staged(std::io::ErrorKind::InvalidInput.into()));
        };
        for parent in parents {
            directory = rustix::fs::openat(&directory, *parent, directory_flags, Mode::empty())
                .map_err(opened)?;
        }
        let file = rustix::fs::openat(
            &directory,
            *name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(opened)?;
        let file = std::fs::File::from(file);
        let metadata = file.metadata().map_err(staged)?;
        if !metadata.is_file() {
            return Err(PublishError::NotRegularFile {
                path: shown.to_path_buf(),
            });
        }
        if metadata.len() > self.max_file_bytes {
            return Err(PublishError::TooLarge {
                path: shown.to_path_buf(),
                limit: self.max_file_bytes,
            });
        }
        Ok(file)
    }

    /// Copy `source` into `out`, returning its SHA-256 and size. Reads at
    /// most one byte past the limit, so a file that grows is refused.
    fn copy_hashing(
        &self,
        source: &mut std::fs::File,
        out: &mut std::fs::File,
        shown: &Path,
        destination: &Path,
    ) -> Result<(Digest, u64), PublishError> {
        let mut hasher = Sha256::new();
        let mut buffer = vec![0; COPY_BUFFER_BYTES];
        let mut copied: u64 = 0;
        let mut limited = source.take(self.max_file_bytes.saturating_add(1));
        loop {
            let read = match limited.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => read,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(source) => {
                    return Err(PublishError::Staged {
                        path: shown.to_path_buf(),
                        source,
                    });
                }
            };
            let chunk = buffer.get(..read).unwrap_or_default();
            copied = copied.saturating_add(read as u64);
            if copied > self.max_file_bytes {
                return Err(PublishError::TooLarge {
                    path: shown.to_path_buf(),
                    limit: self.max_file_bytes,
                });
            }
            hasher.update(chunk);
            out.write_all(chunk)
                .map_err(|source| io_publish("write", destination, source))?;
        }
        Ok((Digest::sha256(hasher.finalize().into()), copied))
    }

    /// The publication `target` already is, if it holds the pinned bytes,
    /// owned and protected as the store requires.
    fn already_present(
        &self,
        target: &Path,
        files: &[PlannedFile],
        digest: Digest,
    ) -> Result<Publication, PublishError> {
        for file in files {
            let path = target.join(file.path);
            let differs = || PublishError::TargetDiffers {
                path: target.to_path_buf(),
            };
            let metadata = match std::fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(differs());
                }
                Err(source) => return Err(io_publish("inspect", &path, source)),
            };
            if !metadata.is_file()
                || metadata.uid() != self.owner
                || metadata.permissions().mode() & 0o022 != 0
            {
                return Err(differs());
            }
            let found = sha256_file(&path).map_err(|source| io_publish("read", &path, source))?;
            if found != file.sha256 {
                return Err(differs());
            }
        }
        Ok(Publication {
            outcome: PublishOutcome::AlreadyPresent,
            path: target.to_path_buf(),
            digest,
        })
    }
}

/// One file a publication copies, and what it must be.
struct PlannedFile {
    path: &'static str,
    sha256: Digest,
    size: Option<u64>,
    mode: u32,
}

/// Refuse a store root that another account could write or swap. Returns
/// the offending path and the problem.
fn check_root(root: &Path, owner: u32) -> Result<(), (PathBuf, &'static str)> {
    for path in [Some(root), root.parent()].into_iter().flatten() {
        let metadata = std::fs::symlink_metadata(path)
            .map_err(|_| (path.to_path_buf(), "missing or unreadable"))?;
        if metadata.file_type().is_symlink() {
            return Err((path.to_path_buf(), "a symlink"));
        }
        if !metadata.is_dir() {
            return Err((path.to_path_buf(), "not a directory"));
        }
        if metadata.uid() != owner {
            return Err((path.to_path_buf(), "owned by another account"));
        }
        if metadata.permissions().mode() & 0o022 != 0 {
            return Err((path.to_path_buf(), "writable by its group or others"));
        }
    }
    Ok(())
}

/// Refuse a release root whose `default` alias does not name one of its
/// `runtime` releases.
fn check_selection(runtime: RuntimeKind, root: &Path) -> Result<(), PublishError> {
    let alias = root.join(DEFAULT_MARKER);
    let no_selection = || PublishError::NoSelection {
        alias: alias.clone(),
    };
    let selected = match std::fs::canonicalize(&alias) {
        Ok(selected) => selected,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Err(no_selection()),
        Err(source) => return Err(io_publish("resolve", &alias, source)),
    };
    let root = std::fs::canonicalize(root).map_err(|source| io_publish("resolve", root, source))?;
    let named_as_release = selected
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix('v'))
        .is_some_and(|version| RuntimeVersion::parse(version).is_ok());
    let complete = ReleaseLayout::of(runtime).is_some_and(|layout| {
        layout.binaries.iter().all(|binary| {
            std::fs::metadata(selected.join(binary)).is_ok_and(|metadata| metadata.is_file())
        })
    });
    if selected.parent() != Some(root.as_path()) || !named_as_release || !complete {
        return Err(no_selection());
    }
    Ok(())
}

/// Refuse to remove `target` while one of `aliases` in `root` selects it.
/// An alias that cannot be resolved for any reason but its absence refuses
/// too: the answer guards what boots.
fn refuse_if_selected(
    root: &Path,
    aliases: &[&str],
    target: &Path,
    subject: &str,
) -> Result<(), RemoveError> {
    let Some(installed) = resolve_if_present(target)? else {
        return Ok(());
    };
    for alias in aliases {
        let alias = root.join(alias);
        if resolve_if_present(&alias)?.as_ref() == Some(&installed) {
            return Err(RemoveError::Selected {
                subject: subject.to_string(),
                alias,
            });
        }
    }
    Ok(())
}

fn resolve_if_present(path: &Path) -> Result<Option<PathBuf>, RemoveError> {
    match std::fs::canonicalize(path) {
        Ok(resolved) => Ok(Some(resolved)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // A dangling alias resolves to nothing, but it is there: one
            // that names a missing directory is broken, not absent.
            match std::fs::symlink_metadata(path) {
                Ok(_) => Err(io("resolve", path, error)),
                Err(_) => Ok(None),
            }
        }
        Err(error) => Err(io("resolve", path, error)),
    }
}

/// Whether `root` holds another release of `runtime` than `target`: a
/// `v<version>` entry holding every binary its release layout names.
fn has_another_release(
    runtime: RuntimeKind,
    root: &Path,
    target: &Path,
) -> Result<bool, RemoveError> {
    let Some(layout) = ReleaseLayout::of(runtime) else {
        return Ok(false);
    };
    for entry in std::fs::read_dir(root).map_err(|source| io("list", root, source))? {
        let candidate = entry.map_err(|source| io("list", root, source))?.path();
        let named_as_release = candidate
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix('v'))
            .is_some_and(|version| RuntimeVersion::parse(version).is_ok());
        if candidate == target || !named_as_release {
            continue;
        }
        let mut complete = true;
        for binary in layout.binaries {
            let binary = candidate.join(binary);
            match std::fs::metadata(&binary) {
                Ok(metadata) if metadata.is_file() => {}
                Ok(_) => complete = false,
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                    ) =>
                {
                    complete = false;
                }
                Err(source) => return Err(io("inspect", &binary, source)),
            }
        }
        if complete {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Rename `target` out of `root`, so it is gone at once, then delete it.
fn remove_directory(root: &Path, target: &Path) -> Result<(), RemoveError> {
    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let removing = root.join(format!("{REMOVING_PREFIX}{name}-{}", unique_suffix()));
    rename_noreplace(target, &removing).map_err(|source| io("rename", target, source))?;
    sync_dir(root).map_err(|source| io("sync", root, source))?;
    if let Err(error) = std::fs::remove_dir_all(&removing) {
        // Gone from the store already; the next start finishes deleting it.
        tracing::warn!(path = %removing.display(), %error, "removed from the store but not yet deleted");
    }
    tracing::info!(path = %target.display(), "removed from the node's store");
    Ok(())
}

fn exists(path: &Path) -> Result<bool, std::io::Error> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn sha256_file(path: &Path) -> Result<Digest, std::io::Error> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; COPY_BUFFER_BYTES];
    loop {
        match file.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => hasher.update(buffer.get(..read).unwrap_or_default()),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(Digest::sha256(hasher.finalize().into()))
}

fn sync_dir(dir: &Path) -> Result<(), std::io::Error> {
    std::fs::File::open(dir).and_then(|file| file.sync_all())
}

fn rename_noreplace(from: &Path, to: &Path) -> Result<(), std::io::Error> {
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        from,
        rustix::fs::CWD,
        to,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(Into::into)
}

fn io(action: &'static str, path: &Path, source: std::io::Error) -> RemoveError {
    RemoveError::Io {
        action,
        path: path.to_path_buf(),
        source,
    }
}

fn io_publish(action: &'static str, path: &Path, source: std::io::Error) -> PublishError {
    PublishError::Io {
        action,
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests;
