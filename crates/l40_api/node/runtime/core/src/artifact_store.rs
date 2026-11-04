//! The node's store of runtime releases and guest kernels, and the only way
//! anything is published into it or removed from it.
//!
//! The store is root-owned and digest-addressed. The runtime manager, which downloads and unpacks
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
//! The privileged service must guard removal of executable identities. This
//! filesystem layer enforces byte identity, not execution policy. External
//! execution roots and aliases remain outside its mutation authority.

use std::io::{Read as _, Write as _};
use std::os::fd::OwnedFd;
use std::os::unix::fs::{
    DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _,
};
use std::path::{Path, PathBuf};

use pplx_space_common_core::types::{ArtifactVersion, Digest, RuntimeIdentity, RuntimeKind};
use rustix::fs::{Mode, OFlags};
use sha2::{Digest as _, Sha256};

use crate::catalog::{ManagedDirectoryName, PinnedKernel, PinnedRelease};

mod boot_artifact;
mod boot_file;
pub use boot_artifact::{BOOT_ARTIFACT_IMAGE, boot_artifact_directory, boot_artifact_staging};
use boot_file::BootFilePlan;
mod protection;
pub use protection::verify_execution_directory;
use protection::{RootProtection, check_root};
mod execution_cache;
pub use execution_cache::{KernelSource, StageExecutionError};

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
    lock_root_with(root, rustix::fs::FlockOperation::LockExclusive)
}

// The caller owns the root lock and has checked directory protection.
fn clear_root_leftovers(root: &Path) -> std::io::Result<usize> {
    let mut removed = 0;
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
    Ok(removed)
}

fn lock_root_with(
    root: &Path,
    operation: rustix::fs::FlockOperation,
) -> Result<OwnedFd, std::io::Error> {
    let directory = rustix::fs::open(
        root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    rustix::fs::flock(&directory, operation)?;
    Ok(directory)
}

/// The directory a release occupies in its root: `v<version>`.
pub fn release_directory(release: &RuntimeIdentity) -> String {
    format!("v{}", release.version)
}

pub fn managed_release_directory(release: &RuntimeIdentity, digest: Digest) -> String {
    ManagedDirectoryName::release(release, digest).to_string()
}

/// Read compatibility for releases published before compact locators. Content
/// authentication still uses the complete digest, never either directory name.
pub fn release_directory_names(release: &RuntimeIdentity, digest: Digest) -> [String; 2] {
    [
        managed_release_directory(release, digest),
        format!("v{}--{}", release.version, digest.hex_string()),
    ]
}

pub fn kernel_directory_names(version: &ArtifactVersion, digest: Digest) -> [String; 2] {
    [
        managed_kernel_directory(version, digest),
        format!("{version}--{}", digest.hex_string()),
    ]
}

/// Prefer the compact copy, but never hide an unreadable or invalid preferred
/// entry by selecting an older copy. Authentication is the caller's next step.
pub fn existing_artifact_path(root: &Path, names: &[String]) -> std::io::Result<PathBuf> {
    let preferred = names.first().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "artifact locator is required",
        )
    })?;
    for name in names {
        let path = root.join(name);
        match std::fs::symlink_metadata(&path) {
            Ok(_) => return Ok(path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(root.join(preferred))
}

pub fn managed_kernel_directory(version: &ArtifactVersion, digest: Digest) -> String {
    ManagedDirectoryName::kernel(version, digest).to_string()
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
    /// The pinned bytes were published.
    Published,
    /// The pinned bytes were already in the store; nothing was written.
    AlreadyPresent,
}

/// A release or kernel at its publication destination.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Publication {
    pub outcome: PublishOutcome,
    /// The release's or kernel's directory.
    pub path: PathBuf,
    /// The release's digest over its binaries, or the kernel image's
    /// SHA-256.
    pub digest: Digest,
}

/// Verified bytes at an existing location; verification never publishes files.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedArtifact {
    pub path: PathBuf,
    pub digest: Digest,
}

#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    #[error("cannot verify image {} without a parent directory", path.display())]
    InvalidImagePath { path: PathBuf },
    #[error("kernel image {} must be named {}", path.display(), KERNEL_IMAGE)]
    KernelImageName { path: PathBuf },
    #[error("cannot verify artifact: {} is {problem}", path.display())]
    UnsafeRoot {
        path: PathBuf,
        problem: &'static str,
    },
    #[error("cannot verify artifact file {}: {problem}", path.display())]
    InvalidFile {
        path: PathBuf,
        problem: &'static str,
    },
    #[error("artifact file {} has SHA-256 {found}, not the pinned {pinned}", path.display())]
    Mismatch {
        path: PathBuf,
        found: Digest,
        pinned: Digest,
    },
    #[error("{action} artifact {} for verification", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl VerifyError {
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Io { source, .. } if pplx_space_common_core::host::path_errors::host_path_io_error_is_transient(source))
    }
}

/// Cache staging publishes a separate inode and may find an existing copy.
#[derive(Debug, thiserror::Error)]
#[error("stage kernel in execution cache: {0}")]
pub struct StageKernelError(#[from] PublishError);

impl StageKernelError {
    pub fn is_transient(&self) -> bool {
        self.0.is_transient()
    }
}

/// What a removal found.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveOutcome {
    Removed,
    /// Nothing was there.
    Absent,
}

/// Why a publication failed. Private work is removed on return. A failure
/// syncing the root after rename may leave a published artifact; retrying
/// verifies that copy and succeeds without replacing it.
#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("{version} is not a visible artifact directory name")]
    InvalidVersion { version: ArtifactVersion },
    #[error("{} is {problem}; artifact directories must be owned by the trusted account and protected from other writers", path.display())]
    UnsafeRoot {
        path: PathBuf,
        problem: &'static str,
    },
    #[error("access copy source {}", path.display())]
    Source {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("copy source {} is not a regular file", path.display())]
    NotRegularFile { path: PathBuf },
    /// The source path is a link or passes through one; links are never
    /// followed, so the same request is refused again.
    #[error("copy source {} is or passes through a link", path.display())]
    Linked { path: PathBuf },
    #[error("copy source {} is over the {limit}-byte limit", path.display())]
    TooLarge { path: PathBuf, limit: u64 },
    #[error("copy source {} has SHA-256 {found}, not the pinned {pinned}", path.display())]
    Mismatch {
        path: PathBuf,
        found: Digest,
        pinned: Digest,
    },
    #[error("copy source {} is {found} bytes, not the pinned {pinned}", path.display())]
    WrongSize {
        path: PathBuf,
        found: u64,
        pinned: u64,
    },
    #[error("existing publication cannot be verified; left in place: {0}")]
    ExistingArtifact(#[source] VerifyError),
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
        match self {
            Self::ExistingArtifact(error) => error.is_transient(),
            Self::Io { source, .. } | Self::Source { source, .. } => {
                pplx_space_common_core::host::path_errors::host_path_io_error_is_transient(source)
            }
            _ => false,
        }
    }
}

/// Why a removal was refused or failed.
#[derive(Debug, thiserror::Error)]
pub enum RemoveError {
    #[error("{version} is not a visible artifact directory name")]
    InvalidVersion { version: ArtifactVersion },
    #[error("{} does not contain the requested component bytes; left in place", path.display())]
    IdentityChanged { path: PathBuf },
    #[error("{} is {problem}; artifact directories must be owned by the trusted account and protected from other writers", path.display())]
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
        matches!(self, Self::Io { source, .. } if pplx_space_common_core::host::path_errors::host_path_io_error_is_transient(source))
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
    verity_support: std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<u64, bool>>>,
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
            verity_support: Default::default(),
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

    /// Verify a managed release before exposing its binaries to execution.
    /// No subprocess is needed to establish the approved release identity.
    pub fn verify_release(&self, pin: &PinnedRelease) -> Result<VerifiedArtifact, VerifyError> {
        RootProtection::Store.verify(&self.releases_root, self.owner)?;
        let _locked = lock_root(&self.releases_root)
            .map_err(|source| io_verify("lock", &self.releases_root, source))?;
        let target = existing_artifact_path(
            &self.releases_root,
            &release_directory_names(&pin.release, pin.digest()),
        )
        .map_err(|source| io_verify("inspect", &self.releases_root, source))?;
        self.verify_release_at(pin, &target)
    }

    /// Verify the selected managed copy without resolving its directory again.
    /// Both historical and compact locators retain the store ancestry checks.
    pub(crate) fn verify_managed_release_at(
        &self,
        pin: &PinnedRelease,
        target: &Path,
    ) -> Result<VerifiedArtifact, VerifyError> {
        RootProtection::Store.verify(&self.releases_root, self.owner)?;
        let _locked = lock_root(&self.releases_root)
            .map_err(|source| io_verify("lock", &self.releases_root, source))?;
        self.verify_release_at(pin, target)
    }

    /// Verify a baked release without adopting or changing its files.
    pub fn verify_release_at(
        &self,
        pin: &PinnedRelease,
        target: &Path,
    ) -> Result<VerifiedArtifact, VerifyError> {
        RootProtection::Store.verify(&target.join("bin"), self.owner)?;
        let files: Vec<_> = pin
            .binaries
            .iter()
            .map(|binary| PlannedFile {
                path: binary.path,
                sha256: binary.sha256,
                size: None,
                mode: 0o755,
            })
            .collect();
        self.verify_files(target, &files, pin.digest(), RootProtection::Store)
    }

    /// Validate a baked E0 kernel in place without adopting its inode.
    pub fn verify_kernel_at(
        &self,
        pin: &PinnedKernel,
        image: &Path,
    ) -> Result<VerifiedArtifact, VerifyError> {
        let directory = kernel_image_directory(image)?;
        self.verify_files(
            directory,
            &[PlannedFile {
                path: KERNEL_IMAGE,
                sha256: pin.sha256,
                size: Some(pin.size),
                mode: 0o644,
            }],
            pin.sha256,
            RootProtection::ExecutionCache,
        )
    }

    /// Reuse a historical cache copy instead of publishing a duplicate. The
    /// selected locator still requires full authentication before execution.
    pub fn execution_kernel_path(
        parent: &Path,
        pin: &PinnedKernel,
    ) -> Result<PathBuf, PublishError> {
        let root = Self::execution_cache_root(parent);
        existing_artifact_path(&root, &kernel_directory_names(&pin.version, pin.sha256))
            .map(|directory| directory.join(KERNEL_IMAGE))
            .map_err(|error| io_publish("locate execution kernel", &root, error))
    }

    fn execution_cache_root(parent: &Path) -> PathBuf {
        parent.join("managed/kernels")
    }

    /// Create an isolated execution-cache namespace below a protected or
    /// sticky root-owned parent. Never repair ownership of existing paths.
    pub fn prepare_execution_cache(&self, parent: &Path) -> Result<PathBuf, PublishError> {
        self.prepare_execution_namespace(parent, "kernels")
    }

    /// Publish a verified, independent kernel inode onto the execution cache
    /// filesystem. The caller supplies a protected cache root, never a path
    /// from an RPC. Existing copies are verified and never overwritten.
    pub fn stage_kernel(
        &self,
        pin: &PinnedKernel,
        cache: &Path,
    ) -> Result<Publication, StageKernelError> {
        self.stage_kernel_copy(pin, cache)
            .map_err(StageKernelError::from)
    }

    fn stage_kernel_copy(
        &self,
        pin: &PinnedKernel,
        cache: &Path,
    ) -> Result<Publication, PublishError> {
        self.stage_boot_file(&BootFilePlan::kernel(self, pin), cache)
    }

    /// Create only missing managed directories under a protected vendor parent.
    /// Existing paths are verified, never re-owned or linked to execution roots.
    /// Failure disables management, not execution. Blocking.
    pub fn prepare_roots(&self) -> Result<(), std::io::Error> {
        for root in [
            &self.releases_root,
            &self.kernels_root,
            &self.boot_artifacts_root(),
        ] {
            let parent = root.parent().ok_or(std::io::ErrorKind::InvalidInput)?;
            let vendor = parent.parent().ok_or(std::io::ErrorKind::InvalidInput)?;
            for directory in [vendor, parent, root.as_path()] {
                match std::fs::symlink_metadata(directory) {
                    Ok(metadata)
                        if metadata.is_dir()
                            && metadata.uid() == self.owner
                            && metadata.mode() & 0o022 == 0 => {}
                    Ok(_) => {
                        return Err(std::io::Error::other(format!(
                            "unsafe managed store directory {}",
                            directory.display()
                        )));
                    }
                    Err(error)
                        if error.kind() == std::io::ErrorKind::NotFound && directory != vendor =>
                    {
                        std::fs::DirBuilder::new().mode(0o755).create(directory)?;
                        sync_dir(directory.parent().ok_or(std::io::ErrorKind::InvalidInput)?)?;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
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
        RootProtection::Store.check(&self.releases_root, self.owner)?;
        let _locked = lock_root(&self.releases_root)
            .map_err(|source| io_publish("lock", &self.releases_root, source))?;
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
        let target = existing_artifact_path(
            &self.releases_root,
            &release_directory_names(&pin.release, pin.digest()),
        )
        .map_err(|source| io_publish("inspect", &self.releases_root, source))?;
        if exists(&target).map_err(|source| io_publish("inspect", &target, source))? {
            return self.already_present(&target, &files, pin.digest(), RootProtection::Store);
        }
        self.publish(
            &self.releases_root,
            &managed_release_directory(&pin.release, pin.digest()),
            PublishSource::Staging {
                namespace: &release_staging(pin.release.kind),
                staged,
                directory: &directory,
            },
            &files,
            pin.digest(),
            RootProtection::Store,
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
        self.publish_boot_file(&BootFilePlan::kernel(self, pin), staged)
    }

    /// Remove `release` from the store. Blocking.
    pub fn remove_release(
        &self,
        release: &RuntimeIdentity,
        digest: Digest,
    ) -> Result<RemoveOutcome, RemoveError> {
        let root = &self.releases_root;
        check_root(root, self.owner).map_err(RemoveError::from)?;
        let _locked = lock_root(root).map_err(|source| io("lock", root, source))?;
        let mut verified = Vec::new();
        for name in release_directory_names(release, digest) {
            let target = root.join(name);
            if !exists(&target).map_err(|source| io("inspect", &target, source))? {
                continue;
            }
            let layout = crate::runtime_inventory::diagnostic::ReleaseLayout::of(release.kind)
                .ok_or_else(|| RemoveError::IdentityChanged {
                    path: target.clone(),
                })?;
            let mut binaries = Vec::new();
            for binary in layout.binaries {
                let path = target.join(binary);
                let metadata = std::fs::symlink_metadata(&path)
                    .map_err(|source| removal_evidence_error("inspect", &path, source))?;
                if !metadata.is_file() || metadata.len() > self.max_file_bytes {
                    return Err(RemoveError::IdentityChanged { path });
                }
                binaries.push((
                    *binary,
                    sha256_file(&path)
                        .map_err(|source| removal_evidence_error("hash", &path, source))?,
                ));
            }
            if crate::catalog::release_digest(binaries) != digest {
                return Err(RemoveError::IdentityChanged { path: target });
            }
            verified.push(target);
        }
        // Validate both layouts before deleting either: a conflicting historical
        // copy must not turn a refused removal into a partial mutation.
        for target in &verified {
            remove_directory(root, target)?;
        }
        Ok(if verified.is_empty() {
            RemoveOutcome::Absent
        } else {
            RemoveOutcome::Removed
        })
    }

    /// Remove kernel `version` from the store. Blocking.
    pub fn remove_kernel(
        &self,
        version: &ArtifactVersion,
        digest: Digest,
    ) -> Result<RemoveOutcome, RemoveError> {
        self.remove_boot_file(
            &self.kernels_root,
            version,
            digest,
            KERNEL_IMAGE,
            &kernel_directory_names(version, digest),
        )
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
        let parent = root.parent().ok_or(std::io::ErrorKind::InvalidInput)?;
        let parent_metadata = std::fs::symlink_metadata(parent)?;
        if !parent_metadata.is_dir()
            || parent_metadata.uid() != self.owner
            || (parent_metadata.mode() & 0o022 != 0 && parent_metadata.mode() & 0o1000 == 0)
        {
            return Err(std::io::Error::other(
                "staging parent must be owner-controlled and sticky when shared",
            ));
        }
        match std::fs::symlink_metadata(root) {
            Ok(metadata)
                if metadata.is_dir()
                    && metadata.uid() == uid
                    && metadata.gid() == gid
                    && metadata.mode() & 0o077 == 0 =>
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
    /// Busy roots are skipped. Call off the readiness path: deletion can be slow.
    /// Returns how many entries were removed. Blocking.
    pub fn clear_leftovers(&self) -> Result<usize, std::io::Error> {
        let mut removed = 0;
        for root in [
            &self.releases_root,
            &self.kernels_root,
            &self.boot_artifacts_root(),
        ] {
            // Listing-only nodes may never provision managed roots.
            match std::fs::symlink_metadata(root) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
                Ok(_) => {}
            }
            if let Err(error) = check_root(root, self.owner) {
                tracing::warn!(%error, "skipping artifact cleanup in an unsafe or absent root");
                continue;
            }
            let _locked =
                match lock_root_with(root, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
                    Ok(locked) => locked,
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::NotFound | std::io::ErrorKind::WouldBlock
                        ) =>
                    {
                        continue;
                    }
                    Err(error) => return Err(error),
                };
            removed += clear_root_leftovers(root)?;
        }
        Ok(removed)
    }

    /// Publish into `root`, whose lock the caller holds.
    fn publish(
        &self,
        root: &Path,
        directory: &str,
        source: PublishSource<'_>,
        files: &[PlannedFile],
        digest: Digest,
        protection: RootProtection,
    ) -> Result<Publication, PublishError> {
        protection.check(root, self.owner)?;
        let target = root.join(directory);
        if exists(&target).map_err(|source| io_publish("inspect", &target, source))? {
            return self.already_present(&target, files, digest, protection);
        }

        // This publication's own directory: created here, so nothing else's
        // bytes are in it, and never one another publication made.
        let incoming = root.join(format!("{INCOMING_PREFIX}{directory}-{}", unique_suffix()));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&incoming)
            .map_err(|source| io_publish("create", &incoming, source))?;
        let _cleanup = IncomingWork(incoming.clone());
        self.copy_source(source, &incoming, files)?;
        std::fs::set_permissions(&incoming, std::fs::Permissions::from_mode(0o755))
            .map_err(|source| io_publish("chmod", &incoming, source))?;
        sync_dir(&incoming).map_err(|source| io_publish("sync", &incoming, source))?;
        match rename_noreplace(&incoming, &target) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                // Published meanwhile: what is there decides.
                return self.already_present(&target, files, digest, protection);
            }
            Err(source) => {
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

    /// Copy each planned file from the specified trust domain into `incoming`, checking it
    /// against its pin, and make every written file and directory durable.
    fn copy_source(
        &self,
        source: PublishSource<'_>,
        incoming: &Path,
        files: &[PlannedFile],
    ) -> Result<(), PublishError> {
        let (source_root, source_components) = source.components(&self.staging_root);
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
            let mut components: Vec<&str> = source_components
                .iter()
                .flat_map(|part| part.split('/'))
                .collect();
            components.extend(file.path.split('/'));
            let shown = source_root.join(components.join("/"));
            let mut source = self.open_source(source_root, &components, &shown)?;
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

    /// Open the source file `components` names under its explicit root, one
    /// component at a time, following no links.
    fn open_source(
        &self,
        root: &Path,
        components: &[&str],
        shown: &Path,
    ) -> Result<std::fs::File, PublishError> {
        let source_error = |source: std::io::Error| PublishError::Source {
            path: shown.to_path_buf(),
            source,
        };
        // openat accepts parent traversal even with NOFOLLOW. Reject every
        // non-normal component before opening any part of the source tree.
        if components.is_empty()
            || components
                .iter()
                .any(|part| part.is_empty() || matches!(*part, "." | "..") || part.contains('/'))
        {
            return Err(source_error(std::io::ErrorKind::InvalidInput.into()));
        }
        // A link where a directory or the file should be fails the
        // no-follow open with ELOOP, or ENOTDIR for a directory component.
        let opened = |errno: rustix::io::Errno| match errno {
            rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR => PublishError::Linked {
                path: shown.to_path_buf(),
            },
            errno => source_error(errno.into()),
        };
        let directory_flags =
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let mut directory: OwnedFd = rustix::fs::open(root, directory_flags, Mode::empty())
            .map_err(|errno| source_error(errno.into()))?;
        let Some((name, parents)) = components.split_last() else {
            return Err(source_error(std::io::ErrorKind::InvalidInput.into()));
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
        let metadata = file.metadata().map_err(source_error)?;
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
                    return Err(PublishError::Source {
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

    fn already_present(
        &self,
        target: &Path,
        files: &[PlannedFile],
        digest: Digest,
        protection: RootProtection,
    ) -> Result<Publication, PublishError> {
        let verified = self
            .verify_files(target, files, digest, protection)
            .map_err(PublishError::ExistingArtifact)?;
        Ok(Publication {
            outcome: PublishOutcome::AlreadyPresent,
            path: verified.path,
            digest: verified.digest,
        })
    }

    fn verify_files(
        &self,
        target: &Path,
        files: &[PlannedFile],
        digest: Digest,
        protection: RootProtection,
    ) -> Result<VerifiedArtifact, VerifyError> {
        for file in files {
            let path = target.join(file.path);
            protection.verify(path.parent().unwrap_or(target), self.owner)?;
            let invalid = |problem| VerifyError::InvalidFile {
                path: path.clone(),
                problem,
            };
            let metadata = match std::fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(invalid("missing"));
                }
                Err(source) => return Err(io_verify("inspect", &path, source)),
            };
            if !metadata.is_file() {
                return Err(invalid("not a regular file"));
            }
            if metadata.uid() != self.owner {
                return Err(invalid("owned by another account"));
            }
            if metadata.permissions().mode() & 0o022 != 0 {
                return Err(invalid("writable by its group or others"));
            }
            if (metadata.permissions().mode() & file.mode & 0o111) != (file.mode & 0o111) {
                return Err(invalid("missing required executable permissions"));
            }
            if metadata.len() > self.max_file_bytes {
                return Err(invalid("exceeds the verification size limit"));
            }
            if file.size.is_some_and(|size| metadata.len() != size) {
                return Err(invalid("size differs from the pinned size"));
            }
            let found = sha256_file(&path).map_err(|source| io_verify("read", &path, source))?;
            if found != file.sha256 {
                return Err(VerifyError::Mismatch {
                    path,
                    found,
                    pinned: file.sha256,
                });
            }
        }
        Ok(VerifiedArtifact {
            path: target.to_path_buf(),
            digest,
        })
    }
}

enum PublishSource<'a> {
    Staging {
        namespace: &'a str,
        staged: &'a StagedName,
        directory: &'a str,
    },
    Store {
        root: &'a Path,
        directory: &'a str,
    },
}

impl<'a> PublishSource<'a> {
    fn components(self, staging_root: &'a Path) -> (&'a Path, Vec<&'a str>) {
        match self {
            Self::Staging {
                namespace,
                staged,
                directory,
            } => (staging_root, vec![namespace, staged.as_str(), directory]),
            Self::Store { root, directory } => (root, vec![directory]),
        }
    }
}

/// One file a publication copies, and what it must be.
#[derive(Clone)]
struct PlannedFile {
    path: &'static str,
    sha256: Digest,
    size: Option<u64>,
    mode: u32,
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

// Both legacy verification and sealed staging consume this exact layout.
// Never authenticate a sibling vmlinux.bin while returning another input path.
fn kernel_image_directory(image: &Path) -> Result<&Path, VerifyError> {
    if image.file_name() != Some(std::ffi::OsStr::new(KERNEL_IMAGE)) {
        return Err(VerifyError::KernelImageName {
            path: image.to_path_buf(),
        });
    }
    image.parent().ok_or_else(|| VerifyError::InvalidImagePath {
        path: image.to_path_buf(),
    })
}

fn sha256_file(path: &Path) -> Result<Digest, std::io::Error> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    sha256_reader(file)
}

fn sha256_reader(mut file: impl std::io::Read) -> Result<Digest, std::io::Error> {
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

// ArtifactVersion is descriptive metadata; the store additionally requires a
// visible single directory component and reserves dot names for its own work.
fn store_version(version: &ArtifactVersion) -> bool {
    !version.as_str().starts_with('.')
        && matches!(
            std::path::Path::new(version.as_str())
                .components()
                .collect::<Vec<_>>()
                .as_slice(),
            [std::path::Component::Normal(_)]
        )
}

fn removal_evidence_error(
    action: &'static str,
    path: &Path,
    source: std::io::Error,
) -> RemoveError {
    if source.kind() == std::io::ErrorKind::NotFound {
        RemoveError::IdentityChanged {
            path: path.to_path_buf(),
        }
    } else {
        io(action, path, source)
    }
}

struct IncomingWork(PathBuf);
impl Drop for IncomingWork {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(path = %self.0.display(), %error, "artifact work cleanup failed");
        }
    }
}

fn io_verify(action: &'static str, path: &Path, source: std::io::Error) -> VerifyError {
    VerifyError::Io {
        action,
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
pub(crate) mod test_support;
