//! Firecracker release download, verification and bounded unpacking.

use super::describe;
use super::executor::InstallExecutor;
use super::release_source::{ReleaseSource, ReleaseSourceError, VerifiableRelease};
use super::staging::{self, StagingError};
use pplx_space_common_core::types::{Arch, Digest, RuntimeIdentity, RuntimeKind};
use pplx_space_common_jobs::JobError;
use pplx_space_node_runtime_core::catalog::{CatalogError, PinnedRelease};
use pplx_space_node_runtime_core::runtime_inventory::diagnostic::{self, ElfProbe, ReleaseLayout};
use sha2::{Digest as _, Sha256};
use std::io::{Read as _, Write as _};
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

const ARCHIVE_FILE: &str = "release.tgz";
const MAX_ARCHIVE_ENTRIES: usize = 256;

#[derive(Clone)]
pub(crate) struct ReleaseContext {
    pub(crate) root: PathBuf,
    pub(crate) source: Arc<dyn ReleaseSource>,
}

/// Why a release's binaries could not be measured.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ReleaseDigestError {
    #[error("{runtime} has no release layout this build knows")]
    NoLayout { runtime: RuntimeKind },
    #[error("read {}", path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{} is not a regular file", path.display())]
    NotRegularFile { path: PathBuf },
}

/// SHA-256 over the release in `release_dir`'s binaries, as
/// [`pplx_space_node_runtime_core::catalog::release_digest`] defines it.
/// Blocking.
pub(crate) fn release_digest(
    runtime: RuntimeKind,
    release_dir: &Path,
    limit: u64,
) -> Result<Digest, ReleaseDigestError> {
    let layout = ReleaseLayout::of(runtime).ok_or(ReleaseDigestError::NoLayout { runtime })?;
    let mut binaries = Vec::new();
    for binary in layout.binaries {
        let path = release_dir.join(binary);
        let read = |source| ReleaseDigestError::Read {
            path: path.clone(),
            source,
        };
        let metadata = std::fs::symlink_metadata(&path).map_err(read)?;
        if !metadata.is_file() || metadata.len() > limit {
            return Err(ReleaseDigestError::NotRegularFile { path });
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW)
            .open(&path)
            .map_err(read)?;
        let mut file = file.take(limit.saturating_add(1));
        let mut total = 0u64;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0; 64 * 1024];
        loop {
            match file.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => {
                    total = total.saturating_add(count as u64);
                    if total > limit {
                        return Err(ReleaseDigestError::NotRegularFile { path });
                    }
                    hasher.update(buffer.get(..count).unwrap_or_default());
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(read(error)),
            }
        }
        binaries.push((*binary, Digest::sha256(hasher.finalize().into())));
    }
    Ok(pplx_space_node_runtime_core::catalog::release_digest(
        binaries,
    ))
}

/// Why an archive could not be unpacked into a release.
#[derive(Debug, thiserror::Error)]
pub(crate) enum UnpackError {
    #[error("read the release archive")]
    Read(#[source] std::io::Error),
    #[error("the archive has no {member}")]
    Missing { member: String },
    #[error("the archive holds {member} more than once")]
    Duplicate { member: String },
    #[error("{member} in the archive is not a regular file")]
    NotRegularFile { member: String },
    #[error("{member} is {size} bytes, over the {limit}-byte limit")]
    TooLarge {
        member: String,
        size: u64,
        limit: u64,
    },
    #[error("{member} ended after {written} of its {size} bytes")]
    Truncated {
        member: String,
        written: u64,
        size: u64,
    },
    #[error("the archive has more than {limit} entries before its binaries")]
    TooManyEntries { limit: usize },
    #[error("the archive expands past {limit} bytes before its binaries")]
    TooMuchData { limit: u64 },
    #[error("cancelled")]
    Cancelled,
    #[error(transparent)]
    Staging(#[from] StagingError),
}

/// Bounds on one unpack.
#[derive(Clone, Copy, Debug)]
pub(crate) struct UnpackLimits {
    /// The largest binary written.
    pub(crate) max_member_bytes: u64,
    /// The most bytes decompressed, skipped entries included.
    pub(crate) max_total_bytes: u64,
    pub(crate) max_entries: usize,
}

/// Stops a decompressing read once it passes a byte limit or its job is
/// cancelled, so a crafted archive cannot keep the blocking thread busy.
struct BoundedRead<'a, R> {
    inner: R,
    remaining: u64,
    cancellation: &'a CancellationToken,
}

#[derive(Debug, thiserror::Error)]
#[error("unpack stopped")]
enum Stopped {
    Limit,
    Cancelled,
}

impl<R: std::io::Read> std::io::Read for BoundedRead<'_, R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.cancellation.is_cancelled() {
            return Err(std::io::Error::other(Stopped::Cancelled));
        }
        let read = self.inner.read(buffer)?;
        self.remaining = self
            .remaining
            .checked_sub(read as u64)
            .ok_or_else(|| std::io::Error::other(Stopped::Limit))?;
        Ok(read)
    }
}

/// The unpack error a read failure is, telling the bounds apart.
fn read_error(error: std::io::Error, limits: UnpackLimits) -> UnpackError {
    match error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<Stopped>())
    {
        Some(Stopped::Limit) => UnpackError::TooMuchData {
            limit: limits.max_total_bytes,
        },
        Some(Stopped::Cancelled) => UnpackError::Cancelled,
        None => UnpackError::Read(error),
    }
}

/// Unpack the binaries the release layout names from the archive at
/// `archive` into `release_dir`, as `<binary>` paths with mode 0755. Nothing
/// else in the archive is written, and nothing it names (links, paths) is
/// followed. Stops as soon as every binary is written, and fails once the
/// archive passes `limits` or `cancellation` fires. Blocking.
pub(crate) fn unpack(
    archive: &Path,
    release_dir: &Path,
    members: &[(&str, String)],
    limits: UnpackLimits,
    cancellation: &CancellationToken,
) -> Result<(), UnpackError> {
    let file = std::fs::File::open(archive).map_err(UnpackError::Read)?;
    let bounded = BoundedRead {
        inner: flate2::read::GzDecoder::new(file),
        remaining: limits.max_total_bytes,
        cancellation,
    };
    let mut tar = tar::Archive::new(bounded);
    let mut pending: Vec<(&str, &str, bool)> = members
        .iter()
        .map(|(binary, member)| (*binary, member.as_str(), false))
        .collect();
    let mut seen = 0usize;
    for entry in tar.entries().map_err(|error| read_error(error, limits))? {
        if pending.iter().all(|(.., written)| *written) {
            break;
        }
        seen += 1;
        if seen > limits.max_entries {
            return Err(UnpackError::TooManyEntries {
                limit: limits.max_entries,
            });
        }
        let mut entry = entry.map_err(|error| read_error(error, limits))?;
        let name = entry
            .path()
            .map_err(UnpackError::Read)?
            .to_string_lossy()
            .trim_start_matches("./")
            .to_string();
        let Some((binary, member, written)) =
            pending.iter_mut().find(|(_, member, _)| *member == name)
        else {
            continue;
        };
        let member = member.to_string();
        if *written {
            return Err(UnpackError::Duplicate { member });
        }
        if entry.header().entry_type() != tar::EntryType::Regular {
            return Err(UnpackError::NotRegularFile {
                member: member.clone(),
            });
        }
        let size = entry.header().size().map_err(UnpackError::Read)?;
        if size > limits.max_member_bytes {
            return Err(UnpackError::TooLarge {
                member: member.clone(),
                size,
                limit: limits.max_member_bytes,
            });
        }
        let destination = release_dir.join(binary);
        if let Some(parent) = destination.parent() {
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o755)
                .create(parent)
                .map_err(|error| StagingError::new("create", parent, error))?;
        }
        let mut out = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o755)
            .open(&destination)
            .map_err(|error| StagingError::new("create", &destination, error))?;
        let copied = std::io::copy(&mut (&mut entry).take(limits.max_member_bytes), &mut out)
            .map_err(|error| match read_error(error, limits) {
                UnpackError::Read(error) => StagingError::new("write", &destination, error).into(),
                bounded => bounded,
            })?;
        if copied != size {
            return Err(UnpackError::Truncated {
                member: member.clone(),
                written: copied,
                size,
            });
        }
        out.flush()
            .and_then(|()| out.sync_all())
            .map_err(|error| StagingError::new("sync", &destination, error))?;
        *written = true;
    }
    if let Some((_, member, _)) = pending.iter().find(|(.., written)| !written) {
        return Err(UnpackError::Missing {
            member: member.to_string(),
        });
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(super) enum DownloadError {
    #[error("cancelled")]
    Cancelled,
    #[error(transparent)]
    Source(#[from] ReleaseSourceError),
    #[error(transparent)]
    Transfer(#[from] super::transfer::TransferError<ReleaseSourceError>),
    #[error("the archive's SHA-256 {found} is not the pinned {pinned}")]
    ChecksumMismatch { found: Digest, pinned: Digest },
    #[error("the catalog cannot be loaded")]
    Catalog(#[from] CatalogError),
    #[error("{release} has no pins for {architecture}")]
    NotPinned {
        release: RuntimeIdentity,
        architecture: Arch,
    },
    #[error("the unpacked binaries are not the pinned ones")]
    BinariesDiffer,
    #[error("the requested release pins differ from the current catalog; submit a new install")]
    PinChanged,
    #[error("the archive cannot be unpacked")]
    Unpack(#[from] UnpackError),
    #[error("{} is not an {architecture} executable", path.display())]
    NotExecutable { path: PathBuf, architecture: Arch },
    #[error(transparent)]
    Measure(#[from] ReleaseDigestError),
    #[error(transparent)]
    Staging(#[from] StagingError),
    #[error("download task failed")]
    Task(#[from] tokio::task::JoinError),
}

// Archive decoders also use io::Error (including Other) for malformed input.
// Retry OS failures, including ones wrapped with archive context, not parser errors.
fn local_read_failure(error: &std::io::Error) -> bool {
    if error.raw_os_error().is_some() {
        return true;
    }
    let mut cause = error
        .get_ref()
        .map(|inner| inner as &(dyn std::error::Error + 'static));
    while let Some(error) = cause {
        if error
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.raw_os_error().is_some())
        {
            return true;
        }
        cause = error.source();
    }
    false
}

impl DownloadError {
    /// Transfer and local failures may clear up on another attempt; bytes
    /// that do not match what the source published will not.
    pub(super) fn into_job_error(self) -> JobError {
        match self {
            Self::Cancelled => JobError::YieldedForRestart,
            Self::Unpack(UnpackError::Cancelled) => JobError::YieldedForRestart,
            Self::Unpack(UnpackError::Staging(_)) => JobError::retriable(describe(&self)),
            Self::Unpack(UnpackError::Read(ref error)) if local_read_failure(error) => {
                JobError::retriable(describe(&self))
            }
            Self::ChecksumMismatch { .. }
            | Self::Catalog(_)
            | Self::NotPinned { .. }
            | Self::PinChanged
            | Self::BinariesDiffer
            | Self::Unpack(_)
            | Self::NotExecutable { .. } => JobError::permanent(describe(&self)),
            Self::Transfer(error) => error.into_job_error(),
            Self::Source(_) | Self::Measure(_) | Self::Staging(_) | Self::Task(_) => {
                JobError::retriable(describe(&self))
            }
        }
    }
}

impl InstallExecutor {
    pub(super) async fn prepare_release(
        &self,
        context: &ReleaseContext,
        release: &VerifiableRelease,
        digest: Digest,
        staged_root: &Path,
        cancellation: &CancellationToken,
    ) -> Result<(Digest, u64), DownloadError> {
        let pins = self.context().pins.clone();
        let (pinned_release, architecture) = (release.release.clone(), release.architecture);
        let pin: PinnedRelease = tokio::task::spawn_blocking(move || {
            pins.catalog()
                .map(|catalog| catalog.release(&pinned_release, architecture).cloned())
        })
        .await??
        .ok_or_else(|| DownloadError::NotPinned {
            release: release.release.clone(),
            architecture,
        })?;

        if pin.digest() != digest || pin.archive_sha256 != release.sha256 {
            return Err(DownloadError::PinChanged);
        }
        let staged = staged_root.join(
            pplx_space_node_runtime_core::artifact_store::release_directory(&release.release),
        );
        let created = staged_root.to_path_buf();
        tokio::task::spawn_blocking(move || staging::create_private_dir(&created)).await??;
        let archive_path = staged_root.join(ARCHIVE_FILE);
        let opened = archive_path.clone();
        let file = tokio::task::spawn_blocking(move || {
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&opened)
                .map_err(|error| StagingError::new("create", &opened, error))
        })
        .await??;

        let chunks = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(DownloadError::Cancelled),
            opened = context.source.open(&release.archive) => opened?,
        };
        let limits = self.context().limits;
        let writer = ArchiveWriter {
            file,
            hasher: Sha256::new(),
            written: 0,
        };
        let writer = super::transfer::receive(
            writer,
            chunks,
            archive_path.clone(),
            limits.max_bytes,
            limits.download_idle_timeout,
            cancellation,
        )
        .await?;
        let received = writer.written;
        let synced = archive_path.clone();
        let found = tokio::task::spawn_blocking(move || {
            writer
                .file
                .sync_all()
                .map_err(|error| StagingError::new("sync", &synced, error))?;
            Ok::<_, StagingError>(Digest::sha256(writer.hasher.finalize().into()))
        })
        .await??;
        if found != release.sha256 {
            return Err(DownloadError::ChecksumMismatch {
                found,
                pinned: release.sha256,
            });
        }
        let layout =
            ReleaseLayout::of(release.release.kind).ok_or(ReleaseDigestError::NoLayout {
                runtime: release.release.kind,
            })?;
        let members: Vec<(&'static str, String)> = layout
            .binaries
            .iter()
            .map(|binary| {
                let name = Path::new(binary)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or(binary);
                (
                    *binary,
                    context
                        .source
                        .archive_member(&release.release, release.architecture, name),
                )
            })
            .collect();
        let (unpacked_archive, unpacked) = (archive_path.clone(), staged.to_path_buf());
        let unpack_limits = UnpackLimits {
            max_member_bytes: limits.max_bytes,
            max_total_bytes: limits.max_bytes.saturating_mul(4),
            max_entries: MAX_ARCHIVE_ENTRIES,
        };
        let unpack_cancellation = cancellation.clone();
        tokio::task::spawn_blocking(move || {
            std::fs::DirBuilder::new()
                .mode(0o755)
                .create(&unpacked)
                .map_err(|error| StagingError::new("create", &unpacked, error))?;
            unpack(
                &unpacked_archive,
                &unpacked,
                &members,
                unpack_limits,
                &unpack_cancellation,
            )
        })
        .await??;

        let architecture = self.context().host_arch;
        let runtime = release.release.kind;
        let checked = staged.to_path_buf();
        let pinned_digest = pin.digest();
        let digest = tokio::task::spawn_blocking(move || -> Result<Digest, DownloadError> {
            for binary in layout.binaries {
                let path = checked.join(binary);
                let probe = diagnostic::probe_elf_file(&path)
                    .map_err(|error| StagingError::new("inspect", &path, error))?;
                if probe
                    != (ElfProbe::Elf {
                        architecture: Some(architecture),
                    })
                {
                    return Err(DownloadError::NotExecutable { path, architecture });
                }
            }
            // Every directory the binaries were created in, deepest first,
            // then the release directory: the rename publishes them, and a
            // committed install must not lose its binaries to a crash.
            let mut directories: Vec<PathBuf> = layout
                .binaries
                .iter()
                .filter_map(|binary| checked.join(binary).parent().map(Path::to_path_buf))
                .filter(|directory| *directory != checked)
                .collect();
            directories.sort_by(|left, right| {
                right
                    .components()
                    .count()
                    .cmp(&left.components().count())
                    .then_with(|| left.cmp(right))
            });
            directories.dedup();
            for directory in &directories {
                staging::sync_dir(directory)?;
            }
            staging::sync_dir(&checked)?;
            let digest = release_digest(runtime, &checked, limits.max_bytes)?;
            // The pins are what node-manager will hold the binaries to;
            // refusing here saves asking it.
            if digest != pinned_digest {
                return Err(DownloadError::BinariesDiffer);
            }
            Ok(digest)
        })
        .await??;
        Ok((digest, received))
    }
}

struct ArchiveWriter {
    file: std::fs::File,
    hasher: Sha256,
    written: u64,
}
impl super::transfer::DownloadWriter for ArchiveWriter {
    fn written(&self) -> u64 {
        self.written
    }
    fn append(&mut self, bytes: &[u8], path: &Path) -> Result<(), StagingError> {
        self.file
            .write_all(bytes)
            .map_err(|error| StagingError::new("write", path, error))?;
        self.hasher.update(bytes);
        self.written += bytes.len() as u64;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
