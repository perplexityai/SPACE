//! The runtime manager's private staging: writing and hashing an image,
//! checking one already on disk, and preparing and clearing the directory
//! node-manager publishes from.
//!
//! Everything here blocks; callers run it on the blocking pool.

use std::io::{Read as _, Write as _};
use std::os::unix::fs::{
    DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _,
};
use std::path::{Path, PathBuf};

use md5::Md5;
use pplx_space_common_core::types::{Arch, ArtifactVersion, Digest};
use pplx_space_node_runtime_core::runtime_inventory::diagnostic::{self, ElfProbe};
use pplx_space_node_runtime_core::runtime_inventory::kernel_release::KernelReleaseScanner;
use sha2::{Digest as _, Sha256};

use super::source::ContentMd5;

/// The image file inside a kernel's directory.
pub(super) use pplx_space_node_runtime_core::artifact_store::KERNEL_IMAGE;
/// Bytes buffered before a write is handed to the blocking pool.
pub(super) const WRITE_BATCH_BYTES: usize = 1024 * 1024;
const HASH_CHUNK_BYTES: usize = 1024 * 1024;

/// A filesystem step that failed, and on what.
#[derive(Debug, thiserror::Error)]
#[error("{action} {}", path.display())]
pub struct StagingError {
    action: &'static str,
    path: PathBuf,
    #[source]
    source: std::io::Error,
}

impl StagingError {
    pub(super) fn new(action: &'static str, path: &Path, source: std::io::Error) -> Self {
        Self {
            action,
            path: path.to_path_buf(),
            source,
        }
    }
}

/// Why an image on disk could not be measured.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ImageDigestError {
    #[error("read {}", path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{} is not a regular file", path.display())]
    NotRegularFile { path: PathBuf },
    #[error("{} is {found} bytes, not {expected}", path.display())]
    WrongSize {
        path: PathBuf,
        found: u64,
        expected: u64,
    },
}

/// The arm64 Linux `Image` header's magic, at byte 56: `ARM\x64`.
const ARM64_IMAGE_MAGIC: &[u8; 4] = b"ARMd";
const ARM64_IMAGE_MAGIC_OFFSET: usize = 56;

/// Whether `path` is a boot image a microVM runtime loads on `architecture`: an
/// ELF `vmlinux` on x86_64, the Linux `Image` format on aarch64. Reads only
/// the header.
pub(super) fn is_boot_image(path: &Path, architecture: Arch) -> Result<bool, StagingError> {
    let inspect = |error| StagingError::new("inspect", path, error);
    match architecture {
        Arch::X86_64 => Ok(matches!(
            diagnostic::probe_elf_file(path).map_err(inspect)?,
            ElfProbe::Elf {
                architecture: Some(Arch::X86_64)
            }
        )),
        Arch::Aarch64 => {
            let mut header = [0u8; ARM64_IMAGE_MAGIC_OFFSET + 4];
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW)
                .open(path)
                .map_err(inspect)?;
            match file.read_exact(&mut header) {
                Ok(()) => Ok(header.get(ARM64_IMAGE_MAGIC_OFFSET..) == Some(ARM64_IMAGE_MAGIC)),
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
                Err(error) => Err(inspect(error)),
            }
        }
    }
}

/// The digests of one image.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ImageDigests {
    pub(super) md5: ContentMd5,
    pub(super) sha256: Digest,
}

/// A staged image once it is on disk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FinishedImage {
    pub(super) digests: ImageDigests,
    /// The release its `Linux version` banner names, if it has one.
    pub(super) release: Option<ArtifactVersion>,
}

/// A kernel image being written into staging, hashed and searched for its
/// release banner as it grows.
pub(super) struct StagedImage {
    file: std::fs::File,
    image: PathBuf,
    md5: Md5,
    sha256: Sha256,
    release: KernelReleaseScanner,
    written: u64,
}

impl StagedImage {
    /// Create the staged kernel directory `staged` inside `staged_root`, both
    /// private, and the empty image inside it.
    pub(super) fn create(staged_root: &Path, staged: &Path) -> Result<Self, StagingError> {
        Self::create_named(staged_root, staged, KERNEL_IMAGE)
    }

    pub(super) fn create_boot_artifact(
        staged_root: &Path,
        staged: &Path,
    ) -> Result<Self, StagingError> {
        Self::create_named(
            staged_root,
            staged,
            pplx_space_node_runtime_core::artifact_store::BOOT_ARTIFACT_IMAGE,
        )
    }

    fn create_named(staged_root: &Path, staged: &Path, name: &str) -> Result<Self, StagingError> {
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        builder
            .create(staged_root)
            .map_err(|error| StagingError::new("create", staged_root, error))?;
        builder
            .create(staged)
            .map_err(|error| StagingError::new("create", staged, error))?;
        let image = staged.join(name);
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&image)
            .map_err(|error| StagingError::new("create", &image, error))?;
        Ok(Self {
            file,
            image,
            md5: Md5::new(),
            sha256: Sha256::new(),
            release: KernelReleaseScanner::new(),
            written: 0,
        })
    }

    pub(super) fn written(&self) -> u64 {
        self.written
    }

    pub(super) fn append(&mut self, bytes: &[u8], path: &Path) -> Result<(), StagingError> {
        self.file
            .write_all(bytes)
            .map_err(|error| StagingError::new("write", path, error))?;
        self.md5.update(bytes);
        self.sha256.update(bytes);
        self.release.update(bytes);
        self.written += bytes.len() as u64;
        Ok(())
    }

    /// Flush the image to disk, make it and its directory readable, and
    /// return what was learned while writing it.
    pub(super) fn finish(self, staged: &Path) -> Result<FinishedImage, StagingError> {
        let image = self.image;
        self.file
            .sync_all()
            .map_err(|error| StagingError::new("sync", &image, error))?;
        std::fs::set_permissions(&image, std::fs::Permissions::from_mode(0o644))
            .map_err(|error| StagingError::new("chmod", &image, error))?;
        std::fs::set_permissions(staged, std::fs::Permissions::from_mode(0o755))
            .map_err(|error| StagingError::new("chmod", staged, error))?;
        sync_dir(staged)?;
        Ok(FinishedImage {
            digests: ImageDigests {
                md5: ContentMd5::new(self.md5.finalize().into()),
                sha256: Digest::sha256(self.sha256.finalize().into()),
            },
            release: self.release.finish(),
        })
    }
}

impl super::transfer::DownloadWriter for StagedImage {
    fn written(&self) -> u64 {
        self.written()
    }
    fn append(&mut self, bytes: &[u8], path: &Path) -> Result<(), StagingError> {
        self.append(bytes, path)
    }
}

/// The digests of the regular file at `path`, which must be `expected_size`
/// bytes. Reads at most that much.
pub(super) fn image_digests(
    path: &Path,
    expected_size: u64,
) -> Result<ImageDigests, ImageDigestError> {
    let read = |source| ImageDigestError::Read {
        path: path.to_path_buf(),
        source,
    };
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW)
        .open(path)
        .map_err(read)?;
    let metadata = file.metadata().map_err(read)?;
    if !metadata.is_file() {
        return Err(ImageDigestError::NotRegularFile {
            path: path.to_path_buf(),
        });
    }
    if metadata.size() != expected_size {
        return Err(ImageDigestError::WrongSize {
            path: path.to_path_buf(),
            found: metadata.size(),
            expected: expected_size,
        });
    }
    let mut md5 = Md5::new();
    let mut sha256 = Sha256::new();
    let mut buffer = vec![0u8; HASH_CHUNK_BYTES];
    let mut file = file.take(expected_size.saturating_add(1));
    let mut total = 0u64;
    loop {
        match file.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                total = total.saturating_add(count as u64);
                if total > expected_size {
                    return Err(ImageDigestError::WrongSize {
                        path: path.to_path_buf(),
                        found: total,
                        expected: expected_size,
                    });
                }
                let chunk = buffer.get(..count).unwrap_or_default();
                md5.update(chunk);
                sha256.update(chunk);
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(read(error)),
        }
    }
    if total != expected_size {
        return Err(ImageDigestError::WrongSize {
            path: path.to_path_buf(),
            found: total,
            expected: expected_size,
        });
    }
    Ok(ImageDigests {
        md5: ContentMd5::new(md5.finalize().into()),
        sha256: Digest::sha256(sha256.finalize().into()),
    })
}

/// Why staging could not be prepared.
#[derive(Debug, thiserror::Error)]
pub enum PrepareStagingError {
    #[error(transparent)]
    Staging(#[from] StagingError),
    #[error("{} is not a directory", path.display())]
    NotADirectory { path: PathBuf },
}

/// Create the namespace directories above a job's staging directory,
/// private like the staging root they are in.
pub(super) fn create_namespace(staged_root: &Path) -> Result<(), StagingError> {
    let Some(namespace) = staged_root.parent() else {
        return Ok(());
    };
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(namespace)
        .map_err(|error| StagingError::new("create", namespace, error))
}

/// Make staging private to this account and empty it. Provisioning creates
/// it where the runtime manager could not, as on the cache; a missing one is
/// created when the parent allows it. Run before any job, when nothing in
/// staging is live.
pub(super) fn prepare(staging: &Path) -> Result<(), PrepareStagingError> {
    match std::fs::DirBuilder::new().mode(0o700).create(staging) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(StagingError::new("create", staging, error).into()),
    }
    directory(staging)?;
    std::fs::set_permissions(staging, std::fs::Permissions::from_mode(0o700))
        .map_err(|error| StagingError::new("chmod", staging, error))?;
    clear_dir(staging)?;
    Ok(())
}

fn directory(path: &Path) -> Result<std::fs::Metadata, PrepareStagingError> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| StagingError::new("inspect", path, error))?;
    if !metadata.is_dir() {
        return Err(PrepareStagingError::NotADirectory {
            path: path.to_path_buf(),
        });
    }
    Ok(metadata)
}

pub(super) fn create_private_dir(path: &Path) -> Result<(), StagingError> {
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(path)
        .map_err(|error| StagingError::new("create", path, error))
}

pub(super) fn sync_dir(dir: &Path) -> Result<(), StagingError> {
    std::fs::File::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(|error| StagingError::new("sync", dir, error))
}

pub(super) fn remove_dir_if_present(dir: &Path) -> Result<(), StagingError> {
    match std::fs::remove_dir_all(dir) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        result => result.map_err(|error| StagingError::new("remove", dir, error)),
    }
}

/// Remove everything inside `dir`, which stays.
fn clear_dir(dir: &Path) -> Result<(), StagingError> {
    let entries = std::fs::read_dir(dir).map_err(|error| StagingError::new("list", dir, error))?;
    for entry in entries {
        let path = entry
            .map_err(|error| StagingError::new("list", dir, error))?
            .path();
        let is_dir = std::fs::symlink_metadata(&path)
            .map_err(|error| StagingError::new("inspect", &path, error))?
            .is_dir();
        let removed = if is_dir {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        removed.map_err(|error| StagingError::new("remove", &path, error))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
