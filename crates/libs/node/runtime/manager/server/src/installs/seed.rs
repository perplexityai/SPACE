//! Kernels the node image pre-installed into the runtime manager's own root.
//!
//! An image build puts its kernels where an install would, as
//! `<kernels root>/<version>/vmlinux.bin`, and lists each in a manifest with
//! the source it came from and the digests it pinned. At startup every
//! listed kernel whose bytes still match is recorded as space-managed, as if
//! the runtime manager had installed it, so it can be uninstalled and
//! reinstalled like any other. A kernel whose bytes do not match, or that is
//! missing, is left unrecorded and stays external. The manifest is the
//! image's evidence: the runtime manager reads it and never writes it.

use std::path::{Path, PathBuf};

use pplx_space_common_core::types::{Arch, Digest, StorageUri};
use rusqlite::Connection;

use super::source::{ContentMd5, KernelVersion, RuntimeLine, storage_uri_text};
use super::staging::{self, ImageDigestError, KERNEL_IMAGE, StagingError};
use super::store::{self, KernelInstall, StoreError};

/// The manifest version this build reads.
pub(crate) const SEED_MANIFEST_SCHEMA: u32 = 1;

/// `seed.json`, as the image build writes it.
#[derive(Debug, serde::Deserialize)]
struct SeedManifest {
    schema: u32,
    #[serde(default)]
    kernels: Vec<SeedKernel>,
}

/// One pre-installed kernel and the evidence for its bytes.
#[derive(Debug, serde::Deserialize)]
struct SeedKernel {
    version: KernelVersion,
    line: RuntimeLine,
    /// The object the image build downloaded it from.
    #[serde(with = "storage_uri_text")]
    source: StorageUri,
    size: u64,
    md5: ContentMd5,
    sha256: Digest,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SeedError {
    #[error("read the seed manifest {}", path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parse the seed manifest {}", path.display())]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error(
        "the seed manifest {} has schema {found}; this runtime manager reads schema {SEED_MANIFEST_SCHEMA}",
        path.display()
    )]
    Schema { path: PathBuf, found: u32 },
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("seed import task failed")]
    Task(#[from] tokio::task::JoinError),
}

/// Why a listed kernel was not recorded.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SeedRefusal {
    #[error("kernel {version} is already recorded with bytes other than the manifest names")]
    RecordedDiffers { version: KernelVersion },
    #[error("kernel {version} at {} cannot be measured", path.display())]
    Unmeasurable {
        version: KernelVersion,
        path: PathBuf,
        #[source]
        source: ImageDigestError,
    },
    #[error("kernel {version} at {} does not hold the bytes the manifest names", path.display())]
    Differs {
        version: KernelVersion,
        path: PathBuf,
    },
    #[error("kernel {version} at {} cannot be inspected", path.display())]
    Uninspectable {
        version: KernelVersion,
        path: PathBuf,
        #[source]
        source: StagingError,
    },
    #[error("kernel {version} at {} is not a {architecture} boot image", path.display())]
    NotBootImage {
        version: KernelVersion,
        path: PathBuf,
        architecture: Arch,
    },
}

/// What an import did with each listed kernel.
#[derive(Debug, Default)]
pub(crate) struct SeedReport {
    /// Recorded by this import.
    pub(crate) seeded: Vec<KernelVersion>,
    /// Recorded already, with the bytes the manifest names.
    pub(crate) recorded: Vec<KernelVersion>,
    pub(crate) refused: Vec<SeedRefusal>,
}

/// Record every kernel `manifest` lists that sits in `kernels_root` with the
/// bytes it names. No manifest seeds nothing. Blocking.
pub(super) fn import(
    db: &Connection,
    kernels_root: &Path,
    architecture: Arch,
    manifest: &Path,
    now: jiff::Timestamp,
) -> Result<SeedReport, SeedError> {
    let text = match std::fs::read(manifest) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(SeedReport::default());
        }
        Err(source) => {
            return Err(SeedError::Read {
                path: manifest.to_path_buf(),
                source,
            });
        }
    };
    let parsed: SeedManifest =
        serde_json::from_slice(&text).map_err(|source| SeedError::Parse {
            path: manifest.to_path_buf(),
            source,
        })?;
    if parsed.schema != SEED_MANIFEST_SCHEMA {
        return Err(SeedError::Schema {
            path: manifest.to_path_buf(),
            found: parsed.schema,
        });
    }

    let mut report = SeedReport::default();
    for kernel in parsed.kernels {
        if let Some(recorded) = store::get_kernel_install(db, &kernel.version)? {
            if recorded.digest == kernel.sha256 {
                report.recorded.push(kernel.version);
            } else {
                report.refused.push(SeedRefusal::RecordedDiffers {
                    version: kernel.version,
                });
            }
            continue;
        }
        let path = kernels_root.join(kernel.version.as_str());
        if let Some(refusal) = verify(&kernel, &path, architecture) {
            report.refused.push(refusal);
            continue;
        }
        let inserted = store::insert_kernel_install(
            db,
            &KernelInstall {
                version: kernel.version.clone(),
                path,
                line: kernel.line,
                location: kernel.source,
                size: kernel.size,
                md5: kernel.md5,
                digest: kernel.sha256,
                job_id: None,
                installed_at: now,
            },
        )?;
        if inserted {
            report.seeded.push(kernel.version);
        } else {
            report.recorded.push(kernel.version);
        }
    }
    Ok(report)
}

/// Why the image at `path` is not the kernel `kernel` names, if it is not.
fn verify(kernel: &SeedKernel, path: &Path, architecture: Arch) -> Option<SeedRefusal> {
    let image = path.join(KERNEL_IMAGE);
    match staging::image_digests(&image, kernel.size) {
        Ok(found) if found.sha256 == kernel.sha256 && found.md5 == kernel.md5 => {}
        Ok(_) | Err(ImageDigestError::WrongSize { .. }) => {
            return Some(SeedRefusal::Differs {
                version: kernel.version.clone(),
                path: path.to_path_buf(),
            });
        }
        Err(source) => {
            return Some(SeedRefusal::Unmeasurable {
                version: kernel.version.clone(),
                path: path.to_path_buf(),
                source,
            });
        }
    }
    match staging::is_boot_image(&image, architecture) {
        Ok(true) => None,
        Ok(false) => Some(SeedRefusal::NotBootImage {
            version: kernel.version.clone(),
            path: path.to_path_buf(),
            architecture,
        }),
        Err(source) => Some(SeedRefusal::Uninspectable {
            version: kernel.version.clone(),
            path: path.to_path_buf(),
            source,
        }),
    }
}

#[cfg(test)]
mod tests;
