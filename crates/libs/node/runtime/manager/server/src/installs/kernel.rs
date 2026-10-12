//! Firecracker kernel download and verification; lifecycle belongs to the shared executor.

use super::describe;
use super::executor::InstallExecutor;
use super::source::{ContentMd5, KernelVersion, SourceError, VerifiableKernel};
use super::staging::{self, FinishedImage, ImageDigests, KERNEL_IMAGE, StagedImage, StagingError};
use pplx_space_common_core::types::{Arch, ArtifactVersion, Digest};
use pplx_space_common_jobs::JobError;
use pplx_space_node_runtime_core::catalog::CatalogError;
use std::path::Path;
use tokio_util::sync::CancellationToken;

/// How a download ended short of verified bytes.
#[derive(Debug, thiserror::Error)]
pub(super) enum DownloadError {
    #[error("cancelled")]
    Cancelled,
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error("the source sent {received} of the {listed} bytes it listed")]
    Short { received: u64, listed: u64 },
    #[error(transparent)]
    Transfer(#[from] super::transfer::TransferError<SourceError>),
    #[error("the download's MD5 {found} is not the {published} the source published")]
    Md5Mismatch {
        found: ContentMd5,
        published: ContentMd5,
    },
    #[error("the download is not an {architecture} kernel boot image")]
    NotKernel { architecture: Arch },
    #[error("the downloaded image announces release {found}, not the {listed} the source listed")]
    WrongRelease {
        found: ArtifactVersion,
        listed: KernelVersion,
    },
    #[error("the catalog cannot be loaded")]
    Catalog(#[from] CatalogError),
    #[error("kernel {version} has no pins for {architecture}")]
    NotPinned {
        version: KernelVersion,
        architecture: Arch,
    },
    #[error("requested kernel digest {requested} differs from current catalog digest {current}")]
    PinChanged { requested: Digest, current: Digest },
    #[error("the download's SHA-256 {found} is not the pinned {pinned}")]
    PinMismatch { found: Digest, pinned: Digest },
    #[error(transparent)]
    Staging(#[from] StagingError),
    #[error("download task failed")]
    Task(#[from] tokio::task::JoinError),
}

impl DownloadError {
    /// Transfer and local failures may clear up on another attempt; bytes
    /// that do not match what the source published, or the pins, will not.
    pub(super) fn into_job_error(self) -> JobError {
        match self {
            Self::Cancelled => JobError::YieldedForRestart,
            Self::Md5Mismatch { .. }
            | Self::NotKernel { .. }
            | Self::WrongRelease { .. }
            | Self::Catalog(_)
            | Self::NotPinned { .. }
            | Self::PinChanged { .. }
            | Self::PinMismatch { .. } => JobError::permanent(describe(&self)),
            Self::Transfer(error) => error.into_job_error(),
            Self::Source(_) | Self::Short { .. } | Self::Staging(_) | Self::Task(_) => {
                JobError::retriable(describe(&self))
            }
        }
    }
}

impl InstallExecutor {
    /// Stream the kernel into `staged`, writing in batches on the blocking
    /// pool, then hold it to the source's size, optional MD5, and version,
    /// to this node's architecture, and to its pins. Returns its SHA-256.
    pub(super) async fn prepare_kernel(
        &self,
        kernel: &VerifiableKernel,
        staged_root: &Path,
        staged: &Path,
        cancellation: &CancellationToken,
    ) -> Result<Digest, DownloadError> {
        let architecture = self.context().host_arch;
        let pins = self.context().pins.clone();
        let pinned_version = kernel.version.clone();
        let pin = tokio::task::spawn_blocking(move || {
            pins.catalog().map(|catalog| {
                catalog
                    .kernel(pinned_version.artifact_version(), architecture)
                    .cloned()
            })
        })
        .await??
        .ok_or_else(|| DownloadError::NotPinned {
            version: kernel.version.clone(),
            architecture,
        })?;

        if pin.sha256 != kernel.digest {
            return Err(DownloadError::PinChanged {
                requested: kernel.digest,
                current: pin.sha256,
            });
        }
        let (created_root, created) = (staged_root.to_path_buf(), staged.to_path_buf());
        let mut writer =
            tokio::task::spawn_blocking(move || StagedImage::create(&created_root, &created))
                .await??;

        let chunks = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(DownloadError::Cancelled),
            opened = self.context().source.open(kernel) => opened?,
        };
        let image = staged.join(KERNEL_IMAGE);
        writer = super::transfer::receive(
            writer,
            chunks,
            image.clone(),
            kernel.size,
            self.context().limits.download_idle_timeout,
            cancellation,
        )
        .await?;
        if writer.written() != kernel.size {
            return Err(DownloadError::Short {
                received: writer.written(),
                listed: kernel.size,
            });
        }

        let finished = staged.to_path_buf();
        let FinishedImage {
            digests: ImageDigests { md5, sha256 },
            release,
        } = tokio::task::spawn_blocking(move || writer.finish(&finished)).await??;
        if let Some(published) = kernel.md5
            && md5 != published
        {
            return Err(DownloadError::Md5Mismatch {
                found: md5,
                published,
            });
        }
        // The pin is what node-manager will hold the bytes to; refusing here
        // saves asking it.
        if sha256 != pin.sha256 || kernel.size != pin.size {
            return Err(DownloadError::PinMismatch {
                found: sha256,
                pinned: pin.sha256,
            });
        }
        // The version is the install's name and what listings report, so an
        // image that names another release is refused. One with no banner
        // has nothing to contradict.
        if let Some(found) = release
            && found.as_str() != kernel.version.release()
        {
            return Err(DownloadError::WrongRelease {
                found,
                listed: kernel.version.clone(),
            });
        }
        let probed = image.clone();
        let boots =
            tokio::task::spawn_blocking(move || staging::is_boot_image(&probed, architecture))
                .await??;
        if boots {
            Ok(sha256)
        } else {
            Err(DownloadError::NotKernel { architecture })
        }
    }
}
