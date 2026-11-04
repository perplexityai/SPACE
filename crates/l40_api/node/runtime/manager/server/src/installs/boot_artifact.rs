//! Offline auxiliary files follow the same bounded staging and node publication
//! pipeline as downloaded artifacts. The feed supplies bytes, never trust.
use super::executor::{InstallExecutor, permanent, retriable};
use super::staging::{StagedImage, StagingError};
use pplx_space_common_jobs::JobError;
use pplx_space_node_runtime_core::artifact_store::{BOOT_ARTIFACT_IMAGE, boot_artifact_directory};
use pplx_space_node_runtime_core::catalog::PinnedBootArtifact;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::Path;
use tokio::io::AsyncReadExt as _;
use tokio_util::sync::CancellationToken;

impl InstallExecutor {
    pub(super) async fn prepare_boot_artifact(
        &self,
        pin: &PinnedBootArtifact,
        staged_root: &Path,
        cancellation: &CancellationToken,
    ) -> Result<(), JobError> {
        let source = self
            .context()
            .boot_source
            .as_ref()
            .ok_or_else(|| JobError::permanent("auxiliary boot source is disabled"))?
            .join(boot_artifact_directory(pin.identity()))
            .join(BOOT_ARTIFACT_IMAGE);
        let pins = self.context().pins.clone();
        let checked = pin.clone();
        let architecture = self.context().host_arch;
        let limit = self.context().limits.max_bytes;
        let staged_root = staged_root.to_path_buf();
        let staged = staged_root.join(pin.identity().version.as_str());
        let finished = staged.clone();
        let (file, writer) = tokio::task::spawn_blocking(move || {
            let catalog = pins.catalog().map_err(|error| permanent(&error))?;
            if checked.architecture() != architecture
                || checked.size() > limit
                || catalog.boot_artifact(checked.identity(), architecture) != Some(&checked)
            {
                return Err(JobError::permanent(
                    "boot artifact pin changed or exceeds install limits",
                ));
            }
            let file = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(&source)
                .map_err(|error| retriable(&StagingError::new("open source", &source, error)))?;
            let metadata = file.metadata().map_err(|error| retriable(&error))?;
            if !metadata.is_file() || metadata.len() != checked.size() {
                return Err(JobError::permanent(
                    "boot artifact source is not a regular file of the pinned size",
                ));
            }
            let writer = StagedImage::create_boot_artifact(&staged_root, &staged)
                .map_err(|error| retriable(&error))?;
            Ok((file, writer))
        })
        .await
        .map_err(|error| retriable(&error))??;
        let chunks = Box::pin(futures_util::stream::try_unfold(
            tokio::fs::File::from_std(file),
            |mut file| async move {
                let mut bytes = vec![0; super::staging::WRITE_BATCH_BYTES];
                let size = file.read(&mut bytes).await?;
                if size == 0 {
                    return Ok::<_, std::io::Error>(None);
                }
                bytes.truncate(size);
                Ok(Some((bytes::Bytes::from(bytes), file)))
            },
        ));
        let writer = super::transfer::receive(
            writer,
            chunks,
            finished.join(BOOT_ARTIFACT_IMAGE),
            pin.size(),
            self.context().limits.download_idle_timeout,
            cancellation,
        )
        .await
        .map_err(super::transfer::TransferError::into_job_error)?;
        if writer.written() != pin.size() {
            return Err(JobError::retriable(
                "boot artifact source became shorter during copy",
            ));
        }
        let result = tokio::task::spawn_blocking(move || writer.finish(&finished))
            .await
            .map_err(|error| retriable(&error))?
            .map_err(|error| retriable(&error))?;
        if result.digests.sha256 != pin.identity().digest {
            return Err(JobError::permanent(
                "boot artifact source differs from catalog SHA-256 pin",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
