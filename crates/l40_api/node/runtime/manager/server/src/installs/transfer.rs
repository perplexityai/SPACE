//! Bounded streaming shared by kernel images and release archives.
use super::{describe, staging::StagingError};
use futures_util::{Stream, StreamExt as _};
use pplx_space_common_jobs::JobError;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub(super) trait DownloadWriter: Send + 'static {
    fn written(&self) -> u64;
    fn append(&mut self, bytes: &[u8], path: &Path) -> Result<(), StagingError>;
}

#[derive(Debug, thiserror::Error)]
pub(super) enum TransferError<E: std::error::Error + 'static> {
    #[error("cancelled")]
    Cancelled,
    #[error("download failed")]
    Source(#[source] E),
    #[error("download stalled for {}s", .0.as_secs())]
    Stalled(Duration),
    #[error("download exceeds {0} bytes")]
    TooLarge(u64),
    #[error(transparent)]
    Write(#[from] StagingError),
    #[error("download write task failed")]
    Task(#[from] tokio::task::JoinError),
}

impl<E: std::error::Error + 'static> TransferError<E> {
    pub(super) fn into_job_error(self) -> JobError {
        match self {
            Self::Cancelled => JobError::YieldedForRestart,
            Self::TooLarge(_) => JobError::permanent(describe(&self)),
            _ => JobError::retriable(describe(&self)),
        }
    }
}

pub(super) async fn receive<W, E>(
    mut writer: W,
    mut chunks: impl Stream<Item = Result<bytes::Bytes, E>> + Unpin,
    path: PathBuf,
    limit: u64,
    idle: Duration,
    cancellation: &CancellationToken,
) -> Result<W, TransferError<E>>
where
    W: DownloadWriter,
    E: std::error::Error + 'static,
{
    let mut batch = Vec::with_capacity(super::staging::WRITE_BATCH_BYTES);
    loop {
        let next = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(TransferError::Cancelled),
            next = tokio::time::timeout(idle, chunks.next()) => next.map_err(|_| TransferError::Stalled(idle))?,
        };
        let Some(chunk) = next else { break };
        let chunk = chunk.map_err(TransferError::Source)?;
        if writer
            .written()
            .saturating_add(batch.len() as u64)
            .saturating_add(chunk.len() as u64)
            > limit
        {
            return Err(TransferError::TooLarge(limit));
        }
        batch.extend_from_slice(&chunk);
        if batch.len() >= super::staging::WRITE_BATCH_BYTES {
            writer = write(writer, std::mem::take(&mut batch), path.clone()).await?;
        }
    }
    if !batch.is_empty() {
        writer = write(writer, batch, path).await?;
    }
    Ok(writer)
}

async fn write<W: DownloadWriter, E: std::error::Error + 'static>(
    mut writer: W,
    bytes: Vec<u8>,
    path: PathBuf,
) -> Result<W, TransferError<E>> {
    Ok(tokio::task::spawn_blocking(move || {
        writer.append(&bytes, &path)?;
        Ok::<_, StagingError>(writer)
    })
    .await??)
}

#[cfg(test)]
mod tests;
