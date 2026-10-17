use futures_util::{Stream, StreamExt};
use std::io;
use std::sync::Arc;

use crate::guest_path;
use crate::identity::GuestAccount;
use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
    WriteBatchRequest, WriteBatchResponse, WriteResponse,
};
use pplx_space_spaced_guest_protocol::proto::worker::{WriteFrame, write_frame};
use tokio::io::AsyncWriteExt;
use tonic::Status;

use super::map_io_error;

mod replace;
mod upload;

use replace::{WriteTarget, open_write_target};
pub(super) use upload::UploadBufferPool;
use upload::{UploadWriter, blocking};

trait DurableFile {
    async fn flush(&mut self) -> io::Result<()>;

    async fn sync_all(&self) -> io::Result<()>;
}

impl DurableFile for tokio::fs::File {
    async fn flush(&mut self) -> io::Result<()> {
        tokio::io::AsyncWriteExt::flush(self).await
    }

    async fn sync_all(&self) -> io::Result<()> {
        tokio::fs::File::sync_all(self).await
    }
}

impl DurableFile for std::fs::File {
    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    async fn sync_all(&self) -> io::Result<()> {
        let file = self.try_clone()?;
        blocking(move || file.sync_all()).await
    }
}

/// Complete a written file's writes before the write is acknowledged. A file
/// written in place is also synced: nothing publishes it atomically, so the
/// sync is what reports a failed writeback to the writer and puts the bytes
/// on the virtual disk before the reply. A replacement is not synced. Its
/// writes are complete (direct, or in the page cache) before the rename
/// publishes it atomically; a sync would make it persistent, and report a
/// failed writeback, before the reply instead of at the filesystem's next
/// writeback or a lifecycle flush. A sync forces a journal commit, unless a
/// concurrent one already started it, and each journal block is its own disk
/// request, so for a small upload it costs several times the requests of the
/// data itself against the drive's operations limit.
async fn finalize_written_file(file: &mut impl DurableFile, replacing: bool) -> Result<(), Status> {
    file.flush().await.map_err(map_io_error)?;
    if !replacing {
        file.sync_all().await.map_err(map_io_error)?;
    }
    Ok(())
}

fn effective_uid() -> u32 {
    nix::unistd::geteuid().as_raw()
}

#[derive(Debug, thiserror::Error)]
pub(super) enum WriteError {
    #[error("write input interrupted")]
    Interrupted,
    #[error(transparent)]
    Failed(#[from] Status),
}

/// `buffers` are the calling filesystem service's upload buffers.
pub(super) async fn write(
    mut stream: impl Stream<Item = Result<WriteFrame, Status>> + Unpin,
    account: &GuestAccount,
    owner: Option<(u32, u32)>,
    cancelled: tokio_util::sync::CancellationToken,
    buffers: Arc<UploadBufferPool>,
) -> Result<WriteResponse, WriteError> {
    let first = stream
        .next()
        .await
        .transpose()?
        .ok_or(WriteError::Interrupted)?;
    let Some(write_frame::Frame::Open(open)) = first.frame else {
        return Err(Status::invalid_argument("worker write requires open").into());
    };
    uuid::Uuid::parse_str(&open.execution_id)
        .map_err(|_| Status::invalid_argument("invalid execution id"))?;
    let path = guest_path::normalize_for_account(&open.path, account)?;
    let owner_uid = owner.map_or_else(effective_uid, |(uid, _)| uid);
    let WriteTarget { file, replacement } =
        open_write_target(path.expose().into(), open.append, owner_uid).await?;
    let mut file = UploadWriter::new(file, open.append, buffers)
        .await
        .map_err(map_io_error)?;
    let mut bytes_written = 0u64;
    let transferred = async {
        loop {
            let message = tokio::select! {
                biased;
                _ = cancelled.cancelled() => return Err(WriteError::Interrupted),
                message = stream.next() => message.transpose()?.ok_or(WriteError::Interrupted)?,
            };
            match message.frame {
                Some(write_frame::Frame::Data(data)) => {
                    file.write(&data).await.map_err(map_io_error)?;
                    bytes_written = bytes_written.saturating_add(data.len() as u64);
                }
                Some(write_frame::Frame::Finish(_)) => return Ok(()),
                Some(write_frame::Frame::Abort(_)) => return Err(WriteError::Interrupted),
                _ => return Err(Status::invalid_argument("unexpected worker write frame").into()),
            }
        }
    }
    .await;
    // Settle the upload before releasing the execution's file and admission
    // permit. An interrupted write in place keeps what was received, as it
    // always has; an interrupted replacement's temporary file is discarded.
    let finished = match (&transferred, &replacement) {
        (Err(_), Some(_)) => Ok(file.discard()),
        _ => file.finish().await.map_err(map_io_error),
    };
    if let Err(error) = transferred {
        drop(finished);
        if let Some(replacement) = replacement {
            replacement.discard().await;
        }
        return Err(error);
    }
    let mut file = finished?;
    finalize_written_file(&mut file, replacement.is_some()).await?;
    if let Some((uid, gid)) = owner {
        super::ownership::chown_open_file(&file, uid, gid)?;
    }
    drop(file);
    if let Some(replacement) = replacement {
        replacement.commit().await?;
    }
    Ok(WriteResponse { bytes_written })
}

pub(super) async fn write_batch(
    request: WriteBatchRequest,
    account: &GuestAccount,
) -> Result<WriteBatchResponse, Status> {
    if request.entries.len() > pplx_space_common_core::spaced::MAX_WRITE_BATCH_ENTRIES {
        return Err(Status::invalid_argument("too many write_batch entries"));
    }
    let mut files_written = 0;
    for entry in request.entries {
        let path = guest_path::normalize_for_account(&entry.path, account)?;
        let WriteTarget {
            mut file,
            replacement,
        } = open_write_target(path.expose().into(), entry.append, effective_uid()).await?;
        let written = file.write_all(&entry.data).await.map_err(map_io_error);
        let flushed = AsyncWriteExt::flush(&mut file).await.map_err(map_io_error);
        written?;
        flushed?;
        finalize_written_file(&mut file, replacement.is_some()).await?;
        drop(file);
        if let Some(replacement) = replacement {
            replacement.commit().await?;
        }
        files_written += 1;
    }
    Ok(WriteBatchResponse { files_written })
}

#[cfg(test)]
mod tests;
