use crate::GuestFileChunkStream;
use pplx_space_common_core::types::GuestPath;
use pplx_space_spaced_common::proto::bridge_guest::FileChunk;
use tokio::io::AsyncReadExt;
use tokio::sync::OwnedSemaphorePermit;
use tonic::Status;

use super::map_io_error;

const FILE_CHUNK_SIZE: usize = 64 * 1024;

#[cfg(all(test, unix))]
mod tests;

pub(super) async fn read_stream(
    path: GuestPath,
    offset: u64,
    length: Option<u64>,
    stream_slot: OwnedSemaphorePermit,
) -> Result<GuestFileChunkStream, Status> {
    let (file, stream_slot) = open_for_read(path, offset, stream_slot).await?;
    Ok(stream_file(
        file,
        length,
        stream_slot,
        std::future::pending(),
    ))
}

/// Open `path` for reading at `offset`, as the calling process's identity.
/// `guard` (the read's admission) travels with the opening work, so a caller
/// cancelled meanwhile still holds it until that work ends, and comes back
/// with the file.
pub(super) async fn open_for_read<G: Send + 'static>(
    path: GuestPath,
    offset: u64,
    guard: G,
) -> Result<(std::fs::File, G), Status> {
    // Read requires seekability. Open nonblocking so a FIFO fails that check
    // without waiting for a writer; accepted descriptors retain blocking I/O.
    tokio::task::spawn_blocking(move || -> std::io::Result<_> {
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NONBLOCK);
        }
        let mut file = options.open(path.expose())?;
        std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(offset))?;
        #[cfg(unix)]
        nix::fcntl::fcntl(
            &file,
            nix::fcntl::FcntlArg::F_SETFL(nix::fcntl::OFlag::empty()),
        )?;
        Ok((file, guard))
    })
    .await
    .map_err(|error| Status::internal(format!("file read opening task failed: {error}")))?
    .map_err(map_io_error)
}

/// Stream `file` from its current position, at most `length` bytes, holding
/// `guard` (its admission, or whatever keeps the descriptor's lease) until
/// the stream ends.
/// Stream `file` in chunks while holding `guard`. If `ended` completes
/// first, the read stops at once and releases the file and `guard`, whether
/// or not the reader is still taking chunks; the stream then yields the chunks
/// already queued, followed by `ended`'s status.
pub(super) fn stream_file(
    file: std::fs::File,
    length: Option<u64>,
    guard: impl Send + 'static,
    ended: impl std::future::Future<Output = Status> + Send + 'static,
) -> GuestFileChunkStream {
    use futures_util::StreamExt;
    let mut file = tokio::fs::File::from_std(file);
    // A few chunks keep the stream busy; the HTTP/2 windows downstream
    // already pipeline, and more here is memory per open read.
    let (chunks_tx, chunks_rx) = tokio::sync::mpsc::channel::<FileChunk>(4);
    // The terminal failure travels beside the queue, so that ending the read
    // never waits for room in it.
    let (failure_tx, failure_rx) = tokio::sync::oneshot::channel::<Status>();
    tokio::spawn(async move {
        let _guard = guard;
        let mut ended = std::pin::pin!(ended);
        let mut remaining = length;
        let failure = loop {
            let read_len = match remaining {
                Some(0) => break None,
                Some(value) => usize::try_from(value)
                    .map_or(FILE_CHUNK_SIZE, |value| FILE_CHUNK_SIZE.min(value)),
                None => FILE_CHUNK_SIZE,
            };
            // Read into the chunk's own buffer, so sending it needs no copy.
            // `take` keeps the read within `read_len` even if the allocation
            // has more spare capacity than was asked for.
            let mut data = Vec::with_capacity(read_len);
            let mut limited = (&mut file).take(read_len as u64);
            let read = tokio::select! {
                biased;
                status = &mut ended => break Some(status),
                read = limited.read_buf(&mut data) => read,
            };
            match read {
                Ok(0) => break None,
                Ok(bytes_read) => {
                    if let Some(value) = remaining.as_mut() {
                        *value = value.saturating_sub(bytes_read as u64);
                    }
                }
                Err(error) => break Some(map_io_error(error)),
            }
            let chunk = FileChunk { data: data.into() };
            tokio::select! {
                biased;
                status = &mut ended => break Some(status),
                sent = chunks_tx.send(chunk) => {
                    if sent.is_err() {
                        break None;
                    }
                }
            }
        };
        if let Some(status) = failure {
            let _ = failure_tx.send(status);
        }
    });
    let failure = futures_util::stream::once(failure_rx)
        .filter_map(|failure| std::future::ready(failure.ok().map(Err)));
    Box::pin(
        tokio_stream::wrappers::ReceiverStream::new(chunks_rx)
            .map(Ok)
            .chain(failure),
    ) as GuestFileChunkStream
}
