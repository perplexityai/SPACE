use crate::GuestFileChunkStream;
use pplx_space_common_core::types::GuestPath;
use pplx_space_spaced_guest_protocol::proto::bridge_guest::FileChunk;
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
    tasks: tokio_util::task::TaskTracker,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<GuestFileChunkStream, Status> {
    let (file, stream_slot) = open_for_read(path, offset, stream_slot).await?;
    Ok(stream_file(
        file,
        length,
        stream_slot,
        std::future::pending(),
        tasks,
        shutdown,
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

/// Stream at most `length` bytes from the current position, retaining `guard`
/// until physical I/O settles. When `ended` completes, stop scheduling reads and
/// queueing chunks, then await any outstanding blocking read before releasing
/// the file and guard. Buffered chunks arrive before the terminal status;
/// cleanup does not wait for the reader to consume them.
pub(super) fn stream_file(
    file: std::fs::File,
    length: Option<u64>,
    guard: impl Send + 'static,
    ended: impl std::future::Future<Output = Status> + Send + 'static,
    tasks: tokio_util::task::TaskTracker,
    shutdown: tokio_util::sync::CancellationToken,
) -> GuestFileChunkStream {
    use futures_util::StreamExt;
    let mut file = tokio::fs::File::from_std(file);
    // A few chunks keep the stream busy; the HTTP/2 windows downstream
    // already pipeline, and more here is memory per open read.
    let (chunks_tx, chunks_rx) = tokio::sync::mpsc::channel::<FileChunk>(4);
    // The terminal failure travels beside the queue, so that ending the read
    // never waits for room in it.
    let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
    tasks.spawn(async move {
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
                _ = shutdown.cancelled() => break Some(Status::unavailable("file read cancelled")),
                _ = chunks_tx.closed() => break None,
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
                _ = shutdown.cancelled() => break Some(Status::unavailable("file read cancelled")),
                _ = chunks_tx.closed() => break None,
                status = &mut ended => break Some(status),
                sent = chunks_tx.send(chunk) => {
                    if sent.is_err() {
                        break None;
                    }
                }
            }
        };
        // A cancelled async read can still have physical I/O in Tokio's blocking
        // pool. Retain the task and descriptor lease until that I/O has settled.
        drop(file.into_std().await);
        let _ = completion_tx.send(failure.map_or(Ok(()), Err));
    });
    Box::pin(super::terminal::with_completion(
        tokio_stream::wrappers::ReceiverStream::new(chunks_rx).map(Ok),
        completion_rx,
    )) as GuestFileChunkStream
}
