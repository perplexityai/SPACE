use std::path::PathBuf;

use crate::GuestFileInfoStream;
use futures_util::StreamExt;
use pplx_space_common_core::types::GuestPath;
use pplx_space_spaced_guest_protocol::proto::bridge_guest::FileType;
use tokio::sync::OwnedSemaphorePermit;
use tonic::Status;

use super::map_io_error;
use super::metadata::file_info_from_path;

const MAX_LIST_DIRECTORIES: usize = 16_384;

fn push_pending_directory(
    pending: &mut Vec<(PathBuf, u32)>,
    directories_enqueued: &mut usize,
    path: PathBuf,
    depth: u32,
) -> Result<(), Status> {
    if *directories_enqueued >= MAX_LIST_DIRECTORIES {
        return Err(Status::resource_exhausted(
            "filesystem list directory budget exceeded",
        ));
    }
    *directories_enqueued += 1;
    pending.push((path, depth));
    Ok(())
}

pub(super) async fn list_stream(
    path: GuestPath,
    depth: Option<u32>,
    stream_slot: OwnedSemaphorePermit,
    tasks: tokio_util::task::TaskTracker,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<GuestFileInfoStream, Status> {
    let root = PathBuf::from(path.expose());
    tokio::fs::symlink_metadata(&root)
        .await
        .map_err(map_io_error)?;
    let max_depth = match depth {
        Some(0) => return Err(Status::invalid_argument("depth must be greater than 0")),
        Some(depth) => depth,
        None => 1,
    };
    let mut pending = vec![(root, 1_u32)];
    let mut directories_enqueued = 1_usize;
    let (tx, rx) = tokio::sync::mpsc::channel(32);
    // A separate terminal status cannot be lost behind a full entry queue.
    let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
    tasks.spawn(async move {
        let _stream_slot = stream_slot;
        let result: Result<(), Status> = async {
            while let Some((directory, depth)) = pending.pop() {
                if shutdown.is_cancelled() {
                    return Err(Status::unavailable("file list cancelled"));
                }
                if tx.is_closed() {
                    return Ok(());
                }
                let mut read_dir = tokio::fs::read_dir(&directory).await.map_err(map_io_error)?;
                loop {
                    if shutdown.is_cancelled() {
                        return Err(Status::unavailable("file list cancelled"));
                    }
                    if tx.is_closed() {
                        return Ok(());
                    }
                    let Some(entry) = read_dir.next_entry().await.map_err(map_io_error)? else {
                        break;
                    };
                    let entry_path = entry.path();
                    let entry = file_info_from_path(&entry_path).await?;
                    if depth < max_depth && entry.file_type() == FileType::Directory {
                        push_pending_directory(
                            &mut pending,
                            &mut directories_enqueued,
                            entry_path,
                            depth + 1,
                        )?;
                    }
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => return Err(Status::unavailable("file list cancelled")),
                        result = tx.send(entry) => if result.is_err() { return Ok(()); },
                    }
                }
            }
            Ok(())
        }.await;
        let _ = completion_tx.send(result);
    });
    Ok(Box::pin(super::terminal::with_completion(
        tokio_stream::wrappers::ReceiverStream::new(rx).map(Ok),
        completion_rx,
    )) as GuestFileInfoStream)
}

#[cfg(test)]
mod tests;
