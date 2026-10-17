use std::path::PathBuf;

use crate::GuestFileInfoStream;
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
    tokio::spawn(async move {
        let _stream_slot = stream_slot;
        while let Some((directory, depth)) = pending.pop() {
            let mut read_dir = match tokio::fs::read_dir(&directory).await {
                Ok(read_dir) => read_dir,
                Err(error) => {
                    let _ = tx.send(Err(map_io_error(error))).await;
                    return;
                }
            };
            loop {
                match read_dir.next_entry().await {
                    Ok(Some(entry)) => {
                        let entry_path = entry.path();
                        let entry = match file_info_from_path(&entry_path).await {
                            Ok(entry) => entry,
                            Err(error) => {
                                let _ = tx.send(Err(error)).await;
                                return;
                            }
                        };
                        if depth < max_depth
                            && entry.file_type() == FileType::Directory
                            && let Err(status) = push_pending_directory(
                                &mut pending,
                                &mut directories_enqueued,
                                entry_path,
                                depth + 1,
                            )
                        {
                            let _ = tx.send(Err(status)).await;
                            return;
                        }
                        if tx.send(Ok(entry)).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => break,
                    Err(error) => {
                        let _ = tx.send(Err(map_io_error(error))).await;
                        return;
                    }
                }
            }
        }
    });
    Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)) as GuestFileInfoStream)
}

#[cfg(test)]
mod tests;
