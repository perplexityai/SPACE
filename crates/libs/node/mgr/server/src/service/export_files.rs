//! Read retained export artifacts at their privileged owner without widening access.
use std::path::Path;

use nix::fcntl::AtFlags;
use nix::sys::stat::{SFlag, fstatat};
use tonic::{Request, Response, Status};

use super::{ALLOW_NODE_ADMIN_OR_SBXMGR, NodeManagerService, artifact_fs, trusted_paths};
use crate::proto::{
    ListSandboxExportFilesRequest, ListSandboxExportFilesResponse, SandboxExportFile,
};

const MAX_EXPORT_ENTRIES: usize = 256;

impl NodeManagerService {
    pub(super) async fn list_sandbox_export_files(
        &self,
        request: Request<ListSandboxExportFilesRequest>,
    ) -> Result<Response<ListSandboxExportFilesResponse>, Status> {
        self.authorize_peer(
            &request,
            "ListSandboxExportFiles",
            ALLOW_NODE_ADMIN_OR_SBXMGR,
        )?;
        let req = request.into_inner();
        let id = trusted_paths::canonical_sandbox_id(&req.sandbox_id)?;
        let root = Path::new(super::CACHE_DIR).join(id.to_string());
        self.off_runtime("list sandbox export files", move || {
            let files = inventory(&root, req.include_snapshot)?;
            Ok(Response::new(ListSandboxExportFilesResponse { files }))
        })
        .await
    }
}

fn inventory(root: &Path, include_snapshot: bool) -> Result<Vec<SandboxExportFile>, Status> {
    let root = trusted_paths::open_directory(root)?;
    let mut files = Vec::new();
    let mut remaining = MAX_EXPORT_ENTRIES;
    for (directory, prefix) in [("instance", ""), ("snapshot", "snapshot/")] {
        if directory == "snapshot" && !include_snapshot {
            continue;
        }
        let directory =
            artifact_fs::directory(&root, directory.as_ref()).map_err(artifact_fs::error)?;
        let mut entries = nix::dir::Dir::openat(
            &directory,
            ".",
            artifact_fs::DIRECTORY,
            nix::sys::stat::Mode::empty(),
        )
        .map_err(artifact_fs::error)?;
        for entry in entries.iter() {
            let entry = entry.map_err(artifact_fs::error)?;
            let name = entry.file_name();
            if matches!(name.to_bytes(), b"." | b"..") {
                continue;
            }
            remaining = remaining
                .checked_sub(1)
                .ok_or_else(|| Status::resource_exhausted("too many entries in export"))?;
            let metadata = fstatat(&directory, name, AtFlags::AT_SYMLINK_NOFOLLOW)
                .map_err(artifact_fs::error)?;
            match SFlag::from_bits_truncate(metadata.st_mode) & SFlag::S_IFMT {
                SFlag::S_IFREG => {
                    let name = name.to_str().map_err(artifact_fs::error)?;
                    let size_bytes = u64::try_from(metadata.st_size).map_err(artifact_fs::error)?;
                    files.push(SandboxExportFile {
                        name: format!("{prefix}{name}"),
                        size_bytes,
                    });
                }
                SFlag::S_IFLNK => {
                    return Err(Status::failed_precondition("export contains a symlink"));
                }
                _ => {}
            }
        }
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(files)
}

type ExportFileStream = std::pin::Pin<
    Box<
        dyn tokio_stream::Stream<Item = Result<crate::proto::SandboxExportFileChunk, Status>>
            + Send,
    >,
>;

fn open_export_file(root: &Path, name: &str) -> Result<std::fs::File, Status> {
    let (directory, name) = match name.strip_prefix("snapshot/") {
        Some(name) => ("snapshot", name),
        None => ("instance", name),
    };
    if name.is_empty() || name.contains('/') || name.contains('\\') || matches!(name, "." | "..") {
        return Err(Status::invalid_argument("expected an export file name"));
    }
    let root = trusted_paths::open_directory(root)?;
    let directory =
        artifact_fs::directory(&root, directory.as_ref()).map_err(artifact_fs::error)?;
    trusted_paths::open_regular_file(&directory, name.as_ref())
}

impl NodeManagerService {
    pub(super) async fn read_sandbox_export_file(
        &self,
        request: Request<crate::proto::ReadSandboxExportFileRequest>,
    ) -> Result<Response<ExportFileStream>, Status> {
        use tokio_stream::StreamExt;
        self.authorize_peer(
            &request,
            "ReadSandboxExportFile",
            ALLOW_NODE_ADMIN_OR_SBXMGR,
        )?;
        let req = request.into_inner();
        let id = trusted_paths::canonical_sandbox_id(&req.sandbox_id)?;
        let root = Path::new(super::CACHE_DIR).join(id.to_string());
        // Share the bounded bulk-reader budget, never the lifecycle mutation lane.
        let permit = self
            .send_source_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("export readers busy; retry"))?;
        let file = self
            .off_runtime("open export file", move || {
                open_export_file(&root, &req.name)
            })
            .await?;
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let cancel = self.pool.shutdown_token.child_token();
        let producer = self.device_op_tasks.spawn_blocking(move || {
            let _permit = permit;
            stream_file(
                file,
                || cancel.is_cancelled() || tx.is_closed(),
                |chunk| {
                    tokio::runtime::Handle::current().block_on(async {
                        tokio::time::timeout(std::time::Duration::from_secs(60), tx.send(chunk))
                            .await
                            .map_err(|_| Status::deadline_exceeded("export reader stalled"))?
                            .map_err(|_| Status::cancelled("export reader disconnected"))
                    })
                },
            )
        });
        // A failed or panicking producer must not turn a truncated export into OK EOF.
        let outcome = futures_util::stream::once(producer).filter_map(|result| match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(Err(error)),
            Err(error) => Some(Err(Status::internal(format!(
                "export reader failed: {error}"
            )))),
        });
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx)
            .map(Ok)
            .chain(outcome);
        Ok(Response::new(Box::pin(stream)))
    }
}

fn stream_file(
    mut file: std::fs::File,
    cancelled: impl Fn() -> bool,
    mut send: impl FnMut(crate::proto::SandboxExportFileChunk) -> Result<(), Status>,
) -> Result<(), Status> {
    use std::io::Read;
    let length = file.metadata().map_err(artifact_fs::error)?.len();
    let mut offset = 0_u64;
    let mut buffer = vec![0; 512 * 1024];
    loop {
        if cancelled() {
            return Err(Status::cancelled("export reader cancelled"));
        }
        let count = file.read(&mut buffer).map_err(artifact_fs::error)?;
        if count == 0 {
            if offset != length {
                return Err(Status::data_loss("export file changed during transfer"));
            }
            return send(crate::proto::SandboxExportFileChunk {
                offset,
                data: Vec::new(),
                eof: true,
            });
        }
        let data = buffer
            .get(..count)
            .ok_or_else(|| Status::internal("export read exceeded buffer"))?;
        if data.iter().any(|byte| *byte != 0) {
            send(crate::proto::SandboxExportFileChunk {
                offset,
                data: data.to_vec(),
                eof: false,
            })?;
        }
        offset = offset
            .checked_add(count as u64)
            .ok_or_else(|| Status::out_of_range("export file size overflow"))?;
    }
}

#[cfg(test)]
mod tests;
