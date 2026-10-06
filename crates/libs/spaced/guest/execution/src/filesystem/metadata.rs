use std::path::Path;
use std::time::SystemTime;

use pplx_space_spaced_guest_protocol::proto::bridge_guest::{FileInfo, FileType};
use tonic::Status;

use super::map_io_error;

pub(super) fn classify_file_type(file_type: std::fs::FileType) -> FileType {
    if file_type.is_file() {
        FileType::File
    } else if file_type.is_dir() {
        FileType::Directory
    } else if file_type.is_symlink() {
        FileType::Symlink
    } else {
        FileType::Other
    }
}

#[cfg(unix)]
pub(super) fn file_mode(metadata: &std::fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode()
}

#[cfg(not(unix))]
pub(super) fn file_mode(_metadata: &std::fs::Metadata) -> u32 {
    0
}

#[cfg(unix)]
pub(super) fn file_owner(metadata: &std::fs::Metadata) -> (u32, u32) {
    use std::os::unix::fs::MetadataExt;
    (metadata.uid(), metadata.gid())
}

#[cfg(not(unix))]
pub(super) fn file_owner(_metadata: &std::fs::Metadata) -> (u32, u32) {
    (0, 0)
}

#[cfg(unix)]
pub(super) fn file_link_count(metadata: &std::fs::Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    Some(metadata.nlink())
}

#[cfg(not(unix))]
pub(super) fn file_link_count(_metadata: &std::fs::Metadata) -> Option<u64> {
    None
}

pub(super) fn path_to_string(path: &Path) -> Result<String, Status> {
    path.to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| Status::invalid_argument("path is not valid UTF-8"))
}

pub(super) async fn symlink_target(
    path: &Path,
    metadata: &std::fs::Metadata,
) -> Result<Option<String>, Status> {
    if metadata.file_type().is_symlink() {
        let Some(target) = tokio::fs::read_link(path)
            .await
            .ok()
            .map(|target| path_to_string(&target).map(Some))
        else {
            return Ok(None);
        };
        target
    } else {
        Ok(None)
    }
}

pub(super) async fn file_info_from_path(path: &Path) -> Result<FileInfo, Status> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(map_io_error)?;
    let (owner_uid, owner_gid) = file_owner(&metadata);
    let name = path
        .file_name()
        .map(|name| {
            name.to_str()
                .map(ToOwned::to_owned)
                .ok_or_else(|| Status::invalid_argument("entry name is not valid UTF-8"))
        })
        .transpose()?
        .ok_or_else(|| Status::invalid_argument("entry has no file name"))?;
    let symlink_target = symlink_target(path, &metadata).await?;
    Ok(FileInfo {
        name: Some(name),
        path: path_to_string(path)?,
        file_type: classify_file_type(metadata.file_type()).into(),
        size_bytes: metadata.len(),
        mode: file_mode(&metadata),
        modified_at: metadata.modified().ok().and_then(system_time_to_timestamp),
        owner_uid,
        owner_gid,
        symlink_target,
        // From the `symlink_metadata` above, so no extra stat.
        link_count: file_link_count(&metadata),
    })
}

pub(super) fn system_time_to_timestamp(time: SystemTime) -> Option<prost_types::Timestamp> {
    pplx_space_spaced_guest_protocol::system_time_to_proto(time).ok()
}

#[cfg(test)]
mod tests;
