//! The worker requests a Native file operation sends, and how their answers
//! read in the public shapes.

use std::sync::Arc;

use pplx_space_common_core::types::SandboxId;
use pplx_space_guest_process::{GuestUserName, IdempotencyKey};
use pplx_space_public_api::{
    FileEventKind, FileInfo, FileModifiedEntry, FileModifiedResponse, FileType,
};
use pplx_space_sbx_mgr_protocol::operations as spaced_ops;
use pplx_space_sbx_mgr_protocol::proto::{
    SpacedDisableFileChangesRequest, SpacedEnableFileChangesRequest, SpacedFileEventKind,
    SpacedFileInfo, SpacedFileType, SpacedGetModifiedFilesRequest, SpacedGetModifiedFilesResponse,
    SpacedListRequest, SpacedMkdirRequest, SpacedReadRequest, SpacedRemoveRequest,
    SpacedRenameRequest, SpacedStatRequest, SpacedWriteBatchEntry, SpacedWriteBatchRequest,
    SpacedWriteOpen, SpacedWriteRequest, spaced_write_request,
};
use time::{Duration as TimeDuration, OffsetDateTime, format_description::well_known::Rfc3339};

use crate::request::FileRequestError;

fn user_name(user: Option<&GuestUserName>) -> Option<String> {
    user.map(|user| user.as_str().to_string())
}

pub fn stat_request(
    sandbox_id: SandboxId,
    path: String,
    user: Option<&GuestUserName>,
) -> SpacedStatRequest {
    SpacedStatRequest {
        sandbox_id: sandbox_id.to_string(),
        operation: Some(spaced_ops::StatRequest {
            path,
            user: user_name(user),
        }),
    }
}

pub fn list_request(
    sandbox_id: SandboxId,
    path: String,
    depth: Option<u32>,
    user: Option<&GuestUserName>,
) -> SpacedListRequest {
    SpacedListRequest {
        sandbox_id: sandbox_id.to_string(),
        operation: Some(spaced_ops::ListRequest {
            path,
            depth,
            user: user_name(user),
        }),
    }
}

pub fn read_request(
    sandbox_id: SandboxId,
    path: String,
    offset: u64,
    length: u64,
    user: Option<&GuestUserName>,
) -> SpacedReadRequest {
    SpacedReadRequest {
        sandbox_id: sandbox_id.to_string(),
        operation: Some(spaced_ops::ReadRequest {
            path,
            offset,
            length: Some(length),
            user: user_name(user),
        }),
    }
}

pub fn spaced_write_requests(
    sandbox_id: SandboxId,
    path: String,
    append: bool,
    idempotency_key: IdempotencyKey,
    chunks: Vec<Vec<u8>>,
    user: Option<GuestUserName>,
) -> Vec<SpacedWriteRequest> {
    let mut requests = Vec::with_capacity(1 + chunks.len());
    requests.push(SpacedWriteRequest {
        sandbox_id: sandbox_id.to_string(),
        operation: Some(spaced_ops::WriteRequest {
            message: Some(spaced_write_request::Message::Open(SpacedWriteOpen {
                path,
                append,
                idempotency_key: Some(idempotency_key.as_str().to_string()),
                user: user.map(|user| user.as_str().to_string()),
            })),
        }),
    });
    requests.extend(
        chunks
            .into_iter()
            .map(|chunk| spaced_write_data(sandbox_id, chunk)),
    );
    requests
}

fn spaced_write_data(sandbox_id: SandboxId, chunk: Vec<u8>) -> SpacedWriteRequest {
    SpacedWriteRequest {
        sandbox_id: sandbox_id.to_string(),
        operation: Some(spaced_ops::WriteRequest {
            message: Some(spaced_write_request::Message::Data(chunk)),
        }),
    }
}

/// [`spaced_write_requests`] over chunks kept for another attempt: each data
/// message copies its chunk only when the stream reaches it.
pub fn shared_spaced_write_requests(
    sandbox_id: SandboxId,
    path: String,
    append: bool,
    idempotency_key: IdempotencyKey,
    chunks: Arc<[Vec<u8>]>,
    user: Option<GuestUserName>,
) -> impl Iterator<Item = SpacedWriteRequest> + Send + 'static {
    let open = spaced_write_requests(sandbox_id, path, append, idempotency_key, Vec::new(), user);
    open.into_iter()
        .chain((0..chunks.len()).filter_map(move |index| {
            chunks
                .get(index)
                .map(|chunk| spaced_write_data(sandbox_id, chunk.clone()))
        }))
}

pub fn write_batch_request(
    sandbox_id: SandboxId,
    entries: Vec<SpacedWriteBatchEntry>,
    idempotency_key: &IdempotencyKey,
    user: Option<&GuestUserName>,
) -> SpacedWriteBatchRequest {
    SpacedWriteBatchRequest {
        sandbox_id: sandbox_id.to_string(),
        operation: Some(spaced_ops::WriteBatchRequest {
            entries,
            idempotency_key: Some(idempotency_key.as_str().to_string()),
            user: user_name(user),
        }),
    }
}

/// The guest mkdir `files:mkdir` issues; a `parents` write creates its
/// directories through the same request, so they match an explicit mkdir.
pub fn spaced_mkdir_request(
    sandbox_id: SandboxId,
    path: String,
    parents: bool,
    user: Option<&GuestUserName>,
) -> SpacedMkdirRequest {
    SpacedMkdirRequest {
        sandbox_id: sandbox_id.to_string(),
        operation: Some(spaced_ops::MkdirRequest {
            path,
            parents,
            exist_ok: true,
            user: user_name(user),
        }),
    }
}

pub fn remove_request(
    sandbox_id: SandboxId,
    path: String,
    recursive: bool,
    user: Option<&GuestUserName>,
) -> SpacedRemoveRequest {
    SpacedRemoveRequest {
        sandbox_id: sandbox_id.to_string(),
        operation: Some(spaced_ops::RemoveRequest {
            path,
            recursive,
            missing_ok: true,
            user: user_name(user),
        }),
    }
}

pub fn rename_request(
    sandbox_id: SandboxId,
    old_path: String,
    new_path: String,
    guest_key: &IdempotencyKey,
    user: Option<&GuestUserName>,
) -> SpacedRenameRequest {
    SpacedRenameRequest {
        sandbox_id: sandbox_id.to_string(),
        operation: Some(spaced_ops::RenameRequest {
            old_path,
            new_path,
            idempotency_key: Some(guest_key.as_str().to_string()),
            user: user_name(user),
        }),
    }
}

/// The guest request for `files:changes_enable`, over roots already checked
/// with [`crate::parse_guest_path`].
pub fn enable_file_changes_request(
    sandbox_id: SandboxId,
    roots: Vec<String>,
    include_globs: Vec<String>,
    exclude_globs: Vec<String>,
    use_default_excludes: Option<bool>,
    user: Option<&GuestUserName>,
) -> SpacedEnableFileChangesRequest {
    SpacedEnableFileChangesRequest {
        sandbox_id: sandbox_id.to_string(),
        operation: Some(spaced_ops::EnableFileChangesRequest {
            roots,
            include_globs,
            exclude_globs,
            use_default_excludes,
            user: user_name(user),
        }),
    }
}

pub fn disable_file_changes_request(
    sandbox_id: SandboxId,
    user: Option<&GuestUserName>,
) -> SpacedDisableFileChangesRequest {
    SpacedDisableFileChangesRequest {
        sandbox_id: sandbox_id.to_string(),
        operation: Some(spaced_ops::DisableFileChangesRequest {
            user: user_name(user),
        }),
    }
}

/// The guest request for `files:modified`, whose filters
/// [`crate::validate_modified_filters`] already checked.
pub fn modified_files_request(
    sandbox_id: SandboxId,
    window_seconds: Option<u64>,
    since: Option<&str>,
    cursor: Option<String>,
    user: Option<&GuestUserName>,
) -> Result<SpacedGetModifiedFilesRequest, FileRequestError> {
    Ok(SpacedGetModifiedFilesRequest {
        sandbox_id: sandbox_id.to_string(),
        operation: Some(spaced_ops::GetModifiedFilesRequest {
            window: window_seconds.map(duration_from_seconds).transpose()?,
            since: since.map(timestamp_from_rfc3339).transpose()?,
            cursor,
            user: user_name(user),
        }),
    })
}

pub fn modified_files_response(response: SpacedGetModifiedFilesResponse) -> FileModifiedResponse {
    let files = response
        .files
        .into_iter()
        .map(|file| FileModifiedEntry {
            path: file.path,
            root: (!file.root.is_empty()).then_some(file.root),
            last_modified_at: file.last_modified_at.and_then(timestamp_to_rfc3339),
            kinds: file.kinds.into_iter().map(public_file_event_kind).collect(),
        })
        .collect();
    FileModifiedResponse {
        files,
        observed_since: response.observed_since.and_then(timestamp_to_rfc3339),
        retained_since: response.retained_since.and_then(timestamp_to_rfc3339),
        cursor: response.cursor,
        monitoring_enabled: response.monitoring_enabled,
        overflowed: response.overflowed,
    }
}

pub fn public_file_info(info: SpacedFileInfo) -> FileInfo {
    FileInfo {
        path: info.path.clone(),
        name: info
            .name
            .unwrap_or_else(|| path_basename(&info.path).to_string()),
        file_type: public_file_type(info.file_type),
        size_bytes: info.size_bytes,
        mode: info.mode,
        modified_at: info.modified_at.and_then(timestamp_to_rfc3339),
        owner_uid: info.owner_uid,
        owner_gid: info.owner_gid,
        symlink_target: info.symlink_target,
        link_count: info.link_count,
    }
}

pub fn public_file_type(file_type: i32) -> FileType {
    match SpacedFileType::try_from(file_type) {
        Ok(SpacedFileType::File) => FileType::File,
        Ok(SpacedFileType::Directory) => FileType::Directory,
        Ok(SpacedFileType::Symlink) => FileType::Symlink,
        Ok(SpacedFileType::Other) | Ok(SpacedFileType::Unspecified) | Err(_) => FileType::Other,
    }
}

pub fn timestamp_to_rfc3339(value: prost_types::Timestamp) -> Option<String> {
    let timestamp = OffsetDateTime::from_unix_timestamp(value.seconds)
        .ok()?
        .checked_add(TimeDuration::nanoseconds(value.nanos.into()))?;
    timestamp
        .format(&Rfc3339)
        .ok()
        .or_else(|| Some(timestamp.unix_timestamp().to_string()))
}

fn timestamp_from_rfc3339(value: &str) -> Result<prost_types::Timestamp, FileRequestError> {
    let timestamp =
        OffsetDateTime::parse(value, &Rfc3339).map_err(|_| FileRequestError::InvalidSince)?;
    Ok(prost_types::Timestamp {
        seconds: timestamp.unix_timestamp(),
        nanos: timestamp.nanosecond() as i32,
    })
}

fn duration_from_seconds(seconds: u64) -> Result<prost_types::Duration, FileRequestError> {
    let seconds = i64::try_from(seconds).map_err(|_| FileRequestError::WindowTooLarge)?;
    Ok(prost_types::Duration { seconds, nanos: 0 })
}

pub fn public_file_event_kind(kind: i32) -> FileEventKind {
    match SpacedFileEventKind::try_from(kind) {
        Ok(SpacedFileEventKind::Created) => FileEventKind::Created,
        Ok(SpacedFileEventKind::Modified) => FileEventKind::Modified,
        Ok(SpacedFileEventKind::Removed) => FileEventKind::Removed,
        Ok(SpacedFileEventKind::Renamed) => FileEventKind::Renamed,
        Ok(SpacedFileEventKind::Chmod) => FileEventKind::Chmod,
        Ok(SpacedFileEventKind::Unspecified) | Err(_) => FileEventKind::Unknown,
    }
}

fn path_basename(path: &str) -> &str {
    path.rsplit('/')
        .find(|segment| !segment.is_empty())
        .unwrap_or(path)
}
