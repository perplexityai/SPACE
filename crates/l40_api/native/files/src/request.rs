//! Checks on a Native file request's fields, before the sandbox sees it. Each
//! error's text is the `invalid_request` message the client gets.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use pplx_space_common_core::spaced::MAX_WRITE_BATCH_ENTRIES;
use pplx_space_guest_process::{
    GuestUserName, PathParseError, ProcessParseError, validate_linux_guest_path_input,
};
use pplx_space_public_api::{
    FileChangeTrackingRequest, FileModifiedRequest, FileWriteBatchEntry, FileWriteBatchRequest,
};
use pplx_space_sbx_mgr_protocol::proto::SpacedWriteBatchEntry;

use crate::limits::{
    DEFAULT_FILE_LIST_DEPTH, FILE_WRITE_CHUNK_BASE64_CHARS, MAX_FILE_LIST_ENTRIES,
    MAX_FILE_READ_BYTES, MAX_FILE_WRITE_BATCH_BYTES, MAX_FILE_WRITE_BYTES,
};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FileRequestError {
    #[error("{0}")]
    Path(#[source] PathParseError),
    #[error("{0}")]
    User(#[source] ProcessParseError),
    #[error("data_base64 must be valid base64")]
    InvalidBase64,
    #[error("file write payload is too large")]
    PayloadTooLarge,
    #[error("entries has {0} items, max is {MAX_WRITE_BATCH_ENTRIES}")]
    TooManyBatchEntries(usize),
    // A batch that met a missing directory is written again once it is
    // created, so the entries that landed first would append twice.
    #[error("parents cannot be combined with append entries")]
    ParentsWithAppend,
    #[error(
        "file write batch is too large: data and paths exceed {MAX_FILE_WRITE_BATCH_BYTES} bytes"
    )]
    BatchTooLarge,
    #[error("depth must be at least 1")]
    ZeroListDepth,
    #[error("file list returned too many entries; use a smaller depth")]
    TooManyListEntries,
    #[error("file read length must be at least 1 byte")]
    ZeroReadLength,
    #[error("file read length exceeds the control-plane API limit")]
    ReadLengthTooLarge,
    #[error("window_seconds, since, and cursor are mutually exclusive")]
    ConflictingModifiedFilters,
    #[error("since must be an RFC3339 timestamp")]
    InvalidSince,
    #[error("window_seconds is too large")]
    WindowTooLarge,
    #[error("file change tracking enable request must set enabled=true")]
    TrackingNotEnabled,
}

pub fn parse_guest_path(path: String) -> Result<String, FileRequestError> {
    validate_linux_guest_path_input(&path).map_err(FileRequestError::Path)?;
    Ok(path)
}

pub fn parse_guest_user(user: Option<String>) -> Result<Option<GuestUserName>, FileRequestError> {
    user.map(GuestUserName::parse)
        .transpose()
        .map_err(FileRequestError::User)
}

/// The depth a `files:list` asks for, at least 1.
pub fn list_depth(depth: Option<u32>) -> Result<u32, FileRequestError> {
    match depth.unwrap_or(DEFAULT_FILE_LIST_DEPTH) {
        0 => Err(FileRequestError::ZeroListDepth),
        depth => Ok(depth),
    }
}

/// Whether a `files:list` answer may take another entry, having taken
/// `listed`.
pub fn admit_list_entry(listed: usize) -> Result<(), FileRequestError> {
    if listed >= MAX_FILE_LIST_ENTRIES {
        return Err(FileRequestError::TooManyListEntries);
    }
    Ok(())
}

pub fn decode_write_payload(data_base64: &str) -> Result<Vec<u8>, FileRequestError> {
    let data = BASE64_STANDARD
        .decode(data_base64)
        .map_err(|_| FileRequestError::InvalidBase64)?;
    if data.len() > MAX_FILE_WRITE_BYTES {
        return Err(FileRequestError::PayloadTooLarge);
    }
    Ok(data)
}

/// Decode a `files:write` payload directly into its data messages, without a
/// buffer for the whole payload or a copy per message.
pub fn decode_write_chunks(data_base64: &str) -> Result<Vec<Vec<u8>>, FileRequestError> {
    if data_base64.len() / 4 * 3 > MAX_FILE_WRITE_BYTES + 3 {
        return Err(FileRequestError::PayloadTooLarge);
    }
    let segments = data_base64.as_bytes().chunks(FILE_WRITE_CHUNK_BASE64_CHARS);
    let last = segments.len().saturating_sub(1);
    let mut chunks = Vec::with_capacity(segments.len());
    let mut total = 0usize;
    for (index, segment) in segments.enumerate() {
        // Padding may only end the payload, as when it is decoded whole.
        if index != last && segment.last() == Some(&b'=') {
            return Err(FileRequestError::InvalidBase64);
        }
        let chunk = BASE64_STANDARD
            .decode(segment)
            .map_err(|_| FileRequestError::InvalidBase64)?;
        total += chunk.len();
        chunks.push(chunk);
    }
    if total > MAX_FILE_WRITE_BYTES {
        return Err(FileRequestError::PayloadTooLarge);
    }
    Ok(chunks)
}

/// The checks on a `files:write_batch` that need none of its entries decoded.
pub fn validate_write_batch_shape(request: &FileWriteBatchRequest) -> Result<(), FileRequestError> {
    if request.entries.len() > MAX_WRITE_BATCH_ENTRIES {
        return Err(FileRequestError::TooManyBatchEntries(request.entries.len()));
    }
    if request.parents && request.entries.iter().any(|entry| entry.append) {
        return Err(FileRequestError::ParentsWithAppend);
    }
    Ok(())
}

/// A batch's entries, decoded, within [`MAX_FILE_WRITE_BATCH_BYTES`] of data
/// and paths together.
pub fn decode_write_batch_entries(
    entries: Vec<FileWriteBatchEntry>,
) -> Result<Vec<SpacedWriteBatchEntry>, FileRequestError> {
    let mut decoded = Vec::with_capacity(entries.len());
    let mut batch_bytes = 0usize;
    for entry in entries {
        let path = parse_guest_path(entry.path)?;
        let data = decode_write_payload(&entry.data_base64)?;
        batch_bytes = batch_bytes
            .saturating_add(data.len())
            .saturating_add(path.len());
        if batch_bytes > MAX_FILE_WRITE_BATCH_BYTES {
            return Err(FileRequestError::BatchTooLarge);
        }
        decoded.push(SpacedWriteBatchEntry {
            path,
            data,
            append: entry.append,
        });
    }
    Ok(decoded)
}

pub fn validate_read_length(length: u64) -> Result<(), FileRequestError> {
    if length == 0 {
        return Err(FileRequestError::ZeroReadLength);
    }
    if length > MAX_FILE_READ_BYTES {
        return Err(FileRequestError::ReadLengthTooLarge);
    }
    Ok(())
}

pub fn validate_modified_filters(request: &FileModifiedRequest) -> Result<(), FileRequestError> {
    let filters = [
        request.window_seconds.is_some(),
        request.since.is_some(),
        request.cursor.is_some(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();
    if filters > 1 {
        return Err(FileRequestError::ConflictingModifiedFilters);
    }
    Ok(())
}

/// A `files:changes_enable` must ask for tracking on.
pub fn validate_tracking_enabled(
    request: &FileChangeTrackingRequest,
) -> Result<(), FileRequestError> {
    if !request.enabled {
        return Err(FileRequestError::TrackingNotEnabled);
    }
    Ok(())
}
