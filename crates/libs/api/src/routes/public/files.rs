use pplx_space_common_core::types::{OrgId, SandboxId};

use std::sync::Arc;

use axum::{
    Extension, Json,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use futures_util::{StreamExt, stream};
use pplx_space_common_core::spaced::{MAX_MESSAGE_BYTES, MAX_WRITE_BATCH_ENTRIES};
use pplx_space_common_core::types::{
    GuestUserName, IdempotencyKey, validate_linux_guest_path_input,
};
use pplx_space_metrics::response::{ResponseKind, ResponseOrigin};
use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_public_api::routes::{
    CREATE_DIRECTORY, REMOVE_FILE, RENAME_FILE, WRITE_FILE, WRITE_FILE_BATCH,
};
use pplx_space_public_api::{
    FileChangeTrackingRequest, FileChangeTrackingResponse, FileEventKind, FileInfo,
    FileListRequest, FileListResponse, FileMkdirRequest, FileModifiedEntry, FileModifiedRequest,
    FileModifiedResponse, FileMutationResponse, FileReadRequest, FileRemoveRequest,
    FileRenameRequest, FileStatRequest, FileStatResponse, FileType, FileWriteBatchRequest,
    FileWriteBatchResponse, FileWriteRequest, FileWriteResponse,
};
use pplx_space_sbx_mgr_client::operations as spaced_ops;
use pplx_space_sbx_mgr_client::proto::{
    SpacedDisableFileChangesRequest, SpacedEnableFileChangesRequest, SpacedFileEventKind,
    SpacedFileInfo, SpacedFileType, SpacedGetModifiedFilesRequest, SpacedMkdirRequest,
    SpacedReadRequest, SpacedRemoveRequest, SpacedRenameRequest, SpacedWriteBatchEntry,
    SpacedWriteBatchRequest, SpacedWriteOpen, SpacedWriteRequest, spaced_write_request,
};
use time::{Duration as TimeDuration, OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    controllers::ControllerError, middleware::auth::AuthOrg, models::Sandbox, routes::AppState,
};

use super::{
    error::{PublicApiError, with_retry_after_hint},
    owner_scope::{AuthorizedSandbox, authorized_sandbox},
    sandboxes::{idempotency_key, parse_public_sandbox_id},
};

const MAX_FILE_LIST_ENTRIES: usize = 10_000;
const MAX_FILE_WRITE_BYTES: usize = 16 * 1024 * 1024;
const MAX_FILE_READ_BYTES: u64 = 16 * 1024 * 1024;
// Raw bytes per data message. A multiple of 3, so each message decodes from
// whole base64 quanta straight into its own buffer.
const FILE_WRITE_CHUNK_BYTES: usize = 1024 * 1024 - 1;
const FILE_WRITE_CHUNK_BASE64_CHARS: usize = FILE_WRITE_CHUNK_BYTES / 3 * 4;
/// The request body limit for `files:write`: the base64 of the largest payload
/// it accepts, plus 64 KiB for the path and the other fields, plus 1/32 more
/// for encoders that escape `/` as `\/`. Unescaped JSON of any accepted
/// payload fits. Escaped JSON fits while at most one base64 character in 32
/// is a `/`, twice the rate in uniformly distributed data; a payload near the
/// limit whose encoding is mostly `/` (long runs of 0xff bytes) can exceed it,
/// and such a client must send it unescaped or split it. Without this limit
/// the framework's 2 MiB default would cap a single write at about 1.5 MiB.
/// Writes stream downstream in `FILE_WRITE_CHUNK_BYTES` messages, so no hop
/// below this needs a larger limit. `files:write_batch` is sized separately
/// below: it is one gRPC message, which every hop caps at 4 MiB.
const MAX_FILE_WRITE_BASE64_CHARS: usize = MAX_FILE_WRITE_BYTES.div_ceil(3) * 4;
pub(crate) const WRITE_FILE_BODY_LIMIT_BYTES: usize =
    MAX_FILE_WRITE_BASE64_CHARS + MAX_FILE_WRITE_BASE64_CHARS / 32 + 64 * 1024;
/// Total decoded data plus path bytes accepted by one `files:write_batch`.
///
/// Unlike `files:write`, a batch is forwarded as a single unary gRPC message,
/// and the hops behind this API decode at most 4 MiB per message. Capping the
/// batch at 3 MiB leaves room for protobuf framing and request metadata, so an
/// accepted batch is not rejected downstream for size. It also bounds API
/// memory per batch request to a few copies of this cap.
pub(crate) const MAX_FILE_WRITE_BATCH_BYTES: usize = 3 * 1024 * 1024;
// A batch must fit one 4 MiB downstream message with room for framing.
const _: () = assert!(MAX_FILE_WRITE_BATCH_BYTES <= MAX_MESSAGE_BYTES - 512 * 1024);
/// Per-entry JSON allowance in a batch: one path escaped at worst two bytes per
/// input byte (quote and backslash) plus field names and punctuation. Paths
/// count toward `MAX_FILE_WRITE_BATCH_BYTES`, so heavier escaping only matters
/// for paths far shorter than the limit.
const FILE_WRITE_BATCH_ENTRY_JSON_OVERHEAD_BYTES: usize =
    2 * pplx_space_common_core::types::MAX_GUEST_PATH_BYTES + 256;
/// The request body limit for `files:write_batch`: the base64 of the batch
/// cap plus 1/32 for `\/` escaping, per-entry JSON overhead for the maximum
/// entry count, and 64 KiB for the remaining fields. Without it the
/// framework's 2 MiB default would cap a batch at about 1.5 MiB of data.
const MAX_FILE_WRITE_BATCH_BASE64_CHARS: usize = MAX_FILE_WRITE_BATCH_BYTES.div_ceil(3) * 4;
pub(crate) const WRITE_FILE_BATCH_BODY_LIMIT_BYTES: usize = MAX_FILE_WRITE_BATCH_BASE64_CHARS
    + MAX_FILE_WRITE_BATCH_BASE64_CHARS / 32
    + MAX_WRITE_BATCH_ENTRIES * FILE_WRITE_BATCH_ENTRY_JSON_OVERHEAD_BYTES
    + 64 * 1024;
const DEFAULT_FILE_LIST_DEPTH: u32 = 1;

pub async fn stat_file(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    Path(sandbox_id): Path<String>,
    Json(request): Json<FileStatRequest>,
) -> Result<Json<FileStatResponse>, PublicApiError> {
    let sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, &sandbox_id).await?;
    let user = parse_guest_user(request.user)?;
    let path = parse_guest_path(request.path)?;
    let response = state
        .sandbox_controller
        .spaced_stat(
            &state.pool,
            &sandbox,
            SandboxApiMetricIdentity::native("files_stat"),
            sandbox.id,
            path,
            user,
        )
        .await
        .map_err(controller_error)?;
    let entry = response
        .info
        .map(public_file_info)
        .ok_or_else(PublicApiError::internal)?;
    Ok(Json(FileStatResponse { entry }))
}

pub async fn list_files(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    Path(sandbox_id): Path<String>,
    Json(request): Json<FileListRequest>,
) -> Result<Json<FileListResponse>, PublicApiError> {
    let depth = request.depth.unwrap_or(DEFAULT_FILE_LIST_DEPTH);
    if depth == 0 {
        return Err(PublicApiError::bad_request("depth must be at least 1"));
    }
    let sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, &sandbox_id).await?;
    let user = parse_guest_user(request.user)?;
    let path = parse_guest_path(request.path)?;
    let mut stream = state
        .sandbox_controller
        .spaced_list(
            &state.pool,
            &sandbox,
            SandboxApiMetricIdentity::native("files_list"),
            sandbox.id,
            crate::controllers::sandbox::SpacedListOptions {
                path,
                depth: Some(depth),
                user,
            },
        )
        .await
        .map_err(controller_error)?;

    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        if entries.len() >= MAX_FILE_LIST_ENTRIES {
            return Err(PublicApiError::bad_request(
                "file list returned too many entries; use a smaller depth",
            ));
        }
        let entry = entry
            .map_err(ControllerError::from)
            .map_err(controller_error)?;
        entries.push(public_file_info(entry));
    }
    Ok(Json(FileListResponse { entries }))
}

pub async fn read_file(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    Path(sandbox_id): Path<String>,
    Json(request): Json<FileReadRequest>,
) -> Result<Response, PublicApiError> {
    let sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, &sandbox_id).await?;
    validate_read_length(request.length)?;
    let user = parse_guest_user(request.user)?;
    let path = parse_guest_path(request.path)?;
    let stream = state
        .sandbox_controller
        .spaced_read(
            &state.pool,
            &sandbox,
            SandboxApiMetricIdentity::native("files_read"),
            SpacedReadRequest {
                sandbox_id: sandbox.id.to_string(),
                operation: Some(spaced_ops::ReadRequest {
                    path,
                    offset: request.offset,
                    length: Some(request.length),
                    user: user.map(|user| user.as_str().to_string()),
                }),
            },
        )
        .await
        .map_err(controller_error)?;
    let stream = stream.map(|chunk| {
        chunk.map(|chunk| chunk.data).map_err(|status| {
            // The 200 and part of the body are already sent, so only the
            // aborted response tells the client. Record it on this side too.
            tracing::warn!(
                code = ?status.code(),
                message = status.message(),
                "native file read failed mid-stream"
            );
            std::io::Error::other(status.message().to_owned())
        })
    });
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        Body::from_stream(stream),
    )
        .into_response())
}

pub async fn write_file(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    headers: HeaderMap,
    Path(sandbox_id): Path<String>,
    Json(request): Json<FileWriteRequest>,
) -> Result<Json<FileWriteResponse>, PublicApiError> {
    let idempotency_key = idempotency_key(&headers)?;
    let sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, &sandbox_id).await?;
    let FileWriteRequest {
        path,
        data_base64,
        append,
        user,
        parents,
    } = request;
    let user = parse_guest_user(user)?;
    let path = parse_guest_path(path)?;
    let chunks = decode_write_chunks(&data_base64)?;
    // The decoded chunks are all the RPC needs; don't hold the encoded copy
    // for the rest of the request.
    drop(data_base64);
    let mut fingerprint = RequestDigest::default();
    fingerprint
        .field(path.as_bytes())
        .flag(append)
        .flag(parents)
        .user(user.as_ref())
        .chunks(&chunks);
    let fingerprint = fingerprint.finish()?;
    let guest_key = guest_key(WRITE_FILE, &idempotency_key, &fingerprint)?;
    bind_request_key(&state, &sandbox, WRITE_FILE, &idempotency_key, &fingerprint).await?;
    let metric = SandboxApiMetricIdentity::native("files_write");
    if !parents {
        let response = state
            .sandbox_controller
            .spaced_write(
                &state.pool,
                &sandbox,
                metric,
                stream::iter(spaced_write_requests(
                    sandbox.id, path, append, guest_key, chunks, user,
                )),
            )
            .await
            .map_err(controller_error)?;
        return Ok(Json(FileWriteResponse {
            bytes_written: response.bytes_written,
        }));
    }
    // Kept for a second attempt; each attempt copies one chunk at a time.
    let chunks: Arc<[Vec<u8>]> = chunks.into();
    let attempt = |key: IdempotencyKey| {
        state.sandbox_controller.spaced_write(
            &state.pool,
            &sandbox,
            metric,
            stream::iter(shared_spaced_write_requests(
                sandbox.id,
                path.clone(),
                append,
                key,
                Arc::clone(&chunks),
                user.clone(),
            )),
        )
    };
    let response = write_creating_parents(
        &guest_key,
        [path.as_str()],
        user.as_ref(),
        |key| attempt(key.clone()),
        |request| mkdir_parent(&state, &sandbox, request),
        sandbox.id,
    )
    .await?;
    Ok(Json(FileWriteResponse {
        bytes_written: response.bytes_written,
    }))
}

pub async fn write_file_batch(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    headers: HeaderMap,
    Path(sandbox_id): Path<String>,
    Json(request): Json<FileWriteBatchRequest>,
) -> Result<Json<FileWriteBatchResponse>, PublicApiError> {
    let idempotency_key = idempotency_key(&headers)?;
    if request.entries.len() > MAX_WRITE_BATCH_ENTRIES {
        return Err(PublicApiError::bad_request(format!(
            "entries has {} items, max is {MAX_WRITE_BATCH_ENTRIES}",
            request.entries.len()
        )));
    }
    if request.parents && request.entries.iter().any(|entry| entry.append) {
        // A batch that met a missing directory is written again once it is
        // created, so the entries that landed first would append twice.
        return Err(PublicApiError::bad_request(
            "parents cannot be combined with append entries",
        ));
    }
    let sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, &sandbox_id).await?;
    let user = parse_guest_user(request.user)?;
    let parents = request.parents;
    let mut entries = Vec::with_capacity(request.entries.len());
    let mut batch_bytes = 0usize;
    for entry in request.entries {
        let path = parse_guest_path(entry.path)?;
        let data = decode_write_payload(&entry.data_base64)?;
        batch_bytes = batch_bytes
            .saturating_add(data.len())
            .saturating_add(path.len());
        if batch_bytes > MAX_FILE_WRITE_BATCH_BYTES {
            return Err(PublicApiError::bad_request(format!(
                "file write batch is too large: data and paths exceed \
                 {MAX_FILE_WRITE_BATCH_BYTES} bytes"
            )));
        }
        entries.push(SpacedWriteBatchEntry {
            path,
            data,
            append: entry.append,
        });
    }
    let mut fingerprint = RequestDigest::default();
    fingerprint
        .flag(parents)
        .user(user.as_ref())
        .count(entries.len());
    for entry in &entries {
        fingerprint
            .field(entry.path.as_bytes())
            .flag(entry.append)
            .field(&entry.data);
    }
    let fingerprint = fingerprint.finish()?;
    let guest_key = guest_key(WRITE_FILE_BATCH, &idempotency_key, &fingerprint)?;
    bind_request_key(
        &state,
        &sandbox,
        WRITE_FILE_BATCH,
        &idempotency_key,
        &fingerprint,
    )
    .await?;
    let attempt = |entries: Vec<SpacedWriteBatchEntry>, key: &IdempotencyKey| {
        state.sandbox_controller.spaced_write_batch(
            &state.pool,
            &sandbox,
            SandboxApiMetricIdentity::native("files_write_batch"),
            SpacedWriteBatchRequest {
                sandbox_id: sandbox.id.to_string(),
                operation: Some(spaced_ops::WriteBatchRequest {
                    entries,
                    idempotency_key: Some(key.as_str().to_string()),
                    user: user.as_ref().map(|user| user.as_str().to_string()),
                }),
            },
        )
    };
    let response = if parents {
        let paths: Vec<String> = entries.iter().map(|entry| entry.path.clone()).collect();
        write_creating_parents(
            &guest_key,
            paths.iter().map(String::as_str),
            user.as_ref(),
            |key| attempt(entries.clone(), key),
            |request| mkdir_parent(&state, &sandbox, request),
            sandbox.id,
        )
        .await?
    } else {
        attempt(entries, &guest_key)
            .await
            .map_err(controller_error)?
    };
    Ok(Json(FileWriteBatchResponse {
        files_written: response.files_written,
    }))
}

pub async fn mkdir_file(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    headers: HeaderMap,
    Path(sandbox_id): Path<String>,
    Json(request): Json<FileMkdirRequest>,
) -> Result<Json<FileMutationResponse>, PublicApiError> {
    let idempotency_key = idempotency_key(&headers)?;
    let sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, &sandbox_id).await?;
    let user = parse_guest_user(request.user)?;
    let path = parse_guest_path(request.path)?;
    let mut fingerprint = RequestDigest::default();
    fingerprint
        .field(path.as_bytes())
        .flag(request.parents)
        .user(user.as_ref());
    bind_request_key(
        &state,
        &sandbox,
        CREATE_DIRECTORY,
        &idempotency_key,
        &fingerprint.finish()?,
    )
    .await?;
    state
        .sandbox_controller
        .spaced_mkdir(
            &state.pool,
            &sandbox,
            SandboxApiMetricIdentity::native("files_mkdir"),
            spaced_mkdir_request(sandbox.id, path, request.parents, user.as_ref()),
        )
        .await
        .map_err(unprotected_mutation_error)?;
    Ok(Json(FileMutationResponse { ok: true }))
}

pub async fn remove_file(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    headers: HeaderMap,
    Path(sandbox_id): Path<String>,
    Json(request): Json<FileRemoveRequest>,
) -> Result<Json<FileMutationResponse>, PublicApiError> {
    let idempotency_key = idempotency_key(&headers)?;
    let sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, &sandbox_id).await?;
    let user = parse_guest_user(request.user)?;
    let path = parse_guest_path(request.path)?;
    let mut fingerprint = RequestDigest::default();
    fingerprint
        .field(path.as_bytes())
        .flag(request.recursive)
        .user(user.as_ref());
    bind_request_key(
        &state,
        &sandbox,
        REMOVE_FILE,
        &idempotency_key,
        &fingerprint.finish()?,
    )
    .await?;
    state
        .sandbox_controller
        .spaced_remove(
            &state.pool,
            &sandbox,
            SandboxApiMetricIdentity::native("files_remove"),
            SpacedRemoveRequest {
                sandbox_id: sandbox.id.to_string(),
                operation: Some(spaced_ops::RemoveRequest {
                    path,
                    recursive: request.recursive,
                    missing_ok: true,
                    user: user.map(|user| user.as_str().to_string()),
                }),
            },
        )
        .await
        .map_err(unprotected_mutation_error)?;
    Ok(Json(FileMutationResponse { ok: true }))
}

pub async fn rename_file(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    headers: HeaderMap,
    Path(sandbox_id): Path<String>,
    Json(request): Json<FileRenameRequest>,
) -> Result<Json<FileMutationResponse>, PublicApiError> {
    let idempotency_key = idempotency_key(&headers)?;
    let sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, &sandbox_id).await?;
    let user = parse_guest_user(request.user)?;
    let old_path = parse_guest_path(request.old_path)?;
    let new_path = parse_guest_path(request.new_path)?;
    let mut fingerprint = RequestDigest::default();
    fingerprint
        .field(old_path.as_bytes())
        .field(new_path.as_bytes())
        .user(user.as_ref());
    let fingerprint = fingerprint.finish()?;
    let guest_key = guest_key(RENAME_FILE, &idempotency_key, &fingerprint)?;
    bind_request_key(
        &state,
        &sandbox,
        RENAME_FILE,
        &idempotency_key,
        &fingerprint,
    )
    .await?;
    state
        .sandbox_controller
        .spaced_rename(
            &state.pool,
            &sandbox,
            SandboxApiMetricIdentity::native("files_rename"),
            SpacedRenameRequest {
                sandbox_id: sandbox.id.to_string(),
                operation: Some(spaced_ops::RenameRequest {
                    old_path,
                    new_path,
                    idempotency_key: Some(guest_key.as_str().to_string()),
                    user: user.map(|user| user.as_str().to_string()),
                }),
            },
        )
        .await
        .map_err(controller_error)?;
    Ok(Json(FileMutationResponse { ok: true }))
}

pub async fn enable_file_change_tracking(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    headers: HeaderMap,
    Path(sandbox_id): Path<String>,
    body: Option<Json<FileChangeTrackingRequest>>,
) -> Result<Json<FileChangeTrackingResponse>, PublicApiError> {
    let _idempotency_key = idempotency_key(&headers)?;
    let request = body.map(|Json(request)| request).unwrap_or_default();
    if !request.enabled {
        return Err(PublicApiError::bad_request(
            "file change tracking enable request must set enabled=true",
        ));
    }
    let sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, &sandbox_id).await?;
    let user = parse_guest_user(request.user)?;
    let roots = request
        .roots
        .into_iter()
        .map(parse_guest_path)
        .collect::<Result<Vec<_>, _>>()?;
    let response = state
        .sandbox_controller
        .spaced_enable_file_changes(
            &state.pool,
            &sandbox,
            SandboxApiMetricIdentity::native("files_changes_enable"),
            SpacedEnableFileChangesRequest {
                sandbox_id: sandbox.id.to_string(),
                operation: Some(spaced_ops::EnableFileChangesRequest {
                    roots,
                    include_globs: request.include_globs,
                    exclude_globs: request.exclude_globs,
                    use_default_excludes: request.use_default_excludes,
                    user: user.map(|user| user.as_str().to_string()),
                }),
            },
        )
        .await
        .map_err(unprotected_mutation_error)?;
    Ok(Json(FileChangeTrackingResponse {
        monitoring_enabled: true,
        observed_since: response.observed_since.and_then(timestamp_to_rfc3339),
    }))
}

pub async fn disable_file_change_tracking(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    headers: HeaderMap,
    Path(sandbox_id): Path<String>,
    body: Option<Json<FileChangeTrackingRequest>>,
) -> Result<Json<FileMutationResponse>, PublicApiError> {
    let _idempotency_key = idempotency_key(&headers)?;
    let request = body.map(|Json(request)| request).unwrap_or_default();
    let sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, &sandbox_id).await?;
    let user = parse_guest_user(request.user)?;
    state
        .sandbox_controller
        .spaced_disable_file_changes(
            &state.pool,
            &sandbox,
            SandboxApiMetricIdentity::native("files_changes_disable"),
            SpacedDisableFileChangesRequest {
                sandbox_id: sandbox.id.to_string(),
                operation: Some(spaced_ops::DisableFileChangesRequest {
                    user: user.map(|user| user.as_str().to_string()),
                }),
            },
        )
        .await
        .map_err(unprotected_mutation_error)?;
    Ok(Json(FileMutationResponse { ok: true }))
}

pub async fn modified_files(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    Path(sandbox_id): Path<String>,
    Json(request): Json<FileModifiedRequest>,
) -> Result<Json<FileModifiedResponse>, PublicApiError> {
    validate_modified_filters(&request)?;
    let sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, &sandbox_id).await?;
    let user = parse_guest_user(request.user)?;
    let response = state
        .sandbox_controller
        .spaced_get_modified_files(
            &state.pool,
            &sandbox,
            SandboxApiMetricIdentity::native("files_modified"),
            SpacedGetModifiedFilesRequest {
                sandbox_id: sandbox.id.to_string(),
                operation: Some(spaced_ops::GetModifiedFilesRequest {
                    window: request
                        .window_seconds
                        .map(duration_from_seconds)
                        .transpose()?,
                    since: request
                        .since
                        .as_deref()
                        .map(timestamp_from_rfc3339)
                        .transpose()?,
                    cursor: request.cursor,
                    user: user.map(|user| user.as_str().to_string()),
                }),
            },
        )
        .await
        .map_err(controller_error)?;
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
    Ok(Json(FileModifiedResponse {
        files,
        observed_since: response.observed_since.and_then(timestamp_to_rfc3339),
        retained_since: response.retained_since.and_then(timestamp_to_rfc3339),
        cursor: response.cursor,
        monitoring_enabled: response.monitoring_enabled,
        overflowed: response.overflowed,
    }))
}

async fn ensure_sandbox_owner(
    authorized: Option<Extension<AuthorizedSandbox>>,
    pool: &sqlx::PgPool,
    org_id: OrgId,
    sandbox_id: &str,
) -> Result<Sandbox, PublicApiError> {
    let sandbox_id = parse_public_sandbox_id(sandbox_id)?;
    authorized_sandbox(authorized, pool, org_id, sandbox_id).await
}

fn parse_guest_path(path: String) -> Result<String, PublicApiError> {
    validate_linux_guest_path_input(&path)
        .map_err(|error| PublicApiError::bad_request(error.to_string()))?;
    Ok(path)
}

fn parse_guest_user(user: Option<String>) -> Result<Option<GuestUserName>, PublicApiError> {
    user.map(GuestUserName::parse)
        .transpose()
        .map_err(|error| PublicApiError::bad_request(error.to_string()))
}

fn decode_write_payload(data_base64: &str) -> Result<Vec<u8>, PublicApiError> {
    let data = BASE64_STANDARD
        .decode(data_base64)
        .map_err(|_| PublicApiError::bad_request("data_base64 must be valid base64"))?;
    if data.len() > MAX_FILE_WRITE_BYTES {
        return Err(PublicApiError::bad_request(
            "file write payload is too large",
        ));
    }
    Ok(data)
}

/// Decode a `files:write` payload directly into its data messages, without a
/// buffer for the whole payload or a copy per message.
fn decode_write_chunks(data_base64: &str) -> Result<Vec<Vec<u8>>, PublicApiError> {
    let invalid = || PublicApiError::bad_request("data_base64 must be valid base64");
    let too_large = || PublicApiError::bad_request("file write payload is too large");
    if data_base64.len() / 4 * 3 > MAX_FILE_WRITE_BYTES + 3 {
        return Err(too_large());
    }
    let segments = data_base64.as_bytes().chunks(FILE_WRITE_CHUNK_BASE64_CHARS);
    let last = segments.len().saturating_sub(1);
    let mut chunks = Vec::with_capacity(segments.len());
    let mut total = 0usize;
    for (index, segment) in segments.enumerate() {
        // Padding may only end the payload, as when it is decoded whole.
        if index != last && segment.last() == Some(&b'=') {
            return Err(invalid());
        }
        let chunk = BASE64_STANDARD.decode(segment).map_err(|_| invalid())?;
        total += chunk.len();
        chunks.push(chunk);
    }
    if total > MAX_FILE_WRITE_BYTES {
        return Err(too_large());
    }
    Ok(chunks)
}

fn validate_read_length(length: u64) -> Result<(), PublicApiError> {
    if length == 0 {
        return Err(PublicApiError::bad_request(
            "file read length must be at least 1 byte",
        ));
    }
    if length > MAX_FILE_READ_BYTES {
        return Err(PublicApiError::bad_request(
            "file read length exceeds the control-plane API limit",
        ));
    }
    Ok(())
}

fn validate_modified_filters(request: &FileModifiedRequest) -> Result<(), PublicApiError> {
    let filters = [
        request.window_seconds.is_some(),
        request.since.is_some(),
        request.cursor.is_some(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();
    if filters > 1 {
        return Err(PublicApiError::bad_request(
            "window_seconds, since, and cursor are mutually exclusive",
        ));
    }
    Ok(())
}

fn spaced_write_requests(
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
fn shared_spaced_write_requests(
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

/// The guest mkdir `files:mkdir` issues; a `parents` write creates its
/// directories through the same request, so they match an explicit mkdir.
fn spaced_mkdir_request(
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
            user: user.map(|user| user.as_str().to_string()),
        }),
    }
}

async fn mkdir_parent(
    state: &AppState,
    sandbox: &Sandbox,
    request: SpacedMkdirRequest,
) -> Result<(), ControllerError> {
    state
        .sandbox_controller
        .spaced_mkdir(
            &state.pool,
            sandbox,
            SandboxApiMetricIdentity::native("files_write_parents"),
            request,
        )
        .await
        .map(drop)
}

/// A `parents` write: write, and if the guest reports the path missing,
/// create the missing directories through the mkdir `files:mkdir` issues and
/// write again. Directories are created only after that answer, never up
/// front: guest mkdir refuses a path through a symlinked directory that a
/// plain write follows, so an eager mkdir would fail writes that succeed
/// today, and costs a request on every write that does not need it.
async fn write_creating_parents<'a, T, Write, WriteFut, Mkdir, MkdirFut>(
    idempotency_key: &IdempotencyKey,
    paths: impl IntoIterator<Item = &'a str>,
    user: Option<&GuestUserName>,
    mut write: Write,
    mut mkdir: Mkdir,
    sandbox_id: SandboxId,
) -> Result<T, PublicApiError>
where
    Write: FnMut(&IdempotencyKey) -> WriteFut,
    WriteFut: std::future::Future<Output = Result<T, ControllerError>>,
    Mkdir: FnMut(SpacedMkdirRequest) -> MkdirFut,
    MkdirFut: std::future::Future<Output = Result<(), ControllerError>>,
{
    let error = match write(idempotency_key).await {
        Err(error) if error.is_guest_path_not_found() => error,
        result => return result.map_err(controller_error),
    };
    let directories = deepest_parent_directories(paths);
    let Some(retry_key) = parents_retry_key(idempotency_key).filter(|_| !directories.is_empty())
    else {
        return Err(controller_error(error));
    };
    for directory in directories {
        mkdir(spaced_mkdir_request(sandbox_id, directory, true, user))
            .await
            .map_err(controller_error)?;
    }
    write(&retry_key).await.map_err(controller_error)
}

/// The key for writing again once the missing directories exist. The guest
/// settled the first key with its not-found outcome, so the retry needs its
/// own; deriving it keeps a client's retry of the whole request idempotent.
/// `None` only when the client's key leaves no room for the suffix.
fn parents_retry_key(key: &IdempotencyKey) -> Option<IdempotencyKey> {
    IdempotencyKey::parse(format!("{}:parents", key.as_str())).ok()
}

/// The directories to create for `paths`, deepest first in path order: one
/// that is an ancestor of another is created by the deeper mkdir.
fn deepest_parent_directories<'a>(paths: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut directories: Vec<String> = Vec::new();
    for directory in paths.into_iter().filter_map(write_parent_directory) {
        if !directories.contains(&directory) {
            directories.push(directory);
        }
    }
    let covered = |candidate: &String| {
        directories.iter().any(|other| {
            other
                .strip_prefix(candidate.as_str())
                .is_some_and(|rest| rest.starts_with('/'))
        })
    };
    directories
        .iter()
        .filter(|candidate| !covered(candidate))
        .cloned()
        .collect()
}

/// The directory a write of `path` lands in, resolved lexically so a `..`
/// cannot create a directory the write never uses. `None` when the path names
/// no file, or its directory is the root or the user's home (a bare relative
/// name), which always exist. A relative result stays relative, so the guest
/// resolves it against the same home as the write.
fn write_parent_directory(path: &str) -> Option<String> {
    use std::path::{Component, Path};
    let path = Path::new(path);
    let absolute = path.is_absolute();
    let mut parts: Vec<&str> = Vec::new();
    let mut names_file = false;
    for component in path.components() {
        names_file = false;
        match component {
            Component::Normal(part) => {
                parts.push(part.to_str()?);
                names_file = true;
            }
            Component::ParentDir => match parts.last() {
                Some(&last) if last != ".." => {
                    parts.pop();
                }
                _ if !absolute => parts.push(".."),
                _ => {}
            },
            Component::RootDir | Component::CurDir | Component::Prefix(_) => {}
        }
    }
    if !names_file {
        return None;
    }
    parts.pop();
    if parts.is_empty() {
        return None;
    }
    let directory = parts.join("/");
    Some(if absolute {
        format!("/{directory}")
    } else {
        directory
    })
}

pub(in crate::routes::public) fn controller_error(error: ControllerError) -> PublicApiError {
    // A lock race is this service's own database, not the sandbox fleet. The
    // status match below cannot tell it from an out-of-capacity 503, so it
    // would attribute it to the cluster or the node.
    if error.is_lock_contention() {
        return PublicApiError::lock_contention();
    }
    let retry_after = error.retry_after_header();
    match error.status() {
        StatusCode::BAD_REQUEST => PublicApiError::bad_request(error.to_string()),
        StatusCode::FORBIDDEN => PublicApiError::forbidden(error.to_string()),
        // Every caller checked ownership before reaching the sandbox, so only
        // the guest's own missing-path answer is told apart from the rest.
        StatusCode::NOT_FOUND if error.is_guest_path_not_found() => {
            PublicApiError::path_not_found(error.to_string())
        }
        StatusCode::NOT_FOUND => PublicApiError::not_found(error.to_string()),
        StatusCode::CONFLICT => PublicApiError::conflict(error.to_string()),
        StatusCode::PRECONDITION_FAILED => PublicApiError::failed_precondition(error.to_string()),
        StatusCode::TOO_MANY_REQUESTS => with_retry_after_hint(
            PublicApiError::rate_limited(error.to_string())
                .with_classification(ResponseOrigin::Node, ResponseKind::Admission),
            retry_after,
        ),
        StatusCode::SERVICE_UNAVAILABLE => with_retry_after_hint(
            PublicApiError::service_unavailable(error.to_string())
                .with_classification(ResponseOrigin::Node, ResponseKind::BackendStatus),
            retry_after,
        ),
        // The sandbox did not answer within the budget; the operation may
        // still complete, so mutations retry only with the same key.
        StatusCode::GATEWAY_TIMEOUT | StatusCode::REQUEST_TIMEOUT => with_retry_after_hint(
            PublicApiError::service_unavailable("sandbox did not answer in time")
                .with_classification(ResponseOrigin::Node, ResponseKind::Timeout),
            retry_after,
        ),
        status if status.is_server_error() => {
            tracing::warn!(%error, "native file operation failed");
            PublicApiError::internal()
                .with_classification(ResponseOrigin::Node, ResponseKind::BackendStatus)
        }
        _ => PublicApiError::internal(),
    }
}

/// For a mutation whose `Idempotency-Key` the sandbox does not see (mkdir,
/// remove, file-change tracking), a failure that may have left the change
/// applied must not invite a blind retry: a repeated remove can delete a file
/// recreated in between. A server error or timeout stays retryable only when
/// the backend said it refused the request before doing anything; a retry
/// hint is not that evidence. A 429, from any layer, is answered before work.
fn unprotected_mutation_error(error: ControllerError) -> PublicApiError {
    let ambiguous = !error.is_refused_before_start()
        && (error.status().is_server_error() || error.status() == StatusCode::REQUEST_TIMEOUT);
    let public = controller_error(error);
    if ambiguous {
        public.not_retryable()
    } else {
        public
    }
}

fn public_file_info(info: SpacedFileInfo) -> FileInfo {
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

fn public_file_type(file_type: i32) -> FileType {
    match SpacedFileType::try_from(file_type) {
        Ok(SpacedFileType::File) => FileType::File,
        Ok(SpacedFileType::Directory) => FileType::Directory,
        Ok(SpacedFileType::Symlink) => FileType::Symlink,
        Ok(SpacedFileType::Other) | Ok(SpacedFileType::Unspecified) | Err(_) => FileType::Other,
    }
}

fn timestamp_to_rfc3339(value: prost_types::Timestamp) -> Option<String> {
    let timestamp = OffsetDateTime::from_unix_timestamp(value.seconds)
        .ok()?
        .checked_add(TimeDuration::nanoseconds(value.nanos.into()))?;
    timestamp
        .format(&Rfc3339)
        .ok()
        .or_else(|| Some(timestamp.unix_timestamp().to_string()))
}

fn timestamp_from_rfc3339(value: &str) -> Result<prost_types::Timestamp, PublicApiError> {
    let timestamp = OffsetDateTime::parse(value, &Rfc3339)
        .map_err(|_| PublicApiError::bad_request("since must be an RFC3339 timestamp"))?;
    Ok(prost_types::Timestamp {
        seconds: timestamp.unix_timestamp(),
        nanos: timestamp.nanosecond() as i32,
    })
}

fn duration_from_seconds(seconds: u64) -> Result<prost_types::Duration, PublicApiError> {
    let seconds = i64::try_from(seconds)
        .map_err(|_| PublicApiError::bad_request("window_seconds is too large"))?;
    Ok(prost_types::Duration { seconds, nanos: 0 })
}

fn public_file_event_kind(kind: i32) -> FileEventKind {
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

mod key_binding;
use key_binding::{RequestDigest, bind_request_key, guest_key};

#[cfg(test)]
mod tests;
