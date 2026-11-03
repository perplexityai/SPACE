use pplx_space_common_core::types::OrgId;

use std::sync::Arc;

use axum::{
    Extension, Json,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures_util::{StreamExt, stream};
use pplx_space_common_core::types::IdempotencyKey;
use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_native_files::{
    BatchEntry, FileCallFailure, admit_list_entry, decode_write_batch_entries, decode_write_chunks,
    guest_key, list_depth, parse_guest_path, parse_guest_user, public_file_info,
    shared_spaced_write_requests, spaced_write_requests, validate_modified_filters,
    validate_read_length, validate_tracking_enabled, validate_write_batch_shape,
    write_creating_parents,
};
use pplx_space_public_api::routes::{
    CREATE_DIRECTORY, REMOVE_FILE, RENAME_FILE, WRITE_FILE, WRITE_FILE_BATCH,
};
use pplx_space_public_api::{
    FileChangeTrackingRequest, FileChangeTrackingResponse, FileListRequest, FileListResponse,
    FileMkdirRequest, FileModifiedRequest, FileModifiedResponse, FileMutationResponse,
    FileReadRequest, FileRemoveRequest, FileRenameRequest, FileStatRequest, FileStatResponse,
    FileWriteBatchRequest, FileWriteBatchResponse, FileWriteRequest, FileWriteResponse,
};
use pplx_space_sbx_mgr_client::proto::{SpacedMkdirRequest, SpacedWriteBatchEntry};

use crate::{
    controllers::ControllerError, middleware::auth::AuthOrg, models::Sandbox, routes::AppState,
};

use super::{
    error::PublicApiError,
    owner_scope::{AuthorizedSandbox, authorized_sandbox},
    sandboxes::{idempotency_key, parse_public_sandbox_id},
};

pub(crate) use pplx_space_native_files::limits::{
    MAX_FILE_WRITE_BATCH_BYTES, WRITE_FILE_BATCH_BODY_LIMIT_BYTES, WRITE_FILE_BODY_LIMIT_BYTES,
};

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
    let depth = list_depth(request.depth)?;
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
        admit_list_entry(entries.len())?;
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
            pplx_space_native_files::read_request(
                sandbox.id,
                path,
                request.offset,
                request.length,
                user.as_ref(),
            ),
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
    let fingerprint =
        pplx_space_native_files::write_fingerprint(&path, append, parents, user.as_ref(), &chunks)?;
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
    .await
    .map_err(controller_error)?;
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
    validate_write_batch_shape(&request)?;
    let sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, &sandbox_id).await?;
    let user = parse_guest_user(request.user)?;
    let parents = request.parents;
    let entries = decode_write_batch_entries(request.entries)?;
    let fingerprint = pplx_space_native_files::write_batch_fingerprint(
        parents,
        user.as_ref(),
        entries.iter().map(|entry| BatchEntry {
            path: &entry.path,
            append: entry.append,
            data: &entry.data,
        }),
    )?;
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
            pplx_space_native_files::write_batch_request(sandbox.id, entries, key, user.as_ref()),
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
        .await
        .map_err(controller_error)?
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
    bind_request_key(
        &state,
        &sandbox,
        CREATE_DIRECTORY,
        &idempotency_key,
        &pplx_space_native_files::mkdir_fingerprint(&path, request.parents, user.as_ref())?,
    )
    .await?;
    state
        .sandbox_controller
        .spaced_mkdir(
            &state.pool,
            &sandbox,
            SandboxApiMetricIdentity::native("files_mkdir"),
            pplx_space_native_files::spaced_mkdir_request(
                sandbox.id,
                path,
                request.parents,
                user.as_ref(),
            ),
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
    bind_request_key(
        &state,
        &sandbox,
        REMOVE_FILE,
        &idempotency_key,
        &pplx_space_native_files::remove_fingerprint(&path, request.recursive, user.as_ref())?,
    )
    .await?;
    state
        .sandbox_controller
        .spaced_remove(
            &state.pool,
            &sandbox,
            SandboxApiMetricIdentity::native("files_remove"),
            pplx_space_native_files::remove_request(
                sandbox.id,
                path,
                request.recursive,
                user.as_ref(),
            ),
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
    let fingerprint =
        pplx_space_native_files::rename_fingerprint(&old_path, &new_path, user.as_ref())?;
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
            pplx_space_native_files::rename_request(
                sandbox.id,
                old_path,
                new_path,
                &guest_key,
                user.as_ref(),
            ),
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
    validate_tracking_enabled(&request)?;
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
            pplx_space_native_files::enable_file_changes_request(
                sandbox.id,
                roots,
                request.include_globs,
                request.exclude_globs,
                request.use_default_excludes,
                user.as_ref(),
            ),
        )
        .await
        .map_err(unprotected_mutation_error)?;
    Ok(Json(FileChangeTrackingResponse {
        monitoring_enabled: true,
        observed_since: response
            .observed_since
            .and_then(pplx_space_native_files::timestamp_to_rfc3339),
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
            pplx_space_native_files::disable_file_changes_request(sandbox.id, user.as_ref()),
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
            pplx_space_native_files::modified_files_request(
                sandbox.id,
                request.window_seconds,
                request.since.as_deref(),
                request.cursor,
                user.as_ref(),
            )?,
        )
        .await
        .map_err(controller_error)?;
    Ok(Json(pplx_space_native_files::modified_files_response(
        response,
    )))
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

pub(in crate::routes::public) fn controller_error(error: ControllerError) -> PublicApiError {
    pplx_space_native_files::file_call_error(&error).into()
}

fn unprotected_mutation_error(error: ControllerError) -> PublicApiError {
    pplx_space_native_files::unprotected_mutation_error(&error).into()
}

impl FileCallFailure for ControllerError {
    fn status(&self) -> StatusCode {
        ControllerError::status(self)
    }

    fn retry_after(&self) -> Option<std::time::Duration> {
        self.retry_after_header()
    }

    fn is_guest_path_not_found(&self) -> bool {
        ControllerError::is_guest_path_not_found(self)
    }

    fn is_refused_before_start(&self) -> bool {
        ControllerError::is_refused_before_start(self)
    }

    fn is_lock_contention(&self) -> bool {
        ControllerError::is_lock_contention(self)
    }
}

mod key_binding;
use key_binding::bind_request_key;

#[cfg(test)]
mod tests;
