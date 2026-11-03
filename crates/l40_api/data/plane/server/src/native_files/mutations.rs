//! `files:mkdir`, `files:remove`, `files:rename`, `files:changes_enable`,
//! `files:changes_disable` and `files:modified`, in the steps and order of the
//! API's handlers for them. A mutation whose key the API binds sends the
//! request's fingerprint to be bound before the sandbox is reached.

use axum::{
    Json,
    body::Body,
    extract::State,
    http::Request,
    response::{IntoResponse, Response},
};
use pplx_space_common_core::types::SandboxId;
use pplx_space_native_files::{
    FileOperation, disable_file_changes_request, enable_file_changes_request, mkdir_fingerprint,
    modified_files_request, modified_files_response, parse_guest_path, parse_guest_user,
    remove_fingerprint, remove_request, rename_fingerprint, rename_request, spaced_mkdir_request,
    timestamp_to_rfc3339, validate_modified_filters, validate_tracking_enabled,
};
use pplx_space_node_api_client::NodeApiOperationBudget;
use pplx_space_public_api::{
    FileChangeTrackingRequest, FileChangeTrackingResponse, FileMkdirRequest, FileModifiedRequest,
    FileMutationResponse, FileRemoveRequest, FileRenameRequest,
};

use super::call::{Reconnect, SandboxCall};
use super::{FileRequest, Refusal, idempotency_key, json_body, optional_json_body, rejected};
use crate::DataPlaneState;

pub(super) async fn mkdir_file(
    State(state): State<DataPlaneState>,
    request: Request<Body>,
) -> Response {
    mkdir(&state, request)
        .await
        .unwrap_or_else(IntoResponse::into_response)
}

pub(super) async fn remove_file(
    State(state): State<DataPlaneState>,
    request: Request<Body>,
) -> Response {
    remove(&state, request)
        .await
        .unwrap_or_else(IntoResponse::into_response)
}

pub(super) async fn rename_file(
    State(state): State<DataPlaneState>,
    request: Request<Body>,
) -> Response {
    rename(&state, request)
        .await
        .unwrap_or_else(IntoResponse::into_response)
}

pub(super) async fn enable_file_change_tracking(
    State(state): State<DataPlaneState>,
    request: Request<Body>,
) -> Response {
    enable_tracking(&state, request)
        .await
        .unwrap_or_else(IntoResponse::into_response)
}

pub(super) async fn disable_file_change_tracking(
    State(state): State<DataPlaneState>,
    request: Request<Body>,
) -> Response {
    disable_tracking(&state, request)
        .await
        .unwrap_or_else(IntoResponse::into_response)
}

pub(super) async fn modified_files(
    State(state): State<DataPlaneState>,
    request: Request<Body>,
) -> Response {
    modified(&state, request)
        .await
        .unwrap_or_else(IntoResponse::into_response)
}

/// A mutation's answer when the sandbox did it.
fn done() -> Response {
    Json(FileMutationResponse { ok: true }).into_response()
}

async fn mkdir(state: &DataPlaneState, request: Request<Body>) -> Result<Response, Refusal> {
    const OPERATION: FileOperation = FileOperation::Mkdir;
    let (request, body) = FileRequest::authorize(state, OPERATION, request).await?;
    let body: FileMkdirRequest = json_body(&request, body).await?;
    idempotency_key(&request.headers)?;
    let user = parse_guest_user(body.user).map_err(rejected)?;
    let path = parse_guest_path(body.path).map_err(rejected)?;
    let fingerprint = mkdir_fingerprint(&path, body.parents, user.as_ref()).map_err(rejected)?;
    let call = SandboxCall::prepare(state, &request, Some(fingerprint)).await?;
    let message = spaced_mkdir_request(call.grant().sandbox_id, path, body.parents, user.as_ref());
    call.call(
        "spaced_mkdir",
        Reconnect::WhenDraining,
        Some(NodeApiOperationBudget::FilesystemMutation),
        message,
        |mut client, request| async move { client.spaced_mkdir(request).await },
    )
    .await
    .map_err(|failure| failure.into_refusal(OPERATION))?;
    Ok(done())
}

async fn remove(state: &DataPlaneState, request: Request<Body>) -> Result<Response, Refusal> {
    const OPERATION: FileOperation = FileOperation::Remove;
    let (request, body) = FileRequest::authorize(state, OPERATION, request).await?;
    let body: FileRemoveRequest = json_body(&request, body).await?;
    idempotency_key(&request.headers)?;
    let user = parse_guest_user(body.user).map_err(rejected)?;
    let path = parse_guest_path(body.path).map_err(rejected)?;
    let fingerprint = remove_fingerprint(&path, body.recursive, user.as_ref()).map_err(rejected)?;
    let call = SandboxCall::prepare(state, &request, Some(fingerprint)).await?;
    let message = remove_request(call.grant().sandbox_id, path, body.recursive, user.as_ref());
    call.call(
        "spaced_remove",
        Reconnect::WhenDraining,
        Some(NodeApiOperationBudget::FilesystemMutation),
        message,
        |mut client, request| async move { client.spaced_remove(request).await },
    )
    .await
    .map_err(|failure| failure.into_refusal(OPERATION))?;
    Ok(done())
}

async fn rename(state: &DataPlaneState, request: Request<Body>) -> Result<Response, Refusal> {
    const OPERATION: FileOperation = FileOperation::Rename;
    let (request, body) = FileRequest::authorize(state, OPERATION, request).await?;
    let body: FileRenameRequest = json_body(&request, body).await?;
    idempotency_key(&request.headers)?;
    let user = parse_guest_user(body.user).map_err(rejected)?;
    let old_path = parse_guest_path(body.old_path).map_err(rejected)?;
    let new_path = parse_guest_path(body.new_path).map_err(rejected)?;
    let fingerprint = rename_fingerprint(&old_path, &new_path, user.as_ref()).map_err(rejected)?;
    let call = SandboxCall::prepare(state, &request, Some(fingerprint)).await?;
    // The API derives the key the sandbox replays the rename by, from the
    // key it bound and the fingerprint.
    let guest_key = call.grant().guest_key()?;
    let message = rename_request(
        call.grant().sandbox_id,
        old_path,
        new_path,
        &guest_key,
        user.as_ref(),
    );
    call.call(
        "spaced_rename",
        Reconnect::WhenDraining,
        Some(NodeApiOperationBudget::FilesystemMutation),
        message,
        |mut client, request| async move { client.spaced_rename(request).await },
    )
    .await
    .map_err(|failure| failure.into_refusal(OPERATION))?;
    Ok(done())
}

async fn enable_tracking(
    state: &DataPlaneState,
    request: Request<Body>,
) -> Result<Response, Refusal> {
    const OPERATION: FileOperation = FileOperation::ChangesEnable;
    let (request, body) = FileRequest::authorize(state, OPERATION, request).await?;
    let body: Option<FileChangeTrackingRequest> = optional_json_body(&request, body).await?;
    // Required, and neither bound nor sent to the sandbox.
    idempotency_key(&request.headers)?;
    let body = body.unwrap_or_default();
    validate_tracking_enabled(&body).map_err(rejected)?;
    let user = parse_guest_user(body.user).map_err(rejected)?;
    let roots = body
        .roots
        .into_iter()
        .map(parse_guest_path)
        .collect::<Result<Vec<_>, _>>()
        .map_err(rejected)?;
    let call = SandboxCall::prepare(state, &request, None).await?;
    let message = enable_file_changes_request(
        call.grant().sandbox_id,
        roots,
        body.include_globs,
        body.exclude_globs,
        body.use_default_excludes,
        user.as_ref(),
    );
    let response = call
        .call(
            "spaced_enable_file_changes",
            Reconnect::WhenDraining,
            Some(NodeApiOperationBudget::FilesystemMutation),
            message,
            |mut client, request| async move { client.spaced_enable_file_changes(request).await },
        )
        .await
        .map_err(|failure| failure.into_refusal(OPERATION))?;
    Ok(Json(FileChangeTrackingResponse {
        monitoring_enabled: true,
        observed_since: response.observed_since.and_then(timestamp_to_rfc3339),
    })
    .into_response())
}

async fn disable_tracking(
    state: &DataPlaneState,
    request: Request<Body>,
) -> Result<Response, Refusal> {
    const OPERATION: FileOperation = FileOperation::ChangesDisable;
    let (request, body) = FileRequest::authorize(state, OPERATION, request).await?;
    let body: Option<FileChangeTrackingRequest> = optional_json_body(&request, body).await?;
    idempotency_key(&request.headers)?;
    let user = parse_guest_user(body.unwrap_or_default().user).map_err(rejected)?;
    let call = SandboxCall::prepare(state, &request, None).await?;
    let message = disable_file_changes_request(call.grant().sandbox_id, user.as_ref());
    call.call(
        "spaced_disable_file_changes",
        Reconnect::WhenDraining,
        Some(NodeApiOperationBudget::FilesystemMutation),
        message,
        |mut client, request| async move { client.spaced_disable_file_changes(request).await },
    )
    .await
    .map_err(|failure| failure.into_refusal(OPERATION))?;
    Ok(done())
}

async fn modified(state: &DataPlaneState, request: Request<Body>) -> Result<Response, Refusal> {
    const OPERATION: FileOperation = FileOperation::Modified;
    let (request, body) = FileRequest::authorize(state, OPERATION, request).await?;
    let body: FileModifiedRequest = json_body(&request, body).await?;
    validate_modified_filters(&body).map_err(rejected)?;
    let user = parse_guest_user(body.user).map_err(rejected)?;
    // Built before the sandbox is reached, as the API builds it, so a filter
    // it cannot read is refused without a wake. The grant names the sandbox.
    let mut message = modified_files_request(
        SandboxId::from_uuid(uuid::Uuid::nil()),
        body.window_seconds,
        body.since.as_deref(),
        body.cursor,
        user.as_ref(),
    )
    .map_err(rejected)?;
    let call = SandboxCall::prepare(state, &request, None).await?;
    message.sandbox_id = call.grant().sandbox_id.to_string();
    let response = call
        .call(
            "spaced_get_modified_files",
            Reconnect::WhenDraining,
            Some(NodeApiOperationBudget::FilesystemMetadata),
            message,
            |mut client, request| async move { client.spaced_get_modified_files(request).await },
        )
        .await
        .map_err(|failure| failure.into_refusal(OPERATION))?;
    Ok(Json(modified_files_response(response)).into_response())
}
