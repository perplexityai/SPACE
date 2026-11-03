//! `files:stat`, `files:list` and `files:read`, in the steps and order of the
//! API's handlers for them.

use axum::{
    Json,
    body::Body,
    extract::State,
    http::{Request, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures_util::StreamExt as _;
use pplx_space_native_files::{
    FileOperation, admit_list_entry, list_depth, list_request, parse_guest_path, parse_guest_user,
    public_file_info, read_request, stat_request, validate_read_length,
};
use pplx_space_node_api_client::NodeApiOperationBudget;
use pplx_space_public_api::{
    FileListRequest, FileListResponse, FileReadRequest, FileStatRequest, FileStatResponse,
};
use pplx_space_public_api_http::PublicApiError;

use super::call::{CallFailure, Reconnect, SandboxCall, WorkerCallError};
use super::{FileRequest, Refusal, json_body};
use crate::DataPlaneState;

pub(super) async fn stat_file(
    State(state): State<DataPlaneState>,
    request: Request<Body>,
) -> Response {
    stat(&state, request)
        .await
        .unwrap_or_else(IntoResponse::into_response)
}

pub(super) async fn list_files(
    State(state): State<DataPlaneState>,
    request: Request<Body>,
) -> Response {
    list(&state, request)
        .await
        .unwrap_or_else(IntoResponse::into_response)
}

pub(super) async fn read_file(
    State(state): State<DataPlaneState>,
    request: Request<Body>,
) -> Response {
    read(&state, request)
        .await
        .unwrap_or_else(IntoResponse::into_response)
}

async fn stat(state: &DataPlaneState, request: Request<Body>) -> Result<Response, Refusal> {
    const OPERATION: FileOperation = FileOperation::Stat;
    let (request, body) = FileRequest::authorize(state, OPERATION, request).await?;
    let body: FileStatRequest = json_body(&request.headers, body).await?;
    let user = parse_guest_user(body.user).map_err(rejected)?;
    let path = parse_guest_path(body.path).map_err(rejected)?;
    let mut call = SandboxCall::prepare(state, &request, None).await?;
    let message = stat_request(call.grant().sandbox_id, path, user.as_ref());
    let response = call
        .call(
            "spaced_stat",
            Reconnect::WhenDraining,
            Some(NodeApiOperationBudget::FilesystemMetadata),
            message,
            |mut client, request| async move { client.spaced_stat(request).await },
        )
        .await
        .map_err(|failure| failure.into_refusal(OPERATION))?;
    let entry = response
        .info
        .map(public_file_info)
        .ok_or_else(PublicApiError::internal)?;
    Ok(Json(FileStatResponse { entry }).into_response())
}

async fn list(state: &DataPlaneState, request: Request<Body>) -> Result<Response, Refusal> {
    const OPERATION: FileOperation = FileOperation::List;
    let (request, body) = FileRequest::authorize(state, OPERATION, request).await?;
    let body: FileListRequest = json_body(&request.headers, body).await?;
    let depth = list_depth(body.depth).map_err(rejected)?;
    let user = parse_guest_user(body.user).map_err(rejected)?;
    let path = parse_guest_path(body.path).map_err(rejected)?;
    let mut call = SandboxCall::prepare(state, &request, None).await?;
    let message = list_request(call.grant().sandbox_id, path, Some(depth), user.as_ref());
    let mut stream = call
        .call(
            "spaced_list",
            Reconnect::BeforeStream,
            Some(NodeApiOperationBudget::FilesystemMetadata),
            message,
            |mut client, request| async move { client.spaced_list(request).await },
        )
        .await
        .map_err(|failure| failure.into_refusal(OPERATION))?;
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        admit_list_entry(entries.len()).map_err(rejected)?;
        let entry = entry.map_err(|status| {
            CallFailure::Worker(WorkerCallError::Status(status)).into_refusal(OPERATION)
        })?;
        entries.push(public_file_info(entry));
    }
    Ok(Json(FileListResponse { entries }).into_response())
}

async fn read(state: &DataPlaneState, request: Request<Body>) -> Result<Response, Refusal> {
    const OPERATION: FileOperation = FileOperation::Read;
    let (request, body) = FileRequest::authorize(state, OPERATION, request).await?;
    let body: FileReadRequest = json_body(&request.headers, body).await?;
    validate_read_length(body.length).map_err(rejected)?;
    let user = parse_guest_user(body.user).map_err(rejected)?;
    let path = parse_guest_path(body.path).map_err(rejected)?;
    let mut call = SandboxCall::prepare(state, &request, None).await?;
    let message = read_request(
        call.grant().sandbox_id,
        path,
        body.offset,
        body.length,
        user.as_ref(),
    );
    let stream = call
        .call(
            "spaced_read",
            Reconnect::BeforeStream,
            Some(NodeApiOperationBudget::FilesystemTransferOpen),
            message,
            |mut client, request| async move { client.spaced_read(request).await },
        )
        .await
        .map_err(|failure| failure.into_refusal(OPERATION))?;
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

/// A field check's refusal, as the API answers it.
fn rejected(error: impl Into<PublicApiError>) -> Refusal {
    error.into().into()
}
