//! `files:write` and `files:write_batch`, in the steps and order of the API's
//! handlers for them, behind the Native upload admission.

use std::sync::Arc;

use axum::{
    Json,
    body::Body,
    extract::State,
    http::{Request, StatusCode, Version, header},
    response::{IntoResponse, Response},
};
use futures_util::stream;
use pplx_space_guest_process::IdempotencyKey;
use pplx_space_metrics::response::{ResponseKind, ResponseOrigin};
use pplx_space_native_files::{
    BatchEntry, FileOperation, decode_write_batch_entries, decode_write_chunks, parse_guest_path,
    parse_guest_user, shared_spaced_write_requests, spaced_write_requests,
    validate_write_batch_shape, write_batch_fingerprint, write_batch_request,
    write_creating_parents, write_fingerprint,
};
use pplx_space_node_api_client::NodeApiOperationBudget;
use pplx_space_public_api::{
    FileWriteBatchRequest, FileWriteBatchResponse, FileWriteRequest, FileWriteResponse,
};
use pplx_space_public_api_http::{PublicApiError, normalize_public_error, with_retry_after_hint};
use pplx_space_sbx_mgr_client::proto::{SpacedMkdirRequest, SpacedWriteBatchEntry};

use super::admission::HTTP2_WAITING_WRITE_MAX_BYTES;
use super::call::{CallFailure, Reconnect, SandboxCall};
use super::{FileRequest, Refusal, idempotency_key, json_body, rejected};
use crate::DataPlaneState;
use crate::state::EcwProxyError;
use crate::upload_admission::{UploadPermit, UploadTenant};

pub(super) async fn write_file(
    State(state): State<DataPlaneState>,
    request: Request<Body>,
) -> Response {
    write(&state, request)
        .await
        .unwrap_or_else(IntoResponse::into_response)
}

pub(super) async fn write_file_batch(
    State(state): State<DataPlaneState>,
    request: Request<Body>,
) -> Response {
    write_batch(&state, request)
        .await
        .unwrap_or_else(IntoResponse::into_response)
}

async fn write(state: &DataPlaneState, request: Request<Body>) -> Result<Response, Refusal> {
    const OPERATION: FileOperation = FileOperation::Write;
    let (request, body) = FileRequest::authorize(state, OPERATION, request).await?;
    let (_permit, body) = admit(&request, body, OPERATION).await?;
    let FileWriteRequest {
        path,
        data_base64,
        append,
        user,
        parents,
    } = json_body(&request, body).await?;
    idempotency_key(&request.headers)?;
    let user = parse_guest_user(user).map_err(rejected)?;
    let path = parse_guest_path(path).map_err(rejected)?;
    let chunks = decode_write_chunks(&data_base64).map_err(rejected)?;
    // The decoded chunks are all the call needs; don't hold the encoded copy
    // for the rest of the request.
    drop(data_base64);
    let fingerprint =
        write_fingerprint(&path, append, parents, user.as_ref(), &chunks).map_err(rejected)?;
    let call = SandboxCall::prepare(state, &request, Some(fingerprint)).await?;
    let grant = call.grant();
    let guest_key = grant.guest_key()?;
    let sandbox_id = grant.sandbox_id;
    let response = if parents {
        // Kept for a second attempt; each attempt copies one chunk at a time.
        let chunks: Arc<[Vec<u8>]> = chunks.into();
        write_creating_parents(
            &guest_key,
            [path.as_str()],
            user.as_ref(),
            |key| {
                let requests = shared_spaced_write_requests(
                    sandbox_id,
                    path.clone(),
                    append,
                    key.clone(),
                    Arc::clone(&chunks),
                    user.clone(),
                );
                let call = &call;
                async move { send_write(call, stream::iter(requests)).await }
            },
            |request| mkdir_parent(&call, request),
            sandbox_id,
        )
        .await
    } else {
        let requests = spaced_write_requests(sandbox_id, path, append, guest_key, chunks, user);
        send_write(&call, stream::iter(requests)).await
    }
    .map_err(|failure| failure.into_refusal(OPERATION))?;
    Ok(Json(FileWriteResponse {
        bytes_written: response.bytes_written,
    })
    .into_response())
}

async fn write_batch(state: &DataPlaneState, request: Request<Body>) -> Result<Response, Refusal> {
    const OPERATION: FileOperation = FileOperation::WriteBatch;
    let (request, body) = FileRequest::authorize(state, OPERATION, request).await?;
    let (_permit, body) = admit(&request, body, OPERATION).await?;
    let body: FileWriteBatchRequest = json_body(&request, body).await?;
    idempotency_key(&request.headers)?;
    validate_write_batch_shape(&body).map_err(rejected)?;
    let user = parse_guest_user(body.user).map_err(rejected)?;
    let parents = body.parents;
    let entries = decode_write_batch_entries(body.entries).map_err(rejected)?;
    let fingerprint = write_batch_fingerprint(
        parents,
        user.as_ref(),
        entries.iter().map(|entry| BatchEntry {
            path: &entry.path,
            append: entry.append,
            data: &entry.data,
        }),
    )
    .map_err(rejected)?;
    let call = SandboxCall::prepare(state, &request, Some(fingerprint)).await?;
    let grant = call.grant();
    let guest_key = grant.guest_key()?;
    let sandbox_id = grant.sandbox_id;
    let attempt = |entries: Vec<SpacedWriteBatchEntry>, key: &IdempotencyKey| {
        let message = write_batch_request(sandbox_id, entries, key, user.as_ref());
        let call = &call;
        async move {
            call.call(
                "spaced_write_batch",
                Reconnect::WhenDraining,
                None,
                message,
                |mut client, request| async move { client.spaced_write_batch(request).await },
            )
            .await
        }
    };
    let response = if parents {
        let paths: Vec<String> = entries.iter().map(|entry| entry.path.clone()).collect();
        write_creating_parents(
            &guest_key,
            paths.iter().map(String::as_str),
            user.as_ref(),
            |key| attempt(entries.clone(), key),
            |request| mkdir_parent(&call, request),
            sandbox_id,
        )
        .await
    } else {
        attempt(entries, &guest_key).await
    }
    .map_err(|failure| failure.into_refusal(OPERATION))?;
    Ok(Json(FileWriteBatchResponse {
        files_written: response.files_written,
    })
    .into_response())
}

/// One write stream. Nothing reconnects it, as the API's spaced client does
/// not: its body may already be partly sent.
async fn send_write<S>(
    call: &SandboxCall<'_>,
    requests: S,
) -> Result<pplx_space_sbx_mgr_client::operations::WriteResponse, CallFailure>
where
    S: futures_util::Stream<Item = pplx_space_sbx_mgr_client::proto::SpacedWriteRequest>
        + Send
        + 'static,
{
    call.send("spaced_write", requests, |mut client, request| async move {
        client.spaced_write(request).await
    })
    .await
}

/// The mkdir a `parents` write creates a missing directory with, as
/// `files:mkdir` sends it.
async fn mkdir_parent(
    call: &SandboxCall<'_>,
    request: SpacedMkdirRequest,
) -> Result<(), CallFailure> {
    call.call(
        "spaced_mkdir",
        Reconnect::WhenDraining,
        Some(NodeApiOperationBudget::FilesystemMutation),
        request,
        |mut client, request| async move { client.spaced_mkdir(request).await },
    )
    .await
    .map(drop)
}

/// Admit an authorized write before its body is read: a body declared larger
/// than the route's limit is refused as the API refuses it, and the write
/// reserves its declared length, or the whole limit when it declares none,
/// from its org's and sandbox's shares of this replica's Native upload
/// budget, waiting for them unless it is a large HTTP/2 write (see
/// [`super::admission`]).
/// The permit is held until the write answers.
async fn admit(
    request: &FileRequest,
    body: Body,
    operation: FileOperation,
) -> Result<(Option<UploadPermit>, Body), Refusal> {
    let limit = super::body_limit(operation) as u64;
    let declared = request
        .headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    if declared.is_some_and(|declared| declared > limit) {
        // The API's extractor reads it up to the limit and answers 413;
        // reading it here would change nothing but the cost.
        return Err(normalize_public_error(StatusCode::PAYLOAD_TOO_LARGE.into_response()).into());
    }
    let Some(uploads) = &request.files.uploads else {
        return Ok((None, body));
    };
    let tenant = UploadTenant {
        org_id: request.org_id,
        sandbox_id: request.sandbox_id,
    };
    // Over HTTP/2 only a small write waits, so the bodies of waiting writes
    // can't fill their connection's window (see `admission`).
    let waits = request.version < Version::HTTP_2
        || declared.is_some_and(|declared| declared <= HTTP2_WAITING_WRITE_MAX_BYTES);
    uploads
        .admit_ungoverned(tenant, declared.unwrap_or(limit), waits)
        .await
        .map(|permit| (Some(permit), body))
        .map_err(|refused| rate_limited(&refused).into())
}

/// The Native answer to an upload the admission refused: `rate_limited`,
/// with its `Retry-After`.
fn rate_limited(refused: &EcwProxyError) -> PublicApiError {
    with_retry_after_hint(
        PublicApiError::rate_limited(refused.message.clone())
            .with_classification(ResponseOrigin::DataPlane, ResponseKind::Admission),
        refused.retry_after,
    )
}
