use std::{
    convert::Infallible,
    io::SeekFrom,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration as StdDuration,
};

use axum::{
    Json,
    body::{Body, Bytes, to_bytes},
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::{Stream, StreamExt, stream};
use http_body_util::BodyExt;
use pplx_space_common_core::types::{GuestUserName, SandboxId, validate_linux_guest_path_input};
use pplx_space_common_svc::bootstrap::wrap_grpc_client_channel;
use pplx_space_node_api_client::{
    NodeApiOperationBudget, proto::worker_api_client::WorkerApiClient,
};
use pplx_space_sbx_mgr_client::operations as spaced_ops;
use pplx_space_sbx_mgr_client::proto::{
    SpacedCreateWatcherRequest, SpacedFileEvent, SpacedFileEventKind, SpacedFileInfo,
    SpacedFileType, SpacedGetWatcherEventsRequest, SpacedListRequest, SpacedMkdirRequest,
    SpacedNormalizeWatchRootRequest, SpacedReadRequest, SpacedRemoveRequest,
    SpacedRemoveWatcherRequest, SpacedRenameRequest, SpacedStatRequest, SpacedWatchRequest,
    SpacedWriteOpen, SpacedWriteRequest, spaced_write_request,
};
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use time::{Duration as TimeDuration, OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use super::{
    AttachedStreamPermit, DataPlaneState, ECW_FILE_WATCH_STREAM_KIND, EcwProxyError,
    EcwResolvedRoute, ResponseKind, ResponseOrigin, acquire_ecw_stream_permit, ecw_backend_channel,
    ecw_connect, node_api_request_with_route_token_state, resolve_ecw_envd_route,
};
use crate::ecw_process::EmptyUnaryConnectResponseE2b;
use crate::runtime_route::{
    EcwRouteAttemptError, bounded_ecw_route_transfer, bounded_node_api_unary,
    retryable_bounded_node_api_unary,
};

const E2B_FILE_CONTENT_LIMIT_BYTES: usize = 256 * 1024 * 1024;
const E2B_FILE_WRITE_CHUNK_BYTES: usize = 3 * 1024 * 1024;
const E2B_PROTO_BODY_LIMIT: usize = 1024 * 1024;
const E2B_JSON_FILE_WRITE_BODY_LIMIT_BYTES: usize = E2B_PROTO_BODY_LIMIT;
/// Request bytes multer may hold without yielding them as field data: part
/// headers, boundaries, and the preamble. Legitimate parts need a few hundred
/// bytes; the cap stops a body without a header terminator or boundary from
/// accumulating in multer's parse buffer.
const E2B_MULTIPART_PARSE_BUFFER_LIMIT_BYTES: usize = 1024 * 1024;

#[derive(Debug, Deserialize)]
pub(super) struct FileReadQueryE2b {
    path: String,
    #[serde(default)]
    username: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct FileWritePathQueryE2b {
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    username: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FileWriteRequestE2b {
    path: String,
    content: Value,
}

#[derive(Debug, Deserialize)]
pub(super) struct FilesystemPathRequestE2b {
    #[serde(default)]
    path: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct FilesystemListDirRequestE2b {
    #[serde(default)]
    path: String,
    #[serde(default)]
    depth: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub(super) struct FilesystemMoveRequestE2b {
    #[serde(default)]
    source: String,
    #[serde(default)]
    destination: String,
}

#[derive(Debug, Serialize)]
pub(super) struct FilesystemEntryInfoE2b {
    name: String,
    #[serde(rename = "type")]
    file_type: i32,
    path: String,
    size: i64,
    mode: u32,
    #[serde(rename = "modifiedTime", skip_serializing_if = "Option::is_none")]
    modified_time: Option<String>,
    #[serde(rename = "symlinkTarget", skip_serializing_if = "Option::is_none")]
    symlink_target: Option<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct FilesystemStatResponseE2b {
    entry: FilesystemEntryInfoE2b,
}

#[derive(Debug, Serialize)]
pub(super) struct FilesystemListDirResponseE2b {
    entries: Vec<FilesystemEntryInfoE2b>,
}

#[derive(Debug, Serialize)]
pub(super) struct FilesystemMakeDirResponseE2b {
    entry: FilesystemEntryInfoE2b,
}

#[derive(Debug, Serialize)]
pub(super) struct FilesystemMoveResponseE2b {
    entry: FilesystemEntryInfoE2b,
}

#[derive(Debug, Serialize)]
pub(super) struct FileWriteInfoE2b {
    name: String,
    #[serde(rename = "type")]
    file_type: &'static str,
    path: String,
}

#[derive(Debug)]
struct MultipartUploadPartE2b {
    filename: Option<String>,
    upload: SpooledUpload,
}

#[derive(Clone, PartialEq, Message, Deserialize)]
pub(super) struct E2bWatchDirRequest {
    #[prost(string, tag = "1")]
    #[serde(default)]
    path: String,
    #[prost(bool, tag = "2")]
    #[serde(default)]
    recursive: bool,
    #[prost(bool, tag = "3")]
    #[serde(rename = "includeEntry", default)]
    _include_entry: bool,
    #[prost(bool, tag = "4")]
    #[serde(rename = "allowNetworkMounts", default)]
    _allow_network_mounts: bool,
}

#[derive(Clone, PartialEq, Message)]
struct E2bFilesystemEvent {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(enumeration = "E2bEventType", tag = "2")]
    event_type: i32,
}

#[derive(Clone, Debug, Serialize)]
struct E2bFilesystemEventJson {
    name: String,
    #[serde(rename = "type")]
    event_type: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum E2bEventType {
    Unspecified = 0,
    Create = 1,
    Write = 2,
    Remove = 3,
    Rename = 4,
    Chmod = 5,
}

impl E2bEventType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Create => "EVENT_TYPE_CREATE",
            Self::Write => "EVENT_TYPE_WRITE",
            Self::Remove => "EVENT_TYPE_REMOVE",
            Self::Rename => "EVENT_TYPE_RENAME",
            Self::Chmod => "EVENT_TYPE_CHMOD",
            Self::Unspecified => "EVENT_TYPE_UNSPECIFIED",
        }
    }
}

#[derive(Clone, PartialEq, Message, Serialize)]
pub(super) struct E2bCreateWatcherResponse {
    #[prost(string, tag = "1")]
    #[serde(rename = "watcherId")]
    watcher_id: String,
}

#[derive(Clone, PartialEq, Message, Deserialize)]
pub(super) struct E2bGetWatcherEventsRequest {
    #[prost(string, tag = "1")]
    #[serde(rename = "watcherId", default)]
    watcher_id: String,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct E2bGetWatcherEventsResponseJson {
    events: Vec<E2bFilesystemEventJson>,
}

#[derive(Clone, PartialEq, Message, Deserialize)]
pub(super) struct E2bRemoveWatcherRequest {
    #[prost(string, tag = "1")]
    #[serde(rename = "watcherId", default)]
    watcher_id: String,
}

#[derive(Clone, PartialEq, Message, Serialize)]
pub(super) struct E2bRemoveWatcherResponse {}

pub(super) async fn read_file(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
    Query(query): Query<FileReadQueryE2b>,
) -> Result<Response, EcwProxyError> {
    let route = resolve_ecw_envd_route(&state, &headers).await?;
    let user = extract_query_user(query.username.as_deref())?;
    let message = SpacedReadRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::ReadRequest {
            path: e2b_request_root(&query.path, user.as_ref())?,
            offset: 0,
            length: None,
            user: user.map(|user| user.as_str().to_string()),
        }),
    };
    let (stream, _route) = bounded_node_api_unary(
        &state,
        route,
        "filesystem_read",
        NodeApiOperationBudget::FilesystemTransferOpen.timeout(),
        message,
        |channel, request| async move {
            WorkerApiClient::new(wrap_grpc_client_channel(channel))
                .spaced_read(request)
                .await
        },
        raw_file_read_error,
    )
    .await?;

    let mut bytes_read = 0usize;
    let stream = stream.map(move |chunk| {
        let chunk = chunk.map_err(|status| std::io::Error::other(status.to_string()))?;
        bytes_read = checked_file_content_len(bytes_read, chunk.data.len())
            .map_err(|err| std::io::Error::other(err.message))?;
        Ok::<Bytes, std::io::Error>(chunk.data)
    });

    Ok((
        [(header::CONTENT_TYPE, "application/octet-stream")],
        Body::from_stream(stream),
    )
        .into_response())
}

pub(super) async fn write_file(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
    Query(query): Query<FileWritePathQueryE2b>,
    body: Body,
) -> Result<Json<Vec<FileWriteInfoE2b>>, EcwProxyError> {
    let route = resolve_ecw_envd_route(&state, &headers).await?;
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json");
    let user = extract_write_user(query.username.as_deref(), &headers)?;
    if is_octet_stream_content_type(content_type) {
        reject_oversized_content_length(&headers)?;
        let Some(path) = query.path else {
            return Err(EcwProxyError::bad_request(
                "path query parameter is required for octet-stream uploads",
            ));
        };
        let path = e2b_request_root(&path, user.as_ref())?;
        spaced_write_body_stream(&state, route, path.clone(), body, user).await?;
        return Ok(Json(vec![FileWriteInfoE2b {
            name: path_basename(&path).to_string(),
            file_type: "file",
            path,
        }]));
    }

    if is_json_content_type(content_type) {
        let request = read_json_file_write_request(&headers, body).await?;
        let path = e2b_request_root(&request.path, user.as_ref())?;
        spaced_write_bytes(
            &state,
            route,
            path.clone(),
            file_write_content_to_bytes(request.content)?,
            user,
        )
        .await?;
        return Ok(Json(vec![FileWriteInfoE2b {
            name: path_basename(&path).to_string(),
            file_type: "file",
            path,
        }]));
    }

    if content_type.starts_with("multipart/form-data") {
        reject_oversized_content_length(&headers)?;
        let boundary = multipart_boundary(content_type).ok_or_else(|| {
            EcwProxyError::bad_request("multipart/form-data boundary is required")
        })?;
        let uploads = spool_multipart_uploads(body, boundary).await?;
        let writes = resolve_multipart_uploads(uploads, query.path, user.as_ref())?;
        let mut written = Vec::with_capacity(writes.len());
        let mut route = route;
        for (path, upload) in writes {
            route = spaced_write_spooled(&state, route, path.clone(), upload, user.clone()).await?;
            written.push(FileWriteInfoE2b {
                name: path_basename(&path).to_string(),
                file_type: "file",
                path,
            });
        }
        return Ok(Json(written));
    }

    Err(EcwProxyError::bad_request(
        "Unsupported Content-Type for file write",
    ))
}

fn reject_oversized_content_length(headers: &HeaderMap) -> Result<(), EcwProxyError> {
    let Some(content_length) = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let content_length = content_length
        .parse::<usize>()
        .map_err(|_| EcwProxyError::bad_request("Invalid Content-Length"))?;
    if content_length > E2B_FILE_CONTENT_LIMIT_BYTES {
        return Err(file_too_large_error());
    }
    Ok(())
}

fn reject_oversized_json_content_length(headers: &HeaderMap) -> Result<(), EcwProxyError> {
    let Some(content_length) = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let content_length = content_length
        .parse::<usize>()
        .map_err(|_| EcwProxyError::bad_request("Invalid Content-Length"))?;
    if content_length > E2B_JSON_FILE_WRITE_BODY_LIMIT_BYTES {
        return Err(json_file_write_body_too_large_error());
    }
    Ok(())
}

fn extract_write_user(
    query_username: Option<&str>,
    headers: &HeaderMap,
) -> Result<Option<GuestUserName>, EcwProxyError> {
    query_username
        .map(str::to_string)
        .or_else(|| envd_basic_username(headers))
        .map(GuestUserName::parse)
        .transpose()
        .map_err(|error| EcwProxyError::bad_request(error.to_string()))
}

fn extract_query_user(
    query_username: Option<&str>,
) -> Result<Option<GuestUserName>, EcwProxyError> {
    query_username
        .map(GuestUserName::parse)
        .transpose()
        .map_err(|error| EcwProxyError::bad_request(error.to_string()))
}

fn extract_basic_user(headers: &HeaderMap) -> Result<Option<GuestUserName>, EcwProxyError> {
    envd_basic_username(headers)
        .map(GuestUserName::parse)
        .transpose()
        .map_err(|error| EcwProxyError::bad_request(error.to_string()))
}

pub(super) async fn filesystem_stat(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
    Json(request): Json<FilesystemPathRequestE2b>,
) -> Result<Json<FilesystemStatResponseE2b>, EcwProxyError> {
    let route = resolve_ecw_envd_route(&state, &headers).await?;
    let user = extract_basic_user(&headers)?;
    let path = e2b_request_root(&request.path, user.as_ref())?;
    let message = SpacedStatRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::StatRequest {
            path,
            user: user.map(|user| user.as_str().to_string()),
        }),
    };
    let (response, _route) = retryable_bounded_node_api_unary(
        &state,
        route,
        "filesystem_stat",
        NodeApiOperationBudget::FilesystemMetadata.timeout(),
        message,
        |channel, request| async move {
            WorkerApiClient::new(wrap_grpc_client_channel(channel))
                .spaced_stat(request)
                .await
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await?;
    let entry = response
        .info
        .map(file_info_to_connect_entry)
        .ok_or_else(|| EcwProxyError::internal("Missing file info"))?;

    Ok(Json(FilesystemStatResponseE2b { entry }))
}

pub(super) async fn filesystem_list_dir(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
    Json(request): Json<FilesystemListDirRequestE2b>,
) -> Result<Json<FilesystemListDirResponseE2b>, EcwProxyError> {
    let depth = request.depth.unwrap_or(1);
    if depth == 0 {
        return Err(EcwProxyError::bad_request("depth must be at least 1"));
    }

    let route = resolve_ecw_envd_route(&state, &headers).await?;
    let user = extract_basic_user(&headers)?;
    let message = SpacedListRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::ListRequest {
            path: e2b_request_root(&request.path, user.as_ref())?,
            depth: Some(depth),
            user: user.map(|user| user.as_str().to_string()),
        }),
    };
    let (mut stream, _route) = bounded_node_api_unary(
        &state,
        route,
        "filesystem_list",
        NodeApiOperationBudget::FilesystemMetadata.timeout(),
        message,
        |channel, request| async move {
            WorkerApiClient::new(wrap_grpc_client_channel(channel))
                .spaced_list(request)
                .await
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await?;

    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        entries.push(file_info_to_connect_entry(
            entry.map_err(ecw_connect::tonic_status_to_ecw_error)?,
        ));
    }

    Ok(Json(FilesystemListDirResponseE2b { entries }))
}

pub(super) async fn filesystem_make_dir(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
    Json(request): Json<FilesystemPathRequestE2b>,
) -> Result<Json<FilesystemMakeDirResponseE2b>, EcwProxyError> {
    let route = resolve_ecw_envd_route(&state, &headers).await?;
    let user = extract_basic_user(&headers)?;
    let path = e2b_request_root(&request.path, user.as_ref())?;
    let message = SpacedMkdirRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::MkdirRequest {
            path: path.clone(),
            parents: true,
            exist_ok: true,
            user: user.clone().map(|user| user.as_str().to_string()),
        }),
    };
    // The stat below runs against the route that actually served the mkdir, so a
    // refresh during the mutation carries forward instead of being rediscovered.
    let (_, route) = retryable_bounded_node_api_unary(
        &state,
        route,
        "filesystem_mkdir",
        NodeApiOperationBudget::FilesystemMutation.timeout(),
        message,
        |channel, request| async move {
            WorkerApiClient::new(wrap_grpc_client_channel(channel))
                .spaced_mkdir(request)
                .await
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await?;
    let entry = stat_path(&state, route, path, user).await?;

    Ok(Json(FilesystemMakeDirResponseE2b { entry }))
}

pub(super) async fn filesystem_move(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
    Json(request): Json<FilesystemMoveRequestE2b>,
) -> Result<Json<FilesystemMoveResponseE2b>, EcwProxyError> {
    let route = resolve_ecw_envd_route(&state, &headers).await?;
    let user = extract_basic_user(&headers)?;
    let source = e2b_request_root(&request.source, user.as_ref())?;
    let destination = e2b_request_root(&request.destination, user.as_ref())?;
    let message = SpacedRenameRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::RenameRequest {
            old_path: source,
            new_path: destination.clone(),
            idempotency_key: None,
            user: user.clone().map(|user| user.as_str().to_string()),
        }),
    };
    let (_, route) = bounded_node_api_unary(
        &state,
        route,
        "filesystem_rename",
        NodeApiOperationBudget::FilesystemMutation.timeout(),
        message,
        |channel, request| async move {
            WorkerApiClient::new(wrap_grpc_client_channel(channel))
                .spaced_rename(request)
                .await
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await?;
    let entry = stat_path(&state, route, destination, user).await?;

    Ok(Json(FilesystemMoveResponseE2b { entry }))
}

pub(super) async fn filesystem_remove(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
    Json(request): Json<FilesystemPathRequestE2b>,
) -> Result<Json<EmptyUnaryConnectResponseE2b>, EcwProxyError> {
    let route = resolve_ecw_envd_route(&state, &headers).await?;
    let user = extract_basic_user(&headers)?;
    let message = SpacedRemoveRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::RemoveRequest {
            path: e2b_request_root(&request.path, user.as_ref())?,
            recursive: true,
            missing_ok: false,
            user: user.map(|user| user.as_str().to_string()),
        }),
    };
    bounded_node_api_unary(
        &state,
        route,
        "filesystem_remove",
        NodeApiOperationBudget::FilesystemMutation.timeout(),
        message,
        |channel, request| async move {
            WorkerApiClient::new(wrap_grpc_client_channel(channel))
                .spaced_remove(request)
                .await
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await?;

    Ok(Json(EmptyUnaryConnectResponseE2b::default()))
}

async fn stat_path(
    state: &DataPlaneState,
    route: EcwResolvedRoute,
    path: String,
    user: Option<GuestUserName>,
) -> Result<FilesystemEntryInfoE2b, EcwProxyError> {
    let message = SpacedStatRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::StatRequest {
            path,
            user: user.map(|user| user.as_str().to_string()),
        }),
    };
    let (response, _route) = retryable_bounded_node_api_unary(
        state,
        route,
        "filesystem_stat",
        NodeApiOperationBudget::FilesystemMetadata.timeout(),
        message,
        |channel, request| async move {
            WorkerApiClient::new(wrap_grpc_client_channel(channel))
                .spaced_stat(request)
                .await
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await?;
    response
        .info
        .map(file_info_to_connect_entry)
        .ok_or_else(|| EcwProxyError::internal("Missing file info"))
}

/// Write one file, returning the route that served it so a caller writing
/// several files in one request carries any refresh forward instead of
/// rediscovering it per file.
async fn spaced_write_bytes(
    state: &DataPlaneState,
    route: EcwResolvedRoute,
    path: String,
    data: Vec<u8>,
    user: Option<GuestUserName>,
) -> Result<EcwResolvedRoute, EcwProxyError> {
    let route = ensure_parent_dir(state, route, &path, user.clone()).await?;

    let requests = ReplayableSpacedWrite::new(route.sandbox_id, path, data, user);
    let (_, route) = bounded_ecw_route_transfer(
        state,
        route,
        "filesystem_write",
        NodeApiOperationBudget::FilesystemTransferOpen.timeout(),
        move |route, _remaining| {
            let requests = requests.clone();
            async move {
                let channel = ecw_backend_channel(state, &route)
                    .await
                    .map_err(EcwRouteAttemptError::Proxy)?;
                let mut client = WorkerApiClient::new(wrap_grpc_client_channel(channel));
                let stream = requests.stream();
                client
                    .spaced_write(node_api_request_with_route_token_state(
                        stream,
                        route.space_api_route_token.as_ref(),
                    )?)
                    .await
                    .map(|response| response.into_inner())
                    .map_err(EcwRouteAttemptError::Status)
            }
        },
        raw_file_write_error,
    )
    .await?;
    Ok(route)
}

/// Create the parent directory of a write target, returning the route that
/// served it.
///
/// The returned route may differ from the one passed in if the mkdir hit a
/// superseded identity and was recovered; the caller's subsequent write must use
/// the refreshed one, avoiding an unnecessary rejected write when mkdir already
/// observed a superseded identity.
async fn ensure_parent_dir(
    state: &DataPlaneState,
    route: EcwResolvedRoute,
    path: &str,
    user: Option<GuestUserName>,
) -> Result<EcwResolvedRoute, EcwProxyError> {
    let Some(parent) = e2b_parent_dir(path) else {
        return Ok(route);
    };
    let message = SpacedMkdirRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::MkdirRequest {
            path: parent,
            parents: true,
            exist_ok: true,
            user: user.map(|user| user.as_str().to_string()),
        }),
    };
    let (_, route) = retryable_bounded_node_api_unary(
        state,
        route,
        "filesystem_mkdir",
        NodeApiOperationBudget::FilesystemMutation.timeout(),
        message,
        |channel, request| async move {
            WorkerApiClient::new(wrap_grpc_client_channel(channel))
                .spaced_mkdir(request)
                .await
        },
        raw_file_write_error,
    )
    .await?;
    Ok(route)
}

async fn spaced_write_body_stream(
    state: &DataPlaneState,
    route: EcwResolvedRoute,
    path: String,
    body: Body,
    user: Option<GuestUserName>,
) -> Result<EcwResolvedRoute, EcwProxyError> {
    let upload = spool_body_upload(body).await?;
    spaced_write_spooled(state, route, path, upload, user).await
}

async fn spool_body_upload(body: Body) -> Result<SpooledUpload, EcwProxyError> {
    spool_body_upload_with_limit(body, E2B_FILE_CONTENT_LIMIT_BYTES).await
}

async fn spool_body_upload_with_limit(
    mut body: Body,
    limit: usize,
) -> Result<SpooledUpload, EcwProxyError> {
    let mut upload = SpooledUpload::new().await?;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| EcwProxyError::bad_request("Invalid request body"))?;
        if let Some(data) = frame.data_ref() {
            upload.write_chunk_with_limit(data, limit).await?;
        }
    }
    upload.rewind().await?;
    Ok(upload)
}

async fn spaced_write_spooled(
    state: &DataPlaneState,
    route: EcwResolvedRoute,
    path: String,
    mut upload: SpooledUpload,
    user: Option<GuestUserName>,
) -> Result<EcwResolvedRoute, EcwProxyError> {
    let route = ensure_parent_dir(state, route, &path, user.clone()).await?;
    upload.rewind().await?;
    let upload_path = upload._path.to_path_buf();

    let (_, route) = bounded_ecw_route_transfer(
        state,
        route,
        "filesystem_write",
        NodeApiOperationBudget::FilesystemTransferOpen.timeout(),
        move |route, _remaining| {
            let upload_path = upload_path.clone();
            let path = path.clone();
            let user = user.clone();
            async move {
                let file = tokio::fs::File::open(upload_path).await.map_err(|error| {
                    tracing::warn!(%error, "failed to reopen ECW upload spool file");
                    tonic::Status::internal("failed to reopen staged upload")
                })?;
                let channel = ecw_backend_channel(state, &route)
                    .await
                    .map_err(EcwRouteAttemptError::Proxy)?;
                let mut client = WorkerApiClient::new(wrap_grpc_client_channel(channel));
                let stream = spooled_write_requests(route.sandbox_id, path, file, user);
                client
                    .spaced_write(node_api_request_with_route_token_state(
                        stream,
                        route.space_api_route_token.as_ref(),
                    )?)
                    .await
                    .map(|response| response.into_inner())
                    .map_err(EcwRouteAttemptError::Status)
            }
        },
        raw_file_write_error,
    )
    .await?;
    Ok(route)
}

#[derive(Debug)]
struct SpooledUpload {
    file: tokio::fs::File,
    _path: tempfile::TempPath,
    bytes_written: usize,
}

impl SpooledUpload {
    async fn new() -> Result<Self, EcwProxyError> {
        let tempfile = tempfile::Builder::new()
            .prefix("space-ecw-upload-")
            .tempfile()
            .map_err(|error| {
                tracing::warn!(%error, "failed to create ECW upload spool file");
                EcwProxyError::internal("Failed to stage upload")
            })?;
        let (file, path) = tempfile.into_parts();
        Ok(Self {
            file: tokio::fs::File::from_std(file),
            _path: path,
            bytes_written: 0,
        })
    }

    async fn write_chunk_with_limit(
        &mut self,
        data: &[u8],
        limit: usize,
    ) -> Result<(), EcwProxyError> {
        self.bytes_written =
            checked_file_content_len_with_limit(self.bytes_written, data.len(), limit)?;
        self.file.write_all(data).await.map_err(|error| {
            tracing::warn!(%error, "failed to write ECW upload spool file");
            EcwProxyError::internal("Failed to stage upload")
        })
    }

    async fn rewind(&mut self) -> Result<(), EcwProxyError> {
        self.file.flush().await.map_err(|error| {
            tracing::warn!(%error, "failed to flush ECW upload spool file");
            EcwProxyError::internal("Failed to stage upload")
        })?;
        self.file.seek(SeekFrom::Start(0)).await.map_err(|error| {
            tracing::warn!(%error, "failed to rewind ECW upload spool file");
            EcwProxyError::internal("Failed to stage upload")
        })?;
        Ok(())
    }
}

struct SpooledWriteState {
    sandbox_id: SandboxId,
    path: String,
    user: Option<GuestUserName>,
    file: tokio::fs::File,
    open_sent: bool,
    done: bool,
}

fn spooled_write_requests(
    sandbox_id: SandboxId,
    path: String,
    file: tokio::fs::File,
    user: Option<GuestUserName>,
) -> impl Stream<Item = SpacedWriteRequest> + Send + 'static {
    stream::unfold(
        SpooledWriteState {
            sandbox_id,
            path,
            user,
            file,
            open_sent: false,
            done: false,
        },
        |mut state| async move {
            if state.done {
                return None;
            }
            if !state.open_sent {
                state.open_sent = true;
                let request = SpacedWriteRequest {
                    sandbox_id: state.sandbox_id.to_string(),
                    operation: Some(spaced_ops::WriteRequest {
                        message: Some(spaced_write_request::Message::Open(SpacedWriteOpen {
                            path: state.path.clone(),
                            append: false,
                            idempotency_key: None,
                            user: state.user.clone().map(|user| user.as_str().to_string()),
                        })),
                    }),
                };
                return Some((request, state));
            }

            let mut chunk = vec![0; E2B_FILE_WRITE_CHUNK_BYTES];
            let read = match state.file.read(&mut chunk).await {
                Ok(read) => read,
                Err(error) => {
                    tracing::warn!(%error, "failed to read ECW upload spool file");
                    return None;
                }
            };
            if read == 0 {
                return None;
            }
            chunk.truncate(read);
            let request = SpacedWriteRequest {
                sandbox_id: state.sandbox_id.to_string(),
                operation: Some(spaced_ops::WriteRequest {
                    message: Some(spaced_write_request::Message::Data(chunk)),
                }),
            };
            Some((request, state))
        },
    )
}

fn checked_file_content_len(current_len: usize, chunk_len: usize) -> Result<usize, EcwProxyError> {
    checked_file_content_len_with_limit(current_len, chunk_len, E2B_FILE_CONTENT_LIMIT_BYTES)
}

fn checked_file_content_len_with_limit(
    current_len: usize,
    chunk_len: usize,
    limit: usize,
) -> Result<usize, EcwProxyError> {
    let next_len = current_len
        .checked_add(chunk_len)
        .ok_or_else(file_too_large_error)?;
    if next_len > limit {
        return Err(file_too_large_error());
    }

    Ok(next_len)
}

#[derive(Clone)]
struct ReplayableSpacedWrite {
    sandbox_id: SandboxId,
    path: String,
    data: Bytes,
    user: Option<GuestUserName>,
}

impl ReplayableSpacedWrite {
    fn new(
        sandbox_id: SandboxId,
        path: String,
        data: Vec<u8>,
        user: Option<GuestUserName>,
    ) -> Self {
        Self {
            sandbox_id,
            path,
            data: Bytes::from(data),
            user,
        }
    }

    fn stream(&self) -> impl Stream<Item = SpacedWriteRequest> + Send + 'static {
        let sandbox_id = self.sandbox_id.to_string();
        let path = self.path.clone();
        let data = self.data.clone();
        let user = self.user.clone();
        let chunk_count = data.len().div_ceil(E2B_FILE_WRITE_CHUNK_BYTES);
        stream::iter(0..=chunk_count).map(move |index| {
            let message = if index == 0 {
                spaced_write_request::Message::Open(SpacedWriteOpen {
                    path: path.clone(),
                    append: false,
                    idempotency_key: None,
                    user: user.as_ref().map(|user| user.as_str().to_string()),
                })
            } else {
                let start = (index - 1) * E2B_FILE_WRITE_CHUNK_BYTES;
                let end = (start + E2B_FILE_WRITE_CHUNK_BYTES).min(data.len());
                let chunk = data.get(start..end).map_or_else(Vec::new, <[u8]>::to_vec);
                spaced_write_request::Message::Data(chunk)
            };
            SpacedWriteRequest {
                sandbox_id: sandbox_id.clone(),
                operation: Some(spaced_ops::WriteRequest {
                    message: Some(message),
                }),
            }
        })
    }
}

fn file_too_large_error() -> EcwProxyError {
    EcwProxyError::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        "File exceeds maximum supported size",
        ResponseOrigin::Client,
        ResponseKind::Validation,
    )
}

fn json_file_write_body_too_large_error() -> EcwProxyError {
    EcwProxyError::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        "JSON file writes are limited to 1 MiB; use multipart/form-data or application/octet-stream for larger files",
        ResponseOrigin::Client,
        ResponseKind::Validation,
    )
}

async fn read_json_file_write_request(
    headers: &HeaderMap,
    body: Body,
) -> Result<FileWriteRequestE2b, EcwProxyError> {
    reject_oversized_json_content_length(headers)?;
    let body = buffer_json_file_write_body(body).await?;
    serde_json::from_slice(&body)
        .map_err(|_| EcwProxyError::bad_request("Invalid JSON file write request"))
}

async fn buffer_json_file_write_body(mut body: Body) -> Result<Bytes, EcwProxyError> {
    let mut buffered = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| EcwProxyError::bad_request("Invalid request body"))?;
        let Some(data) = frame.data_ref() else {
            continue;
        };
        let next_len = buffered
            .len()
            .checked_add(data.len())
            .ok_or_else(json_file_write_body_too_large_error)?;
        if next_len > E2B_JSON_FILE_WRITE_BODY_LIMIT_BYTES {
            return Err(json_file_write_body_too_large_error());
        }
        buffered.extend_from_slice(data);
    }
    Ok(Bytes::from(buffered))
}

fn file_write_content_to_bytes(content: Value) -> Result<Vec<u8>, EcwProxyError> {
    match content {
        Value::String(value) => Ok(value.into_bytes()),
        Value::Array(values) => values
            .into_iter()
            .map(|value| {
                value
                    .as_u64()
                    .filter(|value| *value <= u8::MAX as u64)
                    .map(|value| value as u8)
                    .ok_or_else(|| EcwProxyError::bad_request("Invalid byte array content"))
            })
            .collect(),
        other => serde_json::to_vec(&other)
            .map_err(|_| EcwProxyError::bad_request("Invalid JSON file content")),
    }
}

fn is_json_content_type(content_type: &str) -> bool {
    content_type.starts_with("application/json")
}

fn is_octet_stream_content_type(content_type: &str) -> bool {
    content_type.starts_with("application/octet-stream")
}

fn multipart_boundary(content_type: &str) -> Option<&str> {
    content_type
        .split(';')
        .skip(1)
        .find_map(|part| part.trim().strip_prefix("boundary="))
        .map(|boundary| boundary.trim_matches('"'))
        .filter(|boundary| !boundary.is_empty())
}

async fn spool_multipart_uploads(
    body: Body,
    boundary: &str,
) -> Result<Vec<MultipartUploadPartE2b>, EcwProxyError> {
    spool_multipart_uploads_with_limit(body, boundary, E2B_FILE_CONTENT_LIMIT_BYTES).await
}

async fn spool_multipart_uploads_with_limit(
    body: Body,
    boundary: &str,
    limit: usize,
) -> Result<Vec<MultipartUploadPartE2b>, EcwProxyError> {
    // Capping the whole body, not each part, keeps the pre-streaming contract
    // and bounds the spool disk a single request can use across all its parts.
    let constraints =
        multer::Constraints::new().size_limit(multer::SizeLimit::new().whole_stream(limit as u64));
    let field_bytes = Arc::new(AtomicUsize::new(0));
    let mut multipart = multer::Multipart::with_constraints(
        fair_body_data_stream(body, Arc::clone(&field_bytes)),
        boundary,
        constraints,
    );
    let mut uploads = Vec::new();
    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(multipart_upload_error)?
    {
        if !field.headers().contains_key(header::CONTENT_DISPOSITION) {
            return Err(EcwProxyError::bad_request(
                "Missing multipart Content-Disposition",
            ));
        }
        let is_file = field.name() == Some("file");
        let filename = field
            .file_name()
            .filter(|filename| !filename.is_empty())
            .map(str::to_string);
        let mut upload = if is_file {
            Some(SpooledUpload::new().await?)
        } else {
            None
        };
        while let Some(chunk) = field.chunk().await.map_err(multipart_upload_error)? {
            field_bytes.fetch_add(chunk.len(), Ordering::Relaxed);
            if let Some(upload) = upload.as_mut() {
                upload.write_chunk_with_limit(&chunk, limit).await?;
            }
        }
        if let Some(mut upload) = upload {
            upload.rewind().await?;
            uploads.push(MultipartUploadPartE2b { filename, upload });
        }
    }
    Ok(uploads)
}

fn multipart_upload_error(error: multer::Error) -> EcwProxyError {
    if matches!(error, multer::Error::StreamSizeExceeded { .. }) {
        return file_too_large_error();
    }
    EcwProxyError::bad_request("Invalid multipart body")
}

fn fair_body_data_stream(
    body: Body,
    field_bytes: Arc<AtomicUsize>,
) -> impl Stream<Item = Result<Bytes, axum::Error>> + Send + 'static {
    // Multer drains every immediately-ready frame into its parse buffer. Force
    // one pending poll between frames so request bursts stay frame-bounded.
    let mut body = Box::pin(body.into_data_stream());
    let mut yielded_frame = false;
    let mut fed_bytes = 0usize;
    stream::poll_fn(move |context| {
        if yielded_frame {
            yielded_frame = false;
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        // Bytes fed to multer but not yet yielded as field data are still in
        // its parse buffer, so this bounds that buffer to the cap plus a frame.
        let unparsed = fed_bytes.saturating_sub(field_bytes.load(Ordering::Relaxed));
        if unparsed > E2B_MULTIPART_PARSE_BUFFER_LIMIT_BYTES {
            return Poll::Ready(Some(Err(axum::Error::new(
                "multipart headers or preamble exceed the parse buffer limit",
            ))));
        }
        let next = body.as_mut().poll_next(context);
        if let Poll::Ready(Some(Ok(data))) = &next {
            fed_bytes = fed_bytes.saturating_add(data.len());
            yielded_frame = true;
        }
        next
    })
}

fn resolve_multipart_uploads(
    uploads: Vec<MultipartUploadPartE2b>,
    query_path: Option<String>,
    user: Option<&GuestUserName>,
) -> Result<Vec<(String, SpooledUpload)>, EcwProxyError> {
    if query_path.is_some() && uploads.len() > 1 {
        return Err(EcwProxyError::bad_request(
            "path query parameter only supports a single multipart upload",
        ));
    }
    uploads
        .into_iter()
        .map(|upload| {
            let path = match (&query_path, upload.filename) {
                (Some(path), _) => path.clone(),
                (None, Some(filename)) => filename,
                (None, None) => {
                    return Err(EcwProxyError::bad_request(
                        "multipart uploads require a filename or path query",
                    ));
                }
            };
            Ok((e2b_request_root(&path, user)?, upload.upload))
        })
        .collect()
}

fn file_info_to_connect_entry(info: SpacedFileInfo) -> FilesystemEntryInfoE2b {
    FilesystemEntryInfoE2b {
        name: info
            .name
            .unwrap_or_else(|| path_basename(&info.path).to_string()),
        file_type: native_file_type_to_e2b(info.file_type),
        path: info.path,
        size: info.size_bytes.min(i64::MAX as u64) as i64,
        mode: info.mode,
        modified_time: info.modified_at.and_then(timestamp_to_rfc3339),
        symlink_target: info.symlink_target,
    }
}

fn native_file_type_to_e2b(file_type: i32) -> i32 {
    match SpacedFileType::try_from(file_type) {
        Ok(SpacedFileType::File) => 1,
        Ok(SpacedFileType::Directory) => 2,
        Ok(SpacedFileType::Symlink) => 3,
        Ok(SpacedFileType::Other) | Ok(SpacedFileType::Unspecified) | Err(_) => 0,
    }
}

fn timestamp_to_rfc3339(value: prost_types::Timestamp) -> Option<String> {
    let timestamp = OffsetDateTime::from_unix_timestamp(value.seconds)
        .ok()?
        .checked_add(TimeDuration::nanoseconds(value.nanos.into()))?;
    timestamp.format(&Rfc3339).ok()
}

fn path_basename(path: &str) -> &str {
    path.rsplit('/')
        .find(|segment| !segment.is_empty())
        .unwrap_or(path)
}

fn e2b_parent_dir(path: &str) -> Option<String> {
    let trimmed = path.trim_end_matches('/');
    let idx = trimmed.rfind('/')?;
    if idx == 0 {
        return None;
    }
    trimmed.get(..idx).map(ToOwned::to_owned)
}

pub(super) async fn e2b_watch_dir(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let route = match resolve_ecw_envd_route(&state, &headers).await {
        Ok(route) => route,
        Err(error) => return error.into_response(),
    };
    let user = match extract_basic_user(&headers) {
        Ok(user) => user,
        Err(error) => return error.into_response(),
    };
    let request = match decode_watch_dir_request(&headers, body).await {
        Ok(request) => request,
        Err(error) => return error.into_response(),
    };
    let requested_root = match e2b_request_root(&request.path, user.as_ref()) {
        Ok(root) => root,
        Err(error) => return error.into_response(),
    };
    let (root, route) =
        match normalized_e2b_watch_root(&state, route, requested_root, user.clone()).await {
            Ok(normalized) => normalized,
            Err(error) => return error.into_response(),
        };
    let stream_permit = match acquire_ecw_stream_permit(&state, ECW_FILE_WATCH_STREAM_KIND) {
        Ok(permit) => permit,
        Err(error) => return error.into_response(),
    };
    let message = SpacedWatchRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::WatchRequest {
            path: root.clone(),
            recursive: request.recursive,
            include_metadata_events: true,
            include_globs: Vec::new(),
            exclude_globs: Vec::new(),
            use_default_excludes: None,
            user: user.map(|user| user.as_str().to_string()),
        }),
    };
    // Opening the watch is a unary call whose stream has not yielded anything
    // yet, so a superseded-identity rejection can be recovered here the same way
    // as any other unary; only an already-flowing stream cannot.
    let (stream, _route) = match bounded_node_api_unary(
        &state,
        route,
        "filesystem_watch_open",
        NodeApiOperationBudget::FilesystemTransferOpen.timeout(),
        message,
        |channel, request| async move {
            WorkerApiClient::new(wrap_grpc_client_channel(channel))
                .spaced_watch(request)
                .await
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await
    {
        Ok(stream) => stream,
        Err(error) => return error.into_response(),
    };
    let keepalive_secs = headers
        .get("Keepalive-Ping-Interval")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .unwrap_or(90);
    let keepalive_interval = StdDuration::from_secs(keepalive_secs);
    let mut interval = tokio::time::interval_at(
        tokio::time::Instant::now() + keepalive_interval,
        keepalive_interval,
    );
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let framed = e2b_watch_connect_stream(stream, interval, root, stream_permit);
    ecw_connect::connect_json_stream_response(framed)
}

fn e2b_watch_connect_stream<S>(
    stream: S,
    interval: tokio::time::Interval,
    root: String,
    stream_permit: AttachedStreamPermit,
) -> E2bWatchConnectStream<S>
where
    S: Stream<Item = Result<SpacedFileEvent, tonic::Status>>,
{
    E2bWatchConnectStream {
        inner: Box::pin(stream),
        interval,
        root,
        started: false,
        ended: false,
        close_recorded: false,
        _stream_permit: stream_permit,
    }
}

struct E2bWatchConnectStream<S> {
    inner: Pin<Box<S>>,
    interval: tokio::time::Interval,
    root: String,
    started: bool,
    ended: bool,
    close_recorded: bool,
    _stream_permit: AttachedStreamPermit,
}

impl<S> E2bWatchConnectStream<S> {
    fn record_close(&mut self, result: &'static str) {
        if self.close_recorded {
            return;
        }
        self.close_recorded = true;
        pplx_space_metrics::data_plane::stream_closed(ECW_FILE_WATCH_STREAM_KIND, result);
    }
}

impl<S> Drop for E2bWatchConnectStream<S> {
    fn drop(&mut self) {
        if !self.close_recorded {
            pplx_space_metrics::data_plane::stream_closed(
                ECW_FILE_WATCH_STREAM_KIND,
                "client_closed",
            );
        }
    }
}

impl<S> Stream for E2bWatchConnectStream<S>
where
    S: Stream<Item = Result<SpacedFileEvent, tonic::Status>>,
{
    type Item = Result<Bytes, Infallible>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }

        if !this.started {
            this.started = true;
            let bytes = ecw_connect::connect_json_message(json!({ "start": {} }), false);
            record_ecw_watch_stream_bytes(&bytes);
            return Poll::Ready(Some(Ok(bytes)));
        }

        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(event))) => {
                    if let Some(event) = e2b_event_from_spaced(&this.root, event) {
                        let bytes = ecw_connect::connect_json_message(
                            watch_dir_filesystem_payload(&event),
                            false,
                        );
                        record_ecw_watch_stream_bytes(&bytes);
                        return Poll::Ready(Some(Ok(bytes)));
                    }
                }
                Poll::Ready(Some(Err(status))) => {
                    this.ended = true;
                    this.record_close("backend_status");
                    let bytes =
                        ecw_connect::connect_error_end_stream(status.code(), status.message());
                    record_ecw_watch_stream_bytes(&bytes);
                    return Poll::Ready(Some(Ok(bytes)));
                }
                Poll::Ready(None) => {
                    this.ended = true;
                    this.record_close("ok");
                    return Poll::Ready(None);
                }
                Poll::Pending => match this.interval.poll_tick(cx) {
                    Poll::Ready(_) => {
                        let bytes =
                            ecw_connect::connect_json_message(json!({ "keepalive": {} }), false);
                        record_ecw_watch_stream_bytes(&bytes);
                        return Poll::Ready(Some(Ok(bytes)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}

fn record_ecw_watch_stream_bytes(bytes: &Bytes) {
    pplx_space_metrics::data_plane::stream_bytes_out(
        ECW_FILE_WATCH_STREAM_KIND,
        bytes.len() as u64,
    );
}

async fn decode_watch_dir_request(
    headers: &HeaderMap,
    body: Body,
) -> Result<E2bWatchDirRequest, EcwProxyError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if content_type.starts_with("application/connect+proto") {
        return decode_e2b_proto(body).await;
    }

    ecw_connect::decode_connect_json_request::<E2bWatchDirRequest>(body).await
}

async fn decode_e2b_proto<T: Message + Default>(body: Body) -> Result<T, EcwProxyError> {
    let bytes = to_bytes(body, E2B_PROTO_BODY_LIMIT)
        .await
        .map_err(|error| EcwProxyError::bad_request(format!("Invalid request body: {error}")))?;
    T::decode(bytes)
        .map_err(|error| EcwProxyError::bad_request(format!("Invalid protobuf: {error}")))
}

async fn normalized_e2b_watch_root(
    state: &DataPlaneState,
    route: EcwResolvedRoute,
    root: String,
    user: Option<GuestUserName>,
) -> Result<(String, EcwResolvedRoute), EcwProxyError> {
    let message = SpacedNormalizeWatchRootRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::NormalizeWatchRootRequest {
            path: root,
            user: user.map(|user| user.as_str().to_string()),
        }),
    };
    let (response, route) = bounded_node_api_unary(
        state,
        route,
        "filesystem_normalize_watch_root",
        NodeApiOperationBudget::FilesystemMetadata.timeout(),
        message,
        |channel, request| async move {
            WorkerApiClient::new(wrap_grpc_client_channel(channel))
                .spaced_normalize_watch_root(request)
                .await
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await?;
    Ok((response.root, route))
}

pub(super) async fn e2b_create_watcher(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
    Json(request): Json<E2bWatchDirRequest>,
) -> Result<Json<E2bCreateWatcherResponse>, EcwProxyError> {
    let route = resolve_ecw_envd_route(&state, &headers).await?;
    let user = extract_basic_user(&headers)?;
    let message = SpacedCreateWatcherRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::CreateWatcherRequest {
            root: e2b_request_root(&request.path, user.as_ref())?,
            recursive: request.recursive,
            include_metadata_events: true,
            include_globs: Vec::new(),
            exclude_globs: Vec::new(),
            use_default_excludes: None,
            user: user.map(|user| user.as_str().to_string()),
        }),
    };
    let (response, _route) = bounded_node_api_unary(
        &state,
        route,
        "filesystem_create_watcher",
        NodeApiOperationBudget::FilesystemMutation.timeout(),
        message,
        |channel, request| async move {
            WorkerApiClient::new(wrap_grpc_client_channel(channel))
                .spaced_create_watcher(request)
                .await
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await?;
    let watcher_id = encode_e2b_watcher_id(&response.watcher_id, &response.root);
    Ok(Json(E2bCreateWatcherResponse { watcher_id }))
}

pub(super) async fn e2b_get_watcher_events(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
    Json(request): Json<E2bGetWatcherEventsRequest>,
) -> Result<Json<E2bGetWatcherEventsResponseJson>, EcwProxyError> {
    let route = resolve_ecw_envd_route(&state, &headers).await?;
    let (watcher_id, root) = decode_e2b_watcher_id(&request.watcher_id)?;
    let message = SpacedGetWatcherEventsRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::GetWatcherEventsRequest { watcher_id }),
    };
    let (response, _route) = bounded_node_api_unary(
        &state,
        route,
        "filesystem_get_watcher_events",
        NodeApiOperationBudget::FilesystemMetadata.timeout(),
        message,
        |channel, request| async move {
            WorkerApiClient::new(wrap_grpc_client_channel(channel))
                .spaced_get_watcher_events(request)
                .await
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await?;
    let events = response
        .events
        .into_iter()
        .filter_map(|event| e2b_event_from_spaced(&root, event))
        .map(|event| E2bFilesystemEventJson {
            name: event.name,
            event_type: E2bEventType::try_from(event.event_type)
                .unwrap_or(E2bEventType::Unspecified)
                .as_str(),
        })
        .collect();
    Ok(Json(E2bGetWatcherEventsResponseJson { events }))
}

pub(super) async fn e2b_remove_watcher(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
    Json(request): Json<E2bRemoveWatcherRequest>,
) -> Result<Json<E2bRemoveWatcherResponse>, EcwProxyError> {
    let route = resolve_ecw_envd_route(&state, &headers).await?;
    let (watcher_id, _) = decode_e2b_watcher_id(&request.watcher_id)?;
    let message = SpacedRemoveWatcherRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::RemoveWatcherRequest { watcher_id }),
    };
    bounded_node_api_unary(
        &state,
        route,
        "filesystem_remove_watcher",
        NodeApiOperationBudget::FilesystemMutation.timeout(),
        message,
        |channel, request| async move {
            WorkerApiClient::new(wrap_grpc_client_channel(channel))
                .spaced_remove_watcher(request)
                .await
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await?;
    Ok(Json(E2bRemoveWatcherResponse {}))
}

fn e2b_request_root(path: &str, _user: Option<&GuestUserName>) -> Result<String, EcwProxyError> {
    validate_linux_guest_path_input(path)
        .map_err(|error| EcwProxyError::bad_request(error.to_string()))?;
    Ok(path.to_string())
}

fn encode_e2b_watcher_id(native_id: &str, root: &str) -> String {
    let root = URL_SAFE_NO_PAD.encode(root.as_bytes());
    format!("{native_id}.{root}")
}

fn decode_e2b_watcher_id(watcher_id: &str) -> Result<(String, String), EcwProxyError> {
    let Some((native_id, root)) = watcher_id.split_once('.') else {
        return Err(EcwProxyError::bad_request("Invalid watcher id"));
    };
    let root = URL_SAFE_NO_PAD
        .decode(root)
        .map_err(|_| EcwProxyError::bad_request("Invalid watcher id"))?;
    let root =
        String::from_utf8(root).map_err(|_| EcwProxyError::bad_request("Invalid watcher id"))?;
    Ok((native_id.to_string(), root))
}

fn e2b_event_from_spaced(root: &str, event: SpacedFileEvent) -> Option<E2bFilesystemEvent> {
    let event_type = match SpacedFileEventKind::try_from(event.kind).ok()? {
        SpacedFileEventKind::Created => E2bEventType::Create,
        SpacedFileEventKind::Modified => E2bEventType::Write,
        SpacedFileEventKind::Removed => E2bEventType::Remove,
        SpacedFileEventKind::Renamed => E2bEventType::Rename,
        SpacedFileEventKind::Chmod => E2bEventType::Chmod,
        SpacedFileEventKind::Unspecified => return None,
    };
    let name = e2b_event_name(root, &event.path)?;
    Some(E2bFilesystemEvent {
        name,
        event_type: event_type as i32,
    })
}

fn e2b_event_name(root: &str, path: &str) -> Option<String> {
    if path.is_empty() {
        return None;
    }
    if !path.starts_with('/') {
        return Some(path.strip_prefix("./").unwrap_or(path).to_string());
    }

    let root = normalized_absolute_path(root);
    let path = normalized_absolute_path(path);
    if path == root {
        return Some(".".to_string());
    }

    let root_prefix = if root == "/" {
        "/".to_string()
    } else {
        format!("{root}/")
    };
    path.strip_prefix(&root_prefix).map(str::to_string)
}

fn normalized_absolute_path(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        trimmed.to_string()
    }
}

fn watch_dir_filesystem_payload(event: &E2bFilesystemEvent) -> Value {
    let event_type = E2bEventType::try_from(event.event_type)
        .unwrap_or(E2bEventType::Unspecified)
        .as_str();
    json!({
        "filesystem": {
            "name": event.name,
            "type": event_type,
        }
    })
}

fn envd_basic_username(headers: &HeaderMap) -> Option<String> {
    crate::ecw_process::envd_basic_username(headers)
}

fn raw_file_read_error(status: tonic::Status) -> EcwProxyError {
    let error = ecw_connect::tonic_status_to_ecw_error(status);
    if error.status == StatusCode::CONFLICT {
        return EcwProxyError::sandbox_timeout()
            .with_classification(ResponseOrigin::Spaced, ResponseKind::BackendStatus);
    }
    error
}

fn raw_file_write_error(status: tonic::Status) -> EcwProxyError {
    raw_file_read_error(status)
}

#[cfg(test)]
mod tests;
