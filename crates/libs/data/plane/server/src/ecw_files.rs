use std::{
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
    time::Duration as StdDuration,
};

use axum::{
    Extension, Json,
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
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{
    AttachedStreamPermit, DataPlaneState, ECW_FILE_WATCH_STREAM_KIND, EcwProxyError,
    EcwResolvedRoute, ResponseKind, ResponseOrigin, acquire_ecw_stream_permit, ecw_backend_channel,
    ecw_connect, node_api_request_with_route_token_state, resolve_ecw_envd_route,
};
use crate::ecw_process::EmptyUnaryConnectResponseE2b;
use crate::http_traffic::RequestBodyBytes;
use crate::runtime_route::{
    EcwRouteAttemptError, bounded_ecw_route_transfer, bounded_node_api_unary,
    retryable_bounded_node_api_unary,
};
use crate::upload_admission::UploadPermit;
use multipart::{multipart_boundary, resolve_multipart_uploads, spool_multipart_uploads};

const E2B_FILE_CONTENT_LIMIT_BYTES: usize = 256 * 1024 * 1024;
const E2B_FILE_WRITE_CHUNK_BYTES: usize = 3 * 1024 * 1024;
const E2B_PROTO_BODY_LIMIT: usize = 1024 * 1024;
/// Stated as "1 MiB" in `json_file_write_body_too_large_error`.
const E2B_JSON_FILE_WRITE_BODY_LIMIT_BYTES: usize = 1024 * 1024;

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
    request_bytes: Option<Extension<RequestBodyBytes>>,
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
    // Reject what the headers and query alone can before admission, so a
    // malformed request never waits for or holds an upload permit.
    let (write_body, declared_len) = if is_octet_stream_content_type(content_type) {
        let declared_len = reject_oversized_content_length(
            &headers,
            E2B_FILE_CONTENT_LIMIT_BYTES,
            file_too_large_error,
        )?;
        let Some(path) = query.path.as_deref() else {
            return Err(EcwProxyError::bad_request(
                "path query parameter is required for octet-stream uploads",
            ));
        };
        let path = e2b_request_root(path, user.as_ref())?;
        (WriteBody::OctetStream(path), declared_len)
    } else if is_json_content_type(content_type) {
        let declared_len = reject_oversized_content_length(
            &headers,
            E2B_JSON_FILE_WRITE_BODY_LIMIT_BYTES,
            json_file_write_body_too_large_error,
        )?;
        (WriteBody::Json, declared_len)
    } else if content_type.starts_with("multipart/form-data") {
        let declared_len = reject_oversized_content_length(
            &headers,
            E2B_FILE_CONTENT_LIMIT_BYTES,
            file_too_large_error,
        )?;
        // A query path names the destination, so check it before reading the
        // body rather than after spooling the upload.
        if let Some(path) = &query.path {
            e2b_request_root(path, user.as_ref())?;
        }
        let boundary = multipart_boundary(content_type).ok_or_else(|| {
            EcwProxyError::bad_request(
                "multipart/form-data boundary of 1 to 70 characters is required",
            )
        })?;
        (WriteBody::Multipart(boundary), declared_len)
    } else {
        return Err(EcwProxyError::bad_request(
            "Unsupported Content-Type for file write",
        ));
    };
    // Admit after authentication so unauthenticated requests can't queue for
    // upload slots, and before the body is read so a rejected upload costs
    // the pod no body bytes.
    // Every write path stops reading at its limit, so an undeclared length
    // reserves the whole of it.
    let reserve = declared_len.unwrap_or_else(|| write_body.body_limit()) as u64;
    let upload_permit = match &state.uploads {
        Some(uploads) => Some(uploads.admit(reserve).await?),
        None => None,
    };
    // Track what admission let through, counting the budget it actually holds,
    // so the in-flight gauges measure the load the limiter governs. Each
    // record names its org.
    let tracked = state.upload_tracker.begin(
        upload_permit
            .as_ref()
            .map_or(reserve, UploadPermit::reserved_bytes),
        route.sandbox_id,
        route.org_id,
        request_bytes.map(|Extension(request_bytes)| request_bytes),
    );

    // An async block keeps each `?` below inside it, so every outcome reaches
    // `finish` rather than returning past it as a cancelled upload.
    let result: Result<Json<Vec<FileWriteInfoE2b>>, EcwProxyError> = async {
        match write_body {
            WriteBody::OctetStream(path) => {
                // Create the parent before reading the body, so a destination whose
                // parent can't be created fails without spooling the upload. Other
                // write failures still surface after spooling.
                let route = ensure_parent_dir(&state, route, &path, user.clone()).await?;
                let mut spool = SpoolFiles::default();
                let upload = spool_body_upload(body, &mut spool).await?;
                spaced_write_spooled(&state, route, path.clone(), &upload, user).await?;
                Ok(Json(vec![file_write_info(path)]))
            }
            WriteBody::Json => {
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
                Ok(Json(vec![file_write_info(path)]))
            }
            WriteBody::Multipart(boundary) => {
                // Owns every staged file until the request ends, however it ends.
                let mut spool = SpoolFiles::default();
                let uploads =
                    spool_multipart_uploads(body, boundary, query.path.is_none(), &mut spool)
                        .await?;
                let writes = resolve_multipart_uploads(uploads, query.path, user.as_ref())?;
                let mut written = Vec::with_capacity(writes.len());
                let mut route = route;
                for (path, upload) in writes {
                    route = ensure_parent_dir(&state, route, &path, user.clone()).await?;
                    route =
                        spaced_write_spooled(&state, route, path.clone(), &upload, user.clone())
                            .await?;
                    written.push(file_write_info(path));
                }
                Ok(Json(written))
            }
        }
    }
    .await;
    tracked.finish(match &result {
        Ok(_) => StatusCode::OK,
        Err(error) => error.status,
    });
    result
}

/// How a `POST /files` body is carried, decided from headers alone.
enum WriteBody<'a> {
    /// Spooled to disk, then streamed to the guest; carries the validated
    /// destination path.
    OctetStream(String),
    /// Buffered in memory, up to the JSON write limit.
    Json,
    /// Spooled to disk part by part; carries the boundary.
    Multipart(&'a str),
}

impl WriteBody<'_> {
    /// The most body this write path reads.
    fn body_limit(&self) -> usize {
        match self {
            Self::OctetStream(_) | Self::Multipart(_) => E2B_FILE_CONTENT_LIMIT_BYTES,
            Self::Json => E2B_JSON_FILE_WRITE_BODY_LIMIT_BYTES,
        }
    }
}

fn file_write_info(path: String) -> FileWriteInfoE2b {
    FileWriteInfoE2b {
        name: path_basename(&path).to_string(),
        file_type: "file",
        path,
    }
}

/// Rejects a declared `Content-Length` that is unparsable or over `limit`, and
/// returns the declared length, if any.
fn reject_oversized_content_length(
    headers: &HeaderMap,
    limit: usize,
    too_large: fn() -> EcwProxyError,
) -> Result<Option<usize>, EcwProxyError> {
    let Some(content_length) = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let content_length = content_length
        .parse::<usize>()
        .map_err(|_| EcwProxyError::bad_request("Invalid Content-Length"))?;
    if content_length > limit {
        return Err(too_large());
    }
    Ok(Some(content_length))
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

async fn spool_body_upload(
    mut body: Body,
    spool: &mut SpoolFiles,
) -> Result<StagedUpload, EcwProxyError> {
    let mut upload = spool.create().await?;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| EcwProxyError::bad_request("Invalid request body"))?;
        if let Some(data) = frame.data_ref() {
            upload.write_chunk(data).await?;
        }
    }
    upload.finish().await
}

/// Streams a staged upload to the guest. Fails, rather than reporting
/// success, if the spool file can't be read to the end or the guest
/// acknowledges fewer bytes than were staged.
async fn spaced_write_spooled(
    state: &DataPlaneState,
    route: EcwResolvedRoute,
    path: String,
    upload: &StagedUpload,
    user: Option<GuestUserName>,
) -> Result<EcwResolvedRoute, EcwProxyError> {
    // Callers create the parent directory first. The spool file stays on disk
    // until the request's `SpoolFiles` drops, so every attempt can reopen it.
    let upload_path = upload.path.clone();
    let (response, route) = bounded_ecw_route_transfer(
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
                let (read_failed, read_failure) = tokio::sync::oneshot::channel();
                let stream =
                    spooled_write_requests(route.sandbox_id, path, file, user, read_failed);
                let write = client.spaced_write(node_api_request_with_route_token_state(
                    stream,
                    route.space_api_route_token.as_ref(),
                )?);
                fail_on_spool_read_failure(write, read_failure).await
            }
        },
        raw_file_write_error,
    )
    .await?;
    check_spooled_write_len(response.bytes_written, upload.len)?;
    Ok(route)
}

/// Runs the write call until it finishes or `spooled_write_requests` reports
/// a spool read failure. On a read failure the call is dropped and the HTTP
/// request fails, instead of the stream ending cleanly, which would make the
/// guest finalize the prefix as a complete write. A dropped call still doesn't
/// guarantee the guest discards that prefix: it writes the target in place,
/// and the sandbox manager can end the guest's input cleanly once its own
/// request stream fails.
async fn fail_on_spool_read_failure<W>(
    write: W,
    read_failure: tokio::sync::oneshot::Receiver<()>,
) -> Result<spaced_ops::WriteResponse, EcwRouteAttemptError>
where
    W: std::future::Future<
            Output = Result<tonic::Response<spaced_ops::WriteResponse>, tonic::Status>,
        >,
{
    tokio::select! {
        response = write => response
            .map(tonic::Response::into_inner)
            .map_err(EcwRouteAttemptError::Status),
        // A dropped sender (the stream finished) disables this arm.
        Ok(()) = read_failure => Err(EcwRouteAttemptError::Proxy(
            EcwProxyError::internal("Failed to read staged upload"),
        )),
    }
}

fn check_spooled_write_len(bytes_written: u64, staged_len: usize) -> Result<(), EcwProxyError> {
    if u64::try_from(staged_len).is_ok_and(|staged_len| staged_len == bytes_written) {
        return Ok(());
    }
    tracing::warn!(
        bytes_written,
        staged_len,
        "guest acknowledged a different length than the staged upload"
    );
    Err(EcwProxyError::internal(
        "Staged upload was not written completely",
    ))
}

/// Spool files staged by one request, removed when this guard drops: on
/// success, error, and cancellation alike. Removal normally runs in one
/// blocking task, so a request that staged many parts doesn't unlink them all
/// on a runtime thread; it runs inline only while unwinding or without a
/// runtime.
#[derive(Debug, Default)]
pub(super) struct SpoolFiles {
    paths: Vec<tempfile::TempPath>,
    dir: Option<std::path::PathBuf>,
}

impl SpoolFiles {
    #[cfg(test)]
    pub(super) fn in_dir(dir: std::path::PathBuf) -> Self {
        Self {
            paths: Vec::new(),
            dir: Some(dir),
        }
    }

    /// Creates a spool file this guard owns from the start, so even a part
    /// abandoned mid-write is removed with the rest.
    pub(super) async fn create(&mut self) -> Result<SpooledUpload, EcwProxyError> {
        let dir = self.dir.clone().unwrap_or_else(std::env::temp_dir);
        // Creating the file is a blocking syscall; keep it off the runtime.
        let tempfile = tokio::task::spawn_blocking(move || {
            tempfile::Builder::new()
                .prefix("space-ecw-upload-")
                .tempfile_in(dir)
        })
        .await
        .map_err(|error| {
            tracing::warn!(%error, "ECW upload spool file creation task failed");
            EcwProxyError::internal("Failed to stage upload")
        })?
        .map_err(|error| {
            tracing::warn!(%error, "failed to create ECW upload spool file");
            EcwProxyError::internal("Failed to stage upload")
        })?;
        let (file, path) = tempfile.into_parts();
        let upload = SpooledUpload {
            file: tokio::fs::File::from_std(file),
            path: path.to_path_buf(),
            bytes_written: 0,
        };
        self.paths.push(path);
        Ok(upload)
    }
}

impl Drop for SpoolFiles {
    fn drop(&mut self) {
        let paths = std::mem::take(&mut self.paths);
        if paths.is_empty() {
            return;
        }
        let remove = move || {
            for path in paths {
                if let Err(error) = path.close() {
                    tracing::warn!(%error, "failed to remove ECW upload spool file");
                }
            }
        };
        // While unwinding, a failure to spawn the blocking task would panic
        // again and abort, so remove inline instead.
        if std::thread::panicking() {
            remove();
            return;
        }
        match tokio::runtime::Handle::try_current() {
            // Detached: removal needs nothing from the request that dropped it.
            Ok(runtime) => drop(runtime.spawn_blocking(remove)),
            Err(_) => remove(),
        }
    }
}

/// A finished spool file: its path, owned by a `SpoolFiles`, and its length.
#[derive(Debug)]
pub(super) struct StagedUpload {
    pub(super) path: std::path::PathBuf,
    pub(super) len: usize,
}

#[derive(Debug)]
pub(super) struct SpooledUpload {
    file: tokio::fs::File,
    path: std::path::PathBuf,
    bytes_written: usize,
}

impl SpooledUpload {
    pub(super) async fn write_chunk(&mut self, data: &[u8]) -> Result<(), EcwProxyError> {
        self.bytes_written = checked_file_content_len(self.bytes_written, data.len())?;
        self.file.write_all(data).await.map_err(|error| {
            tracing::warn!(%error, "failed to write ECW upload spool file");
            EcwProxyError::internal("Failed to stage upload")
        })
    }

    /// Flushes and closes the spool file, keeping only its path and length.
    /// Writers reopen it by path, and a request staging many parts must not
    /// hold a descriptor for each one.
    pub(super) async fn finish(mut self) -> Result<StagedUpload, EcwProxyError> {
        self.file.flush().await.map_err(|error| {
            tracing::warn!(%error, "failed to flush ECW upload spool file");
            EcwProxyError::internal("Failed to stage upload")
        })?;
        Ok(StagedUpload {
            path: self.path,
            len: self.bytes_written,
        })
    }
}

struct SpooledWriteState<R> {
    sandbox_id: SandboxId,
    path: String,
    user: Option<GuestUserName>,
    file: R,
    read_failed: Option<tokio::sync::oneshot::Sender<()>>,
    open_sent: bool,
    done: bool,
}

/// The open message, then the spool file in `E2B_FILE_WRITE_CHUNK_BYTES`
/// chunks. A read error signals `read_failed` and leaves the stream pending
/// instead of ending it, so the write fails through
/// `fail_on_spool_read_failure` rather than ending as if the file were
/// complete.
fn spooled_write_requests<R>(
    sandbox_id: SandboxId,
    path: String,
    file: R,
    user: Option<GuestUserName>,
    read_failed: tokio::sync::oneshot::Sender<()>,
) -> impl Stream<Item = SpacedWriteRequest> + Send + 'static
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    stream::unfold(
        SpooledWriteState {
            sandbox_id,
            path,
            user,
            file,
            read_failed: Some(read_failed),
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
                    if let Some(read_failed) = state.read_failed.take()
                        && read_failed.send(()).is_err()
                    {
                        // The receiver is gone only once the call has finished.
                        tracing::debug!("ECW upload write finished before the spool read failed");
                    }
                    return std::future::pending().await;
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
    let next_len = current_len
        .checked_add(chunk_len)
        .ok_or_else(file_too_large_error)?;
    if next_len > E2B_FILE_CONTENT_LIMIT_BYTES {
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
    reject_oversized_content_length(
        headers,
        E2B_JSON_FILE_WRITE_BODY_LIMIT_BYTES,
        json_file_write_body_too_large_error,
    )?;
    let body = buffer_json_file_write_body(body).await?;
    serde_json::from_slice(&body)
        .map_err(|_| EcwProxyError::bad_request("Invalid JSON file write request"))
}

async fn buffer_json_file_write_body(body: Body) -> Result<Bytes, EcwProxyError> {
    to_bytes(body, E2B_JSON_FILE_WRITE_BODY_LIMIT_BYTES)
        .await
        .map_err(|error| {
            let exceeded_limit = std::error::Error::source(&error)
                .is_some_and(|source| source.is::<http_body_util::LengthLimitError>());
            if exceeded_limit {
                json_file_write_body_too_large_error()
            } else {
                EcwProxyError::bad_request("Invalid request body")
            }
        })
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

mod multipart;
#[cfg(test)]
mod tests;
