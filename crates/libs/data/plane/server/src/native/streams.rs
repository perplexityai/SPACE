//! Opening the backend process and file-watch streams that the native
//! WebSocket and SSE paths share.

use super::*;

/// What the attach that may start a process can do with its input.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ProcessInput {
    /// A WebSocket attach forwards stdin frames to the process.
    Writable,
    /// An SSE stream carries no input, so a requested stdin starts at EOF
    /// instead of as a pipe that nothing can write to or close.
    Closed,
    /// An SSE stream can neither write nor close a pty's input, so it never
    /// starts the process: it only connects to one the guest already runs
    /// under the session's derived id. When the guest has none, the open
    /// fails with an error [`is_missing_guest_process`] recognizes.
    ConnectOnly,
}

/// The guest's own answer that it has no such process, as opposed to a
/// `NotFound` from the node, such as a missing sandbox.
pub(super) fn is_missing_guest_process(error: &StreamBridgeError) -> bool {
    matches!(
        error,
        StreamBridgeError::BackendStatus(status)
            if status.code() == tonic::Code::NotFound
                && pplx_space_common_grpc::is_terminal_guest_process_error(status)
    )
}

pub(super) async fn initial_process_stream(
    client: &mut WorkerApiClient<GrpcClientTrace<Channel>>,
    attached: &AttachedSession,
    session_id: DataPlaneSessionId,
    output_cursor: Option<u64>,
    input: ProcessInput,
) -> Result<tonic::Streaming<ProcessEvent>, StreamBridgeError> {
    if let Some(process_id) = attached.backend_process_id.as_ref() {
        return connect_process_stream(
            client,
            &attached.sandbox_id,
            process_id,
            output_cursor,
            attached.space_api_route_token.as_ref(),
        )
        .await;
    }

    if input == ProcessInput::ConnectOnly {
        return connect_process_stream(
            client,
            &attached.sandbox_id,
            &process_id_for_session(session_id),
            output_cursor,
            attached.space_api_route_token.as_ref(),
        )
        .await;
    }

    let mut start = process_start_request(attached, session_id)?;
    if input == ProcessInput::Closed {
        start.stdin = false;
    }
    let started = start_process_stream(
        client,
        &attached.sandbox_id,
        start,
        attached.space_api_route_token.as_ref(),
    )
    .await;

    match started {
        // The guest already holds the process this session assigned, so the
        // start succeeded earlier and its binding was never persisted. The
        // process id is derived rather than discovered, so recovery does not
        // depend on that binding: connect to the existing process instead of
        // running the command a second time.
        Err(StreamBridgeError::BackendStatus(status))
            if status.code() == tonic::Code::AlreadyExists =>
        {
            connect_process_stream(
                client,
                &attached.sandbox_id,
                &process_id_for_session(session_id),
                output_cursor,
                attached.space_api_route_token.as_ref(),
            )
            .await
        }
        result => result,
    }
}

pub(super) async fn open_native_process_stream(
    state: &DataPlaneState,
    session_id: DataPlaneSessionId,
    attached: &mut AttachedSession,
    output_cursor: Option<u64>,
    input: ProcessInput,
) -> Result<
    (
        WorkerApiClient<GrpcClientTrace<Channel>>,
        tonic::Streaming<ProcessEvent>,
    ),
    StreamBridgeError,
> {
    native_route_call(state, attached, move |route| async move {
        let channel = state
            .backend_channels
            .channel(&route.backend_endpoint, session_id)
            .await?;
        let mut client = WorkerApiClient::new(wrap_grpc_client_channel(channel));
        let stream =
            initial_process_stream(&mut client, &route, session_id, output_cursor, input).await?;
        Ok((client, stream))
    })
    .await
}

/// A backend refusal made before this request did anything (an admission
/// limit, or the sandbox shutting down): retryable, with the backend's retry
/// hint. It says nothing about an earlier attempt or an already running
/// process, so the message claims only that this request was refused. The
/// guest's own wording stays in the logs.
pub(super) fn backend_refusal_http_error(error: &StreamBridgeError) -> Option<Response> {
    let StreamBridgeError::BackendStatus(status) = error else {
        return None;
    };
    if !pplx_space_common_grpc::is_refused_before_start(status) {
        return None;
    }
    let (http, code, message, reason) = if status.code() == tonic::Code::ResourceExhausted {
        // The node's per-sandbox request limiter refuses before start too;
        // name it apart from the guest's own capacity refusal.
        let reason =
            if pplx_space_common_grpc::UdsPerSandboxLimitExceeded::from_status(status).is_some() {
                TooManyRequestsReason::NodeSandboxLimit
            } else {
                TooManyRequestsReason::SandboxAtCapacity
            };
        (
            StatusCode::TOO_MANY_REQUESTS,
            DataPlaneHttpErrorCode::RateLimited,
            "sandbox is at capacity; this request was refused",
            Some(reason),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            DataPlaneHttpErrorCode::ServiceUnavailable,
            "sandbox is not accepting requests right now; this request was refused",
            None,
        )
    };
    let mut response = data_plane_http_error(
        http,
        code,
        message,
        true,
        ResponseOrigin::Node,
        ResponseKind::Admission,
    );
    insert_retry_after(&mut response, pplx_space_common_grpc::retry_after(status));
    if let Some(reason) = reason {
        response.extensions_mut().insert(reason);
    }
    Some(response)
}

pub(super) fn guest_storage_full(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::ResourceExhausted
        && status
            .metadata()
            .get(pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            == Some(pplx_space_common_core::spaced::GUEST_STORAGE_FULL)
}

pub(super) fn process_stream_start_http_error(error: &StreamBridgeError) -> Response {
    if let Some(response) = backend_refusal_http_error(error) {
        return response;
    }
    // The sandbox's disk or quota is full: not retryable until space is freed,
    // and named as such rather than as a generic start failure.
    if let StreamBridgeError::BackendStatus(status) = error
        && guest_storage_full(status)
    {
        return data_plane_http_error(
            StatusCode::BAD_REQUEST,
            DataPlaneHttpErrorCode::InvalidRequest,
            "sandbox storage is full",
            false,
            ResponseOrigin::Node,
            ResponseKind::BackendStatus,
        );
    }
    // Guest spawn failures are terminal for this command. Retrying a missing
    // executable or working directory cannot repair it; route identity repair
    // and transport failures must keep their existing retry contract.
    let permanent = match error {
        StreamBridgeError::BackendStatus(status)
            if pplx_space_common_grpc::is_terminal_guest_process_error(status) =>
        {
            match status.code() {
                tonic::Code::NotFound => {
                    Some((StatusCode::NOT_FOUND, DataPlaneHttpErrorCode::NotFound))
                }
                tonic::Code::InvalidArgument
                | tonic::Code::OutOfRange
                | tonic::Code::FailedPrecondition
                | tonic::Code::ResourceExhausted => Some((
                    StatusCode::BAD_REQUEST,
                    DataPlaneHttpErrorCode::InvalidRequest,
                )),
                tonic::Code::PermissionDenied => {
                    Some((StatusCode::FORBIDDEN, DataPlaneHttpErrorCode::Forbidden))
                }
                _ => None,
            }
        }
        _ => None,
    };
    if let Some((status, code)) = permanent {
        return data_plane_http_error(
            status,
            code,
            "command could not be started",
            false,
            ResponseOrigin::Node,
            ResponseKind::BackendTransport,
        );
    }
    data_plane_http_error(
        StatusCode::SERVICE_UNAVAILABLE,
        DataPlaneHttpErrorCode::StreamUnavailable,
        "process event stream is not available",
        true,
        ResponseOrigin::Node,
        ResponseKind::BackendTransport,
    )
}

/// Public messages for the error frames that report a backend or internal
/// error. That error can name internal endpoints or carry the guest's own
/// wording, so these frames carry one of these instead and the detail goes to
/// the log. Frames about the client's own request carry their own fixed text.
pub(super) const PROCESS_STREAM_FAILED: &str = "process event stream failed";
pub(super) const FILE_WATCH_STREAM_FAILED: &str = "file watch event stream failed";
pub(super) const STREAM_OPEN_FAILED: &str = "event stream could not be opened";
pub(super) const STREAM_OPEN_REFUSED: &str = "this request was refused";
pub(super) const RESIZE_REFUSED: &str = "terminal resize is not supported by this process";

pub(super) fn stream_start_error_frame(error: &StreamBridgeError) -> DataPlaneServerFrame {
    // A refusal made before anything started is retryable here as on SSE.
    if let StreamBridgeError::BackendStatus(status) = error
        && pplx_space_common_grpc::is_refused_before_start(status)
    {
        return DataPlaneServerFrame::Error {
            code: DataPlaneErrorCode::BackendStreamError,
            message: STREAM_OPEN_REFUSED.to_string(),
            retryable: true,
        };
    }
    let retryable = matches!(
        error,
        StreamBridgeError::BackendStatus(status)
            if !pplx_space_common_grpc::is_terminal_guest_process_error(status)
                && backend_stream_status_retryable(status)
    );
    // The bridge logs the error.
    DataPlaneServerFrame::Error {
        code: DataPlaneErrorCode::BackendStreamError,
        message: STREAM_OPEN_FAILED.to_string(),
        retryable,
    }
}

pub(crate) fn backend_stream_status_retryable(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::Unavailable || is_refreshable_route_identity_rejection(status)
}

pub(crate) async fn start_process_stream(
    client: &mut WorkerApiClient<GrpcClientTrace<Channel>>,
    sandbox_id: &SandboxId,
    start: StartRequest,
    route_token: Option<&SpaceApiRouteTokenState>,
) -> Result<tonic::Streaming<ProcessEvent>, StreamBridgeError> {
    let request = node_api_request_with_route_token_state(
        SpacedStartProcessRequest {
            sandbox_id: sandbox_id.to_string(),
            operation: Some(start),
        },
        route_token,
    )?;
    Ok(client
        .spaced_start_process(request)
        .await
        .map_err(StreamBridgeError::BackendStatus)?
        .into_inner())
}

pub(crate) async fn connect_process_stream(
    client: &mut WorkerApiClient<GrpcClientTrace<Channel>>,
    sandbox_id: &SandboxId,
    process_id: &ProcessId,
    output_cursor: Option<u64>,
    route_token: Option<&SpaceApiRouteTokenState>,
) -> Result<tonic::Streaming<ProcessEvent>, StreamBridgeError> {
    let request = node_api_request_with_route_token_state(
        SpacedConnectProcessRequest {
            sandbox_id: sandbox_id.to_string(),
            operation: Some(ConnectProcessRequest {
                process_id: process_id.to_string(),
                timeout: None,
                output_cursor,
                // A cursor whose output was evicted starts at the oldest
                // output retained, which `ProcessConnected` reports.
                resume_from_oldest_retained: true,
            }),
        },
        route_token,
    )?;
    Ok(client
        .spaced_connect_process(request)
        .await
        .map_err(StreamBridgeError::BackendStatus)?
        .into_inner())
}

pub(crate) async fn record_session_reconnect(
    state: &DataPlaneState,
    session_id: DataPlaneSessionId,
    reason: ReconnectReason,
    observed_route_generation: u64,
) -> Result<u64, StreamBridgeError> {
    let mut client = state.cluster_api.for_session(&session_id);
    let response = client
        .record_data_plane_session_reconnect(RecordDataPlaneSessionReconnectRequest {
            session_id: session_id.to_string(),
            protocol_session_id: protocol_session_id(session_id),
            reason: proto_reconnect_reason(&reason) as i32,
            observed_route_generation,
        })
        .await
        .map_err(StreamBridgeError::ClusterStatus)?
        .into_inner();
    pplx_space_metrics::data_plane::session_reconnect(session_id.kind().as_str(), reason.as_str());
    Ok(response.reconnect_count)
}

pub(crate) fn reconnect_reason_for_route_change(previous: u64, current: u64) -> ReconnectReason {
    if current != previous {
        ReconnectReason::RouteGenerationChanged
    } else {
        ReconnectReason::WorkerUnavailable
    }
}

pub(crate) fn attached_process_route_changed(
    current: &AttachedSession,
    refreshed: &AttachedSession,
) -> bool {
    current.sandbox_id != refreshed.sandbox_id
        || current.route_generation != refreshed.route_generation
        || current.current_node_id != refreshed.current_node_id
        || current.backend_endpoint != refreshed.backend_endpoint
}

pub(crate) fn record_backend_channel_reset(kind: &str, reason: &ReconnectReason) {
    pplx_space_metrics::data_plane::backend_channel_reset(kind, reason.as_str());
    pplx_space_metrics::data_plane::backend_affected_streams(kind, 1);
}

pub(crate) fn proto_reconnect_reason(reason: &ReconnectReason) -> DataPlaneReconnectReason {
    match reason {
        ReconnectReason::BackendRestart => DataPlaneReconnectReason::BackendRestart,
        ReconnectReason::SandboxMigration => DataPlaneReconnectReason::SandboxMigration,
        ReconnectReason::RouteGenerationChanged => DataPlaneReconnectReason::RouteGenerationChanged,
        ReconnectReason::WorkerUnavailable => DataPlaneReconnectReason::WorkerUnavailable,
        ReconnectReason::DataPlaneRestart => DataPlaneReconnectReason::DataPlaneRestart,
        // Unknown reasons come from a newer data-plane build; the cluster API
        // keeps the count and the public frame keeps the original string.
        ReconnectReason::Other(_) => DataPlaneReconnectReason::Unspecified,
    }
}

pub(super) async fn refresh_file_watch_attached_if_route_token_stale(
    state: &DataPlaneState,
    attached: AttachedSession,
) -> Result<AttachedSession, StreamBridgeError> {
    let stale = attached.clone();
    refresh_file_watch_attached_if_route_token_stale_with(attached, || {
        refresh_session_route(state, &stale)
    })
    .await
}

pub(crate) async fn refresh_file_watch_attached_if_route_token_stale_with<F, Fut>(
    attached: AttachedSession,
    resolve: F,
) -> Result<AttachedSession, StreamBridgeError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<AttachedSession, StreamBridgeError>>,
{
    if file_watch_attached_needs_route_token_refresh(&attached) {
        resolve().await
    } else {
        Ok(attached)
    }
}

pub(crate) fn file_watch_attached_needs_route_token_refresh(attached: &AttachedSession) -> bool {
    space_api_route_token_needs_refresh(&attached.space_api_route_token)
}

pub(crate) async fn watch_stream(
    client: &mut WorkerApiClient<GrpcClientTrace<Channel>>,
    sandbox_id: &SandboxId,
    watch: WatchOperation,
    route_token: Option<&SpaceApiRouteTokenState>,
) -> Result<tonic::Streaming<FileEvent>, StreamBridgeError> {
    let request = node_api_request_with_route_token_state(
        SpacedWatchRequest {
            sandbox_id: sandbox_id.to_string(),
            operation: Some(watch),
        },
        route_token,
    )?;
    Ok(client
        .spaced_watch(request)
        .await
        .map_err(StreamBridgeError::BackendStatus)?
        .into_inner())
}

pub(super) async fn open_native_watch_stream(
    state: &DataPlaneState,
    session_id: DataPlaneSessionId,
    attached: &mut AttachedSession,
    watch: WatchOperation,
) -> Result<
    (
        WorkerApiClient<GrpcClientTrace<Channel>>,
        tonic::Streaming<FileEvent>,
    ),
    StreamBridgeError,
> {
    native_route_call(state, attached, move |route| {
        let watch = watch.clone();
        async move {
            let channel = state
                .backend_channels
                .channel(&route.backend_endpoint, session_id)
                .await?;
            let mut client = WorkerApiClient::new(wrap_grpc_client_channel(channel));
            let stream = watch_stream(
                &mut client,
                &route.sandbox_id,
                watch,
                route.space_api_route_token.as_ref(),
            )
            .await?;
            Ok((client, stream))
        }
    })
    .await
}
