use std::{
    collections::BTreeMap,
    convert::Infallible,
    fmt::Display,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    Json,
    body::Body,
    extract::State,
    http::{HeaderMap, header},
    response::Response,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use bytes::Bytes;
use futures_util::{Stream, StreamExt as _};
use pplx_space_common_core::types::{
    DEFAULT_GUEST_USER, IdempotencyKey, NodeId, ProcessId, SandboxId,
};
use pplx_space_common_grpc::{
    DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY, GrpcRetryContext, UdsPerSandboxLimitExceeded,
    generated_idempotency_key, is_temporary_tcp_failure, retry_grpc_status_with_context_if,
};
use pplx_space_common_svc::bootstrap::wrap_grpc_client_channel;
use pplx_space_metrics::data_plane::{
    ProcessStartDeliveryOutcome, ProcessStartRecoveryInterruption, ProcessStartRecoveryMode,
};
use pplx_space_node_api_client::{
    NodeApiOperationBudget, proto::worker_api_client::WorkerApiClient,
};
use pplx_space_sbx_mgr_client::operations as spaced_ops;
use pplx_space_sbx_mgr_client::proto::{
    SpacedCloseStdinRequest, SpacedConnectProcessRequest, SpacedListProcessesRequest,
    SpacedProcessEvent, SpacedResizePtyRequest, SpacedSendInputRequest, SpacedSendSignalRequest,
    SpacedStartProcessRequest,
};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tokio::sync::OwnedMutexGuard;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

use super::{
    AttachedStreamPermit, BackendEndpoint, DataPlaneState, ECW_PROCESS_INPUT_STREAM_KIND,
    ECW_PROCESS_STREAM_KIND, EcwProxyError, EcwResolvedRoute, acquire_ecw_stream_permit,
    before_stream_deadline, ecw_backend_channel, ecw_connect,
    node_api_request_with_route_token_state, optional_header, refuse_new_ecw_stream_while_draining,
    resolve_ecw_envd_route,
};
use crate::runtime_route::{
    EcwRouteAttemptError, bounded_ecw_route_call_with_timeout_observer, bounded_node_api_unary,
    retryable_bounded_node_api_unary,
};

type StringMap = BTreeMap<String, String>;

#[derive(Debug, Deserialize)]
struct ProcessConfigRequestE2b {
    cmd: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    envs: StringMap,
    #[serde(default)]
    cwd: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ProcessStartConnectRequestE2b {
    process: ProcessConfigRequestE2b,
    #[serde(default)]
    pty: Option<ProcessPtyRequestE2b>,
    #[serde(default)]
    tag: Option<String>,
    #[serde(default)]
    stdin: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ProcessSelectorRequestE2b {
    #[serde(default)]
    pid: Option<u32>,
    #[serde(default)]
    tag: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ProcessConnectRequestE2b {
    process: ProcessSelectorRequestE2b,
}

#[derive(Debug, Deserialize)]
struct ProcessUpdateRequestE2b {
    process: ProcessSelectorRequestE2b,
    #[serde(default)]
    pty: Option<ProcessPtyRequestE2b>,
}

#[derive(Debug, Deserialize, Clone)]
struct ProcessPtyRequestE2b {
    #[serde(default)]
    size: Option<ProcessPtySizeRequestE2b>,
}

#[derive(Debug, Deserialize, Clone)]
struct ProcessPtySizeRequestE2b {
    rows: u32,
    cols: u32,
}

#[derive(Debug, Deserialize)]
pub(super) struct ProcessSendSignalRequestE2b {
    process: ProcessSelectorRequestE2b,
    signal: ProcessSignalValueE2b,
}

#[derive(Debug, Deserialize)]
struct ProcessSendInputRequestE2b {
    process: ProcessSelectorRequestE2b,
    input: ProcessInputRequestE2b,
}

#[derive(Debug, Deserialize)]
struct ProcessStreamInputRequestE2b {
    #[serde(default)]
    start: Option<ProcessStreamInputStartEventE2b>,
    #[serde(default)]
    data: Option<ProcessStreamInputDataEventE2b>,
    #[serde(default)]
    keepalive: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ProcessStreamInputStartEventE2b {
    process: ProcessSelectorRequestE2b,
}

#[derive(Debug, Deserialize)]
struct ProcessStreamInputDataEventE2b {
    input: ProcessInputRequestE2b,
}

enum ProcessStreamInputEventE2b {
    Start(ProcessStreamInputStartEventE2b),
    Data(ProcessStreamInputDataEventE2b),
    Keepalive,
}

#[derive(Debug, Deserialize)]
struct ProcessInputRequestE2b {
    #[serde(default)]
    stdin: Option<String>,
    #[serde(default)]
    pty: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ProcessCloseStdinRequestE2b {
    process: ProcessSelectorRequestE2b,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ProcessSignalValueE2b {
    Name(String),
    Number(i32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EcwProcessSignal {
    Sigkill,
    Sigterm,
}

impl EcwProcessSignal {
    fn native_signal(self) -> i32 {
        match self {
            Self::Sigkill => 9,
            Self::Sigterm => 15,
        }
    }
}

impl std::str::FromStr for EcwProcessSignal {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value {
            "SIGNAL_SIGKILL" => Self::Sigkill,
            "SIGNAL_SIGTERM" => Self::Sigterm,
            _ => return Err(()),
        })
    }
}

impl TryFrom<i32> for EcwProcessSignal {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        Ok(match value {
            9 => Self::Sigkill,
            15 => Self::Sigterm,
            _ => return Err(()),
        })
    }
}

impl TryFrom<ProcessSignalValueE2b> for EcwProcessSignal {
    type Error = ();

    fn try_from(value: ProcessSignalValueE2b) -> Result<Self, Self::Error> {
        match value {
            ProcessSignalValueE2b::Name(signal) => signal.parse(),
            ProcessSignalValueE2b::Number(signal) => Self::try_from(signal),
        }
    }
}

impl ProcessStreamInputRequestE2b {
    fn into_event(self) -> Result<ProcessStreamInputEventE2b, EcwProxyError> {
        let event_count = usize::from(self.start.is_some())
            + usize::from(self.data.is_some())
            + usize::from(self.keepalive.is_some());
        if event_count != 1 {
            return Err(EcwProxyError::bad_request(
                "exactly one stream input event is required",
            ));
        }
        if let Some(start) = self.start {
            return Ok(ProcessStreamInputEventE2b::Start(start));
        }
        if let Some(data) = self.data {
            return Ok(ProcessStreamInputEventE2b::Data(data));
        }
        Ok(ProcessStreamInputEventE2b::Keepalive)
    }
}

#[derive(Debug, Serialize, Default)]
pub(super) struct EmptyUnaryConnectResponseE2b {}

#[derive(Debug, Serialize)]
pub(super) struct ProcessListResponseE2b {
    processes: Vec<ProcessInfoE2b>,
}

#[derive(Debug, Serialize)]
struct ProcessInfoE2b {
    config: ProcessConfigResponseE2b,
    pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    tag: Option<String>,
}

#[derive(Debug, Serialize)]
struct ProcessConfigResponseE2b {
    cmd: String,
    args: Vec<String>,
    envs: StringMap,
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
}

pub(super) async fn process_list(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
) -> Result<Json<ProcessListResponseE2b>, EcwProxyError> {
    let route = resolve_ecw_envd_route(&state, &headers).await?;
    let processes = list_processes(&state, &route).await?;

    Ok(Json(ProcessListResponseE2b {
        processes: processes.into_iter().map(process_info_to_e2b).collect(),
    }))
}

pub(super) async fn process_start(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, EcwProxyError> {
    refuse_new_ecw_stream_while_draining(&state, ECW_PROCESS_STREAM_KIND)?;
    let route = resolve_ecw_envd_route(&state, &headers).await?;
    let request: ProcessStartConnectRequestE2b =
        ecw_connect::decode_connect_json_request(body).await?;
    let native_request = build_process_start_request(&headers, &route, request)?;
    let stdout_stream = if native_request
        .operation
        .as_ref()
        .is_some_and(|operation| operation.pty.is_some())
    {
        ecw_connect::EcwProcessStdoutStream::Pty
    } else {
        ecw_connect::EcwProcessStdoutStream::Stdout
    };
    let stream_permit = acquire_ecw_stream_permit(&state, ECW_PROCESS_STREAM_KIND)?;
    let command_timeout = native_request
        .operation
        .as_ref()
        .and_then(|operation| operation.timeout.as_ref());
    let stall_after = process_stream_stall_after(command_timeout);
    let open_timeout = process_stream_open_timeout(command_timeout);
    let resume_request = native_request.clone();
    // Not raced against the drain deadline: the guest may already be running
    // the command, so the open runs to its own timeout and the stream reports
    // the start (ProcessConnectJsonStream waits for it after the deadline).
    // New starts are refused once the drain begins, so an open still running
    // was admitted before SIGTERM and ends within the 8-second stream-open
    // budget, before any deadline of at least that long. A second stop signal
    // doesn't wait for it: forcing the exit cuts it the way SIGKILL would.
    let ((stream, retried_start), route) = open_process_start_stream_with_refresh(
        &state,
        route,
        native_request,
        ProcessStartResume::Start,
        open_timeout,
    )
    .await?;
    let resume_state = state.clone();
    let resume_route = Arc::new(tokio::sync::Mutex::new(route.clone()));
    let stream = resumable_process_start_stream(stream, retried_start, move |resume| {
        let state = resume_state.clone();
        let route = Arc::clone(&resume_route);
        let request = resume_request.clone();
        async move {
            let current = route.lock().await.clone();
            reset_process_start_stream_channel(&state, &current);
            let resume_for_log = resume.clone();
            let result = open_process_start_stream_with_refresh(
                &state,
                current.clone(),
                request,
                resume,
                open_timeout,
            )
            .await;
            let ((stream, _), refreshed) = match result {
                Ok(value) => {
                    tracing::info!(
                        operation = "Process.Start",
                        sandbox_id = %current.sandbox_id,
                        route_generation = current.route_generation,
                        current_node_id = %current.current_node_id,
                        backend_endpoint = %current.backend_endpoint,
                        resume = %resume_for_log,
                        "reopened ECW process start stream",
                    );
                    value
                }
                Err(error) => {
                    let status = process_start_reopen_status(error, &resume_for_log);
                    tracing::warn!(
                        operation = "Process.Start",
                        sandbox_id = %current.sandbox_id,
                        route_generation = current.route_generation,
                        current_node_id = %current.current_node_id,
                        backend_endpoint = %current.backend_endpoint,
                        resume = %resume_for_log,
                        grpc_code = ?status.code(),
                        status_class = %process_start_status_class(&status),
                        "failed to reopen ECW process start stream",
                    );
                    return Err(status);
                }
            };
            *route.lock().await = refreshed;
            Ok(stream)
        }
    });

    Ok(process_start_json_stream_response(
        stream,
        &route,
        stdout_stream,
        stall_after,
        EcwProcessStreamLease {
            permit: stream_permit,
            shutdown_token: state.stream_shutdown_token.child_token(),
        },
    ))
}

pub(super) async fn process_connect(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, EcwProxyError> {
    refuse_new_ecw_stream_while_draining(&state, ECW_PROCESS_STREAM_KIND)?;
    let route = resolve_ecw_envd_route(&state, &headers).await?;
    let request: ProcessConnectRequestE2b = ecw_connect::decode_connect_json_request(body).await?;
    let process = process_for_selector(&state, &route, request.process).await?;
    let stdout_stream = process_stdout_stream(&process);
    let process_id = process.process_id;
    let stream_permit = acquire_ecw_stream_permit(&state, ECW_PROCESS_STREAM_KIND)?;
    let timeout = connect_timeout(&headers)?;
    let stall_after = process_stream_stall_after(timeout.as_ref());
    let open_timeout = process_stream_open_timeout(timeout.as_ref());
    let message = SpacedConnectProcessRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::ConnectProcessRequest {
            process_id,
            timeout,
            output_cursor: None,
            resume_from_oldest_retained: false,
        }),
    };
    let route_state = state.clone();
    let opened = before_stream_deadline(
        &state.stream_shutdown_token,
        bounded_ecw_route_call_with_timeout_observer(
            &state,
            route,
            "process_connect_open",
            open_timeout,
            Some(log_process_connect_timeout),
            move |route, _remaining| {
                let message = message.clone();
                let state = route_state.clone();
                async move {
                    let channel = ecw_backend_channel(&state, &route)
                        .await
                        .map_err(EcwRouteAttemptError::Proxy)?;
                    let mut client = WorkerApiClient::new(wrap_grpc_client_channel(channel));
                    let result = client
                        .spaced_connect_process(node_api_request_with_route_token_state(
                            message,
                            route.space_api_route_token.as_ref(),
                        )?)
                        .await
                        .map(|response| response.into_inner());
                    if let Err(status) = &result {
                        log_process_stream_open_status("Process.Connect", &route, status);
                    }
                    result.map_err(EcwRouteAttemptError::Status)
                }
            },
            ecw_connect::tonic_status_to_ecw_error,
        ),
    )
    .await;
    let Some(opened) = opened else {
        // The permit above already counted the attach as `ok`.
        pplx_space_metrics::data_plane::stream_closed(ECW_PROCESS_STREAM_KIND, "shutdown");
        return Err(EcwProxyError::draining());
    };
    let (stream, route) = opened?;

    Ok(process_connect_json_stream_response(
        stream,
        &route,
        stdout_stream,
        stall_after,
        EcwProcessStreamLease {
            permit: stream_permit,
            shutdown_token: state.stream_shutdown_token.child_token(),
        },
    ))
}

pub(super) async fn process_update(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<EmptyUnaryConnectResponseE2b>, EcwProxyError> {
    let route = resolve_ecw_envd_route(&state, &headers).await?;
    let request: ProcessUpdateRequestE2b = ecw_connect::decode_connect_json_request(body).await?;
    let pty_size = translate_process_pty_resize(request.pty.as_ref())?;
    let process_id = process_id_for_selector(&state, &route, request.process).await?;
    if let Some(size) = pty_size {
        let message = SpacedResizePtyRequest {
            sandbox_id: route.sandbox_id.to_string(),
            operation: Some(spaced_ops::ResizePtyRequest {
                process_id,
                size: Some(size),
            }),
        };
        retryable_bounded_node_api_unary(
            &state,
            route,
            "process_resize_pty",
            NodeApiOperationBudget::ProcessControl.timeout(),
            message,
            |channel, request| async move {
                WorkerApiClient::new(wrap_grpc_client_channel(channel))
                    .spaced_resize_pty(request)
                    .await
            },
            ecw_connect::tonic_status_to_ecw_error,
        )
        .await?;
    }
    Ok(Json(EmptyUnaryConnectResponseE2b::default()))
}

pub(super) async fn process_send_signal(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<EmptyUnaryConnectResponseE2b>, EcwProxyError> {
    let route = resolve_ecw_envd_route(&state, &headers).await?;
    let request: ProcessSendSignalRequestE2b =
        ecw_connect::decode_connect_json_request(body).await?;
    let process_id = process_id_for_selector(&state, &route, request.process).await?;
    let signal = process_signal_to_native(request.signal)?;
    let message = SpacedSendSignalRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::SendSignalRequest { process_id, signal }),
    };
    bounded_node_api_unary(
        &state,
        route,
        "process_send_signal",
        NodeApiOperationBudget::ProcessControl.timeout(),
        message,
        |channel, request| async move {
            WorkerApiClient::new(wrap_grpc_client_channel(channel))
                .spaced_send_signal(request)
                .await
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await?;

    Ok(Json(EmptyUnaryConnectResponseE2b::default()))
}

pub(super) async fn process_send_input(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<EmptyUnaryConnectResponseE2b>, EcwProxyError> {
    let route = resolve_ecw_envd_route(&state, &headers).await?;
    let request: ProcessSendInputRequestE2b =
        ecw_connect::decode_connect_json_request(body).await?;
    let process_id = process_id_for_selector(&state, &route, request.process).await?;
    let process_key = process_input_key(&process_id)?;
    let data = process_input_bytes(request.input)?;
    let _input_guard = state
        .ecw_process_input_locks
        .lock_for(route.sandbox_id, process_key)
        .lock_owned()
        .await;
    send_process_input(&state, route, process_id, data).await?;

    Ok(Json(EmptyUnaryConnectResponseE2b::default()))
}

pub(super) async fn process_stream_input(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<EmptyUnaryConnectResponseE2b>, EcwProxyError> {
    let streaming = is_connect_json_stream_request(&headers);
    // A streamed input request stays open as long as its client keeps
    // sending, so the drain treats it as a stream; the unary form is served.
    if streaming {
        refuse_new_ecw_stream_while_draining(&state, ECW_PROCESS_INPUT_STREAM_KIND)?;
    }
    let mut route = resolve_ecw_envd_route(&state, &headers).await?;
    let mut process_id = None::<String>;
    let mut input_guard = None::<OwnedMutexGuard<()>>;

    if streaming {
        // Again after the route lookup: SIGTERM may have landed while it ran.
        refuse_new_ecw_stream_while_draining(&state, ECW_PROCESS_INPUT_STREAM_KIND)?;
        let shutdown_token = state.stream_shutdown_token.child_token();
        let input = async {
            let mut body = body;
            let mut buffer = Vec::new();
            let mut seen_message = false;
            while let Some(request) =
                ecw_connect::decode_next_connect_json_stream_message(&mut body, &mut buffer).await?
            {
                seen_message = true;
                handle_process_stream_input_request(
                    &state,
                    &mut route,
                    &mut process_id,
                    &mut input_guard,
                    request,
                )
                .await?;
            }
            Ok::<_, EcwProxyError>(seen_message)
        };
        // The deadline ends the stream whether it is waiting for the client's
        // next message or for a backend call on the last one.
        let seen_message = tokio::select! {
            () = shutdown_token.cancelled() => return Err(EcwProxyError::input_interrupted()),
            seen_message = input => seen_message?,
        };
        if !seen_message {
            return Err(EcwProxyError::bad_request("Empty Connect stream"));
        }
    } else {
        let requests: Vec<ProcessStreamInputRequestE2b> =
            ecw_connect::decode_connect_json_stream_request(body).await?;
        for request in requests {
            handle_process_stream_input_request(
                &state,
                &mut route,
                &mut process_id,
                &mut input_guard,
                request,
            )
            .await?;
        }
    }

    Ok(Json(EmptyUnaryConnectResponseE2b::default()))
}

async fn handle_process_stream_input_request(
    state: &DataPlaneState,
    route: &mut EcwResolvedRoute,
    process_id: &mut Option<String>,
    input_guard: &mut Option<OwnedMutexGuard<()>>,
    request: ProcessStreamInputRequestE2b,
) -> Result<(), EcwProxyError> {
    match request.into_event()? {
        ProcessStreamInputEventE2b::Start(start) => {
            let resolved_process_id = process_id_for_selector(state, route, start.process).await?;
            if process_id.as_deref() != Some(resolved_process_id.as_str()) {
                let process_key = process_input_key(&resolved_process_id)?;
                *input_guard = None;
                *input_guard = Some(
                    state
                        .ecw_process_input_locks
                        .lock_for(route.sandbox_id, process_key)
                        .lock_owned()
                        .await,
                );
                *process_id = Some(resolved_process_id);
            }
        }
        ProcessStreamInputEventE2b::Data(data) => {
            let process_id = process_id.as_ref().ok_or_else(|| {
                EcwProxyError::bad_request("stream input start event is required before data")
            })?;
            let _input_guard = input_guard.as_ref().ok_or_else(|| {
                EcwProxyError::bad_request("stream input start event is required before data")
            })?;
            let data = process_input_bytes(data.input)?;
            *route = send_process_input(state, route.clone(), process_id.clone(), data).await?;
        }
        ProcessStreamInputEventE2b::Keepalive => {}
    }

    Ok(())
}

async fn send_process_input(
    state: &DataPlaneState,
    route: EcwResolvedRoute,
    process_id: String,
    data: Vec<u8>,
) -> Result<EcwResolvedRoute, EcwProxyError> {
    let message = SpacedSendInputRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::SendInputRequest { process_id, data }),
    };
    bounded_node_api_unary(
        state,
        route,
        "process_send_input",
        NodeApiOperationBudget::ProcessControl.timeout(),
        message,
        |channel, request| async move {
            WorkerApiClient::new(wrap_grpc_client_channel(channel))
                .spaced_send_input(request)
                .await
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await
    .map(|(_, route)| route)
}

fn is_connect_json_stream_request(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|content_type| content_type.starts_with("application/connect+json"))
}

pub(super) async fn process_close_stdin(
    State(state): State<DataPlaneState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<EmptyUnaryConnectResponseE2b>, EcwProxyError> {
    let route = resolve_ecw_envd_route(&state, &headers).await?;
    let request: ProcessCloseStdinRequestE2b =
        ecw_connect::decode_connect_json_request(body).await?;
    let process_id = process_id_for_selector(&state, &route, request.process).await?;
    let process_key = process_input_key(&process_id)?;
    let _input_guard = state
        .ecw_process_input_locks
        .lock_for(route.sandbox_id, process_key)
        .lock_owned()
        .await;
    let message = SpacedCloseStdinRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::CloseStdinRequest { process_id }),
    };
    retryable_bounded_node_api_unary(
        &state,
        route,
        "process_close_stdin",
        NodeApiOperationBudget::ProcessControl.timeout(),
        message,
        |channel, request| async move {
            WorkerApiClient::new(wrap_grpc_client_channel(channel))
                .spaced_close_stdin(request)
                .await
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await?;

    Ok(Json(EmptyUnaryConnectResponseE2b::default()))
}

fn build_process_start_request(
    headers: &HeaderMap,
    route: &EcwResolvedRoute,
    request: ProcessStartConnectRequestE2b,
) -> Result<SpacedStartProcessRequest, EcwProxyError> {
    let timeout = connect_timeout(headers)?;
    let command = translate_process_start_request(&request)?;
    let client_key = process_start_client_idempotency_key(headers)?;

    let mut operation = spaced_ops::StartRequest {
        command,
        cwd: request
            .process
            .cwd
            .or_else(|| ecw_default_workdir(route.template_id.as_deref())),
        env: request.process.envs.into_iter().collect(),
        user: envd_basic_username(headers),
        timeout,
        background: false,
        idempotency_key: None,
        tag: request.tag,
        stdin: request.stdin.unwrap_or(true),
        pty: translate_process_pty_config(request.pty.as_ref())?,
        clean_env: false,
        // The E2B path leaves process identity to the guest: its clients
        // address processes by pid or tag, never by process id.
        process_id: None,
    };
    let idempotency_key = match client_key {
        Some(client_key) => client_bound_process_start_key(client_key, &operation),
        None => generated_idempotency_key(),
    };
    operation.idempotency_key = Some(idempotency_key.into());

    Ok(SpacedStartProcessRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(operation),
    })
}

/// A caller that cannot tell whether a start reached the sandbox, such as one
/// whose request was written to a connection that then failed, retries with the
/// same `Idempotency-Key`. spaced then replays the original process stream
/// rather than running the command a second time.
const PROCESS_START_IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
/// The caller's key is digested into the backend key, so this only bounds the
/// value read at this boundary.
const MAX_PROCESS_START_IDEMPOTENCY_KEY_BYTES: usize = 256;

fn process_start_client_idempotency_key(
    headers: &HeaderMap,
) -> Result<Option<&str>, EcwProxyError> {
    let Some(value) = headers.get(PROCESS_START_IDEMPOTENCY_KEY_HEADER) else {
        return Ok(None);
    };
    // Falling back to a generated key would silently drop the retry safety the
    // caller asked for, so a present but unusable key is rejected.
    let key = value.to_str().map(str::trim).unwrap_or_default();
    if key.is_empty() || key.len() > MAX_PROCESS_START_IDEMPOTENCY_KEY_BYTES {
        return Err(EcwProxyError::bad_request("Invalid idempotency-key header"));
    }
    Ok(Some(key))
}

/// spaced replays by key alone. Binding the caller's key to the start it came
/// with makes a key reused for a different command start that command instead
/// of replaying another one's output, and the fixed-size digest keeps the
/// guest's bounded key table independent of caller input.
fn client_bound_process_start_key(
    client_key: &str,
    operation: &spaced_ops::StartRequest,
) -> IdempotencyKey {
    fn update_field(digest: &mut Sha256, tag: u8, value: &[u8]) {
        digest.update([tag]);
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }

    // Retries may reach a different data-plane replica, so the digest must not
    // depend on map iteration order: env is hashed sorted, and the rest of the
    // request has no map fields, which makes its protobuf encoding stable.
    let mut fields = operation.clone();
    fields.idempotency_key = None;
    let env: BTreeMap<String, String> = std::mem::take(&mut fields.env).into_iter().collect();
    let mut digest = Sha256::new();
    update_field(&mut digest, b'k', client_key.as_bytes());
    update_field(&mut digest, b'o', &fields.encode_to_vec());
    for (name, value) in &env {
        update_field(&mut digest, b'n', name.as_bytes());
        update_field(&mut digest, b'v', value.as_bytes());
    }
    IdempotencyKey::parse(format!("ecw-start-{}", hex::encode(digest.finalize())))
        .expect("hex digest is a valid idempotency key")
}

fn ecw_default_workdir(template_id: Option<&str>) -> Option<String> {
    ecw_default_workdir_from_environment(
        template_id,
        &pplx_space_common_core::env::ProcessEnvironment,
    )
}
fn ecw_default_workdir_from_environment(
    template_id: Option<&str>,
    env: &dyn pplx_space_common_core::env::Environment,
) -> Option<String> {
    let template_id = template_id.filter(|name| !name.is_empty())?;
    let templates = env
        .var("SPACE_E2B_HOME_WORKDIR_TEMPLATES")
        .unwrap_or_else(|_| "code-runtime".to_string());
    templates
        .split(',')
        .any(|name| name.trim() == template_id)
        .then(|| format!("/home/{DEFAULT_GUEST_USER}"))
}

fn process_info_to_e2b(info: spaced_ops::ProcessInfo) -> ProcessInfoE2b {
    let mut command = info.command.into_iter();
    let cmd = command.next().unwrap_or_default();
    ProcessInfoE2b {
        config: ProcessConfigResponseE2b {
            cmd,
            args: command.collect(),
            envs: info.env.into_iter().collect(),
            cwd: info.cwd,
        },
        pid: info.pid,
        tag: info.tag,
    }
}

fn connect_timeout(headers: &HeaderMap) -> Result<Option<prost_types::Duration>, EcwProxyError> {
    optional_header(headers, "connect-timeout-ms")
        .map(|value| {
            let milliseconds = value
                .parse::<u64>()
                .map_err(|_| EcwProxyError::bad_request("Invalid connect-timeout-ms header"))?;
            Ok(prost_types::Duration {
                seconds: (milliseconds / 1_000) as i64,
                nanos: ((milliseconds % 1_000) * 1_000_000) as i32,
            })
        })
        .transpose()
}

pub(super) fn envd_basic_username(headers: &HeaderMap) -> Option<String> {
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())?;
    let encoded = authorization.strip_prefix("Basic ")?;
    let decoded = BASE64_STANDARD.decode(encoded).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (username, _) = decoded.split_once(':')?;

    (!username.is_empty()).then(|| username.to_string())
}

fn translate_process_start_request(
    request: &ProcessStartConnectRequestE2b,
) -> Result<Vec<String>, EcwProxyError> {
    if request.process.cmd.is_empty() {
        return Err(EcwProxyError::bad_request("process.cmd is required"));
    }

    let mut command = Vec::with_capacity(1 + request.process.args.len());
    command.push(request.process.cmd.clone());
    command.extend(request.process.args.clone());
    Ok(command)
}

fn translate_process_pty_config(
    pty: Option<&ProcessPtyRequestE2b>,
) -> Result<Option<spaced_ops::PtyConfig>, EcwProxyError> {
    pty.map(|pty| {
        Ok(spaced_ops::PtyConfig {
            size: pty
                .size
                .as_ref()
                .map(translate_process_pty_size)
                .transpose()?,
        })
    })
    .transpose()
}

fn translate_process_pty_resize(
    pty: Option<&ProcessPtyRequestE2b>,
) -> Result<Option<spaced_ops::PtySize>, EcwProxyError> {
    pty.and_then(|pty| pty.size.as_ref())
        .map(translate_process_pty_size)
        .transpose()
}

fn translate_process_pty_size(
    size: &ProcessPtySizeRequestE2b,
) -> Result<spaced_ops::PtySize, EcwProxyError> {
    if size.rows == 0 || size.cols == 0 {
        return Err(EcwProxyError::bad_request(
            "pty size rows and cols must be non-zero",
        ));
    }
    Ok(spaced_ops::PtySize {
        rows: size.rows,
        cols: size.cols,
    })
}

fn process_input_bytes(input: ProcessInputRequestE2b) -> Result<Vec<u8>, EcwProxyError> {
    let (encoded, field) = match (input.stdin, input.pty) {
        (Some(_), Some(_)) => {
            return Err(EcwProxyError::bad_request(
                "only one of input.stdin or input.pty may be set",
            ));
        }
        (Some(stdin), None) => (stdin, "input.stdin"),
        (None, Some(pty)) => (pty, "input.pty"),
        (None, None) => {
            return Err(EcwProxyError::bad_request(
                "input.stdin or input.pty is required",
            ));
        }
    };
    BASE64_STANDARD
        .decode(encoded)
        .map_err(|_| EcwProxyError::bad_request(format!("{field} must be base64")))
}

async fn process_id_for_selector(
    state: &DataPlaneState,
    route: &EcwResolvedRoute,
    selector: ProcessSelectorRequestE2b,
) -> Result<String, EcwProxyError> {
    process_for_selector(state, route, selector)
        .await
        .map(|process| process.process_id)
}

async fn process_for_selector(
    state: &DataPlaneState,
    route: &EcwResolvedRoute,
    selector: ProcessSelectorRequestE2b,
) -> Result<spaced_ops::ProcessInfo, EcwProxyError> {
    let processes = list_processes(state, route).await?;
    if let Some(tag) = selector.tag {
        return processes
            .into_iter()
            .find(|process| process.tag.as_deref() == Some(tag.as_str()))
            .ok_or_else(|| EcwProxyError::not_found("Process"));
    }

    let pid = selector
        .pid
        .ok_or_else(|| EcwProxyError::bad_request("process.pid or process.tag is required"))?;

    processes
        .into_iter()
        .find(|process| process.pid == pid)
        .ok_or_else(|| EcwProxyError::not_found("Process"))
}

fn process_stdout_stream(process: &spaced_ops::ProcessInfo) -> ecw_connect::EcwProcessStdoutStream {
    if process.pty == Some(true) {
        ecw_connect::EcwProcessStdoutStream::Pty
    } else {
        ecw_connect::EcwProcessStdoutStream::Stdout
    }
}

fn process_input_key(process_id: &str) -> Result<ProcessId, EcwProxyError> {
    process_id
        .parse()
        .map_err(|_| EcwProxyError::bad_request("Invalid process id"))
}

async fn list_processes(
    state: &DataPlaneState,
    route: &EcwResolvedRoute,
) -> Result<Vec<spaced_ops::ProcessInfo>, EcwProxyError> {
    let message = SpacedListProcessesRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::ListProcessesRequest {}),
    };
    let (response, _route) = retryable_bounded_node_api_unary(
        state,
        route.clone(),
        "process_list",
        NodeApiOperationBudget::ProcessMetadata.timeout(),
        message,
        |channel, request| async move {
            WorkerApiClient::new(wrap_grpc_client_channel(channel))
                .spaced_list_processes(request)
                .await
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await?;

    Ok(response.processes)
}

fn process_signal_to_native(signal: ProcessSignalValueE2b) -> Result<i32, EcwProxyError> {
    EcwProcessSignal::try_from(signal)
        .map(EcwProcessSignal::native_signal)
        .map_err(|_| EcwProxyError::bad_request("Unsupported process signal"))
}

mod stream;
use stream::*;
#[cfg(test)]
mod tests;
