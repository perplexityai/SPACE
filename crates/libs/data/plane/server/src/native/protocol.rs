use super::*;

pub(crate) fn process_start_request(
    attached: &AttachedSession,
    session_id: DataPlaneSessionId,
) -> Result<StartRequest, StreamBridgeError> {
    let idempotency_key = Some(session_id.to_string());
    let process_id = Some(process_id_for_session(session_id).to_string());
    match attached.kind {
        SessionKind::Terminal => {
            let request =
                serde_json::from_str::<CreateTerminalSessionRequest>(&attached.adapter_state_json)
                    .map_err(StreamBridgeError::InvalidAdapterState)?;
            let command = if request.command.is_empty() {
                DEFAULT_TERMINAL_COMMAND
                    .iter()
                    .map(|value| (*value).to_string())
                    .collect()
            } else {
                request.command
            };
            Ok(StartRequest {
                command,
                cwd: request.cwd,
                env: std::collections::BTreeMap::<String, String>::from(request.env)
                    .into_iter()
                    .collect(),
                user: None,
                timeout: None,
                background: false,
                idempotency_key,
                tag: None,
                stdin: true,
                pty: Some(pty_config(request.size)),
                clean_env: false,
                process_id,
            })
        }
        SessionKind::Process => {
            let request =
                serde_json::from_str::<CreateProcessRequest>(&attached.adapter_state_json)
                    .map_err(StreamBridgeError::InvalidAdapterState)?;
            Ok(StartRequest {
                command: request.command,
                cwd: request.cwd,
                env: std::collections::BTreeMap::<String, String>::from(request.env)
                    .into_iter()
                    .collect(),
                user: request.user,
                timeout: duration_to_proto(request.timeout),
                background: false,
                idempotency_key,
                tag: None,
                stdin: request.stdin.unwrap_or(false),
                pty: request.pty.map(|size| pty_config(Some(size))),
                clean_env: false,
                process_id,
            })
        }
        SessionKind::FileWatch | SessionKind::Port | SessionKind::Protocol => {
            Err(StreamBridgeError::UnsupportedSessionKind)
        }
    }
}

/// Whether a process session asks for a pty, whose input an SSE stream cannot
/// carry or close. An unreadable adapter state answers `false` and is reported
/// by the start itself.
pub(crate) fn process_session_needs_pty(attached: &AttachedSession) -> bool {
    serde_json::from_str::<CreateProcessRequest>(&attached.adapter_state_json)
        .is_ok_and(|request| request.pty.is_some())
}

fn duration_to_proto(timeout: Option<SimpleDuration>) -> Option<prost_types::Duration> {
    let timeout = timeout?.as_duration();
    Some(prost_types::Duration {
        seconds: timeout.as_secs() as i64,
        nanos: timeout.subsec_nanos() as i32,
    })
}

pub(crate) fn pty_config(size: Option<TerminalSize>) -> PtyConfig {
    PtyConfig {
        size: size.map(|size| PtySize {
            rows: u32::from(size.rows),
            cols: u32::from(size.cols),
        }),
    }
}

pub(crate) fn file_watch_request(
    attached: &AttachedSession,
) -> Result<WatchOperation, StreamBridgeError> {
    let request =
        serde_json::from_str::<CreateFileWatchSessionRequest>(&attached.adapter_state_json)
            .map_err(StreamBridgeError::InvalidAdapterState)?;
    if request.path.is_empty() {
        return Err(StreamBridgeError::InvalidWatchPath);
    }
    Ok(WatchOperation {
        path: request.path,
        recursive: request.recursive,
        include_metadata_events: true,
        include_globs: Vec::new(),
        exclude_globs: Vec::new(),
        use_default_excludes: None,
        user: None,
    })
}

#[derive(Debug, PartialEq)]
pub(crate) struct ProcessFrameBatch {
    pub(crate) frames: Vec<DataPlaneServerFrame>,
    pub(crate) output_bytes: u64,
}

pub(crate) fn process_event_frames(event: ProcessEvent, output_seq: &mut u64) -> ProcessFrameBatch {
    match event.event {
        Some(spaced_process_event::Event::Started(_))
        | Some(spaced_process_event::Event::Connected(_)) => ProcessFrameBatch {
            frames: vec![DataPlaneServerFrame::ProcessStarted {
                process_id: event.process_id,
                pid: event.pid,
            }],
            output_bytes: 0,
        },
        Some(spaced_process_event::Event::Stdout(stdout)) => {
            *output_seq = output_seq.saturating_add(1);
            let output_bytes = stdout.data.len() as u64;
            ProcessFrameBatch {
                frames: vec![DataPlaneServerFrame::Stdout {
                    seq: *output_seq,
                    data_base64: BASE64_STANDARD.encode(stdout.data),
                }],
                output_bytes,
            }
        }
        Some(spaced_process_event::Event::Stderr(stderr)) => {
            *output_seq = output_seq.saturating_add(1);
            let output_bytes = stderr.data.len() as u64;
            ProcessFrameBatch {
                frames: vec![DataPlaneServerFrame::Stderr {
                    seq: *output_seq,
                    data_base64: BASE64_STANDARD.encode(stderr.data),
                }],
                output_bytes,
            }
        }
        Some(spaced_process_event::Event::Exit(exit)) => ProcessFrameBatch {
            frames: vec![DataPlaneServerFrame::ProcessExited {
                process_id: event.process_id,
                exit_code: process_exit_code(&exit),
                status: process_exit_status(&exit),
            }],
            output_bytes: 0,
        },
        None => ProcessFrameBatch {
            frames: Vec::new(),
            output_bytes: 0,
        },
    }
}

pub(crate) fn record_process_output_bytes(kind: &str, batch: &ProcessFrameBatch) {
    pplx_space_metrics::data_plane::stream_bytes_out(kind, batch.output_bytes);
}

pub(crate) fn process_frame_advances_backend_cursor(frame: &DataPlaneServerFrame) -> bool {
    matches!(
        frame,
        DataPlaneServerFrame::Stdout { .. } | DataPlaneServerFrame::Stderr { .. }
    )
}

pub(crate) fn file_event_frame(event: FileEvent, seq: u64) -> DataPlaneServerFrame {
    DataPlaneServerFrame::FileEvent {
        seq,
        path: event.path,
        kind: file_event_kind(event.kind),
    }
}

pub(crate) fn file_event_kind(kind: i32) -> FileEventKind {
    match SpacedFileEventKind::try_from(kind).unwrap_or(SpacedFileEventKind::Unspecified) {
        SpacedFileEventKind::Created => FileEventKind::Created,
        SpacedFileEventKind::Modified => FileEventKind::Modified,
        SpacedFileEventKind::Removed => FileEventKind::Removed,
        SpacedFileEventKind::Renamed => FileEventKind::Renamed,
        SpacedFileEventKind::Chmod => FileEventKind::Chmod,
        SpacedFileEventKind::Unspecified => FileEventKind::Unknown,
    }
}

pub(crate) fn process_exit_code(exit: &ProcessExit) -> i32 {
    match &exit.outcome {
        Some(spaced_process_exit::Outcome::Exited(exited)) => exited.exit_code,
        Some(spaced_process_exit::Outcome::Signaled(signaled)) => {
            128 + signaled.signal.unwrap_or_default()
        }
        Some(spaced_process_exit::Outcome::TimedOut(_)) => 124,
        Some(spaced_process_exit::Outcome::Unknown(_)) | None => 1,
    }
}

pub(crate) fn process_exit_status(exit: &ProcessExit) -> DataPlaneProcessExitStatus {
    match &exit.outcome {
        Some(spaced_process_exit::Outcome::Exited(_)) => DataPlaneProcessExitStatus::Exited,
        Some(spaced_process_exit::Outcome::Signaled(_)) => DataPlaneProcessExitStatus::Signaled,
        Some(spaced_process_exit::Outcome::TimedOut(_)) => DataPlaneProcessExitStatus::TimedOut,
        Some(spaced_process_exit::Outcome::Unknown(_)) | None => {
            DataPlaneProcessExitStatus::Unknown
        }
    }
}

pub(crate) fn node_api_request<T>(message: T) -> Result<tonic::Request<T>, tonic::Status> {
    let mut request = tonic::Request::new(message);
    insert_space_bearer_token(request.metadata_mut(), SpaceBearerToken::NodeApi).map_err(
        |error| tonic::Status::unavailable(format!("failed to attach node-api token: {error}")),
    )?;
    Ok(request)
}

pub(crate) fn node_api_request_with_route_token_state<T>(
    message: T,
    route_token: Option<&SpaceApiRouteTokenState>,
) -> Result<tonic::Request<T>, tonic::Status> {
    let mut request = node_api_request(message)?;
    if let Some(route_token) = route_token {
        pplx_space_common_svc::space_api_route_ed25519::insert_space_api_route_ed25519_token(
            request.metadata_mut(),
            route_token.token(),
        )
        .map_err(|error| {
            tonic::Status::unavailable(format!(
                "failed to attach SPACE API Ed25519 route token: {error}"
            ))
        })?;
    }
    Ok(request)
}

pub(crate) fn node_api_request_with_timeout_and_route_token_state<T>(
    message: T,
    timeout: Duration,
    route_token: Option<&SpaceApiRouteTokenState>,
) -> Result<tonic::Request<T>, tonic::Status> {
    let mut request = node_api_request_with_route_token_state(message, route_token)?;
    request.set_timeout(timeout);
    Ok(request)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StreamBridgeError {
    #[error("backend channel pool: {0}")]
    BackendChannel(#[from] BackendChannelPoolError),
    #[error("backend status: {0}")]
    BackendStatus(#[from] tonic::Status),
    #[error("cluster status: {0}")]
    ClusterStatus(#[source] tonic::Status),
    #[error("revocation status: {0}")]
    RevocationStatus(#[source] tonic::Status),
    #[error("revocation state is missing")]
    MissingRevocationState,
    #[error("route state is invalid: {0}")]
    RouteState(&'static str),
    #[error("invalid adapter state: {0}")]
    InvalidAdapterState(#[source] serde_json::Error),
    #[error("invalid watch path")]
    InvalidWatchPath,
    #[error("unsupported session kind")]
    UnsupportedSessionKind,
    #[error("invalid client frame")]
    InvalidClientFrame,
    #[error("client closed")]
    ClientClosed,
    #[error("process binding task: {0}")]
    ProcessBindingTask(#[source] tokio::task::JoinError),
}

pub(crate) fn stream_bridge_error_label(error: &StreamBridgeError) -> &'static str {
    match error {
        StreamBridgeError::BackendChannel(_) => "backend_channel",
        StreamBridgeError::BackendStatus(_) => "backend_status",
        StreamBridgeError::ClusterStatus(_) => "cluster_status",
        StreamBridgeError::RevocationStatus(_) => "revocation_status",
        StreamBridgeError::MissingRevocationState => "missing_revocation_state",
        StreamBridgeError::RouteState(_) => "route_state",
        StreamBridgeError::InvalidAdapterState(_) => "invalid_adapter_state",
        StreamBridgeError::InvalidWatchPath => "invalid_watch_path",
        StreamBridgeError::UnsupportedSessionKind => "unsupported_session_kind",
        StreamBridgeError::InvalidClientFrame => "invalid_client_frame",
        StreamBridgeError::ClientClosed => "client_closed",
        StreamBridgeError::ProcessBindingTask(_) => "process_binding_task",
    }
}
