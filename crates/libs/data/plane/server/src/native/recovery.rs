//! One explicit recovery operation per authenticated command-session socket.
//! There is no automatic start, cursor ownership or retry in this adapter.
use super::*;
use futures_util::Sink;
use pplx_space_public_api::recovery::{
    RecoveryClientFrame as Client, RecoveryProcessHandle as Handle, RecoveryServerFrame as Server,
    RecoveryStart,
};
use pplx_space_sbx_mgr_client::operations as ops;
use pplx_space_spaced_guest_protocol::recovery::RecoveryReason;
use tonic::Status;

pub(super) fn is_recovery_session(attached: &AttachedSession) -> bool {
    attached.kind == SessionKind::Process
        && serde_json::from_str::<CreateProcessRequest>(&attached.adapter_state_json)
            .is_ok_and(|request| request.recovery.is_some())
}

fn handle_for_session(
    handle: Handle,
    start: &RecoveryStart,
    session_id: DataPlaneSessionId,
    sequence: Option<u64>,
) -> Result<ops::RecoveryHandle, Status> {
    if handle.process_id != process_id_for_session(session_id).as_str()
        || handle.instance_id != start.expected_instance_id
        || handle.activation_id != start.expected_activation_id
    {
        return Err(Status::failed_precondition(
            "handle does not belong to this command session",
        ));
    }
    if sequence == Some(0) {
        return Err(Status::invalid_argument(
            "mutation sequence must be positive",
        ));
    }
    Ok(ops::RecoveryHandle {
        contract_version: if sequence.is_some() { 2 } else { 1 },
        expected_instance_id: handle.instance_id,
        expected_activation_id: handle.activation_id,
        process_id: handle.process_id,
        process_incarnation: handle.process_incarnation,
        sequence,
    })
}

fn error_frame(status: Status) -> Server {
    let reason = RecoveryReason::from_status(&status);
    // Do not expose endpoint addresses or transport diagnostics in public frames.
    Server::RecoveryError {
        code: status.code() as i32,
        reason: reason.map(|reason| reason.as_str().to_owned()),
        message: reason.map_or_else(
            || format!("process request failed: {}", status.code()),
            |reason| reason.into_status().message().to_owned(),
        ),
        input_written: status
            .metadata()
            .get("space-process-input-written")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok()),
    }
}

async fn send<S>(sink: &mut S, frame: &Server) -> Result<(), Status>
where
    S: Sink<Message> + Unpin,
{
    let text =
        serde_json::to_string(frame).map_err(|_| Status::internal("invalid recovery frame"))?;
    sink.send(Message::Text(text.into()))
        .await
        .map_err(|_| Status::cancelled("client disconnected"))
}

pub(super) async fn bridge(state: DataPlaneState, attached: AttachedSession, socket: WebSocket) {
    let (mut sink, mut source) = socket.split();
    let session_id = attached.session_id;
    let shutdown = state.stream_shutdown_token.clone();
    let result = async {
        // A client that only attaches never starts work or holds a payload permit.
        let first = tokio::time::timeout(state.attach_deadline, source.next())
            .await
            .map_err(|_| Status::deadline_exceeded("recovery request deadline elapsed"))?
            .ok_or_else(|| Status::cancelled("client disconnected"))?
            .map_err(|_| Status::cancelled("client disconnected"))?;
        let Message::Text(text) = first else {
            return Err(Status::invalid_argument("expected a recovery request"));
        };
        if text.len() > MAX_CLIENT_BYTES_DURING_OPEN {
            return Err(Status::resource_exhausted("recovery request too large"));
        }
        let request = serde_json::from_str::<Client>(&text)
            .map_err(|_| Status::invalid_argument("invalid recovery request"))?;
        let mut revocation = revocation_check_interval(&state);
        let operation = execute(&state, &attached, request, &mut sink);
        tokio::pin!(operation);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Err(Status::unavailable("data plane shutting down")),
                _ = revocation.tick() => {
                    if session_is_revoked(&state, session_id).await.map_err(backend_status)? {
                        return Err(Status::permission_denied("session revoked"));
                    }
                }
                message = source.next() => match message {
                    Some(Ok(Message::Pong(_) | Message::Ping(_))) => {},
                    // A request socket is not a multiplexed control channel.
                    Some(Ok(Message::Text(_) | Message::Binary(_))) => return Err(Status::invalid_argument("one recovery request per socket")),
                    _ => return Err(Status::cancelled("client disconnected")),
                },
                result = &mut operation => return result,
            }
        }
    };
    // Also bounds shutdown while waiting for the first request or revocation I/O.
    let result = tokio::select! {
        biased;
        _ = shutdown.cancelled() => Err(Status::unavailable("data plane shutting down")),
        result = result => result,
    };
    let frame = match result {
        Ok(()) => Server::RecoveryComplete,
        Err(status) => error_frame(status),
    };
    // A slow reader must not hold service shutdown or an admission slot indefinitely.
    let closed = tokio::time::timeout(state.attach_deadline, async {
        send(&mut sink, &frame).await?;
        sink.close()
            .await
            .map_err(|_| Status::cancelled("client disconnected"))
    })
    .await;
    if !matches!(closed, Ok(Ok(()))) {
        tracing::debug!(%session_id, "recovery socket closed before final delivery");
    }
}

fn backend_status(error: StreamBridgeError) -> Status {
    match error {
        StreamBridgeError::BackendStatus(status) | StreamBridgeError::ClusterStatus(status) => {
            status
        }
        _ => Status::unavailable("process route unavailable"),
    }
}

async fn execute<S>(
    state: &DataPlaneState,
    attached: &AttachedSession,
    request: Client,
    sink: &mut S,
) -> Result<(), Status>
where
    S: Sink<Message> + Unpin,
{
    // Resolve the current route before dispatch, including after a restore.
    // This refresh never resubmits an operation and leaves its fences intact.
    let fresh = refresh_session_route(state, attached)
        .await
        .map_err(backend_status)?;
    let attached = &fresh;
    let config = serde_json::from_str::<CreateProcessRequest>(&attached.adapter_state_json)
        .map_err(|_| Status::internal("invalid command session"))?;
    let recovery = config
        .recovery
        .ok_or_else(|| Status::failed_precondition("not a recovery session"))?;
    let channel = tokio::time::timeout(
        state.attach_deadline,
        state
            .backend_channels
            .channel(&attached.backend_endpoint, attached.session_id),
    )
    .await
    .map_err(|_| Status::deadline_exceeded("process route deadline elapsed"))?
    .map_err(|error| backend_status(error.into()))?;
    let mut client = WorkerApiClient::new(wrap_grpc_client_channel(channel));
    let sandbox_id = attached.sandbox_id.to_string();
    let route = attached.space_api_route_token.as_ref();
    let deadline = state.attach_deadline;
    // Exactly one backend dispatch. An uncertain result belongs to the caller;
    // route refresh must never rewrite a fence or fall back to a legacy call.
    macro_rules! unary {
        ($method:ident, $envelope:ident, $operation:expr, $sequence:expr) => {{
            let request = node_api_request_with_route_token_state(
                pplx_space_sbx_mgr_client::proto::$envelope {
                    sandbox_id,
                    operation: Some($operation),
                },
                route,
            )?;
            tokio::time::timeout(deadline, client.$method(request))
                .await
                .map_err(|_| Status::deadline_exceeded("process mutation deadline elapsed"))??;
            send(
                sink,
                &Server::RecoveryAccepted {
                    sequence: $sequence,
                },
            )
            .await
        }};
    }
    match request {
        Client::RecoveryStart => {
            let mut start =
                process_start_request(attached, attached.session_id).map_err(backend_status)?;
            start.background = true;
            pplx_space_spaced_guest_protocol::recovery::validate_start(&start)?;
            let stream = tokio::time::timeout(
                deadline,
                start_process_stream(&mut client, &attached.sandbox_id, start, route),
            )
            .await
            .map_err(|_| Status::deadline_exceeded("process start deadline elapsed"))?
            .map_err(backend_status)?;
            relay(sink, stream, &recovery, None, false).await
        }
        Client::RecoveryConnect {
            handle,
            output_cursor,
            resume_after_output_gap,
        } => {
            let handle = handle_for_session(handle, &recovery, attached.session_id, None)?;
            let request = node_api_request_with_route_token_state(
                SpacedConnectProcessRequest {
                    sandbox_id,
                    operation: Some(ops::ConnectProcessRequest {
                        process_id: String::new(),
                        recovery: Some(handle),
                        timeout: None,
                        output_cursor: Some(output_cursor),
                        resume_from_oldest_retained: false,
                        resume_after_output_gap: Some(resume_after_output_gap),
                    }),
                },
                route,
            )?;
            let stream = tokio::time::timeout(deadline, client.spaced_connect_process(request))
                .await
                .map_err(|_| Status::deadline_exceeded("process connect deadline elapsed"))??
                .into_inner();
            relay(
                sink,
                stream,
                &recovery,
                Some(output_cursor),
                resume_after_output_gap,
            )
            .await
        }
        Client::RecoveryInput {
            handle,
            sequence,
            data_base64,
        } => {
            let handle =
                handle_for_session(handle, &recovery, attached.session_id, Some(sequence))?;
            let data = BASE64_STANDARD
                .decode(data_base64)
                .map_err(|_| Status::invalid_argument("invalid input base64"))?;
            unary!(
                spaced_send_input,
                SpacedSendInputRequest,
                ops::SendInputRequest {
                    process_id: String::new(),
                    recovery: Some(handle),
                    data,
                },
                Some(sequence)
            )
        }
        Client::RecoveryCloseStdin { handle, sequence } => {
            let handle =
                handle_for_session(handle, &recovery, attached.session_id, Some(sequence))?;
            unary!(
                spaced_close_stdin,
                SpacedCloseStdinRequest,
                ops::CloseStdinRequest {
                    process_id: String::new(),
                    recovery: Some(handle),
                },
                Some(sequence)
            )
        }
        Client::RecoverySignal {
            handle,
            sequence,
            signal,
        } => {
            let handle =
                handle_for_session(handle, &recovery, attached.session_id, Some(sequence))?;
            unary!(
                spaced_send_signal,
                SpacedSendSignalRequest,
                ops::SendSignalRequest {
                    process_id: String::new(),
                    recovery: Some(handle),
                    signal,
                },
                Some(sequence)
            )
        }
        Client::RecoveryResize { handle, cols, rows } => {
            if cols == 0 || rows == 0 {
                return Err(Status::invalid_argument("PTY dimensions must be positive"));
            }
            let handle = handle_for_session(handle, &recovery, attached.session_id, None)?;
            unary!(
                spaced_resize_pty,
                SpacedResizePtyRequest,
                ops::ResizePtyRequest {
                    process_id: String::new(),
                    recovery: Some(handle),
                    size: Some(ops::PtySize {
                        cols: u32::from(cols),
                        rows: u32::from(rows)
                    }),
                },
                None
            )
        }
    }
}

async fn relay<S, T>(
    sink: &mut S,
    mut stream: T,
    start: &RecoveryStart,
    requested_cursor: Option<u64>,
    gaps: bool,
) -> Result<(), Status>
where
    S: Sink<Message> + Unpin,
    T: futures_util::Stream<Item = Result<ProcessEvent, Status>> + Unpin,
{
    let mut cursor = requested_cursor.unwrap_or(0);
    let mut first = true;
    let mut exited = false;
    while let Some(event) = stream.next().await {
        let event = event?;
        let frame = event_frame(
            event,
            start,
            &mut cursor,
            first,
            requested_cursor.is_some(),
            gaps,
        )?;
        first = false;
        exited |= matches!(frame, Server::RecoveryExited { .. });
        send(sink, &frame).await?;
    }
    if first || (requested_cursor.is_some() && !exited) {
        return Err(Status::data_loss("incomplete process recovery stream"));
    }
    Ok(())
}

fn event_frame(
    event: ProcessEvent,
    start: &RecoveryStart,
    cursor: &mut u64,
    first: bool,
    connect: bool,
    gaps: bool,
) -> Result<Server, Status> {
    use spaced_process_event::Event;
    if first
        && !matches!(
            (&event.event, connect),
            (Some(Event::Connected(_)), true) | (Some(Event::Started(_)), false)
        )
    {
        return Err(Status::failed_precondition(
            "process recovery was not acknowledged",
        ));
    }
    Ok(match event.event {
        Some(Event::Started(value)) if !connect && first => {
            if value.instance_id.as_deref() != Some(&start.expected_instance_id) {
                return Err(Status::failed_precondition(
                    "process recovery instance was not acknowledged",
                ));
            }
            let incarnation = value
                .process_incarnation
                .filter(|value| {
                    pplx_space_common_core::types::ProcessRecoveryToken::parse(value).is_ok()
                })
                .ok_or_else(|| {
                    Status::failed_precondition("process incarnation was not acknowledged")
                })?;
            Server::RecoveryStarted {
                handle: Handle {
                    instance_id: start.expected_instance_id.clone(),
                    activation_id: start.expected_activation_id.clone(),
                    process_id: event.process_id,
                    process_incarnation: incarnation,
                },
                pid: event.pid,
            }
        }
        Some(Event::Connected(value)) if connect && first => {
            if value.output_cursor != Some(*cursor) || value.resume_after_output_gap != gaps {
                return Err(Status::failed_precondition(
                    "process replay was not acknowledged",
                ));
            }
            Server::RecoveryConnected {
                output_cursor: *cursor,
                resume_after_output_gap: gaps,
            }
        }
        Some(Event::Stdout(value)) => {
            *cursor = cursor
                .checked_add(1)
                .ok_or_else(|| Status::out_of_range("output cursor exhausted"))?;
            Server::RecoveryStdout {
                output_cursor: *cursor,
                data_base64: BASE64_STANDARD.encode(value.data),
            }
        }
        Some(Event::Stderr(value)) => {
            *cursor = cursor
                .checked_add(1)
                .ok_or_else(|| Status::out_of_range("output cursor exhausted"))?;
            Server::RecoveryStderr {
                output_cursor: *cursor,
                data_base64: BASE64_STANDARD.encode(value.data),
            }
        }
        Some(Event::OutputGap(value)) if gaps => {
            if value.output_cursor < *cursor
                || value.skipped_events != value.output_cursor - *cursor
            {
                return Err(Status::data_loss("invalid process output gap"));
            }
            *cursor = value.output_cursor;
            Server::RecoveryOutputGap {
                skipped_events: value.skipped_events,
                output_cursor: *cursor,
                output_bytes_before: value.output_bytes_before,
            }
        }
        Some(Event::Exit(value)) => {
            let status = process_exit_status(&value);
            let (exit_code, signal) = match value.outcome {
                Some(spaced_process_exit::Outcome::Exited(value)) => (Some(value.exit_code), None),
                Some(spaced_process_exit::Outcome::Signaled(value)) => (None, value.signal),
                Some(spaced_process_exit::Outcome::TimedOut(value)) => (None, value.signal),
                _ => (None, None),
            };
            Server::RecoveryExited {
                status,
                exit_code,
                signal,
            }
        }
        _ => return Err(Status::data_loss("unexpected process recovery event")),
    })
}

#[cfg(test)]
mod tests;
