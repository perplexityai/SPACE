//! Native WebSocket attaches: the handshake, the per-kind bridges, and the
//! client control frames they carry.

use super::*;

pub(crate) async fn attach_session(
    State(state): State<DataPlaneState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
    uri: axum::http::Uri,
) -> Result<Response, AttachError> {
    reject_query_tokens(uri.query())?;
    let session_id = session_id
        .parse::<DataPlaneSessionId>()
        .map_err(|_| AttachError::BadRequest("invalid session id"))?;
    let header_token = token_from_headers(&headers)?;
    if state.drain_started.is_cancelled() {
        return Ok(native_draining_response(session_id.kind().as_str()));
    }
    // Take the tracker token before the upgrade, not inside the callback: the
    // HTTP connection finishes when the socket is handed off, and the drain
    // must not see zero tasks in between.
    let task = state.upgrade_tasks.token();
    Ok(upgrade.on_upgrade(move |socket| async move {
        let _task = task;
        handle_socket(state, peer.ip(), session_id, header_token, socket).await;
    }))
}

pub(crate) async fn handle_socket(
    state: DataPlaneState,
    source_ip: IpAddr,
    session_id: DataPlaneSessionId,
    header_token: Option<String>,
    mut socket: WebSocket,
) {
    let attach_deadline = tokio::time::Instant::now() + state.attach_deadline;
    let Ok(_guard) = state.unauthenticated.try_register(source_ip) else {
        pplx_space_metrics::data_plane::attach(
            session_id.kind().as_str(),
            "too_many_unauthenticated_sockets",
        );
        let _ = socket
            .send(Message::Text(
                error_json(DataPlaneErrorCode::TooManyUnauthenticatedSockets).into(),
            ))
            .await;
        let _ = socket.close().await;
        return;
    };

    // Authenticating and resolving the route can wait on the client's first
    // frame, so race it against the drain: a socket that has not attached by
    // the time the drain starts is told to reconnect at once rather than
    // holding the drain open.
    let attached = tokio::select! {
        biased;
        () = state.drain_started.cancelled() => None,
        attached = authenticate_native_socket(
            &state,
            session_id,
            header_token,
            &mut socket,
            attach_deadline,
        ) => {
            match attached {
                Some(attached) => Some(attached),
                None => return,
            }
        }
    };
    let Some(attached) = attached.filter(|_| !state.drain_started.is_cancelled()) else {
        pplx_space_metrics::data_plane::attach(session_id.kind().as_str(), "draining");
        state.revocation_cache.forget(session_id);
        send_restart_close(&mut socket, DRAINING_CLOSE_REASON).await;
        return;
    };
    let native_session_permit = match state.native_sessions.try_acquire(session_id, attached.kind) {
        Ok(permit) => permit,
        Err(()) => {
            pplx_space_metrics::data_plane::attach(
                session_id.kind().as_str(),
                "session_already_attached",
            );
            let _ = socket
                .send(Message::Text(
                    error_json(DataPlaneErrorCode::SessionAlreadyAttached).into(),
                ))
                .await;
            let _ = socket.close().await;
            return;
        }
    };
    let stream_permit = match state.attached_streams.try_acquire_attached(&attached) {
        Ok(permit) => permit,
        Err(limit) => {
            pplx_space_metrics::data_plane::attach(
                session_id.kind().as_str(),
                limit.metric_label(),
            );
            let _ = socket
                .send(Message::Text(
                    error_json(DataPlaneErrorCode::TooManyAttachedStreams).into(),
                ))
                .await;
            let _ = socket.close().await;
            return;
        }
    };

    drop(_guard);
    let _ = socket
        .send(Message::Text(attached_json(&attached).into()))
        .await;
    pplx_space_metrics::data_plane::attach(session_id.kind().as_str(), "ok");
    let _stream_permit = stream_permit;
    let _native_session_permit = native_session_permit;
    let revocation_cache = Arc::clone(&state.revocation_cache);
    bridge_attached_session(state, session_id, attached, socket).await;
    revocation_cache.forget(session_id);
}

/// Authenticate a native WebSocket and resolve its session route by
/// `deadline`. On failure the client already has its error frame and the
/// socket is closed.
pub(super) async fn authenticate_native_socket(
    state: &DataPlaneState,
    session_id: DataPlaneSessionId,
    header_token: Option<String>,
    socket: &mut WebSocket,
    deadline: tokio::time::Instant,
) -> Option<AttachedSession> {
    let attached =
        attach_token_and_route(header_token, socket.recv(), deadline, |token| async move {
            resolve_session_route(state, session_id, &token).await
        })
        .await;
    let frame = match attached {
        Ok(attached) => return Some(attached),
        Err(AttachRefused::Token(error)) => {
            pplx_space_metrics::data_plane::attach(session_id.kind().as_str(), error.as_str());
            error_json(error)
        }
        Err(AttachRefused::Route(error)) => {
            pplx_space_metrics::data_plane::attach(
                session_id.kind().as_str(),
                error.metric_label(),
            );
            route_resolution_error_json(error)
        }
    };
    let _ = socket.send(Message::Text(frame.into())).await;
    let _ = socket.close().await;
    None
}

/// Why a WebSocket attach ended before it had a route.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum AttachRefused {
    Token(DataPlaneErrorCode),
    Route(RouteResolutionError),
}

/// The attach's token, from its header or else its first frame, then its
/// route, waiting while the sandbox is being placed: both by one `deadline`
/// counted from accept, so a client slow to send its token has that much
/// less to wait for the route.
pub(super) async fn attach_token_and_route<T, Recv, E, Resolve, Fut>(
    header_token: Option<String>,
    first_frame: Recv,
    deadline: tokio::time::Instant,
    mut resolve: Resolve,
) -> Result<T, AttachRefused>
where
    Recv: Future<Output = Option<Result<Message, E>>>,
    Resolve: FnMut(String) -> Fut,
    Fut: Future<Output = Result<T, RouteResolutionError>>,
{
    let token = match header_token {
        Some(token) => token,
        None => first_frame_attach_token_from_recv(first_frame, deadline)
            .await
            .map_err(AttachRefused::Token)?,
    };
    retry_not_ready_until(deadline, || resolve(token.clone()))
        .await
        .map_err(AttachRefused::Route)
}

pub(crate) async fn bridge_attached_session(
    state: DataPlaneState,
    session_id: DataPlaneSessionId,
    attached: AttachedSession,
    socket: WebSocket,
) {
    match attached.kind {
        SessionKind::Terminal | SessionKind::Process => {
            let kind = attached.kind.as_str();
            match bridge_process_session(state, session_id, attached, socket).await {
                Ok(end) => pplx_space_metrics::data_plane::stream_closed(kind, end.result_label()),
                Err(error) => {
                    let result = stream_bridge_error_label(&error);
                    pplx_space_metrics::data_plane::stream_closed(kind, result);
                    tracing::warn!(%error, "data-plane process bridge failed");
                }
            }
        }
        SessionKind::FileWatch => {
            let kind = attached.kind.as_str();
            match bridge_file_watch_session(state, session_id, attached, socket).await {
                Ok(end) => pplx_space_metrics::data_plane::stream_closed(kind, end.result_label()),
                Err(error) => {
                    let result = stream_bridge_error_label(&error);
                    pplx_space_metrics::data_plane::stream_closed(kind, result);
                    tracing::warn!(%error, "data-plane file watch bridge failed");
                }
            }
        }
        SessionKind::Port if attached.target_protocol == Some(SessionTargetProtocol::Tcp) => {
            let kind = attached.kind.as_str();
            match bridge_raw_service_session(state, attached, socket).await {
                Ok(end) => pplx_space_metrics::data_plane::stream_closed(kind, end.result_label()),
                Err(error) => {
                    let result = stream_bridge_error_label(&error);
                    pplx_space_metrics::data_plane::stream_closed(kind, result);
                    tracing::warn!(%error, "data-plane raw service bridge failed");
                }
            }
        }
        SessionKind::Port | SessionKind::Protocol => {
            pplx_space_metrics::data_plane::stream_closed(
                attached.kind.as_str(),
                "unsupported_session_kind",
            );
            close_with_error(socket, DataPlaneErrorCode::UnsupportedSessionKind, false).await;
        }
    }
}

pub(super) async fn bridge_raw_service_session(
    state: DataPlaneState,
    mut attached: AttachedSession,
    mut socket: WebSocket,
) -> Result<BridgeEnd, StreamBridgeError> {
    let session_id = attached.session_id;
    let shutdown_token = state.stream_shutdown_token.child_token();
    // The deadline can pass while the ingress is still opening.
    let opened = tokio::select! {
        () = shutdown_token.cancelled() => None,
        opened = native_service_io(&state, &mut attached) => Some(opened),
    };
    let Some(ingress_io) = opened else {
        send_restart_close(&mut socket, SHUTDOWN_CLOSE_REASON).await;
        return Ok(BridgeEnd::Shutdown);
    };
    let (mut ingress_read, mut ingress_write) = tokio::io::split(ingress_io?);
    let (mut sink, mut source) = socket.split();
    let mut revocation_tick = revocation_check_interval(&state);
    let mut buffer = vec![0_u8; 16 * 1024];

    // Raced as a whole rather than with a shutdown branch, so the deadline also
    // interrupts an await inside the loop: a revocation check, a write to an
    // ingress or a client that stopped reading. Dropping the loop releases the
    // sink for the 1012 close.
    let session = async {
        loop {
            tokio::select! {
                _ = revocation_tick.tick() => {
                    if session_is_revoked(&state, session_id).await? {
                        let _ = sink.close().await;
                        return Ok(BridgeEnd::Completed);
                    }
                }
                message = source.next() => {
                    match message {
                        Some(Ok(Message::Binary(data))) => {
                            ingress_write.write_all(&data).await.map_err(|_| {
                                StreamBridgeError::RouteState("service_ingress_write_failed")
                            })?;
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            let _ = ingress_write.shutdown().await;
                            return Ok(BridgeEnd::Completed);
                        }
                        Some(Ok(Message::Ping(data))) => {
                            sink.send(Message::Pong(data))
                                .await
                                .map_err(|_| StreamBridgeError::ClientClosed)?;
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Text(_))) => {
                            return Err(StreamBridgeError::RouteState(
                                "raw_service_requires_binary_frames",
                            ));
                        }
                        Some(Err(_)) => return Err(StreamBridgeError::ClientClosed),
                    }
                }
                read = ingress_read.read(&mut buffer) => {
                    // The ingress pump watches the same deadline, so its end can
                    // reach this branch before the shutdown one does.
                    if !matches!(read, Ok(read) if read > 0) && shutdown_token.is_cancelled() {
                        send_restart_close(&mut sink, SHUTDOWN_CLOSE_REASON).await;
                        return Ok(BridgeEnd::Shutdown);
                    }
                    let read = read.map_err(|_| {
                        StreamBridgeError::RouteState("service_ingress_read_failed")
                    })?;
                    if read == 0 {
                        let _ = sink.close().await;
                        return Ok(BridgeEnd::Completed);
                    }
                    let data = buffer
                        .get(..read)
                        .ok_or(StreamBridgeError::RouteState("service_ingress_read_failed"))?;
                    sink.send(Message::Binary(data.to_vec().into()))
                        .await
                        .map_err(|_| StreamBridgeError::ClientClosed)?;
                }
            }
        }
    };
    let ended = tokio::select! {
        biased;
        () = shutdown_token.cancelled() => None,
        ended = session => Some(ended),
    };
    match ended {
        Some(ended) => ended,
        None => {
            send_restart_close(&mut sink, SHUTDOWN_CLOSE_REASON).await;
            Ok(BridgeEnd::Shutdown)
        }
    }
}

pub(super) async fn native_service_io(
    state: &DataPlaneState,
    attached: &mut AttachedSession,
) -> Result<Box<dyn AsyncReadWrite>, StreamBridgeError> {
    pplx_space_metrics::data_plane::service_route_transport(
        ServiceRouteSurface::Native,
        ServiceRouteTransport::ServicePortIngress,
    );
    let session_id = attached.session_id;
    let _waiter = pplx_space_common_svc::ingress_admission::IngressPreparationPermit::acquire(
        Some(attached.sandbox_id),
    )
    .map_err(StreamBridgeError::ClusterStatus)?;
    let deadline = tokio::time::Instant::now()
        + pplx_space_common_svc::ingress_admission::preparation_timeout();
    let io = tokio::time::timeout_at(deadline, async {
        for attempt in 0..2 {
            match native_service_port_ingress_io(state, attached).await {
                Ok(io) => {
                    let fresh = resolve_session_route_internal(state, session_id).await?;
                    if fresh.route_identity() == attached.route_identity() {
                        return Ok(io);
                    }
                    *attached = fresh;
                }
                Err(error) => {
                    let retryable = error
                        .transport_status
                        .as_ref()
                        .is_some_and(crate::runtime_route::retryable_ingress_attachment);
                    if attempt == 1 || !retryable {
                        return Err(native_service_port_ingress_open_error(error));
                    }
                    *attached = resolve_session_route_internal(state, session_id).await?;
                }
            }
        }
        Err(StreamBridgeError::RouteState("ingress_route_changed"))
    })
    .await
    .map_err(|_| native_route_refresh_timeout())??;
    Ok(Box::new(io))
}

pub(super) trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

pub(crate) async fn close_with_error(
    mut socket: WebSocket,
    code: DataPlaneErrorCode,
    retryable: bool,
) {
    let _ = socket
        .send(Message::Text(
            server_frame_json(&DataPlaneServerFrame::Error {
                message: code.as_str().to_string(),
                code,
                retryable,
                details: Vec::new(),
            })
            .into(),
        ))
        .await;
    let _ = socket.close().await;
}

pub(super) const MAX_CLIENT_FRAMES_DURING_OPEN: usize = 64;
pub(super) const MAX_CLIENT_BYTES_DURING_OPEN: usize = 1024 * 1024;
/// How long a pong may wait for a client that stops reading before the client
/// is treated as gone.
pub(super) const PONG_SEND_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) enum OpenWatch<T> {
    Opened(T, VecDeque<Message>),
    ClientClosed,
    Overflow,
}

/// Drive the initial backend open while reading the client socket, so a client
/// that disconnects mid-open drops the open. Pings are answered as the session
/// loop does; other frames are kept, in order and within a bound, for that loop.
///
/// A pong is sent alongside the open and the client read, never instead of
/// them, so a client that stops reading cannot stall either. Only the latest
/// unanswered ping is kept, which bounds pings as the limits bound other frames.
pub(super) async fn open_while_watching_client<T, S, K>(
    open: impl Future<Output = T>,
    source: &mut S,
    sink: &mut K,
) -> OpenWatch<T>
where
    S: futures_util::Stream<Item = Result<Message, axum::Error>> + Unpin,
    K: futures_util::Sink<Message> + Unpin,
{
    tokio::pin!(open);
    let mut buffered = VecDeque::new();
    let mut buffered_bytes = 0_usize;
    // The payload of the ping to answer, and when its pong must be out by.
    let mut pong: Option<(bytes::Bytes, tokio::time::Instant)> = None;
    loop {
        tokio::select! {
            biased;
            result = &mut open => return OpenWatch::Opened(result, buffered),
            // A send interrupted by another branch is retried; a repeated pong
            // is harmless.
            sent = send_pong(sink, pong.clone()), if pong.is_some() => match sent {
                Ok(()) => pong = None,
                Err(()) => return OpenWatch::ClientClosed,
            },
            message = source.next() => match message {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return OpenWatch::ClientClosed,
                Some(Ok(Message::Ping(payload))) => {
                    let deadline = pong.as_ref().map_or_else(
                        || tokio::time::Instant::now() + PONG_SEND_TIMEOUT,
                        |(_, deadline)| *deadline,
                    );
                    pong = Some((payload, deadline));
                }
                Some(Ok(message)) => {
                    buffered_bytes = buffered_bytes.saturating_add(client_message_len(&message));
                    buffered.push_back(message);
                    if buffered.len() > MAX_CLIENT_FRAMES_DURING_OPEN
                        || buffered_bytes > MAX_CLIENT_BYTES_DURING_OPEN
                    {
                        return OpenWatch::Overflow;
                    }
                }
            },
        }
    }
}

/// Send a pending pong by its deadline; with none pending, never completes.
pub(super) async fn send_pong<K>(
    sink: &mut K,
    pong: Option<(bytes::Bytes, tokio::time::Instant)>,
) -> Result<(), ()>
where
    K: futures_util::Sink<Message> + Unpin,
{
    let Some((payload, deadline)) = pong else {
        return std::future::pending().await;
    };
    match tokio::time::timeout_at(deadline, sink.send(Message::Pong(payload))).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) | Err(_) => Err(()),
    }
}

pub(super) fn client_message_len(message: &Message) -> usize {
    match message {
        Message::Text(text) => text.as_str().len(),
        Message::Binary(data) | Message::Ping(data) | Message::Pong(data) => data.len(),
        Message::Close(_) => 0,
    }
}

pub(crate) async fn bridge_process_session(
    state: DataPlaneState,
    session_id: DataPlaneSessionId,
    mut attached: AttachedSession,
    socket: WebSocket,
) -> Result<BridgeEnd, StreamBridgeError> {
    let shutdown_token = state.stream_shutdown_token.child_token();
    let (mut sink, mut source) = socket.split();
    let sandbox_id = attached.sandbox_id;
    let open = open_native_process_stream(
        &state,
        session_id,
        &mut attached,
        None,
        ProcessInput::Writable,
    );
    // The deadline can pass while the backend stream is still opening.
    let opened = tokio::select! {
        () = shutdown_token.cancelled() => None,
        opened = open_while_watching_client(open, &mut source, &mut sink) => Some(opened),
    };
    let Some(opened) = opened else {
        send_restart_close(&mut sink, SHUTDOWN_CLOSE_REASON).await;
        return Ok(BridgeEnd::Shutdown);
    };
    let (initial, buffered) = match opened {
        OpenWatch::Opened(initial, buffered) => (initial, buffered),
        // Dropping the open resets the backend stream, which cancels a start
        // that is still queued downstream.
        OpenWatch::ClientClosed => return Ok(BridgeEnd::Completed),
        // The open may already have passed the guest's start, so a fresh
        // session could run the command twice; only re-attaching this one is
        // safe, which is not what `retryable` tells a client.
        OpenWatch::Overflow => {
            let _ = send_server_frame(
                &mut sink,
                &DataPlaneServerFrame::Error {
                    code: DataPlaneErrorCode::SessionNotReady,
                    message: "too many client frames before the session was ready".to_string(),
                    retryable: false,
                    details: Vec::new(),
                },
            )
            .await;
            let _ = sink.close().await;
            return Ok(BridgeEnd::Completed);
        }
    };
    let mut source = futures_util::stream::iter(buffered.into_iter().map(Ok)).chain(source);
    let mut stream = match initial {
        Ok((_client, initial)) => initial,
        Err(error) => {
            forget_route_of_refused_sandbox(&state.route_cache, session_id, &error);
            let _ =
                send_server_frame(&mut sink, &stream_start_error_frame(&error, sandbox_id)).await;
            let _ = sink.close().await;
            return Err(error);
        }
    };
    let mut output_seq = 0_u64;
    let mut process_id = attached.backend_process_id.clone();
    let mut process_bound = attached.backend_process_id.is_some();
    let mut binding: Option<PendingProcessBinding> = None;
    let mut revocation_tick = revocation_check_interval(&state);

    // Raced as a whole, as in `bridge_raw_service_session`, so a stalled
    // revocation check, route refresh, backend reopen or control RPC doesn't
    // hold the socket past the deadline.
    let session = async {
        loop {
            tokio::select! {
                _ = revocation_tick.tick() => {
                    if session_is_revoked(&state, session_id).await? {
                        send_server_frame(
                            &mut sink,
                            &DataPlaneServerFrame::Closed {
                                reason: StreamCloseReason::Revoked,
                            },
                        )
                        .await
                        .map_err(|_| StreamBridgeError::ClientClosed)?;
                        let _ = sink.close().await;
                        return Ok(BridgeEnd::Completed);
                    }
                }
                bound = process_binding_settled(&mut binding) => bound?,
                event = stream.next() => {
                    let Some(event) = event else {
                        settle_process_binding(&mut binding).await?;
                        let _ = send_server_frame(
                            &mut sink,
                            &DataPlaneServerFrame::Closed {
                                reason: StreamCloseReason::BackendClosed,
                            },
                        )
                        .await;
                        let _ = sink.close().await;
                        return Ok(BridgeEnd::Completed);
                    };
                    match event {
                        Ok(event) => {
                            if matches!(
                                event.event,
                                Some(spaced_process_event::Event::Started(_))
                                    | Some(spaced_process_event::Event::Connected(_))
                            ) {
                                let event_process_id: ProcessId = event.process_id.parse()
                                    .map_err(|_| StreamBridgeError::RouteState("invalid_backend_process_id"))?;
                                process_id = Some(event_process_id.clone());
                                if !process_bound {
                                    process_bound = true;
                                    binding = Some(PendingProcessBinding::spawn(
                                        &state,
                                        session_id,
                                        event_process_id,
                                        None,
                                    ));
                                }
                            }
                            let batch = process_event_frames(event, &mut output_seq);
                            if batch.frames.iter().any(|frame| {
                                matches!(frame, DataPlaneServerFrame::ProcessExited { .. })
                            }) {
                                settle_process_binding(&mut binding).await?;
                            }
                            record_process_output_bytes(attached.kind.as_str(), &batch);
                            for frame in batch.frames {
                                if send_server_frame(&mut sink, &frame).await.is_err() {
                                    return Ok(BridgeEnd::Completed);
                                }
                                if matches!(frame, DataPlaneServerFrame::ProcessExited { .. }) {
                                    let _ = send_server_frame(
                                        &mut sink,
                                        &DataPlaneServerFrame::Closed {
                                            reason: StreamCloseReason::BackendClosed,
                                        },
                                    )
                                    .await;
                                    let _ = sink.close().await;
                                    return Ok(BridgeEnd::Completed);
                                }
                            }
                        }
                        Err(status) => {
                            if (status.code() == tonic::Code::Unavailable
                                || is_refreshable_route_identity_rejection(&status))
                                && let Some(current_process_id) = process_id.clone()
                            {
                                let (refreshed, refresh_dimension) =
                                    refresh_native_stream_route(&state, &attached, &status).await?;
                                let reason = reconnect_reason_for_route_change(
                                    attached.route_generation,
                                    refreshed.route_generation,
                                );
                                record_backend_channel_reset(attached.kind.as_str(), &reason);
                                send_server_frame(
                                    &mut sink,
                                    &DataPlaneServerFrame::BackendReconnecting {
                                        reason: reason.clone(),
                                        route_generation: attached.route_generation,
                                    },
                                )
                                .await
                                .map_err(|_| StreamBridgeError::ClientClosed)?;
                                record_session_reconnect(
                                    &state,
                                    session_id,
                                    reason,
                                    attached.route_generation,
                                )
                                .await?;
                                let reopened = async {
                                    let channel = state
                                        .backend_channels
                                        .channel(&refreshed.backend_endpoint, session_id).await?;
                                    let mut reconnect_client =
                                        WorkerApiClient::new(wrap_grpc_client_channel(channel));
                                    connect_process_stream(
                                        &mut reconnect_client,
                                        &refreshed.sandbox_id,
                                        &current_process_id,
                                        None,
                                        refreshed.space_api_route_token.as_ref(),
                                    )
                                    .await
                                }
                                .await;
                                stream = finish_native_stream_refresh(refresh_dimension, reopened)?;
                                send_server_frame(
                                    &mut sink,
                                    &DataPlaneServerFrame::BackendReconnected {
                                        route_generation: refreshed.route_generation,
                                        current_node_id: refreshed.current_node_id.to_string(),
                                    },
                                )
                                .await
                                .map_err(|_| StreamBridgeError::ClientClosed)?;
                                send_server_frame(
                                    &mut sink,
                                    &resync_required_frame(
                                        attached.kind.as_str(),
                                        DataPlaneResyncReason::ProcessStreamReconnected,
                                    ),
                                )
                                .await
                                .map_err(|_| StreamBridgeError::ClientClosed)?;
                                attached = refreshed;
                                continue;
                            }
                            let _ = send_server_frame(
                                &mut sink,
                                &DataPlaneServerFrame::Error {
                                    code: DataPlaneErrorCode::BackendStreamError,
                                    message: PROCESS_STREAM_FAILED.to_string(),
                                    retryable: backend_stream_status_retryable(&status),
                                    details: Vec::new(),
                                },
                            )
                            .await;
                            let _ = sink.close().await;
                            return Err(StreamBridgeError::BackendStatus(status));
                        }
                    }
                }
                message = source.next() => {
                    let Some(message) = message else {
                        return Ok(BridgeEnd::Completed);
                    };
                    match message {
                        Ok(Message::Close(_)) | Err(_) => return Ok(BridgeEnd::Completed),
                        Ok(Message::Ping(payload)) => {
                            sink.send(Message::Pong(payload)).await.map_err(|_| StreamBridgeError::ClientClosed)?;
                        }
                        Ok(Message::Text(text)) => {
                            if space_api_route_token_needs_refresh(&attached.space_api_route_token) {
                                let refreshed = refresh_session_route(&state, &attached).await?;
                                if attached_process_route_changed(&attached, &refreshed) {
                                    let Some(current_process_id) = process_id.clone() else {
                                        return Err(StreamBridgeError::RouteState(
                                            "route_token_refresh_process_unbound",
                                        ));
                                    };
                                    let reason = reconnect_reason_for_route_change(
                                        attached.route_generation,
                                        refreshed.route_generation,
                                    );
                                    record_backend_channel_reset(attached.kind.as_str(), &reason);
                                    send_server_frame(
                                        &mut sink,
                                        &DataPlaneServerFrame::BackendReconnecting {
                                            reason: reason.clone(),
                                            route_generation: attached.route_generation,
                                        },
                                    )
                                    .await
                                    .map_err(|_| StreamBridgeError::ClientClosed)?;
                                    record_session_reconnect(
                                        &state,
                                        session_id,
                                        reason,
                                        attached.route_generation,
                                    )
                                    .await?;
                                    let channel = state
                                        .backend_channels
                                        .channel(&refreshed.backend_endpoint, session_id).await?;
                                    let mut reconnect_client =
                                        WorkerApiClient::new(wrap_grpc_client_channel(channel));
                                    stream = connect_process_stream(
                                        &mut reconnect_client,
                                        &refreshed.sandbox_id,
                                        &current_process_id,
                                        None,
                                        refreshed.space_api_route_token.as_ref(),
                                    )
                                    .await?;
                                    send_server_frame(
                                        &mut sink,
                                        &DataPlaneServerFrame::BackendReconnected {
                                            route_generation: refreshed.route_generation,
                                            current_node_id: refreshed.current_node_id.to_string(),
                                        },
                                    )
                                    .await
                                    .map_err(|_| StreamBridgeError::ClientClosed)?;
                                    send_server_frame(
                                        &mut sink,
                                        &resync_required_frame(
                                            attached.kind.as_str(),
                                            DataPlaneResyncReason::ProcessStreamReconnected,
                                        ),
                                    )
                                    .await
                                    .map_err(|_| StreamBridgeError::ClientClosed)?;
                                }
                                attached = refreshed;
                            }
                            if handle_process_client_control_frame(
                                &mut sink,
                                &state,
                                &mut attached,
                                process_id.as_ref(),
                                &text,
                            )
                            .await?
                                == ClientFrameAction::Close
                            {
                                let _ = sink.close().await;
                                return Ok(BridgeEnd::Completed);
                            }
                        }
                        Ok(Message::Binary(_)) => {
                            send_server_frame(
                                &mut sink,
                                &DataPlaneServerFrame::Error {
                                    code: DataPlaneErrorCode::BinaryClientFramesNotSupported,
                                    message: "binary client frames are not supported".to_string(),
                                    retryable: false,
                                    details: Vec::new(),
                                },
                            )
                            .await
                            .map_err(|_| StreamBridgeError::ClientClosed)?;
                        }
                        Ok(Message::Pong(_)) => {}
                    }
                }
            }
        }
    };
    let ended = tokio::select! {
        biased;
        () = shutdown_token.cancelled() => None,
        ended = session => Some(ended),
    };
    match ended {
        Some(ended) => ended,
        None => {
            send_restart_close(&mut sink, SHUTDOWN_CLOSE_REASON).await;
            Ok(BridgeEnd::Shutdown)
        }
    }
}

pub(crate) async fn bridge_file_watch_session(
    state: DataPlaneState,
    session_id: DataPlaneSessionId,
    mut attached: AttachedSession,
    socket: WebSocket,
) -> Result<BridgeEnd, StreamBridgeError> {
    let watch = file_watch_request(&attached)?;
    let shutdown_token = state.stream_shutdown_token.child_token();
    let (mut sink, mut source) = socket.split();
    // The deadline can pass while the backend stream is still opening.
    let opened = tokio::select! {
        () = shutdown_token.cancelled() => None,
        opened = open_native_watch_stream(&state, session_id, &mut attached, watch.clone()) => {
            Some(opened)
        }
    };
    let Some(opened) = opened else {
        send_restart_close(&mut sink, SHUTDOWN_CLOSE_REASON).await;
        return Ok(BridgeEnd::Shutdown);
    };
    // As for a process: a failed opening, a refusal included, reaches the
    // client as an error frame rather than a dropped socket.
    let (_client, mut stream) = match opened {
        Ok(opened) => opened,
        Err(error) => {
            forget_route_of_refused_sandbox(&state.route_cache, session_id, &error);
            let _ = send_server_frame(
                &mut sink,
                &stream_start_error_frame(&error, attached.sandbox_id),
            )
            .await;
            let _ = sink.close().await;
            return Err(error);
        }
    };
    let mut event_seq = 0_u64;
    let mut revocation_tick = revocation_check_interval(&state);

    // Raced as a whole, as in `bridge_raw_service_session`.
    let session = async {
        loop {
            tokio::select! {
                _ = revocation_tick.tick() => {
                    if session_is_revoked(&state, session_id).await? {
                        send_server_frame(
                            &mut sink,
                            &DataPlaneServerFrame::Closed {
                                reason: StreamCloseReason::Revoked,
                            },
                        )
                        .await
                        .map_err(|_| StreamBridgeError::ClientClosed)?;
                        let _ = sink.close().await;
                        return Ok(BridgeEnd::Completed);
                    }
                }
                event = stream.next() => {
                    let Some(event) = event else {
                        let _ = send_server_frame(
                            &mut sink,
                            &DataPlaneServerFrame::Closed {
                                reason: StreamCloseReason::BackendClosed,
                            },
                        )
                        .await;
                        let _ = sink.close().await;
                        return Ok(BridgeEnd::Completed);
                    };
                    match event {
                        Ok(event) => {
                            event_seq = event_seq.saturating_add(1);
                            let frame = file_event_frame(event, event_seq);
                            if send_server_frame(&mut sink, &frame).await.is_err() {
                                return Ok(BridgeEnd::Completed);
                            }
                        }
                        Err(status) => {
                            if status.code() == tonic::Code::Unavailable
                                || is_refreshable_route_identity_rejection(&status)
                            {
                                let (refreshed, refresh_dimension) =
                                    refresh_native_stream_route(&state, &attached, &status).await?;
                                let reason = reconnect_reason_for_route_change(
                                    attached.route_generation,
                                    refreshed.route_generation,
                                );
                                record_backend_channel_reset(attached.kind.as_str(), &reason);
                                send_server_frame(
                                    &mut sink,
                                    &DataPlaneServerFrame::BackendReconnecting {
                                        reason: reason.clone(),
                                        route_generation: attached.route_generation,
                                    },
                                )
                                .await
                                .map_err(|_| StreamBridgeError::ClientClosed)?;
                                record_session_reconnect(
                                    &state,
                                    session_id,
                                    reason,
                                    attached.route_generation,
                                )
                                .await?;
                                let reopened = async {
                                    let channel = state
                                        .backend_channels
                                        .channel(&refreshed.backend_endpoint, session_id).await?;
                                    let mut reconnect_client =
                                        WorkerApiClient::new(wrap_grpc_client_channel(channel));
                                    watch_stream(
                                        &mut reconnect_client,
                                        &refreshed.sandbox_id,
                                        watch.clone(),
                                        refreshed.space_api_route_token.as_ref(),
                                    )
                                    .await
                                }
                                .await;
                                stream = finish_native_stream_refresh(refresh_dimension, reopened)?;
                                send_server_frame(
                                    &mut sink,
                                    &DataPlaneServerFrame::BackendReconnected {
                                        route_generation: refreshed.route_generation,
                                        current_node_id: refreshed.current_node_id.to_string(),
                                    },
                                )
                                .await
                                .map_err(|_| StreamBridgeError::ClientClosed)?;
                                send_server_frame(
                                    &mut sink,
                                    &resync_required_frame(
                                        attached.kind.as_str(),
                                        DataPlaneResyncReason::FileWatchRestarted,
                                    ),
                                )
                                .await
                                .map_err(|_| StreamBridgeError::ClientClosed)?;
                                attached = refreshed;
                                continue;
                            }
                            let _ = send_server_frame(
                                &mut sink,
                                &DataPlaneServerFrame::Error {
                                    code: DataPlaneErrorCode::BackendStreamError,
                                    message: FILE_WATCH_STREAM_FAILED.to_string(),
                                    retryable: backend_stream_status_retryable(&status),
                                    details: Vec::new(),
                                },
                            )
                            .await;
                            let _ = sink.close().await;
                            return Err(StreamBridgeError::BackendStatus(status));
                        }
                    }
                }
                message = source.next() => {
                    let Some(message) = message else {
                        return Ok(BridgeEnd::Completed);
                    };
                    match message {
                        Ok(Message::Close(_)) | Err(_) => return Ok(BridgeEnd::Completed),
                        Ok(Message::Ping(payload)) => {
                            sink.send(Message::Pong(payload)).await.map_err(|_| StreamBridgeError::ClientClosed)?;
                        }
                        Ok(Message::Text(text)) => {
                            if handle_client_control_frame(&mut sink, &text).await?
                                == ClientFrameAction::Close
                            {
                                let _ = sink.close().await;
                                return Ok(BridgeEnd::Completed);
                            }
                        }
                        Ok(Message::Binary(_)) => {
                            send_server_frame(
                                &mut sink,
                                &DataPlaneServerFrame::Error {
                                    code: DataPlaneErrorCode::BinaryClientFramesNotSupported,
                                    message: "binary client frames are not supported".to_string(),
                                    retryable: false,
                                    details: Vec::new(),
                                },
                            )
                            .await
                            .map_err(|_| StreamBridgeError::ClientClosed)?;
                        }
                        Ok(Message::Pong(_)) => {}
                    }
                }
            }
        }
    };
    let ended = tokio::select! {
        biased;
        () = shutdown_token.cancelled() => None,
        ended = session => Some(ended),
    };
    match ended {
        Some(ended) => ended,
        None => {
            send_restart_close(&mut sink, SHUTDOWN_CLOSE_REASON).await;
            Ok(BridgeEnd::Shutdown)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClientFrameAction {
    Continue,
    Close,
}

pub(crate) async fn handle_process_client_control_frame<S>(
    sink: &mut S,
    state: &DataPlaneState,
    attached: &mut AttachedSession,
    process_id: Option<&ProcessId>,
    text: &str,
) -> Result<ClientFrameAction, StreamBridgeError>
where
    S: SinkExt<Message> + Unpin,
{
    let frame = serde_json::from_str::<DataPlaneClientFrame>(text)
        .map_err(|_| StreamBridgeError::InvalidClientFrame)?;
    match frame {
        DataPlaneClientFrame::Attach { .. } => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Error {
                    code: DataPlaneErrorCode::AlreadyAttached,
                    message: "session is already attached".to_string(),
                    retryable: false,
                    details: Vec::new(),
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Input { seq, data_base64 } => {
            let Some(process_id) = process_id else {
                send_server_frame(
                    sink,
                    &DataPlaneServerFrame::Error {
                        code: DataPlaneErrorCode::ProcessNotStarted,
                        message: "process has not started yet".to_string(),
                        retryable: true,
                        details: Vec::new(),
                    },
                )
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
                return Ok(ClientFrameAction::Continue);
            };
            let data = BASE64_STANDARD
                .decode(data_base64)
                .map_err(|_| StreamBridgeError::InvalidClientFrame)?;
            let bytes = data.len() as u64;
            native_node_api_unary(
                state,
                attached,
                SpacedSendInputRequest {
                    sandbox_id: attached.sandbox_id.to_string(),
                    operation: Some(SendInputRequest {
                        process_id: process_id.to_string(),
                        data,
                    }),
                },
                |mut client, request| async move { client.spaced_send_input(request).await },
            )
            .await?;
            pplx_space_metrics::data_plane::stream_bytes_in(attached.kind.as_str(), bytes);
            send_server_frame(sink, &DataPlaneServerFrame::InputAccepted { seq })
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Resize { cols, rows } => {
            let Some(process_id) = process_id else {
                send_server_frame(
                    sink,
                    &DataPlaneServerFrame::Error {
                        code: DataPlaneErrorCode::ProcessNotStarted,
                        message: "process has not started yet".to_string(),
                        retryable: true,
                        details: Vec::new(),
                    },
                )
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
                return Ok(ClientFrameAction::Continue);
            };
            if cols == 0 || rows == 0 {
                send_server_frame(
                    sink,
                    &DataPlaneServerFrame::Error {
                        code: DataPlaneErrorCode::InvalidResize,
                        message: "terminal resize rows and cols must be non-zero".to_string(),
                        retryable: false,
                        details: Vec::new(),
                    },
                )
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
                return Ok(ClientFrameAction::Continue);
            }
            let resize = native_node_api_unary(
                state,
                attached,
                SpacedResizePtyRequest {
                    sandbox_id: attached.sandbox_id.to_string(),
                    operation: Some(ResizePtyRequest {
                        process_id: process_id.to_string(),
                        size: Some(PtySize {
                            rows: u32::from(rows),
                            cols: u32::from(cols),
                        }),
                    }),
                },
                |mut client, request| async move { client.spaced_resize_pty(request).await },
            )
            .await;
            match resize {
                Ok(_) => {}
                Err(StreamBridgeError::BackendStatus(status))
                    if status.code() == tonic::Code::FailedPrecondition =>
                {
                    tracing::debug!(grpc_message = %status.message(), "terminal resize was refused");
                    send_server_frame(
                        sink,
                        &DataPlaneServerFrame::Error {
                            code: DataPlaneErrorCode::ResizeNotSupported,
                            message: RESIZE_REFUSED.to_string(),
                            retryable: false,
                            details: Vec::new(),
                        },
                    )
                    .await
                    .map_err(|_| StreamBridgeError::ClientClosed)?;
                }
                Err(error) => return Err(error),
            }
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Signal { seq, signal } => {
            let Some(process_id) = process_id else {
                send_server_frame(
                    sink,
                    &DataPlaneServerFrame::Error {
                        code: DataPlaneErrorCode::ProcessNotStarted,
                        message: "process has not started yet".to_string(),
                        retryable: true,
                        details: Vec::new(),
                    },
                )
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
                return Ok(ClientFrameAction::Continue);
            };
            native_node_api_unary(
                state,
                attached,
                SpacedSendSignalRequest {
                    sandbox_id: attached.sandbox_id.to_string(),
                    operation: Some(SendSignalRequest {
                        process_id: process_id.to_string(),
                        signal,
                    }),
                },
                |mut client, request| async move { client.spaced_send_signal(request).await },
            )
            .await?;
            send_server_frame(sink, &DataPlaneServerFrame::ControlAccepted { seq })
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::CloseStdin { seq } => {
            let Some(process_id) = process_id else {
                send_server_frame(
                    sink,
                    &DataPlaneServerFrame::Error {
                        code: DataPlaneErrorCode::ProcessNotStarted,
                        message: "process has not started yet".to_string(),
                        retryable: true,
                        details: Vec::new(),
                    },
                )
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
                return Ok(ClientFrameAction::Continue);
            };
            native_node_api_unary(
                state,
                attached,
                SpacedCloseStdinRequest {
                    sandbox_id: attached.sandbox_id.to_string(),
                    operation: Some(CloseStdinRequest {
                        process_id: process_id.to_string(),
                    }),
                },
                |mut client, request| async move { client.spaced_close_stdin(request).await },
            )
            .await?;
            send_server_frame(sink, &DataPlaneServerFrame::ControlAccepted { seq })
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Heartbeat { seq } => {
            send_server_frame(sink, &DataPlaneServerFrame::ControlAccepted { seq })
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Close => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Closed {
                    reason: StreamCloseReason::ClientClosed,
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Close)
        }
    }
}

pub(crate) async fn handle_client_control_frame<S>(
    sink: &mut S,
    text: &str,
) -> Result<ClientFrameAction, StreamBridgeError>
where
    S: SinkExt<Message> + Unpin,
{
    let frame = serde_json::from_str::<DataPlaneClientFrame>(text)
        .map_err(|_| StreamBridgeError::InvalidClientFrame)?;
    match frame {
        DataPlaneClientFrame::Attach { .. } => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Error {
                    code: DataPlaneErrorCode::AlreadyAttached,
                    message: "session is already attached".to_string(),
                    retryable: false,
                    details: Vec::new(),
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Input { .. } => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Error {
                    code: DataPlaneErrorCode::InputNotSupported,
                    message: "interactive stdin is not supported by this backend stream"
                        .to_string(),
                    retryable: false,
                    details: Vec::new(),
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Signal { .. } | DataPlaneClientFrame::CloseStdin { .. } => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Error {
                    code: DataPlaneErrorCode::InputNotSupported,
                    message: "process control is not supported by this backend stream".to_string(),
                    retryable: false,
                    details: Vec::new(),
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Heartbeat { seq } => {
            send_server_frame(sink, &DataPlaneServerFrame::ControlAccepted { seq })
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Resize { .. } => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Error {
                    code: DataPlaneErrorCode::ResizeNotSupported,
                    message: "terminal resize is not supported by this backend stream".to_string(),
                    retryable: false,
                    details: Vec::new(),
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Close => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Closed {
                    reason: StreamCloseReason::ClientClosed,
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Close)
        }
    }
}
