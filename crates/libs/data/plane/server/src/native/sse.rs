//! Native server-sent event streams for process and file-watch sessions.

use super::*;

pub(crate) async fn session_events(
    State(state): State<DataPlaneState>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if let Err(error) = reject_query_tokens(uri.query()) {
        return error.into_response();
    }
    let session_id = match session_id.parse::<DataPlaneSessionId>() {
        Ok(session_id) => session_id,
        Err(_) => {
            return data_plane_http_error(
                StatusCode::BAD_REQUEST,
                DataPlaneHttpErrorCode::InvalidRequest,
                "invalid session id",
                false,
                ResponseOrigin::Client,
                ResponseKind::Validation,
            );
        }
    };
    if !matches!(
        session_id.kind(),
        SessionKind::Process | SessionKind::FileWatch
    ) {
        return data_plane_http_error(
            StatusCode::BAD_REQUEST,
            DataPlaneHttpErrorCode::InvalidRequest,
            "events endpoint only supports process and file-watch sessions",
            false,
            ResponseOrigin::Client,
            ResponseKind::Validation,
        );
    }
    let token = match token_from_headers(&headers) {
        Ok(Some(token)) => token,
        Ok(None) => {
            return data_plane_http_error(
                StatusCode::UNAUTHORIZED,
                DataPlaneHttpErrorCode::Unauthorized,
                "data-plane token required",
                false,
                ResponseOrigin::Client,
                ResponseKind::Auth,
            );
        }
        Err(error) => return error.into_response(),
    };
    if state.drain_started.is_cancelled() {
        return native_draining_response(session_id.kind().as_str());
    }
    let last_event_id = match last_event_id(&headers) {
        Ok(last_event_id) => last_event_id,
        Err(message) => {
            return data_plane_http_error(
                StatusCode::BAD_REQUEST,
                DataPlaneHttpErrorCode::InvalidRequest,
                message,
                false,
                ResponseOrigin::Client,
                ResponseKind::Validation,
            );
        }
    };

    let attached = match resolve_session_route(&state, session_id, &token).await {
        Ok(attached) => attached,
        Err(error) => {
            pplx_space_metrics::data_plane::attach(
                session_id.kind().as_str(),
                error.metric_label(),
            );
            return route_resolution_http_error(error);
        }
    };
    // Again after the route lookup: SIGTERM may have landed while it ran.
    if state.drain_started.is_cancelled() {
        return native_draining_response(session_id.kind().as_str());
    }
    let stream_permit = match state.attached_streams.try_acquire_attached(&attached) {
        Ok(permit) => permit,
        Err(limit) => {
            pplx_space_metrics::data_plane::attach(
                session_id.kind().as_str(),
                limit.metric_label(),
            );
            return too_many_attached_streams_response(limit);
        }
    };
    let stream_state = match attached.kind {
        SessionKind::Process => {
            let start_permit = match state.native_sessions.try_acquire(session_id, attached.kind) {
                Ok(permit) => permit,
                Err(()) => {
                    pplx_space_metrics::data_plane::attach(
                        session_id.kind().as_str(),
                        "session_already_attached",
                    );
                    return data_plane_http_error(
                        StatusCode::CONFLICT,
                        DataPlaneHttpErrorCode::Conflict,
                        "process session is already attached",
                        true,
                        ResponseOrigin::DataPlane,
                        ResponseKind::Admission,
                    );
                }
            };
            // A pty process is never started here: this stream can neither
            // deliver nor close its input, so it would wait and outlive the
            // request. A process already bound to the session reconnects as
            // usual, since reading its output starts nothing. The cached route
            // can predate a binding just made elsewhere, so an unbound one is
            // read again; and a WebSocket start reports the process before its
            // binding commits, so a still-unbound session connects to its
            // derived process id, keeps that stream and binds it, and is
            // refused only when the guest has no such process.
            let (attached, input) = if attached.backend_process_id.is_none()
                && process_session_needs_pty(&attached)
            {
                match resolve_session_route_with_cache(&state, session_id, &token, false).await {
                    Ok(current) if current.backend_process_id.is_some() => {
                        (current, ProcessInput::Closed)
                    }
                    Ok(current) => (current, ProcessInput::ConnectOnly),
                    Err(error) => {
                        pplx_space_metrics::data_plane::attach(
                            session_id.kind().as_str(),
                            error.metric_label(),
                        );
                        return route_resolution_http_error(error);
                    }
                }
            } else {
                (attached, ProcessInput::Closed)
            };
            let stream_shutdown_token = state.stream_shutdown_token.clone();
            let opened = before_stream_deadline(
                &stream_shutdown_token,
                ProcessSseState::new(
                    state,
                    session_id,
                    attached,
                    last_event_id,
                    input,
                    stream_permit,
                    start_permit,
                ),
            )
            .await;
            let Some(opened) = opened else {
                return native_draining_response(session_id.kind().as_str());
            };
            match opened {
                Ok(stream_state) => SessionSseState::Process(stream_state),
                Err(error)
                    if input == ProcessInput::ConnectOnly && is_missing_guest_process(&error) =>
                {
                    pplx_space_metrics::data_plane::attach(
                        session_id.kind().as_str(),
                        "input_unsupported",
                    );
                    return data_plane_http_error(
                        StatusCode::BAD_REQUEST,
                        DataPlaneHttpErrorCode::InvalidRequest,
                        "events stream cannot send input; attach a pty process session over WebSocket",
                        false,
                        ResponseOrigin::Client,
                        ResponseKind::Validation,
                    );
                }
                Err(error) => {
                    let result = stream_bridge_error_label(&error);
                    pplx_space_metrics::data_plane::attach(session_id.kind().as_str(), result);
                    tracing::warn!(%error, "failed to start process event stream");
                    return process_stream_start_http_error(&error);
                }
            }
        }
        SessionKind::FileWatch => {
            let stream_shutdown_token = state.stream_shutdown_token.clone();
            let opened = before_stream_deadline(
                &stream_shutdown_token,
                FileWatchSseState::new(state, session_id, attached, last_event_id, stream_permit),
            )
            .await;
            let Some(opened) = opened else {
                return native_draining_response(session_id.kind().as_str());
            };
            match opened {
                Ok(stream_state) => SessionSseState::FileWatch(stream_state),
                Err(error) => {
                    let result = stream_bridge_error_label(&error);
                    pplx_space_metrics::data_plane::attach(session_id.kind().as_str(), result);
                    tracing::warn!(%error, "failed to start file-watch event stream");
                    if let Some(response) = backend_refusal_http_error(&error) {
                        return response;
                    }
                    return data_plane_http_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        DataPlaneHttpErrorCode::StreamUnavailable,
                        "file-watch stream is not available",
                        true,
                        ResponseOrigin::Node,
                        ResponseKind::BackendTransport,
                    );
                }
            }
        }
        SessionKind::Terminal | SessionKind::Port | SessionKind::Protocol => {
            return data_plane_http_error(
                StatusCode::BAD_REQUEST,
                DataPlaneHttpErrorCode::InvalidRequest,
                "session kind does not support event streaming",
                false,
                ResponseOrigin::Client,
                ResponseKind::Validation,
            );
        }
    };

    pplx_space_metrics::data_plane::attach(session_id.kind().as_str(), "ok");
    let stream = futures_util::stream::unfold(stream_state, next_session_sse_event);
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

pub(crate) enum SessionSseState {
    Process(ProcessSseState),
    FileWatch(FileWatchSseState),
}

pub(crate) async fn next_session_sse_event(
    state: SessionSseState,
) -> Option<(Result<Event, Infallible>, SessionSseState)> {
    match state {
        SessionSseState::Process(state) => next_process_sse_event(state)
            .await
            .map(|(event, state)| (event, SessionSseState::Process(state))),
        SessionSseState::FileWatch(state) => next_file_watch_sse_event(state)
            .await
            .map(|(event, state)| (event, SessionSseState::FileWatch(state))),
    }
}

pub(crate) struct ProcessSseState {
    state: DataPlaneState,
    session_id: DataPlaneSessionId,
    attached: AttachedSession,
    client: WorkerApiClient<GrpcClientTrace<Channel>>,
    stream: tonic::Streaming<ProcessEvent>,
    event_seq: u64,
    backend_output_cursor: Option<u64>,
    process_id: Option<ProcessId>,
    process_bound: bool,
    binding: Option<PendingProcessBinding>,
    revocation_tick: tokio::time::Interval,
    pending: VecDeque<Event>,
    finished: bool,
    shutdown_token: CancellationToken,
    start_permit: Option<NativeSessionAttachPermit>,
    _stream_permit: AttachedStreamPermit,
}

pub(crate) struct ProcessReplayState {
    pub(crate) event_seq: u64,
    pub(crate) backend_output_cursor: Option<u64>,
    pub(crate) pending: VecDeque<Event>,
}

/// What a `Last-Event-ID` resume produces before any live event: the next SSE
/// id and the `resync_required(cursor_replay_unavailable)` frame that carries
/// it. `None` when the client did not send a cursor.
///
/// This is the public reconnection contract for both process and file-watch
/// sessions: the data plane resumes the *live* stream at `last + 1` and does
/// not replay what was missed, because it does not map SSE ids onto the guest
/// output journal. Keep this the single place that decides so the docs, the
/// conformance smoke, and both session kinds cannot drift.
///
/// `last + 1` cannot overflow: the request boundary rejects cursors above
/// [`auth::MAX_LAST_EVENT_ID`] (`i64::MAX`) with a 400, so a resumed stream
/// never has to saturate onto an id the client already acknowledged.
pub(crate) fn resume_resync_frame(
    kind: &str,
    last_event_id: Option<u64>,
) -> Option<(u64, DataPlaneServerFrame)> {
    let after_sequence = last_event_id?;
    debug_assert!(after_sequence <= auth::MAX_LAST_EVENT_ID);
    Some((
        after_sequence.saturating_add(1),
        resync_required_frame(kind, DataPlaneResyncReason::CursorReplayUnavailable),
    ))
}

impl ProcessReplayState {
    pub(crate) fn load(session_id: DataPlaneSessionId, last_event_id: Option<u64>) -> Self {
        let Some((event_seq, frame)) =
            resume_resync_frame(session_id.kind().as_str(), last_event_id)
        else {
            return Self {
                event_seq: 0,
                backend_output_cursor: None,
                pending: VecDeque::new(),
            };
        };

        let mut pending = VecDeque::new();
        pending.push_back(sse_event_from_frame(&frame, Some(event_seq)));

        Self {
            event_seq,
            backend_output_cursor: None,
            pending,
        }
    }
}

impl ProcessSseState {
    async fn new(
        state: DataPlaneState,
        session_id: DataPlaneSessionId,
        mut attached: AttachedSession,
        last_event_id: Option<u64>,
        input: ProcessInput,
        stream_permit: AttachedStreamPermit,
        start_permit: Option<NativeSessionAttachPermit>,
    ) -> Result<Self, StreamBridgeError> {
        let replay = ProcessReplayState::load(session_id, last_event_id);
        let (client, stream) = open_native_process_stream(
            &state,
            session_id,
            &mut attached,
            replay.backend_output_cursor,
            input,
        )
        .await?;
        let mut pending = VecDeque::new();
        pending.push_back(sse_event_from_frame(&attached_frame(&attached), None));
        pending.extend(replay.pending);
        let process_id = attached.backend_process_id.clone();
        let process_bound = process_id.is_some();
        let revocation_tick = revocation_check_interval(&state);
        let backend_output_cursor = replay
            .backend_output_cursor
            .or_else(|| attached.backend_process_id.is_none().then_some(0));
        let shutdown_token = state.stream_shutdown_token.child_token();

        Ok(Self {
            state,
            session_id,
            attached,
            client,
            stream,
            event_seq: replay.event_seq,
            backend_output_cursor,
            process_id,
            process_bound,
            binding: None,
            revocation_tick,
            pending,
            finished: false,
            shutdown_token,
            start_permit,
            _stream_permit: stream_permit,
        })
    }

    fn push_frame(&mut self, frame: DataPlaneServerFrame) {
        self.event_seq = self.event_seq.saturating_add(1);
        self.pending
            .push_back(sse_event_from_frame(&frame, Some(self.event_seq)));
    }

    fn finish(&mut self, result: &'static str) {
        pplx_space_metrics::data_plane::stream_closed(self.session_id.kind().as_str(), result);
        self.finished = true;
    }

    fn finish_with_frame(&mut self, frame: DataPlaneServerFrame, result: &'static str) {
        self.finish(result);
        self.push_frame(frame);
    }

    fn finish_with_error(&mut self, error: StreamBridgeError) {
        let result = stream_bridge_error_label(&error);
        pplx_space_metrics::data_plane::stream_closed(self.session_id.kind().as_str(), result);
        tracing::warn!(%error, "data-plane process event stream failed");
        self.finished = true;
        self.push_frame(DataPlaneServerFrame::Error {
            code: DataPlaneErrorCode::BackendStreamError,
            message: PROCESS_STREAM_FAILED.to_string(),
            retryable: true,
        });
    }
}

impl Drop for ProcessSseState {
    fn drop(&mut self) {
        self.state.revocation_cache.forget(self.session_id);
    }
}

pub(crate) async fn next_process_sse_event(
    mut state: ProcessSseState,
) -> Option<(Result<Event, Infallible>, ProcessSseState)> {
    if let Some(event) = state.pending.pop_front() {
        return Some((Ok(event), state));
    }
    if state.finished {
        return None;
    }
    let shutdown_token = state.shutdown_token.clone();
    // Raced as a whole, so the deadline also interrupts an await inside the
    // loop: a revocation check, a route refresh, a backend reopen. Ending the
    // body lets the client reconnect elsewhere with `Last-Event-ID`.
    let event = tokio::select! {
        biased;
        () = shutdown_token.cancelled() => None,
        event = next_process_sse_frame(&mut state) => Some(event),
    };
    match event {
        Some(event) => event.map(|event| (Ok(event), state)),
        None => {
            if !state.finished {
                state.finish("shutdown");
            }
            None
        }
    }
}

/// The next event of a live process stream, or `None` once it has ended.
async fn next_process_sse_frame(state: &mut ProcessSseState) -> Option<Event> {
    loop {
        tokio::select! {
            _ = state.revocation_tick.tick() => {
                match session_is_revoked(&state.state, state.session_id).await {
                    Ok(true) => {
                        state.finish_with_frame(
                            DataPlaneServerFrame::Closed {
                                reason: StreamCloseReason::Revoked,
                            },
                            "revoked",
                        );
                        return state.pending.pop_front();
                    }
                    Ok(false) => {}
                    Err(error) => {
                        state.finish_with_error(error);
                        return state.pending.pop_front();
                    }
                }
            }
            bound = process_binding_settled(&mut state.binding) => {
                if let Err(error) = bound {
                    state.finish_with_error(error);
                    return state.pending.pop_front();
                }
            }
            event = state.stream.next() => {
                let Some(event) = event else {
                    if let Err(error) = settle_process_binding(&mut state.binding).await {
                        state.finish_with_error(error);
                        return state.pending.pop_front();
                    }
                    state.finish_with_frame(
                        DataPlaneServerFrame::Closed {
                            reason: StreamCloseReason::BackendClosed,
                        },
                        "backend_closed",
                    );
                    return state.pending.pop_front();
                };
                match event {
                    Ok(event) => {
                        if matches!(
                            event.event,
                            Some(spaced_process_event::Event::Started(_))
                                | Some(spaced_process_event::Event::Connected(_))
                        ) {
                            let event_process_id: ProcessId = match event.process_id.parse() {
                                Ok(process_id) => process_id,
                                Err(_) => {
                                    state.finish_with_error(StreamBridgeError::RouteState(
                                        "invalid_backend_process_id",
                                    ));
                                    return state.pending.pop_front();
                                }
                            };
                            state.process_id = Some(event_process_id.clone());
                            if !state.process_bound {
                                state.process_bound = true;
                                state.binding = Some(PendingProcessBinding::spawn(
                                    &state.state,
                                    state.session_id,
                                    event_process_id,
                                    state.start_permit.take(),
                                ));
                            }
                        }
                        let mut output_seq = state.backend_output_cursor.unwrap_or_default();
                        let batch = process_event_frames(event, &mut output_seq);
                        if batch
                            .frames
                            .iter()
                            .any(|frame| matches!(frame, DataPlaneServerFrame::ProcessExited { .. }))
                            && let Err(error) = settle_process_binding(&mut state.binding).await
                        {
                            state.finish_with_error(error);
                            return state.pending.pop_front();
                        }
                        record_process_output_bytes(state.session_id.kind().as_str(), &batch);
                        for frame in batch.frames {
                            let exited =
                                matches!(frame, DataPlaneServerFrame::ProcessExited { .. });
                            let advances_backend_cursor =
                                process_frame_advances_backend_cursor(&frame);
                            state.push_frame(frame);
                            if advances_backend_cursor {
                                state.backend_output_cursor = Some(output_seq);
                            }
                            if exited {
                                state.push_frame(DataPlaneServerFrame::Closed {
                                    reason: StreamCloseReason::BackendClosed,
                                });
                                state.finish("backend_closed");
                            }
                        }
                        if let Some(event) = state.pending.pop_front() {
                            return Some(event);
                        }
                    }
                    Err(status)
                        if status.code() == tonic::Code::Unavailable
                            || is_refreshable_route_identity_rejection(&status) =>
                    {
                        let Some(current_process_id) = state.process_id.clone() else {
                            state.finish_with_error(StreamBridgeError::BackendStatus(status));
                            return state.pending.pop_front();
                        };
                        let current_generation = state.attached.route_generation;
                        let (refreshed, refresh_dimension) =
                            match refresh_native_stream_route(
                                &state.state,
                                &state.attached,
                                &status,
                            )
                            .await
                            {
                            Ok(refreshed) => refreshed,
                            Err(error) => {
                                state.finish_with_error(error);
                                return state.pending.pop_front();
                            }
                        };
                        let reason = reconnect_reason_for_route_change(
                            current_generation,
                            refreshed.route_generation,
                        );
                        record_backend_channel_reset(state.session_id.kind().as_str(), &reason);
                        state.push_frame(DataPlaneServerFrame::BackendReconnecting {
                            reason: reason.clone(),
                            route_generation: current_generation,
                        });
                        if let Err(error) = record_session_reconnect(
                            &state.state,
                            state.session_id,
                            reason,
                            current_generation,
                        )
                        .await
                        {
                            state.finish_with_error(error);
                            return state.pending.pop_front();
                        }
                        let reopened = async {
                            let channel = state
                                .state
                                .backend_channels
                                .channel(&refreshed.backend_endpoint, state.session_id).await?;
                            let mut client =
                                WorkerApiClient::new(wrap_grpc_client_channel(channel));
                            let stream = connect_process_stream(
                                &mut client,
                                &refreshed.sandbox_id,
                                &current_process_id,
                                state.backend_output_cursor,
                                refreshed.space_api_route_token.as_ref(),
                            )
                            .await?;
                            Ok((client, stream))
                        }
                        .await;
                        match finish_native_stream_refresh(refresh_dimension, reopened) {
                            Ok((client, stream)) => {
                                state.client = client;
                                state.stream = stream;
                                state.push_frame(DataPlaneServerFrame::BackendReconnected {
                                    route_generation: refreshed.route_generation,
                                    current_node_id: refreshed.current_node_id.to_string(),
                                });
                                state.push_frame(resync_required_frame(
                                    state.session_id.kind().as_str(),
                                    DataPlaneResyncReason::ProcessStreamReconnected,
                                ));
                                state.attached = refreshed;
                            }
                            Err(error) => state.finish_with_error(error),
                        }
                        return state.pending.pop_front();
                    }
                    Err(status) => {
                        state.finish_with_error(StreamBridgeError::BackendStatus(status));
                        return state.pending.pop_front();
                    }
                }
            }
        }
    }
}

pub(crate) struct FileWatchSseState {
    state: DataPlaneState,
    session_id: DataPlaneSessionId,
    attached: AttachedSession,
    watch: WatchOperation,
    client: WorkerApiClient<GrpcClientTrace<Channel>>,
    stream: tonic::Streaming<FileEvent>,
    event_seq: u64,
    revocation_tick: tokio::time::Interval,
    pending: VecDeque<Event>,
    finished: bool,
    shutdown_token: CancellationToken,
    _stream_permit: AttachedStreamPermit,
}

impl FileWatchSseState {
    async fn new(
        state: DataPlaneState,
        session_id: DataPlaneSessionId,
        attached: AttachedSession,
        last_event_id: Option<u64>,
        stream_permit: AttachedStreamPermit,
    ) -> Result<Self, StreamBridgeError> {
        let mut attached =
            refresh_file_watch_attached_if_route_token_stale(&state, attached).await?;
        let watch = file_watch_request(&attached)?;
        let (client, stream) =
            open_native_watch_stream(&state, session_id, &mut attached, watch.clone()).await?;
        let mut pending = VecDeque::new();
        pending.push_back(sse_event_from_frame(&attached_frame(&attached), None));
        let mut event_seq = last_event_id.unwrap_or_default();
        if let Some((resumed_seq, frame)) =
            resume_resync_frame(session_id.kind().as_str(), last_event_id)
        {
            event_seq = resumed_seq;
            pending.push_back(sse_event_from_frame(&frame, Some(event_seq)));
        }
        let revocation_tick = revocation_check_interval(&state);
        let shutdown_token = state.stream_shutdown_token.child_token();

        Ok(Self {
            state,
            session_id,
            attached,
            watch,
            client,
            stream,
            event_seq,
            revocation_tick,
            pending,
            finished: false,
            shutdown_token,
            _stream_permit: stream_permit,
        })
    }

    fn push_frame(&mut self, frame: DataPlaneServerFrame) {
        self.event_seq = self.event_seq.saturating_add(1);
        self.pending
            .push_back(sse_event_from_frame(&frame, Some(self.event_seq)));
    }

    fn finish(&mut self, result: &'static str) {
        pplx_space_metrics::data_plane::stream_closed(self.session_id.kind().as_str(), result);
        self.finished = true;
    }

    fn finish_with_frame(&mut self, frame: DataPlaneServerFrame, result: &'static str) {
        self.finish(result);
        self.push_frame(frame);
    }

    fn finish_with_error(&mut self, error: StreamBridgeError) {
        let result = stream_bridge_error_label(&error);
        pplx_space_metrics::data_plane::stream_closed(self.session_id.kind().as_str(), result);
        tracing::warn!(%error, "data-plane file watch event stream failed");
        self.finished = true;
        self.push_frame(DataPlaneServerFrame::Error {
            code: DataPlaneErrorCode::BackendStreamError,
            message: FILE_WATCH_STREAM_FAILED.to_string(),
            retryable: true,
        });
    }
}

impl Drop for FileWatchSseState {
    fn drop(&mut self) {
        self.state.revocation_cache.forget(self.session_id);
    }
}

pub(crate) async fn next_file_watch_sse_event(
    mut state: FileWatchSseState,
) -> Option<(Result<Event, Infallible>, FileWatchSseState)> {
    if let Some(event) = state.pending.pop_front() {
        return Some((Ok(event), state));
    }
    if state.finished {
        return None;
    }
    let shutdown_token = state.shutdown_token.clone();
    // Raced as a whole, so the deadline also interrupts an await inside the
    // loop: a revocation check, a route refresh, a backend reopen. Ending the
    // body lets the client reconnect elsewhere with `Last-Event-ID`.
    let event = tokio::select! {
        biased;
        () = shutdown_token.cancelled() => None,
        event = next_file_watch_sse_frame(&mut state) => Some(event),
    };
    match event {
        Some(event) => event.map(|event| (Ok(event), state)),
        None => {
            if !state.finished {
                state.finish("shutdown");
            }
            None
        }
    }
}

/// The next event of a live file watch stream, or `None` once it has ended.
async fn next_file_watch_sse_frame(state: &mut FileWatchSseState) -> Option<Event> {
    loop {
        tokio::select! {
            _ = state.revocation_tick.tick() => {
                match session_is_revoked(&state.state, state.session_id).await {
                    Ok(true) => {
                        state.finish_with_frame(
                            DataPlaneServerFrame::Closed {
                                reason: StreamCloseReason::Revoked,
                            },
                            "revoked",
                        );
                        return state.pending.pop_front();
                    }
                    Ok(false) => {}
                    Err(error) => {
                        state.finish_with_error(error);
                        return state.pending.pop_front();
                    }
                }
            }
            event = state.stream.next() => {
                let Some(event) = event else {
                    state.finish_with_frame(
                        DataPlaneServerFrame::Closed {
                            reason: StreamCloseReason::BackendClosed,
                        },
                        "backend_closed",
                    );
                    return state.pending.pop_front();
                };
                match event {
                    Ok(event) => {
                        state.push_frame(file_event_frame(event, state.event_seq.saturating_add(1)));
                        return state.pending.pop_front();
                    }
                    Err(status)
                        if status.code() == tonic::Code::Unavailable
                            || is_refreshable_route_identity_rejection(&status) =>
                    {
                        let current_generation = state.attached.route_generation;
                        let (refreshed, refresh_dimension) =
                            match refresh_native_stream_route(
                                &state.state,
                                &state.attached,
                                &status,
                            )
                            .await
                            {
                            Ok(refreshed) => refreshed,
                            Err(error) => {
                                state.finish_with_error(error);
                                return state.pending.pop_front();
                            }
                        };
                        let refreshed = match refresh_file_watch_attached_if_route_token_stale(
                            &state.state,
                            refreshed,
                        )
                        .await
                        {
                            Ok(refreshed) => refreshed,
                            Err(error) => {
                                state.finish_with_error(native_stream_refresh_failed(
                                    refresh_dimension,
                                    error,
                                ));
                                return state.pending.pop_front();
                            }
                        };
                        let reason = reconnect_reason_for_route_change(
                            current_generation,
                            refreshed.route_generation,
                        );
                        record_backend_channel_reset(state.session_id.kind().as_str(), &reason);
                        state.push_frame(DataPlaneServerFrame::BackendReconnecting {
                            reason: reason.clone(),
                            route_generation: current_generation,
                        });
                        if let Err(error) = record_session_reconnect(
                            &state.state,
                            state.session_id,
                            reason,
                            current_generation,
                        )
                        .await
                        {
                            state.finish_with_error(error);
                            return state.pending.pop_front();
                        }
                        let reopened = async {
                            let channel = state
                                .state
                                .backend_channels
                                .channel(&refreshed.backend_endpoint, state.session_id).await?;
                            let mut client =
                                WorkerApiClient::new(wrap_grpc_client_channel(channel));
                            let stream = watch_stream(
                                &mut client,
                                &refreshed.sandbox_id,
                                state.watch.clone(),
                                refreshed.space_api_route_token.as_ref(),
                            )
                            .await?;
                            Ok((client, stream))
                        }
                        .await;
                        match finish_native_stream_refresh(refresh_dimension, reopened) {
                            Ok((client, stream)) => {
                                state.client = client;
                                state.stream = stream;
                                state.push_frame(DataPlaneServerFrame::BackendReconnected {
                                    route_generation: refreshed.route_generation,
                                    current_node_id: refreshed.current_node_id.to_string(),
                                });
                                state.push_frame(resync_required_frame(
                                    state.session_id.kind().as_str(),
                                    DataPlaneResyncReason::FileWatchRestarted,
                                ));
                                state.attached = refreshed;
                            }
                            Err(error) => state.finish_with_error(error),
                        }
                        return state.pending.pop_front();
                    }
                    Err(status) => {
                        state.finish_with_error(StreamBridgeError::BackendStatus(status));
                        return state.pending.pop_front();
                    }
                }
            }
        }
    }
}
