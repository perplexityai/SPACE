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
            // Only a forged id leaves no later id to number the stream with.
            let Some(replay) = ProcessReplayState::load(session_id, last_event_id) else {
                return data_plane_http_error(
                    StatusCode::BAD_REQUEST,
                    DataPlaneHttpErrorCode::InvalidRequest,
                    "last-event-id is past every event id this stream can send",
                    false,
                    ResponseOrigin::Client,
                    ResponseKind::Validation,
                );
            };
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
            let stream_shutdown_token = state.stream_shutdown_token.clone();
            let opened = before_stream_deadline(
                &stream_shutdown_token,
                ProcessSseState::new(
                    state,
                    session_id,
                    attached,
                    replay,
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
    event_id: ProcessEventId,
    /// The guest output journal position the client has received output up
    /// to, or `None` once the stream follows the live tail from an unknown one.
    backend_output_cursor: Option<u64>,
    /// The `seq` of the last output frame sent.
    output_seq: u64,
    process_id: Option<ProcessId>,
    process_bound: bool,
    binding: Option<PendingProcessBinding>,
    /// The backend stream was reopened, so the start it reports next repeats
    /// one the client already has.
    reopened: bool,
    revocation_tick: tokio::time::Interval,
    pending: VecDeque<Event>,
    finished: bool,
    shutdown_token: CancellationToken,
    start_permit: Option<NativeSessionAttachPermit>,
    _stream_permit: AttachedStreamPermit,
}

pub(crate) struct ProcessReplayState {
    pub(crate) event_id: ProcessEventId,
    pub(crate) backend_output_cursor: u64,
    pub(crate) pending: VecDeque<Event>,
}

/// What a `Last-Event-ID` resume produces before any live event: the next SSE
/// id and the `resync_required(cursor_replay_unavailable)` frame that carries
/// it. `None` when the client did not send a cursor.
///
/// This is the reconnection contract for file-watch sessions: the data plane
/// resumes the *live* stream at `last + 1` and does not replay what was
/// missed. Keep this the single place that decides so the docs and the
/// conformance smoke cannot drift.
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

/// A process stream's SSE id, which a resume decodes to recover the guest
/// output position to continue from, so any replica can serve it.
///
/// On the wire, bit 62 tags the id: no frame counter an earlier data plane
/// issued reaches it. The low [`PROCESS_EVENT_ID_GUARD_BITS`] bits are zero,
/// so an id that a data plane without this encoding counted on from by `+1`,
/// or that a client rounded through a floating-point number, fails to decode
/// rather than decoding to the wrong position. Above them come a frame counter
/// and a position field, whose top bit marks a block of ids without a position.
/// Those blocks sort above every position, so a stream that loses its
/// position keeps its ids increasing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProcessEventId {
    /// The client has received the guest's output up to `position`.
    Position { position: u64, frame: u64 },
    /// The id carries no position; a resume from it cannot be exact.
    Unknown { block: u64, frame: u64 },
}

const PROCESS_EVENT_ID_TAG: u64 = 1 << 62;
pub(crate) const PROCESS_EVENT_ID_GUARD_BITS: u32 = 16;
const PROCESS_EVENT_ID_FRAME_BITS: u32 = 12;
const PROCESS_EVENT_ID_FIELD_BITS: u32 = 34;
const PROCESS_EVENT_ID_FIELD_SHIFT: u32 = PROCESS_EVENT_ID_GUARD_BITS + PROCESS_EVENT_ID_FRAME_BITS;
pub(crate) const PROCESS_EVENT_ID_MAX_FRAME: u64 = (1 << PROCESS_EVENT_ID_FRAME_BITS) - 1;
const PROCESS_EVENT_ID_UNKNOWN: u64 = 1 << (PROCESS_EVENT_ID_FIELD_BITS - 1);
pub(crate) const PROCESS_EVENT_ID_MAX_POSITION: u64 = PROCESS_EVENT_ID_UNKNOWN - 1;
pub(crate) const PROCESS_EVENT_ID_MAX_RESUME_BLOCK: u64 = 1 << 32;

impl ProcessEventId {
    /// Precedes the first frame of a stream that starts at the beginning of
    /// the guest's output. Never sent.
    pub(crate) const START: Self = Self::Position {
        position: 0,
        frame: 0,
    };

    /// Precedes the first frame of a stream that replays the output from its
    /// start for a client whose `Last-Event-ID` carries no position, so that
    /// every id it sends is above that one. Never sent. A tagged id gets a
    /// fresh block without a position: positions from the start would sort
    /// below it. `None` for an id fewer than 2^32 blocks from the end, which
    /// only a forged one is, so a stream never runs out of blocks.
    pub(crate) fn first_after(last_event_id: u64) -> Option<Self> {
        if last_event_id < PROCESS_EVENT_ID_TAG {
            return Some(Self::START);
        }
        let first_unknown = Self::Unknown { block: 0, frame: 0 }.encode();
        let block = if last_event_id < first_unknown {
            0
        } else {
            let field = (last_event_id >> PROCESS_EVENT_ID_FIELD_SHIFT)
                & ((1 << PROCESS_EVENT_ID_FIELD_BITS) - 1);
            (field & !PROCESS_EVENT_ID_UNKNOWN) + 1
        };
        (block <= PROCESS_EVENT_ID_MAX_RESUME_BLOCK).then_some(Self::Unknown { block, frame: 0 })
    }

    pub(crate) fn encode(self) -> u64 {
        let (field, frame) = match self {
            Self::Position { position, frame } => (position, frame),
            Self::Unknown { block, frame } => (PROCESS_EVENT_ID_UNKNOWN | block, frame),
        };
        PROCESS_EVENT_ID_TAG
            | field << PROCESS_EVENT_ID_FIELD_SHIFT
            | frame << PROCESS_EVENT_ID_GUARD_BITS
    }

    /// `None` for an id this encoding did not produce.
    pub(crate) fn decode(id: u64) -> Option<Self> {
        if id >> 62 != 1 || id & ((1 << PROCESS_EVENT_ID_GUARD_BITS) - 1) != 0 {
            return None;
        }
        let frame = (id >> PROCESS_EVENT_ID_GUARD_BITS) & PROCESS_EVENT_ID_MAX_FRAME;
        let field = (id >> PROCESS_EVENT_ID_FIELD_SHIFT) & ((1 << PROCESS_EVENT_ID_FIELD_BITS) - 1);
        Some(if field & PROCESS_EVENT_ID_UNKNOWN == 0 {
            Self::Position {
                position: field,
                frame,
            }
        } else {
            Self::Unknown {
                block: field & !PROCESS_EVENT_ID_UNKNOWN,
                frame,
            }
        })
    }

    /// The id of the next frame, once the client has received output up to
    /// `position`. Always greater than this one.
    pub(crate) fn next(self, position: Option<u64>) -> Self {
        match self {
            Self::Position {
                position: last,
                frame,
            } => match position {
                Some(position) if position == last && frame < PROCESS_EVENT_ID_MAX_FRAME => {
                    Self::Position {
                        position,
                        frame: frame + 1,
                    }
                }
                Some(position) if position > last && position <= PROCESS_EVENT_ID_MAX_POSITION => {
                    Self::Position { position, frame: 0 }
                }
                // The position is unknown, beyond the field, behind this one,
                // or out of frames: no later id of this stream carries one, and
                // a resume from them replays behind `resync_required`.
                _ => Self::Unknown { block: 0, frame: 0 },
            },
            Self::Unknown { block, frame } if frame < PROCESS_EVENT_ID_MAX_FRAME => Self::Unknown {
                block,
                frame: frame + 1,
            },
            // A resumed stream starts at most at block 2^32
            // (`first_after`), and would need 2^44 frames to exhaust the rest.
            Self::Unknown { block, .. } => Self::Unknown {
                block: block + 1,
                frame: 0,
            },
        }
    }
}

impl ProcessReplayState {
    /// Where a process stream starts in the guest's output journal: a fresh
    /// attach replays the retained output from the start, and a resume
    /// continues from the position its `Last-Event-ID` carries. A resume from
    /// an id without one also replays from the start, behind
    /// `resync_required(cursor_replay_unavailable)`, as output the client
    /// already has may repeat. Every id the stream sends is above
    /// `Last-Event-ID`; `None` when no id can be (see
    /// [`ProcessEventId::first_after`]).
    pub(crate) fn load(session_id: DataPlaneSessionId, last_event_id: Option<u64>) -> Option<Self> {
        let Some(last_event_id) = last_event_id else {
            return Some(Self {
                event_id: ProcessEventId::START,
                backend_output_cursor: 0,
                pending: VecDeque::new(),
            });
        };
        if let Some(event_id @ ProcessEventId::Position { position, .. }) =
            ProcessEventId::decode(last_event_id)
        {
            return Some(Self {
                event_id,
                backend_output_cursor: position,
                pending: VecDeque::new(),
            });
        }
        let event_id = ProcessEventId::first_after(last_event_id)?.next(Some(0));
        let frame = resync_required_frame(
            session_id.kind().as_str(),
            DataPlaneResyncReason::CursorReplayUnavailable,
        );
        Some(Self {
            event_id,
            backend_output_cursor: 0,
            pending: VecDeque::from([sse_event_from_frame(&frame, Some(event_id.encode()))]),
        })
    }
}

impl ProcessSseState {
    async fn new(
        state: DataPlaneState,
        session_id: DataPlaneSessionId,
        mut attached: AttachedSession,
        replay: ProcessReplayState,
        stream_permit: AttachedStreamPermit,
        start_permit: Option<NativeSessionAttachPermit>,
    ) -> Result<Self, StreamBridgeError> {
        let (client, stream) = open_native_process_stream(
            &state,
            session_id,
            &mut attached,
            Some(replay.backend_output_cursor),
        )
        .await?;
        let mut pending = VecDeque::new();
        pending.push_back(sse_event_from_frame(&attached_frame(&attached), None));
        pending.extend(replay.pending);
        let process_id = attached.backend_process_id.clone();
        let process_bound = process_id.is_some();
        let revocation_tick = revocation_check_interval(&state);
        let shutdown_token = state.stream_shutdown_token.child_token();

        Ok(Self {
            state,
            session_id,
            attached,
            client,
            stream,
            event_id: replay.event_id,
            backend_output_cursor: Some(replay.backend_output_cursor),
            output_seq: replay.backend_output_cursor,
            process_id,
            process_bound,
            binding: None,
            reopened: false,
            revocation_tick,
            pending,
            finished: false,
            shutdown_token,
            start_permit,
            _stream_permit: stream_permit,
        })
    }

    /// Counts the client's output from the start of the guest's again, after
    /// the position it resumed from turned out not to be in it. The ids sent
    /// after this carry no position: they must stay above the ones already
    /// sent, which positions from the start would sort below.
    fn restart_from_output_start(&mut self) {
        self.backend_output_cursor = Some(0);
        self.output_seq = 0;
        self.push_frame(resync_required_frame(
            self.session_id.kind().as_str(),
            DataPlaneResyncReason::CursorOutOfWindow,
        ));
    }

    fn push_frame(&mut self, frame: DataPlaneServerFrame) {
        self.event_id = self.event_id.next(self.backend_output_cursor);
        self.pending
            .push_back(sse_event_from_frame(&frame, Some(self.event_id.encode())));
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
                        let mut repeated_start = false;
                        if matches!(
                            event.event,
                            Some(spaced_process_event::Event::Started(_))
                                | Some(spaced_process_event::Event::Connected(_))
                        ) {
                            repeated_start = std::mem::take(&mut state.reopened);
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
                                if matches!(
                                    event.event,
                                    Some(spaced_process_event::Event::Started(_))
                                ) && state.backend_output_cursor != Some(0)
                                {
                                    // This attach started the process, so its
                                    // output begins here: the position the
                                    // client resumed from was another's.
                                    state.restart_from_output_start();
                                }
                                state.process_bound = true;
                                state.binding = Some(PendingProcessBinding::spawn(
                                    &state.state,
                                    state.session_id,
                                    event_process_id,
                                    state.start_permit.take(),
                                ));
                            }
                        }
                        let mut output_seq = state.output_seq;
                        let mut batch = process_event_frames(event, &mut output_seq);
                        if repeated_start {
                            batch.frames.retain(|frame| {
                                !matches!(frame, DataPlaneServerFrame::ProcessStarted { .. })
                            });
                        }
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
                            if process_frame_advances_backend_cursor(&frame) {
                                state.output_seq = output_seq;
                                if let Some(cursor) = state.backend_output_cursor.as_mut() {
                                    *cursor = output_seq;
                                }
                            }
                            state.push_frame(frame);
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
                    Err(status) if is_output_window_miss(&status) => {
                        // The guest does not hold the output this stream asked
                        // for. Report the gap, then reopen: from the start of
                        // the output when the cursor was past its end, else at
                        // the live tail, as what was evicted is gone.
                        let Some(process_id) = state.process_id.clone() else {
                            state.finish_with_error(StreamBridgeError::BackendStatus(status));
                            return state.pending.pop_front();
                        };
                        let reopen_at = (is_output_cursor_ahead(&status)
                            && state.backend_output_cursor != Some(0))
                        .then_some(0);
                        if reopen_at.is_some() {
                            state.restart_from_output_start();
                        } else {
                            state.backend_output_cursor = None;
                            state.push_frame(resync_required_frame(
                                state.session_id.kind().as_str(),
                                DataPlaneResyncReason::CursorOutOfWindow,
                            ));
                        }
                        match connect_process_stream(
                            &mut state.client,
                            &state.attached.sandbox_id,
                            &process_id,
                            reopen_at,
                            state.attached.space_api_route_token.as_ref(),
                        )
                        .await
                        {
                            Ok(stream) => {
                                state.stream = stream;
                                state.reopened = true;
                            }
                            Err(error) => state.finish_with_error(error),
                        }
                        return state.pending.pop_front();
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
                                state.reopened = true;
                                state.push_frame(DataPlaneServerFrame::BackendReconnected {
                                    route_generation: refreshed.route_generation,
                                    current_node_id: refreshed.current_node_id.to_string(),
                                });
                                // From a known position the reopened stream
                                // continues where this one stopped; only one
                                // that follows the live tail can skip output.
                                if state.backend_output_cursor.is_none() {
                                    state.push_frame(resync_required_frame(
                                        state.session_id.kind().as_str(),
                                        DataPlaneResyncReason::ProcessStreamReconnected,
                                    ));
                                }
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

/// Whether the guest ended a process stream because its cursor is outside the
/// retained output: `resource_exhausted` once that output has been evicted (the
/// node's per-sandbox limiter uses the same code, but it refuses a call before
/// any stream exists), or a cursor past the end of the output.
fn is_output_window_miss(status: &tonic::Status) -> bool {
    is_output_cursor_ahead(status)
        || (status.code() == tonic::Code::ResourceExhausted
            && pplx_space_common_grpc::UdsPerSandboxLimitExceeded::from_status(status).is_none())
}

/// A guest older than this check waits for output to reach such a cursor.
fn is_output_cursor_ahead(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::OutOfRange
        && status
            .metadata()
            .get(pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            == Some(pplx_space_common_core::spaced::PROCESS_OUTPUT_CURSOR_AHEAD)
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
