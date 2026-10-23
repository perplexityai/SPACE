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
            let Some(replay) = ProcessReplayState::load(last_event_id) else {
                pplx_space_metrics::data_plane::attach(
                    session_id.kind().as_str(),
                    "invalid_last_event_id",
                );
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
                    replay,
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
    event_id: ProcessEventId,
    /// The guest output journal position the client has received output up
    /// to, or `None` once the stream follows the live tail from an unknown one.
    backend_output_cursor: Option<u64>,
    /// The `seq` of the last output frame sent.
    output_seq: u64,
    process_id: Option<ProcessId>,
    process_bound: bool,
    binding: Option<PendingProcessBinding>,
    /// This response has sent `process_started`; a reopened backend stream
    /// reports the start again, and the client already has it.
    started_sent: bool,
    /// `resync_required(cursor_out_of_window)` was sent and no output has
    /// followed it, so another gap before any output is the same one.
    gap_reported: bool,
    /// The stream moved to the oldest output the guest retains after an
    /// eviction, and no output has followed. Falling behind again before any
    /// does is a race the stream keeps losing, so it follows the live tail.
    at_oldest_retained: bool,
    /// Backend reopens after an output window miss since the last output or
    /// exit, capped at [`PROCESS_OUTPUT_WINDOW_REOPENS_WITHOUT_PROGRESS`].
    window_reopens_without_progress: u32,
    revocation_tick: tokio::time::Interval,
    pending: VecDeque<Event>,
    finished: bool,
    shutdown_token: CancellationToken,
    start_permit: Option<NativeSessionAttachPermit>,
    _stream_permit: AttachedStreamPermit,
}

struct ProcessReplayState {
    event_id: ProcessEventId,
    backend_output_cursor: u64,
    /// Sent with `event_id` before any live event. Counted only once the
    /// stream opens, so a refused attach does not count a frame never sent.
    resync: Option<DataPlaneResyncReason>,
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
/// On the wire, bit 52 tags the id: no frame counter an earlier data plane
/// issued reaches it, and every id stays below 2^53, so a client that parses
/// it into a double still holds it exactly. The low
/// [`PROCESS_EVENT_ID_GUARD_BITS`] bits are zero, so an id that a data plane
/// without this encoding counted on from by `+1` fails to decode rather than
/// decoding to the wrong position, unless it counted a multiple of 2^16
/// frames. Above them come a frame counter and a position field, whose top
/// bit marks a block of ids without a position. Those blocks sort above every
/// position, so a stream that loses its position keeps its ids increasing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProcessEventId {
    /// The client has received the guest's output up to `position`.
    Position { position: u64, frame: u64 },
    /// The id carries no position; a resume from it cannot be exact.
    Unknown { block: u64, frame: u64 },
}

const PROCESS_EVENT_ID_TAG_BIT: u32 = 52;
const PROCESS_EVENT_ID_TAG: u64 = 1 << PROCESS_EVENT_ID_TAG_BIT;
const PROCESS_EVENT_ID_GUARD_BITS: u32 = 16;
/// Identified frames at one position: output moves it, and so does every gap
/// that keeps it, so a position has at most two ids, the unsent
/// [`ProcessEventId::START`] and the `resync_required` that starts a replay
/// for an id without a position (see [`process_frame_has_event_id`]). The
/// rest of the 36 bits below the tag go to positions: ids carry one for a
/// process's first 2^33 - 1 outputs.
const PROCESS_EVENT_ID_FRAME_BITS: u32 = 2;
const PROCESS_EVENT_ID_FIELD_SHIFT: u32 = PROCESS_EVENT_ID_GUARD_BITS + PROCESS_EVENT_ID_FRAME_BITS;
const PROCESS_EVENT_ID_FIELD_BITS: u32 = PROCESS_EVENT_ID_TAG_BIT - PROCESS_EVENT_ID_FIELD_SHIFT;
const PROCESS_EVENT_ID_FIELD_MASK: u64 = (1 << PROCESS_EVENT_ID_FIELD_BITS) - 1;
const PROCESS_EVENT_ID_MAX_FRAME: u64 = (1 << PROCESS_EVENT_ID_FRAME_BITS) - 1;
const PROCESS_EVENT_ID_UNKNOWN: u64 = 1 << (PROCESS_EVENT_ID_FIELD_BITS - 1);
const PROCESS_EVENT_ID_MAX_POSITION: u64 = PROCESS_EVENT_ID_UNKNOWN - 1;
/// The highest block a resumed stream starts in: half the blocks, so every
/// stream has at least 2^(field + frame - 2) = 2^34 ids left to send.
const PROCESS_EVENT_ID_MAX_RESUME_BLOCK: u64 = 1 << (PROCESS_EVENT_ID_FIELD_BITS - 2);

const _: () = {
    // The field holds the no-position marker and at least two resume blocks,
    // and an id, whose bits all lie at or below the tag, is below 2^53.
    assert!(PROCESS_EVENT_ID_FIELD_BITS > 2);
    assert!(PROCESS_EVENT_ID_TAG_BIT < 53);
    // A stream that starts in the highest resume block has blocks left. They
    // are finite: one that sent 2^34 ids without a position would wrap.
    assert!(PROCESS_EVENT_ID_MAX_RESUME_BLOCK < PROCESS_EVENT_ID_MAX_POSITION);
};

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
    /// below it. `None` for an id at or above 2^53, or in a block from
    /// [`PROCESS_EVENT_ID_MAX_RESUME_BLOCK`] on, which only a forged one is, so
    /// a stream never runs out of blocks.
    fn first_after(last_event_id: u64) -> Option<Self> {
        if last_event_id < PROCESS_EVENT_ID_TAG {
            return Some(Self::START);
        }
        if last_event_id >> PROCESS_EVENT_ID_TAG_BIT != 1 {
            return None;
        }
        let first_unknown = Self::Unknown { block: 0, frame: 0 }.encode();
        let block = if last_event_id < first_unknown {
            0
        } else {
            let field =
                (last_event_id >> PROCESS_EVENT_ID_FIELD_SHIFT) & PROCESS_EVENT_ID_FIELD_MASK;
            (field & !PROCESS_EVENT_ID_UNKNOWN) + 1
        };
        (block <= PROCESS_EVENT_ID_MAX_RESUME_BLOCK).then_some(Self::Unknown { block, frame: 0 })
    }

    pub(crate) fn encode(self) -> u64 {
        let (field, frame) = match self {
            Self::Position { position, frame } => {
                debug_assert!(position <= PROCESS_EVENT_ID_MAX_POSITION, "{self:?}");
                (position, frame)
            }
            Self::Unknown { block, frame } => {
                debug_assert!(block <= PROCESS_EVENT_ID_MAX_POSITION, "{self:?}");
                (PROCESS_EVENT_ID_UNKNOWN | block, frame)
            }
        };
        debug_assert!(frame <= PROCESS_EVENT_ID_MAX_FRAME, "{self:?}");
        PROCESS_EVENT_ID_TAG
            | field << PROCESS_EVENT_ID_FIELD_SHIFT
            | frame << PROCESS_EVENT_ID_GUARD_BITS
    }

    /// `None` for an id this encoding did not produce.
    fn decode(id: u64) -> Option<Self> {
        if id >> PROCESS_EVENT_ID_TAG_BIT != 1 || id & ((1 << PROCESS_EVENT_ID_GUARD_BITS) - 1) != 0
        {
            return None;
        }
        let frame = (id >> PROCESS_EVENT_ID_GUARD_BITS) & PROCESS_EVENT_ID_MAX_FRAME;
        let field = (id >> PROCESS_EVENT_ID_FIELD_SHIFT) & PROCESS_EVENT_ID_FIELD_MASK;
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
    /// `position`. Greater than this one while the blocks without a position
    /// last: a stream has at least 2^34 ids after the block it starts in.
    fn next(self, position: Option<u64>) -> Self {
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
            // A resumed stream starts at most at block
            // `PROCESS_EVENT_ID_MAX_RESUME_BLOCK` (`first_after`), and would
            // need 2^34 frames to exhaust the rest; past them this wraps.
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
    fn load(last_event_id: Option<u64>) -> Option<Self> {
        let Some(last_event_id) = last_event_id else {
            return Some(Self {
                event_id: ProcessEventId::START,
                backend_output_cursor: 0,
                resync: None,
            });
        };
        if let Some(event_id @ ProcessEventId::Position { position, .. }) =
            ProcessEventId::decode(last_event_id)
        {
            return Some(Self {
                event_id,
                backend_output_cursor: position,
                resync: None,
            });
        }
        Some(Self {
            event_id: ProcessEventId::first_after(last_event_id)?.next(Some(0)),
            backend_output_cursor: 0,
            resync: Some(DataPlaneResyncReason::CursorReplayUnavailable),
        })
    }
}

impl ProcessSseState {
    async fn new(
        state: DataPlaneState,
        session_id: DataPlaneSessionId,
        mut attached: AttachedSession,
        replay: ProcessReplayState,
        input: ProcessInput,
        stream_permit: AttachedStreamPermit,
        start_permit: Option<NativeSessionAttachPermit>,
    ) -> Result<Self, StreamBridgeError> {
        let (client, stream) = open_native_process_stream(
            &state,
            session_id,
            &mut attached,
            Some(replay.backend_output_cursor),
            input,
        )
        .await?;
        let mut pending = VecDeque::new();
        pending.push_back(sse_event_from_frame(&attached_frame(&attached), None));
        if let Some(reason) = replay.resync {
            pending.push_back(sse_event_from_frame(
                &resync_required_frame(session_id.kind().as_str(), reason),
                Some(replay.event_id.encode()),
            ));
        }
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
            started_sent: false,
            gap_reported: false,
            at_oldest_retained: false,
            window_reopens_without_progress: 0,
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
    fn restart_from_output_start(&mut self, cause: &'static str) {
        self.continue_after_gap(Some(0), cause);
    }

    /// Continues the client's output at `position` of the guest's, or at the
    /// live tail when `None`, behind `resync_required(cursor_out_of_window)`:
    /// the output between what the client has and `position` is not sent.
    fn continue_after_gap(&mut self, position: Option<u64>, cause: &'static str) {
        tracing::info!(
            cause,
            from = ?self.backend_output_cursor,
            to = ?position,
            "process event stream continues after an output gap"
        );
        self.backend_output_cursor = position;
        if let Some(position) = position {
            self.output_seq = position;
        }
        if !self.gap_reported {
            self.gap_reported = true;
            self.push_frame(resync_required_frame(
                self.session_id.kind().as_str(),
                DataPlaneResyncReason::CursorOutOfWindow,
            ));
        }
    }

    fn push_frame(&mut self, frame: DataPlaneServerFrame) {
        match frame {
            DataPlaneServerFrame::ProcessStarted { .. } if self.started_sent => return,
            DataPlaneServerFrame::ProcessStarted { .. } => self.started_sent = true,
            _ => {}
        }
        let id = process_frame_has_event_id(&frame).then(|| {
            self.event_id = self.event_id.next(self.backend_output_cursor);
            self.event_id.encode()
        });
        self.pending.push_back(sse_event_from_frame(&frame, id));
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
            message: match error {
                StreamBridgeError::ProcessNotRetained => PROCESS_NOT_RETAINED_MESSAGE,
                _ => PROCESS_STREAM_FAILED,
            }
            .to_string(),
            // A reattach cannot bring back a process the guest let go of.
            retryable: !matches!(error, StreamBridgeError::ProcessNotRetained),
        });
    }

    /// Continues at `start`, where the guest reports a backend stream starts:
    /// past the client's position when the output there was evicted. The guest
    /// chose it under the lock that makes the stream its primary reader, so
    /// unless another reader holds that place, the output there stays until
    /// the stream reads it.
    fn continue_at_reported_start(&mut self, start: u64) {
        if self
            .backend_output_cursor
            .is_some_and(|cursor| start > cursor)
        {
            self.continue_after_gap(Some(start), "output_evicted");
            self.at_oldest_retained = true;
        }
    }

    /// Reopens the backend stream after the guest refused its cursor as
    /// outside the retained output, behind one
    /// `resync_required(cursor_out_of_window)` for the gap: from the start of
    /// the output when the cursor was past its end, and from the oldest output
    /// the guest still retains when it reports it. The stream follows the live
    /// tail instead when the guest does not report it, or when the stream
    /// already moved there and fell behind again before any output. After
    /// [`PROCESS_OUTPUT_WINDOW_REOPENS_WITHOUT_PROGRESS`] reopens, a cursor
    /// past the output, or a live tail that falls behind, ends the stream.
    async fn reopen_after_window_miss(&mut self, status: tonic::Status, miss: OutputWindowMiss) {
        let Some(process_id) = self.process_id.clone() else {
            self.finish_with_error(StreamBridgeError::BackendStatus(status));
            return;
        };
        let out_of_reopens =
            self.window_reopens_without_progress >= PROCESS_OUTPUT_WINDOW_REOPENS_WITHOUT_PROGRESS;
        match (miss, self.backend_output_cursor) {
            // Only a position the client resumed from can be past the output.
            (OutputWindowMiss::CursorAhead, Some(cursor)) if cursor > 0 && !out_of_reopens => {
                self.restart_from_output_start("cursor_ahead");
            }
            (
                OutputWindowMiss::Evicted {
                    first_retained: Some(first),
                },
                Some(cursor),
            ) if first > cursor && !self.at_oldest_retained => {
                self.continue_after_gap(Some(first), "output_evicted");
                self.at_oldest_retained = true;
            }
            (OutputWindowMiss::Evicted { .. }, Some(_)) => {
                self.continue_after_gap(None, "output_evicted");
            }
            (OutputWindowMiss::Evicted { .. }, None) if !out_of_reopens => {
                self.continue_after_gap(None, "output_evicted");
            }
            _ => {
                self.finish_with_error(StreamBridgeError::BackendStatus(status));
                return;
            }
        }
        self.window_reopens_without_progress += 1;
        match connect_process_stream(
            &mut self.client,
            &self.attached.sandbox_id,
            &process_id,
            self.backend_output_cursor,
            self.attached.space_api_route_token.as_ref(),
        )
        .await
        {
            Ok(stream) => self.stream = stream,
            // The route token may have expired since the stream opened.
            Err(StreamBridgeError::BackendStatus(status)) if is_backend_reconnectable(&status) => {
                self.reconnect_backend(status).await;
            }
            Err(error) => self.finish_with_error(process_not_retained(error)),
        }
    }

    /// Re-resolves the route after the backend stream failed with `status`
    /// and reopens it at the output position the client has reached.
    async fn reconnect_backend(&mut self, status: tonic::Status) {
        let Some(current_process_id) = self.process_id.clone() else {
            self.finish_with_error(StreamBridgeError::BackendStatus(status));
            return;
        };
        let current_generation = self.attached.route_generation;
        let (refreshed, refresh_dimension) =
            match refresh_native_stream_route(&self.state, &self.attached, &status).await {
                Ok(refreshed) => refreshed,
                Err(error) => {
                    self.finish_with_error(error);
                    return;
                }
            };
        let reason =
            reconnect_reason_for_route_change(current_generation, refreshed.route_generation);
        record_backend_channel_reset(self.session_id.kind().as_str(), &reason);
        self.push_frame(DataPlaneServerFrame::BackendReconnecting {
            reason: reason.clone(),
            route_generation: current_generation,
        });
        if let Err(error) =
            record_session_reconnect(&self.state, self.session_id, reason, current_generation).await
        {
            self.finish_with_error(error);
            return;
        }
        let reopened = async {
            let channel = self
                .state
                .backend_channels
                .channel(&refreshed.backend_endpoint, self.session_id)
                .await?;
            let mut client = WorkerApiClient::new(wrap_grpc_client_channel(channel));
            let stream = connect_process_stream(
                &mut client,
                &refreshed.sandbox_id,
                &current_process_id,
                self.backend_output_cursor,
                refreshed.space_api_route_token.as_ref(),
            )
            .await
            .map_err(process_not_retained)?;
            Ok((client, stream))
        }
        .await;
        match finish_native_stream_refresh(refresh_dimension, reopened) {
            Ok((client, stream)) => {
                self.client = client;
                self.stream = stream;
                self.push_frame(DataPlaneServerFrame::BackendReconnected {
                    route_generation: refreshed.route_generation,
                    current_node_id: refreshed.current_node_id.to_string(),
                });
                // From a known position the reopened stream continues where
                // this one stopped; only one that follows the live tail can
                // skip output.
                if self.backend_output_cursor.is_none() {
                    self.push_frame(resync_required_frame(
                        self.session_id.kind().as_str(),
                        DataPlaneResyncReason::ProcessStreamReconnected,
                    ));
                }
                self.attached = refreshed;
            }
            Err(error) => self.finish_with_error(error),
        }
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
                                if matches!(
                                    event.event,
                                    Some(spaced_process_event::Event::Started(_))
                                ) && state.backend_output_cursor != Some(0)
                                {
                                    // This attach started the process, so its
                                    // output begins here: the position the
                                    // client resumed from was another's.
                                    state.restart_from_output_start("foreign_last_event_id");
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
                        let reported_start = match &event.event {
                            Some(spaced_process_event::Event::Connected(connected)) => {
                                connected.output_cursor
                            }
                            _ => None,
                        };
                        let mut output_seq = state.output_seq;
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
                            if process_frame_advances_backend_cursor(&frame) {
                                state.output_seq = output_seq;
                                if let Some(cursor) = state.backend_output_cursor.as_mut() {
                                    *cursor = output_seq;
                                }
                            }
                            if process_frame_advances_backend_cursor(&frame) || exited {
                                state.gap_reported = false;
                                state.at_oldest_retained = false;
                                state.window_reopens_without_progress = 0;
                            }
                            state.push_frame(frame);
                            if exited {
                                state.push_frame(DataPlaneServerFrame::Closed {
                                    reason: StreamCloseReason::BackendClosed,
                                });
                                state.finish("backend_closed");
                            }
                        }
                        if let Some(start) = reported_start {
                            state.continue_at_reported_start(start);
                        }
                        if let Some(event) = state.pending.pop_front() {
                            return Some(event);
                        }
                    }
                    Err(status) => {
                        if let Some(miss) = output_window_miss(&status) {
                            state.reopen_after_window_miss(status, miss).await;
                        } else if is_backend_reconnectable(&status) {
                            state.reconnect_backend(status).await;
                        } else {
                            state.finish_with_error(StreamBridgeError::BackendStatus(status));
                        }
                        // A reopen after a gap already reported sends nothing.
                        if let Some(event) = state.pending.pop_front() {
                            return Some(event);
                        }
                        if state.finished {
                            return None;
                        }
                    }
                }
            }
        }
    }
}

/// Whether a process stream frame carries an SSE id: only one that moves the
/// client's position does. That is output, and `resync_required`, which
/// acknowledges a gap. After evicted output the stream keeps its position, at
/// the oldest output retained, so a resume from that id continues past the
/// gap without reporting it again. After a restart from the start of the
/// output (a cursor past it, a foreign id) or a move to the live tail, the
/// ids carry no position, and a resume from them replays behind
/// `resync_required`. Every other frame (`process_started`, `process_exited`,
/// a backend reconnect's, `error`, `closed`) leaves the client's last id
/// unchanged, so resumes and reconnects against an idle process spend no
/// frames of the [`ProcessEventId`] at its position.
fn process_frame_has_event_id(frame: &DataPlaneServerFrame) -> bool {
    process_frame_advances_backend_cursor(frame)
        || matches!(frame, DataPlaneServerFrame::ResyncRequired { .. })
}

/// Backend reopens after an output window miss, without output or an exit
/// in between, after which a cursor past the output, or a live tail that
/// falls behind, ends the stream with an error; evicted output still
/// continues at the live tail. A cursor past the output restarts at the
/// start, which a guest may in turn report as evicted, so two in a row are
/// expected.
const PROCESS_OUTPUT_WINDOW_REOPENS_WITHOUT_PROGRESS: u32 = 3;

/// Why the guest ended a process stream because its cursor is outside the
/// output it retains.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputWindowMiss {
    /// The cursor is past the output the process has written.
    CursorAhead,
    /// The output at the cursor was evicted. `first_retained` is the cursor
    /// of the oldest output the guest still retains, when it reports one.
    Evicted { first_retained: Option<u64> },
}

/// The window miss a backend stream status reports: an OUT_OF_RANGE or a
/// RESOURCE_EXHAUSTED the guest tagged as one, or, from a guest older than
/// those tags, a RESOURCE_EXHAUSTED with its lag message. Other statuses with
/// these codes (a hop's refusal of an oversized chunk, an HTTP/2 reset) are
/// not misses.
fn output_window_miss(status: &tonic::Status) -> Option<OutputWindowMiss> {
    use pplx_space_common_core::spaced;
    let metadata = status.metadata();
    let reason = metadata
        .get(spaced::ERROR_REASON_METADATA_KEY)
        .and_then(|value| value.to_str().ok());
    match (status.code(), reason) {
        (tonic::Code::OutOfRange, Some(spaced::PROCESS_OUTPUT_CURSOR_AHEAD)) => {
            Some(OutputWindowMiss::CursorAhead)
        }
        (tonic::Code::ResourceExhausted, Some(spaced::PROCESS_OUTPUT_LAGGED)) => {
            Some(OutputWindowMiss::Evicted {
                first_retained: metadata
                    .get(spaced::PROCESS_OUTPUT_FIRST_SEQUENCE_METADATA_KEY)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse().ok()),
            })
        }
        (tonic::Code::ResourceExhausted, None)
            if status.message() == spaced::PROCESS_OUTPUT_LAGGED_MESSAGE =>
        {
            Some(OutputWindowMiss::Evicted {
                first_retained: None,
            })
        }
        _ => None,
    }
}

/// Whether a backend stream that failed with `status` may be reopened after
/// re-resolving its route: the backend is unavailable, or it rejected the
/// route's identity as one a refresh replaces.
fn is_backend_reconnectable(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::Unavailable || is_refreshable_route_identity_rejection(status)
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

#[cfg(test)]
mod tests;
