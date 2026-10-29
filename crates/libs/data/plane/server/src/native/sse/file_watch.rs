//! File watch SSE state and event delivery.

use super::*;

pub(crate) struct FileWatchSseState {
    pub(super) state: DataPlaneState,
    pub(super) session_id: DataPlaneSessionId,
    pub(super) attached: AttachedSession,
    pub(super) watch: WatchOperation,
    pub(super) client: WorkerApiClient<GrpcClientTrace<Channel>>,
    pub(super) stream: tonic::Streaming<FileEvent>,
    pub(super) event_seq: u64,
    pub(super) revocation_tick: tokio::time::Interval,
    pub(super) pending: VecDeque<Event>,
    pub(super) finished: bool,
    pub(super) shutdown_token: CancellationToken,
    pub(super) _stream_permit: AttachedStreamPermit,
}

impl FileWatchSseState {
    pub(super) async fn new(
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

    pub(super) fn push_frame(&mut self, frame: DataPlaneServerFrame) {
        self.event_seq = self.event_seq.saturating_add(1);
        self.pending
            .push_back(sse_event_from_frame(&frame, Some(self.event_seq)));
    }

    pub(super) fn finish(&mut self, result: &'static str) {
        pplx_space_metrics::data_plane::stream_closed(self.session_id.kind().as_str(), result);
        self.finished = true;
    }

    pub(super) fn finish_with_frame(&mut self, frame: DataPlaneServerFrame, result: &'static str) {
        self.finish(result);
        self.push_frame(frame);
    }

    pub(super) fn finish_with_error(&mut self, error: StreamBridgeError) {
        let result = stream_bridge_error_label(&error);
        pplx_space_metrics::data_plane::stream_closed(self.session_id.kind().as_str(), result);
        tracing::warn!(target: LOG_TARGET, %error, "data-plane file watch event stream failed");
        self.finished = true;
        self.push_frame(DataPlaneServerFrame::Error {
            code: DataPlaneErrorCode::BackendStreamError,
            message: FILE_WATCH_STREAM_FAILED.to_string(),
            retryable: true,
            details: Vec::new(),
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
pub(super) async fn next_file_watch_sse_frame(state: &mut FileWatchSseState) -> Option<Event> {
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
