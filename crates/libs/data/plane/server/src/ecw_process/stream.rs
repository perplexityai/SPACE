//! ECW process connect streams: opening, resuming and framing `Process.Start`
//! and `Process.Connect`, and ending them at the drain deadline.

use super::*;

pub(super) fn process_connect_json_stream_response<S>(
    stream: S,
    route: &EcwResolvedRoute,
    stdout_stream: ecw_connect::EcwProcessStdoutStream,
    stall_after: Option<Duration>,
    lease: EcwProcessStreamLease,
) -> Response
where
    S: Stream<Item = Result<SpacedProcessEvent, tonic::Status>> + Send + 'static,
{
    ecw_connect::connect_json_stream_response(process_connect_json_stream(
        stream,
        "Process.Connect",
        route,
        true,
        stdout_stream,
        stall_after,
        lease,
    ))
}

pub(super) fn process_start_json_stream_response<S>(
    stream: S,
    route: &EcwResolvedRoute,
    stdout_stream: ecw_connect::EcwProcessStdoutStream,
    stall_after: Option<Duration>,
    lease: EcwProcessStreamLease,
) -> Response
where
    S: Stream<Item = Result<SpacedProcessEvent, tonic::Status>> + Send + 'static,
{
    ecw_connect::connect_json_stream_response(process_connect_json_stream(
        stream,
        "Process.Start",
        route,
        false,
        stdout_stream,
        stall_after,
        lease,
    ))
}

pub(super) type BoxedSpacedProcessStream =
    Pin<Box<dyn Stream<Item = Result<SpacedProcessEvent, tonic::Status>> + Send>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ProcessStartResume {
    Start,
    Connect {
        process_id: ProcessId,
        output_cursor: u64,
    },
}

impl ProcessStartResume {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Start => "retry_start",
            Self::Connect { .. } => "reattach",
        }
    }

    fn is_connect(&self) -> bool {
        matches!(self, Self::Connect { .. })
    }

    pub(super) fn recovery_mode(&self) -> ProcessStartRecoveryMode {
        match self {
            Self::Start => ProcessStartRecoveryMode::RetryStart,
            Self::Connect { .. } => ProcessStartRecoveryMode::Reattach,
        }
    }
}

impl Display for ProcessStartResume {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub(super) struct ResumableProcessStartState<R> {
    stream: BoxedSpacedProcessStream,
    reopen: R,
    process_id: Option<ProcessId>,
    output_cursor: u64,
    terminal: bool,
    ended: bool,
    reopens_without_progress: usize,
    reopened_start: bool,
    replaying: bool,
}

pub(super) const PROCESS_START_REOPEN_BACKOFF: [Duration; 2] =
    [Duration::from_millis(100), Duration::from_millis(200)];

pub(super) fn process_start_reopen_delay(reopens_without_progress: usize) -> Option<Duration> {
    PROCESS_START_REOPEN_BACKOFF
        .get(reopens_without_progress)
        .copied()
}

impl<R> ResumableProcessStartState<R> {
    fn resume(&self) -> ProcessStartResume {
        match self.process_id.clone() {
            Some(process_id) => ProcessStartResume::Connect {
                process_id,
                output_cursor: self.output_cursor,
            },
            None => ProcessStartResume::Start,
        }
    }

    fn observe(&mut self, event: &SpacedProcessEvent) -> Result<(), tonic::Status> {
        match &event.event {
            Some(
                spaced_ops::process_event::Event::Started(_)
                | spaced_ops::process_event::Event::Connected(_),
            ) => {
                let first_binding = self.process_id.is_none();
                let started = matches!(
                    &event.event,
                    Some(spaced_ops::process_event::Event::Started(_))
                );
                let process_id = parse_backend_process_id(event)?;
                if self
                    .process_id
                    .as_ref()
                    .is_some_and(|current| current != &process_id)
                {
                    return Err(tonic::Status::data_loss(
                        "backend changed process_id while reconnecting",
                    ));
                }
                self.process_id = Some(process_id);
                if first_binding && started {
                    let outcome = if self.reopened_start {
                        ProcessStartDeliveryOutcome::DeduplicatedRetry
                    } else {
                        ProcessStartDeliveryOutcome::NewStart
                    };
                    pplx_space_metrics::data_plane::process_start_delivery(outcome);
                } else if self.replaying
                    && matches!(
                        &event.event,
                        Some(spaced_ops::process_event::Event::Connected(_))
                    )
                {
                    pplx_space_metrics::data_plane::process_start_delivery(
                        ProcessStartDeliveryOutcome::Reattached,
                    );
                }
            }
            Some(
                spaced_ops::process_event::Event::Stdout(_)
                | spaced_ops::process_event::Event::Stderr(_),
            ) => {
                self.ensure_bound_process(event)?;
                self.output_cursor = self.output_cursor.saturating_add(1);
                self.reopens_without_progress = 0;
            }
            Some(spaced_ops::process_event::Event::Exit(_)) => {
                self.ensure_bound_process(event)?;
                self.terminal = true;
                self.reopens_without_progress = 0;
            }
            None => {}
        }
        Ok(())
    }

    fn ensure_bound_process(&self, event: &SpacedProcessEvent) -> Result<(), tonic::Status> {
        let Some(current) = self.process_id.as_ref() else {
            return Err(tonic::Status::data_loss(
                "backend returned a process event before process start",
            ));
        };
        let process_id = parse_backend_process_id(event)?;
        if current != &process_id {
            return Err(tonic::Status::data_loss(
                "backend changed process_id while streaming process events",
            ));
        }
        Ok(())
    }
}

pub(super) fn parse_backend_process_id(
    event: &SpacedProcessEvent,
) -> Result<ProcessId, tonic::Status> {
    ProcessId::parse(event.process_id.clone()).map_err(|error| {
        tonic::Status::data_loss(format!("backend returned invalid process_id: {error}"))
    })
}

pub(super) fn resumable_process_start_stream<S, R, F>(
    stream: S,
    retried_start: bool,
    reopen: R,
) -> impl Stream<Item = Result<SpacedProcessEvent, tonic::Status>> + Send
where
    S: Stream<Item = Result<SpacedProcessEvent, tonic::Status>> + Send + 'static,
    R: FnMut(ProcessStartResume) -> F + Send + 'static,
    F: Future<Output = Result<BoxedSpacedProcessStream, tonic::Status>> + Send + 'static,
{
    futures_util::stream::unfold(
        ResumableProcessStartState {
            stream: Box::pin(stream),
            reopen,
            process_id: None,
            output_cursor: 0,
            terminal: false,
            ended: false,
            reopens_without_progress: 0,
            reopened_start: retried_start,
            replaying: false,
        },
        |mut state| async move {
            if state.ended {
                return None;
            }

            loop {
                if state.terminal {
                    return None;
                }

                // Older guests replay Start from the output tail. Once a retried
                // Start reveals its identity, reconnect at the delivered cursor
                // so recovery also works across guest versions.
                if state.reopened_start && state.process_id.is_some() {
                    let resume = state.resume();
                    let replaying = resume.is_connect();
                    match (state.reopen)(resume).await {
                        Ok(stream) => {
                            state.stream = stream;
                            state.reopened_start = false;
                            state.replaying = replaying;
                        }
                        Err(status) => {
                            state.ended = true;
                            return Some((Err(status), state));
                        }
                    }
                }

                let reopen_status = match state.stream.next().await {
                    Some(Ok(event)) => {
                        if let Err(status) = state.observe(&event) {
                            state.ended = true;
                            return Some((Err(status), state));
                        }
                        return Some((Ok(event), state));
                    }
                    Some(Err(status)) if is_recoverable_process_start_interruption(&status) => {
                        status
                    }
                    Some(Err(status)) => {
                        if state.replaying
                            && let Some(outcome) = process_start_delivery_failure(&status)
                        {
                            pplx_space_metrics::data_plane::process_start_delivery(outcome);
                        }
                        state.ended = true;
                        return Some((Err(status), state));
                    }
                    None if state.terminal => return None,
                    None => {
                        tonic::Status::unavailable("process stream closed before a terminal event")
                    }
                };

                let Some(reopen_delay) = process_start_reopen_delay(state.reopens_without_progress)
                else {
                    state.ended = true;
                    return Some((
                        Err(tonic::Status::unavailable(
                            "process stream repeatedly closed without output or a terminal event",
                        )),
                        state,
                    ));
                };
                state.reopens_without_progress = state.reopens_without_progress.saturating_add(1);
                let resume = state.resume();
                record_process_start_recovery(&reopen_status, resume.recovery_mode());
                tokio::time::sleep(reopen_delay).await;
                let reopened_start = resume == ProcessStartResume::Start;
                let replaying = resume.is_connect();
                match (state.reopen)(resume).await {
                    Ok(stream) => {
                        state.stream = stream;
                        state.reopened_start = reopened_start;
                        state.replaying = replaying;
                    }
                    Err(status) => {
                        state.ended = true;
                        return Some((Err(status), state));
                    }
                }
                tracing::debug!(
                    grpc_code = ?reopen_status.code(),
                    status_class = %process_start_status_class(&reopen_status),
                    "reopened ECW process start stream after backend interruption",
                );
            }
        },
    )
}

pub(super) fn is_recoverable_process_start_interruption(status: &tonic::Status) -> bool {
    // Client cancellation drops the handler future, so a Cancelled status
    // observed here came from the backend while the client was still active.
    status.code() == tonic::Code::Cancelled || is_temporary_tcp_failure(status)
}

pub(super) fn process_start_recovery_interruption(
    status: &tonic::Status,
) -> Option<ProcessStartRecoveryInterruption> {
    match status.code() {
        tonic::Code::Cancelled => Some(ProcessStartRecoveryInterruption::Cancelled),
        tonic::Code::Internal => Some(ProcessStartRecoveryInterruption::Internal),
        tonic::Code::Unavailable => Some(ProcessStartRecoveryInterruption::Unavailable),
        tonic::Code::Unknown => Some(ProcessStartRecoveryInterruption::Unknown),
        _ => None,
    }
}

pub(super) fn record_process_start_recovery(
    status: &tonic::Status,
    mode: ProcessStartRecoveryMode,
) {
    if let Some(interruption) = process_start_recovery_interruption(status) {
        pplx_space_metrics::data_plane::process_start_recovery(interruption, mode);
    }
}

pub(super) async fn open_process_start_stream(
    state: DataPlaneState,
    route: EcwResolvedRoute,
    start_request: SpacedStartProcessRequest,
    resume: ProcessStartResume,
    open_timeout: Duration,
) -> Result<(BoxedSpacedProcessStream, bool), tonic::Status> {
    let retrying_start = resume == ProcessStartResume::Start;
    let result = retry_process_start_open(
        start_request,
        resume.recovery_mode(),
        open_timeout,
        |start_request, attempt_count| {
            let evict_cached_channel = attempt_count > 1;
            let state = state.clone();
            let route = route.clone();
            let resume = resume.clone();
            async move {
                if evict_cached_channel {
                    state
                        .backend_channels
                        .forget_endpoint(&route.backend_endpoint);
                }
                let channel = ecw_backend_channel(&state, &route)
                    .await
                    .map_err(EcwProxyError::into_tonic_status)?;
                let mut client = WorkerApiClient::new(wrap_grpc_client_channel(channel));
                let response = match resume {
                    ProcessStartResume::Start => {
                        client
                            .spaced_start_process(node_api_request_with_route_token_state(
                                start_request,
                                route.space_api_route_token.as_ref(),
                            )?)
                            .await
                    }
                    ProcessStartResume::Connect {
                        process_id,
                        output_cursor,
                    } => {
                        client
                            .spaced_connect_process(node_api_request_with_route_token_state(
                                build_process_start_resume_request(
                                    &route,
                                    &start_request,
                                    &process_id,
                                    output_cursor,
                                ),
                                route.space_api_route_token.as_ref(),
                            )?)
                            .await
                    }
                }?;
                Ok(Box::pin(response.into_inner()) as BoxedSpacedProcessStream)
            }
        },
    )
    .await;
    let (stream, attempt_count) = match result {
        Ok(value) => value,
        Err(status) => {
            log_process_stream_open_status("Process.Start", &route, &status);
            return Err(status);
        }
    };
    Ok((stream, retrying_start && attempt_count > 1))
}

pub(super) async fn open_process_start_stream_with_refresh(
    state: &DataPlaneState,
    route: EcwResolvedRoute,
    start_request: SpacedStartProcessRequest,
    resume: ProcessStartResume,
    open_timeout: Duration,
) -> Result<((BoxedSpacedProcessStream, bool), EcwResolvedRoute), EcwProxyError> {
    bounded_ecw_route_call_with_timeout_observer(
        state,
        route,
        "process_start_open",
        open_timeout,
        Some(log_process_start_timeout),
        move |route, remaining| {
            let state = state.clone();
            let start_request = start_request.clone();
            let resume = resume.clone();
            async move {
                open_process_start_stream(state, route, start_request, resume, remaining)
                    .await
                    .map_err(EcwRouteAttemptError::Status)
            }
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await
}

pub(super) fn build_process_start_resume_request(
    route: &EcwResolvedRoute,
    start_request: &SpacedStartProcessRequest,
    process_id: &ProcessId,
    output_cursor: u64,
) -> SpacedConnectProcessRequest {
    SpacedConnectProcessRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::ConnectProcessRequest {
            process_id: process_id.to_string(),
            timeout: start_request
                .operation
                .as_ref()
                .and_then(|operation| operation.timeout),
            output_cursor: Some(output_cursor),
            resume_from_oldest_retained: false,
        }),
    }
}

pub(super) async fn retry_process_start_open<T, F, Fut>(
    start_request: SpacedStartProcessRequest,
    recovery_mode: ProcessStartRecoveryMode,
    open_timeout: Duration,
    mut operation: F,
) -> Result<(T, u8), tonic::Status>
where
    F: FnMut(SpacedStartProcessRequest, u8) -> Fut,
    Fut: Future<Output = Result<T, tonic::Status>>,
{
    let mut attempt_count = 0_u8;
    let result = retry_grpc_status_with_context_if(
        "space-node-api",
        "ecw_process_start_open",
        DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
        GrpcRetryContext::with_timeout(open_timeout),
        is_recoverable_process_start_interruption,
        || {
            attempt_count = attempt_count.saturating_add(1);
            let start_request = start_request.clone();
            let attempt = operation(start_request, attempt_count);
            async move {
                let result = attempt.await;
                if let Err(status) = &result
                    && is_recoverable_process_start_interruption(status)
                {
                    record_process_start_recovery(status, recovery_mode);
                }
                result
            }
        },
    )
    .await?;
    Ok((result, attempt_count))
}

pub(super) fn is_process_replay_miss(status: &tonic::Status) -> bool {
    match status.code() {
        tonic::Code::NotFound | tonic::Code::FailedPrecondition => true,
        tonic::Code::ResourceExhausted => UdsPerSandboxLimitExceeded::from_status(status).is_none(),
        _ => false,
    }
}

pub(super) fn process_start_delivery_failure(
    status: &tonic::Status,
) -> Option<ProcessStartDeliveryOutcome> {
    if UdsPerSandboxLimitExceeded::from_status(status).is_some() {
        Some(ProcessStartDeliveryOutcome::Backpressure)
    } else if is_process_replay_miss(status) {
        Some(ProcessStartDeliveryOutcome::ReplayMiss)
    } else {
        None
    }
}

pub(super) fn process_start_reopen_status(
    error: EcwProxyError,
    resume: &ProcessStartResume,
) -> tonic::Status {
    let status = error.into_tonic_status();
    if resume.is_connect()
        && let Some(outcome) = process_start_delivery_failure(&status)
    {
        pplx_space_metrics::data_plane::process_start_delivery(outcome);
    }
    status
}

pub(super) fn reset_process_start_stream_channel(state: &DataPlaneState, route: &EcwResolvedRoute) {
    state
        .backend_channels
        .forget_endpoint(&route.backend_endpoint);
    pplx_space_metrics::data_plane::backend_channel_reset("ecw", "worker_unavailable");
    pplx_space_metrics::data_plane::backend_affected_streams(ECW_PROCESS_STREAM_KIND, 1);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProcessStartStatusClass {
    UdsBackpressure,
    ReplayMiss,
    RecoverableInterruption,
    BackendStatus,
}

impl ProcessStartStatusClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::UdsBackpressure => "uds_backpressure",
            Self::ReplayMiss => "replay_miss",
            Self::RecoverableInterruption => "recoverable_interruption",
            Self::BackendStatus => "backend_status",
        }
    }
}

impl Display for ProcessStartStatusClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub(super) fn process_start_status_class(status: &tonic::Status) -> ProcessStartStatusClass {
    if UdsPerSandboxLimitExceeded::from_status(status).is_some() {
        ProcessStartStatusClass::UdsBackpressure
    } else if is_process_replay_miss(status) {
        ProcessStartStatusClass::ReplayMiss
    } else if is_recoverable_process_start_interruption(status) {
        ProcessStartStatusClass::RecoverableInterruption
    } else {
        ProcessStartStatusClass::BackendStatus
    }
}

pub(super) const PROCESS_STREAM_STALL_GRACE: Duration = Duration::from_secs(15);
pub(super) const PROCESS_STREAM_STALL_MESSAGE: &str =
    "sandbox runtime became unreachable; process stream stalled";
pub(super) const PROCESS_STREAM_STALL_EVENT: &str = "ecw_process_stream_stalled";
pub(super) const PROCESS_STREAM_OPEN_TIMEOUT_EVENT: &str = "ecw_process_stream_open_timed_out";
pub(super) const PROCESS_STREAM_OPEN_BACKPRESSURE_EVENT: &str =
    "ecw_process_stream_open_backpressure";
pub(super) const PROCESS_STREAM_OPEN_BACKEND_STATUS_EVENT: &str =
    "ecw_process_stream_open_backend_status";

pub(super) fn log_process_stream_open_status(
    operation: &'static str,
    route: &EcwResolvedRoute,
    status: &tonic::Status,
) {
    if UdsPerSandboxLimitExceeded::from_status(status).is_some() {
        tracing::info!(
            event_type = PROCESS_STREAM_OPEN_BACKPRESSURE_EVENT,
            operation,
            sandbox_id = %route.sandbox_id,
            route_generation = route.route_generation,
            current_node_id = %route.current_node_id,
            backend_endpoint = %route.backend_endpoint,
            grpc_code = ?status.code(),
            status_class = %process_start_status_class(status),
            "ECW process stream open hit sandbox backpressure"
        );
        return;
    }

    tracing::warn!(
        event_type = PROCESS_STREAM_OPEN_BACKEND_STATUS_EVENT,
        operation,
        sandbox_id = %route.sandbox_id,
        route_generation = route.route_generation,
        current_node_id = %route.current_node_id,
        backend_endpoint = %route.backend_endpoint,
        grpc_code = ?status.code(),
        status_class = %process_start_status_class(status),
        "ECW process stream open returned backend status"
    );
}

pub(crate) fn log_process_stream_open_timeout(
    operation: &'static str,
    route: &EcwResolvedRoute,
    open_timeout: Duration,
) {
    tracing::warn!(
        event_type = PROCESS_STREAM_OPEN_TIMEOUT_EVENT,
        operation,
        sandbox_id = %route.sandbox_id,
        route_generation = route.route_generation,
        current_node_id = %route.current_node_id,
        backend_endpoint = %route.backend_endpoint,
        open_timeout_ms = open_timeout.as_millis(),
        "ECW process stream open timed out before receiving a backend stream",
    );
}

pub(super) fn log_process_connect_timeout(route: &EcwResolvedRoute, timeout: Duration) {
    log_process_stream_open_timeout("Process.Connect", route, timeout);
}

pub(super) fn log_process_start_timeout(route: &EcwResolvedRoute, timeout: Duration) {
    log_process_stream_open_timeout("Process.Start", route, timeout);
}

pub(super) fn process_stream_open_timeout(timeout: Option<&prost_types::Duration>) -> Duration {
    let cap = NodeApiOperationBudget::StreamOpen.timeout();
    timeout
        .and_then(process_timeout_duration)
        .map_or(cap, |timeout| timeout.min(cap))
}

pub(super) fn process_stream_stall_after(
    timeout: Option<&prost_types::Duration>,
) -> Option<Duration> {
    let timeout = timeout.and_then(process_timeout_duration)?;
    Some(timeout + PROCESS_STREAM_STALL_GRACE)
}

pub(super) fn process_timeout_duration(timeout: &prost_types::Duration) -> Option<Duration> {
    let seconds = u64::try_from(timeout.seconds).ok()?;
    let nanos = u32::try_from(timeout.nanos.max(0)).ok()?;
    let timeout = Duration::new(seconds, nanos);
    if timeout.is_zero() {
        return None;
    }
    Some(timeout)
}

pub(super) fn process_connect_json_stream<S>(
    stream: S,
    operation: &'static str,
    route: &EcwResolvedRoute,
    connected_as_start: bool,
    stdout_stream: ecw_connect::EcwProcessStdoutStream,
    stall_after: Option<Duration>,
    lease: EcwProcessStreamLease,
) -> ProcessConnectJsonStream<S>
where
    S: Stream<Item = Result<SpacedProcessEvent, tonic::Status>>,
{
    ProcessConnectJsonStream {
        inner: Box::pin(stream),
        operation,
        sandbox_id: route.sandbox_id,
        route_generation: route.route_generation,
        current_node_id: route.current_node_id,
        backend_endpoint: route.backend_endpoint.clone(),
        connected_as_start,
        stdout_stream,
        ended: false,
        close_recorded: false,
        stall_after,
        stall_deadline: stall_after.map(|after| Box::pin(tokio::time::sleep(after))),
        shutdown: Box::pin(lease.shutdown_token.cancelled_owned()),
        flushed_after_shutdown: None,
        delivered_any: false,
        start_wait: None,
        _stream_permit: lease.permit,
    }
}

/// What bounds an ECW process stream's lifetime: the attached-stream slot it
/// holds, and the token that ends it at the drain deadline.
pub(super) struct EcwProcessStreamLease {
    pub(super) permit: AttachedStreamPermit,
    pub(super) shutdown_token: CancellationToken,
}

pub(super) struct ProcessConnectJsonStream<S> {
    inner: Pin<Box<S>>,
    operation: &'static str,
    sandbox_id: SandboxId,
    route_generation: u64,
    current_node_id: NodeId,
    backend_endpoint: BackendEndpoint,
    connected_as_start: bool,
    stdout_stream: ecw_connect::EcwProcessStdoutStream,
    ended: bool,
    close_recorded: bool,
    stall_after: Option<Duration>,
    stall_deadline: Option<Pin<Box<tokio::time::Sleep>>>,
    shutdown: Pin<Box<WaitForCancellationFutureOwned>>,
    /// What has been delivered since the drain deadline passed, or `None`
    /// before it.
    flushed_after_shutdown: Option<ShutdownFlush>,
    /// Whether any message reached the client, so a start knows whether it
    /// has reported itself yet.
    delivered_any: bool,
    /// How long a start that has not reported itself waits after the deadline.
    start_wait: Option<Pin<Box<tokio::time::Sleep>>>,
    _stream_permit: AttachedStreamPermit,
}

/// Messages and bytes a process stream has delivered after the drain deadline.
#[derive(Default)]
pub(super) struct ShutdownFlush {
    messages: usize,
    bytes: usize,
}

impl ShutdownFlush {
    fn exhausted(&self) -> bool {
        self.messages >= SHUTDOWN_FLUSH_LIMIT || self.bytes >= SHUTDOWN_FLUSH_BYTES
    }
}

/// Messages and bytes a process stream may still deliver after the drain
/// deadline before it ends regardless, so a stream that never goes idle stops
/// producing. The cap bounds the work, not the time: a client too slow to
/// take the flush within the 5-second cancel grace is cut by the process exit
/// without the end-stream message.
pub(super) const SHUTDOWN_FLUSH_LIMIT: usize = 64;
pub(super) const SHUTDOWN_FLUSH_BYTES: usize = 1024 * 1024;
/// How long a start that hasn't reported itself yet waits for its first
/// message once the stream sees the deadline, leaving the rest of the
/// 5-second cancel grace to deliver it.
pub(super) const SHUTDOWN_START_WAIT: Duration = Duration::from_secs(2);

impl<S> ProcessConnectJsonStream<S> {
    fn end_for_shutdown(&mut self) -> Bytes {
        self.ended = true;
        self.record_close("shutdown");
        let bytes = if !self.connected_as_start && !self.delivered_any {
            ecw_connect::connect_unreported_start_end_stream()
        } else {
            ecw_connect::connect_shutdown_end_stream()
        };
        record_ecw_process_stream_bytes(&bytes);
        bytes
    }

    fn record_close(&mut self, result: &'static str) {
        if self.close_recorded {
            return;
        }
        self.close_recorded = true;
        pplx_space_metrics::data_plane::stream_closed(ECW_PROCESS_STREAM_KIND, result);
    }
}

impl<S> Drop for ProcessConnectJsonStream<S> {
    fn drop(&mut self) {
        if !self.close_recorded {
            pplx_space_metrics::data_plane::stream_closed(ECW_PROCESS_STREAM_KIND, "client_closed");
        }
    }
}

impl<S> Stream for ProcessConnectJsonStream<S>
where
    S: Stream<Item = Result<SpacedProcessEvent, tonic::Status>>,
{
    type Item = Result<Bytes, Infallible>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        // At the drain deadline, first deliver what the backend has already
        // produced: a start that completed just before the deadline still
        // reaches the client as `start`, so it does not retry a command that
        // ran. The cap keeps a stream that never goes idle from outliving the
        // deadline.
        if this.flushed_after_shutdown.is_none() && this.shutdown.as_mut().poll(cx).is_ready() {
            this.flushed_after_shutdown = Some(ShutdownFlush::default());
        }
        if this
            .flushed_after_shutdown
            .as_ref()
            .is_some_and(ShutdownFlush::exhausted)
        {
            return Poll::Ready(Some(Ok(this.end_for_shutdown())));
        }

        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(event))) => {
                    if let Some(bytes) = ecw_connect::process_event_to_connect_message(
                        event,
                        this.connected_as_start,
                        this.stdout_stream,
                    ) {
                        if let Some(flushed) = this.flushed_after_shutdown.as_mut() {
                            flushed.messages += 1;
                            flushed.bytes += bytes.len();
                        }
                        this.delivered_any = true;
                        record_ecw_process_stream_bytes(&bytes);
                        return Poll::Ready(Some(Ok(bytes)));
                    }
                }
                Poll::Ready(Some(Err(status))) => {
                    this.ended = true;
                    this.record_close("backend_status");
                    let bytes =
                        ecw_connect::connect_error_end_stream(status.code(), status.message());
                    record_ecw_process_stream_bytes(&bytes);
                    return Poll::Ready(Some(Ok(bytes)));
                }
                Poll::Ready(None) => {
                    this.ended = true;
                    this.record_close("ok");
                    let bytes = ecw_connect::connect_json_message(serde_json::json!({}), true);
                    record_ecw_process_stream_bytes(&bytes);
                    return Poll::Ready(Some(Ok(bytes)));
                }
                Poll::Pending if this.flushed_after_shutdown.is_some() => {
                    // A start whose guest command may already be running waits
                    // briefly for it to report itself, within the cancel grace.
                    if !this.connected_as_start && !this.delivered_any {
                        let wait = this.start_wait.get_or_insert_with(|| {
                            Box::pin(tokio::time::sleep(SHUTDOWN_START_WAIT))
                        });
                        if wait.as_mut().poll(cx).is_pending() {
                            return Poll::Pending;
                        }
                    }
                    return Poll::Ready(Some(Ok(this.end_for_shutdown())));
                }
                Poll::Pending => {
                    if let Some(deadline) = this.stall_deadline.as_mut()
                        && std::future::Future::poll(deadline.as_mut(), cx).is_ready()
                    {
                        this.ended = true;
                        this.record_close("stalled");
                        tracing::warn!(
                            event_type = PROCESS_STREAM_STALL_EVENT,
                            operation = this.operation,
                            sandbox_id = %this.sandbox_id,
                            route_generation = this.route_generation,
                            current_node_id = %this.current_node_id,
                            backend_endpoint = %this.backend_endpoint,
                            connected_as_start = this.connected_as_start,
                            stall_after_ms = this.stall_after.map(|after| after.as_millis() as u64),
                            stall_grace_secs = PROCESS_STREAM_STALL_GRACE.as_secs(),
                            "ECW process stream stalled past command timeout; \
                             closing with recoverable Unavailable",
                        );
                        let bytes = ecw_connect::connect_error_end_stream(
                            tonic::Code::Unavailable,
                            PROCESS_STREAM_STALL_MESSAGE,
                        );
                        record_ecw_process_stream_bytes(&bytes);
                        return Poll::Ready(Some(Ok(bytes)));
                    }
                    return Poll::Pending;
                }
            }
        }
    }
}

pub(super) fn record_ecw_process_stream_bytes(bytes: &Bytes) {
    pplx_space_metrics::data_plane::stream_bytes_out(ECW_PROCESS_STREAM_KIND, bytes.len() as u64);
}
