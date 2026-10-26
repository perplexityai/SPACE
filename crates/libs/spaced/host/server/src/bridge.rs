use super::*;
use crate::protocol::*;
use pplx_space_common_connection_admission::{
    ConnectionAdmissionConfig, ConnectionAdmissionObservation, admit_connections,
};
use pplx_space_common_core::{
    operation_budget::RuntimeOperationBudget,
    types::{GuestPath, GuestUserName, IdempotencyKey, ReadyTimeout},
};
use pplx_space_metrics::sandbox::worker as worker_metrics;
use pplx_space_spaced_common::lazy_client::FailedGeneration;
use pplx_space_spaced_common::status_is_request_deadline_exceeded;

const HOST_CONTROL_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const BRIDGE_GUEST_NETWORKING_CONTROL_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(10);
const BRIDGE_GUEST_NETWORKING_OPEN_RESPONSE_SLACK: std::time::Duration =
    std::time::Duration::from_secs(2);
const BRIDGE_GUEST_NETWORKING_OPEN_MAX_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(30);
// Host RPCs include long-lived file and process streams. These are physical
// transport caps, not request caps: capacity is charged before accept and held
// until the HTTP/2 connection closes. Keep the guest-facing partition small so
// a compromised guest cannot consume the trusted host listener's FD reserve.
const MAX_HOST_SERVER_CONNECTIONS: usize = 512;
const MAX_BRIDGE_HOST_CONNECTIONS: usize = 64;
const MIN_NON_GRPC_DESCRIPTOR_RESERVE: usize = 2048;

fn host_request_limiter() -> pplx_space_common_svc::GrpcRateLimiterLayer {
    use pplx_space_common_svc::{
        GrpcRateLimitBucket, GrpcRateLimiterConfig, GrpcRateLimiterLimits,
    };
    let mut config = GrpcRateLimiterConfig::enforce("space-spaced-host")
        .with_default_limits(GrpcRateLimiterLimits::conservative_worker_default())
        .with_bucket_limits(
            GrpcRateLimitBucket::Health,
            GrpcRateLimiterLimits {
                max_global_in_flight: Some(256),
                max_in_flight_per_peer: Some(128),
                max_requests_per_peer: Some(2000),
                per_peer_request_window: Duration::from_secs(1),
            },
        );
    // Long-lived output streams cannot consume the capacity needed to inspect,
    // signal, or suspend their guest. Payload admission still bounds stdin.
    for (service, methods) in [
        (
            "Meta",
            &[
                "Ping",
                "IsConnected",
                "PingGuest",
                "GetStatus",
                "WaitReady",
                "ConfigureProcessEnv",
                "NotifyResume",
                "FlushGuestFilesystems",
                "PrepareSuspend",
            ][..],
        ),
        (
            "Process",
            &["SendSignal", "SendInput", "CloseStdin", "Kill", "ResizePty"][..],
        ),
        (
            "Networking",
            &["OpenLoopbackBridge", "CloseLoopbackBridge"][..],
        ),
    ] {
        for method in methods {
            config = config.with_method_bucket(
                format!("/space.spaced.host.v1.{service}/{method}"),
                GrpcRateLimitBucket::Health,
            );
        }
    }
    pplx_space_common_svc::grpc_rate_limiter_layer(config)
}
const _: () = assert!(
    MAX_HOST_SERVER_CONNECTIONS + MAX_BRIDGE_HOST_CONNECTIONS + MIN_NON_GRPC_DESCRIPTOR_RESERVE
        <= pplx_space_common_core::host::linux::SANDBOX_PROCESS_NOFILE_LIMIT as usize
);

fn observe_connection_admission(
    listener: &'static str,
    observation: ConnectionAdmissionObservation,
) {
    if !observation.is_diagnostic_sample() {
        return;
    }
    let event = observation.event();
    pplx_space_metrics::grpc::server_connection_admission(
        listener,
        event.as_str(),
        event.reason().map_or("none", |reason| reason.as_str()),
    );
}

async fn with_bridge_guest_timeout<T>(
    timeout: std::time::Duration,
    timeout_message: &'static str,
    operation: impl std::future::Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    tokio::time::timeout(timeout, operation)
        .await
        .map_err(|_| Status::deadline_exceeded(timeout_message))?
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BridgeGuestOperationBudget {
    RuntimeMetadata,
    FilesystemMetadata,
    FilesystemMutation,
    FilesystemTransfer,
    FilesystemFlush(Duration),
    ProcessMetadata,
    ProcessControl,
    ProcessStreamOpen,
    NetworkingControl,
    NetworkingOpen {
        wait_timeout: Option<std::time::Duration>,
    },
}

impl BridgeGuestOperationBudget {
    pub(crate) fn timeout(self) -> std::time::Duration {
        match self {
            Self::RuntimeMetadata => RuntimeOperationBudget::RuntimeMetadata.timeout(),
            Self::FilesystemMetadata => RuntimeOperationBudget::FilesystemMetadata.timeout(),
            Self::FilesystemMutation => RuntimeOperationBudget::FilesystemMutation.timeout(),
            Self::FilesystemTransfer => RuntimeOperationBudget::FilesystemTransferOpen.timeout(),
            Self::FilesystemFlush(timeout) => timeout.min(Duration::from_secs(180)),
            Self::ProcessMetadata => RuntimeOperationBudget::ProcessMetadata.timeout(),
            Self::ProcessControl => RuntimeOperationBudget::ProcessControl.timeout(),
            Self::ProcessStreamOpen => RuntimeOperationBudget::StreamOpen.timeout(),
            Self::NetworkingControl => BRIDGE_GUEST_NETWORKING_CONTROL_TIMEOUT,
            Self::NetworkingOpen {
                wait_timeout: Some(wait_timeout),
            } => wait_timeout
                .saturating_add(BRIDGE_GUEST_NETWORKING_OPEN_RESPONSE_SLACK)
                .min(BRIDGE_GUEST_NETWORKING_OPEN_MAX_TIMEOUT),
            Self::NetworkingOpen { wait_timeout: None } => BRIDGE_GUEST_NETWORKING_CONTROL_TIMEOUT,
        }
    }
}

#[cfg(test)]
mod tests;

fn bridge_status_result(status: &Status) -> worker_metrics::SpacedBridgeRpcResult {
    match status.code() {
        tonic::Code::Ok => worker_metrics::SpacedBridgeRpcResult::Ok,
        tonic::Code::Cancelled => worker_metrics::SpacedBridgeRpcResult::Cancelled,
        tonic::Code::Unknown => worker_metrics::SpacedBridgeRpcResult::Unknown,
        tonic::Code::InvalidArgument => worker_metrics::SpacedBridgeRpcResult::InvalidArgument,
        tonic::Code::DeadlineExceeded => worker_metrics::SpacedBridgeRpcResult::DeadlineExceeded,
        tonic::Code::NotFound => worker_metrics::SpacedBridgeRpcResult::NotFound,
        tonic::Code::AlreadyExists => worker_metrics::SpacedBridgeRpcResult::AlreadyExists,
        tonic::Code::PermissionDenied => worker_metrics::SpacedBridgeRpcResult::PermissionDenied,
        tonic::Code::ResourceExhausted => worker_metrics::SpacedBridgeRpcResult::ResourceExhausted,
        tonic::Code::FailedPrecondition => {
            worker_metrics::SpacedBridgeRpcResult::FailedPrecondition
        }
        tonic::Code::Aborted => worker_metrics::SpacedBridgeRpcResult::Aborted,
        tonic::Code::OutOfRange => worker_metrics::SpacedBridgeRpcResult::OutOfRange,
        tonic::Code::Unimplemented => worker_metrics::SpacedBridgeRpcResult::Unimplemented,
        tonic::Code::Internal => worker_metrics::SpacedBridgeRpcResult::Internal,
        tonic::Code::Unavailable => worker_metrics::SpacedBridgeRpcResult::Unavailable,
        tonic::Code::DataLoss => worker_metrics::SpacedBridgeRpcResult::DataLoss,
        tonic::Code::Unauthenticated => worker_metrics::SpacedBridgeRpcResult::Unauthenticated,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BridgeRpcOutcome {
    Ok,
    Status(worker_metrics::SpacedBridgeRpcResult),
    Timeout,
}

impl BridgeRpcOutcome {
    pub(crate) fn from_result<T>(result: &Result<T, Status>) -> Self {
        match result {
            Ok(_) => Self::Ok,
            Err(status) => Self::from_status(status),
        }
    }

    pub(crate) fn from_unbounded_result<T>(result: &Result<T, Status>) -> Self {
        match result {
            Ok(_) => Self::Ok,
            Err(status) => Self::from_unbounded_status(status),
        }
    }

    pub(crate) fn from_unbounded_status(status: &Status) -> Self {
        Self::Status(bridge_status_result(status))
    }

    pub(crate) fn from_status(status: &Status) -> Self {
        if status_is_request_deadline_exceeded(status) {
            Self::Timeout
        } else {
            Self::Status(bridge_status_result(status))
        }
    }

    fn metric_result(self) -> worker_metrics::SpacedBridgeRpcResult {
        match self {
            Self::Ok => worker_metrics::SpacedBridgeRpcResult::Ok,
            Self::Timeout => worker_metrics::SpacedBridgeRpcResult::Timeout,
            Self::Status(result) => result,
        }
    }
}

pub(crate) fn record_bridge_rpc_outcome(
    bridge: &'static str,
    command: &'static str,
    timeout: std::time::Duration,
    started_at: Instant,
    outcome: BridgeRpcOutcome,
) {
    match outcome {
        BridgeRpcOutcome::Ok => worker_metrics::spaced_bridge_rpc_latency(
            bridge,
            command,
            outcome.metric_result(),
            started_at,
        ),
        BridgeRpcOutcome::Timeout => {
            worker_metrics::spaced_bridge_rpc_latency(
                bridge,
                command,
                outcome.metric_result(),
                started_at,
            );
            worker_metrics::spaced_bridge_rpc_timeout(bridge, command);
            tracing::warn!(
                command,
                bridge,
                timeout_ms = timeout.as_millis(),
                "spaced bridge guest RPC timed out"
            );
        }
        BridgeRpcOutcome::Status(_) => worker_metrics::spaced_bridge_rpc_latency(
            bridge,
            command,
            outcome.metric_result(),
            started_at,
        ),
    }
}

pub(crate) fn record_bridge_rpc_unbounded_outcome(
    bridge: &'static str,
    command: &'static str,
    started_at: Instant,
    outcome: BridgeRpcOutcome,
) {
    worker_metrics::spaced_bridge_rpc_latency(bridge, command, outcome.metric_result(), started_at);
}

// Keep both listeners alive through shutdown, including when one fails. The
// deadline bounds a stalled reader; the original failure keeps error priority.
async fn serve_until_shutdown(
    shutdown: CancellationToken,
    host_server: impl std::future::Future<Output = Result<(), RunSpacedHostServerError>>,
    bridge_host_server: impl std::future::Future<Output = Result<(), RunSpacedHostServerError>>,
) -> Result<(), RunSpacedHostServerError> {
    tokio::pin!(host_server);
    tokio::pin!(bridge_host_server);
    let (mut result, host_finished, bridge_finished) = tokio::select! {
        result = &mut host_server => (result, true, false),
        result = &mut bridge_host_server => (result, false, true),
        _ = shutdown.cancelled() => (Ok(()), false, false),
    };
    shutdown.cancel();
    let mut host_outcome = None;
    let mut bridge_outcome = None;
    let drain = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            async {
                host_outcome = Some(if host_finished {
                    Ok(())
                } else {
                    host_server.await
                });
            },
            async {
                bridge_outcome = Some(if bridge_finished {
                    Ok(())
                } else {
                    bridge_host_server.await
                });
            },
        )
    })
    .await;
    if drain.is_err() {
        tracing::warn!("spaced host shutdown drain deadline exceeded");
    }
    for (listener, outcome) in [("host", host_outcome), ("bridge-host", bridge_outcome)] {
        if let Some(Err(error)) = outcome {
            tracing::warn!(listener, %error, "spaced host listener failed during drain");
            if result.is_ok() {
                result = Err(error);
            }
        }
    }
    result
}

/// A bridge guest channel or client and, for vsock, the connection it is pinned to.
type WithPinnedConnection<T> = (T, Option<pplx_space_spaced_common::PinnedConnection>);

/// Stamps out a `call_bridge_guest_{meta,filesystem,process}` wrapper.
///
/// Each wrapper retries connection acquisition; only replay-safe calls also
/// retry dispatched RPCs. Failures invalidate their channel generation. The
/// only per-client variation is (1) which lazy-client pool to borrow a
/// lease from and (2) which error label to attach for diagnostics.
macro_rules! define_bridge_call {
    ($fn:ident, $pool:ident, $get_lease:ident, $client:ty, $label:literal) => {
        define_bridge_call!($fn, $pool, $get_lease, $client, $label, true);
    };
    ($fn:ident, $pool:ident, $get_lease:ident, $client:ty, $label:literal, $replay:literal) => {
        pub(super) async fn $fn<T, F, Fut>(
            &self,
            command: &'static str,
            budget: BridgeGuestOperationBudget,
            call: F,
        ) -> Result<T, Status>
        where
            F: Fn($client) -> Fut + Clone,
            Fut: std::future::Future<Output = Result<Response<T>, Status>>,
        {
            let failed_generation = FailedGeneration::default();
            let timeout = budget.timeout();
            let started_at = Instant::now();
            let result = pplx_space_spaced_common::retry_status_with_context_and_retry_hook(
                "spaced bridge guest",
                command,
                self.inner.bridge_retry_policy,
                RetryContext::with_timeout(timeout).with_cancellation(self.inner.shutdown.clone()),
                || {
                    let call = call.clone();
                    let failed_generation = failed_generation.clone();
                    async move {
                        let lease = self.$get_lease().await?;
                        failed_generation.set(lease.generation());
                        let client = lease.into_client();
                        let response = call(client).await.map(Response::into_inner);
                        if response.is_ok() {
                            failed_generation.clear();
                            self.inner.$pool.set_connected();
                        }
                        // Keep single-dispatch outcomes inside Ok so the retry
                        // loop retries only failures while acquiring a connection.
                        if $replay {
                            response.map(Ok)
                        } else {
                            Ok(response)
                        }
                    }
                },
                || {
                    let failed_generation = failed_generation.clone();
                    async move {
                        failed_generation.invalidate(&self.inner.$pool).await;
                    }
                },
            )
            .await
            .and_then(std::convert::identity);
            if !$replay
                && result.as_ref().is_err_and(|status| {
                    pplx_space_spaced_common::status_is_retryable(status)
                        || status.code() == tonic::Code::Cancelled
                })
            {
                failed_generation.invalidate(&self.inner.$pool).await;
            }
            record_bridge_rpc_outcome(
                $label,
                command,
                timeout,
                started_at,
                BridgeRpcOutcome::from_result(&result),
            );
            result.map_err(|status| host_relay_status(status, command, $label))
        }
    };
}

mod guest_ready;

#[cfg(test)]
use guest_ready::*;

impl SpacedHostService {
    pub(super) fn check_guest_signal_rate(&self, operation: &'static str) -> Result<(), Status> {
        let now = Instant::now();
        let mut state = self.inner.guest_signal_rate.lock().map_err(|error| {
            Status::internal(format!("guest signal rate lock poisoned: {error}"))
        })?;

        while state
            .arrivals
            .front()
            .is_some_and(|arrival| now.duration_since(*arrival) >= GUEST_SIGNAL_RATE_WINDOW)
        {
            state.arrivals.pop_front();
        }

        if state.arrivals.len() >= GUEST_SIGNAL_RATE_LIMIT {
            if state
                .last_warning
                .is_none_or(|last| now.duration_since(last) >= GUEST_SIGNAL_RATE_WINDOW)
            {
                state.last_warning = Some(now);
                tracing::warn!(
                    operation,
                    limit = GUEST_SIGNAL_RATE_LIMIT,
                    window_ms = GUEST_SIGNAL_RATE_WINDOW.as_millis() as u64,
                    "guest bridge signal rate limit exceeded"
                );
            }
            return Err(Status::resource_exhausted(
                "guest signal rate limit exceeded",
            ));
        }

        state.arrivals.push_back(now);
        Ok(())
    }

    pub(super) fn validate_guest_text_field(
        field_name: &'static str,
        value: &str,
        max_bytes: usize,
    ) -> Result<(), Status> {
        if value.len() > max_bytes {
            return Err(Status::invalid_argument(format!(
                "{field_name} exceeds {max_bytes} bytes"
            )));
        }
        Ok(())
    }

    pub fn new(shutdown: CancellationToken) -> Self {
        Self::new_with_config(shutdown, SpacedHostServiceConfig::default())
    }

    pub fn new_with_config(shutdown: CancellationToken, config: SpacedHostServiceConfig) -> Self {
        let bridge_host_expected_peer = match &config.bridge_guest_connector {
            BridgeGuestConnector::FirecrackerVsock { expected_peer, .. } => Some(*expected_peer),
            BridgeGuestConnector::Uds(_) => None,
        };
        Self {
            inner: Arc::new(SpacedHostServiceInner {
                process_recovery: config
                    .process_recovery_activation
                    .map(crate::recovery::RecoveryLifecycle::new),
                shutdown,
                socket_path: config.socket_path,
                bridge_host_socket_path: config.bridge_host_socket_path,
                bridge_host_expected_peer,
                bridge_guest_connector: config.bridge_guest_connector,
                host_control_socket_path: config.host_control_socket_path,
                bridge_guest_meta_client: LazyClient::new(),
                bridge_guest_filesystem_client: LazyClient::new(),
                bridge_guest_process_client: LazyClient::new(),
                bridge_guest_networking_client: LazyClient::new(),
                host_control_client: LazyClient::new(),
                bridge_retry_policy: DEFAULT_COMMAND_RETRY_POLICY,
                process_admission: crate::payload_admission(
                    pplx_space_spaced_common::process_limits::PAYLOAD_SLOTS,
                ),
                filesystem_admission: crate::payload_admission(
                    pplx_space_spaced_common::filesystem_limits::PAYLOAD_SLOTS,
                ),
                guest_readiness: GuestReadinessCoordinator::new(),
                guest_signal_rate: std::sync::Mutex::new(GuestSignalRateState::default()),
                #[cfg(any(test, feature = "test-support"))]
                host_peer_authorization: config.host_peer_authorization,
                host_peer_authorizer: UdsPeerAuthorizer::new(
                    "space-spaced-host",
                    [UdsPeerAccount::with_linux_user(users::SPACE_SBXMGR)],
                ),
            }),
        }
    }

    pub(super) fn authorize_bridge_host_peer<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let Some(expected_peer) = self.inner.bridge_host_expected_peer else {
            return Ok(());
        };
        let credentials = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>()
            .and_then(|info| info.peer_cred)
            .ok_or_else(|| Status::permission_denied("bridge host peer identity is unavailable"))?;
        if credentials.pid() == Some(expected_peer.pid) && credentials.uid() == expected_peer.uid {
            return Ok(());
        }
        Err(Status::permission_denied(
            "bridge host peer does not match the registered Firecracker runtime",
        ))
    }

    pub async fn run(self) -> Result<(), RunSpacedHostServerError> {
        tracing::info!(
            host_socket_path = %self.inner.socket_path.display(),
            bridge_host_socket_path = %self.inner.bridge_host_socket_path.display(),
            bridge_guest_connector = ?self.inner.bridge_guest_connector,
            host_control_socket_path = %self.inner.host_control_socket_path.display(),
            "spaced host service started"
        );
        serve_until_shutdown(
            self.inner.shutdown.clone(),
            self.clone().run_host_server(),
            self.clone().run_bridge_host_server(),
        )
        .await?;

        tracing::info!("spaced host service stopped");
        Ok(())
    }

    pub(super) async fn run_host_server(self) -> Result<(), RunSpacedHostServerError> {
        let socket_path = self.inner.socket_path.clone();
        let incoming = pplx_space_spaced_common::bind_uds_with_mode(&socket_path, Some(0o770))?;
        let incoming = tokio_stream::StreamExt::map(incoming, |io| {
            io.map(|io| pplx_space_common_grpc::PrefaceDeadline::new(io, Duration::from_secs(5)))
        });
        let admission = admit_connections(
            incoming,
            self.inner.shutdown.clone(),
            ConnectionAdmissionConfig::new("spaced-host", MAX_HOST_SERVER_CONNECTIONS),
            observe_connection_admission,
        );
        let result = admission
            .supervise(move |incoming| {
                pplx_space_common_grpc::grpc_server_builder()
                    // Leave HTTP/2 slots above the per-peer output limit so the
                    // separately admitted control requests can reach the router.
                    .max_concurrent_streams(4096)
                    .http2_keepalive_interval(Some(Duration::from_secs(10)))
                    .http2_keepalive_timeout(Some(Duration::from_secs(5)))
                    .layer(crate::authorization::HostAuthorizationLayer(self.clone()))
                    .layer(host_request_limiter())
                    .layer(pplx_space_common_svc::grpc_trace_layer())
                    .add_service(MetaServer::new(self.clone()))
                    .add_service(self.filesystem_grpc_service())
                    .add_service(self.process_grpc_service())
                    .add_service(NetworkingServer::new(self.clone()))
                    .serve_with_incoming_shutdown(incoming, async move {
                        self.inner.shutdown.cancelled().await
                    })
            })
            .await?;
        result.map_err(RunSpacedHostServerError::Serve)?;
        Ok(())
    }

    pub(super) async fn run_bridge_host_server(self) -> Result<(), RunSpacedHostServerError> {
        let socket_path = self.inner.bridge_host_socket_path.clone();
        let incoming = pplx_space_spaced_common::bind_uds(&socket_path)?;
        let incoming = tokio_stream::StreamExt::map(incoming, |io| {
            io.map(|io| pplx_space_common_grpc::PrefaceDeadline::new(io, Duration::from_secs(5)))
        });
        let admission = admit_connections(
            incoming,
            self.inner.shutdown.clone(),
            ConnectionAdmissionConfig::new("spaced-host-bridge", MAX_BRIDGE_HOST_CONNECTIONS),
            observe_connection_admission,
        );
        let result = admission
            .supervise(move |incoming| {
                pplx_space_common_grpc::grpc_server_builder()
                    .max_concurrent_streams(64)
                    .http2_keepalive_interval(Some(Duration::from_secs(10)))
                    .http2_keepalive_timeout(Some(Duration::from_secs(5)))
                    .layer(
                        pplx_space_common_svc::conservative_uds_grpc_rate_limiter_layer(
                            "space-spaced-host-bridge",
                        ),
                    )
                    .layer(pplx_space_common_svc::grpc_trace_layer())
                    .add_service(
                        BridgeHostMetaServer::new(self.clone())
                            .max_decoding_message_size(GUEST_BRIDGE_MAX_MESSAGE_SIZE)
                            .max_encoding_message_size(GUEST_BRIDGE_MAX_MESSAGE_SIZE),
                    )
                    .serve_with_incoming_shutdown(incoming, async move {
                        self.inner.shutdown.cancelled().await
                    })
            })
            .await?;
        result.map_err(RunSpacedHostServerError::Serve)?;
        Ok(())
    }

    pub(super) async fn connect_bridge_guest_channel(
        &self,
    ) -> Result<WithPinnedConnection<tonic::transport::Channel>, ConnectBridgeGuestError> {
        match &self.inner.bridge_guest_connector {
            BridgeGuestConnector::Uds(path) => Ok((
                pplx_space_spaced_common::connect_uds_channel("spaced bridge guest", path).await?,
                None,
            )),
            BridgeGuestConnector::FirecrackerVsock {
                vsock_uds_path,
                port,
                expected_peer,
            } => Ok(
                pplx_space_spaced_common::connect_firecracker_vsock_channel_with_expected_peer(
                    "spaced bridge guest",
                    vsock_uds_path,
                    *port,
                    *expected_peer,
                )
                .await
                .map(|(channel, connection)| (channel, Some(connection)))?,
            ),
        }
    }

    pub(super) async fn connect_bridge_guest(
        &self,
    ) -> Result<
        WithPinnedConnection<BridgeGuestClient<tonic::transport::Channel>>,
        ConnectBridgeGuestError,
    > {
        let (channel, connection) = self.connect_bridge_guest_channel().await?;
        Ok((BridgeGuestClient::new(channel), connection))
    }

    pub(super) async fn connect_bridge_guest_filesystem(
        &self,
    ) -> Result<
        WithPinnedConnection<BridgeGuestFilesystemClient<tonic::transport::Channel>>,
        ConnectBridgeGuestError,
    > {
        let (channel, connection) = self.connect_bridge_guest_channel().await?;
        Ok((BridgeGuestFilesystemClient::new(channel), connection))
    }

    pub(super) async fn connect_bridge_guest_process(
        &self,
    ) -> Result<
        WithPinnedConnection<BridgeGuestProcessClient<tonic::transport::Channel>>,
        ConnectBridgeGuestError,
    > {
        let (channel, connection) = self.connect_bridge_guest_channel().await?;
        Ok((BridgeGuestProcessClient::new(channel), connection))
    }

    pub(super) async fn connect_bridge_guest_networking(
        &self,
    ) -> Result<
        WithPinnedConnection<BridgeGuestNetworkingClient<tonic::transport::Channel>>,
        ConnectBridgeGuestError,
    > {
        let (channel, connection) = self.connect_bridge_guest_channel().await?;
        Ok((BridgeGuestNetworkingClient::new(channel), connection))
    }

    pub(super) async fn connect_host_control(
        &self,
    ) -> Result<
        HostControlClient<tonic::transport::Channel>,
        pplx_space_spaced_common::UdsConnectError,
    > {
        let channel = pplx_space_spaced_common::connect_uds_channel(
            "sandbox host control",
            &self.inner.host_control_socket_path,
        )
        .await?;
        Ok(HostControlClient::new(channel))
    }

    pub async fn notify_resume(
        &self,
        reason: pplx_space_spaced_common::proto::bridge_guest::ResumeReason,
    ) -> Result<(), Status> {
        let now = pplx_space_spaced_common::system_time_to_proto(std::time::SystemTime::now())?;
        let response = self
            .call_bridge_guest_once(
                "bridge_guest_notify_resume",
                BridgeGuestOperationBudget::RuntimeMetadata,
                |mut client| async move {
                    client
                        .notify_resume(BridgeGuestNotifyResumeRequest {
                            reason: reason.into(),
                            host_time: Some(now),
                        })
                        .await
                },
            )
            .await?;
        tracing::debug!(
            clock_sync_status = ?response.clock_sync_status(),
            ready = response.ready,
            "spaced guest resume notification succeeded"
        );
        match response.clock_sync_status() {
            pplx_space_spaced_common::proto::bridge_guest::ClockSyncStatus::Synced => Ok(()),
            pplx_space_spaced_common::proto::bridge_guest::ClockSyncStatus::Failed => {
                Err(Status::failed_precondition("guest clock sync failed"))
            }
            pplx_space_spaced_common::proto::bridge_guest::ClockSyncStatus::Unspecified => Err(
                Status::internal("bridge guest returned unspecified clock sync status"),
            ),
        }
    }

    pub(super) async fn call_bridge_guest_ping(&self) -> Result<(), Status> {
        self.call_bridge_guest(
            "bridge_guest_ping",
            BridgeGuestOperationBudget::RuntimeMetadata,
            |client| async move {
                client
                    .max_decoding_message_size(
                        pplx_space_spaced_common::BRIDGE_GUEST_PING_MAX_DECODE_BYTES,
                    )
                    .ping(BridgeGuestPingRequest::default())
                    .await
            },
        )
        .await
        .map(|_| ())
    }

    define_bridge_call!(
        call_bridge_guest,
        bridge_guest_meta_client,
        bridge_guest_client,
        BridgeGuestClient<tonic::transport::Channel>,
        "bridge_guest_meta"
    );

    pub(super) async fn host_control_client(
        &self,
    ) -> Result<
        pplx_space_spaced_common::LazyClientLease<HostControlClient<tonic::transport::Channel>>,
        Status,
    > {
        self.inner
            .host_control_client
            .get_or_connect_with_generation(|| self.connect_host_control())
            .await
            .map_err(|error| Status::unavailable(error.to_string()))
    }

    pub(super) async fn call_host_control<T, F, Fut>(
        &self,
        command: &'static str,
        call: F,
    ) -> Result<T, Status>
    where
        F: Fn(HostControlClient<tonic::transport::Channel>) -> Fut + Clone,
        Fut: std::future::Future<Output = Result<Response<T>, Status>>,
    {
        let failed_generation = FailedGeneration::default();
        pplx_space_spaced_common::retry_status_with_context_and_retry_hook(
            "sandbox host control",
            command,
            RetryPolicy::none(),
            RetryContext::with_timeout(HOST_CONTROL_CALL_TIMEOUT)
                .with_cancellation(self.inner.shutdown.clone()),
            || {
                let call = call.clone();
                let failed_generation = failed_generation.clone();
                async move {
                    let lease = self.host_control_client().await?;
                    failed_generation.set(lease.generation());
                    let client = lease.into_client();
                    let response = call(client).await?;
                    failed_generation.clear();
                    self.inner.host_control_client.set_connected();
                    Ok(response.into_inner())
                }
            },
            || {
                let failed_generation = failed_generation.clone();
                async move {
                    failed_generation
                        .invalidate(&self.inner.host_control_client)
                        .await;
                }
            },
        )
        .await
        .map_err(|status| host_relay_status(status, command, "host_control"))
    }

    pub(super) async fn call_bridge_guest_read(
        &self,
        request: ReadRequest,
    ) -> Result<tonic::Streaming<pplx_space_spaced_common::proto::bridge_guest::FileChunk>, Status>
    {
        let path = GuestPath::parse(request.path).map_err(invalid_argument_status)?;
        let offset = request.offset;
        let length = request.length;
        let user = request
            .user
            .map(GuestUserName::parse)
            .transpose()
            .map_err(invalid_argument_status)?;
        self.call_bridge_guest_filesystem(
            "bridge_guest_read",
            BridgeGuestOperationBudget::FilesystemTransfer,
            move |mut client| {
                let path = path.expose().to_string();
                let user = user.as_ref().map(|user| user.as_str().to_string());
                async move {
                    client
                        .read(BridgeGuestReadRequest {
                            path,
                            offset,
                            length,
                            user,
                        })
                        .await
                }
            },
        )
        .await
    }

    pub(super) async fn bridge_guest_filesystem_client(
        &self,
    ) -> Result<
        pplx_space_spaced_common::LazyClientLease<
            BridgeGuestFilesystemClient<tonic::transport::Channel>,
        >,
        Status,
    > {
        self.inner
            .bridge_guest_filesystem_client
            .get_or_connect_pinned_with_generation(|| async {
                self.connect_bridge_guest_filesystem()
                    .await
                    .map_err(|error| Status::unavailable(error.to_string()))
            })
            .await
    }

    pub(super) async fn call_bridge_guest_mkdir(
        &self,
        request: MkdirRequest,
    ) -> Result<(), Status> {
        let path = GuestPath::parse(request.path).map_err(invalid_argument_status)?;
        let parents = request.parents;
        let exist_ok = request.exist_ok;
        let user = request
            .user
            .map(GuestUserName::parse)
            .transpose()
            .map_err(invalid_argument_status)?;
        self.call_bridge_guest_filesystem_once(
            "bridge_guest_mkdir",
            BridgeGuestOperationBudget::FilesystemMutation,
            move |mut client| {
                let path = path.expose().to_string();
                let user = user.as_ref().map(|user| user.as_str().to_owned());
                async move {
                    client
                        .mkdir(BridgeGuestMkdirRequest {
                            path,
                            parents,
                            exist_ok,
                            user,
                        })
                        .await
                }
            },
        )
        .await
        .map(|_: pplx_space_spaced_common::proto::bridge_guest::MkdirResponse| ())
    }

    pub(super) async fn call_bridge_guest_remove(
        &self,
        request: RemoveRequest,
    ) -> Result<(), Status> {
        let path = GuestPath::parse(request.path).map_err(invalid_argument_status)?;
        let recursive = request.recursive;
        let missing_ok = request.missing_ok;
        let user = request
            .user
            .map(GuestUserName::parse)
            .transpose()
            .map_err(invalid_argument_status)?;
        self.call_bridge_guest_filesystem_once(
            "bridge_guest_remove",
            BridgeGuestOperationBudget::FilesystemMutation,
            move |mut client| {
                let path = path.expose().to_string();
                let user = user.as_ref().map(|user| user.as_str().to_string());
                async move {
                    client
                        .remove(BridgeGuestRemoveRequest {
                            path,
                            recursive,
                            missing_ok,
                            user,
                        })
                        .await
                }
            },
        )
        .await
        .map(|_: pplx_space_spaced_common::proto::bridge_guest::RemoveResponse| ())
    }

    pub(super) async fn call_bridge_guest_rename(
        &self,
        request: RenameRequest,
    ) -> Result<(), Status> {
        let old_path = GuestPath::parse(request.old_path).map_err(invalid_argument_status)?;
        let new_path = GuestPath::parse(request.new_path).map_err(invalid_argument_status)?;
        let idempotency_key = request
            .idempotency_key
            .map(IdempotencyKey::parse)
            .transpose()
            .map_err(invalid_argument_status)?;
        let user = request
            .user
            .map(GuestUserName::parse)
            .transpose()
            .map_err(invalid_argument_status)?;
        self.call_bridge_guest_filesystem(
            "bridge_guest_rename",
            BridgeGuestOperationBudget::FilesystemMutation,
            move |mut client| {
                let old_path = old_path.expose().to_string();
                let new_path = new_path.expose().to_string();
                let idempotency_key = idempotency_key
                    .as_ref()
                    .map(|idempotency_key| idempotency_key.as_str().to_string());
                let user = user.as_ref().map(|user| user.as_str().to_string());
                async move {
                    client
                        .rename(BridgeGuestRenameRequest {
                            old_path,
                            new_path,
                            idempotency_key,
                            user,
                        })
                        .await
                }
            },
        )
        .await
        .map(|_: pplx_space_spaced_common::proto::bridge_guest::RenameResponse| ())
    }

    define_bridge_call!(
        call_bridge_guest_filesystem,
        bridge_guest_filesystem_client,
        bridge_guest_filesystem_client,
        BridgeGuestFilesystemClient<tonic::transport::Channel>,
        "bridge_guest_filesystem"
    );

    define_bridge_call!(
        call_bridge_guest_process,
        bridge_guest_process_client,
        bridge_guest_process_client,
        BridgeGuestProcessClient<tonic::transport::Channel>,
        "bridge_guest_process"
    );

    define_bridge_call!(
        call_bridge_guest_process_once,
        bridge_guest_process_client,
        bridge_guest_process_client,
        BridgeGuestProcessClient<tonic::transport::Channel>,
        "bridge_guest_process",
        false
    );

    define_bridge_call!(
        call_bridge_guest_once,
        bridge_guest_meta_client,
        bridge_guest_client,
        BridgeGuestClient<tonic::transport::Channel>,
        "bridge_guest_meta",
        false
    );

    define_bridge_call!(
        call_bridge_guest_filesystem_once,
        bridge_guest_filesystem_client,
        bridge_guest_filesystem_client,
        BridgeGuestFilesystemClient<tonic::transport::Channel>,
        "bridge_guest_filesystem",
        false
    );

    define_bridge_call!(
        call_bridge_guest_networking_once,
        bridge_guest_networking_client,
        bridge_guest_networking_client,
        BridgeGuestNetworkingClient<tonic::transport::Channel>,
        "bridge_guest_networking",
        false
    );

    pub(super) async fn clear_all_bridge_guest_clients(&self) {
        self.inner.bridge_guest_meta_client.clear().await;
        self.inner.bridge_guest_filesystem_client.clear().await;
        self.inner.bridge_guest_process_client.clear().await;
        self.inner.bridge_guest_networking_client.clear().await;
    }

    pub(super) async fn bridge_guest_client(
        &self,
    ) -> Result<
        pplx_space_spaced_common::LazyClientLease<BridgeGuestClient<tonic::transport::Channel>>,
        Status,
    > {
        self.inner
            .bridge_guest_meta_client
            .get_or_connect_pinned_with_generation(|| async {
                self.connect_bridge_guest()
                    .await
                    .map_err(|error| Status::unavailable(error.to_string()))
            })
            .await
    }

    pub(super) async fn bridge_guest_process_client(
        &self,
    ) -> Result<
        pplx_space_spaced_common::LazyClientLease<
            BridgeGuestProcessClient<tonic::transport::Channel>,
        >,
        Status,
    > {
        self.inner
            .bridge_guest_process_client
            .get_or_connect_pinned_with_generation(|| async {
                self.connect_bridge_guest_process()
                    .await
                    .map_err(|error| Status::unavailable(error.to_string()))
            })
            .await
    }

    pub(super) async fn bridge_guest_networking_client(
        &self,
    ) -> Result<
        pplx_space_spaced_common::LazyClientLease<
            BridgeGuestNetworkingClient<tonic::transport::Channel>,
        >,
        Status,
    > {
        self.inner
            .bridge_guest_networking_client
            .get_or_connect_pinned_with_generation(|| async {
                self.connect_bridge_guest_networking()
                    .await
                    .map_err(|error| Status::unavailable(error.to_string()))
            })
            .await
    }
}

const LOG_TARGET: &str = module_path!();
