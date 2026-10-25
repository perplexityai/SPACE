//! Clients for a node's node-api (worker, runtime, builder) and the RPC budgets they share.

use super::*;

pub(super) type RuntimeTransport =
    GrpcClientTrace<InterceptedService<Channel, SpaceBearerInterceptor>>;

/// Runtime RPCs and capability checks share the resolved worker connection.
#[derive(Clone, Debug)]
pub struct RuntimeClient {
    pub(super) worker: WorkerApiClient<RuntimeTransport>,
    pub(super) channel: Channel,
    /// Set when `channel` is a cached channel that other callers share.
    pub(super) kept: Option<pplx_space_common_grpc::ConnectionWatch>,
    /// Set for a Native data-plane request's client; see [`KeptCallReport`].
    #[cfg(feature = "kubernetes")]
    pub(super) report: Option<std::sync::Arc<KeptCallReport>>,
}

/// How long a Native data-plane request may hold its worker client before
/// [`KeptCallReport`] describes the connection it used.
#[cfg(feature = "kubernetes")]
pub(super) const SLOW_KEPT_CALL: Duration = Duration::from_millis(2500);

/// Describes, once the last clone of a Native request's worker client is
/// dropped after at least [`SLOW_KEPT_CALL`], the connection the request
/// used, so a slow call can be attributed. A call that waited on a dialed
/// connection which then had to be redialed opened a new connection; one on
/// a connection whose peer went silent ends with it closed (its keepalive
/// timed out); one that waited on a live connection, behind loss or flow
/// control, ends with neither.
#[cfg(feature = "kubernetes")]
#[derive(Debug)]
pub(super) struct KeptCallReport {
    pub(super) handed_out: std::time::Instant,
    pub(super) kept: bool,
    pub(super) connection_age: Duration,
    pub(super) idle: Duration,
    pub(super) connections_at_handout: usize,
    pub(super) watch: pplx_space_common_grpc::ConnectionWatch,
}

#[cfg(feature = "kubernetes")]
impl KeptCallReport {
    /// What [`Drop`] reports, if anything: the fields of its event.
    pub(super) fn slow_call(&self) -> Option<SlowKeptCall> {
        let held = self.handed_out.elapsed();
        (held >= SLOW_KEPT_CALL).then(|| SlowKeptCall {
            held,
            kept: self.kept,
            connection_age: self.connection_age,
            idle: self.idle,
            redials: self
                .watch
                .connections_opened()
                .saturating_sub(self.connections_at_handout),
            connection_closed: self.watch.is_closed(),
        })
    }
}

#[cfg(feature = "kubernetes")]
#[derive(Debug, PartialEq, Eq)]
pub(super) struct SlowKeptCall {
    pub(super) held: Duration,
    pub(super) kept: bool,
    pub(super) connection_age: Duration,
    pub(super) idle: Duration,
    pub(super) redials: usize,
    pub(super) connection_closed: bool,
}

#[cfg(feature = "kubernetes")]
impl Drop for KeptCallReport {
    fn drop(&mut self) {
        if let Some(slow) = self.slow_call() {
            tracing::warn!(
                held_ms = slow.held.as_millis() as u64,
                kept = slow.kept,
                connection_age_ms = slow.connection_age.as_millis() as u64,
                idle_before_ms = slow.idle.as_millis() as u64,
                redials = slow.redials,
                connection_closed = slow.connection_closed,
                "native worker call held its pod connection a long time"
            );
        }
    }
}

impl RuntimeClient {
    pub(super) fn new(channel: Channel) -> Self {
        let transport = node_api_channel(channel.clone(), SpaceBearerToken::NodeApi);
        Self {
            worker: WorkerApiClient::new(transport),
            channel,
            kept: None,
            #[cfg(feature = "kubernetes")]
            report: None,
        }
    }

    /// A client on a channel a cache keeps while `watch` is open.
    #[cfg(feature = "kubernetes")]
    pub(super) fn kept(channel: Channel, watch: pplx_space_common_grpc::ConnectionWatch) -> Self {
        Self {
            kept: Some(watch),
            ..Self::new(channel)
        }
    }

    /// This kept client, reporting its use as [`KeptCallReport`] describes.
    #[cfg(feature = "kubernetes")]
    pub(super) fn with_report(
        mut self,
        kept: bool,
        connection_age: Duration,
        idle: Duration,
    ) -> Self {
        if let Some(watch) = &self.kept {
            self.report = Some(std::sync::Arc::new(KeptCallReport {
                handed_out: std::time::Instant::now(),
                kept,
                connection_age,
                idle,
                connections_at_handout: watch.connections_opened(),
                watch: watch.clone(),
            }));
        }
        self
    }

    /// Stop handing this client's cached channel to later callers, so the
    /// next one dials afresh. Call it when the peer showed that new calls
    /// should reach a new connection, such as a draining node-api; this
    /// client and its clones keep working. A no-op for an uncached channel.
    pub(crate) fn retire_kept_connection(&self) {
        if let Some(watch) = &self.kept {
            watch.close();
        }
    }

    pub(in crate::controllers) async fn require_activation_policy_support(
        &mut self,
        timeout: Duration,
    ) -> Result<(), ControllerError> {
        require_activation_policy_support(
            node_api_channel(self.channel.clone(), SpaceBearerToken::NodeApi),
            timeout,
        )
        .await
    }

    pub(super) fn internal_client(&self) -> RuntimeInternalClient {
        WorkerApiInternalClient::new(node_api_channel(
            self.channel.clone(),
            SpaceBearerToken::NodeApiInternal,
        ))
    }
}

impl std::ops::Deref for RuntimeClient {
    type Target = WorkerApiClient<RuntimeTransport>;

    fn deref(&self) -> &Self::Target {
        &self.worker
    }
}

impl std::ops::DerefMut for RuntimeClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.worker
    }
}

pub type RuntimeInternalClient =
    WorkerApiInternalClient<GrpcClientTrace<InterceptedService<Channel, SpaceBearerInterceptor>>>;

pub type BuilderClient =
    BuilderApiClient<GrpcClientTrace<InterceptedService<Channel, SpaceBearerInterceptor>>>;

/// Per-request node-RPC channel timeout (the tonic `Endpoint::timeout` on
/// worker/guest channels). This — NOT the ALB — is the binding internal cut
/// behind the create/restore latency cliff: a slow-but-not-stuck node RPC that
/// rides up to it is severed by tonic as `Code::Cancelled`, which surfaces as
/// an opaque HTTP 408 with no sandbox id and no retry semantics. Every API-side
/// blocking wait must finish, *and* a structured retryable response must flush,
/// below it. (The ECW ALB idle timeout is far higher — see
/// [`ALB_IDLE_TIMEOUT_SECS`] — so the ALB is not the binding constraint.)
///
/// The 150s timeout leaves a 135s blocking budget after reserving 15s for
/// completing the response.
///
/// The value lives in `common-core`'s `restore_ownership` module because the
/// placement crate's restore lease TTL must equal it; deriving both from one
/// constant is what keeps a future timeout bump from silently splitting the
/// lease-outlives-RPC invariant across crates.
pub(crate) const NODE_RPC_TIMEOUT: Duration =
    pplx_space_common_core::restore_ownership::NODE_RPC_TIMEOUT;

pub(crate) const NODE_RPC_TIMEOUT_SECS: u64 = NODE_RPC_TIMEOUT.as_secs();

pub(super) const NODE_API_HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);

pub(super) const NODE_API_HTTP2_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(5);

/// Headroom below [`NODE_RPC_TIMEOUT`] for the API poll loop / wait wrapper to
/// detect exhaustion and return a structured, retryable response (carrying the
/// sandbox id + `Retry-After`) before tonic severs the channel with a bare
/// `Cancelled` 408.
pub(super) const BLOCKING_WAIT_MARGIN_SECS: u64 = 15;

/// API-side blocking-wait budget for create/restore/wake/access, held below
/// [`NODE_RPC_TIMEOUT`] so a slow wait surfaces as a structured retryable
/// `SubstrateError::Timeout` before the per-request channel timeout fires. The
/// single budget every blocking lifecycle wait is sized against.
///
/// `pub` (not `pub(crate)`) so out-of-crate callers that pass a
/// `ready_timeout` — e.g. the cluster-controller's node-drain restore —
/// can report the effective (clamped) per-attempt wait instead of
/// hand-copying this value into comments and logs.
pub const NODE_RPC_BLOCKING_BUDGET: Duration =
    Duration::from_secs(NODE_RPC_TIMEOUT_SECS - BLOCKING_WAIT_MARGIN_SECS);

/// The ECW ALB idle timeout (`idle_timeout.timeout_seconds`), set via the AWS
/// LB controller annotation on the deployment's Ingress. It is the OUTER
/// ceiling, far above the node-RPC
/// timeout — documented here so the hierarchy is explicit and version-checked;
/// keep it in sync with the Helm values. The static assert pins
/// `NODE_RPC_TIMEOUT < ALB idle`, so the binding cut can never silently exceed
/// the gateway.
pub(crate) const ALB_IDLE_TIMEOUT_SECS: u64 = 1800;

const _: () = assert!(
    NODE_RPC_TIMEOUT_SECS < ALB_IDLE_TIMEOUT_SECS,
    "node-RPC channel timeout must stay below the ALB idle timeout",
);

pub(crate) fn node_api_endpoint(
    addr: &str,
    connect_timeout: Duration,
    request_timeout: Duration,
) -> Result<Endpoint, SubstrateError> {
    let uri = pplx_space_common_grpc::internal_transport::qualify_endpoint(addr);
    Endpoint::from_shared(uri)
        .map(|endpoint| configure_node_api_endpoint(endpoint, connect_timeout, request_timeout))
        .map_err(|err| SubstrateError::Failed {
            message: format!("invalid node-api endpoint '{addr}': {err}"),
        })
}

pub(super) fn configure_node_api_endpoint(
    endpoint: Endpoint,
    connect_timeout: Duration,
    request_timeout: Duration,
) -> Endpoint {
    pplx_space_node_api_client::configure_node_api_endpoint(
        endpoint,
        NodeApiEndpointConfig::default()
            .with_connect_timeout(connect_timeout)
            .with_request_timeout(request_timeout)
            .with_http2_keep_alive(
                NODE_API_HTTP2_KEEP_ALIVE_INTERVAL,
                NODE_API_HTTP2_KEEP_ALIVE_TIMEOUT,
            ),
    )
}

pub(super) fn retrying_worker_node_api(
    endpoint_addr: &str,
    request_timeout: Duration,
) -> Result<NodeApiClient, SubstrateError> {
    let endpoint = node_api_endpoint(endpoint_addr, Duration::from_secs(3), request_timeout)?;
    Ok(NodeApiClient::with_service_name(
        "worker-node-api",
        endpoint,
    ))
}

pub async fn connect_node_api_client_for_target<A>(
    substrate: &A,
    target: &NodeApiTarget<'_>,
    request_timeout: Duration,
) -> Result<(String, RuntimeClient), SubstrateError>
where
    A: SubstrateAdapter + ?Sized,
{
    let endpoint_addr = substrate.resolve_node_api_endpoint(target).await?;
    let endpoint = node_api_endpoint(&endpoint_addr, Duration::from_secs(3), request_timeout)?;
    let client = connect_worker(endpoint)
        .await
        .map_err(|err| SubstrateError::Unavailable {
            message: format!("node-api not reachable at {endpoint_addr}: {err}"),
        })?;
    Ok((endpoint_addr, client))
}

pub async fn connect_retrying_node_api_client_for_target<A>(
    substrate: &A,
    target: &NodeApiTarget<'_>,
    request_timeout: Duration,
) -> Result<(String, NodeApiClient), SubstrateError>
where
    A: SubstrateAdapter + ?Sized,
{
    let endpoint_addr = substrate.resolve_node_api_endpoint(target).await?;
    let client = retrying_worker_node_api(&endpoint_addr, request_timeout)?;
    Ok((endpoint_addr, client))
}

/// Wrap a connected node-api `Channel` with the bearer-token interceptor (so a
/// local token read/mount failure fails the call with `Status::unavailable`, not a
/// header-less request the node rejects as a 401) and the client trace layer.
/// Every node-api client is built through this so auth is attached in one place.
pub fn node_api_channel(
    channel: Channel,
    kind: SpaceBearerToken,
) -> GrpcClientTrace<InterceptedService<Channel, SpaceBearerInterceptor>> {
    wrap_grpc_client_channel(InterceptedService::new(
        channel,
        SpaceBearerInterceptor::new(kind),
    ))
}

/// Apply the node-api internal transport to `endpoint` for the TCP connect
/// helpers, carrying the TLS handshake bound the transport applies.
pub(super) async fn node_api_connect_endpoint(
    endpoint: Endpoint,
) -> Result<pplx_space_common_grpc::internal_transport::ConnectEndpoint, ControllerError> {
    pplx_space_common_grpc::internal_transport::client_connect_endpoint(
        pplx_space_common_grpc::internal_transport::Service::NodeApi,
        endpoint,
    )
    .await
    .map_err(ControllerError::node_api_transport)
}

pub(in crate::controllers) async fn require_activation_policy_support(
    transport: RuntimeTransport,
    timeout: Duration,
) -> Result<(), ControllerError> {
    let mut health = tonic_health::pb::health_client::HealthClient::new(transport);
    let mut request = tonic::Request::new(tonic_health::pb::HealthCheckRequest {
        service: pplx_space_sbx_mgr_client::ACTIVATION_POLICY_HEALTH_SERVICE.to_owned(),
    });
    request.set_timeout(timeout);
    match tokio::time::timeout(timeout, health.check(request)).await {
        Ok(Ok(response))
            if response.get_ref().status
                == tonic_health::pb::health_check_response::ServingStatus::Serving as i32 =>
        {
            Ok(())
        }
        Ok(Ok(_)) => Err(ControllerError::service_unavailable(
            "worker does not support activation policy",
        )),
        Ok(Err(error)) => Err(ControllerError::service_unavailable(format!(
            "worker activation policy capability check failed: {error}"
        ))),
        Err(_) => Err(ControllerError::service_unavailable(
            "worker activation policy capability check timed out",
        )),
    }
}

pub async fn connect_worker(endpoint: Endpoint) -> Result<RuntimeClient, ControllerError> {
    let channel = node_api_connect_endpoint(endpoint)
        .await?
        .connect_with_retry(
            "worker-node-api",
            "worker-node-api",
            pplx_space_common_grpc::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
        )
        .await
        .map_err(|err| {
            ControllerError::service_unavailable(format!("node-api not reachable: {err}"))
        })?;
    Ok(RuntimeClient::new(channel))
}

/// [`connect_worker`] for a channel a cache keeps: the same dial, plus the
/// watch that closes once the connection it was dialed with ends.
#[cfg(feature = "kubernetes")]
pub(super) async fn connect_worker_watched(
    endpoint: Endpoint,
) -> Result<(Channel, pplx_space_common_grpc::ConnectionWatch), ControllerError> {
    connect_worker_watched_with(
        endpoint,
        pplx_space_common_grpc::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
    )
    .await
}

/// [`connect_worker_watched`] with its first dial retried under
/// `retry_policy`.
#[cfg(feature = "kubernetes")]
pub(super) async fn connect_worker_watched_with(
    endpoint: Endpoint,
    retry_policy: pplx_space_common_grpc::GrpcRetryPolicy,
) -> Result<(Channel, pplx_space_common_grpc::ConnectionWatch), ControllerError> {
    // A kept connection carries every call to its pod, so it needs its own
    // liveness check: ping it while idle too, so a peer lost without a FIN is
    // torn down before the next caller reuses it, rather than only once a
    // call is in flight.
    //
    // It also carries concurrent streams that used to get a connection each,
    // so it takes the multiplexed connection window.
    let endpoint = endpoint
        .keep_alive_while_idle(true)
        .initial_connection_window_size(
            pplx_space_node_api_client::MULTIPLEXED_CONNECTION_WINDOW_BYTES,
        );
    node_api_connect_endpoint(endpoint)
        .await?
        .connect_with_retry_watched("worker-node-api", "worker-node-api", retry_policy)
        .await
        .map_err(|err| {
            ControllerError::service_unavailable(format!("node-api not reachable: {err}"))
        })
}

pub async fn connect_worker_internal(
    endpoint: Endpoint,
) -> Result<RuntimeInternalClient, ControllerError> {
    let channel = node_api_connect_endpoint(endpoint)
        .await?
        .connect_with_retry(
            "worker-node-api-internal",
            "worker-node-api-internal",
            pplx_space_common_grpc::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
        )
        .await
        .map_err(|err| {
            ControllerError::service_unavailable(format!("node-api not reachable: {err}"))
        })?;
    Ok(WorkerApiInternalClient::new(wrap_grpc_client_channel(
        InterceptedService::new(
            channel,
            SpaceBearerInterceptor::new(SpaceBearerToken::NodeApiInternal),
        ),
    )))
}

pub async fn connect_builder(endpoint: Endpoint) -> Result<BuilderClient, ControllerError> {
    let channel = node_api_connect_endpoint(endpoint)
        .await?
        .connect_with_retry(
            "builder-node-api",
            "builder-node-api",
            pplx_space_common_grpc::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
        )
        .await
        .map_err(|err| {
            ControllerError::service_unavailable(format!("node-api not reachable: {err}"))
        })?;
    Ok(BuilderApiClient::new(wrap_grpc_client_channel(
        InterceptedService::new(
            channel,
            SpaceBearerInterceptor::new(SpaceBearerToken::NodeApi),
        ),
    )))
}
