use std::path::{Path, PathBuf};

use crate::connection_watch::{ConnectionWatch, WatchedTcpConnector};
use crate::policy::GrpcRetryPolicy;
use tracing::Instrument as _;

const LOCAL_ENDPOINT_URI: &str = "http://[::]:50051";

#[derive(Debug, thiserror::Error)]
pub enum UdsConnectError {
    #[error("failed to create gRPC endpoint: {0}")]
    Endpoint(#[source] tonic::transport::Error),
    #[error("failed to connect to {service_name} at {path}: {source}")]
    Transport {
        service_name: &'static str,
        path: PathBuf,
        #[source]
        source: tonic::transport::Error,
    },
    #[error("connecting to {service_name} at {path} exceeded the {budget:?} total connect budget")]
    Timeout {
        service_name: &'static str,
        path: PathBuf,
        budget: std::time::Duration,
    },
}

/// Why [`connect_tcp_channel_with_retry`] gave up.
#[derive(Debug, thiserror::Error)]
pub enum TcpConnectError {
    /// The endpoint was refused before any dial: see [`TcpEndpointError`].
    #[error(transparent)]
    Endpoint(#[from] TcpEndpointError),
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    /// The last attempt neither connected nor failed within its cap and was
    /// abandoned.
    #[error("connect attempt to {service_name} at {target} did not finish within {cap:?}")]
    AttemptTimeout {
        service_name: &'static str,
        target: String,
        cap: std::time::Duration,
    },
}

/// Marks a failure of SPACE's TCP connector: resolving the target's name or
/// dialing it failed, so no connection existed and no request bytes were sent.
/// The connector's own error is the [`source`](std::error::Error::source), so
/// a check for an [`std::io::Error`] kind still finds it, and the message is
/// the source's, as with tonic's own `ConnectError`. Only the connector
/// constructs it. TLS, when the endpoint configures it, runs on the connection
/// the dial returned, so its failures never carry this marker.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ConnectPhaseError(#[source] Box<dyn std::error::Error + Send + Sync>);

impl ConnectPhaseError {
    fn new(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self(source.into())
    }

    /// `error` marked, unless it already is.
    fn mark(error: Box<dyn std::error::Error + Send + Sync>) -> Self {
        match error.downcast::<Self>() {
            Ok(marked) => *marked,
            Err(error) => Self(error),
        }
    }
}

/// Why SPACE's TCP connector refused an endpoint before dialing it.
#[derive(Debug, thiserror::Error)]
pub enum TcpEndpointError {
    /// An `https` endpoint came without the bound its TLS config sets on the
    /// handshake. Tonic bounds a custom connector's whole connect, TLS
    /// included, so without it that bound would cut the handshake short.
    #[error("an https endpoint needs the TLS handshake bound its TLS config sets")]
    MissingTlsHandshakeBound,
    /// The endpoint names a Unix socket, which SPACE's TCP connector would
    /// try to resolve as a host name.
    #[error("a Unix-socket endpoint cannot be dialed over TCP; use connect_uds_channel")]
    UnixSocket,
}

/// Why [`connect_tcp_channel`], [`connect_tcp_channel_with_retrying_redials`]
/// or [`crate::RotatingChannel::connect`] failed.
#[derive(Debug, thiserror::Error)]
pub enum TcpChannelError {
    #[error(transparent)]
    Endpoint(#[from] TcpEndpointError),
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
}

/// What a TCP connect attempt allows for name resolution, which runs before
/// the TCP dial and which no tonic timeout bounds.
const NAME_RESOLUTION_ALLOWANCE: std::time::Duration = std::time::Duration::from_secs(3);

/// The TCP dial bound SPACE's TCP connector applies to an endpoint that sets
/// no `connect_timeout`, since hyper-util leaves a dial unbounded without one.
const DEFAULT_TCP_DIAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// How far past its name-resolution, dial and TLS bounds a connect attempt
/// runs before [`retry_tcp_connect`] abandons it, so a stall in any of them
/// fails the attempt with that bound's own cause first.
const ATTEMPT_CAP_MARGIN: std::time::Duration = std::time::Duration::from_millis(500);

/// How far past the attempt cap tonic's whole-connect bound fires. It is a
/// backstop the connector's, the TLS handshake's and the attempt's own
/// bounds always beat. Should it ever fire, its failure is a bare `TimedOut`
/// outside tonic's `ConnectError`, reported as `Unknown` and unmarked, so it
/// is conservatively not replay-safe.
const WHOLE_CONNECT_BACKSTOP_MARGIN: std::time::Duration = std::time::Duration::from_secs(1);

/// How long SPACE's TCP connector gives name resolution and the TCP dial
/// together before failing them with a [`ConnectPhaseError`].
fn connect_phase_bound(dial_timeout: std::time::Duration) -> std::time::Duration {
    NAME_RESOLUTION_ALLOWANCE + dial_timeout
}

/// How long one TCP connect attempt may run: the [`connect_phase_bound`],
/// then the TLS handshake, if any, within its own bound, then
/// [`ATTEMPT_CAP_MARGIN`]. A connect phase or handshake that reaches its
/// bound therefore fails with that cause before the attempt is abandoned.
/// The HTTP/2 preface only writes to a fresh socket and shares the margin.
fn tcp_attempt_cap(
    dial_timeout: std::time::Duration,
    tls_handshake_timeout: Option<std::time::Duration>,
) -> std::time::Duration {
    connect_phase_bound(dial_timeout)
        + tls_handshake_timeout.unwrap_or_default()
        + ATTEMPT_CAP_MARGIN
}

/// Tonic's whole-connect bound for SPACE's TCP connector: the connect phase,
/// the TLS handshake and [`WHOLE_CONNECT_BACKSTOP_MARGIN`], which outlasts
/// the attempt cap's margin.
fn whole_connect_backstop(
    dial_timeout: std::time::Duration,
    tls_handshake_timeout: Option<std::time::Duration>,
) -> std::time::Duration {
    connect_phase_bound(dial_timeout)
        + tls_handshake_timeout.unwrap_or_default()
        + WHOLE_CONNECT_BACKSTOP_MARGIN
}

/// The URI tonic reports for a Unix-socket endpoint. Tonic has no accessor
/// that says an endpoint is a Unix socket, so this fallback, which no TCP
/// endpoint SPACE dials uses, is how [`check_tcp_endpoint`] recognizes one.
const TONIC_UDS_FALLBACK_URI: &str = "http://tonic";

pub(crate) fn is_unix_socket_endpoint(endpoint: &tonic::transport::Endpoint) -> bool {
    *endpoint.uri() == http::Uri::from_static(TONIC_UDS_FALLBACK_URI)
}

/// Refuse an endpoint SPACE's TCP connector would dial wrongly.
pub(crate) fn check_tcp_endpoint(
    endpoint: &tonic::transport::Endpoint,
    tls_handshake_timeout: Option<std::time::Duration>,
) -> Result<(), TcpEndpointError> {
    if is_unix_socket_endpoint(endpoint) {
        return Err(TcpEndpointError::UnixSocket);
    }
    if endpoint.uri().scheme_str() == Some("https") && tls_handshake_timeout.is_none() {
        return Err(TcpEndpointError::MissingTlsHandshakeBound);
    }
    Ok(())
}

/// Retry only initial Unix-socket channel establishment.
///
/// This returns a raw channel. Service crates should wrap it with their
/// standard gRPC client trace/auth layers before constructing a generated
/// client. Do not call this from a [`crate::GrpcClient`] connect closure or a
/// [`crate::retry_grpc_status_with_context`] operation unless a separate
/// connect-only retry budget is intentional.
/// The retry policy's `max_elapsed` bounds all initial connection attempts and
/// backoff, including a stalled attempt. It does not time out later RPCs.
pub async fn connect_uds_channel_with_retry(
    service_name: &'static str,
    socket_path: &Path,
    retry_policy: GrpcRetryPolicy,
) -> Result<tonic::transport::Channel, UdsConnectError> {
    let endpoint = tonic::transport::Endpoint::try_from(LOCAL_ENDPOINT_URI)
        .map_err(UdsConnectError::Endpoint)?;
    connect_uds_channel_with_endpoint_with_retry(service_name, socket_path, endpoint, retry_policy)
        .await
}

pub async fn connect_uds_channel_with_endpoint_with_retry(
    service_name: &'static str,
    socket_path: &Path,
    endpoint: tonic::transport::Endpoint,
    retry_policy: GrpcRetryPolicy,
) -> Result<tonic::transport::Channel, UdsConnectError> {
    retry_uds_connect(service_name, socket_path, retry_policy, || {
        connect_uds_channel_with_endpoint(service_name, socket_path, endpoint.clone())
    })
    .await
}

async fn retry_uds_connect<F, Fut>(
    service_name: &'static str,
    socket_path: &Path,
    retry_policy: GrpcRetryPolicy,
    mut connect: F,
) -> Result<tonic::transport::Channel, UdsConnectError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<tonic::transport::Channel, UdsConnectError>>,
{
    let started_at = tokio::time::Instant::now();
    let mut retry_index = 0;

    loop {
        let connect_span = ConnectSpan::start(
            service_name,
            "Connect",
            Some("uds"),
            Some(socket_path.display().to_string()),
            retry_index,
        );
        let remaining = retry_policy
            .max_elapsed
            .saturating_sub(started_at.elapsed());
        let result = tokio::time::timeout(remaining, connect().instrument(connect_span.span()))
            .await
            .unwrap_or_else(|_| {
                Err(UdsConnectError::Timeout {
                    service_name,
                    path: socket_path.to_path_buf(),
                    budget: retry_policy.max_elapsed,
                })
            });
        match result {
            Ok(channel) => {
                connect_span.record_ok();
                return Ok(channel);
            }
            Err(error) => {
                connect_span.record_error(&error);
                let Some(delay) = retry_policy.backoff_delay(retry_index) else {
                    log_uds_connect_exhausted(
                        service_name,
                        socket_path,
                        started_at,
                        retry_index + 1,
                        &error,
                    );
                    return Err(error);
                };
                let elapsed = started_at.elapsed();
                if elapsed >= retry_policy.max_elapsed
                    || delay > retry_policy.max_elapsed.saturating_sub(elapsed)
                {
                    log_uds_connect_exhausted(
                        service_name,
                        socket_path,
                        started_at,
                        retry_index + 1,
                        &error,
                    );
                    return Err(error);
                }
                retry_index += 1;
                pplx_space_grpc_metrics::client_retry(
                    service_name,
                    "Connect",
                    "connect",
                    "connect_error",
                );
                pplx_space_observability::with_peer_service!(tracing::debug!(
                    peer.service = service_name,
                    retry = retry_index,
                    retry_in = ?delay,
                    uds_path = %socket_path.display(),
                    "failed to connect to UDS gRPC service; will retry"
                ));
                tokio::time::sleep(delay).await;
            }
        }
    }
}

pub async fn connect_uds_channel(
    service_name: &'static str,
    socket_path: &Path,
) -> Result<tonic::transport::Channel, UdsConnectError> {
    let endpoint = tonic::transport::Endpoint::try_from(LOCAL_ENDPOINT_URI)
        .map_err(UdsConnectError::Endpoint)?;
    connect_uds_channel_with_endpoint(service_name, socket_path, endpoint).await
}

pub async fn connect_uds_channel_with_endpoint(
    service_name: &'static str,
    socket_path: &Path,
    endpoint: tonic::transport::Endpoint,
) -> Result<tonic::transport::Channel, UdsConnectError> {
    let path = socket_path.to_path_buf();
    endpoint
        .connect_with_connector(tower::service_fn(move |_| {
            let socket_path = path.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(&socket_path).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await
        .map_err(|source| UdsConnectError::Transport {
            service_name,
            path: socket_path.to_path_buf(),
            source,
        })
}

/// Build a channel that opens the Unix socket when an RPC needs it.
///
/// Tonic keeps reconnecting this channel after the server or socket is
/// replaced. Replay-safe RPCs should still use the bounded retry helper so a
/// request that observes the replacement gap can be attempted again.
pub fn connect_uds_channel_lazy(
    socket_path: &Path,
    endpoint: tonic::transport::Endpoint,
) -> tonic::transport::Channel {
    let path = socket_path.to_path_buf();
    endpoint.connect_with_connector_lazy(tower::service_fn(move |_| {
        let socket_path = path.clone();
        async move {
            let stream = tokio::net::UnixStream::connect(&socket_path).await?;
            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
        }
    }))
}

/// Retry only initial TCP channel establishment.
///
/// This returns a raw channel. Service crates should wrap it with their
/// standard gRPC client trace/auth layers before constructing a generated
/// client. Do not call this from a [`crate::GrpcClient`] connect closure or a
/// [`crate::retry_grpc_status_with_context`] operation unless a separate
/// connect-only retry budget is intentional.
///
/// `tls_handshake_timeout` is the bound the endpoint's TLS config sets on its
/// handshake (`ClientTlsConfig::timeout`), or `None` for a plaintext endpoint;
/// tonic does not expose it, so the caller states it, and an `https` endpoint
/// without it fails with [`TcpConnectError::Endpoint`] before any dial.
/// Internal-RPC callers must connect through
/// `internal_transport::ConnectEndpoint::connect_with_retry` instead, which
/// passes the bound its transport applied.
///
/// Each attempt dials through [`connect_tcp_channel`], so a stall reports its
/// own cause, in this order: name resolution and the TCP dial together fail at
/// a 3s allowance plus the endpoint's `connect_timeout` (3s when it sets none)
/// with a [`crate::ConnectPhaseError`]; the TLS handshake fails at its own
/// bound; the attempt is abandoned with [`TcpConnectError::AttemptTimeout`]
/// 0.5s after both; tonic's whole-connect backstop sits 0.5s later still.
/// Failed attempts are redialed while the policy's `max_elapsed` remains, and
/// the budget is checked between attempts, so the call takes at most
/// `max_elapsed` plus one attempt cap. With
/// [`crate::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY`] and a 3s
/// `connect_timeout`: 31.5s with the 15s internal-transport handshake bound,
/// 19.5s with a 3s one, 16.5s for plaintext. The returned channel's later
/// reconnects keep [`connect_tcp_channel`]'s bounds and are outside the retry
/// budget.
pub async fn connect_tcp_channel_with_retry(
    service_name: &'static str,
    target: impl AsRef<str>,
    endpoint: tonic::transport::Endpoint,
    tls_handshake_timeout: Option<std::time::Duration>,
    retry_policy: GrpcRetryPolicy,
) -> Result<tonic::transport::Channel, TcpConnectError> {
    connect_tcp_channel_with_retry_observed(
        service_name,
        target,
        endpoint,
        tls_handshake_timeout,
        retry_policy,
        |_| {},
    )
    .await
}

/// A failed TCP connect attempt that a retrying connect is about to redial.
#[non_exhaustive]
pub struct TcpConnectRetry<'a, E = TcpConnectError> {
    /// Attempts made so far, the failed one included.
    pub attempts: usize,
    /// Time since the first attempt started.
    pub elapsed: std::time::Duration,
    /// Why the attempt failed.
    pub error: &'a E,
}

/// [`connect_tcp_channel_with_retry`] that also hands `on_retry` each failed
/// attempt it is about to redial, so a caller with a long retry budget can
/// report progress while it waits. The final failure is returned, not
/// reported.
pub async fn connect_tcp_channel_with_retry_observed(
    service_name: &'static str,
    target: impl AsRef<str>,
    endpoint: tonic::transport::Endpoint,
    tls_handshake_timeout: Option<std::time::Duration>,
    retry_policy: GrpcRetryPolicy,
    on_retry: impl FnMut(&TcpConnectRetry<'_>),
) -> Result<tonic::transport::Channel, TcpConnectError> {
    check_tcp_endpoint(&endpoint, tls_handshake_timeout)?;
    let dial_timeout = endpoint
        .get_connect_timeout()
        .unwrap_or(DEFAULT_TCP_DIAL_TIMEOUT);
    let endpoint = endpoint.connect_timeout(dial_timeout);
    let attempt_cap = tcp_attempt_cap(dial_timeout, tls_handshake_timeout);
    retry_tcp_connect_observed(
        service_name,
        target.as_ref(),
        retry_policy,
        attempt_cap,
        || {
            let endpoint = endpoint.clone();
            async move {
                dial_tcp_channel(endpoint, tls_handshake_timeout)
                    .await
                    .map_err(TcpConnectError::from)
            }
        },
        on_retry,
    )
    .await
}

/// [`connect_tcp_channel_with_retry`] for a channel meant to be kept per peer
/// instance: the same endpoint checks, attempts, bounds, retries, spans and
/// metrics, and a [`ConnectionWatch`] that closes once the connection the
/// channel was dialed with ends. A cache that hands the channel out only while
/// the watch is open gives each caller either that live connection or a fresh
/// dial whose failure it sees, as it would without the cache. The channel
/// itself still redials after its connection drops, for callers already
/// holding it, through the same SPACE TCP connector, so a redial that never
/// connects fails its RPC with a [`ConnectPhaseError`] too.
///
/// Internal-RPC callers must connect through
/// `internal_transport::ConnectEndpoint::connect_with_retry_watched`.
pub async fn connect_tcp_channel_with_retry_watched(
    service_name: &'static str,
    target: impl AsRef<str>,
    endpoint: tonic::transport::Endpoint,
    tls_handshake_timeout: Option<std::time::Duration>,
    retry_policy: GrpcRetryPolicy,
) -> Result<(tonic::transport::Channel, ConnectionWatch), TcpConnectError> {
    check_tcp_endpoint(&endpoint, tls_handshake_timeout)?;
    let dial_timeout = endpoint
        .get_connect_timeout()
        .unwrap_or(DEFAULT_TCP_DIAL_TIMEOUT);
    let endpoint = endpoint.connect_timeout(dial_timeout);
    let attempt_cap = tcp_attempt_cap(dial_timeout, tls_handshake_timeout);
    retry_tcp_connect(
        service_name,
        target.as_ref(),
        retry_policy,
        attempt_cap,
        || {
            let endpoint = endpoint.clone();
            async move {
                dial_watched_tcp_channel(
                    endpoint,
                    tls_handshake_timeout,
                    hyper_util::client::legacy::connect::HttpConnector::new(),
                )
                .await
                .map_err(TcpConnectError::from)
            }
        },
    )
    .await
}

/// Run `attempt` until it succeeds or `retry_policy` is spent, with the span,
/// retry metric and logs every TCP connect shares. An attempt still running
/// after `attempt_cap` fails with [`TcpConnectError::AttemptTimeout`], counted
/// and retried like any other failure, so the whole call ends within
/// `max_elapsed` plus one `attempt_cap`.
async fn retry_tcp_connect<T, E, F, Fut>(
    service_name: &'static str,
    target: &str,
    retry_policy: GrpcRetryPolicy,
    attempt_cap: std::time::Duration,
    attempt: F,
) -> Result<T, E>
where
    E: std::fmt::Display + From<TcpConnectError>,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    retry_tcp_connect_observed(
        service_name,
        target,
        retry_policy,
        attempt_cap,
        attempt,
        |_| {},
    )
    .await
}

/// [`retry_tcp_connect`] that hands `on_retry` each failed attempt before its
/// redial.
pub(crate) async fn retry_tcp_connect_observed<T, E, F, Fut>(
    service_name: &'static str,
    target: &str,
    retry_policy: GrpcRetryPolicy,
    attempt_cap: std::time::Duration,
    mut attempt: F,
    mut on_retry: impl FnMut(&TcpConnectRetry<'_, E>),
) -> Result<T, E>
where
    E: std::fmt::Display + From<TcpConnectError>,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let started_at = tokio::time::Instant::now();
    let mut retry_index = 0;
    loop {
        let connect_span = ConnectSpan::start(
            service_name,
            "Connect",
            Some("tcp"),
            Some(target.to_owned()),
            retry_index,
        );
        let outcome = tokio::time::timeout(attempt_cap, attempt().instrument(connect_span.span()))
            .await
            .unwrap_or_else(|_| {
                Err(E::from(TcpConnectError::AttemptTimeout {
                    service_name,
                    target: target.to_owned(),
                    cap: attempt_cap,
                }))
            });
        let error = match outcome {
            Ok(connected) => {
                connect_span.record_ok();
                return Ok(connected);
            }
            Err(error) => error,
        };
        connect_span.record_error(&error);
        let elapsed = started_at.elapsed();
        let delay = retry_policy.backoff_delay(retry_index).filter(|delay| {
            elapsed < retry_policy.max_elapsed
                && *delay <= retry_policy.max_elapsed.saturating_sub(elapsed)
        });
        let Some(delay) = delay else {
            log_tcp_connect_exhausted(service_name, target, started_at, retry_index + 1, &error);
            return Err(error);
        };
        on_retry(&TcpConnectRetry {
            attempts: retry_index + 1,
            elapsed,
            error: &error,
        });
        retry_index += 1;
        pplx_space_grpc_metrics::client_retry(service_name, "Connect", "connect", "connect_error");
        pplx_space_observability::with_peer_service!(tracing::debug!(
            peer.service = service_name,
            target,
            retry = retry_index,
            retry_in = ?delay,
            "failed to connect to TCP gRPC service; will retry"
        ));
        tokio::time::sleep(delay).await;
    }
}

/// Connect a TCP channel meant to be kept and shared. Like
/// [`connect_tcp_channel_with_retry`] it retries the first dial, and the
/// channel retries every later redial, after a connection drops, with the same
/// policy, spans and metrics: a call that arrives while the peer restarts
/// waits for it, as a call on a freshly dialed channel would. Each attempt
/// resolves and dials through SPACE's TCP connector, which fails it at a 3s
/// name-resolution allowance plus `attempt_timeout`, and is abandoned 0.5s
/// after that. Only those attempts are retried: TLS, when the endpoint
/// configures it, is layered on top by the channel once one succeeds, its
/// handshake bounded by `tls_handshake_timeout`, which must be the bound the
/// endpoint's TLS config sets (`None` for plaintext; an `https` endpoint
/// without it fails with [`TcpChannelError::Endpoint`]). Internal-RPC callers
/// must connect through
/// `internal_transport::ConnectEndpoint::connect_with_retrying_redials`
/// instead, which passes the bound its transport applied. Each whole dial or
/// redial is bounded at [`retrying_redial_bound`]. A dial or redial whose
/// attempts all fail, or are abandoned at their cap, fails with a
/// [`ConnectPhaseError`], as one through [`connect_tcp_channel`] does.
pub async fn connect_tcp_channel_with_retrying_redials(
    service_name: &'static str,
    endpoint: tonic::transport::Endpoint,
    attempt_timeout: std::time::Duration,
    tls_handshake_timeout: Option<std::time::Duration>,
    retry_policy: GrpcRetryPolicy,
) -> Result<tonic::transport::Channel, TcpChannelError> {
    check_tcp_endpoint(&endpoint, tls_handshake_timeout)?;
    Ok(connect_retrying_redials(
        service_name,
        endpoint,
        attempt_timeout,
        tls_handshake_timeout,
        retry_policy,
    )
    .await?)
}

/// [`connect_tcp_channel_with_retrying_redials`] for an endpoint already
/// known to be a TCP endpoint whose `https` scheme, if any, comes with its
/// handshake bound.
pub(crate) async fn connect_retrying_redials(
    service_name: &'static str,
    endpoint: tonic::transport::Endpoint,
    attempt_timeout: std::time::Duration,
    tls_handshake_timeout: Option<std::time::Duration>,
    retry_policy: GrpcRetryPolicy,
) -> Result<tonic::transport::Channel, tonic::transport::Error> {
    let connector = RetryingTcpConnector {
        tcp: TcpConnector::new(
            endpoint_http_connector(
                hyper_util::client::legacy::connect::HttpConnector::new(),
                &endpoint,
                attempt_timeout,
            ),
            attempt_timeout,
        ),
        service_name,
        retry_policy,
        attempt_cap: tcp_attempt_cap(attempt_timeout, None),
    };
    // The connector bounds each attempt; the endpoint's bound is the backstop
    // over the retries and the TLS handshake after them.
    endpoint
        .connect_timeout(retrying_redial_bound(
            retry_policy,
            attempt_timeout,
            tls_handshake_timeout,
        ))
        .connect_with_connector(connector)
        .await
}

/// How long one whole dial or redial of a
/// [`connect_tcp_channel_with_retrying_redials`] channel may take: every
/// attempt the policy starts, the last one's cap, the TLS handshake after it
/// and a 1s backstop margin. With
/// [`crate::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY`] and a 3s
/// `attempt_timeout`: 32.5s with the 15s internal-transport handshake bound,
/// 17.5s for plaintext.
pub fn retrying_redial_bound(
    retry_policy: GrpcRetryPolicy,
    attempt_timeout: std::time::Duration,
    tls_handshake_timeout: Option<std::time::Duration>,
) -> std::time::Duration {
    retry_policy.max_elapsed
        + tcp_attempt_cap(attempt_timeout, None)
        + tls_handshake_timeout.unwrap_or_default()
        + WHOLE_CONNECT_BACKSTOP_MARGIN
}

#[derive(Clone)]
struct RetryingTcpConnector {
    tcp: TcpConnector,
    service_name: &'static str,
    retry_policy: GrpcRetryPolicy,
    attempt_cap: std::time::Duration,
}

impl tower::Service<http::Uri> for RetryingTcpConnector {
    type Response = <TcpConnector as tower::Service<http::Uri>>::Response;
    type Error = ConnectPhaseError;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        tower::Service::poll_ready(&mut self.tcp, cx)
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        let Self {
            tcp,
            service_name,
            retry_policy,
            attempt_cap,
        } = self.clone();
        Box::pin(async move {
            let target = uri.to_string();
            retry_tcp_connect::<_, Box<dyn std::error::Error + Send + Sync>, _, _>(
                service_name,
                &target,
                retry_policy,
                attempt_cap,
                || {
                    let mut tcp = tcp.clone();
                    let uri = uri.clone();
                    async move { Ok(tower::Service::call(&mut tcp, uri).await?) }
                },
            )
            .await
            // An abandoned attempt never got past the lookup and dial either.
            .map_err(ConnectPhaseError::mark)
        })
    }
}

/// Connect a TCP channel through SPACE's TCP connector, which resolves and
/// dials the peer and fails with a [`ConnectPhaseError`] when either step
/// does, on the first dial and every later redial. An RPC that such a redial
/// fails therefore carries the marker, which
/// [`crate::is_client_connect_failure`] reads as proof that it was never sent.
/// Internal-RPC callers connect through
/// `internal_transport::ConnectEndpoint::connect` instead, which passes the
/// handshake bound its transport applied.
///
/// The endpoint's settings apply as [`tonic::transport::Endpoint::connect`]
/// applies them, except its local address, which tonic does not expose (no
/// SPACE endpoint sets one), and its `connect_timeout`, which bounds the TCP
/// dial and defaults to 3s when unset. A stall reports its own cause: name
/// resolution and the dial together fail at a 3s allowance plus the dial
/// bound with a [`ConnectPhaseError`], and the TLS handshake at
/// `tls_handshake_timeout`, which must be the bound the endpoint's TLS config
/// sets (`None` for plaintext). Tonic bounds a custom connector's whole
/// connect, TLS included, at the endpoint's `connect_timeout`, so the endpoint
/// is given both bounds plus a 1s margin instead, as a backstop that neither
/// cuts the handshake short nor fires before them. An `https` endpoint without
/// `tls_handshake_timeout`, or a Unix-socket endpoint, fails with
/// [`TcpChannelError::Endpoint`] before any dial.
pub async fn connect_tcp_channel(
    endpoint: tonic::transport::Endpoint,
    tls_handshake_timeout: Option<std::time::Duration>,
) -> Result<tonic::transport::Channel, TcpChannelError> {
    check_tcp_endpoint(&endpoint, tls_handshake_timeout)?;
    Ok(dial_tcp_channel(endpoint, tls_handshake_timeout).await?)
}

/// [`connect_tcp_channel`] for a channel that dials when an RPC first needs
/// it.
pub fn connect_tcp_channel_lazy(
    endpoint: tonic::transport::Endpoint,
    tls_handshake_timeout: Option<std::time::Duration>,
) -> Result<tonic::transport::Channel, TcpEndpointError> {
    check_tcp_endpoint(&endpoint, tls_handshake_timeout)?;
    Ok(lazy_tcp_channel(endpoint, tls_handshake_timeout))
}

/// [`connect_tcp_channel`] for an endpoint already known to be a TCP endpoint
/// whose `https` scheme, if any, comes with its handshake bound.
pub(crate) async fn dial_tcp_channel(
    endpoint: tonic::transport::Endpoint,
    tls_handshake_timeout: Option<std::time::Duration>,
) -> Result<tonic::transport::Channel, tonic::transport::Error> {
    let (endpoint, connector) = tcp_connector(
        endpoint,
        tls_handshake_timeout,
        hyper_util::client::legacy::connect::HttpConnector::new(),
    );
    endpoint.connect_with_connector(connector).await
}

/// [`dial_tcp_channel`] over `http` through SPACE's TCP connector wrapped in
/// a [`WatchedTcpConnector`], and the watch its connections close.
async fn dial_watched_tcp_channel<R>(
    endpoint: tonic::transport::Endpoint,
    tls_handshake_timeout: Option<std::time::Duration>,
    http: hyper_util::client::legacy::connect::HttpConnector<R>,
) -> Result<(tonic::transport::Channel, ConnectionWatch), tonic::transport::Error>
where
    R: tower::Service<
            hyper_util::client::legacy::connect::dns::Name,
            Response: Iterator<Item = std::net::SocketAddr>,
            Error: Into<Box<dyn std::error::Error + Send + Sync>>,
            Future: Send,
        > + Clone
        + Send
        + Sync
        + 'static,
{
    let watch = ConnectionWatch::default();
    let (endpoint, connector) = tcp_connector(endpoint, tls_handshake_timeout, http);
    let channel = endpoint
        .connect_with_connector(WatchedTcpConnector::new(connector, watch.clone()))
        .await?;
    Ok((channel, watch))
}

/// [`connect_tcp_channel_lazy`] for an endpoint already known to be a TCP
/// endpoint whose `https` scheme, if any, comes with its handshake bound.
pub(crate) fn lazy_tcp_channel(
    endpoint: tonic::transport::Endpoint,
    tls_handshake_timeout: Option<std::time::Duration>,
) -> tonic::transport::Channel {
    let (endpoint, connector) = tcp_connector(
        endpoint,
        tls_handshake_timeout,
        hyper_util::client::legacy::connect::HttpConnector::new(),
    );
    endpoint.connect_with_connector_lazy(connector)
}

/// SPACE's TCP connector over `http`, configured from `endpoint`, and the
/// endpoint to hand tonic with it: see [`connect_tcp_channel`].
fn tcp_connector<R>(
    endpoint: tonic::transport::Endpoint,
    tls_handshake_timeout: Option<std::time::Duration>,
    http: hyper_util::client::legacy::connect::HttpConnector<R>,
) -> (
    tonic::transport::Endpoint,
    TcpConnector<hyper_util::client::legacy::connect::HttpConnector<R>>,
) {
    let dial_timeout = endpoint
        .get_connect_timeout()
        .unwrap_or(DEFAULT_TCP_DIAL_TIMEOUT);
    let connector = TcpConnector::new(
        endpoint_http_connector(http, &endpoint, dial_timeout),
        dial_timeout,
    );
    let endpoint =
        endpoint.connect_timeout(whole_connect_backstop(dial_timeout, tls_handshake_timeout));
    (endpoint, connector)
}

/// `http` configured from `endpoint` as tonic's `Endpoint::connect`
/// configures its connector, with `dial_timeout` as the dial bound. Tonic has
/// no getter for an endpoint's local address, so it is not carried over: the
/// connector keeps the default, which lets the kernel choose. No SPACE
/// endpoint sets one.
fn endpoint_http_connector<R>(
    mut http: hyper_util::client::legacy::connect::HttpConnector<R>,
    endpoint: &tonic::transport::Endpoint,
    dial_timeout: std::time::Duration,
) -> hyper_util::client::legacy::connect::HttpConnector<R> {
    http.enforce_http(false);
    http.set_nodelay(endpoint.get_tcp_nodelay());
    http.set_keepalive(endpoint.get_tcp_keepalive());
    http.set_keepalive_interval(endpoint.get_tcp_keepalive_interval());
    http.set_keepalive_retries(endpoint.get_tcp_keepalive_retries());
    http.set_connect_timeout(Some(dial_timeout));
    http
}

/// SPACE's TCP connector: resolves and dials through `inner`, hyper-util's
/// `HttpConnector` outside tests, bounds the two together at
/// [`connect_phase_bound`], and marks every error with [`ConnectPhaseError`].
/// The endpoint's local address is not carried over: see
/// [`endpoint_http_connector`].
#[derive(Clone)]
struct TcpConnector<S = hyper_util::client::legacy::connect::HttpConnector> {
    inner: S,
    bound: std::time::Duration,
}

impl<S> TcpConnector<S> {
    fn new(inner: S, dial_timeout: std::time::Duration) -> Self {
        Self {
            inner,
            bound: connect_phase_bound(dial_timeout),
        }
    }
}

/// Name resolution and the TCP dial did not finish within the bound SPACE's
/// TCP connector gives them.
#[derive(Debug, thiserror::Error)]
#[error("name resolution and TCP dial did not finish within {bound:?}")]
struct ConnectPhaseTimeout {
    bound: std::time::Duration,
}

impl<S> tower::Service<http::Uri> for TcpConnector<S>
where
    S: tower::Service<http::Uri>,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = ConnectPhaseError;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        tower::Service::poll_ready(&mut self.inner, cx).map_err(ConnectPhaseError::new)
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        let bound = self.bound;
        let connecting = tokio::time::timeout(bound, tower::Service::call(&mut self.inner, uri));
        Box::pin(async move {
            match connecting.await {
                Ok(connected) => connected.map_err(ConnectPhaseError::new),
                Err(_) => Err(ConnectPhaseError::new(ConnectPhaseTimeout { bound })),
            }
        })
    }
}

/// One connect attempt's `grpc.client.connect` span.
///
/// An attempt made inside a traced span is that span's child. A background
/// dial, with no traced span current, records nothing when it connects, so a
/// reconnecting loop does not open a root trace per dial; when it fails, a
/// root span records the error.
pub(crate) struct ConnectSpan {
    service_name: &'static str,
    command: &'static str,
    transport: Option<&'static str>,
    target: Option<String>,
    retry_index: usize,
    span: tracing::Span,
}

impl ConnectSpan {
    pub(crate) fn start(
        service_name: &'static str,
        command: &'static str,
        transport: Option<&'static str>,
        target: Option<String>,
        retry_index: usize,
    ) -> Self {
        let mut connect = Self {
            service_name,
            command,
            transport,
            target,
            retry_index,
            span: tracing::Span::none(),
        };
        if pplx_space_observability::trace_propagation::has_traced_current_span() {
            connect.span = connect.open();
        }
        connect
    }

    fn open(&self) -> tracing::Span {
        pplx_space_observability::with_peer_service!(tracing::info_span!(
            "grpc.client.connect",
            otel.kind = "client",
            otel.name = "grpc.client.connect",
            otel.status_code = tracing::field::Empty,
            otel.status_description = tracing::field::Empty,
            "operation.name" = "grpc.client.connect",
            rpc.grpc.status_code = tracing::field::Empty,
            peer.service = self.service_name,
            command = self.command,
            transport = self.transport,
            target = self.target.as_deref(),
            retry = self.retry_index,
        ))
    }

    pub(crate) fn span(&self) -> tracing::Span {
        self.span.clone()
    }

    pub(crate) fn record_ok(&self) {
        self.span.record("otel.status_code", "OK");
    }

    pub(crate) fn record_error(&self, error: &dyn std::fmt::Display) {
        self.failed_span()
            .record("otel.status_description", error.to_string().as_str());
    }

    pub(crate) fn record_status(&self, status: &tonic::Status) {
        let span = self.failed_span();
        span.record("otel.status_description", status.message());
        span.record("rpc.grpc.status_code", status.code() as i32);
    }

    fn failed_span(&self) -> tracing::Span {
        let span = if self.span.is_none() {
            self.open()
        } else {
            self.span.clone()
        };
        span.record("otel.status_code", "ERROR");
        span
    }
}

fn log_uds_connect_exhausted(
    service_name: &'static str,
    socket_path: &Path,
    started_at: tokio::time::Instant,
    attempts: usize,
    error: &UdsConnectError,
) {
    pplx_space_observability::with_peer_service!(tracing::warn!(
        peer.service = service_name,
        endpoint = %socket_path.display(),
        elapsed_ms = started_at.elapsed().as_millis(),
        attempts,
        error = %error,
        "exhausted UDS gRPC connect retry budget"
    ));
}

fn log_tcp_connect_exhausted(
    service_name: &'static str,
    target: &str,
    started_at: tokio::time::Instant,
    attempts: usize,
    error: &dyn std::fmt::Display,
) {
    pplx_space_observability::with_peer_service!(tracing::warn!(
        peer.service = service_name,
        endpoint = target,
        elapsed_ms = started_at.elapsed().as_millis(),
        attempts,
        error = %error,
        "exhausted TCP gRPC connect retry budget"
    ));
}

#[cfg(test)]
mod tests;
