use std::path::{Path, PathBuf};

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

/// What a TCP connect attempt allows for name resolution, which runs before
/// the TCP dial and which no tonic timeout bounds.
const NAME_RESOLUTION_ALLOWANCE: std::time::Duration = std::time::Duration::from_secs(3);

/// The TCP dial bound [`connect_tcp_channel_with_retry`] applies to an
/// endpoint that sets no `connect_timeout`.
const DEFAULT_TCP_DIAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// How long one TCP connect attempt may run: [`NAME_RESOLUTION_ALLOWANCE`],
/// then its TCP dial, then the TLS handshake, if any, within its own bound.
/// An attempt whose lookup, dial and handshake each finish within their share
/// fits, so a handshake that reaches its TLS bound fails with that cause
/// before the attempt is abandoned. The HTTP/2 preface only writes to a fresh
/// socket and gets no share.
fn tcp_attempt_cap(
    dial_timeout: std::time::Duration,
    tls_handshake_timeout: Option<std::time::Duration>,
) -> std::time::Duration {
    NAME_RESOLUTION_ALLOWANCE + dial_timeout + tls_handshake_timeout.unwrap_or_default()
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
        let connect_span = connect_span(
            service_name,
            "Connect",
            "uds",
            &socket_path.display(),
            retry_index,
        );
        let remaining = retry_policy
            .max_elapsed
            .saturating_sub(started_at.elapsed());
        let result = tokio::time::timeout(remaining, connect().instrument(connect_span.clone()))
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
                record_connect_ok(&connect_span);
                return Ok(channel);
            }
            Err(error) => {
                record_connect_error(&connect_span, &error);
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
                pplx_space_metrics::grpc::client_retry(
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
/// tonic does not expose it, so the caller states it. Internal-RPC callers
/// must connect through `internal_transport::ConnectEndpoint::connect_with_retry`
/// instead, which passes the bound its transport applied. Each attempt (name
/// resolution, the TCP dial the endpoint's `connect_timeout` bounds, TLS and
/// the HTTP/2 preface) is abandoned after a 3s name-resolution allowance plus
/// the endpoint's `connect_timeout` plus `tls_handshake_timeout`, and redialed
/// while the policy's `max_elapsed` remains. An endpoint that sets no
/// `connect_timeout` is given 3s, since hyper-util leaves a dial unbounded
/// without one. An attempt whose lookup takes at most that allowance therefore
/// fails with the dial's or the handshake's own cause when either reaches its
/// bound. The budget is checked between attempts, so the call takes at
/// most `max_elapsed` plus one attempt cap. With
/// [`crate::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY`] and a 3s
/// `connect_timeout`: 31s with the 15s internal-transport handshake bound, 19s
/// with a 3s one, 16s for plaintext. The returned channel's later reconnects
/// are outside the cap and budget; only the dial bound and the TLS config's
/// own timeout bound them.
pub async fn connect_tcp_channel_with_retry(
    service_name: &'static str,
    target: impl AsRef<str>,
    endpoint: tonic::transport::Endpoint,
    tls_handshake_timeout: Option<std::time::Duration>,
    retry_policy: GrpcRetryPolicy,
) -> Result<tonic::transport::Channel, TcpConnectError> {
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
            async move { endpoint.connect().await.map_err(TcpConnectError::from) }
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
    mut attempt: F,
) -> Result<T, E>
where
    E: std::fmt::Display + From<TcpConnectError>,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let started_at = tokio::time::Instant::now();
    let mut retry_index = 0;
    loop {
        let connect_span = connect_span(service_name, "Connect", "tcp", &target, retry_index);
        let outcome = tokio::time::timeout(attempt_cap, attempt().instrument(connect_span.clone()))
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
                record_connect_ok(&connect_span);
                return Ok(connected);
            }
            Err(error) => error,
        };
        record_connect_error(&connect_span, &error);
        let elapsed = started_at.elapsed();
        let delay = retry_policy.backoff_delay(retry_index).filter(|delay| {
            elapsed < retry_policy.max_elapsed
                && *delay <= retry_policy.max_elapsed.saturating_sub(elapsed)
        });
        let Some(delay) = delay else {
            log_tcp_connect_exhausted(service_name, target, started_at, retry_index + 1, &error);
            return Err(error);
        };
        retry_index += 1;
        pplx_space_metrics::grpc::client_retry(service_name, "Connect", "connect", "connect_error");
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
/// waits for it, as a call on a freshly dialed channel would. Each TCP dial is
/// bounded by `attempt_timeout`, and each attempt, by a 3s name-resolution
/// allowance plus `attempt_timeout`. Only those attempts are retried: TLS,
/// when the endpoint configures it, is layered on top by the channel once one
/// succeeds, its handshake bounded by `tls_handshake_timeout`, which must be
/// the bound the endpoint's TLS config sets (`None` for plaintext).
/// Internal-RPC callers must connect through
/// `internal_transport::ConnectEndpoint::connect_with_retrying_redials`
/// instead, which passes the bound its transport applied. Each whole dial or
/// redial is bounded at [`retrying_redial_bound`].
pub async fn connect_tcp_channel_with_retrying_redials(
    service_name: &'static str,
    endpoint: tonic::transport::Endpoint,
    attempt_timeout: std::time::Duration,
    tls_handshake_timeout: Option<std::time::Duration>,
    retry_policy: GrpcRetryPolicy,
) -> Result<tonic::transport::Channel, tonic::transport::Error> {
    // The connector bounds each attempt; the endpoint's bound covers the
    // retries and the TLS handshake after them.
    let endpoint = endpoint.connect_timeout(retrying_redial_bound(
        retry_policy,
        attempt_timeout,
        tls_handshake_timeout,
    ));
    let mut http = hyper_util::client::legacy::connect::HttpConnector::new();
    http.enforce_http(false);
    http.set_nodelay(true);
    http.set_connect_timeout(Some(attempt_timeout));
    endpoint
        .connect_with_connector(RetryingTcpConnector {
            http,
            service_name,
            retry_policy,
            attempt_cap: tcp_attempt_cap(attempt_timeout, None),
        })
        .await
}

/// How long one whole dial or redial of a
/// [`connect_tcp_channel_with_retrying_redials`] channel may take: every
/// attempt the policy starts, the last one's cap, and the TLS handshake after
/// it. With [`crate::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY`] and a 3s
/// `attempt_timeout`: 31s with the 15s internal-transport handshake bound,
/// 16s for plaintext.
pub fn retrying_redial_bound(
    retry_policy: GrpcRetryPolicy,
    attempt_timeout: std::time::Duration,
    tls_handshake_timeout: Option<std::time::Duration>,
) -> std::time::Duration {
    retry_policy.max_elapsed
        + tcp_attempt_cap(attempt_timeout, None)
        + tls_handshake_timeout.unwrap_or_default()
}

#[derive(Clone)]
struct RetryingTcpConnector {
    http: hyper_util::client::legacy::connect::HttpConnector,
    service_name: &'static str,
    retry_policy: GrpcRetryPolicy,
    attempt_cap: std::time::Duration,
}

impl tower::Service<http::Uri> for RetryingTcpConnector {
    type Response =
        <hyper_util::client::legacy::connect::HttpConnector as tower::Service<http::Uri>>::Response;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.http.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        let Self {
            http,
            service_name,
            retry_policy,
            attempt_cap,
        } = self.clone();
        Box::pin(async move {
            let target = uri.to_string();
            retry_tcp_connect(service_name, &target, retry_policy, attempt_cap, || {
                let mut http = http.clone();
                let uri = uri.clone();
                async move { Ok(tower::Service::call(&mut http, uri).await?) }
            })
            .await
        })
    }
}

fn connect_span(
    service_name: &'static str,
    command: &'static str,
    transport: &'static str,
    target: &dyn std::fmt::Display,
    retry_index: usize,
) -> tracing::Span {
    pplx_space_observability::with_peer_service!(tracing::info_span!(
        "grpc.client.connect",
        otel.kind = "client",
        otel.name = "grpc.client.connect",
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        "operation.name" = "grpc.client.connect",
        peer.service = service_name,
        command,
        transport,
        target = %target,
        retry = retry_index,
    ))
}

fn record_connect_ok(span: &tracing::Span) {
    span.record("otel.status_code", "OK");
}

fn record_connect_error(span: &tracing::Span, error: &dyn std::fmt::Display) {
    let description = error.to_string();
    span.record("otel.status_code", "ERROR");
    span.record("otel.status_description", description.as_str());
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
