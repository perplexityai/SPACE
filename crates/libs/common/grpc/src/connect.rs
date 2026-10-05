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
pub async fn connect_tcp_channel_with_retry(
    service_name: &'static str,
    target: impl AsRef<str>,
    endpoint: tonic::transport::Endpoint,
    retry_policy: GrpcRetryPolicy,
) -> Result<tonic::transport::Channel, tonic::transport::Error> {
    retry_tcp_connect(service_name, target.as_ref(), retry_policy, || {
        let endpoint = endpoint.clone();
        async move { endpoint.connect().await }
    })
    .await
}

/// Run `attempt` until it succeeds or `retry_policy` is spent, with the span,
/// retry metric and logs every TCP connect shares.
async fn retry_tcp_connect<T, E, F, Fut>(
    service_name: &'static str,
    target: &str,
    retry_policy: GrpcRetryPolicy,
    mut attempt: F,
) -> Result<T, E>
where
    E: std::fmt::Display,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let started_at = tokio::time::Instant::now();
    let mut retry_index = 0;
    loop {
        let connect_span = connect_span(service_name, "Connect", "tcp", &target, retry_index);
        let error = match attempt().instrument(connect_span.clone()).await {
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
/// waits for it, as a call on a freshly dialed channel would. Each dial is
/// bounded by `attempt_timeout`. Only the TCP dial is retried; TLS, when the
/// endpoint configures it, is layered on top by the channel.
pub async fn connect_tcp_channel_with_retrying_redials(
    service_name: &'static str,
    endpoint: tonic::transport::Endpoint,
    attempt_timeout: std::time::Duration,
    retry_policy: GrpcRetryPolicy,
) -> Result<tonic::transport::Channel, tonic::transport::Error> {
    // The connector bounds each dial; the endpoint's bound covers the retries.
    let endpoint = endpoint.connect_timeout(retry_policy.max_elapsed + attempt_timeout);
    let mut http = hyper_util::client::legacy::connect::HttpConnector::new();
    http.enforce_http(false);
    http.set_nodelay(true);
    http.set_connect_timeout(Some(attempt_timeout));
    endpoint
        .connect_with_connector(RetryingTcpConnector {
            http,
            service_name,
            retry_policy,
        })
        .await
}

#[derive(Clone)]
struct RetryingTcpConnector {
    http: hyper_util::client::legacy::connect::HttpConnector,
    service_name: &'static str,
    retry_policy: GrpcRetryPolicy,
}

impl tower::Service<http::Uri> for RetryingTcpConnector {
    type Response =
        <hyper_util::client::legacy::connect::HttpConnector as tower::Service<http::Uri>>::Response;
    type Error =
        <hyper_util::client::legacy::connect::HttpConnector as tower::Service<http::Uri>>::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.http.poll_ready(cx)
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        let Self {
            http,
            service_name,
            retry_policy,
        } = self.clone();
        Box::pin(async move {
            let target = uri.to_string();
            retry_tcp_connect(service_name, &target, retry_policy, || {
                let mut http = http.clone();
                let uri = uri.clone();
                async move { tower::Service::call(&mut http, uri).await }
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
