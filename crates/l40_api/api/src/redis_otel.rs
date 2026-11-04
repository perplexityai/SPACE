//! Shared OTel instrumentation for the api crate's Redis clients.
//!
//! Both Redis-backed limiters — the cluster activation token bucket
//! ([`crate::cluster_admission`]) and the per-client concurrency store
//! ([`crate::client_concurrency`]) — wrap every round trip in an identically
//! shaped `redis.client` span so command latency and fail-open errors surface
//! in Datadog consistently. Keeping the span/endpoint helpers here is the single
//! source of truth: a field change can't silently drift between the two clients.

use redis::{ConnectionAddr, IntoConnectionInfo as _};

/// A Redis server endpoint parsed from a connection URL, for the span's
/// `server.*` / `network.transport` attributes. Never carries credentials.
#[derive(Debug, Clone)]
pub(crate) struct RedisEndpoint {
    transport: &'static str,
    address: String,
    port: Option<u16>,
}

impl RedisEndpoint {
    pub(crate) fn from_url(url: &str) -> Result<Self, redis::RedisError> {
        let info = url.into_connection_info()?;
        Ok(match info.addr() {
            ConnectionAddr::Tcp(host, port) => Self {
                transport: "tcp",
                address: host.clone(),
                port: Some(*port),
            },
            ConnectionAddr::TcpTls { host, port, .. } => Self {
                transport: "tls",
                address: host.clone(),
                port: Some(*port),
            },
            ConnectionAddr::Unix(path) => Self {
                transport: "unix",
                address: path.display().to_string(),
                port: None,
            },
            _ => Self {
                transport: "unknown",
                address: "unknown".to_string(),
                port: None,
            },
        })
    }
}

/// Build the `redis.client` client span for one command against `endpoint`.
/// `otel.status_code` / `otel.status_description` start `Empty` and are filled
/// by [`record_redis_result`] on every exit path.
pub(crate) fn redis_client_span(command: &'static str, endpoint: &RedisEndpoint) -> tracing::Span {
    let otel_name = format!("redis {command}");
    tracing::info_span!(
        "redis.client",
        otel.kind = "client",
        otel.name = %otel_name,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        "operation.name" = "redis.client",
        db.system = "redis",
        db.operation = command,
        db.operation.name = command,
        server.address = %endpoint.address,
        server.port = endpoint.port.map(i64::from).unwrap_or_default(),
        network.transport = endpoint.transport,
    )
}

/// Record the terminal status on `span` for a completed Redis op. Call on every
/// exit path, including timeouts and fail-open errors. Callers with a non-`()`
/// success payload pass `result.as_ref().map(|_| ())`.
pub(crate) fn record_redis_result<E: std::fmt::Display>(
    span: &tracing::Span,
    result: Result<(), &E>,
) {
    match result {
        Ok(()) => {
            span.record("otel.status_code", "OK");
        }
        Err(error) => {
            span.record("otel.status_code", "ERROR");
            span.record("otel.status_description", error.to_string().as_str());
        }
    }
}
