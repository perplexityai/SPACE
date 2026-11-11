//! Preserve the error returned by HTTP setup or serving without interpreting opaque sources.
use super::super::tls::TlsTransportError;

#[derive(Debug, thiserror::Error)]
pub(in crate::tcp_proxy) enum HttpError {
    #[error("TLS transport failed: {0}")]
    Transport(#[from] TlsTransportError),
    #[error("HTTP operation failed")]
    Protocol(#[from] hyper::Error),
    #[error("HTTP server failed")]
    Server(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl HttpError {
    pub(in crate::tcp_proxy) fn as_str(&self) -> &'static str {
        match self {
            Self::Transport(error) => error.as_str(),
            Self::Protocol(_) | Self::Server(_) => "http_connection_error",
        }
    }
    pub(in crate::tcp_proxy) fn record(&self, stage: &'static str) {
        match self {
            Self::Transport(error) => error.record(stage),
            Self::Protocol(error) => tracing::info!(
                stage,
                http_reason = http_reason(error),
                "HTTP operation failed"
            ),
            Self::Server(_) => {
                tracing::info!(stage, reason = self.as_str(), "HTTP connection ended")
            }
        }
    }
}

/// Only Hyper's documented predicates are stable; native codes behind its
/// opaque source chain remain unavailable.
pub(in crate::tcp_proxy) fn http_reason(error: &hyper::Error) -> &'static str {
    if error.is_parse() {
        "parse"
    } else if error.is_timeout() {
        "timeout"
    } else if error.is_canceled() {
        "cancelled"
    } else if error.is_incomplete_message() {
        "incomplete_message"
    } else if error.is_closed() {
        "closed"
    } else if error.is_body_write_aborted() {
        "body_write_aborted"
    } else if error.is_shutdown() {
        "shutdown"
    } else if error.is_user() {
        "user"
    } else {
        "other"
    }
}
