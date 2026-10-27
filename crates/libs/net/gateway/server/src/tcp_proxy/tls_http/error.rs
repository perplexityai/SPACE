//! Preserve the error returned by HTTP setup or serving without interpreting opaque sources.
use super::super::tls::TlsTransportError;
use super::super::{Fault, event_at};

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
    /// The guest-facing HTTP server fails only through the guest, except
    /// when a response body streamed from the destination fails: Hyper calls
    /// that a user error, and this server's only other user code, its
    /// service, cannot fail.
    pub(in crate::tcp_proxy) fn fault(&self) -> Fault {
        match self {
            Self::Transport(error) => error.fault(),
            // Only origin requests and their senders return a bare Hyper error.
            Self::Protocol(_) => Fault::OriginHttp,
            Self::Server(error) => {
                if error
                    .downcast_ref::<hyper::Error>()
                    .is_some_and(hyper::Error::is_user)
                {
                    Fault::OriginHttp
                } else {
                    Fault::GuestHttp
                }
            }
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
            Self::Server(error) => {
                let fault = self.fault();
                event_at!(
                    fault.severity(),
                    stage,
                    reason = self.as_str(),
                    fault = fault.as_str(),
                    http_reason = error.downcast_ref::<hyper::Error>().map(http_reason),
                    "HTTP connection ended"
                );
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
