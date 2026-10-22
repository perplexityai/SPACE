//! Streaming HTTP over inspected TLS. Hyper owns framing and flow control;
//! HTTP requests pass the authority gate; CONNECT preserves opaque forwarding.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use http::{Request, Response, StatusCode, Version, header};
use http_body_util::{Either, Empty};
use hyper::body::{Body as HttpBody, Bytes, Incoming};
#[cfg(test)]
use hyper::client::conn::{http1, http2};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::Instrument;

use super::accounting::{Attempt, Outcome};
use super::http_host::HostBinding;
use super::routing_headers::SanitizedRequest;
#[cfg(test)]
use super::tls::TlsTransportError;
use super::upstream::{self, Upstream};
use crate::certificates::CertificateLease;
use crate::tls_sni::HttpAlpn;
pub(super) use error::HttpError;
pub(super) use error::http_reason;

mod connect;
mod credentials;
mod dispatch;
mod error;
mod forward;
mod upgrade;

type Body = Either<ForwardingBody, Empty<Bytes>>;
const HEADER_TIMEOUT: Duration = Duration::from_secs(30);
const H2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);
const H2_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_HEADERS: usize = 128;
const MAX_HEADER_BYTES: u32 = 32 * 1024;
const MAX_STREAMS: u32 = 32;
// Guest-facing HTTP/2 receive windows. h2 (>= 0.4.16) closes the connection
// with GOAWAY ENHANCE_YOUR_CALM once DATA frames under 256 bytes, buffered but
// not yet read, exceed a budget. Each such frame costs `256 - len`, and the
// default budget is half the connection window. With hyper's defaults (1 MiB
// stream and connection windows, so a 512 KiB budget), one request streaming
// ~50-byte frames exhausts the budget after ~125 KiB sits unread, e.g. git's
// pkt-line bodies while the upstream applies backpressure.
//
// The budget follows only the connection window, while a stream's window caps
// what that stream can buffer. A 128 KiB stream window under a 4 MiB
// connection window (2 MiB budget) lets a few streams fill their whole window
// with 50-byte frames (4.12 budget bytes per payload byte). Floods of tinier
// frames still exhaust it. The guest link is local, so the smaller stream
// window doesn't limit upload throughput.
const GUEST_H2_STREAM_WINDOW: u32 = 128 * 1024;
const GUEST_H2_CONNECTION_WINDOW: u32 = 4 * 1024 * 1024;

struct ForwardingBody {
    inner: Incoming,
    attempt: Attempt,
}

impl ForwardingBody {
    fn new(inner: Incoming, protocol: &'static str, operation: &'static str) -> Self {
        let mut attempt = Attempt::forwarding(protocol, operation);
        if inner.is_end_stream() {
            attempt.finish(Outcome::Success, "body_complete");
        }
        Self { inner, attempt }
    }
}

impl HttpBody for ForwardingBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(context);
        match &result {
            Poll::Ready(Some(Err(_))) => self.attempt.finish(Outcome::Error, "body_error"),
            Poll::Ready(None) => self.attempt.finish(Outcome::Success, "body_complete"),
            Poll::Ready(Some(Ok(_))) if self.inner.is_end_stream() => {
                self.attempt.finish(Outcome::Success, "body_complete");
            }
            Poll::Ready(Some(Ok(_))) | Poll::Pending => {}
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

struct Session<F> {
    binding: HostBinding,
    lease: Arc<CertificateLease>,
    upstream: Upstream<F, Body>,
    upgrades: TaskTracker,
    shutdown: CancellationToken,
    draining: CancellationToken,
}

/// Serve inspected HTTP on an established guest TLS stream.
///
/// The HTTP version is detected from the guest's first bytes: an HTTP/2
/// connection preface selects hyper's h2 server, anything else selects h1.
/// Go's `net/http` offers `h2` to an `https://` forward proxy and then writes
/// an HTTP/1.1 `CONNECT`; parsing that as h2 would answer the CONNECT with a
/// SETTINGS frame and a `PROTOCOL_ERROR` GOAWAY.
///
/// Protocol detection has no timeout; hyper's timers apply after it completes.
/// `connect` lazily opens the origin with the detected guest protocol.
pub(super) async fn serve<C, F, Fut, U>(
    guest: C,
    binding: HostBinding,
    lease: Arc<CertificateLease>,
    connect: F,
) -> Result<(), HttpError>
where
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: FnOnce(HttpProtocol) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(U, HttpProtocol), upstream::Error>> + Send + 'static,
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let shutdown = CancellationToken::new();
    // Dropping this future on revocation also closes every origin socket.
    let _cancel = shutdown.clone().drop_guard();
    let draining = CancellationToken::new();
    let session = Arc::new(Session {
        binding,
        lease,
        upstream: Upstream::new(connect, shutdown.clone(), draining.clone()),
        upgrades: TaskTracker::new(),
        shutdown,
        draining,
    });
    let service = service_fn(|request: Request<Incoming>| {
        let session = session.clone();
        let protocol = HttpProtocol::of(request.version());
        tracing::Span::current().record("protocol", protocol.as_str());
        let request = SanitizedRequest::try_from(request);
        async move {
            Ok::<_, Infallible>(match request {
                Ok(request) => session.forward(request).await,
                Err(_) => session.refuse(protocol, "invalid_headers", StatusCode::BAD_REQUEST),
            })
        }
    });
    let builder = guest_http_builder();
    let connection = builder.serve_connection_with_upgrades(TokioIo::new(guest), service);
    tokio::pin!(connection);
    let result = tokio::select! {
        result = &mut connection => result,
        () = session.draining.cancelled() => {
            connection.as_mut().graceful_shutdown();
            connection.await
        }
    };
    session.upgrades.close();
    session.upgrades.wait().await;
    session.shutdown.cancel();
    session.upstream.shutdown().await;
    result.map_err(HttpError::from)
}

fn guest_http_builder() -> auto::Builder<TokioExecutor> {
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_TIMEOUT)
        .max_headers(MAX_HEADERS)
        .max_buf_size(MAX_HEADER_BYTES as usize);
    builder
        .http2()
        .timer(TokioTimer::new())
        .max_concurrent_streams(MAX_STREAMS)
        .max_header_list_size(MAX_HEADER_BYTES)
        .max_send_buf_size(64 * 1024)
        .initial_stream_window_size(GUEST_H2_STREAM_WINDOW)
        .initial_connection_window_size(GUEST_H2_CONNECTION_WINDOW)
        .keep_alive_interval(Some(H2_KEEP_ALIVE_INTERVAL))
        .keep_alive_timeout(H2_KEEP_ALIVE_TIMEOUT);
    builder
}

impl<F, Fut, U> Session<F>
where
    F: FnOnce(HttpProtocol) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(U, HttpProtocol), upstream::Error>> + Send + 'static,
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn forwarding_body(
        protocol: HttpProtocol,
        body: Incoming,
        operation: &'static str,
    ) -> ForwardingBody {
        ForwardingBody::new(body, protocol.as_str(), operation)
    }

    fn refuse(
        &self,
        protocol: HttpProtocol,
        reason: &'static str,
        status: StatusCode,
    ) -> Response<Body> {
        Self::record(protocol, reason, status);
        let mut response = Response::new(Either::Right(Empty::new()));
        *response.status_mut() = status;
        if protocol == HttpProtocol::Http1 {
            response
                .headers_mut()
                .insert(header::CONNECTION, http::HeaderValue::from_static("close"));
        }
        response
    }

    fn record(protocol: HttpProtocol, outcome: &'static str, status: StatusCode) {
        pplx_space_metrics::gateway::domain_binding("tcp", "tls_http", outcome, None);
        tracing::debug!(
            protocol = protocol.as_str(),
            outcome,
            status = status.as_u16(),
            "inspected HTTP request"
        );
        if !matches!(outcome, "allowed" | "websocket" | "connect_forwarded") {
            tracing::info!(
                protocol = protocol.as_str(),
                outcome,
                status = status.as_u16(),
                "inspected HTTP request refused"
            );
        }
    }
}

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HttpProtocol {
    Http1,
    Http2,
}

impl HttpProtocol {
    /// The protocol hyper actually parsed a request with. This, not the ALPN
    /// result, decides how the request is forwarded.
    pub(super) fn of(version: http::Version) -> Self {
        if version == http::Version::HTTP_2 {
            Self::Http2
        } else {
            Self::Http1
        }
    }

    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Http1 => "http1",
            Self::Http2 => "http2",
        }
    }
}

impl From<HttpAlpn> for HttpProtocol {
    fn from(protocol: HttpAlpn) -> Self {
        match protocol {
            HttpAlpn::Http1 => Self::Http1,
            HttpAlpn::Http2 => Self::Http2,
        }
    }
}
