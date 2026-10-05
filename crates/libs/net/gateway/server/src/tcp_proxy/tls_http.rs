//! Streaming HTTP over inspected TLS. Hyper owns framing and flow control;
//! HTTP requests pass the authority gate; CONNECT preserves opaque forwarding.

use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use http::{Request, Response, StatusCode, Version, header};
use http_body_util::{Either, Empty};
use hyper::body::{Body as HttpBody, Bytes, Incoming};
use hyper::client::conn::{http1, http2};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Mutex, MutexGuard, OnceCell};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::Instrument;

use super::accounting::{Attempt, Outcome};
use super::http_host::HostBinding;
use super::routing_headers::{SanitizedBody, SanitizedRequest};
use super::tls::{HttpProtocol, TlsTransportError};
use crate::certificates::CertificateLease;

mod connect;
mod credentials;
mod forward;
mod leg;
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

enum Sender {
    Http1(Mutex<http1::SendRequest<SanitizedBody<Body>>>),
    Http2(http2::SendRequest<SanitizedBody<Body>>),
}

enum ReadySender<'a> {
    Http1(MutexGuard<'a, http1::SendRequest<SanitizedBody<Body>>>),
    Http2(http2::SendRequest<SanitizedBody<Body>>),
}

impl Sender {
    async fn ready(&self) -> Result<ReadySender<'_>, hyper::Error> {
        match self {
            Self::Http1(sender) => {
                let mut sender = sender.lock().await;
                sender.ready().await?;
                Ok(ReadySender::Http1(sender))
            }
            // Each HTTP/2 stream gets its own flow-control handle.
            Self::Http2(sender) => Ok(ReadySender::Http2(sender.clone())),
        }
    }
}

impl ReadySender<'_> {
    const fn is_http1(&self) -> bool {
        matches!(self, Self::Http1(_))
    }

    async fn send(
        self,
        request: SanitizedRequest<Body>,
    ) -> Result<Response<Incoming>, hyper::Error> {
        match self {
            Self::Http1(mut sender) => request.send_http1(&mut sender).await,
            Self::Http2(mut sender) => request.send_http2(&mut sender).await,
        }
    }
}

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
    connect: Mutex<Option<F>>,
    upstream: OnceCell<Sender>,
    tasks: TaskTracker,
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
) -> Result<(), TlsTransportError>
where
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: FnOnce(HttpProtocol) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(U, HttpProtocol), TlsTransportError>> + Send,
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let shutdown = CancellationToken::new();
    // Dropping this future on revocation also closes every origin socket.
    let _cancel = shutdown.clone().drop_guard();
    let session = Arc::new(Session {
        binding,
        lease,
        connect: Mutex::new(Some(connect)),
        upstream: OnceCell::new(),
        tasks: TaskTracker::new(),
        upgrades: TaskTracker::new(),
        shutdown,
        draining: CancellationToken::new(),
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
    session.tasks.close();
    session.tasks.wait().await;
    result.map_err(TlsTransportError::from)
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
    Fut: Future<Output = Result<(U, HttpProtocol), TlsTransportError>> + Send,
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    /// The origin leg is established lazily, by the first request that passes
    /// authorization. `guest_protocol` is that request's HTTP version so the
    /// upstream ALPN offer can match what the guest actually speaks.
    async fn sender(&self, guest_protocol: HttpProtocol) -> Result<&Sender, TlsTransportError> {
        self.upstream
            .get_or_try_init(|| async {
                // A failed or cancelled initializer has consumed the connector.
                // Drain this guest session so another stream cannot reuse it.
                let failed = self.draining.clone().drop_guard();
                let connect = self
                    .connect
                    .lock()
                    .await
                    .take()
                    .ok_or(TlsTransportError::Invalidated)?;
                let (io, protocol) = connect(guest_protocol).await?;
                let io = TokioIo::new(UpstreamIo {
                    inner: io,
                    _lifetime: self.draining.clone().drop_guard(),
                });
                let sender = match protocol {
                    HttpProtocol::Http1 => {
                        let (sender, connection) = http1::Builder::new()
                            .max_headers(MAX_HEADERS)
                            .handshake(io)
                            .await?;
                        self.drive(connection.with_upgrades());
                        Sender::Http1(Mutex::new(sender))
                    }
                    HttpProtocol::Http2 => {
                        let (sender, connection) = http2::Builder::new(TokioExecutor::new())
                            .max_header_list_size(MAX_HEADER_BYTES)
                            .max_send_buf_size(64 * 1024)
                            .handshake(io)
                            .await?;
                        self.drive(connection);
                        Sender::Http2(sender)
                    }
                };
                failed.disarm();
                Ok(sender)
            })
            .await
    }

    fn forwarding_body(
        protocol: HttpProtocol,
        body: Incoming,
        operation: &'static str,
    ) -> ForwardingBody {
        ForwardingBody::new(body, protocol.as_str(), operation)
    }

    fn drive(&self, connection: impl Future<Output = Result<(), hyper::Error>> + Send + 'static) {
        let shutdown = self.shutdown.clone();
        self.tasks.spawn(
            async move {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {},
                    result = connection => {
                        if let Err(error) = result {
                            // Hyper Display can contain peer-supplied text.
                            let failure = super::tls::HttpFailure::classify(&error);
                            tracing::info!(
                                stage = "upstream_http",
                                parse = error.is_parse(),
                                closed = error.is_closed(),
                                http_reason = failure.reason,
                                io_kind = ?failure.io_kind,
                                h2_reason = failure.h2_reason,
                                h2_origin = failure.h2_origin,
                                "inspected HTTP connection ended"
                            );
                        }
                    }
                }
            }
            .in_current_span(),
        );
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

/// An HTTP driver hands its socket to an upgrade without closing it. Drain the
/// guest only when that socket is dropped, preserving the handshake and relay.
struct UpstreamIo<T> {
    inner: T,
    _lifetime: tokio_util::sync::DropGuard,
}

impl<T: AsyncRead + Unpin> AsyncRead for UpstreamIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for UpstreamIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }
}

#[cfg(test)]
mod tests;
