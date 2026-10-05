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
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Mutex, MutexGuard};
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
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_HEADERS: usize = 128;
const MAX_HEADER_BYTES: u32 = 32 * 1024;
const MAX_STREAMS: u32 = 32;
const MAX_RETIREMENTS: u32 = MAX_STREAMS;
const UPSTREAM_PREPARATION_TIMEOUT: Duration = Duration::from_secs(10);
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
            Self::Http2(sender) => {
                let mut sender = sender.clone();
                sender.ready().await?;
                Ok(ReadySender::Http2(sender))
            }
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
    protocol: HttpProtocol,
    binding: HostBinding,
    lease: Arc<CertificateLease>,
    upstream: Mutex<Upstream<F>>,
    tasks: TaskTracker,
    upgrades: TaskTracker,
    shutdown: CancellationToken,
    draining: CancellationToken,
}

struct Upstream<F> {
    connect: F,
    current: Option<Arc<Generation>>,
}

struct Generation {
    sender: Sender,
    retired: CancellationToken,
}

pub(super) async fn serve<C, F, Fut, U>(
    guest: C,
    protocol: HttpProtocol,
    binding: HostBinding,
    lease: Arc<CertificateLease>,
    connect: F,
) -> Result<(), TlsTransportError>
where
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(U, HttpProtocol), TlsTransportError>> + Send,
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let shutdown = CancellationToken::new();
    // Dropping this future on revocation also closes every origin socket.
    let _cancel = shutdown.clone().drop_guard();
    let session = Arc::new(Session {
        protocol,
        binding,
        lease,
        upstream: Mutex::new(Upstream {
            connect,
            current: None,
        }),
        tasks: TaskTracker::new(),
        upgrades: TaskTracker::new(),
        shutdown,
        draining: CancellationToken::new(),
    });
    let service = service_fn(|request: Request<Incoming>| {
        let session = session.clone();
        let request = SanitizedRequest::try_from(request);
        async move {
            Ok::<_, Infallible>(match request {
                Ok(request) => session.forward(request).await,
                Err(_) => session.refuse("invalid_headers", StatusCode::BAD_REQUEST),
            })
        }
    });
    let result = match protocol {
        HttpProtocol::Http1 => {
            let connection = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(HEADER_TIMEOUT)
                .max_headers(MAX_HEADERS)
                .max_buf_size(MAX_HEADER_BYTES as usize)
                .serve_connection(TokioIo::new(guest), service)
                .with_upgrades();
            tokio::pin!(connection);
            tokio::select! {
                result = &mut connection => result,
                () = session.draining.cancelled() => {
                    connection.as_mut().graceful_shutdown();
                    connection.await
                }
            }
        }
        HttpProtocol::Http2 => {
            let connection = guest_http2_builder().serve_connection(TokioIo::new(guest), service);
            tokio::pin!(connection);
            tokio::select! {
                result = &mut connection => result,
                () = session.draining.cancelled() => {
                    connection.as_mut().graceful_shutdown();
                    connection.await
                }
            }
        }
    };
    session.upgrades.close();
    session.upgrades.wait().await;
    session.shutdown.cancel();
    session.tasks.close();
    session.tasks.wait().await;
    result.map_err(Into::into)
}

fn guest_http2_builder() -> hyper::server::conn::http2::Builder<TokioExecutor> {
    let mut builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
    builder
        .timer(TokioTimer::new())
        .max_concurrent_streams(MAX_STREAMS)
        .max_header_list_size(MAX_HEADER_BYTES)
        .max_send_buf_size(64 * 1024)
        .initial_stream_window_size(GUEST_H2_STREAM_WINDOW)
        .initial_connection_window_size(GUEST_H2_CONNECTION_WINDOW)
        .keep_alive_interval(Some(Duration::from_secs(30)))
        .keep_alive_timeout(HEADER_TIMEOUT);
    builder
}

impl<F, Fut, U> Session<F>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(U, HttpProtocol), TlsTransportError>> + Send,
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    async fn sender(&self) -> Result<Arc<Generation>, TlsTransportError> {
        let mut upstream = self.upstream.lock().await;
        if self.draining.is_cancelled() || !self.lease.is_usable() {
            return Err(TlsTransportError::Invalidated);
        }
        if let Some(current) = &upstream.current
            && !current.retired.is_cancelled()
        {
            return Ok(current.clone());
        }
        let failed = self.draining.clone().drop_guard();
        let (io, protocol) = (upstream.connect)().await?;
        let retired = CancellationToken::new();
        let io = TokioIo::new(UpstreamIo {
            inner: io,
            _lifetime: retired.clone().drop_guard(),
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
        let generation = Arc::new(Generation { sender, retired });
        upstream.current = Some(generation.clone());
        failed.disarm();
        Ok(generation)
    }

    fn forwarding_body(&self, body: Incoming, operation: &'static str) -> ForwardingBody {
        ForwardingBody::new(body, self.protocol.as_str(), operation)
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

    fn refuse(&self, reason: &'static str, status: StatusCode) -> Response<Body> {
        self.record(reason, status);
        let mut response = Response::new(Either::Right(Empty::new()));
        *response.status_mut() = status;
        if self.protocol == HttpProtocol::Http1 {
            response
                .headers_mut()
                .insert(header::CONNECTION, http::HeaderValue::from_static("close"));
        }
        response
    }

    fn record(&self, outcome: &'static str, status: StatusCode) {
        pplx_space_metrics::gateway::domain_binding("tcp", "tls_http", outcome, None);
        tracing::debug!(
            protocol = self.protocol.as_str(),
            outcome,
            status = status.as_u16(),
            "inspected HTTP request"
        );
        if !matches!(outcome, "allowed" | "websocket" | "connect_forwarded") {
            tracing::info!(
                protocol = self.protocol.as_str(),
                outcome,
                status = status.as_u16(),
                "inspected HTTP request refused"
            );
        }
    }
}

/// Retirement belongs to this socket's generation, including after an upgrade.
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
