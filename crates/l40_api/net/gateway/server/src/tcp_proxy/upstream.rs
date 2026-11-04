//! One lazy origin connection per downstream connection. Waiters never own setup.
use super::{
    routing_headers::{SanitizedBody, SanitizedRequest},
    tls::TlsTransportError,
    tls_http::{HttpProtocol, http_reason},
    transport,
};
use http::Response;
use hyper::{
    body::{Body as HttpBody, Bytes, Incoming},
    client::conn::{http1, http2},
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex as StdMutex},
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{Mutex, MutexGuard, watch},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::Instrument;

const MAX_HEADERS: usize = 128;
const MAX_HEADER_BYTES: u32 = 32 * 1024;

#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error("origin TCP connect failed: {0}")]
    Connect(#[from] transport::Error),
    #[error("origin TLS setup failed: {0}")]
    Tls(#[from] TlsTransportError),
    #[error("origin HTTP handshake failed")]
    Http(#[from] hyper::Error),
    #[error("upstream connection setup cancelled")]
    Cancelled,
}

impl Error {
    pub(super) fn as_str(&self) -> &'static str {
        match self {
            Self::Connect(_) => "upstream_connect_error",
            Self::Tls(error) => error.as_str(),
            Self::Http(_) => "http_connection_error",
            Self::Cancelled => "session_ended",
        }
    }
    pub(super) fn record(&self, stage: &'static str) {
        match self {
            Self::Tls(error) => error.record(stage),
            Self::Http(error) => tracing::info!(
                stage,
                http_reason = http_reason(error),
                "origin HTTP setup failed"
            ),
            Self::Connect(error) => {
                tracing::info!(stage, io_kind = ?error.io_kind(), "origin TCP setup failed")
            }
            Self::Cancelled => tracing::info!(stage, "origin setup cancelled"),
        }
    }
}

pub(super) enum Sender<B> {
    Http1(Mutex<http1::SendRequest<SanitizedBody<B>>>),
    Http2(http2::SendRequest<SanitizedBody<B>>),
}

pub(super) enum ReadySender<'a, B> {
    Http1(MutexGuard<'a, http1::SendRequest<SanitizedBody<B>>>),
    Http2(http2::SendRequest<SanitizedBody<B>>),
}

impl<B> Sender<B>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    pub(super) async fn ready(&self) -> Result<ReadySender<'_, B>, hyper::Error> {
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

impl<B> ReadySender<'_, B>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    pub(super) const fn is_http1(&self) -> bool {
        matches!(self, Self::Http1(_))
    }

    pub(super) async fn send(
        self,
        request: SanitizedRequest<B>,
    ) -> Result<Response<Incoming>, hyper::Error> {
        match self {
            Self::Http1(mut sender) => request.send_http1(&mut sender).await,
            Self::Http2(mut sender) => request.send_http2(&mut sender).await,
        }
    }
}

type SetupResult<B> = Result<Arc<Sender<B>>, Arc<Error>>;

pub(super) struct Upstream<F, B> {
    connect: StdMutex<Option<F>>,
    result: watch::Sender<Option<SetupResult<B>>>,
    tasks: TaskTracker,
    shutdown: CancellationToken,
    draining: CancellationToken,
}

impl<F, B> Upstream<F, B> {
    pub(super) fn new(
        connect: F,
        shutdown: CancellationToken,
        draining: CancellationToken,
    ) -> Self {
        Self {
            connect: StdMutex::new(Some(connect)),
            result: watch::channel(None).0,
            tasks: TaskTracker::new(),
            shutdown,
            draining,
        }
    }

    pub(super) async fn shutdown(&self) {
        self.shutdown.cancel();
        // Seal an unused connector before waiting: TaskTracker::close alone
        // still allows a late caller to register new work.
        if self
            .connect
            .lock()
            .expect("upstream connector lock poisoned")
            .take()
            .is_some()
        {
            self.result
                .send_replace(Some(Err(Arc::new(Error::Cancelled))));
        }
        self.tasks.close();
        self.tasks.wait().await;
    }
}

impl<F, Fut, U, B> Upstream<F, B>
where
    F: FnOnce(HttpProtocol) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(U, HttpProtocol), Error>> + Send + 'static,
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    pub(super) async fn sender(&self, protocol: HttpProtocol) -> SetupResult<B> {
        // No await between consuming the FnOnce and registering its task.
        if let Some(connect) = self
            .connect
            .lock()
            .expect("upstream connector lock poisoned")
            .take()
        {
            let result = self.result.clone();
            let tasks = self.tasks.clone();
            let shutdown = self.shutdown.clone();
            let draining = self.draining.clone();
            self.tasks.spawn(async move {
                let failed = draining.clone().drop_guard();
                let setup = tokio::select! {
                    biased;
                    () = shutdown.cancelled() => Err(Error::Cancelled),
                    setup = async {
                        let (io, protocol) = connect(protocol).await?;
                        let io = TokioIo::new(UpstreamIo { inner: io, _lifetime: draining.drop_guard() });
                        match protocol {
                            HttpProtocol::Http1 => {
                                let (sender, connection) = http1::Builder::new().max_headers(MAX_HEADERS).handshake(io).await?;
                                drive(&tasks, shutdown.clone(), connection.with_upgrades());
                                Ok(Sender::Http1(Mutex::new(sender)))
                            }
                            HttpProtocol::Http2 => {
                                let (sender, connection) = http2::Builder::new(TokioExecutor::new()).max_header_list_size(MAX_HEADER_BYTES).max_send_buf_size(64 * 1024).handshake(io).await?;
                                drive(&tasks, shutdown.clone(), connection);
                                Ok(Sender::Http2(sender))
                            }
                        }
                    } => setup,
                };
                let success = setup.is_ok();
                result.send_replace(Some(setup.map(Arc::new).map_err(Arc::new)));
                if success { failed.disarm(); }
            }.in_current_span());
        }
        let mut result = self.result.subscribe();
        loop {
            if let Some(result) = result.borrow_and_update().clone() {
                return result;
            }
            tokio::select! {
                biased;
                changed = result.changed() => changed.expect("upstream owns result publisher"),
                () = self.draining.cancelled() => return self.result.borrow().clone().unwrap_or_else(|| Err(Arc::new(Error::Cancelled))),
            }
        }
    }
}

fn drive(
    tasks: &TaskTracker,
    shutdown: CancellationToken,
    connection: impl Future<Output = Result<(), hyper::Error>> + Send + 'static,
) {
    tasks.spawn(async move {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {},
            result = connection => if let Err(error) = result {
                tracing::info!(stage = "upstream_http", http_reason = http_reason(&error), "origin HTTP connection ended");
            }
        }
    }.in_current_span());
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
