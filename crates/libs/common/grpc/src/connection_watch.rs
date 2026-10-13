//! Whether the connection a kept channel was dialed with is still open.
//!
//! A `tonic::transport::Channel` hides its connection: after the peer closes
//! it, the channel quietly redials on the next call. A cache that keeps
//! channels per peer instance needs the opposite: once the connection it
//! dialed is gone, the peer may be gone too, and the next caller should dial
//! afresh, so a failure to connect reaches the caller as it would without the
//! cache. [`ConnectionWatch`] reports that: it closes when the transport
//! stream of the channel's connection is dropped, which hyper does when the
//! peer closes or resets it, an I/O error ends it, or an HTTP/2 keepalive
//! times out. A holder can also close it on its own evidence, such as a
//! peer that reported it is draining.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Closed once the connection it watches ends, or [`Self::close`] is called.
/// Clones share one state, and a closed watch never reopens.
#[derive(Clone, Debug, Default)]
pub struct ConnectionWatch {
    closed: Arc<AtomicBool>,
}

impl ConnectionWatch {
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Mark the connection unusable for new callers. The connection itself
    /// stays up for whoever still holds its channel.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
}

/// A TCP stream that closes its watch when dropped.
pub(crate) struct WatchedStream<T> {
    inner: T,
    watch: ConnectionWatch,
}

impl<T> Drop for WatchedStream<T> {
    fn drop(&mut self) {
        self.watch.close();
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for WatchedStream<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for WatchedStream<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// The TCP connector tonic builds for an endpoint, whose streams close
/// `watch` when dropped. Every connection the channel opens shares the watch,
/// so it closes with the first one and stays closed.
#[derive(Clone)]
pub(crate) struct WatchedTcpConnector {
    http: HttpConnector,
    watch: ConnectionWatch,
}

impl WatchedTcpConnector {
    /// Dial like `endpoint.connect()` would: the endpoint's TCP options and
    /// `dial_timeout` for the TCP connect alone.
    pub(crate) fn new(
        endpoint: &tonic::transport::Endpoint,
        dial_timeout: std::time::Duration,
        watch: ConnectionWatch,
    ) -> Self {
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_nodelay(endpoint.get_tcp_nodelay());
        http.set_keepalive(endpoint.get_tcp_keepalive());
        http.set_keepalive_interval(endpoint.get_tcp_keepalive_interval());
        http.set_keepalive_retries(endpoint.get_tcp_keepalive_retries());
        http.set_connect_timeout(Some(dial_timeout));
        Self { http, watch }
    }
}

impl tower::Service<http::Uri> for WatchedTcpConnector {
    type Response = TokioIo<WatchedStream<tokio::net::TcpStream>>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.http.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        let connecting = self.http.call(uri);
        let watch = self.watch.clone();
        Box::pin(async move {
            let stream = connecting.await?.into_inner();
            Ok(TokioIo::new(WatchedStream {
                inner: stream,
                watch,
            }))
        })
    }
}
