use std::future::Future;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Sleep;
use tonic::transport::server::Connected;

// HTTP/2 keepalive starts after the client preface. Without a separate
// deadline, an accepted peer that sends nothing can retain an admission slot
// forever, even when keepalive is configured on the server.
/// Bounds HTTP/2 client-preface arrival without timing established streams.
pub struct PrefaceDeadline<T> {
    inner: T,
    remaining: usize,
    deadline: Option<Pin<Box<Sleep>>>,
}

impl<T> PrefaceDeadline<T> {
    pub fn new(inner: T, timeout: Duration) -> Self {
        Self {
            inner,
            remaining: b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".len(),
            deadline: Some(Box::pin(tokio::time::sleep(timeout))),
        }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for PrefaceDeadline<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if let Some(deadline) = &mut this.deadline
            && deadline.as_mut().poll(cx).is_ready()
        {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "HTTP/2 client preface timed out",
            )));
        }
        let before = buf.filled().len();
        ready!(Pin::new(&mut this.inner).poll_read(cx, buf))?;
        this.remaining = this.remaining.saturating_sub(buf.filled().len() - before);
        if this.remaining == 0 {
            // Tonic validates the bytes; this wrapper only bounds their arrival.
            // Established streams are governed by HTTP/2 liveness, not age.
            this.deadline = None;
        }
        Poll::Ready(Ok(()))
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for PrefaceDeadline<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

impl<T: Connected> Connected for PrefaceDeadline<T> {
    type ConnectInfo = T::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.inner.connect_info()
    }
}

#[cfg(test)]
mod tests;
