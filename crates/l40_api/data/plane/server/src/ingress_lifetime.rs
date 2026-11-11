//! Downstream ownership is independent of the HTTP client's transport EOF.
use axum::body::Body;
use bytes::Bytes;
use hyper::body::{Body as HttpBody, Frame, SizeHint};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(super) struct Progress {
    last: Arc<Mutex<tokio::time::Instant>>,
    pub abort: CancellationToken,
}

impl Progress {
    pub fn new() -> Self {
        Self {
            last: Arc::new(Mutex::new(tokio::time::Instant::now())),
            abort: CancellationToken::new(),
        }
    }
    pub fn touch(&self) {
        *self
            .last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = tokio::time::Instant::now();
    }
    pub async fn expired(&self, timeout: Duration) {
        loop {
            let deadline = *self
                .last
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                + timeout;
            tokio::time::sleep_until(deadline).await;
            if tokio::time::Instant::now().duration_since(
                *self
                    .last
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            ) >= timeout
            {
                self.abort.cancel();
                return;
            }
        }
    }
}

struct CompletionInner(CancellationToken);
impl Drop for CompletionInner {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[derive(Clone)]
pub(super) struct Completion {
    _owner: Arc<CompletionInner>,
    pub progress: Progress,
}
impl Completion {
    pub fn new(done: CancellationToken, progress: Progress) -> Self {
        Self {
            _owner: Arc::new(CompletionInner(done)),
            progress,
        }
    }
    pub fn body(&self, body: Body) -> Body {
        Body::new(OwnedBody {
            body,
            owner: Some(self.clone()),
            aborted: Box::pin(self.progress.abort.clone().cancelled_owned()),
        })
    }
}

struct OwnedBody {
    body: Body,
    owner: Option<Completion>,
    aborted: Pin<Box<dyn Future<Output = ()> + Send>>,
}
impl HttpBody for OwnedBody {
    type Data = Bytes;
    type Error = std::io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        if self.aborted.as_mut().poll(cx).is_ready() {
            self.owner.take();
            return Poll::Ready(Some(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "ingress completion timed out",
            ))));
        }
        match Pin::new(&mut self.body).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if frame.data_ref().is_some_and(|data| !data.is_empty())
                    && let Some(owner) = &self.owner
                {
                    owner.progress.touch();
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.owner.take();
                Poll::Ready(Some(Err(std::io::Error::other(error))))
            }
            Poll::Ready(None) => {
                self.owner.take();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

pub(super) struct IngressIo {
    io: DuplexStream,
    owner: Option<Completion>,
    progress: Progress,
    track: bool,
}
impl IngressIo {
    pub fn new(io: DuplexStream, completion: Completion) -> Self {
        Self {
            io,
            progress: completion.progress.clone(),
            owner: Some(completion),
            track: true,
        }
    }
    /// HTTP ownership transfers to upload/body or upgrade task. The connection
    /// driver itself must not keep a finished keep-alive response protected.
    pub fn track_transport_progress(&mut self) {
        self.track = true;
    }
    pub fn take_completion(&mut self) -> Completion {
        self.track = false;
        self.owner.take().expect("ingress owner transferred once")
    }
}
impl AsyncRead for IngressIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.io).poll_read(cx, buf);
        if self.track && buf.filled().len() > before {
            self.progress.touch();
        }
        result
    }
}
impl AsyncWrite for IngressIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let result = Pin::new(&mut self.io).poll_write(cx, buf);
        if self.track && matches!(result, Poll::Ready(Ok(n)) if n > 0) {
            self.progress.touch();
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
