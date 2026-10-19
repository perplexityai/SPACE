//! The request body of a forwarded client-streaming call that commits on EOF.
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::sync::mpsc;

enum Frame<T> {
    Item(T),
    End,
}

/// A body whose senders can end it only with [`UploadSender::finish`].
///
/// A receiving service that commits on EOF must not see a failed input as
/// complete. Closing an ordinary channel body ends it, and the transport sends
/// that end on its own task, so dropping the call afterwards can't take it
/// back. If every sender drops without finishing, this body stays pending
/// instead, so the only way the call ends is the reset of the dropped call.
pub fn upload_channel<T>(capacity: usize) -> (UploadSender<T>, UploadBody<T>) {
    let (tx, rx) = mpsc::channel(capacity);
    (UploadSender(tx), UploadBody { rx, ended: false })
}

/// The receiver end of the body is gone.
#[derive(Debug, thiserror::Error)]
#[error("upload body closed")]
pub struct UploadBodyClosed;

pub struct UploadSender<T>(mpsc::Sender<Frame<T>>);

impl<T> Clone for UploadSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> UploadSender<T> {
    pub async fn send(&self, item: T) -> Result<(), UploadBodyClosed> {
        self.0
            .send(Frame::Item(item))
            .await
            .map_err(|_| UploadBodyClosed)
    }

    /// Ends the body for every sender, once every item sent before it has
    /// been read. Items any clone sends afterwards are never read.
    pub async fn finish(self) -> Result<(), UploadBodyClosed> {
        self.0.send(Frame::End).await.map_err(|_| UploadBodyClosed)
    }
}

pub struct UploadBody<T> {
    rx: mpsc::Receiver<Frame<T>>,
    ended: bool,
}

impl<T> futures_core::Stream for UploadBody<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if self.ended {
            return Poll::Ready(None);
        }
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(Frame::Item(item))) => Poll::Ready(Some(item)),
            Poll::Ready(Some(Frame::End)) => {
                self.ended = true;
                Poll::Ready(None)
            }
            // Every sender dropped without finishing: nothing will wake this
            // again, and the call's reset is what ends it.
            Poll::Ready(None) | Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests;
