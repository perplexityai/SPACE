//! A Unix listener served until shutdown, then closed.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use tokio::net::{UnixListener, UnixStream};
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

/// `listener`'s connections until `shutdown` fires, when the listener itself
/// is dropped. Serve it with `std::future::pending()` as the shutdown future:
/// the server stops accepting when this stream ends and still drains the
/// connections it accepted.
///
/// Tonic keeps the stream it serves until every accepted connection has
/// drained. A listener kept with it goes on completing connects into a
/// backlog nothing accepts, so a caller's call hangs until the drain ends and
/// then fails with the connection reset. Closed, it refuses them at once, and
/// a caller's retry reaches the successor as soon as one binds the socket.
pub fn unix_incoming_until_shutdown(
    listener: UnixListener,
    shutdown: CancellationToken,
) -> UnixIncomingUntilShutdown {
    UnixIncomingUntilShutdown {
        listener: Some(UnixListenerStream::new(listener)),
        shutdown: Box::pin(shutdown.cancelled_owned()),
    }
}

/// See [`unix_incoming_until_shutdown`].
pub struct UnixIncomingUntilShutdown {
    listener: Option<UnixListenerStream>,
    shutdown: Pin<Box<WaitForCancellationFutureOwned>>,
}

impl Stream for UnixIncomingUntilShutdown {
    type Item = io::Result<UnixStream>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Checked first, so a listener with a connect always ready cannot
        // outlive shutdown.
        if self.listener.is_some() && self.shutdown.as_mut().poll(cx).is_ready() {
            self.listener = None;
        }
        match self.listener.as_mut() {
            Some(listener) => Pin::new(listener).poll_next(cx),
            None => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod tests;
