//! Bound sockets negotiating TLS before they reach RPC admission.

use futures_core::Stream;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::{CancellationToken, PollSemaphore, WaitForCancellationFutureOwned};
use tonic::transport::server::{Connected, TcpConnectInfo};

const MAX_PENDING_HANDSHAKES: usize = 64;

/// Incoming sockets with a per-listener budget for unfinished TLS handshakes.
/// Established connections do not retain a slot. Shutdown drops the listening
/// stream even when every slot is occupied, allowing a replacement to bind.
pub struct Incoming<S> {
    incoming: Option<Pin<Box<S>>>,
    slots: PollSemaphore,
    permit: Option<OwnedSemaphorePermit>,
    shutdown: Pin<Box<WaitForCancellationFutureOwned>>,
}

impl<S> Incoming<S> {
    pub fn new(incoming: S, shutdown: CancellationToken) -> Self {
        Self::with_capacity(incoming, shutdown, MAX_PENDING_HANDSHAKES)
    }

    pub(super) fn with_capacity(incoming: S, shutdown: CancellationToken, capacity: usize) -> Self {
        Self {
            incoming: Some(Box::pin(incoming)),
            slots: PollSemaphore::new(Arc::new(Semaphore::new(capacity))),
            permit: None,
            shutdown: Box::pin(shutdown.cancelled_owned()),
        }
    }
}

impl<S> Stream for Incoming<S>
where
    S: Stream<Item = io::Result<TcpStream>>,
{
    type Item = io::Result<Connection>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Check cancellation before waiting for capacity: saturated handshakes
        // must not keep an old listening socket in an SO_REUSEPORT group.
        if self.shutdown.as_mut().poll(cx).is_ready() {
            self.incoming = None;
            self.permit = None;
            return Poll::Ready(None);
        }
        if self.incoming.is_none() {
            return Poll::Ready(None);
        }
        if self.permit.is_none() {
            let Some(permit) = ready!(self.slots.poll_acquire(cx)) else {
                self.incoming = None;
                return Poll::Ready(None);
            };
            self.permit = Some(permit);
        }
        match ready!(
            self.incoming
                .as_mut()
                .expect("incoming was checked")
                .as_mut()
                .poll_next(cx)
        ) {
            Some(Ok(socket)) => Poll::Ready(Some(Ok(Connection {
                socket,
                handshake: Mutex::new(self.permit.take()),
            }))),
            Some(Err(error)) => {
                self.permit = None;
                Poll::Ready(Some(Err(error)))
            }
            None => {
                self.incoming = None;
                self.permit = None;
                Poll::Ready(None)
            }
        }
    }
}

/// The owned permit also releases capacity on handshake failure or cancellation.
pub struct Connection {
    socket: TcpStream,
    handshake: Mutex<Option<OwnedSemaphorePermit>>,
}

impl Connected for Connection {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        // Tonic extracts connection metadata after its TLS acceptor succeeds,
        // before serving HTTP/2. Release only the negotiation budget here; the
        // established channel can remain open for long-lived RPC streams.
        self.handshake
            .lock()
            .expect("handshake permit poisoned")
            .take();
        self.socket.connect_info()
    }
}

impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_read(cx, buf)
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.socket).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}
