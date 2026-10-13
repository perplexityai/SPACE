//! Per-leg attribution for bidirectional relays.
//!
//! `copy_bidirectional` reports a single `io::Error` with no indication of
//! which peer produced it, so a guest that hangs up mid-tunnel is otherwise
//! indistinguishable from an origin failure. Wrapping each side records the
//! first fault, the first end-of-stream, and the bytes each peer sent.

use std::io;
use std::pin::Pin;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Which peer of a relay an observation came from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::tcp_proxy) enum Side {
    Guest,
    Origin,
}

impl Side {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Guest => "guest",
            Self::Origin => "origin",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    Read,
    Write,
    Flush,
    Shutdown,
}

/// The first I/O failure in a relay, as a bounded `side_operation` label.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::tcp_proxy) struct RelayFault {
    side: Side,
    operation: Operation,
}

impl RelayFault {
    #[cfg(test)]
    pub(in crate::tcp_proxy) const fn side(self) -> Side {
        self.side
    }

    pub(in crate::tcp_proxy) const fn as_str(self) -> &'static str {
        match (self.side, self.operation) {
            (Side::Guest, Operation::Read) => "guest_read",
            (Side::Guest, Operation::Write) => "guest_write",
            (Side::Guest, Operation::Flush) => "guest_flush",
            (Side::Guest, Operation::Shutdown) => "guest_shutdown",
            (Side::Origin, Operation::Read) => "origin_read",
            (Side::Origin, Operation::Write) => "origin_write",
            (Side::Origin, Operation::Flush) => "origin_flush",
            (Side::Origin, Operation::Shutdown) => "origin_shutdown",
        }
    }
}

/// Shared first-fault and first-close cells. Both legs write to the same cells
/// so EOF describes the first observation by the relay, not causal ownership
/// of the connection close. Borrowed cells remain readable after cancellation.
#[derive(Default)]
pub(in crate::tcp_proxy) struct RelayWitness {
    fault: OnceLock<RelayFault>,
    closed_by: OnceLock<Side>,
    guest_bytes: AtomicU64,
    origin_bytes: AtomicU64,
    guest_written: AtomicU64,
    origin_written: AtomicU64,
}

impl RelayWitness {
    pub(in crate::tcp_proxy) fn fault(&self) -> Option<RelayFault> {
        self.fault.get().copied()
    }

    /// `none` until one peer reaches end-of-stream, so a relay torn down by
    /// shutdown is not misreported as a peer-initiated close.
    pub(in crate::tcp_proxy) fn closed_by(&self) -> &'static str {
        self.closed_by.get().map_or("none", |side| side.as_str())
    }

    pub(in crate::tcp_proxy) fn bytes(&self) -> (u64, u64) {
        (
            self.guest_bytes.load(Ordering::Relaxed),
            self.origin_bytes.load(Ordering::Relaxed),
        )
    }

    pub(in crate::tcp_proxy) fn written(&self) -> (u64, u64) {
        (
            self.origin_written.load(Ordering::Relaxed),
            self.guest_written.load(Ordering::Relaxed),
        )
    }

    pub(in crate::tcp_proxy) fn guest<T>(&self, inner: T) -> RelayLeg<'_, T> {
        RelayLeg {
            inner,
            side: Side::Guest,
            bytes: &self.guest_bytes,
            written: &self.guest_written,
            witness: self,
        }
    }

    pub(in crate::tcp_proxy) fn origin<T>(&self, inner: T) -> RelayLeg<'_, T> {
        RelayLeg {
            inner,
            side: Side::Origin,
            bytes: &self.origin_bytes,
            written: &self.origin_written,
            witness: self,
        }
    }
}

pub(in crate::tcp_proxy) struct RelayLeg<'a, T> {
    inner: T,
    side: Side,
    bytes: &'a AtomicU64,
    written: &'a AtomicU64,
    witness: &'a RelayWitness,
}

impl<T> RelayLeg<'_, T> {
    fn note(&self, operation: Operation, result: &Poll<io::Result<impl Sized>>) {
        if let Poll::Ready(Err(_)) = result {
            let _ = self.witness.fault.set(RelayFault {
                side: self.side,
                operation,
            });
        }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for RelayLeg<'_, T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buffer.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(context, buffer);
        if let Poll::Ready(Ok(())) = &result {
            let read = buffer.filled().len().saturating_sub(before) as u64;
            if read == 0 {
                let _ = self.witness.closed_by.set(self.side);
            }
            self.bytes.fetch_add(read, Ordering::Relaxed);
        }
        self.note(Operation::Read, &result);
        result
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for RelayLeg<'_, T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(context, buffer);
        if let Poll::Ready(Ok(written)) = result {
            self.written.fetch_add(written as u64, Ordering::Relaxed);
        }
        self.note(Operation::Write, &result);
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.inner).poll_flush(context);
        self.note(Operation::Flush, &result);
        result
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.inner).poll_shutdown(context);
        self.note(Operation::Shutdown, &result);
        result
    }
}

#[cfg(test)]
mod tests;
