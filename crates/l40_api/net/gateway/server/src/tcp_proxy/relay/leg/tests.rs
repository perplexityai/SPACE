use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::{RelayWitness, Side};

/// A stream whose every operation fails, so a relay's attribution has a peer
/// that is unambiguously at fault.
struct Broken;

impl AsyncRead for Broken {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::from(io::ErrorKind::ConnectionReset)))
    }
}

impl AsyncWrite for Broken {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn relay_attributes_a_failure_to_the_peer_that_produced_it() {
    for (name, guest_broken) in [("guest", true), ("origin", false)] {
        let witness = RelayWitness::default();
        let (working, _keep_open) = tokio::io::duplex(64);
        let result = if guest_broken {
            let mut guest = witness.guest(Broken);
            let mut origin = witness.origin(working);
            tokio::io::copy_bidirectional(&mut guest, &mut origin).await
        } else {
            let mut guest = witness.guest(working);
            let mut origin = witness.origin(Broken);
            tokio::io::copy_bidirectional(&mut guest, &mut origin).await
        };

        assert!(result.is_err(), "{name}: expected the relay to fail");
        let fault = witness.fault().expect("a failing peer must be recorded");
        let expected = if guest_broken {
            Side::Guest
        } else {
            Side::Origin
        };
        assert_eq!(fault.side(), expected, "{name}: wrong peer blamed");
        assert_eq!(
            witness.closed_by(),
            "none",
            "{name}: a failure is not an orderly close"
        );
    }
}

#[tokio::test]
async fn relay_records_the_peer_that_closed_first_and_the_bytes_it_sent() {
    let witness = RelayWitness::default();
    let (guest_side, mut guest_peer) = tokio::io::duplex(64);
    let (origin_side, mut origin_peer) = tokio::io::duplex(64);

    // The origin stays readable until the relay shuts it down, so the guest is
    // unambiguously the first peer to reach end-of-stream.
    let origin = tokio::spawn(async move {
        let mut received = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut origin_peer, &mut received)
            .await
            .expect("drain the origin side of the relay");
        received
    });

    tokio::io::AsyncWriteExt::write_all(&mut guest_peer, b"hello")
        .await
        .expect("write to the guest side of the relay");
    drop(guest_peer);

    let mut guest = witness.guest(guest_side);
    let mut origin_leg = witness.origin(origin_side);
    tokio::io::copy_bidirectional(&mut guest, &mut origin_leg)
        .await
        .expect("an orderly close is not a relay failure");

    assert_eq!(
        origin.await.expect("origin drain task"),
        b"hello",
        "the relay must forward the guest payload"
    );
    assert_eq!(witness.closed_by(), "guest");
    assert!(witness.fault().is_none(), "an orderly close is not a fault");
    let (guest_bytes, origin_bytes) = witness.bytes();
    assert_eq!(guest_bytes, 5, "bytes the guest sent");
    assert_eq!(origin_bytes, 0, "the origin sent nothing");
}
