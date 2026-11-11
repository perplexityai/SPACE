use std::net::SocketAddr;
#[cfg(target_os = "linux")]
use std::time::Duration;

use super::*;

/// Serve protocol connections on an ephemeral port, through the same
/// `listener` + `protocol_connection` pair `serve` uses.
fn spawn_protocol_upstream() -> SocketAddr {
    let listening = listener(0).expect("bind an ephemeral port");
    let addr = listening.local_addr().expect("read the bound address");
    tokio::spawn(async move {
        while let Ok((stream, _peer)) = listening.accept().await {
            tokio::spawn(async move {
                let _ = protocol_connection(stream).await;
            });
        }
    });
    addr
}

async fn connect(addr: SocketAddr) -> TcpStream {
    TcpStream::connect(("127.0.0.1", addr.port()))
        .await
        .expect("connect to the bench upstream")
}

/// The upstream reports what it received *after* the header it consumed. Reading
/// its count as covering the header too made `bulk-upload` fail by exactly
/// `wire::HEADER_LEN` bytes — an accounting mismatch that looked like the gateway
/// losing nine bytes of every upload.
#[tokio::test]
async fn sink_counts_the_payload_and_not_the_header() {
    let addr = spawn_protocol_upstream();
    let payload = vec![b'p'; 4096];

    let mut stream = connect(addr).await;
    stream
        .write_all(&wire::encode(Op::Sink, 0))
        .await
        .expect("write the header");
    stream.write_all(&payload).await.expect("write the payload");
    stream.shutdown().await.expect("half-close");

    let mut reported = [0u8; 8];
    stream
        .read_exact(&mut reported)
        .await
        .expect("read the reported count");
    assert_eq!(u64::from_be_bytes(reported), payload.len() as u64);
}

#[tokio::test]
async fn echo_returns_every_payload_byte_and_nothing_else() {
    let addr = spawn_protocol_upstream();
    let payload = vec![b'e'; 3000];

    let mut stream = connect(addr).await;
    stream
        .write_all(&wire::encode(Op::Echo, 0))
        .await
        .expect("write the header");
    stream.write_all(&payload).await.expect("write the payload");
    stream.shutdown().await.expect("half-close");

    let mut echoed = Vec::new();
    stream
        .read_to_end(&mut echoed)
        .await
        .expect("read the echo back");
    assert_eq!(echoed, payload);
}

#[tokio::test]
async fn source_writes_exactly_the_requested_byte_count() {
    let addr = spawn_protocol_upstream();
    // Crosses the upstream's block size, so the loop's final short write runs.
    let requested = (BLOCK as u64) + 777;

    let mut stream = connect(addr).await;
    stream
        .write_all(&wire::encode(Op::Source, requested))
        .await
        .expect("write the header");

    let mut received = Vec::new();
    stream
        .read_to_end(&mut received)
        .await
        .expect("drain the source");
    assert_eq!(received.len() as u64, requested);
}

#[tokio::test]
async fn an_unrecognised_op_is_refused_rather_than_served() {
    let addr = spawn_protocol_upstream();
    let mut header = wire::encode(Op::Echo, 0);
    if let Some(code) = header.first_mut() {
        *code = 0xff;
    }

    let mut stream = connect(addr).await;
    stream.write_all(&header).await.expect("write the header");
    stream.write_all(b"payload").await.expect("write a payload");
    stream.shutdown().await.expect("half-close");

    let mut echoed = Vec::new();
    // Either a clean close or a reset; what must not happen is the payload
    // coming back as though the op had been understood.
    let _ = stream.read_to_end(&mut echoed).await;
    assert!(
        echoed.is_empty(),
        "an unrecognised op must not be served; got {echoed:?}"
    );
}

/// A burst deeper than tokio's default, so the assertion turns on `BACKLOG`.
/// Deriving it from `BACKLOG` instead would make the test vacuous: it would
/// shrink along with a regression and keep passing.
#[cfg(target_os = "linux")]
const BURST: usize = 1200;

/// The discriminating half of the backlog guard: connections queue rather than
/// being dropped when nothing is accepting them. At tokio's default depth the
/// kernel starts dropping handshakes partway through this burst and those
/// connects never complete.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn the_upstream_queues_a_burst_that_nothing_is_accepting() {
    const SOMAXCONN: &str = "/proc/sys/net/core/somaxconn";
    let somaxconn: usize = std::fs::read_to_string(SOMAXCONN)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0);
    // `somaxconn` caps the effective depth whatever `listen` asked for, so a host
    // below the burst cannot exercise this. Say that rather than pass vacuously.
    assert!(
        somaxconn >= BURST,
        "{SOMAXCONN} is {somaxconn}, below the {BURST}-connection burst this \
         guard needs; raise it to test the upstream's backlog"
    );

    // Deliberately never accepted: every connection has to sit in the queue.
    let listening = listener(0).expect("bind an ephemeral port");
    let addr = listening.local_addr().expect("read the bound address");

    let mut connected = Vec::with_capacity(BURST);
    for index in 0..BURST {
        let stream = tokio::time::timeout(
            Duration::from_secs(5),
            TcpStream::connect(("127.0.0.1", addr.port())),
        )
        .await
        .unwrap_or_else(|_timeout| {
            panic!("connection {index} of {BURST} was dropped rather than queued")
        })
        .unwrap_or_else(|error| panic!("connection {index} of {BURST} failed: {error}"));
        connected.push(stream);
    }
    assert_eq!(connected.len(), BURST);
}
