use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn a_half_close_still_allows_the_reverse_direction_to_finish() {
    let (guest, mut client) = tokio::io::duplex(64);
    let (origin, mut server) = tokio::io::duplex(64);
    let relay =
        tokio::spawn(async move { run(guest, origin, &CancellationToken::new(), None).await });
    client.write_all(b"request").await.unwrap();
    client.shutdown().await.unwrap();
    let mut received = Vec::new();
    server.read_to_end(&mut received).await.unwrap();
    assert_eq!(received, b"request");
    server.write_all(b"response").await.unwrap();
    server.shutdown().await.unwrap();
    received.clear();
    client.read_to_end(&mut received).await.unwrap();
    assert_eq!(received, b"response");
    let report = relay.await.unwrap();
    report.result.unwrap();
    assert_eq!(report.written, (7, 8));
    assert_eq!(report.first_eof, "guest");
}

#[tokio::test]
async fn cancellation_retains_partial_writes_without_counting_buffered_bytes_as_delivered() {
    let (guest, mut client) = tokio::io::duplex(64);
    let (origin, mut server) = tokio::io::duplex(4);
    let shutdown = CancellationToken::new();
    let relay = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { run(guest, origin, &shutdown, None).await }
    });
    client.write_all(&[42; 64]).await.unwrap();
    let mut received = [0; 4];
    server.read_exact(&mut received).await.unwrap();
    shutdown.cancel();
    let report = relay.await.unwrap();
    assert!(matches!(report.result, Err(Error::Cancelled)));
    assert_eq!(received, [42; 4]);
    assert_eq!(report.read.0, 64);
    assert!((4..=8).contains(&report.written.0));
    assert_eq!(report.first_eof, "none");
    assert!(report.fault.is_none());
}
