use tokio::net::{TcpListener, TcpStream};

use super::*;

async fn with_loopback_pair<W, Wf, F, Ff, T>(client_writes: W, f: F) -> T
where
    W: FnOnce(TcpStream) -> Wf + Send + 'static,
    Wf: std::future::Future<Output = ()> + Send,
    F: FnOnce(TcpStream) -> Ff,
    Ff: std::future::Future<Output = T>,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = tokio::spawn(async move {
        let stream = TcpStream::connect(addr).await.unwrap();
        client_writes(stream).await;
    });
    let (server, _) = listener.accept().await.unwrap();
    let result = f(server).await;
    client.await.unwrap();
    result
}

#[tokio::test]
async fn sniff_client_hello_reassembles_segmented_client_hello() {
    use tokio::io::AsyncWriteExt;

    let hello = crate::tls_sni::test_fixtures::client_hello(Some("split.example.com"));
    let (head, tail) = hello.split_at(8);
    let (head, tail) = (head.to_vec(), tail.to_vec());
    let scan = with_loopback_pair(
        move |mut client| async move {
            // Deliver the record header first, then the rest, so the first
            // peek sees an incomplete ClientHello and the loop must re-peek.
            client.write_all(&head).await.unwrap();
            client.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await;
            client.write_all(&tail).await.unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
        },
        |server| async move { client_hello(&server).await },
    )
    .await;

    assert_eq!(
        scan.0.server_name().map(|name| name.as_str().to_string()),
        Some("split.example.com".to_string())
    );
}

#[tokio::test]
async fn sniff_client_hello_peek_does_not_consume_bytes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let hello = crate::tls_sni::test_fixtures::client_hello(Some("intact.example.com"));
    let expected = hello.clone();
    let read_back = with_loopback_pair(
        move |mut client| async move {
            client.write_all(&hello).await.unwrap();
            client.shutdown().await.unwrap();
        },
        |mut server| async move {
            let (scan, _) = client_hello(&server).await;
            assert!(scan.server_name().is_some(), "sniff should see the SNI");
            // The real proxy reads after sniffing: every peeked byte must
            // still be in the stream.
            let mut buf = Vec::new();
            server.read_to_end(&mut buf).await.unwrap();
            buf
        },
    )
    .await;

    assert_eq!(read_back, expected, "peek must leave the stream intact");
}

#[tokio::test]
async fn sniff_accepts_a_client_hello_larger_than_four_kibibytes() {
    use tokio::io::AsyncWriteExt;

    use crate::tls_sni::test_fixtures::{
        client_hello_with_extensions, filler_extension, sni_extension,
    };

    let mut extensions = filler_extension(0x0015, 5_000);
    extensions.extend_from_slice(&sni_extension(&[(0x00, b"large.example.com")]));
    let hello = client_hello_with_extensions(&extensions);
    assert!(hello.len() > 4096);
    let scan = with_loopback_pair(
        move |mut client| async move {
            client.write_all(&hello).await.unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
        },
        |server| async move { client_hello(&server).await },
    )
    .await;

    assert_eq!(
        scan.0.server_name().map(|name| name.as_str().to_string()),
        Some("large.example.com".to_string())
    );
}

#[tokio::test]
async fn a_late_client_hello_is_seen_only_when_its_absence_would_be_denied() {
    use tokio::io::AsyncWriteExt;

    for (missing, expected) in [
        (MissingClientHello::Proceeds, None),
        (MissingClientHello::Denied, Some("late.example.com")),
    ] {
        let hello = crate::tls_sni::test_fixtures::client_hello(Some("late.example.com"));
        let observation = with_loopback_pair(
            move |mut client| async move {
                // Past the 50 ms budget, well within the fail-closed one.
                tokio::time::sleep(Duration::from_millis(150)).await;
                let _ = client.write_all(&hello).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            },
            move |server| async move { client_hello_with_detail(&server, missing).await },
        )
        .await;

        assert_eq!(
            observation
                .scan
                .server_name()
                .map(|name| name.as_str().to_string()),
            expected.map(str::to_string),
            "{missing:?}"
        );
        if expected.is_none() {
            assert_eq!(
                observation.missing_sni_detail,
                Some(MissingSniDetail::FirstByteTimeout)
            );
        }
    }
}

#[tokio::test]
async fn the_completion_budget_starts_with_the_first_handshake_bytes() {
    use tokio::io::AsyncWriteExt;

    let hello = crate::tls_sni::test_fixtures::client_hello(Some("split-late.example.com"));
    let (head, tail) = hello.split_at(8);
    let (head, tail) = (head.to_vec(), tail.to_vec());
    let observation = with_loopback_pair(
        move |mut client| async move {
            // The record header lands after COMPLETION_DEADLINE has passed
            // since the accept; the rest follows well within the budget
            // counted from the header.
            tokio::time::sleep(COMPLETION_DEADLINE + Duration::from_millis(200)).await;
            client.write_all(&head).await.unwrap();
            client.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await;
            client.write_all(&tail).await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        },
        |server| async move { client_hello_with_detail(&server, MissingClientHello::Denied).await },
    )
    .await;

    assert_eq!(
        observation
            .scan
            .server_name()
            .map(|name| name.as_str().to_string()),
        Some("split-late.example.com".to_string())
    );
}

#[tokio::test]
async fn a_client_that_never_writes_still_times_out_under_the_fail_closed_budget() {
    let observation = with_loopback_pair(
        |client| async move {
            tokio::time::sleep(FAIL_CLOSED_FIRST_BYTES_TIMEOUT + Duration::from_millis(200)).await;
            drop(client);
        },
        |server| async move { client_hello_with_detail(&server, MissingClientHello::Denied).await },
    )
    .await;

    assert!(matches!(observation.scan, ClientHelloScan::Incomplete));
    assert_eq!(
        observation.missing_sni_detail,
        Some(MissingSniDetail::FirstByteTimeout)
    );
    assert!(
        observation.elapsed >= FAIL_CLOSED_FIRST_BYTES_TIMEOUT,
        "waited only {:?}",
        observation.elapsed
    );
}
