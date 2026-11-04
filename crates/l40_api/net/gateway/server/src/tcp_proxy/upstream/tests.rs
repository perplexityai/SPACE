use super::*;
use http_body_util::Empty;

#[tokio::test]
async fn cancelling_the_first_waiter_does_not_cancel_connection_setup() {
    let (origin, _origin_peer) = tokio::io::duplex(4096);
    let (started, started_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    let upstream = Arc::new(Upstream::<_, Empty<Bytes>>::new(
        move |_| async move {
            started.send(()).unwrap();
            release_rx.await.unwrap();
            Ok((origin, HttpProtocol::Http1))
        },
        CancellationToken::new(),
        CancellationToken::new(),
    ));
    let waiting = tokio::spawn({
        let upstream = upstream.clone();
        async move { upstream.sender(HttpProtocol::Http2).await.map(|_| ()) }
    });
    started_rx.await.unwrap();
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    release
        .send(())
        .expect("the connector must outlive an individual waiter");
    let sender = upstream
        .sender(HttpProtocol::Http2)
        .await
        .expect("a second stream reuses connection-owned setup");
    let reused = upstream.sender(HttpProtocol::Http2).await.unwrap();
    assert!(Arc::ptr_eq(&sender, &reused));
    upstream.shutdown().await;
}

#[tokio::test]
async fn failed_setup_is_shared_without_retrying_or_losing_the_native_error() {
    let upstream = Upstream::<_, Empty<Bytes>>::new(
        |_| async {
            Err::<(tokio::io::DuplexStream, HttpProtocol), _>(Error::Connect(
                io::Error::from_raw_os_error(libc::ECONNREFUSED).into(),
            ))
        },
        CancellationToken::new(),
        CancellationToken::new(),
    );
    let first = upstream.sender(HttpProtocol::Http2).await.err().unwrap();
    let second = upstream.sender(HttpProtocol::Http2).await.err().unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    let Error::Connect(transport::Error::Io(error)) = first.as_ref() else {
        panic!("expected native connect error")
    };
    assert_eq!(error.raw_os_error(), Some(libc::ECONNREFUSED));
    upstream.shutdown().await;
}
