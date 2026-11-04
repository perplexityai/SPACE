use std::time::Duration;

#[tokio::test]
async fn incomplete_requests_are_bounded_per_connection_and_release_capacity() {
    use crate::{
        LocalMetaServer, MAX_LOCAL_GUEST_STREAMS, SpacedGuestService, SpacedGuestServiceConfig,
        guest_grpc_server_builder,
    };
    use tokio_util::sync::CancellationToken;

    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("guest.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let shutdown = CancellationToken::new();
    let service =
        SpacedGuestService::with_config(shutdown.clone(), SpacedGuestServiceConfig::default())
            .expect("guest service");
    let stop = shutdown.clone();
    let server = tokio::spawn(async move {
        guest_grpc_server_builder(MAX_LOCAL_GUEST_STREAMS)
            .add_service(LocalMetaServer::new(service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::UnixListenerStream::new(listener),
                stop.cancelled_owned(),
            )
            .await
            .unwrap();
    });
    let socket = tokio::net::UnixStream::connect(socket).await.unwrap();
    let (mut client, mut connection) = h2::client::Builder::new()
        .handshake::<_, tonic::codegen::Bytes>(socket)
        .await
        .unwrap();
    let mut ping = connection.ping_pong().unwrap();
    let connection = tokio::spawn(connection);
    // Wait for peer SETTINGS before filling its stream capacity.
    tokio::time::timeout(Duration::from_secs(2), ping.ping(h2::Ping::opaque()))
        .await
        .unwrap()
        .unwrap();
    let request = || {
        tonic::codegen::http::Request::builder()
            .method("POST")
            .uri("http://guest/space.spaced.guest.v1.Meta/Ping")
            .header("content-type", "application/grpc")
            .body(())
            .unwrap()
    };
    let mut pending = Vec::new();
    for _ in 0..MAX_LOCAL_GUEST_STREAMS {
        client = tokio::time::timeout(Duration::from_secs(2), client.ready())
            .await
            .unwrap()
            .unwrap();
        // No DATA/END_STREAM: each request remains inside the decoder.
        pending.push(client.send_request(request(), false).unwrap());
    }
    // Flush queued HEADERS before checking backpressure. h2 permits one extra
    // locally queued request beyond the peer's active-stream limit.
    tokio::time::timeout(Duration::from_secs(2), ping.ping(h2::Ping::opaque()))
        .await
        .unwrap()
        .unwrap();
    pending.push(client.send_request(request(), false).unwrap());
    let ready = client.ready();
    tokio::pin!(ready);
    tokio::time::timeout(Duration::from_millis(20), &mut ready)
        .await
        .expect_err("incomplete streams retain connection capacity");
    pending.swap_remove(0).1.send_reset(h2::Reason::CANCEL);
    let client = tokio::time::timeout(Duration::from_secs(2), ready)
        .await
        .unwrap()
        .unwrap();
    drop(pending);
    drop(client);
    connection.abort();
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}
