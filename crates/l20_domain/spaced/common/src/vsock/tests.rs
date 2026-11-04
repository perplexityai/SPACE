use super::*;

#[tokio::test]
async fn acknowledgement_is_bounded_complete_and_preserves_following_bytes() {
    for invalid in [
        b"OK 1".as_slice(),
        b"OK nope\n",
        b"OK 123456789012345678901234567890123\n",
        b"ERR 1\n",
    ] {
        assert!(read_firecracker_ack(&mut &*invalid).await.is_err());
    }
    let mut input = b"OK 123\nHTTP2".as_slice();
    read_firecracker_ack(&mut input).await.unwrap();
    assert_eq!(input, b"HTTP2");
}

#[tokio::test]
async fn firecracker_connector_rejects_an_unexpected_server_process() {
    let socket_path = std::path::PathBuf::from(format!("/tmp/sv-{}.sock", uuid::Uuid::now_v7()));
    let _listener = tokio::net::UnixListener::bind(&socket_path).unwrap();

    let wrong_pid = i32::try_from(std::process::id()).unwrap() + 1;
    let uid = std::os::unix::fs::MetadataExt::uid(&std::fs::metadata(&socket_path).unwrap());
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        connect_firecracker_vsock_channel_with_expected_peer(
            "test Firecracker",
            &socket_path,
            BRIDGE_GUEST_VSOCK_PORT,
            crate::ExpectedUdsPeer::new(wrong_pid, uid),
        ),
    )
    .await
    .expect("peer validation must reject before the Firecracker handshake can block");

    assert!(result.is_err(), "a connection to the wrong PID must fail");
    let _ = std::fs::remove_file(socket_path);
}

#[tokio::test]
async fn pinned_connection_reports_when_the_guest_side_closes() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

    let socket_path = std::path::PathBuf::from(format!("/tmp/sv-{}.sock", uuid::Uuid::now_v7()));
    let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
    let (close_tx, close_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = tokio::io::BufReader::new(stream);
        let mut line = String::new();
        stream.read_line(&mut line).await.unwrap();
        assert_eq!(line, format!("CONNECT {BRIDGE_GUEST_VSOCK_PORT}\n"));
        stream.get_mut().write_all(b"OK 1024\n").await.unwrap();
        let _ = close_rx.await;
    });

    let pid = i32::try_from(std::process::id()).unwrap();
    let uid = std::os::unix::fs::MetadataExt::uid(&std::fs::metadata(&socket_path).unwrap());
    let (_channel, connection) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        connect_firecracker_vsock_channel_with_expected_peer(
            "test Firecracker",
            &socket_path,
            BRIDGE_GUEST_VSOCK_PORT,
            crate::ExpectedUdsPeer::new(pid, uid),
        ),
    )
    .await
    .expect("connection must complete")
    .expect("the expected peer must be accepted");
    assert!(!connection.is_closed());

    close_tx.send(()).unwrap();
    server.await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !connection.is_closed() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a closed guest side must mark the pinned connection closed");
    let _ = std::fs::remove_file(socket_path);
}
