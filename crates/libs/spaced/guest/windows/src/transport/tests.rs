use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn bridge_preserves_payload_and_half_close_and_releases_workers() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    let socket = Socket::from(peer.into_std().unwrap());
    let shutdown = CancellationToken::new();
    let workers = TaskTracker::new();
    let permits = Arc::new(Semaphore::new(1));
    let mut stream = Stream::new(
        socket,
        shutdown.clone(),
        &workers,
        &tokio::runtime::Handle::current(),
        permits.clone().acquire_owned().await.unwrap(),
    )
    .unwrap();
    let payload = vec![0xA7; BUFFER_BYTES * 3];
    let expected = payload.clone();
    let remote = tokio::spawn(async move {
        let mut client = client;
        let mut received = Vec::new();
        client.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, expected);
        client.write_all(b"response after EOF").await.unwrap();
        client.shutdown().await.unwrap();
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        stream.write_all(&payload).await.unwrap();
        stream.shutdown().await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"response after EOF");
        remote.await.unwrap();
    })
    .await
    .unwrap();
    drop(stream);
    shutdown.cancel();
    workers.close();
    tokio::time::timeout(Duration::from_secs(2), workers.wait())
        .await
        .unwrap();
    assert_eq!(permits.available_permits(), 1);
}

#[tokio::test]
async fn cancellation_wakes_an_idle_connection() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    let shutdown = CancellationToken::new();
    let workers = TaskTracker::new();
    let mut stream = Stream::new(
        Socket::from(peer.into_std().unwrap()),
        shutdown.clone(),
        &workers,
        &tokio::runtime::Handle::current(),
        Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap(),
    )
    .unwrap();
    let cancel = shutdown.clone();
    tokio::spawn(async move {
        tokio::task::yield_now().await;
        cancel.cancel();
    });
    let mut data = [0; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), stream.read(&mut data))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    drop(stream);
    drop(client);
    workers.close();
    tokio::time::timeout(Duration::from_secs(2), workers.wait())
        .await
        .unwrap();
}

#[test]
fn windows_peer_address_requires_host_cid_and_complete_abi() {
    assert_eq!(std::mem::size_of::<super::VmAddress>(), 12);
    for (cid, length, accepted) in [(2_u32, 12, true), (3, 12, false), (2, 8, false)] {
        let mut storage = socket2::SockAddrStorage::zeroed();
        // SAFETY: the initialized prefix fits the aligned address storage.
        let raw = unsafe { storage.view_as::<super::VmAddress>() };
        *raw = super::VmAddress {
            family: 40,
            reserved: 0,
            port: 5002,
            cid,
        };
        // SAFETY: initialized sockaddr storage with a valid family; the short
        // length deliberately exercises refusal before the peer CID is read.
        let address = unsafe { socket2::SockAddr::new(storage, length) };
        assert_eq!(super::is_host(&address), accepted);
    }
}
