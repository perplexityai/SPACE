use super::*;
use axum::serve::Listener;

#[tokio::test]
async fn accepted_connections_have_nodelay_set() {
    let mut listener = nodelay(TcpListener::bind("127.0.0.1:0").await.unwrap());
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (accepted, peer) = listener.accept().await;
    assert_eq!(peer, client.local_addr().unwrap());
    assert!(accepted.nodelay().unwrap());
}
