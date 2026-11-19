use super::*;
use std::time::Duration;

use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

#[tokio::test]
async fn shutdown_closes_the_listener_while_accepted_connections_drain() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("service.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let shutdown = CancellationToken::new();
    let (_reporter, health_service) = crate::grpc_health_service();
    let server = tokio::spawn(
        crate::grpc_server_builder()
            .add_service(health_service)
            .serve_with_incoming_shutdown(
                unix_incoming_until_shutdown(listener, shutdown.clone()),
                std::future::pending(),
            ),
    );
    let channel = pplx_space_common_grpc::connect_uds_channel("unix-incoming-test", &path)
        .await
        .unwrap();
    let mut watch = HealthClient::new(channel)
        .watch(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    watch.message().await.unwrap().unwrap();

    shutdown.cancel();

    // A connect into a listener that is still open succeeds even though
    // nothing will accept it; a closed one is refused.
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match UnixStream::connect(&path).await {
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => break,
                _ => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .expect("a draining server must refuse new connections instead of queueing them");
    assert!(
        !server.is_finished(),
        "the accepted connection is still draining"
    );

    drop(watch);
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("the server ends once its accepted connections drain")
        .unwrap()
        .unwrap();
}
