use std::path::Path;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::Request;
use tonic_health::pb::health_client::HealthClient;

async fn serve_health(socket_path: &Path) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::UnixListener::bind(socket_path).expect("bind test socket");
    let (reporter, service) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve test health service");
    });
    (shutdown_tx, task)
}

fn health_request() -> Request<tonic_health::pb::HealthCheckRequest> {
    let mut request = Request::new(tonic_health::pb::HealthCheckRequest {
        service: String::new(),
    });
    request.set_timeout(Duration::from_secs(1));
    request
}

#[tokio::test(start_paused = true)]
async fn stalled_uds_attempt_shares_total_budget_with_prior_retries() {
    let budget = Duration::from_secs(1);
    let policy = crate::GrpcRetryPolicy {
        max_elapsed: budget,
        jitter_factor: 0.0,
        ..crate::DEFAULT_GRPC_CONNECT_RETRY_POLICY
    };
    let started = tokio::time::Instant::now();
    let attempts = std::cell::Cell::new(0);
    // A pending connector represents socket establishment or preface-write
    // backpressure, not a peer withholding SETTINGS (which need not block connect).
    let connect = super::retry_uds_connect("stalled", Path::new("/pending.sock"), policy, || {
        let attempt = attempts.get() + 1;
        attempts.set(attempt);
        async move {
            if attempt == 1 {
                Err(super::UdsConnectError::Endpoint(
                    tonic::transport::Endpoint::from_shared("http://[".to_string())
                        .expect_err("invalid URI"),
                ))
            } else {
                std::future::pending().await
            }
        }
    });
    let result = tokio::time::timeout(budget * 2, connect)
        .await
        .expect("an in-flight attempt must not bypass the total connect budget");
    assert!(matches!(
        result,
        Err(super::UdsConnectError::Timeout { .. })
    ));
    assert_eq!(attempts.get(), 2);
    assert_eq!(started.elapsed(), budget);
}

#[tokio::test]
async fn lazy_uds_channel_reconnects_after_socket_replacement() {
    let dir = tempfile::tempdir().expect("tempdir");
    let socket_path = dir.path().join("backend.sock");
    let (predecessor_shutdown, predecessor_task) = serve_health(&socket_path).await;
    let endpoint =
        tonic::transport::Endpoint::try_from("http://[::]:50051").expect("static test endpoint");
    let channel = super::connect_uds_channel_lazy(&socket_path, endpoint);
    let mut client = HealthClient::new(channel);

    client
        .check(health_request())
        .await
        .expect("predecessor health check");
    predecessor_shutdown
        .send(())
        .expect("stop predecessor health service");
    predecessor_task.await.expect("join predecessor");
    std::fs::remove_file(&socket_path).expect("remove predecessor socket");

    let successor_path = socket_path.clone();
    let successor = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        serve_health(&successor_path).await
    });
    let response = crate::fwd_retry!("test-backend", client, check, health_request())
        .expect("health check reconnects to successor");

    assert_eq!(
        response.into_inner().status,
        tonic_health::pb::health_check_response::ServingStatus::Serving as i32
    );
    let (successor_shutdown, successor_task) = successor.await.expect("start successor");
    successor_shutdown
        .send(())
        .expect("stop successor health service");
    successor_task.await.expect("join successor");
}

async fn serve_tcp_health(
    listener: tokio::net::TcpListener,
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let (reporter, service) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
            .expect("serve test health service");
    });
    (shutdown_tx, task)
}

// A kept channel whose peer restarts waits for it on the next call: the redial
// retries until the peer listens again, instead of failing the call.
#[tokio::test]
async fn a_kept_tcp_channel_waits_for_its_peer_to_come_back() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, server) = serve_tcp_health(listener).await;
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}")).unwrap();
    let channel = super::connect_tcp_channel_with_retrying_redials(
        "test-backend",
        endpoint,
        Duration::from_secs(1),
        crate::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
    )
    .await
    .expect("first dial");
    let mut client = HealthClient::new(channel);
    client.check(health_request()).await.expect("served");

    stop.send(()).unwrap();
    server.await.unwrap();
    let mut waiting = client.clone();
    let call = tokio::spawn(async move {
        let mut request = health_request();
        request.set_timeout(Duration::from_secs(10));
        waiting.check(request).await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!call.is_finished(), "the call waits while the peer is down");
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    let (stop, server) = serve_tcp_health(listener).await;
    call.await
        .unwrap()
        .expect("the redial reaches the restarted peer");
    stop.send(()).unwrap();
    server.await.unwrap();
}
