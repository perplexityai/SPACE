use super::*;
use std::convert::Infallible;
use tower::ServiceExt;

#[tokio::test(start_paused = true)]
async fn long_method_finishes_while_ordinary_method_is_bounded() {
    fn budget(path: &str) -> Duration {
        Duration::from_secs(if path == "/test.Service/Capture" {
            960
        } else {
            120
        })
    }
    for (path, status) in [
        ("/test.Service/Capture", None),
        ("/test.Service/Probe", Some("4")),
    ] {
        let service = MethodTimeoutLayer(budget).layer(tower::service_fn(|_: Request<()>| async {
            tokio::time::sleep(Duration::from_secs(121)).await;
            Ok::<_, Infallible>(Response::new(()))
        }));
        let response = service
            .oneshot(Request::builder().uri(path).body(()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response
                .headers()
                .get("grpc-status")
                .map(|v| v.to_str().unwrap()),
            status
        );
    }
}

#[tokio::test]
async fn method_budget_preserves_shorter_client_deadlines_on_the_real_transport() {
    use tonic_health::pb::{HealthCheckRequest, health_client::HealthClient};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (_, health) = tonic_health::server::health_reporter();
    let rpc = tower::service_fn(move |request: Request<tonic::body::Body>| {
        let health = health.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            health.oneshot(request).await
        }
    });
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        crate::grpc_server_builder_with_request_timeout(Duration::from_secs(1))
            .layer(MethodTimeoutLayer(|path| {
                Duration::from_millis(if path.ends_with("/Check") { 200 } else { 10 })
            }))
            .serve_with_incoming_shutdown(
                rpc,
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = stopped.await;
                },
            ),
    );
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = HealthClient::new(channel);
    client
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .unwrap();
    assert_eq!(
        client
            .watch(HealthCheckRequest {
                service: String::new()
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::DeadlineExceeded
    );
    let mut short = tonic::Request::new(HealthCheckRequest {
        service: String::new(),
    });
    short.set_timeout(Duration::from_millis(5));
    // Tonic reports its outer transport deadline as Cancelled. The method's
    // longer allowance must not override the caller's grpc-timeout header.
    assert_eq!(
        client.check(short).await.unwrap_err().code(),
        tonic::Code::Cancelled
    );
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}
