use super::*;
use pplx_space_spaced_common::proto::bridge_guest as guest_protocol;

#[derive(Clone)]
struct CloseGuest {
    calls: Arc<std::sync::atomic::AtomicUsize>,
    reply_error: Option<tonic::Code>,
}

#[tonic::async_trait]
impl guest_protocol::networking_server::Networking for CloseGuest {
    async fn open_loopback_bridge(
        &self,
        _: Request<guest_protocol::OpenLoopbackBridgeRequest>,
    ) -> Result<Response<guest_protocol::OpenLoopbackBridgeResponse>, Status> {
        Err(Status::unimplemented("unused test RPC"))
    }

    async fn close_loopback_bridge(
        &self,
        _: Request<guest_protocol::CloseLoopbackBridgeRequest>,
    ) -> Result<Response<guest_protocol::CloseLoopbackBridgeResponse>, Status> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        match self.reply_error {
            Some(code) => Err(Status::new(code, "close applied; reply lost")),
            None => Ok(Response::new(
                guest_protocol::CloseLoopbackBridgeResponse {},
            )),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn close_retries_connection_establishment_but_never_a_dispatched_rpc() {
    use crate::proto::networking_server::Networking;
    use pplx_space_spaced_common::proto::bridge_guest::networking_server::NetworkingServer;
    use std::sync::atomic::{AtomicUsize, Ordering};

    for reply_error in [None, Some(tonic::Code::Unavailable)] {
        let directory = tempfile::tempdir().expect("directory");
        let socket = directory.path().join("guest.sock");
        let shutdown = CancellationToken::new();
        let mut service = SpacedHostService::new_with_config(
            shutdown.clone(),
            SpacedHostServiceConfig {
                bridge_guest_connector: BridgeGuestConnector::Uds(socket.clone()),
                ..Default::default()
            },
        );
        Arc::get_mut(&mut service.inner)
            .expect("unique service")
            .bridge_retry_policy = RetryPolicy {
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(1),
            steps: 1,
            max_retry_count: 2,
        };
        let close =
            service.close_loopback_bridge(Request::new(crate::proto::CloseLoopbackBridgeRequest {
                port: 8080,
            }));
        tokio::pin!(close);
        // The absent socket fails before dispatch; the call must remain alive
        // through backoff, rather than returning the first connection failure.
        tokio::time::timeout(Duration::from_millis(100), &mut close)
            .await
            .expect_err("connection failure must leave time for retry");
        let calls = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::UnixListener::bind(socket).expect("guest socket");
        let guest = CloseGuest {
            calls: calls.clone(),
            reply_error,
        };
        let stop = shutdown.clone();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(NetworkingServer::new(guest))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::UnixListenerStream::new(listener),
                    stop.cancelled_owned(),
                )
                .await
                .expect("guest server")
        });
        let result = close.await;
        match reply_error {
            Some(code) => assert_eq!(result.expect_err("ambiguous close").code(), code),
            None => {
                result.expect("close after connection recovery");
            }
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "never replay a dispatched close"
        );
        assert_eq!(
            service.inner.bridge_guest_networking_client.is_connected(),
            reply_error.is_none()
        );
        shutdown.cancel();
        server.await.expect("guest task");
    }
}

#[tokio::test(start_paused = true)]
async fn listener_failure_preserves_sibling_drain_and_error_through_deadline() {
    for initial_failure in [false, true] {
        for stalled in [false, true] {
            for fail_host in [false, true] {
                let shutdown = CancellationToken::new();
                if !initial_failure {
                    shutdown.cancel();
                }
                let drained = std::sync::atomic::AtomicBool::new(false);
                let failed = async {
                    if !initial_failure {
                        // Ensure shutdown wins the initial select, then fail in drain.
                        tokio::task::yield_now().await;
                    }
                    Err(RunSpacedHostServerError::Bind(
                        pplx_space_spaced_common::BindUdsError::Bind {
                            path: PathBuf::from("failed.sock"),
                            source: std::io::Error::from(std::io::ErrorKind::BrokenPipe),
                        },
                    ))
                };
                let sibling = async {
                    shutdown.cancelled().await;
                    if stalled {
                        std::future::pending::<()>().await;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    drained.store(true, std::sync::atomic::Ordering::Relaxed);
                    Ok(())
                };
                let started = tokio::time::Instant::now();
                let result = if fail_host {
                    serve_until_shutdown(shutdown.clone(), failed, sibling).await
                } else {
                    serve_until_shutdown(shutdown.clone(), sibling, failed).await
                };
                assert!(matches!(result, Err(RunSpacedHostServerError::Bind(_))));
                assert_eq!(drained.load(std::sync::atomic::Ordering::Relaxed), !stalled);
                assert_eq!(
                    started.elapsed(),
                    Duration::from_secs(if stalled { 10 } else { 1 })
                );
            }
        }
    }
}

#[test]
fn networking_open_budget_adds_headroom_above_guest_wait() {
    assert_eq!(
        BridgeGuestOperationBudget::NetworkingOpen { wait_timeout: None }.timeout(),
        std::time::Duration::from_secs(10)
    );
    assert_eq!(
        BridgeGuestOperationBudget::NetworkingOpen {
            wait_timeout: Some(std::time::Duration::from_secs(2)),
        }
        .timeout(),
        std::time::Duration::from_secs(4)
    );
    assert_eq!(
        BridgeGuestOperationBudget::NetworkingOpen {
            wait_timeout: Some(std::time::Duration::from_secs(60)),
        }
        .timeout(),
        std::time::Duration::from_secs(30)
    );
}

#[test]
fn readiness_response_timeout_adds_transport_headroom() {
    assert_eq!(
        bridge_guest_readiness_response_timeout(READY_WAIT_RETRY_INTERVAL),
        std::time::Duration::from_millis(105)
    );
}

#[tokio::test(start_paused = true)]
async fn bridge_flush_deadline_preserves_foreground_and_auto_lifecycle_budgets() {
    for (budget, work, succeeds) in [(30, 31, false), (180, 137, true), (400, 181, false)] {
        let result = with_bridge_guest_timeout(
            BridgeGuestOperationBudget::FilesystemFlush(Duration::from_secs(budget)).timeout(),
            "test bridge timeout",
            async {
                tokio::time::sleep(Duration::from_secs(work)).await;
                Ok(())
            },
        )
        .await;
        if succeeds {
            result.expect("normal slow flush fits the auto-lifecycle budget");
        } else {
            assert_eq!(
                result.expect_err("flush deadline").code(),
                tonic::Code::DeadlineExceeded
            );
        }
    }
}

#[tokio::test]
async fn saturated_output_streams_leave_control_capacity_and_release_on_drop() {
    use tonic::codegen::http;
    use tonic::transport::server::Connected;
    use tower::{Layer, Service};

    let (stream, _peer) = tokio::net::UnixStream::pair().expect("connected UDS pair");
    let identity = stream.connect_info();
    let inner = tower::service_fn(|_: http::Request<tonic::body::Body>| async {
        Ok::<_, std::convert::Infallible>(http::Response::new(PendingResponseBody))
    });
    let mut service = host_request_limiter().layer(inner);
    let request = |method: &str| {
        let mut request = http::Request::builder()
            .uri(format!("/space.spaced.host.v1.{method}"))
            .body(tonic::body::Body::empty())
            .expect("request");
        request.extensions_mut().insert(identity.clone());
        request
    };
    let mut output_streams = Vec::new();
    for _ in 0..2048 {
        let response = service
            .call(request("Process/Connect"))
            .await
            .expect("response");
        assert!(response.headers().get("grpc-status").is_none());
        output_streams.push(response);
    }
    let rejected = service
        .call(request("Process/Connect"))
        .await
        .expect_err("output capacity is bounded");
    assert_eq!(
        tonic::Status::from_error(rejected).code(),
        tonic::Code::ResourceExhausted
    );
    for method in [
        "Meta/Ping",
        "Meta/PrepareSuspend",
        "Process/SendSignal",
        "Process/SendInput",
    ] {
        let response = service
            .call(request(method))
            .await
            .expect("control response");
        assert!(response.headers().get("grpc-status").is_none(), "{method}");
    }
    output_streams.pop();
    let response = service
        .call(request("Process/Connect"))
        .await
        .expect("released capacity");
    assert!(response.headers().get("grpc-status").is_none());
}

struct PendingResponseBody;

impl http_body::Body for PendingResponseBody {
    type Data = tonic::codegen::Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        std::task::Poll::Pending
    }
}
