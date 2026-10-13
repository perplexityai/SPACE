use crate::bridge_host::{BridgeHostFixture, connect_bridge_host};
use crate::duration::duration_from_millis;
use crate::fixture::HostFixture;
use crate::mock_bridge_guest::start_mock_bridge_guest;
use pplx_space_spaced_common::proto::bridge_guest::meta_server::{
    Meta as BridgeGuestMeta, MetaServer as BridgeGuestMetaServer,
};
use pplx_space_spaced_common::proto::bridge_guest::{
    ConfigureProcessEnvRequest as BridgeGuestConfigureProcessEnvRequest,
    ConfigureProcessEnvResponse as BridgeGuestConfigureProcessEnvResponse,
    FlushFilesystemsRequest as BridgeGuestFlushFilesystemsRequest,
    FlushFilesystemsResponse as BridgeGuestFlushFilesystemsResponse, GuestDiagnosticCallsite,
    NotifyResumeRequest as BridgeGuestNotifyResumeRequest,
    NotifyResumeResponse as BridgeGuestNotifyResumeResponse, PingRequest as BridgeGuestPingRequest,
    PingResponse as BridgeGuestPingResponse, WaitReadyRequest as BridgeGuestWaitReadyRequest,
    WaitReadyResponse as BridgeGuestWaitReadyResponse,
};
use pplx_space_spaced_common::proto::bridge_host::{
    ActivityState, RequestActiveForRequest as BridgeHostRequestActiveForRequest,
    RequestShutdownRequest as BridgeHostShutdownRequest,
    SetActivityStateRequest as BridgeHostSetActivityStateRequest,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::{UnixListener, UnixStream};
use tokio_stream::Stream;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};

struct ReadyWaitGuest {
    version: &'static str,
    family: &'static str,
    wait_ready_calls: Arc<AtomicUsize>,
    active_wait_ready_calls: Option<Arc<AtomicUsize>>,
    ready: Arc<AtomicBool>,
    response_delay: Duration,
    response_errors: Arc<std::sync::Mutex<std::collections::VecDeque<tonic::Code>>>,
    flush_deadlines: Option<Arc<std::sync::Mutex<Vec<Duration>>>>,
    ping_callsites: usize,
}

struct ActiveWaitReadyCall(Arc<AtomicUsize>);

impl Drop for ActiveWaitReadyCall {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tonic::async_trait]
impl BridgeGuestMeta for ReadyWaitGuest {
    async fn ping(
        &self,
        _request: Request<BridgeGuestPingRequest>,
    ) -> Result<Response<BridgeGuestPingResponse>, Status> {
        Ok(Response::new(BridgeGuestPingResponse {
            diagnostics: (0..self.ping_callsites)
                .map(|_| GuestDiagnosticCallsite {
                    level: "warn".to_string(),
                    target: "guest::target".repeat(4),
                    file: "src/lib.rs".to_string(),
                    line: 1,
                    count: 1,
                })
                .collect(),
            diagnostics_dropped: 0,
        }))
    }

    async fn wait_ready(
        &self,
        _request: Request<BridgeGuestWaitReadyRequest>,
    ) -> Result<Response<BridgeGuestWaitReadyResponse>, Status> {
        self.wait_ready_calls.fetch_add(1, Ordering::SeqCst);
        let _active_call = self.active_wait_ready_calls.as_ref().map(|active| {
            active.fetch_add(1, Ordering::SeqCst);
            ActiveWaitReadyCall(active.clone())
        });
        tokio::time::sleep(self.response_delay).await;
        if let Some(code) = self.response_errors.lock().unwrap().pop_front() {
            return Err(Status::new(code, "readiness connection interrupted"));
        }
        let ready = self.ready.load(Ordering::SeqCst);
        Ok(Response::new(BridgeGuestWaitReadyResponse {
            ready,
            guest_uptime: ready.then(|| duration_from_millis(42)),
            version: self.version.to_string(),
            compatibility_family: self.family.to_string(),
            mem_total_bytes: ready.then_some(8_589_934_592),
            vcpu_count: ready.then_some(4),
        }))
    }

    async fn configure_process_env(
        &self,
        _request: Request<BridgeGuestConfigureProcessEnvRequest>,
    ) -> Result<Response<BridgeGuestConfigureProcessEnvResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn notify_resume(
        &self,
        _request: Request<BridgeGuestNotifyResumeRequest>,
    ) -> Result<Response<BridgeGuestNotifyResumeResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn flush_filesystems(
        &self,
        request: Request<BridgeGuestFlushFilesystemsRequest>,
    ) -> Result<Response<BridgeGuestFlushFilesystemsResponse>, Status> {
        if let Some(deadlines) = &self.flush_deadlines {
            let remaining = pplx_space_spaced_common::RetryContext::none()
                .with_grpc_timeout_metadata(request.metadata())
                .remaining()
                .ok_or_else(|| Status::invalid_argument("missing flush deadline"))?;
            deadlines.lock().unwrap().push(remaining);
            return Ok(Response::new(BridgeGuestFlushFilesystemsResponse {}));
        }
        Err(Status::unimplemented("not used"))
    }
}

impl ReadyWaitGuest {
    fn new(wait_ready_calls: Arc<AtomicUsize>) -> Self {
        Self {
            version: "0.1.0",
            family: "0.1",
            wait_ready_calls,
            active_wait_ready_calls: None,
            ready: Arc::new(AtomicBool::new(true)),
            response_delay: Duration::ZERO,
            response_errors: Arc::default(),
            flush_deadlines: None,
            ping_callsites: 0,
        }
    }

    async fn start(self, socket_path: std::path::PathBuf) -> tokio::task::JoinHandle<()> {
        let listener = UnixListener::bind(socket_path).expect("guest bridge socket bound");
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(BridgeGuestMetaServer::new(self))
                .serve_with_incoming(UnixListenerStream::new(listener))
                .await
                .expect("mock guest server");
        })
    }
}

#[tokio::test]
async fn bridge_host_request_shutdown_is_ignored_by_default() {
    let fixture = BridgeHostFixture::start().await;
    let mut client = connect_bridge_host(fixture.bridge_host_socket_path.clone()).await;

    let response = client
        .request_shutdown(BridgeHostShutdownRequest {
            reason: Some("test".to_string()),
        })
        .await
        .expect("shutdown response")
        .into_inner();
    assert!(response.outcome.is_none());
    assert!(
        fixture.calls.lock().unwrap().is_empty(),
        "shutdown is not forwarded"
    );

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_host_rejects_a_connection_from_outside_the_expected_runtime() {
    let fixture = BridgeHostFixture::start_with_expected_peer(
        pplx_space_spaced_common::ExpectedUdsPeer::new(i32::MAX, u32::MAX),
    )
    .await;
    let mut client = connect_bridge_host(fixture.bridge_host_socket_path.clone()).await;

    let error = client
        .request_shutdown(BridgeHostShutdownRequest {
            reason: Some("test".to_string()),
        })
        .await
        .expect_err("only the registered Firecracker process may call the host bridge");
    assert_eq!(error.code(), tonic::Code::PermissionDenied);

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_host_activity_calls_relay_requests_and_hide_downstream_outcomes() {
    use crate::bridge_host::HostControlCall;
    use pplx_space_spaced_common::proto::host_control;
    for fail in [false, true] {
        let fixture = if fail {
            BridgeHostFixture::start_with_failing_host_control().await
        } else {
            BridgeHostFixture::start().await
        };
        let mut client = connect_bridge_host(fixture.bridge_host_socket_path.clone()).await;
        let state = ActivityState::Idle as i32;
        let hold_for = Some(duration_from_millis(750));
        assert!(
            client
                .set_activity_state(BridgeHostSetActivityStateRequest { state })
                .await
                .unwrap()
                .into_inner()
                .outcome
                .is_none()
        );
        assert!(
            client
                .request_active_for(BridgeHostRequestActiveForRequest { hold_for })
                .await
                .unwrap()
                .into_inner()
                .outcome
                .is_none()
        );
        assert_eq!(
            *fixture.calls.lock().unwrap(),
            [
                HostControlCall::Activity(host_control::SetActivityStateRequest { state }),
                HostControlCall::ActiveFor(host_control::RequestActiveForRequest { hold_for }),
            ],
            "downstream fail={fail}"
        );
        let directory = fixture.bridge_host_socket_path.parent().unwrap().to_owned();
        fixture.stop().await;
        assert!(
            !directory.exists(),
            "fixture removes its sockets and directory"
        );
    }
}

#[tokio::test]
async fn bridge_host_request_shutdown_rejects_empty_reason() {
    let fixture = BridgeHostFixture::start().await;
    let mut client = connect_bridge_host(fixture.bridge_host_socket_path.clone()).await;

    let error = client
        .request_shutdown(BridgeHostShutdownRequest {
            reason: Some(String::new()),
        })
        .await
        .expect_err("empty reason rejected");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);

    fixture.stop().await;
}

#[tokio::test]
async fn local_ping_returns_ok() {
    let fixture = HostFixture::start(None).await;
    let client = fixture.client().await;

    client.ping().await.expect("ping response");

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_host_rate_limits_guest_messages() {
    let fixture = BridgeHostFixture::start().await;
    let mut client = connect_bridge_host(fixture.bridge_host_socket_path.clone()).await;

    for _ in 0..5 {
        client
            .set_activity_state(BridgeHostSetActivityStateRequest {
                state: ActivityState::Idle.into(),
            })
            .await
            .expect("set_activity_state within rate limit");
    }

    let error = client
        .set_activity_state(BridgeHostSetActivityStateRequest {
            state: ActivityState::Idle.into(),
        })
        .await
        .expect_err("sixth guest message in one second should be rate-limited");
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);

    fixture.stop().await;
}

#[tokio::test]
async fn ping_guest_returns_unavailable_without_guest_bridge() {
    let fixture = HostFixture::start(None).await;
    let client = fixture.client().await;

    let error = client.ping_guest().await.expect_err("guest unavailable");
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(
        error
            .metadata()
            .get("spaced-relay-command")
            .and_then(|value| value.to_str().ok()),
        Some("bridge_guest_ping")
    );
    assert_eq!(
        error
            .metadata()
            .get("spaced-relay-bridge")
            .and_then(|value| value.to_str().ok()),
        Some("bridge_guest_meta")
    );

    fixture.stop().await;
}

struct CountingUnixListenerStream {
    inner: UnixListenerStream,
    accepted_connections: Arc<AtomicUsize>,
}

impl Stream for CountingUnixListenerStream {
    type Item = std::io::Result<UnixStream>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match std::pin::Pin::new(&mut this.inner).poll_next(cx) {
            std::task::Poll::Ready(Some(Ok(stream))) => {
                this.accepted_connections.fetch_add(1, Ordering::SeqCst);
                std::task::Poll::Ready(Some(Ok(stream)))
            }
            result => result,
        }
    }
}

async fn start_counting_flush_guest(
    socket_path: std::path::PathBuf,
    accepted_connections: Arc<AtomicUsize>,
    flush_calls: Arc<AtomicUsize>,
) -> tokio::task::JoinHandle<()> {
    let listener = UnixListener::bind(socket_path).expect("guest bridge socket bound");
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(BridgeGuestMetaServer::new(
                crate::mock_bridge_guest::MockBridgeGuest {
                    flush_calls,
                    ..Default::default()
                },
            ))
            .serve_with_incoming(CountingUnixListenerStream {
                inner: UnixListenerStream::new(listener),
                accepted_connections,
            })
            .await;
    })
}

async fn wait_for_connections(accepted_connections: &AtomicUsize, expected: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while accepted_connections.load(Ordering::SeqCst) < expected {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {expected} guest bridge connections"));
}

#[tokio::test]
async fn prepare_suspend_flushes_on_a_fresh_bridge_connection_and_clears_it() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_guest_socket = temp_dir.path().join("spaced-bridge-guest.sock");
    let accepted_connections = Arc::new(AtomicUsize::new(0));
    let flush_calls = Arc::new(AtomicUsize::new(0));
    let bridge_task = start_counting_flush_guest(
        bridge_guest_socket.clone(),
        accepted_connections.clone(),
        flush_calls.clone(),
    )
    .await;
    let fixture = HostFixture::start(Some(bridge_guest_socket)).await;
    let client = fixture.client().await;

    client
        .flush_guest_filesystems(Duration::from_secs(5))
        .await
        .expect("initial cached bridge guest connection");
    wait_for_connections(&accepted_connections, 1).await;
    assert_eq!(
        flush_calls.load(Ordering::SeqCst),
        1,
        "each response waits for its guest flush"
    );

    client
        .prepare_suspend(Duration::from_secs(5))
        .await
        .expect("prepare suspend response");
    wait_for_connections(&accepted_connections, 2).await;
    assert_eq!(
        flush_calls.load(Ordering::SeqCst),
        2,
        "each response waits for its guest flush"
    );

    client
        .flush_guest_filesystems(Duration::from_secs(5))
        .await
        .expect("bridge connection after prepare suspend");
    wait_for_connections(&accepted_connections, 3).await;
    assert_eq!(
        flush_calls.load(Ordering::SeqCst),
        3,
        "each response waits for its guest flush"
    );

    fixture.stop().await;
    bridge_task.abort();
}

#[tokio::test]
async fn get_status_returns_not_ready_without_guest_bridge() {
    let fixture = HostFixture::start(None).await;
    let client = fixture.client().await;

    let status = client.get_status().await.expect("status response");
    assert!(!status.ready);
    assert!(status.guest_uptime.is_none());
    assert!(status.guest_version.is_none());

    fixture.stop().await;
}

#[tokio::test]
async fn is_connected_returns_false_without_guest_bridge() {
    let fixture = HostFixture::start(None).await;
    let client = fixture.client().await;

    let response = client.is_connected().await.expect("is_connected response");
    assert!(!response.connected);

    fixture.stop().await;
}

#[tokio::test]
async fn is_connected_recovers_after_guest_bridge_starts_late() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_guest_socket_path = temp_dir.path().join("spaced-bridge-guest.sock");
    let fixture = HostFixture::start(Some(bridge_guest_socket_path.clone())).await;
    let client = fixture.client().await;

    let disconnected = client.is_connected().await.expect("is_connected response");
    assert!(!disconnected.connected);

    let bridge_task = start_mock_bridge_guest(bridge_guest_socket_path).await;
    let connected = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let response = client.is_connected().await.expect("is_connected response");
            if response.connected {
                return response;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("guest bridge did not reconnect");
    assert!(connected.connected);

    fixture.stop().await;
    bridge_task.abort();
}

#[tokio::test]
async fn readiness_requires_a_supported_guest_and_preserves_legacy_support() {
    // The common crate owns the full identity matrix; this exercises transport,
    // terminal status propagation, and readiness publication.
    for (version, family, supported) in [
        ("0.1.0", "0.1", true),
        ("0.8.0", "0.8", true),
        ("0.9.0", "0.9", false),
        ("invalid", "0.1", false),
    ] {
        let temp_dir = tempfile::tempdir().expect("temp dir created");
        let bridge_guest_socket_path = temp_dir.path().join("spaced-bridge-guest.sock");
        let wait_ready_calls = Arc::new(AtomicUsize::new(0));
        let bridge_task = ReadyWaitGuest {
            version,
            family,
            ..ReadyWaitGuest::new(wait_ready_calls.clone())
        }
        .start(bridge_guest_socket_path.clone())
        .await;
        let fixture = HostFixture::start(Some(bridge_guest_socket_path)).await;
        let client = fixture.client().await;

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            client.wait_ready(Duration::from_secs(1)),
        )
        .await
        .expect("wait_ready timed out at test level");
        if supported {
            let ready = result.expect("supported guest is ready");
            assert_eq!(ready.guest_uptime, Some(duration_from_millis(42)));
            assert_eq!(ready.mem_total_bytes, Some(8_589_934_592));
            assert_eq!(ready.vcpu_count, Some(4));
            let identity = ready.guest_version.expect("ready guest identity");
            assert_eq!(identity.version, version);
            assert_eq!(identity.compatibility_family, family);
        } else {
            assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
        }
        assert_eq!(wait_ready_calls.load(Ordering::SeqCst), 1);
        let status = client.get_status().await.unwrap();
        assert_eq!(status.ready, supported);
        assert_eq!(status.guest_version.is_some(), supported);

        fixture.stop().await;
        bridge_task.abort();
    }
}

#[tokio::test]
async fn slow_ready_guest_returns_identity_without_a_second_probe() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("guest.sock");
    let calls = Arc::new(AtomicUsize::new(0));
    let guest = ReadyWaitGuest {
        // Longer than GetStatus's readiness response budget. Real transport
        // timing exercises the mismatch without a VM or external services.
        response_delay: Duration::from_millis(200),
        ..ReadyWaitGuest::new(calls.clone())
    };
    let server = guest.start(path.clone()).await;
    let fixture = HostFixture::start(Some(path)).await;
    let client = fixture.client().await;
    let ready = client
        .wait_ready(Duration::from_secs(2))
        .await
        .expect("guest is ready");
    fixture.stop().await;
    server.abort();

    let identity = ready
        .guest_version
        .expect("readiness must include guest identity");
    pplx_space_spaced_common::validate_spaced_guest_compatibility(
        Some(&identity.version),
        Some(&identity.compatibility_family),
    )
    .expect("supported guest identity");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn concurrent_readiness_callers_share_one_guest_probe() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_guest_socket_path = temp_dir.path().join("spaced-bridge-guest.sock");
    let wait_ready_calls = Arc::new(AtomicUsize::new(0));
    let bridge_task = ReadyWaitGuest {
        response_delay: Duration::from_millis(100),
        ..ReadyWaitGuest::new(wait_ready_calls.clone())
    }
    .start(bridge_guest_socket_path.clone())
    .await;
    let fixture = HostFixture::start(Some(bridge_guest_socket_path)).await;
    let mut clients = Vec::new();
    for _ in 0..15 {
        clients.push(fixture.client().await);
    }
    let status_client = fixture.client().await;

    let waits = clients
        .into_iter()
        .map(|client| tokio::spawn(async move { client.wait_ready(Duration::from_secs(2)).await }))
        .collect::<Vec<_>>();
    tokio::time::timeout(Duration::from_secs(2), async {
        while wait_ready_calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("guest readiness probe did not start");
    let status = tokio::spawn(async move { status_client.get_status().await });
    for wait in waits {
        wait.await
            .expect("wait_ready task must not panic")
            .expect("wait_ready response");
    }
    assert!(
        status
            .await
            .expect("get_status task must not panic")
            .expect("get_status response")
            .ready
    );

    assert_eq!(
        wait_ready_calls.load(Ordering::SeqCst),
        1,
        "WaitReady and GetStatus callers must subscribe to one guest readiness probe"
    );

    fixture.stop().await;
    bridge_task.abort();
}

#[tokio::test]
async fn caller_cancellation_and_host_shutdown_release_the_guest_probe() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_guest_socket_path = temp_dir.path().join("spaced-bridge-guest.sock");
    let wait_ready_calls = Arc::new(AtomicUsize::new(0));
    let active_wait_ready_calls = Arc::new(AtomicUsize::new(0));
    let bridge_task = ReadyWaitGuest {
        active_wait_ready_calls: Some(active_wait_ready_calls.clone()),
        ready: Arc::new(AtomicBool::new(false)),
        response_delay: Duration::from_secs(30),
        ..ReadyWaitGuest::new(wait_ready_calls)
    }
    .start(bridge_guest_socket_path.clone())
    .await;
    let fixture = HostFixture::start(Some(bridge_guest_socket_path)).await;
    let client = fixture.client().await;

    let wait = tokio::spawn(async move { client.wait_ready(Duration::from_secs(3_600)).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while active_wait_ready_calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("guest readiness probe did not start");

    wait.abort();
    let _ = wait.await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while active_wait_ready_calls.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cancelled WaitReady retained its guest readiness probe");

    let client = fixture.client().await;
    let wait = tokio::spawn(async move { client.wait_ready(Duration::from_secs(3_600)).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while active_wait_ready_calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("guest readiness probe did not restart");
    fixture.stop_gracefully().await;
    assert_eq!(
        wait.await.unwrap().unwrap_err().code(),
        tonic::Code::Cancelled
    );
    bridge_task.abort();
}

#[tokio::test]
async fn wait_ready_revalidates_cached_ready_with_live_guest_bridge() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_guest_socket_path = temp_dir.path().join("spaced-bridge-guest.sock");
    let wait_ready_calls = Arc::new(AtomicUsize::new(0));
    let bridge_task = ReadyWaitGuest::new(wait_ready_calls.clone())
        .start(bridge_guest_socket_path.clone())
        .await;
    let fixture = HostFixture::start(Some(bridge_guest_socket_path)).await;
    let client = fixture.client().await;

    let ready = client
        .wait_ready(Duration::from_secs(1))
        .await
        .expect("initial wait_ready response");
    assert_eq!(ready.guest_uptime, Some(duration_from_millis(42)));
    assert_eq!(wait_ready_calls.load(Ordering::SeqCst), 1);

    bridge_task.abort();
    let _ = bridge_task.await;

    let error = tokio::time::timeout(
        Duration::from_secs(2),
        client.wait_ready(Duration::from_millis(100)),
    )
    .await
    .expect("cached wait_ready timed out at test level")
    .expect_err("cached readiness should require live guest command plane");
    // The first failure clears cached evidence; subsequent client attempts
    // wait for fresh readiness until their caller deadline.
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);

    fixture.stop().await;
}

#[tokio::test]
async fn cached_ready_revalidation_allows_transport_response_slack() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_guest_socket_path = temp_dir.path().join("spaced-bridge-guest.sock");
    let wait_ready_calls = Arc::new(AtomicUsize::new(0));
    let bridge_task = ReadyWaitGuest {
        response_delay: Duration::from_millis(20),
        ..ReadyWaitGuest::new(wait_ready_calls.clone())
    }
    .start(bridge_guest_socket_path.clone())
    .await;
    let fixture = HostFixture::start(Some(bridge_guest_socket_path)).await;
    let client = fixture.client().await;

    client
        .wait_ready(Duration::from_secs(1))
        .await
        .expect("initial wait_ready response");
    client
        .wait_ready(Duration::from_millis(100))
        .await
        .expect("cached readiness should allow transport response slack");

    assert_eq!(wait_ready_calls.load(Ordering::SeqCst), 2);

    fixture.stop().await;
    bridge_task.abort();
}

#[tokio::test]
async fn cached_ready_probe_timeout_continues_within_caller_deadline() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_guest_socket_path = temp_dir.path().join("spaced-bridge-guest.sock");
    let wait_ready_calls = Arc::new(AtomicUsize::new(0));
    let bridge_task = ReadyWaitGuest {
        response_delay: Duration::from_millis(150),
        ..ReadyWaitGuest::new(wait_ready_calls.clone())
    }
    .start(bridge_guest_socket_path.clone())
    .await;
    let fixture = HostFixture::start(Some(bridge_guest_socket_path)).await;
    let client = fixture.client().await;

    client
        .wait_ready(Duration::from_secs(1))
        .await
        .expect("initial wait_ready response");
    client
        .wait_ready(Duration::from_millis(500))
        .await
        .expect("probe timeout should fall through to the readiness wait loop");

    assert!(wait_ready_calls.load(Ordering::SeqCst) >= 3);

    fixture.stop().await;
    bridge_task.abort();
}

#[tokio::test]
async fn readiness_transport_failures_recover_within_the_original_deadline() {
    for cached in [false, true] {
        for code in [tonic::Code::Cancelled, tonic::Code::Unavailable] {
            for recovers in [true, false] {
                let temp_dir = tempfile::tempdir().expect("temp dir created");
                let socket = temp_dir.path().join("guest.sock");
                let calls = Arc::new(AtomicUsize::new(0));
                let guest = ReadyWaitGuest::new(calls.clone());
                let errors = guest.response_errors.clone();
                let bridge_task = guest.start(socket.clone()).await;
                let fixture = HostFixture::start(Some(socket)).await;
                // Observe one server-side readiness budget, without client retries.
                let config = pplx_space_spaced_host_client::SpacedHostClientConfig::new()
                    .with_socket_path(fixture.socket_path.clone())
                    .with_command_retry_policy(pplx_space_spaced_common::RetryPolicy::none());
                let _connected = fixture.client().await;
                let client =
                    pplx_space_spaced_host_client::SpacedHostClient::new_with_config(config)
                        .await
                        .unwrap();
                if cached {
                    client.wait_ready(Duration::from_secs(1)).await.unwrap();
                }
                // Persistent failures outlast the 100ms readiness budget at the
                // coordinator's existing retry cadence.
                errors
                    .lock()
                    .unwrap()
                    .extend(std::iter::repeat_n(code, if recovers { 1 } else { 100 }));
                let result = tokio::time::timeout(
                    Duration::from_secs(2),
                    client.wait_ready(Duration::from_millis(100)),
                )
                .await
                .expect("readiness exceeded its bounded wait");
                fixture.stop().await;
                bridge_task.abort();
                let _ = bridge_task.await;
                if recovers {
                    assert!(result.is_ok(), "cached={cached}, code={code:?}: {result:?}");
                } else {
                    assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
                }
                assert!(calls.load(Ordering::SeqCst) >= 2);
            }
        }
    }
}

#[tokio::test]
async fn wait_ready_does_not_return_cached_ready_when_live_guest_reports_not_ready() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_guest_socket_path = temp_dir.path().join("spaced-bridge-guest.sock");
    let wait_ready_calls = Arc::new(AtomicUsize::new(0));
    let ready = Arc::new(AtomicBool::new(true));
    let bridge_task = ReadyWaitGuest {
        ready: ready.clone(),
        ..ReadyWaitGuest::new(wait_ready_calls.clone())
    }
    .start(bridge_guest_socket_path.clone())
    .await;
    let fixture = HostFixture::start(Some(bridge_guest_socket_path)).await;
    let client = fixture.client().await;

    let initial_ready = client
        .wait_ready(Duration::from_secs(1))
        .await
        .expect("initial wait_ready response");
    assert_eq!(initial_ready.guest_uptime, Some(duration_from_millis(42)));

    ready.store(false, Ordering::SeqCst);
    let status = client.get_status().await.expect("live not-ready status");
    assert!(
        !status.ready,
        "historical readiness cannot override a live negative probe"
    );
    assert!(status.guest_version.is_none());
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        client.wait_ready(Duration::from_millis(100)),
    )
    .await
    .expect("cached wait_ready timed out at test level")
    .expect_err("cached readiness should not be returned after live not-ready probe");
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    assert!(wait_ready_calls.load(Ordering::SeqCst) >= 2);

    fixture.stop().await;
    bridge_task.abort();
}

#[tokio::test]
async fn get_status_pulls_bridge_guest_wait_ready() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_guest_socket_path = temp_dir.path().join("spaced-bridge-guest.sock");
    let wait_ready_calls = Arc::new(AtomicUsize::new(0));
    let bridge_task = ReadyWaitGuest::new(wait_ready_calls.clone())
        .start(bridge_guest_socket_path.clone())
        .await;
    let fixture = HostFixture::start(Some(bridge_guest_socket_path)).await;
    let client = fixture.client().await;

    // A live status probe may time out under scheduler contention. It must
    // converge on fresh guest evidence when the command plane responds.
    let status = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = client.get_status().await.expect("status response");
            if status.ready {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("live guest readiness observed");
    assert_eq!(status.guest_uptime, Some(duration_from_millis(42)));
    assert!(wait_ready_calls.load(Ordering::SeqCst) >= 1);

    fixture.stop().await;
    bridge_task.abort();
}

#[tokio::test]
async fn flush_forwards_foreground_and_suspend_deadlines_to_the_guest() {
    let directory = tempfile::tempdir().expect("guest directory");
    let socket = directory.path().join("guest.sock");
    let deadlines = Arc::new(std::sync::Mutex::new(Vec::new()));
    let guest = ReadyWaitGuest {
        flush_deadlines: Some(deadlines.clone()),
        ..ReadyWaitGuest::new(Arc::default())
    }
    .start(socket.clone())
    .await;
    let fixture = HostFixture::start(Some(socket)).await;
    let client = fixture.client().await;
    client
        .flush_guest_filesystems(Duration::from_secs(30))
        .await
        .expect("foreground flush");
    client
        .prepare_suspend(Duration::from_secs(180))
        .await
        .expect("suspend flush");
    let observed = deadlines.lock().unwrap().clone();
    assert_eq!(observed.len(), 2, "each flush is dispatched exactly once");
    for (remaining, budget) in observed.into_iter().zip([30, 180]) {
        assert!(remaining <= Duration::from_secs(budget));
        assert!(
            remaining > Duration::from_secs(budget - 5),
            "the caller's lifecycle budget survives both relays"
        );
    }
    fixture.stop().await;
    guest.abort();
    assert!(guest.await.expect_err("guest cancelled").is_cancelled());
}

#[tokio::test]
async fn failed_environment_dispatch_invalidates_channel_without_replaying() {
    for code in [
        tonic::Code::Unavailable,
        tonic::Code::Unknown,
        tonic::Code::Cancelled,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("guest.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let connections = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let service = crate::mock_bridge_guest::MockBridgeGuest {
            env_reply_failure: Some((calls.clone(), code)),
            ..Default::default()
        };
        let incoming = CountingUnixListenerStream {
            inner: UnixListenerStream::new(listener),
            accepted_connections: connections.clone(),
        };
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(BridgeGuestMetaServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let fixture = HostFixture::start(Some(path)).await;
        let client = fixture.client().await;
        let env = std::collections::HashMap::from([("MODE".into(), "synthetic".into())]);
        assert_eq!(
            client
                .configure_process_env(env.clone())
                .await
                .unwrap_err()
                .code(),
            code
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "no replay inside either hop"
        );
        client.configure_process_env(env).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            connections.load(Ordering::SeqCst),
            2,
            "next caller must use a new guest channel"
        );
        server.abort();
    }
}

/// The guest is tenant-controlled, so the host decodes at most a bounded Ping
/// answer: a drain-sized answer is accepted, a much larger one refused.
#[tokio::test]
async fn guest_ping_answer_is_decoded_only_up_to_its_bound() {
    for (callsites, accepted) in [(32, true), (4_000, false)] {
        let temp_dir = tempfile::tempdir().expect("temp dir created");
        let bridge_guest_socket_path = temp_dir.path().join("spaced-bridge-guest.sock");
        let bridge_task = ReadyWaitGuest {
            ping_callsites: callsites,
            ..ReadyWaitGuest::new(Arc::new(AtomicUsize::new(0)))
        }
        .start(bridge_guest_socket_path.clone())
        .await;
        let fixture = HostFixture::start(Some(bridge_guest_socket_path)).await;
        let client = fixture.client().await;

        let result = client.ping_guest().await;
        assert_eq!(
            result.is_ok(),
            accepted,
            "{callsites} callsites: {result:?}"
        );

        fixture.stop().await;
        bridge_task.abort();
    }
}
