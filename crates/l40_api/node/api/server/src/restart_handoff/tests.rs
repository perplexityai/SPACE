use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pplx_space_common_svc::space_api_route::{
    RouteIdentityRemediation, SpaceApiRouteIdentityDimension, route_identity_dimension_from_status,
    route_identity_remediation_from_status, set_route_identity_rejection_metadata,
};
use tokio_util::sync::CancellationToken;

use super::*;

fn restart_drain_status() -> Status {
    let mut status =
        Status::unavailable("sandbox-manager state changes are draining; retry on successor");
    status.metadata_mut().insert(
        pplx_space_sbx_mgr_client::RESTART_RETRY_ON_SUCCESSOR_METADATA,
        tonic::metadata::MetadataValue::from_static("true"),
    );
    status
}

/// Zero TTL: every attempt dials, preserving per-attempt semantics for
/// the single-caller tests. Single-flight coalescing has its own test.
fn test_dialer<C: Clone>() -> RestartHandoffDialer<C> {
    RestartHandoffDialer::new(Duration::ZERO)
}

fn fast_handoff_policy() -> GrpcRetryPolicy {
    GrpcRetryPolicy {
        max_retries: 16,
        initial_delay: Duration::from_millis(1),
        backoff_multiplier: 1.0,
        max_delay: Duration::from_millis(1),
        max_elapsed: Duration::from_secs(5),
        jitter_factor: 0.0,
        max_server_retry_after: Duration::from_secs(1),
    }
}

#[tokio::test]
async fn fatal_status_preserves_structured_rejection_metadata() {
    let mut rejection = Status::permission_denied("stale route identity");
    set_route_identity_rejection_metadata(
        &mut rejection,
        RouteIdentityRemediation::RefreshRoute,
        Some(SpaceApiRouteIdentityDimension::ActivationAttemptId),
    );

    let status = retry_restart_handoff(
        "SpacedMkdir",
        fast_handoff_policy(),
        RpcReplaySafety::RequiresProof,
        MetadataMap::new(),
        (),
        0usize,
        &test_dialer(),
        || std::future::ready(Ok(1usize)),
        move |_client, _request| {
            let rejection = rejection.clone();
            std::future::ready(Err::<Response<()>, _>(rejection))
        },
    )
    .await
    .expect_err("permission denial is forwarded without replay");

    assert_eq!(
        route_identity_remediation_from_status(&status),
        Some(RouteIdentityRemediation::RefreshRoute)
    );
    assert_eq!(
        route_identity_dimension_from_status(&status),
        Some(SpaceApiRouteIdentityDimension::ActivationAttemptId)
    );
}

/// Connection 0 is the shared client; `dial` hands out connections 1, 2,
/// 3, … so tests can assert exactly which connection served each call.
struct HandoffHarness {
    dials: Arc<AtomicUsize>,
    calls: Arc<std::sync::Mutex<Vec<usize>>>,
}

impl HandoffHarness {
    fn new() -> Self {
        Self {
            dials: Arc::new(AtomicUsize::new(0)),
            calls: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn dialer(&self) -> impl Fn() -> std::future::Ready<Result<usize, Status>> + Send {
        let dials = Arc::clone(&self.dials);
        move || std::future::ready(Ok(dials.fetch_add(1, Ordering::SeqCst) + 1))
    }

    fn failing_dialer(
        &self,
        failures: usize,
    ) -> impl Fn() -> std::future::Ready<Result<usize, Status>> + Send {
        let dials = Arc::clone(&self.dials);
        move || {
            let connection = dials.fetch_add(1, Ordering::SeqCst) + 1;
            std::future::ready(if connection <= failures {
                Err(Status::unavailable("dial sandbox-manager: refused"))
            } else {
                Ok(connection)
            })
        }
    }

    fn recording_call<F>(
        &self,
        respond: F,
    ) -> impl Fn(usize, Request<()>) -> std::future::Ready<Result<Response<&'static str>, Status>> + Send
    where
        F: Fn(usize) -> Result<Response<&'static str>, Status> + Send,
    {
        let calls = Arc::clone(&self.calls);
        move |connection, _request| {
            calls.lock().unwrap().push(connection);
            std::future::ready(respond(connection))
        }
    }

    fn calls(&self) -> Vec<usize> {
        self.calls.lock().unwrap().clone()
    }
}

#[tokio::test]
async fn handoff_replays_use_fresh_dials_never_the_shared_client() {
    let harness = HandoffHarness::new();

    let response = retry_restart_handoff(
        "WakeSandbox",
        fast_handoff_policy(),
        RpcReplaySafety::RequiresProof,
        MetadataMap::new(),
        (),
        0usize,
        &test_dialer(),
        harness.dialer(),
        harness.recording_call(|connection| match connection {
            // Shared client and the first fresh connection both still
            // reach the draining predecessor.
            0 | 1 => Err(restart_drain_status()),
            _ => Ok(Response::new("woken")),
        }),
    )
    .await
    .unwrap();

    assert_eq!(response.into_inner(), "woken");
    assert_eq!(harness.calls(), vec![0, 1, 2]);
}

#[tokio::test]
async fn handoff_does_not_replay_generic_unavailable() {
    let harness = HandoffHarness::new();

    let status = retry_restart_handoff(
        "WakeSandbox",
        fast_handoff_policy(),
        RpcReplaySafety::RequiresProof,
        MetadataMap::new(),
        (),
        0usize,
        &test_dialer(),
        harness.dialer(),
        harness.recording_call(|_connection| {
            Err(Status::unavailable(
                "transport error: connection reset by peer",
            ))
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert_eq!(harness.calls(), vec![0]);
    assert_eq!(harness.dials.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn handoff_stops_replaying_after_non_marker_failure_on_fresh_connection() {
    let harness = HandoffHarness::new();

    let status = retry_restart_handoff(
        "WakeSandbox",
        fast_handoff_policy(),
        RpcReplaySafety::RequiresProof,
        MetadataMap::new(),
        (),
        0usize,
        &test_dialer(),
        harness.dialer(),
        harness.recording_call(|connection| match connection {
            0 => Err(restart_drain_status()),
            _ => Err(Status::internal("wake failed on successor")),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(harness.calls(), vec![0, 1]);
}

#[tokio::test]
async fn handoff_keeps_waiting_through_dial_failures() {
    let harness = HandoffHarness::new();

    let response = retry_restart_handoff(
        "WakeSandbox",
        fast_handoff_policy(),
        RpcReplaySafety::RequiresProof,
        MetadataMap::new(),
        (),
        0usize,
        &test_dialer(),
        harness.failing_dialer(2),
        harness.recording_call(|connection| match connection {
            0 => Err(restart_drain_status()),
            _ => Ok(Response::new("woken")),
        }),
    )
    .await
    .unwrap();

    assert_eq!(response.into_inner(), "woken");
    assert_eq!(harness.calls(), vec![0, 3]);
    assert_eq!(harness.dials.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn handoff_is_bounded_by_caller_grpc_timeout() {
    let harness = HandoffHarness::new();
    let mut metadata = MetadataMap::new();
    metadata.insert("grpc-timeout", "10m".parse().unwrap());

    let status = retry_restart_handoff(
        "WakeSandbox",
        fast_handoff_policy(),
        RpcReplaySafety::RequiresProof,
        metadata,
        (),
        0usize,
        &test_dialer(),
        harness.dialer(),
        harness.recording_call(|_connection| Err(restart_drain_status())),
    )
    .await
    .unwrap_err();

    assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
}

#[tokio::test]
async fn handoff_returns_last_drain_status_when_retries_exhaust() {
    let harness = HandoffHarness::new();
    let policy = GrpcRetryPolicy {
        max_retries: 3,
        ..fast_handoff_policy()
    };

    let status = retry_restart_handoff(
        "WakeSandbox",
        policy,
        RpcReplaySafety::RequiresProof,
        MetadataMap::new(),
        (),
        0usize,
        &test_dialer(),
        harness.dialer(),
        harness.recording_call(|_connection| Err(restart_drain_status())),
    )
    .await
    .unwrap_err();

    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert!(status.message().contains("retry on successor"));
    assert_eq!(harness.calls(), vec![0, 1, 2, 3]);
}

/// A live, healthy handler can legitimately return unmarked
/// `UNAVAILABLE` (for example, a started sandbox whose spaced-host
/// socket is absent). Reads may probe once on a fresh connection in
/// case the pinned channel was the problem — but must never spend the
/// drain-sized budget re-executing a live handler.
#[tokio::test]
async fn read_only_rpcs_fail_fast_when_a_live_server_keeps_erroring() {
    let harness = HandoffHarness::new();

    let status = retry_restart_handoff(
        "SpacedGetStatus",
        fast_handoff_policy(),
        RpcReplaySafety::ReadOnly,
        MetadataMap::new(),
        (),
        0usize,
        &test_dialer(),
        harness.dialer(),
        harness.recording_call(|_connection| {
            Err(Status::unavailable(
                "spaced-host socket is not present for sandbox",
            ))
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert!(status.message().contains("spaced-host socket"));
    assert_eq!(
        harness.calls(),
        vec![0, 1],
        "an unmarked server error earns exactly one fresh-connection probe, not a drain-sized loop"
    );
}

/// Same property against a real server: a persistent unmarked
/// `UNAVAILABLE` from a live process (wire status, no marker, no local
/// error chain) must execute the handler exactly twice in total.
#[tokio::test]
async fn read_only_rpcs_fail_fast_against_a_live_erroring_server() {
    let dir = tempfile::tempdir().expect("tempdir");
    let socket_path = dir.path().join("sandbox-manager.sock");

    let server = ScriptedHealth::rejecting_with(|| {
        Status::unavailable("spaced-host socket is not present for sandbox")
    });
    serve_scripted_health(&socket_path, server.clone(), CancellationToken::new());
    let cached = loop {
        match dial_scripted_health(socket_path.clone()).await {
            Ok(client) => break client,
            Err(_) => tokio::time::sleep(Duration::from_millis(2)).await,
        }
    };

    let dial_path = socket_path.clone();
    let status = retry_restart_handoff(
        "SpacedGetStatus",
        fast_handoff_policy(),
        RpcReplaySafety::ReadOnly,
        MetadataMap::new(),
        tonic_health::pb::HealthCheckRequest {
            service: "guest-outage".into(),
        },
        cached,
        &test_dialer(),
        move || dial_scripted_health(dial_path.clone()),
        |mut client, request| async move { client.check(request).await },
    )
    .await
    .unwrap_err();

    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert_eq!(
        server.calls.load(Ordering::SeqCst),
        2,
        "original attempt plus exactly one fresh-connection probe"
    );
}

#[tokio::test]
async fn read_only_rpcs_replay_ambiguous_transport_failures_on_fresh_dials() {
    let harness = HandoffHarness::new();

    let response = retry_restart_handoff(
        "SandboxState",
        fast_handoff_policy(),
        RpcReplaySafety::ReadOnly,
        MetadataMap::new(),
        (),
        0usize,
        &test_dialer(),
        harness.dialer(),
        harness.recording_call(|connection| match connection {
            // The pinned shared channel dies mid-flight; a read is safe
            // to replay anyway, and must do so on a fresh dial.
            0 => Err(Status::unavailable(
                "transport error: connection reset by peer",
            )),
            _ => Ok(Response::new("state")),
        }),
    )
    .await
    .unwrap();

    assert_eq!(response.into_inner(), "state");
    assert_eq!(harness.calls(), vec![0, 1]);
}

/// The reviewer-requested fan-out proof: many concurrent marked callers
/// must coalesce onto single-flighted dials instead of each running its
/// own dial loop.
#[tokio::test]
async fn concurrent_marked_callers_share_single_flight_dials() {
    const CALLERS: usize = 32;

    let dials = Arc::new(AtomicUsize::new(0));
    let successor_calls = Arc::new(AtomicUsize::new(0));
    let dialer = Arc::new(RestartHandoffDialer::<usize>::new(Duration::from_millis(
        50,
    )));
    let policy = GrpcRetryPolicy {
        max_retries: 64,
        initial_delay: Duration::from_millis(5),
        backoff_multiplier: 1.0,
        max_delay: Duration::from_millis(5),
        max_elapsed: Duration::from_secs(5),
        jitter_factor: 0.0,
        max_server_retry_after: Duration::from_secs(1),
    };

    let mut callers = Vec::new();
    for _ in 0..CALLERS {
        let dials = Arc::clone(&dials);
        let successor_calls = Arc::clone(&successor_calls);
        let dialer = Arc::clone(&dialer);
        callers.push(tokio::spawn(async move {
            let dial = move || {
                let connection = dials.fetch_add(1, Ordering::SeqCst) + 1;
                // The first two dial windows fail (exit-to-bind gap),
                // then the successor owns the socket.
                std::future::ready(if connection <= 2 {
                    Err(Status::unavailable("dial sandbox-manager: refused"))
                } else {
                    Ok(connection)
                })
            };
            let successor_calls = Arc::clone(&successor_calls);
            let call = move |connection: usize, _request: Request<()>| {
                std::future::ready(if connection == 0 {
                    Err(restart_drain_status())
                } else {
                    successor_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(Response::new("woken"))
                })
            };
            retry_restart_handoff(
                "WakeSandbox",
                policy,
                RpcReplaySafety::RequiresProof,
                MetadataMap::new(),
                (),
                0usize,
                &dialer,
                dial,
                call,
            )
            .await
        }));
    }

    for caller in callers {
        let response = caller
            .await
            .expect("caller task completes")
            .expect("every marked caller succeeds on the successor");
        assert_eq!(response.into_inner(), "woken");
    }

    assert_eq!(
        successor_calls.load(Ordering::SeqCst),
        CALLERS,
        "each caller executes its own RPC exactly once"
    );
    let total_dials = dials.load(Ordering::SeqCst);
    assert!(
        total_dials <= 6,
        "dials must scale with elapsed dial windows, not callers: {total_dials}"
    );
}

/// A stalled leader dial must not head-of-line block the process: the
/// generation times out, releases the single-flight lock, and the next
/// generation reaches the live successor long before the drain-sized
/// budget.
#[tokio::test]
async fn stalled_leader_dial_does_not_block_followers() {
    const CALLERS: usize = 8;

    let dials = Arc::new(AtomicUsize::new(0));
    let dialer = Arc::new(RestartHandoffDialer::<usize>::new(Duration::from_millis(
        20,
    )));
    let policy = GrpcRetryPolicy {
        max_retries: 128,
        initial_delay: Duration::from_millis(5),
        backoff_multiplier: 1.0,
        max_delay: Duration::from_millis(5),
        max_elapsed: Duration::from_secs(1),
        jitter_factor: 0.0,
        max_server_retry_after: Duration::from_secs(1),
    };

    let started_at = tokio::time::Instant::now();
    let mut callers = Vec::new();
    for _ in 0..CALLERS {
        let dials = Arc::clone(&dials);
        let dialer = Arc::clone(&dialer);
        callers.push(tokio::spawn(async move {
            let dial = move || {
                let connection = dials.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if connection == 1 {
                        // The first generation's connect was accepted by
                        // the dying predecessor and hangs forever.
                        std::future::pending::<Result<usize, Status>>().await
                    } else {
                        Ok(connection)
                    }
                }
            };
            let call = move |connection: usize, _request: Request<()>| {
                std::future::ready(if connection == 0 {
                    Err(restart_drain_status())
                } else {
                    Ok(Response::new("woken"))
                })
            };
            retry_restart_handoff(
                "WakeSandbox",
                policy,
                RpcReplaySafety::RequiresProof,
                MetadataMap::new(),
                (),
                0usize,
                &dialer,
                dial,
                call,
            )
            .await
        }));
    }

    for caller in callers {
        caller
            .await
            .expect("caller task completes")
            .expect("followers must reach the successor despite the stalled leader dial");
    }
    assert!(
        started_at.elapsed() < Duration::from_millis(500),
        "the stalled generation must yield within its TTL, not a drain-sized budget"
    );
    assert!(dials.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn handoff_bounds_hung_dials_by_the_policy_budget() {
    let harness = HandoffHarness::new();
    let policy = GrpcRetryPolicy {
        // This test isolates the elapsed-time bound. Keep retry exhaustion
        // from winning first on fast executors.
        max_retries: usize::MAX,
        max_elapsed: Duration::from_millis(50),
        ..fast_handoff_policy()
    };

    // No grpc-timeout metadata: each dial generation is bounded by the
    // dialer TTL and the loop by the policy budget.
    let status = tokio::time::timeout(
        Duration::from_secs(5),
        retry_restart_handoff(
            "WakeSandbox",
            policy,
            RpcReplaySafety::RequiresProof,
            MetadataMap::new(),
            (),
            0usize,
            &test_dialer(),
            std::future::pending::<Result<usize, Status>>,
            harness.recording_call(|_connection| Err(restart_drain_status())),
        ),
    )
    .await
    .expect("handoff must give up within its budget")
    .unwrap_err();

    assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    assert!(
        status
            .message()
            .contains("restart handoff deadline exceeded")
    );
    assert_eq!(harness.calls(), vec![0]);
}

#[tokio::test]
async fn handoff_bounds_hung_replays_by_the_policy_budget() {
    let calls = Arc::new(AtomicUsize::new(0));
    let policy = GrpcRetryPolicy {
        max_elapsed: Duration::from_millis(50),
        ..fast_handoff_policy()
    };
    let call = {
        let calls = Arc::clone(&calls);
        move |_connection: usize, _request: Request<()>| {
            let call_index = calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if call_index == 0 {
                    Err(restart_drain_status())
                } else {
                    std::future::pending::<Result<Response<&'static str>, Status>>().await
                }
            })
                as Pin<Box<dyn Future<Output = Result<Response<&'static str>, Status>> + Send>>
        }
    };

    let status = tokio::time::timeout(
        Duration::from_secs(5),
        retry_restart_handoff(
            "WakeSandbox",
            policy,
            RpcReplaySafety::RequiresProof,
            MetadataMap::new(),
            (),
            0usize,
            &test_dialer(),
            || std::future::ready(Ok(1usize)),
            call,
        ),
    )
    .await
    .expect("handoff must give up within its budget")
    .unwrap_err();

    // The hung replay may be executing on the server, so it must
    // surface as a deadline, never as another replay.
    assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn handoff_stops_all_work_when_the_caller_drops_the_future() {
    let harness = HandoffHarness::new();

    {
        let dialer = test_dialer();
        let future = retry_restart_handoff(
            "WakeSandbox",
            fast_handoff_policy(),
            RpcReplaySafety::RequiresProof,
            MetadataMap::new(),
            (),
            0usize,
            &dialer,
            harness.dialer(),
            harness.recording_call(|_connection| Err(restart_drain_status())),
        );
        let mut future = std::pin::pin!(future);
        let poll_result = tokio::time::timeout(Duration::from_millis(20), &mut future).await;
        assert!(poll_result.is_err(), "handoff should still be retrying");
        // The future is dropped at the end of this scope — that drop is
        // the caller cancellation under test.
    }

    let calls_at_drop = harness.calls().len();
    assert!(calls_at_drop >= 1);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        harness.calls().len(),
        calls_at_drop,
        "no detached work may continue after the future is dropped"
    );
}

#[test]
fn handoff_retry_budget_spans_a_full_predecessor_drain() {
    assert!(RESTART_HANDOFF_RETRY_POLICY.max_elapsed > DEFAULT_DRAIN_TIMEOUT);

    let mut worst_case = Duration::ZERO;
    for retry_index in 0..RESTART_HANDOFF_RETRY_POLICY.max_retries {
        let Some(delay) = RESTART_HANDOFF_RETRY_POLICY.backoff_delay(retry_index) else {
            break;
        };
        worst_case += delay;
    }
    assert!(
        worst_case > RESTART_HANDOFF_RETRY_POLICY.max_elapsed,
        "retry count must exhaust after max_elapsed, not before: {worst_case:?}"
    );
}

#[derive(Clone)]
struct ScriptedHealth {
    rejection: Option<fn() -> Status>,
    calls: Arc<AtomicUsize>,
    seen_requests: Arc<std::sync::Mutex<Vec<(String, String)>>>,
}

impl ScriptedHealth {
    fn serving() -> Self {
        Self::with_rejection(None)
    }

    fn rejecting_with_marker() -> Self {
        Self::with_rejection(Some(restart_drain_status))
    }

    fn rejecting_with(rejection: fn() -> Status) -> Self {
        Self::with_rejection(Some(rejection))
    }

    fn with_rejection(rejection: Option<fn() -> Status>) -> Self {
        Self {
            rejection,
            calls: Arc::new(AtomicUsize::new(0)),
            seen_requests: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
}

#[tonic::async_trait]
impl tonic_health::pb::health_server::Health for ScriptedHealth {
    async fn check(
        &self,
        request: Request<tonic_health::pb::HealthCheckRequest>,
    ) -> Result<Response<tonic_health::pb::HealthCheckResponse>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(rejection) = self.rejection {
            return Err(rejection());
        }
        let caller = request
            .metadata()
            .get("x-test-caller")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        self.seen_requests
            .lock()
            .unwrap()
            .push((request.get_ref().service.clone(), caller));
        Ok(Response::new(tonic_health::pb::HealthCheckResponse {
            status: tonic_health::pb::health_check_response::ServingStatus::Serving as i32,
        }))
    }

    type WatchStream = Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<tonic_health::pb::HealthCheckResponse, Status>>
                + Send,
        >,
    >;

    async fn watch(
        &self,
        _request: Request<tonic_health::pb::HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        Err(Status::unimplemented("watch"))
    }
}

fn serve_scripted_health(
    socket_path: &std::path::Path,
    service: ScriptedHealth,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let listener = tokio::net::UnixListener::bind(socket_path).expect("bind test socket");
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(tonic_health::pb::health_server::HealthServer::new(service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::UnixListenerStream::new(listener),
                shutdown.cancelled_owned(),
            )
            .await
            .expect("serve test health service");
    })
}

async fn dial_scripted_health(
    socket_path: std::path::PathBuf,
) -> Result<tonic_health::pb::health_client::HealthClient<tonic::transport::Channel>, Status> {
    let endpoint =
        tonic::transport::Endpoint::try_from("http://[::]:50051").expect("static test uri");
    let channel = pplx_space_common_grpc::connect_uds_channel_with_endpoint(
        "test-sandbox-manager",
        &socket_path,
        endpoint,
    )
    .await
    .map_err(|error| Status::unavailable(format!("dial test sandbox-manager: {error}")))?;
    Ok(tonic_health::pb::health_client::HealthClient::new(channel))
}

/// A server can legitimately return connect-failure *wording* (for
/// example, proxying a guest's "connection refused"). Such a status was
/// produced by an executed handler and must never be replayed — only a
/// client-side dial failure with a typed local error chain may be.
#[tokio::test]
async fn handoff_never_replays_server_echoed_connect_failure_text() {
    let dir = tempfile::tempdir().expect("tempdir");
    let socket_path = dir.path().join("sandbox-manager.sock");

    let server = ScriptedHealth::rejecting_with(|| {
        Status::unavailable("error trying to connect: Connection refused (os error 61)")
    });
    serve_scripted_health(&socket_path, server.clone(), CancellationToken::new());
    let cached = loop {
        match dial_scripted_health(socket_path.clone()).await {
            Ok(client) => break client,
            Err(_) => tokio::time::sleep(Duration::from_millis(2)).await,
        }
    };

    let dial_path = socket_path.clone();
    let status = retry_restart_handoff(
        "SpacedStartProcess",
        fast_handoff_policy(),
        RpcReplaySafety::RequiresProof,
        MetadataMap::new(),
        tonic_health::pb::HealthCheckRequest {
            service: "echoed-text".into(),
        },
        cached,
        &test_dialer(),
        move || dial_scripted_health(dial_path.clone()),
        |mut client, request| async move { client.check(request).await },
    )
    .await
    .unwrap_err();

    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert_eq!(
        server.calls.load(Ordering::SeqCst),
        1,
        "a wire status with connect-failure wording must not be replayed"
    );
}

/// The exit-to-bind gap: the predecessor has already exited before the
/// request is sent, so no marker is ever observed — the first attempt
/// fails with a client-side dial error (typed, provably untransmitted).
/// The replay must wait out the gap and execute exactly once on the
/// successor.
#[tokio::test]
async fn handoff_replays_when_the_first_attempt_hits_a_dead_socket() {
    let dir = tempfile::tempdir().expect("tempdir");
    let socket_path = dir.path().join("sandbox-manager.sock");

    // Establish the cached channel against a predecessor, then tear it
    // down before any request is sent.
    let predecessor = ScriptedHealth::serving();
    let predecessor_shutdown = CancellationToken::new();
    let predecessor_task = serve_scripted_health(
        &socket_path,
        predecessor.clone(),
        predecessor_shutdown.clone(),
    );
    let cached = loop {
        match dial_scripted_health(socket_path.clone()).await {
            Ok(client) => break client,
            Err(_) => tokio::time::sleep(Duration::from_millis(2)).await,
        }
    };
    predecessor_shutdown.cancel();
    predecessor_task.await.expect("predecessor exits");
    std::fs::remove_file(&socket_path).expect("remove predecessor socket");

    let successor = ScriptedHealth::serving();
    let bind_successor = tokio::spawn({
        let successor = successor.clone();
        let socket_path = socket_path.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            serve_scripted_health(&socket_path, successor, CancellationToken::new());
        }
    });

    let gap_policy = GrpcRetryPolicy {
        max_retries: 64,
        initial_delay: Duration::from_millis(20),
        backoff_multiplier: 1.0,
        max_delay: Duration::from_millis(20),
        max_elapsed: Duration::from_secs(10),
        jitter_factor: 0.0,
        max_server_retry_after: Duration::from_secs(1),
    };
    let dial_path = socket_path.clone();
    let response = retry_restart_handoff(
        "TouchSandboxAutoLifecycle",
        gap_policy,
        RpcReplaySafety::RequiresProof,
        MetadataMap::new(),
        tonic_health::pb::HealthCheckRequest {
            service: "exit-to-bind-gap".into(),
        },
        cached,
        &test_dialer(),
        move || dial_scripted_health(dial_path.clone()),
        |mut client, request| async move { client.check(request).await },
    )
    .await
    .expect("gap replay succeeds on the successor");

    bind_successor.await.expect("successor binds");
    assert_eq!(
        response.into_inner().status,
        tonic_health::pb::health_check_response::ServingStatus::Serving as i32
    );
    assert_eq!(
        predecessor.calls.load(Ordering::SeqCst),
        0,
        "the predecessor exited before the request was ever sent"
    );
    assert_eq!(successor.calls.load(Ordering::SeqCst), 1);
}

/// Simulates the deploy handoff end to end: a predecessor server that
/// rejects with the pre-dispatch marker is torn down mid-retry and a
/// successor binds the same socket path. The replay must reach the
/// successor exactly once, with metadata and body intact, without ever
/// succeeding against the predecessor.
#[tokio::test]
async fn handoff_reaches_the_successor_exactly_once_across_a_socket_swap() {
    let dir = tempfile::tempdir().expect("tempdir");
    let socket_path = dir.path().join("sandbox-manager.sock");

    let predecessor = ScriptedHealth::rejecting_with_marker();
    let predecessor_shutdown = CancellationToken::new();
    let predecessor_task = serve_scripted_health(
        &socket_path,
        predecessor.clone(),
        predecessor_shutdown.clone(),
    );
    let cached = loop {
        match dial_scripted_health(socket_path.clone()).await {
            Ok(client) => break client,
            Err(_) => tokio::time::sleep(Duration::from_millis(2)).await,
        }
    };

    // Swap processes once the predecessor has rejected the shared-client
    // attempt and at least one fresh-dial replay, while the retry loop is
    // sleeping its backoff.
    let successor = ScriptedHealth::serving();
    let swap = tokio::spawn({
        let predecessor_calls = Arc::clone(&predecessor.calls);
        let successor = successor.clone();
        let socket_path = socket_path.clone();
        async move {
            while predecessor_calls.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            predecessor_shutdown.cancel();
            predecessor_task.await.expect("predecessor exits");
            std::fs::remove_file(&socket_path).expect("remove predecessor socket");
            serve_scripted_health(&socket_path, successor, CancellationToken::new());
        }
    });

    let swap_window_policy = GrpcRetryPolicy {
        max_retries: 64,
        initial_delay: Duration::from_millis(50),
        backoff_multiplier: 1.0,
        max_delay: Duration::from_millis(50),
        max_elapsed: Duration::from_secs(10),
        jitter_factor: 0.0,
        max_server_retry_after: Duration::from_secs(1),
    };
    let mut metadata = MetadataMap::new();
    metadata.insert("x-test-caller", "node-api".parse().unwrap());
    let dial_path = socket_path.clone();
    let response = retry_restart_handoff(
        "Check",
        swap_window_policy,
        RpcReplaySafety::RequiresProof,
        metadata,
        tonic_health::pb::HealthCheckRequest {
            service: "wake-sandbox".into(),
        },
        cached,
        &test_dialer(),
        move || dial_scripted_health(dial_path.clone()),
        |mut client, request| async move { client.check(request).await },
    )
    .await
    .expect("handoff replay succeeds on the successor");

    swap.await.expect("swap completes");
    assert_eq!(
        response.into_inner().status,
        tonic_health::pb::health_check_response::ServingStatus::Serving as i32
    );
    assert!(predecessor.calls.load(Ordering::SeqCst) >= 2);
    assert_eq!(successor.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        successor.seen_requests.lock().unwrap().clone(),
        vec![("wake-sandbox".to_owned(), "node-api".to_owned())]
    );
}
