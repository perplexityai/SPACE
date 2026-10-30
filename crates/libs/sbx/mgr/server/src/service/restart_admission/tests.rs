use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::*;

const SANDBOX_MANAGER_PROTO: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../../../proto/sandbox/manager/v1/manager.proto"
));
const HOST_CONTROL_PROTO: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../../../proto/spaced/host_control/v1/host_control.proto"
));

fn test_service() -> SandboxManagerService {
    let connection = crate::db::test_conn();
    SandboxManagerService::for_test(connection)
}

#[tokio::test]
async fn retained_writer_capacity_preserves_cleanup_progress_and_releases_slots() {
    let manager = test_service();
    let mut leases = (0..super::super::drain::MAX_ACTIVE_STATE_WRITERS)
        .map(|_| manager.state_writer_lease("BoundedTest").unwrap())
        .collect::<Vec<_>>();
    let error = match manager.state_writer_lease("Overflow") {
        Ok(_) => panic!("retained mutations must be bounded"),
        Err(error) => error,
    };
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    // Foreground callers may be waiting for durable cleanup. Its separately
    // bounded executor and emergency teardown must not share that starvation.
    let cleanup = manager.state_writer_lease("AsyncCleanup").unwrap();
    let kill = manager
        .state_writer_lease("/space.sandbox.manager.v1.SandboxManager/Kill")
        .unwrap();
    drop(cleanup);
    drop(kill);
    leases.pop();
    let replacement = manager.state_writer_lease("Replacement").unwrap();
    drop(leases);
    drop(replacement);
    assert_eq!(manager.active_state_transition_count(), 0);
}

#[tokio::test]
async fn critical_worker_exit_preserves_failure_status_through_shutdown() {
    for (expected_exit, panic) in [(true, false), (false, false), (false, true)] {
        let manager = test_service();
        let expected = tokio_util::sync::CancellationToken::new();
        if expected_exit {
            expected.cancel();
        }
        let guard = manager.critical_task_guard("test worker", &expected);
        let task = tokio::spawn(async move {
            let _guard = guard;
            assert!(!panic, "injected critical worker panic");
        });
        assert_eq!(task.await.is_err(), panic);
        assert_eq!(manager.shutdown_token.is_cancelled(), !expected_exit);
        assert_eq!(manager.critical_worker_failed(), !expected_exit);
    }
}

fn assert_proto_methods_classified(proto: &str, service: &str) -> usize {
    let mut count = 0;
    for line in proto.lines() {
        let Some(declaration) = line.trim().strip_prefix("rpc ") else {
            continue;
        };
        let method = declaration.split('(').next().unwrap();
        let path = format!("/{service}/{method}");
        path.parse::<RestartRpc>()
            .unwrap_or_else(|error| panic!("{error}"));
        count += 1;
    }
    count
}

#[test]
fn every_served_rpc_has_explicit_restart_admission() {
    let sandbox_methods = assert_proto_methods_classified(
        SANDBOX_MANAGER_PROTO,
        "space.sandbox.manager.v1.SandboxManager",
    );
    let host_methods = assert_proto_methods_classified(
        HOST_CONTROL_PROTO,
        "space.spaced.host.control.v1.HostControl",
    );

    assert_eq!(RestartRpc::ALL.len(), sandbox_methods + host_methods + 2);
}

#[test]
fn every_server_streaming_rpc_is_handoff_cancellable() {
    for line in SANDBOX_MANAGER_PROTO.lines() {
        let Some(declaration) = line.trim().strip_prefix("rpc ") else {
            continue;
        };
        let method = declaration.split('(').next().unwrap();
        let path = format!("/space.sandbox.manager.v1.SandboxManager/{method}");
        let rpc = path
            .parse::<RestartRpc>()
            .unwrap_or_else(|error| panic!("{error}"));
        let server_streaming = declaration.contains("returns (stream ");

        assert_eq!(
            rpc.handoff_cancellable_response_stream_name(),
            server_streaming.then_some(rpc.as_str()),
            "{path}"
        );
    }

    assert!(
        RestartRpc::HealthWatch
            .handoff_cancellable_response_stream_name()
            .is_some(),
        "health Watch is server streaming"
    );
    assert!(
        RestartRpc::HealthCheck
            .handoff_cancellable_response_stream_name()
            .is_none(),
        "health Check is unary"
    );
}

#[tokio::test]
async fn unclassified_rpc_cannot_serve_before_restart_drain() {
    let manager = test_service();
    let called = Arc::new(AtomicBool::new(false));
    let mut service = manager.restart_admission_layer().layer(tower::service_fn({
        let called = Arc::clone(&called);
        move |_request: Request<()>| {
            called.store(true, Ordering::Release);
            async { Ok::<_, Infallible>(Response::new(tonic::body::Body::default())) }
        }
    }));

    let response = service
        .call(
            Request::builder()
                .uri("/space.sandbox.manager.v1.SandboxManager/FutureMethod")
                .body(())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response
            .headers()
            .get("grpc-status")
            .and_then(|value| value.to_str().ok()),
        Some("12")
    );
    assert!(!called.load(Ordering::Acquire));
}

#[tokio::test]
async fn restart_drain_rejects_writers_but_serves_both_read_classes() {
    let manager = test_service();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut service = manager.restart_admission_layer().layer(tower::service_fn({
        let calls = Arc::clone(&calls);
        move |_request: Request<()>| {
            calls.fetch_add(1, Ordering::AcqRel);
            async { Ok::<_, Infallible>(Response::new(tonic::body::Body::default())) }
        }
    }));
    manager.begin_draining_for_restart();

    for path in [
        "/space.sandbox.manager.v1.SandboxManager/List",
        "/space.sandbox.manager.v1.SandboxManager/SpacedStat",
    ] {
        let response = service
            .call(Request::builder().uri(path).body(()).unwrap())
            .await
            .unwrap();
        assert_ne!(
            response
                .headers()
                .get("grpc-status")
                .and_then(|value| value.to_str().ok()),
            Some("14")
        );
    }
    for path in [
        "/space.sandbox.manager.v1.SandboxManager/Stats",
        "/space.sandbox.manager.v1.SandboxManager/EvictClusterDeletedSandbox",
        "/space.spaced.host.control.v1.HostControl/SetActivityState",
        "/space.spaced.host.control.v1.HostControl/IssueGatewayIntermediateCertificate",
    ] {
        let response = service
            .call(Request::builder().uri(path).body(()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response
                .headers()
                .get("grpc-status")
                .and_then(|value| value.to_str().ok()),
            Some("14")
        );
        assert_eq!(
            response
                .headers()
                .get(pplx_space_sbx_mgr_client::RESTART_RETRY_ON_SUCCESSOR_METADATA)
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
    }
    assert_eq!(calls.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn state_writer_is_tracked_until_handler_returns() {
    let manager = test_service();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut service = manager.restart_admission_layer().layer(tower::service_fn({
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        move |_request: Request<()>| {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            async move {
                started.notify_one();
                release.notified().await;
                Ok::<_, Infallible>(Response::new(tonic::body::Body::default()))
            }
        }
    }));
    let request = Request::builder()
        .uri("/space.sandbox.manager.v1.SandboxManager/Start")
        .body(())
        .unwrap();

    let call = tokio::spawn(async move { service.call(request).await });
    started.notified().await;

    assert_eq!(manager.active_state_transition_count(), 1);
    release.notify_one();
    call.await.unwrap().unwrap();
    assert_eq!(manager.active_state_transition_count(), 0);
}

#[tokio::test]
async fn cancelling_request_keeps_admitted_mutation_owned_until_completion() {
    let manager = test_service();
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let completed = Arc::new(AtomicBool::new(false));
    let mut service = manager.restart_admission_layer().layer(tower::service_fn({
        let lock = lock.clone();
        let started = started.clone();
        let release = release.clone();
        let completed = completed.clone();
        move |_request: Request<()>| {
            let lock = lock.clone();
            let started = started.clone();
            let release = release.clone();
            let completed = completed.clone();
            async move {
                let _owned = wait_for_activation_admission("owned", lock.lock_owned())
                    .await
                    .unwrap();
                started.notify_one();
                release.notified().await;
                completed.store(true, Ordering::Release);
                Ok::<_, Infallible>(Response::new(tonic::body::Body::default()))
            }
        }
    }));
    let call = tokio::spawn(async move {
        service
            .call(
                Request::builder()
                    .uri("/space.sandbox.manager.v1.SandboxManager/Resume")
                    .body(())
                    .unwrap(),
            )
            .await
    });
    started.notified().await;
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    manager.begin_draining_for_restart();
    assert_eq!(manager.active_state_transition_count(), 1);
    lock.try_lock()
        .expect_err("admitted work must retain exclusion");
    assert!(!completed.load(Ordering::Acquire));
    release.notify_one();
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        manager.wait_for_state_transitions_drained(),
    )
    .await
    .unwrap();
    assert!(completed.load(Ordering::Acquire));
    drop(
        lock.try_lock()
            .expect("completed work must release exclusion"),
    );
}

#[tokio::test(start_paused = true)]
async fn cancelled_activation_retries_release_admission_without_releasing_the_lock_owner() {
    use crate::proto::{SpaceApiRouteIdentity, StartRequest, WakeRequest, WakeStartContext};
    use crate::service::activation_support::internal_request;
    use crate::service::tests::service_tests::insert_test_sandbox;
    use crate::state::SandboxState;
    use pplx_space_common_core::types::{NodeId, SandboxActivationAttemptId, SandboxId};

    enum LockSite {
        Staging,
        Artifact,
        Wake,
    }
    for (rpc, state, site) in [
        (RestartRpc::SandboxStart, None, LockSite::Staging),
        (RestartRpc::SandboxWake, None, LockSite::Staging),
        (
            RestartRpc::SandboxStart,
            Some(SandboxState::Suspended),
            LockSite::Artifact,
        ),
        (
            RestartRpc::SandboxWake,
            Some(SandboxState::Paused),
            LockSite::Wake,
        ),
        (
            RestartRpc::SandboxWake,
            Some(SandboxState::Paused),
            LockSite::Artifact,
        ),
    ] {
        let mut manager = test_service();
        let node_id = NodeId::new();
        manager.local_node_id = Some(node_id);
        let sandbox_id = SandboxId::new().to_string();
        if let Some(state) = state {
            insert_test_sandbox(&manager.db.lock().unwrap(), &sandbox_id, state);
        }
        let lock = match site {
            LockSite::Staging => manager.auto_lifecycle.staging_lock(&sandbox_id),
            LockSite::Artifact => manager.auto_lifecycle.artifact_lock(&sandbox_id),
            LockSite::Wake => manager.auto_lifecycle.wake_lock(&sandbox_id),
        };
        let held = lock.lock().await;
        let identity = SpaceApiRouteIdentity {
            sandbox_id: Some(sandbox_id.clone()),
            route_generation: Some(8),
            execution_generation: Some(8),
            current_node_id: Some(node_id.to_string()),
            activation_attempt_id: Some(SandboxActivationAttemptId::new().to_string()),
            substrate_kind: crate::proto::SandboxSubstrateKind::Kubernetes as i32,
            pod_uid: Some(uuid::Uuid::now_v7().to_string()),
        };
        // Each retry must leave the writer set while the original holder stays
        // blocked. Releasing that holder to make the assertion pass would hide
        // the abandoned-waiter regression.
        for _ in 0..3 {
            let waiting = Arc::new(tokio::sync::Notify::new());
            let (outcome_tx, mut outcome_rx) = tokio::sync::mpsc::channel(1);
            let mut service = manager.restart_admission_layer().layer(tower::service_fn({
                let manager = manager.clone();
                let sandbox_id = sandbox_id.clone();
                let identity = identity.clone();
                let waiting = waiting.clone();
                move |_request: Request<()>| {
                    let manager = manager.clone();
                    let sandbox_id = sandbox_id.clone();
                    let identity = identity.clone();
                    let waiting = waiting.clone();
                    let outcome_tx = outcome_tx.clone();
                    async move {
                        let operation = async {
                            match rpc {
                                RestartRpc::SandboxStart => {
                                    Box::pin(manager.start(internal_request(StartRequest {
                                        sandbox_id,
                                        checkpoint: Some("suspend".to_owned()),
                                        route_identity: Some(identity),
                                        ..Default::default()
                                    })))
                                    .await
                                    .map(|_| ())
                                }
                                RestartRpc::SandboxWake => {
                                    Box::pin(manager.wake(internal_request(WakeRequest {
                                        implicit_wake_source: 0,
                                        sandbox_id,
                                        start_context: Some(WakeStartContext {
                                            route_identity: Some(identity),
                                            netns_path: Some(
                                                "/run/netns/activation-admission-test".to_owned(),
                                            ),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    })))
                                    .await
                                    .map(|_| ())
                                }
                                _ => unreachable!(),
                            }
                        };
                        tokio::pin!(operation);
                        let first_poll = futures_util::poll!(&mut operation);
                        assert!(
                            first_poll.is_pending(),
                            "{rpc:?} failed before the lock: {first_poll:?}"
                        );
                        waiting.notify_one();
                        outcome_tx.send(operation.await).await.unwrap();
                        Ok::<_, Infallible>(Response::new(tonic::body::Body::default()))
                    }
                }
            }));
            let call = tokio::spawn(async move {
                service
                    .call(Request::builder().uri(rpc.as_str()).body(()).unwrap())
                    .await
            });
            tokio::time::timeout(std::time::Duration::from_secs(1), waiting.notified())
                .await
                .expect("handler must reach restore admission");
            call.abort();
            assert!(call.await.unwrap_err().is_cancelled());
            let error = tokio::time::timeout(std::time::Duration::from_secs(1), outcome_rx.recv())
                .await
                .expect("a disconnected restore waiter must stop without acquiring the lock")
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::Cancelled);
            manager.wait_for_state_transitions_drained().await;
            assert_eq!(manager.active_state_transition_count(), 0);
            lock.try_lock()
                .expect_err("cancelling a waiter must not release its owner");
            match state {
                Some(state) => {
                    assert_eq!(manager.lookup_sandbox(&sandbox_id).unwrap().state, state)
                }
                None => assert_eq!(
                    manager.lookup_sandbox(&sandbox_id).err().unwrap().code(),
                    tonic::Code::NotFound
                ),
            }
        }
        drop(held);
        drop(
            lock.try_lock()
                .expect("the original owner can release normally"),
        );
    }
}

#[tokio::test(start_paused = true)]
async fn activation_admission_bounds_live_waiters_and_rejects_cancelled_ready_work() {
    let lock = tokio::sync::Mutex::new(());
    let held = lock.lock().await;
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(6),
        wait_for_activation_admission("contended", lock.lock()),
    )
    .await
    .expect("live callers must get a bounded contention response")
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    drop(held);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let error = CALLER_CANCELLED
        .scope(
            cancelled,
            wait_for_activation_admission("cancelled", lock.lock()),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Cancelled);
    // Cancellation must win even when the lock is immediately available.
    drop(
        lock.try_lock()
            .expect("cancelled admission must not acquire"),
    );

    let child_lock = Arc::new(tokio::sync::Mutex::new(()));
    let cancelled = CancellationToken::new();
    let child = CALLER_CANCELLED.sync_scope(cancelled.clone(), || {
        tokio::spawn(inherit_admission_cancellation(async move {
            wait_for_activation_admission("owned_child", child_lock.lock_owned()).await
        }))
    });
    cancelled.cancel();
    assert_eq!(
        child.await.unwrap().unwrap_err().code(),
        tonic::Code::Cancelled
    );

    let held = lock.lock().await;
    let mut admission = Box::pin(wait_for_activation_admission("briefly_busy", lock.lock()));
    assert!(futures_util::poll!(&mut admission).is_pending());
    drop(held);
    drop(admission.await.unwrap());
    drop(lock.try_lock().expect("a completed admission must release"));
}

#[tokio::test(start_paused = true)]
async fn blocking_wake_without_client_deadline_has_bounded_passive_wait() {
    use crate::service::tests::service_tests::insert_test_sandbox;
    use crate::state::SandboxState;
    use pplx_space_common_core::types::SandboxId;

    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &sandbox_id, SandboxState::Starting);
    let manager = SandboxManagerService::for_test(conn);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(31),
        manager.wake_with_start_config(sandbox_id.clone(), None, Default::default(), true, None),
    )
    .await
    .expect("observing another activation must not wait forever without grpc-timeout");
    assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
    assert_eq!(
        manager.lookup_sandbox(&sandbox_id).unwrap().state,
        SandboxState::Starting
    );
    drop(
        manager
            .auto_lifecycle
            .wake_lock(&sandbox_id)
            .try_lock()
            .expect("passive wait timeout must release wake exclusion"),
    );
}

#[tokio::test]
async fn admitted_finalizer_inherits_ownership_after_drain_closes_admission() {
    let manager = test_service();
    let started = Arc::new(tokio::sync::Notify::new());
    let spawn_child = Arc::new(tokio::sync::Notify::new());
    let finish_child = Arc::new(tokio::sync::Notify::new());
    let mut service = manager.restart_admission_layer().layer(tower::service_fn({
        let manager = manager.clone();
        let started = started.clone();
        let spawn_child = spawn_child.clone();
        let finish_child = finish_child.clone();
        move |_request: Request<()>| {
            let manager = manager.clone();
            let started = started.clone();
            let spawn_child = spawn_child.clone();
            let finish_child = finish_child.clone();
            async move {
                started.notify_one();
                spawn_child.notified().await;
                manager
                    .spawn_state_writer("TestFinalizer", async move {
                        finish_child.notified().await;
                    })
                    .unwrap();
                Ok::<_, Infallible>(Response::new(tonic::body::Body::default()))
            }
        }
    }));
    let call = tokio::spawn(async move {
        service
            .call(
                Request::builder()
                    .uri("/space.sandbox.manager.v1.SandboxManager/Start")
                    .body(())
                    .unwrap(),
            )
            .await
    });
    started.notified().await;
    manager.begin_draining_for_restart();
    assert!(manager.state_writer_lease("UnrelatedWriter").is_err());
    spawn_child.notify_one();
    call.await.unwrap().unwrap();
    assert_eq!(manager.active_state_transition_count(), 1);
    finish_child.notify_one();
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        manager.wait_for_state_transitions_drained(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn disconnected_caller_abandons_a_process_start_still_waiting_on_the_guest() {
    struct StartDropped(Arc<AtomicBool>);
    impl Drop for StartDropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let manager = test_service();
    let started = Arc::new(tokio::sync::Notify::new());
    let start_dropped = Arc::new(AtomicBool::new(false));
    let outcome = Arc::new(std::sync::Mutex::new(None));
    let mut service = manager.restart_admission_layer().layer(tower::service_fn({
        let started = started.clone();
        let start_dropped = start_dropped.clone();
        let outcome = outcome.clone();
        move |_request: Request<()>| {
            let started = started.clone();
            let start_dropped = start_dropped.clone();
            let outcome = outcome.clone();
            async move {
                // Stands in for a start queued at spaced-host admission.
                let start = async move {
                    let _guard = StartDropped(start_dropped);
                    std::future::pending::<Result<(), tonic::Status>>().await
                };
                started.notify_one();
                let result = abandon_start_if_caller_cancelled(start).await;
                *outcome.lock().unwrap() = Some(matches!(result, GuestStartOutcome::CallerGone));
                Ok::<_, Infallible>(Response::new(tonic::body::Body::default()))
            }
        }
    }));
    let call = tokio::spawn(async move {
        service
            .call(
                Request::builder()
                    .uri("/space.sandbox.manager.v1.SandboxManager/SpacedStartProcess")
                    .body(())
                    .unwrap(),
            )
            .await
    });
    started.notified().await;
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        manager.wait_for_state_transitions_drained(),
    )
    .await
    .expect("an abandoned start must not keep its writer lease");
    assert!(start_dropped.load(Ordering::Acquire));
    assert_eq!(
        *outcome.lock().unwrap(),
        Some(true),
        "a disconnected caller is reported apart from guest start failures"
    );
}

#[tokio::test]
async fn guest_start_failures_are_reported_as_failures_not_caller_disconnects() {
    let failed = CALLER_CANCELLED
        .scope(
            CancellationToken::new(),
            abandon_start_if_caller_cancelled(async {
                Err::<(), _>(tonic::Status::unavailable("guest gone"))
            }),
        )
        .await;
    assert!(
        matches!(failed, GuestStartOutcome::Failed(status) if status.code() == tonic::Code::Unavailable)
    );
    let internal = abandon_start_if_caller_cancelled(async { Ok::<_, tonic::Status>(8) }).await;
    assert!(matches!(internal, GuestStartOutcome::Opened(8)));
}

/// A passive observation, such as a retried activation's question to the
/// control plane, ends as soon as its caller disconnects, even mid-flight,
/// instead of running until it gives up; a live caller gets the answer.
#[tokio::test(start_paused = true)]
async fn observation_ends_with_a_disconnected_caller() {
    let cancelled = CancellationToken::new();
    let (started, observing) = tokio::sync::oneshot::channel();
    let observation = tokio::spawn(CALLER_CANCELLED.scope(cancelled.clone(), async move {
        observe_unless_caller_cancelled("disconnected", async move {
            let _ = started.send(());
            std::future::pending::<()>().await
        })
        .await
    }));
    observing.await.expect("the observation starts");
    cancelled.cancel();
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), observation)
        .await
        .expect("the observation ends when its caller disconnects")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Cancelled);

    let answer = CALLER_CANCELLED
        .scope(
            CancellationToken::new(),
            observe_unless_caller_cancelled("live", std::future::ready(7)),
        )
        .await
        .expect("a live caller gets the observation");
    assert_eq!(answer, 7);
}
