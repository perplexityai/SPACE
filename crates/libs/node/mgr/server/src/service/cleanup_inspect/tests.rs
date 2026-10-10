use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[tokio::test]
async fn every_failed_stop_preserves_the_active_lease_and_reservation() {
    use crate::runtime_supervisor_client::tests::{stop_failure_client, stop_supervisor_server};
    for code in [
        tonic::Code::Cancelled,
        tonic::Code::Unknown,
        tonic::Code::DeadlineExceeded,
        tonic::Code::Unavailable,
        tonic::Code::ResourceExhausted,
        tonic::Code::Internal,
        tonic::Code::PermissionDenied,
        tonic::Code::Unimplemented,
    ] {
        let (_socket, client, server) = stop_failure_client(code).await;
        let mut service = crate::service::tests::service_tests::service_for_unit_tests().await;
        service.runtime_supervisor = client;
        service.pool.set_all_slots_ready_for_test();
        let id = SandboxId::new();
        let execution_fence = pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
            None,
        );
        // Staging can fail before Allocate publishes node authority. Cleanup
        // must forward the explicit request identity, not an unfenced stop.
        assert_eq!(
            service
                .run_cleanup_sandbox_operation(
                    id,
                    CleanupSandboxRequest {
                        execution_fence: Some(execution_fence.into()),
                        sandbox_id: id.to_string(),
                        force: true,
                        ..Default::default()
                    },
                    None
                )
                .await
                .unwrap_err()
                .code(),
            code
        );
        assert!(
            service
                .pool
                .execution_fence(&id.to_string())
                .unwrap()
                .is_none()
        );
        service
            .pool
            .bind_execution(&id.to_string(), execution_fence)
            .unwrap();
        service
            .pool
            .allocate(
                &id.to_string(),
                None,
                Some(crate::slot_pool::NewLeaseMemoryReservation {
                    memory_reservation_mib: 1024,
                }),
            )
            .await
            .unwrap();
        let before = service.pool.all_records().unwrap();
        let error = service
            .run_cleanup_sandbox_operation(
                id,
                CleanupSandboxRequest {
                    execution_fence: Some(execution_fence.into()),
                    sandbox_id: id.to_string(),
                    force: true,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), code);
        let mut release = Request::new(ReleaseRequest {
            execution_fence: Some(execution_fence.into()),
            sandbox_id: id.to_string(),
            force: true,
        });
        release
            .extensions_mut()
            .insert(pplx_space_common_svc::PeerIdentity::new(0, 0, None));
        assert_eq!(service.release(release).await.unwrap_err().code(), code);
        let after = service.pool.all_records().unwrap();
        assert_eq!(after[0].state, before[0].state);
        assert_eq!(after[0].info.generation, before[0].info.generation);
        assert_eq!(after[0].info.sandbox_id, id.to_string());
        assert_eq!(after[0].memory_reservation_mib, Some(1024));
        stop_supervisor_server(server).await;
    }
}

#[rstest::rstest]
#[case("019d84bd-280d-7331-99c6-b63377f3a3a2")]
#[case("019d84bd280d733199c6b63377f3a3a2")]
fn cleanup_request_uses_the_canonical_sandbox_id(#[case] raw_sandbox_id: &str) {
    let request = CleanupSandboxRequest {
        execution_fence: None,
        sandbox_id: raw_sandbox_id.to_string(),
        ..CleanupSandboxRequest::default()
    };

    let (sandbox_id, request) = parse_cleanup_request(request).unwrap();

    assert_eq!(
        sandbox_id.to_string(),
        "019d84bd-280d-7331-99c6-b63377f3a3a2"
    );
    assert_eq!(request.sandbox_id, sandbox_id.to_string());
}

#[test]
fn cleanup_claim_stays_registered_until_result_publication() {
    let operations = Arc::new(dashmap::DashMap::new());
    let sandbox_id = "019d84bd-280d-7331-99c6-b63377f3a3a2"
        .parse::<SandboxId>()
        .unwrap();
    let request = CleanupSandboxRequest {
        execution_fence: None,
        sandbox_id: sandbox_id.to_string(),
        ..CleanupSandboxRequest::default()
    };
    let (_result_tx, result_rx) = tokio::sync::watch::channel(None);
    operations.insert(
        sandbox_id,
        InFlightCleanupOperation {
            request,
            result: result_rx,
            started_at: Instant::now(),
        },
    );
    let claim = CleanupOperationClaim {
        operations: Arc::clone(&operations),
        sandbox_id,
        armed: true,
    };

    claim.complete(|| {
        assert!(
            operations.contains_key(&sandbox_id),
            "a retry must not claim the sandbox before the incumbent result is visible"
        );
    });

    assert!(!operations.contains_key(&sandbox_id));
}

#[tokio::test]
async fn cleanup_operation_survives_a_cancelled_waiter_and_deduplicates_its_retry() {
    let task_tracker = tokio_util::task::TaskTracker::new();
    let registry = CleanupOperationRegistry::new(task_tracker.clone());
    let sandbox_id = "019d84bd-280d-7331-99c6-b63377f3a3a2"
        .parse::<SandboxId>()
        .unwrap();
    let request = CleanupSandboxRequest {
        execution_fence: None,
        sandbox_id: sandbox_id.to_string(),
        force: true,
        ..CleanupSandboxRequest::default()
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));

    let first = tokio::spawn({
        let registry = registry.clone();
        let request = request.clone();
        let calls = Arc::clone(&calls);
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        async move {
            registry
                .run(sandbox_id, request, move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    started.add_permits(1);
                    release.acquire().await.unwrap().forget();
                    Ok(CleanupSandboxResponse::default())
                })
                .await
        }
    });
    started.acquire().await.unwrap().forget();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    task_tracker.close();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), task_tracker.wait())
            .await
            .is_err(),
        "service shutdown must wait for the detached cleanup mutation"
    );

    let mut retry = tokio::spawn({
        let registry = registry.clone();
        let calls = Arc::clone(&calls);
        async move {
            registry
                .run(sandbox_id, request, move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(CleanupSandboxResponse::default())
                })
                .await
        }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut retry)
            .await
            .is_err(),
        "the retry must join the blocked operation rather than run its closure"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    release.add_permits(1);
    retry.await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    tokio::time::timeout(Duration::from_secs(1), task_tracker.wait())
        .await
        .expect("the task tracker should drain after cleanup completes");
}

#[tokio::test]
async fn cleanup_owner_waits_for_result_while_duplicate_waiter_is_bounded() {
    let task_tracker = tokio_util::task::TaskTracker::new();
    let registry = CleanupOperationRegistry::with_waiter_timeout(
        task_tracker.clone(),
        Duration::from_millis(20),
    );
    let sandbox_id = "019d84bd-280d-7331-99c6-b63377f3a3a2"
        .parse::<SandboxId>()
        .unwrap();
    let request = CleanupSandboxRequest {
        execution_fence: None,
        sandbox_id: sandbox_id.to_string(),
        force: true,
        ..CleanupSandboxRequest::default()
    };
    let started = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let owner = tokio::spawn({
        let registry = registry.clone();
        let request = request.clone();
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        async move {
            registry
                .run(sandbox_id, request, move || async move {
                    started.add_permits(1);
                    release.acquire().await.unwrap().forget();
                    Ok(CleanupSandboxResponse::default())
                })
                .await
        }
    });
    started.acquire().await.unwrap().forget();

    let duplicate = registry
        .run(sandbox_id, request, || async {
            panic!("a duplicate request must join the incumbent operation")
        })
        .await
        .unwrap_err();
    assert_eq!(duplicate.code(), tonic::Code::ResourceExhausted);
    assert!(
        !owner.is_finished(),
        "the operation owner must keep waiting"
    );

    release.add_permits(1);
    owner.await.unwrap().unwrap();
    task_tracker.close();
    task_tracker.wait().await;
}

#[tokio::test]
async fn cleanup_shutdown_releases_waiter_and_drains_owned_mutation() {
    let task_tracker = tokio_util::task::TaskTracker::new();
    let registry = CleanupOperationRegistry::new(task_tracker.clone());
    let sandbox_id = "019d84bd-280d-7331-99c6-b63377f3a3a2"
        .parse::<SandboxId>()
        .unwrap();
    let request = CleanupSandboxRequest {
        execution_fence: None,
        sandbox_id: sandbox_id.to_string(),
        force: true,
        ..CleanupSandboxRequest::default()
    };
    let started = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let waiter = tokio::spawn({
        let registry = registry.clone();
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        async move {
            registry
                .run(sandbox_id, request, move || async move {
                    started.add_permits(1);
                    release.acquire().await.unwrap().forget();
                    Ok(CleanupSandboxResponse::default())
                })
                .await
        }
    });
    started.acquire().await.unwrap().forget();

    registry.stop_admitting();
    task_tracker.close();
    let error = waiter.await.unwrap().unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);

    assert!(
        tokio::time::timeout(Duration::from_millis(20), task_tracker.wait())
            .await
            .is_err(),
        "shutdown must keep waiting for the detached mutation"
    );
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), task_tracker.wait())
        .await
        .expect("the tracker should drain after the owned mutation completes");

    let error = registry
        .run(
            sandbox_id,
            CleanupSandboxRequest {
                execution_fence: None,
                sandbox_id: sandbox_id.to_string(),
                ..CleanupSandboxRequest::default()
            },
            || async { Ok(CleanupSandboxResponse::default()) },
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
}

#[tokio::test]
async fn cleanup_phase_acquisition_stops_when_shutdown_begins() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let wait = wait_for_cleanup_phase(
        &shutdown_token,
        std::future::pending::<()>(),
        "phase timed out",
    );
    tokio::pin!(wait);

    shutdown_token.cancel();
    let error = tokio::time::timeout(Duration::from_secs(1), &mut wait)
        .await
        .expect("shutdown must release phase acquisition")
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::Unavailable);
}
#[tokio::test(start_paused = true)]
async fn busy_device_cleanup_times_out_before_mutating_the_sandbox() {
    let service = crate::service::tests::service_tests::service_for_unit_tests().await;
    let id = SandboxId::new();
    let device = service.sandbox_device_stack_lock(&id).await;
    let ownership = device.lock_owned().await;
    let request = CleanupSandboxRequest {
        execution_fence: Some(crate::service::tests::service_tests::bind_test_execution(
            &service,
            &id.to_string(),
        )),
        sandbox_id: id.to_string(),
        ..Default::default()
    };
    let registry = CleanupOperationRegistry::with_waiter_timeout(
        tokio_util::task::TaskTracker::new(),
        Duration::from_secs(60),
    );
    let retry_request = request.clone();
    let cleanup = registry.run(id, request.clone(), move || async move {
        service
            .run_cleanup_sandbox_operation(id, request, None)
            .await
    });
    tokio::pin!(cleanup);
    assert!(futures_util::poll!(&mut cleanup).is_pending());
    tokio::task::yield_now().await;
    tokio::time::advance(CLEANUP_PHASE_ACQUIRE_TIMEOUT).await;
    let error = cleanup.await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(error.message().contains("device-stack"));
    assert!(!registry.operations.contains_key(&id));
    drop(ownership);
    registry
        .run(id, retry_request, || async {
            Ok(CleanupSandboxResponse::default())
        })
        .await
        .expect("timed-out cleanup must release the registry claim for retry");
}

struct CleanupNode {
    service: NodeManagerService,
    sandbox_id: SandboxId,
    fence: pplx_space_node_mgr_client::execution_fence::ExecutionFence,
    _supervisor_socket: tempfile::TempDir,
    supervisor: crate::runtime_supervisor_client::tests::StubSupervisorServer,
}

impl CleanupNode {
    /// A bound, leased sandbox whose supervisor acknowledges runtime stop.
    async fn leased(script: crate::runtime_supervisor_client::tests::SupervisorScript) -> Self {
        let node = Self::unbound(script).await;
        node.service
            .pool
            .bind_execution(&node.sandbox_id.to_string(), node.fence)
            .unwrap();
        node.service
            .pool
            .allocate(
                &node.sandbox_id.to_string(),
                None,
                Some(crate::slot_pool::NewLeaseMemoryReservation {
                    memory_reservation_mib: 1024,
                }),
            )
            .await
            .unwrap();
        node
    }

    async fn unbound(script: crate::runtime_supervisor_client::tests::SupervisorScript) -> Self {
        let (supervisor_socket, client, supervisor) =
            crate::runtime_supervisor_client::tests::scripted_client(script).await;
        let mut service = crate::service::tests::service_tests::service_for_unit_tests().await;
        service.runtime_supervisor = client;
        service.pool =
            crate::slot_pool::NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(2));
        service.pool.set_all_slots_ready_for_test();
        Self {
            service,
            sandbox_id: SandboxId::new(),
            fence: pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(
                pplx_space_common_core::types::SandboxActivationAttemptId::new(),
                None,
            ),
            _supervisor_socket: supervisor_socket,
            supervisor,
        }
    }

    fn request(&self) -> CleanupSandboxRequest {
        CleanupSandboxRequest {
            execution_fence: Some(self.fence.into()),
            sandbox_id: self.sandbox_id.to_string(),
            force: true,
            ..Default::default()
        }
    }

    async fn cleanup(
        &self,
        request: CleanupSandboxRequest,
        caller: Option<launch::ProcessHandle>,
    ) -> CleanupOperationResult {
        self.service
            .run_cleanup_sandbox_operation(self.sandbox_id, request, caller)
            .await
    }

    fn lease(&self) -> Option<(u32, u64, crate::slot_pool::SlotState)> {
        self.service
            .pool
            .get_record(&self.sandbox_id.to_string())
            .unwrap()
            .map(|record| (record.info.slot, record.info.generation, record.state))
    }

    fn bound_execution(
        &self,
    ) -> Option<pplx_space_node_mgr_client::execution_fence::ExecutionFence> {
        self.service
            .pool
            .execution_fence(&self.sandbox_id.to_string())
            .unwrap()
    }

    async fn stop(self) {
        crate::runtime_supervisor_client::tests::stop_supervisor_server(self.supervisor).await;
    }
}

#[derive(Clone, Copy, Debug)]
enum CleanupBlocker {
    Capacity,
    ArtifactTransfer,
    CgroupGate,
}

#[rstest::rstest]
#[case::capacity(CleanupBlocker::Capacity, "timed out waiting for cleanup capacity")]
#[case::artifact_transfer(
    CleanupBlocker::ArtifactTransfer,
    "timed out waiting for artifact transfer ownership"
)]
#[case::cgroup_gate(
    CleanupBlocker::CgroupGate,
    "timed out waiting for sandbox cleanup ownership"
)]
#[tokio::test(start_paused = true)]
async fn blocked_cleanup_phase_times_out_before_stopping_the_runtime(
    #[case] blocker: CleanupBlocker,
    #[case] message: &str,
) {
    // The default supervisor client is unreachable: a cleanup that skipped
    // the wait would fail on runtime stop instead of on this phase.
    let service = crate::service::tests::service_tests::service_for_unit_tests().await;
    service.pool.set_all_slots_ready_for_test();
    let id = SandboxId::new();
    let execution_fence =
        crate::service::tests::service_tests::bind_test_execution(&service, &id.to_string());
    service
        .pool
        .allocate(
            &id.to_string(),
            None,
            Some(crate::slot_pool::NewLeaseMemoryReservation {
                memory_reservation_mib: 1024,
            }),
        )
        .await
        .unwrap();
    let before = service.pool.get(&id.to_string()).unwrap().unwrap();
    let mut held: Vec<Box<dyn std::any::Any + Send>> = Vec::new();
    match blocker {
        CleanupBlocker::Capacity => loop {
            let mut acquire = Box::pin(
                service
                    .lifecycle_limiter
                    .acquire(crate::lifecycle_limiter::LifecycleMethod::CleanupSandbox),
            );
            match futures_util::poll!(&mut acquire) {
                std::task::Poll::Ready(permit) => held.push(Box::new(permit.unwrap())),
                std::task::Poll::Pending => break,
            }
        },
        CleanupBlocker::ArtifactTransfer => held.push(Box::new(
            service
                .lock_sandbox_artifacts(&id.to_string())
                .await
                .unwrap(),
        )),
        CleanupBlocker::CgroupGate => {
            held.push(Box::new(
                service.lock_sandbox_cgroups(&id.to_string()).await,
            ));
        }
    }
    let cleanup = service.run_cleanup_sandbox_operation(
        id,
        CleanupSandboxRequest {
            execution_fence: Some(execution_fence),
            sandbox_id: id.to_string(),
            force: true,
            ..Default::default()
        },
        None,
    );
    tokio::pin!(cleanup);
    assert!(futures_util::poll!(&mut cleanup).is_pending());
    tokio::time::advance(CLEANUP_PHASE_ACQUIRE_TIMEOUT).await;

    let error = cleanup.await.unwrap_err();

    assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
    assert_eq!(error.message(), message);
    let after = service.pool.get_record(&id.to_string()).unwrap().unwrap();
    assert_eq!(after.state, crate::slot_pool::SlotState::Leased);
    assert_eq!(
        (after.info.slot, after.info.generation),
        (before.slot, before.generation)
    );
    drop(held);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CleanupRefusal {
    ExitedCaller,
    MissingExecution,
    SupersededExecution,
}

#[rstest::rstest]
#[case::exited_caller(
    CleanupRefusal::ExitedCaller,
    tonic::Code::Aborted,
    "lifecycle caller exited before node mutation"
)]
#[case::missing_execution(
    CleanupRefusal::MissingExecution,
    tonic::Code::FailedPrecondition,
    "sandbox execution identity is missing or superseded"
)]
#[case::superseded_execution(
    CleanupRefusal::SupersededExecution,
    tonic::Code::FailedPrecondition,
    "sandbox execution identity is missing or superseded"
)]
#[tokio::test]
async fn cleanup_refusal_keeps_the_lease_and_execution(
    #[case] refusal: CleanupRefusal,
    #[case] code: tonic::Code,
    #[case] message: &str,
) {
    let node = CleanupNode::leased(Default::default()).await;
    let mut request = node.request();
    let mut exited = None;
    match refusal {
        CleanupRefusal::ExitedCaller => {
            exited = Some(crate::service::runtime::tests::exited_process());
        }
        CleanupRefusal::MissingExecution => request.execution_fence = None,
        CleanupRefusal::SupersededExecution => {
            request.execution_fence = Some(
                pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(
                    pplx_space_common_core::types::SandboxActivationAttemptId::new(),
                    None,
                )
                .into(),
            );
        }
    }
    let caller = exited
        .as_ref()
        .map(|child| launch::ProcessHandle::open(child.id()).unwrap());
    let before = node.lease();

    let error = node.cleanup(request, caller).await.unwrap_err();

    assert_eq!(error.code(), code, "{error}");
    assert_eq!(error.message(), message);
    assert_eq!(node.lease(), before);
    assert_eq!(node.bound_execution(), Some(node.fence));
    if let Some(mut child) = exited {
        child.wait().unwrap();
    }
    node.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_does_not_stop_a_lease_replaced_while_it_waited_for_the_gate() {
    let node = CleanupNode::leased(Default::default()).await;
    let sandbox_id = node.sandbox_id.to_string();
    let gate = node.service.lock_sandbox_cgroups(&sandbox_id).await;
    let mut cleanup = Box::pin(node.cleanup(node.request(), None));
    assert!(futures_util::poll!(&mut cleanup).is_pending());
    node.service.pool.release(&sandbox_id).unwrap();
    node.service
        .pool
        .allocate(
            &sandbox_id,
            None,
            Some(crate::slot_pool::NewLeaseMemoryReservation {
                memory_reservation_mib: 1024,
            }),
        )
        .await
        .unwrap();
    let replacement = node.lease();
    drop(gate);

    let error = cleanup.await.unwrap_err();

    assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
    assert_eq!(error.message(), "sandbox lease changed before stop; retry");
    assert_eq!(node.lease(), replacement);
    node.stop().await;
}

// Removing the filesystem needs the host's fixed cache root, so these cases
// keep it; retirement after removal is not reachable here.
#[tokio::test]
async fn cleanup_keeps_execution_authority_when_supervisor_bind_fails() {
    let node = CleanupNode::leased(crate::runtime_supervisor_client::tests::SupervisorScript {
        bind_execution_failure: Some(tonic::Code::Unavailable),
        ..Default::default()
    })
    .await;
    let request = CleanupSandboxRequest {
        keep_filesystem: true,
        ..node.request()
    };

    let error = node.cleanup(request, None).await.unwrap_err();

    assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
    assert_eq!(error.message(), "bind execution unavailable");
    // The slot is released before reconciliation; the retry still holds authority.
    assert_eq!(node.lease(), None);
    assert_eq!(node.bound_execution(), Some(node.fence));
    node.stop().await;
}

#[rstest::rstest]
#[case::bound(true, false)]
#[case::bound_return_after_slot_release(true, true)]
#[case::unbound(false, false)]
#[tokio::test]
async fn retained_cleanup_releases_the_slot_and_acknowledges_only_a_bound_execution(
    #[case] bound: bool,
    #[case] return_after_slot_release: bool,
) {
    let node = if bound {
        CleanupNode::leased(Default::default()).await
    } else {
        CleanupNode::unbound(Default::default()).await
    };
    let request = CleanupSandboxRequest {
        keep_filesystem: true,
        return_after_slot_release,
        ..node.request()
    };

    let response = node.cleanup(request, None).await.unwrap();

    assert_eq!(node.lease(), None);
    assert_eq!(
        response
            .actions
            .iter()
            .any(|action| action == "Released network slot"),
        bound,
        "{:?}",
        response.actions
    );
    assert_eq!(
        response.retained_execution_id,
        bound.then(|| node.fence.current().to_string())
    );
    assert_eq!(node.bound_execution(), bound.then_some(node.fence));
    node.stop().await;
}
