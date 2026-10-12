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
async fn timed_out_cleanup_releases_the_registry_claim_for_retry() {
    let (_supervisor_socket, client, supervisor) =
        crate::runtime_supervisor_client::tests::scripted_client(Default::default()).await;
    let mut service = crate::service::tests::service_tests::service_for_unit_tests().await;
    service.runtime_supervisor = client;
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
    assert!(!registry.operations.contains_key(&id));
    drop(ownership);
    registry
        .run(id, retry_request, || async {
            Ok(CleanupSandboxResponse::default())
        })
        .await
        .expect("timed-out cleanup must release the registry claim for retry");
    crate::runtime_supervisor_client::tests::stop_supervisor_server(supervisor).await;
}

struct CleanupNode {
    service: NodeManagerService,
    sandbox_id: SandboxId,
    fence: pplx_space_node_mgr_client::execution_fence::ExecutionFence,
    _supervisor_socket: tempfile::TempDir,
    supervisor: crate::runtime_supervisor_client::tests::StubSupervisorServer,
}

impl CleanupNode {
    /// A bound, leased sandbox. Its test-owned supervisor refuses runtime
    /// stop, so a cleanup refusal that regresses ends there instead of
    /// touching host artifacts or a real supervisor. The lease is claimed off
    /// the Tokio runtime, so allocation never dials the fixed node-ctl socket.
    async fn leased() -> Self {
        let (supervisor_socket, client, supervisor) =
            crate::runtime_supervisor_client::tests::scripted_client(Default::default()).await;
        let mut service = crate::service::tests::service_tests::service_for_unit_tests().await;
        service.runtime_supervisor = client;
        // A second ready slot lets a replacement lease be granted mid-cleanup.
        service.pool =
            crate::slot_pool::NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(2));
        service.pool.set_all_slots_ready_for_test();
        let node = Self {
            service,
            sandbox_id: SandboxId::new(),
            fence: pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(
                pplx_space_common_core::types::SandboxActivationAttemptId::new(),
                None,
            ),
            _supervisor_socket: supervisor_socket,
            supervisor,
        };
        node.service
            .pool
            .bind_execution(&node.sandbox_id.to_string(), node.fence)
            .unwrap();
        crate::service::tests::service_tests::allocate_test_lease(
            &node.service,
            &node.sandbox_id.to_string(),
        );
        node
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CleanupBlocker {
    Capacity,
    ArtifactTransfer,
    CgroupGate,
    DeviceStack,
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
#[case::device_stack(
    CleanupBlocker::DeviceStack,
    "timed out waiting for device-stack cleanup ownership"
)]
#[tokio::test(start_paused = true)]
async fn blocked_cleanup_phase_times_out_before_stopping_the_runtime(
    #[case] blocker: CleanupBlocker,
    #[case] message: &str,
) {
    let mut node = CleanupNode::leased().await;
    // One held permit fills the cleanup class only at a known limit.
    node.service.lifecycle_limiter = Arc::new(crate::lifecycle_limiter::tests::limiter_with(
        crate::lifecycle_limiter::LifecycleMethod::CleanupSandbox,
        1,
        1,
    ));
    let service = &node.service;
    let id = node.sandbox_id;
    let before = node.lease();
    let held: Box<dyn std::any::Any + Send> = match blocker {
        // The artifact lock guards the next phase, so a cleanup that ignores
        // capacity stops there, before it reads the run directory.
        CleanupBlocker::Capacity => Box::new((
            service
                .lifecycle_limiter
                .acquire(crate::lifecycle_limiter::LifecycleMethod::CleanupSandbox)
                .await
                .unwrap(),
            service
                .lock_sandbox_artifacts(&id.to_string())
                .await
                .unwrap(),
        )),
        CleanupBlocker::ArtifactTransfer => Box::new(
            service
                .lock_sandbox_artifacts(&id.to_string())
                .await
                .unwrap(),
        ),
        CleanupBlocker::CgroupGate => Box::new(service.lock_sandbox_cgroups(&id.to_string()).await),
        CleanupBlocker::DeviceStack => Box::new(
            service
                .sandbox_device_stack_lock(&id)
                .await
                .lock_owned()
                .await,
        ),
    };
    let mut cleanup = Box::pin(node.cleanup(node.request(), None));
    assert!(futures_util::poll!(&mut cleanup).is_pending());
    tokio::time::advance(CLEANUP_PHASE_ACQUIRE_TIMEOUT).await;

    let error = cleanup.await.unwrap_err();

    assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
    assert_eq!(error.message(), message);
    assert_eq!(node.lease(), before);
    drop(held);
    node.stop().await;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CleanupRefusal {
    // Only Linux pidfds can observe that a pinned caller exited.
    #[cfg(target_os = "linux")]
    ExitedCaller,
    MissingExecution,
    SupersededExecution,
}

#[rstest::rstest]
#[cfg_attr(
    target_os = "linux",
    case::exited_caller(
        CleanupRefusal::ExitedCaller,
        tonic::Code::Aborted,
        "lifecycle caller exited before node mutation"
    )
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
    let node = CleanupNode::leased().await;
    let mut request = node.request();
    let exited: Option<std::process::Child> = match refusal {
        #[cfg(target_os = "linux")]
        CleanupRefusal::ExitedCaller => Some(crate::service::runtime::tests::exited_process()),
        CleanupRefusal::MissingExecution => {
            request.execution_fence = None;
            None
        }
        CleanupRefusal::SupersededExecution => {
            request.execution_fence = Some(
                pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(
                    pplx_space_common_core::types::SandboxActivationAttemptId::new(),
                    None,
                )
                .into(),
            );
            None
        }
    };
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

// Before this refusal, cleanup only reads pid links under a run directory
// named by the fresh sandbox id, and a failed read means no tracked process,
// so the outcome cannot depend on host state.
#[tokio::test(flavor = "current_thread")]
async fn cleanup_does_not_stop_a_lease_replaced_while_it_waited_for_the_gate() {
    let node = CleanupNode::leased().await;
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

/// Cleanup owns the sandbox exclusively: it refuses while startup work,
/// cleanup or reset holds the claim, and gives the claim back when it returns.
/// `cleanup_sandbox` also inspects the fixed cache root on its way out, so the
/// claim is exercised through the operation it runs.
#[tokio::test(start_paused = true)]
async fn cleanup_refuses_a_claimed_sandbox_and_keeps_no_claim() {
    type Claim = fn(
        &crate::slot_pool::NetworkPool,
        &str,
    ) -> Option<Arc<crate::slot_pool::SandboxMutationClaim>>;
    let service = crate::service::tests::service_tests::service_for_unit_tests().await;
    let id = SandboxId::new();
    let sandbox_id = id.to_string();
    // Barrier for a regressed cleanup that ignores the claim: the held cgroup
    // gate stops it before any host phase, with a different timeout message.
    let _gate = service
        .sandbox_cgroup_gate_lock(&sandbox_id)
        .await
        .read_owned()
        .await;
    let holders: [(&str, Claim); 2] = [
        (
            "startup work",
            crate::slot_pool::NetworkPool::try_claim_sandbox_work,
        ),
        (
            "cleanup or reset",
            crate::slot_pool::NetworkPool::try_claim_sandbox_mutation,
        ),
    ];
    for (holder, claim) in holders {
        let owner = claim(&service.pool, &sandbox_id).unwrap();
        let error = service
            .run_cleanup_sandbox_operation(
                id,
                CleanupSandboxRequest {
                    sandbox_id: sandbox_id.clone(),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable, "{holder}: {error}");
        assert_eq!(
            error.message(),
            "sandbox host mutation or reset is in progress; retry",
            "{holder}"
        );
        drop(owner);
        drop(
            service
                .pool
                .try_claim_sandbox_mutation(&sandbox_id)
                .unwrap_or_else(|| panic!("{holder}: refused cleanup kept a claim")),
        );
    }
}
