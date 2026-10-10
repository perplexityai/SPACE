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
