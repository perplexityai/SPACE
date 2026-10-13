use super::*;

#[tokio::test]
async fn dirty_outbox_poison_requests_fatal_service_drain_without_unwinding_callers() {
    let shutdown = CancellationToken::new();
    let tracker = TaskTracker::new();
    let service = SandboxManagerService::new_with_options(
        crate::db::test_conn(),
        SandboxManagerServiceOptions {
            shutdown_token: shutdown.clone(),
            task_tracker: tracker.clone(),
            ..SandboxManagerServiceOptions::for_test()
        },
    );
    let db = Arc::clone(&service.db);
    assert!(
        std::thread::spawn(move || {
            let _guard = db.lock().unwrap();
            panic!("poison the database for the supervision regression");
        })
        .join()
        .is_err()
    );

    service.start_node_state_dirty_syncer(shutdown.child_token(), &tracker);
    // This remains a best-effort helper for already-committed mutations, but
    // poison must request process repair instead of killing the calling task.
    crate::node_state_sync::mark_sandbox_dirty(&service.db, "sandbox");
    tokio::time::timeout(Duration::from_secs(5), shutdown.cancelled())
        .await
        .expect("poison must terminate the supervised outbox worker");
    assert!(service.critical_worker_failed());
    assert!(
        service.db.is_poisoned(),
        "never recover the poisoned connection"
    );
    tracker.close();
    tracker.wait().await;
}

fn insert_due_suspended_sandbox(
    service: &SandboxManagerService,
    storage_id: &str,
    overdue_by_secs: u64,
) {
    use pplx_space_common_core::types::{
        AutoLifecycleConditionSet, AutoLifecycleConditionSetId, AutoLifecycleStage,
    };

    let policy = AutoLifecyclePolicy {
        wake: Default::default(),
        condition_sets: BTreeMap::from([(
            AutoLifecycleConditionSetId::AccessIdle,
            AutoLifecycleConditionSet::access_idle(),
        )]),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::AccessIdle,
            at: 1,
            action: AutoLifecycleIdleAction::Stop,
        }],
    }
    .normalized();
    let mut runtime_state = initial_runtime_state(
        &policy,
        now_secs().saturating_sub(overdue_by_secs.saturating_add(1)),
    );
    recompute_runtime_state(&mut runtime_state, &policy);
    let db = service.db.lock().expect("mutex poisoned");
    crate::service::tests::service_tests::insert_test_sandbox_with_policy(
        &db,
        storage_id,
        SandboxState::Suspended,
        &policy,
    );
    crate::db::update_auto_lifecycle_state(&db, storage_id, &runtime_state)
        .expect("make scheduler test sandbox due");
}

// Each held test lock has two owners: this test's `locks` vector and its
// guard. The registry is weak. Admission clones the Arc once more before
// waiting for the guard.
fn scheduler_has_admitted(lock: &Arc<crate::auto_lifecycle::LifecycleMutex>) -> bool {
    Arc::strong_count(lock) >= 3
}

// Each scheduler tick scans for due work on the blocking pool, so admission
// waits on another thread, and no fixed number of yields covers that on a
// loaded runner. The wait is bounded in real time; yielding, unlike sleeping,
// never moves a paused clock.
async fn poll_until(mut ready: impl FnMut() -> bool) -> bool {
    let give_up = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if ready() {
            return true;
        }
        if std::time::Instant::now() >= give_up {
            return false;
        }
        tokio::task::yield_now().await;
    }
}

async fn wait_for_scheduler_waiters(
    locks: &[Arc<crate::auto_lifecycle::LifecycleMutex>],
    expected: usize,
) {
    let admitted = poll_until(|| {
        locks
            .iter()
            .filter(|lock| scheduler_has_admitted(lock))
            .count()
            >= expected
    })
    .await;
    assert!(admitted, "scheduler did not admit {expected} transitions");
}

async fn wait_for_state(
    service: &SandboxManagerService,
    sandbox_id: SandboxId,
    expected: SandboxState,
) {
    let reached = poll_until(|| {
        service
            .lookup_sandbox(&sandbox_id.to_string())
            .expect("scheduler test sandbox exists")
            .state
            == expected
    })
    .await;
    assert!(reached, "sandbox {sandbox_id} did not reach {expected}");
}

#[tokio::test(start_paused = true)]
async fn auto_lifecycle_scheduler_refreshes_due_work_before_backlog_drains() {
    let conn = crate::db::test_conn();
    let service = SandboxManagerService::for_test(conn);
    let old_ids = (0..12).map(|_| SandboxId::new()).collect::<Vec<_>>();
    for (index, sandbox_id) in old_ids.iter().enumerate() {
        insert_due_suspended_sandbox(&service, &sandbox_id.to_string(), 20 - index as u64);
    }

    let mut held = Vec::new();
    let mut locks = Vec::new();
    for sandbox_id in &old_ids {
        let lock = service.auto_lifecycle.wake_lock(&sandbox_id.to_string());
        held.push(Arc::clone(&lock).lock_owned().await);
        locks.push(lock);
    }

    let shutdown = CancellationToken::new();
    let scheduler = tokio::spawn(
        service
            .clone()
            .run_auto_lifecycle_scheduler(shutdown.clone()),
    );
    wait_for_scheduler_waiters(&locks, AUTO_LIFECYCLE_RECONCILE_CONCURRENCY).await;
    assert_eq!(
        locks
            .iter()
            .filter(|lock| scheduler_has_admitted(lock))
            .count(),
        AUTO_LIFECYCLE_RECONCILE_CONCURRENCY,
        "the scheduler must not admit more than its concurrency cap"
    );

    let newly_due = SandboxId::new();
    insert_due_suspended_sandbox(&service, &newly_due.to_string(), 1);
    let admitted = locks
        .iter()
        .position(scheduler_has_admitted)
        .expect("one old transition is in flight");
    let admitted_id = old_ids
        .get(admitted)
        .copied()
        .expect("admitted lock belongs to an old sandbox");
    drop(held.swap_remove(admitted));
    locks.swap_remove(admitted);
    wait_for_state(&service, admitted_id, SandboxState::Stopped).await;

    tokio::time::advance(AUTO_LIFECYCLE_RECONCILE_INTERVAL).await;
    wait_for_state(&service, newly_due, SandboxState::Stopped).await;
    assert!(
        old_ids.iter().any(|sandbox_id| {
            service
                .lookup_sandbox(&sandbox_id.to_string())
                .expect("old scheduler test sandbox exists")
                .state
                == SandboxState::Suspended
        }),
        "new work must run before the old backlog drains"
    );

    shutdown.cancel();
    drop(held);
    scheduler.await.expect("scheduler joins after draining");
}

#[tokio::test]
async fn auto_lifecycle_scheduler_preserves_storage_keys_and_drains_admitted_work() {
    let id = SandboxId::new().to_string();
    // Import used to persist the received spelling. Scheduling must read and
    // update that exact row, including when shutdown waits for its transition.
    for storage_id in [
        id.clone(),
        id.to_uppercase(),
        id.replace('-', ""),
        format!("{{{id}}}"),
        format!("urn:uuid:{id}"),
    ] {
        let conn = crate::db::test_conn();
        let service = SandboxManagerService::for_test(conn);
        insert_due_suspended_sandbox(&service, &storage_id, 10);
        let lock = service.auto_lifecycle.wake_lock(&storage_id);
        let held = Arc::clone(&lock).lock_owned().await;
        let shutdown = CancellationToken::new();
        let scheduler = tokio::spawn(
            service
                .clone()
                .run_auto_lifecycle_scheduler(shutdown.clone()),
        );
        wait_for_scheduler_waiters(&[lock], 1).await;
        shutdown.cancel();
        tokio::task::yield_now().await;
        assert!(
            !scheduler.is_finished(),
            "shutdown must drain admitted work"
        );
        drop(held);
        scheduler.await.unwrap();
        let db = service.db.lock().unwrap();
        let row = crate::db::get_sandbox(&db, &storage_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Stopped, "{storage_id}");
        assert_eq!(row.auto_lifecycle_state.next_auto_event_at, None);
        if storage_id != id {
            assert!(!crate::db::sandbox_exists(&db, &id).unwrap());
        }
    }
}

/// The error handler's read-modify-write must take the wake lock keyed on the
/// STORED id spelling — the same key the transition/activity path uses — so
/// the stale-disarm race stays closed for legacy uppercase/braced ids. Proven
/// by holding the storage-keyed lock and observing the handler block; a
/// handler keyed on the canonical uuid would take a different lock and finish.
#[tokio::test]
async fn auto_lifecycle_error_takes_the_storage_keyed_wake_lock() {
    let id = SandboxId::new();
    let storage_id = id.to_string().to_uppercase();
    let conn = crate::db::test_conn();
    let service = SandboxManagerService::for_test(conn);
    insert_due_suspended_sandbox(&service, &storage_id, 10);

    let lock = service.auto_lifecycle.wake_lock(&storage_id);
    let held = Arc::clone(&lock).lock_owned().await;
    let handler = {
        let service = service.clone();
        let storage_id = storage_id.clone();
        tokio::spawn(async move {
            service
                .handle_auto_lifecycle_error(&storage_id, id, &Status::internal("capture failed"))
                .await;
        })
    };

    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert!(
        !handler.is_finished(),
        "handler must block on the storage-keyed wake lock"
    );

    drop(held);
    handler.await.unwrap();
    let db = service.db.lock().unwrap();
    let row = crate::db::get_sandbox(&db, &storage_id).unwrap().unwrap();
    assert!(
        row.auto_lifecycle_state.last_auto_error.is_some(),
        "the read-modify-write ran once the lock was free"
    );
}

#[tokio::test]
async fn auto_lifecycle_error_after_row_removal_does_not_scan_inventory() {
    use rusqlite::trace::{TraceEvent, TraceEventCodes};
    use std::sync::atomic::{AtomicUsize, Ordering};
    static STEPS: AtomicUsize = AtomicUsize::new(0);
    fn trace(event: TraceEvent<'_>) {
        if let TraceEvent::Profile(statement, _) = event {
            STEPS.fetch_add(
                usize::try_from(statement.get_status(rusqlite::StatementStatus::VmStep)).unwrap(),
                Ordering::Relaxed,
            );
        }
    }
    let conn = crate::db::test_conn();
    for _ in 0..1_000 {
        crate::service::tests::service_tests::insert_test_sandbox(
            &conn,
            &SandboxId::new().to_string(),
            SandboxState::Suspended,
        );
    }
    let service = SandboxManagerService::for_test(conn);
    let removed_id = SandboxId::new();
    service
        .db
        .lock()
        .unwrap()
        .trace_v2(TraceEventCodes::SQLITE_TRACE_PROFILE, Some(trace));
    service
        .handle_auto_lifecycle_error(
            &removed_id.to_string(),
            removed_id,
            &Status::not_found("sandbox removed after discovery"),
        )
        .await;
    service
        .db
        .lock()
        .unwrap()
        .trace_v2(TraceEventCodes::empty(), None);
    assert!(
        STEPS.load(Ordering::Relaxed) < 500,
        "error handling scanned unrelated sandboxes"
    );
}

// A gateway process can outlive its sandbox's serving life: a failed
// teardown (cleanup_failed) or a suspend leak leaves the sidecar running
// with netgtw_pid still set, so gateway liveness alone must not admit a
// sandbox to the reconcile's push path. Without this gate the periodic
// sweep pushed control RPCs (route identity, policy reads/refreshes) at
// such gateways on every tick forever — warn logs and error client spans
// that can never converge.
#[test]
fn reconcile_entry_skips_states_that_cannot_authorize_a_gateway() {
    for state in [
        SandboxState::Creating,
        SandboxState::Created,
        SandboxState::Stopped,
        SandboxState::Suspended,
        SandboxState::Deleting,
        SandboxState::CleanupRequired,
        SandboxState::CleanupFailed,
        SandboxState::Quarantined,
        SandboxState::Failed,
    ] {
        assert_eq!(
            network_policy_reconcile_entry_decision(state),
            Some(ApplyDecisionKind::Skip),
            "{state:?} cannot have a serving gateway; the reconcile must \
             skip it without touching the gateway process"
        );
    }
    for state in [
        SandboxState::Starting,
        SandboxState::Started,
        SandboxState::Stopping,
        SandboxState::Pausing,
        SandboxState::Paused,
        SandboxState::Resuming,
        SandboxState::Checkpointing,
        SandboxState::Suspending,
        SandboxState::Restoring,
    ] {
        assert_eq!(
            network_policy_reconcile_entry_decision(state),
            None,
            "{state:?} is in the serving window; the reconcile must \
             proceed to the gateway pushes"
        );
    }
}
#[tokio::test]
async fn activation_reconcile_hydrates_policy_before_runtime_identity_publication() {
    use pplx_space_common_core::types::{NodeId, RouteGeneration, SandboxActivationAttemptId};
    use tonic::Code;

    // Full suspend retains execution metadata, but clears the live route.
    let retained_identity = SpaceApiRouteIdentity {
        sandbox_id: Some(SandboxId::new()),
        execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
        pod_uid: Some("019f8000-0000-7000-8000-0000000000cc".parse().unwrap()),
        ..Default::default()
    };
    let published_identity = SpaceApiRouteIdentity {
        route_generation: Some(RouteGeneration::new(2).unwrap()),
        current_node_id: Some(NodeId::new()),
        substrate_kind: Some(NodeManagedBy::Kubernetes),
        activation_attempt_id: Some(SandboxActivationAttemptId::new()),
        ..retained_identity.clone()
    };
    for (state, identity, expected_operations, expected_error) in [
        (
            SandboxState::Restoring,
            retained_identity.clone(),
            &["policy"][..],
            None,
        ),
        (
            SandboxState::Starting,
            SpaceApiRouteIdentity::default(),
            &["policy"][..],
            None,
        ),
        (
            SandboxState::Starting,
            published_identity.clone(),
            &["route_identity", "policy", "route_keys"][..],
            None,
        ),
        (
            SandboxState::Restoring,
            published_identity.clone(),
            &["route_identity", "policy", "route_keys"][..],
            None,
        ),
        (
            SandboxState::Starting,
            SpaceApiRouteIdentity {
                pod_uid: None,
                ..published_identity.clone()
            },
            &["policy"][..],
            None,
        ),
        (
            SandboxState::Restoring,
            SpaceApiRouteIdentity {
                pod_uid: None,
                ..published_identity.clone()
            },
            &["policy"][..],
            None,
        ),
        (
            SandboxState::Starting,
            SpaceApiRouteIdentity {
                substrate_kind: Some(NodeManagedBy::Space),
                ..published_identity.clone()
            },
            &["policy"][..],
            Some(Code::FailedPrecondition),
        ),
        (
            SandboxState::Restoring,
            SpaceApiRouteIdentity {
                substrate_kind: Some(NodeManagedBy::Space),
                ..published_identity
            },
            &["policy"][..],
            Some(Code::FailedPrecondition),
        ),
        (
            SandboxState::Started,
            retained_identity,
            &["route_identity", "policy"][..],
            Some(Code::InvalidArgument),
        ),
    ] {
        let operations = Mutex::new(Vec::new());
        let result = reconcile_policy_and_route_security(
            state,
            &identity,
            async {
                if identity.is_absent() {
                    return Ok(NetGatewayRouteIdentityPushOutcome::Absent);
                }
                operations.lock().unwrap().push("route_identity");
                if identity.route_generation.is_none() {
                    return Err(Status::invalid_argument(
                        "route identity update requires route_generation",
                    ));
                }
                Ok(NetGatewayRouteIdentityPushOutcome::Applied {
                    identity_profile_version: 1,
                })
            },
            async {
                operations.lock().unwrap().push("policy");
                Ok(ApplyDecisionKind::Apply)
            },
            async |identity_profile_version| {
                assert_eq!(identity_profile_version, Some(1));
                operations.lock().unwrap().push("route_keys");
                Ok(())
            },
        )
        .await;

        let actual_error = result.err().map(|error| match error {
            NetworkPolicyReconcileError::Operation(status) => status.code(),
            other => panic!("unexpected reconciliation error: {other}"),
        });
        assert_eq!(actual_error, expected_error, "{state:?}: {identity:?}");
        assert_eq!(
            *operations.lock().unwrap(),
            expected_operations,
            "{state:?}"
        );
    }
}

#[tokio::test]
async fn route_security_failure_does_not_prevent_policy_reconciliation() {
    let operations = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let policy_operations = operations.clone();
    let route_operations = operations.clone();
    let route_key_operations = operations.clone();

    let error = reconcile_policy_and_route_security(
        SandboxState::Started,
        &SpaceApiRouteIdentity::default(),
        async move {
            route_operations
                .lock()
                .expect("operations mutex poisoned")
                .push("route_identity");
            Ok(NetGatewayRouteIdentityPushOutcome::Applied {
                identity_profile_version: 1,
            })
        },
        async move {
            policy_operations
                .lock()
                .expect("operations mutex poisoned")
                .push("policy");
            Ok(ApplyDecisionKind::Apply)
        },
        move |identity_profile_version| async move {
            assert_eq!(identity_profile_version, Some(1));
            route_key_operations
                .lock()
                .expect("operations mutex poisoned")
                .push("route_keys");
            Err(Status::unavailable("verification-key push failed"))
        },
    )
    .await
    .expect_err("verification-key failure must keep the reconcile pending");

    assert!(!error.is_terminal());
    assert_eq!(
        *operations.lock().expect("operations mutex poisoned"),
        ["route_identity", "policy", "route_keys"],
        "identity must be pushed once before policy, then reused for verification keys"
    );
}

#[tokio::test]
async fn transient_route_identity_failure_keeps_reconciliation_pending() {
    let operations = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let policy_operations = operations.clone();
    let route_key_operations = operations.clone();

    let error = reconcile_policy_and_route_security(
        SandboxState::Started,
        &SpaceApiRouteIdentity::default(),
        async { Err(Status::unavailable("net-gateway socket unavailable")) },
        async move {
            policy_operations
                .lock()
                .expect("operations mutex poisoned")
                .push("policy");
            Ok(ApplyDecisionKind::Apply)
        },
        move |_| async move {
            route_key_operations
                .lock()
                .expect("operations mutex poisoned")
                .push("route_keys");
            Ok(())
        },
    )
    .await
    .expect_err("transient identity failure must retry");

    assert!(!error.is_terminal());
    // Policy still converges first; the key push is left for the retry
    // rather than running against an unreachable gateway.
    assert_eq!(
        operations
            .lock()
            .expect("operations mutex poisoned")
            .as_slice(),
        ["policy"]
    );
}

// Divergence arrives as its own outcome, so a FailedPrecondition status
// reaching the reconciler carries no divergence meaning. Reading one as
// convergence would silently report success for an unrelated failure.
#[tokio::test]
async fn unexpected_failed_precondition_is_not_read_as_route_identity_divergence() {
    let operations = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let policy_operations = operations.clone();
    let route_key_operations = operations.clone();

    let error = reconcile_policy_and_route_security(
        SandboxState::Started,
        &SpaceApiRouteIdentity::default(),
        async { Err(Status::failed_precondition("net-gateway is not ready")) },
        async move {
            policy_operations
                .lock()
                .expect("operations mutex poisoned")
                .push("policy");
            Ok(ApplyDecisionKind::Apply)
        },
        move |_| async move {
            route_key_operations
                .lock()
                .expect("operations mutex poisoned")
                .push("route_keys");
            Ok(())
        },
    )
    .await
    .expect_err("an unclassified identity failure must keep the reconcile pending");

    assert!(!error.is_terminal());
    assert_eq!(
        operations
            .lock()
            .expect("operations mutex poisoned")
            .as_slice(),
        ["policy"]
    );
}

#[tokio::test]
async fn persistent_route_identity_divergence_does_not_prevent_verification_key_push() {
    let operations = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let policy_operations = operations.clone();
    let route_key_operations = operations.clone();

    let decision = reconcile_policy_and_route_security(
        SandboxState::Started,
        &SpaceApiRouteIdentity::default(),
        async { Ok(NetGatewayRouteIdentityPushOutcome::Diverged) },
        async move {
            policy_operations
                .lock()
                .expect("operations mutex poisoned")
                .push("policy");
            Ok(ApplyDecisionKind::Apply)
        },
        move |identity_profile_version| async move {
            assert_eq!(identity_profile_version, None);
            route_key_operations
                .lock()
                .expect("operations mutex poisoned")
                .push("route_keys");
            Ok(())
        },
    )
    .await
    .expect("persistent identity divergence must not starve verification-key push");

    assert_eq!(decision, ApplyDecisionKind::Apply);
    assert_eq!(
        operations
            .lock()
            .expect("operations mutex poisoned")
            .as_slice(),
        ["policy", "route_keys"]
    );
}

#[test]
fn only_abandoned_network_policy_refresh_is_terminal() {
    let abandoned = NetworkPolicyReconcileError::from(NetworkPolicyRefreshError::Abandoned(
        RefreshAbandonReason::SandboxRowGone,
    ));
    assert!(abandoned.is_terminal());

    let retryable_refresh = NetworkPolicyReconcileError::from(
        NetworkPolicyRefreshError::Retryable(NetworkPolicyRefreshAttemptError::Unavailable),
    );
    assert!(!retryable_refresh.is_terminal());

    let unrelated_precondition = NetworkPolicyReconcileError::from(Status::failed_precondition(
        "unrelated operation precondition",
    ));
    assert!(!unrelated_precondition.is_terminal());
}

#[tokio::test(start_paused = true)]
async fn network_policy_queue_admits_pending_work_fairly_while_gateways_are_blocked() {
    let queue = super::super::network_policy::NetworkPolicyReconcileQueue::default();
    let shutdown = CancellationToken::new();
    let blocked = Arc::new(tokio::sync::Semaphore::new(0));
    let (started_tx, mut started_rx) = tokio::sync::mpsc::channel(64);
    let (applied_tx, mut applied_rx) = tokio::sync::mpsc::channel(64);
    queue.enqueue("blocked");
    let worker = tokio::spawn(run_network_policy_queue(queue.clone(), shutdown.clone(), {
        let blocked = blocked.clone();
        move |id| {
            let started_tx = started_tx.clone();
            let applied_tx = applied_tx.clone();
            let blocked = blocked.clone();
            async move {
                started_tx.send(id.clone()).await.unwrap();
                if id != "healthy" {
                    blocked.acquire().await.unwrap().forget();
                }
                applied_tx.send(id).await.unwrap();
                Ok(())
            }
        }
    }));
    assert_eq!(started_rx.recv().await.unwrap(), "blocked");
    queue.enqueue("blocked");
    queue.enqueue("healthy");
    let applied = tokio::time::timeout(Duration::from_secs(1), applied_rx.recv())
        .await
        .expect("healthy policy waited behind unrelated gateway");
    assert_eq!(applied.unwrap(), "healthy");
    assert_eq!(started_rx.recv().await.unwrap(), "healthy");

    for index in 0..NETWORK_POLICY_RECONCILE_CONCURRENCY {
        queue.enqueue(format!("blocked-{index:02}"));
    }
    for _ in 1..NETWORK_POLICY_RECONCILE_CONCURRENCY {
        assert_ne!(started_rx.recv().await.unwrap(), "blocked");
    }
    assert!(
        tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
            .await
            .is_err(),
        "concurrency cap or per-sandbox ordering violated"
    );

    // A sweep requeues active jobs behind the sandbox still waiting for a slot.
    for index in 0..NETWORK_POLICY_RECONCILE_CONCURRENCY {
        queue.enqueue(format!("blocked-{index:02}"));
    }
    blocked.add_permits(1);
    assert_eq!(applied_rx.recv().await.unwrap(), "blocked");
    assert_eq!(started_rx.recv().await.unwrap(), "blocked");
    blocked.add_permits(1);
    assert_eq!(applied_rx.recv().await.unwrap(), "blocked-00");
    assert_eq!(
        started_rx.recv().await.unwrap(),
        format!("blocked-{:02}", NETWORK_POLICY_RECONCILE_CONCURRENCY - 1),
        "older pending work must run before a requeued sandbox"
    );
    shutdown.cancel();
    worker.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn network_policy_queue_survives_panics_and_retries_the_failed_sandbox() {
    let queue = super::super::network_policy::NetworkPolicyReconcileQueue::default();
    let shutdown = CancellationToken::new();
    let (started_tx, mut started_rx) = tokio::sync::mpsc::channel(4);
    queue.enqueue("panics");
    let worker = tokio::spawn(run_network_policy_queue(
        queue.clone(),
        shutdown.clone(),
        move |id| {
            let started_tx = started_tx.clone();
            async move {
                started_tx.send(id.clone()).await.unwrap();
                tokio::task::yield_now().await;
                assert_ne!(id, "panics", "injected per-sandbox panic");
                Ok(())
            }
        },
    ));
    assert_eq!(started_rx.recv().await.as_deref(), Some("panics"));
    queue.enqueue("healthy");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
            .await
            .unwrap()
            .as_deref(),
        Some("healthy")
    );
    assert_eq!(
        tokio::time::timeout(
            NETWORK_POLICY_RECONCILE_RETRY_DELAY + Duration::from_secs(1),
            started_rx.recv()
        )
        .await
        .unwrap()
        .as_deref(),
        Some("panics"),
        "the failed sandbox must be released and retried"
    );
    shutdown.cancel();
    worker
        .await
        .expect("a sandbox panic must not stop the queue");
}
