use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::Notify;

#[cfg(feature = "kubernetes")]
mod kubernetes;

struct Backend {
    state: Mutex<Observation>,
    calls: AtomicUsize,
    reachable: AtomicBool,
    pause_probe: AtomicBool,
    probe_entered: Notify,
    probe_release: Notify,
    entered: Notify,
    release: Notify,
    outcome: AccessOutcome,
    wakes_asked: Mutex<Vec<ExecutionActivityKind>>,
}
#[tonic::async_trait]
impl AccessBackend for Backend {
    async fn queue_implicit_wake(
        &self,
        _: SandboxId,
        activity: ExecutionActivityKind,
    ) -> Result<bool, Status> {
        self.wakes_asked.lock().unwrap().push(activity);
        Ok(false)
    }
    async fn observe(&self, _: SandboxId) -> Result<Observation, Status> {
        Ok(*self.state.lock().unwrap())
    }
    async fn reachable(&self, _: SandboxId) -> Result<bool, Status> {
        self.probe_entered.notify_one();
        if self.pause_probe.load(Ordering::SeqCst) {
            self.probe_release.notified().await;
        }
        Ok(
            self.state.lock().unwrap().lifecycle == SandboxLifecycle::Started
                && self.reachable.load(Ordering::SeqCst),
        )
    }
    async fn access(&self, _: SandboxId) -> Result<AccessOutcome, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.release.notified().await;
        Ok(self.outcome)
    }
}
fn fixture(state: Observation, outcome: AccessOutcome) -> (Arc<Backend>, Activator) {
    let backend = Arc::new(Backend {
        state: Mutex::new(state),
        calls: AtomicUsize::new(0),
        reachable: AtomicBool::new(state.lifecycle == SandboxLifecycle::Started),
        pause_probe: AtomicBool::new(false),
        probe_entered: Notify::new(),
        probe_release: Notify::new(),
        entered: Notify::new(),
        release: Notify::new(),
        outcome,
        wakes_asked: Mutex::new(Vec::new()),
    });
    let activator = Activator {
        backend: backend.clone(),
        owners: ActivationOwners {
            tasks: TaskTracker::new(),
            slots: Arc::new(Semaphore::new(1)),
            shutdown: CancellationToken::new(),
        },
    };
    (backend, activator)
}
fn suspended(owned: bool) -> Observation {
    Observation {
        lifecycle: SandboxLifecycle::Suspended,
        access_wake: true,
        restore_owned: owned,
    }
}

#[tokio::test(start_paused = true)]
async fn accepted_activation_outlives_timeout_and_disconnect_and_drains() {
    for (disconnect, lifecycle) in [
        (false, SandboxLifecycle::Suspended),
        (true, SandboxLifecycle::Suspended),
        (false, SandboxLifecycle::Paused),
        (true, SandboxLifecycle::Paused),
        (false, SandboxLifecycle::Started),
        (true, SandboxLifecycle::Started),
    ] {
        let (backend, activator) = fixture(
            Observation {
                lifecycle,
                ..suspended(false)
            },
            AccessOutcome::Ready,
        );
        // Started with an absent pod must retain exactly the same owner as restore.
        backend.reachable.store(false, Ordering::SeqCst);
        let activator = Arc::new(activator);
        let waiter = tokio::spawn({
            let activator = activator.clone();
            async move {
                activator
                    .prepare(SandboxId::new(), Duration::from_secs(15))
                    .await
            }
        });
        backend.entered.notified().await;
        if disconnect {
            waiter.abort();
            waiter.await.unwrap_err();
        } else {
            assert_eq!(
                waiter.await.unwrap().unwrap_err().code(),
                tonic::Code::DeadlineExceeded
            );
        }
        assert_eq!(activator.owners.slots.available_permits(), 0);
        assert_eq!(
            activator
                .prepare(SandboxId::new(), Duration::from_secs(15))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted,
        );
        activator.owners.shutdown.cancel();
        assert_eq!(
            activator
                .prepare(SandboxId::new(), Duration::from_secs(15))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable
        );
        activator.owners.tasks.close();
        backend.release.notify_one();
        activator.owners.tasks.wait().await;
        assert_eq!(activator.owners.slots.available_permits(), 1);
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn restore_followers_only_observe_completion_or_expiry() {
    for completes in [false, true] {
        let (backend, activator) = fixture(suspended(true), AccessOutcome::Ready);
        let waiter = activator.prepare(SandboxId::new(), Duration::from_secs(15));
        tokio::pin!(waiter);
        tokio::select! { biased; result = &mut waiter => panic!("follower ended early: {result:?}"), _ = tokio::time::sleep(Duration::from_millis(1)) => {} }
        *backend.state.lock().unwrap() = Observation {
            lifecycle: if completes {
                SandboxLifecycle::Started
            } else {
                SandboxLifecycle::Suspended
            },
            restore_owned: false,
            access_wake: true,
        };
        let result = waiter.await;
        if completes {
            result.unwrap();
        } else {
            assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
        }
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn wake_disabled_refuses_dormant_but_allows_running_access() {
    for lifecycle in [
        SandboxLifecycle::Paused,
        SandboxLifecycle::Suspended,
        SandboxLifecycle::Started,
        SandboxLifecycle::Deleting,
    ] {
        let (backend, activator) = fixture(
            Observation {
                lifecycle,
                restore_owned: false,
                access_wake: false,
            },
            AccessOutcome::Ready,
        );
        backend.release.notify_one();
        let result = activator
            .prepare(SandboxId::new(), Duration::from_secs(15))
            .await;
        if lifecycle == SandboxLifecycle::Started {
            result.unwrap();
            assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
        } else {
            assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
            assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn losing_restore_claim_observes_without_retrying_activation() {
    let (backend, activator) = fixture(suspended(false), AccessOutcome::RestoreOwned);
    backend.release.notify_one();
    let waiter = activator.prepare(SandboxId::new(), Duration::from_secs(15));
    tokio::pin!(waiter);
    tokio::select! { biased; result = &mut waiter => panic!("owner race ended early: {result:?}"), _ = backend.entered.notified() => {} }
    *backend.state.lock().unwrap() = suspended(true);
    tokio::select! { biased; result = &mut waiter => panic!("follower ended early: {result:?}"), _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
    backend.state.lock().unwrap().lifecycle = SandboxLifecycle::Started;
    waiter.await.unwrap();
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn healthy_ingress_bypasses_exhausted_activation_capacity() {
    let (backend, mut activator) = fixture(
        Observation {
            lifecycle: SandboxLifecycle::Started,
            access_wake: false,
            restore_owned: false,
        },
        AccessOutcome::Ready,
    );
    activator.owners.slots = Arc::new(Semaphore::new(128));
    let _occupied = activator.owners.slots.acquire_many(128).await.unwrap();
    activator
        .prepare(SandboxId::new(), Duration::from_secs(15))
        .await
        .unwrap();
    assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    assert!(activator.owners.tasks.is_empty());
}

#[tokio::test(start_paused = true)]
async fn lifecycle_change_during_probe_enters_the_cancellation_independent_owner() {
    let (backend, activator) = fixture(
        Observation {
            lifecycle: SandboxLifecycle::Started,
            ..suspended(false)
        },
        AccessOutcome::Ready,
    );
    backend.pause_probe.store(true, Ordering::SeqCst);
    let activator = Arc::new(activator);
    let waiter = tokio::spawn({
        let activator = activator.clone();
        async move {
            activator
                .prepare(SandboxId::new(), Duration::from_secs(15))
                .await
        }
    });
    backend.probe_entered.notified().await;
    *backend.state.lock().unwrap() = suspended(false);
    backend.probe_release.notify_one();
    backend.entered.notified().await;
    waiter.abort();
    waiter.await.unwrap_err();
    assert_eq!(activator.owners.slots.available_permits(), 0);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    activator.owners.tasks.close();
    backend.release.notify_one();
    activator.owners.tasks.wait().await;
    assert_eq!(activator.owners.slots.available_permits(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn stored_ingress_access_resumes_paused_execution_before_reporting_ready()
-> pplx_space_test_env::TestResult {
    use pplx_space_common_core::types::{AutoLifecycleWakeSource, NodeManagedBy};
    use pplx_space_db_test_fixtures::try_pg_test_db;
    use pplx_space_node_api_sim::{SandboxRecord, SimWorkerHarness};
    use prost::Message;
    use uuid::Uuid;
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node = Uuid::now_v7();
    let worker = SimWorkerHarness::builder(node.to_string())
        .with_internal_api()
        .start()
        .await?;
    sqlx::query(sql!("INSERT INTO nodes (id, address, role, managed_by, state) VALUES ($1, '127.0.0.1', 'worker', 'space', 'ready')"))
        .bind(node).execute(&db.pool).await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, node_api_endpoint) VALUES ($1, $2)"
    ))
    .bind(node)
    .bind(worker.endpoint())
    .execute(&db.pool)
    .await?;
    let backend = StoredAccess {
        pool: db.pool.clone(),
        controller: SandboxController::new(Substrate::from_direct(
            crate::controllers::substrate::direct::DirectSubstrate::new(db.pool.clone()),
        )),
    };
    for (wake_enabled, pause_after_observation, worker_allows_access) in [
        (true, false, true),
        (false, false, true),
        (true, true, true),
        (false, true, true),
        (true, false, false),
        (true, true, false),
    ] {
        let id = SandboxId::new();
        let attempt = Uuid::now_v7();
        let mut policy = AutoLifecyclePolicy::disabled();
        if wake_enabled {
            policy.wake.on.push(AutoLifecycleWakeSource::Access);
        }
        let lifecycle = if pause_after_observation {
            "started"
        } else {
            "paused"
        };
        sqlx::query(sql!("INSERT INTO sandboxes (id, user_id, org_id, managed_by, lifecycle, placement, active_node_id, template_id, content_hash, auto_lifecycle_json, network_policy_generation)
            VALUES ($1, '00000000-0000-0000-0000-000000000010', '00000000-0000-0000-0000-000000000001', 'space', $2, 'on_node', $3, 'seed', 'hash-seed', $4, 1)"))
            .bind(id.as_uuid()).bind(lifecycle).bind(node).bind(sqlx::types::Json(&policy)).execute(&db.pool).await?;
        sqlx::query(sql!("INSERT INTO sandbox_direct_route_identities (sandbox_id, route_generation, current_node_id, substrate_kind, execution_generation, activation_attempt_id) VALUES ($1, 1, $2, 'space', 1, $3)"))
            .bind(id.as_uuid()).bind(node).bind(attempt).execute(&db.pool).await?;
        let mut record = SandboxRecord::new_creating(
            id.to_string(),
            "seed".into(),
            "fc".into(),
            "test".into(),
            None,
            false,
        );
        // The worker regression pins Local authority against cluster replay;
        // this fixture models the resulting effective policy at the RPC boundary.
        record.auto_lifecycle_authority = pplx_space_common_core::types::PolicyAuthority::Local;
        let mut effective_policy = AutoLifecyclePolicy::disabled();
        if worker_allows_access {
            effective_policy
                .wake
                .on
                .push(AutoLifecycleWakeSource::Access);
        }
        record.access_idle_policy =
            crate::controllers::auto_lifecycle::policy_to_proto(&effective_policy).encode_to_vec();
        record.status = "paused".into();
        record.route_generation = Some(1);
        record.execution_generation = Some(1);
        record.activation_attempt_id = Some(attempt.to_string());
        record.route_substrate_kind = Some(NodeManagedBy::Space);
        worker.state().insert_new(record).await?;
        let observed = backend.observe(id).await?;
        if pause_after_observation {
            assert_eq!(observed.lifecycle, SandboxLifecycle::Started);
            sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle = 'paused' WHERE id = $1"
            ))
            .bind(id.as_uuid())
            .execute(&db.pool)
            .await?;
        }
        let outcome = backend.access(id).await;
        if wake_enabled && worker_allows_access {
            assert_eq!(outcome?, AccessOutcome::Ready);
            assert_eq!(
                worker.state().get(&id.to_string()).await?.status,
                "running",
                "fresh ingress preparation must resume the worker before bridge setup"
            );
            let current = fetch_sandbox(&db.pool, id).await?.unwrap();
            assert_eq!(current.lifecycle, "started");
            assert_eq!(current.active_execution_generation, Some(1));
        } else {
            let expected = if wake_enabled {
                tonic::Code::Unavailable
            } else {
                tonic::Code::FailedPrecondition
            };
            assert_eq!(outcome.unwrap_err().code(), expected);
            assert_eq!(worker.state().get(&id.to_string()).await?.status, "paused");
            assert_eq!(
                fetch_sandbox(&db.pool, id).await?.unwrap().lifecycle,
                "paused"
            );
        }
    }
    Ok(())
}

/// The resolver names what the waiting session will do; an activity it does
/// not name is access, the stricter rule.
#[tokio::test]
async fn a_queued_implicit_wake_is_judged_for_the_activity_the_resolver_names() {
    let (backend, activator) = fixture(suspended(false), AccessOutcome::Ready);
    for activity in [
        ImplicitWakeActivity::Command,
        ImplicitWakeActivity::Access,
        ImplicitWakeActivity::Unspecified,
    ] {
        activator
            .queue_implicit_wake(SandboxId::new(), activity, Duration::from_secs(1))
            .await
            .unwrap();
    }
    assert_eq!(
        *backend.wakes_asked.lock().unwrap(),
        vec![
            ExecutionActivityKind::Command,
            ExecutionActivityKind::Access,
            ExecutionActivityKind::Access,
        ]
    );
}
