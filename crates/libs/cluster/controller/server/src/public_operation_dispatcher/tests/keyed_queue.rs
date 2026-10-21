//! The dispatcher as a keyed work queue: one executing operation per
//! sandbox, deferred retries that hand their slot back, and a capped,
//! backed-off retry budget.

use super::*;
use pplx_space_api::public_operations::PUBLIC_OPERATION_CLAIM_LOCK_KEY;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

const STUCK_PREFIX: &str = "sbx_stuck_";
const SLOW_PREFIX: &str = "sbx_slow_";

/// Succeeds every operation after a short pause, except on `sbx_stuck_*`
/// sandboxes, which always fail retryably the way a wake behind a lingering
/// pod does: inside the dispatcher's deferral scope at once, otherwise only
/// after sleeping out a long readiness budget. Records per-sandbox overlap
/// and the order operations ran in.
#[derive(Clone, Default)]
struct KeyedRecordingExecutor {
    running_per_key: Arc<Mutex<HashMap<String, usize>>>,
    max_overlap_per_key: Arc<AtomicUsize>,
    stuck_attempts: Arc<AtomicUsize>,
    undeferred_stuck_attempts: Arc<AtomicUsize>,
    completed: Arc<Mutex<Vec<(String, OperationId)>>>,
}

#[async_trait::async_trait]
impl PublicOperationExecutor for KeyedRecordingExecutor {
    fn supported_kinds(&self) -> &'static [OperationKind] {
        &[
            OperationKind::CreateSandbox,
            OperationKind::WakeSandbox,
            OperationKind::StopSandbox,
            OperationKind::DeleteSandbox,
        ]
    }

    async fn execute(
        &self,
        _claim_token: &str,
        operation: ClaimedPublicOperation,
    ) -> OperationExecutionResult {
        let key = operation.target_id.clone().unwrap_or_default();
        {
            let mut running = self.running_per_key.lock().expect("lock");
            let count = running.entry(key.clone()).or_default();
            *count += 1;
            self.max_overlap_per_key.fetch_max(*count, Ordering::SeqCst);
        }
        let result = if key.starts_with(STUCK_PREFIX) {
            self.stuck_attempts.fetch_add(1, Ordering::SeqCst);
            if !pplx_space_api::retry_deferral::defer_retry_wait("test") {
                self.undeferred_stuck_attempts
                    .fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            OperationExecutionResult {
                state: OperationState::Queued,
                progress_message: "deferring restore: lingering prior-generation pod".to_string(),
                result_json: None,
                terminal_error_code: None,
                terminal_error_json: None,
                retry_after: Some(Duration::from_secs(1)),
                requeue_reason: PublicOperationRequeueReason::TransientFailure,
            }
        } else {
            let run_for = if key.starts_with(SLOW_PREFIX) {
                300
            } else {
                15
            };
            tokio::time::sleep(Duration::from_millis(run_for)).await;
            self.completed
                .lock()
                .expect("lock")
                .push((key.clone(), operation.id));
            OperationExecutionResult {
                state: OperationState::Succeeded,
                progress_message: "completed".to_string(),
                result_json: Some(serde_json::json!({})),
                terminal_error_code: None,
                terminal_error_json: None,
                retry_after: None,
                requeue_reason: PublicOperationRequeueReason::TransientFailure,
            }
        };
        *self
            .running_per_key
            .lock()
            .expect("lock")
            .get_mut(&key)
            .expect("key was counted") -= 1;
        result
    }
}

async fn create_op(
    store: &PublicOperationStore,
    org_id: OrgId,
    kind: OperationKind,
    target_id: &str,
) -> Result<OperationId, pplx_space_api::public_operations::PublicOperationStoreError> {
    create_queued_operation(store, org_id, ApiProduct::Native, kind, target_id).await
}

async fn defer_until(pool: &PgPool, operation_id: OperationId, secs: i64) -> TestResult {
    sqlx::query(sql!(
        "UPDATE public_operations
         SET next_attempt_at = NOW() + make_interval(secs => $2::BIGINT)
         WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .bind(secs)
    .execute(pool)
    .await?;
    Ok(())
}

async fn claim_all(
    store: &PublicOperationStore,
) -> Result<PublicOperationClaim, pplx_space_api::public_operations::PublicOperationStoreError> {
    store
        .claim_operations(ClaimPublicOperations {
            limit: 64,
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: NATIVE_PUBLIC_OPERATION_KINDS,
        })
        .await
}

fn claimed_ids(claim: &PublicOperationClaim) -> Vec<OperationId> {
    claim
        .operations
        .iter()
        .map(|operation| operation.id)
        .collect()
}

#[tokio::test]
async fn stuck_sandboxes_hand_back_their_slots_and_never_starve_other_sandboxes() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(8).await?
    else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    // More stuck sandboxes than slots, each with a queue of wakes: under a
    // slot-holding retry these alone would occupy the dispatcher.
    let mut stuck = Vec::new();
    for sandbox in 0..4 {
        for _ in 0..3 {
            stuck.push(
                create_op(
                    &store,
                    org_id,
                    OperationKind::WakeSandbox,
                    &format!("{STUCK_PREFIX}{sandbox}"),
                )
                .await?,
            );
        }
    }
    let mut healthy = Vec::new();
    for sandbox in 0..20 {
        healthy.push(
            create_op(
                &store,
                org_id,
                OperationKind::CreateSandbox,
                &format!("sbx_healthy_{sandbox}"),
            )
            .await?,
        );
    }
    let executor = KeyedRecordingExecutor::default();
    let shutdown = CancellationToken::new();
    let dispatcher = tokio::spawn(run(
        fast_loop_config(2),
        store.clone(),
        executor.clone(),
        shutdown.clone(),
    ));

    for operation_id in &healthy {
        wait_for_state(&db.pool, *operation_id, OperationState::Succeeded).await?;
    }
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;

    assert!(executor.stuck_attempts.load(Ordering::SeqCst) >= 4);
    assert_eq!(
        executor.undeferred_stuck_attempts.load(Ordering::SeqCst),
        0,
        "dispatcher executions run with retry waits deferred"
    );
    assert_eq!(executor.max_overlap_per_key.load(Ordering::SeqCst), 1);
    // Each stuck sandbox ran only its first wake; the later wakes wait
    // behind it in request order, without being claimed or charged.
    let later_wakes_untouched: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM public_operations
         WHERE operation_id = ANY($1) AND attempts = 0 AND state = 'queued'"
    ))
    .bind(
        stuck
            .chunks(3)
            .flat_map(|wakes| wakes[1..].iter().map(|id| id.as_uuid()))
            .collect::<Vec<_>>(),
    )
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(later_wakes_untouched, 8);
    // A deferred wake is requeued with backoff rather than parked running.
    for first_wake in stuck.chunks(3).map(|wakes| wakes[0]) {
        assert_eq!(
            operation_state(&db.pool, first_wake).await?,
            OperationState::Queued.as_str()
        );
    }
    Ok(())
}

#[tokio::test]
async fn one_sandboxs_operations_never_overlap_across_two_dispatchers() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(12).await?
    else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let kinds = [
        OperationKind::WakeSandbox,
        OperationKind::StopSandbox,
        OperationKind::WakeSandbox,
        OperationKind::StopSandbox,
        OperationKind::WakeSandbox,
    ];
    let mut expected: HashMap<String, Vec<OperationId>> = HashMap::new();
    for kind in kinds {
        for sandbox in ["sbx_shared_a", "sbx_shared_b", "sbx_shared_c"] {
            let operation_id = create_op(&store, org_id, kind, sandbox).await?;
            expected
                .entry(sandbox.to_string())
                .or_default()
                .push(operation_id);
        }
    }
    // A delete passes a wake in backoff; the wake comes due while the delete
    // still runs, so neither dispatcher may start it until the delete ends.
    let mut bypassed = Vec::new();
    for sandbox in 0..4 {
        let key = format!("{SLOW_PREFIX}{sandbox}");
        let wake = create_op(&store, org_id, OperationKind::WakeSandbox, &key).await?;
        sqlx::query(sql!(
            "UPDATE public_operations
             SET next_attempt_at = NOW() + INTERVAL '100 milliseconds'
             WHERE operation_id = $1"
        ))
        .bind(wake.as_uuid())
        .execute(&db.pool)
        .await?;
        bypassed.push(wake);
        bypassed.push(create_op(&store, org_id, OperationKind::DeleteSandbox, &key).await?);
    }
    let executor = KeyedRecordingExecutor::default();
    let shutdown = CancellationToken::new();
    let dispatchers = [
        tokio::spawn(run(
            fast_loop_config(4),
            store.clone(),
            executor.clone(),
            shutdown.clone(),
        )),
        tokio::spawn(run(
            fast_loop_config(4),
            store.clone(),
            executor.clone(),
            shutdown.clone(),
        )),
    ];

    for operation_id in expected.values().flatten().chain(&bypassed) {
        wait_for_state(&db.pool, *operation_id, OperationState::Succeeded).await?;
    }
    shutdown.cancel();
    for dispatcher in dispatchers {
        tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;
    }

    assert_eq!(executor.max_overlap_per_key.load(Ordering::SeqCst), 1);
    let completed = executor.completed.lock().expect("lock").clone();
    for (sandbox, operation_ids) in expected {
        let ran = completed
            .iter()
            .filter(|(key, _)| *key == sandbox)
            .map(|(_, operation_id)| *operation_id)
            .collect::<Vec<_>>();
        assert_eq!(ran, operation_ids, "{sandbox} ran in request order");
    }
    Ok(())
}

#[tokio::test]
async fn claim_holds_back_operations_behind_their_sandboxs_live_or_earlier_work() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let first = create_op(&store, org_id, OperationKind::WakeSandbox, "sbx_keyed").await?;
    let second = create_op(&store, org_id, OperationKind::StopSandbox, "sbx_keyed").await?;
    let other = create_op(&store, org_id, OperationKind::WakeSandbox, "sbx_other").await?;
    // Forks keep their own source lock and stay outside the key.
    let fork = create_op(&store, org_id, OperationKind::ForkSandbox, "sbx_keyed").await?;

    let claim = claim_all(&store).await?;
    assert_eq!(claimed_ids(&claim), vec![first, other, fork]);
    // The first operation is running under a live claim.
    assert!(claimed_ids(&claim_all(&store).await?).is_empty());

    // Requeued into backoff, it still holds back the later stop.
    store
        .release_claimed_operation(ReleaseClaimedPublicOperation {
            claim_token: claim.claim_token.clone(),
            operation_id: first,
            progress_message: "retry".to_string(),
            retry_after: Duration::from_secs(300),
            count_attempt: true,
        })
        .await?;
    assert!(claimed_ids(&claim_all(&store).await?).is_empty());

    defer_until(&db.pool, first, 0).await?;
    assert_eq!(claimed_ids(&claim_all(&store).await?), vec![first]);
    assert_eq!(
        operation_state(&db.pool, second).await?,
        OperationState::Queued.as_str()
    );
    Ok(())
}

#[tokio::test]
async fn delete_supersedes_backed_off_work_but_not_a_pending_create() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());

    // A wake due now is about to run: the delete waits for it. A second
    // wake, a client retry, queues behind the first.
    let wake = create_op(
        &store,
        org_id,
        OperationKind::WakeSandbox,
        "sbx_wake_delete",
    )
    .await?;
    let second_wake = create_op(
        &store,
        org_id,
        OperationKind::WakeSandbox,
        "sbx_wake_delete",
    )
    .await?;
    let delete = create_op(
        &store,
        org_id,
        OperationKind::DeleteSandbox,
        "sbx_wake_delete",
    )
    .await?;
    // A create in backoff still owns the sandbox's future: a delete that
    // ran first would let the create resurrect it afterwards.
    let create = create_op(
        &store,
        org_id,
        OperationKind::CreateSandbox,
        "sbx_create_delete",
    )
    .await?;
    let delete_after_create = create_op(
        &store,
        org_id,
        OperationKind::DeleteSandbox,
        "sbx_create_delete",
    )
    .await?;
    defer_until(&db.pool, create, 300).await?;
    // Another create stays schedulable even while this sandbox waits.
    let unrelated = create_op(
        &store,
        org_id,
        OperationKind::CreateSandbox,
        "sbx_unrelated",
    )
    .await?;

    let claim = claim_all(&store).await?;
    assert_eq!(claimed_ids(&claim), vec![wake, unrelated]);

    // The first wake fails retryably into a long backoff: the delete no
    // longer waits for it, nor for the second wake queued behind it.
    store
        .release_claimed_operation(ReleaseClaimedPublicOperation {
            claim_token: claim.claim_token.clone(),
            operation_id: wake,
            progress_message: "deferring restore".to_string(),
            retry_after: Duration::from_secs(300),
            count_attempt: true,
        })
        .await?;
    assert_eq!(claimed_ids(&claim_all(&store).await?), vec![delete]);
    for held in [second_wake, delete_after_create] {
        assert_eq!(
            operation_state(&db.pool, held).await?,
            OperationState::Queued.as_str()
        );
    }
    Ok(())
}

#[tokio::test]
async fn stale_claim_recovery_keeps_the_sandboxs_order() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let first = create_op(&store, org_id, OperationKind::WakeSandbox, "sbx_stale").await?;
    let first_claim = claim_all(&store).await?;
    assert_eq!(claimed_ids(&first_claim), vec![first]);
    let later = create_op(&store, org_id, OperationKind::DeleteSandbox, "sbx_stale").await?;

    // The replica running `first` stops renewing.
    sqlx::query(sql!(
        "UPDATE public_operations
         SET claimed_at = NOW() - INTERVAL '120 seconds'
         WHERE operation_id = $1"
    ))
    .bind(first.as_uuid())
    .execute(&db.pool)
    .await?;

    let recovered = claim_all(&store).await?;
    assert_eq!(claimed_ids(&recovered), vec![first]);
    let operation = &recovered.operations[0];
    assert_eq!(operation.attempts, 2);
    assert_eq!(operation.claim_generation, 2);
    // The old claim is fenced off.
    assert!(
        !store
            .release_claimed_operation(ReleaseClaimedPublicOperation {
                claim_token: first_claim.claim_token.clone(),
                operation_id: first,
                progress_message: "late".to_string(),
                retry_after: Duration::from_secs(1),
                count_attempt: true,
            })
            .await?
    );
    assert_eq!(
        operation_state(&db.pool, later).await?,
        OperationState::Queued.as_str()
    );

    // A delete that passed a wake in backoff goes stale. Once the wake is
    // due, the delete is reclaimed first, fencing its old execution, rather
    // than letting the wake start beside it.
    let wake = create_op(
        &store,
        org_id,
        OperationKind::WakeSandbox,
        "sbx_stale_bypass",
    )
    .await?;
    defer_until(&db.pool, wake, 600).await?;
    let delete = create_op(
        &store,
        org_id,
        OperationKind::DeleteSandbox,
        "sbx_stale_bypass",
    )
    .await?;
    assert_eq!(claimed_ids(&claim_all(&store).await?), vec![delete]);
    sqlx::query(sql!(
        "UPDATE public_operations
         SET claimed_at = NOW() - INTERVAL '120 seconds'
         WHERE operation_id = $1"
    ))
    .bind(delete.as_uuid())
    .execute(&db.pool)
    .await?;
    defer_until(&db.pool, wake, 0).await?;
    assert_eq!(claimed_ids(&claim_all(&store).await?), vec![delete]);
    Ok(())
}

#[tokio::test]
async fn claim_takes_nothing_while_another_replica_is_mid_claim() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(4).await?
    else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let operation = create_op(&store, org_id, OperationKind::WakeSandbox, "sbx_locked").await?;

    let mut other_replica = db.pool.begin().await?;
    let held: bool = sqlx::query_scalar(sql!("SELECT pg_try_advisory_xact_lock($1)"))
        .bind(PUBLIC_OPERATION_CLAIM_LOCK_KEY)
        .fetch_one(&mut *other_replica)
        .await?;
    assert!(held);
    assert!(claimed_ids(&claim_all(&store).await?).is_empty());
    other_replica.rollback().await?;

    assert_eq!(claimed_ids(&claim_all(&store).await?), vec![operation]);
    Ok(())
}

#[tokio::test]
async fn an_unclaimable_kind_never_holds_back_its_sandbox() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    // Only some other claimer could run this stop; it must not strand the
    // wake requested after it.
    let _foreign = create_op(&store, org_id, OperationKind::StopSandbox, "sbx_foreign").await?;
    let wake = create_op(&store, org_id, OperationKind::WakeSandbox, "sbx_foreign").await?;

    let claim = store
        .claim_operations(ClaimPublicOperations {
            limit: 8,
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::WakeSandbox],
        })
        .await?;
    assert_eq!(claimed_ids(&claim), vec![wake]);
    Ok(())
}

#[tokio::test]
async fn kill_switch_restores_the_unkeyed_claim() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    for _ in 0..2 {
        create_op(&store, org_id, OperationKind::WakeSandbox, "sbx_unkeyed").await?;
    }
    let config = ReconcilerConfig {
        keyed_claim_enabled: false,
        claim_limit: 8,
        max_retry_attempts: 4,
        ..single_claim_config()
    };

    let report = tick(&config, &store, &AlwaysRetryExecutor).await?;
    // Unkeyed, both wakes on the sandbox run in the same pass.
    assert_eq!(report.claimed, 2);
    assert_eq!(report.retried, 2);
    Ok(())
}

#[test]
fn keyed_claim_kill_switch_reads_from_env() {
    let mut env = pplx_space_test_env::TestEnv::default();
    assert!(ReconcilerConfig::from_environment(&env).keyed_claim_enabled);
    env.set(KEYED_CLAIM_ENABLED_ENV, "false");
    assert!(!ReconcilerConfig::from_environment(&env).keyed_claim_enabled);
    env.set(KEYED_CLAIM_ENABLED_ENV, "true");
    assert!(ReconcilerConfig::from_environment(&env).keyed_claim_enabled);
}

/// Regression: a delete requested after a create must not run first and
/// hard-delete the row, or the create would rebuild the sandbox under the
/// same id. The delete waits for the create, even while the create sits in
/// retry backoff.
#[tokio::test]
async fn a_delete_requested_after_a_create_never_runs_first() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let create = create_op(
        &store,
        org_id,
        OperationKind::CreateSandbox,
        "sbx_resurrect",
    )
    .await?;
    let delete = create_op(
        &store,
        org_id,
        OperationKind::DeleteSandbox,
        "sbx_resurrect",
    )
    .await?;
    // The create failed once and is waiting out its backoff.
    defer_until(&db.pool, create, 300).await?;
    let executor = KeyedRecordingExecutor::default();
    let config = ReconcilerConfig {
        claim_limit: 8,
        execution_concurrency: 8,
        ..single_claim_config()
    };

    let report = tick(&config, &store, &executor).await?;
    assert_eq!(
        report.claimed, 0,
        "the delete must wait for the pending create"
    );

    defer_until(&db.pool, create, 0).await?;
    while tick(&config, &store, &executor).await?.claimed > 0 {}

    let completed = executor.completed.lock().expect("lock").clone();
    let order: Vec<_> = completed.into_iter().map(|(_, id)| id).collect();
    assert_eq!(order, vec![create, delete]);
    Ok(())
}

/// Runs a delete until it blocks and returns how long the next claim on its
/// sandbox was handed back. Without `second_delete`, the running delete's
/// claim goes stale under it and the delete is reclaimed. With it, the store
/// records the running delete as finished before its execution returns (it
/// is not yet reaped), and a second delete on the sandbox is claimed.
async fn busy_key_hand_back_delay(
    second_delete: bool,
) -> Result<Option<(f64, ReconcilerConfig)>, Box<dyn std::error::Error + Send + Sync>> {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(8).await?
    else {
        return Ok(None);
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let running = create_op(&store, org_id, OperationKind::DeleteSandbox, "sbx_busy").await?;
    let executor = BlockingExecutor::default();
    let config = fast_loop_config(2);
    let shutdown = CancellationToken::new();
    let dispatcher = tokio::spawn(run(
        config.clone(),
        store.clone(),
        executor.clone(),
        shutdown.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(5), executor.started.notified()).await?;
    let handed_back = if second_delete {
        create_op(&store, org_id, OperationKind::DeleteSandbox, "sbx_busy").await?
    } else {
        running
    };
    let stale = if second_delete {
        sqlx::query(sql!(
            "UPDATE public_operations
             SET state = 'succeeded', claim_token = '', completed_at = NOW()
             WHERE operation_id = $1"
        ))
    } else {
        sqlx::query(sql!(
            "UPDATE public_operations SET claimed_at = NOW() - INTERVAL '1 hour'
             WHERE operation_id = $1"
        ))
    };
    stale.bind(running.as_uuid()).execute(&db.pool).await?;
    let delay_secs: f64 = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let delay: Option<f64> = sqlx::query_scalar(sql!(
                "SELECT EXTRACT(EPOCH FROM next_attempt_at - updated_at)::DOUBLE PRECISION
                 FROM public_operations
                 WHERE operation_id = $1 AND state = 'queued'
                   AND progress_message = 'waiting for an earlier operation on the same sandbox'"
            ))
            .bind(handed_back.as_uuid())
            .fetch_optional(&db.pool)
            .await?;
            if let Some(delay) = delay {
                return Ok::<f64, sqlx::Error>(delay);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    executor.release.notify_one();
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;
    Ok(Some((delay_secs, config)))
}

#[tokio::test]
async fn a_reclaim_of_a_locally_running_operation_waits_for_its_next_renewal() -> TestResult {
    // The claim goes stale under the still-running execution, so the next
    // tick reclaims it and finds its key busy here.
    let Some((delay_secs, config)) = busy_key_hand_back_delay(false).await? else {
        return Ok(());
    };
    // Handed back until the running execution's next renewal notices it
    // lost the claim, not re-claimed every second until then.
    let renewal = config.claim_stale_after / 3;
    assert!(
        (delay_secs - renewal.as_secs_f64()).abs() < 0.5,
        "handed back for {delay_secs}s, renewal is {renewal:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_claim_onto_a_key_busy_with_another_operation_retries_after_a_second() -> TestResult {
    let Some((delay_secs, _)) = busy_key_hand_back_delay(true).await? else {
        return Ok(());
    };
    assert!(
        (delay_secs - 1.0).abs() < 0.3,
        "handed back for {delay_secs}s"
    );
    Ok(())
}

#[tokio::test]
async fn an_execution_finishing_during_a_claim_frees_its_sandbox_for_that_claim() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(8).await?
    else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    create_op(&store, org_id, OperationKind::DeleteSandbox, "sbx_reap").await?;
    let executor = BlockingExecutor::default();
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hook: super::BeforeClaimHook = {
        let armed = armed.clone();
        let executor = executor.clone();
        Arc::new(move || {
            let armed = armed.clone();
            let executor = executor.clone();
            Box::pin(async move {
                if !armed.swap(false, Ordering::SeqCst) {
                    return;
                }
                // The first delete finishes after this tick's reap and before
                // its claim. Its task drops its executor clone as it
                // completes; on this current-thread runtime that means it has
                // finished and waits unreaped.
                let alive = Arc::strong_count(&executor.started);
                executor.release.notify_one();
                while Arc::strong_count(&executor.started) >= alive {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
        })
    };
    let shutdown = CancellationToken::new();
    let dispatcher = tokio::spawn(super::BEFORE_CLAIM.scope(
        hook,
        run(
            fast_loop_config(2),
            store.clone(),
            executor.clone(),
            shutdown.clone(),
        ),
    ));
    tokio::time::timeout(Duration::from_secs(5), executor.started.notified()).await?;
    let second = create_op(&store, org_id, OperationKind::DeleteSandbox, "sbx_reap").await?;
    armed.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(5), executor.started.notified()).await?;
    executor.release.notify_one();
    super::wait_for_state(&db.pool, second, OperationState::Succeeded).await?;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;

    // Claimed once and run, not handed back as if the finished delete were
    // still running here.
    let claims: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM public_operation_events
         WHERE operation_id = $1 AND event_type = $2"
    ))
    .bind(second.as_uuid())
    .bind(PublicOperationEventType::Claimed.as_str())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(claims, 1);
    Ok(())
}
