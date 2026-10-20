//! The dispatcher as a keyed work queue: one executing operation per
//! sandbox, deferred retries that hand their slot back, and a capped,
//! backed-off retry budget.

use super::*;
use pplx_space_api::public_operation_driver::counted_retry_backoff;
use pplx_space_api::public_operations::PUBLIC_OPERATION_CLAIM_LOCK_KEY;
use pplx_space_api::retry_deferral::retry_waits_deferred;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

const STUCK_PREFIX: &str = "sbx_stuck_";

/// Succeeds every operation after a short pause, except on `sbx_stuck_*`
/// sandboxes, which always fail retryably the way a wake behind a lingering
/// pod does: inside the dispatcher's deferral scope at once, otherwise only
/// after sleeping out a long readiness budget. Records per-sandbox overlap
/// and the order operations ran in.
#[derive(Clone, Default)]
struct KeyedRecordingExecutor {
    running_per_key: Arc<Mutex<HashMap<String, usize>>>,
    max_overlap_per_key: Arc<AtomicUsize>,
    stuck_running: Arc<AtomicUsize>,
    max_stuck_running: Arc<AtomicUsize>,
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
            let now = self.stuck_running.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_stuck_running.fetch_max(now, Ordering::SeqCst);
            self.stuck_attempts.fetch_add(1, Ordering::SeqCst);
            if !retry_waits_deferred() {
                self.undeferred_stuck_attempts
                    .fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            self.stuck_running.fetch_sub(1, Ordering::SeqCst);
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
            tokio::time::sleep(Duration::from_millis(15)).await;
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

    for operation_id in expected.values().flatten() {
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

    let claim = claim_all(&store).await?;
    assert_eq!(claimed_ids(&claim), vec![first, other]);
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
async fn delete_supersedes_a_backed_off_wake_but_not_a_pending_create() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());

    // A wake due now is about to run: the delete waits for it.
    let wake = create_op(
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

    // The wake fails retryably into a long backoff: the delete no longer
    // waits for it.
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
    assert_eq!(
        operation_state(&db.pool, delete_after_create).await?,
        OperationState::Queued.as_str()
    );
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
async fn counted_retries_back_off_and_fail_terminally_at_the_cap() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let operation_id = create_op(&store, org_id, OperationKind::WakeSandbox, "sbx_capped").await?;
    let config = ReconcilerConfig {
        max_retry_attempts: 4,
        retry_backoff_base: Duration::from_secs(8),
        retry_backoff_max: Duration::from_secs(20),
        ..single_claim_config()
    };

    let mut delays = Vec::new();
    for _ in 0..3 {
        let report = tick(&config, &store, &AlwaysRetryExecutor).await?;
        assert_eq!(report.retried, 1);
        let delay_secs: f64 = sqlx::query_scalar(sql!(
            "SELECT EXTRACT(EPOCH FROM next_attempt_at - NOW())::DOUBLE PRECISION
             FROM public_operations WHERE operation_id = $1"
        ))
        .bind(operation_id.as_uuid())
        .fetch_one(&db.pool)
        .await?;
        delays.push(delay_secs);
        defer_until(&db.pool, operation_id, 0).await?;
    }
    // Ceilings of 8s, 16s, then the 20s cap, each jittered into its upper
    // half and floored at the executor's 1s hint.
    for (delay, (low, high)) in delays.iter().zip([(3.0, 8.5), (7.0, 16.5), (9.0, 20.5)]) {
        assert!((low..=high).contains(delay), "{delays:?}");
    }

    let report = tick(&config, &store, &AlwaysRetryExecutor).await?;
    assert_eq!(report.failed, 1);
    let operation = store
        .fetch_operation(org_id, operation_id)
        .await?
        .expect("operation remains visible");
    assert_eq!(operation.state, OperationState::Failed);
    assert_eq!(
        operation.terminal_error_code,
        Some(PublicErrorCode::Internal)
    );
    Ok(())
}

#[test]
fn counted_retry_backoff_doubles_to_its_cap_with_bounded_jitter_and_honors_the_hint() {
    let base = Duration::from_secs(2);
    let max = Duration::from_secs(60);
    let hint = Duration::from_millis(500);
    for _ in 0..50 {
        let operation_id = OperationId::new();
        let mut ceiling = base;
        for attempts in 1..=16 {
            let delay = counted_retry_backoff(operation_id, attempts, hint, base, max);
            assert!(
                delay >= ceiling / 2 && delay <= ceiling,
                "{attempts}: {delay:?}"
            );
            assert_eq!(
                delay,
                counted_retry_backoff(operation_id, attempts, hint, base, max)
            );
            ceiling = (ceiling * 2).min(max);
        }
    }
    let long_hint = Duration::from_secs(30);
    assert_eq!(
        counted_retry_backoff(OperationId::new(), 1, long_hint, base, max),
        long_hint
    );
    assert_eq!(
        counted_retry_backoff(OperationId::new(), 3, hint, Duration::ZERO, max),
        hint
    );
}

#[test]
fn retry_cap_and_backoff_read_bounded_env_values() {
    let read = |env: &pplx_space_test_env::TestEnv| {
        let config = ReconcilerConfig::from_environment(env);
        (
            config.max_retry_attempts,
            config.retry_backoff_base,
            config.retry_backoff_max,
        )
    };
    let mut env = pplx_space_test_env::TestEnv::default();
    assert_eq!(
        read(&env),
        (
            DEFAULT_MAX_RETRY_ATTEMPTS,
            DEFAULT_RETRY_BACKOFF_BASE,
            DEFAULT_RETRY_BACKOFF_MAX
        )
    );
    env.set(MAX_RETRY_ATTEMPTS_ENV, "5");
    env.set(RETRY_BACKOFF_BASE_ENV, "3");
    env.set(RETRY_BACKOFF_MAX_ENV, "120");
    assert_eq!(
        read(&env),
        (5, Duration::from_secs(3), Duration::from_secs(120))
    );
    env.set(MAX_RETRY_ATTEMPTS_ENV, "100000");
    env.set(RETRY_BACKOFF_BASE_ENV, "100000");
    env.set(RETRY_BACKOFF_MAX_ENV, "100000");
    assert_eq!(
        read(&env),
        (
            i32::try_from(MAX_RETRY_ATTEMPTS_CEILING).expect("ceiling fits"),
            RETRY_BACKOFF_BASE_CEILING,
            RETRY_BACKOFF_MAX_CEILING
        )
    );
    for invalid in ["0", "-1", "abc", ""] {
        env.set(MAX_RETRY_ATTEMPTS_ENV, invalid);
        env.set(RETRY_BACKOFF_BASE_ENV, invalid);
        env.set(RETRY_BACKOFF_MAX_ENV, invalid);
        assert_eq!(
            read(&env),
            (
                DEFAULT_MAX_RETRY_ATTEMPTS,
                DEFAULT_RETRY_BACKOFF_BASE,
                DEFAULT_RETRY_BACKOFF_MAX
            ),
            "{invalid:?}"
        );
    }
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
async fn kill_switch_restores_the_unkeyed_claim_and_fixed_retry_pacing() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let first = create_op(&store, org_id, OperationKind::WakeSandbox, "sbx_unkeyed").await?;
    let second = create_op(&store, org_id, OperationKind::WakeSandbox, "sbx_unkeyed").await?;
    let config = ReconcilerConfig {
        keyed_claim_enabled: false,
        retry_backoff_base: Duration::from_secs(30),
        claim_limit: 8,
        max_retry_attempts: 4,
        ..single_claim_config()
    };

    let report = tick(&config, &store, &AlwaysRetryExecutor).await?;
    // Both wakes on the sandbox ran in the same pass, as before keying.
    assert_eq!(report.claimed, 2);
    assert_eq!(report.retried, 2);
    for operation_id in [first, second] {
        let delay_secs: f64 = sqlx::query_scalar(sql!(
            "SELECT EXTRACT(EPOCH FROM next_attempt_at - NOW())::DOUBLE PRECISION
             FROM public_operations WHERE operation_id = $1"
        ))
        .bind(operation_id.as_uuid())
        .fetch_one(&db.pool)
        .await?;
        // The executor's 1s hint, not the 30s backoff base.
        assert!(delay_secs <= 1.5, "{delay_secs}");
    }
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
