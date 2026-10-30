//! The dispatcher as a keyed work queue: one executing operation per
//! sandbox, and deferred retries that hand their slot back.

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
            limits: ClaimLimits::any(64),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: NATIVE_PUBLIC_OPERATION_KINDS,
        })
        .await
}

/// Claims `create`, the only due operation, lets a delete of its sandbox be
/// requested while it holds the claim, so the delete cannot cancel it, then
/// requeues it straight into a five-minute backoff through the store, as no
/// dispatcher execution would. Returns the delete.
async fn delete_behind_a_create_in_backoff(
    store: &PublicOperationStore,
    org_id: OrgId,
    create: OperationId,
    target_id: &str,
) -> Result<OperationId, Box<dyn std::error::Error + Send + Sync>> {
    let claim = claim_all(store).await?;
    assert_eq!(claimed_ids(&claim), vec![create]);
    let delete = create_op(store, org_id, OperationKind::DeleteSandbox, target_id).await?;
    assert_eq!(
        store
            .release_claimed_operation(ReleaseClaimedPublicOperation {
                claim_token: claim.claim_token.clone(),
                operation_id: create,
                progress_message: "sandbox pod is not ready".to_string(),
                retry_after: Duration::from_secs(300),
                count_attempt: true,
                undo_first_start: false,
                cancel_if_superseded: false,
                carried_error: None,
            })
            .await?,
        ReleasedClaim::Requeued
    );
    Ok(delete)
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

/// One round of a lifecycle route admitting work inline on each of
/// `sandboxes`: it records an operation, and runs it through `executor`
/// beside the dispatchers when admission claims it, as an API process does.
/// Returns the operations it recorded and how many it claimed.
async fn admit_inline_round(
    store: &PublicOperationStore,
    org_id: OrgId,
    executor: &KeyedRecordingExecutor,
    sandboxes: &[&str],
    round: usize,
    inline_runs: &mut JoinSet<ExecutionOutcome>,
) -> Result<(Vec<OperationId>, usize), Box<dyn std::error::Error + Send + Sync>> {
    use pplx_space_api::public_operations::{
        CreateInlinePublicOperationResult, CreatePublicOperation, CreatePublicOperationIdempotency,
        HttpOperationMethod, OperationPathTemplate,
    };

    let mut recorded = Vec::new();
    let mut claimed = 0;
    for sandbox in sandboxes {
        let key = pplx_space_common_core::types::IdempotencyKey::parse(format!(
            "inline-{round}-{sandbox}"
        ))?;
        let fingerprint = pplx_space_public_api::RequestFingerprint::parse("a".repeat(64))?;
        let idempotency = CreatePublicOperationIdempotency {
            method: HttpOperationMethod::Post,
            path_template: OperationPathTemplate::new("/v1/sandboxes/{sandbox_id}:wake"),
            key: &key,
            request_fingerprint: &fingerprint,
        };
        let kind = if round.is_multiple_of(2) {
            OperationKind::WakeSandbox
        } else {
            OperationKind::StopSandbox
        };
        let result = store
            .begin_operation_admission(org_id, ApiProduct::Native, &idempotency)
            .await?
            .create_claimed_if_sole_active(CreatePublicOperation {
                org_id,
                actor_user_id: None,
                api_product: ApiProduct::Native,
                kind,
                target_type: Some(pplx_space_public_api::OperationTargetType::Sandbox),
                target_id: Some(sandbox),
                payload_json: serde_json::json!({}),
                idempotency: Some(idempotency.clone()),
                traceparent: None,
            })
            .await?;
        match result {
            CreateInlinePublicOperationResult::Claimed {
                claim_token,
                operation,
            } => {
                claimed += 1;
                recorded.push(operation.id);
                let executor = executor.clone();
                let store = store.clone();
                inline_runs.spawn(async move {
                    let execution = {
                        let claim_token = claim_token.clone();
                        let operation = operation.clone();
                        async move { executor.execute(&claim_token, operation).await }
                    };
                    drive_deferrable_operation(
                        store,
                        claim_token,
                        OperationExecutionConfig::default(),
                        operation,
                        OperationExecutor::Inline,
                        execution,
                    )
                    .await
                });
            }
            CreateInlinePublicOperationResult::Queued(operation_id) => recorded.push(operation_id),
            CreateInlinePublicOperationResult::NotCreated(other) => {
                panic!("a new key records an operation: {other:?}")
            }
        }
    }
    Ok((recorded, claimed))
}

/// Two dispatchers and an API process claiming the operations it admits all
/// run one sandbox's operations one at a time, in request order.
#[tokio::test]
async fn one_sandboxs_operations_never_overlap_across_two_dispatchers() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(16).await?
    else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let shared = ["sbx_shared_a", "sbx_shared_b", "sbx_shared_c"];
    let kinds = [
        OperationKind::WakeSandbox,
        OperationKind::StopSandbox,
        OperationKind::WakeSandbox,
        OperationKind::StopSandbox,
        OperationKind::WakeSandbox,
    ];
    let mut recorded = Vec::new();
    for kind in kinds {
        for sandbox in shared {
            recorded.push(create_op(&store, org_id, kind, sandbox).await?);
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
    let mut inline_runs = JoinSet::new();
    // Work queued on every sandbox: admission queues behind it.
    let (queued, claimed) =
        admit_inline_round(&store, org_id, &executor, &shared, 0, &mut inline_runs).await?;
    assert_eq!(claimed, 0);
    recorded.extend(queued);

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
    // Admission keeps racing the dispatchers as they drain the sandboxes.
    for round in 1..=30 {
        let (more, _) =
            admit_inline_round(&store, org_id, &executor, &shared, round, &mut inline_runs).await?;
        recorded.extend(more);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for operation_id in recorded.iter().chain(&bypassed) {
        wait_for_state(&db.pool, *operation_id, OperationState::Succeeded).await?;
    }
    // Every sandbox idle again: admission claims each new operation.
    let (last, claimed) =
        admit_inline_round(&store, org_id, &executor, &shared, 31, &mut inline_runs).await?;
    assert_eq!(claimed, shared.len());
    while let Some(run) = inline_runs.join_next().await {
        run??;
    }
    for operation_id in &last {
        wait_for_state(&db.pool, *operation_id, OperationState::Succeeded).await?;
    }
    shutdown.cancel();
    for dispatcher in dispatchers {
        tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;
    }

    assert_eq!(executor.max_overlap_per_key.load(Ordering::SeqCst), 1);
    let completed = executor.completed.lock().expect("lock").clone();
    for sandbox in shared {
        let ran = completed
            .iter()
            .filter(|(key, _)| key == sandbox)
            .map(|(_, operation_id)| operation_id.as_uuid())
            .collect::<Vec<_>>();
        let requested: Vec<uuid::Uuid> = sqlx::query_scalar(sql!(
            "SELECT operation_id FROM public_operations
             WHERE target_id = $1 ORDER BY requested_at, operation_id"
        ))
        .bind(sandbox)
        .fetch_all(&db.pool)
        .await?;
        assert_eq!(ran, requested, "{sandbox} ran in request order");
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
            undo_first_start: false,
            cancel_if_superseded: false,
            carried_error: None,
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

    // A create in backoff that the delete could not cancel still owns the
    // sandbox's future: a delete that ran first would let the create
    // resurrect it afterwards.
    let create = create_op(
        &store,
        org_id,
        OperationKind::CreateSandbox,
        "sbx_create_delete",
    )
    .await?;
    let delete_after_create =
        delete_behind_a_create_in_backoff(&store, org_id, create, "sbx_create_delete").await?;

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
            undo_first_start: false,
            cancel_if_superseded: false,
            carried_error: None,
        })
        .await?;
    assert_eq!(claimed_ids(&claim_all(&store).await?), vec![delete]);
    for held in [second_wake, delete_after_create] {
        assert_eq!(
            operation_state(&db.pool, held).await?,
            OperationState::Queued.as_str()
        );
    }
    // The wake comes due while the delete runs: the delete's live claim
    // holds it back.
    defer_until(&db.pool, wake, 0).await?;
    assert_eq!(claimed_ids(&claim_all(&store).await?), Vec::new());
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
    assert_eq!(
        store
            .release_claimed_operation(ReleaseClaimedPublicOperation {
                claim_token: first_claim.claim_token.clone(),
                operation_id: first,
                progress_message: "late".to_string(),
                retry_after: Duration::from_secs(1),
                count_attempt: true,
                undo_first_start: false,
                cancel_if_superseded: false,
                carried_error: None,
            })
            .await?,
        ReleasedClaim::Lost
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
            limits: ClaimLimits::any(8),
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
/// same id. A delete cancels a create that is only queued; one it could
/// not cancel, because the create held a claim when the delete arrived, the
/// delete waits for, even while the create sits in retry backoff.
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
    let delete = delete_behind_a_create_in_backoff(&store, org_id, create, "sbx_resurrect").await?;
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
                   AND EXISTS (
                       SELECT 1 FROM public_operation_events
                       WHERE operation_id = $1 AND event_type = $2
                   )"
            ))
            .bind(handed_back.as_uuid())
            .bind(PublicOperationEventType::Requeued.as_str())
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
    let first = create_op(&store, org_id, OperationKind::DeleteSandbox, "sbx_reap").await?;
    let executor = BlockingExecutor::default();
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hook: super::BeforeClaimHook = {
        let armed = armed.clone();
        let executor = executor.clone();
        let pool = db.pool.clone();
        Arc::new(move || {
            let armed = armed.clone();
            let executor = executor.clone();
            let pool = pool.clone();
            Box::pin(async move {
                if !armed.swap(false, Ordering::SeqCst) {
                    return;
                }
                // The first delete finishes after this tick's reap and before
                // its claim. Its execution drops its executor clone before
                // the driver writes its completion, so wait for both: once
                // the completion is visible, this claim's snapshot sees the
                // sandbox free while the task waits unreaped.
                let alive = Arc::strong_count(&executor.started);
                executor.release.notify_one();
                while Arc::strong_count(&executor.started) >= alive {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                super::wait_for_state(&pool, first, OperationState::Succeeded)
                    .await
                    .expect("the first delete completes");
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

async fn assert_superseded(
    store: &PublicOperationStore,
    org_id: OrgId,
    create: OperationId,
    delete: OperationId,
) -> TestResult {
    let operation = store
        .fetch_operation(org_id, create)
        .await?
        .expect("the create remains visible");
    assert_eq!(operation.state, OperationState::Canceled, "{create}");
    assert_eq!(
        operation.terminal_error_code,
        Some(PublicErrorCode::OperationFailed)
    );
    assert!(operation.completed_at.is_some());
    let error = operation
        .terminal_error
        .expect("a canceled create carries its error envelope");
    assert_eq!(error["reason"], "superseded_by_delete");
    assert_eq!(error["delete_operation_id"], delete.to_string());
    let events = store
        .fetch_operation_events(org_id, create, None, 20)
        .await?
        .expect("the create's events remain visible");
    let event = events.last().expect("the cancel is recorded");
    assert_eq!(event.event_type, PublicOperationEventType::Canceled);
    assert_eq!(event.event["error"], error);
    Ok(())
}

#[tokio::test]
async fn a_delete_cancels_its_sandboxs_queued_create_which_then_never_runs() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let other_org = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    // One create is waiting out a retry backoff, one has never been claimed.
    let backed_off =
        create_op(&store, org_id, OperationKind::CreateSandbox, "sbx_cancel_1").await?;
    defer_until(&db.pool, backed_off, 300).await?;
    let fresh = create_op(&store, org_id, OperationKind::CreateSandbox, "sbx_cancel_2").await?;
    // Neither another sandbox's create nor another organization's create of
    // the same id is the delete's to cancel.
    let unrelated = create_op(&store, org_id, OperationKind::CreateSandbox, "sbx_cancel_3").await?;
    let foreign = create_op(
        &store,
        other_org,
        OperationKind::CreateSandbox,
        "sbx_cancel_1",
    )
    .await?;
    let delete_1 = create_op(&store, org_id, OperationKind::DeleteSandbox, "sbx_cancel_1").await?;
    let delete_2 = create_op(&store, org_id, OperationKind::DeleteSandbox, "sbx_cancel_2").await?;
    assert_superseded(&store, org_id, backed_off, delete_1).await?;
    assert_superseded(&store, org_id, fresh, delete_2).await?;

    let executor = KeyedRecordingExecutor::default();
    let config = ReconcilerConfig {
        claim_limit: 8,
        execution_concurrency: 8,
        ..single_claim_config()
    };
    defer_until(&db.pool, backed_off, 0).await?;
    while tick(&config, &store, &executor).await?.claimed > 0 {}

    let mut ran = executor
        .completed
        .lock()
        .expect("lock")
        .iter()
        .map(|(_, id)| *id)
        .collect::<Vec<_>>();
    ran.sort();
    let mut expected = vec![unrelated, foreign, delete_1, delete_2];
    expected.sort();
    assert_eq!(ran, expected);
    assert_superseded(&store, org_id, backed_off, delete_1).await?;
    Ok(())
}

/// A create whose attempt is running when a delete of its sandbox is
/// requested: the delete arrives from inside the attempt, which then fails
/// retryably.
#[derive(Clone)]
struct DeleteDuringCreate {
    store: PublicOperationStore,
    org_id: OrgId,
    delete: Arc<Mutex<Option<OperationId>>>,
    ran: Arc<Mutex<Vec<OperationId>>>,
}

#[async_trait::async_trait]
impl PublicOperationExecutor for DeleteDuringCreate {
    fn supported_kinds(&self) -> &'static [OperationKind] {
        &[OperationKind::CreateSandbox, OperationKind::DeleteSandbox]
    }

    async fn execute(
        &self,
        _claim_token: &str,
        operation: ClaimedPublicOperation,
    ) -> OperationExecutionResult {
        self.ran.lock().expect("lock").push(operation.id);
        if operation.kind == OperationKind::CreateSandbox {
            let delete = create_op(
                &self.store,
                self.org_id,
                OperationKind::DeleteSandbox,
                operation
                    .target_id
                    .as_deref()
                    .expect("a create names its sandbox"),
            )
            .await
            .expect("the delete is requested");
            *self.delete.lock().expect("lock") = Some(delete);
            return OperationExecutionResult {
                state: OperationState::Queued,
                progress_message: "timed out waiting for sandbox pod to become ready".to_string(),
                result_json: None,
                terminal_error_code: None,
                terminal_error_json: Some(serde_json::json!({
                    "reason": "sandbox_not_ready",
                    "readiness_phase": "pod_never_ready",
                })),
                retry_after: Some(Duration::from_secs(1)),
                requeue_reason: PublicOperationRequeueReason::TransientFailure,
            };
        }
        OperationExecutionResult {
            state: OperationState::Succeeded,
            progress_message: "completed".to_string(),
            result_json: Some(serde_json::json!({})),
            terminal_error_code: None,
            terminal_error_json: None,
            retry_after: None,
            requeue_reason: PublicOperationRequeueReason::TransientFailure,
        }
    }
}

#[tokio::test]
async fn a_create_mid_attempt_when_a_delete_arrives_ends_canceled_instead_of_retrying() -> TestResult
{
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let create = create_op(
        &store,
        org_id,
        OperationKind::CreateSandbox,
        "sbx_mid_attempt",
    )
    .await?;
    let executor = DeleteDuringCreate {
        store: store.clone(),
        org_id,
        delete: Arc::default(),
        ran: Arc::default(),
    };
    let config = ReconcilerConfig {
        claim_limit: 8,
        execution_concurrency: 8,
        max_retry_attempts: 10,
        ..single_claim_config()
    };

    // The delete waits for the running attempt; the attempt then ends the
    // create instead of requeueing it.
    let report = tick(&config, &store, &executor).await?;
    assert_eq!((report.canceled, report.retried), (1, 0), "{report:?}");
    let delete = executor
        .delete
        .lock()
        .expect("lock")
        .expect("the delete was requested");
    assert_superseded(&store, org_id, create, delete).await?;
    // The record keeps why the attempt failed, under its own reason.
    let error = store
        .fetch_operation(org_id, create)
        .await?
        .and_then(|operation| operation.terminal_error)
        .expect("a canceled create carries its error envelope");
    assert_eq!(
        error["last_progress_message"],
        "timed out waiting for sandbox pod to become ready"
    );
    assert_eq!(error["readiness_phase"], "pod_never_ready");

    while tick(&config, &store, &executor).await?.claimed > 0 {}
    assert_eq!(*executor.ran.lock().expect("lock"), vec![create, delete]);
    assert_eq!(
        operation_state(&db.pool, delete).await?,
        OperationState::Succeeded.as_str()
    );
    Ok(())
}

#[tokio::test]
async fn a_create_handed_back_after_its_delete_arrives_ends_canceled() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let handed_back = create_op(
        &store,
        org_id,
        OperationKind::CreateSandbox,
        "sbx_handed_back_create",
    )
    .await?;
    let still_executing = create_op(
        &store,
        org_id,
        OperationKind::CreateSandbox,
        "sbx_still_executing_create",
    )
    .await?;
    let claim = claim_all(&store).await?;
    // Each delete arrives while its create is claimed, so it cannot cancel it.
    let delete = create_op(
        &store,
        org_id,
        OperationKind::DeleteSandbox,
        "sbx_handed_back_create",
    )
    .await?;
    create_op(
        &store,
        org_id,
        OperationKind::DeleteSandbox,
        "sbx_still_executing_create",
    )
    .await?;
    for operation in claim.operations {
        // The second create's own earlier execution still runs here, so
        // canceling it would let its delete start under that execution.
        let executing_here = operation.id == still_executing;
        hand_back_busy_key(
            store.clone(),
            claim.claim_token.clone(),
            operation,
            BUSY_KEY_RETRY_AFTER,
            executing_here,
        )
        .await;
    }

    assert_superseded(&store, org_id, handed_back, delete).await?;
    assert_eq!(
        operation_state(&db.pool, still_executing).await?,
        OperationState::Queued.as_str()
    );
    Ok(())
}

#[tokio::test]
async fn a_delete_waits_for_a_claim_holding_its_create_and_cancels_what_it_left_queued()
-> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let create = create_op(
        &store,
        org_id,
        OperationKind::CreateSandbox,
        "sbx_create_a_claim_considered",
    )
    .await?;
    // A claim has the create locked as a candidate, and will drop it.
    let mut claim = db.pool.begin().await?;
    sqlx::query(sql!(
        "SELECT operation_id FROM public_operations WHERE operation_id = $1 FOR UPDATE"
    ))
    .bind(create.as_uuid())
    .execute(&mut *claim)
    .await?;
    let requested = tokio::spawn({
        let store = store.clone();
        async move {
            create_op(
                &store,
                org_id,
                OperationKind::DeleteSandbox,
                "sbx_create_a_claim_considered",
            )
            .await
        }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!requested.is_finished(), "the delete waits for the claim");

    claim.rollback().await?;
    let delete = tokio::time::timeout(Duration::from_secs(5), requested).await???;
    assert_superseded(&store, org_id, create, delete).await?;
    Ok(())
}

#[tokio::test]
async fn a_create_released_while_its_delete_commits_ends_canceled() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(4).await?
    else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let create = create_op(
        &store,
        org_id,
        OperationKind::CreateSandbox,
        "sbx_released_meanwhile",
    )
    .await?;
    let claim = claim_all(&store).await?;
    // The delete is requested, but not yet committed, when the attempt ends.
    let mut requesting = db.pool.begin().await?;
    let (requested, _superseded) = pplx_space_api::public_operations::create_operation_tx(
        &mut requesting,
        pplx_space_api::public_operations::CreatePublicOperation {
            org_id,
            actor_user_id: None,
            api_product: ApiProduct::Native,
            kind: OperationKind::DeleteSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some("sbx_released_meanwhile"),
            payload_json: serde_json::json!({}),
            idempotency: None,
            traceparent: None,
        },
    )
    .await?;
    let pplx_space_api::public_operations::CreatePublicOperationResult::Created(delete) = requested
    else {
        panic!("expected a created delete, got {requested:?}");
    };
    let released = tokio::spawn({
        let store = store.clone();
        let claim_token = claim.claim_token.clone();
        async move {
            store
                .release_claimed_operation(ReleaseClaimedPublicOperation {
                    claim_token,
                    operation_id: create,
                    progress_message: "sandbox pod is not ready".to_string(),
                    retry_after: Duration::from_secs(1),
                    count_attempt: true,
                    undo_first_start: false,
                    cancel_if_superseded: true,
                    carried_error: None,
                })
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!released.is_finished(), "the release waits for the delete");

    requesting.commit().await?;
    let released = tokio::time::timeout(Duration::from_secs(5), released).await???;
    assert_eq!(released, ReleasedClaim::Superseded { delete_id: delete });
    assert_superseded(&store, org_id, create, delete).await?;
    Ok(())
}
