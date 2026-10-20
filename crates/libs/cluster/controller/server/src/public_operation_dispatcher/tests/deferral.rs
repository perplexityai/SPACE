//! Retry deferral: a requeue after an execution handed its wait back spends
//! no attempt, is bounded by the operation's age, and has its own switch.

use super::*;
use pplx_space_api::retry_deferral::defer_retry_wait;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// A wake behind a lingering prior-generation pod: until `fence_cleared`,
/// every attempt fails retryably, handing the wait back when the dispatcher
/// defers and otherwise waiting it out in the slot.
#[derive(Clone, Default)]
struct FencedRestoreExecutor {
    fence_cleared: Arc<AtomicBool>,
    deferred_attempts: Arc<AtomicUsize>,
    in_slot_attempts: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl PublicOperationExecutor for FencedRestoreExecutor {
    fn supported_kinds(&self) -> &'static [OperationKind] {
        &[OperationKind::WakeSandbox, OperationKind::StartSandbox]
    }

    async fn execute(
        &self,
        _claim_token: &str,
        _operation: ClaimedPublicOperation,
    ) -> OperationExecutionResult {
        if self.fence_cleared.load(Ordering::SeqCst) {
            return OperationExecutionResult {
                state: OperationState::Succeeded,
                progress_message: "completed".to_string(),
                result_json: Some(serde_json::json!({})),
                terminal_error_code: None,
                terminal_error_json: None,
                retry_after: None,
                requeue_reason: PublicOperationRequeueReason::TransientFailure,
            };
        }
        if defer_retry_wait("test") {
            self.deferred_attempts.fetch_add(1, Ordering::SeqCst);
        } else {
            self.in_slot_attempts.fetch_add(1, Ordering::SeqCst);
        }
        OperationExecutionResult {
            state: OperationState::Queued,
            progress_message: "restore claim is fenced by a lingering pod".to_string(),
            result_json: None,
            terminal_error_code: None,
            terminal_error_json: None,
            retry_after: Some(Duration::from_secs(5)),
            requeue_reason: PublicOperationRequeueReason::TransientFailure,
        }
    }
}

fn deferral_config() -> ReconcilerConfig {
    ReconcilerConfig {
        // One counted attempt: any requeue that spent one would fail.
        max_retry_attempts: 1,
        ..single_claim_config()
    }
}

/// Moves the operation's clock: it was requested `age_secs` ago and is due
/// now.
async fn age_and_make_due(pool: &PgPool, operation_id: OperationId, age_secs: i64) -> TestResult {
    sqlx::query(sql!(
        "UPDATE public_operations
         SET created_at = NOW() - make_interval(secs => $2::BIGINT),
             requested_at = NOW() - make_interval(secs => $2::BIGINT),
             next_attempt_at = NOW()
         WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .bind(age_secs)
    .execute(pool)
    .await?;
    Ok(())
}

async fn operation_row(
    pool: &PgPool,
    operation_id: OperationId,
) -> Result<(String, i32, f64, Option<serde_json::Value>), sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT state,
                attempts,
                EXTRACT(EPOCH FROM next_attempt_at - NOW())::DOUBLE PRECISION,
                terminal_error_json
         FROM public_operations
         WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .fetch_one(pool)
    .await
}

#[tokio::test]
async fn a_restore_fenced_for_over_ten_minutes_still_converges() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let operation_id = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Native,
        OperationKind::WakeSandbox,
        "sbx_fenced",
    )
    .await?;
    let executor = FencedRestoreExecutor::default();
    let config = deferral_config();

    // Many deferred passes over the operation's first twelve minutes: far
    // more than the one counted attempt allows.
    for age_secs in (0..=720).step_by(30) {
        age_and_make_due(&db.pool, operation_id, age_secs).await?;
        let report = tick(&config, &store, &executor).await?;
        assert_eq!(report.retried, 1, "deferred at age {age_secs}s");
        let (state, attempts, delay_secs, _) = operation_row(&db.pool, operation_id).await?;
        assert_eq!(state, "queued");
        assert_eq!(attempts, 0, "a deferred requeue spends no attempt");
        // Paced at the executor's hint, not the counted backoff.
        assert!(
            (4.0..=5.5).contains(&delay_secs),
            "age {age_secs}s: delay {delay_secs}s is not the 5 s hint"
        );
    }
    assert_eq!(executor.deferred_attempts.load(Ordering::SeqCst), 25);
    assert_eq!(executor.in_slot_attempts.load(Ordering::SeqCst), 0);

    // The lingering pod is finally confirmed gone.
    executor.fence_cleared.store(true, Ordering::SeqCst);
    age_and_make_due(&db.pool, operation_id, 750).await?;
    let report = tick(&config, &store, &executor).await?;
    assert_eq!(report.succeeded, 1);
    let (state, _, _, _) = operation_row(&db.pool, operation_id).await?;
    assert_eq!(state, "succeeded");
    Ok(())
}

#[tokio::test]
async fn a_deferred_operation_past_its_age_deadline_fails_cleanly() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    // A start is outside the native age deadline for counted retries; a
    // deferred one is still bounded by age.
    let operation_id = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Native,
        OperationKind::StartSandbox,
        "sbx_fenced_forever",
    )
    .await?;
    let executor = FencedRestoreExecutor::default();
    let config = deferral_config();
    let max_age = config
        .m1_max_age
        .expect("default config has an age deadline");

    age_and_make_due(&db.pool, operation_id, max_age.as_secs() as i64 + 1).await?;
    let report = tick(&config, &store, &executor).await?;

    assert_eq!(report.failed, 1);
    let (state, attempts, _, error) = operation_row(&db.pool, operation_id).await?;
    assert_eq!(state, "failed");
    assert_eq!(attempts, 1);
    let error = error.expect("a failed operation carries its error envelope");
    assert_eq!(error["reason"], "operation_deadline_exceeded");
    assert_eq!(error["last_requeue_reason"], "retry_deferred");
    assert_eq!(error["max_age_seconds"], max_age.as_secs());
    Ok(())
}

#[tokio::test]
async fn each_switch_turns_off_only_its_own_mechanism() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());

    for (keyed_claim_enabled, retry_deferral_enabled) in
        [(true, true), (true, false), (false, true), (false, false)]
    {
        let sandbox_id = format!("sbx_switches_{keyed_claim_enabled}_{retry_deferral_enabled}");
        let first = create_queued_operation(
            &store,
            org_id,
            ApiProduct::Native,
            OperationKind::WakeSandbox,
            &sandbox_id,
        )
        .await?;
        let second = create_queued_operation(
            &store,
            org_id,
            ApiProduct::Native,
            OperationKind::WakeSandbox,
            &sandbox_id,
        )
        .await?;
        let executor = FencedRestoreExecutor::default();
        let config = ReconcilerConfig {
            keyed_claim_enabled,
            retry_deferral_enabled,
            claim_limit: 8,
            max_retry_attempts: 4,
            ..single_claim_config()
        };

        let report = tick(&config, &store, &executor).await?;

        let combination = format!("keyed={keyed_claim_enabled} deferral={retry_deferral_enabled}");
        // The keyed claim alone decides whether one sandbox's operations
        // run together.
        let expected_claimed = if keyed_claim_enabled { 1 } else { 2 };
        assert_eq!(report.claimed, expected_claimed, "{combination}");
        // Retry deferral alone decides whether the wait leaves the slot,
        // and whether the requeue spends an attempt.
        let deferred = executor.deferred_attempts.load(Ordering::SeqCst);
        let in_slot = executor.in_slot_attempts.load(Ordering::SeqCst);
        if retry_deferral_enabled {
            assert_eq!((deferred, in_slot), (expected_claimed, 0), "{combination}");
        } else {
            assert_eq!((deferred, in_slot), (0, expected_claimed), "{combination}");
        }
        let (_, attempts, _, _) = operation_row(&db.pool, first).await?;
        let expected_attempts = i32::from(!retry_deferral_enabled);
        assert_eq!(attempts, expected_attempts, "{combination}");
        // Mark both done so the next combination starts from an empty queue.
        sqlx::query(sql!(
            "UPDATE public_operations
             SET state = 'succeeded', claim_token = '', completed_at = NOW()
             WHERE operation_id = ANY($1)"
        ))
        .bind(vec![first.as_uuid(), second.as_uuid()])
        .execute(&db.pool)
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn the_run_loop_reads_the_deferral_switch() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    for retry_deferral_enabled in [true, false] {
        create_queued_operation(
            &store,
            org_id,
            ApiProduct::Native,
            OperationKind::WakeSandbox,
            &format!("sbx_run_loop_{retry_deferral_enabled}"),
        )
        .await?;
        let executor = FencedRestoreExecutor::default();
        let shutdown = CancellationToken::new();
        let dispatcher = tokio::spawn(run(
            ReconcilerConfig {
                retry_deferral_enabled,
                ..fast_loop_config(1)
            },
            store.clone(),
            executor.clone(),
            shutdown.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while executor.deferred_attempts.load(Ordering::SeqCst)
                + executor.in_slot_attempts.load(Ordering::SeqCst)
                == 0
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;
        let deferred = executor.deferred_attempts.load(Ordering::SeqCst) > 0;
        let in_slot = executor.in_slot_attempts.load(Ordering::SeqCst) > 0;
        assert_eq!(
            (deferred, in_slot),
            (retry_deferral_enabled, !retry_deferral_enabled),
            "deferral={retry_deferral_enabled}"
        );
    }
    Ok(())
}

#[test]
fn retry_deferral_switch_reads_from_env() {
    let mut env = pplx_space_test_env::TestEnv::default();
    let config = ReconcilerConfig::from_environment(&env);
    assert!(config.retry_deferral_enabled);
    env.set(RETRY_DEFERRAL_ENABLED_ENV, "false");
    let config = ReconcilerConfig::from_environment(&env);
    assert!(!config.retry_deferral_enabled);
    assert!(config.keyed_claim_enabled, "the switches are independent");
}
