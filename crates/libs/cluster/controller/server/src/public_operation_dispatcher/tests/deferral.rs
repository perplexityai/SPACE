//! Retry deferral: a requeue after an execution handed its wait back spends
//! no attempt, is spaced and bounded by the time since its first claim, and
//! has its own switch.

use super::*;
use pplx_space_api::retry_deferral::defer_retry_wait;
use pplx_space_common_core::types::{SandboxId, SnapshotId};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// A wake behind a lingering prior-generation pod: until `fence_cleared`,
/// every attempt fails retryably, handing the wait back when the dispatcher
/// defers and otherwise waiting it out in the slot.
#[derive(Clone, Default)]
struct FencedRestoreExecutor {
    fence_cleared: Arc<AtomicBool>,
    deferred_attempts: Arc<AtomicUsize>,
    in_slot_attempts: Arc<AtomicUsize>,
    /// Requeue for activation backpressure instead of a transient failure.
    backpressure: Arc<AtomicBool>,
    /// Carry the fence's conflict as the error to record if the driver fails
    /// the operation instead of requeueing it.
    carry_error: Arc<AtomicBool>,
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
        let carry_error = self.carry_error.load(Ordering::SeqCst);
        OperationExecutionResult {
            state: OperationState::Queued,
            progress_message: "restore claim is fenced by a lingering pod".to_string(),
            result_json: None,
            terminal_error_code: carry_error.then_some(PublicErrorCode::Conflict),
            terminal_error_json: carry_error
                .then(|| serde_json::json!({ "message": "restore claim is fenced" })),
            retry_after: Some(Duration::from_secs(5)),
            requeue_reason: if self.backpressure.load(Ordering::SeqCst) {
                PublicOperationRequeueReason::ActivationBackpressure
            } else {
                PublicOperationRequeueReason::TransientFailure
            },
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

/// Moves the operation's clock: it was requested and first claimed
/// `age_secs` ago and is due now.
async fn age_and_make_due(pool: &PgPool, operation_id: OperationId, age_secs: i64) -> TestResult {
    sqlx::query(sql!(
        "UPDATE public_operations
         SET created_at = NOW() - make_interval(secs => $2::BIGINT),
             requested_at = NOW() - make_interval(secs => $2::BIGINT),
             started_at = NOW() - make_interval(secs => $2::BIGINT),
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
        // The 5 s hint plus an equal-jittered eighth of the time since the
        // first claim, that term capped at a minute.
        let ceiling = (age_secs as f64 / 8.0).min(60.0);
        assert!(
            (5.0 + ceiling / 2.0 - 0.5..=5.0 + ceiling + 0.5).contains(&delay_secs),
            "age {age_secs}s: delay {delay_secs}s"
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
async fn a_deferred_operation_past_its_window_fails_with_its_existing_error() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let executor = FencedRestoreExecutor::default();
    let config = deferral_config();
    let max_age = config
        .m1_max_age
        .expect("default config has an age deadline");
    let window = pplx_space_api::public_operation_driver::DEFERRED_RETRY_WINDOW;

    // A start has no age deadline: past the deferral window it fails as an
    // exhausted retry budget, the error it would have reached before.
    let start = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Native,
        OperationKind::StartSandbox,
        "sbx_fenced_start",
    )
    .await?;
    age_and_make_due(&db.pool, start, max_age.as_secs() as i64 + 1).await?;
    assert_eq!(tick(&config, &store, &executor).await?.retried, 1);
    // The window runs from the first claim, not from creation.
    age_and_make_due(&db.pool, start, window.as_secs() as i64 + 1).await?;
    sqlx::query(sql!(
        "UPDATE public_operations SET started_at = NOW() WHERE operation_id = $1"
    ))
    .bind(start.as_uuid())
    .execute(&db.pool)
    .await?;
    assert_eq!(tick(&config, &store, &executor).await?.retried, 1);
    age_and_make_due(&db.pool, start, window.as_secs() as i64 + 1).await?;
    assert_eq!(tick(&config, &store, &executor).await?.failed, 1);
    let (state, _, _, error) = operation_row(&db.pool, start).await?;
    assert_eq!(state, "failed");
    let error = error.expect("a failed operation carries its error envelope");
    assert_eq!(error["reason"], "retry_attempts_exhausted");
    assert_eq!(error["retry_reason"], "retry_deferred");
    // Its attempts were given back, so the envelope names the window.
    assert_eq!(error["deferred_retry_window_ms"], window.as_secs() * 1000);
    let elapsed_ms = error["elapsed_since_first_claim_ms"]
        .as_u64()
        .expect("the envelope carries the elapsed time");
    assert!(
        (window.as_secs() * 1000..(window.as_secs() + 60) * 1000).contains(&elapsed_ms),
        "{elapsed_ms}"
    );

    // A wake keeps its age deadline.
    let wake = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Native,
        OperationKind::WakeSandbox,
        "sbx_fenced_wake",
    )
    .await?;
    let aging = AgesWhileRunning {
        pool: db.pool.clone(),
        inner: executor.clone(),
    };
    assert_eq!(tick(&config, &store, &aging).await?.failed, 1);
    let (_, _, _, error) = operation_row(&db.pool, wake).await?;
    let error = error.expect("a failed operation carries its error envelope");
    assert_eq!(error["reason"], "operation_deadline_exceeded");
    assert_eq!(error["last_requeue_reason"], "retry_deferred");
    Ok(())
}

/// A requeue that carries its error fails with that error's code and fields,
/// beside the driver's own evidence, whether the deferral window or the age
/// deadline ends it.
#[tokio::test]
async fn a_carried_error_joins_the_evidence_of_either_failure() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let executor = FencedRestoreExecutor::default();
    executor.carry_error.store(true, Ordering::SeqCst);
    let config = deferral_config();
    let max_age = config
        .m1_max_age
        .expect("default config has an age deadline");
    let window = pplx_space_api::public_operation_driver::DEFERRED_RETRY_WINDOW;

    // A start has no age deadline, so the deferral window ends it; a wake
    // keeps its deadline.
    for (kind, age, reason) in [
        (
            OperationKind::StartSandbox,
            window,
            "retry_attempts_exhausted",
        ),
        (
            OperationKind::WakeSandbox,
            max_age,
            "operation_deadline_exceeded",
        ),
    ] {
        let operation_id = create_queued_operation(
            &store,
            org_id,
            ApiProduct::Native,
            kind,
            &format!("sbx_carried_{kind}"),
        )
        .await?;
        age_and_make_due(&db.pool, operation_id, age.as_secs() as i64 + 1).await?;
        assert_eq!(tick(&config, &store, &executor).await?.failed, 1, "{kind}");
        let code: Option<String> = sqlx::query_scalar(sql!(
            "SELECT terminal_error_code FROM public_operations WHERE operation_id = $1"
        ))
        .bind(operation_id.as_uuid())
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(code.as_deref(), Some("conflict"), "{kind}");
        let (_, _, _, error) = operation_row(&db.pool, operation_id).await?;
        let error = error.expect("a failed operation carries its error envelope");
        assert_eq!(error["message"], "restore claim is fenced", "{kind}");
        assert_eq!(error["reason"], reason, "{kind}");
        assert_eq!(error["operation_id"], operation_id.to_string(), "{kind}");
    }
    Ok(())
}

#[tokio::test]
async fn an_executor_supplied_error_also_names_the_exhausted_window() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let executor = FencedRestoreExecutor::default();
    executor.carry_error.store(true, Ordering::SeqCst);
    let window = pplx_space_api::public_operation_driver::DEFERRED_RETRY_WINDOW;
    let operation_id = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Native,
        OperationKind::StartSandbox,
        "sbx_fenced_start_own_error",
    )
    .await?;
    age_and_make_due(&db.pool, operation_id, window.as_secs() as i64 + 1).await?;
    assert_eq!(tick(&deferral_config(), &store, &executor).await?.failed, 1);
    let (_, _, _, error) = operation_row(&db.pool, operation_id).await?;
    let error = error.expect("a failed operation carries its error envelope");
    assert_eq!(error["message"], "restore claim is fenced");
    assert_eq!(error["deferred_retry_window_ms"], window.as_secs() * 1000);
    assert!(error["elapsed_since_first_claim_ms"].as_u64().is_some());
    Ok(())
}

#[tokio::test]
async fn only_a_transient_retry_is_marked_deferred() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let executor = FencedRestoreExecutor::default();
    executor.backpressure.store(true, Ordering::SeqCst);
    let operation_id = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Native,
        OperationKind::WakeSandbox,
        "sbx_deferred_backpressure",
    )
    .await?;
    age_and_make_due(&db.pool, operation_id, 720).await?;
    assert_eq!(
        tick(&deferral_config(), &store, &executor).await?.retried,
        1
    );
    assert_eq!(executor.deferred_attempts.load(Ordering::SeqCst), 1);
    // Backpressure keeps its own pacing (the 5 s hint), not the deferred
    // spacing, which after 12 minutes would be at least 35 s.
    let (_, _, delay_secs, _) = operation_row(&db.pool, operation_id).await?;
    assert!(delay_secs < 6.0, "{delay_secs}");
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

const UPLOAD_CONFLICT_MESSAGE: &str = "sandbox has a suspend-snapshot upload in flight; retry the DELETE after the snapshot finalizes";

/// A native delete queued against a suspended sandbox whose snapshot is still
/// uploading. Returns the operation, the sandbox and the uploading snapshot.
async fn native_delete_behind_an_upload(
    pool: &PgPool,
    store: &PublicOperationStore,
) -> TestResult<(OperationId, SandboxId, SnapshotId)> {
    let org_id = OrgId::from_uuid(seed_org(pool).await?);
    let user_id = UserId::from_uuid(seed_user(pool, org_id).await?);
    let sandbox_id =
        pplx_space_db_test_fixtures::insert_suspended_sandbox(pool, org_id, user_id).await?;
    let snapshot_id =
        pplx_space_db_test_fixtures::insert_uploading_snapshot(pool, sandbox_id, 0, None).await?;
    let public_sandbox_id = PublicSandboxId::from(sandbox_id);
    let target_id = public_sandbox_id.to_string();
    let operation_id = match store
        .create_operation(pplx_space_api::public_operations::CreatePublicOperation {
            org_id,
            actor_user_id: Some(user_id),
            api_product: ApiProduct::Native,
            kind: OperationKind::DeleteSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(&target_id),
            payload_json:
                pplx_space_api::public_sandbox_operations::SandboxLifecycleOperationPayload {
                    sandbox_id: public_sandbox_id,
                    action: pplx_space_public_api::SandboxLifecycleAction::Delete,
                    rollback: None,
                    wake_activity: None,
                }
                .into_json(),
            idempotency: None,
            traceparent: None,
        })
        .await?
    {
        pplx_space_api::public_operations::CreatePublicOperationResult::Created(id) => id,
        other => return Err(format!("expected a created delete, got {other:?}").into()),
    };
    Ok((operation_id, sandbox_id, snapshot_id))
}

/// A native delete behind a snapshot upload waits as workflow progress, with
/// retry deferral on or off: no attempt is spent, so it outlasts the
/// one-attempt budget, and the spacing grows with the wait. It succeeds once
/// the upload finalizes.
#[tokio::test]
async fn a_native_delete_waits_out_an_upload_without_spending_attempts() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let store = PublicOperationStore::new(db.pool.clone());
    let executor = direct_native_executor(&db.pool, false, None, EgressProfiles::default());

    for retry_deferral_enabled in [true, false] {
        let (operation_id, sandbox_id, snapshot_id) =
            native_delete_behind_an_upload(&db.pool, &store).await?;
        let config = ReconcilerConfig {
            retry_deferral_enabled,
            ..deferral_config()
        };
        for age_secs in (0..=300).step_by(60) {
            let at = format!("deferral={retry_deferral_enabled}, age {age_secs}s");
            age_and_make_due(&db.pool, operation_id, age_secs).await?;
            let report = tick(&config, &store, &executor).await?;
            assert_eq!(report.retried, 1, "{at}");
            let (state, attempts, delay_secs, _) = operation_row(&db.pool, operation_id).await?;
            assert_eq!(state, "queued", "{at}");
            assert_eq!(
                attempts, 0,
                "{at}: waiting for the upload spends no attempt"
            );
            // The 3 s floor plus an equal-jittered eighth of the time waiting.
            let ceiling = (age_secs as f64 / 8.0).min(60.0);
            assert!(
                (3.0 + ceiling / 2.0 - 0.5..=3.0 + ceiling + 0.5).contains(&delay_secs),
                "{at}: delay {delay_secs}s"
            );
        }

        sqlx::query(sql!("UPDATE snapshots SET state = 'ready' WHERE id = $1"))
            .bind(snapshot_id)
            .execute(&db.pool)
            .await?;
        age_and_make_due(&db.pool, operation_id, 330).await?;
        assert_eq!(tick(&config, &store, &executor).await?.succeeded, 1);
        let (state, attempts, _, _) = operation_row(&db.pool, operation_id).await?;
        // Only the claim that succeeded is counted.
        assert_eq!((state.as_str(), attempts), ("succeeded", 1));
        let remaining: i64 =
            sqlx::query_scalar(sql!("SELECT COUNT(*) FROM sandboxes WHERE id = $1"))
                .bind(sandbox_id)
                .fetch_one(&db.pool)
                .await?;
        assert_eq!(remaining, 0);
    }
    Ok(())
}

/// A native delete that reaches its M1 deadline while still behind an upload
/// (here it sat queued past the deadline before its first claim, so its own
/// wait is still short) fails with the upload conflict, beside the deadline's
/// own evidence, rather than with an error that hides the cause.
#[tokio::test]
async fn a_native_delete_at_its_deadline_names_the_upload() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let store = PublicOperationStore::new(db.pool.clone());
    let (operation_id, _, _) = native_delete_behind_an_upload(&db.pool, &store).await?;
    let executor = direct_native_executor(&db.pool, false, None, EgressProfiles::default());
    let config = deferral_config();
    let max_age = config
        .m1_max_age
        .expect("default config has an age deadline");

    sqlx::query(sql!(
        "UPDATE public_operations
         SET created_at = NOW() - make_interval(secs => $2::BIGINT),
             requested_at = NOW() - make_interval(secs => $2::BIGINT)
         WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .bind(max_age.as_secs() as i64 + 1)
    .execute(&db.pool)
    .await?;
    assert_eq!(tick(&config, &store, &executor).await?.failed, 1);
    let (state, _, _, error) = operation_row(&db.pool, operation_id).await?;
    assert_eq!(state, "failed");
    let code: Option<String> = sqlx::query_scalar(sql!(
        "SELECT terminal_error_code FROM public_operations WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(code.as_deref(), Some("conflict"));
    let error = error.expect("a failed operation carries its error envelope");
    assert_eq!(error["message"], UPLOAD_CONFLICT_MESSAGE);
    assert_eq!(error["reason"], "operation_deadline_exceeded");
    assert_eq!(error["last_requeue_reason"], "workflow_progress");
    Ok(())
}
