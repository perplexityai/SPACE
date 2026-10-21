//! The M1 age deadline before an operation starts: Native work older than
//! the deadline expires instead of starting, except a delete. Work that has
//! already started gets its attempt.

use super::*;

/// Succeeds every operation and records which ones it ran.
#[derive(Clone, Default)]
struct RecordingExecutor {
    ran: Arc<Mutex<Vec<OperationId>>>,
}

#[async_trait::async_trait]
impl PublicOperationExecutor for RecordingExecutor {
    fn supported_kinds(&self) -> &'static [OperationKind] {
        &[
            OperationKind::CreateSandbox,
            OperationKind::PauseSandbox,
            OperationKind::SuspendSandbox,
            OperationKind::WakeSandbox,
            OperationKind::DeleteSandbox,
        ]
    }

    async fn execute(
        &self,
        _claim_token: &str,
        operation: ClaimedPublicOperation,
    ) -> OperationExecutionResult {
        self.ran.lock().expect("lock").push(operation.id);
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

impl RecordingExecutor {
    fn ran(&self) -> Vec<OperationId> {
        let mut ran = self.ran.lock().expect("lock").clone();
        ran.sort();
        ran
    }
}

async fn claim_everything(store: &PublicOperationStore) -> TestResult {
    store
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(64),
            claim_stale_after: DEFAULT_CLAIM_STALE_AFTER,
            claimable_kinds: NATIVE_PUBLIC_OPERATION_KINDS,
        })
        .await?;
    Ok(())
}

async fn assert_expired(
    store: &PublicOperationStore,
    pool: &PgPool,
    org_id: OrgId,
    operation_id: OperationId,
) -> TestResult {
    let operation = store
        .fetch_operation(org_id, operation_id)
        .await?
        .expect("operation remains visible");
    assert_eq!(operation.state, OperationState::Expired, "{operation_id}");
    assert_eq!(
        operation.terminal_error_code,
        Some(PublicErrorCode::OperationFailed)
    );
    assert_eq!(
        operation.progress_message,
        "Operation did not reach a terminal result within 30m; \
         inspect the target state before retrying."
    );
    assert!(operation.completed_at.is_some());
    // It never ran, so it holds no attempt and no start.
    let (attempts, started): (i32, bool) = sqlx::query_as(sql!(
        "SELECT attempts, started_at IS NOT NULL
         FROM public_operations
         WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .fetch_one(pool)
    .await?;
    assert_eq!((attempts, started), (0, false), "{operation_id}");
    let error = operation
        .terminal_error
        .expect("an expired operation carries its error envelope");
    assert_eq!(error["reason"], "operation_deadline_exceeded");
    assert_eq!(error["max_age_seconds"], DEFAULT_M1_MAX_AGE.as_secs());
    assert_eq!(error["kind"], operation.kind.as_str());
    assert_eq!(error["attempts"], 0);
    let events = store
        .fetch_operation_events(org_id, operation_id, None, 10)
        .await?
        .expect("operation events remain visible");
    let event = events.last().expect("the expiry is recorded");
    assert_eq!(event.event_type, PublicOperationEventType::Expired);
    assert_eq!(event.event["error"], error);
    Ok(())
}

#[tokio::test]
async fn work_first_claimed_past_its_deadline_expires_unrun_but_started_work_and_deletes_run()
-> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    // A create whose claim went stale while the controller was down: it
    // started, so it may have half-built its sandbox, and gets its attempt.
    let stale_create = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Native,
        OperationKind::CreateSandbox,
        "sbx_stale_create",
    )
    .await?;
    claim_everything(&store).await?;
    sqlx::query(sql!(
        "UPDATE public_operations
         SET claimed_at = NOW() - INTERVAL '2 minutes'
         WHERE operation_id = $1"
    ))
    .bind(stale_create.as_uuid())
    .execute(&db.pool)
    .await?;
    let mut overdue = Vec::new();
    for (kind, target_id) in [
        (OperationKind::CreateSandbox, "sbx_old_create"),
        (OperationKind::PauseSandbox, "sbx_old_pause"),
        (OperationKind::SuspendSandbox, "sbx_old_suspend"),
        (OperationKind::WakeSandbox, "sbx_old_wake"),
    ] {
        overdue.push(
            create_queued_operation(&store, org_id, ApiProduct::Native, kind, target_id).await?,
        );
    }
    let delete = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Native,
        OperationKind::DeleteSandbox,
        "sbx_old_delete",
    )
    .await?;
    let admin_create = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Admin,
        OperationKind::CreateSandbox,
        "sbx_old_admin_create",
    )
    .await?;
    let young_wake = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Native,
        OperationKind::WakeSandbox,
        "sbx_young_wake",
    )
    .await?;
    let mut old = overdue.clone();
    old.extend([stale_create, delete, admin_create]);
    backdate_operations(&db.pool, &old).await?;
    let executor = RecordingExecutor::default();
    let config = ReconcilerConfig {
        claim_limit: 64,
        execution_concurrency: 8,
        ..ReconcilerConfig::default()
    };

    let report = tick(&config, &store, &executor).await?;

    assert_eq!((report.expired, report.succeeded), (4, 4), "{report:?}");
    let mut ran = vec![stale_create, delete, admin_create, young_wake];
    ran.sort();
    assert_eq!(executor.ran(), ran);
    for operation_id in overdue {
        assert_expired(&store, &db.pool, org_id, operation_id).await?;
    }
    Ok(())
}

#[tokio::test]
async fn work_handed_back_unrun_at_its_first_claim_still_expires_past_its_deadline() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let pause = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Native,
        OperationKind::PauseSandbox,
        "sbx_handed_back_pause",
    )
    .await?;
    let claim = store
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(64),
            claim_stale_after: DEFAULT_CLAIM_STALE_AFTER,
            claimable_kinds: NATIVE_PUBLIC_OPERATION_KINDS,
        })
        .await?;
    let operation = claim
        .operations
        .into_iter()
        .next()
        .expect("the pause is claimed");
    // Its sandbox was still executing here, so the dispatcher hands the pause
    // back without running it.
    hand_back_busy_key(store.clone(), claim.claim_token, operation, Duration::ZERO).await;
    backdate_operations(&db.pool, &[pause]).await?;
    let executor = RecordingExecutor::default();
    let config = ReconcilerConfig {
        claim_limit: 64,
        execution_concurrency: 8,
        ..ReconcilerConfig::default()
    };

    let report = tick(&config, &store, &executor).await?;

    assert_eq!((report.expired, report.succeeded), (1, 0), "{report:?}");
    assert!(executor.ran().is_empty());
    assert_expired(&store, &db.pool, org_id, pause).await?;
    Ok(())
}

#[tokio::test]
async fn queued_work_past_its_deadline_expires_though_nothing_claims_it() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    // A create still running under a live claim, past the deadline it
    // crossed while running, holds back the wake after it.
    let running = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Native,
        OperationKind::CreateSandbox,
        "sbx_busy",
    )
    .await?;
    claim_everything(&store).await?;
    let held_back_wake = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Native,
        OperationKind::WakeSandbox,
        "sbx_busy",
    )
    .await?;
    // Retry backoffs keep the rest from being claimed.
    let mut backed_off = Vec::new();
    for (api_product, kind, target_id) in [
        (
            ApiProduct::Native,
            OperationKind::PauseSandbox,
            "sbx_backoff_pause",
        ),
        (
            ApiProduct::Native,
            OperationKind::DeleteSandbox,
            "sbx_backoff_delete",
        ),
        (
            ApiProduct::Admin,
            OperationKind::WakeSandbox,
            "sbx_backoff_admin_wake",
        ),
        (
            ApiProduct::Native,
            OperationKind::CreateSandbox,
            "sbx_backoff_young_create",
        ),
    ] {
        backed_off
            .push(create_queued_operation(&store, org_id, api_product, kind, target_id).await?);
    }
    let [pause, delete, admin_wake, young_create] = backed_off[..] else {
        unreachable!("four operations were created");
    };
    // A suspend that already ran an attempt keeps it: it is retried, and the
    // requeue deadline decides, so a half-applied suspend is not abandoned.
    let started_suspend = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Native,
        OperationKind::SuspendSandbox,
        "sbx_backoff_started_suspend",
    )
    .await?;
    backed_off.push(started_suspend);
    sqlx::query(sql!(
        "UPDATE public_operations
         SET next_attempt_at = NOW() + INTERVAL '1 hour',
             started_at = CASE WHEN operation_id = $2 THEN NOW() ELSE started_at END,
             attempts = CASE WHEN operation_id = $2 THEN 1 ELSE attempts END
         WHERE operation_id = ANY($1)"
    ))
    .bind(
        backed_off
            .iter()
            .map(|operation_id| operation_id.as_uuid())
            .collect::<Vec<_>>(),
    )
    .bind(started_suspend.as_uuid())
    .execute(&db.pool)
    .await?;
    backdate_operations(
        &db.pool,
        &[
            running,
            held_back_wake,
            pause,
            delete,
            admin_wake,
            started_suspend,
        ],
    )
    .await?;
    let executor = RecordingExecutor::default();
    let shutdown = CancellationToken::new();
    let dispatcher = tokio::spawn(run(
        fast_loop_config(4),
        store.clone(),
        executor.clone(),
        shutdown.clone(),
    ));

    wait_for_state(&db.pool, held_back_wake, OperationState::Expired).await?;
    wait_for_state(&db.pool, pause, OperationState::Expired).await?;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;

    assert_expired(&store, &db.pool, org_id, held_back_wake).await?;
    assert_expired(&store, &db.pool, org_id, pause).await?;
    for (operation_id, state) in [
        (running, OperationState::Running),
        (delete, OperationState::Queued),
        (admin_wake, OperationState::Queued),
        (young_create, OperationState::Queued),
        (started_suspend, OperationState::Queued),
    ] {
        assert_eq!(
            operation_state(&db.pool, operation_id).await?,
            state.as_str(),
            "{operation_id}"
        );
    }
    assert!(executor.ran().is_empty());
    Ok(())
}
