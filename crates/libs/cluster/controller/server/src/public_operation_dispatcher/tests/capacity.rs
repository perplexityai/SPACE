//! How the dispatcher shares its execution slots: some are kept for
//! teardowns (sandbox deletes and kills), so a teardown starts while other
//! work holds every other slot, and some for other work, so it starts while
//! teardowns hold every other slot.

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Holds the operations it holds until `release` is cancelled, and
/// completes the others at once: it holds everything but teardowns, or with
/// `hold_teardowns`, everything. Records how many held operations ran at the
/// same time.
#[derive(Clone, Default)]
struct HeldWork {
    hold_teardowns: bool,
    release: CancellationToken,
    running: Arc<AtomicUsize>,
    max_running: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl PublicOperationExecutor for HeldWork {
    fn supported_kinds(&self) -> &'static [OperationKind] {
        &[
            OperationKind::CreateSandbox,
            OperationKind::DeleteSandbox,
            OperationKind::KillSandbox,
        ]
    }

    async fn execute(
        &self,
        _claim_token: &str,
        operation: ClaimedPublicOperation,
    ) -> OperationExecutionResult {
        if self.hold_teardowns || !TEARDOWN_KINDS.contains(&operation.kind) {
            let running = self.running.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_running.fetch_max(running, Ordering::SeqCst);
            self.release.cancelled().await;
            self.running.fetch_sub(1, Ordering::SeqCst);
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

async fn claim_with(
    store: &PublicOperationStore,
    limits: ClaimLimits,
) -> Result<Vec<OperationId>, pplx_space_api::public_operations::PublicOperationStoreError> {
    let claim = store
        .claim_operations(ClaimPublicOperations {
            limits,
            claim_stale_after: DEFAULT_CLAIM_STALE_AFTER,
            claimable_kinds: NATIVE_PUBLIC_OPERATION_KINDS,
        })
        .await?;
    let mut claimed = claim
        .operations
        .iter()
        .map(|operation| operation.id)
        .collect::<Vec<_>>();
    claimed.sort();
    Ok(claimed)
}

async fn queue_all(
    store: &PublicOperationStore,
    org_id: OrgId,
    operations: &[(OperationKind, &str)],
) -> Result<Vec<OperationId>, pplx_space_api::public_operations::PublicOperationStoreError> {
    let mut queued = Vec::new();
    for (kind, target_id) in operations {
        queued.push(
            create_queued_operation(store, org_id, ApiProduct::Native, *kind, target_id).await?,
        );
    }
    Ok(queued)
}

async fn wait_for_running(executor: &HeldWork, count: usize) -> TestResult {
    tokio::time::timeout(Duration::from_secs(10), async {
        while executor.running.load(Ordering::SeqCst) < count {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    Ok(())
}

#[test]
fn reserved_slots_parse_from_the_environment() {
    let reserved = |env: &pplx_space_test_env::TestEnv| {
        let config = ReconcilerConfig::from_environment(env);
        (
            config.delete_reserved_slots,
            config.non_delete_reserved_slots,
        )
    };
    let mut env = pplx_space_test_env::TestEnv::default();
    assert_eq!(reserved(&env), (2, 2));
    for (raw, delete, non_delete) in [
        ("0", 0, 0),
        ("5", 5, 5),
        ("500", 127, 128),
        ("two", 2, 2),
        ("", 2, 2),
    ] {
        env.set(DELETE_RESERVED_SLOTS_ENV, raw);
        env.set(NON_DELETE_RESERVED_SLOTS_ENV, raw);
        assert_eq!(reserved(&env), (delete, non_delete), "{raw:?}");
    }
}

#[tokio::test]
async fn a_claim_takes_no_more_than_its_non_teardown_limit_of_other_work() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let requested = queue_all(
        &store,
        org_id,
        &[
            (OperationKind::CreateSandbox, "sbx_capacity_create_1"),
            (OperationKind::DeleteSandbox, "sbx_capacity_delete_1"),
            (OperationKind::CreateSandbox, "sbx_capacity_create_2"),
            (OperationKind::KillSandbox, "sbx_capacity_kill_2"),
            (OperationKind::CreateSandbox, "sbx_capacity_create_3"),
            (OperationKind::DeleteSandbox, "sbx_capacity_delete_3"),
            // Still waits for the create requested before it on its sandbox.
            (OperationKind::DeleteSandbox, "sbx_capacity_create_1"),
        ],
    )
    .await?;
    let [create_1, delete_1, _, kill_2, _, delete_3, _] = requested[..] else {
        unreachable!("seven operations were requested");
    };

    // Other work takes no more than its limit; teardowns fill the rest.
    assert_eq!(
        claim_with(
            &store,
            ClaimLimits {
                non_teardown: 1,
                ..ClaimLimits::any(4)
            }
        )
        .await?,
        [create_1, delete_1, kill_2, delete_3]
    );
    // With no room for other work, only a teardown may start, and none here
    // can: the one left waits for its sandbox's create.
    assert!(
        claim_with(
            &store,
            ClaimLimits {
                non_teardown: 0,
                ..ClaimLimits::any(4)
            }
        )
        .await?
        .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn teardowns_take_slots_kept_for_other_work_only_while_none_is_due() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let limits = ClaimLimits {
        teardown_while_other_due: 1,
        ..ClaimLimits::any(3)
    };
    let requested = queue_all(
        &store,
        org_id,
        &[
            (OperationKind::DeleteSandbox, "sbx_floor_delete_1"),
            (OperationKind::KillSandbox, "sbx_floor_kill_2"),
            (OperationKind::CreateSandbox, "sbx_floor_create"),
        ],
    )
    .await?;
    let [delete_1, kill_2, create] = requested[..] else {
        unreachable!("three operations were requested");
    };

    // Other work is due, so teardowns stay within their limit.
    assert_eq!(claim_with(&store, limits).await?, [delete_1, create]);
    // With none due, teardowns may take every free slot.
    let mut later = queue_all(
        &store,
        org_id,
        &[(OperationKind::DeleteSandbox, "sbx_floor_delete_3")],
    )
    .await?;
    later.insert(0, kill_2);
    assert_eq!(claim_with(&store, limits).await?, later);
    Ok(())
}

#[tokio::test]
async fn a_teardown_starts_while_other_work_holds_every_unreserved_slot() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let mut creates = Vec::new();
    for index in 0..4 {
        creates.push(
            create_queued_operation(
                &store,
                org_id,
                ApiProduct::Native,
                OperationKind::CreateSandbox,
                &format!("sbx_held_create_{index}"),
            )
            .await?,
        );
    }
    let executor = HeldWork::default();
    let shutdown = CancellationToken::new();
    let dispatcher = tokio::spawn(run(
        ReconcilerConfig {
            delete_reserved_slots: 1,
            ..fast_loop_config(3)
        },
        store.clone(),
        executor.clone(),
        shutdown.clone(),
    ));
    wait_for_running(&executor, 2).await?;

    let teardowns = queue_all(
        &store,
        org_id,
        &[
            (OperationKind::DeleteSandbox, "sbx_deleted_meanwhile"),
            (OperationKind::KillSandbox, "sbx_killed_meanwhile"),
        ],
    )
    .await?;
    for teardown in teardowns {
        wait_for_state(&db.pool, teardown, OperationState::Succeeded).await?;
    }
    assert_eq!(executor.running.load(Ordering::SeqCst), 2);
    assert_eq!(executor.max_running.load(Ordering::SeqCst), 2);

    executor.release.cancel();
    for create in creates {
        wait_for_state(&db.pool, create, OperationState::Succeeded).await?;
    }
    assert_eq!(executor.max_running.load(Ordering::SeqCst), 2);
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;
    Ok(())
}

#[tokio::test]
async fn other_work_starts_while_teardowns_hold_every_other_slot() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let targets = (0..4)
        .map(|index| format!("sbx_held_delete_{index}"))
        .collect::<Vec<_>>();
    let mut queued = targets
        .iter()
        .map(|target_id| (OperationKind::DeleteSandbox, target_id.as_str()))
        .collect::<Vec<_>>();
    queued.push((OperationKind::CreateSandbox, "sbx_create_meanwhile"));
    let operations = queue_all(&store, org_id, &queued).await?;
    let create = *operations.last().expect("the create was queued");
    let executor = HeldWork {
        hold_teardowns: true,
        ..HeldWork::default()
    };
    let shutdown = CancellationToken::new();
    let dispatcher = tokio::spawn(run(
        ReconcilerConfig {
            delete_reserved_slots: 0,
            non_delete_reserved_slots: 1,
            ..fast_loop_config(3)
        },
        store.clone(),
        executor.clone(),
        shutdown.clone(),
    ));

    // Two held deletes and the create, though all four deletes came first.
    wait_for_running(&executor, 3).await?;
    assert_eq!(
        operation_state(&db.pool, create).await?,
        OperationState::Running.as_str()
    );

    executor.release.cancel();
    for operation in operations {
        wait_for_state(&db.pool, operation, OperationState::Succeeded).await?;
    }
    assert_eq!(executor.max_running.load(Ordering::SeqCst), 3);
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;
    Ok(())
}

/// Make `operations` due now, or with `due` unset, an hour from now.
async fn set_due(pool: &PgPool, operations: &[OperationId], due: bool) -> TestResult {
    sqlx::query(sql!(
        "UPDATE public_operations
         SET next_attempt_at = CASE WHEN $2 THEN NOW() ELSE NOW() + INTERVAL '1 hour' END
         WHERE operation_id = ANY($1)"
    ))
    .bind(
        operations
            .iter()
            .map(|operation_id| operation_id.as_uuid())
            .collect::<Vec<_>>(),
    )
    .bind(due)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn other_work_starts_while_other_work_already_runs_and_teardowns_are_due() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let first = queue_all(
        &store,
        org_id,
        &[(OperationKind::CreateSandbox, "sbx_first_create")],
    )
    .await?[0];
    let targets = (0..4)
        .map(|index| format!("sbx_wave_delete_{index}"))
        .collect::<Vec<_>>();
    let mut queued = targets
        .iter()
        .map(|target_id| (OperationKind::DeleteSandbox, target_id.as_str()))
        .collect::<Vec<_>>();
    queued.push((OperationKind::CreateSandbox, "sbx_create_after_the_wave"));
    let mut operations = queue_all(&store, org_id, &queued).await?;
    let create = *operations.last().expect("the create was queued");
    set_due(&db.pool, &operations, false).await?;
    let executor = HeldWork {
        hold_teardowns: true,
        ..HeldWork::default()
    };
    let shutdown = CancellationToken::new();
    let dispatcher = tokio::spawn(run(
        ReconcilerConfig {
            delete_reserved_slots: 0,
            non_delete_reserved_slots: 1,
            ..fast_loop_config(3)
        },
        store.clone(),
        executor.clone(),
        shutdown.clone(),
    ));
    // The first create already holds a slot when the deletes and the second
    // create come due, all at once and the deletes first in order.
    wait_for_running(&executor, 1).await?;
    set_due(&db.pool, &operations, true).await?;

    // One held delete and the create share the two free slots.
    wait_for_running(&executor, 3).await?;
    assert_eq!(
        operation_state(&db.pool, create).await?,
        OperationState::Running.as_str()
    );

    executor.release.cancel();
    operations.push(first);
    for operation in operations {
        wait_for_state(&db.pool, operation, OperationState::Succeeded).await?;
    }
    assert_eq!(executor.max_running.load(Ordering::SeqCst), 3);
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;
    Ok(())
}

#[tokio::test]
async fn a_claim_only_teardowns_could_use_waits_before_the_next() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let targets = (0..3)
        .map(|index| format!("sbx_outage_create_{index}"))
        .collect::<Vec<_>>();
    let creates = queue_all(
        &store,
        org_id,
        &targets
            .iter()
            .map(|target_id| (OperationKind::CreateSandbox, target_id.as_str()))
            .collect::<Vec<_>>(),
    )
    .await?;
    let executor = HeldWork::default();
    let claims = Arc::new(AtomicUsize::new(0));
    let hook: BeforeClaimHook = {
        let claims = claims.clone();
        Arc::new(move || {
            claims.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {})
        })
    };
    let shutdown = CancellationToken::new();
    let dispatcher = tokio::spawn(super::BEFORE_CLAIM.scope(
        hook,
        run(
            ReconcilerConfig {
                delete_reserved_slots: 1,
                non_delete_reserved_slots: 0,
                ..fast_loop_config(3)
            },
            store.clone(),
            executor.clone(),
            shutdown.clone(),
        ),
    ));
    // Other work holds every slot open to it, so only the slot kept for
    // teardowns is free, and the third create is due but cannot start.
    wait_for_running(&executor, 2).await?;
    let before = claims.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(1)).await;
    // About four claims a second, not one every 20 ms tick.
    let during = claims.load(Ordering::SeqCst) - before;
    assert!((1..=8).contains(&during), "{during} claims in a second");

    // A teardown requested meanwhile still starts in its slot.
    let delete = queue_all(
        &store,
        org_id,
        &[(
            OperationKind::DeleteSandbox,
            "sbx_deleted_during_the_outage",
        )],
    )
    .await?[0];
    wait_for_state(&db.pool, delete, OperationState::Succeeded).await?;
    assert_eq!(executor.running.load(Ordering::SeqCst), 2);

    executor.release.cancel();
    for create in creates {
        wait_for_state(&db.pool, create, OperationState::Succeeded).await?;
    }
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;
    Ok(())
}
