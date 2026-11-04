//! How the dispatcher shares its execution slots: some are kept for
//! teardowns (sandbox deletes and kills), so a teardown starts while other
//! work holds every other slot, and some for other work, so it starts while
//! teardowns hold every other slot; and no organization's other work holds
//! more than its limit while another organization's work waits.

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
        (config.teardown_reserved_slots, config.other_reserved_slots)
    };
    let mut env = pplx_space_test_env::TestEnv::default();
    assert_eq!(reserved(&env), (2, 2));
    for (raw, teardown, other) in [
        ("0", 0, 0),
        ("5", 5, 5),
        ("500", 127, 128),
        ("two", 2, 2),
        ("", 2, 2),
    ] {
        env.set(TEARDOWN_RESERVED_SLOTS_ENV, raw);
        env.set(OTHER_RESERVED_SLOTS_ENV, raw);
        assert_eq!(reserved(&env), (teardown, other), "{raw:?}");
    }
}

#[test]
fn slot_policy_keeps_each_class_its_slots() {
    // (execution_concurrency, teardown_reserved, other_reserved) as
    // configured, the policy they clamp to, then (in_flight,
    // other_in_flight) and the claim's (free, free_other,
    // free_teardown_while_other_due).
    type Case = (
        (usize, usize, usize),
        (usize, usize, usize),
        (usize, usize),
        (usize, usize, usize),
    );
    let cases: &[Case] = &[
        // The defaults on an idle dispatcher: other work may use 14, and
        // teardowns leave it 2 while it is due.
        ((16, 2, 2), (16, 2, 2), (0, 0), (16, 14, 14)),
        // Other work holds every slot open to it: only the reservation is
        // free, and teardowns may take both slots of it.
        ((16, 2, 2), (16, 2, 2), (14, 14), (2, 0, 2)),
        // The floor keeps free slots, however many other work holds.
        ((16, 2, 2), (16, 2, 2), (10, 10), (6, 4, 4)),
        // Teardowns hold more than their reservation: other work may still
        // use what is free, and nothing more is owed to teardowns.
        ((4, 1, 1), (4, 1, 1), (3, 0), (1, 1, 0)),
        ((16, 2, 2), (16, 2, 2), (12, 4), (4, 4, 2)),
        // No reservation: other work may use every free slot, but the
        // floor still leaves a teardown one while none runs...
        ((4, 0, 3), (4, 0, 3), (0, 0), (4, 4, 1)),
        // ...even with other work running, which takes from what other
        // work may use but not from what teardowns are owed: the floor is
        // bounded by `free - owed`, not by `min(free, free_other)`.
        ((4, 0, 3), (4, 0, 3), (1, 1), (3, 3, 1)),
        // With a teardown running, the floor may keep every free slot.
        ((4, 0, 3), (4, 0, 3), (1, 0), (3, 3, 0)),
        // Both off: one shared pool.
        ((8, 0, 0), (8, 0, 0), (3, 1), (5, 5, 5)),
        // One slot: no reservation either way, and both classes share it.
        ((1, 1, 1), (1, 0, 0), (0, 0), (1, 1, 1)),
        ((1, 5, 5), (1, 0, 0), (1, 1), (0, 0, 0)),
        // Zero slots configured means one.
        ((0, 0, 0), (1, 0, 0), (0, 0), (1, 1, 1)),
        // Settings above the slot count leave each class a slot.
        ((3, 9, 9), (3, 2, 1), (0, 0), (3, 1, 2)),
        ((3, 0, 9), (3, 0, 2), (0, 0), (3, 3, 1)),
        // More running than slots, as after a lower setting: no underflow.
        ((4, 1, 1), (4, 1, 1), (6, 5), (0, 0, 0)),
    ];
    let policy_for = |execution_concurrency, teardown_reserved_slots, other_reserved_slots| {
        SlotPolicy::new(&ReconcilerConfig {
            execution_concurrency,
            teardown_reserved_slots,
            other_reserved_slots,
            ..ReconcilerConfig::default()
        })
    };
    for &(configured, policy, (in_flight, other_in_flight), claim) in cases {
        let slots = policy_for(configured.0, configured.1, configured.2);
        assert_eq!(
            (
                slots.concurrency,
                slots.teardown_reserved,
                slots.other_reserved
            ),
            policy,
            "{configured:?}"
        );
        let got = slots.claim_slots(in_flight, other_in_flight);
        assert_eq!(
            (got.free, got.free_other, got.free_teardown_while_other_due),
            claim,
            "{configured:?} with {in_flight} running, {other_in_flight} other"
        );
    }

    // And for every small setting and load: neither class gets more than is
    // free, other work never takes the teardown reservation, the floor keeps
    // no more than its setting and never the slots owed to teardowns, and
    // an idle dispatcher has a slot open to each class.
    for concurrency in 0..=6 {
        for teardown_reserved in 0..=7 {
            for other_reserved in 0..=7 {
                let slots = policy_for(concurrency, teardown_reserved, other_reserved);
                for in_flight in 0..=slots.concurrency + 1 {
                    for other_in_flight in 0..=in_flight {
                        let case =
                            format!("{slots:?} with {in_flight} running, {other_in_flight} other");
                        let got = slots.claim_slots(in_flight, other_in_flight);
                        assert!(got.free_other <= got.free, "{case}");
                        assert!(got.free_teardown_while_other_due <= got.free, "{case}");
                        assert!(
                            other_in_flight + got.free_other
                                <= slots.other_concurrency().max(other_in_flight),
                            "{case}"
                        );
                        assert!(
                            got.free - got.free_teardown_while_other_due <= slots.other_reserved,
                            "{case}"
                        );
                        let owed = slots
                            .teardown_reserved
                            .max(1)
                            .saturating_sub(in_flight - other_in_flight)
                            .min(got.free);
                        assert!(got.free_teardown_while_other_due >= owed, "{case}");
                    }
                }
                let idle = slots.claim_slots(0, 0);
                assert!(idle.free_other >= 1, "{slots:?}");
                assert!(idle.free_teardown_while_other_due >= 1, "{slots:?}");
            }
        }
    }
}

#[tokio::test]
async fn a_claim_takes_no_more_than_its_non_teardown_limit_of_other_work() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    queue_all(
        &store,
        org_id,
        &[(OperationKind::CreateSandbox, "sbx_capacity_running")],
    )
    .await?;
    claim_with(&store, ClaimLimits::any(1)).await?;
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
            // Still waits for the create running on its sandbox.
            (OperationKind::DeleteSandbox, "sbx_capacity_running"),
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
    // can: the one left waits for the create running on its sandbox.
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
            teardown_reserved_slots: 1,
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
            teardown_reserved_slots: 0,
            other_reserved_slots: 1,
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
            teardown_reserved_slots: 0,
            other_reserved_slots: 1,
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
                teardown_reserved_slots: 1,
                other_reserved_slots: 0,
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

#[tokio::test]
async fn a_teardown_still_starts_with_no_reservation_and_a_floor_at_every_slot() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let first = queue_all(
        &store,
        org_id,
        &[(OperationKind::CreateSandbox, "sbx_first_held_create")],
    )
    .await?[0];
    let mut operations = queue_all(
        &store,
        org_id,
        &[
            (OperationKind::DeleteSandbox, "sbx_unreserved_delete"),
            (OperationKind::CreateSandbox, "sbx_second_held_create"),
            (OperationKind::CreateSandbox, "sbx_third_held_create"),
        ],
    )
    .await?;
    let [delete, second, third] = operations[..] else {
        unreachable!("three operations were queued");
    };
    set_due(&db.pool, &operations, false).await?;
    let executor = HeldWork {
        hold_teardowns: true,
        ..HeldWork::default()
    };
    let shutdown = CancellationToken::new();
    let dispatcher = tokio::spawn(run(
        ReconcilerConfig {
            teardown_reserved_slots: 0,
            other_reserved_slots: 3,
            ..fast_loop_config(3)
        },
        store.clone(),
        executor.clone(),
        shutdown.clone(),
    ));
    wait_for_running(&executor, 1).await?;
    set_due(&db.pool, &operations, true).await?;

    // The floor leaves the delete one slot, and the second create the other.
    wait_for_running(&executor, 3).await?;
    for (operation, state) in [
        (delete, OperationState::Running),
        (second, OperationState::Running),
        (third, OperationState::Queued),
    ] {
        assert_eq!(
            operation_state(&db.pool, operation).await?,
            state.as_str(),
            "{operation}"
        );
    }

    executor.release.cancel();
    operations.push(first);
    for operation in operations {
        wait_for_state(&db.pool, operation, OperationState::Succeeded).await?;
    }
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;
    Ok(())
}

#[test]
fn org_slot_settings_parse_from_the_environment() {
    let settings = |env: &pplx_space_test_env::TestEnv| {
        let config = ReconcilerConfig::from_environment(env);
        (
            config.org_slot_limit.map(NonZeroU32::get),
            config.org_slot_headroom,
        )
    };
    let mut env = pplx_space_test_env::TestEnv::default();
    assert_eq!(settings(&env), (Some(8), 2));
    for (raw, limit, headroom) in [
        ("0", None, 0),
        ("3", Some(3), 3),
        ("500", Some(128), 128),
        ("eight", Some(8), 2),
        ("", Some(8), 2),
    ] {
        env.set(ORG_SLOT_LIMIT_ENV, raw);
        env.set(ORG_SLOT_HEADROOM_ENV, raw);
        assert_eq!(settings(&env), (limit, headroom), "{raw:?}");
    }
}

fn org_limit(contended: u32, headroom: u32) -> OrgSlotLimit {
    OrgSlotLimit {
        contended: NonZeroU32::new(contended).expect("a positive limit"),
        headroom,
    }
}

#[tokio::test]
async fn an_organizations_limit_holds_only_while_another_organization_waits() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let busy_org = OrgId::from_uuid(seed_org(&db.pool).await?);
    let other_org = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    // The busy organization already has one operation running.
    queue_all(
        &store,
        busy_org,
        &[(OperationKind::CreateSandbox, "sbx_busy_running")],
    )
    .await?;
    claim_with(&store, ClaimLimits::any(1)).await?;
    let busy = queue_all(
        &store,
        busy_org,
        &[
            (OperationKind::CreateSandbox, "sbx_busy_create_0"),
            (OperationKind::CreateSandbox, "sbx_busy_create_1"),
            (OperationKind::CreateSandbox, "sbx_busy_create_2"),
            (OperationKind::DeleteSandbox, "sbx_busy_delete"),
            (OperationKind::KillSandbox, "sbx_busy_kill"),
        ],
    )
    .await?;
    let other = queue_all(
        &store,
        other_org,
        &[
            (OperationKind::CreateSandbox, "sbx_other_create_0"),
            (OperationKind::CreateSandbox, "sbx_other_create_1"),
        ],
    )
    .await?;
    let limits = ClaimLimits {
        per_org_non_teardown: Some(org_limit(2, 0)),
        ..ClaimLimits::any(64)
    };

    // While the other organization waits, one more for the busy one, as it
    // already runs one. Teardowns are outside the limit.
    let mut expected = vec![busy[0], busy[3], busy[4], other[0], other[1]];
    expected.sort();
    assert_eq!(claim_with(&store, limits).await?, expected);
    // Nothing else waits now, so the busy organization may go past it.
    assert_eq!(claim_with(&store, limits).await?, [busy[1], busy[2]]);
    Ok(())
}

#[tokio::test]
async fn organizations_at_their_limit_share_the_slots_no_one_waits_for() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let first_org = OrgId::from_uuid(seed_org(&db.pool).await?);
    let second_org = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    // Each organization already runs its limit of two.
    for (org_id, prefix) in [(first_org, "sbx_first"), (second_org, "sbx_second")] {
        let running = [format!("{prefix}_running_0"), format!("{prefix}_running_1")];
        queue_all(
            &store,
            org_id,
            &running
                .iter()
                .map(|target_id| (OperationKind::CreateSandbox, target_id.as_str()))
                .collect::<Vec<_>>(),
        )
        .await?;
    }
    claim_with(&store, ClaimLimits::any(4)).await?;
    // Then both queue more, in turn.
    let mut queued = Vec::new();
    for index in 0..3 {
        for (org_id, prefix) in [(first_org, "sbx_first"), (second_org, "sbx_second")] {
            let target_id = format!("{prefix}_more_{index}");
            queued.extend(
                queue_all(
                    &store,
                    org_id,
                    &[(OperationKind::CreateSandbox, target_id.as_str())],
                )
                .await?,
            );
        }
    }
    let limits = ClaimLimits {
        per_org_non_teardown: Some(org_limit(2, 1)),
        ..ClaimLimits::any(6)
    };

    // Neither waits, both being at their limit, so both go past it, and
    // together they leave one of the six free slots untaken.
    let mut expected = queued[..5].to_vec();
    expected.sort();
    assert_eq!(claim_with(&store, limits).await?, expected);
    Ok(())
}

#[tokio::test]
async fn work_behind_another_organizations_backlog_is_still_claimed() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let busy_org = OrgId::from_uuid(seed_org(&db.pool).await?);
    let other_org = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    // The busy organization's backlog is longer than the claim reads of any
    // one organization, and all of it comes first.
    let targets = (0..6)
        .map(|index| format!("sbx_backlog_{index}"))
        .collect::<Vec<_>>();
    let backlog = queue_all(
        &store,
        busy_org,
        &targets
            .iter()
            .map(|target_id| (OperationKind::CreateSandbox, target_id.as_str()))
            .collect::<Vec<_>>(),
    )
    .await?;
    let other = queue_all(
        &store,
        other_org,
        &[(OperationKind::CreateSandbox, "sbx_behind_the_backlog")],
    )
    .await?[0];
    let limits = ClaimLimits {
        per_org_non_teardown: Some(org_limit(1, 0)),
        ..ClaimLimits::any(2)
    };

    // Both wait, so each gets one.
    let mut expected = vec![backlog[0], other];
    expected.sort();
    assert_eq!(claim_with(&store, limits).await?, expected);
    Ok(())
}

#[tokio::test]
async fn work_run_as_it_was_created_holds_no_slot_under_the_limit() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let busy_org = OrgId::from_uuid(seed_org(&db.pool).await?);
    let other_org = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    // Two creates the API claimed as it created them and runs itself.
    for target_id in ["sbx_inline_create_0", "sbx_inline_create_1"] {
        let created = store
            .create_claimed_operation(pplx_space_api::public_operations::CreatePublicOperation {
                org_id: busy_org,
                actor_user_id: None,
                api_product: ApiProduct::Native,
                kind: OperationKind::CreateSandbox,
                target_type: Some(OperationTargetType::Sandbox),
                target_id: Some(target_id),
                payload_json: serde_json::json!({}),
                idempotency: None,
                traceparent: None,
            })
            .await?;
        assert!(matches!(
            created,
            pplx_space_api::public_operations::CreateClaimedPublicOperationResult::Claimed { .. }
        ));
    }
    let pause = queue_all(
        &store,
        busy_org,
        &[(OperationKind::PauseSandbox, "sbx_busy_pause")],
    )
    .await?[0];
    let other_create = queue_all(
        &store,
        other_org,
        &[(OperationKind::CreateSandbox, "sbx_other_org_create")],
    )
    .await?[0];
    let limits = ClaimLimits {
        per_org_non_teardown: Some(org_limit(2, 0)),
        ..ClaimLimits::any(64)
    };

    // The inline creates hold no dispatcher slot, so the pause is within
    // its organization's limit though another organization waits.
    let mut expected = vec![pause, other_create];
    expected.sort();
    assert_eq!(claim_with(&store, limits).await?, expected);
    Ok(())
}

#[tokio::test]
async fn one_organizations_held_work_leaves_slots_for_another() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let busy_org = OrgId::from_uuid(seed_org(&db.pool).await?);
    let other_org = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let targets = (0..4)
        .map(|index| format!("sbx_busy_held_{index}"))
        .collect::<Vec<_>>();
    let mut creates = queue_all(
        &store,
        busy_org,
        &targets
            .iter()
            .map(|target_id| (OperationKind::CreateSandbox, target_id.as_str()))
            .collect::<Vec<_>>(),
    )
    .await?;
    let executor = HeldWork::default();
    let shutdown = CancellationToken::new();
    let dispatcher = tokio::spawn(run(
        ReconcilerConfig {
            teardown_reserved_slots: 0,
            other_reserved_slots: 0,
            org_slot_limit: NonZeroU32::new(2),
            org_slot_headroom: 1,
            ..fast_loop_config(4)
        },
        store.clone(),
        executor.clone(),
        shutdown.clone(),
    ));
    // Alone, the busy organization goes past its limit but leaves one slot.
    wait_for_running(&executor, 3).await?;

    let other_create = queue_all(
        &store,
        other_org,
        &[(OperationKind::CreateSandbox, "sbx_other_meanwhile")],
    )
    .await?[0];
    wait_for_running(&executor, 4).await?;
    assert_eq!(
        operation_state(&db.pool, other_create).await?,
        OperationState::Running.as_str()
    );

    executor.release.cancel();
    creates.push(other_create);
    for create in creates {
        wait_for_state(&db.pool, create, OperationState::Succeeded).await?;
    }
    assert_eq!(executor.max_running.load(Ordering::SeqCst), 4);
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;
    Ok(())
}

#[tokio::test]
async fn an_organization_alone_leaves_its_headroom_of_the_free_slots() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let holding_org = OrgId::from_uuid(seed_org(&db.pool).await?);
    let busy_org = OrgId::from_uuid(seed_org(&db.pool).await?);
    let late_org = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let mut creates = queue_all(
        &store,
        holding_org,
        &[(OperationKind::CreateSandbox, "sbx_holding_create")],
    )
    .await?;
    let executor = HeldWork::default();
    let shutdown = CancellationToken::new();
    let dispatcher = tokio::spawn(run(
        ReconcilerConfig {
            teardown_reserved_slots: 0,
            other_reserved_slots: 0,
            org_slot_limit: NonZeroU32::new(2),
            org_slot_headroom: 1,
            ..fast_loop_config(4)
        },
        store.clone(),
        executor.clone(),
        shutdown.clone(),
    ));
    // Another organization already holds a slot when the busy one queues.
    wait_for_running(&executor, 1).await?;
    let targets = (0..4)
        .map(|index| format!("sbx_bursting_create_{index}"))
        .collect::<Vec<_>>();
    creates.extend(
        queue_all(
            &store,
            busy_org,
            &targets
                .iter()
                .map(|target_id| (OperationKind::CreateSandbox, target_id.as_str()))
                .collect::<Vec<_>>(),
        )
        .await?,
    );
    // Nothing else waits, so it may pass its cap, but only up to leaving
    // one of the three free slots free.
    wait_for_running(&executor, 3).await?;

    let late_create = queue_all(
        &store,
        late_org,
        &[(OperationKind::CreateSandbox, "sbx_late_create")],
    )
    .await?[0];
    wait_for_running(&executor, 4).await?;
    assert_eq!(
        operation_state(&db.pool, late_create).await?,
        OperationState::Running.as_str()
    );

    executor.release.cancel();
    creates.push(late_create);
    for create in creates {
        wait_for_state(&db.pool, create, OperationState::Succeeded).await?;
    }
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;
    Ok(())
}

/// The operations a claim takes, sorted, and whether it says the
/// organization limit held back all other work.
async fn claim_reporting(
    store: &PublicOperationStore,
    limits: ClaimLimits,
) -> Result<(Vec<OperationId>, bool), pplx_space_api::public_operations::PublicOperationStoreError>
{
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
    Ok((claimed, claim.org_limit_held_back))
}

#[tokio::test]
async fn a_claim_reports_work_held_back_only_by_the_organization_limit() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let limits = |non_teardown, contended, headroom| ClaimLimits {
        non_teardown,
        per_org_non_teardown: Some(org_limit(contended, headroom)),
        ..ClaimLimits::any(non_teardown)
    };
    // A create runs under a live claim this caller does not execute, as on
    // another replica, and a pause of its sandbox is due behind it.
    queue_all(
        &store,
        org_id,
        &[(OperationKind::CreateSandbox, "sbx_running_elsewhere")],
    )
    .await?;
    claim_with(&store, ClaimLimits::any(1)).await?;
    queue_all(
        &store,
        org_id,
        &[(OperationKind::PauseSandbox, "sbx_running_elsewhere")],
    )
    .await?;
    // Its sandbox holds the pause back, not the organization limit.
    assert_eq!(
        claim_reporting(&store, limits(4, 2, 0)).await?,
        (vec![], false)
    );

    // Another transaction holds the earliest of two creates locked: the
    // claim takes the next one instead, both being within the limit.
    let creates = queue_all(
        &store,
        org_id,
        &[
            (OperationKind::CreateSandbox, "sbx_locked_create"),
            (OperationKind::CreateSandbox, "sbx_next_create"),
        ],
    )
    .await?;
    let mut lock = db.pool.begin().await?;
    sqlx::query(sql!(
        "SELECT 1 FROM public_operations WHERE operation_id = $1 FOR UPDATE"
    ))
    .bind(creates[0].as_uuid())
    .execute(&mut *lock)
    .await?;
    assert_eq!(
        claim_reporting(&store, limits(1, 4, 0)).await?,
        (vec![creates[1]], false)
    );
    // With only the locked one left, nothing is held back by the limit.
    assert_eq!(
        claim_reporting(&store, limits(1, 4, 0)).await?,
        (vec![], false)
    );
    lock.rollback().await?;

    // The organization runs its limit of two, no other waits, and its
    // headroom is every free slot: the limit holds the create back.
    assert_eq!(
        claim_reporting(&store, limits(1, 2, 1)).await?,
        (vec![], true)
    );
    assert_eq!(
        claim_reporting(&store, limits(1, 2, 0)).await?,
        (vec![creates[0]], false)
    );
    Ok(())
}

#[tokio::test]
async fn a_sandbox_blocked_operation_does_not_slow_claims() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let busy_org = OrgId::from_uuid(seed_org(&db.pool).await?);
    let other_org = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    // A create runs under a live claim this dispatcher does not hold, and a
    // delete of its sandbox is due behind it.
    queue_all(
        &store,
        busy_org,
        &[(OperationKind::CreateSandbox, "sbx_busy_elsewhere")],
    )
    .await?;
    claim_with(&store, ClaimLimits::any(1)).await?;
    queue_all(
        &store,
        busy_org,
        &[(OperationKind::DeleteSandbox, "sbx_busy_elsewhere")],
    )
    .await?;
    let executor = HeldWork::default();
    executor.release.cancel();
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
            fast_loop_config(4),
            store.clone(),
            executor.clone(),
            shutdown.clone(),
        ),
    ));

    // The loop keeps claiming every tick, not about four times a second, so
    // unrelated work is picked up at once.
    let before = claims.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let during = claims.load(Ordering::SeqCst) - before;
    assert!(during >= 10, "{during} claims in half a second");
    let unrelated = queue_all(
        &store,
        other_org,
        &[(OperationKind::CreateSandbox, "sbx_unrelated_create")],
    )
    .await?[0];
    wait_for_state(&db.pool, unrelated, OperationState::Succeeded).await?;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;
    Ok(())
}

#[tokio::test]
async fn a_claim_that_finds_its_work_held_back_waits_before_the_next() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let targets = (0..3)
        .map(|index| format!("sbx_capped_{index}"))
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
    // One slot for the organization, the others free but closed to it.
    let dispatcher = tokio::spawn(super::BEFORE_CLAIM.scope(
        hook,
        run(
            ReconcilerConfig {
                teardown_reserved_slots: 0,
                other_reserved_slots: 0,
                org_slot_limit: NonZeroU32::new(1),
                org_slot_headroom: 3,
                ..fast_loop_config(4)
            },
            store.clone(),
            executor.clone(),
            shutdown.clone(),
        ),
    ));
    wait_for_running(&executor, 1).await?;
    let before = claims.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(1)).await;
    // About four claims a second, not one every 20 ms tick.
    let during = claims.load(Ordering::SeqCst) - before;
    assert!((1..=8).contains(&during), "{during} claims in a second");

    // A finished execution frees a slot, and the next claim follows at once.
    executor.release.cancel();
    for create in creates {
        wait_for_state(&db.pool, create, OperationState::Succeeded).await?;
    }
    // Work waiting out a backoff is not held back: claims keep every tick.
    let later = queue_all(
        &store,
        org_id,
        &[(OperationKind::CreateSandbox, "sbx_capped_later")],
    )
    .await?[0];
    sqlx::query(sql!(
        "UPDATE public_operations
         SET next_attempt_at = NOW() + INTERVAL '1 hour'
         WHERE operation_id = $1"
    ))
    .bind(later.as_uuid())
    .execute(&db.pool)
    .await?;
    let before = claims.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let idle = claims.load(Ordering::SeqCst) - before;
    assert!(idle >= 10, "{idle} claims in half a second");
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;
    Ok(())
}
