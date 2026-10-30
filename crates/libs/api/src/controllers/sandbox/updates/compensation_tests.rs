//! Create compensation against a direct worker run by the in-process
//! simulator: who may tear the guest down, and when.

use pplx_space_common_core::types::{NodeId, NodeManagedBy, SandboxId};
use pplx_space_db_test_fixtures::pg_test_db_with_max_connections;
use pplx_space_node_api_sim::{SandboxRecord, SimWorkerHarness};
use pplx_space_test_env::TestResult;

use super::*;
use crate::test_support::direct_substrate;

async fn start_sim_worker(pool: &PgPool) -> TestResult<(SimWorkerHarness, NodeId)> {
    let node_id = NodeId::new();
    let harness = SimWorkerHarness::builder(node_id.to_string())
        .with_internal_api()
        .start()
        .await?;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'space', 'ready')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, node_api_endpoint) VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(harness.endpoint())
    .execute(pool)
    .await?;
    Ok((harness, node_id))
}

async fn insert_resident(
    worker: &SimWorkerHarness,
    sandbox_id: SandboxId,
    status: &str,
) -> TestResult {
    let mut record = SandboxRecord::new_creating(
        sandbox_id.to_string(),
        "seed".to_owned(),
        "fc".to_owned(),
        "root".to_owned(),
        Some("hash-seed".to_owned()),
        false,
    )
    .with_route_identity_provenance(1, 1, Some(NodeManagedBy::Space));
    record.status = status.to_owned();
    worker.state().insert_new(record).await?;
    Ok(())
}

async fn resident_exists(worker: &SimWorkerHarness, sandbox_id: SandboxId) -> bool {
    worker.state().get(&sandbox_id.to_string()).await.is_ok()
}

/// A direct sandbox row with no route receipt, on `active_node_id`.
async fn insert_direct_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
    lifecycle: &str,
    active_node_id: NodeId,
) -> TestResult {
    let org_id = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, 'Compensation Test', $2)"
    ))
    .bind(org_id)
    .bind(format!("compensation-{org_id}"))
    .execute(pool)
    .await?;
    let user_id: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'unused', 'Compensation Test', $3, 'member')
         RETURNING id"
    ))
    .bind(Uuid::now_v7())
    .bind(format!("compensation-{org_id}@example.com"))
    .bind(org_id)
    .fetch_one(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement, active_node_id,
             network_policy_generation
         )
         VALUES ($1, $2, $3, 'space', $4, 'on_node', $5, 1)"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(user_id)
    .bind(org_id)
    .bind(lifecycle)
    .bind(active_node_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn lifecycle(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Option<String>> {
    Ok(
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id.as_uuid())
            .fetch_optional(pool)
            .await?,
    )
}

fn outcome(sandbox_id: SandboxId, node_id: NodeId) -> CreateExecutionOutcome {
    CreateExecutionOutcome {
        sandbox_id,
        binding: None,
        rollback_fence: CreateRollbackExecutionFence::Direct {
            node_id,
            route_identity: None,
        },
        initial_lifecycle: "started",
        initial_placement: "on_node",
        active_node_id: Some(node_id),
        command_ready: true,
        lease_completion: None,
    }
}

/// A queued compensation must not tear down a sandbox an in-flight action
/// took over before the teardown started. A stop that lands its result first
/// leaves the row `stopped` with no route receipt and `active_node_id`
/// unchanged, which the direct fence's absent-receipt arm accepts, so only an
/// ownership check ahead of the node call keeps the stopped state the user
/// owns. The control is the row still `deleting`, which is torn down.
#[tokio::test(flavor = "current_thread")]
async fn compensation_leaves_a_sandbox_that_left_deleting_untouched() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let controller = SandboxController::new(direct_substrate(pool));
    let (worker, node_id) = start_sim_worker(pool).await?;

    for (row, expected) in [
        ("stopped", CreateCompensation::Declined),
        ("deleting", CreateCompensation::TornDown),
    ] {
        let sandbox_id = SandboxId::new();
        insert_direct_sandbox(pool, sandbox_id, row, node_id).await?;
        insert_resident(&worker, sandbox_id, "stopped").await?;

        let result = controller
            .delete_created_sandbox_for_rollback(
                pool,
                sandbox_id,
                &outcome(sandbox_id, node_id).rollback_fence,
                None,
            )
            .await?;

        assert_eq!(result, expected, "row {row}");
        match expected {
            CreateCompensation::Declined => {
                assert!(
                    resident_exists(&worker, sandbox_id).await,
                    "the stopped sandbox's node state must survive"
                );
                assert_eq!(
                    lifecycle(pool, sandbox_id).await?.as_deref(),
                    Some("stopped")
                );
            }
            CreateCompensation::TornDown => {
                assert!(!resident_exists(&worker, sandbox_id).await);
                assert_eq!(lifecycle(pool, sandbox_id).await?, None);
            }
        }
    }
    Ok(())
}

/// Create compensation tears the guest down before the create returns, with
/// no dispatcher running. The operation is still recorded, and recorded as
/// done, so a dispatcher that does run has nothing left to do.
#[tokio::test(flavor = "current_thread")]
async fn create_rollback_tears_down_without_a_dispatcher() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let controller = SandboxController::new(direct_substrate(pool));
    let (worker, node_id) = start_sim_worker(pool).await?;
    let sandbox_id = SandboxId::new();
    insert_direct_sandbox(pool, sandbox_id, "creating", node_id).await?;
    insert_resident(&worker, sandbox_id, "running").await?;

    controller
        .rollback_created_sandbox(pool, &outcome(sandbox_id, node_id))
        .await;

    assert!(!resident_exists(&worker, sandbox_id).await);
    assert_eq!(lifecycle(pool, sandbox_id).await?, None);
    let operations: Vec<(String, String)> = sqlx::query_as(sql!(
        "SELECT kind, state FROM public_operations WHERE target_type = 'sandbox'"
    ))
    .fetch_all(pool)
    .await?;
    assert_eq!(
        operations,
        vec![(
            "compensate_create_sandbox".to_string(),
            "succeeded".to_string()
        )]
    );
    Ok(())
}

/// The fallback that runs when the operation can't be queued makes the same
/// claim the queued path does, so it still tears the guest down.
#[tokio::test(flavor = "current_thread")]
async fn create_rollback_falls_back_to_a_claimed_teardown_when_queueing_fails() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let controller = SandboxController::new(direct_substrate(pool));
    let (worker, node_id) = start_sim_worker(pool).await?;
    sqlx::query(sql!(
        "CREATE FUNCTION test_refuse_public_operation() RETURNS trigger
         LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected queue failure'; END $$"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TRIGGER test_refuse_public_operation BEFORE INSERT ON public_operations
         FOR EACH ROW EXECUTE FUNCTION test_refuse_public_operation()"
    ))
    .execute(pool)
    .await?;
    let sandbox_id = SandboxId::new();
    insert_direct_sandbox(pool, sandbox_id, "creating", node_id).await?;
    insert_resident(&worker, sandbox_id, "running").await?;

    controller
        .rollback_created_sandbox(pool, &outcome(sandbox_id, node_id))
        .await;

    assert!(!resident_exists(&worker, sandbox_id).await);
    assert_eq!(lifecycle(pool, sandbox_id).await?, None);
    Ok(())
}

/// The claim declines a row whose execution is no longer the one the create
/// recorded, and leaves it as it found it, instead of moving a successor to
/// `deleting` with no operation to finish the job. The control is the same
/// row still on the create's node.
#[tokio::test(flavor = "current_thread")]
async fn compensation_claim_declines_a_successor_execution() -> TestResult {
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let node_id = NodeId::new();
    for (active_node_id, claims) in [(NodeId::new(), false), (node_id, true)] {
        let sandbox_id = SandboxId::new();
        insert_direct_sandbox(pool, sandbox_id, "started", active_node_id).await?;

        let mut tx = pool.begin().await?;
        let claimed =
            claim_created_sandbox_for_compensation(&mut tx, &outcome(sandbox_id, node_id)).await?;
        tx.commit().await?;

        assert_eq!(claimed.is_some(), claims);
        assert_eq!(
            lifecycle(pool, sandbox_id).await?.as_deref(),
            Some(if claims { "deleting" } else { "started" })
        );
    }
    Ok(())
}

/// A direct sandbox's route receipt for `node_id`, and the fence naming it.
async fn insert_receipt(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
) -> TestResult<CreateRollbackExecutionFence> {
    use pplx_space_common_core::types::{
        ExecutionGeneration, RouteGeneration, SandboxActivationAttemptId, SandboxRouteIdentity,
    };
    let attempt = SandboxActivationAttemptId::from_uuid(Uuid::now_v7());
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_identities (
             sandbox_id, route_generation, current_node_id, substrate_kind,
             execution_generation, activation_attempt_id
         )
         VALUES ($1, 3, $2, 'space', 3, $3)"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(node_id)
    .bind(attempt.as_uuid())
    .execute(pool)
    .await?;
    Ok(CreateRollbackExecutionFence::Direct {
        node_id,
        route_identity: Some(SandboxRouteIdentity::try_new(
            sandbox_id,
            RouteGeneration::new(3)?,
            node_id,
            NodeManagedBy::Space,
            ExecutionGeneration::try_from(3i64)?,
            attempt,
            None,
        )?),
    })
}

/// Wait until some backend is blocked on a lock `holder_pid` holds.
async fn wait_until_blocked_by(pool: &PgPool, holder_pid: i32) -> TestResult {
    for _ in 0..500 {
        let blocked: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity WHERE $1 = ANY (pg_blocking_pids(pid))
             )"
        ))
        .bind(holder_pid)
        .fetch_one(pool)
        .await?;
        if blocked {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    Err("no backend blocked on the held receipt lock".into())
}

/// A teardown whose route receipt disappears while it runs: something else
/// cleared it, such as the attempt of this compensation that holds the claim
/// this one lost, or a retire. Run here without a claim, as the claimed
/// immediate teardown runs, so the node alone decides. The property: when the
/// node no longer has the sandbox, the teardown finishes, with the guest and
/// the row gone, rather than ending terminal with the row in `deleting`. The
/// control keeps successor protection: when the node still has the sandbox
/// once the receipt is gone, something landed after this attempt's delete, and
/// it stops with a conflict without deleting the row. Under a claim the same
/// path records the teardown first
/// (`an_unconfirmed_teardown_stays_retryable_and_is_only_verified`).
///
/// The interleaving is forced, not hoped for. The test holds the receipt row,
/// so this attempt passes its receipt check and its node delete, then waits in
/// its receipt clear. The other writer's clear is the receipt delete the test
/// commits while this one waits.
#[tokio::test(flavor = "current_thread")]
async fn overlapping_compensation_attempts_finish_the_teardown() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(6).await?;
    let pool = &db.pool;
    let controller = SandboxController::new(direct_substrate(pool));
    let (worker, node_id) = start_sim_worker(pool).await?;

    for successor_landed in [false, true] {
        let sandbox_id = SandboxId::new();
        insert_direct_sandbox(pool, sandbox_id, "deleting", node_id).await?;
        let fence = insert_receipt(pool, sandbox_id, node_id).await?;
        insert_resident(&worker, sandbox_id, "running").await?;

        let mut other_attempt = pool.begin().await?;
        let holder_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
            .fetch_one(&mut *other_attempt)
            .await?;
        sqlx::query(sql!(
            "SELECT 1 FROM sandbox_direct_route_identities WHERE sandbox_id = $1 FOR UPDATE"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_one(&mut *other_attempt)
        .await?;

        let attempt = tokio::spawn({
            let controller = controller.clone();
            let pool = pool.clone();
            let fence = fence.clone();
            async move {
                controller
                    .delete_created_sandbox_for_rollback(&pool, sandbox_id, &fence, None)
                    .await
            }
        });
        wait_until_blocked_by(pool, holder_pid).await?;
        assert!(
            !resident_exists(&worker, sandbox_id).await,
            "the attempt must be past its node delete when it waits, or this isn't the race"
        );

        sqlx::query(sql!(
            "DELETE FROM sandbox_direct_route_identities WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id.as_uuid())
        .execute(&mut *other_attempt)
        .await?;
        if successor_landed {
            insert_resident(&worker, sandbox_id, "running").await?;
        }
        other_attempt.commit().await?;
        let result = attempt.await?;

        if successor_landed {
            let error = result.expect_err("a sandbox the node still has must stop the teardown");
            assert_eq!(error.status(), axum::http::StatusCode::CONFLICT, "{error}");
            assert!(resident_exists(&worker, sandbox_id).await);
            assert_eq!(
                lifecycle(pool, sandbox_id).await?.as_deref(),
                Some("deleting")
            );
        } else {
            assert_eq!(result?, CreateCompensation::TornDown);
            assert!(!resident_exists(&worker, sandbox_id).await);
            assert_eq!(lifecycle(pool, sandbox_id).await?, None);
        }
    }
    Ok(())
}

/// An attempt cancelled after its receipt clear but before its row cleanup
/// leaves the guest gone, the receipt gone and the row `deleting`. The next
/// attempt must finish the job: confirm the absence on the node and delete
/// the row. It finds no receipt before its node call, so it takes the
/// absent-receipt arm, which already handled this; the test guards that path,
/// and only the overlapping case above exercises the post-delete probe.
#[tokio::test(flavor = "current_thread")]
async fn a_compensation_cancelled_before_its_row_cleanup_is_finished_by_the_next_attempt()
-> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let controller = SandboxController::new(direct_substrate(pool));
    let (worker, node_id) = start_sim_worker(pool).await?;
    let sandbox_id = SandboxId::new();
    insert_direct_sandbox(pool, sandbox_id, "deleting", node_id).await?;
    let fence = insert_receipt(pool, sandbox_id, node_id).await?;
    insert_resident(&worker, sandbox_id, "running").await?;

    // The cancelled attempt: its substrate teardown, and not the row cleanup
    // that follows it.
    controller
        .substrate
        .delete_created_execution(sandbox_id, &fence, None)
        .await?;
    assert!(!resident_exists(&worker, sandbox_id).await);
    assert_eq!(
        lifecycle(pool, sandbox_id).await?.as_deref(),
        Some("deleting")
    );

    assert_eq!(
        controller
            .delete_created_sandbox_for_rollback(pool, sandbox_id, &fence, None)
            .await?,
        CreateCompensation::TornDown
    );
    assert_eq!(lifecycle(pool, sandbox_id).await?, None);
    Ok(())
}

/// With `SPACE_CREATE_COMPENSATION_QUEUE` off, a failed create writes no
/// `compensate_create_sandbox` operation, which is what lets a rollback stop
/// new ones while the controller drains the queued ones. It still claims the
/// sandbox before its immediate teardown. The node here isn't registered, so
/// the teardown fails and the claimed row stays `deleting`. The default writes
/// the operation (`create_rollback_keeps_db_ownership_when_substrate_delete_fails`).
#[tokio::test(flavor = "current_thread")]
async fn create_rollback_queues_nothing_with_the_queue_switch_off() -> TestResult {
    if !pplx_space_test_env::enter_test_process(&[(
        CREATE_COMPENSATION_QUEUE_ENV,
        Some(std::ffi::OsStr::new("false")),
    )]) {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let controller = SandboxController::new(direct_substrate(pool));
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_direct_sandbox(pool, sandbox_id, "creating", node_id).await?;

    controller
        .rollback_created_sandbox(pool, &outcome(sandbox_id, node_id))
        .await;

    let operations: i64 = sqlx::query_scalar(sql!("SELECT COUNT(*) FROM public_operations"))
        .fetch_one(pool)
        .await?;
    assert_eq!(operations, 0);
    assert_eq!(
        lifecycle(pool, sandbox_id).await?.as_deref(),
        Some("deleting")
    );
    Ok(())
}

/// Queue a compensation operation for `sandbox_id`, already claimed, as the
/// create rollback does.
async fn queue_claimed_compensation(
    pool: &PgPool,
    sandbox_id: SandboxId,
    fence: &CreateRollbackExecutionFence,
) -> TestResult<(String, ClaimedPublicOperation)> {
    use crate::public_operations::CreateClaimedPublicOperationResult;
    let org_id: Uuid = sqlx::query_scalar(sql!("SELECT org_id FROM sandboxes WHERE id = $1"))
        .bind(sandbox_id.as_uuid())
        .fetch_one(pool)
        .await?;
    let public_sandbox_id = PublicSandboxId::from_uuid(sandbox_id.as_uuid());
    let target_id = public_sandbox_id.to_string();
    let created = PublicOperationStore::new(pool.clone())
        .create_claimed_operation(CreatePublicOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: None,
            api_product: ApiProduct::Native,
            kind: OperationKind::CompensateCreateSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(&target_id),
            payload_json: SandboxLifecycleOperationPayload {
                sandbox_id: public_sandbox_id,
                action: PublicSandboxLifecycleAction::Delete,
                rollback: None,
                create_rollback_fence: Some(fence.clone()),
                create_rollback_teardown_issued: false,
                wake_activity: None,
            }
            .into_json(),
            idempotency: None,
            traceparent: None,
        })
        .await?;
    match created {
        CreateClaimedPublicOperationResult::Claimed {
            claim_token,
            operation,
        } => Ok((claim_token, operation)),
        other => Err(format!("expected a claimed operation, got {other:?}").into()),
    }
}

/// One attempt of `operation` through the driver and executor the dispatcher
/// uses.
async fn drive_compensation(
    pool: PgPool,
    controller: SandboxController,
    claim_token: String,
    operation: ClaimedPublicOperation,
) -> TestResult {
    let executor = SandboxLifecycleOperationExecutor::new(pool.clone(), controller);
    let execution = {
        let operation = operation.clone();
        let claim_token = claim_token.clone();
        async move {
            executor
                .execute_claimed(&claim_token, &operation)
                .await
                .into()
        }
    };
    drive_claimed_operation(
        PublicOperationStore::new(pool),
        claim_token,
        OperationExecutionConfig::default(),
        operation,
        OperationExecutor::Inline,
        execution,
    )
    .await?;
    Ok(())
}

/// Reclaim a compensation the way the dispatcher does once it is due: a
/// requeued one, or one whose claim went stale.
async fn reclaim_compensation(pool: &PgPool) -> TestResult<(String, ClaimedPublicOperation)> {
    use crate::public_operations::ClaimPublicOperations;
    sqlx::query(sql!(
        "UPDATE public_operations
            SET next_attempt_at = NOW(), claimed_at = NOW() - INTERVAL '1 hour'
          WHERE kind = 'compensate_create_sandbox'"
    ))
    .execute(pool)
    .await?;
    let mut claim = PublicOperationStore::new(pool.clone())
        .claim_operations(ClaimPublicOperations {
            limits: crate::public_operations::ClaimLimits::any(1),
            claim_stale_after: std::time::Duration::from_secs(60),
            claimable_kinds: &[OperationKind::CompensateCreateSandbox],
        })
        .await?;
    let operation = claim
        .operations
        .pop()
        .ok_or("the compensation was not reclaimable")?;
    Ok((claim.claim_token, operation))
}

/// The compensation operation's state and teardown marker.
async fn compensation_state(pool: &PgPool) -> TestResult<(String, bool)> {
    Ok(sqlx::query_as(sql!(
        "SELECT state,
                COALESCE((payload_json->>'create_rollback_teardown_issued')::BOOLEAN, FALSE)
           FROM public_operations
          WHERE kind = 'compensate_create_sandbox'"
    ))
    .fetch_one(pool)
    .await?)
}

/// The property: a compensation whose node delete ran but whose teardown the
/// node then couldn't confirm is retried, not failed, and every retry only
/// verifies it. It never repeats the delete, which names the sandbox and not
/// the execution. Once the node answers "missing", it succeeds and the row is
/// gone; a sandbox the node still has is a conflict, and survives it.
///
/// Forced through the operation driver: the test holds the route receipt row,
/// so the attempt passes its receipt check and node delete and waits in its
/// receipt clear; the test then deletes the receipt (whatever cleared it, it
/// wasn't this compensation) and stops the worker, so every probe fails.
#[tokio::test(flavor = "current_thread")]
async fn an_unconfirmed_teardown_stays_retryable_and_is_only_verified() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    for successor_landed in [false, true] {
        let db = pg_test_db_with_max_connections(6).await?;
        let pool = &db.pool;
        let controller = SandboxController::new(direct_substrate(pool));
        let (worker, node_id) = start_sim_worker(pool).await?;
        let sandbox_id = SandboxId::new();
        insert_direct_sandbox(pool, sandbox_id, "deleting", node_id).await?;
        let fence = insert_receipt(pool, sandbox_id, node_id).await?;
        insert_resident(&worker, sandbox_id, "running").await?;
        let (claim_token, operation) = queue_claimed_compensation(pool, sandbox_id, &fence).await?;

        let mut other_writer = pool.begin().await?;
        let holder_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
            .fetch_one(&mut *other_writer)
            .await?;
        sqlx::query(sql!(
            "SELECT 1 FROM sandbox_direct_route_identities WHERE sandbox_id = $1 FOR UPDATE"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_one(&mut *other_writer)
        .await?;
        let attempt = tokio::spawn(drive_compensation(
            pool.clone(),
            controller.clone(),
            claim_token,
            operation,
        ));
        wait_until_blocked_by(pool, holder_pid).await?;
        assert!(
            !resident_exists(&worker, sandbox_id).await,
            "the delete ran"
        );
        sqlx::query(sql!(
            "DELETE FROM sandbox_direct_route_identities WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id.as_uuid())
        .execute(&mut *other_writer)
        .await?;
        drop(worker);
        other_writer.commit().await?;
        attempt.await??;

        assert_eq!(
            compensation_state(pool).await?,
            ("queued".to_string(), true),
            "an unconfirmed teardown is requeued with its teardown recorded"
        );
        assert_eq!(
            lifecycle(pool, sandbox_id).await?.as_deref(),
            Some("deleting")
        );

        // The node comes back. A successor that landed on it must survive the
        // retry, which only verifies.
        let worker = SimWorkerHarness::builder(node_id.to_string())
            .with_internal_api()
            .start()
            .await?;
        sqlx::query(sql!(
            "UPDATE node_direct SET node_api_endpoint = $2 WHERE node_id = $1"
        ))
        .bind(node_id)
        .bind(worker.endpoint())
        .execute(pool)
        .await?;
        if successor_landed {
            insert_resident(&worker, sandbox_id, "running").await?;
        }
        let (claim_token, operation) = reclaim_compensation(pool).await?;
        drive_compensation(pool.clone(), controller.clone(), claim_token, operation).await?;

        if successor_landed {
            assert_eq!(compensation_state(pool).await?.0, "failed");
            assert!(
                resident_exists(&worker, sandbox_id).await,
                "the retry must not delete what the node has"
            );
            assert_eq!(
                lifecycle(pool, sandbox_id).await?.as_deref(),
                Some("deleting")
            );
        } else {
            assert_eq!(compensation_state(pool).await?.0, "succeeded");
            assert_eq!(lifecycle(pool, sandbox_id).await?, None);
        }
    }
    Ok(())
}

/// The property: an attempt cancelled after its teardown, before its row
/// cleanup, leaves the operation reclaimable, and the attempt that reclaims it
/// only verifies the teardown. It finds no receipt, but it must not take that
/// as license to delete by sandbox id: a successor on the node survives it.
///
/// The cancellation is forced: the test holds the sandbox row against the row
/// cleanup's lock (not against the receipt clear's foreign-key check), waits
/// until the attempt is blocked there, and aborts it.
#[tokio::test(flavor = "current_thread")]
async fn a_compensation_cancelled_after_its_teardown_is_only_verified_on_reclaim() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    for successor_landed in [false, true] {
        let db = pg_test_db_with_max_connections(6).await?;
        let pool = &db.pool;
        let controller = SandboxController::new(direct_substrate(pool));
        let (worker, node_id) = start_sim_worker(pool).await?;
        let sandbox_id = SandboxId::new();
        insert_direct_sandbox(pool, sandbox_id, "deleting", node_id).await?;
        let fence = insert_receipt(pool, sandbox_id, node_id).await?;
        insert_resident(&worker, sandbox_id, "running").await?;
        let (claim_token, operation) = queue_claimed_compensation(pool, sandbox_id, &fence).await?;

        let mut row_holder = pool.begin().await?;
        let holder_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
            .fetch_one(&mut *row_holder)
            .await?;
        sqlx::query(sql!(
            "SELECT 1 FROM sandboxes WHERE id = $1 FOR NO KEY UPDATE"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_one(&mut *row_holder)
        .await?;
        let attempt = tokio::spawn(drive_compensation(
            pool.clone(),
            controller.clone(),
            claim_token,
            operation,
        ));
        wait_until_blocked_by(pool, holder_pid).await?;
        attempt.abort();
        let _ = attempt.await;
        row_holder.rollback().await?;

        assert_eq!(
            compensation_state(pool).await?,
            ("running".to_string(), true),
            "the cancelled attempt recorded its teardown before its row cleanup"
        );
        assert!(!resident_exists(&worker, sandbox_id).await);
        assert_eq!(
            lifecycle(pool, sandbox_id).await?.as_deref(),
            Some("deleting")
        );

        if successor_landed {
            insert_resident(&worker, sandbox_id, "running").await?;
        }
        let (claim_token, operation) = reclaim_compensation(pool).await?;
        drive_compensation(pool.clone(), controller.clone(), claim_token, operation).await?;

        if successor_landed {
            assert_eq!(compensation_state(pool).await?.0, "failed");
            assert!(
                resident_exists(&worker, sandbox_id).await,
                "the reclaiming attempt must not delete what the node has"
            );
        } else {
            assert_eq!(compensation_state(pool).await?.0, "succeeded");
            assert_eq!(lifecycle(pool, sandbox_id).await?, None);
        }
    }
    Ok(())
}

/// A legacy create carries no route receipt, so nothing clears one and its
/// teardown is recorded on its own. The property: once its node delete has
/// run, the operation carries the record. What a later attempt does with the
/// record is `a_compensation_cancelled_after_its_teardown_is_only_verified_on_reclaim`.
#[tokio::test(flavor = "current_thread")]
async fn a_legacy_teardown_records_itself() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let controller = SandboxController::new(direct_substrate(pool));
    let (worker, node_id) = start_sim_worker(pool).await?;
    let sandbox_id = SandboxId::new();
    insert_direct_sandbox(pool, sandbox_id, "deleting", node_id).await?;
    insert_resident(&worker, sandbox_id, "running").await?;
    let fence = outcome(sandbox_id, node_id).rollback_fence;
    let (claim_token, operation) = queue_claimed_compensation(pool, sandbox_id, &fence).await?;

    drive_compensation(pool.clone(), controller, claim_token, operation).await?;

    assert!(!resident_exists(&worker, sandbox_id).await);
    assert_eq!(
        compensation_state(pool).await?,
        ("succeeded".to_string(), true)
    );
    Ok(())
}
