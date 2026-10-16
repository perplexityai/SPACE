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
