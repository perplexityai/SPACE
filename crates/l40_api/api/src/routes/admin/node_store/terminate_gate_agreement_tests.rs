//! The admin `:terminate` guard and the node-drain gate must count the same
//! rows.
//!
//! They did not. The guard asked `lifecycle = 'suspended'`; the reconciler's
//! gate also counts a `suspending` row, and a `restoring` row that is not an
//! evacuation restore, when either is pinned to the node by `target_node_id`
//! (`drain_gate_target_pinned_blocks`). When such a row's `active_node_id` is
//! NULL, the guard's `active` half, which keys on `active_node_id`, cannot see
//! it either; an in-flight user restore of a suspended sandbox is the common
//! case. So a `drained` node that had picked up such a row since it drained,
//! one the gate would hold, could be terminated through the admin endpoint —
//! and `space-rollout-mng.py` drives that endpoint unattended.
//!
//! Each test below asserts the agreement *and* shows the fixture is one where
//! the pre-change predicate disagreed. Without the second half these would
//! pass just as well against the statement they replaced.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::NodeId;
use pplx_space_common_svc::sql::sandbox_residency::ResidencyEvidence;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_sandbox_portability::{
    drain_gate_blockers_for_node, inflight_suspend_upload_from_node_exists,
    snapshot_durable_for_source_release,
};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

/// The `local_snapshots` predicate exactly as the guard computed it before it
/// adopted `drain_gate_blockers_for_node`, kept here so "they used to disagree" is measured against the real
/// prior statement rather than against a remembered description of it.
fn pre_change_guard_query() -> String {
    let durable = snapshot_durable_for_source_release("sandboxes");
    let inflight = inflight_suspend_upload_from_node_exists("sandboxes", "$1");
    format!(
        "SELECT COUNT(*) FILTER (
             WHERE (
                     lifecycle = 'suspended'
                     AND target_node_id = $1
                     AND NOT {durable}
                   )
                OR ({inflight} AND NOT {durable})
         )::BIGINT
           FROM sandboxes"
    )
}

async fn insert_node(pool: &PgPool) -> TestResult<Uuid> {
    let node_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.71'::inet, 'worker', 'k8s', 'registered')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// A sandbox pinned to `node` by `target_node_id` with `active_node_id` NULL —
/// the shape the guard's `active` half cannot see.
async fn insert_target_pinned(
    pool: &PgPool,
    node: Uuid,
    lifecycle: &str,
    evacuation_restore_target: Option<&str>,
) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             target_node_id, evacuation_restore_target, placement_changed_at,
             lifecycle_changed_at, network_policy_generation
         ) VALUES (
             $1, '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $3, 'unplaced', 'k8s', $2, $4,
             NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour', 1
         )"
    ))
    .bind(id)
    .bind(node)
    .bind(lifecycle)
    .bind(evacuation_restore_target)
    .execute(pool)
    .await?;
    Ok(id)
}

/// What the node-drain reconciler's gate counts for this node.
async fn reconciler_gate(pool: &PgPool, node: Uuid) -> TestResult<i64> {
    let inner = drain_gate_blockers_for_node("$1");
    let query = format!("SELECT COUNT(*)::BIGINT FROM (\n{inner}\n) g");
    Ok(
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(node)
            .fetch_one(pool)
            .await?,
    )
}

/// What the `:terminate` guard counts, through the production function.
async fn terminate_guard(pool: &PgPool, node: Uuid) -> TestResult<(i64, i64)> {
    let mut tx = pool.begin().await?;
    let occ = super::sandbox_occupancy_for_node_in_tx(
        &mut tx,
        NodeId::from_uuid(node),
        ResidencyEvidence::LifecycleOnly,
    )
    .await?;
    tx.commit().await?;
    Ok((occ.active, occ.drain_blockers))
}

async fn pre_change_guard(pool: &PgPool, node: Uuid) -> TestResult<i64> {
    let q = pre_change_guard_query();
    Ok(
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql!(q.as_str())))
            .bind(node)
            .fetch_one(pool)
            .await?,
    )
}

/// A node holding a `suspending` row pinned only by `target_node_id` must not
/// be terminable through the admin endpoint.
///
/// `suspending` is the dangerous one: the guest is being checkpointed, so the
/// only copy of its memory is on the node, and no snapshot row exists yet for
/// any durability predicate to find.
#[tokio::test(flavor = "current_thread")]
async fn a_suspending_row_blocks_terminate_and_the_gate_agrees() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool).await?;
    insert_target_pinned(pool, node, "suspending", None).await?;

    let gate = reconciler_gate(pool, node).await?;
    let (active, blockers) = terminate_guard(pool, node).await?;

    assert_eq!(
        gate, 1,
        "the reconciler's gate must count the suspending row"
    );
    assert_eq!(
        blockers, gate,
        "the `:terminate` guard must count exactly what the reconciler counts; \
         disagreement here is a node terminated while the reconciler still \
         holds it in `draining`"
    );
    assert_eq!(
        active, 0,
        "and the `active` half must not be what saves it — the row has a NULL \
         `active_node_id`, so if `active` covered this the test would prove \
         nothing about the gate"
    );

    assert_eq!(
        pre_change_guard(pool, node).await?,
        0,
        "the guard's previous statement must return zero here. If it does not, \
         this fixture is not one where the two disagreed and the agreement \
         asserted above is not evidence of anything"
    );
    Ok(())
}

/// A non-evacuation `restoring` row blocks too; an evacuation restore does not.
///
/// The distinction is load-bearing and is why the gate spells out
/// `evacuation_restore_target IS NULL`: a drain's *own* evacuation restore is
/// the mechanism moving the sandbox off, so counting it would make the drain
/// block on itself.
#[tokio::test(flavor = "current_thread")]
async fn a_non_evacuation_restoring_row_blocks_but_an_evacuation_restore_does_not() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    let blocking = insert_node(pool).await?;
    insert_target_pinned(pool, blocking, "restoring", None).await?;
    let gate = reconciler_gate(pool, blocking).await?;
    let (_, blockers) = terminate_guard(pool, blocking).await?;
    assert_eq!(gate, 1, "a plain restoring row is a gate blocker");
    assert_eq!(blockers, gate, "and the guard must agree");
    assert_eq!(
        pre_change_guard(pool, blocking).await?,
        0,
        "pre-change the guard missed it"
    );

    let evacuating = insert_node(pool).await?;
    insert_target_pinned(pool, evacuating, "restoring", Some("started")).await?;
    let gate = reconciler_gate(pool, evacuating).await?;
    let (_, blockers) = terminate_guard(pool, evacuating).await?;
    assert_eq!(
        gate, 0,
        "an evacuation restore is the drain's own work and must not block it"
    );
    assert_eq!(
        blockers, gate,
        "the guard must inherit that exemption rather than re-deriving it — \
         two definitions is how this diverged in the first place"
    );
    Ok(())
}
