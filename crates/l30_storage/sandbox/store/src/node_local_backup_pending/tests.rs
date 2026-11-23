//! The census delete's plan, read against the statement
//! [`super::delete_durable_absent`] executes.

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, insert_ready_worker_node, try_pg_test_db,
};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::DELETE_DURABLE_ABSENT;

/// Neither disjunct of the delete guard may fall back to reading a sandbox's
/// whole snapshot history per queue row. Asserted on the index the plan names
/// rather than on a buffer count, because counts move with the machine and the
/// statistics while the index is the property.
///
/// An earlier revision nested the disjunction inside one correlated `EXISTS`.
/// It selected exactly the same rows and cost 20,403 shared buffers against
/// 1,203, because no index covered an `OR` spanning two different key sets —
/// invisible to every test that checks only which rows come back, which is why
/// this one reads the plan.
///
/// It EXPLAINs [`DELETE_DURABLE_ABSENT`] itself, so the `USING` join and the
/// node filter around the shared predicate are pinned here too.
#[tokio::test(flavor = "current_thread")]
async fn the_delete_guard_resolves_through_indexes_not_a_history_scan() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    seed_at_scale(&db.pool, node_id).await?;

    let plan = sqlx::query_as::<_, (String,)>(sqlx::AssertSqlSafe(sql!(
        format!("EXPLAIN (COSTS OFF) {}", DELETE_DURABLE_ABSENT.as_str()).as_str()
    )))
    .bind(node_id)
    .bind(Vec::<Uuid>::new())
    .fetch_all(&db.pool)
    .await?
    .into_iter()
    .map(|(line,)| line)
    .collect::<Vec<_>>()
    .join("\n");

    // Both disjuncts, counted rather than merely present. They are the same
    // fragment over different artifact expressions, so a single hit would be
    // satisfied by the own-bytes arm alone and the assertion would say nothing
    // about whether the departed-marker arm still had an index.
    let index_hits = plan.matches("idx_snapshots_ready_local_artifact").count();
    assert_eq!(
        index_hits, 2,
        "both artifact-identity probes must resolve through \
         idx_snapshots_ready_local_artifact; found {index_hits}, plan was:\n{plan}"
    );
    assert!(
        !plan.contains("Seq Scan on snapshots"),
        "a sequential scan over snapshots means a probe lost its index; \
         plan was:\n{plan}"
    );
    Ok(())
}

/// Enough rows that the planner prefers an index, and enough snapshots per
/// sandbox that a lost index shows up as a history scan rather than as noise.
///
/// **The row count has a floor.** At roughly 40k snapshots (2,000 sandboxes
/// x 20) or fewer, the planner costs a sequential scan of `snapshots` below
/// the index probes and the plan assertion fails on a fixture that is
/// perfectly correct, so a smaller fixture would be testing the planner's
/// costing thresholds rather than whether the predicate is sargable. 2,000
/// sandboxes x 50 snapshots (100k) clears that floor with margin.
///
/// Every seeded id is `gen_random_uuid()` rather than `space_uuid_v7()`,
/// including the `snapshot_storage_locations.id` that would otherwise come
/// from its column default. Both `snapshots.id` and
/// `snapshot_storage_locations.id` default to the PL/pgSQL `space_uuid_v7()`,
/// which costs about 8x a `gen_random_uuid()` call, and at 100k rows per table
/// that dominates the whole test. Nothing here depends on the ids being
/// time-ordered: the plan is keyed on `(sandbox_id, local_artifact_id,
/// artifact_kind)` and the fixture never reads an id back.
///
/// The two 100k-row inserts run with `session_replication_role = replica`,
/// which turns off their foreign-key checks. Postgres checks those row by row,
/// two or three lookups per row, and they were most of the fixture's cost.
/// Every seeded reference is taken from a row that already exists, so there is
/// nothing for a check to catch, and neither table has a trigger of its own
/// that the setting would also skip. The sandboxes insert keeps its triggers.
async fn seed_at_scale(pool: &PgPool, node: NodeId) -> TestResult<()> {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             target_node_id, placement_changed_at, lifecycle_changed_at,
             current_local_artifact_id, current_local_artifact_kind,
             current_local_artifact_origin, network_policy_generation
         )
         SELECT gen_random_uuid(), $1, $2, 'suspended', 'unplaced', 'k8s', $3,
                NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour',
                gen_random_uuid(), 'full', 'suspend', 1
           FROM generate_series(1, 2000)"
    ))
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(node)
    .execute(pool)
    .await?;
    let mut bulk = pool.begin().await?;
    sqlx::query(sql!("SET LOCAL session_replication_role = replica"))
        .execute(&mut *bulk)
        .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, source_node_id, org_id, timestamp,
             local_artifact_id, artifact_kind, artifact_origin
         )
         SELECT gen_random_uuid(), s.id, 'full', 'ready', $1, $2,
                NOW() - (g * INTERVAL '1 minute'), gen_random_uuid(), 'full', 'suspend'
           FROM sandboxes s CROSS JOIN generate_series(1, 50) g"
    ))
    .bind(node)
    .bind(SEED_ORG_ID)
    .execute(&mut *bulk)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
             (id, snapshot_id, storage_connection_string, org_id)
         SELECT gen_random_uuid(), id, 's3://space-test/plan-probe', $1 FROM snapshots"
    ))
    .bind(SEED_ORG_ID)
    .execute(&mut *bulk)
    .await?;
    bulk.commit().await?;
    sqlx::query(sql!(
        "INSERT INTO node_local_backup_pending (
             node_id, sandbox_id, artifact_id, artifact_kind
         )
         SELECT $1, s.id, gen_random_uuid(), 'full' FROM sandboxes s LIMIT 200"
    ))
    .bind(node)
    .execute(pool)
    .await?;
    // Only the four tables the plan reads. A database-wide ANALYZE also walks
    // every table the migrations create, none of which this statement touches.
    sqlx::query(sql!(
        "ANALYZE sandboxes, snapshots, snapshot_storage_locations, node_local_backup_pending"
    ))
    .execute(pool)
    .await?;
    Ok(())
}
