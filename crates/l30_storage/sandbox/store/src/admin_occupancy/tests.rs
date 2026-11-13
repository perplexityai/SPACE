use pplx_space_accounts::NodeId;
use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_sandbox_residency::ResidencyEvidence;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::fleet_occupancy;

async fn insert_node(pool: &PgPool) -> TestResult<NodeId> {
    let node = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.12'::INET, 'worker', 'k8s', 'ready')"
    ))
    .bind(node)
    .execute(pool)
    .await?;
    Ok(node)
}

/// A `restoring` sandbox pinned to `node` through `target_node_id`, with its
/// active node cleared as a restore leaves it, and the drain's marker if
/// `marker` names one.
async fn insert_target_pinned_restore(
    pool: &PgPool,
    node: NodeId,
    marker: Option<&str>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             target_node_id, evacuation_restore_target, lifecycle_changed_at,
             network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'restoring', 'unplaced', 'k8s',
             $2, $3, NOW(), 1
         )"
    ))
    .bind(Uuid::now_v7())
    .bind(node)
    .bind(marker)
    .execute(pool)
    .await?;
    Ok(())
}

/// The fleet read's drainability counters tell a drain's restore from a
/// user's: a restore without the marker holds its target node's drain open,
/// one the drain marked does not. Under either residency evidence, since the
/// marker term is the same in both statements.
#[tokio::test]
async fn ranked_fleet_occupancy_counts_only_unmarked_restores_as_drain_blockers() -> TestResult {
    let db = pg_test_db().await?;
    let marked = insert_node(&db.pool).await?;
    let unmarked = insert_node(&db.pool).await?;
    // The marker is the restore's target lifecycle, not a node id.
    insert_target_pinned_restore(&db.pool, marked, Some("started")).await?;
    insert_target_pinned_restore(&db.pool, unmarked, None).await?;

    for residency in [
        ResidencyEvidence::LifecycleOnly,
        ResidencyEvidence::NodeCensusAware,
    ] {
        let rows = fleet_occupancy(&db.pool, &[marked, unmarked], 86_400, true, residency).await?;
        let drain_blockers = |node| {
            rows.iter()
                .find(|row| row.node_id == node)
                .map(|row| (row.active, row.drain_blockers))
        };
        assert_eq!(drain_blockers(marked), Some((0, 0)), "{residency:?}");
        assert_eq!(drain_blockers(unmarked), Some((0, 1)), "{residency:?}");
    }
    Ok(())
}
