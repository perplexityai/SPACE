use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{pg_test_db, try_pg_test_db};
use pplx_space_sandbox_residency::ResidencyEvidence;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{fleet_occupancy, terminate_occupancy};

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

/// The `:terminate` guard's `active` count applies the census discount, so it
/// refuses exactly what node-drain's gate and `mark_terminating_query` refuse.
///
/// This is the wedge the guard would otherwise be: `active` used to restate the
/// exclusion list with no census term. A worker whose own census disproves its
/// phantom `active_node_id` rows clears the census-aware drain gate, reaches
/// `drained` — and a guard that still counted those rows would 409 every
/// `:terminate` for the rest of the node's life, with `space-rollout-mng.py`
/// permanently excluding it.
///
/// The `LifecycleOnly` half of the loop is the kill switch: with the discount
/// off, the phantom row counts again and the guard refuses. Fail-closed.
#[tokio::test(flavor = "current_thread")]
async fn terminate_guard_active_count_honors_the_census_discount() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.71'::inet, 'worker', 'k8s', 'registered')"
    ))
    .bind(node)
    .execute(&db.pool)
    .await?;
    // Reachable with a fresh heartbeat: the only state in which the census may
    // be read as proof of absence.
    sqlx::query(sql!(
        // `sandbox_inventory_pruned_at` is the second, independent precondition
        // of the census discount (`node_census_fresh`): a fresh heartbeat alone
        // no longer earns it. Stamped current so this fixture isolates the thing
        // it is about — whether the guard applies the discount — rather than
        // withholding it for a reason the test never mentions.
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, role,
             scheduling_status, services, sandbox_inventory_pruned_at, updated_at
         )
         VALUES ($1, NOW(), NOW() + INTERVAL '30 seconds', TRUE,
                 'worker', 'ready', '[]'::jsonb, NOW(), NOW())"
    ))
    .bind(node)
    .execute(&db.pool)
    .await?;
    // A phantom: `active_node_id` names the node, placement settled long ago,
    // and the node's census carries no row for it.
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, placement_changed_at, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'started', 'on_node', 'k8s',
             $2, NOW() - INTERVAL '1 hour', 1
         )"
    ))
    .bind(Uuid::now_v7())
    .bind(node)
    .execute(&db.pool)
    .await?;

    for (residency, want_active) in [
        (ResidencyEvidence::NodeCensusAware, 0),
        (ResidencyEvidence::LifecycleOnly, 1),
    ] {
        let mut tx = db.pool.begin().await?;
        let occ = terminate_occupancy(&mut *tx, node, residency).await?;
        tx.commit().await?;
        assert_eq!(
            occ.active, want_active,
            "{residency}: a census-absent row must be discounted here exactly as \
             the drain gate discounts it, or `:terminate` refuses a node the \
             drain gate already released"
        );
    }
    Ok(())
}

/// The ordinary admin listing asks for no drainability counters, so its
/// occupancy CTE must stay streamable. Materializing it would spool every
/// active sandbox row in the listed set into a tuplestore — on a fleet listing
/// that is the whole `sandboxes` working set, and the listing predates the
/// ranking, so paying that cost for it is a regression rather than a tradeoff.
///
/// Materialization is only owed to the mode that plans a correlated census
/// subquery per row, where re-evaluating it three times is the worse cost.
///
/// This guards the *decision*, not the plan: it pins which of the two modes
/// gets the optimizer fence, which is the regression that matters. It does not
/// verify the plan-shape reasoning in `row_cte_materialization`'s docstring —
/// no `EXPLAIN` runs anywhere in this module, so the per-aggregate re-expansion
/// and `jit_above_cost` claims rest on the shared fragment's own measurements
/// rather than on this assertion.
#[test]
fn only_census_aware_ranking_materializes_the_per_row_cte() {
    for residency in [
        ResidencyEvidence::LifecycleOnly,
        ResidencyEvidence::NodeCensusAware,
    ] {
        assert_eq!(
            super::row_cte_materialization(false, residency),
            "",
            "a listing with no ranking has no expensive flag to protect, \
             whatever the cluster's residency policy is"
        );
    }

    assert_eq!(
        super::row_cte_materialization(true, ResidencyEvidence::LifecycleOnly),
        "",
        "lifecycle-only ranking plans no census probe, so the flags are a \
         constant and an `IN` test over a column already on the row"
    );
    assert_eq!(
        super::row_cte_materialization(true, ResidencyEvidence::NodeCensusAware),
        " MATERIALIZED",
        "three aggregates over an inlined census predicate is three \
         evaluations of the absence proof per row"
    );

    // The fleet statements spell the fence out; pin each shape to the
    // decision above.
    for ranked in [false, true] {
        for residency in [
            ResidencyEvidence::LifecycleOnly,
            ResidencyEvidence::NodeCensusAware,
        ] {
            assert_eq!(
                super::fleet_occupancy_statement(ranked, residency)
                    .contains("active_rows AS MATERIALIZED ("),
                ranked && residency == ResidencyEvidence::NodeCensusAware,
                "the fleet statement fences active_rows only for census-aware ranking \
                 (ranked {ranked}, {residency:?})"
            );
        }
    }
}
