//! Postgres-backed coverage for the census term on the resident guard shared
//! by `mark_terminating_query` and `delete_drained_node_if_orphan_query`.
//!
//! The pairing with node-drain is the point. Node-drain's `draining → drained`
//! gate discounts rows a node's own census proves absent; if this guard did not
//! discount the same rows, a node would clear that gate and then never leave
//! `drained` — the UPDATE here would match nothing on every tick, and
//! node-drain stops looking at a node once it is no longer `draining`. The
//! fail-closed direction is unchanged: a node the control plane cannot see
//! keeps every row blocking.

use pplx_space_common_core::types::{NodeId, SandboxId, SubstrateKind};

use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_svc::sql::sandbox_residency::{
    CENSUS_INVENTORY_MAX_AGE, CENSUS_PLACEMENT_SETTLE,
};
use pplx_space_db_test_fixtures::try_pg_test_db;
use sqlx::PgPool;

use super::{
    PHASE_DRAINED, PHASE_TERMINATING, ResidencyEvidence, delete_refusal_diagnostics_query,
    mark_terminating_query,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// How current the node's last **accepted** sandbox census is —
/// `node_status_snapshots.sandbox_inventory_pruned_at`. Separate from heartbeat
/// freshness: a node can still answer while its inventory publisher is wedged.
#[derive(Debug, Clone, Copy)]
enum Census {
    /// A complete inventory was pruned against just now.
    Fresh,
    /// Published, but older than [`CENSUS_INVENTORY_MAX_AGE`] — wedged publisher.
    Stale,
    /// NULL: no complete inventory has ever been accepted from this node, which
    /// is every row's state at rollout.
    Never,
}

impl Census {
    fn secs_ago(self) -> Option<f64> {
        match self {
            Self::Fresh => Some(0.0),
            Self::Stale => Some((CENSUS_INVENTORY_MAX_AGE * 2).as_secs() as f64),
            Self::Never => None,
        }
    }
}

/// A drained node publishing complete inventories normally, so the census is
/// never what a test is varying. Census-age cases call
/// [`insert_drained_node_with_census`].
async fn insert_drained_node(pool: &PgPool, reachable_and_fresh: bool) -> TestResult<NodeId> {
    insert_drained_node_with_census(pool, reachable_and_fresh, Census::Fresh).await
}

async fn insert_drained_node_with_census(
    pool: &PgPool,
    reachable_and_fresh: bool,
    census: Census,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_started_at)
         VALUES ($1, '10.0.0.7'::inet, 'worker', 'k8s', 'registered', 'drained', NOW())"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, role,
             scheduling_status, services, sandbox_inventory_pruned_at, updated_at
         )
         VALUES (
             $1, NOW(), NOW() + make_interval(secs => $3), $2,
             'worker', 'ready', '[]'::jsonb,
             CASE WHEN $4::FLOAT8 IS NULL
                  THEN NULL
                  ELSE NOW() - make_interval(secs => $4)
             END,
             NOW()
         )"
    ))
    .bind(node_id)
    .bind(reachable_and_fresh)
    .bind(if reachable_and_fresh { 30.0_f64 } else { -30.0 })
    .bind(census.secs_ago())
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// Placed outside `CENSUS_PLACEMENT_SETTLE` so the census term is what most of
/// these tests exercise. `None` writes a NULL `placement_changed_at` — the
/// legacy-row shape whose blocking behavior this gate has to preserve.
async fn insert_started_sandbox(pool: &PgPool, node_id: NodeId) -> TestResult<SandboxId> {
    insert_started_sandbox_placed(pool, node_id, Some(CENSUS_PLACEMENT_SETTLE * 2)).await
}

async fn insert_started_sandbox_placed(
    pool: &PgPool,
    node_id: NodeId,
    placed_ago: Option<Duration>,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, network_policy_generation, placement_changed_at
         ) VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'started', 'on_node', 'k8s', $2, 1,
             CASE WHEN $3::FLOAT8 IS NULL
                  THEN NULL
                  ELSE NOW() - make_interval(secs => $3)
             END
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(placed_ago.map(|ago| ago.as_secs() as f64))
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

async fn observe_sandbox(pool: &PgPool, sandbox_id: SandboxId, node_id: NodeId) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, observed_at, updated_at)
         VALUES ($1, $2, 'started', NOW(), NOW())"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn try_mark_terminating(
    pool: &PgPool,
    node_id: NodeId,
    residency: ResidencyEvidence,
) -> TestResult<u64> {
    let query = mark_terminating_query(residency);
    Ok(sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node_id)
        .bind(PHASE_TERMINATING)
        .bind(PHASE_DRAINED)
        .execute(pool)
        .await?
        .rows_affected())
}

/// A phantom `started` row on a node whose census does not list it must not
/// hold the node in `drained`. Under the kill switch the same row blocks, which
/// is the behavior that predates the census join.
#[tokio::test(flavor = "current_thread")]
async fn a_census_absent_row_no_longer_parks_the_node_in_drained() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    let blocked = insert_drained_node(pool, true).await?;
    insert_started_sandbox(pool, blocked).await?;
    assert_eq!(
        try_mark_terminating(pool, blocked, ResidencyEvidence::LifecycleOnly).await?,
        0,
        "without the census term the phantom row blocks — the pre-change behavior"
    );

    assert_eq!(
        try_mark_terminating(pool, blocked, ResidencyEvidence::NodeCensusAware).await?,
        1,
        "a fresh node that does not report the sandbox must release the node"
    );
    Ok(())
}

/// The row the node *does* report still blocks: this guard is what catches a
/// sandbox that landed on a drained node after a lost cordon.
#[tokio::test(flavor = "current_thread")]
async fn a_reported_row_still_blocks_termination() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    let node_id = insert_drained_node(pool, true).await?;
    let sandbox_id = insert_started_sandbox(pool, node_id).await?;
    observe_sandbox(pool, sandbox_id, node_id).await?;

    assert_eq!(
        try_mark_terminating(pool, node_id, ResidencyEvidence::NodeCensusAware).await?,
        0,
        "a sandbox the node reports holding must block termination"
    );
    Ok(())
}

/// Fail-closed: an expired heartbeat means the census cannot be trusted to be
/// current, so every row keeps blocking — the direction that costs a stalled
/// node rather than a terminated one.
#[tokio::test(flavor = "current_thread")]
async fn an_unseen_node_keeps_blocking_even_with_an_empty_census() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    let node_id = insert_drained_node(pool, false).await?;
    insert_started_sandbox(pool, node_id).await?;

    assert_eq!(
        try_mark_terminating(pool, node_id, ResidencyEvidence::NodeCensusAware).await?,
        0,
        "a stale heartbeat must not let an empty census release the node"
    );
    Ok(())
}

/// The refusal diagnostics count residents through the same predicate, so the
/// reason an operator is shown matches the guard that actually refused. A
/// diagnostic that still counted phantoms would report `active_resident` on
/// every refusal and send the operator hunting for sandboxes that do not exist.
#[tokio::test(flavor = "current_thread")]
async fn refusal_diagnostics_count_residents_through_the_same_predicate() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    let node_id = insert_drained_node(pool, true).await?;
    insert_started_sandbox(pool, node_id).await?;

    let query = delete_refusal_diagnostics_query(ResidencyEvidence::NodeCensusAware);
    let (_, _, _, active_residents): (bool, bool, i64, i64) =
        sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(node_id)
            .bind(Option::<String>::None)
            .bind(SubstrateKind::Kubernetes)
            .fetch_one(pool)
            .await?;
    assert_eq!(
        active_residents, 0,
        "the diagnosis must discount the same rows the guard does"
    );
    Ok(())
}

/// Fail-closed on the legacy-row shape, through the termination UPDATE rather
/// than through the generated SQL string: a NULL `placement_changed_at` means
/// the row's placement age is unknown, which is not evidence of absence, so it
/// must keep holding the node in `drained` even with an empty census and a
/// fresh heartbeat.
#[tokio::test(flavor = "current_thread")]
async fn a_null_placement_age_keeps_blocking_termination() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    let node_id = insert_drained_node(pool, true).await?;
    insert_started_sandbox_placed(pool, node_id, None).await?;

    assert_eq!(
        try_mark_terminating(pool, node_id, ResidencyEvidence::NodeCensusAware).await?,
        0,
        "unknown placement age must never be read as census-proven absence"
    );
    Ok(())
}

/// The terminator half of the census-freshness gate, and the gate that actually
/// destroys the node: a fresh heartbeat on a node whose complete-inventory
/// publisher has wedged must not release `drained → terminating`. Every other
/// facet keeps that heartbeat fresh while the census ages, so on the heartbeat
/// alone this UPDATE would fire and terminate a node still holding the VM.
///
/// Paired with node-drain's `a_fresh_heartbeat_with_no_current_census_discounts_nothing`
/// on purpose — the two gates read one predicate from one env var, and a census
/// term present in only one of them is how a node either loses a live sandbox or
/// parks in `drained` forever.
#[tokio::test(flavor = "current_thread")]
async fn a_stale_census_keeps_blocking_termination_however_fresh_the_heartbeat() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    for census in [Census::Stale, Census::Never] {
        let node_id = insert_drained_node_with_census(pool, true, census).await?;
        // The shape that IS released on a node with a current census: settled
        // placement, empty census, fresh heartbeat.
        insert_started_sandbox(pool, node_id).await?;

        assert_eq!(
            try_mark_terminating(pool, node_id, ResidencyEvidence::NodeCensusAware).await?,
            0,
            "{census:?}: an empty census that nobody is updating must not release \
             the node for termination"
        );
    }
    Ok(())
}
