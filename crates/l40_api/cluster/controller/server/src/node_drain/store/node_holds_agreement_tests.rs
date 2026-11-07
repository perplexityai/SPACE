//! The admin holds read must tag the rows node-drain's own statements count.
//!
//! `pplx_space_sandbox_store::node_holds` tags each row with the gate terms
//! that count it, and the API's agreement test holds those terms to the
//! drainability counters and the `:terminate` guard. This holds them to the
//! drain itself: for one row of every stored lifecycle, in every placement,
//! reported by its node's census or not, under both evidence modes, the rows
//! tagged `resident`, `drain_blocker` and `active_resident` are exactly as
//! many as [`resident_sandbox_counts_query`]'s `occupying`,
//! [`drain_blocking_sandboxes_query`]'s count and the summed
//! [`termination_blocking_residents_query`], each run through the sink that
//! runs it in production. A drift in any of those statements that changes how
//! lifecycle, placement or the census count fails here. The sweep seeds no
//! snapshot or evacuation marker; the API agreement test covers those states
//! against the drainability counters.

use std::num::NonZeroUsize;

use pplx_space_common_core::types::{NodeId, SandboxId};
use pplx_space_sandbox_residency::CENSUS_PLACEMENT_SETTLE;
use pplx_space_sandbox_store::StoredSandboxLifecycle;
use pplx_space_sandbox_store::node_holds;
use pplx_space_sandbox_store::test_support::NON_CANONICAL_ROWS;

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const LIMIT: NonZeroUsize = NonZeroUsize::new(500).expect("non-zero");

/// The tests [`every_term_counts_what_node_drain_counts`] deals its cells
/// into. Coprime with one lifecycle's eight cells, so each shard mixes
/// placements and census states, about 14 cells each.
const SHARDS: usize = 11;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Placement {
    Active,
    Target,
    Both,
    Neither,
}

#[derive(Debug, Clone, Copy)]
struct Cell {
    lifecycle: &'static str,
    placement: Placement,
    reported: bool,
}

/// Every cell, in one fixed order: each stored lifecycle, the values the
/// column holds outside the vocabulary included, across placement and
/// census.
fn cells() -> Vec<Cell> {
    let lifecycles = StoredSandboxLifecycle::all_as_str()
        .chain(NON_CANONICAL_ROWS.iter().map(|(_, stored)| *stored));
    let mut cells = Vec::new();
    for lifecycle in lifecycles {
        for placement in [
            Placement::Active,
            Placement::Target,
            Placement::Both,
            Placement::Neither,
        ] {
            for reported in [true, false] {
                cells.push(Cell {
                    lifecycle,
                    placement,
                    reported,
                });
            }
        }
    }
    cells
}

/// A worker with a fresh heartbeat and a fresh complete census.
async fn insert_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_started_at)
         VALUES ($1, '10.0.0.43'::inet, 'worker', 'k8s', 'registered', $2, NOW())"
    ))
    .bind(node_id)
    .bind(PHASE_DRAINING)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, role,
             scheduling_status, services, sandbox_inventory_pruned_at, updated_at
         )
         VALUES ($1, NOW(), NOW() + INTERVAL '1 hour', TRUE,
                 'worker', 'ready', '[]'::jsonb, NOW(), NOW())"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// Places `cell` on `node` outside [`CENSUS_PLACEMENT_SETTLE`], so the census
/// can prove absence, and reported or omitted by that node's census.
async fn insert_cell(pool: &PgPool, node: NodeId, cell: Cell) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    let active = matches!(cell.placement, Placement::Active | Placement::Both);
    let target = matches!(cell.placement, Placement::Target | Placement::Both);
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, target_node_id,
             placement_changed_at, lifecycle_changed_at, network_policy_generation
         ) VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $2, 'placed', 'k8s',
             CASE WHEN $3 THEN $5::UUID END, CASE WHEN $4 THEN $5::UUID END,
             NOW() - make_interval(secs => $6), NOW() - INTERVAL '1 hour', 1
         )"
    ))
    .bind(id)
    .bind(cell.lifecycle)
    .bind(active)
    .bind(target)
    .bind(node)
    .bind((CENSUS_PLACEMENT_SETTLE * 2).as_secs_f64())
    .execute(pool)
    .await?;
    if cell.reported {
        sqlx::query(sql!(
            "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, observed_at, updated_at)
             VALUES ($1, $2, $3, NOW(), NOW())"
        ))
        .bind(id)
        .bind(node)
        .bind(cell.lifecycle)
        .execute(pool)
        .await?;
    }
    Ok(id)
}

fn sink(pool: PgPool, residency: ResidencyEvidence) -> SubstrateSink {
    let mut sink = SubstrateSink::with_suspend_execution(pool, |_| async move {
        unreachable!("the gate counts never suspend");
    });
    sink.residency_evidence = residency;
    sink
}

/// The cells are dealt into [`SHARDS`] tests by position, so every cell, a
/// new lifecycle's included, lands in exactly one of them.
#[rstest::rstest]
#[tokio::test(flavor = "current_thread")]
async fn every_term_counts_what_node_drain_counts(
    #[values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)] shard: usize,
) -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let control_node = insert_node(pool).await?;
    let mut held_somewhere = 0;
    for cell in cells().into_iter().skip(shard).step_by(SHARDS) {
        let node = insert_node(pool).await?;
        insert_cell(pool, node, cell).await?;
        insert_cell(pool, control_node, cell).await?;
        for residency in [
            ResidencyEvidence::LifecycleOnly,
            ResidencyEvidence::NodeCensusAware,
        ] {
            let holds = node_holds::list(pool, node, residency, LIMIT).await?;
            let sink = sink(pool.clone(), residency);
            let resident = sink.count_resident_sandboxes_on_node(node).await?;
            let blockers = sink.count_drain_blocking_sandboxes_on_node(node).await?;
            let termination_blocking: u64 = sink
                .count_termination_blocking_residents_on_node(node)
                .await?
                .by_lifecycle
                .iter()
                .map(|(_, count)| count)
                .sum();

            let tagged = |term: fn(&node_holds::NodeHoldTerms) -> bool| -> u64 {
                holds.rows.iter().filter(|row| term(&row.terms)).count() as u64
            };
            let context = format!("{cell:?} under {residency}: {:?}", holds.rows);
            assert_eq!(
                tagged(|terms| terms.resident),
                resident.occupying,
                "resident vs the resident scan's occupying: {context}"
            );
            assert_eq!(
                tagged(|terms| terms.drain_blocker),
                blockers.unsigned_abs(),
                "drain_blocker vs the drain-blocking count: {context}"
            );
            assert_eq!(
                tagged(|terms| terms.active_resident),
                termination_blocking,
                "active_resident vs the termination-blocking residents: {context}"
            );
            held_somewhere += u32::from(holds.rows.iter().any(|row| row.terms.counted()));
        }
    }
    // The sweep is only evidence if it reaches rows the drain counts.
    assert!(held_somewhere > 0);
    Ok(())
}
