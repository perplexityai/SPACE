//! The holds list must count what the gates count.
//!
//! `pplx_space_sandbox_store::node_holds` lists rows and tags each with the
//! gate terms that count it. The gates themselves only count. This sweep puts
//! one row of every stored lifecycle, in every placement, census, snapshot and
//! evacuation-marker state, on its own node beside a control row on another
//! node, and checks under both evidence modes that the rows tagged with each
//! term are exactly as many as that term's own count: the drainability
//! counters and the `:terminate` guard, each read through its production
//! function. The `counted_by` flags are checked for both termination-blocking
//! settings by combining those counts the way node-drain does. A term that
//! drifts from its gate fails here for the cell that exposes it. The cells
//! are dealt into [`SHARDS`] tests so each fits CI's per-test time budget.

use std::num::NonZeroUsize;
use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId, SandboxLifecycle, SubstrateKind};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_sandbox_residency::{NON_RESIDENT_LIFECYCLES, ResidencyEvidence};
use pplx_space_sandbox_store::node_holds::{self, NodeHoldPlacement, NodeHoldReason};
use pplx_space_test_env::TestResult;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::super::nodes::terminate_blocked_by_occupancy;
use super::{AdminNodeStore, sandbox_occupancy_for_node_in_tx};

const LIMIT: NonZeroUsize = NonZeroUsize::new(500).expect("non-zero");
const LONG_RUNNER_AGE: Duration = Duration::from_secs(24 * 60 * 60);

/// The tests [`every_term_counts_what_its_gate_counts`] deals its cells into:
/// one per `shard` value it runs. Coprime with one lifecycle's 48 cells, so
/// each shard gets about 20 cells across several lifecycles and states, and
/// stays well inside CI's per-test time limit.
const SHARDS: usize = 41;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Placement {
    Active,
    Target,
    Both,
    /// Neither column: only an upload from the node can hold it.
    Neither,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Snapshot {
    None,
    /// `ready`, with a live storage location, after the placement.
    Durable,
    /// `creating` from the cell's node, after the placement.
    Uploading,
}

#[derive(Debug, Clone, Copy)]
struct Cell {
    lifecycle: &'static str,
    placement: Placement,
    reported: bool,
    snapshot: Snapshot,
    marker: Option<&'static str>,
}

/// `count` workers, each with a fresh heartbeat and a fresh complete census.
async fn insert_nodes(pool: &PgPool, count: usize) -> TestResult<Vec<NodeId>> {
    let nodes: Vec<NodeId> = (0..count).map(|_| NodeId::new()).collect();
    let ids: Vec<Uuid> = nodes.iter().map(|node| node.as_uuid()).collect();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         SELECT id, '10.0.0.41'::INET, 'worker', 'k8s', 'registered'
           FROM UNNEST($1::UUID[]) AS n(id)"
    ))
    .bind(&ids)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, services,
             sandbox_inventory_pruned_at, updated_at
         )
         SELECT id, NOW(), NOW() + INTERVAL '1 hour', TRUE, '[]'::JSONB, NOW(), NOW()
           FROM UNNEST($1::UUID[]) AS n(id)"
    ))
    .bind(&ids)
    .execute(pool)
    .await?;
    Ok(nodes)
}

/// Places each `(node, cell)` on its node, settled two hours ago so the
/// census can prove absence, reported or omitted by that node's census, with
/// the cell's snapshot stamped after the placement. One statement per table,
/// so a shard's fixture costs four round trips.
async fn insert_cells(pool: &PgPool, placements: &[(NodeId, Cell)]) -> TestResult {
    let ids: Vec<Uuid> = placements
        .iter()
        .map(|_| SandboxId::new().as_uuid())
        .collect();
    let nodes: Vec<Uuid> = placements.iter().map(|(node, _)| node.as_uuid()).collect();
    let lifecycles: Vec<&str> = placements.iter().map(|(_, cell)| cell.lifecycle).collect();
    let active: Vec<bool> = placements
        .iter()
        .map(|(_, cell)| matches!(cell.placement, Placement::Active | Placement::Both))
        .collect();
    let target: Vec<bool> = placements
        .iter()
        .map(|(_, cell)| matches!(cell.placement, Placement::Target | Placement::Both))
        .collect();
    let markers: Vec<Option<&str>> = placements.iter().map(|(_, cell)| cell.marker).collect();
    let reported: Vec<bool> = placements.iter().map(|(_, cell)| cell.reported).collect();
    let snapshots: Vec<&str> = placements
        .iter()
        .map(|(_, cell)| match cell.snapshot {
            Snapshot::None => "none",
            Snapshot::Durable => "durable",
            Snapshot::Uploading => "uploading",
        })
        .collect();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, target_node_id, evacuation_restore_target,
             placement_changed_at, lifecycle_changed_at, network_policy_generation
         )
         SELECT c.id, $7, $8, c.lifecycle, 'placed', $9,
                CASE WHEN c.active THEN c.node END, CASE WHEN c.target THEN c.node END,
                c.marker, NOW() - INTERVAL '2 hours', NOW() - INTERVAL '1 hour', 1
           FROM UNNEST($1::UUID[], $2::UUID[], $3::TEXT[], $4::BOOL[], $5::BOOL[], $6::TEXT[])
                AS c(id, node, lifecycle, active, target, marker)"
    ))
    .bind(&ids)
    .bind(&nodes)
    .bind(&lifecycles)
    .bind(&active)
    .bind(&target)
    .bind(&markers)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(SubstrateKind::Kubernetes)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status)
         SELECT c.id, c.node, c.lifecycle
           FROM UNNEST($1::UUID[], $2::UUID[], $3::TEXT[], $4::BOOL[])
                AS c(id, node, lifecycle, reported)
          WHERE c.reported"
    ))
    .bind(&ids)
    .bind(&nodes)
    .bind(&lifecycles)
    .bind(&reported)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (sandbox_id, type, state, timestamp, source_node_id)
         SELECT c.id, 'full', 'creating', NOW() - INTERVAL '1 minute', c.node
           FROM UNNEST($1::UUID[], $2::UUID[], $3::TEXT[]) AS c(id, node, snapshot)
          WHERE c.snapshot = 'uploading'"
    ))
    .bind(&ids)
    .bind(&nodes)
    .bind(&snapshots)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "WITH ready AS (
             INSERT INTO snapshots (sandbox_id, type, state, timestamp, source_node_id)
             SELECT c.id, 'full', 'ready', NOW() - INTERVAL '30 minutes', c.node
               FROM UNNEST($1::UUID[], $2::UUID[], $3::TEXT[]) AS c(id, node, snapshot)
              WHERE c.snapshot = 'durable'
             RETURNING id
         )
         INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         SELECT id, 's3://synthetic-bucket/agreement' FROM ready"
    ))
    .bind(&ids)
    .bind(&nodes)
    .bind(&snapshots)
    .execute(pool)
    .await?;
    Ok(())
}

/// Every stored lifecycle: each [`SandboxLifecycle`] and the values the
/// column holds outside it.
fn stored_lifecycles() -> Vec<&'static str> {
    let mut lifecycles: Vec<&'static str> = SandboxLifecycle::all()
        .iter()
        .map(|lifecycle| lifecycle.as_str())
        .collect();
    lifecycles.extend(
        NON_RESIDENT_LIFECYCLES
            .iter()
            .filter(|stored| stored.parse::<SandboxLifecycle>().is_err()),
    );
    lifecycles
}

/// Every cell, in one fixed order: each stored lifecycle across placement,
/// census, snapshot and evacuation-marker states.
fn cells() -> Vec<Cell> {
    let mut cells = Vec::new();
    for lifecycle in stored_lifecycles() {
        for placement in [
            Placement::Active,
            Placement::Target,
            Placement::Both,
            Placement::Neither,
        ] {
            for reported in [true, false] {
                for snapshot in [Snapshot::None, Snapshot::Durable, Snapshot::Uploading] {
                    for marker in [None, Some("started")] {
                        cells.push(Cell {
                            lifecycle,
                            placement,
                            reported,
                            snapshot,
                            marker,
                        });
                    }
                }
            }
        }
    }
    cells
}

/// The cells are dealt into [`SHARDS`] tests by position, so every cell, a
/// new lifecycle's included, lands in exactly one of them. Each cell's row is
/// on its own node, and a copy of it is on one control node the shard shares.
#[rstest::rstest]
#[tokio::test(flavor = "current_thread")]
async fn every_term_counts_what_its_gate_counts(
    #[values(
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40
    )]
    shard: usize,
) -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = AdminNodeStore::new(pool.clone());
    let cells: Vec<Cell> = cells().into_iter().skip(shard).step_by(SHARDS).collect();
    let mut nodes = insert_nodes(pool, cells.len() + 1).await?;
    let control_node = nodes.pop().expect("one node per cell and the control");
    let placements: Vec<(NodeId, Cell)> = nodes
        .iter()
        .zip(&cells)
        .flat_map(|(node, cell)| [(*node, *cell), (control_node, *cell)])
        .collect();
    insert_cells(pool, &placements).await?;
    // The guard's statement runs in a transaction, as it does in production;
    // one rolled-back transaction serves every check.
    let mut terminate_tx = pool.begin().await?;
    let mut held_somewhere = 0;
    for (node, cell) in nodes.into_iter().zip(cells) {
        for residency in [
            ResidencyEvidence::LifecycleOnly,
            ResidencyEvidence::NodeCensusAware,
        ] {
            held_somewhere +=
                assert_cell_agrees(pool, &store, &mut terminate_tx, node, cell, residency).await?;
        }
    }
    terminate_tx.rollback().await?;
    // The sweep is only evidence if it reaches rows the gates count.
    assert!(held_somewhere > 0);
    Ok(())
}

/// Returns 1 when the cell's row holds its node in this mode.
async fn assert_cell_agrees(
    pool: &PgPool,
    store: &AdminNodeStore,
    terminate_tx: &mut Transaction<'_, Postgres>,
    node: NodeId,
    cell: Cell,
    residency: ResidencyEvidence,
) -> TestResult<u32> {
    let holds = node_holds::list(pool, node, residency, LIMIT).await?;
    let occupancy = store
        .occupancy_for_nodes(&[node], Some(LONG_RUNNER_AGE), residency)
        .await?
        .remove(&node)
        .expect("every requested node is reported");
    let live = occupancy.live.expect("counters were requested");
    let terminate = sandbox_occupancy_for_node_in_tx(terminate_tx, node, residency).await?;

    let tagged = |term: fn(&node_holds::NodeHold) -> bool| -> i64 {
        holds.rows.iter().filter(|row| term(row)).count() as i64
    };
    let context = format!("{cell:?} under {residency}: {:?}", holds.rows);
    assert!(holds.rows.len() <= 1, "only the cell's row: {context}");
    for row in &holds.rows {
        if row.reason == NodeHoldReason::CensusAbsent {
            assert!(
                !row.terms.drain_blocker,
                "census_absent is never a drain blocker: {context}"
            );
        }
        if matches!(
            row.reason,
            NodeHoldReason::UploadInFlight | NodeHoldReason::LocalOnlySnapshot
        ) {
            assert!(
                row.terms.drain_blocker,
                "{} is a drain blocker: {context}",
                row.reason
            );
        }
    }
    assert_eq!(
        tagged(|row| row.terms.resident),
        live.live_residents,
        "resident vs live_residents: {context}"
    );
    assert_eq!(
        tagged(|row| row.terms.drain_blocker),
        live.drain_blockers,
        "drain_blocker vs drainability drain_blockers: {context}"
    );
    assert_eq!(
        tagged(|row| row.terms.drain_blocker),
        terminate.drain_blockers,
        "drain_blocker vs :terminate drain_blockers: {context}"
    );
    assert_eq!(
        tagged(|row| row.terms.active_resident),
        live.terminate_blockers,
        "active_resident vs drainability terminate_blockers: {context}"
    );
    assert_eq!(
        tagged(|row| row.terms.active_resident),
        terminate.active,
        "active_resident vs :terminate active: {context}"
    );
    // The listing itself is census-free: every occupancy-scope row the raw
    // `active` count sees is listed, discounted or not.
    assert_eq!(
        holds
            .rows
            .iter()
            .filter(|row| matches!(
                row.placement,
                NodeHoldPlacement::Active | NodeHoldPlacement::Both
            ) && !NON_RESIDENT_LIFECYCLES.contains(&row.lifecycle.as_str()))
            .count() as i64,
        occupancy.occupancy.active,
        "listed occupancy rows vs raw active: {context}"
    );
    assert_eq!(
        holds.rows.iter().any(|row| row.terms.terminate()),
        terminate_blocked_by_occupancy(&terminate),
        "counted_by.terminate vs the :terminate refusal: {context}"
    );
    // Node-drain's `draining → drained` rule: residents and blockers always,
    // and the termination-blocking residents only while that gate is on.
    for gate_on in [false, true] {
        let gate_holds = live.live_residents > 0
            || live.drain_blockers > 0
            || (gate_on && live.terminate_blockers > 0);
        assert_eq!(
            holds.rows.iter().any(|row| {
                if gate_on {
                    row.terms.drain_with_termination_blocking_gate()
                } else {
                    row.terms.drain()
                }
            }),
            gate_holds,
            "counted_by.drain vs the drain gate with the termination-blocking gate \
             {}: {context}",
            if gate_on { "on" } else { "off" }
        );
    }
    Ok(u32::from(
        holds.rows.iter().any(|row| row.terms.terminate()),
    ))
}
