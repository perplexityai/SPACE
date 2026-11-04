//! Postgres-backed truth table for the live-resident drainability counts.
//!
//! The shared `active_node_proves_sandbox_absent` fragment has its own tests;
//! these assert that *this* query composes it in the safe direction. The
//! property under test is that the discount fires only on positive evidence — a
//! currently reachable node with an unexpired heartbeat whose census omits the
//! sandbox — and that every other combination leaves the row counting. Getting
//! the direction wrong reads a node outage as an empty node, and an operator who
//! drains on that reading terminates a host with running sandboxes on it.

use pplx_space_common_core::types::{NodeId, SandboxId};

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

use pplx_space_common_svc::sql::sandbox_residency::ResidencyEvidence;

use super::{AdminNodeStore, NodeLiveResidents};

const LONG_RUNNER_AGE: Duration = Duration::from_secs(24 * 60 * 60);

/// This file's subject is the census discount, which is opt-in per environment
/// and defaults off. Every case here therefore runs census-aware explicitly;
/// `lifecycle_only_counts_every_occupying_row` covers the default mode.
const CENSUS_AWARE: ResidencyEvidence = ResidencyEvidence::NodeCensusAware;

/// Heartbeat state of the node under test.
///
/// Heartbeat freshness is one of two independent things the discount requires;
/// the other is that the node's last **accepted** sandbox census is itself
/// recent (`node_status_snapshots.sandbox_inventory_pruned_at`). Every variant
/// below that writes a row stamps a current census, so each case isolates the
/// heartbeat as the single variable — otherwise the stale-heartbeat cases would
/// withhold the discount for two reasons at once and prove neither.
#[derive(Debug, Clone, Copy)]
enum Heartbeat {
    /// Reachable, `stale_after` in the future.
    Fresh,
    /// Reachable, but `stale_after` already passed.
    Expired,
    /// Heartbeat is current, but the node is unreachable.
    Unreachable,
    /// No `node_status_snapshots` row at all.
    Absent,
}

async fn insert_node(pool: &PgPool, heartbeat: Heartbeat) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.12'::INET, 'worker', 'space', 'registered')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    let (stale_after, reachable) = match heartbeat {
        Heartbeat::Fresh => ("NOW() + INTERVAL '1 hour'", true),
        Heartbeat::Expired => ("NOW() - INTERVAL '1 hour'", true),
        Heartbeat::Unreachable => ("NOW() + INTERVAL '1 hour'", false),
        Heartbeat::Absent => return Ok(node_id),
    };
    let statement = format!(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, services,
             sandbox_inventory_pruned_at, updated_at
         )
         VALUES ($1, NOW(), {stale_after}, $2, '[]'::JSONB, NOW(), NOW())"
    );
    sqlx::query(sqlx::AssertSqlSafe(sql!(statement.as_str())))
        .bind(node_id)
        .bind(reachable)
        .execute(pool)
        .await?;
    Ok(node_id)
}

/// `lifecycle_changed_at` carries a `BEFORE UPDATE` trigger that resets it to
/// `NOW()` on any lifecycle change, so ageing a row means writing the column
/// directly rather than through a lifecycle transition.
///
/// `placement_changed_at` is backdated well past `CENSUS_PLACEMENT_SETTLE`
/// because the shared absence proof requires settled placement: a
/// recently-placed row is legitimately missing from the newest census, so its
/// omission proves nothing. Tests that want the discount to be *reachable* need
/// settled rows; `unsettled_placement_is_never_proven_absent` covers the other
/// side deliberately.
async fn insert_sandbox(
    pool: &PgPool,
    active_node_id: NodeId,
    lifecycle: &str,
    lifecycle_age: Duration,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, lifecycle_changed_at, placement_changed_at,
             network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $3, 'placed', 'k8s',
             $2, NOW() - make_interval(secs => $4::BIGINT),
             NOW() - INTERVAL '2 hours', 1
         )"
    ))
    .bind(sandbox_id)
    .bind(active_node_id)
    .bind(lifecycle)
    .bind(i64::try_from(lifecycle_age.as_secs())?)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

async fn observe_on_node(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    status: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status)
         VALUES ($1, $2, $3)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(status)
    .execute(pool)
    .await?;
    Ok(())
}

async fn live_at(pool: &PgPool, node_id: NodeId, age: Duration) -> TestResult<NodeLiveResidents> {
    let store = AdminNodeStore::new(pool.clone());
    // Every requested node comes back, occupied or not: the row also carries the
    // node's heartbeat freshness, which the ranking needs regardless.
    let row = store
        .occupancy_for_nodes(std::slice::from_ref(&node_id), Some(age), CENSUS_AWARE)
        .await?
        .get(&node_id)
        .copied()
        .expect("every requested node is reported");
    let live = row
        .live
        .expect("a present row must carry the counters when a threshold was supplied");
    // Checked on every read in this file rather than in one test: both counts
    // come from a single statement precisely so that this cannot be violated,
    // and the operator-facing phantom-row subtraction depends on it.
    assert!(
        live.live_residents <= row.occupancy.active,
        "live residents ({}) must be a subset of active ({})",
        live.live_residents,
        row.occupancy.active
    );
    assert!(
        live.live_long_runners <= live.live_residents,
        "long runners ({}) must be a subset of live residents ({})",
        live.live_long_runners,
        live.live_residents
    );
    // The sandwich `AdminNodeDrainability::terminate_blockers` advertises. Also
    // checked on every read rather than in one case: it is the relation that
    // makes the term a usable tiebreak, and one dedicated test only covers the
    // phantom/reported split at a single set of counts.
    assert!(
        live.live_residents <= live.terminate_blockers,
        "live residents ({}) must be a subset of terminate blockers ({})",
        live.live_residents,
        live.terminate_blockers
    );
    assert!(
        live.terminate_blockers <= row.occupancy.active,
        "terminate blockers ({}) must be a subset of active ({})",
        live.terminate_blockers,
        row.occupancy.active
    );
    // Deliberately *not* asserted here: `local_snapshots <= drain_blockers`.
    // It reads plausibly — both describe pinned snapshots — and it is false. The
    // listing's in-flight branch inlines the legacy freshness gate while the
    // blocker fragment takes the artifact-exact arm, so a `creating` row naming
    // a different artifact than `current_local_artifact_id` counts as a local
    // snapshot and not as a gate blocker. See
    // `an_artifact_mismatched_inflight_upload_counts_the_sandbox_once`.
    Ok(live)
}

/// `local_snapshots` as the in-transaction `:terminate` guard computes it, for
/// the cases that assert the listing and the guard agree.
///
/// The residency mode is immaterial to this helper: the guard applies the census
/// discount to `active` only, and `local_snapshots` is reached through the
/// snapshot fragments, which have no census term. `LifecycleOnly` is passed to
/// keep the guard reading as it did before the discount existed.
async fn in_tx_local_snapshots(pool: &PgPool, node: NodeId) -> TestResult<i64> {
    let mut tx = pool.begin().await?;
    let occupancy =
        super::sandbox_occupancy_for_node_in_tx(&mut tx, node, ResidencyEvidence::LifecycleOnly)
            .await?;
    tx.commit().await?;
    Ok(occupancy.drain_blockers)
}

async fn live(pool: &PgPool, node_id: NodeId) -> TestResult<NodeLiveResidents> {
    live_at(pool, node_id, LONG_RUNNER_AGE).await
}

/// A `started` row on a reachable, freshly-heartbeating node that reports it
/// counts; an identical row the same node does not report is discounted. This
/// is the whole point of the counter — the discounted row is the phantom
/// backlog a gentle drain would otherwise wait on forever.
#[tokio::test(flavor = "current_thread")]
async fn reported_row_counts_and_unreported_row_is_discounted() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool, Heartbeat::Fresh).await?;

    let reported = insert_sandbox(pool, node, "started", Duration::from_secs(60)).await?;
    observe_on_node(pool, reported, node, "started").await?;
    insert_sandbox(pool, node, "started", Duration::from_secs(60)).await?;

    let live = live(pool, node).await?;
    assert_eq!(live.live_residents, 1, "only the reported row is live");
    assert_eq!(live.live_long_runners, 0);
    Ok(())
}

/// Fail-closed: with the node unreachable, heartbeat-expired, or never having
/// published a status snapshot, the control plane cannot distinguish "the
/// sandbox is gone" from "the node stopped talking", so every resident row
/// keeps counting.
#[tokio::test(flavor = "current_thread")]
async fn unreported_rows_count_when_the_node_is_not_authoritative() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for heartbeat in [
        Heartbeat::Expired,
        Heartbeat::Unreachable,
        Heartbeat::Absent,
    ] {
        let node = insert_node(pool, heartbeat).await?;
        insert_sandbox(pool, node, "started", Duration::from_secs(60)).await?;

        let live = live(pool, node).await?;
        assert_eq!(
            live.live_residents, 1,
            "{heartbeat:?}: an unreported row on a non-authoritative node must keep counting"
        );
    }
    Ok(())
}

/// The observed row has to belong to the node being asked about.
/// `sandbox_observed_states` is keyed by `sandbox_id` alone, so a row left
/// behind by the node a sandbox has already migrated off must not read as
/// residency on its current node — nor keep the old node's count up.
#[tokio::test(flavor = "current_thread")]
async fn observation_from_another_node_does_not_prove_residency() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let current = insert_node(pool, Heartbeat::Fresh).await?;
    let previous = insert_node(pool, Heartbeat::Fresh).await?;

    let sandbox = insert_sandbox(pool, current, "started", Duration::from_secs(60)).await?;
    observe_on_node(pool, sandbox, previous, "started").await?;

    assert_eq!(
        live(pool, current).await?.live_residents,
        0,
        "a stale observation from the previous node must not count on the current one"
    );
    assert_eq!(
        live(pool, previous).await?.live_residents,
        0,
        "the previous node hosts nothing: the sandbox's active placement moved"
    );
    Ok(())
}

/// The node's reported status legitimately disagrees with the control-plane
/// lifecycle mid-transition — a node reports `started` for a row the control
/// plane still calls `restoring`. Existence on the node is the residency
/// evidence, so a mismatch still counts; requiring equality would discount
/// live sandboxes and let a drain complete over them.
#[tokio::test(flavor = "current_thread")]
async fn status_mismatch_still_counts_as_a_live_resident() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool, Heartbeat::Fresh).await?;

    let sandbox = insert_sandbox(pool, node, "restoring", Duration::from_secs(60)).await?;
    observe_on_node(pool, sandbox, node, "started").await?;

    assert_eq!(live(pool, node).await?.live_residents, 1);
    Ok(())
}

/// Long-runners are the live residents past the age threshold, and only those:
/// an unreported row older than the threshold is not a long-runner, because it
/// is not a resident at all.
#[tokio::test(flavor = "current_thread")]
async fn long_runners_count_only_aged_live_residents() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool, Heartbeat::Fresh).await?;

    let aged = insert_sandbox(pool, node, "started", Duration::from_secs(48 * 60 * 60)).await?;
    observe_on_node(pool, aged, node, "started").await?;
    let young = insert_sandbox(pool, node, "started", Duration::from_secs(60 * 60)).await?;
    observe_on_node(pool, young, node, "started").await?;
    // Aged, but the node does not report it.
    insert_sandbox(pool, node, "started", Duration::from_secs(72 * 60 * 60)).await?;

    let live = live(pool, node).await?;
    assert_eq!(live.live_residents, 2);
    assert_eq!(live.live_long_runners, 1);

    // A wider threshold reclassifies the 48 h resident as short-lived.
    let wide = live_at(pool, node, Duration::from_secs(7 * 24 * 60 * 60)).await?;
    assert_eq!(wide.live_residents, 2);
    assert_eq!(wide.live_long_runners, 0);
    Ok(())
}

/// Terminal and migrated-off lifecycles are outside **both** scopes: they are
/// excluded from `sandboxes_active` and from the resident set, so they
/// contribute to neither count and cannot inflate the difference between them.
///
/// The two scopes are not identical — `non_evacuable_lifecycles_occupy_the_node_without_blocking_a_drain`
/// pins the rows that occupy without being evacuable — so
/// `sandboxes_active - live_residents` is the phantom backlog *plus* those
/// non-evacuable rows. This case is what keeps terminal rows out of that
/// difference.
#[tokio::test(flavor = "current_thread")]
async fn non_resident_lifecycles_are_excluded_even_when_reported() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool, Heartbeat::Fresh).await?;

    for lifecycle in ["failed", "cleanup_required", "deleted", "suspended"] {
        let sandbox = insert_sandbox(pool, node, lifecycle, Duration::from_secs(60)).await?;
        observe_on_node(pool, sandbox, node, lifecycle).await?;
    }
    let resident = insert_sandbox(pool, node, "paused", Duration::from_secs(60)).await?;
    observe_on_node(pool, resident, node, "paused").await?;

    let live = live(pool, node).await?;
    assert_eq!(live.live_residents, 1);

    let store = AdminNodeStore::new(pool.clone());
    let occupancy = store
        .occupancy_for_nodes(std::slice::from_ref(&node), None, CENSUS_AWARE)
        .await?
        .get(&node)
        .copied()
        .unwrap_or_default();
    assert_eq!(
        occupancy.occupancy.active, 1,
        "the active count must scope the same lifecycles as the live count"
    );
    assert!(
        occupancy.live.is_none(),
        "without a threshold the counters must be absent, not zero"
    );
    Ok(())
}

/// Batching must attribute per node rather than summing across the request.
/// A node with no *live* resident is still reported — with a zero
/// `live_residents` next to its non-zero `active`, which is the phantom-row
/// pairing an operator reads off the two columns.
#[tokio::test(flavor = "current_thread")]
async fn batched_lookup_attributes_counts_per_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let busy = insert_node(pool, Heartbeat::Fresh).await?;
    let empty = insert_node(pool, Heartbeat::Fresh).await?;

    for age in [Duration::from_secs(60), Duration::from_secs(48 * 60 * 60)] {
        let sandbox = insert_sandbox(pool, busy, "started", age).await?;
        observe_on_node(pool, sandbox, busy, "started").await?;
    }
    insert_sandbox(pool, empty, "started", Duration::from_secs(60)).await?;

    let store = AdminNodeStore::new(pool.clone());
    let counts = store
        .occupancy_for_nodes(&[busy, empty], Some(LONG_RUNNER_AGE), CENSUS_AWARE)
        .await?;
    let busy_counts = counts
        .get(&busy)
        .copied()
        .unwrap_or_default()
        .live
        .expect("a threshold was supplied");
    assert_eq!(busy_counts.live_residents, 2);
    assert_eq!(busy_counts.live_long_runners, 1);
    // The other node's only resident is a phantom row: non-zero `active`, zero
    // `live_residents`. That pairing is the phantom-row backlog an operator
    // reads off the two columns, and it is only expressible because one
    // statement produces both.
    let empty_counts = counts
        .get(&empty)
        .copied()
        .expect("every requested node is reported");
    assert_eq!(empty_counts.occupancy.active, 1);
    assert_eq!(
        empty_counts
            .live
            .expect("a threshold was supplied")
            .live_residents,
        0
    );
    Ok(())
}

/// A reported `stopped` sandbox occupies the node — it blocks `:terminate` and
/// shows in `sandboxes_active` — but the drain reconciler does not wait on it,
/// because sandbox-manager cannot suspend from `stopped`. The ranking predicts
/// what a drain waits on, so it must not count it either; a node whose only
/// resident is `stopped` is a prime candidate, not a blocked one. Same for
/// `created`, which has no pod placed at all.
#[tokio::test(flavor = "current_thread")]
async fn non_evacuable_lifecycles_occupy_the_node_without_blocking_a_drain() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool, Heartbeat::Fresh).await?;

    for lifecycle in ["stopped", "created"] {
        let sandbox =
            insert_sandbox(pool, node, lifecycle, Duration::from_secs(48 * 60 * 60)).await?;
        observe_on_node(pool, sandbox, node, lifecycle).await?;
    }

    let store = AdminNodeStore::new(pool.clone());
    let row = store
        .occupancy_for_nodes(
            std::slice::from_ref(&node),
            Some(LONG_RUNNER_AGE),
            CENSUS_AWARE,
        )
        .await?
        .get(&node)
        .copied()
        .expect("both rows occupy the node");
    let live = row.live.expect("a threshold was supplied");

    assert_eq!(
        row.occupancy.active, 2,
        "both rows still occupy placement and block :terminate"
    );
    assert_eq!(
        live.live_residents, 0,
        "neither lifecycle is one the drain pipeline can evacuate"
    );
    assert_eq!(
        live.live_long_runners, 0,
        "a non-resident row cannot be a long-runner however old it is"
    );
    Ok(())
}

/// Heartbeat freshness is reported from the same statement as the counts, for
/// every requested node — including one holding nothing at all. The ranking's
/// first term reads it, so deriving it from a separate query would let a node
/// that flipped reachable between the two be ordered by one snapshot's
/// visibility and another's occupancy.
#[tokio::test(flavor = "current_thread")]
async fn heartbeat_freshness_is_reported_per_node_alongside_the_counts() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let fresh = insert_node(pool, Heartbeat::Fresh).await?;
    let expired = insert_node(pool, Heartbeat::Expired).await?;
    let unreachable = insert_node(pool, Heartbeat::Unreachable).await?;
    let no_snapshot = insert_node(pool, Heartbeat::Absent).await?;

    let store = AdminNodeStore::new(pool.clone());
    let rows = store
        .occupancy_for_nodes(
            &[fresh, expired, unreachable, no_snapshot],
            Some(LONG_RUNNER_AGE),
            CENSUS_AWARE,
        )
        .await?;

    for (node, expected, label) in [
        (fresh, true, "reachable with an unexpired heartbeat"),
        (expired, false, "heartbeat expired"),
        (unreachable, false, "last poll found it unreachable"),
        (no_snapshot, false, "no status row at all"),
    ] {
        let row = rows
            .get(&node)
            .copied()
            .expect("every requested node is reported, occupied or not");
        assert_eq!(row.heartbeat_fresh, expected, "{label}");
        assert_eq!(row.occupancy.active, 0, "{label}: holds nothing");
    }
    Ok(())
}

/// Without a threshold the counters are absent, but heartbeat freshness is
/// still reported — it is not part of the opt-in cost.
#[tokio::test(flavor = "current_thread")]
async fn heartbeat_freshness_is_reported_without_the_drainability_counters() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool, Heartbeat::Fresh).await?;

    let store = AdminNodeStore::new(pool.clone());
    let row = store
        .occupancy_for_nodes(std::slice::from_ref(&node), None, CENSUS_AWARE)
        .await?
        .get(&node)
        .copied()
        .expect("every requested node is reported");

    assert!(row.heartbeat_fresh);
    assert!(
        row.live.is_none(),
        "the counters — including the drain-gate blockers, which cost a per-node \
         scan — stay opt-in"
    );
    Ok(())
}

/// The case the residency counts cannot see: a user restore in flight sits in
/// `restoring` with `target_node_id` on this node and `active_node_id` cleared.
/// The reconciler's gate counts it and refuses `draining → drained`, so a
/// ranking that reported zero everywhere would offer this node as the best
/// candidate for a drain that cannot finish.
#[tokio::test(flavor = "current_thread")]
async fn target_pinned_restore_blocks_the_gate_while_residency_counts_read_zero() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool, Heartbeat::Fresh).await?;

    // `active_node_id` NULL, `target_node_id` = node: the shape a suspended
    // sandbox takes while a user restore is in flight.
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             target_node_id, lifecycle_changed_at, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'restoring', 'unplaced', 'k8s',
             $2, NOW(), 1
         )"
    ))
    .bind(Uuid::now_v7())
    .bind(node)
    .execute(pool)
    .await?;

    let store = AdminNodeStore::new(pool.clone());
    let row = store
        .occupancy_for_nodes(
            std::slice::from_ref(&node),
            Some(LONG_RUNNER_AGE),
            CENSUS_AWARE,
        )
        .await?
        .get(&node)
        .copied()
        .expect("every requested node is reported");
    let live = row.live.expect("a threshold was supplied");

    assert_eq!(
        row.occupancy.active, 0,
        "no active placement: the row pins through target_node_id"
    );
    assert_eq!(live.live_residents, 0, "and so no live resident either");
    assert_eq!(
        live.drain_blockers, 1,
        "but the drain gate counts it, so the ranking must too"
    );
    Ok(())
}

/// An evacuation restore is excluded from the gate — it reads from the durable
/// backup, so counting it would re-couple the source drain to restore
/// completion. The ranking inherits that exclusion by sharing the predicate.
#[tokio::test(flavor = "current_thread")]
async fn evacuation_marked_restore_does_not_block_the_gate() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool, Heartbeat::Fresh).await?;

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
    // The marker is the restore's target lifecycle, not a node id.
    .bind("started")
    .execute(pool)
    .await?;

    let store = AdminNodeStore::new(pool.clone());
    let row = store
        .occupancy_for_nodes(
            std::slice::from_ref(&node),
            Some(LONG_RUNNER_AGE),
            CENSUS_AWARE,
        )
        .await?
        .get(&node)
        .copied()
        .expect("every requested node is reported");

    assert_eq!(
        row.live.expect("a threshold was supplied").drain_blockers,
        0
    );
    Ok(())
}

/// The `drain_blockers` this endpoint reports must equal the reconciler's own
/// single-node predicate, per node. The shared fragments' two forms are already
/// compared against each other in `sandbox_portability`'s differential test;
/// what this adds is the composition in between — the `blockers` CTE's grouping,
/// its `$1::UUID[]` scope, and the join back onto `scope` — every one of which
/// can misattribute a count while both fragments stay correct.
///
/// The awkward case is a sandbox whose in-flight upload source *is* its target:
/// it satisfies both batched branches, and the single-node form counts it once.
/// A `UNION ALL` in the batched form would double it, and only a differential
/// check catches that.
#[tokio::test(flavor = "current_thread")]
async fn batched_blocker_count_matches_the_single_node_predicate_per_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let target = insert_node(pool, Heartbeat::Fresh).await?;
    let other_source = insert_node(pool, Heartbeat::Fresh).await?;
    let quiet = insert_node(pool, Heartbeat::Fresh).await?;

    // Target-pinned mid-flight states.
    for lifecycle in ["suspending", "restoring"] {
        insert_target_pinned(pool, target, lifecycle, None).await?;
    }
    // Excluded: an evacuation restore reads from the durable backup.
    insert_target_pinned(pool, target, "restoring", Some("started")).await?;
    // Target-pinned suspended with no durable snapshot.
    insert_target_pinned(pool, target, "suspended", None).await?;

    // Upload in flight from the *target* itself: satisfies both batched
    // branches, and must still count once.
    let same_node = insert_target_pinned(pool, target, "suspended", None).await?;
    insert_creating_snapshot(pool, same_node, target).await?;

    // Upload in flight from a node that is not the sandbox's target: only the
    // snapshot-driven branch can see this one.
    let drifted = insert_target_pinned(pool, target, "suspended", None).await?;
    insert_creating_snapshot(pool, drifted, other_source).await?;

    let store = AdminNodeStore::new(pool.clone());
    let batched = store
        .occupancy_for_nodes(
            &[target, other_source, quiet],
            Some(LONG_RUNNER_AGE),
            CENSUS_AWARE,
        )
        .await?;

    for node in [target, other_source, quiet] {
        let single: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(format!(
            "SELECT COUNT(*)::BIGINT FROM sandboxes s WHERE {}",
            pplx_space_sandbox_portability::drain_gate_blocks_source_release("s", "$1")
        ))))
        .bind(node)
        .fetch_one(pool)
        .await?;
        let from_listing = batched
            .get(&node)
            .copied()
            .expect("every requested node is reported")
            .live
            .expect("a threshold was supplied")
            .drain_blockers;

        assert_eq!(
            from_listing, single,
            "batched and single-node blocker counts disagree for {node}"
        );
    }

    // Guard the guard: the fixture has to actually exercise the branches, or the
    // comparison above passes on two zeros.
    let target_count = batched
        .get(&target)
        .copied()
        .expect("target is reported")
        .live
        .expect("threshold supplied")
        .drain_blockers;
    assert_eq!(
        target_count, 5,
        "suspending + restoring + 3 suspended-not-durable, with the \
         evacuation-marked restore excluded and the same-node upload counted once"
    );
    assert_eq!(
        batched
            .get(&other_source)
            .copied()
            .expect("source is reported")
            .live
            .expect("threshold supplied")
            .drain_blockers,
        1,
        "the drifted upload attributes to its source node"
    );
    Ok(())
}

async fn insert_target_pinned(
    pool: &PgPool,
    target_node_id: NodeId,
    lifecycle: &str,
    evacuation_restore_target: Option<&str>,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             target_node_id, evacuation_restore_target, placement_changed_at,
             lifecycle_changed_at, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $3, 'unplaced', 'k8s',
             $2, $4, NOW() - INTERVAL '1 hour', NOW(), 1
         )"
    ))
    .bind(sandbox_id)
    .bind(target_node_id)
    .bind(lifecycle)
    .bind(evacuation_restore_target)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

async fn insert_creating_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'creating', $3, NOW())"
    ))
    .bind(Uuid::now_v7())
    .bind(sandbox_id)
    .bind(source_node_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// A row placed moments ago is legitimately missing from the newest census —
/// the node publishes its complete inventory on a separate, slower cycle than
/// the status heartbeat — so its omission proves nothing and it keeps counting.
/// The ranking inherits that window from the shared absence proof rather than
/// reasoning about publish lag itself; this pins the inheritance, so a future
/// change to the settle window shows up here.
#[tokio::test(flavor = "current_thread")]
async fn unsettled_placement_is_never_proven_absent() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool, Heartbeat::Fresh).await?;

    // Placed just now, absent from the census, on a freshly-heartbeating node:
    // every other condition for the discount holds.
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, lifecycle_changed_at, placement_changed_at,
             network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'started', 'placed', 'k8s',
             $2, NOW(), NOW(), 1
         )"
    ))
    .bind(Uuid::now_v7())
    .bind(node)
    .execute(pool)
    .await?;

    assert_eq!(
        live(pool, node).await?.live_residents,
        1,
        "a just-placed row the census has not caught up to must keep counting"
    );
    Ok(())
}

/// The distinction the final ranking term turns on. Both nodes hold occupancy
/// the drain pipeline ignores, but only one of them will actually block
/// `drained → terminating`: the terminator applies the census discount, so
/// phantom rows drop out while a census-reported `stopped` row does not. Ranking
/// on the raw occupancy count would order these two backwards.
#[tokio::test(flavor = "current_thread")]
async fn terminate_blockers_drop_phantoms_and_keep_reported_non_evacuable_rows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let phantoms = insert_node(pool, Heartbeat::Fresh).await?;
    let stopped = insert_node(pool, Heartbeat::Fresh).await?;

    // Three occupying rows, none reported by the node's census.
    for _ in 0..3 {
        insert_sandbox(pool, phantoms, "started", Duration::from_secs(60)).await?;
    }
    // One occupying row the census does report, in a lifecycle the drain
    // pipeline cannot evacuate.
    let reported = insert_sandbox(pool, stopped, "stopped", Duration::from_secs(60)).await?;
    observe_on_node(pool, reported, stopped, "stopped").await?;

    let store = AdminNodeStore::new(pool.clone());
    let rows = store
        .occupancy_for_nodes(&[phantoms, stopped], Some(LONG_RUNNER_AGE), CENSUS_AWARE)
        .await?;

    let phantom_row = rows.get(&phantoms).copied().expect("reported");
    let stopped_row = rows.get(&stopped).copied().expect("reported");

    assert_eq!(phantom_row.occupancy.active, 3, "raw occupancy counts them");
    assert_eq!(
        phantom_row
            .live
            .expect("threshold supplied")
            .terminate_blockers,
        0,
        "but the terminator discounts every one, so the node terminates cleanly"
    );

    assert_eq!(stopped_row.occupancy.active, 1);
    assert_eq!(
        stopped_row
            .live
            .expect("threshold supplied")
            .terminate_blockers,
        1,
        "a census-reported stopped row survives the discount and parks the node"
    );
    assert_eq!(
        stopped_row.live.expect("threshold supplied").live_residents,
        0,
        "while the drain pipeline still ignores it"
    );
    Ok(())
}

/// The figure that *refuses* equals a figure the listing *publishes*.
///
/// `:terminate`'s in-transaction guard counts `active` through
/// `active_resident_predicate`, whose census term is the same
/// `occupies_node_predicate` the listing evaluates as its `present` flag. So the
/// guard's `active` must equal the listing's `terminate_blockers` for the same
/// node under the same mode — not its `active`, which stays raw so an operator
/// can still see the discounted rows.
///
/// This is the equality the drain/terminator gate agreement is about, one layer
/// out from the reconcilers.
/// Before the guard read the census, the phantom node below answered 3 here
/// while the listing reported 0 `terminate_blockers`: `space-rollout-mng.py`
/// POSTs `:terminate` for every `drained` source node, so those three phantom
/// rows refused an automated termination forever on a node the drain gate had
/// already, correctly, released. Asserting against `terminate_blockers` rather
/// than a hardcoded 0/1 is deliberate — it fails if either side moves.
#[tokio::test(flavor = "current_thread")]
async fn the_guard_refuses_exactly_what_the_listing_calls_terminate_blockers() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let phantoms = insert_node(pool, Heartbeat::Fresh).await?;
    let stopped = insert_node(pool, Heartbeat::Fresh).await?;

    for _ in 0..3 {
        insert_sandbox(pool, phantoms, "started", Duration::from_secs(60)).await?;
    }
    let reported = insert_sandbox(pool, stopped, "stopped", Duration::from_secs(60)).await?;
    observe_on_node(pool, reported, stopped, "stopped").await?;

    let store = AdminNodeStore::new(pool.clone());
    let rows = store
        .occupancy_for_nodes(&[phantoms, stopped], Some(LONG_RUNNER_AGE), CENSUS_AWARE)
        .await?;

    for (node, label) in [(phantoms, "phantom-only"), (stopped, "census-reported")] {
        let listed = rows.get(&node).copied().expect("reported");
        let predicted = listed.live.expect("threshold supplied").terminate_blockers;

        let mut tx = pool.begin().await?;
        let guard = super::sandbox_occupancy_for_node_in_tx(&mut tx, node, CENSUS_AWARE).await?;
        tx.commit().await?;

        assert_eq!(
            guard.active, predicted,
            "{label}: the guard refuses on {} while the listing predicts {predicted}",
            guard.active
        );
    }

    // And the two figures genuinely differ on the phantom node, so the equality
    // above is not passing because all three counts coincide. Pinning only the
    // raw count admitted `(guard, predicted, raw) == (3, 3, 3)`: rendering
    // `occupies_node_predicate` as a literal `TRUE` drives the guard's census
    // term and the listing's `present` flag — one shared function — to 3
    // together, so the equality holds and the raw count is untouched. Pinning
    // `predicted` is what excludes that.
    let phantom = rows.get(&phantoms).copied().expect("reported");
    let phantom_predicted = phantom.live.expect("threshold supplied").terminate_blockers;
    assert_eq!(
        phantom_predicted, 0,
        "the discount must drop every phantom, or the equality proves nothing"
    );
    assert_eq!(
        phantom.occupancy.active, 3,
        "raw occupancy still shows the phantoms the guard now discounts"
    );
    Ok(())
}

/// The mode every cluster runs as shipped. `ResidencyEvidence::LifecycleOnly`
/// is the default because a fresh heartbeat does not prove the census is
/// current, so discounting on census absence can end a drain early and
/// terminate a live VM.
///
/// Under it the ranking must count every occupying row — phantom or not —
/// because that is what the drain and terminator gates do in that mode. A
/// ranking that discounted unconditionally would report this node as empty
/// while its drain waits on all three rows.
#[tokio::test(flavor = "current_thread")]
async fn lifecycle_only_counts_every_occupying_row() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool, Heartbeat::Fresh).await?;

    // Three settled, unreported rows: census-aware would discount all three.
    for _ in 0..3 {
        insert_sandbox(pool, node, "started", Duration::from_secs(48 * 60 * 60)).await?;
    }

    let store = AdminNodeStore::new(pool.clone());
    let default_mode = store
        .occupancy_for_nodes(
            std::slice::from_ref(&node),
            Some(LONG_RUNNER_AGE),
            ResidencyEvidence::default(),
        )
        .await?
        .get(&node)
        .copied()
        .expect("every requested node is reported");
    let live = default_mode.live.expect("a threshold was supplied");

    assert_eq!(
        ResidencyEvidence::default(),
        ResidencyEvidence::LifecycleOnly,
        "if this flips, revisit the default counts below"
    );
    assert_eq!(live.live_residents, 3, "no census discount in this mode");
    assert_eq!(live.live_long_runners, 3);
    assert_eq!(live.terminate_blockers, 3);
    assert_eq!(live.residency, ResidencyEvidence::LifecycleOnly);

    // Same fixture, census-aware: now all three are proven absent.
    let census_aware = live_at(pool, node, LONG_RUNNER_AGE).await?;
    assert_eq!(
        census_aware.live_residents, 0,
        "the discount is what the mode buys, and it is opt-in"
    );
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
/// no `EXPLAIN` runs anywhere in this suite, so the per-aggregate re-expansion
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
}

/// A node holding **no sandbox row at all** is the best gentle-drain candidate
/// there is, and the ranking has to be able to see it as one. The statement
/// drives off the requested-node scope precisely so such a node is reported
/// with zeroed counts and its *real* heartbeat, instead of being absent.
///
/// Absence would not read as "zero" at the call site: the handler falls back to
/// `unwrap_or_default()`, whose `heartbeat_fresh: false` is the fail-closed
/// blind reading, and blind sorts last. So an empty, reachable node would rank
/// below a node with fifty live residents — an inversion of the whole ordering,
/// produced by the one case the ranking most needs to get right.
#[tokio::test(flavor = "current_thread")]
async fn a_node_with_no_sandboxes_is_reported_with_its_real_heartbeat() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let reachable = insert_node(pool, Heartbeat::Fresh).await?;
    let blind = insert_node(pool, Heartbeat::Expired).await?;

    let store = AdminNodeStore::new(pool.clone());
    let counts = store
        .occupancy_for_nodes(&[reachable, blind], Some(LONG_RUNNER_AGE), CENSUS_AWARE)
        .await?;

    for node in [reachable, blind] {
        let row = counts
            .get(&node)
            .copied()
            .expect("every requested node is reported, sandboxes or not");
        assert_eq!(row.occupancy.active, 0);
        assert_eq!(row.occupancy.local_snapshots, 0);
        let live = row.live.expect("a threshold was supplied");
        assert_eq!(live.live_residents, 0);
        assert_eq!(live.live_long_runners, 0);
        assert_eq!(live.drain_blockers, 0);
        assert_eq!(live.terminate_blockers, 0);
    }

    // The heartbeat is the point: it is read per requested node in the same
    // scope, so it survives having nothing to aggregate.
    assert!(
        counts[&reachable].heartbeat_fresh,
        "an empty node that is reachable must not be reported blind — blind \
         sorts last, and this node is the best candidate"
    );
    assert!(
        !counts[&blind].heartbeat_fresh,
        "an expired heartbeat is still blind when there is nothing on the node"
    );
    Ok(())
}

/// The threshold the counts were computed against travels back with them, so a
/// caller can tell a clamped or defaulted age from the one it asked for.
///
/// The clamp is the case that needs a live statement rather than arithmetic: an
/// unclamped `u32::MAX` hours reaches `make_interval` as more microseconds than
/// an `int64` holds, which is a Postgres `interval out of range` error rather
/// than a large threshold. Asserting the echoed value proves the clamp both
/// applied *and* was reported.
#[tokio::test(flavor = "current_thread")]
async fn the_effective_long_runner_age_is_reported_back_and_clamped() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool, Heartbeat::Fresh).await?;
    let store = AdminNodeStore::new(pool.clone());

    let requested = Duration::from_secs(7 * 24 * 60 * 60);
    let live = store
        .occupancy_for_nodes(std::slice::from_ref(&node), Some(requested), CENSUS_AWARE)
        .await?
        .get(&node)
        .copied()
        .expect("every requested node is reported")
        .live
        .expect("a threshold was supplied");
    assert_eq!(
        live.long_runner_age, requested,
        "an in-range threshold is echoed unchanged"
    );

    // A hostile threshold: the statement has to execute, and the age it reports
    // has to be the clamped one rather than what was asked for.
    let absurd = Duration::from_secs(u64::from(u32::MAX) * 3600);
    let live = store
        .occupancy_for_nodes(std::slice::from_ref(&node), Some(absurd), CENSUS_AWARE)
        .await?
        .get(&node)
        .copied()
        .expect("every requested node is reported")
        .live
        .expect("a threshold was supplied");
    assert_eq!(
        live.long_runner_age,
        super::MAX_LONG_RUNNER_AGE,
        "an out-of-range threshold is reported at the ceiling it was clamped to \
         — asserting merely `< requested` would pass for any clamp at all"
    );
    assert_eq!(live.live_long_runners, 0, "no row is that old");
    Ok(())
}

/// `local_snapshots` counts *sandboxes*, so one sandbox on one node counts once
/// however many of the statement's three branches describe it.
///
/// The case that broke it: a `suspended` sandbox with a `current_local_artifact_id`
/// set, targeted at node N, with an in-flight `creating` snapshot from N that
/// names a **different** artifact. The suspended branch's exclusion asks the
/// artifact-aware shared fragment, which says no (the artifacts differ), so it
/// keeps the row; the in-flight branch inlines only the legacy freshness gate,
/// which does not look at artifacts, so it counts the row too. Row-counting
/// therefore reported 2 for one sandbox — and disagreed with the in-transaction
/// `:terminate` guard, whose per-sandbox `EXISTS` always counted it once.
///
/// `inflight_upload_from_the_target_node_counts_once_not_twice` covers the
/// artifact-*matched* collision and passes on the exclusion alone; it cannot
/// reach this one, because its fixture leaves the artifact columns NULL.
#[tokio::test(flavor = "current_thread")]
async fn an_artifact_mismatched_inflight_upload_counts_the_sandbox_once() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool, Heartbeat::Fresh).await?;
    let sandbox = Uuid::now_v7();

    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             target_node_id, current_local_artifact_id, current_local_artifact_kind,
             current_local_artifact_origin, placement_changed_at,
             lifecycle_changed_at, network_policy_generation
         ) VALUES (
             $1, '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'suspended', 'unplaced', 'k8s', $2, $3, 'full', 'suspend',
             NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours', 1
         )"
    ))
    .bind(sandbox)
    .bind(node)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    // Names a different artifact than the sandbox's current one, which is what
    // makes the artifact-aware exclusion decline to suppress the row.
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, source_node_id,
             local_artifact_id, artifact_kind, artifact_origin, timestamp
         ) VALUES ($1, $2, 'full', 'creating', $3, $4, 'full', 'suspend', NOW())"
    ))
    .bind(Uuid::now_v7())
    .bind(sandbox)
    .bind(node)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;

    let store = AdminNodeStore::new(pool.clone());
    let row = store
        .occupancy_for_nodes(
            std::slice::from_ref(&node),
            Some(LONG_RUNNER_AGE),
            CENSUS_AWARE,
        )
        .await?
        .get(&node)
        .copied()
        .expect("every requested node is reported");
    assert_eq!(
        row.occupancy.local_snapshots, 1,
        "one sandbox pinned to one node counts once, whatever its artifact ids say"
    );
    // The half of the claim the docstring rests on: the listing disagreeing with
    // the guard is *why* the double count mattered, so assert the guard too
    // rather than arguing from it.
    assert_eq!(
        in_tx_local_snapshots(pool, node).await?,
        1,
        "the in-transaction :terminate guard must agree with the listing"
    );
    Ok(())
}
