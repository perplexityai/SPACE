//! Postgres differential check between the two drain-gate shapes.
//!
//! [`super::drain_gate_blocks_source_release`] is where the gate's meaning is
//! written down. Nothing executes it: it gives the planner no indexable driver
//! over `sandboxes`, so both gates run [`super::drain_gate_blockers_for_node`]
//! over [`super::drain_gate_blocker_pairs`] instead. That makes this file the
//! only thing keeping the executed forms honest to the definition, which is why
//! the agreement is asserted against a live database rather than argued from
//! the decomposition.
//!
//! Every conjunct that can change the *answer* has to be load-bearing under
//! this fixture, or the comparison passes on a predicate that isn't being read.
//! Deleting any one of `UNION`-not-`UNION ALL`, the `driver` alias,
//! `AND NOT {durable}`, or the artifact pin must fail this test — which is what
//! the durable rows and the artifact-pinned row are here for.
//!
//! Two conjuncts are deliberately **not** covered, because no fixture can cover
//! them: branch 2's `driver.type = 'full'` and `driver.state = 'creating'` are
//! planner pre-filters, and the inner in-flight fragment re-checks both itself.
//! They can only narrow the candidate nodes to ones the inner fragment could
//! already be true for, so removing them cannot change the result for any data.
//! Their job is to give the planner an indexable driver, not to decide anything.

use pplx_space_common_core::types::{NodeId, SandboxId, SnapshotId};

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, insert_ready_worker_node, try_pg_test_db,
};
use sqlx::PgPool;
use std::collections::HashMap;
use uuid::Uuid;

use super::{
    drain_gate_blocker_pairs, drain_gate_blockers_for_node, drain_gate_blocks_source_release,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The batched blocker count and the reconciler's single-node predicate must
/// agree per node. They are two SQL shapes for one question, so this compares
/// them directly over a fixture that exercises every branch.
///
/// Two cases carry the check, and each pins a decomposition mistake that reads
/// as correct SQL:
///
/// * A sandbox whose in-flight upload source *is* its target satisfies both
///   batched branches, and the single-node form counts it once. `UNION ALL`
///   would double it.
/// * A sandbox holding a stale `creating` remnant on one node while a fresh
///   upload runs from another is the only shape that separates a per-driver
///   node comparison from a shadowed one. The shared in-flight fragment names
///   its own subquery `sn`, so driving the batched branch off `snapshots sn`
///   rather than `snapshots driver` collapses that comparison to a tautology —
///   after which the fragment answers "this sandbox has *some* fresh upload"
///   and attributes the stale node's remnant as if it were live.
#[tokio::test(flavor = "current_thread")]
async fn batched_blocker_count_matches_the_single_node_predicate_per_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let target = insert_node(pool).await?;
    let other_source = insert_node(pool).await?;
    let stale_source = insert_node(pool).await?;
    let durable_source = insert_node(pool).await?;
    let quiet = insert_node(pool).await?;

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
    // snapshot-driven branch can attribute this one to `other_source`.
    let drifted = insert_target_pinned(pool, target, "suspended", None).await?;
    insert_creating_snapshot(pool, drifted, other_source).await?;

    // A live upload from `other_source` alongside a prior-cycle `creating`
    // remnant on `stale_source`. The remnant is a driver row — the branch
    // pre-filters on state, not freshness — so only a per-driver node
    // comparison keeps `stale_source` out of the result.
    let two_sourced = insert_target_pinned(pool, target, "suspended", None).await?;
    insert_creating_snapshot(pool, two_sourced, other_source).await?;
    insert_stale_creating_snapshot(pool, two_sourced, stale_source).await?;

    // Durable, and therefore released, despite a lingering `creating` row from
    // `durable_source`. This is the only shape that makes the `NOT durable`
    // conjunct load-bearing: without it the batched form would emit
    // `(durable_source, this)` and report a node that is free to terminate as
    // permanently blocked.
    let durable = insert_target_pinned(pool, target, "suspended", None).await?;
    insert_ready_snapshot(pool, durable, target, None).await?;
    insert_creating_snapshot(pool, durable, durable_source).await?;

    // Artifact-pinned, with a ready snapshot for a *different* artifact. The two
    // arms of `artifact_identity_or_legacy` disagree about this row — the exact
    // arm requires the snapshot's `local_artifact_id` to match the sandbox's
    // current one and so calls it not-durable, while the legacy arm would see a
    // fresh `ready` snapshot and release it. Pinning the artifact is therefore
    // load-bearing in the count, not just present in the fixture: drop the
    // `UPDATE` and this row stops blocking.
    let durable_pinned = insert_target_pinned(pool, target, "suspended", None).await?;
    set_current_artifact(pool, durable_pinned, Uuid::now_v7()).await?;
    insert_ready_snapshot(pool, durable_pinned, target, Some(Uuid::now_v7())).await?;

    // Evacuation-marked: `snapshot_durable_for_source_release` switches to the
    // newest-snapshot-ready arm for these, so the marker branch gets compared
    // rather than assumed.
    let marked_durable = insert_target_pinned(pool, target, "suspended", Some("started")).await?;
    insert_ready_snapshot(pool, marked_durable, target, None).await?;

    let nodes = [target, other_source, stale_source, durable_source, quiet];
    let batched = batched_blocker_counts(pool, &nodes).await?;

    for node in nodes {
        let single = single_node_blocker_count(pool, node).await?;
        assert_eq!(
            batched.get(&node).copied().unwrap_or(0),
            single,
            "batched and single-node blocker counts disagree for {node}"
        );
        // The per-node projection of the batched form is what both gates
        // actually run — the node-drain blocker count and all four of the
        // drained-node terminator's statements — so it is compared here too
        // rather than trusted to follow from the grouped form above.
        assert_eq!(
            single_node_projection_count(pool, node).await?,
            single,
            "the per-node blockers projection disagrees with the single-node \
             predicate for {node}"
        );
    }

    // Guard the guard: the fixture has to actually exercise the branches, or the
    // comparison above passes on five zeros.
    assert_eq!(
        batched.get(&target).copied().unwrap_or(0),
        7,
        "suspending + restoring + 4 suspended-not-durable + the artifact-pinned \
         row whose ready snapshot is for a different artifact, with the \
         evacuation-marked restore excluded, the same-node upload counted once, \
         and the two genuinely-durable rows released"
    );
    assert_eq!(
        batched.get(&other_source).copied().unwrap_or(0),
        2,
        "both drifted uploads attribute to the node they run from"
    );
    assert_eq!(
        batched.get(&stale_source).copied().unwrap_or(0),
        0,
        "this sandbox is on the legacy arm, where a `creating` row older than \
         `placement_changed_at` is a prior-cycle remnant rather than a live upload"
    );
    assert_eq!(
        batched.get(&durable_source).copied().unwrap_or(0),
        0,
        "a durably-uploaded sandbox releases every node, including one still \
         holding a `creating` row for it"
    );
    assert_eq!(
        batched.get(&quiet).copied().unwrap_or(0),
        0,
        "a node with no sandboxes holds no gate open"
    );
    Ok(())
}

/// Distinct blocking sandboxes per node, from the batched form.
async fn batched_blocker_counts(
    pool: &PgPool,
    nodes: &[NodeId],
) -> TestResult<HashMap<NodeId, i64>> {
    let pairs = drain_gate_blocker_pairs("$1::UUID[]");
    let statement = format!(
        "SELECT node_id, COUNT(*)::BIGINT AS blockers
           FROM ({pairs}) pairs
          GROUP BY node_id"
    );
    let rows: Vec<(NodeId, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(sql!(statement.as_str())))
        .bind(nodes)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().collect())
}

/// Blocking sandboxes for one node, counted through
/// [`super::drain_gate_blockers_for_node`] — the form the node-drain gate and
/// the drained-node terminator both execute.
async fn single_node_projection_count(pool: &PgPool, node: NodeId) -> TestResult<i64> {
    let blockers = drain_gate_blockers_for_node("$1");
    let statement = format!("SELECT COUNT(*)::BIGINT FROM ({blockers}) node_blockers");
    let count = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(statement.as_str())))
        .bind(node)
        .fetch_one(pool)
        .await?;
    Ok(count)
}

/// Blocking sandboxes for one node, from the predicate the reconciler runs.
async fn single_node_blocker_count(pool: &PgPool, node: NodeId) -> TestResult<i64> {
    let blocks = drain_gate_blocks_source_release("s", "$1");
    let statement = format!("SELECT COUNT(*)::BIGINT FROM sandboxes s WHERE {blocks}");
    let count = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(statement.as_str())))
        .bind(node)
        .fetch_one(pool)
        .await?;
    Ok(count)
}

async fn insert_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;
    Ok(node_id)
}

/// `placement_changed_at` is backdated so a `creating` snapshot stamped `NOW()`
/// reads as belonging to the current placement cycle — the freshness gate the
/// in-flight fragment applies. `evacuation_restore_target` carries the
/// restore's target lifecycle, not a node id.
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
             $1, $2, $3, $4, 'unplaced', 'k8s',
             $5, $6, NOW() - INTERVAL '1 hour', NOW(), 1
         )"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(lifecycle)
    .bind(target_node_id)
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
    insert_creating_snapshot_at(pool, sandbox_id, source_node_id, "NOW()").await
}

/// A `creating` row stamped before the sandbox's `placement_changed_at`, i.e. a
/// remnant of a prior placement cycle rather than a live upload.
async fn insert_stale_creating_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
) -> TestResult {
    insert_creating_snapshot_at(
        pool,
        sandbox_id,
        source_node_id,
        "NOW() - INTERVAL '2 hours'",
    )
    .await
}

async fn insert_creating_snapshot_at(
    pool: &PgPool,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    timestamp_expr: &str,
) -> TestResult {
    let statement = format!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'creating', $3, {timestamp_expr})"
    );
    sqlx::query(sqlx::AssertSqlSafe(sql!(statement.as_str())))
        .bind(Uuid::now_v7())
        .bind(sandbox_id)
        .bind(source_node_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Pin the sandbox's current local artifact, so every
/// `artifact_identity_or_legacy` composition takes its **exact** arm rather
/// than the legacy one. All three columns move together or the
/// `sandboxes_current_local_artifact_identity_check` constraint rejects the row.
async fn set_current_artifact(
    pool: &PgPool,
    sandbox_id: SandboxId,
    artifact_id: Uuid,
) -> TestResult {
    let result = sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact_id)
    .execute(pool)
    .await?;
    // A helper that silently matched nothing would move the row back onto the
    // legacy arm and make an assertion pass for the wrong reason.
    assert_eq!(result.rows_affected(), 1, "artifact pin matched no row");
    Ok(())
}

/// A durably-uploaded snapshot: `state = 'ready'` **and** a storage-location
/// row. Both arms of `snapshot_durable_for_source_release` require the join, so
/// without the location row the snapshot is not durable however ready it looks.
///
/// `artifact` opts the row into the exact artifact-identity arm; pass `None` to
/// leave it on the legacy arm. The three artifact columns move together or
/// `snapshots_artifact_identity_check` rejects the row, so kind and origin are
/// derived from `artifact` rather than set unconditionally.
async fn insert_ready_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    artifact: Option<Uuid>,
) -> TestResult {
    let snapshot_id = SnapshotId::new();
    let artifact_kind = artifact.map(|_| "full");
    let artifact_origin = artifact.map(|_| "suspend");
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, source_node_id, timestamp,
             local_artifact_id, artifact_kind, artifact_origin
         )
         VALUES ($1, $2, 'full', 'ready', $3, NOW(), $4, $5, $6)"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(source_node_id)
    .bind(artifact)
    .bind(artifact_kind)
    .bind(artifact_origin)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (id, snapshot_id, storage_connection_string)
         VALUES ($1, $2, 's3://space-test/durable')"
    ))
    .bind(Uuid::now_v7())
    .bind(snapshot_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// The bulk-join shape must retain the source-release contract for both
/// artifact-pinned rows and nullable rollout-skew rows.
#[tokio::test(flavor = "current_thread")]
async fn source_release_join_preserves_snapshot_selection() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool).await?;
    let artifact = Uuid::now_v7();
    let mut sandboxes = Vec::new();
    for pin in [None, Some(artifact), Some(Uuid::now_v7())] {
        for marker in [None, Some("started")] {
            let sandbox = insert_target_pinned(pool, node, "suspended", marker).await?;
            if let Some(pin) = pin {
                set_current_artifact(pool, sandbox, pin).await?;
            }
            insert_ready_snapshot(pool, sandbox, node, Some(artifact)).await?;
            sandboxes.push(sandbox);
        }
    }
    assert_source_release(pool, &sandboxes, &[true, true, true, true, false, false]).await?;

    // A newer unfinished upload blocks legacy evacuation, but not a backup of
    // the exact pinned artifact; ordinary legacy suspend still uses freshness.
    for sandbox in &sandboxes {
        insert_creating_snapshot_at(pool, *sandbox, node, "NOW() + INTERVAL '1 hour'").await?;
    }
    assert_source_release(pool, &sandboxes, &[true, false, true, true, false, false]).await?;

    sqlx::query(sql!(
        "UPDATE snapshots SET timestamp = NOW() - INTERVAL '2 hours' WHERE state = 'ready'"
    ))
    .execute(pool)
    .await?;
    assert_source_release(pool, &sandboxes, &[false, false, true, true, false, false]).await?;

    sqlx::query(sql!("UPDATE sandboxes SET placement_changed_at = NULL"))
        .execute(pool)
        .await?;
    assert_source_release(pool, &sandboxes, &[true, false, true, true, false, false]).await?;

    sqlx::query(sql!(
        "UPDATE snapshots SET state = 'failed' WHERE state = 'ready'"
    ))
    .execute(pool)
    .await?;
    assert_source_release(pool, &sandboxes, &[false; 6]).await?;
    sqlx::query(sql!(
        "UPDATE snapshots SET state = 'ready' WHERE state = 'failed'"
    ))
    .execute(pool)
    .await?;

    sqlx::query(sql!(
        "UPDATE snapshot_storage_locations SET orphaned_at = NOW()"
    ))
    .execute(pool)
    .await?;
    assert_source_release(pool, &sandboxes, &[false; 6]).await?;
    sqlx::query(sql!("DELETE FROM snapshot_storage_locations"))
        .execute(pool)
        .await?;
    assert_source_release(pool, &sandboxes, &[false; 6]).await?;
    Ok(())
}

async fn assert_source_release(
    pool: &PgPool,
    sandboxes: &[SandboxId],
    expected: &[bool],
) -> TestResult {
    let bulk = super::snapshot_durable_for_source_release("s");
    let latest = super::latest_full_snapshot_ready("s");
    let fresh = super::fresh_ready_snapshot_exists("s");
    // The pre-optimization spelling is intentionally independent of the new
    // combined EXISTS, so a misplaced branch condition cannot pass both sides.
    let query = format!(
        "SELECT s.id, {bulk},
                ((s.evacuation_restore_target IS NOT NULL AND {latest})
                 OR (s.evacuation_restore_target IS NULL AND {fresh}))
           FROM sandboxes s WHERE s.id = ANY($1)"
    );
    let rows: Vec<(SandboxId, bool, bool)> =
        sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(sandboxes)
            .fetch_all(pool)
            .await?;
    assert_eq!(rows.len(), sandboxes.len());
    let expected: HashMap<_, _> = sandboxes
        .iter()
        .copied()
        .zip(expected.iter().copied())
        .collect();
    for (sandbox, bulk, previous) in rows {
        assert_eq!(
            bulk, previous,
            "source-release semantics changed for {sandbox}"
        );
        assert_eq!(
            bulk, expected[&sandbox],
            "fixture did not exercise {sandbox}"
        );
    }
    Ok(())
}
