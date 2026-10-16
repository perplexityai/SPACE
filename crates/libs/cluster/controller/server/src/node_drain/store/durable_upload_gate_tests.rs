use pplx_space_common_core::types::{LocalArtifactId, NodeId, SandboxId, SnapshotId};
use uuid::Uuid;

use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_sandbox_portability::fresh_ready_snapshot_exists;

use super::*;

use pplx_space_test_env::TestResult;

/// Insert a sandbox owned by the seeded default user/org, in `lifecycle`,
/// pinned to `target_node_id`, with `placement_changed_at = NOW() - 1h`
/// (so the freshness gate is exercised).
async fn insert_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
    lifecycle: &str,
    target_node_id: Option<NodeId>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             target_node_id, placement_changed_at, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $2, 'unplaced', 'k8s',
             $3, NOW() - INTERVAL '1 hour', 1
         )"
    ))
    .bind(sandbox_id)
    .bind(lifecycle)
    .bind(target_node_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Insert a `type='full'` snapshot in `state` (`'creating'` for an
/// in-flight upload, `'ready'` for a finalized one), with `source_node_id`
/// and a `timestamp` of NOW(). Ready snapshots also get the
/// `snapshot_storage_locations` row the freshness predicate requires.
async fn insert_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    state: &str,
    source_node_id: NodeId,
) -> TestResult {
    let snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', $3, $4, NOW())"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(state)
    .bind(source_node_id)
    .execute(pool)
    .await?;
    if state == "ready" {
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
             VALUES ($1, $2)"
        ))
        .bind(snapshot_id)
        .bind(format!("s3://test-bucket/{snapshot_id}"))
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Insert a `state='creating'` full snapshot whose `timestamp` is
/// `age_secs` in the past — used to model an old-but-still-uploading row
/// (the gate is state-based, so age must not change its protection).
async fn insert_aged_creating_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    age_secs: i64,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'creating', $3, NOW() - make_interval(secs => $4))"
    ))
    .bind(Uuid::now_v7())
    .bind(sandbox_id)
    .bind(source_node_id)
    .bind(age_secs)
    .execute(pool)
    .await?;
    Ok(())
}

async fn pinned_local_only(pool: &PgPool, node_id: NodeId) -> TestResult<Vec<SandboxId>> {
    Ok(sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(sql!(
        pinned_local_only_sandboxes_query(false, PINNED_LOCAL_ONLY_ENUMERATION_CAP)
    )))
    .bind(node_id)
    .fetch_all(pool)
    .await?)
}

async fn drain_blocking_count(pool: &PgPool, node_id: NodeId) -> TestResult<i64> {
    Ok(
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql!(drain_blocking_sandboxes_query())))
            .bind(node_id)
            .fetch_one(pool)
            .await?,
    )
}

async fn lifecycle_of(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<String> {
    Ok(
        sqlx::query_scalar::<_, String>(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

/// Run the reupload give-up CAS ([`adopt_remote_snapshot_for_drain_query`])
/// and return the number of rows flipped.
async fn adopt_remote_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    settle_secs: i64,
) -> TestResult<u64> {
    Ok(sqlx::query(sqlx::AssertSqlSafe(sql!(
        adopt_remote_snapshot_for_drain_query()
    )))
    .bind(sandbox_id)
    .bind(source_node_id)
    .bind(settle_secs)
    .execute(pool)
    .await?
    .rows_affected())
}

/// Read the columns the give-up clears: `(evacuation_restore_target,
/// current_local_artifact_id, placement_changed_at IS NULL)`.
async fn give_up_columns(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<(Option<String>, Option<Uuid>, bool)> {
    let row: (Option<String>, Option<Uuid>, bool) = sqlx::query_as(sql!(
        "SELECT evacuation_restore_target,
                current_local_artifact_id,
                placement_changed_at IS NULL
           FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// Insert a `state='ready'` `type='full'` snapshot whose `timestamp` is
/// `age_secs` in the past (plus the `snapshot_storage_locations` row the
/// durability predicates require). `insert_sandbox` anchors
/// `placement_changed_at` at `NOW() - 1h`, so a ready snapshot older than
/// that reads placement-stale: the row stays pinned despite a durable copy.
/// The give-up's placement reset must unblock it.
async fn insert_ready_snapshot_aged(
    pool: &PgPool,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    age_secs: i64,
) -> TestResult {
    let snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'ready', $3, NOW() - make_interval(secs => $4))"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(source_node_id)
    .bind(age_secs)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(snapshot_id)
    .bind(format!("s3://test-bucket/{snapshot_id}"))
    .execute(pool)
    .await?;
    Ok(())
}

/// Backdate `lifecycle_changed_at` so the row is past the give-up settle
/// window. The dedicated UPDATE sticks because the
/// `sandboxes_lifecycle_changed_at` trigger only rewrites the column when
/// `lifecycle` changes, which this leaves untouched.
async fn age_lifecycle(pool: &PgPool, sandbox_id: SandboxId, age_secs: i64) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle_changed_at = NOW() - make_interval(secs => $2)
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(age_secs)
    .execute(pool)
    .await?;
    Ok(())
}

/// Bump `placement_changed_at` to the future (after every snapshot's
/// timestamp), modelling the stuck restore that placed a pod and advanced
/// the freshness anchor past the (otherwise-current) ready snapshot — so the
/// row reads placement-stale and stays pinned despite a durable copy. This
/// is the wedge the give-up's placement reset clears.
async fn bump_placement_to_future(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes SET placement_changed_at = NOW() + INTERVAL '1 hour' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// The decisive reproduction: a suspended sandbox whose durable
/// upload is still in flight from this node must be **excluded** from the
/// promote/reupload work queue (otherwise the reupload probe reads pruned
/// on-disk artifacts, returns `ArtifactMissing`, and the sandbox is marked
/// `failed` while the upload is about to finalize). A genuinely local-only
/// suspended sandbox with no in-flight upload must still appear.
#[tokio::test(flavor = "current_thread")]
async fn pinned_local_only_excludes_inflight_upload_but_keeps_genuine_local_only() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    // Upload in flight from this node — must NOT be promoted.
    let uploading = SandboxId::new();
    insert_sandbox(pool, uploading, "suspended", Some(node)).await?;
    insert_snapshot(pool, uploading, "creating", node).await?;

    // Genuinely local-only (no snapshot row at all) — must be promoted.
    let local_only = SandboxId::new();
    insert_sandbox(pool, local_only, "suspended", Some(node)).await?;

    let pinned = pinned_local_only(pool, node).await?;
    assert!(
        !pinned.contains(&uploading),
        "sandbox with in-flight upload must be excluded from promote queue; got {pinned:?}"
    );
    assert!(
        pinned.contains(&local_only),
        "genuinely local-only sandbox must still be promoted; got {pinned:?}"
    );
    Ok(())
}

/// An in-flight upload from this node blocks `draining → drained` even
/// when the sandbox row's `target_node_id` has drifted to another node
/// (the upload worker still runs on, and the snapshot still lives on,
/// *this* node). Pre-fix the count keyed only on `target_node_id` and
/// returned 0 here.
#[tokio::test(flavor = "current_thread")]
async fn drain_blocking_counts_inflight_upload_even_when_target_drifted() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let source = NodeId::new();
    let other = NodeId::new();
    let sandbox = SandboxId::new();
    insert_sandbox(pool, sandbox, "suspended", Some(other)).await?;
    insert_snapshot(pool, sandbox, "creating", source).await?;

    assert_eq!(
        drain_blocking_count(pool, source).await?,
        1,
        "in-flight upload from the source node must block its drain"
    );
    Ok(())
}

/// A sandbox that is already portable (a fresh `ready` snapshot) must NOT
/// keep blocking just because an earlier `creating` row from a prior /
/// parallel attempt lingers — the reupload path allocates a fresh snapshot
/// row, so a stuck `creating` alongside a finalized `ready` is realistic.
#[tokio::test(flavor = "current_thread")]
async fn portable_sandbox_does_not_block_despite_leftover_creating_row() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    insert_sandbox(pool, sandbox, "suspended", Some(node)).await?;
    // Earlier attempt left a `creating` row; a later attempt finalized
    // `ready` (durable) — the sandbox is portable.
    insert_snapshot(pool, sandbox, "creating", node).await?;
    insert_snapshot(pool, sandbox, "ready", node).await?;

    assert_eq!(
        drain_blocking_count(pool, node).await?,
        0,
        "a portable sandbox must not block on a lingering creating row"
    );
    // And it is not in the promote queue (already portable).
    assert!(
        !pinned_local_only(pool, node).await?.contains(&sandbox),
        "portable sandbox must not be in the promote queue"
    );
    Ok(())
}

/// `mark_sandbox_lost` must be a no-op while the upload is in flight (the
/// sandbox is recoverable — the worker is about to finalize `ready`), but
/// must still fire for a genuinely lost sandbox (no snapshot anywhere).
#[tokio::test(flavor = "current_thread")]
async fn mark_sandbox_lost_waits_for_inflight_upload() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    // Upload in flight: mark-lost must NOT flip it.
    let uploading = SandboxId::new();
    insert_sandbox(pool, uploading, "suspended", Some(node)).await?;
    insert_snapshot(pool, uploading, "creating", node).await?;
    let affected = sqlx::query(sqlx::AssertSqlSafe(sql!(mark_sandbox_lost_query())))
        .bind(uploading)
        .bind(node)
        .execute(pool)
        .await?
        .rows_affected();
    assert_eq!(
        affected, 0,
        "must not mark failed while upload is in flight"
    );
    assert_eq!(lifecycle_of(pool, uploading).await?, "suspended");

    // Drift: target_node_id = node (so the mark-lost CAS would match on
    // target), but the upload is in flight from a DIFFERENT source node.
    // Marking the sandbox failed is global, so it must still be refused —
    // the node-agnostic guard catches the cross-node upload that a
    // `source_node_id = $2` check would miss.
    let drifted = SandboxId::new();
    let other_source = NodeId::new();
    insert_sandbox(pool, drifted, "suspended", Some(node)).await?;
    insert_snapshot(pool, drifted, "creating", other_source).await?;
    let affected = sqlx::query(sqlx::AssertSqlSafe(sql!(mark_sandbox_lost_query())))
        .bind(drifted)
        .bind(node)
        .execute(pool)
        .await?
        .rows_affected();
    assert_eq!(
        affected, 0,
        "must not mark failed while upload is in flight from another node"
    );
    assert_eq!(lifecycle_of(pool, drifted).await?, "suspended");

    // Genuinely lost (no snapshot): mark-lost must fire.
    let lost = SandboxId::new();
    insert_sandbox(pool, lost, "suspended", Some(node)).await?;
    let affected = sqlx::query(sqlx::AssertSqlSafe(sql!(mark_sandbox_lost_query())))
        .bind(lost)
        .bind(node)
        .execute(pool)
        .await?
        .rows_affected();
    assert_eq!(affected, 1, "genuinely lost sandbox must be marked failed");
    assert_eq!(lifecycle_of(pool, lost).await?, "failed");
    Ok(())
}

/// The give-up path is gated on snapshot state + freshness, not absolute
/// age. A **fresh** `creating` row (the current upload) protects the
/// sandbox; a **stale** prior-cycle `creating` remnant (timestamp before
/// the sandbox's last placement) is ignored and released; a `failed`
/// snapshot (worker gave up) is released. `insert_sandbox` sets
/// `placement_changed_at = NOW() - 1h`, so a NOW() creating row is fresh
/// and a 2h-old one is stale.
#[tokio::test(flavor = "current_thread")]
async fn give_up_path_gated_on_state_and_freshness() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    // Fresh current upload → protected.
    let fresh = SandboxId::new();
    insert_sandbox(pool, fresh, "suspended", Some(node)).await?;
    insert_snapshot(pool, fresh, "creating", node).await?;

    // Stale prior-cycle remnant (2h old < placement_changed_at 1h) → released.
    let stale = SandboxId::new();
    insert_sandbox(pool, stale, "suspended", Some(node)).await?;
    insert_aged_creating_snapshot(pool, stale, node, 2 * 3600).await?;

    // Worker gave up → snapshot `failed` → released.
    let failed = SandboxId::new();
    insert_sandbox(pool, failed, "suspended", Some(node)).await?;
    insert_snapshot(pool, failed, "failed", node).await?;

    let pinned = pinned_local_only(pool, node).await?;
    assert!(
        !pinned.contains(&fresh),
        "a fresh current upload must stay protected; got {pinned:?}"
    );
    assert!(
        pinned.contains(&stale),
        "a stale prior-cycle remnant must be released; got {pinned:?}"
    );
    assert!(
        pinned.contains(&failed),
        "a failed-snapshot sandbox must be released; got {pinned:?}"
    );

    // mark-lost refuses the fresh upload, fires for the stale + failed ones.
    let mark = |id| async move {
        sqlx::query(sqlx::AssertSqlSafe(sql!(mark_sandbox_lost_query())))
            .bind(id)
            .bind(node)
            .execute(pool)
            .await
            .map(|r| r.rows_affected())
    };
    assert_eq!(mark(fresh).await?, 0, "must not mark a fresh upload lost");
    assert_eq!(lifecycle_of(pool, fresh).await?, "suspended");
    assert_eq!(mark(stale).await?, 1, "stale remnant must be markable lost");
    assert_eq!(lifecycle_of(pool, stale).await?, "failed");
    assert_eq!(
        mark(failed).await?,
        1,
        "failed-snapshot must be markable lost"
    );
    assert_eq!(lifecycle_of(pool, failed).await?, "failed");
    Ok(())
}

/// A `suspended` sandbox whose on-node reupload is
/// permanently refused (stuck `restoring`). Its newest full snapshot is
/// `ready` (the current canonical capture), but a stuck restore bumped
/// `placement_changed_at` past the snapshot's timestamp, so the
/// placement-freshness gate reads it stale and the row stays pinned +
/// blocking despite the durable copy. Once past the settle window, the
/// give-up adopts the newest ready snapshot — resetting the placement anchor
/// so it reads fresh again (and clearing the marker/pointer, no-ops for an
/// unmarked legacy row). The row drops out of both the pinned-local queue
/// and the drain blocker count and stays `suspended` (recoverable) — never
/// marked `failed`.
#[tokio::test(flavor = "current_thread")]
async fn adopt_remote_snapshot_unblocks_drain_when_ready_snapshot_exists() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    // Legacy nullable row (the reupload path only fires for these):
    // suspended, pinned here, with its newest full snapshot `ready` but made
    // placement-stale by a stuck restore that bumped `placement_changed_at`
    // into the future. `lifecycle_changed_at` is aged independently (the
    // give-up keys adoption on the newest snapshot, NOT on
    // `lifecycle_changed_at` — which an evacuation advances past the
    // snapshot — so the two timestamps are unrelated here on purpose).
    let sandbox = SandboxId::new();
    insert_sandbox(pool, sandbox, "suspended", Some(node)).await?;
    insert_snapshot(pool, sandbox, "ready", node).await?;
    bump_placement_to_future(pool, sandbox).await?;
    // Past the settle window.
    age_lifecycle(pool, sandbox, 3600).await?;

    // Pre-state: newest snapshot ready but placement-stale → pinned +
    // blocking despite the durable copy.
    assert!(
        pinned_local_only(pool, node).await?.contains(&sandbox),
        "a placement-stale ready snapshot must leave the row pinned (the wedge)"
    );
    assert_eq!(drain_blocking_count(pool, node).await?, 1);

    // Give-up fires: one row flipped.
    assert_eq!(adopt_remote_snapshot(pool, sandbox, node, 60).await?, 1);

    // Post-state: marker + artifact pointer + placement anchor cleared.
    let (marker, artifact, placement_null) = give_up_columns(pool, sandbox).await?;
    assert_eq!(marker, None, "evacuation marker must be cleared");
    assert_eq!(artifact, None, "current artifact pointer must be cleared");
    assert!(placement_null, "placement_changed_at must be reset to NULL");

    // The row is now durable from the ready snapshot: out of the pinned
    // queue and no longer blocking, but still suspended (recoverable).
    assert!(!pinned_local_only(pool, node).await?.contains(&sandbox));
    assert_eq!(drain_blocking_count(pool, node).await?, 0);
    assert_eq!(lifecycle_of(pool, sandbox).await?, "suspended");
    Ok(())
}

/// The give-up is gated: it must NOT fire while still inside the settle
/// window (a quick transient), when no ready remote snapshot exists (the
/// sandbox would be lost), while a suspend upload is in flight from any node
/// (it may finalize on its own), or when the row drifted off the node. Each
/// no-op leaves the row exactly as-is.
#[tokio::test(flavor = "current_thread")]
async fn adopt_remote_snapshot_respects_settle_ready_inflight_and_drift_gates() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    let other_node = NodeId::new();

    // (a) Ready snapshot and otherwise eligible, but kept inside the settle
    //     window → no-op.
    let settling = SandboxId::new();
    insert_sandbox(pool, settling, "suspended", Some(node)).await?;
    insert_snapshot(pool, settling, "ready", node).await?;
    age_lifecycle(pool, settling, 30).await?; // 30s < 60s settle
    assert_eq!(
        adopt_remote_snapshot(pool, settling, node, 60).await?,
        0,
        "must not give up inside the settle window"
    );

    // (b) Past settle, but NO ready remote snapshot (only a `creating`
    //     one) → no-op (giving up would lose the sandbox).
    let no_ready = SandboxId::new();
    insert_sandbox(pool, no_ready, "suspended", Some(node)).await?;
    insert_snapshot(pool, no_ready, "creating", node).await?; // not ready
    age_lifecycle(pool, no_ready, 3600).await?;
    assert_eq!(
        adopt_remote_snapshot(pool, no_ready, node, 60).await?,
        0,
        "must not give up without a ready remote snapshot"
    );

    // (c) Ready snapshot, past settle, but a suspend upload is in flight
    //     (a fresh `creating` row alongside the ready one) → no-op; the
    //     live upload may make the current artifact durable on its own.
    //     Placement is left at `insert_sandbox`'s `NOW() - 1h` so the
    //     `creating` row reads fresh and counts as in-flight.
    let inflight = SandboxId::new();
    insert_sandbox(pool, inflight, "suspended", Some(node)).await?;
    insert_snapshot(pool, inflight, "ready", node).await?;
    insert_snapshot(pool, inflight, "creating", node).await?;
    age_lifecycle(pool, inflight, 3600).await?;
    assert_eq!(
        adopt_remote_snapshot(pool, inflight, node, 60).await?,
        0,
        "must not give up while a suspend upload is in flight"
    );

    // (d) Eligible row, but called for the wrong source node (drifted) →
    //     no-op (the CAS is scoped to `target_node_id`).
    let drifted = SandboxId::new();
    insert_sandbox(pool, drifted, "suspended", Some(node)).await?;
    insert_snapshot(pool, drifted, "ready", node).await?;
    age_lifecycle(pool, drifted, 3600).await?;
    assert_eq!(
        adopt_remote_snapshot(pool, drifted, other_node, 60).await?,
        0,
        "must not give up for a node the row is not pinned to"
    );
    // It still fires for the correct node.
    assert_eq!(adopt_remote_snapshot(pool, drifted, node, 60).await?, 1);
    Ok(())
}

/// Data-loss guard: the give-up adopts only when the **newest** full
/// snapshot is ready, never a stale prior-cycle one. A
/// `wake → modify → re-suspend` whose new upload **failed** leaves an old,
/// pre-modification ready snapshot plus a newer `failed` snapshot. The
/// newest full snapshot is then the failed one, so `latest_full_snapshot_ready`
/// is false and the give-up must NOT fire — adopting the old ready snapshot
/// (and clearing the placement anchor) would let the drain proceed and a
/// later restore load pre-modification bytes. The row stays pinned
/// (visible) instead, columns untouched.
#[tokio::test(flavor = "current_thread")]
async fn adopt_remote_snapshot_refuses_when_newest_snapshot_unready() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    // Unmarked suspended row pinned here: an old (2h) ready snapshot from a
    // prior cycle, plus a newer `failed` snapshot from a re-suspend whose
    // upload failed. `insert_sandbox` anchors `placement_changed_at` at
    // NOW()-1h, so the 2h-old ready snapshot also reads placement-stale →
    // pinned. The newest full snapshot is the failed one.
    let sandbox = SandboxId::new();
    insert_sandbox(pool, sandbox, "suspended", Some(node)).await?;
    insert_ready_snapshot_aged(pool, sandbox, node, 7200).await?;
    insert_snapshot(pool, sandbox, "failed", node).await?;
    age_lifecycle(pool, sandbox, 3600).await?;

    assert!(
        pinned_local_only(pool, node).await?.contains(&sandbox),
        "a row whose newest snapshot is unready (only a stale ready one) must stay pinned"
    );

    // Give-up must NOT fire — the newest full snapshot is `failed`, so
    // adopting the stale ready one would resurrect pre-modification bytes.
    assert_eq!(
        adopt_remote_snapshot(pool, sandbox, node, 60).await?,
        0,
        "must not adopt when the newest snapshot is unready (would cause stale-restore data loss)"
    );

    // The row is untouched: still pinned, placement anchor intact.
    let (_, _, placement_null) = give_up_columns(pool, sandbox).await?;
    assert!(
        !placement_null,
        "placement_changed_at must be left intact when the give-up is refused"
    );
    assert!(pinned_local_only(pool, node).await?.contains(&sandbox));
    Ok(())
}

async fn set_evacuation_marker(pool: &PgPool, sandbox_id: SandboxId, target: &str) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(target)
    .execute(pool)
    .await?;
    Ok(())
}

async fn set_current_local_artifact(
    pool: &PgPool,
    sandbox_id: SandboxId,
    local_artifact_id: LocalArtifactId,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(local_artifact_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn restore_pending(pool: &PgPool) -> TestResult<Vec<(SandboxId, String)>> {
    restore_pending_with(pool, true).await
}

async fn restore_pending_with(
    pool: &PgPool,
    honor_backoff_windows: bool,
) -> TestResult<Vec<(SandboxId, String)>> {
    Ok(
        sqlx::query_as::<_, (SandboxId, String)>(sqlx::AssertSqlSafe(sql!(
            restore_pending_evacuations_query(honor_backoff_windows),
        )))
        .fetch_all(pool)
        .await?,
    )
}

async fn redrive_stranded(pool: &PgPool) -> TestResult<Vec<SandboxId>> {
    Ok(sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(sql!(
        redrive_stranded_suspending_query()
    )))
    .fetch_all(pool)
    .await?)
}

/// Set `active_node_id` on a row to model a suspend that has NOT yet run
/// `finish_suspended_execution` (which clears it). `insert_sandbox` leaves
/// `active_node_id` NULL, modeling the post-finish (redrivable) state.
async fn set_active_node(pool: &PgPool, sandbox_id: SandboxId, node: NodeId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node)
    .execute(pool)
    .await?;
    Ok(())
}

/// Rollout-skew rows without local artifact identity retain the legacy
/// recovery rule: only a durable ready snapshot permits advancing a marked
/// post-retirement `suspending` row.
#[tokio::test(flavor = "current_thread")]
async fn redrive_legacy_rows_require_ready_snapshot() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    // Crash-stranded evacuation: suspending, marked, snapshot finalized
    // ready, suspend cleanup done (active_node_id NULL) → advanced.
    let stranded_ready = SandboxId::new();
    insert_sandbox(pool, stranded_ready, "suspending", Some(node)).await?;
    insert_snapshot(pool, stranded_ready, "ready", node).await?;
    set_evacuation_marker(pool, stranded_ready, "started").await?;

    // Evacuation suspend still uploading: suspending, marked, snapshot
    // creating → nothing to restore yet, must stay suspending.
    let still_uploading = SandboxId::new();
    insert_sandbox(pool, still_uploading, "suspending", Some(node)).await?;
    insert_snapshot(pool, still_uploading, "creating", node).await?;
    set_evacuation_marker(pool, still_uploading, "paused").await?;

    // Plain user-suspend stranded in suspending (NULL marker), even with a
    // ready snapshot → not the re-drive's job; left for the sweeper.
    let user_suspending = SandboxId::new();
    insert_sandbox(pool, user_suspending, "suspending", Some(node)).await?;
    insert_snapshot(pool, user_suspending, "ready", node).await?;

    // Crash BEFORE `finish_suspended_execution`: ready snapshot + marker, but
    // `active_node_id` is still set (execution not yet retired, source pod may
    // exist). Advancing would race the cleanup, so the re-drive must skip it.
    let finish_not_done = SandboxId::new();
    insert_sandbox(pool, finish_not_done, "suspending", Some(node)).await?;
    insert_snapshot(pool, finish_not_done, "ready", node).await?;
    set_evacuation_marker(pool, finish_not_done, "started").await?;
    set_active_node(pool, finish_not_done, node).await?;

    // Already-suspended marked row with a ready snapshot → not matched (the
    // UPDATE only touches `suspending`); restore-pending owns it.
    let already_suspended = SandboxId::new();
    insert_sandbox(pool, already_suspended, "suspended", Some(node)).await?;
    insert_snapshot(pool, already_suspended, "ready", node).await?;
    set_evacuation_marker(pool, already_suspended, "started").await?;

    let advanced = redrive_stranded(pool).await?;
    assert_eq!(
        advanced,
        vec![stranded_ready],
        "only the marked, ready, cleaned-up, still-suspending row is re-driven; got {advanced:?}"
    );

    assert_eq!(lifecycle_of(pool, stranded_ready).await?, "suspended");
    assert_eq!(lifecycle_of(pool, still_uploading).await?, "suspending");
    assert_eq!(lifecycle_of(pool, user_suspending).await?, "suspending");
    assert_eq!(
        lifecycle_of(pool, finish_not_done).await?,
        "suspending",
        "a row whose suspend cleanup hasn't run (active_node_id set) must not be re-driven"
    );
    assert_eq!(lifecycle_of(pool, already_suspended).await?, "suspended");

    // The advanced row is now exactly what restore-pending selects, so the
    // crash-stranded sandbox becomes restorable in the same sweep.
    let pending = restore_pending(pool).await?;
    assert!(
        pending.contains(&(stranded_ready, "started".to_string())),
        "the re-driven row must be restore-pending; got {pending:?}"
    );
    Ok(())
}

/// Once local artifact publication and execution retirement are persisted,
/// a controller crash before backup enqueue must not strand evacuation in
/// `suspending`. Redrive makes the row promotable, but not portable:
/// cross-node restore remains blocked until a matching backup becomes ready.
#[tokio::test(flavor = "current_thread")]
async fn redrive_published_identity_before_backup_is_enqueued() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    let sandbox = SandboxId::new();

    insert_sandbox(pool, sandbox, "suspending", Some(node)).await?;
    set_evacuation_marker(pool, sandbox, "started").await?;
    set_current_local_artifact(pool, sandbox, LocalArtifactId::new()).await?;

    assert_eq!(redrive_stranded(pool).await?, vec![sandbox]);
    assert_eq!(lifecycle_of(pool, sandbox).await?, "suspended");
    assert!(
        restore_pending(pool).await?.is_empty(),
        "a local-only published artifact is not eligible for cross-node restore"
    );
    assert!(
        pinned_local_only(pool, node).await?.contains(&sandbox),
        "the re-driven row must be selected for exact-artifact backup"
    );
    Ok(())
}

/// A marked `suspending` row whose **newest** full snapshot is still
/// `creating` (a `wake → modify → evacuation-suspend` cycle left an older
/// `ready` snapshot behind) must NOT be re-driven — advancing it would let
/// the cohort restore load stale prior-cycle bytes. `latest_full_snapshot_ready`
/// waits for the current upload, exactly as the restore-pending scan does.
#[tokio::test(flavor = "current_thread")]
async fn redrive_waits_for_current_upload_over_stale_ready() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    let sandbox = SandboxId::new();
    insert_sandbox(pool, sandbox, "suspending", Some(node)).await?;
    set_evacuation_marker(pool, sandbox, "started").await?;

    // Older ready snapshot (prior cycle) ...
    let stale_ready = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'ready', $3, NOW() - make_interval(secs => 7200))"
    ))
    .bind(stale_ready)
    .bind(sandbox)
    .bind(node)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, 's3://test-bucket/stale-prior-cycle')"
    ))
    .bind(stale_ready)
    .execute(pool)
    .await?;
    // ... and the current upload still creating (newest).
    insert_snapshot(pool, sandbox, "creating", node).await?;

    assert!(
        redrive_stranded(pool).await?.is_empty(),
        "must wait for the current upload, not advance on a stale prior-cycle ready snapshot"
    );
    assert_eq!(lifecycle_of(pool, sandbox).await?, "suspending");
    Ok(())
}

/// The crash-before-`BeginArtifactBackup` corner: a controller crash after
/// the `suspending` claim but before this cycle's snapshot row is inserted
/// leaves a prior-cycle `ready` snapshot as the *newest* full snapshot. The
/// placement-agnostic `latest_full_snapshot_ready` would call that "ready"
/// and advance the row, restoring pre-wake bytes. The claim-anchored
/// `current_suspend_snapshot_ready` gate (snapshot `timestamp >=
/// lifecycle_changed_at`) must refuse to advance, since the only `ready`
/// snapshot predates this suspend cycle.
#[tokio::test(flavor = "current_thread")]
async fn redrive_skips_when_only_prior_cycle_ready_snapshot() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    let sandbox = SandboxId::new();
    // `insert_sandbox` sets lifecycle_changed_at to NOW(); the only snapshot
    // is a prior-cycle `ready` whose timestamp is two hours earlier.
    insert_sandbox(pool, sandbox, "suspending", Some(node)).await?;
    set_evacuation_marker(pool, sandbox, "started").await?;

    let prior_ready = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'ready', $3, NOW() - make_interval(secs => 7200))"
    ))
    .bind(prior_ready)
    .bind(sandbox)
    .bind(node)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, 's3://test-bucket/prior-cycle-only')"
    ))
    .bind(prior_ready)
    .execute(pool)
    .await?;

    // Sanity: the placement-agnostic newest-ready predicate WOULD advance it
    // (proving the data-loss bug the claim anchor fixes is real).
    let latest_gated = format!(
        "SELECT EXISTS(SELECT 1 FROM sandboxes s WHERE s.id = $1 AND {})",
        latest_full_snapshot_ready("s")
    );
    let latest_ready: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(latest_gated.as_str())))
        .bind(sandbox)
        .fetch_one(pool)
        .await?;
    assert!(
        latest_ready,
        "the prior-cycle ready snapshot IS the newest (the bug the anchor fixes)"
    );

    assert!(
        redrive_stranded(pool).await?.is_empty(),
        "must not advance on a prior-cycle ready snapshot that predates the suspending claim"
    );
    assert_eq!(lifecycle_of(pool, sandbox).await?, "suspending");
    Ok(())
}

/// The stranded-suspended reproduction at the query layer: the
/// cluster-wide restore-pending scan must surface **only** a sandbox that
/// (a) carries the `evacuation_restore_target` marker (it was evacuated,
/// not plain user-suspended) and (b) has a fresh `ready` snapshot (the
/// durable upload finalized, so the cross-node restore can proceed). A
/// plain user-suspended sandbox (NULL marker) must never appear; an
/// evacuation-marked sandbox whose upload is still `creating` must not
/// appear yet (nothing to restore); a terminal `failed` row with a stale
/// marker must not appear (the restore-pending scan requires `suspended`).
/// The scan is node-agnostic — the restore is decoupled from drain — and
/// the source-node drain blocks only on **uploads**, not on the marker.
#[tokio::test(flavor = "current_thread")]
async fn restore_pending_selects_only_evacuation_marked_ready_sandboxes() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    // Evacuated, snapshot ready → must be restored, target 'started'.
    let evac_ready = SandboxId::new();
    insert_sandbox(pool, evac_ready, "suspended", Some(node)).await?;
    insert_snapshot(pool, evac_ready, "ready", node).await?;
    set_evacuation_marker(pool, evac_ready, "started").await?;

    // Plain user-suspended (NULL marker), even with a ready snapshot →
    // must never be auto-restored.
    let user_suspended = SandboxId::new();
    insert_sandbox(pool, user_suspended, "suspended", Some(node)).await?;
    insert_snapshot(pool, user_suspended, "ready", node).await?;

    // Evacuated but upload still in flight → not restorable yet, but
    // must keep blocking drain via the marker.
    let evac_uploading = SandboxId::new();
    insert_sandbox(pool, evac_uploading, "suspended", Some(node)).await?;
    insert_snapshot(pool, evac_uploading, "creating", node).await?;
    set_evacuation_marker(pool, evac_uploading, "paused").await?;

    // A marked evacuation row that a backstop moved to a terminal
    // `failed` lifecycle WITHOUT clearing the marker — must not appear in
    // the restore-pending scan (it requires `suspended`).
    let evac_failed = SandboxId::new();
    insert_sandbox(pool, evac_failed, "failed", Some(node)).await?;
    insert_snapshot(pool, evac_failed, "ready", node).await?;
    set_evacuation_marker(pool, evac_failed, "started").await?;

    let pending = restore_pending(pool).await?;
    assert_eq!(
        pending,
        vec![(evac_ready, "started".to_string())],
        "only the evacuation-marked, snapshot-ready sandbox is restore-pending; got {pending:?}"
    );

    // Drain blocks only on uploads now (the cohort restore is decoupled):
    // only the still-`creating` `evac_uploading` blocks. The two portable
    // (`ready`) suspended sandboxes — marked `evac_ready` and unmarked
    // `user_suspended` — and the terminal `failed` row do NOT block: the
    // source node drains once its snapshots are uploaded, and the marker
    // no longer gates drain completion.
    assert_eq!(
        drain_blocking_count(pool, node).await?,
        1,
        "only the still-uploading sandbox blocks drain; the marker does not gate drain"
    );
    Ok(())
}

/// Regression (no permanent wedge): a marked sandbox whose **current**
/// evacuation upload `failed` must be picked up by the reupload queue so it
/// self-heals, even with an older placement-fresh `ready` snapshot present.
/// Its newest full snapshot is `failed`, so it is not restore-pending and
/// still blocks source release — but because the reupload queue is now
/// marker-aware (`snapshot_durable_for_source_release`, not `fresh_ready`),
/// it IS enumerated for reupload → a new `ready` snapshot → restore. The old
/// freshness-gated reupload queue would have skipped it (the older `ready`
/// is placement-fresh), wedging it: blocks drain, not restore-pending, not
/// reuploaded.
#[tokio::test(flavor = "current_thread")]
async fn failed_current_upload_is_reupload_eligible_not_wedged() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    let sandbox = SandboxId::new();
    // insert_sandbox sets placement_changed_at = NOW() - 1h.
    insert_sandbox(pool, sandbox, "suspended", Some(node)).await?;
    set_evacuation_marker(pool, sandbox, "started").await?;

    // Older `ready` snapshot that is placement-fresh (30m old > the 1h-old
    // placement) — this is what the old `fresh_ready` reupload gate keyed on
    // and would have used to (wrongly) skip the row.
    let stale_ready = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'ready', $3, NOW() - make_interval(secs => 1800))"
    ))
    .bind(stale_ready)
    .bind(sandbox)
    .bind(node)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, 's3://test-bucket/older-ready')"
    ))
    .bind(stale_ready)
    .execute(pool)
    .await?;

    // The current evacuation upload, newest full snapshot, FAILED.
    let failed = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'failed', $3, NOW())"
    ))
    .bind(failed)
    .bind(sandbox)
    .bind(node)
    .execute(pool)
    .await?;

    // Wedge components: not restore-pending (newest isn't ready), and it
    // still blocks source release.
    assert!(
        restore_pending(pool).await?.is_empty(),
        "a failed-newest sandbox must not be restore-pending"
    );
    assert_eq!(
        drain_blocking_count(pool, node).await?,
        1,
        "a failed-newest marked sandbox keeps blocking the source drain"
    );
    // The escape: the marker-aware reupload queue enumerates it, so the
    // controller retries the upload and the sandbox self-heals.
    assert!(
        pinned_local_only(pool, node).await?.contains(&sandbox),
        "a failed-newest marked sandbox must be reupload-eligible (no permanent wedge)"
    );
    Ok(())
}

/// Regression: a **failed** evacuation restore must not strand the
/// sandbox out of the restore-pending sweep. When the rebuilt pod is
/// placed, `persist_pod_node_placement` bumps `placement_changed_at` to
/// `NOW()` (past the snapshot's `timestamp`) and moves `target_node_id`
/// onto the cohort node; if the restore then fails the row is left
/// `restoring` with the marker still set. The old freshness-gated scan
/// (`fresh_ready_snapshot_exists`) classified the snapshot as stale and
/// dropped the row, so the sweep never retried it. `latest_full_snapshot_ready`
/// keys on the newest snapshot being ready (no placement reference), so it
/// keeps the row selectable and the next sweep re-drives.
#[tokio::test(flavor = "current_thread")]
async fn restore_pending_keeps_failed_restore_with_bumped_placement() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let cohort_node = NodeId::new();

    // Failed evacuation restore: pod was placed on the cohort node
    // (target moved, placement bumped), restore then failed → row left
    // `restoring`, marker still set, snapshot durable in S3.
    let failed_restore = SandboxId::new();
    insert_sandbox(pool, failed_restore, "restoring", Some(cohort_node)).await?;
    insert_snapshot(pool, failed_restore, "ready", cohort_node).await?;
    set_evacuation_marker(pool, failed_restore, "started").await?;
    // Bump placement_changed_at past the snapshot timestamp, exactly as a
    // rebuilt-pod placement would. With the freshness gate this hid the
    // snapshot; with ready-gating it stays restore-pending.
    sqlx::query(sql!(
        "UPDATE sandboxes SET placement_changed_at = NOW() + INTERVAL '1 hour' WHERE id = $1"
    ))
    .bind(failed_restore)
    .execute(pool)
    .await?;

    // Sanity: the freshness-gated predicate would indeed drop this row —
    // proving the bug the marker-gated predicate fixes is real, not
    // hypothetical.
    let fresh_gated = format!(
        "SELECT EXISTS(SELECT 1 FROM sandboxes s WHERE s.id = $1 AND {})",
        fresh_ready_snapshot_exists("s")
    );
    let still_fresh: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(fresh_gated.as_str())))
        .bind(failed_restore)
        .fetch_one(pool)
        .await?;
    assert!(
        !still_fresh,
        "the bumped placement must make the freshness gate drop this row (the bug)"
    );

    let pending = restore_pending(pool).await?;
    assert_eq!(
        pending,
        vec![(failed_restore, "started".to_string())],
        "a failed restore (marker set, placement bumped) must stay restore-pending; got {pending:?}"
    );
    Ok(())
}

/// Regression (data-loss): a marked sandbox with a **stale prior-cycle**
/// `ready` snapshot must NOT be restore-pending while the **current**
/// evacuation suspend upload is still `creating`. A `wake → modify →
/// evacuation-suspend` sandbox carries an old `ready` snapshot (the
/// previous cycle) alongside the fresh `creating` upload; a plain
/// "any ready snapshot exists" gate would select the old one and restore
/// stale bytes. `latest_full_snapshot_ready` (newest full snapshot is
/// ready) waits for the current upload to finalize first.
#[tokio::test(flavor = "current_thread")]
async fn restore_pending_waits_for_current_upload_over_stale_snapshot() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    let sandbox = SandboxId::new();
    insert_sandbox(pool, sandbox, "suspended", Some(node)).await?;
    set_evacuation_marker(pool, sandbox, "started").await?;

    // Stale prior-cycle ready snapshot (2h old) with a storage location.
    let stale_snapshot = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'ready', $3, NOW() - make_interval(secs => 7200))"
    ))
    .bind(stale_snapshot)
    .bind(sandbox)
    .bind(node)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, 's3://test-bucket/stale-prior-cycle')"
    ))
    .bind(stale_snapshot)
    .execute(pool)
    .await?;

    // Current evacuation-suspend upload still in flight (newer, creating).
    let current_snapshot = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'creating', $3, NOW())"
    ))
    .bind(current_snapshot)
    .bind(sandbox)
    .bind(node)
    .execute(pool)
    .await?;

    let pending = restore_pending(pool).await?;
    assert!(
        pending.is_empty(),
        "must wait for the current upload, not restore the stale snapshot; got {pending:?}"
    );

    // Current upload finalizes → newest full snapshot is now ready.
    sqlx::query(sql!("UPDATE snapshots SET state = 'ready' WHERE id = $1"))
        .bind(current_snapshot)
        .execute(pool)
        .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, 's3://test-bucket/current-suspend')"
    ))
    .bind(current_snapshot)
    .execute(pool)
    .await?;

    let pending = restore_pending(pool).await?;
    assert_eq!(
        pending,
        vec![(sandbox, "started".to_string())],
        "once the current upload is ready the sandbox is restore-pending; got {pending:?}"
    );
    Ok(())
}

/// Composed ordering matrix (attempt tiers × recency): the restore-pending
/// scan orders by ascending `restore_attempt_count` first, then
/// most-recently-active (`lifecycle_changed_at DESC`) within a tier, so
/// the sweep's bounded slot budget — filled in list order — goes to
/// never-failed sandboxes first, hottest leading. The key case is the
/// most-failed row also being the most-recently-active one: every failed
/// restore cycle bumps `lifecycle_changed_at` (the claim and the revert
/// both fire the lifecycle-changed trigger), so recency alone would
/// perpetually rank a serial failer "hottest" — attempt count as the
/// primary key is what keeps it behind every fresher tier despite that.
#[tokio::test(flavor = "current_thread")]
async fn restore_pending_orders_attempt_tiers_then_recency() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    // Five restore-eligible evacuations across three attempt tiers, with
    // recency deliberately cutting across the tiers. Insertion order is
    // not the expected output order, so a missing ORDER BY (which would
    // likely echo insertion order) fails the assert.
    //
    // tier 0 (never failed): hot (1m ago), cold (2h ago)
    let t0_cold = SandboxId::new();
    insert_sandbox(pool, t0_cold, "suspended", Some(node)).await?;
    insert_snapshot(pool, t0_cold, "ready", node).await?;
    set_evacuation_marker(pool, t0_cold, "started").await?;
    age_lifecycle(pool, t0_cold, 7200).await?;

    // tier 5 (serial failer, window expired): the most-recently-active
    // row of the whole set — modelling the failed-cycle lifecycle bumps —
    // yet it must sort dead last.
    let t5_hottest = SandboxId::new();
    insert_sandbox(pool, t5_hottest, "suspended", Some(node)).await?;
    insert_snapshot(pool, t5_hottest, "ready", node).await?;
    set_evacuation_marker(pool, t5_hottest, "started").await?;
    set_backoff_state(pool, t5_hottest, 5, Some(-10)).await?;
    age_lifecycle(pool, t5_hottest, 30).await?;

    let t0_hot = SandboxId::new();
    insert_sandbox(pool, t0_hot, "suspended", Some(node)).await?;
    insert_snapshot(pool, t0_hot, "ready", node).await?;
    set_evacuation_marker(pool, t0_hot, "paused").await?;
    age_lifecycle(pool, t0_hot, 60).await?;

    // tier 2 (mild failer, window expired): hot (5m ago), cold (3h ago).
    let t2_cold = SandboxId::new();
    insert_sandbox(pool, t2_cold, "suspended", Some(node)).await?;
    insert_snapshot(pool, t2_cold, "ready", node).await?;
    set_evacuation_marker(pool, t2_cold, "started").await?;
    set_backoff_state(pool, t2_cold, 2, Some(-10)).await?;
    age_lifecycle(pool, t2_cold, 10800).await?;

    let t2_hot = SandboxId::new();
    insert_sandbox(pool, t2_hot, "suspended", Some(node)).await?;
    insert_snapshot(pool, t2_hot, "ready", node).await?;
    set_evacuation_marker(pool, t2_hot, "started").await?;
    set_backoff_state(pool, t2_hot, 2, Some(-10)).await?;
    age_lifecycle(pool, t2_hot, 300).await?;

    let pending = restore_pending(pool).await?;
    assert_eq!(
        pending,
        vec![
            (t0_hot, "paused".to_string()),
            (t0_cold, "started".to_string()),
            (t2_hot, "started".to_string()),
            (t2_cold, "started".to_string()),
            (t5_hottest, "started".to_string()),
        ],
        "attempt tiers must outrank recency; recency orders within a tier; got {pending:?}"
    );
    Ok(())
}

/// Regression: a marked, suspended sandbox whose **current** evacuation
/// upload is still `creating` must keep blocking the source drain even when
/// an older (placement-fresh) `ready` snapshot exists. The source-release
/// guard is keyed on the same `latest_full_snapshot_ready` the cohort
/// restore waits on, so the node can't drain/terminate while the current
/// upload is in flight — which would kill the only in-flight copy and
/// strand the sandbox (the restore refuses the stale prior snapshot). The
/// old freshness-gated guard counted the stale `ready` as portable and let
/// the source drain (count 0); the marker-aware guard blocks (count 1).
#[tokio::test(flavor = "current_thread")]
async fn drain_blocks_marked_sandbox_until_current_upload_ready() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    let sandbox = SandboxId::new();
    // insert_sandbox sets placement_changed_at = NOW() - 1h.
    insert_sandbox(pool, sandbox, "suspended", Some(node)).await?;
    set_evacuation_marker(pool, sandbox, "started").await?;

    // Older ready snapshot that is *placement-fresh* (30m old > the 1h-old
    // placement) — the old freshness gate would treat this as portable.
    let stale_ready = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'ready', $3, NOW() - make_interval(secs => 1800))"
    ))
    .bind(stale_ready)
    .bind(sandbox)
    .bind(node)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, 's3://test-bucket/older-ready')"
    ))
    .bind(stale_ready)
    .execute(pool)
    .await?;

    // The current evacuation upload, still in flight (newest full snapshot).
    insert_snapshot(pool, sandbox, "creating", node).await?;

    assert_eq!(
        drain_blocking_count(pool, node).await?,
        1,
        "a marked sandbox must block drain while its current upload is creating, \
         even with an older fresh ready snapshot present"
    );

    // Finalize the current upload → newest full snapshot is ready → the
    // source is released (and the cohort restore can now proceed).
    sqlx::query(sql!(
        "UPDATE snapshots SET state = 'ready' WHERE sandbox_id = $1 AND state = 'creating'"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;
    let newest: Uuid = sqlx::query_scalar(sql!(
        "SELECT id FROM snapshots WHERE sandbox_id = $1 ORDER BY timestamp DESC LIMIT 1"
    ))
    .bind(sandbox)
    .fetch_one(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, 's3://test-bucket/current-upload')"
    ))
    .bind(newest)
    .execute(pool)
    .await?;
    assert_eq!(
        drain_blocking_count(pool, node).await?,
        0,
        "once the current upload finalizes, the source drains"
    );
    Ok(())
}

/// An in-flight **evacuation** restore must not re-couple the source
/// node's drain to restore completion. Between `begin_restore_lifecycle`
/// and the rebuilt pod's placement, the sandbox sits in
/// `lifecycle='restoring'` with `target_node_id` still equal to the
/// source. Because the snapshot is durable in S3 (the restore reads from
/// there), the source must NOT be blocked by it. A non-evacuation
/// `restoring` row (NULL marker) and a mid-`suspending` row still block.
#[tokio::test(flavor = "current_thread")]
async fn evacuation_restoring_does_not_recouple_source_drain() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    // Evacuation restore in flight (durable snapshot, marker set,
    // target still = source): must NOT block the source drain.
    let evac_restoring = SandboxId::new();
    insert_sandbox(pool, evac_restoring, "restoring", Some(node)).await?;
    insert_snapshot(pool, evac_restoring, "ready", node).await?;
    set_evacuation_marker(pool, evac_restoring, "started").await?;

    // Non-evacuation restore (NULL marker): still blocks (conservative —
    // it may be reading from the source's local disk).
    let user_restoring = SandboxId::new();
    insert_sandbox(pool, user_restoring, "restoring", Some(node)).await?;

    // Mid-suspend: always blocks (snapshot not durable yet).
    let suspending = SandboxId::new();
    insert_sandbox(pool, suspending, "suspending", Some(node)).await?;

    assert_eq!(
        drain_blocking_count(pool, node).await?,
        2,
        "evacuation restore (durable, marker set) must not block the source drain; \
         a non-evacuation restoring row and a suspending row do"
    );
    Ok(())
}

/// The DB trigger auto-clears `evacuation_restore_target` the moment a
/// sandbox leaves the suspend/restore window by ANY write path — so a
/// user Start/restore that takes over an evacuation-suspended sandbox
/// can't leave a stale marker that the sweep would later act on. The
/// marker is kept while still `suspending`/`restoring` (mid-evacuation).
#[tokio::test(flavor = "current_thread")]
async fn evacuation_marker_auto_cleared_when_leaving_suspend_window() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    let sandbox = SandboxId::new();
    insert_sandbox(pool, sandbox, "suspended", Some(node)).await?;
    set_evacuation_marker(pool, sandbox, "started").await?;

    let marker_of = |id: SandboxId| async move {
        sqlx::query_scalar::<_, Option<String>>(sql!(
            "SELECT evacuation_restore_target FROM sandboxes WHERE id = $1"
        ))
        .bind(id)
        .fetch_one(pool)
        .await
    };

    // suspended → restoring keeps the marker (still mid-evacuation).
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;
    assert_eq!(
        marker_of(sandbox).await?.as_deref(),
        Some("started"),
        "marker is kept while the sandbox is still restoring"
    );

    // restoring → started (a user Start, or the cohort-restore finalize)
    // clears the marker via the trigger, on any path.
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;
    assert_eq!(
        marker_of(sandbox).await?,
        None,
        "marker is auto-cleared when the sandbox leaves the suspend/restore window"
    );
    Ok(())
}

/// Insert a `drain_phase = 'draining'` k8s worker node.
async fn insert_draining_node(pool: &PgPool, node_id: NodeId) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_started_at)
         VALUES ($1, '10.0.0.50'::inet, 'worker', 'k8s', 'ready', 'draining', NOW())"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Pin a sandbox onto `node` as its active node, optionally set the
/// evacuation marker, and backdate `lifecycle_changed_at` by `age_secs`.
/// The dedicated `UPDATE` of `lifecycle_changed_at` sticks because the
/// `sandboxes_lifecycle_changed_at` trigger only rewrites the column when
/// `lifecycle` itself changes — which this UPDATE leaves untouched.
async fn pin_active_and_age(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node: NodeId,
    evacuation_marker: Option<&str>,
    age_secs: i64,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = $2,
                evacuation_restore_target = $3,
                lifecycle_changed_at = NOW() - make_interval(secs => $4)
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node)
    .bind(evacuation_marker)
    .bind(age_secs)
    .execute(pool)
    .await?;
    Ok(())
}

async fn abandon_stale(pool: &PgPool, node: NodeId, age_secs: i64) -> TestResult<Vec<SandboxId>> {
    let lifecycle_strs: Vec<&'static str> = NON_EVACUATABLE_RESIDENT_LIFECYCLES
        .iter()
        .map(|lc| lc.as_str())
        .collect();
    // The query returns `(id, pre_failure_lifecycle)`; the tests only
    // assert on the claimed ids.
    let query = abandon_stale_resident_sandboxes_query();
    let rows: Vec<(SandboxId, String)> = sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node)
        .bind(age_secs)
        .bind(&lifecycle_strs)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|(id, _)| id).collect())
}

/// Exercise the evacuation carve-outs on a node wedged in `draining` with
/// several residents. The stale-abandon sweep must flip
/// the genuinely-stuck ones to `failed` so the drain converges, while
/// leaving alone (a) the drain's OWN evacuation *restore* (marked
/// `restoring`, re-driven elsewhere) and (b) a recently-changed restore not
/// yet past the age threshold — but it MUST still abandon a wedged
/// evacuation *suspend* (marked `suspending`), which nothing else
/// recovers, AND a wedged restore pinned only by `target_node_id` (its
/// `active_node_id` cleared mid-restore) which the drain-complete gate
/// counts as a blocker. Also asserts the in-SQL draining guard: the sweep
/// is a no-op once the node leaves `draining`.
#[tokio::test(flavor = "current_thread")]
async fn abandon_sweep_flips_stale_nonevacuation_transient_only() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    insert_draining_node(pool, node).await?;

    // (A) wedged user restore — no evacuation marker, aged out → swept.
    let user_restore = SandboxId::new();
    insert_sandbox(pool, user_restore, "restoring", Some(node)).await?;
    pin_active_and_age(pool, user_restore, node, None, 3600).await?;

    // (B) the drain's own evacuation restore — marker set → spared.
    let evac_restore = SandboxId::new();
    insert_sandbox(pool, evac_restore, "restoring", Some(node)).await?;
    pin_active_and_age(pool, evac_restore, node, Some("started"), 3600).await?;

    // (C) freshly-changed user restore — under the age threshold → spared.
    let fresh_restore = SandboxId::new();
    insert_sandbox(pool, fresh_restore, "restoring", Some(node)).await?;
    pin_active_and_age(pool, fresh_restore, node, None, 0).await?;

    // (D) a wedged evacuation *suspend* — marker set but `suspending` has
    // no other recovery path, so it MUST still be swept.
    let evac_suspend = SandboxId::new();
    insert_sandbox(pool, evac_suspend, "suspending", Some(node)).await?;
    pin_active_and_age(pool, evac_suspend, node, Some("started"), 3600).await?;

    // (E) a wedged user restore that began before the drain: its
    // `active_node_id` was cleared, but `target_node_id` still points here
    // (insert_sandbox sets target_node_id). The drain-complete gate counts
    // it as a blocker via `target_node_id`, so the sweep must match it via
    // `target_node_id` too — an `active_node_id`-only sweep would wedge the
    // drain.
    let target_pinned_restore = SandboxId::new();
    insert_sandbox(pool, target_pinned_restore, "restoring", Some(node)).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle_changed_at = NOW() - make_interval(secs => 3600)
          WHERE id = $1"
    ))
    .bind(target_pinned_restore)
    .execute(pool)
    .await?;

    let mut claimed = abandon_stale(pool, node, 600).await?;
    claimed.sort();
    let mut expected = vec![user_restore, evac_suspend, target_pinned_restore];
    expected.sort();
    assert_eq!(
        claimed, expected,
        "the aged unmarked restore (active-pinned and target-only-pinned) and the wedged \
         marked suspend must be abandoned; the marked restore and the fresh restore must not"
    );
    assert_eq!(lifecycle_of(pool, user_restore).await?, "failed");
    assert_eq!(lifecycle_of(pool, evac_suspend).await?, "failed");
    assert_eq!(lifecycle_of(pool, target_pinned_restore).await?, "failed");
    assert_eq!(
        lifecycle_of(pool, evac_restore).await?,
        "restoring",
        "the drain's own evacuation restore (marker set) must never be abandoned"
    );
    assert_eq!(
        lifecycle_of(pool, fresh_restore).await?,
        "restoring",
        "a restore that hasn't aged past the threshold must be left to finish"
    );

    // Drift guard: once the node is no longer draining (admin `:undrain`
    // clears `drain_phase` to NULL), the sweep is a no-op even for an
    // aged, unmarked transient.
    let other = NodeId::new();
    insert_draining_node(pool, other).await?;
    sqlx::query(sql!("UPDATE nodes SET drain_phase = NULL WHERE id = $1"))
        .bind(other)
        .execute(pool)
        .await?;
    let stuck = SandboxId::new();
    insert_sandbox(pool, stuck, "starting", Some(other)).await?;
    pin_active_and_age(pool, stuck, other, None, 3600).await?;
    let claimed = abandon_stale(pool, other, 600).await?;
    assert!(
        claimed.is_empty(),
        "sweep must not fire on a node that is no longer draining"
    );
    assert_eq!(lifecycle_of(pool, stuck).await?, "starting");
    Ok(())
}

/// The per-node stale-abandon sweep must spare a marked `suspending` row
/// owned by crash recovery: either a newly published local artifact waiting
/// for backup enqueue or a legacy current-cycle backup in progress/ready.
/// A nullable legacy row with a failed or absent snapshot still ages out.
#[tokio::test(flavor = "current_thread")]
async fn abandon_sweep_spares_redrivable_marked_suspend() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    insert_draining_node(pool, node).await?;

    // Marked suspend, current upload finalized `ready` → redrivable, spared.
    let ready = SandboxId::new();
    insert_sandbox(pool, ready, "suspending", Some(node)).await?;
    insert_snapshot(pool, ready, "ready", node).await?;
    pin_active_and_age(pool, ready, node, Some("started"), 3600).await?;

    // Marked suspend, upload still `creating` → redrivable once ready, spared.
    let creating = SandboxId::new();
    insert_sandbox(pool, creating, "suspending", Some(node)).await?;
    insert_snapshot(pool, creating, "creating", node).await?;
    pin_active_and_age(pool, creating, node, Some("started"), 3600).await?;

    // New path: local publication and retirement completed, but the
    // controller crashed before it could enqueue backup. Redrive owns it.
    let published = SandboxId::new();
    insert_sandbox(pool, published, "suspending", Some(node)).await?;
    set_evacuation_marker(pool, published, "started").await?;
    set_current_local_artifact(pool, published, LocalArtifactId::new()).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle_changed_at = NOW() - make_interval(secs => 3600)
          WHERE id = $1"
    ))
    .bind(published)
    .execute(pool)
    .await?;

    // Stale prior identity while the source execution is still active:
    // local publication for the current suspend has not completed, so this
    // row is not redrivable and must be abandoned once aged out.
    let stale_identity_active = SandboxId::new();
    insert_sandbox(pool, stale_identity_active, "suspending", Some(node)).await?;
    set_current_local_artifact(pool, stale_identity_active, LocalArtifactId::new()).await?;
    pin_active_and_age(pool, stale_identity_active, node, Some("started"), 3600).await?;

    // Marked suspend, upload `failed` → nothing to recover, must be swept.
    let failed_snap = SandboxId::new();
    insert_sandbox(pool, failed_snap, "suspending", Some(node)).await?;
    insert_snapshot(pool, failed_snap, "failed", node).await?;
    pin_active_and_age(pool, failed_snap, node, Some("started"), 3600).await?;

    // Marked suspend, no snapshot at all → nothing to recover, must be swept.
    let no_snap = SandboxId::new();
    insert_sandbox(pool, no_snap, "suspending", Some(node)).await?;
    pin_active_and_age(pool, no_snap, node, Some("started"), 3600).await?;

    let mut claimed = abandon_stale(pool, node, 600).await?;
    claimed.sort();
    let mut expected = vec![stale_identity_active, failed_snap, no_snap];
    expected.sort();
    assert_eq!(
        claimed, expected,
        "only the marked suspends with no recoverable current snapshot may be abandoned"
    );
    assert_eq!(
        lifecycle_of(pool, ready).await?,
        "suspending",
        "a marked suspend with a ready current snapshot must be spared for the redrive"
    );
    assert_eq!(
        lifecycle_of(pool, creating).await?,
        "suspending",
        "a marked suspend with a creating current snapshot must be spared for the redrive"
    );
    assert_eq!(
        lifecycle_of(pool, published).await?,
        "suspending",
        "a published local artifact must survive until redrive enqueues its backup"
    );
    assert_eq!(lifecycle_of(pool, stale_identity_active).await?, "failed");
    assert_eq!(lifecycle_of(pool, failed_snap).await?, "failed");
    assert_eq!(lifecycle_of(pool, no_snap).await?, "failed");
    Ok(())
}

/// Run `mark_sandbox_failed_on_node_query`, returning the pre-flip
/// lifecycle strings of the rows it claimed (empty = CAS missed).
async fn mark_failed_on_node(
    pool: &PgPool,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
) -> TestResult<Vec<String>> {
    Ok(
        sqlx::query_scalar::<_, String>(mark_sandbox_failed_on_node_query())
            .bind(sandbox_id)
            .bind(source_node_id)
            .bind(mark_sandbox_failed_on_node_lifecycles())
            .fetch_all(pool)
            .await?,
    )
}

async fn evacuation_marker_of(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Option<String>> {
    Ok(sqlx::query_scalar::<_, Option<String>>(sql!(
        "SELECT evacuation_restore_target FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// The failed-on-node give-up flip. A sandbox `started`/`paused`
/// and active on the draining node is flipped to `failed` with its
/// evacuation marker cleared; a sandbox that drifted to another node, is in a
/// non-evacuable lifecycle, or is `stopped` is left untouched (the CAS misses).
#[tokio::test(flavor = "current_thread")]
async fn mark_failed_on_node_flips_evacuable_active_rows_only() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    let other = NodeId::new();
    insert_draining_node(pool, node).await?;
    insert_draining_node(pool, other).await?;

    // (A) started + active on node → flipped, returns pre-lifecycle.
    let started = SandboxId::new();
    insert_sandbox(pool, started, "started", Some(node)).await?;
    pin_active_and_age(pool, started, node, None, 0).await?;

    // (B) paused + active on node + leftover evacuation marker → flipped,
    //     marker cleared.
    let paused = SandboxId::new();
    insert_sandbox(pool, paused, "paused", Some(node)).await?;
    pin_active_and_age(pool, paused, node, Some("paused"), 0).await?;

    // (C) drifted: started but active on a *different* node → CAS misses.
    let drifted = SandboxId::new();
    insert_sandbox(pool, drifted, "started", Some(other)).await?;
    pin_active_and_age(pool, drifted, other, None, 0).await?;

    // (D) non-evacuable lifecycle (suspended) active on node → CAS misses.
    let suspended = SandboxId::new();
    insert_sandbox(pool, suspended, "suspended", Some(node)).await?;
    pin_active_and_age(pool, suspended, node, None, 0).await?;

    // (E) stopped + active on node → CAS misses. `stopped` is evacuable, but
    //     not by the suspend pipeline whose refusal this flip answers, and its
    //     rootfs is the sandbox's only copy: a user stopping the sandbox
    //     between the refused suspend and this write must not lose it.
    let stopped = SandboxId::new();
    insert_sandbox(pool, stopped, "stopped", Some(node)).await?;
    pin_active_and_age(pool, stopped, node, None, 0).await?;

    assert_eq!(
        mark_failed_on_node(pool, started, node).await?,
        vec!["started".to_string()],
        "a started sandbox active on the node is flipped, returning its pre-lifecycle"
    );
    assert_eq!(lifecycle_of(pool, started).await?, "failed");

    assert_eq!(
        mark_failed_on_node(pool, paused, node).await?,
        vec!["paused".to_string()]
    );
    assert_eq!(lifecycle_of(pool, paused).await?, "failed");
    assert_eq!(
        evacuation_marker_of(pool, paused).await?,
        None,
        "the evacuation marker must be cleared by the give-up flip"
    );

    assert!(
        mark_failed_on_node(pool, drifted, node).await?.is_empty(),
        "a sandbox that drifted to another node must not be flipped"
    );
    assert_eq!(lifecycle_of(pool, drifted).await?, "started");

    assert!(
        mark_failed_on_node(pool, suspended, node).await?.is_empty(),
        "a non-evacuable lifecycle must not be flipped"
    );
    assert_eq!(lifecycle_of(pool, suspended).await?, "suspended");

    assert!(
        mark_failed_on_node(pool, stopped, node).await?.is_empty(),
        "a stopped sandbox must not be flipped to failed: the flip deletes its execution, \
         and a stopped rootfs is the only copy"
    );
    assert_eq!(lifecycle_of(pool, stopped).await?, "stopped");
    Ok(())
}

// -----------------------------------------------------------------
// Cohort-restore retry backoff: the pending query's
// `next_restore_attempt_at` gate + attempt-count ordering, the
// failure-bump UPDATE, and the trigger-driven reset.
// -----------------------------------------------------------------

/// Read `(restore_attempt_count, next_restore_attempt_at IS NULL)` — the
/// backoff bookkeeping columns.
async fn backoff_columns(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<(i32, bool)> {
    Ok(sqlx::query_as(sql!(
        "SELECT restore_attempt_count, next_restore_attempt_at IS NULL
           FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// Run the failure bump ([`schedule_restore_retry_query`]) and return the
/// post-increment attempt count (`None` = the marker guard missed).
async fn schedule_retry(
    pool: &PgPool,
    sandbox_id: SandboxId,
    base_secs: i64,
    cap_secs: i64,
) -> TestResult<Option<i32>> {
    Ok(sqlx::query_scalar::<_, i32>(schedule_restore_retry_query())
        .bind(sandbox_id)
        .bind(base_secs)
        .bind(cap_secs)
        .fetch_optional(pool)
        .await?)
}

/// Whole seconds from `NOW()` to the row's `next_restore_attempt_at`.
async fn next_attempt_delay_secs(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<i64> {
    Ok(sqlx::query_scalar::<_, i64>(sql!(
        "SELECT EXTRACT(EPOCH FROM (next_restore_attempt_at - NOW()))::BIGINT
           FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// Plant backoff state directly (`offset_secs` is relative to `NOW()`;
/// `None` leaves `next_restore_attempt_at` NULL). Only meaningful on a
/// **marked** row — on an unmarked one the reset trigger wipes the write.
async fn set_backoff_state(
    pool: &PgPool,
    sandbox_id: SandboxId,
    attempt_count: i32,
    offset_secs: Option<i64>,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_attempt_count = $2,
                next_restore_attempt_at = CASE
                    WHEN $3::BIGINT IS NULL THEN NULL
                    ELSE NOW() + make_interval(secs => $3::BIGINT)
                END
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(attempt_count)
    .bind(offset_secs)
    .execute(pool)
    .await?;
    Ok(())
}

/// The pending scan excludes a marked, snapshot-ready sandbox whose
/// backoff window is still open, re-admits it once the window passes,
/// and a never-failed row (NULL `next_restore_attempt_at`) is always
/// eligible — and outranks the re-admitted failer in the ordering.
#[tokio::test(flavor = "current_thread")]
async fn restore_pending_gates_on_next_restore_attempt_at() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    // Failed recently: backoff window open for another hour → excluded.
    let in_window = SandboxId::new();
    insert_sandbox(pool, in_window, "suspended", Some(node)).await?;
    insert_snapshot(pool, in_window, "ready", node).await?;
    set_evacuation_marker(pool, in_window, "started").await?;
    set_backoff_state(pool, in_window, 3, Some(3600)).await?;

    // Failed earlier: window already passed → eligible again.
    let window_expired = SandboxId::new();
    insert_sandbox(pool, window_expired, "suspended", Some(node)).await?;
    insert_snapshot(pool, window_expired, "ready", node).await?;
    set_evacuation_marker(pool, window_expired, "started").await?;
    set_backoff_state(pool, window_expired, 3, Some(-5)).await?;

    // Never failed: NULL window → eligible, and ahead of the failer.
    let fresh = SandboxId::new();
    insert_sandbox(pool, fresh, "suspended", Some(node)).await?;
    insert_snapshot(pool, fresh, "ready", node).await?;
    set_evacuation_marker(pool, fresh, "started").await?;

    let pending = restore_pending(pool).await?;
    assert_eq!(
        pending,
        vec![
            (fresh, "started".to_string()),
            (window_expired, "started".to_string()),
        ],
        "open-window failer excluded; fresh row ordered before the re-admitted failer; \
         got {pending:?}"
    );
    Ok(())
}

/// Disabling the backoff must bypass windows that failures wrote
/// *before* the disable: with `honor_backoff_windows = false` the scan
/// re-admits a marked, snapshot-ready row whose `next_restore_attempt_at`
/// is still in the future — the operator escape hatch restores
/// retry-next-sweep behavior immediately instead of waiting out stale
/// state (up to the configured cap). The bypass covers **both** halves of
/// the backoff's influence on the scan: the window gate AND the
/// attempt-tier ordering key — a frozen pre-disable `restore_attempt_count`
/// must not keep sorting a row behind every fresh evacuation (under slot
/// pressure that would starve it just as effectively as the gate), so
/// disabled mode falls back to the pure recency ordering.
#[tokio::test(flavor = "current_thread")]
async fn restore_pending_ignores_stale_windows_when_backoff_disabled() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    // Backed off before the operator disabled the backoff: window still
    // open for another hour, and the most-recently-active row of the set.
    let stale_window = SandboxId::new();
    insert_sandbox(pool, stale_window, "suspended", Some(node)).await?;
    insert_snapshot(pool, stale_window, "ready", node).await?;
    set_evacuation_marker(pool, stale_window, "started").await?;
    set_backoff_state(pool, stale_window, 3, Some(3600)).await?;
    age_lifecycle(pool, stale_window, 60).await?;

    // Never-failed but colder evacuation.
    let fresh_cold = SandboxId::new();
    insert_sandbox(pool, fresh_cold, "suspended", Some(node)).await?;
    insert_snapshot(pool, fresh_cold, "ready", node).await?;
    set_evacuation_marker(pool, fresh_cold, "paused").await?;
    age_lifecycle(pool, fresh_cold, 7200).await?;

    assert_eq!(
        restore_pending_with(pool, true).await?,
        vec![(fresh_cold, "paused".to_string())],
        "with the backoff enabled, the open window must exclude the failer"
    );
    assert_eq!(
        restore_pending_with(pool, false).await?,
        vec![
            (stale_window, "started".to_string()),
            (fresh_cold, "paused".to_string()),
        ],
        "with the backoff disabled, stale windows must not suppress the row and \
         frozen attempt counts must not order it behind fresh rows — pure recency"
    );
    Ok(())
}

/// The failure bump doubles the delay per consecutive failure from the
/// base, saturating at the cap, and the count clamp keeps an absurdly
/// long-failing row from overflowing the interval arithmetic.
#[tokio::test(flavor = "current_thread")]
async fn schedule_restore_retry_backs_off_exponentially_to_cap() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    let sandbox = SandboxId::new();
    insert_sandbox(pool, sandbox, "suspended", Some(node)).await?;
    set_evacuation_marker(pool, sandbox, "started").await?;

    // base 60s, cap 1800s: 60 → 120 → 240 → 480 → 960 → 1800 (1920
    // capped) → 1800.
    for (i, expected) in [60i64, 120, 240, 480, 960, 1800, 1800].iter().enumerate() {
        let attempt = i as i32 + 1;
        assert_eq!(
            schedule_retry(pool, sandbox, 60, 1800).await?,
            Some(attempt),
            "bump must report the post-increment attempt count"
        );
        let delay = next_attempt_delay_secs(pool, sandbox).await?;
        assert!(
            (expected - 5..=*expected).contains(&delay),
            "attempt {attempt}: expected ~{expected}s of backoff, got {delay}s"
        );
    }

    // Far past the exponent clamp the delay stays pinned to the cap
    // (and the arithmetic must not overflow).
    set_backoff_state(pool, sandbox, 1000, None).await?;
    assert_eq!(schedule_retry(pool, sandbox, 60, 1800).await?, Some(1001));
    let delay = next_attempt_delay_secs(pool, sandbox).await?;
    assert!(
        (1795..=1800).contains(&delay),
        "clamped attempt must still cap at 1800s, got {delay}s"
    );
    Ok(())
}

/// The bump's marker guard: a sandbox whose `evacuation_restore_target`
/// was cleared (user takeover raced the failed attempt) matches no row
/// and gains no backoff state — the row is the user's now.
#[tokio::test(flavor = "current_thread")]
async fn schedule_restore_retry_requires_evacuation_marker() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    let user_owned = SandboxId::new();
    insert_sandbox(pool, user_owned, "suspended", Some(node)).await?;

    assert_eq!(
        schedule_retry(pool, user_owned, 60, 1800).await?,
        None,
        "an unmarked sandbox must not gain backoff state"
    );
    assert_eq!(backoff_columns(pool, user_owned).await?, (0, true));
    Ok(())
}

/// The `sandboxes_clear_evacuation_restore_target` trigger wipes the
/// backoff state the moment the marker clears, through both clearing
/// paths: a user takeover's lifecycle transition (the trigger nulls the
/// marker itself, then observes it NULL within the same row write) and
/// an explicit finalize-shaped `evacuation_restore_target = NULL` write.
/// A later re-evacuation then starts from a clean slate.
#[tokio::test(flavor = "current_thread")]
async fn backoff_state_resets_when_marker_clears() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    // User takeover: lifecycle leaves the suspend/restore window, the
    // marker-clear trigger drops the marker, and the backoff resets.
    let taken_over = SandboxId::new();
    insert_sandbox(pool, taken_over, "restoring", Some(node)).await?;
    set_evacuation_marker(pool, taken_over, "started").await?;
    set_backoff_state(pool, taken_over, 3, Some(600)).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
    ))
    .bind(taken_over)
    .execute(pool)
    .await?;
    assert_eq!(evacuation_marker_of(pool, taken_over).await?, None);
    assert_eq!(
        backoff_columns(pool, taken_over).await?,
        (0, true),
        "a user takeover must reset the backoff state with the marker"
    );

    // Finalize shape: the cohort restore's own UPDATE clears the marker
    // explicitly; the reset rides the same row update.
    let finalized = SandboxId::new();
    insert_sandbox(pool, finalized, "suspended", Some(node)).await?;
    set_evacuation_marker(pool, finalized, "started").await?;
    set_backoff_state(pool, finalized, 4, Some(600)).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = NULL WHERE id = $1"
    ))
    .bind(finalized)
    .execute(pool)
    .await?;
    assert_eq!(
        backoff_columns(pool, finalized).await?,
        (0, true),
        "clearing the marker explicitly must reset the backoff state"
    );

    // While the marker is still set, the backoff state must survive
    // unrelated row updates (the reset is marker-scoped, not a wipe-on-
    // every-write).
    let still_marked = SandboxId::new();
    insert_sandbox(pool, still_marked, "suspended", Some(node)).await?;
    set_evacuation_marker(pool, still_marked, "started").await?;
    set_backoff_state(pool, still_marked, 2, Some(600)).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET placement_changed_at = NOW() WHERE id = $1"
    ))
    .bind(still_marked)
    .execute(pool)
    .await?;
    let (count, next_is_null) = backoff_columns(pool, still_marked).await?;
    assert_eq!(
        count, 2,
        "marked row's backoff must survive unrelated updates"
    );
    assert!(!next_is_null);
    Ok(())
}

// -----------------------------------------------------------------
// KubeNodeGone recovery queries
// -----------------------------------------------------------------

async fn insert_node_k8s(pool: &PgPool, node_id: NodeId, uid: Option<&str>) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, 'ip-10-0-0-50.ec2.internal', $2)"
    ))
    .bind(node_id)
    .bind(uid)
    .execute(pool)
    .await?;
    Ok(())
}

/// Run the KubeNodeGone bulk give-up
/// ([`fail_resident_sandboxes_on_gone_node_query`]) and return the
/// claimed ids.
async fn fail_on_gone_node(
    pool: &PgPool,
    node: NodeId,
    expected_uid: Option<&str>,
) -> TestResult<Vec<SandboxId>> {
    let lifecycle_strs = resident_lifecycle_strs();
    let query = fail_resident_sandboxes_on_gone_node_query();
    let rows: Vec<(SandboxId, String)> = sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node)
        .bind(&lifecycle_strs)
        .bind(expected_uid)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|(id, _)| id).collect())
}

async fn mark_lost_on_gone_node(
    pool: &PgPool,
    sandbox: SandboxId,
    node: NodeId,
    expected_uid: Option<&str>,
) -> TestResult<u64> {
    Ok(sqlx::query(sqlx::AssertSqlSafe(sql!(
        mark_sandbox_lost_on_gone_node_query()
    )))
    .bind(sandbox)
    .bind(node)
    .bind(expected_uid)
    .execute(pool)
    .await?
    .rows_affected())
}

/// Run the gone-node adopt CAS ([`adopt_remote_snapshot_on_gone_node_query`])
/// and return the number of rows flipped.
async fn adopt_on_gone_node(
    pool: &PgPool,
    sandbox: SandboxId,
    node: NodeId,
    expected_uid: Option<&str>,
    settle_secs: i64,
) -> TestResult<u64> {
    Ok(sqlx::query(sqlx::AssertSqlSafe(sql!(
        adopt_remote_snapshot_on_gone_node_query(),
    )))
    .bind(sandbox)
    .bind(node)
    .bind(expected_uid)
    .bind(settle_secs)
    .execute(pool)
    .await?
    .rows_affected())
}

/// Insert a `state='ready'` full snapshot (with its storage-location row)
/// whose `timestamp` is `age_secs` in the past — used to model a
/// placement-stale ready snapshot (older than `placement_changed_at`).
async fn insert_aged_ready_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    age_secs: i64,
) -> TestResult {
    let snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'ready', $3, NOW() - make_interval(secs => $4))"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(source_node_id)
    .bind(age_secs)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(snapshot_id)
    .bind(format!("s3://test-bucket/{snapshot_id}"))
    .execute(pool)
    .await?;
    Ok(())
}

/// The KubeNodeGone bulk give-up's full guard matrix on one draining
/// node: dead residents flip (an active `started`, a marked `suspending`
/// with no recoverable evidence — marker cleared on the flip), recoverable
/// rows are spared (a marked `restoring` re-driven from S3, a marked
/// `suspending` with a current-cycle in-flight backup), `suspended` rows
/// are out of scope (their adopt/mark-lost give-up is per-sandbox), and
/// the kube-uid + drain-phase guards refuse wholesale.
#[tokio::test(flavor = "current_thread")]
async fn gone_node_bulk_fail_flips_dead_residents_only() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    insert_draining_node(pool, node).await?;
    insert_node_k8s(pool, node, Some("uid-1")).await?;

    // (A) live VM at instance death -> dead, flipped.
    let started = SandboxId::new();
    insert_sandbox(pool, started, "started", Some(node)).await?;
    pin_active_and_age(pool, started, node, None, 3600).await?;

    // (B) a suspended row whose `active_node_id` was
    // never cleared. Its recoverability depends on its remote snapshot,
    // so the bulk sweep must leave it to the per-sandbox give-up.
    let suspended = SandboxId::new();
    insert_sandbox(pool, suspended, "suspended", Some(node)).await?;
    pin_active_and_age(pool, suspended, node, None, 3600).await?;

    // (C) marked restoring -> re-driven from S3 by the restore sweep,
    // spared.
    let evac_restore = SandboxId::new();
    insert_sandbox(pool, evac_restore, "restoring", Some(node)).await?;
    pin_active_and_age(pool, evac_restore, node, Some("started"), 3600).await?;

    // (D) marked suspending with a current-cycle in-flight backup ->
    // recoverable evidence (the stuck-snapshot reaper settles it), spared.
    let evac_suspend_recoverable = SandboxId::new();
    insert_sandbox(pool, evac_suspend_recoverable, "suspending", Some(node)).await?;
    pin_active_and_age(pool, evac_suspend_recoverable, node, Some("started"), 3600).await?;
    insert_snapshot(pool, evac_suspend_recoverable, "creating", node).await?;

    // (E) marked suspending with no recoverable evidence -> its suspend
    // died with the instance, flipped (and the marker cleared).
    let evac_suspend_dead = SandboxId::new();
    insert_sandbox(pool, evac_suspend_dead, "suspending", Some(node)).await?;
    pin_active_and_age(pool, evac_suspend_dead, node, Some("started"), 3600).await?;

    // Kube-uid guard: a same-name replacement bumped the uid -> refuse.
    assert!(
        fail_on_gone_node(pool, node, Some("uid-2"))
            .await?
            .is_empty(),
        "a kube uid mismatch must refuse the whole sweep"
    );

    let mut claimed = fail_on_gone_node(pool, node, Some("uid-1")).await?;
    claimed.sort();
    let mut expected = vec![started, evac_suspend_dead];
    expected.sort();
    assert_eq!(
        claimed, expected,
        "only the dead residents flip: the suspended row, the marked restore, \
         and the recoverable marked suspend must be spared"
    );
    assert_eq!(lifecycle_of(pool, started).await?, "failed");
    assert_eq!(lifecycle_of(pool, evac_suspend_dead).await?, "failed");
    let marker: Option<String> = sqlx::query_scalar(sql!(
        "SELECT evacuation_restore_target FROM sandboxes WHERE id = $1"
    ))
    .bind(evac_suspend_dead)
    .fetch_one(pool)
    .await?;
    assert!(
        marker.is_none(),
        "the flip must clear the evacuation marker"
    );
    assert_eq!(lifecycle_of(pool, suspended).await?, "suspended");
    assert_eq!(lifecycle_of(pool, evac_restore).await?, "restoring");
    assert_eq!(
        lifecycle_of(pool, evac_suspend_recoverable).await?,
        "suspending"
    );

    // Drain-phase guard: an admin undrain landing before the sweep makes
    // it a no-op, even with residents still pinned.
    let late = SandboxId::new();
    insert_sandbox(pool, late, "started", Some(node)).await?;
    pin_active_and_age(pool, late, node, None, 3600).await?;
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = 'drained' WHERE id = $1"
    ))
    .bind(node)
    .execute(pool)
    .await?;
    assert!(
        fail_on_gone_node(pool, node, Some("uid-1"))
            .await?
            .is_empty(),
        "the sweep must refuse once the node leaves drain_phase = 'draining'"
    );
    assert_eq!(lifecycle_of(pool, late).await?, "started");
    Ok(())
}

/// The gone-node mark-lost is stricter than the artifact-missing one on
/// purpose: any ready full snapshot — even one the placement-freshness
/// portability test calls stale — refuses the terminal flip, because the
/// adopt CAS may recover such a row on a later tick. Only a row with no
/// ready snapshot anywhere (and no upload in flight) is declared lost.
#[tokio::test(flavor = "current_thread")]
async fn gone_node_mark_lost_refuses_any_ready_snapshot() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    insert_draining_node(pool, node).await?;
    insert_node_k8s(pool, node, Some("uid-1")).await?;

    // (A) placement-stale ready snapshot (older than the 1h-old
    // placement_changed_at): the legacy freshness-anchored guard would
    // flip this row, the gone-node guard must refuse it.
    let stale_ready = SandboxId::new();
    insert_sandbox(pool, stale_ready, "suspended", Some(node)).await?;
    insert_aged_ready_snapshot(pool, stale_ready, node, 7200).await?;
    assert_eq!(
        mark_lost_on_gone_node(pool, stale_ready, node, Some("uid-1")).await?,
        0,
        "a ready snapshot must refuse the gone-node mark-lost regardless of freshness"
    );
    assert_eq!(lifecycle_of(pool, stale_ready).await?, "suspended");

    // Contrast pinning the divergence (and why the dedicated query
    // exists): the artifact-missing mark-lost flips an identical row,
    // because its guard only sees the placement-fresh snapshot set.
    let stale_ready_legacy = SandboxId::new();
    insert_sandbox(pool, stale_ready_legacy, "suspended", Some(node)).await?;
    insert_aged_ready_snapshot(pool, stale_ready_legacy, node, 7200).await?;
    let legacy_flipped = sqlx::query(sqlx::AssertSqlSafe(sql!(mark_sandbox_lost_query())))
        .bind(stale_ready_legacy)
        .bind(node)
        .execute(pool)
        .await?
        .rows_affected();
    assert_eq!(
        legacy_flipped, 1,
        "the freshness-anchored guard flips the same shape; the gone-node \
         variant exists exactly to not do this after a transient adopt miss"
    );

    // (B) one failed snapshot, nothing else.
    // No recoverable copy exists anywhere -> lost.
    let unrecoverable = SandboxId::new();
    insert_sandbox(pool, unrecoverable, "suspended", Some(node)).await?;
    insert_snapshot(pool, unrecoverable, "failed", node).await?;
    // Identity guards first: a bumped kube uid refuses even this
    // genuinely unrecoverable row (it may belong to a same-name
    // replacement now)...
    assert_eq!(
        mark_lost_on_gone_node(pool, unrecoverable, node, Some("uid-2")).await?,
        0,
        "a kube uid mismatch must refuse the gone-node mark-lost"
    );
    // ...then the matching uid flips it.
    assert_eq!(
        mark_lost_on_gone_node(pool, unrecoverable, node, Some("uid-1")).await?,
        1
    );
    assert_eq!(lifecycle_of(pool, unrecoverable).await?, "failed");

    // (C) an in-flight `creating` upload row refuses the flip until the
    // stuck-snapshot reaper terminalizes it.
    let inflight = SandboxId::new();
    insert_sandbox(pool, inflight, "suspended", Some(node)).await?;
    insert_snapshot(pool, inflight, "creating", node).await?;
    assert_eq!(
        mark_lost_on_gone_node(pool, inflight, node, Some("uid-1")).await?,
        0
    );
    assert_eq!(lifecycle_of(pool, inflight).await?, "suspended");

    // (D) drain-phase guard: once the node leaves `draining`, the
    // verdict no longer applies — refuse wholesale.
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = 'drained' WHERE id = $1"
    ))
    .bind(node)
    .execute(pool)
    .await?;
    let after_undrain = SandboxId::new();
    insert_sandbox(pool, after_undrain, "suspended", Some(node)).await?;
    assert_eq!(
        mark_lost_on_gone_node(pool, after_undrain, node, Some("uid-1")).await?,
        0,
        "the gone-node mark-lost must refuse once the node leaves drain_phase = 'draining'"
    );
    Ok(())
}

/// The gone-node adopt CAS's full guard matrix: it adopts only a
/// current-suspend-cycle ready snapshot, under the same node-identity
/// guards as the bulk fail. The stale-legacy contrast pins why the
/// dedicated query exists: for a nullable row whose last re-suspend
/// registered no snapshot record, the reupload-path adopt
/// ([`adopt_remote_snapshot_for_drain_query`]) accepts the prior cycle's
/// ready snapshot — safe there only because that path holds a
/// restoring-on-node proof — while the gone-node variant must refuse it
/// (adopting would silently roll the sandbox back to pre-modification
/// bytes).
#[tokio::test(flavor = "current_thread")]
async fn gone_node_adopt_requires_current_cycle_snapshot_and_identity() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    insert_draining_node(pool, node).await?;
    insert_node_k8s(pool, node, Some("uid-1")).await?;

    // (A) current-cycle ready snapshot (registered after the row entered
    // `suspended`): adoptable — but only under the observed uid.
    let current = SandboxId::new();
    insert_sandbox(pool, current, "suspended", Some(node)).await?;
    age_lifecycle(pool, current, 3600).await?;
    insert_snapshot(pool, current, "ready", node).await?;
    assert_eq!(
        adopt_on_gone_node(pool, current, node, Some("uid-2"), 600).await?,
        0,
        "a kube uid mismatch must refuse the gone-node adopt"
    );
    assert_eq!(
        adopt_on_gone_node(pool, current, node, Some("uid-1"), 600).await?,
        1,
        "a current-cycle ready snapshot under the observed uid must adopt"
    );

    // (B) stale legacy snapshot: the only (hence newest) ready snapshot
    // predates the row's last lifecycle change — a wake -> modify ->
    // re-suspend whose backup never registered a record. The gone-node
    // adopt must refuse...
    let stale = SandboxId::new();
    insert_sandbox(pool, stale, "suspended", Some(node)).await?;
    insert_aged_ready_snapshot(pool, stale, node, 7200).await?;
    age_lifecycle(pool, stale, 3600).await?;
    assert_eq!(
        adopt_on_gone_node(pool, stale, node, Some("uid-1"), 600).await?,
        0,
        "a prior-cycle ready snapshot must not be adopted without a \
         restoring-on-node proof"
    );
    // ...while the reupload-path adopt accepts the same shape (its
    // caller holds the restoring-on-node proof that makes the snapshot
    // canonical) — the contrast that justifies the dedicated query.
    assert_eq!(adopt_remote_snapshot(pool, stale, node, 600).await?, 1);

    // (C) drain-phase guard: an undrained node refuses wholesale.
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = 'drained' WHERE id = $1"
    ))
    .bind(node)
    .execute(pool)
    .await?;
    let late = SandboxId::new();
    insert_sandbox(pool, late, "suspended", Some(node)).await?;
    age_lifecycle(pool, late, 3600).await?;
    insert_snapshot(pool, late, "ready", node).await?;
    assert_eq!(
        adopt_on_gone_node(pool, late, node, Some("uid-1"), 600).await?,
        0,
        "the gone-node adopt must refuse once the node leaves drain_phase = 'draining'"
    );
    Ok(())
}

async fn fail_inflight_snapshots(
    pool: &PgPool,
    node: NodeId,
    expected_uid: Option<&str>,
) -> TestResult<u64> {
    let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(sqlx::AssertSqlSafe(sql!(
        fail_inflight_snapshots_on_gone_node_query(),
    )))
    .bind(node)
    .bind(expected_uid)
    .bind(GONE_NODE_UPLOAD_FAILURE_REASON)
    .fetch_all(pool)
    .await?;
    Ok(rows.len() as u64)
}

/// The wedge the snapshot terminalize closes, pinned end-to-end: a
/// bulk-failed resident whose `creating` upload row survives keeps the
/// drain blocker count nonzero (the in-flight term is lifecycle-blind),
/// so without this flip the node waits on the stuck-snapshot reaper's
/// age window — or forever where that reaper is disabled. Plus the guard
/// matrix: uid mismatch and a left-`draining` node refuse wholesale, and
/// only this node's `creating` rows are touched.
#[tokio::test(flavor = "current_thread")]
async fn gone_node_inflight_snapshot_flip_unblocks_drain_gate() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    insert_draining_node(pool, node).await?;
    insert_node_k8s(pool, node, Some("uid-1")).await?;

    // The flagged shape: a sandbox the bulk fail just flipped, with a
    // surviving in-flight upload row from the dead node.
    let failed_resident = SandboxId::new();
    insert_sandbox(pool, failed_resident, "failed", Some(node)).await?;
    insert_snapshot(pool, failed_resident, "creating", node).await?;
    assert_eq!(
        drain_blocking_count(pool, node).await?,
        1,
        "the lifecycle-blind in-flight term must count the failed \
         sandbox's creating upload — the wedge under test"
    );

    // Untouchable bystanders: a creating upload from ANOTHER node, and a
    // ready row from this node.
    let other_node = NodeId::new();
    let other_nodes_upload = SandboxId::new();
    insert_sandbox(pool, other_nodes_upload, "suspended", Some(other_node)).await?;
    insert_snapshot(pool, other_nodes_upload, "creating", other_node).await?;
    let durable = SandboxId::new();
    insert_sandbox(pool, durable, "suspended", Some(node)).await?;
    insert_snapshot(pool, durable, "ready", node).await?;

    // Identity guard: a bumped uid refuses the flip.
    assert_eq!(
        fail_inflight_snapshots(pool, node, Some("uid-2")).await?,
        0,
        "a kube uid mismatch must refuse the in-flight snapshot flip"
    );

    // The flip under the observed uid: exactly this node's creating row.
    assert_eq!(fail_inflight_snapshots(pool, node, Some("uid-1")).await?, 1);
    let (state, reason): (String, Option<String>) = sqlx::query_as(sql!(
        "SELECT state, failure_reason FROM snapshots WHERE sandbox_id = $1"
    ))
    .bind(failed_resident)
    .fetch_one(pool)
    .await?;
    assert_eq!(state, "failed");
    assert_eq!(reason.as_deref(), Some(GONE_NODE_UPLOAD_FAILURE_REASON));
    assert_eq!(
        drain_blocking_count(pool, node).await?,
        0,
        "terminalizing the dead node's upload must release the drain gate"
    );
    let other_state: String =
        sqlx::query_scalar(sql!("SELECT state FROM snapshots WHERE sandbox_id = $1"))
            .bind(other_nodes_upload)
            .fetch_one(pool)
            .await?;
    assert_eq!(
        other_state, "creating",
        "another node's in-flight upload must be untouched"
    );

    // Drain-phase guard: once the node leaves `draining`, refuse.
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = 'drained' WHERE id = $1"
    ))
    .bind(node)
    .execute(pool)
    .await?;
    let late_upload = SandboxId::new();
    insert_sandbox(pool, late_upload, "suspended", Some(node)).await?;
    insert_snapshot(pool, late_upload, "creating", node).await?;
    assert_eq!(
        fail_inflight_snapshots(pool, node, Some("uid-1")).await?,
        0,
        "the flip must refuse once the node leaves drain_phase = 'draining'"
    );
    Ok(())
}
