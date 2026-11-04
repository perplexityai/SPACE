use pplx_space_common_core::types::NodeId;
use pplx_space_common_core::types::SandboxId;

use pplx_space_common_core::sql;
use pplx_space_common_svc::sql::sandbox_residency::ResidencyEvidence;
use pplx_space_db_test_fixtures::try_pg_test_db;
use sqlx::PgPool;
use std::num::NonZeroUsize;
use uuid::Uuid;

use super::{AdminNodeStore, list_pinned_local_only_sandboxes, sandbox_occupancy_for_node_in_tx};

use pplx_space_test_env::TestResult;

async fn insert_sandbox(pool: &PgPool, id: SandboxId, target_node_id: NodeId) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             target_node_id, placement_changed_at, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'suspended', 'unplaced', 'k8s',
             $2, NOW() - INTERVAL '1 hour', 1
         )"
    ))
    .bind(id)
    .bind(target_node_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn in_tx_local_snapshots(pool: &PgPool, node: NodeId) -> TestResult<i64> {
    let mut tx = pool.begin().await?;
    let occ =
        sandbox_occupancy_for_node_in_tx(&mut tx, node, ResidencyEvidence::LifecycleOnly).await?;
    tx.commit().await?;
    Ok(occ.drain_blockers)
}

/// `target_node_id` drifted to another node, but the suspend upload is
/// still in flight from the source node. The strand risk must be reported
/// so `:terminate` is refused — and crucially the single-node `:terminate`
/// guard and the multi-node admin listing must **agree** per node, so an
/// operator never sees `0` in the listing for a node the guard then 409s.
#[tokio::test(flavor = "current_thread")]
async fn inflight_upload_strand_risk_is_consistent_across_guard_and_listing() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let source = NodeId::new();
    let target = NodeId::new();
    let sandbox = Uuid::now_v7();
    // Suspended, target drifted to `target`, upload still in flight from
    // `source` (`source != target`).
    insert_sandbox(pool, SandboxId::from_uuid(sandbox), target).await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'creating', $3, NOW())"
    ))
    .bind(Uuid::now_v7())
    .bind(sandbox)
    .bind(source)
    .execute(pool)
    .await?;

    // In-tx `:terminate` guard: source blocked (in-flight disjunct),
    // target blocked (suspended+target disjunct — pre-existing behavior).
    assert_eq!(
        in_tx_local_snapshots(pool, source).await?,
        1,
        "guard(source)"
    );
    assert_eq!(
        in_tx_local_snapshots(pool, target).await?,
        1,
        "guard(target)"
    );

    // Admin listing must report the same per-node counts (no drift).
    let store = AdminNodeStore::new(pool.clone());
    let listing = store
        .occupancy_for_nodes(&[source, target], None, ResidencyEvidence::default())
        .await?;
    assert_eq!(
        listing.get(&source).map(|o| o.occupancy.local_snapshots),
        Some(1),
        "listing(source) must match guard(source)"
    );
    assert_eq!(
        listing.get(&target).map(|o| o.occupancy.local_snapshots),
        Some(1),
        "listing(target) must match guard(target)"
    );
    Ok(())
}

/// A **stale** prior-cycle `creating` remnant (timestamp before the
/// sandbox's last placement) must NOT count as strand risk — the source no
/// longer blocks on it (freshness-gated out in guard and listing alike),
/// while the target still blocks via the suspended-not-durable path.
/// `insert_sandbox` sets `placement_changed_at = NOW() - 1h`, so a 2h-old
/// `creating` row is stale.
#[tokio::test(flavor = "current_thread")]
async fn stale_inflight_remnant_is_not_strand_risk_across_guard_and_listing() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let source = NodeId::new();
    let target = NodeId::new();
    let sandbox = Uuid::now_v7();
    insert_sandbox(pool, SandboxId::from_uuid(sandbox), target).await?;
    // Stale (2h old, < placement_changed_at 1h) `creating` from `source`.
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'creating', $3, NOW() - make_interval(secs => 7200))"
    ))
    .bind(Uuid::now_v7())
    .bind(sandbox)
    .bind(source)
    .execute(pool)
    .await?;

    // Source no longer blocks (stale remnant freshness-gated out)...
    assert_eq!(
        in_tx_local_snapshots(pool, source).await?,
        0,
        "guard(source)"
    );
    // ...target still blocks via the suspended-not-durable path.
    assert_eq!(
        in_tx_local_snapshots(pool, target).await?,
        1,
        "guard(target)"
    );

    let store = AdminNodeStore::new(pool.clone());
    let listing = store
        .occupancy_for_nodes(&[source, target], None, ResidencyEvidence::default())
        .await?;
    assert_eq!(
        listing
            .get(&source)
            .map(|o| o.occupancy.local_snapshots)
            .unwrap_or(0),
        0,
        "listing(source) must match guard(source) — stale remnant ignored"
    );
    assert_eq!(
        listing.get(&target).map(|o| o.occupancy.local_snapshots),
        Some(1),
        "listing(target) must match guard(target)"
    );
    Ok(())
}

/// A sandbox already portable via a fresh `ready` snapshot must NOT count
/// as strand risk in either the guard or the listing, even if an earlier
/// `creating` row from the same node lingers.
#[tokio::test(flavor = "current_thread")]
async fn portable_sandbox_is_not_strand_risk_despite_leftover_creating() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    let sandbox = Uuid::now_v7();
    insert_sandbox(pool, SandboxId::from_uuid(sandbox), node).await?;
    // Lingering `creating` row + a finalized fresh `ready` snapshot (with
    // its storage-location row) from the same node → portable.
    let creating = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'creating', $3, NOW())"
    ))
    .bind(creating)
    .bind(sandbox)
    .bind(node)
    .execute(pool)
    .await?;
    let ready = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'ready', $3, NOW())"
    ))
    .bind(ready)
    .bind(sandbox)
    .bind(node)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(ready)
    .bind(format!("s3://test-bucket/{ready}"))
    .execute(pool)
    .await?;

    assert_eq!(
        in_tx_local_snapshots(pool, node).await?,
        0,
        "guard: portable sandbox must not be strand risk"
    );
    let store = AdminNodeStore::new(pool.clone());
    let listing = store
        .occupancy_for_nodes(&[node], None, ResidencyEvidence::default())
        .await?;
    assert_eq!(
        listing
            .get(&node)
            .map(|o| o.occupancy.local_snapshots)
            .unwrap_or(0),
        0,
        "listing: portable sandbox must not be strand risk"
    );
    Ok(())
}

/// The `:force-upload-local-snapshots` enumeration must skip sandboxes
/// whose durable upload is already in flight (force-reuploading them is
/// redundant and racy — a resulting newer `failed` snapshot could strand an
/// evacuation restore via `latest_full_snapshot_ready`), mirroring the
/// node-drain reupload queue. A genuinely local-only suspended sandbox is
/// still enumerated.
#[tokio::test(flavor = "current_thread")]
async fn force_upload_enumeration_excludes_inflight_uploads() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();

    // Genuinely local-only: suspended, no snapshot at all → needs upload.
    let needs_upload = SandboxId::new();
    insert_sandbox(pool, needs_upload, node).await?;

    // Upload already in flight (creating) from this node → must be skipped.
    let inflight = Uuid::now_v7();
    insert_sandbox(pool, SandboxId::from_uuid(inflight), node).await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'creating', $3, NOW())"
    ))
    .bind(Uuid::now_v7())
    .bind(inflight)
    .bind(node)
    .execute(pool)
    .await?;

    let listed =
        list_pinned_local_only_sandboxes(pool, node, NonZeroUsize::new(100).unwrap()).await?;
    assert_eq!(
        listed,
        vec![needs_upload],
        "only the genuinely local-only sandbox is enumerated; the in-flight upload is skipped"
    );
    Ok(())
}

async fn set_evacuation_marker(pool: &PgPool, id: Uuid, target: &str) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = $2 WHERE id = $1"
    ))
    .bind(id)
    .bind(target)
    .execute(pool)
    .await?;
    Ok(())
}

/// The admin `:terminate` strand-risk guard + listing are marker-aware,
/// matching the node-drain source-release guards. For an evacuation-marked
/// sandbox the question is "is the **newest** full snapshot ready?", not
/// placement-freshness:
///
/// * A marked sandbox whose newest snapshot is `ready` but is NOT
///   placement-fresh (a failed restore bumped `placement_changed_at`) is
///   durably restorable → **not** strand risk. The old freshness gate would
///   over-count it and wrongly 409 a legitimate `:terminate`.
/// * A marked sandbox whose current upload is still `creating` is **not**
///   yet restorable → strand risk (terminating kills the only copy).
///
/// Guard and listing must agree per node in both cases.
#[tokio::test(flavor = "current_thread")]
async fn evacuation_marked_strand_risk_tracks_newest_snapshot() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = AdminNodeStore::new(pool.clone());

    // Node A: marked sandbox, newest snapshot ready but placement bumped
    // past it (not placement-fresh) → durably restorable → NOT strand risk.
    let node_durable = NodeId::new();
    let durable_sb = Uuid::now_v7();
    insert_sandbox(pool, SandboxId::from_uuid(durable_sb), node_durable).await?;
    set_evacuation_marker(pool, durable_sb, "started").await?;
    let ready = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'ready', $3, NOW())"
    ))
    .bind(ready)
    .bind(durable_sb)
    .bind(node_durable)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(ready)
    .bind(format!("s3://test-bucket/{ready}"))
    .execute(pool)
    .await?;
    // Bump placement past the snapshot so the freshness gate would (wrongly)
    // treat it as stale; the newest-snapshot gate still sees it ready.
    sqlx::query(sql!(
        "UPDATE sandboxes SET placement_changed_at = NOW() + INTERVAL '1 hour' WHERE id = $1"
    ))
    .bind(durable_sb)
    .execute(pool)
    .await?;
    assert_eq!(
        in_tx_local_snapshots(pool, node_durable).await?,
        0,
        "guard: marked durable-but-not-fresh sandbox must not be strand risk"
    );

    // Node B: marked sandbox, current upload still creating → strand risk.
    let node_uploading = NodeId::new();
    let uploading_sb = Uuid::now_v7();
    insert_sandbox(pool, SandboxId::from_uuid(uploading_sb), node_uploading).await?;
    set_evacuation_marker(pool, uploading_sb, "started").await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'creating', $3, NOW())"
    ))
    .bind(Uuid::now_v7())
    .bind(uploading_sb)
    .bind(node_uploading)
    .execute(pool)
    .await?;
    assert_eq!(
        in_tx_local_snapshots(pool, node_uploading).await?,
        1,
        "guard: marked sandbox mid-upload must be strand risk"
    );

    // Guard and listing agree per node.
    let listing = store
        .occupancy_for_nodes(
            &[node_durable, node_uploading],
            None,
            ResidencyEvidence::default(),
        )
        .await?;
    assert_eq!(
        listing
            .get(&node_durable)
            .map(|o| o.occupancy.local_snapshots)
            .unwrap_or(0),
        0,
        "listing(durable) must match guard"
    );
    assert_eq!(
        listing
            .get(&node_uploading)
            .map(|o| o.occupancy.local_snapshots)
            .unwrap_or(0),
        1,
        "listing(uploading) must match guard"
    );
    Ok(())
}

/// The **non-drift** case: the suspend upload is in flight from the same node
/// the sandbox is still targeted at (`source_node_id == target_node_id`). Two
/// of the occupancy statement's UNION branches describe this one sandbox on
/// this one node. Deduplication must preserve a single strand-risk count.
///
/// Left undeduplicated it reads 2, and `local_snapshots` stops being a count of
/// sandboxes: an operator reading "2 pinned snapshots" on a node holding one
/// would over-estimate the strand risk of draining it, and the figure would no
/// longer agree with the in-transaction `:terminate` guard that actually
/// refuses the termination.
///
/// The drifted case (`source != target`, both nodes counting 1) is covered by
/// `inflight_upload_strand_risk_is_consistent_across_guard_and_listing`; this
/// is the collapsing case, which that test cannot reach.
#[tokio::test(flavor = "current_thread")]
async fn inflight_upload_from_the_target_node_counts_once_not_twice() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    let sandbox = Uuid::now_v7();

    // Suspended and targeted at `node`, with the upload in flight from that
    // same node — the two branches now collide on one (node, sandbox) pair.
    insert_sandbox(pool, SandboxId::from_uuid(sandbox), node).await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'creating', $3, NOW())"
    ))
    .bind(Uuid::now_v7())
    .bind(sandbox)
    .bind(node)
    .execute(pool)
    .await?;

    let store = AdminNodeStore::new(pool.clone());
    let listing = store
        .occupancy_for_nodes(&[node], None, ResidencyEvidence::default())
        .await?;
    assert_eq!(
        listing.get(&node).map(|o| o.occupancy.local_snapshots),
        Some(1),
        "one sandbox on one node must count once, however many branches \
         describe it"
    );

    // And the in-transaction guard has to agree, or the listing tells an
    // operator one thing and the `:terminate` refusal another.
    assert_eq!(
        in_tx_local_snapshots(pool, node).await?,
        1,
        "guard must match the listing"
    );

    // The enumeration operators read to decide what to do about it excludes
    // in-flight uploads, so the sandbox is counted but not offered for a
    // force-upload — those are different questions about the same row.
    let pinned =
        list_pinned_local_only_sandboxes(pool, node, NonZeroUsize::new(10).unwrap()).await?;
    assert!(
        !pinned.contains(&SandboxId::from_uuid(sandbox)),
        "an upload already in flight is not a force-upload candidate"
    );
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
        let occ = sandbox_occupancy_for_node_in_tx(&mut tx, node, residency).await?;
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

/// Exact artifact identity keeps an upload relevant even after placement has
/// advanced. The legacy upload branch's freshness filter must not hide the
/// target's only copy by excluding it from the suspended branch as well.
#[tokio::test(flavor = "current_thread")]
async fn exact_inflight_artifact_older_than_placement_still_counts_on_target() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    let sandbox = Uuid::now_v7();
    let artifact = Uuid::now_v7();
    insert_sandbox(pool, SandboxId::from_uuid(sandbox), node).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(artifact)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, source_node_id, timestamp,
             local_artifact_id, artifact_kind, artifact_origin
         ) VALUES ($1, $2, 'full', 'creating', $3, NOW() - INTERVAL '2 hours',
                   $4, 'full', 'suspend')"
    ))
    .bind(Uuid::now_v7())
    .bind(sandbox)
    .bind(node)
    .bind(artifact)
    .execute(pool)
    .await?;

    let store = AdminNodeStore::new(pool.clone());
    for ranking in [None, Some(std::time::Duration::from_secs(3600))] {
        let listing = store
            .occupancy_for_nodes(&[node], ranking, ResidencyEvidence::default())
            .await?;
        assert_eq!(listing[&node].occupancy.local_snapshots, 1);
        assert_eq!(listing[&node].occupancy.active, 0);
    }
    assert_eq!(in_tx_local_snapshots(pool, node).await?, 1);
    Ok(())
}
