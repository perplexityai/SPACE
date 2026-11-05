//! Disk artifacts against real Postgres: the sweep reaches `stop` disk rows
//! past the age gate, the restore-source guard shields the ones restore can
//! load, named and fork disk rows stay unswept, and disk rows have no keep-N
//! floor and don't count toward a `full` row's.

use super::super::{FakeBackend, Reconciler, config, registry_with};
use super::*;

/// A stopped sandbox's catalog shape: a `ready` `stop` disk snapshot carrying
/// its artifact identity, dated `age_secs` in the past, with a usable
/// location.
async fn insert_stop_disk(
    pool: &PgPool,
    sandbox_id: SandboxId,
    age_secs: i64,
    artifact_id: Uuid,
) -> TestResult<SnapshotId> {
    insert_typed_artifact_snapshot(pool, sandbox_id, "disk", age_secs, artifact_id, STOP_DISK).await
}

/// [`insert_stop_disk`] with a fresh artifact identity.
async fn insert_unpinned_stop_disk(
    pool: &PgPool,
    sandbox_id: SandboxId,
    age_secs: i64,
) -> TestResult<SnapshotId> {
    insert_stop_disk(pool, sandbox_id, age_secs, Uuid::now_v7()).await
}

/// Move the sandbox to `lifecycle` and pin `artifact_id` the way node drain
/// does for a stopped evacuation.
async fn pin_stop_disk(
    pool: &PgPool,
    sandbox_id: SandboxId,
    lifecycle: &str,
    artifact_id: Uuid,
) -> TestResult {
    sqlx::query("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1")
        .bind(sandbox_id)
        .bind(lifecycle)
        .execute(pool)
        .await?;
    pin_artifact(pool, sandbox_id, artifact_id, STOP_DISK).await
}

/// The `(kind, origin)` a stopped sandbox's disk artifact carries.
const STOP_DISK: (&str, &str) = ("disk", "stop");

async fn candidate_ids(
    store: &SnapshotRetentionDbStore,
    keep_n: i64,
) -> TestResult<Vec<SnapshotId>> {
    Ok(store
        .list_candidates(keep_n, 100, None, None)
        .await?
        .candidates
        .iter()
        .map(|candidate| candidate.snapshot_id)
        .collect())
}

#[tokio::test(flavor = "current_thread")]
async fn sweep_keeps_pinned_and_young_disk_rows_and_prunes_the_superseded_ones() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool).await?;
    // Oldest-first: four superseded evacuations, two backup attempts of the
    // pinned artifact, and a young unpinned row. With keep_n = 5 a disk
    // keep-N floor would keep two of the superseded rows; there is none.
    let mut superseded = Vec::new();
    for age in [9000, 8500, 8000, 7500] {
        superseded.push(insert_unpinned_stop_disk(pool, sandbox, age).await?);
    }
    let pinned_artifact = Uuid::now_v7();
    let pinned = [
        insert_stop_disk(pool, sandbox, 5400, pinned_artifact).await?,
        insert_stop_disk(pool, sandbox, 5000, pinned_artifact).await?,
    ];
    let young = insert_unpinned_stop_disk(pool, sandbox, 1800).await?;
    pin_stop_disk(pool, sandbox, "stopped", pinned_artifact).await?;

    let backend = FakeBackend::new();
    let mut policy = config(100);
    policy.keep_n = 5;
    policy.min_age = Some(std::time::Duration::from_secs(3600));
    let mut reconciler = Reconciler::new(
        policy,
        SnapshotRetentionDbStore::new(pool.clone()),
        registry_with(backend.clone()),
    );
    let report = reconciler.tick().await;

    assert_eq!(report.pruned, superseded.len(), "{report:?}");
    assert_eq!(
        backend.calls(),
        superseded
            .iter()
            .map(|id| format!("s3://test-bucket/snap/{id}?region=us-east-1"))
            .collect::<Vec<_>>()
    );
    for gone in &superseded {
        assert!(!snapshot_exists(pool, *gone).await?);
        assert!(!location_exists(pool, *gone).await?);
    }
    for kept in pinned.into_iter().chain([young]) {
        assert_eq!(
            snapshot_state(pool, kept).await?.as_deref(),
            Some("ready"),
            "disk row {kept} must survive the sweep"
        );
    }
    let store = SnapshotRetentionDbStore::new(pool.clone());
    for id in pinned {
        assert_eq!(
            store.claim_prune(id, 5).await?,
            ClaimOutcome::Refused(ClaimRefusalReason::ProtectedRestoreSource),
            "the claim must refuse every row of the pinned artifact"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restoring_stopped_evacuation_keeps_its_pinned_disk_source() -> TestResult {
    // The `restoring` shape that loads a disk artifact: the evacuation
    // restore sweep re-places a `restoring` row marked
    // `evacuation_restore_target = 'stopped'` from its pin. The retry window
    // doesn't shield a pinned disk row, so the pin is what must.
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool).await?;
    let before = insert_unpinned_stop_disk(pool, sandbox, 9000).await?;
    let pinned_artifact = Uuid::now_v7();
    let pinned = [
        insert_stop_disk(pool, sandbox, 5000, pinned_artifact).await?,
        insert_stop_disk(pool, sandbox, 4500, pinned_artifact).await?,
    ];
    pin_stop_disk(pool, sandbox, "restoring", pinned_artifact).await?;
    sqlx::query(
        "UPDATE sandboxes
            SET evacuation_restore_target = 'stopped',
                lifecycle_changed_at = NOW() - make_interval(secs => 4000)
          WHERE id = $1",
    )
    .bind(sandbox)
    .execute(pool)
    .await?;
    let after = insert_unpinned_stop_disk(pool, sandbox, 900).await?;

    let store = SnapshotRetentionDbStore::new(pool.clone());
    assert_eq!(
        candidate_ids(&store, 1).await?,
        vec![before, after],
        "only the unpinned rows are prunable"
    );
    for id in pinned {
        assert_eq!(
            store.claim_prune(id, 1).await?,
            ClaimOutcome::Refused(ClaimRefusalReason::ProtectedRestoreSource)
        );
        assert_eq!(snapshot_state(pool, id).await?.as_deref(), Some("ready"));
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn named_fork_and_markerless_disk_rows_stay_unswept() -> TestResult {
    // Only `stop` disk rows are swept. A named (`snapshot`) disk row may be
    // its artifact's only copy once the node drops its local bytes, so it,
    // a fork's copied row and a legacy markerless row are never candidates.
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool).await?;
    let named = insert_snapshot(pool, sandbox, "disk", "ready", 9000).await?;
    sqlx::query(
        "UPDATE snapshots
            SET local_artifact_id = $2,
                artifact_kind = 'disk',
                artifact_origin = 'snapshot',
                artifact_snapshot_name = 'user-named'
          WHERE id = $1",
    )
    .bind(named)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    let fork = insert_snapshot(pool, sandbox, "disk", "ready", 8500).await?;
    sqlx::query(
        "UPDATE snapshots
            SET artifact_kind = 'disk',
                artifact_origin = 'fork'
          WHERE id = $1",
    )
    .bind(fork)
    .execute(pool)
    .await?;
    let markerless = insert_snapshot(pool, sandbox, "disk", "ready", 8000).await?;
    let stop = insert_unpinned_stop_disk(pool, sandbox, 7500).await?;

    let store = SnapshotRetentionDbStore::new(pool.clone());
    assert_eq!(
        candidate_ids(&store, 1).await?,
        vec![stop],
        "the `stop` row is the control: without it this asserts nothing"
    );
    for id in [named, fork, markerless] {
        assert_eq!(
            store.claim_prune(id, 1).await?,
            ClaimOutcome::Refused(ClaimRefusalReason::KeepNFloor),
            "disk row {id} is not a `stop` row and must not be claimed"
        );
        assert_eq!(snapshot_state(pool, id).await?.as_deref(), Some("ready"));
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn full_keep_n_is_unchanged_by_disk_rows() -> TestResult {
    // Two full rows, then three newer unpinned `stop` disk rows. With
    // keep_n = 2 the full rows are exactly their keep-set, so the disk rows
    // must not push them out, and every disk row is prunable, the newest
    // included.
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool).await?;
    let full = [
        insert_snapshot(pool, sandbox, "full", "ready", 500).await?,
        insert_snapshot(pool, sandbox, "full", "ready", 400).await?,
    ];
    let mut disk = Vec::new();
    for age in [300, 200, 100] {
        disk.push(insert_unpinned_stop_disk(pool, sandbox, age).await?);
    }

    let store = SnapshotRetentionDbStore::new(pool.clone());
    assert_eq!(candidate_ids(&store, 2).await?, disk);
    for id in full {
        assert_eq!(
            store.claim_prune(id, 2).await?,
            ClaimOutcome::Refused(ClaimRefusalReason::KeepNFloor),
            "newer disk rows must not satisfy a full row's floor"
        );
    }
    let newest_disk = disk[2];
    let claimed = expect_claimed(store.claim_prune(newest_disk, 2).await?);
    assert_eq!(
        store.finalize_prune(claimed.snapshot_id, 2).await?,
        PruneFinalize::Pruned {
            floor_ok: true,
            marker_ok: true,
        },
        "a `stop` disk row has no floor for finalize to report breached"
    );
    assert!(!snapshot_exists(pool, newest_disk).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_claimed_disk_row_pages_through_recovery() -> TestResult {
    // A disk claim parked by a crash between claim and finalize must page
    // through recovery like a full one, so a later tick can finish it.
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool).await?;
    // Only a `stop` disk row can be claimed, so park one.
    let parked = insert_unpinned_stop_disk(pool, sandbox, 300).await?;
    sqlx::query("UPDATE snapshots SET state = 'deleting' WHERE id = $1")
        .bind(parked)
        .execute(pool)
        .await?;
    insert_unpinned_stop_disk(pool, sandbox, 100).await?;

    let store = SnapshotRetentionDbStore::new(pool.clone());
    let epoch = store
        .claimed_scan_epoch()
        .await?
        .expect("the parked disk claim opens a recovery epoch");
    assert_eq!(epoch.through, parked);
    let claimed = store.list_claimed(10, None, epoch.through).await?;
    assert_eq!(
        claimed.iter().map(|c| c.snapshot_id).collect::<Vec<_>>(),
        vec![parked]
    );
    Ok(())
}
