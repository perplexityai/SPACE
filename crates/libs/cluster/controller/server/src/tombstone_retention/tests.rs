use pplx_space_common_core::types::{LocalArtifactId, NodeId, SandboxId};

use std::sync::{Arc, Mutex};

use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, insert_pool_slot_receipt, pg_test_db, pool_slot_receipt_state,
    publish_ready_disk_artifact, try_pg_test_db,
};
use pplx_space_placement::test_support::stored_lifecycle_matrix;
use uuid::Uuid;

use super::*;

use pplx_space_test_env::TestResult;

#[derive(Clone, Default)]
struct FakeSink {
    requests: Arc<Mutex<Vec<(Duration, u32)>>>,
    reports: Arc<Mutex<Vec<TickReport>>>,
}

impl FakeSink {
    fn push_report(&self, report: TickReport) {
        self.reports.lock().unwrap().push(report);
    }

    fn requests(&self) -> Vec<(Duration, u32)> {
        self.requests.lock().unwrap().clone()
    }
}

impl RetentionSink for FakeSink {
    async fn prune_tombstones(
        &self,
        retention: Duration,
        max_records: u32,
    ) -> Result<TickReport, sqlx::Error> {
        self.requests.lock().unwrap().push((retention, max_records));
        Ok(self.reports.lock().unwrap().pop().unwrap_or_default())
    }
}

#[tokio::test]
async fn tick_uses_configured_batch_and_retention() {
    let sink = FakeSink::default();
    sink.push_report(TickReport {
        direct_nodes_deleted: 2,
        templates_deleted: 3,
        deleted_sandboxes_deleted: 5,
        direct_node_tombstones: 7,
        template_tombstones: 11,
        deleted_sandbox_rows: 13,
    });
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            tick: Duration::from_secs(1),
            retention: Duration::from_secs(60),
            max_records_per_tick: 17,
        },
        sink.clone(),
    );

    let report = reconciler.tick().await.expect("tick succeeds");

    assert_eq!(
        sink.requests(),
        vec![(Duration::from_secs(60), 17)],
        "configured retention and max batch should be passed to the sink"
    );
    assert_eq!(report.direct_nodes_deleted, 2);
    assert_eq!(report.templates_deleted, 3);
    assert_eq!(report.deleted_sandboxes_deleted, 5);
}

#[test]
fn config_defaults_to_bounded_retention() {
    let config = ReconcilerConfig::default();

    assert_eq!(config.tick, DEFAULT_TICK);
    assert_eq!(config.retention, DEFAULT_RETENTION);
    assert_eq!(config.max_records_per_tick, DEFAULT_MAX_RECORDS_PER_TICK);
}

#[tokio::test(flavor = "current_thread")]
async fn store_prunes_only_expired_direct_node_tombstones() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let old_node = Uuid::now_v7();
    let recent_node = Uuid::now_v7();
    for (node_id, endpoint) in [
        (old_node, "10.0.0.11:9802"),
        (recent_node, "10.0.0.12:9802"),
    ] {
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '10.0.0.1'::INET, 'worker', 'space', 'deleted')"
        ))
        .bind(node_id)
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_direct (node_id, node_api_endpoint, deleted_at)
             VALUES ($1, $2, NOW())"
        ))
        .bind(node_id)
        .bind(endpoint)
        .execute(&db.pool)
        .await?;
    }
    sqlx::query(sql!(
        "UPDATE node_direct SET deleted_at = NOW() - INTERVAL '8 days' WHERE node_id = $1"
    ))
    .bind(old_node)
    .execute(&db.pool)
    .await?;

    let report = TombstoneRetentionStore::new(db.pool.clone())
        .prune_tombstones(Duration::from_secs(7 * 24 * 60 * 60), 100)
        .await?;

    assert_eq!(report.direct_nodes_deleted, 1);
    let old_exists: bool =
        sqlx::query_scalar(sql!("SELECT EXISTS(SELECT 1 FROM nodes WHERE id = $1)"))
            .bind(old_node)
            .fetch_one(&db.pool)
            .await?;
    let recent_exists: bool =
        sqlx::query_scalar(sql!("SELECT EXISTS(SELECT 1 FROM nodes WHERE id = $1)"))
            .bind(recent_node)
            .fetch_one(&db.pool)
            .await?;
    assert!(!old_exists);
    assert!(recent_exists);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_prunes_expired_deleted_template_without_releases_or_inventory() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let old_template_id = Uuid::now_v7();
    let recent_template_id = Uuid::now_v7();
    sqlx::query(
        sql!("INSERT INTO templates (id, name, description, org_id, desired_state, delete_requested_at, updated_at)
         VALUES ($1, 'old-deleted-template', NULL, NULL, 'deleted', NOW() - INTERVAL '8 days', NOW() - INTERVAL '8 days'),
                ($2, 'recent-deleted-template', NULL, NULL, 'deleted', NOW(), NOW())"),
    )
    .bind(old_template_id)
    .bind(recent_template_id)
    .execute(&db.pool)
    .await?;

    let report = TombstoneRetentionStore::new(db.pool.clone())
        .prune_tombstones(Duration::from_secs(7 * 24 * 60 * 60), 100)
        .await?;

    assert_eq!(report.templates_deleted, 1);
    let old_exists: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS(SELECT 1 FROM templates WHERE name = 'old-deleted-template')"
    ))
    .fetch_one(&db.pool)
    .await?;
    let recent_exists: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS(SELECT 1 FROM templates WHERE name = 'recent-deleted-template')"
    ))
    .fetch_one(&db.pool)
    .await?;
    assert!(!old_exists);
    assert!(recent_exists);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_prunes_expired_deleted_sandbox_and_its_warm_pool_receipt() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let old_sandbox = SandboxId::new();
    let recent_sandbox = SandboxId::new();
    for (sandbox_id, aged_interval) in [(old_sandbox, "8 days"), (recent_sandbox, "1 hour")] {
        sqlx::query(sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, managed_by, lifecycle, placement,
                 network_policy_generation, created_at, updated_at
             )
             VALUES (
                 $1,
                 '00000000-0000-0000-0000-000000000010',
                 '00000000-0000-0000-0000-000000000001',
                 'k8s',
                 'deleted',
                 'unplaced',
                 1,
                 NOW() - $2::INTERVAL,
                 NOW() - $2::INTERVAL
             )"
        ))
        .bind(sandbox_id)
        .bind(aged_interval)
        .execute(&db.pool)
        .await?;
        insert_pool_slot_receipt(&db.pool, sandbox_id, "bound").await?;
    }

    let report = TombstoneRetentionStore::new(db.pool.clone())
        .prune_tombstones(Duration::from_secs(7 * 24 * 60 * 60), 100)
        .await?;

    assert_eq!(report.deleted_sandboxes_deleted, 1);
    let old_exists: bool =
        sqlx::query_scalar(sql!("SELECT EXISTS(SELECT 1 FROM sandboxes WHERE id = $1)"))
            .bind(old_sandbox)
            .fetch_one(&db.pool)
            .await?;
    assert!(!old_exists);
    assert_eq!(pool_slot_receipt_state(&db.pool, old_sandbox).await?, None);
    assert_eq!(
        pool_slot_receipt_state(&db.pool, recent_sandbox).await?,
        Some("bound".to_string())
    );
    Ok(())
}

/// A census tombstone keeps its snapshots and their attached storage
/// locations, whose foreign key to `snapshots` is `ON DELETE RESTRICT`. Its
/// prune detaches them for the snapshot-storage sweeper instead of failing the
/// batch it shares with an ordinary tombstone.
#[tokio::test(flavor = "current_thread")]
async fn store_prunes_deleted_sandbox_with_attached_storage_location_in_same_batch() -> TestResult {
    let db = pg_test_db().await?;
    let mut sandboxes = Vec::new();
    for _ in 0..2 {
        let sandbox_id = SandboxId::new();
        sqlx::query(sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, managed_by, lifecycle, placement,
                 network_policy_generation, created_at, updated_at
             )
             VALUES (
                 $1, $2, $3, 'space', 'deleted', 'unplaced', 1,
                 NOW() - INTERVAL '8 days', NOW() - INTERVAL '8 days'
             )"
        ))
        .bind(sandbox_id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .execute(&db.pool)
        .await?;
        sandboxes.push(sandbox_id);
    }
    publish_ready_disk_artifact(
        &db.pool,
        sandboxes[0],
        LocalArtifactId::new(),
        "s3://snapshots/tombstone-with-location",
    )
    .await?;
    // The fixture's pin update touched `updated_at`; age the row past
    // retention again.
    sqlx::query(sql!(
        "UPDATE sandboxes SET updated_at = NOW() - INTERVAL '8 days' WHERE id = $1"
    ))
    .bind(sandboxes[0])
    .execute(&db.pool)
    .await?;
    // Rows outside the candidate set keep their attached locations: a
    // tombstone still inside retention and an aged row in another lifecycle.
    let mut kept = Vec::new();
    for (lifecycle, age, storage_uri) in [
        (
            "deleted",
            "1 day",
            "s3://snapshots/tombstone-inside-retention",
        ),
        ("stopped", "8 days", "s3://snapshots/stopped-past-retention"),
    ] {
        let sandbox_id = SandboxId::new();
        sqlx::query(sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, managed_by, lifecycle, placement,
                 network_policy_generation
             )
             VALUES ($1, $2, $3, 'space', $4, 'unplaced', 1)"
        ))
        .bind(sandbox_id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(lifecycle)
        .execute(&db.pool)
        .await?;
        publish_ready_disk_artifact(&db.pool, sandbox_id, LocalArtifactId::new(), storage_uri)
            .await?;
        sqlx::query(sql!(
            "UPDATE sandboxes SET updated_at = NOW() - $2::INTERVAL WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(age)
        .execute(&db.pool)
        .await?;
        kept.push((lifecycle, sandbox_id, storage_uri));
    }

    let report = TombstoneRetentionStore::new(db.pool.clone())
        .prune_tombstones(Duration::from_secs(7 * 24 * 60 * 60), 100)
        .await;

    let remaining: Vec<SandboxId> = sqlx::query_scalar(sql!(
        "SELECT id FROM sandboxes WHERE id = ANY($1) ORDER BY id"
    ))
    .bind(&sandboxes)
    .fetch_all(&db.pool)
    .await?;
    let location: (Option<Uuid>, bool) = sqlx::query_as(sql!(
        "SELECT snapshot_id, orphaned_at IS NOT NULL
           FROM snapshot_storage_locations
          WHERE storage_connection_string = 's3://snapshots/tombstone-with-location'"
    ))
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        remaining,
        Vec::<SandboxId>::new(),
        "both expired tombstones are pruned; prune returned {report:?}"
    );
    assert_eq!(report?.deleted_sandboxes_deleted, 2);
    assert_eq!(
        location,
        (None, true),
        "the location is detached and stamped for the sweeper"
    );
    // (lifecycle, row exists, location attached, location orphaned)
    let mut kept_states = Vec::new();
    for (lifecycle, sandbox_id, storage_uri) in kept {
        let exists: bool =
            sqlx::query_scalar(sql!("SELECT EXISTS(SELECT 1 FROM sandboxes WHERE id = $1)"))
                .bind(sandbox_id)
                .fetch_one(&db.pool)
                .await?;
        let (snapshot_id, orphaned): (Option<Uuid>, bool) = sqlx::query_as(sql!(
            "SELECT snapshot_id, orphaned_at IS NOT NULL
               FROM snapshot_storage_locations
              WHERE storage_connection_string = $1"
        ))
        .bind(storage_uri)
        .fetch_one(&db.pool)
        .await?;
        kept_states.push((lifecycle, exists, snapshot_id.is_some(), orphaned));
    }
    assert_eq!(
        kept_states,
        [
            ("deleted", true, true, false),
            ("stopped", true, true, false)
        ],
        "rows outside the candidates remain, their locations attached and unstamped"
    );
    Ok(())
}

/// The sandbox prune deletes only `deleted` tombstones: a row past retention
/// in any other stored lifecycle survives. `A`: pruned; `.`: kept.
#[tokio::test(flavor = "current_thread")]
async fn store_prunes_only_expired_deleted_sandboxes_of_every_stored_lifecycle() -> TestResult {
    const EXPECTED: &[&str] = &[
        "created           .",
        "creating          .",
        "starting          .",
        "started           .",
        "restoring         .",
        "pausing           .",
        "paused            .",
        "resuming          .",
        "suspending        .",
        "suspended         .",
        "stopping          .",
        "stopped           .",
        "deleting          .",
        "checkpointing     .",
        "quarantined       .",
        "failed            .",
        "deleted           A",
        "cleanup_required  .",
        "bogus             .",
        "_Suspended_       .",
    ];
    let db = pg_test_db().await?;
    let matrix = stored_lifecycle_matrix(["prune"], EXPECTED);
    let mut rows = Vec::new();
    for &(stored, [(_, pruned)]) in &matrix {
        let pruned = pruned.applies();
        let sandbox_id = SandboxId::new();
        sqlx::query(sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, managed_by, lifecycle, placement,
                 network_policy_generation, created_at, updated_at
             )
             VALUES (
                 $1, $2, $3, 'k8s', $4, 'unplaced', 1,
                 NOW() - INTERVAL '8 days', NOW() - INTERVAL '8 days'
             )"
        ))
        .bind(sandbox_id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(stored)
        .execute(&db.pool)
        .await?;
        rows.push((stored, sandbox_id, pruned));
    }

    let report = TombstoneRetentionStore::new(db.pool.clone())
        .prune_tombstones(Duration::from_secs(7 * 24 * 60 * 60), 100)
        .await?;

    let mut mismatches = Vec::new();
    for (stored, sandbox_id, pruned) in rows {
        let exists: bool =
            sqlx::query_scalar(sql!("SELECT EXISTS(SELECT 1 FROM sandboxes WHERE id = $1)"))
                .bind(sandbox_id)
                .fetch_one(&db.pool)
                .await?;
        if exists == pruned {
            mismatches.push(format!("{stored}: row exists {exists}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    let expected_pruned = matrix
        .iter()
        .filter(|(_, [(_, pruned)])| pruned.applies())
        .count();
    assert_eq!(report.deleted_sandboxes_deleted, expected_pruned as u64);
    Ok(())
}

/// The direct-node tombstone prune must refuse a node whose only pinning
/// sandbox points at it via `target_node_id` (the at-rest case: suspended,
/// active NULL) — pinning the target half of the split placement gate — and
/// prune once that sandbox goes terminal.
#[tokio::test(flavor = "current_thread")]
async fn store_keeps_direct_tombstone_pinned_only_by_target_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.1'::INET, 'worker', 'space', 'deleted')"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, node_api_endpoint, deleted_at)
         VALUES ($1, '10.0.0.13:9802', NOW() - INTERVAL '8 days')"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle,
             active_node_id, target_node_id, network_policy_generation
         )
         VALUES ($1, $2::uuid, $3::uuid, 'space', 'suspended', NULL, $4, 1)"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    let store = TombstoneRetentionStore::new(db.pool.clone());
    let retention = Duration::from_secs(7 * 24 * 60 * 60);
    let report = store.prune_tombstones(retention, 100).await?;
    assert_eq!(
        report.direct_nodes_deleted, 0,
        "target-only-pinned tombstone must survive the prune"
    );

    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'deleted' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let report = store.prune_tombstones(retention, 100).await?;
    assert_eq!(
        report.direct_nodes_deleted, 1,
        "terminal pinning releases the tombstone"
    );
    Ok(())
}

/// The direct-node prune and its tombstone count both bind the direct (`space`)
/// substrate: an expired tombstone shape on a Kubernetes node row is neither
/// deleted nor counted.
#[tokio::test(flavor = "current_thread")]
async fn store_prunes_and_counts_only_direct_node_tombstones() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let direct_node = Uuid::now_v7();
    let k8s_node = Uuid::now_v7();
    for (node_id, managed_by, endpoint) in [
        (direct_node, "space", "10.0.0.21:9802"),
        (k8s_node, "k8s", "10.0.0.22:9802"),
    ] {
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '10.0.0.1'::INET, 'worker', $2, 'deleted')"
        ))
        .bind(node_id)
        .bind(managed_by)
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_direct (node_id, node_api_endpoint, deleted_at)
             VALUES ($1, $2, NOW() - INTERVAL '8 days')"
        ))
        .bind(node_id)
        .bind(endpoint)
        .execute(&db.pool)
        .await?;
    }

    let report = TombstoneRetentionStore::new(db.pool.clone())
        .prune_tombstones(Duration::from_secs(7 * 24 * 60 * 60), 100)
        .await?;

    assert_eq!(report.direct_nodes_deleted, 1);
    assert_eq!(report.direct_node_tombstones, 0);
    let k8s_exists: bool =
        sqlx::query_scalar(sql!("SELECT EXISTS(SELECT 1 FROM nodes WHERE id = $1)"))
            .bind(k8s_node)
            .fetch_one(&db.pool)
            .await?;
    assert!(k8s_exists);
    Ok(())
}
