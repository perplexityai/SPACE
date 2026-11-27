//! The restore candidate reads against Postgres, with the workers'
//! `node_direct` rows seeded: both shapes of each statement.
//!
//! The sandbox entered `restoring` between two full snapshots: a ready one
//! its nodes can load, from before the claim, and a newer failed one they
//! can't, from after it. A fresh restore reads the newest; a retry reads the
//! pre-claim source. The remote leg restores the capture the shared rule
//! selects for the mode.

use pplx_space_accounts::{NodeId, OrgId, UserId};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDbResult, insert_suspended_sandbox, pg_test_db,
};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::PgPool;
use uuid::Uuid;

use super::{RestoreCandidateRow, source_candidates};
use crate::restore_source::{
    RestoreSourceMode, RestoreSourcePlacement, RestoreSourceSelection, select_restore_source,
};

const STORAGE_URI: &str = "s3://bucket/snapshots/sandbox/source";

/// A registered, reachable, ready Direct worker that can load
/// `firecracker-snapshot` 9.x on `fc` 1.15.1 for linux/aarch64.
async fn insert_ready_worker(pool: &PgPool, node_id: NodeId, endpoint: &str) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'space', 'registered')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, node_api_endpoint) VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(endpoint)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, role,
             scheduling_status, health_healthy, services, updated_at
         )
         VALUES (
             $1, NOW(), NOW() + INTERVAL '1 hour', TRUE, 'worker',
             'ready', TRUE, '[]'::JSONB, NOW()
         )"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_runtime_installations (node_id, runtime, runtime_version)
         VALUES ($1, 'fc', '1.15.1')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_runtime_guests
             (node_id, runtime, runtime_version, guest_os, guest_arch)
         VALUES ($1, 'fc', '1.15.1', 'linux', 'aarch64')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_runtime_state_formats (
             node_id, runtime, runtime_version, state_format_name,
             state_format_version, can_create, can_load
         )
         VALUES ($1, 'fc', '1.15.1', 'firecracker-snapshot', '9.0.0', TRUE, TRUE)"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// A full snapshot of `sandbox_id` from `source_node_id`, taken
/// `offset_ms` after the row entered its lifecycle.
async fn insert_full_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    state: &str,
    state_format_version: &str,
    offset_ms: i64,
) -> TestDbResult<Uuid> {
    let snapshot_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, source_node_id, org_id, timestamp,
             template_id, content_hash, runtime, runtime_version, guest_os,
             guest_arch, state_format_name, state_format_version
         )
         SELECT $1, s.id, 'full', $3, $4, s.org_id,
                s.lifecycle_changed_at + make_interval(secs => $6),
                'seed', 'hash-seed', 'fc', '1.15.1', 'linux',
                'aarch64', 'firecracker-snapshot', $5
           FROM sandboxes s
          WHERE s.id = $2"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(state)
    .bind(source_node_id)
    .bind(state_format_version)
    .bind(offset_ms as f64 / 1000.0)
    .execute(pool)
    .await?;
    Ok(snapshot_id)
}

struct Seeded {
    sandbox_id: SandboxId,
    source: NodeId,
    alternate: NodeId,
    after_claim: Uuid,
}

async fn seed(pool: &PgPool) -> TestDbResult<Seeded> {
    let (source, alternate) = (NodeId::new(), NodeId::new());
    insert_ready_worker(pool, source, "http://10.0.0.1:9800").await?;
    insert_ready_worker(pool, alternate, "http://10.0.0.2:9800").await?;
    let sandbox_id = insert_suspended_sandbox(
        pool,
        OrgId::from_uuid(SEED_ORG_ID),
        UserId::from_uuid(SEED_USER_ID),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'restoring',
                placement = 'unplaced',
                placement_changed_at = NULL,
                active_node_id = NULL,
                target_node_id = $2
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(source)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, snapshots_enabled)
         VALUES ($1, $2, 'suspended', TRUE)"
    ))
    .bind(sandbox_id)
    .bind(source)
    .execute(pool)
    .await?;
    let before_claim =
        insert_full_snapshot(pool, sandbox_id, source, "ready", "9.0.0", -2_000).await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, $3)"
    ))
    .bind(before_claim)
    .bind(STORAGE_URI)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    let after_claim =
        insert_full_snapshot(pool, sandbox_id, source, "failed", "10.0.0", 1_000).await?;
    Ok(Seeded {
        sandbox_id,
        source,
        alternate,
        after_claim,
    })
}

/// The remote candidates for the capture the shared rule selects, as the
/// Direct restore asks for them; none when it selects nothing.
async fn remote_candidates(
    pool: &PgPool,
    sandbox_id: SandboxId,
    restore_retry: bool,
    limit: i64,
    environment_refuses: bool,
) -> TestDbResult<Vec<RestoreCandidateRow>> {
    let mode = RestoreSourceMode::or_retry(RestoreSourceMode::NewestFull, restore_retry);
    let selection =
        select_restore_source(pool, sandbox_id, mode, RestoreSourcePlacement::Any).await?;
    let RestoreSourceSelection::Selected(selected) = selection else {
        return Ok(vec![]);
    };
    Ok(super::remote_candidates(pool, sandbox_id, &selected, limit, environment_refuses).await?)
}

#[tokio::test(flavor = "current_thread")]
async fn each_mode_reads_its_own_source_snapshot() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let seeded = seed(pool).await?;
    let source_row = RestoreCandidateRow {
        id: seeded.source,
        node_api_endpoint: "http://10.0.0.1:9800".to_owned(),
        storage_uri: None,
        source_node_id: Some(seeded.source),
        environment_refusal: None,
    };
    let remote_row = RestoreCandidateRow {
        id: seeded.alternate,
        node_api_endpoint: "http://10.0.0.2:9800".to_owned(),
        storage_uri: Some(STORAGE_URI.to_owned()),
        source_node_id: Some(seeded.source),
        environment_refusal: None,
    };
    let id = seeded.sandbox_id;

    // Fresh: the newest snapshot is the failed one no node can load, and it
    // also leaves the older ready copy stale.
    assert_eq!(source_candidates(pool, id, false, false).await?, vec![]);
    assert_eq!(remote_candidates(pool, id, false, 7, false).await?, vec![]);

    // Retry: the pre-claim source qualifies both legs.
    assert_eq!(
        source_candidates(pool, id, true, false).await?,
        vec![source_row.clone()]
    );
    assert_eq!(
        remote_candidates(pool, id, true, 7, false).await?,
        vec![remote_row.clone()]
    );
    assert_eq!(remote_candidates(pool, id, true, 0, false).await?, vec![]);

    // Without the post-claim attempt the ready copy is the newest, and the
    // fresh statements agree with the retry ones.
    sqlx::query(sql!("DELETE FROM snapshots WHERE id = $1"))
        .bind(seeded.after_claim)
        .execute(pool)
        .await?;
    for restore_retry in [false, true] {
        assert_eq!(
            source_candidates(pool, id, restore_retry, false).await?,
            vec![source_row.clone()],
            "restore_retry = {restore_retry}"
        );
        assert_eq!(
            remote_candidates(pool, id, restore_retry, 7, false).await?,
            vec![remote_row.clone()],
            "restore_retry = {restore_retry}"
        );
    }
    Ok(())
}
