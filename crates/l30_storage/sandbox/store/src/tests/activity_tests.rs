//! [`advance_published_activity`]'s write throttle: a published clock less
//! than a minute past the stored one is dropped, unless a full snapshot falls
//! between them. Recovery compares these clocks with a snapshot's timestamp to
//! decide whether restoring it drops activity, so a dropped value must never
//! move activity from after a snapshot to before it.

use sqlx::types::Json;

use super::*;
use crate::ingest::advance_published_activity;

const STORED: &str = "2026-01-01T00:00:00Z";
const PUBLISHED: &str = "2026-01-01T00:00:30Z";

async fn placed_sandbox(pool: &PgPool) -> TestResult<(Uuid, NodeId)> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.10', 'worker', 'k8s', 'ready')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    let sandbox = insert_sandbox(pool, None, "started").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = $2,
                last_access_at = $3::TIMESTAMPTZ,
                last_command_at = $3::TIMESTAMPTZ,
                last_used_at = $3::TIMESTAMPTZ
          WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(node_id)
    .bind(STORED)
    .execute(pool)
    .await?;
    Ok((sandbox, node_id))
}

async fn publish(pool: &PgPool, node_id: NodeId, sandbox: Uuid, clock: &str) -> TestResult {
    advance_published_activity(
        pool,
        node_id,
        Json(serde_json::json!([{
            "sandbox_id": sandbox,
            "last_used_at": clock,
            "last_access_at": clock,
            "last_command_at": clock,
        }])),
    )
    .await?;
    Ok(())
}

/// Whether `last_access_at` and `last_command_at` both read `at`.
async fn clocks_read(pool: &PgPool, sandbox: Uuid, at: &str) -> TestResult<bool> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT last_access_at = $2::TIMESTAMPTZ AND last_command_at = $2::TIMESTAMPTZ
           FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(at)
    .fetch_one(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn a_throttled_clock_still_advances_across_a_snapshot() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (sandbox, node_id) = placed_sandbox(&db.pool).await?;
    // A suspend 10 s after the stored activity, and activity 20 s after the
    // suspend: within the minute, but on the other side of the snapshot.
    insert_full_snapshot(
        &db.pool,
        sandbox,
        Uuid::now_v7(),
        "ready",
        "2026-01-01T00:00:10Z",
        false,
    )
    .await?;

    publish(&db.pool, node_id, sandbox, PUBLISHED).await?;

    assert!(clocks_read(&db.pool, sandbox, PUBLISHED).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_throttled_clock_with_no_snapshot_between_is_still_dropped() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (sandbox, node_id) = placed_sandbox(&db.pool).await?;
    // Snapshots on either side of the window don't count: one before the
    // stored value, and one after the published value.
    for timestamp in ["2025-12-31T23:59:50Z", "2026-01-01T00:00:40Z"] {
        insert_full_snapshot(&db.pool, sandbox, Uuid::now_v7(), "ready", timestamp, false).await?;
    }

    publish(&db.pool, node_id, sandbox, PUBLISHED).await?;

    assert!(clocks_read(&db.pool, sandbox, STORED).await?);
    Ok(())
}

/// The window is the readers' own comparison, `sn.timestamp >= clock`: a
/// snapshot taken at the stored activity's instant is after it, so newer
/// activity crosses it; one taken at the published activity's instant isn't
/// before that activity, so nothing crosses it.
#[tokio::test(flavor = "current_thread")]
async fn a_snapshot_on_either_edge_of_the_throttle_window() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    for (snapshot_at, advances) in [(STORED, true), (PUBLISHED, false)] {
        let (sandbox, node_id) = placed_sandbox(&db.pool).await?;
        insert_full_snapshot(
            &db.pool,
            sandbox,
            Uuid::now_v7(),
            "ready",
            snapshot_at,
            false,
        )
        .await?;

        publish(&db.pool, node_id, sandbox, PUBLISHED).await?;

        let expected = if advances { PUBLISHED } else { STORED };
        assert!(
            clocks_read(&db.pool, sandbox, expected).await?,
            "snapshot at {snapshot_at}"
        );
    }
    Ok(())
}
