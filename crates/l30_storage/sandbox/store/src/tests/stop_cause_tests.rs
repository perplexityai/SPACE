//! The cause [`converge_published_stopped`] records when a node publishes
//! `stopped` for a sandbox nobody asked to stop, and the
//! `sandboxes_clear_stop_cause` trigger that ends it with the stop.

use sqlx::types::Json;

use super::*;
use crate::action_stamp::{StartSubstrate, stamp_started_after_start};
use crate::ingest::converge_published_stopped;

async fn stop_cause(
    pool: &PgPool,
    id: Uuid,
) -> TestResult<(String, Option<String>, Option<time::OffsetDateTime>)> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, stop_cause, stop_cause_at FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

async fn place(pool: &PgPool, id: Uuid, node_id: NodeId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes SET placement = 'on_node', active_node_id = $2 WHERE id = $1"
    ))
    .bind(id)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_published_stop_cause_lasts_until_the_sandbox_runs_again() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let crashed = insert_sandbox(&db.pool, Some("store-crashed"), "started").await?;
    let requested = insert_sandbox(&db.pool, Some("store-requested"), "started").await?;
    for id in [crashed, requested] {
        place(&db.pool, id, node_id).await?;
    }
    let stopped_at = time::macros::datetime!(2026-11-05 12:00:00 UTC);

    let converged = converge_published_stopped(
        &db.pool,
        node_id,
        Json(serde_json::json!([
            {
                "sandbox_id": crashed,
                "published_lifecycle": "stopped",
                "stop_cause": "pod_terminated",
                "stop_cause_at": "2026-11-05T12:00:00Z",
            },
            // A requested stop publishes no cause.
            { "sandbox_id": requested, "published_lifecycle": "stopped", "stop_cause": null },
        ])),
    )
    .await?;
    assert_eq!(converged.len(), 2);
    assert_eq!(
        stop_cause(&db.pool, crashed).await?,
        (
            "stopped".into(),
            Some("pod_terminated".into()),
            Some(stopped_at)
        )
    );
    assert_eq!(
        stop_cause(&db.pool, requested).await?,
        ("stopped".into(), None, None)
    );

    let expected = (
        "stopped".to_owned(),
        Some("pod_terminated".to_owned()),
        Some(stopped_at),
    );
    // A drain's evacuation of the stopped sandbox (`stopped` -> `restoring`
    // -> `stopped`) keeps the cause.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = 'stopped', active_node_id = NULL
          WHERE id = $1"
    ))
    .bind(crashed)
    .execute(&db.pool)
    .await?;
    let claimed = crate::restore_lease::claim_evacuation_restore(
        &db.pool,
        SandboxId::from_uuid(crashed),
        60,
        60,
        Some(pplx_space_common_core::types::SandboxLifecycle::Stopped),
    )
    .await?;
    assert!(
        claimed.is_some(),
        "the evacuation claims the stopped sandbox"
    );
    assert_eq!(stop_cause(&db.pool, crashed).await?.1, expected.1);
    // The drain finalizes it `stopped` on its new node.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'stopped', evacuation_restore_target = NULL
          WHERE id = $1"
    ))
    .bind(crashed)
    .execute(&db.pool)
    .await?;
    place(&db.pool, crashed, node_id).await?;
    assert_eq!(stop_cause(&db.pool, crashed).await?, expected);

    // A start that restores the stopped sandbox elsewhere and ends stopped
    // reports no cause of its own; the sandbox never ran, so it keeps one.
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1"
    ))
    .bind(crashed)
    .execute(&db.pool)
    .await?;
    converge_published_stopped(
        &db.pool,
        node_id,
        Json(
            serde_json::json!([{ "sandbox_id": crashed, "published_lifecycle": "stopped", "stop_cause": null }]),
        ),
    )
    .await?;
    assert_eq!(stop_cause(&db.pool, crashed).await?, expected);

    // The next start ends the stop the cause described.
    assert!(
        stamp_started_after_start(
            &db.pool,
            SandboxId::from_uuid(crashed),
            StartSubstrate::ClaimsRestoring,
        )
        .await?
    );
    assert_eq!(
        stop_cause(&db.pool, crashed).await?,
        ("started".into(), None, None)
    );
    Ok(())
}
