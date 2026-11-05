//! The node's stop attribution as [`record_published_stop_attributions`]
//! applies it, after [`converge_published_stopped`] as the publish ingest
//! runs them, and the `sandboxes_clear_stop_cause` trigger that ends it when
//! the sandbox runs again.

use sqlx::types::Json;

use super::*;
use crate::action_stamp::{
    ActionStamp, StartSubstrate, stamp_action_lifecycle, stamp_started_after_start,
};
use crate::ingest::{converge_published_stopped, record_published_stop_attributions};

type Stored = (String, Option<String>, Option<time::OffsetDateTime>);

const T1: time::OffsetDateTime = time::macros::datetime!(2026-11-05 12:00:00.100 UTC);
const T2: time::OffsetDateTime = time::macros::datetime!(2026-11-05 12:05:00.200 UTC);

async fn stored(pool: &PgPool, id: Uuid) -> TestResult<Stored> {
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

fn rfc3339(at: time::OffsetDateTime) -> String {
    at.format(&time::format_description::well_known::Rfc3339)
        .unwrap()
}

/// One node publication of `stopped`, as the ingest applies it: the lifecycle
/// convergence, then the attribution statement, returning the ids the
/// attribution statement wrote. `attribution` is `None` for an older node,
/// whose row carries no attribution fields, and `Some((None, at))` for a
/// requested stop.
async fn publish_stopped(
    pool: &PgPool,
    node_id: NodeId,
    id: Uuid,
    attribution: Option<(Option<&str>, time::OffsetDateTime)>,
) -> TestResult<Vec<SandboxId>> {
    converge_published_stopped(
        pool,
        node_id,
        Json(serde_json::json!([{ "sandbox_id": id, "published_lifecycle": "stopped" }])),
    )
    .await?;
    let rows = match attribution {
        Some((cause, at)) => serde_json::json!([{
            "sandbox_id": id,
            "published_lifecycle": "stopped",
            "stop_cause": cause,
            "stop_cause_at": rfc3339(at),
        }]),
        None => serde_json::json!([{ "sandbox_id": id, "published_lifecycle": "stopped" }]),
    };
    Ok(record_published_stop_attributions(pool, node_id, Json(rows)).await?)
}

async fn placed(pool: &PgPool, node_id: NodeId, name: &str, lifecycle: &str) -> TestResult<Uuid> {
    let id = insert_sandbox(pool, Some(name), lifecycle).await?;
    place(pool, id, node_id).await?;
    Ok(id)
}

#[tokio::test(flavor = "current_thread")]
async fn a_published_stop_attribution_is_applied_on_its_own() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node_id = NodeId::new();
    let vm_exited = |at| ("stopped".to_owned(), Some("vm_exited".to_owned()), Some(at));

    // The node reports the crash with the stopped status: converged, then
    // attributed.
    let crashed = placed(pool, node_id, "store-crashed", "started").await?;
    publish_stopped(pool, node_id, crashed, Some((Some("vm_exited"), T1))).await?;
    assert_eq!(stored(pool, crashed).await?, vm_exited(T1));

    // The VM exits, and an operator's Stop is stamped `stopped` before the
    // node publishes: the convergence skips the row, the attribution doesn't.
    let stamped = placed(pool, node_id, "store-stamped", "started").await?;
    assert!(stamp_action_lifecycle(pool, SandboxId::from_uuid(stamped), ActionStamp::Stop).await?);
    publish_stopped(pool, node_id, stamped, Some((Some("vm_exited"), T1))).await?;
    assert_eq!(stored(pool, stamped).await?, vm_exited(T1));

    // An earlier crash, then a start the operator stops before it is ready:
    // the row never reached `started`, and the node's newer attribution of
    // the requested stop clears the cause.
    let restarted = placed(pool, node_id, "store-restarted", "started").await?;
    publish_stopped(pool, node_id, restarted, Some((Some("vm_exited"), T1))).await?;
    publish_stopped(pool, node_id, restarted, Some((None, T2))).await?;
    assert_eq!(
        stored(pool, restarted).await?,
        ("stopped".to_owned(), None, Some(T2))
    );

    // The same attribution republished, an older one, and an older node's
    // report of the stopped sandbox without one each write nothing.
    let written = |ids: Vec<SandboxId>| !ids.is_empty();
    assert!(!written(
        publish_stopped(pool, node_id, crashed, Some((Some("vm_exited"), T1))).await?
    ));
    assert!(!written(
        publish_stopped(
            pool,
            node_id,
            crashed,
            Some((None, T1 - time::Duration::seconds(1))),
        )
        .await?
    ));
    assert!(!written(
        publish_stopped(pool, node_id, crashed, None).await?
    ));
    assert_eq!(stored(pool, crashed).await?, vm_exited(T1));
    // Nor where nothing is stored: a row an older node stopped stays without
    // an attribution.
    let unattributed = placed(pool, node_id, "store-unattributed", "stopped").await?;
    assert!(!written(
        publish_stopped(pool, node_id, unattributed, None).await?
    ));
    assert_eq!(
        stored(pool, unattributed).await?,
        ("stopped".to_owned(), None, None)
    );

    // Another node's report, and any report for a running row, write nothing.
    let running = placed(pool, node_id, "store-running", "started").await?;
    record_published_stop_attributions(
        pool,
        node_id,
        Json(serde_json::json!([{
            "sandbox_id": running,
            "published_lifecycle": "stopped",
            "stop_cause": "vm_exited",
            "stop_cause_at": rfc3339(T1),
        }])),
    )
    .await?;
    assert_eq!(
        stored(pool, running).await?,
        ("started".to_owned(), None, None)
    );
    publish_stopped(pool, NodeId::new(), crashed, Some((None, T2))).await?;
    assert_eq!(stored(pool, crashed).await?, vm_exited(T1));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_stop_attribution_lasts_until_the_sandbox_runs_again() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node_id = NodeId::new();
    let crashed = placed(pool, node_id, "store-evacuated", "started").await?;
    publish_stopped(pool, node_id, crashed, Some((Some("pod_terminated"), T1))).await?;
    let expected = (
        "stopped".to_owned(),
        Some("pod_terminated".to_owned()),
        Some(T1),
    );

    // A drain's evacuation of the stopped sandbox (`stopped` -> `restoring`
    // -> `stopped`) keeps it.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = 'stopped', active_node_id = NULL
          WHERE id = $1"
    ))
    .bind(crashed)
    .execute(pool)
    .await?;
    let claimed = crate::restore_lease::claim_evacuation_restore(
        pool,
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
    assert_eq!(stored(pool, crashed).await?.1, expected.1);
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'stopped', evacuation_restore_target = NULL
          WHERE id = $1"
    ))
    .bind(crashed)
    .execute(pool)
    .await?;
    let destination = NodeId::new();
    place(pool, crashed, destination).await?;
    assert_eq!(stored(pool, crashed).await?, expected);

    // On its new node, that node's newer attribution applies; the old node's
    // report, even a newer one, no longer does.
    let requested = ("stopped".to_owned(), None, Some(T2));
    assert!(
        !publish_stopped(pool, destination, crashed, Some((None, T2)))
            .await?
            .is_empty()
    );
    assert_eq!(stored(pool, crashed).await?, requested);
    let later = T2 + time::Duration::minutes(1);
    assert!(
        publish_stopped(pool, node_id, crashed, Some((Some("vm_exited"), later)))
            .await?
            .is_empty()
    );
    assert_eq!(stored(pool, crashed).await?, requested);

    // The next start ends the stop it described.
    assert!(
        stamp_started_after_start(
            pool,
            SandboxId::from_uuid(crashed),
            StartSubstrate::ClaimsRestoring,
        )
        .await?
    );
    assert_eq!(
        stored(pool, crashed).await?,
        ("started".to_owned(), None, None)
    );
    Ok(())
}
