//! The start of a stopped sandbox a v2 drain released
//! ([`KubernetesSubstrate::materialize_unplaced_stopped`]).

use std::time::Duration;

use crate::metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use uuid::Uuid;

use super::*;
use crate::controllers::substrate::ActivationOptions;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn options() -> ActivationOptions {
    ActivationOptions {
        blocking: true,
        ready_timeout: Duration::from_secs(2),
    }
}

/// The row's lifecycle, placement and lease, and its active executions.
async fn landing(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> Result<(String, String, Option<Uuid>, i64), BoxError> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, placement, restore_lease_token,
                (SELECT COUNT(*) FROM sandbox_k8s_executions e
                  WHERE e.sandbox_id = s.id AND e.active)
           FROM sandboxes s
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// A stopped row main's own paths left unplaced with its stored disk pinned
/// (a v1 re-placement landed it, then its pod went) isn't one a v2 drain
/// released: the start doesn't materialize it, writing nothing, and it takes
/// main's start, pinned to the node it last ran on.
#[tokio::test(flavor = "current_thread")]
async fn a_start_leaves_a_stopped_row_main_unplaced_to_mains_path() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    let substrate = kubernetes_tests::substrate_with_store(pool.clone());
    let sandbox_id = SandboxId::new();
    let node_id = pplx_space_accounts::NodeId::new();
    kubernetes_tests::insert_test_sandbox_row(pool, sandbox_id, "stopped").await?;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.1'::INET, 'worker', 'k8s', 'registered')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace,
             k8s_node_name, state, active
         )
         VALUES ($1, 1, 'pod-of-the-re-placement', 'space', 'worker-re-placed', 'running', true)"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    // The re-placement's pod bind stamps the placement clock; the pin is
    // the stored disk it re-placed the sandbox from.
    pplx_space_sandbox_store::wake::record_pod_node_placement(pool, sandbox_id, node_id).await?;
    let artifact = Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2, current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact)
    .execute(pool)
    .await?;
    let snapshot_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind,
             artifact_origin
         )
         VALUES ($1, $2, 'disk', 'ready', NOW(), $3, 'disk', 'stop')"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(artifact)
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
    // Its pod went: the pod teardown retires the execution and unplaces it.
    let mut tx = pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE, state = 'forgotten'
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&mut *tx)
    .await?;
    pplx_space_sandbox_store::pod_tracking::unplace_after_pod_teardown(&mut tx, sandbox_id).await?;
    tx.commit().await?;
    let image = |pool: sqlx::PgPool| async move {
        let row: String = sqlx::query_scalar(sql!(
            "SELECT row_to_json(s)::TEXT FROM sandboxes s WHERE s.id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        Ok::<_, BoxError>(row)
    };
    let before = image(pool.clone()).await?;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(pool, sandbox_id)
        .await?
        .expect("the row exists");

    let started_from = substrate
        .materialize_unplaced_stopped(
            SandboxApiMetricIdentity::native("start"),
            sandbox,
            options(),
        )
        .await?;

    assert_eq!(started_from.lifecycle, "stopped");
    assert_eq!(started_from.active_node_id, None);
    assert_eq!(image(pool.clone()).await?, before, "nothing written");
    assert_eq!(landing(pool, sandbox_id).await?.3, 0, "no execution");
    let source = substrate.store.restore_source_plan(sandbox_id).await?;
    assert_eq!(source.node_name.as_deref(), Some("worker-re-placed"));
    assert!(!source.stopped_evacuation_unplaced);
    Ok(())
}
