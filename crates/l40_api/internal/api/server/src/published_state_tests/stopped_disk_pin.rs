//! The status ingest's projection of the stopped disk pin
//! ([`publish_sandbox_states_under_drain`]): only for a node whose drain is
//! v2, never from a degraded publication, and with no statement of its own
//! for any other node.

use pplx_space_node_model::NodeDrain;
use pplx_space_sandbox_model::AbandonedRestoreRetirement;

use super::*;

const NODE_NAME: &str = "worker-ingest.synthetic.internal";

/// A worker node named [`NODE_NAME`] with a v2 drain's engine.
async fn v2_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;
    sqlx::query(sql!(
        "UPDATE nodes SET drain_engine = 'v2', drain_phase = 'draining' WHERE id = $1"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(NODE_NAME)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// A sandbox stopped an hour ago at home on `node_id`, its execution there
/// (generation 1, `pod-uid-1`) created before that, and a ready, stored disk
/// taken since: the artifact the node publishes.
async fn stopped_resident(pool: &PgPool, node_id: NodeId) -> TestResult<(SandboxId, Uuid)> {
    let sandbox_id = SandboxId::new();
    insert_active_sandbox(pool, sandbox_id, node_id, "stopped").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle_changed_at = NOW() - INTERVAL '1 hour' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    insert_k8s_execution(pool, sandbox_id, 1, "running", None).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET k8s_node_name = $2, created_at = NOW() - INTERVAL '2 hours'
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .bind(NODE_NAME)
    .execute(pool)
    .await?;
    let artifact = Uuid::now_v7();
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
    Ok((sandbox_id, artifact))
}

/// What the node publishes for the stopped resident: stopped, its disk
/// artifact, from execution generation 1.
fn published_stopped(sandbox_id: SandboxId, artifact: Uuid) -> PublishedSandboxState {
    let mut published = published_sandbox_state(sandbox_id, "stopped");
    published.execution_generation = Some(1);
    published.pod_uid = Some("pod-uid-1".to_string());
    published.current_local_artifact_id = Some(artifact.to_string());
    published.current_local_artifact_kind =
        Some(pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk as i32);
    published.current_local_artifact_origin =
        Some(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Stop as i32);
    published
}

async fn pin(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Option<Uuid>> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT current_local_artifact_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

async fn row_image(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<serde_json::Value> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT to_jsonb(s) - 'updated_at' FROM sandboxes s WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// Publish `states` from `node_id` under `drain` in one transaction, and
/// return every statement it sent, as the text sqlx logs.
async fn publish(
    pool: &PgPool,
    node_id: NodeId,
    drain: Option<NodeDrain>,
    states: &[PublishedSandboxState],
) -> TestResult<Vec<String>> {
    use pplx_space_observability::test_capture::{CapturedEventsLayer, field_value};
    use tracing_subscriber::layer::SubscriberExt;

    static ALWAYS_ENABLED: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();
    let layer = CapturedEventsLayer::new().with_target("sqlx::query");
    let events = layer.events();
    {
        ALWAYS_ENABLED.get_or_init(|| tracing::Dispatch::new(tracing_subscriber::registry()));
        let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        tracing::callsite::rebuild_interest_cache();
        let mut tx = pool.begin().await?;
        publish_sandbox_states_under_drain(
            &mut tx,
            node_id,
            drain,
            states,
            AbandonedRestoreRetirement::Disabled,
        )
        .await?;
        tx.commit().await?;
    }
    Ok(events
        .events()
        .iter()
        .map(|event| {
            let statement = field_value(event, "db.statement").unwrap_or_default();
            if statement.is_empty() {
                field_value(event, "summary").unwrap_or_default().to_owned()
            } else {
                statement.to_owned()
            }
        })
        // sqlx's own type lookups, which it sends the first time a
        // connection meets a type, aren't the ingest's.
        .filter(|statement| !statement.contains("pg_type"))
        .collect())
}

/// A v2 drain, draining, as the call's origin read builds it.
fn v2_draining() -> Option<NodeDrain> {
    NodeDrain::from_columns(Some("v2"), Some("draining"), None)
}

/// A v2-drained node's stopped resident takes the disk it publishes as its
/// pin, in one statement of the ingest's own; a degraded publication of the
/// same is never projected.
#[tokio::test(flavor = "current_thread")]
async fn a_v2_nodes_stopped_publication_projects_the_disk_pin() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = v2_node(pool).await?;

    let (degraded, artifact) = stopped_resident(pool, node_id).await?;
    let mut published = published_stopped(degraded, artifact);
    published.state_degraded = true;
    publish(pool, node_id, v2_draining(), &[published]).await?;
    assert_eq!(
        pin(pool, degraded).await?,
        None,
        "a degraded publication pinned"
    );

    let (sandbox_id, artifact) = stopped_resident(pool, node_id).await?;
    let sent = publish(
        pool,
        node_id,
        v2_draining(),
        &[published_stopped(sandbox_id, artifact)],
    )
    .await?;
    assert_eq!(pin(pool, sandbox_id).await?, Some(artifact));
    let projections = sent
        .iter()
        .filter(|statement| statement.contains("drain_restore"))
        .count();
    assert_eq!(projections, 1, "{sent:#?}");
    Ok(())
}

/// For a node whose drain isn't v2 (none, or a v1 drain), the ingest is
/// exactly what it was: the same statements as a publication with no drain
/// read, none of them the projection, and the row byte for byte as that
/// publication leaves it. A v2 drain adds the one projection statement.
#[tokio::test(flavor = "current_thread")]
async fn a_publication_from_a_node_without_a_v2_drain_is_unchanged() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let v1 = NodeDrain::from_columns(None, Some("draining"), Some("forced"));
    let mut baseline: Option<(Vec<String>, serde_json::Value)> = None;
    for (case, drain) in [("no drain read", None), ("a v1 drain", v1)] {
        // The node's row has the v2 engine, so only the drain the call read
        // decides whether the ingest projects anything.
        let node_id = v2_node(pool).await?;
        let (sandbox_id, artifact) = stopped_resident(pool, node_id).await?;
        let sent = publish(
            pool,
            node_id,
            drain,
            &[published_stopped(sandbox_id, artifact)],
        )
        .await?;
        assert_eq!(pin(pool, sandbox_id).await?, None, "{case}");
        assert!(
            !sent
                .iter()
                .any(|statement| statement.contains("drain_restore")),
            "{case}: {sent:#?}"
        );
        // Ids and clocks differ per row; compare the row's shape apart from
        // them, and the statements sent.
        let mut image = row_image(pool, sandbox_id).await?;
        if let Some(image) = image.as_object_mut() {
            for column in [
                "id",
                "active_node_id",
                "target_node_id",
                "created_at",
                "lifecycle_changed_at",
                "placement_changed_at",
                "last_seen_at",
            ] {
                image.remove(column);
            }
        }
        match &baseline {
            None => baseline = Some((sent, image)),
            Some((sent_before, image_before)) => {
                assert_eq!(&sent, sent_before, "{case}: statements");
                assert_eq!(&image, image_before, "{case}: row");
            }
        }
    }
    let node_id = v2_node(pool).await?;
    let (sandbox_id, artifact) = stopped_resident(pool, node_id).await?;
    let sent = publish(
        pool,
        node_id,
        v2_draining(),
        &[published_stopped(sandbox_id, artifact)],
    )
    .await?;
    let (baseline, _) = baseline.ok_or("baseline")?;
    assert_eq!(sent.len(), baseline.len() + 1, "{sent:#?}");
    Ok(())
}
