//! The terminating node's delete against Postgres: it refuses a row outside
//! `terminating`, a v2 drain's row and a Direct row, writing nothing, and
//! deletes a v1 drain's terminating Kubernetes row with its registration.

use pplx_space_db_test_fixtures::{TestDbResult, pg_test_db};
use sqlx::PgPool;

use super::*;

/// A registered node in `drain_phase`, under `engine` and `managed_by`.
async fn node(
    pool: &PgPool,
    drain_phase: &str,
    engine: Option<&str>,
    managed_by: SubstrateKind,
) -> TestDbResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes
            (id, address, role, managed_by, state, drain_phase, drain_started_at,
             drain_engine, drain_status)
         VALUES ($1, '10.0.0.9'::INET, 'worker', $2, 'registered', $3, NOW(), $4,
                 CASE WHEN $4::TEXT IS NOT NULL
                      THEN '{\"generation\": 1, \"deadline_at\": 0}'::JSONB END)"
    ))
    .bind(node_id)
    .bind(managed_by)
    .bind(drain_phase)
    .bind(engine)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, $2, 'uid-synthetic')"
    ))
    .bind(node_id)
    .bind(format!(
        "worker-{}.synthetic.internal",
        node_id.as_uuid().simple()
    ))
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// Whether the node's `nodes` and `node_k8s` rows exist.
async fn rows(pool: &PgPool, node_id: NodeId) -> TestDbResult<(bool, bool)> {
    Ok(sqlx::query_as(sql!(
        "SELECT EXISTS (SELECT 1 FROM nodes WHERE id = $1),
                EXISTS (SELECT 1 FROM node_k8s WHERE node_id = $1)"
    ))
    .bind(node_id)
    .fetch_one(pool)
    .await?)
}

#[tokio::test]
async fn delete_terminating_k8s_node_refuses_all_but_a_v1_terminating_row() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    for (case, phase, engine, managed_by) in [
        ("drained", "drained", None, SubstrateKind::Kubernetes),
        ("v2", "terminating", Some("v2"), SubstrateKind::Kubernetes),
        ("direct", "terminating", None, SubstrateKind::Space),
    ] {
        let node_id = node(pool, phase, engine, managed_by).await?;
        assert!(!delete_terminating_k8s_node(pool, node_id).await?, "{case}");
        assert_eq!(rows(pool, node_id).await?, (true, true), "{case}");
    }

    let terminating = node(pool, "terminating", None, SubstrateKind::Kubernetes).await?;
    assert!(delete_terminating_k8s_node(pool, terminating).await?);
    assert_eq!(rows(pool, terminating).await?, (false, false));
    assert!(!delete_terminating_k8s_node(pool, terminating).await?);
    Ok(())
}
