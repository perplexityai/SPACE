//! Every v1 writer keyed on drain phase skips a v2 drain's node: the node
//! drain's draining and terminating lists, the drained-node terminator's list,
//! its reap and its orphan delete, and the terminating delete with its
//! tombstone. Each test runs the writer against a v1 row and a v2 row in the
//! same phase: the v1 row is the control the writer still takes.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeDrainEngine, NodeId, SubstrateKind};
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_sandbox_residency::ResidencyEvidence;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::{
    delete_terminating_node_and_record_tombstone, draining_nodes_query, terminating_nodes_query,
};
use crate::drained_node_terminator::DrainedNodeTerminatorSink as _;
use crate::drained_node_terminator::store::{
    NodeDbStore, delete_drained_node_if_orphan_query, mark_terminating_query,
};

const UID: &str = "uid-1";

/// A Kubernetes node in `phase` under `engine`, with its `node_k8s` row.
async fn node(pool: &PgPool, phase: &str, engine: NodeDrainEngine) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_mode,
                            drain_started_at, drain_engine, drain_status)
         VALUES ($1, '10.0.0.60', 'worker', 'k8s', 'registered', $2, 'forced', NOW(), $3,
                 CASE WHEN $3::TEXT IS NULL THEN NULL ELSE '{\"epoch\": 1}'::JSONB END)"
    ))
    .bind(node_id)
    .bind(phase)
    .bind(engine.column())
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid) VALUES ($1, $2, $3)"
    ))
    .bind(node_id)
    .bind(format!("node-{node_id}"))
    .bind(UID)
    .execute(pool)
    .await?;
    Ok(node_id)
}

async fn phase(pool: &PgPool, node_id: NodeId) -> TestResult<Option<String>> {
    Ok(
        sqlx::query_scalar(sql!("SELECT drain_phase FROM nodes WHERE id = $1"))
            .bind(node_id)
            .fetch_optional(pool)
            .await?
            .flatten(),
    )
}

async fn listed(pool: &PgPool, statement: &str, phase: &str) -> TestResult<Vec<NodeId>> {
    Ok(sqlx::query_scalar(sqlx::AssertSqlSafe(statement))
        .bind(phase)
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(pool)
        .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn the_draining_list_skips_a_v2_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let v1 = node(&db.pool, "draining", NodeDrainEngine::V1).await?;
    let v2 = node(&db.pool, "draining", NodeDrainEngine::V2).await?;
    let rows: Vec<(NodeId, Option<String>)> =
        sqlx::query_as(sqlx::AssertSqlSafe(draining_nodes_query()))
            .bind("draining")
            .bind(SubstrateKind::Kubernetes)
            .fetch_all(&db.pool)
            .await?;
    let ids: Vec<_> = rows.into_iter().map(|(id, _)| id).collect();
    assert!(ids.contains(&v1) && !ids.contains(&v2), "{ids:?}");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_terminating_list_skips_a_v2_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let v1 = node(&db.pool, "terminating", NodeDrainEngine::V1).await?;
    let v2 = node(&db.pool, "terminating", NodeDrainEngine::V2).await?;
    let ids = listed(&db.pool, terminating_nodes_query(), "terminating").await?;
    assert!(ids.contains(&v1) && !ids.contains(&v2), "{ids:?}");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_drained_terminators_list_skips_a_v2_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let v1 = node(&db.pool, "drained", NodeDrainEngine::V1).await?;
    let v2 = node(&db.pool, "drained", NodeDrainEngine::V2).await?;
    let ids = NodeDbStore::new(db.pool.clone(), ResidencyEvidence::default())
        .list_drained_nodes()
        .await?;
    assert!(ids.contains(&v1) && !ids.contains(&v2), "{ids:?}");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_drained_terminators_reap_skips_a_v2_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    for (engine, reaped) in [(NodeDrainEngine::V1, true), (NodeDrainEngine::V2, false)] {
        let node_id = node(&db.pool, "drained", engine).await?;
        sqlx::query(sqlx::AssertSqlSafe(sql!(mark_terminating_query(
            ResidencyEvidence::default()
        ))))
        .bind(node_id)
        .bind("terminating")
        .bind("drained")
        .execute(&db.pool)
        .await?;
        let expected = if reaped { "terminating" } else { "drained" };
        assert_eq!(
            phase(&db.pool, node_id).await?.as_deref(),
            Some(expected),
            "{engine:?}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_drained_terminators_orphan_delete_skips_a_v2_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    for (engine, deleted) in [(NodeDrainEngine::V1, true), (NodeDrainEngine::V2, false)] {
        let node_id = node(&db.pool, "drained", engine).await?;
        sqlx::query(sqlx::AssertSqlSafe(sql!(
            delete_drained_node_if_orphan_query(ResidencyEvidence::default())
        )))
        .bind(node_id)
        .bind(UID)
        .bind(SubstrateKind::Kubernetes)
        .execute(&db.pool)
        .await?;
        assert_eq!(
            phase(&db.pool, node_id).await?.is_none(),
            deleted,
            "{engine:?}"
        );
    }
    Ok(())
}

/// The terminating delete, and the tombstone it records, take only a v1
/// drain's node: a v2 node's row stays, with no tombstone, for the dead-node
/// verb to delete.
#[tokio::test(flavor = "current_thread")]
async fn the_terminating_delete_and_its_tombstone_skip_a_v2_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let v1 = node(&db.pool, "terminating", NodeDrainEngine::V1).await?;
    let v2 = node(&db.pool, "terminating", NodeDrainEngine::V2).await?;
    assert!(!delete_terminating_node_and_record_tombstone(&db.pool, v2).await?);
    assert!(delete_terminating_node_and_record_tombstone(&db.pool, v1).await?);
    assert!(phase(&db.pool, v1).await?.is_none());
    assert_eq!(phase(&db.pool, v2).await?.as_deref(), Some("terminating"));
    let tombstoned: Vec<NodeId> = sqlx::query_scalar(sql!(
        "SELECT node_id FROM node_k8s_tombstones WHERE node_id IN ($1, $2)"
    ))
    .bind(v1)
    .bind(v2)
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(tombstoned, [v1]);
    Ok(())
}
