//! Every v1 writer keyed on drain phase skips a v2 drain's node: the node
//! drain's draining and terminating lists and its phase write, the
//! drained-node terminator's list, its reap and its orphan delete, and the
//! terminating delete with its tombstone. Each writer runs against a v1 row
//! and a v2 row in the same phase: the v1 row is the control the writer
//! still takes.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeDrainEngine, NodeId, SubstrateKind};
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_sandbox_residency::ResidencyEvidence;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::{
    GuardScope, delete_terminating_node_and_record_tombstone, draining_nodes_query,
    set_drain_phase_query, terminating_nodes_query,
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

/// A v1 writer keyed on drain phase.
#[derive(Clone, Copy, Debug)]
enum Writer {
    DrainingList,
    PhaseWrite,
    TerminatingList,
    DrainedList,
    Reap,
    OrphanDelete,
    TerminatingDelete,
}

impl Writer {
    const ALL: [Self; 7] = [
        Self::DrainingList,
        Self::PhaseWrite,
        Self::TerminatingList,
        Self::DrainedList,
        Self::Reap,
        Self::OrphanDelete,
        Self::TerminatingDelete,
    ];

    /// The phase the writer reads its node in.
    fn phase(self) -> &'static str {
        match self {
            Self::DrainingList | Self::PhaseWrite => "draining",
            Self::TerminatingList | Self::TerminatingDelete => "terminating",
            Self::DrainedList | Self::Reap | Self::OrphanDelete => "drained",
        }
    }

    /// Run the writer over the node; whether it took it.
    async fn takes(self, pool: &PgPool, node_id: NodeId) -> TestResult<bool> {
        Ok(match self {
            Self::DrainingList => {
                let rows: Vec<(NodeId, Option<String>)> =
                    sqlx::query_as(sqlx::AssertSqlSafe(draining_nodes_query()))
                        .bind("draining")
                        .bind(SubstrateKind::Kubernetes)
                        .fetch_all(pool)
                        .await?;
                rows.iter().any(|(id, _)| *id == node_id)
            }
            Self::PhaseWrite => {
                sqlx::query(sqlx::AssertSqlSafe(set_drain_phase_query(
                    ResidencyEvidence::LifecycleOnly,
                    GuardScope::None,
                )))
                .bind(node_id)
                .bind("drained")
                .bind(None::<String>)
                .bind("draining")
                .execute(pool)
                .await?;
                phase(pool, node_id).await?.as_deref() == Some("drained")
            }
            Self::TerminatingList => listed(pool, terminating_nodes_query(), "terminating")
                .await?
                .contains(&node_id),
            Self::DrainedList => NodeDbStore::new(pool.clone(), ResidencyEvidence::default())
                .list_drained_nodes()
                .await?
                .contains(&node_id),
            Self::Reap => {
                sqlx::query(sqlx::AssertSqlSafe(sql!(mark_terminating_query(
                    ResidencyEvidence::default()
                ))))
                .bind(node_id)
                .bind("terminating")
                .bind("drained")
                .execute(pool)
                .await?;
                phase(pool, node_id).await?.as_deref() == Some("terminating")
            }
            Self::OrphanDelete => {
                sqlx::query(sqlx::AssertSqlSafe(sql!(
                    delete_drained_node_if_orphan_query(ResidencyEvidence::default())
                )))
                .bind(node_id)
                .bind(UID)
                .bind(SubstrateKind::Kubernetes)
                .execute(pool)
                .await?;
                phase(pool, node_id).await?.is_none()
            }
            Self::TerminatingDelete => {
                let deleted = delete_terminating_node_and_record_tombstone(pool, node_id).await?;
                let tombstoned: bool = sqlx::query_scalar(sql!(
                    "SELECT EXISTS (SELECT 1 FROM node_k8s_tombstones WHERE node_id = $1)"
                ))
                .bind(node_id)
                .fetch_one(pool)
                .await?;
                deleted || tombstoned || phase(pool, node_id).await?.is_none()
            }
        })
    }
}

/// Each v1 writer takes the v1 node and leaves the v2 node in the same phase:
/// a v2 node's row stays, with no tombstone, for the v2 loop and the
/// dead-node verb.
#[tokio::test(flavor = "current_thread")]
async fn every_v1_writer_skips_a_v2_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    for writer in Writer::ALL {
        let v1 = node(&db.pool, writer.phase(), NodeDrainEngine::V1).await?;
        let v2 = node(&db.pool, writer.phase(), NodeDrainEngine::V2).await?;
        assert!(
            !writer.takes(&db.pool, v2).await?,
            "{writer:?} took the v2 node"
        );
        assert!(
            writer.takes(&db.pool, v1).await?,
            "{writer:?} left the v1 control"
        );
    }
    Ok(())
}
