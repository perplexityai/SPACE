//! Postgres-backed [`super::DeadNodeSink`].
//!
//! The node reads are written here because no store owns `nodes`; the
//! sandbox reads and the report go through `pplx_space_sandbox_store`'s
//! `dead_node` module. Every read is a plain `SELECT`: the dry run takes no
//! lock and writes nothing but its report.

use pplx_space_common_core::types::{NodeId, SubstrateKind};

use std::sync::LazyLock;
use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_sandbox_residency::node_heartbeat_fresh;
use pplx_space_sandbox_store::dead_node::{
    self as sandbox_dead_node, DeadNodeReportWrite, DeadNodeRow,
};
use sqlx::PgPool;

use super::{DeadNodeSink, ReferenceNode, StaleNode};

pub struct DeadNodeDbStore {
    pool: PgPool,
}

impl DeadNodeDbStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// `$1` = substrate, `$2` = cursor (exclusive, NULL from the start), `$3` =
/// limit. Nodes outside any drain whose heartbeat is not fresh by the API's
/// node-status rule: no `node_status_snapshots` row, an unreachable one, or
/// one past `stale_after`. The freshness fragment is per row here rather than
/// on a bound id; `node_status_snapshots` holds one row per node.
static STALE_NODES: LazyLock<String> = LazyLock::new(|| {
    let fresh = node_heartbeat_fresh("n.id");
    sql!(
        format!(
            "SELECT n.id, k.k8s_node_name, k.k8s_node_uid
               FROM nodes n
               LEFT JOIN node_k8s k ON k.node_id = n.id
              WHERE n.managed_by = $1
                AND n.drain_phase IS NULL
                AND ($2::UUID IS NULL OR n.id > $2)
                AND NOT {fresh}
              ORDER BY n.id
              LIMIT $3"
        )
        .as_str()
    )
});

/// `$1` = substrate, `$2` = limit. Fresh nodes with a recorded Kubernetes
/// identity, newest heartbeat first, as references for the instance lookup.
static REFERENCE_NODES: LazyLock<String> = LazyLock::new(|| {
    let fresh = node_heartbeat_fresh("n.id");
    sql!(
        format!(
            "SELECT k.k8s_node_name, k.k8s_node_uid
               FROM nodes n
               JOIN node_k8s k ON k.node_id = n.id
               JOIN node_status_snapshots nss ON nss.node_id = n.id
              WHERE n.managed_by = $1
                AND k.k8s_node_uid IS NOT NULL
                AND {fresh}
              ORDER BY nss.observed_at DESC, n.id
              LIMIT $2"
        )
        .as_str()
    )
});

impl DeadNodeSink for DeadNodeDbStore {
    async fn stale_nodes(
        &self,
        managed_by: SubstrateKind,
        after: Option<NodeId>,
        limit: u32,
    ) -> Result<Vec<StaleNode>, sqlx::Error> {
        let rows: Vec<(NodeId, Option<String>, Option<String>)> =
            sqlx::query_as(sqlx::AssertSqlSafe(STALE_NODES.as_str()))
                .bind(managed_by)
                .bind(after)
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await?;
        Ok(rows
            .into_iter()
            .map(|(node_id, k8s_node_name, k8s_node_uid)| StaleNode {
                node_id,
                managed_by,
                k8s_node_name,
                k8s_node_uid,
            })
            .collect())
    }

    async fn reference_nodes(
        &self,
        managed_by: SubstrateKind,
        limit: u32,
    ) -> Result<Vec<ReferenceNode>, sqlx::Error> {
        let rows: Vec<(String, String)> =
            sqlx::query_as(sqlx::AssertSqlSafe(REFERENCE_NODES.as_str()))
                .bind(managed_by)
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await?;
        Ok(rows
            .into_iter()
            .map(|(k8s_node_name, k8s_node_uid)| ReferenceNode {
                k8s_node_name,
                k8s_node_uid,
            })
            .collect())
    }

    async fn rows_on_node(
        &self,
        node: &StaleNode,
        upload_settle: Duration,
        limit: u32,
    ) -> Result<Vec<DeadNodeRow>, sqlx::Error> {
        sandbox_dead_node::rows_on_node(
            &self.pool,
            node.node_id,
            node.k8s_node_name.as_deref(),
            node.managed_by,
            upload_settle,
            limit,
        )
        .await
    }

    async fn write_report(&self, report: &DeadNodeReportWrite<'_>) -> Result<(), sqlx::Error> {
        sandbox_dead_node::upsert_report(&self.pool, report).await
    }
}

#[cfg(test)]
mod tests;
