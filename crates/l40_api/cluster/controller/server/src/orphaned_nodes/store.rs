//! Postgres-backed implementation of [`super::OrphanedNodesSink`], over
//! `pplx_space_node_registration_store::orphaned_nodes`, whose docs give the
//! eligibility the candidate scan and the conditional DELETE share.
//!
//! The DELETE additionally checks `node_k8s.k8s_node_uid` against the value
//! we observed during the scan. This catches the same-private-DNS ASG
//! replacement race: `nodes.id` is `Uuid::new_v5(k8s_node_name, role)`, so a
//! replacement instance lands on the same row id. Between candidate-scan
//! and DELETE, kube node-sync upserts the row and bumps the UID. The DELETE
//! refuses to act if the UID has moved.

use pplx_space_accounts::NodeId;
use pplx_space_node_registration_store::orphaned_nodes::{self, OrphanCandidateRow};
use sqlx::PgPool;

use super::{OrphanCandidate, OrphanedNodesSink};

pub struct NodeDbStore {
    pool: PgPool,
}

impl NodeDbStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl OrphanedNodesSink for NodeDbStore {
    async fn list_candidates(
        &self,
        limit: i64,
        after: Option<NodeId>,
    ) -> Result<Vec<OrphanCandidate>, sqlx::Error> {
        // `after` is the per-sweep cursor: the reconciler passes the
        // last-scanned id from the prior tick so a fixed `max_per_tick` cap
        // can't permanently starve later candidates when the front of the
        // ascending-`id` order is dominated by live K8s nodes. `None` lets
        // the first tick of a sweep start from the beginning.
        let rows = orphaned_nodes::candidates(&self.pool, after, limit).await?;
        Ok(rows
            .into_iter()
            .map(
                |OrphanCandidateRow {
                     node_id,
                     k8s_node_name,
                     k8s_node_uid,
                 }| OrphanCandidate {
                    node_id,
                    k8s_node_name,
                    k8s_node_uid,
                },
            )
            .collect())
    }

    async fn delete_node_if_orphan(
        &self,
        node_id: NodeId,
        expected_uid: Option<String>,
    ) -> Result<bool, sqlx::Error> {
        orphaned_nodes::delete_if_orphan(&self.pool, node_id, expected_uid.as_deref()).await
    }
}
