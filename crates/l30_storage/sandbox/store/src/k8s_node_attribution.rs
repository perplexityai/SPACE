//! The registered Kubernetes nodes durably attributed to a sandbox: the nodes
//! that can legitimately be carrying work for it, or for one snapshot it
//! names, at once. More than one is normal during a handoff.
//!
//! The candidates come from the sandbox's own records (its active and target
//! placement, the node of its latest Kubernetes execution) and from the
//! snapshot's source node. The statement reads `snapshots`, which
//! `pplx-space-snapshot-store` owns, and resolves each node through
//! `node_k8s`, which `pplx-space-node-registration-store` owns. It is one
//! statement because it answers one question about the sandbox, and it writes
//! nothing.

use pplx_space_k8s_ids::{K8sNodeName, K8sNodeUid};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;
use pplx_space_sql_locator::sql;

/// A node attributed to the sandbox, by its Kubernetes identity.
#[derive(Clone, Debug, Eq, PartialEq, sqlx::FromRow)]
pub struct AttributedK8sNode {
    pub k8s_node_name: K8sNodeName,
    pub k8s_node_uid: K8sNodeUid,
}

/// Every registered node, with a UID, that `sandbox_id`'s records attribute
/// to it: its active and target placement, the node of its latest Kubernetes
/// execution, and the source node of `snapshot_id` when that snapshot is the
/// sandbox's own or a fork copy in the sandbox's organization. In no
/// particular order; a node attributed more than once is listed once. An
/// unknown sandbox has none.
pub async fn attributed_k8s_nodes(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    snapshot_id: SnapshotId,
) -> Result<Vec<AttributedK8sNode>, sqlx::Error> {
    sqlx::query_as::<_, AttributedK8sNode>(sql!(
        "WITH latest_execution AS (
             SELECT k8s_node_name
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND k8s_node_name IS NOT NULL
             ORDER BY generation DESC
             LIMIT 1
         ), candidate_nodes AS (
             SELECT active_node_id AS node_id FROM sandboxes WHERE id = $1
             UNION
             SELECT target_node_id AS node_id FROM sandboxes WHERE id = $1
             UNION
             SELECT snapshot.source_node_id AS node_id
             FROM snapshots snapshot
             JOIN sandboxes subject ON subject.id = $1
             WHERE snapshot.id = $2
               AND snapshot.source_node_id IS NOT NULL
               AND (
                   snapshot.sandbox_id = $1
                   OR (
                       snapshot.artifact_origin = 'fork'
                       AND snapshot.org_id = subject.org_id
                   )
               )
             UNION
             SELECT k.node_id
             FROM latest_execution e
             JOIN node_k8s k ON k.k8s_node_name = e.k8s_node_name
         )
         SELECT k.k8s_node_name, k.k8s_node_uid
         FROM candidate_nodes candidates
         JOIN node_k8s k ON k.node_id = candidates.node_id
         WHERE k.k8s_node_uid IS NOT NULL"
    ))
    .bind(sandbox_id)
    .bind(snapshot_id)
    .fetch_all(executor)
    .await
}
