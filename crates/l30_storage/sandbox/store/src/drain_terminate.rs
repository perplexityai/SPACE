//! What a forced v2 drain's terminate reads and writes of the sandbox rows
//! and executions that still name its node.
//!
//! A node is terminated, and its row deleted, only once nothing names it
//! ([`node_references`], [`node_unreferenced`]): no non-terminal row's
//! `active_node_id` or `target_node_id`, and no active execution on its
//! Kubernetes name. A refused check names what holds the node. A released
//! row's execution on the node is retired ([`retire_source_execution`]).

use pplx_space_accounts::NodeId;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::SandboxId;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

/// What still names a node, as [`node_references`] reads it: each list holds
/// up to the read's limit of sandbox ids, by id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeReferences {
    /// Rows outside `failed` and `deleted` whose `active_node_id` names the
    /// node.
    pub active: Vec<SandboxId>,
    /// Rows outside `failed` and `deleted` whose `target_node_id` names the
    /// node.
    pub target: Vec<SandboxId>,
    /// Sandboxes with an active execution on the node's Kubernetes name.
    pub executions: Vec<SandboxId>,
}

impl NodeReferences {
    pub fn is_empty(&self) -> bool {
        self.active.is_empty() && self.target.is_empty() && self.executions.is_empty()
    }

    /// The first guard that holds the node, and the ids that hold it:
    /// `active_node_id`, `target_node_id` or `active execution`. `None` when
    /// nothing does.
    pub fn first_guard(&self) -> Option<(&'static str, &[SandboxId])> {
        [
            ("active_node_id", self.active.as_slice()),
            ("target_node_id", self.target.as_slice()),
            ("active execution", self.executions.as_slice()),
        ]
        .into_iter()
        .find(|(_, ids)| !ids.is_empty())
    }
}

impl std::fmt::Display for NodeReferences {
    /// Each guard that holds the node, with its ids: `active_node_id held by
    /// <ids>; ...`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for (guard, ids) in [
            ("active_node_id", &self.active),
            ("target_node_id", &self.target),
            ("active execution", &self.executions),
        ] {
            if ids.is_empty() {
                continue;
            }
            if !first {
                f.write_str("; ")?;
            }
            first = false;
            write!(f, "{guard} held by ")?;
            for (n, id) in ids.iter().enumerate() {
                if n > 0 {
                    f.write_str(",")?;
                }
                write!(f, "{id}")?;
            }
        }
        Ok(())
    }
}

/// Boolean SQL fragment: TRUE iff nothing names the node `node_param` (a
/// node id) with Kubernetes name `name_param`: no row outside `failed` and
/// `deleted` whose `active_node_id` or `target_node_id` is the node, and no
/// active execution on the name. A NULL name matches no execution. Both are
/// placeholders or expressions of the caller's statement. [`node_references`]
/// reads the same three guards.
pub fn node_unreferenced(node_param: &str, name_param: &str) -> String {
    format!(
        "NOT EXISTS (
             SELECT 1 FROM sandboxes
              WHERE active_node_id = {node_param}
                AND lifecycle NOT IN ('failed', 'deleted')
         )
         AND NOT EXISTS (
             SELECT 1 FROM sandboxes
              WHERE target_node_id = {node_param}
                AND lifecycle NOT IN ('failed', 'deleted')
         )
         AND NOT EXISTS (
             SELECT 1 FROM sandbox_k8s_executions
              WHERE active
                AND k8s_node_name = {name_param}
         )"
    )
}

/// What still names `node_id` (Kubernetes name `k8s_node_name`), by the
/// guards of [`node_unreferenced`]: up to `limit` sandbox ids of each, by id.
/// A read: it takes no lock. Empty is what [`node_unreferenced`] reads as
/// TRUE at the same snapshot.
pub async fn node_references(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    k8s_node_name: Option<&str>,
    limit: u32,
) -> Result<NodeReferences, sqlx::Error> {
    let rows: Vec<(String, SandboxId)> = sqlx::query_as(sql!(
        "(SELECT 'active', id FROM sandboxes
           WHERE active_node_id = $1
             AND lifecycle NOT IN ('failed', 'deleted')
           ORDER BY id
           LIMIT $3)
         UNION ALL
         (SELECT 'target', id FROM sandboxes
           WHERE target_node_id = $1
             AND lifecycle NOT IN ('failed', 'deleted')
           ORDER BY id
           LIMIT $3)
         UNION ALL
         (SELECT 'execution', sandbox_id FROM sandbox_k8s_executions
           WHERE active
             AND k8s_node_name = $2
           ORDER BY sandbox_id
           LIMIT $3)"
    ))
    .bind(node_id)
    .bind(k8s_node_name)
    .bind(i64::from(limit))
    .fetch_all(executor)
    .await?;
    let mut references = NodeReferences::default();
    for (guard, sandbox_id) in rows {
        match guard.as_str() {
            "active" => references.active.push(sandbox_id),
            "target" => references.target.push(sandbox_id),
            _ => references.executions.push(sandbox_id),
        }
    }
    Ok(references)
}

/// Retire `execution_id`, the row's source execution, as `forgotten`, the
/// pod-tracking write-off state: its pod died with the machine. Only that
/// execution, and only while it is active; returns whether it changed.
pub async fn retire_source_execution(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    execution_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let retired = sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE,
                state = 'forgotten',
                pod_ip = NULL,
                terminated_at = COALESCE(terminated_at, NOW()),
                updated_at = NOW()
          WHERE id = $2
            AND sandbox_id = $1
            AND active"
    ))
    .bind(sandbox_id)
    .bind(execution_id)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    Ok(retired == 1)
}
