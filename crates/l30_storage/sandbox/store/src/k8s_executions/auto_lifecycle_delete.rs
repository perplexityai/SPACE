//! The internal API's lock of a sandbox's latest execution when it consumes a
//! node's auto-lifecycle delete completion receipt, with the evidence the
//! receipt's fencing reads beside it.
//!
//! The caller takes the sandbox row lock first
//! ([`crate::auto_lifecycle_delete_completion::lock_completion_authority`]),
//! and keeps its per-receipt savepoint, its `lock_timeout`, the
//! classification of a contended lock (`NOWAIT` fails with
//! `lock_not_available`), and every decision it makes from the evidence.

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::{ExecutionGeneration, SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;
use sqlx::PgConnection;

/// The latest execution's fencing evidence, as
/// [`lock_latest_execution_evidence`] reads it.
#[derive(Debug, sqlx::FromRow)]
pub struct CompletionEvidenceRow {
    pub latest_execution_generation: ExecutionGeneration,
    pub route_activation_id: Option<SandboxActivationAttemptId>,
    /// Whether the execution's Kubernetes node is registered as the
    /// publishing node.
    pub publisher_owns_execution: bool,
    /// Whether any of the sandbox's snapshots is still `creating`.
    pub creating_snapshot_exists: bool,
}

/// Lock the sandbox's latest execution, active or not, `FOR UPDATE OF
/// execution NOWAIT`, and read its fencing evidence against
/// `publisher_node_id`; `None` when the sandbox has no execution. Fails at
/// once with `lock_not_available` when another transaction holds the row.
/// The lock lasts until the caller's transaction ends.
///
/// Run it after the sandbox row lock, as its own statement: that lock
/// serializes this read against execution and snapshot inserts through their
/// `sandboxes` foreign key, and PostgreSQL need not take a row lock before it
/// evaluates the subqueries of the locking `SELECT` itself.
pub async fn lock_latest_execution_evidence(
    conn: &mut PgConnection,
    sandbox_id: SandboxId,
    publisher_node_id: NodeId,
) -> Result<Option<CompletionEvidenceRow>, sqlx::Error> {
    sqlx::query_as::<_, CompletionEvidenceRow>(sql!(
        "SELECT execution.generation AS latest_execution_generation,
                execution.route_activation_id,
                EXISTS (
                    SELECT 1
                      FROM node_k8s
                     WHERE node_k8s.node_id = $2
                       AND node_k8s.k8s_node_name = execution.k8s_node_name
                ) AS publisher_owns_execution,
                EXISTS (
                    SELECT 1
                      FROM snapshots snapshot
                     WHERE snapshot.sandbox_id = $1
                       AND snapshot.state = 'creating'
                ) AS creating_snapshot_exists
           FROM sandbox_k8s_executions execution
          WHERE execution.sandbox_id = $1
          ORDER BY execution.generation DESC
          LIMIT 1
          FOR UPDATE OF execution NOWAIT"
    ))
    .bind(sandbox_id)
    .bind(publisher_node_id)
    .fetch_optional(conn)
    .await
}

#[cfg(test)]
mod tests;
