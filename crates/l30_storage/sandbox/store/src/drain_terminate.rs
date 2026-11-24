//! What a v2 drain's terminate reads and writes of the sandbox rows
//! and executions that still name its node.
//!
//! A node is terminated, and its row deleted, only once nothing names it
//! ([`node_references`], [`node_unreferenced`]): no non-terminal row's
//! `active_node_id` or `target_node_id`, and no active execution on its
//! Kubernetes name. A refused check names what holds the node. A released
//! row's execution on the node is retired ([`retire_source_execution`]).
//!
//! Before that check, the terminate's fence transaction cleans up
//! the references of rows that no longer need the sealed, drained node, the
//! only sandbox-row writes a v2 drain's controller makes: it releases the
//! `suspended` rows that still name it ([`release_durable_suspended`]): a
//! durable snapshot holds their state, and no restore or activation of
//! theirs is in flight. Rows suspended before the drain, which the node
//! never evacuates, are among them. A released suspended row keeps its
//! lifecycle, is placed nowhere, and its next wake restores it on any node.
//! A v2 drain never moves a stopped sandbox: nothing here writes a stopped
//! row, and one that still names the node holds it.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_portability::snapshot_durable_for_source_release;
use pplx_space_sql_locator::sql;
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
/// pod-tracking write-off state: the row no longer runs on the node, and its
/// pod dies with the machine. Only that execution, and only while it is
/// active; returns whether it changed.
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

/// Boolean SQL fragment over the row aliased `s`: a restore of it may be in
/// flight: an active execution in `creating`, `created`, `recovering` or
/// `running` updated within `ttl_param` seconds, the window a restore's
/// execution owns the row for.
fn restore_in_flight(ttl_param: &str) -> String {
    format!(
        "EXISTS (
             SELECT 1
               FROM sandbox_k8s_executions e
              WHERE e.sandbox_id = s.id
                AND e.active
                AND e.state IN ('creating', 'created', 'recovering', 'running')
                AND e.updated_at > NOW() - make_interval(secs => {ttl_param})
         )"
    )
}

/// Boolean SQL fragment over the row aliased `s`: an activation of it is in
/// flight: an active execution whose activation attempt hasn't expired.
fn activation_in_flight() -> &'static str {
    "EXISTS (
         SELECT 1
           FROM sandbox_k8s_executions e
          WHERE e.sandbox_id = s.id
            AND e.active
            AND e.activation_attempt_expires_at > NOW()
     )"
}

/// Release from `node_id` (Kubernetes name `k8s_node_name`) every
/// `suspended` row that names it, active or target, whose state a durable
/// snapshot holds ([`snapshot_durable_for_source_release`], the v1 drain's
/// source-release test), whose restore isn't in flight (no active
/// execution younger than `execution_ownership_ttl_secs`) and whose
/// activation isn't either (no active execution whose activation attempt is
/// unexpired). Each row is taken under its row lock, the lock a restore
/// claim takes, and re-checked after it; then its node references and
/// placement are cleared and its execution on the node retired. Writes an
/// **operation step** (the release), in the caller's transaction. Returns
/// the released ids, by id. A row it skips keeps naming the node, so the
/// terminate's reference check ([`node_unreferenced`]) refuses the node.
///
/// Only for a node sealed under its drain's generation that reported nothing
/// pending or blocked: the seal refuses every activation on it, so nothing
/// can start on it again,
/// and a released row restores elsewhere. Releasing a row whose drain is
/// later undrained is harmless: its state is durable, and a wake places it
/// anywhere.
pub async fn release_durable_suspended(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
    k8s_node_name: Option<&str>,
    execution_ownership_ttl_secs: i64,
) -> Result<Vec<SandboxId>, sqlx::Error> {
    /// `$1` = node id, `$2` = ownership TTL in seconds.
    static CANDIDATES: LazyLock<String> = LazyLock::new(|| {
        let durable = snapshot_durable_for_source_release("s");
        let in_flight = restore_in_flight("$2");
        let activating = activation_in_flight();
        sql!(format!(
            "SELECT s.id
               FROM sandboxes s
              WHERE s.id IN (
                      SELECT id FROM sandboxes WHERE active_node_id = $1
                      UNION
                      SELECT id FROM sandboxes WHERE target_node_id = $1
                    )
                AND s.lifecycle = 'suspended'
                AND {durable}
                AND NOT {in_flight}
                AND NOT {activating}
              ORDER BY s.id"
        ))
    });
    /// `$1` = sandbox id, `$2` = node id, `$3` = ownership TTL in seconds.
    /// Run under the row's lock, in a statement after the one that took it,
    /// so it reads what committed before the lock was granted.
    static RELEASE: LazyLock<String> = LazyLock::new(|| {
        let durable = snapshot_durable_for_source_release("s");
        let in_flight = restore_in_flight("$3");
        let activating = activation_in_flight();
        sql!(format!(
            "UPDATE sandboxes s
                SET active_node_id = NULL,
                    target_node_id = NULL,
                    placement = 'unplaced',
                    command_ready_pending = FALSE,
                    placement_changed_at = NULL
              WHERE s.id = $1
                AND s.lifecycle = 'suspended'
                AND (s.active_node_id = $2 OR s.target_node_id = $2)
                AND {durable}
                AND NOT {in_flight}
                AND NOT {activating}"
        ))
    });
    let candidates: Vec<SandboxId> = sqlx::query_scalar(sqlx::AssertSqlSafe(CANDIDATES.as_str()))
        .bind(node_id)
        .bind(execution_ownership_ttl_secs)
        .fetch_all(&mut **tx)
        .await?;
    let mut released = Vec::new();
    for sandbox_id in candidates {
        sqlx::query(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE"))
            .bind(sandbox_id)
            .execute(&mut **tx)
            .await?;
        let source: Option<Uuid> = sqlx::query_scalar(sql!(
            "SELECT id FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active AND k8s_node_name = $2
              ORDER BY generation DESC
              LIMIT 1"
        ))
        .bind(sandbox_id)
        .bind(k8s_node_name)
        .fetch_optional(&mut **tx)
        .await?;
        let changed = sqlx::query(sqlx::AssertSqlSafe(RELEASE.as_str()))
            .bind(sandbox_id)
            .bind(node_id)
            .bind(execution_ownership_ttl_secs)
            .execute(&mut **tx)
            .await?
            .rows_affected();
        if changed == 0 {
            continue;
        }
        if let Some(execution_id) = source {
            retire_source_execution(tx, sandbox_id, execution_id).await?;
        }
        released.push(sandbox_id);
    }
    Ok(released)
}

#[cfg(test)]
mod tests;
