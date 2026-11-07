//! Reads and writes of the cluster controller's dead-node verb.
//!
//! The verb judges which EC2 instances a node's rows ran on are dead, decides
//! for every sandbox row the node still holds what reconciling it does, and in
//! live mode applies that. Deciding is the controller's; this module supplies
//! the facts each decision reads ([`rows_on_node`], [`row_on_node`]), the
//! guarded writes of each outcome ([`lock_row`], [`release_from_node`],
//! [`mark_lost`], [`retire_source_execution`]; a `deleting` row finishes
//! through [`crate::delete::delete_with_dependents`] and a lost one is written
//! off through [`crate::lost::write_off_lost_sandbox`]), and the verb's last
//! result per node in `nodes.dead_node_status` ([`write_status`],
//! [`statuses`]) for the admin API, which runs in another process.
//!
//! [`rows_on_node`] writes nothing and takes no lock. In a dry run the status
//! is the only value the verb writes.

use pplx_space_common_core::types::{NodeId, SandboxId, SubstrateKind};

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_sandbox_portability::{
    fresh_ready_snapshot_exists, inflight_suspend_upload_from_other_node_exists,
    snapshot_has_durable_copy, stopped_artifact_durable,
};
use pplx_space_sandbox_residency::live_node_reports_sandbox;
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::{Postgres, Transaction};
#[cfg(test)]
use time::OffsetDateTime;
use uuid::Uuid;

use crate::StoredSandboxLifecycle;

/// One sandbox row a node holds, with the facts its dead-node outcome is
/// decided from. Every fact is read in one statement, so they agree with each
/// other at that statement's snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadNodeRow {
    pub sandbox_id: SandboxId,
    /// The stored lifecycle; `None` for text outside the stored vocabulary,
    /// which the column, having no `CHECK`, can hold.
    pub lifecycle: Option<StoredSandboxLifecycle>,
    /// `sandboxes.lifecycle` as stored, for the report.
    pub stored_lifecycle: String,
    /// The portability predicate,
    /// [`pplx_space_sandbox_portability::fresh_ready_snapshot_exists`]: a
    /// durable snapshot of the row's current state exists.
    pub covered_by_snapshot: bool,
    /// Any `ready` snapshot of the sandbox, of any type and age, still has a
    /// storage location
    /// ([`pplx_space_sandbox_portability::snapshot_has_durable_copy`]).
    pub ready_snapshot: bool,
    /// [`pplx_space_sandbox_portability::stopped_artifact_durable`]: a
    /// `stopped` row whose pinned stop artifact has a durable copy.
    pub stop_artifact_durable: bool,
    /// A `creating` full upload may still finalize `ready`: one from the node
    /// that heartbeated (or was created) within the quiet window, the
    /// [`crate::lost::DEAD_UPLOAD_SETTLE`] settle of the existing give-ups, or one
    /// from another node or no recorded node for the row's current suspend
    /// ([`pplx_space_sandbox_portability::inflight_suspend_upload_from_other_node_exists`]),
    /// which those give-ups also wait for.
    pub upload_pending: bool,
    /// An active `sandbox_k8s_executions` row runs off the node: on another
    /// Kubernetes node, or not scheduled yet (`k8s_node_name` NULL); or a
    /// restore lease is live, so a restore, a user's or a drain's, is placing
    /// one.
    pub execution_elsewhere: bool,
    /// The sandbox's latest execution (highest generation), when it ran on
    /// the node's Kubernetes name: the execution whose machine held the row's
    /// current state, and the only one a write-off retires.
    pub source_execution: Option<SourceExecution>,
    /// A node with a fresh heartbeat and a fresh complete census reports
    /// holding the sandbox
    /// ([`pplx_space_sandbox_residency::live_node_reports_sandbox`]).
    pub reported_by_live_node: bool,
}

/// A row's latest execution on the node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceExecution {
    pub id: Uuid,
    /// `node_instance_id`: the EC2 instance its pod was bound to, `None` when
    /// none was known.
    pub instance_id: Option<String>,
    pub active: bool,
}

/// The statement's row; [`DeadNodeRow`] parses the lifecycle.
#[derive(Debug, sqlx::FromRow)]
struct FactsRow {
    id: SandboxId,
    lifecycle: String,
    covered_by_snapshot: bool,
    ready_snapshot: bool,
    stop_artifact_durable: bool,
    upload_pending: bool,
    execution_elsewhere: bool,
    source_execution_id: Option<Uuid>,
    source_instance_id: Option<String>,
    source_active: Option<bool>,
    reported_by_live_node: bool,
}

impl From<FactsRow> for DeadNodeRow {
    fn from(row: FactsRow) -> Self {
        Self {
            sandbox_id: row.id,
            lifecycle: StoredSandboxLifecycle::from_str(&row.lifecycle).ok(),
            stored_lifecycle: row.lifecycle,
            covered_by_snapshot: row.covered_by_snapshot,
            ready_snapshot: row.ready_snapshot,
            stop_artifact_durable: row.stop_artifact_durable,
            upload_pending: row.upload_pending,
            execution_elsewhere: row.execution_elsewhere,
            source_execution: row.source_execution_id.map(|id| SourceExecution {
                id,
                instance_id: row.source_instance_id,
                active: row.source_active.unwrap_or(false),
            }),
            reported_by_live_node: row.reported_by_live_node,
        }
    }
}

/// `$1` = node id, `$2` = the node's Kubernetes name (NULL for a Direct node),
/// `$3` = the upload quiet window in seconds, `$4` = row limit, `$5` = the
/// node's substrate, `$6` = one sandbox id to read, or NULL for all, `$7` =
/// the keyset cursor (exclusive), or NULL from the start.
///
/// A `failed` row or tombstone that only keeps the node as its last place
/// (`target_node_id`, nothing active on it) is settled and not read: it pins
/// nothing, and reading it every tick would hold the read cap.
///
/// The selection is three probes, so each walks its own index: the
/// `active_node_id` and `target_node_id` indexes, and the active-execution
/// index for executions on the node, whose `k8s_node_name` has no index of
/// its own.
///
/// The pending-upload probe is the two upload guards of
/// [`crate::lost::mark_unrecoverable_sandboxes_lost_query`] read positively:
/// its settle guard for the node's own uploads, and its veto for an upload
/// from any other node or from no recorded node.
static ROWS_ON_NODE: LazyLock<String> = LazyLock::new(|| {
    let covered = fresh_ready_snapshot_exists("s");
    let durable = snapshot_has_durable_copy("ready_snapshot");
    let stop_durable = stopped_artifact_durable("s");
    let inflight_elsewhere = inflight_suspend_upload_from_other_node_exists("s", "$1");
    let reported = live_node_reports_sandbox("s");
    sql!(
        format!(
            "SELECT s.id,
                    s.lifecycle,
                    {covered} AS covered_by_snapshot,
                    EXISTS (
                        SELECT 1
                          FROM snapshots ready_snapshot
                         WHERE ready_snapshot.sandbox_id = s.id
                           AND {durable}
                    ) AS ready_snapshot,
                    {stop_durable} AS stop_artifact_durable,
                    (EXISTS (
                        SELECT 1
                          FROM snapshots sn
                         WHERE sn.sandbox_id = s.id
                           AND sn.source_node_id = $1
                           AND sn.type = 'full'
                           AND sn.state = 'creating'
                           AND COALESCE(sn.last_attempt_at, sn.timestamp)
                               >= NOW() - make_interval(secs => $3::BIGINT)
                     ) OR {inflight_elsewhere}) AS upload_pending,
                    (
                        EXISTS (
                            SELECT 1
                              FROM sandbox_k8s_executions e
                             WHERE e.sandbox_id = s.id
                               AND e.active
                               AND e.k8s_node_name IS DISTINCT FROM $2
                        )
                        OR COALESCE(s.restore_lease_expires_at > NOW(), FALSE)
                    ) AS execution_elsewhere,
                    latest.id AS source_execution_id,
                    latest.node_instance_id AS source_instance_id,
                    latest.active AS source_active,
                    {reported} AS reported_by_live_node
               FROM sandboxes s
               LEFT JOIN LATERAL (
                    SELECT e.id, e.node_instance_id, e.active
                      FROM (
                        SELECT id, node_instance_id, active, k8s_node_name
                          FROM sandbox_k8s_executions
                         WHERE sandbox_id = s.id
                         ORDER BY generation DESC
                         LIMIT 1
                      ) e
                     WHERE e.k8s_node_name = $2
               ) latest ON TRUE
              WHERE s.managed_by = $5
                AND ($6::UUID IS NULL OR s.id = $6)
                AND ($7::UUID IS NULL OR s.id > $7)
                AND NOT (
                    s.lifecycle IN ('failed', 'deleted')
                    AND s.active_node_id IS DISTINCT FROM $1
                    AND NOT EXISTS (
                        SELECT 1 FROM sandbox_k8s_executions e
                         WHERE e.sandbox_id = s.id AND e.active AND e.k8s_node_name = $2
                    )
                )
                AND s.id IN (
                    SELECT id FROM sandboxes WHERE active_node_id = $1
                    UNION
                    SELECT id FROM sandboxes WHERE target_node_id = $1
                    UNION
                    SELECT e.sandbox_id
                      FROM sandbox_k8s_executions e
                     WHERE e.active
                       AND e.k8s_node_name = $2
                )
              ORDER BY s.id
              LIMIT $4"
        )
        .as_str()
    )
});

/// The node a dead-node read is about.
#[derive(Debug, Clone, Copy)]
pub struct NodeRef<'a> {
    pub node_id: NodeId,
    /// `node_k8s.k8s_node_name`; `None` for a Direct node.
    pub k8s_node_name: Option<&'a str>,
    pub managed_by: SubstrateKind,
}

/// Up to `limit` sandbox rows of `node.managed_by` that `node` holds, by id
/// after `after`: rows whose `active_node_id` or `target_node_id` names it,
/// and rows with an active execution on its Kubernetes name, apart from
/// settled terminal rows. `upload_settle` is the quiet window for
/// [`DeadNodeRow::upload_pending`].
///
/// A read: it writes nothing and takes no lock.
pub async fn rows_on_node(
    executor: impl sqlx::PgExecutor<'_>,
    node: NodeRef<'_>,
    after: Option<SandboxId>,
    upload_settle: Duration,
    limit: u32,
) -> Result<Vec<DeadNodeRow>, sqlx::Error> {
    read_rows(executor, node, upload_settle, limit, None, after).await
}

/// [`rows_on_node`] for one sandbox: its facts if `node` still holds it.
/// Run after [`lock_row`] in the same transaction, so the facts a write
/// decides on are the committed ones and stay so until it commits.
pub async fn row_on_node(
    tx: &mut Transaction<'_, Postgres>,
    node: NodeRef<'_>,
    sandbox_id: SandboxId,
    upload_settle: Duration,
) -> Result<Option<DeadNodeRow>, sqlx::Error> {
    Ok(
        read_rows(&mut **tx, node, upload_settle, 1, Some(sandbox_id), None)
            .await?
            .pop(),
    )
}

async fn read_rows(
    executor: impl sqlx::PgExecutor<'_>,
    node: NodeRef<'_>,
    upload_settle: Duration,
    limit: u32,
    sandbox_id: Option<SandboxId>,
    after: Option<SandboxId>,
) -> Result<Vec<DeadNodeRow>, sqlx::Error> {
    let rows: Vec<FactsRow> = sqlx::query_as(sqlx::AssertSqlSafe(ROWS_ON_NODE.as_str()))
        .bind(node.node_id)
        .bind(node.k8s_node_name)
        .bind(upload_settle.as_secs().cast_signed())
        .bind(i64::from(limit))
        .bind(node.managed_by)
        .bind(sandbox_id)
        .bind(after)
        .fetch_all(executor)
        .await?;
    Ok(rows.into_iter().map(DeadNodeRow::from).collect())
}

/// Lock the sandbox row for a dead-node write, `FOR UPDATE`. Returns whether
/// the row exists. Take the node's locks first (nodes, `node_k8s`, then
/// sandboxes, the placement order), then re-read the row with
/// [`row_on_node`] before writing.
pub async fn lock_row(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
) -> Result<bool, sqlx::Error> {
    let locked: Option<i32> =
        sqlx::query_scalar(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE"))
            .bind(sandbox_id)
            .fetch_optional(&mut **tx)
            .await?;
    Ok(locked.is_some())
}

/// Release the row from `node_id` keeping its lifecycle: clear
/// `active_node_id` when it names the node, and `target_node_id` too unless
/// `keep_target` (a `failed` row or tombstone keeps it as the last place it
/// lived, as [`crate::lost::release_lost_placement`] does), leaving the row
/// `unplaced` once nothing runs it. Guarded by the `stored_lifecycle` the
/// caller re-read under [`lock_row`]; returns whether the row changed.
///
/// Writes an **inference**: the dead-node verb concluding the machine that
/// held the row is gone, so a restore or start goes elsewhere.
pub async fn release_from_node(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    node_id: NodeId,
    stored_lifecycle: &str,
    keep_target: bool,
) -> Result<bool, sqlx::Error> {
    let released = sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = CASE WHEN active_node_id = $2 THEN NULL ELSE active_node_id END,
                target_node_id = CASE
                    WHEN target_node_id = $2 AND NOT $4 THEN NULL
                    ELSE target_node_id
                END,
                placement = CASE
                    WHEN active_node_id IS NULL OR active_node_id = $2 THEN 'unplaced'
                    ELSE placement
                END,
                command_ready_pending = FALSE,
                placement_changed_at = NULL
          WHERE id = $1
            AND lifecycle = $3
            AND (active_node_id = $2 OR (target_node_id = $2 AND NOT $4))"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(stored_lifecycle)
    .bind(keep_target)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    Ok(released == 1)
}

/// Flip the row to `failed` as lost, from the `stored_lifecycle` the caller
/// re-read under [`lock_row`]; returns whether it flipped. Any snapshot is
/// kept. The caller writes the row off in the same transaction with
/// [`crate::lost::write_off_lost_sandbox`].
///
/// Writes an **inference**: the dead-node verb concluding the row's only
/// current state died with its machine.
pub async fn mark_lost(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    stored_lifecycle: &str,
) -> Result<bool, sqlx::Error> {
    let flipped = sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'failed'
          WHERE id = $1
            AND lifecycle = $2"
    ))
    .bind(sandbox_id)
    .bind(stored_lifecycle)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    Ok(flipped == 1)
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

/// One sampled row of a dead-node report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadNodeSample {
    pub sandbox_id: SandboxId,
    /// `sandboxes.lifecycle` as stored when the verb read it.
    pub lifecycle: String,
}

/// The verb's result for one node, as the controller writes it. Mode,
/// verdict, reason and outcome keys are the controller's vocabulary, stored
/// as text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeadNodeStatusWrite<'a> {
    pub node_id: NodeId,
    /// `dry-run` or `live`.
    pub mode: &'a str,
    pub verdict: &'a str,
    pub reason: &'a str,
    /// The per-node row cap cut the read short; the counts are a lower bound.
    pub rows_truncated: bool,
    /// Each instance the verb asked about, and its verdict (`dead`, `live`,
    /// or `unknown:<reason>`).
    pub instances: BTreeMap<&'a str, &'a str>,
    /// Rows per outcome the verb decided.
    pub counts: BTreeMap<&'a str, u64>,
    /// Rows per outcome whose write committed; empty in a dry run.
    pub applied: BTreeMap<&'a str, u64>,
    pub samples: BTreeMap<&'a str, Vec<DeadNodeSample>>,
}

/// The stored result for one node, as the admin node listing returns it.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DeadNodeStatus {
    #[serde(skip)]
    pub node_id: NodeId,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    pub mode: String,
    pub verdict: String,
    pub reason: String,
    pub rows_truncated: bool,
    pub instances: BTreeMap<String, String>,
    pub counts: BTreeMap<String, u64>,
    pub applied: BTreeMap<String, u64>,
    pub samples: BTreeMap<String, Vec<DeadNodeSample>>,
}

/// Replace `nodes.dead_node_status` of `status.node_id` with `status`,
/// stamped `observed_at` by the database clock. Only the verb writes the
/// column. Returns whether the node row still exists: a node deleted since
/// the verb read it gets no status, and the next tick no longer lists it.
pub async fn write_status(
    executor: impl sqlx::PgExecutor<'_>,
    status: &DeadNodeStatusWrite<'_>,
) -> Result<bool, sqlx::Error> {
    let written = sqlx::query(sql!(
        "UPDATE nodes
            SET dead_node_status = $2::JSONB
                || jsonb_build_object('observed_at', to_jsonb(NOW()))
          WHERE id = $1"
    ))
    .bind(status.node_id)
    .bind(Json(status))
    .execute(executor)
    .await?
    .rows_affected();
    Ok(written == 1)
}

/// Up to `limit` stored statuses, by node id.
#[cfg(test)]
pub async fn statuses(
    executor: impl sqlx::PgExecutor<'_>,
    limit: u32,
) -> Result<Vec<DeadNodeStatus>, sqlx::Error> {
    let rows: Vec<(NodeId, Json<DeadNodeStatus>)> = sqlx::query_as(sql!(
        "SELECT id, dead_node_status
           FROM nodes
          WHERE dead_node_status IS NOT NULL
          ORDER BY id
          LIMIT $1"
    ))
    .bind(i64::from(limit))
    .fetch_all(executor)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(node_id, Json(status))| DeadNodeStatus { node_id, ..status })
        .collect())
}

#[cfg(test)]
mod tests;
