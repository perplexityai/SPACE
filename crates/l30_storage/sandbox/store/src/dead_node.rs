//! Reads and the report row of the cluster controller's dead-node verb.
//!
//! The verb judges whether a node is lost and decides, for every sandbox row
//! the node still holds, what reconciling that row would do. Deciding is the
//! controller's; this module supplies the facts each decision reads
//! ([`rows_on_node`]) and keeps the verb's last result per node in
//! `dead_node_dry_runs` ([`upsert_report`], [`reports`]) for the admin API,
//! which runs in another process.
//!
//! [`rows_on_node`] writes nothing and takes no lock. The report is the only
//! table the verb writes in its dry run.

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
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use time::OffsetDateTime;

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
    /// Kubernetes node, or not scheduled yet (`k8s_node_name` NULL).
    pub execution_elsewhere: bool,
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
        }
    }
}

/// `$1` = node id, `$2` = the node's Kubernetes name (NULL for a Direct node),
/// `$3` = the upload quiet window in seconds, `$4` = row limit, `$5` = the
/// node's substrate.
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
                    EXISTS (
                        SELECT 1
                          FROM sandbox_k8s_executions e
                         WHERE e.sandbox_id = s.id
                           AND e.active
                           AND e.k8s_node_name IS DISTINCT FROM $2
                    ) AS execution_elsewhere
               FROM sandboxes s
              WHERE s.managed_by = $5
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

/// Up to `limit` sandbox rows of `managed_by` that `node_id` holds, by id:
/// rows whose `active_node_id` or `target_node_id` names it, and rows with an
/// active execution on `k8s_node_name`. `upload_settle` is the quiet window
/// for [`DeadNodeRow::upload_pending`].
///
/// A read: it writes nothing and takes no lock.
pub async fn rows_on_node(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    k8s_node_name: Option<&str>,
    managed_by: SubstrateKind,
    upload_settle: Duration,
    limit: u32,
) -> Result<Vec<DeadNodeRow>, sqlx::Error> {
    let rows: Vec<FactsRow> = sqlx::query_as(sqlx::AssertSqlSafe(ROWS_ON_NODE.as_str()))
        .bind(node_id)
        .bind(k8s_node_name)
        .bind(upload_settle.as_secs().cast_signed())
        .bind(i64::from(limit))
        .bind(managed_by)
        .fetch_all(executor)
        .await?;
    Ok(rows.into_iter().map(DeadNodeRow::from).collect())
}

/// One sampled row of a dead-node report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadNodeSample {
    pub sandbox_id: SandboxId,
    /// `sandboxes.lifecycle` as stored when the verb read it.
    pub lifecycle: String,
}

/// The verb's result for one node, as the controller writes it. Verdict,
/// reason and outcome keys are the controller's vocabulary, stored as text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadNodeReportWrite<'a> {
    pub node_id: NodeId,
    pub verdict: &'a str,
    pub reason: &'a str,
    /// The per-node row cap cut the read short; the counts are a lower bound.
    pub rows_truncated: bool,
    pub counts: BTreeMap<&'a str, u64>,
    pub samples: BTreeMap<&'a str, Vec<DeadNodeSample>>,
}

/// The stored result for one node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadNodeReport {
    pub node_id: NodeId,
    pub observed_at: OffsetDateTime,
    pub verdict: String,
    pub reason: String,
    pub rows_truncated: bool,
    pub counts: BTreeMap<String, u64>,
    pub samples: BTreeMap<String, Vec<DeadNodeSample>>,
}

#[derive(Debug, sqlx::FromRow)]
struct ReportRow {
    node_id: NodeId,
    observed_at: OffsetDateTime,
    verdict: String,
    reason: String,
    rows_truncated: bool,
    counts: Json<BTreeMap<String, u64>>,
    samples: Json<BTreeMap<String, Vec<DeadNodeSample>>>,
}

/// Replace `node_id`'s report with `report`, stamped with the database clock.
///
/// The node row's foreign key deletes the report with the node, and refuses a
/// report for a node that is already gone; that refusal is an error here, and
/// the next tick, which no longer lists the node, writes nothing for it.
pub async fn upsert_report(
    executor: impl sqlx::PgExecutor<'_>,
    report: &DeadNodeReportWrite<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO dead_node_dry_runs
            (node_id, observed_at, verdict, reason, rows_truncated, counts, samples)
         VALUES ($1, NOW(), $2, $3, $4, $5, $6)
         ON CONFLICT (node_id) DO UPDATE
            SET observed_at = EXCLUDED.observed_at,
                verdict = EXCLUDED.verdict,
                reason = EXCLUDED.reason,
                rows_truncated = EXCLUDED.rows_truncated,
                counts = EXCLUDED.counts,
                samples = EXCLUDED.samples"
    ))
    .bind(report.node_id)
    .bind(report.verdict)
    .bind(report.reason)
    .bind(report.rows_truncated)
    .bind(Json(&report.counts))
    .bind(Json(&report.samples))
    .execute(executor)
    .await?;
    Ok(())
}

/// Up to `limit` stored reports, by node id.
pub async fn reports(
    executor: impl sqlx::PgExecutor<'_>,
    limit: u32,
) -> Result<Vec<DeadNodeReport>, sqlx::Error> {
    let rows: Vec<ReportRow> = sqlx::query_as(sql!(
        "SELECT node_id, observed_at, verdict, reason, rows_truncated, counts, samples
           FROM dead_node_dry_runs
          ORDER BY node_id
          LIMIT $1"
    ))
    .bind(i64::from(limit))
    .fetch_all(executor)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| DeadNodeReport {
            node_id: row.node_id,
            observed_at: row.observed_at,
            verdict: row.verdict,
            reason: row.reason,
            rows_truncated: row.rows_truncated,
            counts: row.counts.0,
            samples: row.samples.0,
        })
        .collect())
}

#[cfg(test)]
mod tests;
