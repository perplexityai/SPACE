//! Postgres-backed [`super::DeadNodeSink`].
//!
//! The node reads, the node re-check and the node delete are written here
//! because no store owns `nodes`; the sandbox reads, each outcome's write and
//! the status go through `pplx_space_sandbox_store`'s `dead_node`, `lost` and
//! `delete` modules, and a lost row's dead uploads through
//! `pplx_space_snapshot_store::lost`.
//!
//! A row's write runs in its own transaction, in the placement lock order:
//! the node rows `FOR SHARE` ([`node_identity_holds`] and the recorded
//! instance), then the sandbox row `FOR UPDATE`, then its in-flight uploads.
//! Under those locks it re-reads the row and writes only if the plan still
//! holds, so a wake, a restore claim, a registration of a replacement or a
//! drain that committed meanwhile refuses the write instead of racing it.

use pplx_space_common_core::types::{NodeId, SandboxId, SnapshotId, SubstrateKind};

use std::sync::LazyLock;
use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_sandbox_residency::node_heartbeat_fresh;
use pplx_space_sandbox_store::dead_node::{
    self as sandbox_dead_node, DeadNodeRow, DeadNodeStatusWrite, NodeRef,
};
use pplx_space_sandbox_store::{delete as sandbox_delete, lost as sandbox_lost};
use pplx_space_snapshot_store::lost::fail_quiet_full_uploads_from_node;
use sqlx::{PgPool, Postgres, Transaction};

use super::outcome::RowOutcome;
use super::verdict::Heartbeat;
use super::{CandidateNode, DeadNodeSink, ReferenceNode, RowWrite, WriteBasis, WriteResult};
use crate::gone_node_write_off::node_identity_holds;

pub struct DeadNodeDbStore {
    pool: PgPool,
}

impl DeadNodeDbStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// `$1` = substrate, `$2` = cursor (exclusive, NULL from the start), `$3` =
/// limit. Nodes outside any drain, or a v2 drain's node in `terminating`
/// (the controller's terminator has it; nothing else deletes its row),
/// whose heartbeat is not fresh by the API's
/// node-status rule (no `node_status_snapshots` row, an unreachable one, or
/// one past `stale_after`), or which a non-terminal row pins
/// (`active_node_id` or `target_node_id`) whose latest execution ran on the
/// node's Kubernetes name stamped with an instance other than the node's
/// recorded one: the rows a same-name replacement's predecessor left. The
/// freshness fragment is per row here rather than on a bound id;
/// `node_status_snapshots` holds one row per node.
static CANDIDATE_NODES: LazyLock<String> = LazyLock::new(|| {
    let fresh = node_heartbeat_fresh("n.id");
    sql!(
        format!(
            "SELECT n.id, k.k8s_node_name, k.k8s_node_uid, k.instance_id,
                    {fresh} AS heartbeat_fresh,
                    n.drain_phase IS NOT NULL AS v2_terminating
               FROM nodes n
               LEFT JOIN node_k8s k ON k.node_id = n.id
              WHERE n.managed_by = $1
                AND (
                    n.drain_phase IS NULL
                    OR (n.drain_engine = 'v2' AND n.drain_phase = 'terminating')
                )
                AND ($2::UUID IS NULL OR n.id > $2)
                AND (
                    NOT {fresh}
                    OR EXISTS (
                        SELECT 1
                          FROM sandboxes s
                         CROSS JOIN LATERAL (
                              SELECT e.k8s_node_name, e.node_instance_id
                                FROM sandbox_k8s_executions e
                               WHERE e.sandbox_id = s.id
                               ORDER BY e.generation DESC
                               LIMIT 1
                         ) latest
                         WHERE s.id IN (
                               SELECT id FROM sandboxes WHERE active_node_id = n.id
                               UNION
                               SELECT id FROM sandboxes WHERE target_node_id = n.id
                           )
                           AND s.lifecycle NOT IN ('failed', 'deleted')
                           AND latest.k8s_node_name = k.k8s_node_name
                           AND latest.node_instance_id IS NOT NULL
                           AND latest.node_instance_id IS DISTINCT FROM k.instance_id
                    )
                )
              ORDER BY n.id
              LIMIT $3"
        )
        .as_str()
    )
});

/// `$1` = substrate, `$2` = limit. Fresh nodes with a recorded Kubernetes
/// identity and instance, newest heartbeat first, as references for the
/// instance lookup.
static REFERENCE_NODES: LazyLock<String> = LazyLock::new(|| {
    let fresh = node_heartbeat_fresh("n.id");
    sql!(
        format!(
            "SELECT k.k8s_node_uid, k.instance_id
               FROM nodes n
               JOIN node_k8s k ON k.node_id = n.id
               JOIN node_status_snapshots nss ON nss.node_id = n.id
              WHERE n.managed_by = $1
                AND k.k8s_node_uid IS NOT NULL
                AND k.instance_id IS NOT NULL
                AND {fresh}
              ORDER BY nss.observed_at DESC, n.id
              LIMIT $2"
        )
        .as_str()
    )
});

/// `$1` = node id. Whether the node's heartbeat is still stale, read after
/// the node rows are locked: a row selected by the node's verdict alone is
/// written only while no live machine heartbeats for the node id.
static HEARTBEAT_FRESH: LazyLock<String> = LazyLock::new(|| {
    let fresh = node_heartbeat_fresh("$1");
    sql!(format!("SELECT {fresh}").as_str())
});

/// `$1` = node id, `$2` = substrate, `$3` = the node's Kubernetes name, `$4`
/// = whether the tick read a v2 drain's terminating node. The lost node's
/// delete, run with the node row locked and its `node_k8s` row re-checked:
/// still outside any drain (or still that v2 drain's terminating node), its
/// heartbeat still stale, and no
/// non-terminal row pinning it and no active execution on its name left, as
/// `orphaned_nodes` requires.
static DELETE_NODE: LazyLock<String> = LazyLock::new(|| {
    let fresh = node_heartbeat_fresh("$1");
    sql!(
        format!(
            "DELETE FROM nodes n
              WHERE n.id = $1
                AND n.managed_by = $2
                AND CASE WHEN $4
                         THEN n.drain_engine = 'v2' AND n.drain_phase = 'terminating'
                         ELSE n.drain_phase IS NULL
                    END
                AND NOT {fresh}
                AND NOT EXISTS (
                    SELECT 1 FROM sandboxes
                     WHERE active_node_id = $1
                       AND lifecycle NOT IN ('failed', 'deleted')
                )
                AND NOT EXISTS (
                    SELECT 1 FROM sandboxes
                     WHERE target_node_id = $1
                       AND lifecycle NOT IN ('failed', 'deleted')
                )
                AND NOT EXISTS (
                    SELECT 1 FROM sandbox_k8s_executions
                     WHERE active
                       AND k8s_node_name = $3
                )"
        )
        .as_str()
    )
});

fn node_ref(node: &CandidateNode) -> NodeRef<'_> {
    NodeRef {
        node_id: node.node_id,
        k8s_node_name: node.k8s_node_name.as_deref(),
        managed_by: node.managed_by,
    }
}

/// Whether the node's `node_k8s` row still records `instance_id`, read
/// `FOR SHARE` so a registration that would replace it waits for this
/// transaction. A node with no `node_k8s` row holds only while `instance_id`
/// is `None` too.
async fn recorded_instance_holds(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
    instance_id: Option<&str>,
) -> Result<bool, sqlx::Error> {
    let recorded: Option<Option<String>> = sqlx::query_scalar(sql!(
        "SELECT instance_id FROM node_k8s WHERE node_id = $1 FOR SHARE"
    ))
    .bind(node_id)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(recorded.flatten().as_deref() == instance_id)
}

/// The node checks every row write rests on, under the node rows' shared
/// locks: still outside any drain (or still the v2 drain's terminating node)
/// with the uid and instance the tick read,
/// and, for a row the node's verdict selected, still without a fresh
/// heartbeat.
async fn node_holds(
    tx: &mut Transaction<'_, Postgres>,
    node: &CandidateNode,
    basis: WriteBasis,
) -> Result<bool, sqlx::Error> {
    // The verb runs on Kubernetes only: Direct nodes have no Node or
    // instance to judge.
    let phase = node.v2_terminating.then_some("terminating");
    if !node_identity_holds(tx, node.node_id, phase, node.k8s_node_uid.as_deref()).await? {
        return Ok(false);
    }
    if !recorded_instance_holds(tx, node.node_id, node.instance_id.as_deref()).await? {
        return Ok(false);
    }
    match basis {
        WriteBasis::InstanceDead => Ok(true),
        WriteBasis::NodeLost => {
            let fresh: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(HEARTBEAT_FRESH.as_str()))
                .bind(node.node_id)
                .fetch_one(&mut **tx)
                .await?;
            Ok(!fresh)
        }
    }
}

/// Apply `outcome` to the re-read `row`. Returns whether anything changed.
async fn apply(
    tx: &mut Transaction<'_, Postgres>,
    node: &CandidateNode,
    row: &DeadNodeRow,
    outcome: RowOutcome,
    upload_settle: Duration,
) -> Result<bool, sqlx::Error> {
    let sandbox_id = row.sandbox_id;
    let source = row
        .source_execution
        .as_ref()
        .filter(|execution| execution.active)
        .map(|execution| execution.id);
    match outcome {
        RowOutcome::FinishDelete => {
            sandbox_delete::delete_with_dependents(tx, sandbox_id).await?;
            Ok(true)
        }
        RowOutcome::LostSnapshotRetained | RowOutcome::Lost => {
            if !sandbox_dead_node::mark_lost(tx, sandbox_id, &row.stored_lifecycle).await? {
                return Ok(false);
            }
            fail_quiet_full_uploads_from_node(tx, node.node_id, &[sandbox_id], upload_settle)
                .await?;
            sandbox_lost::write_off_lost_sandbox(tx, node.node_id, sandbox_id, source).await?;
            Ok(true)
        }
        RowOutcome::StaysSuspended
        | RowOutcome::StaysStopped
        | RowOutcome::QuarantineKept
        | RowOutcome::QuarantineLost
        | RowOutcome::ReferencesCleared => {
            // A `failed` row or tombstone keeps `target_node_id` as the last
            // place it lived, as the lost write-off leaves it; a row that
            // stays restorable drops both, so it restores or starts elsewhere.
            let keep_target = outcome == RowOutcome::ReferencesCleared;
            let released = sandbox_dead_node::release_from_node(
                tx,
                sandbox_id,
                node.node_id,
                &row.stored_lifecycle,
                keep_target,
            )
            .await?;
            let retired = match source {
                Some(execution_id) => {
                    sandbox_dead_node::retire_source_execution(tx, sandbox_id, execution_id).await?
                }
                None => false,
            };
            Ok(released || retired)
        }
        RowOutcome::HeldByPublication
        | RowOutcome::Wait
        | RowOutcome::HeldByExecution
        | RowOutcome::Unselected => Ok(false),
    }
}

impl DeadNodeSink for DeadNodeDbStore {
    async fn candidate_nodes(
        &self,
        managed_by: SubstrateKind,
        after: Option<NodeId>,
        limit: u32,
    ) -> Result<Vec<CandidateNode>, sqlx::Error> {
        type Row = (
            NodeId,
            Option<String>,
            Option<String>,
            Option<String>,
            bool,
            bool,
        );
        let rows: Vec<Row> = sqlx::query_as(sqlx::AssertSqlSafe(CANDIDATE_NODES.as_str()))
            .bind(managed_by)
            .bind(after)
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(
                |(node_id, k8s_node_name, k8s_node_uid, instance_id, fresh, v2_terminating)| {
                    CandidateNode {
                        node_id,
                        managed_by,
                        k8s_node_name,
                        k8s_node_uid,
                        instance_id,
                        heartbeat: if fresh {
                            Heartbeat::Fresh
                        } else {
                            Heartbeat::Stale
                        },
                        v2_terminating,
                    }
                },
            )
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
            .map(|(k8s_node_uid, instance_id)| ReferenceNode {
                k8s_node_uid,
                instance_id,
            })
            .collect())
    }

    async fn rows_on_node(
        &self,
        node: &CandidateNode,
        after: Option<SandboxId>,
        upload_settle: Duration,
        limit: u32,
    ) -> Result<Vec<DeadNodeRow>, sqlx::Error> {
        sandbox_dead_node::rows_on_node(&self.pool, node_ref(node), after, upload_settle, limit)
            .await
    }

    async fn write_row(
        &self,
        node: &CandidateNode,
        write: &RowWrite<'_>,
        upload_settle: Duration,
    ) -> Result<WriteResult, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        if !node_holds(&mut tx, node, write.basis).await? {
            tx.rollback().await?;
            return Ok(WriteResult::NodeChanged);
        }
        if !sandbox_dead_node::lock_row(&mut tx, write.sandbox_id).await? {
            tx.rollback().await?;
            return Ok(WriteResult::RowChanged);
        }
        // Lock the sandbox's in-flight uploads before re-reading the facts
        // their state decides, as `lost_started_sandboxes` does: a finalize
        // that committed first is visible to the re-read, and one racing this
        // transaction takes the locks in the opposite order, so Postgres
        // aborts one of the two and the next tick judges again.
        let _locked_uploads: Vec<SnapshotId> = sqlx::query_scalar(sql!(
            "SELECT id FROM snapshots
              WHERE sandbox_id = $1
                AND type = 'full'
                AND state = 'creating'
              ORDER BY id
              FOR UPDATE"
        ))
        .bind(write.sandbox_id)
        .fetch_all(&mut *tx)
        .await?;
        let Some(row) = sandbox_dead_node::row_on_node(
            &mut tx,
            node_ref(node),
            write.sandbox_id,
            upload_settle,
        )
        .await?
        else {
            tx.rollback().await?;
            return Ok(WriteResult::RowChanged);
        };
        if (write.decide)(&row) != write.outcome
            || row.source_execution.as_ref().map(|execution| execution.id) != write.source_execution
        {
            tx.rollback().await?;
            return Ok(WriteResult::RowChanged);
        }
        if !apply(&mut tx, node, &row, write.outcome, upload_settle).await? {
            tx.rollback().await?;
            return Ok(WriteResult::NothingToChange);
        }
        tx.commit().await?;
        Ok(WriteResult::Applied)
    }

    async fn delete_node(&self, node: &CandidateNode) -> Result<bool, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        // The exclusive lock holds a registration of a replacement off until
        // this commits, or waits for one in flight and then reads what it
        // committed; a moved uid or instance refuses.
        let locked: Option<i32> =
            sqlx::query_scalar(sql!("SELECT 1 FROM nodes WHERE id = $1 FOR UPDATE"))
                .bind(node.node_id)
                .fetch_optional(&mut *tx)
                .await?;
        if locked.is_none() {
            tx.rollback().await?;
            return Ok(false);
        }
        let identity: Option<(Option<String>, Option<String>)> = sqlx::query_as(sql!(
            "SELECT k8s_node_uid, instance_id FROM node_k8s WHERE node_id = $1 FOR SHARE"
        ))
        .bind(node.node_id)
        .fetch_optional(&mut *tx)
        .await?;
        let (uid, instance_id) = identity.unwrap_or_default();
        if uid != node.k8s_node_uid || instance_id != node.instance_id {
            tx.rollback().await?;
            return Ok(false);
        }
        let deleted = sqlx::query(sqlx::AssertSqlSafe(DELETE_NODE.as_str()))
            .bind(node.node_id)
            .bind(node.managed_by)
            .bind(node.k8s_node_name.as_deref())
            .bind(node.v2_terminating)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok(deleted == 1)
    }

    async fn write_status(&self, status: &DeadNodeStatusWrite<'_>) -> Result<bool, sqlx::Error> {
        sandbox_dead_node::write_status(&self.pool, status).await
    }
}

#[cfg(test)]
mod tests;
