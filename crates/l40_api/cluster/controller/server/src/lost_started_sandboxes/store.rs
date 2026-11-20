//! Postgres-backed [`super::LostStartedSink`].

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;

use std::collections::HashMap;
use std::sync::LazyLock;

use pplx_space_events::{SandboxEventDetails, SandboxEventTrigger};
use pplx_space_sandbox_portability::{
    snapshot_durable_for_source_release, started_restore_source_absent,
};
use pplx_space_sandbox_residency::{
    ResidencyEvidence, occupies_node_predicate, resident_lifecycle,
};
use pplx_space_sandbox_store::admin_occupancy::fleet_occupancy;
use pplx_space_sandbox_store::lost::{
    DEAD_UPLOAD_SETTLE, mark_started_lost, write_off_lost_sandboxes,
};
use pplx_space_snapshot_store::lost::fail_quiet_full_uploads_from_node;
use pplx_space_sql_locator::sql;
use sqlx::PgPool;

use super::{LostCandidate, LostStartedSink, NodeResidents, SourceNode, StrandedCounts};
use crate::gone_node_write_off::{emit_lost_and_failed, node_identity_holds};

type CandidateRow = (SandboxId, i64, NodeId, bool, Option<String>, Option<String>);

/// `$1` = [`SubstrateKind::Kubernetes`], `$2` = scan cursor (nullable),
/// `$3` = limit, `$4` = [`DEAD_UPLOAD_SETTLE`] seconds. The candidate scan.
///
/// The execution must track no pod. A recovery that materialized a
/// replacement pod records its identity before the pod's placement moves
/// `active_node_id`, so a tracked pod may be live on another node; the
/// failed-recovery convergence likewise waits for an authoritative absence
/// check to clear it.
///
/// A sandbox whose source node is in any drain phase is left to `node_drain`
/// and `drained_node_terminator`, which own residents of a drained node. The
/// restore-source condition is the one the flip re-checks
/// ([`started_restore_source_absent`]).
static LIST_CANDIDATES: LazyLock<String> = LazyLock::new(|| {
    let no_restore_source = started_restore_source_absent("s", "$4");
    sql!(
        format!(
            "SELECT s.id,
                    e.generation,
                    s.active_node_id,
                    n.id IS NOT NULL AS node_row_exists,
                    nk.k8s_node_name,
                    nk.k8s_node_uid
               FROM sandboxes s
               JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id
                AND e.active
                AND e.state = 'recovery_failed'
                AND NULLIF(BTRIM(e.pod_uid), '') IS NULL
                AND e.pod_ip IS NULL
                AND e.k8s_node_name IS NULL
               LEFT JOIN nodes n ON n.id = s.active_node_id
               LEFT JOIN node_k8s nk ON nk.node_id = n.id
              WHERE s.managed_by = $1
                AND s.lifecycle = 'started'
                AND s.active_node_id IS NOT NULL
                AND (n.id IS NULL OR (n.managed_by = $1 AND n.drain_phase IS NULL))
                AND ($2::UUID IS NULL OR s.id > $2)
                AND {no_restore_source}
              ORDER BY s.id
              LIMIT $3"
        )
        .as_str()
    )
});

/// `$1` = [`SubstrateKind::Kubernetes`]. The stranded-sandbox census's cheap
/// first pass: per node outside a drain, including nodes whose row is gone,
/// how many Kubernetes sandboxes it may still hold state for.
///
/// Three populations, each keyed on the column that names its node:
///
/// - residents by `active_node_id`, in [`resident_lifecycle`] or `stopped`;
/// - `suspended` rows by `target_node_id`. A suspend clears `active_node_id`
///   and leaves the source in `target_node_id`, which is where
///   `admin_occupancy` and the drain gate look for a local snapshot;
/// - in-flight full uploads by `source_node_id`, the admin console's other
///   local-snapshot source.
///
/// The count is an upper bound with no durability check, only used to skip
/// nodes holding nothing; the second pass applies the real definitions.
/// Drain phases are excluded because `node_drain` and
/// `drained_node_terminator` own those residents and report them
/// (`node_drain.kube_node_gone_parked`).
static NODE_RESIDENTS: LazyLock<String> = LazyLock::new(|| {
    let resident = resident_lifecycle("s");
    sql!(
        format!(
            "WITH held AS (
                 SELECT s.active_node_id AS node_id
                   FROM sandboxes s
                  WHERE s.managed_by = $1
                    AND s.active_node_id IS NOT NULL
                    AND ({resident} OR s.lifecycle = 'stopped')
                 UNION ALL
                 SELECT s.target_node_id
                   FROM sandboxes s
                  WHERE s.managed_by = $1
                    AND s.target_node_id IS NOT NULL
                    AND s.lifecycle = 'suspended'
                 UNION ALL
                 SELECT sn.source_node_id
                   FROM snapshots sn
                   JOIN sandboxes s ON s.id = sn.sandbox_id
                  WHERE sn.type = 'full'
                    AND sn.state = 'creating'
                    AND sn.source_node_id IS NOT NULL
                    AND s.managed_by = $1
             )
             SELECT h.node_id,
                    n.id IS NOT NULL AS node_row_exists,
                    nk.k8s_node_name,
                    COUNT(*) AS residents
               FROM held h
               LEFT JOIN nodes n ON n.id = h.node_id
               LEFT JOIN node_k8s nk ON nk.node_id = n.id
              WHERE n.id IS NULL OR (n.managed_by = $1 AND n.drain_phase IS NULL)
              GROUP BY h.node_id, n.id, nk.k8s_node_name"
        )
        .as_str()
    )
});

/// `$1` = [`SubstrateKind::Kubernetes`], `$2` = suspect node ids. The half of
/// the census's second pass keyed on `active_node_id`, under `residency`:
///
/// - `running`: the drain gate's residents, [`resident_lifecycle`] and
///   [`occupies_node_predicate`], whose state lived only on the node;
/// - `stopped_without_copy`: `stopped` residents without a durable copy
///   ([`snapshot_durable_for_source_release`]). A stopped sandbox keeps its
///   filesystem on the node, and neither the resident include-list nor the
///   admin console's local snapshots count it.
///
/// The at-rest half keyed on `target_node_id` is the admin console's
/// `local_snapshots`, read through `admin_occupancy::fleet_occupancy`.
fn active_node_counts(residency: ResidencyEvidence) -> String {
    let resident = resident_lifecycle("s");
    let occupies = occupies_node_predicate("s", "s.active_node_id", residency);
    let durable = snapshot_durable_for_source_release("s");
    sql!(
        format!(
            "SELECT s.active_node_id,
                    COUNT(*) FILTER (WHERE {resident} AND {occupies}) AS running,
                    COUNT(*) FILTER (
                        WHERE s.lifecycle = 'stopped' AND NOT {durable}
                    ) AS stopped_without_copy
               FROM sandboxes s
              WHERE s.managed_by = $1
                AND s.active_node_id = ANY($2::UUID[])
                AND ({resident} OR s.lifecycle = 'stopped')
              GROUP BY s.active_node_id"
        )
        .as_str()
    )
}

static ACTIVE_NODE_COUNTS: LazyLock<String> =
    LazyLock::new(|| active_node_counts(ResidencyEvidence::LifecycleOnly));
static ACTIVE_NODE_COUNTS_CENSUS_AWARE: LazyLock<String> =
    LazyLock::new(|| active_node_counts(ResidencyEvidence::NodeCensusAware));

fn active_node_counts_statement(residency: ResidencyEvidence) -> &'static str {
    match residency {
        ResidencyEvidence::LifecycleOnly => &ACTIVE_NODE_COUNTS,
        ResidencyEvidence::NodeCensusAware => &ACTIVE_NODE_COUNTS_CENSUS_AWARE,
    }
}

pub struct LostStartedDbStore {
    pool: PgPool,
    /// The residency evidence the drain and terminate gates read, so the
    /// census counts a node's residents the way they do.
    residency: ResidencyEvidence,
}

impl LostStartedDbStore {
    pub fn new(pool: PgPool, residency: ResidencyEvidence) -> Self {
        Self { pool, residency }
    }
}

impl LostStartedSink for LostStartedDbStore {
    async fn list_candidates(
        &self,
        limit: u32,
        after: Option<SandboxId>,
    ) -> Result<Vec<LostCandidate>, sqlx::Error> {
        let rows: Vec<CandidateRow> = sqlx::query_as(sqlx::AssertSqlSafe(LIST_CANDIDATES.as_str()))
            .bind(SubstrateKind::Kubernetes)
            .bind(after)
            .bind(i64::from(limit))
            .bind(DEAD_UPLOAD_SETTLE.as_secs().cast_signed())
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(
                |(sandbox_id, generation, source_node_id, node_row_exists, name, uid)| {
                    let source_node = match (node_row_exists, name) {
                        (false, _) => SourceNode::RowDeleted,
                        (true, Some(k8s_node_name)) => SourceNode::Registered {
                            k8s_node_name,
                            k8s_node_uid: uid,
                        },
                        (true, None) => SourceNode::Unverifiable,
                    };
                    LostCandidate {
                        sandbox_id,
                        generation,
                        source_node_id,
                        source_node,
                    }
                },
            )
            .collect())
    }

    async fn node_residents(&self) -> Result<Vec<NodeResidents>, sqlx::Error> {
        let rows: Vec<(NodeId, bool, Option<String>, i64)> =
            sqlx::query_as(sqlx::AssertSqlSafe(NODE_RESIDENTS.as_str()))
                .bind(SubstrateKind::Kubernetes)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows
            .into_iter()
            .map(
                |(node_id, node_row_exists, name, residents)| NodeResidents {
                    node_id,
                    source_node: match (node_row_exists, name) {
                        (false, _) => SourceNode::RowDeleted,
                        (true, Some(k8s_node_name)) => SourceNode::Registered {
                            k8s_node_name,
                            k8s_node_uid: None,
                        },
                        (true, None) => SourceNode::Unverifiable,
                    },
                    residents: residents.unsigned_abs(),
                },
            )
            .collect())
    }

    async fn stranded_counts(
        &self,
        node_ids: &[NodeId],
    ) -> Result<HashMap<NodeId, StrandedCounts>, sqlx::Error> {
        let mut counts: HashMap<NodeId, StrandedCounts> = HashMap::new();
        // The admin console's local snapshots. Without the drainability
        // counters the statement ignores residency and the long-runner age.
        for occupancy in fleet_occupancy(&self.pool, node_ids, 0, false, self.residency).await? {
            counts
                .entry(occupancy.node_id)
                .or_default()
                .at_rest_without_copy += occupancy.local_snapshots.unsigned_abs();
        }
        let rows: Vec<(NodeId, i64, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(
            active_node_counts_statement(self.residency),
        ))
        .bind(SubstrateKind::Kubernetes)
        .bind(node_ids)
        .fetch_all(&self.pool)
        .await?;
        for (node_id, running, stopped_without_copy) in rows {
            let entry = counts.entry(node_id).or_default();
            entry.running += running.unsigned_abs();
            entry.at_rest_without_copy += stopped_without_copy.unsigned_abs();
        }
        Ok(counts)
    }

    async fn mark_lost(&self, candidate: &LostCandidate) -> Result<bool, sqlx::Error> {
        let node_id = candidate.source_node_id;
        let mut tx = self.pool.begin().await?;

        // Re-check that the node is gone the way the scan found it, before
        // taking the sandbox lock: pod placement and the drained-node give-up
        // both lock the node rows before the sandbox row, so this order can't
        // deadlock against them.
        let gone = match &candidate.source_node {
            // `FOR SHARE` on a missing row locks nothing, so a registration
            // committing after this read isn't fenced. A row that reappears
            // before the read refuses the flip; one that reappears after it
            // can't make the flip wrong. `nodes.id` is derived from the node
            // name and role, so the row is a new instance under the old name,
            // and the sandbox's runtime and local disk went with the old one.
            SourceNode::RowDeleted => {
                let row: Option<i32> =
                    sqlx::query_scalar(sql!("SELECT 1 FROM nodes WHERE id = $1 FOR SHARE"))
                        .bind(node_id)
                        .fetch_optional(&mut *tx)
                        .await?;
                row.is_none()
            }
            // Reached only after the tick found the node absent from
            // Kubernetes with no live EC2 instance. The row must still be
            // outside any drain and carry the uid the scan observed, so a
            // same-name replacement refuses.
            SourceNode::Registered { k8s_node_uid, .. } => {
                node_identity_holds(&mut tx, node_id, None, k8s_node_uid.as_deref()).await?
            }
            SourceNode::Unverifiable => false,
        };
        if !gone {
            tx.rollback().await?;
            return Ok(false);
        }

        // Started recovery's claim takes the sandbox row lock before it re-arms
        // the execution, so a wake either commits first (the flip's execution
        // check then refuses) or waits for this transaction, then finds the
        // row `failed` and claims nothing.
        let locked: Option<String> = sqlx::query_scalar(sql!(
            "SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE"
        ))
        .bind(candidate.sandbox_id)
        .fetch_optional(&mut *tx)
        .await?;
        if locked.as_deref() != Some("started") {
            tx.rollback().await?;
            return Ok(false);
        }

        // Lock the sandbox's in-flight uploads before the flip evaluates its
        // restore-source condition. A finalize that committed first is visible
        // to the flip's statement snapshot and spares the sandbox. A finalize
        // racing this transaction updates its snapshot row before it locks the
        // sandbox row, the opposite order, so Postgres aborts one of the two.
        // An aborted flip is retried on a later tick. An aborted finalize from
        // the gone node isn't retried, so the flip lands and the uploaded
        // bytes are left for the storage sweeper: the residual the drained-node
        // give-up accepts for the same race.
        let _locked_uploads: Vec<SnapshotId> = sqlx::query_scalar(sql!(
            "SELECT id FROM snapshots
              WHERE sandbox_id = $1
                AND type = 'full'
                AND state = 'creating'
              ORDER BY id
              FOR UPDATE"
        ))
        .bind(candidate.sandbox_id)
        .fetch_all(&mut *tx)
        .await?;

        if !mark_started_lost(
            &mut tx,
            candidate.sandbox_id,
            node_id,
            candidate.generation,
            DEAD_UPLOAD_SETTLE,
        )
        .await?
        {
            tx.rollback().await?;
            return Ok(false);
        }
        fail_quiet_full_uploads_from_node(
            &mut tx,
            node_id,
            &[candidate.sandbox_id],
            DEAD_UPLOAD_SETTLE,
        )
        .await?;
        write_off_lost_sandboxes(&mut tx, node_id, &[candidate.sandbox_id]).await?;
        tx.commit().await?;

        let details = SandboxEventDetails::default()
            .with_from_state("started")
            .with_to_state("failed")
            .source_node_id(node_id.to_string())
            .execution_generation(candidate.generation.to_string())
            .reason(candidate.source_node.gone_reason())
            .trigger(SandboxEventTrigger::System);
        emit_lost_and_failed(candidate.sandbox_id, details, "lost_started_sandboxes");
        Ok(true)
    }
}
