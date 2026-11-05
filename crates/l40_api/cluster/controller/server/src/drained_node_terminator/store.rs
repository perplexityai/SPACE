use pplx_space_common_core::types::NodeId;
use pplx_space_common_core::types::SubstrateKind;

use pplx_space_common_core::sql;
use std::time::Duration;

use pplx_space_common_core::types::SandboxId;
use pplx_space_common_svc::sql::sandbox_residency::{self, ResidencyEvidence};
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_sandbox_portability::drain_gate_blockers_for_node;
use pplx_space_sandbox_store::gone_node::{self, ExpectedPhase, NodeIdentityGuard, WriteOff};
use sqlx::PgPool;

use super::{
    DRAIN_KEEP_ANNOTATION, DrainKeepAnnotationLookup, DrainKeepStatus, DrainedNodeTerminatorSink,
    OrphanDeleteOutcome, OrphanDeleteRefusal, PHASE_DRAINED, PHASE_TERMINATING,
};

/// Inner `WHERE` predicate (over a `sandboxes s` scan, `$1` = node id) for the
/// snapshot guard shared by [`local_snapshots_count_query`] and the atomic
/// guard in [`mark_terminating_query`]: a sandbox blocks termination if it is
/// mid-flight (`suspending`, or a **non-evacuation** `restoring`) or
/// suspended-without-a-portable-S3-snapshot on this node, **or** if its durable
/// suspend-snapshot upload is still in flight *from* this node
/// (`state='creating'`, `source_node_id=$1`). The in-flight clause is
/// load-bearing: do not terminate a node while it is still uploading a
/// snapshot, or the upload worker dies with the only durable copy.
///
/// This is the same predicate the `draining → drained` gate counts, so the two
/// stay in lockstep by construction rather than by matching prose: both call
/// `drain_gate_blocks_source_release`. The shared definition carries the
/// per-state rationale — why an evacuation-marked sandbox stops blocking once
/// its newest full snapshot is durable, why a non-evacuation `restoring` row
/// keeps blocking, and why the in-flight term is itself gated on `NOT durable`
/// so a lingering earlier `creating` row cannot pin a node that is already
/// releasable.
///
/// Rendered as [`drain_gate_blockers_for_node`] — a complete `SELECT` of the
/// blocking sandbox ids — rather than as the bare predicate over a `sandboxes`
/// scan. The predicate's two disjuncts attribute to different columns, so an
/// `OR` over both gives the planner no indexable driver and it reads the whole
/// table. Giving each attribution an indexable driver makes work track rows
/// pinned to this node. A node with no blockers still pays a per-row probe
/// for every row pinned to it.
fn snapshot_blockers_for_node() -> String {
    drain_gate_blockers_for_node("$1")
}

/// `$1` = node id. Inner predicate (over the bare `sandboxes` table) of the
/// resident guard, shared by [`resident_blocking_clause`] (the guard form) and
/// [`delete_refusal_diagnostics_query`] (the count form) so the guard and its
/// diagnosis can't drift.
///
/// The predicate itself lives in `pplx-space-common-svc` because node-drain's
/// `draining → drained` advance counts the same rows: a node this guard would
/// refuse is held in `draining` rather than converging to `drained` and parking
/// where nothing looks at it. Its docs carry the per-lifecycle rationale —
/// which four lifecycles are resident here and not to node-drain's include
/// list, and why `quarantined` is deliberately one of them.
fn active_resident_predicate(residency: ResidencyEvidence) -> String {
    sandbox_residency::active_resident_predicate("sandboxes", "$1", residency)
}

/// `$1` = node id. Shared resident guard (a `NOT EXISTS (…)` clause): a
/// genuinely-active, non-`suspended` sandbox still pinned to the node via
/// `active_node_id` blocks both the `drained → terminating` flip and the
/// KubeNodeGone reap — advancing/reaping would kill a sandbox that landed on
/// the node after a lost cordon. Shared by [`mark_terminating_query`] and
/// [`delete_drained_node_if_orphan_query`] so the two can't drift.
fn resident_blocking_clause(residency: ResidencyEvidence) -> String {
    let predicate = active_resident_predicate(residency);
    format!(
        "NOT EXISTS (
             SELECT 1 FROM sandboxes
             WHERE {predicate}
         )"
    )
}

/// `$1` = node id. Count of sandboxes whose snapshot state blocks
/// `drained → terminating`. See [`snapshot_blockers_for_node`].
pub(crate) fn local_snapshots_count_query() -> String {
    let blockers = snapshot_blockers_for_node();
    format!("SELECT COUNT(*)::BIGINT FROM (\n{blockers}\n) node_blockers")
}

/// `$1` = node id, `$2` = new phase (`terminating`), `$3` = current/expected
/// phase (`drained`). Atomic `drained → terminating` flip with the same
/// snapshot guard re-checked inside the UPDATE (TOCTOU defense) plus the
/// resident guard.
pub(crate) fn mark_terminating_query(residency: ResidencyEvidence) -> String {
    let snapshot_blockers = snapshot_blockers_for_node();
    let resident_clause = resident_blocking_clause(residency);
    format!(
        "UPDATE nodes
         SET drain_phase = $2,
             drain_last_error = NULL
         WHERE id = $1
           AND drain_phase = $3
           -- Snapshot guard: block on local-disk lifecycles and in-flight
           -- durable uploads (same predicate as local_snapshots_count_query;
           -- atomic here to close the TOCTOU with the pre-check).
           AND NOT EXISTS ({snapshot_blockers})
           -- Resident guard (shared with delete_drained_node_if_orphan_query):
           -- if cordon was somehow lost and a new sandbox landed on a drained
           -- node, do not advance it to terminating — the sandbox would be
           -- killed when the node is destroyed. node_drain::handle_draining_node
           -- would need to evacuate it first, but that path only runs for
           -- 'draining' nodes, not 'drained'. Blocking here lets an operator
           -- catch the anomaly.
           AND {resident_clause}"
    )
}

/// The node identity guard of the reap and its diagnosis: `$1` = node id,
/// `$2` = the uid observed at the EC2 cross-check, `$3` =
/// [`SubstrateKind::Kubernetes`].
const DRAINED_IDENTITY: NodeIdentityGuard<'static> = NodeIdentityGuard {
    node: "$1",
    uid: "$2",
    phase: ExpectedPhase::Is(PHASE_DRAINED),
    managed_by: Some("$3"),
};

/// `$1` = node id, `$3` = [`SubstrateKind::Kubernetes`]. Guarded hard-DELETE
/// for the KubeNodeGone recovery path: reaps a `drained`, Kubernetes-managed
/// row whose backing EC2 instance the caller has already confirmed
/// terminated. Keyed on `drain_phase = 'drained'` (the orphaned-nodes scan
/// only touches `drain_phase IS NULL`, so it never reaps a drained row — that
/// gap is exactly what this path closes).
///
/// The snapshot + resident `NOT EXISTS` guards are **identical to
/// [`mark_terminating_query`]**: a drained row is reapable via this direct
/// delete on exactly the condition that would let it advance
/// `drained → terminating` normally. That keeps the recovery from being
/// stricter than the regular drain path — in particular a user-`suspended`
/// sandbox whose snapshot is already durable in S3 (its `target_node_id` pin
/// lingers until the user wakes it, and no sweeper clears it) drops out of the
/// marker-aware [`snapshot_blockers_for_node`], so it does **not** block the
/// reap. Only sandboxes whose only copy is still pinned to the (now-gone)
/// node, or a genuinely-resident active sandbox, block — and those stay parked
/// until the sandbox sweepers or an operator clear them.
///
/// `$2` = the `node_k8s.k8s_node_uid` observed when the EC2 cross-check
/// resolved this node's private DNS. The identity guard ([`DRAINED_IDENTITY`])
/// refuses the reap once the uid changed, so a same-name replacement's row is
/// never reaped. Mirrors `orphaned_nodes`' uid-guarded delete.
pub(crate) fn delete_drained_node_if_orphan_query(residency: ResidencyEvidence) -> String {
    let snapshot_blockers = snapshot_blockers_for_node();
    let resident_clause = resident_blocking_clause(residency);
    let node = DRAINED_IDENTITY.node_predicate();
    let uid_row = DRAINED_IDENTITY.uid_row();
    format!(
        "DELETE FROM nodes
         WHERE {node}
           -- Identity guard: refuse if a same-name kube Node replacement
           -- bumped node_k8s.k8s_node_uid since the EC2 cross-check observed
           -- it (the row would now belong to the fresh node).
           AND EXISTS ({uid_row})
           -- Snapshot guard: same marker-aware predicate as
           -- mark_terminating_query, so a durably-portable suspended sandbox
           -- does not block the reap but a local-only one does.
           AND NOT EXISTS ({snapshot_blockers})
           -- Resident guard: same shared clause as mark_terminating_query — a
           -- genuinely-active (non-suspended) sandbox still pinned to the node
           -- blocks the reap.
           AND {resident_clause}"
    )
}

/// `$1` = node id, `$2` = the kube uid observed at the EC2 cross-check,
/// `$3` = [`SubstrateKind::Kubernetes`]. Post-refusal diagnosis for
/// [`delete_drained_node_if_orphan_query`]: re-evaluates each of its guards
/// separately so a refusal can be logged and counted per-reason instead of
/// as one opaque "delete refused". Each column mirrors one guard verbatim —
/// `phase_drifted` and `uid_mismatch` are the negations of the DELETE's
/// eligibility and identity guards, and the two counts reuse the exact
/// predicates behind its `NOT EXISTS` guards.
///
/// Runs as a separate statement *after* the DELETE matched no row, so it can
/// race concurrent writes; it feeds logging, metrics, and the mark-lost
/// pre-check only, while the DELETE and the mark-lost UPDATE re-check every
/// guard atomically. All four columns clear at once means the blocker
/// resolved in the window between the two statements.
pub(crate) fn delete_refusal_diagnostics_query(residency: ResidencyEvidence) -> String {
    let snapshot_blockers = snapshot_blockers_for_node();
    let resident_predicate = active_resident_predicate(residency);
    let node_row = DRAINED_IDENTITY.node_row();
    let uid_row = DRAINED_IDENTITY.uid_row();
    format!(
        "SELECT
             NOT EXISTS ({node_row}) AS phase_drifted,
             NOT EXISTS ({uid_row}) AS uid_mismatch,
             (SELECT COUNT(*)::BIGINT
                FROM ({snapshot_blockers}) node_blockers) AS snapshot_blockers,
             (SELECT COUNT(*)::BIGINT FROM sandboxes
               WHERE {resident_predicate}) AS active_residents"
    )
}

/// Quiet period a same-node `creating` snapshot row must have sat without a
/// worker heartbeat (`COALESCE(last_attempt_at, timestamp)`) before a gone-node
/// give-up treats it as dead: the `dead_upload_settle` of
/// [`gone_node::write_off_on_gone_node`]. A `FinalizeArtifactBackup(READY)`
/// RPC still in the API path lives for seconds (gRPC deadline), so ten quiet
/// minutes is overwhelmingly past it: a row still inside the window defers
/// the flip, and the next ticks re-check.
pub(crate) const DEAD_UPLOAD_SETTLE: Duration = Duration::from_mins(10);

/// Outcome of looking up the `space.pplx.net/drain-keep` annotation for
/// a SPACE node id.
#[derive(Debug, thiserror::Error)]
pub enum AnnotationLookupError {
    #[error("DB lookup for node_k8s row failed: {0}")]
    Db(String),
    #[error("node {0} has no node_k8s row; cannot resolve substrate node name")]
    Unregistered(NodeId),
    #[error("kube node lookup failed: {0}")]
    Kube(String),
    /// The kube Node object is gone. We must not auto-terminate without
    /// being able to read the override annotation — leave the row in
    /// `drained` for operator review.
    #[error("kube Node missing; cannot verify drain-keep annotation")]
    KubeNodeGone,
    /// The kube Node exists but `spec.unschedulable` is not set. A
    /// `drained` node must be cordoned so no new sandbox pods land in
    /// the window between this flip and `node_drain` asserting cordon
    /// on the `terminating` tick. Defer; `node_drain` will cordon on
    /// its next draining/terminating pass, or an operator should
    /// explicitly cordon/undrain the node.
    #[error("kube Node is not cordoned; deferring auto-termination to avoid scheduling window")]
    Uncordoned,
    /// The current kube Node's UID differs from the UID recorded in
    /// `node_k8s`. A same-name replacement has appeared; defer to the
    /// inventory reflector (it will reset `drain_phase` on its next tick).
    #[error("kube Node UID changed under us; deferring to inventory reconcile")]
    StaleIdentity,
}

/// Emit the `sandbox_lost` and `sandbox_failed` pair for a committed
/// gone-node give-up. The failed event also names the `operation` that gave
/// the sandbox up. Shared with `lost_started_sandboxes`.
pub(crate) fn emit_lost_and_failed(
    sandbox_id: SandboxId,
    details: SandboxEventDetails,
    operation: &str,
) {
    SandboxEvent::new(
        sandbox_id,
        SandboxEventType::SandboxLost,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details.clone());
    SandboxEvent::new(
        sandbox_id,
        SandboxEventType::SandboxFailed,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details.operation(operation));
}

// ---------------------------------------------------------------------------
// DB-only sink — no kube dependency
// ---------------------------------------------------------------------------

/// Production [`DrainedNodeTerminatorSink`] backed by Postgres.
/// Contains no kube types; safely compiled without the `kubernetes` feature.
pub struct NodeDbStore {
    pool: PgPool,
    /// Evidence required before a `sandboxes` row stops counting against its
    /// `active_node_id`. Must match what node-drain's `draining → drained`
    /// gate used, or a node converges past that gate and parks here — see
    /// [`active_resident_predicate`].
    residency_evidence: ResidencyEvidence,
}

impl NodeDbStore {
    pub fn new(pool: PgPool, residency_evidence: ResidencyEvidence) -> Self {
        Self {
            pool,
            residency_evidence,
        }
    }
}

impl DrainedNodeTerminatorSink for NodeDbStore {
    async fn list_drained_nodes(&self) -> Result<Vec<NodeId>, sqlx::Error> {
        // Scoped to Kubernetes-managed rows. Direct/admin rows must not be
        // fed into the Kubernetes-specific annotation lookup path.
        sqlx::query_scalar::<_, NodeId>(sql!(
            "SELECT id FROM nodes WHERE drain_phase = $1 AND managed_by = $2"
        ))
        .bind(PHASE_DRAINED)
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(&self.pool)
        .await
    }

    async fn count_local_snapshots(&self, node_id: NodeId) -> Result<i64, sqlx::Error> {
        // Mirrors the admin `:terminate` snapshot guard
        // (`sandbox_occupancy_for_node_in_tx`), routed through the same
        // marker-aware `snapshot_durable_for_source_release` predicate, so the
        // two agree on portability: a `suspended` sandbox that is durably
        // restorable from S3 drops out of both.
        //
        // `suspending` and **non-evacuation** `restoring` always block (no
        // portable snapshot exists yet) — their local-disk artifacts aren't
        // promotable, so they can't be filtered by the freshness predicate:
        //
        // `suspending`: controller crashed after suspend RPC cleared
        //   `active_node_id` but before writing `lifecycle = 'suspended'`;
        //   `node_drain` may then declare the source drained while the
        //   snapshot still lives on disk.
        //
        // `restoring` (NULL marker): after `restore_execution` begins,
        //   `lifecycle` moves to `restoring` and `active_node_id` is NULL, but
        //   the snapshot remains on the source node until the new pod is placed.
        //
        // An **evacuation** `restoring` row (`evacuation_restore_target IS NOT
        // NULL`) is excluded — it reads from the durable S3 snapshot, so the
        // source can terminate without losing it. See
        // [`snapshot_blockers_for_node`].
        let query = local_snapshots_count_query();
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(node_id)
            .fetch_one(&self.pool)
            .await
    }

    async fn mark_terminating(&self, node_id: NodeId) -> Result<bool, sqlx::Error> {
        // `NOT EXISTS` makes the snapshot absence check atomic with the
        // phase flip, closing the TOCTOU between count_local_snapshots
        // and this UPDATE (a sandbox completing suspend in that window
        // could otherwise race past the guard). Routes the same shared
        // freshness predicate as `count_local_snapshots` so the snapshot
        // check is atomic-consistent across the two queries.
        //
        // `drain_last_error` is explicitly cleared so a stale error from
        // a prior phase doesn't carry into the new state.
        let query = mark_terminating_query(self.residency_evidence);
        let result = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(node_id)
            .bind(PHASE_TERMINATING)
            .bind(PHASE_DRAINED)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn delete_drained_node_if_orphan(
        &self,
        node_id: NodeId,
        expected_uid: Option<String>,
    ) -> Result<OrphanDeleteOutcome, sqlx::Error> {
        let query = delete_drained_node_if_orphan_query(self.residency_evidence);
        let result = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(node_id)
            .bind(expected_uid.clone())
            .bind(SubstrateKind::Kubernetes)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() > 0 {
            return Ok(OrphanDeleteOutcome::Deleted);
        }
        // Refused: re-evaluate each guard separately so the caller can log
        // and count the refusal per-reason. Racy by construction (a separate
        // read after the DELETE) — fine for diagnostics; every state change
        // re-checks the guards atomically.
        let diagnostics = delete_refusal_diagnostics_query(self.residency_evidence);
        let (phase_drifted, uid_mismatch, snapshot_blockers, active_residents): (
            bool,
            bool,
            i64,
            i64,
        ) = sqlx::query_as(sqlx::AssertSqlSafe(sql!(diagnostics.as_str())))
            .bind(node_id)
            .bind(expected_uid)
            .bind(SubstrateKind::Kubernetes)
            .fetch_one(&self.pool)
            .await?;
        Ok(OrphanDeleteOutcome::Refused(OrphanDeleteRefusal {
            phase_drifted,
            uid_mismatch,
            snapshot_blockers,
            active_residents,
        }))
    }

    async fn mark_unrecoverable_sandboxes_lost(
        &self,
        node_id: NodeId,
        expected_uid: Option<String>,
    ) -> Result<Vec<SandboxId>, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let ids = gone_node::write_off_on_gone_node(
            &mut tx,
            node_id,
            WriteOff::UnrecoverableSuspended {
                expected_uid: expected_uid.as_deref(),
            },
            DEAD_UPLOAD_SETTLE,
        )
        .await?;
        tx.commit().await?;
        let node_label = node_id.to_string();
        for sandbox_id in &ids {
            let details = SandboxEventDetails::default()
                .with_from_state("suspended")
                .with_to_state("failed")
                .source_node_id(node_label.clone())
                .reason("gone_node_unrecoverable")
                .trigger(SandboxEventTrigger::System);
            emit_lost_and_failed(*sandbox_id, details, "drained_node_terminator");
        }
        Ok(ids)
    }
}

// ---------------------------------------------------------------------------
// Kube annotation lookup — gated behind `features = ["kubernetes"]`
// ---------------------------------------------------------------------------

/// Production [`DrainKeepAnnotationLookup`] that reads the
/// `space.pplx.net/drain-keep` annotation from the live kube Node.
/// Requires `features = ["kubernetes"]`.
#[cfg(feature = "kubernetes")]
pub struct KubeAnnotationLookup {
    pool: PgPool,
    kube_client: kube::Client,
}

#[cfg(feature = "kubernetes")]
impl KubeAnnotationLookup {
    pub fn new(pool: PgPool, kube_client: kube::Client) -> Self {
        Self { pool, kube_client }
    }
}

#[cfg(feature = "kubernetes")]
impl DrainKeepAnnotationLookup for KubeAnnotationLookup {
    async fn lookup_drain_keep(
        &self,
        node_id: NodeId,
    ) -> Result<DrainKeepStatus, AnnotationLookupError> {
        let row: Option<(String, Option<String>)> = sqlx::query_as(sql!(
            "SELECT k8s_node_name, k8s_node_uid FROM node_k8s WHERE node_id = $1"
        ))
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| AnnotationLookupError::Db(err.to_string()))?;
        let (node_name, stored_uid) = row.ok_or(AnnotationLookupError::Unregistered(node_id))?;

        let nodes: kube::Api<k8s_openapi::api::core::v1::Node> =
            kube::Api::all(self.kube_client.clone());
        let node = match nodes.get(&node_name).await {
            Ok(node) => node,
            Err(kube::Error::Api(resp)) if resp.code == 404 => {
                return Err(AnnotationLookupError::KubeNodeGone);
            }
            Err(err) => return Err(AnnotationLookupError::Kube(err.to_string())),
        };

        // UID guard against same-name kube Node replacement.
        if let (Some(stored), Some(current)) = (stored_uid.as_deref(), node.metadata.uid.as_deref())
            && stored != current
        {
            return Err(AnnotationLookupError::StaleIdentity);
        }

        // Cordon guard: a `drained` node must have `spec.unschedulable=true`
        // before we auto-advance it to `terminating`. If cordon was
        // accidentally lost, flipping now would open a window where the
        // kube-scheduler can place new pods before `node_drain` re-cordons
        // on its next `terminating` tick. Defer with a warning so an
        // operator can investigate; `node_drain` will re-cordon on its
        // next draining/terminating pass if the row is still in those phases.
        let cordoned = node
            .spec
            .as_ref()
            .and_then(|s| s.unschedulable)
            .unwrap_or(false);
        if !cordoned {
            return Err(AnnotationLookupError::Uncordoned);
        }

        match node
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(DRAIN_KEEP_ANNOTATION))
        {
            Some(reason) => Ok(DrainKeepStatus::Kept(reason.clone())),
            None => Ok(DrainKeepStatus::Absent),
        }
    }
}

/// Postgres-backed regression test for the `drained → terminating` gate: a
/// node must not advance to `terminating` while a suspend-snapshot upload is
/// still in flight from it.
#[cfg(test)]
mod inflight_upload_terminate_gate_tests;

/// Postgres-backed coverage for the resident guard's census term: it must
/// discount exactly the rows node-drain's `draining → drained` gate discounts,
/// and keep every row when the node cannot be seen.
#[cfg(test)]
mod resident_census_gate_tests;

/// Postgres-backed coverage for the Kubernetes substrate scope bound into
/// every terminator statement.
#[cfg(test)]
mod substrate_scope_tests;

/// Postgres-backed stored-lifecycle matrix for the mark-lost give-up.
#[cfg(test)]
mod stored_lifecycle_matrix_tests;

/// Postgres-backed coverage for what the give-up's write-off finalizes and
/// keeps, and for its lock on the kube uid.
#[cfg(test)]
mod gone_node_scope_tests;

/// Plan regression for the give-up claim on a node with a large, durable
/// pinned history.
#[cfg(test)]
mod giveup_claim_tests;
