use pplx_space_common_core::types::NodeId;
use pplx_space_common_core::types::SnapshotId;
use pplx_space_common_core::types::SubstrateKind;

use pplx_space_common_core::sql;
use std::time::Duration;

use pplx_space_common_core::types::SandboxId;
use pplx_space_common_svc::sql::node_placement_fence::lock_node_for_phase_write;
use pplx_space_common_svc::sql::sandbox_residency::{self, ResidencyEvidence};
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_sandbox_portability::{
    drain_gate_blockers_for_node, inflight_suspend_upload_from_other_node_exists,
    snapshot_durable_for_source_release,
};
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

/// How long the `drained → terminating` flip waits for the `nodes` row lock
/// before deferring to the next tick.
///
/// Deliberately the same value as `node_drain::store::DRAIN_COMPLETION_LOCK_TIMEOUT`
/// and for the same reason, but a separate constant rather than a shared one:
/// the two reconcilers have different tick budgets and different candidate-list
/// sizes, so a future change to one's patience should not silently move the
/// other's. If they are ever unified, unify the tick loops first.
pub(crate) const MARK_TERMINATING_LOCK_TIMEOUT: Duration = Duration::from_secs(5);

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
/// resolved this node's private DNS. The `IS NOT DISTINCT FROM` identity guard
/// refuses the reap if the uid changed — a same-name (same `nodes.id`, since
/// it's `v5(name, role)`) kube Node replacement that registered after the kube
/// 404 bumps `node_k8s.k8s_node_uid` before the inventory reflector resets
/// `drain_phase`, and without this guard the fresh node's row could be reaped.
/// Mirrors `orphaned_nodes`' uid-guarded delete.
pub(crate) fn delete_drained_node_if_orphan_query(residency: ResidencyEvidence) -> String {
    let snapshot_blockers = snapshot_blockers_for_node();
    let resident_clause = resident_blocking_clause(residency);
    format!(
        "DELETE FROM nodes
         WHERE id = $1
           AND drain_phase = 'drained'
           AND managed_by = $3
           -- Identity guard: refuse if a same-name kube Node replacement
           -- bumped node_k8s.k8s_node_uid since the EC2 cross-check observed
           -- it (the row would now belong to the fresh node).
           AND EXISTS (
               SELECT 1 FROM node_k8s
               WHERE node_id = $1
                 AND k8s_node_uid IS NOT DISTINCT FROM $2
           )
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
    format!(
        "SELECT
             NOT EXISTS (
                 SELECT 1 FROM nodes
                 WHERE id = $1 AND drain_phase = 'drained' AND managed_by = $3
             ) AS phase_drifted,
             NOT EXISTS (
                 SELECT 1 FROM node_k8s
                 WHERE node_id = $1
                   AND k8s_node_uid IS NOT DISTINCT FROM $2
             ) AS uid_mismatch,
             (SELECT COUNT(*)::BIGINT
                FROM ({snapshot_blockers}) node_blockers) AS snapshot_blockers,
             (SELECT COUNT(*)::BIGINT FROM sandboxes
               WHERE {resident_predicate}) AS active_residents"
    )
}

/// Quiet period a same-node `creating` snapshot row must have sat without a
/// worker heartbeat (`COALESCE(last_attempt_at, timestamp)`) before the
/// mark-lost give-up treats it as provably dead. EC2 `terminated` already
/// proves the upload *worker* died with the instance — what it cannot bound
/// is a `FinalizeArtifactBackup(READY)` RPC that left the node before the
/// instance died and is still queued in the API path with all bytes already
/// durable in S3. Such an RPC lives for seconds (gRPC deadline), so ten
/// quiet minutes is overwhelmingly past it: a row still inside the window
/// defers the flip (the sandbox stays parked and the next ticks re-check);
/// one quiet for longer can no longer become `ready` and is finalized
/// `failed` so the reap converges.
pub(crate) const DEAD_UPLOAD_SETTLE: Duration = Duration::from_mins(10);

/// `$1` = node id, `$2` = the kube uid observed at the EC2 cross-check,
/// `$3` = the candidate ids the caller has already `FOR UPDATE`-claimed in
/// the same transaction (only those may flip — see the inline comment),
/// `$4` = the [`DEAD_UPLOAD_SETTLE`] quiet seconds for same-node `creating`
/// rows, `$5` = [`SubstrateKind::Kubernetes`]. Mark-lost give-up for the
/// KubeNodeGone recovery: flips every `suspended` sandbox pinned to the node
/// via `target_node_id` that has no recoverable copy anywhere to
/// `lifecycle = 'failed'`, returning the flipped ids.
///
/// The caller has already confirmed the node's backing EC2 instance is
/// **terminated**. That premise drives the two deliberate differences from
/// `node_drain`'s per-sandbox `mark_sandbox_lost_query`:
///
/// * No on-node `ArtifactMissing` probe precedes the flip — none is possible
///   (there is no node to ask) and none is needed: the local artifact died
///   with the instance, so "no durable snapshot" alone proves
///   unrecoverability.
/// * The in-flight-upload veto is scoped to uploads from **other** nodes
///   ([`inflight_suspend_upload_from_other_node_exists`]) instead of the
///   node-agnostic check: an upload still `creating` from the terminated
///   node can never finalize (the upload worker and its staged copies died
///   with the box), and letting that dead row veto the flip would re-create
///   the permanent park this give-up exists to break. An upload from any
///   still-live node may yet finalize `ready`, so it still vetoes.
///
/// The portability test is the same marker-aware
/// [`snapshot_durable_for_source_release`] the snapshot guard and
/// `mark_sandbox_lost_query` use, so "blocks the reap" and "eligible for
/// give-up" can't drift: a durably-portable suspended sandbox neither blocks
/// nor is flipped; a local-only one both blocks and (here) is flipped.
/// Mid-flight rows (`suspending` / `restoring`) are deliberately out of
/// scope — they keep blocking and stay parked for their own sweepers.
///
/// The `EXISTS` guards re-check the node's `drained` + Kubernetes-managed
/// phase and the `node_k8s.k8s_node_uid` identity (`IS NOT DISTINCT FROM
/// $2`) **inside the UPDATE**, mirroring [`delete_drained_node_if_orphan_query`]:
/// a same-name kube replacement or an admin undrain that lands after the EC2
/// cross-check makes this match nothing, so the terminal flip can never
/// outlive the observation that justified it.
///
/// **These guards alone are snapshot reads, not serialization.** Unlike the
/// hard-DELETE — which modifies the `nodes` row itself and therefore
/// serializes on its row lock — this UPDATE only reads `nodes` / `node_k8s`,
/// so an undrain or uid bump committing concurrently would be invisible to
/// the statement snapshot. The caller
/// ([`NodeDbStore::mark_unrecoverable_sandboxes_lost`]) closes that hole by
/// `FOR SHARE`-locking and re-checking both guard rows in the same
/// transaction before running this statement; run it only under that
/// protocol.
pub(crate) fn mark_unrecoverable_sandboxes_lost_query() -> String {
    let durable = snapshot_durable_for_source_release("sandboxes");
    let inflight_from_other_node =
        inflight_suspend_upload_from_other_node_exists("sandboxes", "$1");
    format!(
        "UPDATE sandboxes
            SET lifecycle = 'failed'
          WHERE lifecycle = 'suspended'
            AND target_node_id = $1
            -- Settle guard on same-node creating uploads ($4 = quiet
            -- seconds, see DEAD_UPLOAD_SETTLE): EC2 'terminated' proves the
            -- worker is dead but not that a READY finalize for already-
            -- durable bytes isn't still in the API path. A row heartbeated
            -- (or created) inside the window defers the flip; the sandbox
            -- keeps blocking the reap and later ticks re-check.
            AND NOT EXISTS (
                SELECT 1 FROM snapshots sn
                 WHERE sn.sandbox_id = sandboxes.id
                   AND sn.source_node_id = $1
                   AND sn.type = 'full'
                   AND sn.state = 'creating'
                   AND COALESCE(sn.last_attempt_at, sn.timestamp)
                       >= NOW() - make_interval(secs => $4::BIGINT)
            )
            -- Only rows the caller FOR UPDATE-claimed may flip. Under READ
            -- COMMITTED the claim only fences rows that matched at its own
            -- snapshot; without this restriction a row entering the
            -- candidate set between the claim and this statement would flip
            -- un-fenced, re-opening the FK-lock race with a concurrent
            -- BeginArtifactBackup insert. Late arrivals wait for the next
            -- tick's claim instead.
            AND id = ANY($3)
            AND NOT {durable}
            AND NOT {inflight_from_other_node}
            AND EXISTS (
                SELECT 1 FROM nodes
                WHERE id = $1 AND drain_phase = 'drained' AND managed_by = $5
            )
            AND EXISTS (
                SELECT 1 FROM node_k8s
                WHERE node_id = $1
                  AND k8s_node_uid IS NOT DISTINCT FROM $2
            )
          RETURNING id"
    )
}

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
        //
        // **The `FOR UPDATE` in its own statement is what makes those guards mean
        // anything against a concurrent placement**, and it is the terminate half
        // of the fence in `node_placement_fence`. Both reasons — the READ
        // COMMITTED rule that leaves a blocked UPDATE's `NOT EXISTS` over *other*
        // rows stale, and `FOR NO KEY UPDATE` not conflicting with the placement
        // writers' `FOR KEY SHARE` — live on
        // [`lock_node_for_phase_write`] rather than being restated here.
        //
        // The first of those was a live defect, not a precaution: before this,
        // `mark_terminating` was a bare pooled `UPDATE nodes`, so a placement that
        // committed while it waited was invisible to the guard and the node
        // advanced to `terminating` holding it.
        let mut tx = self.pool.begin().await?;
        lock_node_for_phase_write(&mut tx, node_id.as_uuid(), MARK_TERMINATING_LOCK_TIMEOUT)
            .await?;
        let query = mark_terminating_query(self.residency_evidence);
        let result = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(node_id)
            .bind(PHASE_TERMINATING)
            .bind(PHASE_DRAINED)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(result.rows_affected() > 0)
    }

    async fn delete_drained_node_if_orphan(
        &self,
        node_id: NodeId,
        expected_uid: Option<String>,
    ) -> Result<OrphanDeleteOutcome, sqlx::Error> {
        // Same `FOR UPDATE` preamble as `mark_terminating`, and it became
        // necessary *because* of the placement fence rather than in spite of it.
        // This is a hard `DELETE FROM nodes`, which takes `FOR UPDATE` on the row
        // — a mode the placement writers' new `FOR KEY SHARE` conflicts with. So
        // this statement can now block on a placement transaction where it never
        // could before, and a blocked statement's `NOT EXISTS` over `sandboxes`
        // is evaluated on the pre-wait snapshot (the READ COMMITTED rule
        // [`lock_node_for_phase_write`] documents). Measured: the DELETE waited
        // ~5s on a placement holding `FOR KEY SHARE`, then reported `DELETE 1`,
        // hard-deleting the `nodes` row while `sandboxes.active_node_id` had just
        // been committed pointing at it.
        //
        // The harm is a phantom row rather than a destroyed sandbox — this path
        // runs only after an EC2 cross-check confirmed the instance terminated,
        // so the guest is already gone — but a phantom created by a guard that
        // *looked* atomic is exactly what this feature exists to stop.
        let mut tx = self.pool.begin().await?;
        lock_node_for_phase_write(&mut tx, node_id.as_uuid(), MARK_TERMINATING_LOCK_TIMEOUT)
            .await?;
        let query = delete_drained_node_if_orphan_query(self.residency_evidence);
        let result = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(node_id)
            .bind(expected_uid.clone())
            .bind(SubstrateKind::Kubernetes)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
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
        // Intentionally does NOT bump `placement_changed_at`, matching
        // node_drain's mark-lost: that column tracks placement, and bumping
        // it here could mask freshness checks if a future read path still
        // consults `failed` rows.
        //
        // One transaction: the lifecycle flip plus the stale-bookkeeping
        // sweep the flip would otherwise orphan. A node-side auto-suspended
        // candidate (the API-driven `finish_suspended_execution` never ran
        // for it) still carries an `active = TRUE` execution row, a recorded
        // pod IP, and `placement = 'on_node'` / `active_node_id` pinned to
        // the gone node. The suspended-execution reaper that normally
        // retires those scans `lifecycle = 'suspended'` only, so flipping to
        // `failed` first would hide the rows from it forever — leaving a
        // phantom active execution and an `active_node_id` dangling at a
        // node row the next tick hard-deletes. Retiring here mirrors the
        // reaper's retire (execution write-off + pod-IP clear + unplace);
        // `'forgotten'` is the established pod-tracking write-off state (the
        // pod died with the node — it was never cleanly suspended). The
        // flip's row locks serialize this against the reaper's
        // `FOR UPDATE` + lifecycle re-check and against wake's
        // `suspended -> restoring` CAS, so either order commutes. Port
        // exposures are deliberately left alone: the sandbox is terminal, so
        // re-queueing them (the reaper's behavior for recoverable rows)
        // would only create work that can never apply.
        let mut tx = self.pool.begin().await?;
        // Lock-and-recheck the identity guards before the terminal flip
        // (the suspended-execution reaper's `FOR UPDATE` + re-check
        // pattern). The UPDATE's own `EXISTS` guards evaluate against the
        // statement snapshot without locking `nodes` / `node_k8s`, so a
        // concurrent admin undrain or reflector uid bump that commits
        // mid-transaction would not be observed — the hard-DELETE doesn't
        // have this hole only because it modifies the `nodes` row itself
        // and therefore serializes on its row lock. `FOR SHARE` is enough:
        // the guard rows are read, not written, and the shared lock blocks
        // the undrain/reflector UPDATE until this transaction commits (or
        // waits for theirs and then re-reads the latest committed version,
        // refusing on drift).
        let node_still_eligible: Option<i32> = sqlx::query_scalar(sql!(
            "SELECT 1 FROM nodes
              WHERE id = $1 AND drain_phase = 'drained' AND managed_by = $2
              FOR SHARE"
        ))
        .bind(node_id)
        .bind(SubstrateKind::Kubernetes)
        .fetch_optional(&mut *tx)
        .await?;
        let uid_still_matches: Option<i32> = sqlx::query_scalar(sql!(
            "SELECT 1 FROM node_k8s
              WHERE node_id = $1 AND k8s_node_uid IS NOT DISTINCT FROM $2
              FOR SHARE"
        ))
        .bind(node_id)
        .bind(expected_uid.clone())
        .fetch_optional(&mut *tx)
        .await?;
        if node_still_eligible.is_none() || uid_still_matches.is_none() {
            tx.rollback().await?;
            return Ok(Vec::new());
        }
        // Claim the candidate rows before evaluating the in-flight veto. A
        // concurrent `BeginArtifactBackup` INSERT holds only a `KEY SHARE`
        // FK lock on the referenced sandbox row, which does not conflict
        // with the flip UPDATE's `NO KEY UPDATE` row lock — so without this
        // claim the veto could evaluate before the insert commits and
        // terminally fail a sandbox whose durable copy is about to start
        // uploading. `FOR UPDATE` does conflict with `KEY SHARE`: the insert
        // either committed first (the flip's fresh statement snapshot sees
        // the `creating` row and vetoes) or blocks until this transaction
        // commits. `ORDER BY id` keeps the multi-row lock order
        // deterministic against other multi-row lockers. (An insert that
        // lands entirely after the commit is out of this transaction's
        // reach — same pre-existing semantics as node_drain's
        // `mark_sandbox_lost`, since `BeginArtifactBackup` does not check
        // the sandbox lifecycle.)
        let claimed: Vec<SandboxId> = sqlx::query_scalar(sql!(
            "SELECT id FROM sandboxes
              WHERE lifecycle = 'suspended' AND target_node_id = $1
              ORDER BY id
              FOR UPDATE"
        ))
        .bind(node_id)
        .fetch_all(&mut *tx)
        .await?;
        if claimed.is_empty() {
            tx.rollback().await?;
            return Ok(Vec::new());
        }
        // Lock the candidates' same-node `creating` upload rows before the
        // flip evaluates. The upload may have durably completed with its
        // `FinalizeArtifactBackup(READY)` still in flight when EC2 declared
        // the instance terminated; that finalizer only touches `snapshots`,
        // so the sandbox row locks above do not serialize it. With these
        // locks, a finalize that committed first is visible to the flip's
        // fresh statement snapshot (durable -> the sandbox is spared); one
        // that arrives while this transaction holds the lock waits and then
        // refuses on its own `state IN ('creating', …)` guard rather than
        // resurrecting the row this transaction finalizes `failed` — the
        // S3 object it uploaded is left for the storage sweeper. That
        // residual (terminal flip beats a finalize RPC still in flight one
        // tick after the instance was confirmed terminated) is accepted:
        // the bytes are preserved in S3 and the row is operator-revivable.
        let _locked_dead_uploads: Vec<SnapshotId> = sqlx::query_scalar(sql!(
            "SELECT id FROM snapshots
              WHERE sandbox_id = ANY($1)
                AND source_node_id = $2
                AND type = 'full'
                AND state = 'creating'
              ORDER BY id
              FOR UPDATE"
        ))
        .bind(&claimed)
        .bind(node_id)
        .fetch_all(&mut *tx)
        .await?;
        // The flip is restricted to the claimed ids: the claim only fences
        // rows that matched at its own snapshot, so a row entering the
        // candidate set after it must wait for the next tick's claim rather
        // than flip un-fenced.
        let query = mark_unrecoverable_sandboxes_lost_query();
        let ids: Vec<SandboxId> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(node_id)
            .bind(expected_uid)
            .bind(&claimed)
            .bind(DEAD_UPLOAD_SETTLE.as_secs().cast_signed())
            .bind(SubstrateKind::Kubernetes)
            .fetch_all(&mut *tx)
            .await?;
        if !ids.is_empty() {
            // Finalize the flipped sandboxes' dead `creating` uploads from
            // this node — the upload worker died with the instance, so no
            // `FinalizeArtifactBackup` will ever terminalize them. Without
            // this, the snapshot guard's lifecycle-agnostic in-flight arm
            // (`inflight_suspend_upload_from_node_exists`) keeps treating the
            // row as a live upload and refuses the reap forever — on a
            // sandbox that is now `failed` and therefore outside this
            // give-up's own candidate set. Scoped to `source_node_id = $2`
            // (uploads from other nodes vetoed the flip entirely, so none of
            // the flipped ids have one) and mirroring the worker's own
            // failure terminalization (`state = 'failed'`).
            sqlx::query(sql!(
                "UPDATE snapshots
                    SET state = 'failed'
                  WHERE sandbox_id = ANY($1)
                    AND source_node_id = $2
                    AND type = 'full'
                    AND state = 'creating'
                    AND COALESCE(last_attempt_at, timestamp)
                        < NOW() - make_interval(secs => $3::BIGINT)"
            ))
            .bind(&ids)
            .bind(node_id)
            .bind(DEAD_UPLOAD_SETTLE.as_secs().cast_signed())
            .execute(&mut *tx)
            .await?;
            sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions
                    SET active = FALSE,
                        state = 'forgotten',
                        pod_ip = NULL,
                        terminated_at = COALESCE(terminated_at, NOW()),
                        updated_at = NOW()
                  WHERE sandbox_id = ANY($1)
                    AND active"
            ))
            .bind(&ids)
            .execute(&mut *tx)
            .await?;
            sqlx::query(sql!(
                "UPDATE sandboxes
                    SET placement = 'unplaced',
                        active_node_id = NULL
                  WHERE id = ANY($1)"
            ))
            .bind(&ids)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        let node_label = node_id.to_string();
        for sandbox_id in &ids {
            let details = SandboxEventDetails::default()
                .with_from_state("suspended")
                .with_to_state("failed")
                .source_node_id(node_label.clone())
                .reason("gone_node_unrecoverable")
                .trigger(SandboxEventTrigger::System);
            SandboxEvent::new(
                *sandbox_id,
                SandboxEventType::SandboxLost,
                SandboxEventSource::ClusterController,
            )
            .emit_with(details.clone());
            SandboxEvent::new(
                *sandbox_id,
                SandboxEventType::SandboxFailed,
                SandboxEventSource::ClusterController,
            )
            .emit_with(details.operation("drained_node_terminator"));
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
