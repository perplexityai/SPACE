use pplx_space_accounts::NodeId;
use pplx_space_node_model::{NodeDrainPhase, SubstrateKind};
#[cfg(feature = "kubernetes")]
use pplx_space_node_registration_store::lookup::{self as node_lookup, K8sNodeIdentity};
use pplx_space_node_registration_store::{drain_reads, drained_node, orphan_reap_refusal};
use pplx_space_snapshot_model::SnapshotId;

use pplx_space_events::{SandboxEventDetails, SandboxEventTrigger};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_residency::ResidencyEvidence;
use pplx_space_sandbox_store::drain_gate_blockers;
use pplx_space_sandbox_store::lost::{self, DEAD_UPLOAD_SETTLE, write_off_lost_sandboxes};
use pplx_space_snapshot_store::lost::{
    fail_quiet_full_uploads_from_node, lock_creating_full_uploads_from_node,
};
use sqlx::PgPool;

use crate::gone_node_write_off::{emit_lost_and_failed, node_identity_holds};

use super::{
    DRAIN_KEEP_ANNOTATION, DrainKeepAnnotationLookup, DrainKeepStatus, DrainedNodeTerminatorSink,
    OrphanDeleteOutcome, OrphanDeleteRefusal, PHASE_DRAINED,
};

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
    /// `pplx_space_sandbox_residency::active_resident_predicate`.
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
        drain_reads::nodes_in_drain_phase(
            &self.pool,
            NodeDrainPhase::Drained,
            SubstrateKind::Kubernetes,
        )
        .await
    }

    async fn count_local_snapshots(&self, node_id: NodeId) -> Result<i64, sqlx::Error> {
        // Mirrors the admin `:terminate` snapshot guard
        // (`admin_occupancy::terminate_occupancy`), routed through the same
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
        // `pplx_space_sandbox_portability::drain_gate_blockers_for_node`.
        drain_gate_blockers::count_on_node(&self.pool, node_id).await
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
        drained_node::mark_terminating(&self.pool, node_id, self.residency_evidence).await
    }

    async fn delete_drained_node_if_orphan(
        &self,
        node_id: NodeId,
        expected_uid: Option<String>,
    ) -> Result<OrphanDeleteOutcome, sqlx::Error> {
        if drained_node::delete_if_orphan(
            &self.pool,
            node_id,
            expected_uid.as_deref(),
            self.residency_evidence,
        )
        .await?
        {
            return Ok(OrphanDeleteOutcome::Deleted);
        }
        // Refused: re-evaluate each guard separately so the caller can log
        // and count the refusal per-reason. Racy by construction (a separate
        // read after the DELETE) — fine for diagnostics; every state change
        // re-checks the guards atomically.
        let orphan_reap_refusal::OrphanReapRefusal {
            phase_drifted,
            uid_mismatch,
            snapshot_blockers,
            active_residents,
        } = orphan_reap_refusal::diagnose(
            &self.pool,
            node_id,
            expected_uid.as_deref(),
            SubstrateKind::Kubernetes,
            self.residency_evidence,
        )
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
        if !node_identity_holds(
            &mut tx,
            node_id,
            Some(PHASE_DRAINED),
            expected_uid.as_deref(),
        )
        .await?
        {
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
        //
        // Only rows without a durable copy at the claim's snapshot are
        // claimed; see [`lost::lock_unrecoverable_candidates`] for why the flip
        // could act on no other row at that snapshot, and for the one-tick miss
        // on a row that loses durability after it.
        let claimed = lost::lock_unrecoverable_candidates(&mut tx, node_id).await?;
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
        let _locked_dead_uploads: Vec<SnapshotId> =
            lock_creating_full_uploads_from_node(&mut *tx, &claimed, node_id).await?;
        // The flip is restricted to the claimed ids: the claim only fences
        // rows that matched at its own snapshot, so a row entering the
        // candidate set after it must wait for the next tick's claim rather
        // than flip un-fenced.
        let ids = lost::mark_unrecoverable_sandboxes_lost(
            &mut tx,
            node_id,
            expected_uid.as_deref(),
            &claimed,
            DEAD_UPLOAD_SETTLE,
        )
        .await?;
        if !ids.is_empty() {
            fail_quiet_full_uploads_from_node(&mut *tx, node_id, &ids, DEAD_UPLOAD_SETTLE).await?;
            write_off_lost_sandboxes(&mut tx, node_id, &ids).await?;
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
        let K8sNodeIdentity {
            k8s_node_name: node_name,
            k8s_node_uid: stored_uid,
        } = node_lookup::k8s_node_identity(&self.pool, node_id)
            .await
            .map_err(|err| AnnotationLookupError::Db(err.to_string()))?
            .ok_or(AnnotationLookupError::Unregistered(node_id))?;

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
