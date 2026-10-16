//! The API side of the termination fence.
//!
//! `pplx_space_common_svc::sql::node_placement_fence` owns the lock and the
//! verdict; this module owns what a refusal *looks like* — one metric, one WARN,
//! one message — so the six writers that commit `sandboxes.active_node_id` do
//! not each invent their own. The mechanism, the phases it fences, and why the
//! lock is `FOR KEY SHARE` are all documented there; read that first.
//!
//! Each writer keeps its own error type, because a refusal has to land on that
//! writer's existing teardown path rather than a shared one. Refusing to record
//! a placement does not stop the pod that Kubernetes already started; what
//! makes the refusal safe is that the operation then *fails*, and the caller's
//! rollback tears down what the fence declined to record. A writer that
//! swallowed this verdict and returned success would leave a row claiming a
//! healthy sandbox on an instance that is about to stop existing — which is the
//! outcome the fence exists to prevent, reached the long way round.

use pplx_space_common_core::types::{NodeId, SandboxId};
use pplx_space_common_svc::sql::node_placement_fence::{
    PlacementFence, Reaffirmation, fence_node_for_placement, placement_fence_enabled,
};

/// Which writer asked.
///
/// An enum rather than a module of `&'static str` consts, for the reason this
/// whole feature exists: a `&str` parameter accepts a literal or a typo and
/// compiles, and the risk model here is precisely "the enumeration is a list, not
/// a schema constraint." Six variants are exhaustively checkable; six consts are
/// not. [`Self::as_str`] is the only place the wire strings live, and they are the
/// `writer` tag on `placement_fence.refusals`, so renaming one breaks a monitor
/// rather than a build.
///
/// **One variant per statement, not per code path**, which is why there are six
/// for what reads like five: an ordinary create runs both
/// [`Self::PostCreatePlacement`] and [`Self::CreateFinalize`], and a restore runs
/// only the second. Fencing the first and not the second would have left the
/// restore path open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Writer {
    PooledPostWake,
    PostCreatePlacement,
    /// `finalize_created_sandbox` — the create/restore finalizer, and the only
    /// `active_node_id` write left on the restore path's *controller* leg: the
    /// placement-only `UPDATE` further up `create_sandbox` is gated on
    /// `restore_operation_id.is_none()` and so is skipped there.
    ///
    /// Not the only writer the column sees on a restore, though. On the
    /// Kubernetes substrate `persist_pod_node_placement` writes it earlier, off
    /// the pod-IP watch, and is fenced in its own right ([`Self::PodPlacement`]).
    CreateFinalize,
    PooledCreate,
    DirectRestore,
    /// `persist_pod_node_placement` — the pod-IP watch's write. The only writer
    /// that can name the execution its write belongs to, and the only one that
    /// has to: see [`Self::takes_reaffirmation_carve_out`].
    PodPlacement,
}

impl Writer {
    /// Whether this writer is *eligible* for the re-affirmation carve-out.
    ///
    /// Eligible, not granted: [`Self::PodPlacement`] additionally has to prove the
    /// re-statement is its own, by naming the node its execution is on
    /// ([`check_placement_fence_for_execution`]). Everything below is the first of
    /// those two conditions.
    ///
    /// **The carve-out's whole justification is the teardown**, so it belongs only
    /// to the writers that have one: refusing them makes the caller delete a live
    /// sandbox, which is worse than letting a re-statement through. A writer whose
    /// refusal is *not* a teardown has no such trade and must take the plain
    /// verdict.
    ///
    /// [`Self::DirectRestore`] is the one this distinction was found on.
    /// `active_node_id` survives a suspend on the direct substrate, so a restore
    /// aimed back at the *source* node meets a row that still names it — which
    /// looks identical to a re-affirmation and is not one: the guest is not
    /// running there, this is a fresh attach, and letting it through publishes a
    /// newly restored sandbox on a node that may already be `terminating`. Its
    /// refusal returns `RestoreInFlight`, which retries the restore. The restore
    /// that already ran on the node is lost: a same-node restore's VM is left on
    /// a node that is leaving, and a remote restore's destination VM is deleted
    /// best-effort.
    ///
    /// [`Self::PooledCreate`] cannot meet an already-attached row: it *inserts*
    /// the row. [`Self::PostCreatePlacement`] can — on the Kubernetes substrate
    /// `persist_pod_node_placement` has usually written the column before it runs,
    /// so its `UPDATE` is frequently a rewrite of the same value. An earlier
    /// revision of this comment claimed it "runs once on a row whose
    /// `active_node_id` is NULL"; that is true only on the direct substrate.
    /// Withholding the carve-out from it is therefore a real choice rather than a
    /// no-op, and the right one: its refusal *is* a teardown
    /// (`rollback_created_sandbox`), but it runs before the guest is published, so
    /// tearing down costs a retry rather than a live sandbox.
    ///
    /// **Why [`Self::CreateFinalize`] keeping the carve-out is safe**, given it
    /// runs on the restore path where its row may already name the node: the
    /// interlock is not this predicate but the pair of orderings around it. If the
    /// node reached `drained` *before* the pod landed,
    /// [`Self::PodPlacement`] refuses — the column is still NULL there (a
    /// Kubernetes suspend clears it), so its carve-out does not fire. If the pod
    /// landed first, the sandbox is `restoring`, which is in
    /// `RESIDENT_LIFECYCLES`, so node-drain's `draining → drained` guard — a
    /// `NOT EXISTS` evaluated in a statement *after* its `FOR UPDATE`, hence on a
    /// fresh snapshot — sees the attachment and refuses to complete the drain. So
    /// by the time the finalizer runs, either the placement was already refused or
    /// the node cannot have reached a refusing phase. The carve-out is
    /// re-affirming something that is still true, which is exactly its contract.
    /// On the direct substrate `active_node_id` survives a suspend, so the
    /// carve-out can fire on a restore back to the source node; what keeps that
    /// safe there is that [`Self::DirectRestore`], which takes no carve-out,
    /// already fenced the same node inside the substrate's restore.
    /// **Why [`Self::PodPlacement`] needs the execution condition and the others
    /// do not.** A `started` sandbox keeps `active_node_id` across an execution
    /// replacement, so this writer — which re-runs on every pod watch event — can
    /// meet a row that names its node because a *predecessor* ran there. Node id
    /// alone cannot tell that from a re-statement, and the miss is not merely a
    /// placement onto a departing node: this writer's `UPDATE` no-ops when
    /// `active_node_id`, `target_node_id` and `placement` all already match, so a
    /// wrongly-granted carve-out also leaves `placement_changed_at` stale — and
    /// that staleness is what let the census discount the sandbox and the drain
    /// complete. The other two eligible writers run once per operation on a row
    /// whose placement they are themselves establishing, so they have no
    /// predecessor to inherit from.
    const fn takes_reaffirmation_carve_out(self) -> bool {
        match self {
            Self::CreateFinalize | Self::PodPlacement | Self::PooledPostWake => true,
            Self::PostCreatePlacement | Self::PooledCreate | Self::DirectRestore => false,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PooledPostWake => "pooled_post_wake",
            Self::PostCreatePlacement => "post_create_placement",
            Self::CreateFinalize => "create_finalize",
            Self::PooledCreate => "pooled_create",
            Self::DirectRestore => "direct_restore",
            Self::PodPlacement => "pod_placement",
        }
    }
}

/// Take the fence in `conn`'s transaction and report the verdict, emitting the
/// refusal metric and WARN as a side effect.
///
/// Reporting lives here rather than at the call sites because the refusal is
/// rare, load-bearing, and easy to log inconsistently: the operator needs the
/// node, the sandbox, and which writer raced, and a site that logged only its
/// own error message would lose the node id — the one field that makes the
/// event correlatable with the drain it lost to.
///
/// # It fences attachments, not re-affirmations
///
/// A sandbox whose `active_node_id` is **already** this node is reported
/// [`PlacementFence::Open`] even when the node's phase refuses — but only for the
/// writers [`Writer::takes_reaffirmation_carve_out`] grants it to, and that
/// scoping is the load-bearing part. Refusing those writers makes their caller
/// tear the sandbox down, so a *live, already-placed* sandbox on a node that has
/// since reached `drained` would be deleted by the very mechanism that exists to
/// stop sandboxes being destroyed — a parked node for an operator to resolve by
/// hand, not one to resolve by deleting someone's sandbox.
/// A writer without that teardown has no such trade and takes the plain verdict.
///
/// **The carve-out is not a shortcut past the lock.** The fence always issues its
/// locking `SELECT … FOR KEY SHARE` on the `nodes` row and classifies the phase
/// first; the sandbox read that can override the verdict runs only *after* that,
/// and only when the verdict already refuses. So the re-affirmation path takes the
/// row lock like any other — which matters for reasoning about the pod-watch
/// caller, the highest-frequency one — and an ordinary placement onto an
/// unfenced node costs exactly one statement, because the second never runs.
///
/// **It never skips a first attach**, and getting that right cost two attempts.
/// The sandbox read is a separate statement after the lock, under
/// `FOR NO KEY UPDATE` on the sandbox row, so its answer is fresh at the moment
/// the lock is held *and* pinned until the caller's write. An intermediate
/// revision folded it into the locking `SELECT` as a correlated `EXISTS` — one
/// round trip, and apparently one snapshot — which inverted the property:
/// `FOR KEY SHARE` re-reads the locked tuple through EvalPlanQual but evaluates
/// the rest of the plan on the pre-wait snapshot, so a detach committing during
/// the wait left the carve-out reporting a placement that no longer existed and
/// the fence returned open for a first attach onto a `drained` node.
/// `node_placement_fence`'s own comment carries the measurement.
///
/// In every path where the carve-out fires, the write that originally recorded
/// that node id went through the fence itself — the pooled create at
/// `insert_pooled_sandbox`, a plain create at `post_create_placement`, a restore
/// at whichever of the two ran, a reconciling pod placement at the create or wake
/// that first recorded it. So it is always re-stating a decision the fence has
/// already made, never making a new one. The corollary for `pooled_post_wake` is
/// that its own check is a no-op in practice; it stays for the next caller of that
/// `pub` helper, which may well attach from NULL.
///
pub(crate) async fn check_placement_fence(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    sandbox_id: SandboxId,
    writer: Writer,
) -> Result<PlacementFence, sqlx::Error> {
    check_placement_fence_for_execution(conn, node_id, sandbox_id, writer, None).await
}

/// [`check_placement_fence`] for a writer that can name the Kubernetes node its
/// write is recording.
///
/// Only `pod_placement` can, and passing the name is what stops its carve-out
/// from reading an *inherited* placement as a re-statement of its own: a
/// `started` sandbox keeps `active_node_id` across an execution replacement, so
/// a fresh recovery execution landing on the old node matches on node id alone.
/// See [`Reaffirmation::active_k8s_node_name`].
pub(crate) async fn check_placement_fence_for_execution(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    sandbox_id: SandboxId,
    writer: Writer,
    active_k8s_node_name: Option<&str>,
) -> Result<PlacementFence, sqlx::Error> {
    let fence = fence_node_for_placement(
        conn,
        node_id.as_uuid(),
        // Scoped, not unconditional: see [`Writer::takes_reaffirmation_carve_out`].
        // An earlier revision landed that predicate and its rationale but left
        // this argument as a bare `Some(sandbox_id)`, so the carve-out applied
        // to every writer including the one whose doc says it must not — dead
        // code carrying a live hole.
        writer
            .takes_reaffirmation_carve_out()
            .then_some(Reaffirmation {
                sandbox_id: sandbox_id.as_uuid(),
                active_k8s_node_name,
            }),
        placement_fence_enabled(),
    )
    .await?;
    if fence.refuses() {
        pplx_space_metrics::placement_fence::refusal(writer.as_str(), fence.as_str());
        tracing::warn!(
            node_id = %node_id,
            sandbox_id = %sandbox_id,
            writer = writer.as_str(),
            verdict = fence.as_str(),
            "placement_fence: refusing to attach a sandbox to a node that is \
             leaving the fleet; the operation will fail without committing the placement"
        );
    }
    Ok(fence)
}

/// The message every refusal reports, so a client seeing one from any of the
/// six writers sees the same explanation.
///
/// Names the node and the verdict: "which node" is what an operator correlating
/// against a rollout needs, and the verdict distinguishes "the instance is being
/// destroyed right now" (`terminating`) from "one reconciler tick away"
/// (`drained`). Those two plus `missing_node` and `unrecognized_phase` are the
/// whole vocabulary: a `draining` node is never refused in either mode, while a
/// missing node is fail-closed and reaches this function.
pub(crate) fn fence_refusal_message(node_id: NodeId, fence: PlacementFence) -> String {
    format!(
        "node {node_id} is leaving the fleet ({}); refusing to place this sandbox on it — retry \
         to be placed elsewhere",
        fence.as_str()
    )
}
