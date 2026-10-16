//! Periodic reconciler that auto-advances `nodes.drain_phase` from
//! `drained` to `terminating` unless the corresponding k8s Node carries
//! the operator override annotation `space.pplx.net/drain-keep`.
//!
//! Ownership is split from `node_drain`:
//!
//! - `node_drain` owns the `draining -> drained` and
//!   `terminating -> deleted` transitions (busy: per-sandbox evacuation
//!   pipeline, default tick 5 s).
//! - This loop owns the *policy* transition `drained -> terminating`
//!   (default tick 60 s) and is intentionally annotation-aware so an
//!   operator can park a node in `drained` for AMI debug, incident
//!   response, etc. by setting the override annotation.
//!
//! The annotation value is a free-text reason (`ami-debug`,
//! `outage-investigation`, ...) recorded for audit; its presence (any
//! value) blocks auto-termination. Removing the annotation re-enables
//! it on the next tick. The annotation lives on the k8s Node, not in
//! Postgres — survives manual operator interventions, no DB schema
//! dependency, and removing/re-applying is a single `kubectl
//! annotate` round-trip.
//!
//! Like `node_drain`, this loop runs in the cluster-singleton
//! controller binary (`space-cluster-controller`); per the
//! repo-wide rule against background work in API servers, no part of
//! the policy lives in the API process.

use pplx_space_common_core::types::NodeId;
use pplx_space_common_core::types::SandboxId;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use pplx_space_metrics::{
    node_drain as node_drain_metrics, placement_fence as placement_fence_metrics,
};
use tokio_util::sync::CancellationToken;

use crate::env_config;
use crate::warn_pacer::PerNodeWarnPacer;
// Reuse the orphaned-nodes EC2 vocabulary verbatim so the two reconcilers
// don't fork the "is this instance gone?" verdict / error types. The
// fail-closed contract (`Gone` only on an explicit `terminated` state) is
// defined once in `orphaned_nodes::aws`.
pub use crate::orphaned_nodes::{InstanceLookupError, InstanceLookupOutcome};

#[cfg(feature = "kubernetes")]
pub mod aws;
pub mod store;

#[cfg(test)]
mod tests;

#[cfg(feature = "kubernetes")]
pub use aws::Ec2InstanceStateLookup;
#[cfg(feature = "kubernetes")]
pub use store::KubeAnnotationLookup;
pub use store::{AnnotationLookupError, NodeDbStore};

pub(crate) const PHASE_DRAINED: &str = "drained";
pub(crate) const PHASE_TERMINATING: &str = "terminating";
/// Synthetic `to` value for the `node_drain.transitions` metric on the
/// KubeNodeGone recovery delete. There is no `'deleted'` drain phase —
/// the row is hard-deleted — so this mirrors `node_drain`'s
/// `terminating -> deleted` synthetic transition.
pub(crate) const PHASE_DELETED: &str = "deleted";

/// Operator override annotation. Presence on the k8s Node (any value)
/// blocks the `drained -> terminating` flip. Value is a free-text
/// reason recorded for audit.
pub const DRAIN_KEEP_ANNOTATION: &str = "space.pplx.net/drain-keep";

pub(crate) const DEFAULT_TICK: Duration = Duration::from_secs(60);

/// Minimum spacing between successive WARN-level "delete refused" logs for
/// the same parked node. The refusal re-fires every tick (default 60 s) for
/// as long as the node stays parked, and a single permanently-parked node
/// has dominated an environment's warn volume before; between warns the
/// per-tick record is kept at info level, and the
/// `node_drain.kube_node_gone_delete_refused` counter is emitted every tick
/// regardless, so dashboards never lose resolution.
pub(crate) const REFUSAL_WARN_INTERVAL: Duration = Duration::from_secs(3600);

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// Master enable gate. Defaults to `false` so the reconciler is
    /// inert in every environment until the operator explicitly opts
    /// in via `SPACE_DRAINED_NODE_TERMINATOR_ENABLED=true`. The opt-in
    /// is per-environment, gated behind the same operational
    /// readiness as `node_drain` (durable suspend + IRSA on the
    /// terminating side).
    ///
    /// Opt-in for the same reason as `node_drain`: with the default `aws`
    /// terminator backend, enabling it on a non-AWS cluster would initialize
    /// the AWS SDK and attempt instance termination. It is force-disabled
    /// while `node_drain` is disabled.
    pub enabled: bool,
    pub tick: Duration,
    /// Gate for the KubeNodeGone mark-lost give-up: when the guarded
    /// hard-delete of a gone-node `drained` row is refused by the snapshot
    /// guard and the backing EC2 instance is confirmed terminated, mark the
    /// blocking suspended sandboxes that have no recoverable copy anywhere
    /// as `lifecycle = 'failed'` so the reap converges on the next tick.
    /// Always `true` in production configuration: once the instance is
    /// confirmed terminated, the row is flipped to `lost`. Tests set `false`
    /// to exercise the parked path.
    pub mark_lost_enabled: bool,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tick: DEFAULT_TICK,
            mark_lost_enabled: true,
        }
    }
}

impl ReconcilerConfig {
    /// Read config from env vars. Helm renders these under
    /// `clusterController.reconcilers.drainedNodeTerminator.*`.
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let config = env_config::EnvConfig(env);
        let default = Self::default();
        Self {
            enabled: config
                .truthy_default("SPACE_DRAINED_NODE_TERMINATOR_ENABLED", default.enabled),
            tick: default.tick,
            mark_lost_enabled: true,
        }
    }
}

/// Result of checking the `space.pplx.net/drain-keep` annotation on
/// the kube Node backing a SPACE drained node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainKeepStatus {
    /// Annotation absent — node is eligible for auto-termination.
    Absent,
    /// Annotation present with this value (free-text reason); skip
    /// this tick.
    Kept(String),
}

/// Pluggable annotation resolver. Decouples the kube Node lookup from the
/// DB-side sink so the reconciler core stays substrate-agnostic —
/// identical role to [`super::node_drain::InstanceTerminator`].
///
/// Production impl: [`KubeAnnotationLookup`] (kube-feature-gated).
/// Test impl: `FakeAnnotationLookup` in tests.
pub trait DrainKeepAnnotationLookup: Send + Sync + 'static {
    /// Resolve whether the k8s Node backing `node_id` carries the
    /// `space.pplx.net/drain-keep` operator override annotation.
    /// The implementation owns the UID guard against same-name kube
    /// Node replacements (see [`AnnotationLookupError::StaleIdentity`]).
    fn lookup_drain_keep(
        &self,
        node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<DrainKeepStatus, AnnotationLookupError>> + Send;
}

/// Outcome of the KubeNodeGone recovery's guarded hard-delete
/// ([`DrainedNodeTerminatorSink::delete_drained_node_if_orphan`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrphanDeleteOutcome {
    /// The guarded DELETE removed the row.
    Deleted,
    /// The guarded DELETE matched no row; the diagnosis names the guard(s)
    /// that refused so the park can be logged per-reason instead of as one
    /// opaque "delete refused".
    Refused(OrphanDeleteRefusal),
}

/// Per-guard diagnosis of a refused KubeNodeGone hard-delete. Read in a
/// separate statement *after* the DELETE matched no row, so it can race
/// concurrent writes — it informs logging, metrics, and the mark-lost
/// pre-check only; the DELETE and the mark-lost UPDATE re-check every guard
/// atomically. Several guards can refuse at once; a diagnosis where nothing
/// refuses ([`Self::is_transient_race`]) means the blocker resolved in the
/// window between the DELETE and the diagnostic read.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OrphanDeleteRefusal {
    /// The row is gone, no longer in `drain_phase = 'drained'`, or not
    /// `managed_by = 'k8s'` (admin undrain, manual terminate, concurrent
    /// reap).
    pub phase_drifted: bool,
    /// `node_k8s.k8s_node_uid` no longer matches the uid observed when the
    /// EC2 cross-check resolved the node — a same-name kube Node replacement
    /// registered after the kube 404.
    pub uid_mismatch: bool,
    /// Sandboxes blocking via the snapshot guard: suspended without a durable
    /// portable snapshot, mid-flight `suspending` / non-evacuation
    /// `restoring` pinned to the node, or an in-flight suspend upload from
    /// it. These are the mark-lost give-up's candidates (the `suspended`
    /// subset of them with no recoverable copy anywhere).
    pub snapshot_blockers: i64,
    /// Genuinely-active (non-suspended, non-terminal) sandboxes still pinned
    /// to the node via `active_node_id` — the resident guard. On a
    /// confirmed-terminated instance this is a state divergence to
    /// investigate, never something to auto-clear here.
    pub active_residents: i64,
}

impl OrphanDeleteRefusal {
    /// True when the diagnosis found every guard clear — the refusal was a
    /// race against a concurrent write that resolved before the diagnostic
    /// read (e.g. a snapshot finalized, or another reaper deleted the row's
    /// blocker). The next tick converges on its own.
    pub fn is_transient_race(&self) -> bool {
        !self.phase_drifted
            && !self.uid_mismatch
            && self.snapshot_blockers == 0
            && self.active_residents == 0
    }
}

/// Postgres-side surface. Pure DB operations — no kube dependency.
/// Production impl: [`NodeDbStore`]. Test impl: `FakeSink` in tests.
pub trait DrainedNodeTerminatorSink: Send + Sync + 'static {
    /// Node IDs currently in `nodes.drain_phase = 'drained'` (scoped to
    /// `managed_by = 'k8s'` — the SPACE direct/admin path has its own
    /// rows and must not be fed to a Kubernetes-specific pipeline).
    fn list_drained_nodes(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<NodeId>, sqlx::Error>> + Send;

    /// Count `sandboxes.lifecycle IN ('suspending','suspended','restoring')`
    /// rows whose `target_node_id = node_id`. These are the on-node
    /// local-disk snapshots whose only copy lives on the drained node;
    /// auto-advancing the node to `terminating` while any are present
    /// would let `node_drain` destroy the node and strand the snapshot.
    /// Mirrors the guard the admin `:terminate` endpoint enforces under
    /// `FOR UPDATE`. Removing it requires proof that every retained snapshot
    /// has a durable copy independent of this node.
    fn count_local_snapshots(
        &self,
        node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<i64, sqlx::Error>> + Send;

    /// Flip `nodes.drain_phase = 'terminating'` guarded on
    /// `drain_phase = 'drained'`. Returns `true` if the row was
    /// updated, `false` if the phase drifted under us (admin
    /// undrain, manual terminate, etc.).
    fn mark_terminating(
        &self,
        node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<bool, sqlx::Error>> + Send;

    /// Hard-delete a `drained` row whose backing EC2 instance is confirmed
    /// terminated and whose k8s Node is gone (the KubeNodeGone recovery
    /// path). There is no terminate / cordon step to run — the instance is
    /// already gone and there is no Node to cordon — so this mirrors
    /// `orphaned_nodes`' guarded DELETE rather than advancing to
    /// `terminating` (where `node_drain` would just re-park it for manual
    /// review, since it also can't cordon or ASG-terminate a missing node).
    ///
    /// Guarded atomically on `drain_phase = 'drained'` (admin-drift safe)
    /// plus the *same* snapshot + resident guards as `mark_terminating`, so a
    /// drained row is reapable on exactly the condition that would let it
    /// advance `drained -> terminating` normally — a durably-portable
    /// suspended sandbox doesn't block, but a local-only snapshot or a
    /// genuinely-resident active sandbox does (those stay parked for the
    /// sandbox sweepers and an operator).
    ///
    /// `expected_uid` is the `node_k8s.k8s_node_uid` observed when the EC2
    /// cross-check resolved this node's private DNS. The DELETE re-checks it
    /// (`IS NOT DISTINCT FROM`) and refuses if it changed, so a same-name
    /// kube Node replacement that registered after the kube 404 but before
    /// the inventory reflector reset `drain_phase` can't be reaped out from
    /// under the fresh node — same identity guard as `orphaned_nodes`'
    /// delete.
    ///
    /// Returns [`OrphanDeleteOutcome::Deleted`] if a row was deleted, or
    /// [`OrphanDeleteOutcome::Refused`] with the per-guard diagnosis if a
    /// guard refused.
    fn delete_drained_node_if_orphan(
        &self,
        node_id: NodeId,
        expected_uid: Option<String>,
    ) -> impl std::future::Future<Output = Result<OrphanDeleteOutcome, sqlx::Error>> + Send;

    /// Mark-lost give-up for the KubeNodeGone recovery: flip every
    /// `suspended` sandbox pinned to `node_id` via `target_node_id` that has
    /// **no recoverable copy anywhere** to `lifecycle = 'failed'` — the same
    /// terminal semantics as `node_drain`'s `mark_sandbox_lost` after an
    /// `ArtifactMissing` reupload (no durable portable snapshot, and no
    /// in-flight upload that could still produce one).
    ///
    /// **Only call this after EC2 confirmed the node's backing instance
    /// terminated** ([`InstanceLookupOutcome::Gone`]). That premise is what
    /// justifies the two ways this differs from the drain-path give-up:
    /// the sandbox's local artifact is *provably* gone (no on-node probe is
    /// possible or needed — the disk died with the box), and an in-flight
    /// suspend upload **from this node** can never finalize (the upload
    /// worker died with the box too), so only an upload from some *other*
    /// node vetoes the flip.
    ///
    /// The UPDATE re-checks the same kube-uid + drain-phase identity guards
    /// as [`Self::delete_drained_node_if_orphan`] atomically — `expected_uid`
    /// is the uid observed when the EC2 cross-check resolved the node — so
    /// the flip can't outlive the observation that justified it (a same-name
    /// kube replacement or an admin undrain between the cross-check and this
    /// UPDATE matches nothing).
    ///
    /// In the same transaction, the flip retires the stale bookkeeping it
    /// would otherwise orphan on a node-side auto-suspended candidate: any
    /// still-`active` execution row is written off (`'forgotten'`), the
    /// recorded pod IPs are cleared, and the sandbox is unplaced
    /// (`placement = 'unplaced'`, `active_node_id = NULL`) — the
    /// suspended-execution reaper that normally does this scans
    /// `lifecycle = 'suspended'` only, so a bare flip would hide the rows
    /// from it forever while `active_node_id` dangled at a node row the next
    /// tick hard-deletes. The flipped sandboxes' dead `creating` upload rows
    /// from this node are finalized `'failed'` too: the snapshot guard's
    /// in-flight arm is lifecycle-agnostic, so an un-terminalized dead
    /// upload would keep refusing the reap even after the flip. A same-node
    /// `creating` row only counts as dead once it has sat without a worker
    /// heartbeat for the `DEAD_UPLOAD_SETTLE` quiet period — EC2
    /// `terminated` proves the worker died, but a READY finalize for
    /// already-durable bytes can still be in the API path for a few more
    /// seconds; until the window passes, the sandbox defers (stays parked)
    /// rather than flips.
    ///
    /// Returns the ids of the sandboxes marked lost; an empty list means no
    /// pinned suspended sandbox was provably unrecoverable (or a guard
    /// drifted), and the row keeps parking for the sweepers / an operator.
    fn mark_unrecoverable_sandboxes_lost(
        &self,
        node_id: NodeId,
        expected_uid: Option<String>,
    ) -> impl std::future::Future<Output = Result<Vec<SandboxId>, sqlx::Error>> + Send;
}

/// Result of an [`InstanceStateLookup`]: the EC2 verdict plus the
/// `node_k8s.k8s_node_uid` observed in the same read that resolved the
/// node's private DNS. The uid is carried into the guarded delete so the
/// reap refuses on a same-name kube replacement (see
/// [`DrainedNodeTerminatorSink::delete_drained_node_if_orphan`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInstanceState {
    pub outcome: InstanceLookupOutcome,
    pub expected_uid: Option<String>,
}

/// EC2 cross-check seam for the KubeNodeGone recovery path. Resolves
/// whether the EC2 instance backing a `drained` node is definitively
/// terminated, so the reconciler can safely reap a row whose k8s Node is
/// gone (no `space.pplx.net/drain-keep` override can exist without a Node,
/// and a terminated instance has nothing left to protect).
///
/// Keyed by `node_id` (not private DNS) to match [`DrainKeepAnnotationLookup`]
/// — the production impl owns the `node_id -> node_k8s.k8s_node_name`
/// resolution, same as the annotation lookup does. Dyn-compatible (boxed
/// future) so the reconciler can hold it as an *optional* trait object:
/// present on the real-EC2 (`SPACE_NODE_DRAIN_TERMINATOR_KIND=aws`) deploy,
/// absent on the `kube-delete` / `noop` substrates (kind / Tilt / smokes)
/// where there is no EC2 to query and the fail-safe park is preserved.
///
/// Production impl: [`Ec2InstanceStateLookup`] (kube-feature-gated). Test
/// impl: `FakeSink` in tests.
pub trait InstanceStateLookup: Send + Sync + 'static {
    fn lookup_instance_state(
        &self,
        node_id: NodeId,
    ) -> Pin<Box<dyn Future<Output = Result<NodeInstanceState, InstanceLookupError>> + Send + '_>>;
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    pub drained_nodes: usize,
    pub kept: usize,
    pub flipped: usize,
    pub lookup_errors: usize,
    pub drift_skips: usize,
    /// Nodes whose `drained → terminating` flip gave up waiting for the `nodes`
    /// row lock ([`store::MARK_TERMINATING_LOCK_TIMEOUT`]) and deferred to the
    /// next tick. Separate from [`Self::drift_skips`] because nothing drifted
    /// and separate from [`Self::lookup_errors`] because nothing failed: some
    /// conflicting nodes-row writer held the row. Placement fences,
    /// registration/heartbeat, inventory upserts, and node-drain completion can
    /// all be that holder. A sustained nonzero value here reports contention,
    /// not a broken operation.
    pub contention_defers: usize,
    /// Nodes skipped because they still hold local-disk suspended
    /// snapshots. See [`DrainedNodeTerminatorSink::count_local_snapshots`].
    pub snapshot_holds: usize,
    /// Rows whose k8s Node is gone and which the KubeNodeGone recovery
    /// path advanced to terminal by hard-deleting the row (EC2 confirmed
    /// the backing instance terminated). See [`InstanceStateLookup`].
    pub kube_node_gone_recovered: usize,
    /// Rows whose k8s Node is gone but which could not be safely
    /// auto-recovered this tick: no EC2 cross-check configured, the
    /// instance is still live-but-deregistered, the EC2 lookup errored,
    /// or the guarded delete was refused. Left in `drained` for manual
    /// review. Surfaced via the `node_drain.kube_node_gone_parked` gauge.
    pub kube_node_gone_parked: usize,
    /// Suspended sandboxes the KubeNodeGone mark-lost give-up flipped to
    /// `lifecycle = 'failed'` this tick because their pinned node's EC2
    /// instance is confirmed terminated and no recoverable copy exists
    /// anywhere. See [`DrainedNodeTerminatorSink::mark_unrecoverable_sandboxes_lost`].
    pub sandboxes_marked_lost: usize,
}

pub struct Reconciler<S: DrainedNodeTerminatorSink, A: DrainKeepAnnotationLookup> {
    sink: Arc<S>,
    annotation_lookup: Arc<A>,
    /// EC2 cross-check for the KubeNodeGone recovery path. `None` on
    /// substrates without a real EC2 to query (`kube-delete` / `noop`);
    /// when absent, a missing k8s Node is parked for manual review exactly
    /// as before this recovery path existed.
    instance_lookup: Option<Arc<dyn InstanceStateLookup>>,
    /// Gate for the KubeNodeGone mark-lost give-up. See
    /// [`ReconcilerConfig::mark_lost_enabled`].
    mark_lost_enabled: bool,
    /// Paces the per-node "delete refused" WARN at [`REFUSAL_WARN_INTERVAL`].
    /// Pruned every tick down to the nodes still listed as `drained`, so a
    /// converged or undrained node can't leak its slot.
    refusal_warn_pacer: PerNodeWarnPacer,
}

impl<S: DrainedNodeTerminatorSink, A: DrainKeepAnnotationLookup> Reconciler<S, A> {
    /// `instance_lookup` is the EC2 cross-check for the KubeNodeGone
    /// recovery path; pass `None` on substrates without a real EC2
    /// (`kube-delete` / `noop`), where a missing k8s Node is parked for
    /// manual review exactly as before this recovery path existed.
    /// `mark_lost_enabled` gates the KubeNodeGone mark-lost give-up
    /// (see [`ReconcilerConfig::mark_lost_enabled`]); it is only reachable
    /// when `instance_lookup` is present, since the give-up requires an EC2
    /// `Gone` verdict.
    pub fn new(
        sink: S,
        annotation_lookup: A,
        instance_lookup: Option<Arc<dyn InstanceStateLookup>>,
        mark_lost_enabled: bool,
    ) -> Self {
        Self {
            sink: Arc::new(sink),
            annotation_lookup: Arc::new(annotation_lookup),
            instance_lookup,
            mark_lost_enabled,
            refusal_warn_pacer: PerNodeWarnPacer::new(REFUSAL_WARN_INTERVAL),
        }
    }

    pub async fn tick(&self) -> TickReport {
        let mut report = TickReport::default();
        let nodes = match self.sink.list_drained_nodes().await {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(error = %err, "drained_node_terminator: list_drained_nodes failed");
                return report;
            }
        };
        report.drained_nodes = nodes.len();
        // Prune warn-rate-limit slots for nodes that left the drained set
        // (converged, undrained, or reaped) so the map stays bounded by the
        // live drained-node cardinality.
        self.refusal_warn_pacer.retain_live(&nodes);

        for node_id in nodes {
            match self.annotation_lookup.lookup_drain_keep(node_id).await {
                Ok(DrainKeepStatus::Kept(reason)) => {
                    tracing::info!(
                        node_id = %node_id,
                        reason = %reason,
                        annotation = DRAIN_KEEP_ANNOTATION,
                        "drained_node_terminator: keeping drained node (drain-keep annotation set)"
                    );
                    node_drain_metrics::drain_keep_skips(&node_id.to_string());
                    report.kept += 1;
                }
                Ok(DrainKeepStatus::Absent) => {
                    self.advance_drained_node(node_id, &mut report).await;
                }
                Err(AnnotationLookupError::StaleIdentity) => {
                    // Same-name kube Node replacement. The inventory
                    // reflector will reset `drain_phase` shortly; skip
                    // silently so the next tick sees the current state.
                    tracing::info!(
                        node_id = %node_id,
                        "drained_node_terminator: deferring — kube Node UID changed, \
                         awaiting inventory reconcile"
                    );
                }
                Err(AnnotationLookupError::KubeNodeGone) => {
                    self.handle_kube_node_gone(node_id, &mut report).await;
                }
                Err(AnnotationLookupError::Uncordoned) => {
                    // The kube Node lost its cordon (manual kubectl uncordon,
                    // or a controller bug). Advancing to `terminating` now
                    // would leave a scheduling window open. Defer silently
                    // for one tick; `node_drain` will re-cordon if the row
                    // is still in a draining or terminating phase, and an
                    // operator should investigate the cordon loss.
                    tracing::warn!(
                        node_id = %node_id,
                        "drained_node_terminator: kube Node is not cordoned — \
                         deferring auto-termination to prevent sandbox scheduling window; \
                         check for manual kubectl uncordon or cordon-controller bug"
                    );
                    report.lookup_errors += 1;
                }
                Err(err) => {
                    tracing::warn!(
                        node_id = %node_id,
                        error = %err,
                        "drained_node_terminator: drain-keep annotation lookup failed"
                    );
                    report.lookup_errors += 1;
                }
            }
        }

        // Emit every tick, including zero, so an alert can evaluate
        // `kube_node_gone_parked > 0` directly without the series going
        // stale once the parked rows finally converge. Tagged with this
        // loop's phase; the node-drain reconciler emits the `draining`
        // series for its own KubeNodeGone recovery.
        node_drain_metrics::kube_node_gone_parked(
            PHASE_DRAINED,
            report.kube_node_gone_parked as u64,
        );

        report
    }

    /// Advance a `drained` node toward terminal via the normal annotation-
    /// absent path: snapshot guard, then the atomic `drained -> terminating`
    /// flip (which re-checks the snapshot + resident guards inside the
    /// UPDATE). Shared by the annotation-`Absent` arm and the KubeNodeGone
    /// recovery's terminated branch is *not* routed here — that branch
    /// hard-deletes instead (see [`Self::handle_kube_node_gone`]).
    async fn advance_drained_node(&self, node_id: NodeId, report: &mut TickReport) {
        // Local-snapshot guard. Mirrors the admin `:terminate` endpoint's
        // check; without it, auto-advancing through `terminating` could
        // strand a `suspended` sandbox whose only snapshot lives on the
        // drained node's local disk. Drop once durable suspend lands.
        match self.sink.count_local_snapshots(node_id).await {
            Ok(0) => {}
            Ok(n) => {
                tracing::warn!(
                    node_id = %node_id,
                    local_snapshots = n,
                    "drained_node_terminator: holding drained node — \
                     still has suspended sandboxes pinned to local disk; \
                     terminating would strand their snapshots"
                );
                report.snapshot_holds += 1;
                return;
            }
            Err(err) => {
                tracing::warn!(
                    node_id = %node_id,
                    error = %err,
                    "drained_node_terminator: count_local_snapshots failed"
                );
                report.lookup_errors += 1;
                return;
            }
        }

        match self.sink.mark_terminating(node_id).await {
            Ok(true) => {
                node_drain_metrics::transition(PHASE_DRAINED, PHASE_TERMINATING);
                tracing::info!(
                    node_id = %node_id,
                    "drained_node_terminator: drained -> terminating"
                );
                report.flipped += 1;
            }
            Ok(false) => {
                // Phase drifted between list and mark — admin undrain,
                // manual terminate, or `node_drain` resetting under an
                // inventory race. Leave the row alone and re-evaluate next
                // tick.
                tracing::info!(
                    node_id = %node_id,
                    "drained_node_terminator: skipping flip; node phase changed under us"
                );
                report.drift_skips += 1;
            }
            Err(err) if crate::node_drain::store::is_lock_timeout(&err) => {
                // The flip's bounded `nodes`-row lock wait fired, not a database
                // problem: another transaction holds the row — a placement
                // writer inside the fence, or the node-drain reconciler's own
                // completion write. Debug, not warn, for the same reason
                // node-drain demotes it: a node whose row is held by a long
                // writer would otherwise emit one WARN per tick, forever, and
                // read identically to an outage. Not counted as a lookup error
                // either — nothing failed to be looked up, and the next tick
                // re-asks.
                tracing::debug!(
                    node_id = %node_id,
                    timeout_ms = store::MARK_TERMINATING_LOCK_TIMEOUT.as_millis() as u64,
                    "drained_node_terminator: flip deferred; nodes row contended"
                );
                placement_fence_metrics::termination_contention_deferral();
                report.contention_defers += 1;
            }
            Err(err) => {
                tracing::warn!(
                    node_id = %node_id,
                    error = %err,
                    "drained_node_terminator: mark_terminating failed"
                );
                report.lookup_errors += 1;
            }
        }
    }

    /// KubeNodeGone recovery. The k8s Node backing this `drained` row is
    /// gone, so the `space.pplx.net/drain-keep` override annotation can't be
    /// read. Rather than park indefinitely, cross-check EC2:
    ///
    /// - **Instance terminated** (`Gone`): no override can exist without a
    ///   Node, and the box is already gone, so hard-delete the row directly.
    ///   We don't advance to `terminating` — `node_drain` can neither cordon
    ///   a missing Node nor ASG-terminate a dead instance, so it would just
    ///   re-park the row in `terminating` for manual review.
    /// - **Instance live-but-deregistered** (`Live`): the genuinely
    ///   interesting case (manual Node deletion on a running box, reflector
    ///   blip). Keep parking for manual review — never auto-delete a row
    ///   whose instance is still alive.
    /// - **No EC2 cross-check configured / lookup errored**: fail safe, park,
    ///   retry next tick.
    async fn handle_kube_node_gone(&self, node_id: NodeId, report: &mut TickReport) {
        let Some(lookup) = self.instance_lookup.as_ref() else {
            tracing::warn!(
                node_id = %node_id,
                "drained_node_terminator: kube Node missing and no EC2 cross-check \
                 configured; leaving in drained for manual review"
            );
            report.kube_node_gone_parked += 1;
            return;
        };

        let (outcome, expected_uid) = match lookup.lookup_instance_state(node_id).await {
            Ok(state) => (state.outcome, state.expected_uid),
            Err(err) => {
                tracing::warn!(
                    node_id = %node_id,
                    error = %err,
                    "drained_node_terminator: kube Node missing and EC2 cross-check \
                     failed; leaving in drained for manual review"
                );
                report.lookup_errors += 1;
                report.kube_node_gone_parked += 1;
                return;
            }
        };

        match outcome {
            InstanceLookupOutcome::Gone => {
                match self
                    .sink
                    .delete_drained_node_if_orphan(node_id, expected_uid.clone())
                    .await
                {
                    Ok(OrphanDeleteOutcome::Deleted) => {
                        node_drain_metrics::transition(PHASE_DRAINED, PHASE_DELETED);
                        node_drain_metrics::clear_node_gauges(&node_id.to_string());
                        tracing::info!(
                            node_id = %node_id,
                            "drained_node_terminator: kube Node gone and EC2 instance \
                             terminated; hard-deleted orphaned drained row"
                        );
                        report.kube_node_gone_recovered += 1;
                    }
                    Ok(OrphanDeleteOutcome::Refused(refusal)) => {
                        // Guard refused. EC2 already confirmed the instance
                        // terminated, so if the refuser is the snapshot guard
                        // the blocking suspended sandboxes can never become
                        // portable on their own — attempt the mark-lost
                        // give-up so a later tick converges. Everything else
                        // (uid drift, phase drift, active resident) parks for
                        // the inventory reflector / sweepers / an operator.
                        let marked_lost = self
                            .give_up_unrecoverable_sandboxes(node_id, expected_uid, &refusal)
                            .await;
                        report.sandboxes_marked_lost += marked_lost;
                        self.log_delete_refusal(node_id, &refusal, marked_lost);
                        report.kube_node_gone_parked += 1;
                    }
                    Err(err) if crate::node_drain::store::is_lock_timeout(&err) => {
                        // Same classification `mark_terminating` gets, and for the
                        // same reason: the reap's bounded `nodes`-row lock wait
                        // fired because a conflicting writer holds the row. This
                        // can be placement, registration/heartbeat, inventory, or
                        // another lifecycle write. Not a `lookup_error` — that
                        // counter's own doc says contention must not read as an
                        // error, since nothing failed to be looked up — and debug
                        // rather than warn, because a row held by a long writer
                        // would otherwise emit one WARN per tick forever.
                        //
                        // Still counted as parked: the row did not reach terminal
                        // this tick, which is what that gauge reports.
                        tracing::debug!(
                            node_id = %node_id,
                            "drained_node_terminator: reap deferred; nodes row contended"
                        );
                        placement_fence_metrics::termination_contention_deferral();
                        report.contention_defers += 1;
                        report.kube_node_gone_parked += 1;
                    }
                    Err(err) => {
                        tracing::warn!(
                            node_id = %node_id,
                            error = %err,
                            "drained_node_terminator: delete_drained_node_if_orphan failed"
                        );
                        report.lookup_errors += 1;
                        report.kube_node_gone_parked += 1;
                    }
                }
            }
            InstanceLookupOutcome::Live { instance_id, state } => {
                tracing::warn!(
                    node_id = %node_id,
                    %instance_id,
                    %state,
                    "drained_node_terminator: kube Node missing but EC2 instance still \
                     live (deregistered node); leaving in drained for manual review"
                );
                report.kube_node_gone_parked += 1;
            }
        }
    }

    /// Mark-lost give-up half of the KubeNodeGone recovery. Called only from
    /// the EC2-`Gone` arm after the guarded delete was refused; fires only
    /// when the snapshot guard is among the refusers (those are its only
    /// candidates) and the give-up is enabled. The SQL re-checks every
    /// guard — lifecycle, portability, in-flight uploads from still-live
    /// nodes, kube-uid identity, drain phase — atomically, so the
    /// possibly-stale `refusal` diagnosis is only a pre-filter here.
    ///
    /// Returns the number of sandboxes marked lost this tick.
    async fn give_up_unrecoverable_sandboxes(
        &self,
        node_id: NodeId,
        expected_uid: Option<String>,
        refusal: &OrphanDeleteRefusal,
    ) -> usize {
        if refusal.snapshot_blockers == 0 {
            return 0;
        }
        if !self.mark_lost_enabled {
            tracing::info!(
                node_id = %node_id,
                snapshot_blockers = refusal.snapshot_blockers,
                "drained_node_terminator: gone-node reap blocked by non-portable \
                 sandboxes whose instance is terminated; mark-lost give-up disabled \
                 (the fixed-enabled mark-lost policy unset), leaving \
                 them suspended"
            );
            return 0;
        }
        match self
            .sink
            .mark_unrecoverable_sandboxes_lost(node_id, expected_uid)
            .await
        {
            Ok(ids) if ids.is_empty() => {
                // The snapshot guard's blockers weren't eligible: mid-flight
                // lifecycles (`suspending`/`restoring`), an upload still in
                // flight from another node, or a guard drifted under us.
                // Those park for the sweepers; nothing is unrecoverable yet.
                tracing::info!(
                    node_id = %node_id,
                    snapshot_blockers = refusal.snapshot_blockers,
                    "drained_node_terminator: no pinned suspended sandbox was provably \
                     unrecoverable; leaving the gone-node row parked"
                );
                0
            }
            Ok(ids) => {
                // Deliberately WARN every time it fires (not rate-limited):
                // declaring customer sandboxes lost is rare and terminal, and
                // each firing converges the node, so it cannot loop.
                tracing::warn!(
                    node_id = %node_id,
                    sandbox_ids = ?ids,
                    count = ids.len(),
                    "drained_node_terminator: marked suspended sandboxes lost \
                     (lifecycle = 'failed') — their pinned node's EC2 instance is \
                     terminated and no durable snapshot or live upload exists, so no \
                     recoverable copy remains; the gone-node reap converges next tick"
                );
                node_drain_metrics::kube_node_gone_sandboxes_marked_lost(
                    &node_id.to_string(),
                    ids.len() as u64,
                );
                ids.len()
            }
            Err(err) => {
                tracing::warn!(
                    node_id = %node_id,
                    error = %err,
                    "drained_node_terminator: mark_unrecoverable_sandboxes_lost failed; \
                     will retry on next tick"
                );
                0
            }
        }
    }

    /// Log a refused KubeNodeGone hard-delete with the per-guard reasons,
    /// WARN-rate-limited per node ([`REFUSAL_WARN_INTERVAL`]) so one
    /// permanently-parked node can't dominate warn volume; between warns the
    /// same record is kept at info. The per-reason counter is emitted every
    /// tick regardless, so dashboards keep full resolution.
    fn log_delete_refusal(
        &self,
        node_id: NodeId,
        refusal: &OrphanDeleteRefusal,
        marked_lost: usize,
    ) {
        let node_label = node_id.to_string();
        let mut reasons: Vec<&'static str> = Vec::new();
        if refusal.phase_drifted {
            reasons.push("phase_drift");
        }
        if refusal.uid_mismatch {
            reasons.push("uid_drift");
        }
        if refusal.snapshot_blockers > 0 {
            reasons.push("non_portable_suspended");
        }
        if refusal.active_residents > 0 {
            reasons.push("active_resident");
        }
        if refusal.is_transient_race() {
            reasons.push("transient_race");
        }
        for reason in &reasons {
            node_drain_metrics::kube_node_gone_delete_refused(&node_label, reason);
        }
        let reasons = reasons.join(",");
        if self.refusal_warn_due(node_id) {
            tracing::warn!(
                node_id = %node_id,
                reasons = %reasons,
                snapshot_blockers = refusal.snapshot_blockers,
                active_residents = refusal.active_residents,
                marked_lost,
                "drained_node_terminator: kube Node gone and EC2 instance terminated, \
                 but delete refused; leaving in drained (further refusals for this \
                 node log at info for the next hour)"
            );
        } else {
            tracing::info!(
                node_id = %node_id,
                reasons = %reasons,
                snapshot_blockers = refusal.snapshot_blockers,
                active_residents = refusal.active_residents,
                marked_lost,
                "drained_node_terminator: kube Node gone and EC2 instance terminated, \
                 but delete refused; leaving in drained (warn rate-limited)"
            );
        }
    }

    /// True iff the per-node refusal WARN is due (first refusal for the node,
    /// or [`REFUSAL_WARN_INTERVAL`] elapsed since the last one) — and if so,
    /// records the firing. The window walk itself is
    /// [`PerNodeWarnPacer`]'s and tested there.
    fn refusal_warn_due(&self, node_id: NodeId) -> bool {
        self.refusal_warn_pacer.due(node_id, "delete_refused")
    }
}

/// Drive the reconciler loop. Returns when `shutdown_token` fires.
///
/// When `config.enabled = false` (the default), the function logs the
/// inert state once and then waits on the shutdown token without ever
/// running a tick.
pub async fn run<S: DrainedNodeTerminatorSink, A: DrainKeepAnnotationLookup>(
    config: ReconcilerConfig,
    sink: S,
    annotation_lookup: A,
    instance_lookup: Option<Arc<dyn InstanceStateLookup>>,
    shutdown_token: CancellationToken,
) {
    if !config.enabled {
        tracing::info!(
            "drained-node-terminator reconciler is disabled \
             (SPACE_DRAINED_NODE_TERMINATOR_ENABLED unset); \
             waiting on shutdown without running any ticks"
        );
        shutdown_token.cancelled().await;
        tracing::info!("drained-node-terminator reconciler stopping");
        return;
    }

    tracing::info!(
        tick_secs = config.tick.as_secs(),
        ec2_cross_check = instance_lookup.is_some(),
        mark_lost_enabled = config.mark_lost_enabled,
        "starting drained-node-terminator reconciler"
    );
    let reconciler = Reconciler::new(
        sink,
        annotation_lookup,
        instance_lookup,
        config.mark_lost_enabled,
    );
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Consume the immediate first tick without processing. This gives
    // operators a full terminator tick window
    // to apply `space.pplx.net/drain-keep` after enabling the reconciler
    // (e.g. on a fresh deploy or env-var flip) before any existing drained
    // nodes are advanced to `terminating`.
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let _ = reconciler.tick().await;
            }
            _ = shutdown_token.cancelled() => {
                break;
            }
        }
    }

    tracing::info!("drained-node-terminator reconciler stopping");
}
