//! Cluster-singleton garbage collector for sandbox pods wedged in
//! `ContainerCreating`.
//!
//! When the containerd shim (`containerd-shim-space-v2`) or the on-node
//! volume-manager cannot finish creating a sandbox's microVM, the pod sits in
//! `Pending` with its container reporting `waiting.reason = "ContainerCreating"`
//! indefinitely. kubelet keeps retrying `SyncPod` for every such pod, and the
//! control plane plus volume-manager take sustained load from thousands of
//! never-completing creates — a backlog of several thousand such pods can drive
//! a cascading retry storm that overloads the apiserver and volume-manager.
//!
//! This loop scans the sandbox-pod reflector cache (no apiserver LIST — the
//! watch-backed `Store`), classifies pods that have been stuck in
//! `ContainerCreating` past a configurable [`ReconcilerConfig::threshold`], and
//! deletes them so the wedged create stops retrying. Deleting a bare sandbox
//! pod is terminal (no owning controller recreates it); the sandbox's DB row is
//! then converged by whichever reconciler owns its lifecycle. A stuck pod
//! normally belongs to a `creating`/`starting`/`started` sandbox (the latter
//! via the started-recovery `ensure_pod_exists` path) — the orphaned-sandbox
//! reconciler's domain. But a user suspend/checkpoint can race a `started` row
//! into `suspending`/`checkpointing` while its pod is still wedged, and those
//! transient states — plus a `failed` row whose best-effort execution
//! retirement didn't finish — are the transient-lifecycle sweeper's domain, not
//! orphaned-sandbox's. So **enabling live deletion requires *both* convergence
//! paths to be live: the orphaned-sandbox reconciler (enabled, not dry-run)
//! *and* the transient-lifecycle sweeper (enabled, with the suspending and
//! checkpointing *effective* ages bounded — not disabled, and not
//! a default large enough to prevent timely cleanup — and failed-sandbox cleanup
//! enabled).
//! Otherwise a deleted pod's row can be left stuck with no reconciler to retire
//! it** (this loop reads Postgres but never writes it). This is enforced at
//! runtime: a GC missing either convergence path fails closed and refuses to
//! run.
//!
//! Safeguards, in order of application:
//! - **Restore exclusion.** A pod whose restore is still in flight is never
//!   touched: it holds snapshot state mid-transition, and its state is one the
//!   orphaned-sandbox convergence prerequisite doesn't handle. The pod's restore
//!   annotations (checkpoint, or a suspend-restore source-node / storage-URI,
//!   covering both node-local and remote restores) select *which* pods to
//!   check, but the live rows decide: those annotations are stamped at build
//!   time and never cleared, so on their own they would exclude a
//!   long-since-restored pod for the rest of its life. "In flight" is two
//!   things, not one — a `restoring` row, **or** a started recovery, which
//!   deliberately leaves the row in `started` and records ownership on a
//!   `recovering` execution lease instead. Reading only the lifecycle would
//!   delete the pod a live started recovery is in the middle of creating.
//! - **Warm-pool exclusion.** A pod the warm pool still holds — inventory, a
//!   claim whose bind hasn't committed, or a slot the pool still owes a delete
//!   for — is never touched: the warm-pool reconciler owns it (deleting it
//!   under a Postgres slot lock), and such a slot has no logical `sandboxes`
//!   row for either convergence reconciler to retire, so deleting one here
//!   would leave a slot pinned against pool capacity. As with restores, the
//!   `space.pplx.net/pool-role=warm` label survives the bind, so the live slot
//!   row is what decides.
//! - **Fail-closed marker resolution.** Both exclusions above resolve against
//!   Postgres. Any answer the database can't give — a failed or timed-out
//!   query, a row whose state this build doesn't recognize, or a pod with no
//!   sandbox id — keeps the exclusion, so the fallback is always the
//!   over-excluding direction. A marker that only appears on the authoritative
//!   re-read is looked up there, before the delete.
//! - **Progress guard.** A pod is a candidate only if it is `Pending`, has a
//!   container waiting with reason `ContainerCreating`, and has **no** running
//!   container (an init or app container that is already `Running` means the
//!   pod is progressing, not stuck).
//! - **Continuous-stuck age.** The pod object has no reliable "in
//!   `ContainerCreating` since" timestamp (`status.startTime` is the pod's
//!   lifetime, which an old pod flipping from `ImagePullBackOff` keeps), so age
//!   is measured by an in-memory observation clock keyed on pod UID: a pod is a
//!   candidate only once it has held the stuck shape continuously for
//!   `threshold`. The clock resets on controller restart (the backlog then
//!   re-ages over one threshold before any deletion), which is the safe
//!   direction; a pod with no UID to anchor the clock is never acted on. The
//!   clock is driven by the periodic scan of the watch-coalesced reflector
//!   store, so a pod that leaves and re-enters `ContainerCreating` entirely
//!   *between* scans keeps its old first-seen time; proving continuity across
//!   an unobserved flap needs a signal the store snapshot doesn't carry and is
//!   tracked separately.
//! - **Node-state probe.** The pod's node must be present in the node reflector
//!   cache, `Ready`, and schedulable. A `NotReady` node's stuck pods are a
//!   node-level failure that node-drain / node remediation owns; a cordoned
//!   node is being drained; an unknown node cannot be verified. While the node
//!   is ineligible the **observation clock is paused** (reset each tick in the
//!   scan phase), so a pod doesn't accrue deletable age during a node outage
//!   and then get deleted the instant the node recovers — kubelet gets the full
//!   healthy-node threshold to finish the resumed create. The eligibility is
//!   also re-checked (from the watch-backed node cache — best-effort, not a
//!   fresh node GET) as a pre-GET filter and again immediately before the
//!   DELETE, since a node can cordon or go `NotReady` during the verify GET (up
//!   to the tick budget); the pod-identity fence (UID + resourceVersion) is the
//!   authoritative guard against removing a healthy pod.
//! - **Verify before destroy.** Immediately before deleting, the pod is
//!   re-read authoritatively (cache-bypassing GET) and must still classify as
//!   stuck, non-restore, non-warm-pool, with an unchanged UID. This catches a
//!   pod that recovered to `Running`, or revealed a restore/pool annotation
//!   absent from the stale cache, between the cached scan and the delete.
//! - **UID + resourceVersion preconditions.** The delete carries both the
//!   observed pod UID and the fresh GET's resourceVersion, so neither a
//!   same-name pod from a newer execution generation nor a same-UID pod whose
//!   status changed between the verify GET and the delete is collaterally
//!   removed.
//! - **Rate limit.** At most [`ReconcilerConfig::max_deletes_per_minute`]
//!   candidates are acted on per rolling minute — each costs at most one
//!   apiserver GET (verify) plus one DELETE — so clearing a large backlog
//!   cannot itself become a new storm.
//! - **Tick budget.** The per-candidate action phase is bounded by
//!   [`ReconcilerConfig::tick_budget`]; once it elapses the remaining
//!   candidates defer to the next tick. During an apiserver slowdown a single
//!   GET/DELETE can take the kube client's multi-minute timeout, so without
//!   this one tick could run for hours and starve subsequent scans.
//!
//! Ships disabled. When enabled, it runs live after verifying that the sibling
//! reconciliation paths can converge every sandbox state whose pod it deletes.
//!
//! ## Module layout
//! - [`config`] — deploy-time knobs (`ReconcilerConfig`, defaults, `from_env`).
//! - [`classify`] — pure `Pod` → [`classify::PodSnapshot`] extraction and the
//!   stuck / node-eligibility vocabulary.
//! - [`backend`] — the `GcBackend` dependency-injection seam and its live
//!   Kubernetes implementation.
//! - [`store`] — the `GcStore` seam: the Postgres lookups that turn the
//!   build-time pod markers into live ownership answers.
//! - [`events`] — `cleanup_*` sandbox-event emission.
//! - [`reconciler`] — the tick loop (rate-limit budget, fairness cursor,
//!   verify-then-delete action flow) and the `run` entrypoint.

mod backend;
mod classify;
mod config;
mod events;
mod reconciler;
mod store;

#[cfg(test)]
mod tests;

pub use config::ReconcilerConfig;
pub use reconciler::run;

// Internal vocabulary re-exported for the test module's `use super::*`.
#[cfg(test)]
pub(crate) use backend::{BackendError, DeleteOutcome, GcBackend};
#[cfg(test)]
pub(crate) use classify::{
    Lookup, MarkerOwnership, NodeEligibility, Owner, PodSnapshot, is_stuck_shaped,
};
#[cfg(test)]
pub(crate) use config::DEFAULT_INTERVAL;
#[cfg(test)]
pub(crate) use reconciler::{
    Clock, Reconciler, SkipReason, TickReport, convergence_allows_live, transient_lifecycle_covered,
};
#[cfg(test)]
pub(crate) use store::{GcStore, PostgresGcStore, StoreError};
