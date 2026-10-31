//! Cluster-singleton garbage collector for orphaned restore pods.
//!
//! ## The orphan this closes
//!
//! Source-aware restore placement pins a restore pod to a shortlist of nodes
//! via a required `nodeAffinity.matchFields: metadata.name` term. If a busy
//! source leaves the pod `Unschedulable`, the API restore path re-drives it
//! cross-node **in the same request** — an `Unschedulable` pod has never bound,
//! so tearing it down can't strand a workload. But if the restore is
//! interrupted (the process crashes, or the request is abandoned) while its
//! pinned pod is still `Unschedulable`, nothing in-process deletes the pod: the
//! in-request re-drive/cancel fences are `Drop`-based, so a hard crash
//! (SIGKILL/OOM) runs no destructor. If the source later frees capacity,
//! kube-scheduler binds the stale pod and the shim starts a microVM for a row
//! that no longer owns it — an **unrouted VM** holding a network slot and
//! memory with no routable owner.
//!
//! The existing settle paths cover most of this but leave residual gaps:
//! - The transient-lifecycle sweeper reverts an aged `restoring` row and, on
//!   revert, retires the execution — which *does* tear down the tracked pod.
//!   But that teardown is **best-effort with no retry** (a failed delete just
//!   logs and leaks), and it only fires for rows the sweeper itself
//!   transitions — a row moved out of `restoring` by any other settle path is
//!   never swept, so its stranded pod is never torn down.
//! - The *next* restore of the same sandbox runs a lingering-pod sweep, but it
//!   only deletes **strictly-older-generation** pods, and only if a later
//!   restore actually arrives.
//!
//! So a stranded restore pod can still bind after its row was reverted and sit
//! unrouted indefinitely. This reaper is the backstop that closes that window
//! promptly (and, shipping dry-run-first, measures how often it actually
//! happens).
//!
//! ## What it does
//!
//! Pod-cache-driven, like the stuck-ContainerCreating GC — but where that loop
//! *excludes* restore pods, this one *targets* them. Each tick it snapshots the
//! restore pods in the reflector cache, looks up each owning `sandboxes` row in
//! batched queries, and reaps the pods that are **provably superseded** (see
//! [`classify::classify_disposition`]). Reaping an inert `Unschedulable` pod
//! closes the window *before* it binds; reaping a bound pod hands the unrouted
//! VM to the node's generation-fenced pod teardown — the same path the restore
//! path's lingering-pod sweep already drives for these pods (`retire_execution`
//! instead stops or deletes the sandbox on the node before its pod DELETE
//! whenever it can reach the runtime, so it doesn't normally start a teardown
//! suspend). That teardown suspends a resident VM and registers the capture
//! with `BeginArtifactBackup`, whose generation fence refuses it only when
//! `pplx_space_sandbox_portability::execution_supersession::execution_generation_superseded`
//! holds for the pod's generation. The fence checks only an artifact's first
//! registration that carries an execution generation; any other registration is
//! admitted unchecked. An admitted capture registers as a `ready` snapshot the
//! next restore selects, so this loop reaps only pods that same predicate
//! supersedes, read over the same execution rows. A pod below the active
//! generation always qualifies. A pod on a `suspended`/`failed` row with no
//! active execution qualifies when a restore whose unmaterialized claim the
//! request abandoned (the re-drive cancellation guard), or that the API's
//! rollback or the transient-lifecycle sweeper reverted with abandoned-restore
//! retirement enabled, retired its execution as `replaced`, or once a newer
//! execution materialized. So, with retirement enabled, does a restore its
//! node rolled back, when the control plane applied the rollback receipt to a
//! row still `restoring` whose restore selection is proven to choose a durable
//! pre-restore snapshot; with it off, that execution reads `suspended`, and
//! this loop skips the pod (below). The refusal says the execution was superseded, and
//! the node releases the VM and discards the capture. A successful DELETE
//! therefore means that teardown has begun (the pod is terminating), not that
//! the VM is gone: it's reported as `requested_orphaned_restore_pod_delete`,
//! and the reap is only confirmed (`reaped_orphaned_restore_pod`,
//! `cleanup_completed`) once the pod object is observed gone — normally the
//! kubelet has stopped its containers (within the 10s grace period); a
//! force-delete also removes it, even while a partitioned node still runs the
//! VM. That's evidence about the pod object, not about what node-side teardown
//! did with the VM's state. A restore pod that stays terminating past
//! [`ReconcilerConfig::teardown_stuck_after`] is reported once as
//! `teardown_stuck` (with `cleanup_failed`) when this loop deleted it; a
//! restore pod stuck terminating that this loop didn't delete — including one
//! it had deleted before a controller restart — is logged at `warn` instead.
//! The record of which DELETEs this loop issued is in memory: after a restart a
//! pod it had deleted is still tracked for stuck reporting, but its eventual
//! disappearance can't be attributed, so that `cleanup_requested` gets no
//! closing event.
//!
//! ## Why the delete is safe without a lock
//!
//! Execution generations are per-sandbox monotonic (`MAX(generation) + 1`), so
//! a pod is *unadoptable* — no current or future restore can adopt it — when
//! its generation is strictly below the active execution's, or the row has no
//! active execution (a future restore claims a strictly-higher generation).
//! The reaper deletes **only** unadoptable pods. That is what makes the delete
//! safe without a lock: a concurrent restore racing in, or a DELETE the
//! deadline cancels yet the apiserver still applies, can only remove an
//! unadoptable pod — never a live runtime. No DB lock is held across the k8s
//! DELETE, sidestepping the lock-across-external-call hazards
//! (idle-in-transaction termination, a cancelled-but-applied DELETE) entirely.
//!
//! Unadoptable isn't the same as holding nothing unique. The
//! no-active-execution case is reaped only on the settle states of a reverted
//! restore (`suspended`/`failed`), and only when the shared predicate
//! supersedes the pod's generation (above). Any retirement other than
//! `replaced` with nothing newer leaves a pod the fence would admit. For
//! example: a plain suspend whose pod outlived it, a node-published activation
//! rollback to `suspended` with abandoned-restore retirement off, and an older
//! control plane's suspend finalization over a node-side rollback to `paused`,
//! the last two with the VM still resident. Such pods are skipped as
//! `SkipClass::CaptureAdmitted`: this loop never starts their capture. On a
//! `suspended` row, the sandbox's next restore normally removes the pod: a
//! restore whose plan resolves sweeps lingering older-generation pods, while
//! one whose plan never resolves abandons its claim without sweeping and leaves
//! the pod. When that restore sweeps before it mints its claim, a resident VM's
//! teardown capture still finds nothing newer and the fence admits it. The
//! teardown publishes the capture into the node's local suspend directory, and
//! a restore that lands on its source node, which is that node (the row's
//! target node), restores from those local bytes whatever storage URI it
//! selected (sbx-mgr's `decide_suspend_restore`), so it boots the capture. A
//! restore that lands on another node and stages its selected URI doesn't. One
//! that resolves its own source after the sweep can still select the capture on
//! a row with no current-artifact marker if it's ready by then, unless it's a
//! retry whose pre-restore source window excludes it. A restore whose plan
//! doesn't resolve before the claim sweeps only after it, and the fence refuses
//! the capture if it registers while that claim is still active; see below for
//! when it isn't. For these pods the skip behaves as this loop being off does.
//! Compared with reaping them, it means no capture for a sandbox that's never
//! restored and none loaded by a cross-node restore that stages its selected
//! URI; a restore on the source node loads the capture either way. A `failed`
//! row can't restore until `failed_restorable_repair` moves it back to
//! `suspended`, and that repair waits while a node still reports the sandbox,
//! which a resident VM's node does. Until then, or until the sandbox is
//! deleted, the pod stays. A `created`/`started`/`paused`/`stopped` row with no
//! active execution is an inconsistency another reconciler owns (a `paused` VM
//! lives only in its pod), and a `quarantined` row is parked for an operator;
//! both are skipped.
//!
//! The fence decides when the teardown capture registers, after the delete,
//! so the reap also relies on the verdict holding from this loop's last read
//! to then. `replaced` on an inactive row and a materialized pod are both
//! final, so a verdict that rests on either holds. A verdict that rests only on
//! a newer execution being active doesn't: a claim abandoned as `replaced`
//! before it created a pod stops superseding. That window comes with the
//! strictly-below-the-active-execution reap and predates the fence check; the
//! recheck right before the delete narrows it.
//!
//! A pod at or *above* the active generation is never reaped, even if the
//! active execution names a different UID: restore re-drives reuse the same
//! generation, so a same-generation pod is still adoptable by a restore that
//! resumes it, and deleting it unlocked would race that adoption.
//!
//! ## The orphans this loop deliberately does NOT handle
//!
//! A pod whose execution was retired as something other than `replaced`, with
//! nothing newer active or materialized, is unadoptable but left alone
//! (`SkipClass::CaptureAdmitted`, above): the backup fence would register its
//! teardown capture as the sandbox's newest snapshot.
//!
//! A same-generation pod on a `suspended` row whose execution is still active
//! is left alone. With abandoned-restore retirement enabled
//! (`SPACE_ABANDONED_RESTORE_RETIREMENT_ENABLED`, off by default), both
//! restore reverts (the API's rollback and the transient-lifecycle sweeper's)
//! retire their claim's execution as `replaced` in the same transaction, once
//! restore selection is proven to choose a durable pre-restore source, which
//! turns their leftover pod into the no-active-execution orphan this loop
//! reaps. What remains in this shape is left by a revert with retirement
//! disabled, or that predates it, or couldn't prove the source, or by a
//! suspend write that raced a wake and put a live, routed sandbox back to
//! `suspended`. The row can't tell them apart, and in the last case the pod
//! holds the only copy of the sandbox's state, so this loop only tallies them
//! (`SkipClass::SuspendedSameGeneration`). The next restore of the sandbox
//! clears such a pod, or this loop reaps it once the generation advances past
//! it.
//!
//! Safeguards, in order:
//! - **Restore-only.** Only pods carrying a restore annotation are considered;
//!   fresh-create pods are skipped at the source.
//! - **Supersession disposition.** A row that is `restoring` (including a
//!   same-generation re-drive) or in any other transient lifecycle, or whose
//!   active execution is at or above the pod's generation, is never reaped.
//!   Only a pod strictly below the active execution — or with no active
//!   execution on a `suspended`/`failed` row, and a generation the backup
//!   fence supersedes — is an orphan; `quarantined` rows and rows the loop
//!   can't vouch for are skipped. A deleted row is also an orphan (its pod is
//!   unadoptable), which the row-driven orphaned-sandbox reconciler can't
//!   discover, so this pod-driven loop owns it.
//! - **Continuous-orphan age.** A pod is a candidate only once it has
//!   classified as superseded continuously for [`ReconcilerConfig::threshold`]
//!   on an in-memory observation clock keyed on pod UID. Defense in depth:
//!   an in-flight restore's execution claim is inserted at the pod's
//!   generation before the pod is created, so the fence already skips it; the
//!   threshold rides out cache-vs-Postgres skew on top. The clock resets on
//!   controller restart (orphans re-age over one threshold), on a cold cache
//!   or failed scan, when the pod's fresh read or Postgres recheck fails, and
//!   whenever the pod stops classifying as superseded.
//! - **Verify before destroy.** Immediately before deleting, the pod is
//!   re-read authoritatively (cache-bypassing GET) — same UID, restore marker,
//!   sandbox id, and generation, and not terminating — and the owning row is
//!   re-classified from Postgres — so a pod the row re-claimed
//!   (a same-generation re-drive → `restoring`) or that became the current
//!   runtime between the cached scan and the delete is left alone.
//! - **UID + resourceVersion preconditions.** The delete carries both, so
//!   neither a same-name pod from a newer generation nor a same-UID pod that
//!   bound to a node between the verify GET and the delete is collaterally
//!   removed.
//! - **Rate limit + tick budget.** At most
//!   [`ReconcilerConfig::max_deletes_per_minute`] orphans are acted on per
//!   rolling minute, and each tick (classification plus the per-candidate
//!   action phase) is bounded by [`ReconcilerConfig::tick_budget`].
//!
//! Ships default-disabled; when enabled, starts in dry-run. Dry-run runs the
//! entire authoritative decision path (verify GET + DB recheck) and diverges
//! only at the DELETE, and reports each would-reap pod once (not once per
//! tick), so its would-reap volume is a faithful preview of what live mode
//! would delete.
//! A dry-run emits only the `cluster_reconciler.outcome` metric (`mode:dry_run`)
//! and the log — no `space.sandbox` cleanup event, since nothing was deleted.
//!
//! Unlike the stuck-ContainerCreating GC, this reaper needs no DB-convergence
//! companion: a superseded pod's owning row already reflects the desired state,
//! so the deleted pod strands no lifecycle for another reconciler to retire.
//!
//! ## Module layout
//! - [`config`] — deploy-time knobs (`ReconcilerConfig`, defaults, `from_env`).
//! - [`classify`] — pure `Pod` → snapshot extraction and the orphan
//!   disposition matrix.
//! - [`backend`] — the `PodBackend` + `SandboxStore` dependency-injection seams
//!   and the live Kubernetes pod backend.
//! - [`store`] — the Postgres `SandboxStore` classification query.
//! - [`events`] — `cleanup_*` sandbox-event emission.
//! - [`reconciler`] — the tick loop (observation clock, rate-limit budget,
//!   fairness cursor, verify-then-delete flow) and the `run` entrypoint.

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

// Internal vocabulary re-exported for the sibling test module's `use super::*`.
#[cfg(test)]
pub(crate) use backend::{BackendError, DeleteOutcome, PodBackend, SandboxStore};
#[cfg(test)]
pub(crate) use classify::{
    Disposition, OrphanReason, RestorePodSnapshot, RowLifecycle, SandboxRestoreState, SkipClass,
    classify_disposition,
};
#[cfg(test)]
pub(crate) use reconciler::{Clock, Reconciler};
