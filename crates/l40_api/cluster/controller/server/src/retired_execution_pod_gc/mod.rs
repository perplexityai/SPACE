//! Retired-execution pod GC: deletes sandbox pods whose execution row is
//! retired (`active = FALSE`) and that no runtime will ever use again.
//!
//! ## Why these pods exist
//!
//! An execution can be retired in Postgres by a writer that has no Kubernetes
//! client, so its pod object is left behind:
//!
//! - **Node-published suspend finalization.** When a node suspends an idle
//!   sandbox on its own, the internal API applies the node's receipt
//!   (`finalize_suspended`) and retires the execution as `suspended`. The VM is
//!   gone and the pod's containers have exited, so the pod sits in `Succeeded`.
//!   Nothing deletes it until the sandbox's next restore sweeps older
//!   generations, or until kube-controller-manager's terminated-pod GC trims
//!   the namespace above `--terminated-pod-gc-threshold`.
//! - **Node-published activation rollback.** When a cold activation is
//!   interrupted on the node, the internal API applies the rollback receipt and
//!   retires the execution as `replaced`. The pod is still `Pending`, and
//!   kubelet keeps retrying its sandbox create, which sandbox-manager refuses
//!   because the generation is retired. The create's own cancel cleanup
//!   deletes the pod when it runs; this loop is the backstop when it doesn't
//!   (API restart, a cancel that never fires, a failed delete).
//!
//! ## Why the delete is safe
//!
//! A retired execution generation is final. Every activation assignment
//! requires `active`, generations are per-sandbox monotonic, `pod_name` is
//! unique across execution rows and names one generation, and sandbox-manager
//! refuses to start a retired generation's runtime. So a pod whose execution
//! row reads retired can't be adopted by any current or future runtime.
//!
//! What that alone doesn't rule out is a resident VM whose teardown would
//! capture state. The loop therefore reaps only two shapes:
//!
//! - **Terminal** (`TerminalRetired`): the pod is `Succeeded` or `Failed` and
//!   every container has terminated. There is no VM left to capture.
//! - **Never started** (`ReplacedNeverStarted`): the row was retired as
//!   `replaced`, the pod is still `Pending`, and it is not a restore pod.
//!   A `replaced` generation's capture is refused by the backup fence, and a
//!   `Pending` fresh-create pod never booted a VM. Restore pods in this shape
//!   are left to `orphaned_restore_pod_gc`, which applies the restore-specific
//!   supersession rules.
//!
//! Every other retired pod (`Running`, or a non-`replaced` retirement with
//! live containers) is skipped: its VM may still hold state, and the
//! suspended-execution reaper and the restore path own those.
//!
//! ## Safeguards
//!
//! - **Continuous observation.** A pod is a candidate only after it has
//!   classified as reapable for [`ReconcilerConfig::threshold`] on an
//!   in-memory clock keyed on pod UID. The clock resets on restart, on a cold
//!   cache or failed scan, and whenever the pod stops classifying as reapable.
//! - **Verify before destroy.** Immediately before deleting, the pod is
//!   re-read with a cache-bypassing GET and its row re-read from Postgres; the
//!   pod must keep its UID and still classify as reapable.
//! - **UID + resourceVersion preconditions** on the DELETE, so a pod that
//!   changed between the verify GET and the delete is left alone.
//! - **Rate limit + tick budget.** At most
//!   [`ReconcilerConfig::max_deletes_per_minute`] candidates are acted on per
//!   rolling minute, and each tick is bounded by
//!   [`ReconcilerConfig::tick_budget`].
//!
//! Ships default-disabled; when enabled, starts in dry-run. Dry-run runs the
//! full verify path and diverges only at the DELETE, reporting each would-reap
//! pod once.
//!
//! The loop needs no DB-convergence companion: the row already reflects the
//! desired state, so deleting the pod strands no lifecycle.

mod backend;
mod classify;
mod config;
mod reconciler;
mod store;

#[cfg(test)]
mod tests;

pub use config::ReconcilerConfig;
pub use reconciler::run;
