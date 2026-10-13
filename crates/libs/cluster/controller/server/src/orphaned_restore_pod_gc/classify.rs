//! Pure classification vocabulary: the reflector-object → [`RestorePodSnapshot`]
//! extraction and the disposition that decides, from a restore pod plus its
//! owning sandbox row, whether the pod is an orphan to reap. No kube client and
//! no async — everything here is a pure function, so the matrix is trivially
//! unit-testable.
//!
//! There is no age here. How long a pod has been an orphan is measured by the
//! reconciler's in-memory observation clock (keyed on pod UID), not a pod
//! timestamp — see the reconciler's `first_seen_orphan` doc.

use pplx_space_api::controllers::substrate::kubernetes::{SandboxPodKey, SandboxPodView};
use pplx_space_common_core::types::{PodUid, SandboxId, SandboxLifecycle};
use uuid::Uuid;

/// Flattened, time-library-agnostic view of the parts of a `Pod` the reaper
/// reasons about. Extracted once from the reflector object so the classifier
/// and tests never touch `k8s_openapi` types directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestorePodSnapshot {
    pub pod_name: String,
    /// Parsed into the shared [`PodUid`] newtype at this boundary. `None` when
    /// the pod object carries no (or an empty) `metadata.uid`; such a pod is
    /// never acted on (no UID to anchor the observation clock or the delete
    /// precondition — real pods always carry one).
    pub pod_uid: Option<PodUid>,
    /// `metadata.resourceVersion` — used as a delete precondition so a
    /// concurrent status update (e.g. the pod binding to a node after the
    /// verify GET) fails the delete with a benign conflict rather than racing.
    pub resource_version: Option<String>,
    /// Parsed into the shared [`SandboxId`] newtype at this boundary.
    pub sandbox_id: Option<SandboxId>,
    /// The pod's execution generation (`space.pplx.net/execution-generation`):
    /// the supersession-fence input, also recorded on the emitted event/log.
    pub generation: Option<i64>,
    /// `spec.nodeName` if the pod has been bound. `None` while still
    /// `Unschedulable` (inert — the pre-bind orphan the reaper closes before it
    /// can start a VM).
    pub node_name: Option<String>,
    /// `metadata.deletionTimestamp` is set — the pod is already terminating.
    pub terminating: bool,
    /// The pod carries a restore annotation (see
    /// [`SandboxPodView::has_restore_annotation`]). Always
    /// true on the cached scan (the backend filters on it); re-checked on the
    /// fresh read so a pod whose annotations changed isn't deleted.
    pub is_restore: bool,
}

impl RestorePodSnapshot {
    pub(crate) fn from_pod(pod: &SandboxPodView) -> Self {
        // The shared pod builder stamps the sandbox id into both the annotation
        // and a same-keyed label; read the annotation first and fall back to
        // the label (same convention as the stuck-ContainerCreating GC).
        let sandbox_id = pod
            .annotation_or_label(SandboxPodKey::SandboxId)
            .and_then(|raw| Uuid::parse_str(raw).ok())
            .map(SandboxId::from_uuid);
        // Execution generations start at 1. A zero or negative value is
        // malformed and would compare as strictly older than any active
        // execution, so it leaves the pod ineligible, like a missing one.
        let generation = pod
            .annotation_or_label(SandboxPodKey::ExecutionGeneration)
            .and_then(|raw| raw.parse::<i64>().ok())
            .filter(|generation| *generation > 0);
        let node_name = pod
            .node_name()
            .filter(|name| !name.is_empty())
            .map(str::to_owned);
        Self {
            pod_name: pod.name().to_owned(),
            pod_uid: pod.uid().and_then(|uid| uid.parse().ok()),
            resource_version: pod.resource_version().map(str::to_owned),
            sandbox_id,
            generation,
            node_name,
            terminating: pod.is_deleting(),
            is_restore: pod.has_restore_annotation(),
        }
    }
}

/// The owning `sandboxes` row as the classifier needs it: the lifecycle plus
/// the generation of the row's *active* execution. Produced by the store's
/// batched lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxRestoreState {
    pub lifecycle: RowLifecycle,
    /// `generation` of the sandbox's active execution row, or `None` when there
    /// is no active execution. Execution generations are per-sandbox monotonic
    /// (`MAX(generation) + 1`), so this is the fence: a pod whose generation is
    /// strictly below it — or that has no active execution at all — is provably
    /// superseded, and no current or future restore can adopt it. A pod at or
    /// above the active generation is left alone: it is the current execution,
    /// is still materializing, or sits at a generation a re-drive could resume
    /// and adopt.
    pub active_generation: Option<i64>,
}

/// The owning row's lifecycle as far as this loop can vouch for it.
///
/// `sandboxes.lifecycle` can hold values `SandboxLifecycle` has no variant for
/// (e.g. the node-local `cleanup_required`), and `managed_by` can name a
/// substrate other than Kubernetes. Decoding either straight into the enum — or
/// filtering such rows out of the query — is wrong for a destructive loop: a
/// decode error fails the whole batch every tick (silently disabling the
/// reaper), and a filtered-out row reads as `SandboxMissing` (reaping the pod of
/// a row that still exists). Both surface here as `Unrecognized` and are
/// skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowLifecycle {
    Known(SandboxLifecycle),
    Unrecognized,
}

/// Whether a restore pod is an orphan to reap, or why it is left alone. The
/// skip reasons are internal accounting (`TickReport` tallies + the
/// tick-complete log) — the reaper emits per-pod `cluster_reconciler.outcome`
/// only for the actionable orphan set, not for every scanned restore pod.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Reap: the owning row cannot legitimately own this restore pod.
    Orphaned(OrphanReason),
    Skip(SkipClass),
}

/// Why a restore pod is an orphan — carried onto the reaped event/log so a
/// reverted-row orphan and a deleted-row orphan are distinguishable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanReason {
    /// The owning row exists in a resident lifecycle with the pod's generation
    /// strictly below the row's active execution, or is `suspended`/`failed`
    /// with no active execution at all (the restore was reverted and its
    /// execution retired).
    RowReverted,
    /// No owning `sandboxes` row: it was deleted (sandbox delete / create
    /// rollback) after the pod teardown failed. The row-driven orphaned-sandbox
    /// reconciler can't discover a pod with no row, so this reaper — the only
    /// pod-driven cleanup for restore pods — owns it, or the VM leaks forever.
    SandboxMissing,
}

impl OrphanReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::RowReverted => "row_reverted",
            Self::SandboxMissing => "sandbox_missing",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SkipClass {
    /// The row is `restoring` — a restore is legitimately in flight (possibly a
    /// newer generation re-driving this very pod). Never interfere.
    RowRestoring,
    /// The row is in some other transient lifecycle (a concurrent
    /// create/start/pause/resume/suspend/stop/checkpoint/delete). Defer to that
    /// operation and the transient-lifecycle sweeper rather than race them.
    RowTransient,
    /// The pod's generation is at or above the row's active execution, so it is
    /// not provably superseded — never reap. This covers the live VM
    /// (`started`/`paused`), the failed-cleanup repair probe (`failed`, which
    /// can recover the row to `started` when the node reports a live VM), the
    /// current execution still materializing its UID, and a same-generation pod
    /// a restore re-drive could resume and adopt. Pod names embed the
    /// generation and are unique, so no second same-generation pod can coexist
    /// with the current one. A lingering active execution on a `suspended` row
    /// also lands here; retiring it is the suspend subsystem's job, not this
    /// loop's.
    ActiveRuntime,
    /// The owning row exists but isn't Kubernetes-managed, or holds a
    /// lifecycle value this loop doesn't recognize. It has an owner this loop
    /// can't reason about, so leave the pod alone.
    UnrecognizedRow,
    /// The row is `quarantined`: parked for an operator because its state is
    /// ambiguous. Never override that hold by deleting its pod.
    RowQuarantined,
    /// The row has no active execution but sits in a lifecycle that should
    /// carry one (`created`/`started`/`paused`/`stopped`). The pod is
    /// unadoptable, but nothing proves it holds no unique state (a `paused`
    /// VM lives only in its pod), so the inconsistency is left to the
    /// reconciler that owns it. Only a reverted restore's settle states
    /// (`suspended`/`failed`) with no active execution are reaped.
    NoActiveExecution,
}

impl SkipClass {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::RowRestoring => "row_restoring",
            Self::RowTransient => "row_transient",
            Self::ActiveRuntime => "active_runtime",
            Self::UnrecognizedRow => "unrecognized_row",
            Self::RowQuarantined => "row_quarantined",
            Self::NoActiveExecution => "no_active_execution",
        }
    }
}

/// Decide a restore pod's disposition from its owning row.
///
/// The reaper only ever deletes a **provably-superseded** pod — one that no
/// current or future restore can adopt — which is what makes the delete safe
/// without holding a lock across it (see the reconciler). Because execution
/// generations are per-sandbox monotonic (`MAX(generation) + 1`), a pod is
/// superseded exactly when its generation is strictly below the active
/// execution's, or when there is no active execution at all (a future restore
/// will claim a strictly-higher generation). Both cases are unadoptable: no
/// restore ever reuses a retired generation. Unadoptable isn't the same as
/// holding nothing unique, so the no-active-execution case is reaped only on
/// the reverted restore's settle states (`suspended`/`failed`), and a
/// `quarantined` row is never touched.
///
/// A pod at or *above* the active generation is **never** reaped, even if the
/// active execution names a different UID. Restore re-drives reuse the same
/// generation, so a same-generation pod can still be adopted by a restore that
/// resumes that generation — deleting it unlocked would race that adoption and
/// could destroy a live or recoverable runtime. Such a pod is the current
/// execution or is still materializing its UID; on a `suspended` row it may be
/// the carrier a failed revert teardown left behind, which the suspend
/// subsystem owns. Its writers clear `active` only once that pod is gone or a
/// durable snapshot exists, which is what makes this loop's
/// no-active-execution reap of a `suspended` row safe; a writer that cleared
/// `active` with the carrier still holding the only copy would break that.
///
/// Across the resident lifecycles: the live VM of a `started`/`paused` row, a
/// `failed` row's recovery candidate the failed-cleanup probe may repair, and a
/// lingering active execution on a `suspended` row all sit at or above the
/// active generation and are skipped; a reverted `suspended`/`failed` row whose
/// execution was retired, a deleted row, or a stale strictly-prior-generation
/// pod are superseded and reaped.
///
/// The classifier is deliberately conservative on `restoring` and every other
/// transient lifecycle: it never reaps a pod whose sandbox is mid-operation.
///
/// The match is **exhaustive over every `SandboxLifecycle` variant on purpose**
/// (no `_` arm): this is a destructive loop, so a lifecycle variant added later
/// must break compilation here and force an explicit reap/skip decision.
pub fn classify_disposition(
    state: Option<&SandboxRestoreState>,
    pod_generation: i64,
) -> Disposition {
    use SandboxLifecycle as L;
    let Some(state) = state else {
        // No row at all: the sandbox was deleted while this restore pod
        // lingered. No row-driven reconciler can find it, so reap it (a future
        // restore, if any, gets a fresh sandbox — it can't adopt this pod).
        return Disposition::Orphaned(OrphanReason::SandboxMissing);
    };
    let lifecycle = match state.lifecycle {
        RowLifecycle::Known(lifecycle) => lifecycle,
        RowLifecycle::Unrecognized => return Disposition::Skip(SkipClass::UnrecognizedRow),
    };
    match lifecycle {
        // In-flight restore (possibly a same-generation re-drive of this very
        // pod). Never interfere.
        L::Restoring => Disposition::Skip(SkipClass::RowRestoring),
        // Some other transient operation is in flight — defer to it and the
        // transient-lifecycle sweeper rather than race them.
        L::Creating
        | L::Starting
        | L::Pausing
        | L::Resuming
        | L::Suspending
        | L::Stopping
        | L::Deleting
        | L::Checkpointing => Disposition::Skip(SkipClass::RowTransient),
        // Parked for an operator — never override the hold.
        L::Quarantined => Disposition::Skip(SkipClass::RowQuarantined),
        // Resident / terminal states: reap iff the pod is provably superseded.
        L::Created | L::Started | L::Paused | L::Suspended | L::Stopped | L::Failed => {
            match state.active_generation {
                // No active execution. For a reverted restore's settle states
                // (`suspended`/`failed`) the pod is the abandoned attempt's
                // leftover: whatever retired its execution already meant the
                // pod to be gone, and a future restore claims a higher
                // generation. Any other resident lifecycle should carry an
                // active execution; without one nothing proves the pod holds no
                // unique state, so skip.
                None => match lifecycle {
                    L::Suspended | L::Failed => Disposition::Orphaned(OrphanReason::RowReverted),
                    _ => Disposition::Skip(SkipClass::NoActiveExecution),
                },
                // Strictly below the active execution — provably superseded by
                // the row's current runtime.
                Some(active_generation) if pod_generation < active_generation => {
                    Disposition::Orphaned(OrphanReason::RowReverted)
                }
                // At or above the active generation — the current execution,
                // still materializing, or a same-generation pod a re-drive could
                // resume and adopt. Not provably superseded, so never reap it
                // here.
                Some(_) => Disposition::Skip(SkipClass::ActiveRuntime),
            }
        }
    }
}
