//! Pure classification vocabulary: the reflector-object → [`PodSnapshot`]
//! extraction, the "is this pod wedged in `ContainerCreating`" shape check, and
//! node eligibility. No kube client and no async — everything here is a pure
//! function of the pod object, which keeps it trivially unit-testable.
//!
//! Note there is no age here: how long a pod has been wedged is **not** a
//! reliable pod-object property (`status.startTime` is the pod's lifetime, not
//! time-in-`ContainerCreating` — an old pod that just flipped from
//! `ImagePullBackOff`, or was recreated after a kubelet restart, keeps its
//! original `startTime`). The reconciler measures continuous-`ContainerCreating`
//! age with an in-memory observation clock instead.

use pplx_space_api::controllers::substrate::kubernetes::{SandboxPodKey, SandboxPodView};
use pplx_space_common_core::kubernetes::SANDBOX_POOL_ROLE_WARM;
use pplx_space_common_core::types::{PodUid, SandboxId};
use uuid::Uuid;

/// kubelet's `waiting.reason` while the container runtime is creating the
/// container (for SPACE sandbox pods, the shim setting up the microVM).
const CONTAINER_CREATING_REASON: &str = "ContainerCreating";

/// Flattened, time-library-agnostic view of the parts of a `Pod` the GC reasons
/// about. Extracted once from the reflector object so the classifier and tests
/// never touch `k8s_openapi` types directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodSnapshot {
    pub pod_name: String,
    /// Parsed into the shared [`PodUid`] newtype at this boundary and kept typed
    /// through the reconciler (observation-clock key, delete UID precondition).
    /// `None` when the pod object carries no (or an empty) `metadata.uid`.
    pub pod_uid: Option<PodUid>,
    /// `metadata.resourceVersion` — used as a delete precondition so a
    /// concurrent status update (e.g. the pod transitioning to `Running` after
    /// the verify GET but before the delete) fails the delete with a benign
    /// conflict rather than terminating a now-healthy pod.
    pub resource_version: Option<String>,
    /// Parsed into the shared [`SandboxId`] newtype at this boundary and kept
    /// typed through the reconciler/event call graph (AGENTS.md).
    pub sandbox_id: Option<SandboxId>,
    pub node_name: Option<String>,
    /// The pod carries a restore annotation (checkpoint, or a suspend-restore
    /// source-node / storage-URI) — it is a user-restore pod. Those are
    /// excluded from the GC: their sandbox sits in `restoring`, which the
    /// orphaned-sandbox convergence prerequisite does not handle, and they
    /// carry snapshot state, so a restore-aware path (not this loop) owns them.
    ///
    /// Conservative by design: the annotation persists after a restore
    /// completes, so a restored sandbox that later re-wedges in
    /// `ContainerCreating` is still excluded. This is the safe direction (the
    /// GC never deletes a pod that ever restored), at the cost of not cleaning
    /// that edge case — a re-wedged formerly-restore pod. Basing the exclusion
    /// on live restore state (clearing the marker on restore completion) is a
    /// restore-subsystem change tracked as a follow-up, out of scope here.
    pub is_restore: bool,
    /// The pod carries `space.pplx.net/pool-role=warm` — a warm-pool slot pod
    /// owned by the warm-pool reconciler (which deletes it under a Postgres
    /// slot lock). No logical `sandboxes` row exists for a warming slot yet, so
    /// neither convergence reconciler can retire it; deleting it here would
    /// leave a `warming` slot counted against pool capacity. Excluded from the
    /// GC — the warm-pool reconciler's own timeout cleanup owns these.
    ///
    /// Conservative by design: claiming a slot updates Postgres but not this
    /// label, so a claimed-then-wedged pod (now a logical sandbox) is still
    /// excluded. Safe direction (never deletes a pool pod), at the cost of not
    /// cleaning that edge case. Relabelling on claim is a warm-pool-subsystem
    /// change tracked as a follow-up, out of scope here.
    pub is_warm_pool: bool,
    /// `metadata.deletionTimestamp` is set — the pod is already terminating.
    pub terminating: bool,
    /// `status.phase == "Pending"`.
    pub phase_pending: bool,
    /// Some container is `waiting` with reason `ContainerCreating`.
    pub has_container_creating: bool,
    /// Some app or init container is already `running` — the pod is
    /// progressing, not wedged.
    pub any_container_running: bool,
}

impl PodSnapshot {
    pub(crate) fn from_pod(pod: &SandboxPodView) -> Self {
        // The shared pod builder stamps the sandbox id into both the annotation
        // and a same-keyed label; the constant is documented as an annotation,
        // so read the annotation first and fall back to the label. Reading only
        // one would classify a real stuck pod as `missing_sandbox_id` and skip
        // it if a future change ever dropped that copy.
        let sandbox_id = pod
            .annotation_or_label(SandboxPodKey::SandboxId)
            .and_then(|raw| Uuid::parse_str(raw).ok())
            .map(SandboxId::from_uuid);
        let node_name = pod
            .node_name()
            .filter(|name| !name.is_empty())
            .map(str::to_owned);
        // Restore-only annotations (`SandboxPodKey::RESTORE_ANNOTATIONS`): a
        // remote restore carries the storage URI; a node-local restore carries
        // the source node and/or checkpoint. Fresh creates carry none of them.
        let is_restore = pod.has_restore_annotation();
        let is_warm_pool = pod.label(SandboxPodKey::PoolRole) == Some(SANDBOX_POOL_ROLE_WARM);
        let phase_pending = pod.phase() == Some("Pending");
        let (has_container_creating, any_container_running) = container_signals(pod);
        Self {
            pod_name: pod.name().to_owned(),
            pod_uid: pod.uid().and_then(|uid| uid.parse().ok()),
            resource_version: pod.resource_version().map(str::to_owned),
            sandbox_id,
            node_name,
            is_restore,
            is_warm_pool,
            terminating: pod.is_deleting(),
            phase_pending,
            has_container_creating,
            any_container_running,
        }
    }
}

/// Reads the container-state signals the classifier needs out of the pod's
/// statuses, across **both** app and init containers: whether any is `waiting`
/// with reason `ContainerCreating` (the wedged signal — a pod can be stuck
/// creating an init container just as easily as an app container), and
/// whether any has reached `running` (the progress guard — a running init
/// container means the pod is progressing through initialization, not wedged).
fn container_signals(pod: &SandboxPodView) -> (bool, bool) {
    let mut has_container_creating = false;
    let mut any_running = false;
    for state in pod.container_statuses().app_and_init() {
        if state.running {
            any_running = true;
        }
        if let Some(waiting) = &state.waiting
            && waiting.reason.as_deref() == Some(CONTAINER_CREATING_REASON)
        {
            has_container_creating = true;
        }
    }
    (has_container_creating, any_running)
}

/// Whether the pod is currently wedged in `ContainerCreating`: `Pending`, some
/// container `waiting` with reason `ContainerCreating`, no running container,
/// and not already terminating. This is the shape signal; how long it has held
/// this shape is the reconciler's observation clock, not this predicate.
pub(crate) fn is_stuck_shaped(snapshot: &PodSnapshot) -> bool {
    !snapshot.terminating
        && snapshot.phase_pending
        && snapshot.has_container_creating
        && !snapshot.any_container_running
}

/// Whether the pod's node is fit for the GC to act on the pod.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeEligibility {
    /// Node is present, `Ready`, and schedulable.
    Eligible,
    /// Node is present but not `Ready` — defer to node-level remediation.
    NotReady,
    /// Node is cordoned (`spec.unschedulable`) — node-drain owns it.
    Cordoned,
    /// Node is absent from the reflector cache — cannot verify.
    Unknown,
}
