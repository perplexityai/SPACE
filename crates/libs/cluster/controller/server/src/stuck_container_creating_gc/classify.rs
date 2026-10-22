//! Pure classification vocabulary: the reflector-object → [`PodSnapshot`]
//! extraction, the "is this pod wedged in `ContainerCreating`" shape check,
//! node eligibility, and the marker-ownership decisions the [`super::store`]
//! lookups feed. No kube client and no async — everything here is a pure
//! function of its inputs, which keeps it trivially unit-testable.
//!
//! Note there is no age here: how long a pod has been wedged is **not** a
//! reliable pod-object property (`status.startTime` is the pod's lifetime, not
//! time-in-`ContainerCreating` — an old pod that just flipped from
//! `ImagePullBackOff`, or was recreated after a kubelet restart, keeps its
//! original `startTime`). The reconciler measures continuous-`ContainerCreating`
//! age with an in-memory observation clock instead.

use std::collections::HashMap;

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
    /// source-node / storage-URI). A build-time marker, never cleared — see
    /// [`MarkerOwnership::restore_excludes`] for what actually decides the
    /// exclusion.
    pub is_restore: bool,
    /// The pod carries `space.pplx.net/pool-role=warm`. Also build-time only —
    /// see [`MarkerOwnership::warm_pool_excludes`].
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
    /// Fold `other`'s markers into this snapshot. A stale view can only ever
    /// *miss* a marker, never invent one, so the union of the cached scan object
    /// and the authoritative re-read is the conservative view to resolve
    /// ownership against.
    pub(crate) fn absorb_markers(&mut self, other: &Self) {
        self.is_restore |= other.is_restore;
        self.is_warm_pool |= other.is_warm_pool;
    }

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

/// Result of asking the database whether a pod's build-time marker still has a
/// live owner behind it.
///
/// Distinguishing [`Self::Unknown`] from [`Self::Absent`] is what keeps the
/// fallback safe: an answer we couldn't get must not read as "no row, therefore
/// the marker is stale, therefore delete".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lookup<T> {
    /// Not asked, not answerable, or a value this build doesn't recognize.
    Unknown,
    /// The lookup succeeded and no row exists.
    Absent,
    Known(T),
}

/// What the live rows say about who owns a marked pod.
///
/// For a restore-annotated pod the rows are the sandbox's: it's `InFlight` while
/// the row is `restoring` or a started recovery holds a fresh lease on its
/// execution, since a started recovery deliberately leaves the row in `started`
/// (`claim_started_recovery`). For a `pool-role=warm` pod it's the slot row, as
/// [`pplx_space_cluster_pool::SlotOwnership::pool_owns_pod`] decides it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Owner {
    /// The restore path or the warm-pool reconciler still owns the pod.
    InFlight,
    /// Ownership has moved on — the marker is a leftover.
    Released,
}

impl Lookup<Owner> {
    /// Whether this answer keeps the marker's exclusion. Only a successful
    /// lookup releases the pod: no row, or a row saying ownership moved on.
    fn excludes(self) -> bool {
        match self {
            Self::Unknown => true,
            Self::Absent => false,
            Self::Known(owner) => owner == Owner::InFlight,
        }
    }
}

/// One tick's ownership answers, keyed the way the reconciler asks for them. A
/// key is present only if it was both requested and resolved, so a map miss is
/// [`Lookup::Unknown`] — which covers a failed query, a pod whose marker only
/// surfaced on the authoritative re-read, and a pod with no sandbox id, without
/// any of them needing a special case.
#[derive(Debug, Default)]
pub(crate) struct MarkerOwnership {
    pub(crate) restores: HashMap<SandboxId, Lookup<Owner>>,
    pub(crate) slots: HashMap<String, Lookup<Owner>>,
}

impl MarkerOwnership {
    /// Whether a restore or started recovery still owns this pod.
    ///
    /// Only an in-flight one does — a `restoring` row, or a started recovery
    /// holding a fresh lease. Those hold snapshot state mid-transition and sit
    /// outside what the orphaned-sandbox convergence prerequisite covers. Once
    /// the restore has finished, or the sandbox was retired, the marker is a
    /// leftover and the pod is an ordinary candidate. A row that vanished
    /// releases the pod too: no row means nothing to strand, which is already
    /// how the GC treats an unmarked pod.
    ///
    /// This asks about the *sandbox*, so it covers pooled pods as well. A
    /// started recovery adopts the wedged pod under its existing generation
    /// rather than creating a replacement, and a pooled pod carries no restore
    /// annotation, so its slot row releasing it does not mean nothing owns it.
    /// The marker gate below is only a cost bound — unmarked pods are the
    /// overwhelming majority and are never looked up.
    pub(crate) fn restore_excludes(&self, pod: &PodSnapshot) -> bool {
        if !pod.is_restore && !pod.is_warm_pool {
            return false;
        }
        self.restore_owner(pod.sandbox_id).excludes()
    }

    /// Whether the pod's `pool-role=warm` label still means "the warm-pool
    /// reconciler owns this pod".
    ///
    /// It does while the pool still holds the pod — see
    /// [`pplx_space_cluster_pool::SlotOwnership::pool_owns_pod`] for which slot
    /// states that covers and why the state alone can't decide it.
    /// No slot row at all means the pool has finished with the pod entirely.
    pub(crate) fn warm_pool_excludes(&self, pod: &PodSnapshot) -> bool {
        if !pod.is_warm_pool {
            return false;
        }
        self.warm_pool_owner(&pod.pod_name).excludes()
    }

    fn restore_owner(&self, sandbox_id: Option<SandboxId>) -> Lookup<Owner> {
        sandbox_id
            .and_then(|id| self.restores.get(&id).copied())
            .unwrap_or(Lookup::Unknown)
    }

    fn warm_pool_owner(&self, pod_name: &str) -> Lookup<Owner> {
        self.slots.get(pod_name).copied().unwrap_or(Lookup::Unknown)
    }
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
