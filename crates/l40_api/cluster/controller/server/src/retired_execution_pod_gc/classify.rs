//! Pure classification: the reflector-object → [`PodSnapshot`] extraction and
//! the disposition that decides, from a sandbox pod plus the execution row
//! that names it, whether the pod is a retired execution's leftover to reap.
//! No kube client and no async, so the matrix is unit-testable.

use pplx_space_api::controllers::substrate::kubernetes::{SandboxPodKey, SandboxPodView};
use pplx_space_common_core::types::{PodUid, SandboxId};
use uuid::Uuid;

/// The pod phase as the classifier needs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodPhase {
    Pending,
    Running,
    Succeeded,
    Failed,
    /// `Unknown`, absent, or an unrecognised value. Never reaped.
    Other,
}

impl PodPhase {
    fn parse(phase: Option<&str>) -> Self {
        match phase {
            Some("Pending") => Self::Pending,
            Some("Running") => Self::Running,
            Some("Succeeded") => Self::Succeeded,
            Some("Failed") => Self::Failed,
            _ => Self::Other,
        }
    }
}

/// Flattened view of the parts of a sandbox `Pod` the GC reasons about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodSnapshot {
    pub pod_name: String,
    /// `None` when the pod carries no `metadata.uid`; such a pod is never acted
    /// on (no UID to anchor the observation clock or the delete precondition).
    pub pod_uid: Option<PodUid>,
    /// Used as a delete precondition so a pod that changed after the verify GET
    /// fails the delete with a benign conflict.
    pub resource_version: Option<String>,
    pub sandbox_id: Option<SandboxId>,
    pub generation: Option<i64>,
    pub node_name: Option<String>,
    /// `metadata.deletionTimestamp` is set; the pod is already going away.
    pub terminating: bool,
    pub is_restore: bool,
    /// `status.podIP` is set. The sandbox network comes up with the VM in
    /// `RunPodSandbox`, so a pod without an IP never got a running sandbox.
    pub has_pod_ip: bool,
    pub phase: PodPhase,
    /// Every app, init and ephemeral container has terminated, and there is at
    /// least one app container status to say so.
    pub all_containers_terminated: bool,
}

impl PodSnapshot {
    pub(crate) fn from_pod(pod: &SandboxPodView) -> Self {
        let sandbox_id = pod
            .annotation_or_label(SandboxPodKey::SandboxId)
            .and_then(|raw| Uuid::parse_str(raw).ok())
            .map(SandboxId::from_uuid);
        let generation = pod
            .annotation_or_label(SandboxPodKey::ExecutionGeneration)
            .and_then(|raw| raw.parse::<i64>().ok())
            .filter(|generation| *generation > 0);
        let node_name = pod
            .node_name()
            .filter(|name| !name.is_empty())
            .map(str::to_owned);
        let statuses = pod.container_statuses();
        let all_containers_terminated = !statuses.containers.is_empty()
            && statuses.all().all(|state| state.terminated.is_some());
        Self {
            pod_name: pod.name().to_owned(),
            pod_uid: pod.uid().and_then(|uid| uid.parse().ok()),
            resource_version: pod.resource_version().map(str::to_owned),
            sandbox_id,
            generation,
            node_name,
            terminating: pod.is_deleting(),
            is_restore: pod.has_restore_annotation(),
            has_pod_ip: pod.pod_ip().is_some_and(|ip| !ip.is_empty()),
            phase: PodPhase::parse(pod.phase()),
            all_containers_terminated,
        }
    }

    /// Cheap pre-filter for the cached scan: a `Running` pod or one already
    /// terminating can never classify as reapable, so it isn't looked up.
    pub(crate) fn worth_classifying(&self) -> bool {
        !self.terminating && self.phase != PodPhase::Running
    }
}

/// The `sandbox_k8s_executions` row that names a pod (`pod_name` is unique).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionRow {
    pub sandbox_id: SandboxId,
    pub generation: i64,
    pub active: bool,
    pub state: String,
}

/// Why a pod is reaped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReapReason {
    /// The execution is retired and the pod is `Succeeded`/`Failed` with every
    /// container terminated: no VM is left. The node-published suspend shape.
    /// Restore pods included: a suspended sandbox's latest pod is often its
    /// restore pod, which `orphaned_restore_pod_gc` doesn't treat as
    /// superseded.
    TerminalRetired,
    /// The execution was retired as `replaced` and its fresh-create pod is
    /// still `Pending` with no pod IP: its sandbox never came up, and
    /// sandbox-manager refuses to start it. The node-published
    /// activation-rollback shape.
    ReplacedNeverStarted,
}

impl ReapReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TerminalRetired => "terminal_retired",
            Self::ReplacedNeverStarted => "replaced_never_started",
        }
    }
}

/// Why a pod is left alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// Already terminating.
    Terminating,
    /// No UID, sandbox id or generation on the pod.
    MissingIdentity,
    /// No execution row names this pod (an unclaimed warm-pool pod, or a
    /// sandbox row already purged). Other loops own those.
    NoExecutionRow,
    /// The row naming this pod disagrees with the pod's own sandbox id or
    /// generation. Never act on a mismatch.
    IdentityMismatch,
    /// The execution is active: a live or in-flight runtime owns the pod.
    ActiveExecution,
    /// Retired, but the pod may still hold a VM, or is a non-terminal restore
    /// pod (`orphaned_restore_pod_gc` owns those). Left to the loops that own
    /// them.
    RetiredNotProvablyIdle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    Reap(ReapReason),
    Skip(SkipReason),
}

/// Decide whether `pod` is a retired execution's leftover safe to delete.
/// `row` is the execution row whose `pod_name` equals the pod's name, if any.
pub fn classify(pod: &PodSnapshot, row: Option<&ExecutionRow>) -> Disposition {
    if pod.terminating {
        return Disposition::Skip(SkipReason::Terminating);
    }
    let (Some(_), Some(sandbox_id), Some(generation)) =
        (pod.pod_uid.as_ref(), pod.sandbox_id, pod.generation)
    else {
        return Disposition::Skip(SkipReason::MissingIdentity);
    };
    let Some(row) = row else {
        return Disposition::Skip(SkipReason::NoExecutionRow);
    };
    if row.sandbox_id != sandbox_id || row.generation != generation {
        return Disposition::Skip(SkipReason::IdentityMismatch);
    }
    if row.active {
        return Disposition::Skip(SkipReason::ActiveExecution);
    }
    match pod.phase {
        PodPhase::Succeeded | PodPhase::Failed if pod.all_containers_terminated => {
            Disposition::Reap(ReapReason::TerminalRetired)
        }
        PodPhase::Pending if row.state == "replaced" && !pod.is_restore && !pod.has_pod_ip => {
            Disposition::Reap(ReapReason::ReplacedNeverStarted)
        }
        _ => Disposition::Skip(SkipReason::RetiredNotProvablyIdle),
    }
}
