use pplx_space_common_core::types::{NodeId, SandboxId};

use super::support::*;
use super::*;
use crate::controllers::substrate::kubernetes::store::{
    ActiveExecutionState, ActiveExecutionStatusRow, FailedRecoveryPodAbsenceOutcome,
    PlacementRetryDisposition, PodNodePlacement, PodTrackingCleanupCause, SandboxExecutionClaim,
    StartedRecoveryReplacement, StartedRecoveryRestoreContext, StartedRecoveryRestoreSource,
};
use crate::node_group_placement::{
    PlacementSubject, ResolvePath as NodeGroupResolvePath, ResolvedPlacement, RestoreSourceNode,
    check_local_restore_source,
};
use pplx_space_common_core::kubernetes::{EvacuationPlacement, NodeGroupPlacement, PodToleration};
use pplx_space_common_core::types::{
    ExecutionGeneration, NodeRole as SpaceNodeRole, PodUid, PolicyAuthority,
    SandboxActivationAttemptId, SandboxLifecycle, SandboxOwner, SandboxTransitionTargetState,
};
use pplx_space_events::{
    SandboxEvent, SandboxEventBoolField, SandboxEventDetails, SandboxEventSource, SandboxEventType,
    SandboxRestoreProvenance,
};
use rand::RngExt;

mod create_request;
mod delete;
mod ensure_pod;
mod read;
mod redrive;
mod restore;
mod started_recovery;
mod wake;
mod worker_clients;
#[cfg(test)]
use delete::{K8S_THROTTLE_DELETE_RETRY_ATTEMPTS, retry_kube_delete_throttling};
use delete::{delete_sandbox_pod_with_throttle_retry, lingering_sandbox_pod_names};
#[cfg(test)]
use ensure_pod::restore_placement_sample;
#[cfg(test)]
use read::{
    succeeded_pod_publication_grace_remaining, terminal_cleanup_phase_after_uncached_check,
};
pub(super) use redrive::RedriveOutcome;
#[cfg(test)]
use redrive::{PreconditionReread, classify_precondition_reread};
pub use restore::ExecutionHydrationOutcome;
use restore::pod_matches_execution_generation;
#[cfg(test)]
use restore::restored_pod_routability;
use started_recovery::started_recovery_replacement_event_details;
pub(super) use wake::{ActivationWakeFailure, WakeDispatchStatus};

fn is_kube_api_throttling_error(err: &KubeError) -> bool {
    matches!(err, kube::Error::Api(resp) if resp.code == 429)
}

fn is_transient_kube_read_error(err: &KubeError) -> bool {
    match err {
        kube::Error::Api(response) => {
            response.code == 408 || response.code == 429 || response.code >= 500
        }
        kube::Error::HyperError(_) | kube::Error::Service(_) => true,
        // Serialization, request construction, configuration, and auth errors
        // are not made healthy by polling the same selected Pod.
        _ => false,
    }
}

fn kube_error_for_substrate(operation: &str, err: KubeError) -> SubstrateError {
    // This helper does not retry by itself. Selection loops consume the
    // retryable timeout shape at their slow cadence and existing one-shot
    // callers return it to their retry owner, avoiding a tight retry loop
    // against an already-overloaded apiserver.
    if is_transient_kube_read_error(&err) {
        return SubstrateError::Timeout {
            message: if is_kube_api_throttling_error(&err) {
                format!("{operation}: Kubernetes API throttling: {err}")
            } else {
                format!("{operation}: transient Kubernetes API failure: {err}")
            },
            phase: None,
        };
    }
    SubstrateError::Failed {
        message: format!("{operation}: {err}"),
    }
}

/// Context describing how a sandbox pod should be built. Parsed by the
/// substrate entry point (start vs user-restore vs cluster-controller
/// evacuation) and consumed by [`KubernetesSubstrate::sandbox_pod_request`].
pub(super) enum PodCreateContext<'a> {
    /// First start of a sandbox; no source-node affinity, no extra tolerations.
    OnDemand,
    /// Restart/restore from existing sandbox state.
    Restore(RestoreContext<'a>),
}

pub(super) enum RestoreContext<'a> {
    /// User-initiated restore. Carries restore intent to the pod even when a
    /// durable snapshot lets scheduler choose any eligible node.
    UserRestore {
        suspend_restore_storage_uri: Option<&'a str>,
        suspend_restore_source_node: Option<&'a str>,
        direct_node_name: Option<&'a str>,
        preferred_node_name: Option<&'a str>,
    },
    /// Cluster-controller-initiated evacuation. Drops the source-node pin so
    /// kube-scheduler can place the pod on the new cohort. `extra_tolerations`
    /// and `extra_required_affinity` are derived together upstream from
    /// the cluster-controller's target-cohort placement: both empty/None
    /// for a steady-state evacuation (no rollout), both populated for a
    /// two-MNG blue/green rollout.
    Evacuation {
        suspend_restore_storage_uri: Option<&'a str>,
        extra_tolerations: &'a [PodToleration],
        extra_required_affinity: Option<(&'a str, &'a str)>,
    },
}

/// Per-build inputs that are resolved or serialized by the caller and handed
/// to [`KubernetesSubstrate::sandbox_pod_request`] together. Grouped because
/// each one has to be materialized before the (synchronous) build — two JSON
/// encodings and a database read — and because passing them individually puts
/// the builder over clippy's argument ceiling.
pub(super) struct SandboxPodRequestInputs<'a> {
    pub(super) sandbox_owner: &'a SandboxOwner,
    pub(super) auto_lifecycle_json: &'a str,
    pub(super) network_policy_json: &'a str,
    /// `None` renders no node-group constraints: the gate is off, or the
    /// cluster has no generic node group.
    pub(super) node_group_placement: Option<&'a NodeGroupPlacement>,
}

impl PodCreateContext<'_> {
    /// Which pod-build path this is, for the placement resolver's metrics.
    /// `OnDemand` here is the reconcile-side *materialization* of an on-demand
    /// sandbox, not a user create — the user create resolves in the adapter
    /// with a request in hand, and the two failure surfaces are different
    /// enough (a 409 to a caller vs. a retried reconcile) to keep apart.
    fn node_group_resolve_path(&self) -> NodeGroupResolvePath {
        match self {
            Self::OnDemand => NodeGroupResolvePath::Materialize,
            Self::Restore(RestoreContext::UserRestore { .. }) => NodeGroupResolvePath::Restore,
            Self::Restore(RestoreContext::Evacuation { .. }) => NodeGroupResolvePath::Evacuation,
        }
    }

    /// The node this build will pre-bind via `spec.nodeName`, bypassing
    /// kube-scheduler. Only the local-only restore shape sets one.
    fn direct_node_name(&self) -> Option<&str> {
        match self {
            Self::OnDemand | Self::Restore(RestoreContext::Evacuation { .. }) => None,
            Self::Restore(RestoreContext::UserRestore {
                direct_node_name, ..
            }) => *direct_node_name,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StartedRecoveryOwnership {
    CreatedExecution { generation: i64 },
    ExistingExecution { generation: i64 },
}

impl StartedRecoveryOwnership {
    pub(super) const fn generation(self) -> i64 {
        match self {
            Self::CreatedExecution { generation } | Self::ExistingExecution { generation } => {
                generation
            }
        }
    }

    const fn created_execution(self) -> bool {
        matches!(self, Self::CreatedExecution { .. })
    }
}

/// Carries a proposed activation attempt into materialization and publishes
/// the database-assigned canonical ID back to the caller.
///
/// This is intentionally a shared handle rather than a value snapshot:
/// clones share the same `assigned_id` cell. `record_assigned` may run on a
/// clone inside the materialization path, and the originating caller must
/// observe that exact ID for subsequent error, timeout, and redrive fencing.
#[derive(Clone, Debug)]
pub(super) struct ActivationAttemptAssignment {
    requested_id: SandboxActivationAttemptId,
    target_state: SandboxTransitionTargetState,
    assigned: std::sync::Arc<std::sync::Mutex<Option<(SandboxActivationAttemptId, i64)>>>,
}

impl ActivationAttemptAssignment {
    pub(super) fn new(
        requested_id: SandboxActivationAttemptId,
        target_state: SandboxTransitionTargetState,
    ) -> Self {
        Self {
            requested_id,
            target_state,
            assigned: std::sync::Arc::new(std::sync::Mutex::new(None)),
        }
    }

    fn request(&self) -> (SandboxActivationAttemptId, SandboxTransitionTargetState) {
        (self.assigned_or_requested(), self.target_state)
    }

    fn record_assigned(&self, assigned_id: SandboxActivationAttemptId, generation: i64) {
        *self
            .assigned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((assigned_id, generation));
    }

    pub(super) fn assigned_id(&self) -> Option<SandboxActivationAttemptId> {
        self.assigned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|(id, _)| *id)
    }

    pub(super) fn assigned(&self) -> Option<(SandboxActivationAttemptId, i64)> {
        *self
            .assigned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn assigned_or_requested(&self) -> SandboxActivationAttemptId {
        self.assigned_id().unwrap_or(self.requested_id)
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct PodMaterializationOwnership {
    /// Started-sandbox recovery claim, when that reconciler rather than a
    /// lifecycle request owns materialization.
    started_recovery: Option<StartedRecoveryOwnership>,
    /// User restore/wake lease. A lapsed leader must not retire a successor's
    /// execution while cleaning up a dropped cross-node re-drive.
    restore_lease_token: Option<Uuid>,
    /// Per-attempt fence because an activation retry may reuse an execution
    /// generation.
    activation_attempt: Option<ActivationAttemptAssignment>,
}

impl PodMaterializationOwnership {
    pub(super) fn new(
        started_recovery: Option<StartedRecoveryOwnership>,
        restore_lease_token: Option<Uuid>,
        activation_attempt: Option<ActivationAttemptAssignment>,
    ) -> Self {
        Self {
            started_recovery,
            restore_lease_token,
            activation_attempt,
        }
    }
}

#[cfg(test)]
mod tests;

/// Decide how a pod should be built, and — for a restore — how it should be
/// placed.
///
/// The placement shape is a function of the `(source_node_name,
/// suspend_restore_storage_uri)` pair. All three combinations are meaningful:
///
/// * node only -> `direct_node_name`, i.e. `spec.nodeName`. The pod is
///   pre-bound, bypassing kube-scheduler; kubelet admission is terminal and
///   there is no cross-node re-drive.
/// * URI only -> no source pin; kube-scheduler places the pod anywhere
///   eligible and the node pulls the artifact from S3.
/// * both -> the source node stops being a hard binding and the URI becomes
///   the fallback whichever node the pod lands on restores from. What the
///   source node renders as then depends on `restore_source_node_hard_pin`:
///   off (the default) it is the weight-100 preferred term, so the scheduler
///   may place elsewhere; on, it is a required `matchFields` shortlist — and
///   with a shortlist of one (a cold node cache) that degrades to a *bare
///   required pin with no preference*, which can still leave the pod
///   Unschedulable if the source node is gone. The in-request watch re-drives
///   that case cross-node.
///
/// So a caller that can resolve both should pass both: the pair is what
/// selects the third shape, and suppressing either one collapses it to a
/// strictly worse placement. `SandboxPodRequest::Restore`'s field docs in
/// `pplx_space_common_core::k8s` carry the rendering details.
pub(super) fn parse_pod_create_context<'a>(
    sandbox_id: SandboxId,
    generation: i64,
    lifecycle: &str,
    prior_state_exists: bool,
    evacuation: Option<EvacuationContext<'a>>,
    source_node_name: Option<&'a str>,
    suspend_restore_storage_uri: Option<&'a str>,
) -> Result<PodCreateContext<'a>, SubstrateError> {
    let lifecycle_state = normalize_lifecycle(lifecycle);

    if let Some(EvacuationContext {
        extra_tolerations,
        extra_required_affinity,
    }) = evacuation
    {
        return Ok(PodCreateContext::Restore(RestoreContext::Evacuation {
            suspend_restore_storage_uri,
            extra_tolerations,
            extra_required_affinity,
        }));
    }

    if source_node_name.is_some()
        || suspend_restore_storage_uri.is_some()
        || matches!(lifecycle_state.as_str(), "suspended" | "restoring")
    {
        if source_node_name.is_none() && suspend_restore_storage_uri.is_none() {
            return Err(SubstrateError::Timeout {
                message:
                    "cannot restore sandbox before source node or remote snapshot is available"
                        .to_string(),
                phase: None,
            });
        }
        return Ok(PodCreateContext::Restore(RestoreContext::UserRestore {
            suspend_restore_storage_uri,
            suspend_restore_source_node: source_node_name,
            direct_node_name: suspend_restore_storage_uri
                .is_none()
                .then_some(source_node_name)
                .flatten(),
            preferred_node_name: suspend_restore_storage_uri
                .is_some()
                .then_some(source_node_name)
                .flatten(),
        }));
    }

    if generation <= 1 || !prior_state_exists {
        return Ok(PodCreateContext::OnDemand);
    }

    Err(SubstrateError::RestoreContextUnavailable {
        message: format!(
            "refusing fresh Kubernetes pod for sandbox {sandbox_id} generation {generation} \
             without restore context while lifecycle is {lifecycle}; recovery requires \
             intervention or new verified restore state"
        ),
    })
}

/// Borrowed view of an in-flight evacuation, materialized once by
/// [`KubernetesSubstrate::ensure_pod_exists`] so the toleration and the
/// required affinity reference upstream-owned storage (no per-call clones
/// into `SandboxPodRequest::Restore`'s borrowed slots).
#[derive(Clone, Copy)]
pub(super) struct EvacuationContext<'a> {
    pub extra_tolerations: &'a [PodToleration],
    pub extra_required_affinity: Option<(&'a str, &'a str)>,
}

/// Identity of a pod that proves the active execution routable: the values to
/// hydrate onto the execution row.
struct RoutablePodIdentity {
    pod_uid: Option<String>,
    pod_ip: String,
}

struct StartedRecoveryPodReplacement<'a> {
    reason: String,
    restore_context: Option<StartedRecoveryRestoreContext>,
    explicit_source_node: Option<&'a str>,
    explicit_storage_uri: Option<&'a str>,
    activation_assignment: Option<&'a ActivationAttemptAssignment>,
}
