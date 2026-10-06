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
use pplx_space_common_core::kubernetes::{
    EvacuationPlacement, NodeGroupPlacement, PodToleration,
    SANDBOX_EXECUTION_GENERATION_ANNOTATION, SANDBOX_ID_ANNOTATION,
};
use pplx_space_common_core::types::{
    ExecutionGeneration, NodeRole as SpaceNodeRole, PodUid, PolicyAuthority,
    SandboxActivationAttemptId, SandboxLifecycle, SandboxOwner, SandboxTransitionTargetState,
};
use pplx_space_events::{
    SandboxEvent, SandboxEventBoolField, SandboxEventDetails, SandboxEventSource, SandboxEventType,
    SandboxRestoreProvenance,
};
use rand::RngExt;

const K8S_THROTTLE_DELETE_RETRY_ATTEMPTS: usize = 3;
const K8S_THROTTLE_DELETE_RETRY_BASE_DELAY: Duration = Duration::from_millis(100);
const K8S_THROTTLE_DELETE_RETRY_MAX_JITTER: Duration = Duration::from_millis(75);
const SUCCEEDED_POD_SUSPEND_PUBLICATION_GRACE: Duration = Duration::from_secs(180);

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

async fn retry_kube_delete_throttling<T, Fut, F>(
    operation: &'static str,
    mut call: F,
) -> Result<T, KubeError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, KubeError>>,
{
    let mut attempt = 0;
    loop {
        attempt += 1;
        match call().await {
            Ok(value) => return Ok(value),
            Err(err)
                if is_kube_api_throttling_error(&err)
                    && attempt < K8S_THROTTLE_DELETE_RETRY_ATTEMPTS =>
            {
                let jitter_ms = rand::rng().random_range(
                    0..=K8S_THROTTLE_DELETE_RETRY_MAX_JITTER
                        .as_millis()
                        .try_into()
                        .unwrap_or(u64::MAX),
                );
                let delay = K8S_THROTTLE_DELETE_RETRY_BASE_DELAY
                    * u32::try_from(attempt).unwrap_or(u32::MAX)
                    + Duration::from_millis(jitter_ms);
                tracing::warn!(
                    operation,
                    attempt,
                    max_attempts = K8S_THROTTLE_DELETE_RETRY_ATTEMPTS,
                    delay_ms = delay.as_millis(),
                    error = %err,
                    "retrying throttled Kubernetes pod delete"
                );
                sleep(delay).await;
            }
            Err(err) => return Err(err),
        }
    }
}

async fn delete_sandbox_pod_with_throttle_retry(
    k8s: &KubernetesState,
    pod_name: &str,
    operation: &'static str,
) -> Result<(), KubeError> {
    retry_kube_delete_throttling(operation, || delete_sandbox_pod(k8s, pod_name)).await
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SelectedWakeOutcome {
    pub(super) dispatch: WakeDispatchStatus,
    pub(super) landed: bool,
    pub(super) pod_ip: Option<PodIp>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WakeDispatchStatus {
    NotDispatched,
    MayHaveLanded,
}

impl WakeDispatchStatus {
    pub(super) const fn was_dispatched(self) -> bool {
        matches!(self, Self::MayHaveLanded)
    }

    /// An operation owner may be released after failure only when no node-side
    /// work or delayed completion receipt can exist.
    pub(super) const fn operation_release_is_safe(self) -> bool {
        matches!(self, Self::NotDispatched)
    }
}

#[derive(Debug)]
pub(super) struct ActivationWakeFailure {
    pub(super) error: SubstrateError,
    pub(super) dispatch: WakeDispatchStatus,
}

#[derive(Clone, Copy, Debug)]
struct SelectedWakeSettings {
    policy: WakeWaitPolicy,
    require_split_receipt_capability: bool,
    phase_deadline: Instant,
}

impl ActivationWakeFailure {
    pub(super) const fn was_dispatched(&self) -> bool {
        self.dispatch.was_dispatched()
    }

    pub(super) const fn operation_release_is_safe(&self) -> bool {
        self.dispatch.operation_release_is_safe()
    }

    pub(super) fn into_error(self) -> SubstrateError {
        self.error
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

/// Result of a cross-node re-drive attempt on an `Unschedulable` source-pinned
/// restore pod.
pub(super) enum RedriveOutcome {
    /// The pinned pod was confirmed unschedulable, torn down, and a source-less
    /// successor was created and persisted under the same generation.
    Redriven(Box<Pod>),
    /// Nothing was torn down: the authoritative read found the pod bound after
    /// all (the caller's `Unschedulable` verdict was stale) or already replaced
    /// by a concurrent re-drive's successor (required pin absent). The live pod
    /// drives the restore. `cross_node` distinguishes a retained cross-node
    /// successor / fallback bind from the original pinned pod binding on the
    /// source — the caller's placement metric must reflect what's running.
    PodRetained { cross_node: bool },
    /// Materialization ownership was revoked mid-flight; the successor was
    /// discarded and the winning attempt drives the restore.
    OwnershipLost,
}

/// Classification of a pod re-read after its UID+resourceVersion preconditioned
/// re-drive delete was rejected with 409. A conflict only proves the RV moved;
/// this decides what actually changed so the re-drive can never retain a pod
/// that's gone (→ no successor) or bounce a still-impossible pod to the caller.
#[derive(Debug, PartialEq, Eq)]
enum PreconditionReread {
    /// No pod at the name. A conflict does not imply a successor was created —
    /// a concurrent cleanup may have removed the pod. The re-drive must fall
    /// through to its successor POST (whose 409 handling resolves a concurrent
    /// creator), NOT retain a deleted pod.
    Absent,
    /// A live pod survives and drives the restore: bound (`cross_node` set by
    /// landing node vs the caller's source) or unpinned (a concurrent re-drive
    /// already installed a cross-node successor). Retain it.
    Retain { cross_node: bool },
    /// The same required-pinned pod is still unschedulable/undecided; only a
    /// status write bumped the RV. No concurrent winner is driving a successor,
    /// so retry the preconditioned delete with this fresh identity.
    RetryDelete {
        uid: String,
        resource_version: String,
    },
    /// Still required-pinned but the apiserver exposed no UID/RV to precondition
    /// on (not expected from a live GET). Defer rather than POST past a pod that
    /// may still exist.
    Indeterminate,
}

fn classify_precondition_reread(
    now_live: Option<&Pod>,
    caller_source_node: Option<&str>,
) -> PreconditionReread {
    let Some(pod) = now_live else {
        return PreconditionReread::Absent;
    };
    match scheduling_outcome_of(pod) {
        // Bound: the pin landed after all. A shortlist pod keeps its required
        // pin whether it bound on the source or a fallback, so classify by the
        // landing node, not pin-presence.
        Some(SchedulingOutcome::Scheduled { node_name }) => PreconditionReread::Retain {
            cross_node: Some(node_name.as_str()) != caller_source_node,
        },
        // Not bound, pin gone: a concurrent re-drive's cross-node successor runs.
        _ if !pod_has_required_source_pin(pod) => PreconditionReread::Retain { cross_node: true },
        // Not bound, still pinned: status-only RV bump. Retry with fresh identity.
        _ => match pod
            .metadata
            .uid
            .clone()
            .zip(pod.metadata.resource_version.clone())
        {
            Some((uid, resource_version)) => PreconditionReread::RetryDelete {
                uid,
                resource_version,
            },
            None => PreconditionReread::Indeterminate,
        },
    }
}

/// Cancellation guard for the re-drive's post-delete window. The pinned pod
/// is already gone; if the request future is dropped (client disconnect,
/// shutdown) before the successor is persisted, a still-`creating` claim
/// would have nothing behind it and every retry would take the
/// create-in-flight early return until the 300s stale-create age expires.
/// Unless disarmed on normal completion, `Drop` spawns a best-effort recovery:
///
/// 1. Retire the claim. The retirement CASes on `creating + pod_uid IS NULL +
///    pod_ip IS NULL`, so a row that was materialized before the drop (the
///    persist won the race with the cancellation) is never touched.
/// 2. **Only if the retirement actually fired**, delete the same-name pod: the
///    cancellation may have hit after the successor POST succeeded, and
///    retiring the claim without removing that pod would leave a schedulable
///    pod with no active execution behind it. A no-op retirement means the
///    row owns whatever pod exists — leave it alone.
///
/// Both steps are additionally fenced on the activation-attempt identity and,
/// when present, the restore lease. A dropped leader can outlive its lease and
/// execution generations are reused by retries; either fence changing makes
/// the delayed cleanup a no-op before it can retire the successor's claim.
struct RedriveClaimGuard {
    handles: Option<(K8sStore, KubernetesState)>,
    span: tracing::Span,
    sandbox_id: SandboxId,
    pod_name: String,
    generation: i64,
    restore_lease_token: Option<Uuid>,
    activation_attempt_id: Option<pplx_space_common_core::types::SandboxActivationAttemptId>,
}

impl RedriveClaimGuard {
    /// Always armed: the re-drive constructs this only after resetting the
    /// execution row to `creating`, so the retirement CAS in `Drop` matches
    /// regardless of the (possibly stale `Created`) in-memory `claim.row.state`
    /// on the retry-recovery path. Arming off that stale state would leave a
    /// cancellation after the successor POST unable to clean the orphan up.
    fn armed(
        store: &K8sStore,
        k8s: &KubernetesState,
        sandbox_id: SandboxId,
        claim: &SandboxExecutionClaim,
        restore_lease_token: Option<Uuid>,
        activation_attempt_id: Option<pplx_space_common_core::types::SandboxActivationAttemptId>,
    ) -> Self {
        Self {
            handles: Some((store.clone(), k8s.clone())),
            span: tracing::Span::current(),
            sandbox_id,
            pod_name: claim.row.pod_name.clone(),
            generation: claim.row.generation,
            restore_lease_token,
            activation_attempt_id,
        }
    }

    fn disarm(&mut self) {
        self.handles = None;
    }
}

impl Drop for RedriveClaimGuard {
    fn drop(&mut self) {
        let Some((store, k8s)) = self.handles.take() else {
            return;
        };
        let sandbox_id = self.sandbox_id;
        let generation = self.generation;
        let restore_lease_token = self.restore_lease_token;
        let activation_attempt_id = self.activation_attempt_id;
        let pod_name = std::mem::take(&mut self.pod_name);
        tracing::warn!(
            sandbox_id = %sandbox_id,
            generation,
            "re-drive dropped mid-flight; retiring creating claim in background"
        );
        let span = tracing::info_span!(parent: &self.span, "substrate.wake.redrive_cleanup", sandbox_id = %sandbox_id, generation);
        tokio::spawn(
            async move {
                let retired = match store
                    .abandon_unmaterialized_create_claim_by_generation(
                        sandbox_id,
                        generation,
                        restore_lease_token,
                        activation_attempt_id,
                    )
                    .await
                {
                    Ok(retired) => retired,
                    Err(err) => {
                        tracing::warn!(
                            sandbox_id = %sandbox_id,
                            generation,
                            error = %err,
                            "failed to retire creating claim after cancelled re-drive"
                        );
                        return;
                    }
                };
                if !retired {
                    // The persist won: the row owns the pod. Nothing to clean.
                    return;
                }
                // The claim was still unmaterialized — any same-name pod is this
                // generation's successor POSTed just before the drop; without an
                // active execution it must not run.
                if let Err(err) = delete_sandbox_pod_with_throttle_retry(
                    &k8s,
                    &pod_name,
                    "redrive_cancel_cleanup",
                )
                .await
                    && !is_not_found_error(&err)
                {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        pod_name = %pod_name,
                        generation,
                        error = %err,
                        "failed to delete orphaned successor after cancelled re-drive"
                    );
                }
            }
            .instrument(span),
        );
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

/// Derive the per-restore placement decision a [`PodCreateContext`] renders,
/// for the `sandbox.restore.placement` metric. `None` for non-restore creates
/// (on-demand / pooled), which carry no placement decision. `hard_pin` is the
/// `restore_source_node_hard_pin` config flag, which distinguishes a required
/// `matchFields` pin from the legacy preferred term for the same inputs.
fn restore_placement_sample(
    context: &PodCreateContext<'_>,
    hard_pin: bool,
) -> Option<pplx_space_metrics::sandbox::restore::PlacementSample> {
    use pplx_space_metrics::sandbox::restore::{PlacementSample, RestorePlacement, SourcePin};
    match context {
        PodCreateContext::OnDemand => None,
        // Evacuation deliberately drops the source pin: always cross-node.
        PodCreateContext::Restore(RestoreContext::Evacuation { .. }) => Some(PlacementSample {
            placement: RestorePlacement::CrossNode,
            local_snapshot_present: false,
            pin: SourcePin::None,
        }),
        PodCreateContext::Restore(RestoreContext::UserRestore {
            direct_node_name,
            preferred_node_name,
            suspend_restore_source_node,
            ..
        }) => {
            let (placement, pin) = if direct_node_name.is_some() {
                (RestorePlacement::SameNode, SourcePin::NodeName)
            } else if preferred_node_name.is_some() {
                let pin = if hard_pin {
                    SourcePin::RequiredAffinity
                } else {
                    SourcePin::PreferredAffinity
                };
                (RestorePlacement::SameNode, pin)
            } else {
                (RestorePlacement::CrossNode, SourcePin::None)
            };
            Some(PlacementSample {
                placement,
                local_snapshot_present: suspend_restore_source_node.is_some(),
                pin,
            })
        }
    }
}

fn terminal_cleanup_phase_after_uncached_check(
    cached_phase: TerminalPodPhase,
    authoritative_pod: Option<&Pod>,
) -> Option<TerminalPodPhase> {
    match authoritative_pod {
        Some(pod) => terminal_pod_phase(pod.status.as_ref()),
        None => Some(cached_phase),
    }
}

fn succeeded_pod_publication_grace_remaining(pod: &Pod, now: jiff::Timestamp) -> Option<Duration> {
    if terminal_pod_phase(pod.status.as_ref()) != Some(TerminalPodPhase::Succeeded) {
        return None;
    }
    let status = pod.status.as_ref()?;
    let finished_at = [
        status.container_statuses.as_deref().unwrap_or_default(),
        status
            .ephemeral_container_statuses
            .as_deref()
            .unwrap_or_default(),
        status
            .init_container_statuses
            .as_deref()
            .unwrap_or_default(),
    ]
    .into_iter()
    .flatten()
    .filter_map(|status| {
        status
            .state
            .as_ref()?
            .terminated
            .as_ref()?
            .finished_at
            .as_ref()
    })
    .filter_map(|finished_at| finished_at.0.to_string().parse::<jiff::Timestamp>().ok())
    .max()?;
    let grace = jiff::SignedDuration::from_secs(
        i64::try_from(SUCCEEDED_POD_SUSPEND_PUBLICATION_GRACE.as_secs()).ok()?,
    );
    let deadline = finished_at.checked_add(grace).ok()?;
    let remaining = deadline.duration_since(now);
    Some(
        Duration::try_from(remaining)
            .ok()?
            .min(SUCCEEDED_POD_SUSPEND_PUBLICATION_GRACE),
    )
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

pub(super) struct SuspendRestorePlan {
    pub source_node_name: Option<String>,
    pub storage_uri: Option<String>,
}

/// Outcome of [`KubernetesSubstrate::hydrate_restored_execution`]: the
/// kube-verified repair of an active execution row whose pod metadata a dead
/// wake handler never persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionHydrationOutcome {
    /// A live Running pod matching the active execution's generation was
    /// found and its `pod_uid`/`pod_ip` were persisted onto the execution
    /// row. The execution is now routable.
    Hydrated,
    /// The execution row was already routable (`running` + `pod_ip`) — a
    /// concurrent hydration (e.g. the real wake path) won the race.
    AlreadyRoutable,
    /// No routable backing pod: the row has no active execution, the pod is
    /// gone from the apiserver, or the pod that exists does not match the
    /// active execution (wrong sandbox/generation, not Running, deleting, or
    /// no IP yet). Nothing was written.
    NotRoutable,
}

impl ExecutionHydrationOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hydrated => "hydrated",
            Self::AlreadyRoutable => "already_routable",
            Self::NotRoutable => "not_routable",
        }
    }
}

impl fmt::Display for ExecutionHydrationOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Identity of a pod that proves the active execution routable: the values to
/// hydrate onto the execution row.
struct RoutablePodIdentity {
    pod_uid: Option<String>,
    pod_ip: String,
}

/// Names of this sandbox's live pods from generations **strictly older** than
/// `claim_generation` — the pods a restore create must sweep before POSTing
/// its own. Matched by the sandbox-id + execution-generation annotations
/// stamped at create time. The strict generation bound is the safety
/// property: if this request's claim was superseded between its ownership
/// check and this sweep, the usurper's pod carries a generation >= ours and
/// is never selected — a name-only exclusion would delete it. Pods without a
/// parseable generation annotation are conservatively skipped.
fn lingering_sandbox_pod_names<'a>(
    pods: impl IntoIterator<Item = &'a Pod>,
    sandbox_id: SandboxId,
    claim_generation: i64,
) -> Vec<String> {
    let sandbox_id = sandbox_id.to_string();
    pods.into_iter()
        .filter(|pod| {
            let Some(annotations) = pod.metadata.annotations.as_ref() else {
                return false;
            };
            annotations.get(SANDBOX_ID_ANNOTATION) == Some(&sandbox_id)
                && annotations
                    .get(SANDBOX_EXECUTION_GENERATION_ANNOTATION)
                    .and_then(|value| value.parse::<i64>().ok())
                    .is_some_and(|generation| generation < claim_generation)
        })
        .filter_map(|pod| pod.metadata.name.clone())
        .collect()
}

/// Whether `pod`'s identity annotations match this sandbox execution — the
/// sandbox-id and execution-generation stamps written at create time. Phase-
/// agnostic (unlike [`restored_pod_routability`]): used to recognize a
/// still-Pending successor a prior interrupted attempt created.
fn pod_matches_execution_generation(pod: &Pod, sandbox_id: SandboxId, generation: i64) -> bool {
    let Some(annotations) = pod.metadata.annotations.as_ref() else {
        return false;
    };
    annotations.get(SANDBOX_ID_ANNOTATION) == Some(&sandbox_id.to_string())
        && annotations
            .get(SANDBOX_EXECUTION_GENERATION_ANNOTATION)
            .and_then(|value| value.parse::<i64>().ok())
            == Some(generation)
}

/// Pure routability check for a pod fetched to back an active execution.
/// Returns the identity to hydrate only when **all** of these hold:
///
/// * the pod's sandbox-id and execution-generation annotations match this
///   execution ([`pod_matches_execution_generation`] — never trust a name
///   collision or a stale prior-generation pod),
/// * the pod is phase `Running` and not being deleted,
/// * the pod has an IP.
fn restored_pod_routability(
    pod: &Pod,
    sandbox_id: SandboxId,
    generation: i64,
) -> Option<RoutablePodIdentity> {
    if !pod_matches_execution_generation(pod, sandbox_id, generation) {
        return None;
    }
    if pod.metadata.deletion_timestamp.is_some() {
        return None;
    }
    let status = pod.status.as_ref()?;
    if status.phase.as_deref() != Some("Running") {
        return None;
    }
    let pod_ip = status.pod_ip.clone()?;
    Some(RoutablePodIdentity {
        pod_uid: pod.metadata.uid.clone(),
        pod_ip,
    })
}

fn started_recovery_replacement_event_details(
    details: SandboxEventDetails,
    restore_source_node: Option<&str>,
    restore_storage_uri: Option<&str>,
    handoff_source: Option<&StartedRecoveryRestoreSource>,
) -> SandboxEventDetails {
    if let Some(source_node) = restore_source_node {
        return details
            .source_node_name(source_node)
            .restore_provenance(SandboxRestoreProvenance::LocalOnly);
    }
    if restore_storage_uri.is_some() {
        return details
            .bool_field(SandboxEventBoolField::RestoreStorageUriPresent, true)
            .restore_provenance(SandboxRestoreProvenance::RemoteMatchesLocal);
    }
    match handoff_source {
        Some(StartedRecoveryRestoreSource::Local { source_node_name }) => details
            .source_node_name(source_node_name)
            .restore_provenance(SandboxRestoreProvenance::LocalOnly),
        Some(StartedRecoveryRestoreSource::Remote { .. }) => details
            .bool_field(SandboxEventBoolField::RestoreStorageUriPresent, true)
            .restore_provenance(SandboxRestoreProvenance::RemoteMatchesLocal),
        None => details,
    }
}

struct StartedRecoveryPodReplacement<'a> {
    reason: String,
    restore_context: Option<StartedRecoveryRestoreContext>,
    explicit_source_node: Option<&'a str>,
    explicit_storage_uri: Option<&'a str>,
    activation_assignment: Option<&'a ActivationAttemptAssignment>,
}

impl KubernetesSubstrate {
    pub(super) fn started_recovery_source_node_available(&self, source_node_name: &str) -> bool {
        if !self.k8s.caches.nodes_ready() || !self.pod_reflector_ready.is_ready() {
            return false;
        }
        let config = &self.k8s.sandbox_pod_config;
        let selector = config.base_node_selector();
        let pods = self.pod_store.state();
        self.k8s.caches.nodes.state().iter().any(|node| {
            node.metadata.name.as_deref() == Some(source_node_name)
                && adapter::node_schedulable(node)
                && selector.matches(node)
                && (!config.ssd_headroom_tier_affinity || adapter::node_not_placement_blocked(node))
                && adapter::node_network_slot_available(node, &pods)
        })
    }

    async fn handoff_started_recovery_execution(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
        restore_context: Option<StartedRecoveryRestoreContext>,
    ) -> Result<Option<(SandboxExecutionClaim, StartedRecoveryRestoreSource)>, SubstrateError> {
        let restore_context = match restore_context {
            Some(restore_context) => restore_context,
            None => {
                self.store
                    .started_recovery_restore_context(sandbox_id, generation)
                    .await?
            }
        };
        let allowed_local_source = match restore_context {
            StartedRecoveryRestoreContext::Ready(StartedRecoveryRestoreSource::Local {
                source_node_name,
            }) if self.started_recovery_source_node_available(&source_node_name) => {
                Some(source_node_name)
            }
            StartedRecoveryRestoreContext::Ready(StartedRecoveryRestoreSource::Local {
                ..
            })
            | StartedRecoveryRestoreContext::Pending => {
                return Err(SubstrateError::restore_source_pending(sandbox_id));
            }
            StartedRecoveryRestoreContext::Ready(StartedRecoveryRestoreSource::Remote {
                ..
            }) => None,
            StartedRecoveryRestoreContext::Unavailable => {
                return Err(SubstrateError::RestoreContextUnavailable {
                    message: format!(
                        "sandbox {sandbox_id} generation {generation} has no verified local or \
                         remote restore source; leaving the execution unchanged"
                    ),
                });
            }
        };

        match self
            .store
            .replace_started_recovery_execution(
                sandbox_id,
                generation,
                allowed_local_source.as_deref(),
            )
            .await?
        {
            StartedRecoveryReplacement::Replaced { claim, source } => Ok(Some((*claim, source))),
            StartedRecoveryReplacement::Pending => {
                Err(SubstrateError::restore_source_pending(sandbox_id))
            }
            StartedRecoveryReplacement::Unavailable => {
                Err(SubstrateError::RestoreContextUnavailable {
                    message: format!(
                        "sandbox {sandbox_id} generation {generation} lost its verified restore \
                         source before execution handoff; leaving the execution unchanged"
                    ),
                })
            }
            StartedRecoveryReplacement::OwnershipLost => Ok(None),
        }
    }

    async fn replace_started_recovery_pod(
        &self,
        sandbox: &Sandbox,
        claim: &mut SandboxExecutionClaim,
        pod_name: &mut String,
        restore_source: &mut Option<StartedRecoveryRestoreSource>,
        replacement: StartedRecoveryPodReplacement<'_>,
    ) -> Result<bool, SubstrateError> {
        let StartedRecoveryPodReplacement {
            reason,
            restore_context,
            explicit_source_node,
            explicit_storage_uri,
            activation_assignment,
        } = replacement;
        let sandbox_id = sandbox.id;
        let replaced_generation = claim.row.generation;
        let replaced_pod_name = pod_name.clone();
        let generation = ExecutionGeneration::new(replaced_generation).map_err(|err| {
            SubstrateError::Failed {
                message: format!(
                    "invalid started recovery execution generation {replaced_generation}: {err}"
                ),
            }
        })?;
        let Some((replacement_claim, source)) = self
            .handoff_started_recovery_execution(sandbox_id, generation, restore_context)
            .await?
        else {
            return Ok(false);
        };
        if let Some(assignment) = activation_assignment
            && let Some(id) = replacement_claim.row.activation_attempt_id
        {
            assignment.record_assigned(id, replacement_claim.row.generation);
        }
        let replacement_event_details = started_recovery_replacement_event_details(
            SandboxEventDetails::default()
                .operation("ensure_pod_exists")
                .reason(reason)
                .lifecycle_state(sandbox.lifecycle.as_str())
                .execution_generation(replaced_generation.to_string())
                .pod_name(replaced_pod_name),
            explicit_source_node,
            explicit_storage_uri,
            Some(&source),
        );
        SandboxEvent::new(
            sandbox_id,
            SandboxEventType::ExecutionReplaced,
            SandboxEventSource::Substrate,
        )
        .emit_with(replacement_event_details);
        *claim = replacement_claim;
        *pod_name = claim.row.pod_name.clone();
        *restore_source = Some(source);
        Ok(true)
    }

    pub fn lookup_sandbox_pod(&self, pod_name: &str) -> Option<Pod> {
        let key = ObjectRef::<Pod>::new(pod_name).within(&self.k8s.sandbox_namespace);
        self.pod_store.get(&key).map(|arc| (*arc).clone())
    }

    /// Delete a sandbox pod and block until its deletion is observed, bounded
    /// by `k8s_blocking_delete_timeout`. Used on suspend so the control plane
    /// does not publish `suspended` while kubelet/containerd teardown of the
    /// old execution pod can still run later.
    ///
    /// In the healthy path this rides the shared sandbox-pod reflector: after
    /// an accepted `DELETE` it waits on the reflector delete signal
    /// (`PodEvents`) and then confirms absence with one authoritative GET. The
    /// GET is required because the filtered reflector store also drops terminal
    /// pods that can still exist behind a finalizer. If the object is still
    /// present, one final GET at the deadline decides the bounded outcome.
    pub(super) async fn k8s_blocking_delete_sandbox_pod_if_uid(
        &self,
        pod_name: &str,
        expected_uid: &PodUid,
    ) -> Result<K8sBlockingDeleteOutcome, K8sBlockingDeleteError> {
        self.k8s_blocking_delete_sandbox_pod_for_operation(
            pod_name,
            "suspend",
            Some((expected_uid.as_str(), None)),
        )
        .await
    }

    /// Delete a terminal pod that is only being kept as stale projection
    /// carrier state, then wait until absence is observed.
    pub async fn delete_sandbox_pod_for_projection_cleanup(
        &self,
        pod_name: &str,
    ) -> Result<K8sBlockingDeleteOutcome, K8sBlockingDeleteError> {
        self.k8s_blocking_delete_sandbox_pod_for_operation(pod_name, "projection_cleanup", None)
            .await
    }

    /// Blocking, absence-confirming delete of a lingering strictly-older-
    /// generation pod before a restore create. Name-scoped (no UID/RV
    /// precondition): selection is already generation-fenced
    /// ([`lingering_sandbox_pod_names`]) so the name targets exactly one prior
    /// generation, and — unlike the same-generation re-drive teardown — a
    /// bound/running older-generation pod is precisely the orphan hazard we
    /// must remove, so a precondition that stepped aside on a just-bound pod
    /// would defeat the fence. Confirms observed absence (delete signal +
    /// authoritative GET at the deadline) so the caller can make it a create
    /// prerequisite.
    async fn delete_lingering_prior_generation_pod(
        &self,
        pod_name: &str,
    ) -> Result<K8sBlockingDeleteOutcome, K8sBlockingDeleteError> {
        self.k8s_blocking_delete_sandbox_pod_for_operation(
            pod_name,
            "lingering_prior_generation",
            None,
        )
        .await
    }

    /// UID + resourceVersion preconditioned blocking delete for the cross-node
    /// re-drive: only deletes the pod if it is still in the exact state the
    /// caller inspected. The UID fences a same-name successor created by a
    /// concurrent re-drive; the resourceVersion fences a concurrent scheduler
    /// bind of the *same* pod (bind keeps the UID but bumps the RV). A
    /// precondition rejection surfaces as
    /// [`K8sBlockingDeleteOutcome::PreconditionFailed`].
    async fn delete_sandbox_pod_for_redrive(
        &self,
        pod_name: &str,
        expected_uid: &str,
        expected_resource_version: &str,
    ) -> Result<K8sBlockingDeleteOutcome, K8sBlockingDeleteError> {
        self.k8s_blocking_delete_sandbox_pod_for_operation(
            pod_name,
            "redrive",
            Some((expected_uid, Some(expected_resource_version))),
        )
        .await
    }

    async fn k8s_blocking_delete_sandbox_pod_for_operation(
        &self,
        pod_name: &str,
        metric_operation: &'static str,
        preconditions: Option<(&str, Option<&str>)>,
    ) -> Result<K8sBlockingDeleteOutcome, K8sBlockingDeleteError> {
        let pods: Api<Pod> = Api::namespaced(self.k8s.client.clone(), &self.k8s.sandbox_namespace);
        let metric_started_at = std::time::Instant::now();
        let timeout_budget = self.k8s.k8s_blocking_delete_timeout;
        let deadline = Instant::now() + timeout_budget;
        let timeout_secs = timeout_budget.as_secs();
        let mut waited = false;

        // Capture whether the reflector already had this pod cached *before* the
        // delete. The pod is a running execution, so normally yes; seeding the
        // wait with this lets it confirm immediately if the reflector removes
        // the pod between the accepted DELETE and the wait starting (fast-delete
        // race) instead of treating the now-absent store as an ambiguous miss.
        let pod_key = ObjectRef::<Pod>::new(pod_name).within(&self.k8s.sandbox_namespace);
        let present_before_delete = self.pod_store.get(&pod_key).is_some();

        let delete_params = match preconditions {
            Some((uid, resource_version)) => DeleteParams {
                preconditions: Some(kube::api::Preconditions {
                    uid: Some(uid.to_string()),
                    resource_version: resource_version.map(str::to_string),
                }),
                ..Default::default()
            },
            None => DeleteParams::default(),
        };
        let result = async {
            match timeout_at(deadline, pods.delete(pod_name, &delete_params)).await {
                Ok(Ok(_)) => {}
                Ok(Err(err)) if is_not_found_error(&err) => {
                    return Ok(K8sBlockingDeleteOutcome::NotFound);
                }
                // A precondition rejection (409 Conflict) means the pod at this
                // name is no longer in the state the caller inspected —
                // replaced (UID) or mutated, e.g. bound (resourceVersion).
                Ok(Err(kube::Error::Api(response)))
                    if preconditions.is_some() && response.code == 409 =>
                {
                    return Ok(K8sBlockingDeleteOutcome::PreconditionFailed);
                }
                Ok(Err(err)) => {
                    return Err(K8sBlockingDeleteError::Delete {
                        pod_name: pod_name.to_string(),
                        source: Box::new(err),
                    });
                }
                Err(_) => {
                    return Err(K8sBlockingDeleteError::DeleteTimeout {
                        pod_name: pod_name.to_string(),
                        timeout_secs,
                    });
                }
            }

            // Healthy path: observe pod absence through the shared reflector,
            // driven by the delete signal — no apiserver GET while the watch
            // keeps up. The wait runs out the full budget in the unconfirmed
            // case (it confirms early on the observed present->absent
            // transition), so a pod still `Terminating` mid-budget but deleted
            // before the deadline is still confirmed and a cold cache doesn't
            // collapse to an immediate timeout.
            waited = true;
            let reflector_absence = pod_reflector::wait_for_pod_absence(
                &self.pod_store,
                &self.k8s.sandbox_namespace,
                &self.pod_events,
                pod_name,
                deadline,
                present_before_delete,
            )
            .await;

            if reflector_absence == pod_reflector::ReflectorAbsence::Confirmed {
                match timeout(
                    K8S_BLOCKING_DELETE_FALLBACK_GET_TIMEOUT,
                    fetch_sandbox_pod_uncached(&self.k8s, pod_name),
                )
                .await
                {
                    Ok(Ok(None)) => return Ok(K8sBlockingDeleteOutcome::Deleted),
                    Ok(Ok(Some(_))) | Ok(Err(_)) | Err(_) => {
                        // A terminal pod disappears from the filtered reflector
                        // before the Kubernetes object is necessarily deleted.
                        // Preserve the caller's remaining budget and let the
                        // authoritative deadline GET make the final decision.
                        tokio::time::sleep_until(deadline).await;
                    }
                }
            }

            // Single authoritative GET at the deadline: covers reflector lag /
            // watch failure so we don't publish `suspended` on a stale cache.
            // It carries its own short budget, so the operation can run up to
            // `K8S_BLOCKING_DELETE_FALLBACK_GET_TIMEOUT` past the deadline — the
            // intended "GET at the deadline" fallback. A timeout keeps the
            // execution active and makes the operation retryable, so this
            // bounded overrun is preferable to retiring an unconfirmed pod.
            match timeout(
                K8S_BLOCKING_DELETE_FALLBACK_GET_TIMEOUT,
                fetch_sandbox_pod_uncached(&self.k8s, pod_name),
            )
            .await
            {
                Ok(Ok(None)) => Ok(K8sBlockingDeleteOutcome::Deleted),
                Ok(Ok(Some(_))) => Ok(K8sBlockingDeleteOutcome::TimedOut),
                Ok(Err(err)) => {
                    tracing::warn!(
                        pod_name,
                        error = %err,
                        "failed to observe Kubernetes pod deletion after accepted delete"
                    );
                    Ok(K8sBlockingDeleteOutcome::ObserveError)
                }
                Err(_) => Ok(K8sBlockingDeleteOutcome::TimedOut),
            }
        }
        .await;

        let result_tag = result
            .as_ref()
            .map(|outcome| outcome.as_str())
            .unwrap_or_else(K8sBlockingDeleteError::result_tag);
        pplx_space_metrics::sandbox::api::k8s_blocking_delete_latency(
            "pod",
            metric_operation,
            result_tag,
            waited,
            metric_started_at,
        );

        result
    }

    async fn effective_network_policy_for_sandbox(
        &self,
        sandbox: &Sandbox,
    ) -> Result<pplx_space_net_common::NetworkPolicy, SubstrateError> {
        if let Some(policy) = sandbox.network_policy.as_ref() {
            return Ok(policy.0.clone());
        }
        let Some(template) = sandbox.template_id.as_deref() else {
            return Ok(pplx_space_net_common::NetworkPolicyV1::default());
        };
        Ok(self
            .store
            .template_network_policy(template)
            .await?
            .unwrap_or_default())
    }

    /// Read a sandbox pod directly from the apiserver, bypassing the
    /// reflector cache. Use on paths that must verify cached terminal state
    /// before deleting a carrier pod.
    pub async fn read_sandbox_pod_uncached(
        &self,
        pod_name: &str,
    ) -> Result<Option<Pod>, KubeError> {
        fetch_sandbox_pod_uncached(&self.k8s, pod_name).await
    }

    /// Reads a sandbox pod with cache-first, event-wait, kube-apiserver-
    /// fallback semantics.
    ///
    /// 1. **Cache hit + settled** → return cached.
    /// 2. **Cache miss or unsettled** → wait briefly (`REFLECTOR_WAIT_BUDGET`)
    ///    for a reflector event referencing this pod. On event, re-check the
    ///    cache; if now settled, return cached.
    /// 3. **Event budget elapsed** or **event arrived but still unsettled** →
    ///    fall through to a direct `GET /pods/{name}` (the pre-A4 behavior).
    ///
    /// "Settled" means the cached entry has reached a status that callers
    /// branch on: assigned IP, terminal phase, or scheduler-marked
    /// unschedulable.
    ///
    /// The reflector watch is independent from the per-pod watches used by
    /// `wait_for_pod_proxy_reachable`, so it can briefly lag a Modified event
    /// that the per-pod watch already observed. The bounded event wait closes
    /// most of that window without putting load on the kube apiserver under
    /// burst; the GET fallback preserves correctness when the reflector
    /// genuinely lags or the pod is unknown to the cache.
    ///
    /// Steady-state behavior is unchanged: once a pod is running with an IP
    /// (or terminal/unschedulable), the cache is authoritative and no GET
    /// happens. Extra GETs are bounded to the brief window between pod
    /// creation/scheduling and the reflector observing the bind event.
    pub async fn read_sandbox_pod(&self, pod_name: &str) -> Result<Option<Pod>, KubeError> {
        if let Some(pod) = self.lookup_sandbox_pod(pod_name)
            && pod_status_is_settled(&pod)
        {
            pplx_space_metrics::sandbox::api::read_sandbox_pod_outcome("cache_settled");
            return Ok(Some(pod));
        }
        // Wait for either a matching reflector event or the budget to elapse.
        // The boolean is informational — re-check the cache regardless. The
        // reflector store is updated by the same task that drives the event
        // broadcast, so even on a `Lagged`-then-timeout path the store may
        // already have the assignment we were waiting for.
        let _ = self
            .pod_events
            .wait_for(pod_name, REFLECTOR_WAIT_BUDGET)
            .await;
        if let Some(pod) = self.lookup_sandbox_pod(pod_name)
            && pod_status_is_settled(&pod)
        {
            pplx_space_metrics::sandbox::api::read_sandbox_pod_outcome("event_settled");
            return Ok(Some(pod));
        }
        pplx_space_metrics::sandbox::api::read_sandbox_pod_outcome("get_fallback");
        fetch_sandbox_pod_uncached(&self.k8s, pod_name).await
    }

    /// Watch a freshly-created source-pinned restore pod for the scheduler's
    /// first placement decision, bounded by `budget`. Returns as soon as the
    /// pod is bound (`spec.nodeName` set) or marked `Unschedulable`, riding the
    /// shared reflector's event signal (no apiserver polling). A required
    /// single-node pin resolves in one scheduling cycle, so the common cases
    /// return well under `budget`. On budget exhaustion it does one
    /// authoritative apiserver GET; a pod that is *still undecided* (or gone)
    /// returns `None`. Undecided is deliberately NOT coerced to
    /// `Unschedulable`: only an explicit `PodScheduled=False/Unschedulable`
    /// verdict justifies tearing down the pin. A scheduler that is slow or
    /// wedged delays unpinned pods just the same, so re-driving cross-node on
    /// "no decision yet" would burn the locality this feature exists to keep
    /// while gaining nothing — the caller instead proceeds to the readiness
    /// wait, and retries / the transient-lifecycle sweeper bound a pod that
    /// never gets a decision.
    async fn await_scheduling_outcome(
        &self,
        pod_name: &str,
        budget: Duration,
    ) -> Result<Option<SchedulingOutcome>, SubstrateError> {
        let deadline = Instant::now() + budget;
        // Inspect the cached `Arc<Pod>` in place rather than via
        // `lookup_sandbox_pod` (which deep-clones the whole Pod): the classifier
        // reads only `status.conditions` and `spec.nodeName`, and this loop can
        // re-check up to ~20 times against a lagging scheduler.
        let key = ObjectRef::<Pod>::new(pod_name).within(&self.k8s.sandbox_namespace);
        loop {
            if let Some(pod) = self.pod_store.get(&key)
                && let Some(outcome) = scheduling_outcome_of(&pod)
            {
                return Ok(Some(outcome));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let _ = self
                .pod_events
                .wait_for(pod_name, remaining.min(REFLECTOR_WAIT_BUDGET))
                .await;
        }
        let pod = fetch_sandbox_pod_uncached(&self.k8s, pod_name)
            .await
            .map_err(|err| {
                kube_error_for_substrate("failed to read pod while awaiting scheduling", err)
            })?;
        Ok(pod.as_ref().and_then(scheduling_outcome_of))
    }

    /// Tear down a source-pinned restore pod that came back `Unschedulable` and
    /// re-create it cross-node under the same generation. Shared by the
    /// fresh-create watch and the retry path: an interrupted create can leave a
    /// pinned pod that a retry would otherwise reuse forever, so the retry
    /// re-drives it here instead.
    ///
    /// Two safety properties are enforced here rather than at the call sites:
    ///
    /// * **The destructive delete never trusts the reflector.** Both callers
    ///   observed `Unschedulable` (possibly from cache); an authoritative GET
    ///   re-checks the live pod first, and a pod that has since bound is left
    ///   alone (`PodRetained`). Reflector staleness is unbounded, so a
    ///   cached verdict alone could tear down a pod whose VM is already
    ///   restoring on the source. After the GET only the benign ms-scale bind
    ///   race remains: a pod bound in that window has no IP/VM yet, and the
    ///   blocking delete removes it before the successor is created.
    /// * **Source provenance survives the re-drive.** The successor keeps the
    ///   wedged pod's `suspend-restore-source-node` annotation (the node whose
    ///   local bytes are trusted) while rendering **no** scheduling affinity.
    ///   Dropping it entirely would poison a later retry: `suspend_restore_plan`
    ///   prefers the live pod's annotations, and without them it falls back to
    ///   `restore_source_node_name`, which reads placement — by then the
    ///   *destination* node — so sbx-mgr there could trust stale local bytes of
    ///   an older restore instead of the selected snapshot URI. When the pod is
    ///   already gone (a concurrent re-drive deleted it first, but this request
    ///   wins the successor POST), `caller_source_node` — the caller's plan
    ///   source, derived while the pinned pod still existed — is the fallback
    ///   provenance, so that interleaving cannot strip the annotation either.
    ///
    /// The blocking delete confirms absence before the same-name re-POST so it
    /// can't 409.
    async fn redrive_unschedulable_restore_cross_node(
        &self,
        sandbox: &Sandbox,
        claim: &SandboxExecutionClaim,
        claim_lifecycle: SandboxLifecycle,
        caller_source_node: Option<&str>,
        suspend_restore_storage_uri: Option<&str>,
        restore_lease_token: Option<Uuid>,
    ) -> Result<RedriveOutcome, SubstrateError> {
        let pod_name = &claim.row.pod_name;
        let activation_attempt_id = claim.row.activation_attempt_id;
        let live = fetch_sandbox_pod_uncached(&self.k8s, pod_name)
            .await
            .map_err(|err| {
                kube_error_for_substrate("failed to re-verify pod before cross-node re-drive", err)
            })?;
        // Tear down only the pinned predecessor the caller believed it saw:
        // a bound pod (stale caller verdict) or a pod without the required pin
        // (a concurrent re-drive already replaced it with a same-name
        // successor) is left alone.
        let live_identity = match live.as_ref() {
            Some(pod)
                if matches!(
                    scheduling_outcome_of(pod),
                    Some(SchedulingOutcome::Scheduled { .. })
                ) || !pod_has_required_source_pin(pod) =>
            {
                tracing::info!(
                    sandbox_id = %sandbox.id,
                    pod_name = %pod_name,
                    generation = claim.row.generation,
                    "live pod bound or already re-driven before teardown; leaving it in place"
                );
                // Bound: classify by the landing node — a shortlist pod keeps
                // its required pin whether it bound on the source or a
                // fallback, so pin-presence alone cannot distinguish them.
                // Unbound with the pin absent: a concurrent re-drive's
                // cross-node successor is running instead.
                let cross_node = match scheduling_outcome_of(pod) {
                    Some(SchedulingOutcome::Scheduled { node_name }) => {
                        Some(node_name.as_str()) != caller_source_node
                    }
                    _ => !pod_has_required_source_pin(pod),
                };
                return Ok(RedriveOutcome::PodRetained { cross_node });
            }
            Some(pod) => pod
                .metadata
                .uid
                .clone()
                .zip(pod.metadata.resource_version.clone()),
            None => None,
        };
        // The wedged pod's annotation is the authoritative source-node
        // provenance; the DB derivation is placement-based and unreliable once
        // pods start moving. When the pod is already gone (concurrent delete),
        // fall back to the caller's plan source — derived while the pinned pod
        // still carried the annotation — so a successor this request creates
        // never ships without provenance.
        let source_node_annotation: Option<String> = live
            .as_ref()
            .and_then(|pod| pod.metadata.annotations.as_ref())
            .and_then(|annotations| annotations.get(SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION))
            .filter(|value| !value.trim().is_empty())
            .cloned()
            .or_else(|| caller_source_node.map(str::to_string));
        // UID + resourceVersion preconditioned delete: it can only remove the
        // exact pod *state* inspected above. A same-name successor created in
        // the GET->DELETE window has a different UID; a concurrent scheduler
        // bind of the SAME pod keeps the UID but bumps the resourceVersion —
        // both are rejected with a 409 precondition failure instead of tearing
        // down a pod that may already be starting its VM. When the GET found no
        // pod there is nothing of ours to tear down and **no delete runs at
        // all**; if a successor appears before our POST, the 409 step-aside
        // below retains it.
        if let Some(mut delete_identity) = live_identity {
            let mut precondition_retries = 0usize;
            loop {
                // Clone the identity for this attempt so the RetryDelete arm can
                // reassign `delete_identity` without aliasing a borrow held
                // across the `.await`. Bounded, so the clone cost is negligible.
                let (uid, resource_version) = delete_identity.clone();
                match self
                    .delete_sandbox_pod_for_redrive(pod_name, &uid, &resource_version)
                    .await
                {
                    Ok(K8sBlockingDeleteOutcome::Deleted | K8sBlockingDeleteOutcome::NotFound) => {
                        break;
                    }
                    Ok(K8sBlockingDeleteOutcome::PreconditionFailed) => {
                        // The pod changed under us: replaced, bound, or a
                        // status-only RV bump. Re-read once and classify; never
                        // tear down blind on a conflict.
                        let now_live = fetch_sandbox_pod_uncached(&self.k8s, pod_name)
                            .await
                            .map_err(|err| {
                                kube_error_for_substrate(
                                    "failed to re-classify pod after delete precondition \
                                         rejection",
                                    err,
                                )
                            })?;
                        match classify_precondition_reread(now_live.as_ref(), caller_source_node) {
                            // Absent: a conflict does not prove a successor
                            // exists. Fall through to the successor POST (its
                            // own 409 handling steps aside for a real creator)
                            // rather than retain a deleted pod.
                            PreconditionReread::Absent => {
                                tracing::info!(
                                    sandbox_id = %sandbox.id,
                                    pod_name = %pod_name,
                                    generation = claim.row.generation,
                                    "pinned pod gone after preconditioned delete rejection; \
                                     re-creating cross-node successor"
                                );
                                break;
                            }
                            // A live pod (bound, or an unpinned concurrent
                            // successor) drives the restore.
                            PreconditionReread::Retain { cross_node } => {
                                tracing::info!(
                                    sandbox_id = %sandbox.id,
                                    pod_name = %pod_name,
                                    generation = claim.row.generation,
                                    cross_node,
                                    "pod bound or already re-driven under preconditioned delete; \
                                     leaving it in place"
                                );
                                return Ok(RedriveOutcome::PodRetained { cross_node });
                            }
                            // Same required-pinned pod, only the RV moved (a
                            // status write). Retry the preconditioned delete
                            // with the fresh identity — do NOT bounce the caller
                            // back onto the same impossible pod.
                            PreconditionReread::RetryDelete {
                                uid,
                                resource_version,
                            } => {
                                precondition_retries += 1;
                                if precondition_retries >= REDRIVE_PRECONDITION_RETRY_ATTEMPTS {
                                    tracing::warn!(
                                        sandbox_id = %sandbox.id,
                                        pod_name = %pod_name,
                                        generation = claim.row.generation,
                                        precondition_retries,
                                        "deferring cross-node re-drive: unschedulable source-pinned \
                                         pod kept changing resourceVersion under preconditioned \
                                         delete; retry"
                                    );
                                    return Err(SubstrateError::Unavailable {
                                        message: format!(
                                            "deferring restore: unschedulable source-pinned pod \
                                             {pod_name} for sandbox {} kept changing \
                                             resourceVersion; retry",
                                            sandbox.id
                                        ),
                                    });
                                }
                                delete_identity = (uid, resource_version);
                                continue;
                            }
                            PreconditionReread::Indeterminate => {
                                return Err(SubstrateError::Unavailable {
                                    message: format!(
                                        "deferring restore: unschedulable source-pinned pod \
                                         {pod_name} for sandbox {} exposed no identity to \
                                         precondition on; retry",
                                        sandbox.id
                                    ),
                                });
                            }
                        }
                    }
                    outcome => {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            pod_name = %pod_name,
                            generation = claim.row.generation,
                            ?outcome,
                            "deferring in-request cross-node re-drive: unschedulable source-pinned \
                             pod not confirmed gone; retry"
                        );
                        return Err(SubstrateError::Unavailable {
                            message: format!(
                                "deferring restore: unschedulable source-pinned pod {pod_name} for \
                                 sandbox {} not confirmed gone; retry",
                                sandbox.id
                            ),
                        });
                    }
                }
            }
        }

        // The pinned pod's UID was persisted before the scheduler ruled it
        // Unschedulable, so the row is `created` (pod_ip NULL) even though that
        // pod is now deleted. Reset it to `creating` before building the
        // successor: the cleanup below (guard + failure handler) retires only a
        // `creating` claim, so this reset is what makes a drop or failure
        // mid-re-drive tear the successor down instead of stranding it. It is a
        // hard prerequisite, not best-effort — proceed only on a confirmed
        // reset:
        //   * `Err`: the row is still `created`; POSTing a successor now would
        //     reopen the orphan gap, so fail retryably before creating one.
        //   * `Ok(false)`: a concurrent winner already materialized the row, or
        //     this attempt lost the restore-lease fence — step aside.
        match self
            .store
            .reset_active_execution_to_creating_if_generation(
                sandbox.id,
                claim.row.generation,
                restore_lease_token,
                activation_attempt_id,
            )
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                tracing::info!(
                    sandbox_id = %sandbox.id,
                    pod_name = %pod_name,
                    generation = claim.row.generation,
                    "cross-node re-drive stepping aside: create-claim reset matched no \
                     unmaterialized row (materialized by a concurrent winner or lease lost)"
                );
                return Ok(RedriveOutcome::OwnershipLost);
            }
            Err(err) => return Err(err),
        }

        // Everything past the confirmed delete + reset runs under one failure
        // handler: the pinned pod is gone and the row is `creating`, so an error
        // below (context rebuild, POST, persist) would otherwise leave a
        // `creating` claim with no pod behind it — and every subsequent ensure
        // call would take the create-in-flight early return until the stale-
        // create age (300s) expires. On failure the unmaterialized claim is
        // retired so an immediate retry re-creates instead of stalling. The
        // guard covers the path the failure handler cannot: the request future
        // being *dropped* (client disconnect) mid-block, which skips every
        // handler below. Always armed — the reset just guaranteed `creating`,
        // so the retirement CAS matches even when `claim.row.state` is a stale
        // `Created` from the retry-recovery path.
        let mut claim_guard = RedriveClaimGuard::armed(
            &self.store,
            &self.k8s,
            sandbox.id,
            claim,
            restore_lease_token,
            activation_attempt_id,
        );
        let redrive_result: Result<RedriveOutcome, SubstrateError> = async {
            // Build the successor context directly: keep the portable S3
            // snapshot and the source-node annotation (provenance for later
            // retries and for sbx-mgr's local-bytes trust check), but render no
            // *source-node* affinity — no pin, no shortlist, no preference — so
            // the successor schedules anywhere the boundary allows. It does
            // still carry the node-group boundary once that gate is on, which
            // is the point: "anywhere" means anywhere in the candidate set, not
            // anywhere in the fleet.
            // `parse_pod_create_context` can't express "source as annotation
            // only" — it always pairs a known source with a placement pin.
            let cross_node_context = PodCreateContext::Restore(RestoreContext::UserRestore {
                suspend_restore_storage_uri,
                suspend_restore_source_node: source_node_annotation.as_deref(),
                direct_node_name: None,
                preferred_node_name: None,
            });
            let auto_lifecycle = self
                .store
                .auto_lifecycle_policy_after_claim(sandbox.id)
                .await?;
            let auto_lifecycle_json =
                serde_json::to_string(&auto_lifecycle).map_err(|err| SubstrateError::Failed {
                    message: format!("failed to serialize auto lifecycle policy: {err}"),
                })?;
            let network_policy = self.effective_network_policy_for_sandbox(sandbox).await?;
            let network_policy_json =
                serde_json::to_string(&network_policy).map_err(|err| SubstrateError::Failed {
                    message: format!("failed to serialize network policy: {err}"),
                })?;
            ensure_network_policy_annotation_size(&network_policy_json)?;
            let sandbox_owner = self.store.sandbox_owner_for_user(sandbox.user_id);
            // Inside the guard's block on purpose: a resolution failure here
            // retires the reset claim through `RedriveClaimGuard` rather than
            // leaving a `creating` row with no pod behind it.
            let node_group_placement = self
                .resolve_node_group_placement(sandbox, &cross_node_context)
                .await?;

            let cross_node_pod = match create_sandbox_pod(
                &self.k8s,
                self.sandbox_pod_request(
                    sandbox,
                    &claim.row,
                    SandboxPodRequestInputs {
                        sandbox_owner: &sandbox_owner,
                        auto_lifecycle_json: &auto_lifecycle_json,
                        network_policy_json: &network_policy_json,
                        node_group_placement: node_group_placement.placement(),
                    },
                    cross_node_context,
                ),
            )
            .await
            {
                Ok(pod) => pod,
                // A concurrent re-drive won the POST race — its successor owns
                // the name. Step aside rather than fail: the failure handler
                // below would retire the shared still-unmaterialized claim,
                // making the winner's persist no-op and the winner delete its
                // own freshly-created pod.
                Err(kube::Error::Api(response)) if response.code == 409 => {
                    tracing::info!(
                        sandbox_id = %sandbox.id,
                        pod_name = %pod_name,
                        generation = claim.row.generation,
                        "concurrent re-drive already created the successor; leaving it in place"
                    );
                    return Ok(RedriveOutcome::PodRetained { cross_node: true });
                }
                Err(err) => {
                    return Err(SubstrateError::Failed {
                        message: format!("failed to re-create restore pod cross-node: {err}"),
                    });
                }
            };

            match self
                .store
                .persist_materialized_pod_metadata(
                    sandbox.id,
                    claim.row.generation,
                    claim_lifecycle,
                    cross_node_pod.metadata.uid.as_deref(),
                    None,
                )
                .await
            {
                Ok(Some(_)) => Ok(RedriveOutcome::Redriven(Box::new(cross_node_pod))),
                Ok(None) => {
                    // Materialization ownership was revoked between the two
                    // creates; drop the cross-node pod we just made and let the
                    // winner drive.
                    self.delete_pod_and_forget_tracking(
                        sandbox.id,
                        pod_name,
                        claim.row.generation,
                        PodTrackingCleanupCause::ExplicitTeardown,
                    )
                    .await?;
                    Ok(RedriveOutcome::OwnershipLost)
                }
                Err(error) => {
                    // Mirror the fresh-create path: never leave an untracked
                    // pod behind a failed persist.
                    if let Err(delete_error) = delete_sandbox_pod_with_throttle_retry(
                        &self.k8s,
                        pod_name,
                        "delete_pod_after_redrive_persistence_failure",
                    )
                    .await
                        && !is_not_found_error(&delete_error)
                    {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            pod_name = %pod_name,
                            generation = claim.row.generation,
                            error = %delete_error,
                            "failed to delete cross-node restore pod after persistence failure"
                        );
                    }
                    Err(error)
                }
            }
        }
        .await;
        // Reached only when the block ran to completion; a drop mid-block
        // leaves the guard armed and its `Drop` retires the claim.
        claim_guard.disarm();

        match redrive_result {
            Ok(outcome) => {
                if matches!(outcome, RedriveOutcome::Redriven(_)) {
                    tracing::info!(
                        sandbox_id = %sandbox.id,
                        pod_name = %pod_name,
                        generation = claim.row.generation,
                        "source-pinned restore was Unschedulable; re-drove cross-node in-request"
                    );
                }
                Ok(outcome)
            }
            Err(err) => {
                // The pinned pod is deleted, any successor was torn down inside
                // the block, and the reset left the row `creating` — retire it
                // (best-effort) so the next retry re-creates immediately instead
                // of stalling on the create-in-flight early return. The
                // retirement CASes on `creating + pod_uid IS NULL + pod_ip IS
                // NULL`, so a row a concurrent attempt already materialized is
                // never touched.
                if let Err(abandon_error) = self
                    .store
                    .abandon_unmaterialized_create_claim(sandbox.id, claim)
                    .await
                {
                    tracing::warn!(
                        sandbox_id = %sandbox.id,
                        generation = claim.row.generation,
                        error = %abandon_error,
                        "failed to retire creating claim after cross-node re-drive failure"
                    );
                }
                Err(err)
            }
        }
    }

    /// Kube-verified repair of an active execution row whose pod metadata was
    /// never persisted — the wake handler created the pod and the node-side
    /// restore succeeded, but the handler died before `persist_pod_metadata`
    /// ran, leaving the execution unroutable (`pod_ip` NULL / not `running`)
    /// while a healthy pod serves traffic. Used by the cluster controller's
    /// transient-lifecycle sweeper before it converges an alive `restoring`
    /// row to `started`: in this substrate `started` is a contract that a
    /// routable execution exists, and asserting it without one wedges the
    /// sandbox behind the gen≥2 fresh-pod refusal.
    ///
    /// Truth-copying, not truth-inventing: the only write is
    /// [`K8sStore::persist_pod_metadata`] — the exact statement the wake path
    /// uses, CAS-guarded on `sandbox_id + generation + active` — and only
    /// with values read from an **uncached** apiserver GET of a pod whose
    /// sandbox-id and generation annotations match the active execution
    /// ([`restored_pod_routability`]). Placement is never written: an
    /// execution row with a probe-reachable node necessarily had its node
    /// placement persisted transactionally with `k8s_node_name` already.
    ///
    /// `budget` bounds the apiserver GET so a wedged apiserver can't stall
    /// the caller's loop. A timeout or transport error is returned as an
    /// error (the caller defers and retries next tick), never as
    /// `NotRoutable`.
    ///
    /// A hydration write that no-ops because the active execution was
    /// retired or replaced between the apiserver GET and the persist (the
    /// generation-guarded `persist_pod_metadata` reports this) returns
    /// `NotRoutable`, never `Hydrated`. Even so, the returned outcome is
    /// advisory, not a fence — `Hydrated`/`AlreadyRoutable` can go stale the
    /// instant this returns. The sweeper's converge CAS re-checks
    /// routability inside the `UPDATE` itself; do not treat this outcome as
    /// proof for any other lifecycle write.
    pub async fn hydrate_restored_execution(
        &self,
        sandbox_id: SandboxId,
        budget: Duration,
    ) -> Result<ExecutionHydrationOutcome, SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let Some(execution) = self.store.active_execution_status(sandbox_uuid).await? else {
            return Ok(ExecutionHydrationOutcome::NotRoutable);
        };
        if execution.state == ActiveExecutionState::Running && execution.pod_ip.is_some() {
            return Ok(ExecutionHydrationOutcome::AlreadyRoutable);
        }
        // Authoritative uncached GET: DB state is about to be written from
        // this answer, so reflector lag must not stand in for the apiserver.
        let pod = match timeout(budget, self.read_sandbox_pod_uncached(&execution.pod_name)).await {
            Ok(Ok(pod)) => pod,
            Ok(Err(err)) => {
                return Err(SubstrateError::Failed {
                    message: format!(
                        "failed to fetch Kubernetes pod {} for execution hydration: {err}",
                        execution.pod_name
                    ),
                });
            }
            Err(_) => {
                return Err(SubstrateError::Timeout {
                    message: format!(
                        "timed out fetching Kubernetes pod {} for execution hydration",
                        execution.pod_name
                    ),
                    phase: None,
                });
            }
        };
        let Some(identity) = pod
            .as_ref()
            .and_then(|pod| restored_pod_routability(pod, sandbox_uuid, execution.generation))
        else {
            return Ok(ExecutionHydrationOutcome::NotRoutable);
        };
        match self
            .store
            .persist_pod_metadata(
                sandbox_uuid,
                execution.generation,
                identity.pod_uid.as_deref(),
                Some(identity.pod_ip.as_str()),
            )
            .await?
        {
            // `Hydrated` is claimed only when the resulting row satisfies
            // the exact predicate the converge CAS checks: `running` with a
            // pod IP.
            Some(ActiveExecutionState::Running) => Ok(ExecutionHydrationOutcome::Hydrated),
            // The row exists but is not `running` — e.g. `recovering`, whose
            // promote to `running` is owned by the node-side recovery flow
            // and deliberately not overridden by a metadata write. The pod
            // IP is recorded, but the execution is not routable yet.
            Some(_) => Ok(ExecutionHydrationOutcome::NotRoutable),
            // The active execution was retired or replaced (a concurrent
            // wake claimed a new generation) between the apiserver GET and
            // the persist — nothing was written and the pod we verified no
            // longer backs the active execution.
            None => Ok(ExecutionHydrationOutcome::NotRoutable),
        }
    }

    async fn existing_suspend_restore_annotations(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<SuspendRestorePlan>, SubstrateError> {
        let Some(execution) = self.store.active_execution_status(sandbox_id).await? else {
            return Ok(None);
        };
        let Some(pod) = self
            .read_sandbox_pod(&execution.pod_name)
            .await
            .map_err(|err| SubstrateError::Failed {
                message: format!("failed to query Kubernetes restore pod: {err}"),
            })?
        else {
            return Ok(None);
        };
        let annotations = pod.metadata.annotations.unwrap_or_default();
        let storage_uri = annotations
            .get(SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION)
            .cloned();
        if let Some(storage_uri) = &storage_uri {
            StorageUri::parse(storage_uri).map_err(|err| SubstrateError::Failed {
                message: format!("invalid suspend restore storage URI on Kubernetes pod: {err}"),
            })?;
        }
        let source_node_name = annotations
            .get(SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION)
            .filter(|value| !value.trim().is_empty())
            .cloned();
        if source_node_name.is_none() && storage_uri.is_none() {
            return Ok(None);
        }
        Ok(Some(SuspendRestorePlan {
            source_node_name,
            storage_uri,
        }))
    }

    pub(super) async fn suspend_restore_plan(
        &self,
        sandbox: &Sandbox,
        evacuation: bool,
    ) -> Result<SuspendRestorePlan, SubstrateError> {
        let existing = self
            .existing_suspend_restore_annotations(sandbox.id)
            .await?;
        let storage_uri = match existing.as_ref().and_then(|plan| plan.storage_uri.clone()) {
            Some(storage_uri) => Some(storage_uri),
            None => {
                // Evacuation restores and restore retries select the newest
                // full snapshot rather than the placement-fresh one: it waits
                // for the current suspend upload (not a stale prior-cycle
                // snapshot) and survives a failed restore attempt's placement
                // bump (see `selected_suspend_restore_storage_uri`).
                self.store
                    .selected_suspend_restore_storage_uri(sandbox, evacuation)
                    .await?
            }
        };
        if evacuation {
            return storage_uri
                .map(|storage_uri| SuspendRestorePlan {
                    source_node_name: None,
                    storage_uri: Some(storage_uri),
                })
                .ok_or_else(|| SubstrateError::Conflict {
                    message: "cannot evacuate suspended sandbox without a ready remote snapshot"
                        .to_string(),
                });
        }
        let source_node_name = match existing.and_then(|plan| plan.source_node_name) {
            Some(source_node_name) => Some(source_node_name),
            None => self.store.restore_source_node_name(sandbox).await?,
        };
        if source_node_name.is_none() && storage_uri.is_none() {
            return Err(SubstrateError::Timeout {
                message: "cannot restore suspended sandbox before source node or remote snapshot is available"
                    .to_string(),
                phase: None,
            });
        }
        Ok(SuspendRestorePlan {
            source_node_name,
            storage_uri,
        })
    }

    /// Resolve where a `stopped` sandbox is restarted from: its source node,
    /// its evacuated disk artifact in S3, or both.
    ///
    /// **The two are deliberately not mutually exclusive.** The pair is what
    /// selects [`parse_pod_create_context`]'s third placement shape — the
    /// source node as a preference or a required shortlist rather than a
    /// binding, with S3 as the fallback — and that shape is the one worth
    /// having, so neither source is dropped when the other is present.
    /// See that function's docs for the `restore_source_node_hard_pin` case
    /// where the pair still renders a required pin.
    /// Suppressing the node once a URI exists would strip that preference from
    /// every stopped restart, including the same-node retry between the
    /// artifact becoming durable and the local row being deleted, when the
    /// rootfs is still on the node. Suppressing the URI once a node is known
    /// would keep the pre-bound `spec.nodeName` shape, whose failure mode is
    /// exactly what evacuating a stopped sandbox exists to remove: the pod
    /// cannot land anywhere else, and a source node that can no longer run it
    /// ends the restore instead of re-driving it cross-node.
    ///
    /// Both absent stays [`SubstrateError::Timeout`] -> 503 + `Retry-After`. A
    /// `start` racing the evacuation window is asked to retry, not told the
    /// sandbox is broken.
    ///
    /// ## Known asymmetry with [`Self::suspend_restore_plan`]
    ///
    /// That one consults [`Self::existing_suspend_restore_annotations`] first,
    /// reusing whatever plan is already stamped on the live pod; this one
    /// always re-derives from Postgres. The *write* half is shared — the pod
    /// builder stamps both annotations whenever the plan carries them — so on
    /// a retry against a pod a prior attempt already created for this
    /// generation, the two attempts can resolve different URIs if the pin
    /// advanced in between. That window is unreachable today: nothing writes a
    /// disk pin to the cluster, so `storage_uri` is always `None` here.
    /// Wiring the read-back belongs with the change that makes the URI half
    /// live, because doing it now would also change how an existing stopped
    /// restart resolves its *node* on retry — live behaviour this change
    /// deliberately leaves alone.
    async fn stopped_restore_plan(
        &self,
        sandbox: &Sandbox,
    ) -> Result<SuspendRestorePlan, SubstrateError> {
        let source_node_name = self.store.restore_source_node_name(sandbox).await?;
        let storage_uri = self
            .store
            .selected_stopped_restore_storage_uri(sandbox)
            .await?;
        if source_node_name.is_none() && storage_uri.is_none() {
            return Err(SubstrateError::Timeout {
                message: "cannot restart stopped sandbox before its source node or evacuated \
                          disk artifact is available"
                    .to_string(),
                phase: None,
            });
        }
        Ok(SuspendRestorePlan {
            source_node_name,
            storage_uri,
        })
    }

    pub(super) fn endpoint_for_pod_ip(
        pod_ip: PodIp,
        port: u16,
    ) -> Result<tonic::transport::Endpoint, SubstrateError> {
        let addr = pod_ip.socket_addr(port).to_string();
        node_api_endpoint(
            &addr,
            POD_WORKER_CONNECT_TIMEOUT,
            crate::controllers::substrate::NODE_RPC_TIMEOUT,
        )
    }

    pub(super) async fn pod_worker_client(
        pod_ip: PodIp,
        port: u16,
        connect_timeout: Duration,
    ) -> Option<crate::controllers::substrate::RuntimeClient> {
        Self::pod_worker_channel(pod_ip, port, connect_timeout)
            .await
            .map(crate::controllers::substrate::RuntimeClient::new)
    }

    pub(super) async fn pod_worker_channel(
        pod_ip: PodIp,
        port: u16,
        connect_timeout: Duration,
    ) -> Option<Channel> {
        let addr = pod_ip.socket_addr(port).to_string();
        let endpoint = node_api_endpoint(
            &addr,
            connect_timeout,
            crate::controllers::substrate::NODE_RPC_TIMEOUT,
        )
        .ok()?;
        crate::controllers::substrate::node_api_transport(endpoint)
            .await
            .ok()?
            .connect()
            .await
            .ok()
    }

    pub(super) async fn pod_proxy_accepts_tcp(
        pod_ip: PodIp,
        port: u16,
        connect_timeout: Duration,
    ) -> bool {
        let addr = pod_ip.socket_addr(port);
        matches!(
            timeout(connect_timeout, TcpStream::connect(addr)).await,
            Ok(Ok(_stream))
        )
    }

    pub(super) async fn pod_worker_internal_client(
        pod_ip: PodIp,
        port: u16,
        connect_timeout: Duration,
        require_split_receipt_capability: bool,
    ) -> Result<Option<crate::controllers::substrate::RuntimeInternalClient>, SubstrateError> {
        let Some(channel) = Self::pod_worker_channel(pod_ip, port, connect_timeout).await else {
            return Ok(None);
        };
        let policy_health_channel = crate::controllers::substrate::node_api_channel(
            channel.clone(),
            pplx_space_common_svc::bearer_token::SpaceBearerToken::NodeApiInternal,
        );
        crate::controllers::substrate::require_activation_policy_support(
            policy_health_channel,
            connect_timeout,
        )
        .await
        .map_err(|error| SubstrateError::Unavailable {
            message: error.to_string(),
        })?;
        if !require_split_receipt_capability {
            return Ok(Some(WorkerApiInternalClient::new(
                crate::controllers::substrate::node_api_channel(
                    channel,
                    pplx_space_common_svc::bearer_token::SpaceBearerToken::NodeApiInternal,
                ),
            )));
        }
        let health_channel = crate::controllers::substrate::node_api_channel(
            channel.clone(),
            pplx_space_common_svc::bearer_token::SpaceBearerToken::NodeApiInternal,
        );
        // This is the producer's proof that it reached a node-api which can
        // preserve the separate operation owner. Bound the local probe by the
        // same short budget as connection establishment; the surrounding Wake
        // phase has a much larger deadline and must not be consumed by a hung
        // health service before the actual lifecycle RPC is attempted.
        let response = match timeout(
            connect_timeout,
            tonic_health::pb::health_client::HealthClient::new(health_channel).check(
                tonic_health::pb::HealthCheckRequest {
                    service: pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE
                        .to_string(),
                },
            ),
        )
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                return Err(SubstrateError::Unavailable {
                    message: format!(
                        "worker node-api operation-receipt capability check failed: {error}"
                    ),
                });
            }
            Err(_) => {
                return Err(SubstrateError::Unavailable {
                    message: format!(
                        "worker node-api operation-receipt capability check exceeded {connect_timeout:?}"
                    ),
                });
            }
        };
        if response.into_inner().status
            != tonic_health::pb::health_check_response::ServingStatus::Serving as i32
        {
            return Err(SubstrateError::Unavailable {
                message: "worker node-api does not advertise split operation-receipt support"
                    .to_string(),
            });
        }
        Ok(Some(WorkerApiInternalClient::new(
            crate::controllers::substrate::node_api_channel(
                channel,
                pplx_space_common_svc::bearer_token::SpaceBearerToken::NodeApiInternal,
            ),
        )))
    }

    /// Resolve the dial address from an authoritative Kubernetes Pod GET, not
    /// from the execution row's cached IP. The complete route and immutable
    /// Pod UID fence both the lookup and the persistence CAS so a stale cached
    /// address or same-name replacement cannot receive node-api credentials.
    pub(super) async fn selected_wake_pod_ip(
        &self,
        sandbox_id: SandboxId,
        expected: &pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
    ) -> Result<Option<PodIp>, SubstrateError> {
        let execution = self
            .store
            .active_execution_status(sandbox_id)
            .await?
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "selected execution disappeared while waiting for Wake".to_string(),
            })?;
        let mut selected = k8s_execution_route_identity(&execution)?;
        // A previously unresolved node may be filled by placement publication.
        // This matches the Wake response's runtime-authoritative node contract.
        if expected.current_node_id.is_none() {
            selected.current_node_id = None;
        }
        if selected != *expected {
            return Err(SubstrateError::Unavailable {
                message: "selected route changed while waiting for Wake; retry against the current execution".to_string(),
            });
        }
        let pod_uid = execution
            .pod_uid
            .as_ref()
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "selected Kubernetes route lost its Pod UID".to_string(),
            })?;
        let route_activation_id = execution
            .route_activation_id
            .or(execution.activation_attempt_id)
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "selected Kubernetes route lost its activation identity".to_string(),
            })?;
        let pod = fetch_sandbox_pod_uncached(&self.k8s, &execution.pod_name)
            .await
            .map_err(|error| {
                kube_error_for_substrate("failed to resolve selected Wake pod", error)
            })?;
        let Some(pod) = pod else {
            return Ok(None);
        };
        let live_pod_uid = pod
            .metadata
            .uid
            .as_deref()
            .and_then(|uid| PodUid::parse(uid).ok())
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "selected Wake pod has no valid Kubernetes UID".to_string(),
            })?;
        if live_pod_uid != *pod_uid {
            return Err(SubstrateError::Unavailable {
                message: "selected Wake pod UID changed; refusing to dial a replacement pod"
                    .to_string(),
            });
        }
        if pod.metadata.deletion_timestamp.is_some()
            || terminal_pod_phase(pod.status.as_ref()).is_some()
        {
            // Never dial a stopped Pod, including while node-local suspend
            // finishes its backup. Only the exact selected execution may ask
            // its recovery owner to wait for that handoff and reselect.
            let generation = ExecutionGeneration::new(execution.generation).map_err(|error| {
                SubstrateError::Failed {
                    message: format!("invalid selected Wake execution generation: {error}"),
                }
            })?;
            if self
                .store
                .local_suspend_backup_pending(sandbox_id, generation, pod_uid)
                .await?
            {
                return Err(SubstrateError::suspend_in_flight(sandbox_id));
            }
            return Err(SubstrateError::Unavailable {
                message: "selected Wake pod is terminating or terminal; retry execution recovery"
                    .to_string(),
            });
        }
        let Some(pod_ip) = assigned_pod_ip(pod.status.as_ref())? else {
            return Ok(None);
        };
        let live_pod_ip = pod_ip.to_string();
        if execution.pod_ip.as_deref() != Some(live_pod_ip.as_str())
            && !self
                .store
                .persist_selected_route_pod_ip(
                    sandbox_id,
                    execution.generation,
                    &execution.pod_name,
                    route_activation_id,
                    pod_uid,
                    pod_ip,
                )
                .await?
        {
            return Err(SubstrateError::Unavailable {
                message: "selected route changed before its live pod IP could be persisted"
                    .to_string(),
            });
        }

        self.ensure_selected_wake_route_current(sandbox_id, expected, pod_ip)
            .await?;
        Ok(Some(pod_ip))
    }

    /// Re-fence cached Pod evidence before another side-effecting Wake. This
    /// is deliberately store-only: retrying against the same established
    /// connection does not need another uncached Kubernetes GET, but it must
    /// still stop if a concurrent operation superseded the selected route.
    async fn ensure_selected_wake_route_current(
        &self,
        sandbox_id: SandboxId,
        expected: &pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
        pod_ip: PodIp,
    ) -> Result<(), SubstrateError> {
        let current = self
            .store
            .active_execution_status(sandbox_id)
            .await?
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "selected execution disappeared before Wake".to_string(),
            })?;
        let mut current_route = k8s_execution_route_identity(&current)?;
        if expected.current_node_id.is_none() {
            current_route.current_node_id = None;
        }
        let selected_pod_ip = pod_ip.to_string();
        if current_route != *expected || current.pod_ip.as_deref() != Some(selected_pod_ip.as_str())
        {
            return Err(SubstrateError::Unavailable {
                message: "selected route changed before Wake; refusing the stale pod address"
                    .to_string(),
            });
        }
        Ok(())
    }

    /// Re-fence request-local Pod evidence before publishing readiness. This
    /// is intentionally a store-only check: the authoritative Kubernetes GET
    /// and Pod-UID check happened before the authenticated connection, while
    /// this read prevents a concurrent route replacement from producing a
    /// binding that mixes the predecessor's endpoint with the successor's
    /// generation and Pod name.
    pub(super) async fn selected_wake_binding(
        &self,
        sandbox_id: SandboxId,
        expected: &pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
        pod_ip: PodIp,
    ) -> Result<ExecutionBinding, SubstrateError> {
        let current = self
            .store
            .active_execution_status(sandbox_id)
            .await?
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "selected execution disappeared before readiness publication".to_string(),
            })?;
        let mut current_route = k8s_execution_route_identity(&current)?;
        if expected.current_node_id.is_none() {
            current_route.current_node_id = None;
        }
        let selected_pod_ip = pod_ip.to_string();
        if current_route != *expected || current.pod_ip.as_deref() != Some(selected_pod_ip.as_str())
        {
            return Err(SubstrateError::Unavailable {
                message: "selected route changed before readiness publication; retry against the current execution".to_string(),
            });
        }
        Ok(ExecutionBinding {
            execution_object_id: current.pod_name,
            runtime_endpoint: Self::endpoint_for_pod_ip(pod_ip, self.k8s.sandbox_proxy_port)?,
            execution_generation: execution_generation_to_u64(current.generation)?,
        })
    }

    /// Build a binding for a fence whose lifecycle operation already
    /// completed. A complete route keeps the live Pod/UID/store fences; a
    /// pre-route legacy row uses the same resolver fallback that existed
    /// before route identities were introduced.
    pub(super) async fn completed_execution_binding(
        &self,
        sandbox_id: SandboxId,
        execution: &ActiveExecutionStatusRow,
        route_identity: Option<&pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity>,
    ) -> Result<ExecutionBinding, SubstrateError> {
        if let Some(route_identity) = route_identity {
            let pod_ip = self
                .selected_wake_pod_ip(sandbox_id, route_identity)
                .await?
                .ok_or_else(|| SubstrateError::Unavailable {
                    message: "completed execution has no live Kubernetes pod IP".to_string(),
                })?;
            return self
                .selected_wake_binding(sandbox_id, route_identity, pod_ip)
                .await;
        }

        let pod_ip = match execution.pod_ip.as_deref() {
            Some(pod_ip) => PodIp::parse(pod_ip)?,
            None => self.resolve_pod_ip(sandbox_id).await?.ok_or_else(|| {
                SubstrateError::Unavailable {
                    message: "completed legacy execution has no pod IP".to_string(),
                }
            })?,
        };
        Ok(ExecutionBinding {
            execution_object_id: execution.pod_name.clone(),
            runtime_endpoint: Self::endpoint_for_pod_ip(pod_ip, self.k8s.sandbox_proxy_port)?,
            execution_generation: execution_generation_to_u64(execution.generation)?,
        })
    }

    /// Execute one already-classified activation action. Keeping this match in
    /// one place prevents individual lifecycle entry points from collapsing a
    /// completed fence into a joined operation or applying the split-receipt
    /// capability to B/B initialization.
    pub(super) async fn execute_activation_wake_action<F, D>(
        &self,
        sandbox_id: SandboxId,
        action: ActivationWakeAction,
        phase_deadline: Instant,
        build_request: F,
        on_dispatch: D,
    ) -> Result<
        (
            SelectedWakeOutcome,
            Option<pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity>,
        ),
        ActivationWakeFailure,
    >
    where
        F: FnOnce(
            &pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
            pplx_space_sbx_mgr_client::proto::AutoLifecyclePolicy,
        ) -> pplx_space_sbx_mgr_client::proto::WakeRequest,
        D: FnOnce(),
    {
        match action {
            ActivationWakeAction::SkipCompleted { route_identity } => Ok((
                SelectedWakeOutcome {
                    dispatch: WakeDispatchStatus::NotDispatched,
                    landed: false,
                    pod_ip: None,
                },
                route_identity,
            )),
            ActivationWakeAction::Execute {
                execution_generation,
                route_identity,
                policy,
                require_split_receipt_capability,
                publish_legacy_route_after_ack,
            } => {
                let activation_policy = self
                    .store
                    .auto_lifecycle_policy_after_claim(sandbox_id)
                    .await
                    .map_err(|error| ActivationWakeFailure {
                        error,
                        dispatch: WakeDispatchStatus::NotDispatched,
                    })?;
                let request = build_request(&route_identity, activation_policy.into());
                let outcome = self
                    .wake_selected_execution(
                        sandbox_id,
                        request,
                        &route_identity,
                        SelectedWakeSettings {
                            policy,
                            require_split_receipt_capability,
                            phase_deadline,
                        },
                        on_dispatch,
                    )
                    .await?;
                if let Some(route_activation_id) = publish_legacy_route_after_ack {
                    let expected_pod_uid =
                        route_identity
                            .pod_uid
                            .as_deref()
                            .ok_or_else(|| ActivationWakeFailure {
                                error: SubstrateError::Failed {
                                    message: "acknowledged legacy route has no Pod UID".to_string(),
                                },
                                dispatch: WakeDispatchStatus::MayHaveLanded,
                            })?;
                    self.store
                        .publish_legacy_route_identity_after_ack(
                            sandbox_id,
                            execution_generation,
                            route_activation_id,
                            expected_pod_uid,
                        )
                        .await
                        .map_err(|error| ActivationWakeFailure {
                            error,
                            dispatch: WakeDispatchStatus::MayHaveLanded,
                        })?;
                }
                Ok((outcome, Some(route_identity)))
            }
        }
    }

    /// Retry connection establishment and recoverable Wake results through
    /// the caller's readiness deadline. A pod can be materialized before its
    /// proxy or sandbox-manager runtime context is ready; neither condition is
    /// evidence that route-identity acknowledgement failed terminally.
    async fn wake_selected_execution<D>(
        &self,
        sandbox_id: SandboxId,
        request: pplx_space_sbx_mgr_client::proto::WakeRequest,
        expected_route_identity: &pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
        settings: SelectedWakeSettings,
        on_dispatch: D,
    ) -> Result<SelectedWakeOutcome, ActivationWakeFailure>
    where
        D: FnOnce(),
    {
        let SelectedWakeSettings {
            policy,
            require_split_receipt_capability,
            phase_deadline,
        } = settings;
        let probe = policy == WakeWaitPolicy::ResidentProbe;
        let deadline = if probe {
            phase_deadline.min(Instant::now() + RESIDENT_WAKE_PROBE_TIMEOUT)
        } else {
            phase_deadline
        };
        let mut last_pod_ip = None;
        let mut wake_may_have_landed = false;
        let mut last_pre_dispatch_error = None;
        let mut on_dispatch = Some(on_dispatch);
        let wait = async {
            let started_at = Instant::now();
            let mut selected_pod_ip = None;
            let mut connection: Option<(
                PodIp,
                crate::controllers::substrate::RuntimeInternalClient,
            )> = None;
            loop {
                let reused_selection = selected_pod_ip.is_some();
                let mut waiting_for_pod_ip = false;
                if selected_pod_ip.is_none() {
                    match self
                        .selected_wake_pod_ip(sandbox_id, expected_route_identity)
                        .await
                    {
                        Ok(pod_ip) => {
                            selected_pod_ip = pod_ip;
                            waiting_for_pod_ip = selected_pod_ip.is_none();
                        }
                        Err(SubstrateError::Timeout { message, .. }) => {
                            last_pre_dispatch_error = Some(message);
                            waiting_for_pod_ip = true;
                        }
                        Err(error) => return Err(error),
                    }
                }
                if let Some(pod_ip) = selected_pod_ip {
                    if reused_selection {
                        self.ensure_selected_wake_route_current(
                            sandbox_id,
                            expected_route_identity,
                            pod_ip,
                        )
                        .await?;
                    }
                    last_pod_ip = Some(pod_ip);
                    if connection
                        .as_ref()
                        .is_none_or(|(connected_ip, _)| *connected_ip != pod_ip)
                    {
                        connection = match Self::pod_worker_internal_client(
                            pod_ip,
                            self.k8s.sandbox_proxy_port,
                            START_WAIT_CONNECT_TIMEOUT,
                            require_split_receipt_capability,
                        )
                        .await
                        {
                            Ok(Some(client)) => {
                                last_pre_dispatch_error = None;
                                Some((pod_ip, client))
                            }
                            Ok(None) => {
                                last_pre_dispatch_error =
                                    Some("worker node-api TCP connection is not ready".to_string());
                                // A failed connection is the point at which a
                                // once-live address may have become stale.
                                selected_pod_ip = None;
                                waiting_for_pod_ip = true;
                                None
                            }
                            Err(error) if probe => {
                                tracing::warn!(
                                    sandbox_id = %sandbox_id,
                                    %error,
                                    "resident Wake skipped because required worker capabilities could not be verified"
                                );
                                return Ok(SelectedWakeOutcome {
                                    dispatch: WakeDispatchStatus::NotDispatched,
                                    landed: false,
                                    pod_ip: Some(pod_ip),
                                });
                            }
                            Err(error) => {
                                // The capability probe's channel is discarded,
                                // so the next attempt will reconnect. Re-run
                                // the live Pod UID/IP fence before sending
                                // internal credentials to that new channel.
                                last_pre_dispatch_error = Some(error.to_string());
                                selected_pod_ip = None;
                                waiting_for_pod_ip = true;
                                None
                            }
                        };
                    }
                    if let Some((_, client)) = connection.as_mut() {
                        // Once the RPC future is polled, transport failure does
                        // not prove the server skipped the request. Preserve
                        // durable lifecycle/operation evidence on deadline.
                        if let Some(on_dispatch) = on_dispatch.take() {
                            on_dispatch();
                        }
                        wake_may_have_landed = true;
                        match client.wake_sandbox(request.clone()).await {
                            Ok(response) => {
                                if let Err(error) = validate_wake_route_identity_if_present(
                                    response.get_ref(),
                                    &request.sandbox_id,
                                    expected_route_identity,
                                    policy.requires_route_identity(),
                                ) {
                                    // The server returned a successful Wake,
                                    // so lifecycle work may already be live
                                    // even though its route evidence is unusable.
                                    // Preserve the restoring/operation owner for
                                    // reconciliation instead of reporting a
                                    // clean pre-dispatch failure that callers
                                    // are allowed to roll back.
                                    return Err(SubstrateError::Timeout {
                                        message: format!(
                                            "Wake completed without usable route identity evidence: {error}"
                                        ),
                                        phase: None,
                                    });
                                }
                                return Ok(SelectedWakeOutcome {
                                    dispatch: WakeDispatchStatus::MayHaveLanded,
                                    landed: true,
                                    pod_ip: Some(pod_ip),
                                });
                            }
                            Err(error) => match classify_wake_rpc_error(&error) {
                                WakeRpcErrorDisposition::ReadinessPoll(_) if probe => {
                                    return Ok(SelectedWakeOutcome {
                                        dispatch: WakeDispatchStatus::MayHaveLanded,
                                        landed: false,
                                        pod_ip: Some(pod_ip),
                                    });
                                }
                                WakeRpcErrorDisposition::ReadinessPoll(_) => {}
                                WakeRpcErrorDisposition::Fail => {
                                    return Err(SubstrateError::Conflict {
                                        message: format!("sandbox wake failed: {error}"),
                                    });
                                }
                            },
                        }
                    }
                }

                if probe {
                    return Ok(SelectedWakeOutcome {
                        dispatch: WakeDispatchStatus::NotDispatched,
                        landed: false,
                        pod_ip: None,
                    });
                }
                // An authoritative GET is intentionally required before
                // credentials are sent, but an unassigned Pod IP does not
                // justify polling kube-apiserver four times per second.
                let delay = if waiting_for_pod_ip {
                    START_WAIT_SLOW_POLL_INTERVAL
                } else {
                    start_wait_poll_interval(started_at)
                };
                tokio::time::sleep(delay).await;
            }
        };
        // Bound selection, connect, RPC, and retries together. An optional
        // resident probe may time out, but an explicit rejection never falls back.
        let result = match tokio::time::timeout_at(deadline, wait).await {
            Ok(result) => result,
            Err(_) if probe => Ok(SelectedWakeOutcome {
                dispatch: if wake_may_have_landed {
                    WakeDispatchStatus::MayHaveLanded
                } else {
                    WakeDispatchStatus::NotDispatched
                },
                landed: false,
                pod_ip: last_pod_ip,
            }),
            Err(_) if wake_may_have_landed => Err(SubstrateError::Timeout {
                message: "required Wake did not complete within the readiness budget; sandbox-manager may still be completing the same activation".to_string(),
                phase: None,
            }),
            Err(_) => Err(SubstrateError::Unavailable {
                message: last_pre_dispatch_error.unwrap_or_else(|| {
                    "required Wake was not dispatched before the readiness deadline".to_string()
                }),
            }),
        };
        result.map_err(|error| ActivationWakeFailure {
            error,
            dispatch: if wake_may_have_landed {
                WakeDispatchStatus::MayHaveLanded
            } else {
                WakeDispatchStatus::NotDispatched
            },
        })
    }

    pub(super) async fn builder_node_client(&self) -> Result<BuilderClient, ControllerError> {
        self.builder_node_client_at("space-builder-node-api:9800")
            .await
    }

    pub(super) async fn builder_node_client_at(
        &self,
        addr: &str,
    ) -> Result<BuilderClient, ControllerError> {
        let endpoint = node_api_endpoint(
            addr,
            POD_WORKER_CONNECT_TIMEOUT,
            crate::controllers::substrate::NODE_RPC_TIMEOUT,
        )
        .map_err(ControllerError::from)?;
        let endpoint = crate::controllers::substrate::node_api_transport(endpoint).await?;
        let channel = pplx_space_common_grpc::connect_tcp_channel_with_retry(
            "builder-node-api",
            addr,
            endpoint,
            pplx_space_common_grpc::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
        )
        .await
        .map_err(|err| {
            ControllerError::service_unavailable(format!("builder node-api not available: {err}"))
        })?;
        Ok(BuilderClient::new(
            crate::controllers::substrate::node_api_channel(
                channel,
                pplx_space_common_svc::bearer_token::SpaceBearerToken::NodeApi,
            ),
        ))
    }

    /// `retry` states what the calling operation's retry would do; see
    /// [`PlacementRetryDisposition`]. It is a parameter because this helper
    /// serves both `create` (whose retry mints a new sandbox) and
    /// `start`/`wake`/`access` (whose retry converges).
    ///
    /// Two call sites pass a constant that is not their operation's:
    /// `resolve_pod_ip` and `wait_ready`'s `watch_until_pod_ip_assigned` are
    /// each reached from create readiness *and* from wake/access/delete, and
    /// both pass `RetryDuplicatesExecution`. That is the safe direction — it
    /// costs a converging caller a 500 where a 503 would have done, rather
    /// than risking a duplicate sandbox — but it does mean contention on those
    /// two paths is not retryable even for operations that converge. Relaxing
    /// either one requires threading the disposition down through it, not
    /// flipping the constant.
    pub(super) async fn remember_pod_node_placement(
        &self,
        sandbox_id: SandboxId,
        pod: &Pod,
        retry: PlacementRetryDisposition,
    ) -> Result<Option<NodeId>, SubstrateError> {
        let Some(node_name) = assigned_pod_node_name(pod) else {
            return Ok(None);
        };
        // Watcher-backed cache is the warm path. Fall back to a direct API
        // get when the reflector hasn't populated this Node yet — startup
        // races and slow API-server lists must not break placement that the
        // scheduler has already committed.
        let node = match self.k8s.caches.node_by_name(node_name) {
            Some(node) => node,
            None => {
                let nodes: Api<Node> = Api::all(self.k8s.client.clone());
                let fetched = nodes.get(node_name).await.map_err(|err| match err {
                    kube::Error::Api(resp) if resp.code == 404 => SubstrateError::Unavailable {
                        message: format!(
                            "Kubernetes node {node_name} no longer exists; cannot preserve node-local sandbox placement"
                        ),
                    },
                    other => SubstrateError::Failed {
                        message: format!("failed to query Kubernetes node {node_name}: {other}"),
                    },
                })?;
                std::sync::Arc::new(fetched)
            }
        };
        let node_uid = node
            .metadata
            .uid
            .as_deref()
            .ok_or_else(|| SubstrateError::Failed {
                message: format!("Kubernetes node {node_name} has no metadata.uid"),
            })?;
        let role = SpaceNodeRole::Worker;
        let node_id = space_k8s_node_id(node_name, role.as_str());
        let address = k8s_node_internal_ip(&node).ok_or_else(|| SubstrateError::Unavailable {
            message: format!(
                "Kubernetes node {node_name} has no InternalIP; cannot authenticate node-local reconciliation"
            ),
        })?;

        self.store
            .persist_pod_node_placement_after_pod_exists(
                PodNodePlacement {
                    sandbox_id,
                    node_name,
                    node_uid,
                    node_id,
                    role,
                    address: Some(address.as_str()),
                },
                retry,
            )
            .await?;
        Ok(Some(node_id))
    }

    pub(super) async fn delete_pod_and_forget_tracking(
        &self,
        sandbox_id: SandboxId,
        pod_name: &str,
        generation: i64,
        cause: PodTrackingCleanupCause,
    ) -> Result<(), SubstrateError> {
        match self
            .delete_sandbox_pod_for_projection_cleanup(pod_name)
            .await
        {
            Ok(K8sBlockingDeleteOutcome::Deleted | K8sBlockingDeleteOutcome::NotFound) => {}
            Ok(K8sBlockingDeleteOutcome::TimedOut) => {
                return Err(SubstrateError::Timeout {
                    message: format!(
                        "timed out waiting for terminal Kubernetes pod {pod_name} deletion; \
                         keeping execution tracking"
                    ),
                    phase: None,
                });
            }
            Ok(
                K8sBlockingDeleteOutcome::ObserveError
                | K8sBlockingDeleteOutcome::PreconditionFailed,
            ) => {
                return Err(SubstrateError::Unavailable {
                    message: format!(
                        "could not confirm terminal Kubernetes pod {pod_name} deletion; \
                         keeping execution tracking"
                    ),
                });
            }
            Err(err) => {
                return Err(SubstrateError::Failed {
                    message: format!("failed to delete terminal Kubernetes pod {pod_name}: {err}"),
                });
            }
        }
        self.store
            .forget_pod_tracking_if_generation(sandbox_id, generation, cause)
            .await?;
        Ok(())
    }

    /// Resolve this pod build's node-group candidate set, and pre-check a
    /// local-only restore's source-node pin against it.
    ///
    /// The candidate set is recomputed from the *current* template visibility
    /// and the **sandbox's own** owning org at every build — never replayed
    /// from a snapshot taken at create, and never derived from the actor who
    /// triggered the work: a restore or an evacuation is usually driven by a
    /// controller loop or a managing-org admin, and feeding their org into the
    /// compatibility predicate would reject the owner org's own template as
    /// cross-org.
    /// `pub(super)` for the sibling test module, which drives it end to end —
    /// the pre-flight below is a data-isolation check whose wiring nothing else
    /// would catch if it stopped being called.
    pub(super) async fn resolve_node_group_placement(
        &self,
        sandbox: &Sandbox,
        context: &PodCreateContext<'_>,
    ) -> Result<ResolvedPlacement, SubstrateError> {
        let resolved = self
            .node_group_placement
            .resolve(
                context.node_group_resolve_path(),
                PlacementSubject::Sandbox(sandbox.org_id),
                sandbox_template_name(sandbox),
            )
            .await?;
        // A pre-bound pod skips kube-scheduler, so the boundary is evaluated
        // only by kubelet admission and a rejection there is terminal for the
        // restore. Pre-check the source node's live label so the failure is an
        // actionable "migrate this sandbox" instead of a pod that lands, takes
        // a network slot, and dies. The reflector store is the read, not a
        // `GET /nodes/{name}`: this is a request path, and the reflector holds
        // the apiserver's Node object — the same object whose labels kubelet
        // admission will evaluate, which the mirrored `nodes.node_group_id`
        // column is only a lagging copy of.
        if let (Some(placement), Some(node_name)) =
            (resolved.placement(), context.direct_node_name())
        {
            let node = self.k8s.caches.node_by_name(node_name);
            let source = match node.as_deref() {
                Some(node) => RestoreSourceNode::Resolved(node),
                None if self.k8s.caches.nodes_ready() => RestoreSourceNode::Absent,
                None => RestoreSourceNode::CacheCold,
            };
            check_local_restore_source(placement, sandbox.id, node_name, source)?;
        }
        Ok(resolved)
    }

    /// [`Self::resolve_node_group_placement`] with the claim discipline
    /// `pod_create_context_for_claim` uses: a claim this call is about to
    /// leave unmaterialized is retired before the error propagates, so the
    /// next ensure re-creates instead of stalling behind the create-in-flight
    /// early return until the stale-create age expires.
    async fn resolve_node_group_placement_for_claim(
        &self,
        sandbox: &Sandbox,
        claim: &SandboxExecutionClaim,
        context: &PodCreateContext<'_>,
    ) -> Result<ResolvedPlacement, SubstrateError> {
        match self.resolve_node_group_placement(sandbox, context).await {
            Ok(resolved) => Ok(resolved),
            Err(err) => {
                if execution_pod_create_in_flight(claim.row.state) {
                    // The retirement's own failure must not replace `err`: the
                    // caller needs to know why the *pod build* failed, and a
                    // store error here only means the claim outlives this
                    // attempt (the stale-create age then clears it). Log both,
                    // return the one that explains the outcome.
                    match self
                        .store
                        .abandon_created_unmaterialized_create_claim(sandbox.id, claim)
                        .await
                    {
                        Ok(true) => tracing::warn!(
                            sandbox_id = %sandbox.id,
                            generation = claim.row.generation,
                            error = %err,
                            "retired unmaterialized Kubernetes pod create claim after node-group \
                             placement could not be resolved"
                        ),
                        Ok(false) => {}
                        Err(retire_error) => tracing::warn!(
                            sandbox_id = %sandbox.id,
                            generation = claim.row.generation,
                            error = %err,
                            %retire_error,
                            "could not retire the unmaterialized create claim after node-group \
                             placement failed; the claim clears at the stale-create age"
                        ),
                    }
                }
                Err(err)
            }
        }
    }

    /// `pub(super)` for the sibling test module, which renders each context's
    /// pod to assert the boundary actually reaches the PodSpec.
    pub(super) fn sandbox_pod_request<'a>(
        &self,
        sandbox: &'a Sandbox,
        execution: &'a SandboxExecutionRow,
        inputs: SandboxPodRequestInputs<'a>,
        pod_create_context: PodCreateContext<'a>,
    ) -> SandboxPodRequest<'a> {
        let SandboxPodRequestInputs {
            sandbox_owner,
            auto_lifecycle_json,
            network_policy_json,
            node_group_placement,
        } = inputs;
        let disk_io_limits = u64::try_from(sandbox.disk_io_max_operations_per_sec)
            .ok()
            .zip(u64::try_from(sandbox.disk_io_max_bandwidth_mib_per_sec).ok())
            .and_then(|(operations, bandwidth)| {
                pplx_space_common_core::types::DiskIoLimits::try_new(operations, bandwidth).ok()
            })
            .unwrap_or_else(|| {
                tracing::warn!(
                    sandbox_id = %sandbox.id,
                    operations_per_sec = sandbox.disk_io_max_operations_per_sec,
                    bandwidth_mib_per_sec = sandbox.disk_io_max_bandwidth_mib_per_sec,
                    "invalid persisted sandbox disk I/O limits; using platform defaults"
                );
                pplx_space_common_core::types::DiskIoLimits::default()
            });
        let runtime = SandboxRuntimePodMetadata {
            sandbox_owner,
            user_id: sandbox.user_id,
            org_id: sandbox.org_id,
            auto_lifecycle_json,
            auto_lifecycle_authority: PolicyAuthority::Cluster,
            network_policy_json,
            network_policy_generation: sandbox_effective_network_policy_generation(sandbox),
            network_policy_authority: PolicyAuthority::Cluster,
            vcpu: sandbox_vcpu_option(sandbox),
            memory_mib: sandbox_memory_mib_option(sandbox),
            disk_mib: sandbox_disk_mib_option(sandbox),
            disk_io_limits,
            volume_encryption: sandbox.volume_encryption,
        };
        let common = SandboxPodCommon {
            pod_name: &execution.pod_name,
            sandbox_id: sandbox.id,
            execution_generation: execution.generation,
            activation_attempt_id: execution.bootstrap_route_activation_id(),
            template: sandbox_template_name(sandbox),
            content_hash: sandbox.content_hash.as_deref(),
            env: &sandbox.env.0,
            traceparent: pplx_space_observability::trace_propagation::current_traceparent(),
        };

        // `node_group_placement` is resolved by the caller — the pod build is
        // synchronous and resolution is a database read — and is `None`
        // whenever the gate is off or the cluster has no generic node group,
        // which renders no node-group constraints at all. It is recomputed from
        // the current template visibility and the sandbox's owning org at every
        // build, never replayed from a snapshot taken at create time.
        match pod_create_context {
            // The reconcile/materialization path has no create request in hand,
            // and the pool pin is create-time-only (not persisted), so a pod
            // rebuilt here carries no pin.
            PodCreateContext::OnDemand => SandboxPodRequest::OnDemand {
                common,
                runtime,
                node_pool: None,
                node_group_placement,
            },
            PodCreateContext::Restore(RestoreContext::UserRestore {
                suspend_restore_storage_uri,
                suspend_restore_source_node,
                direct_node_name,
                preferred_node_name,
            }) => {
                // Source-aware shortlist, computed at request-build time so
                // every path that renders a source preference gets it: the
                // source plus schedulable fallbacks, keeping the whole set
                // under kube-scheduler's prefilter floor so the weight-100
                // preferred source term is guaranteed to be scored. Only for
                // portable restores (an S3 fallback must exist for a fallback
                // node to restore from); the re-drive successor passes
                // `preferred_node_name: None` and stays shortlist-free.
                let restore_node_shortlist = match preferred_node_name {
                    Some(source)
                        if self.k8s.sandbox_pod_config.restore_source_node_hard_pin
                            && suspend_restore_storage_uri.is_some() =>
                    {
                        self.restore_node_shortlist(sandbox.id, source, node_group_placement)
                    }
                    _ => Vec::new(),
                };
                SandboxPodRequest::Restore {
                    artifact_kind: if sandbox.lifecycle.parse::<SandboxLifecycle>()
                        == Ok(SandboxLifecycle::Stopped)
                    {
                        pplx_space_common_core::types::ArtifactKind::Disk
                    } else {
                        pplx_space_common_core::types::ArtifactKind::Full
                    },
                    common,
                    runtime,
                    // Rendered alongside `direct_node_name` when the restore is
                    // local-only. The pin stands: the caller has already
                    // pre-checked the source node's live label against this
                    // set, and dropping the pin is not available — the field is
                    // populated only when there is no portable snapshot, so any
                    // other node in the set has neither the local bytes nor a
                    // storage URI to restore from.
                    node_group_placement,
                    suspend_restore_storage_uri,
                    suspend_restore_source_node,
                    fork_disk_restore: None,
                    direct_node_name,
                    preferred_node_name,
                    restore_node_shortlist,
                    extra_tolerations: &[],
                    extra_required_affinity: None,
                }
            }
            PodCreateContext::Restore(RestoreContext::Evacuation {
                suspend_restore_storage_uri,
                extra_tolerations,
                extra_required_affinity,
            }) => SandboxPodRequest::Restore {
                artifact_kind: if sandbox.lifecycle.parse::<SandboxLifecycle>()
                    == Ok(SandboxLifecycle::Stopped)
                {
                    pplx_space_common_core::types::ArtifactKind::Disk
                } else {
                    pplx_space_common_core::types::ArtifactKind::Full
                },
                common,
                runtime,
                node_group_placement,
                suspend_restore_storage_uri,
                suspend_restore_source_node: None,
                fork_disk_restore: None,
                direct_node_name: None,
                preferred_node_name: None,
                restore_node_shortlist: Vec::new(),
                extra_tolerations,
                extra_required_affinity,
            },
        }
    }

    async fn pod_create_context_for_claim<'a>(
        &self,
        sandbox: &'a Sandbox,
        claim: &SandboxExecutionClaim,
        evacuation_context: Option<EvacuationContext<'a>>,
        suspend_restore_source_node: Option<&'a str>,
        suspend_restore_storage_uri: Option<&'a str>,
    ) -> Result<PodCreateContext<'a>, SubstrateError> {
        let sandbox_id = sandbox.id;
        match parse_pod_create_context(
            sandbox_id,
            claim.row.generation,
            &sandbox.lifecycle,
            self.store.prior_state_exists(sandbox_id).await?,
            evacuation_context,
            suspend_restore_source_node,
            suspend_restore_storage_uri,
        ) {
            Ok(context) => Ok(context),
            Err(err) => {
                if execution_pod_create_in_flight(claim.row.state) {
                    let retired = self
                        .store
                        .abandon_created_unmaterialized_create_claim(sandbox.id, claim)
                        .await?;
                    if retired {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            generation = claim.row.generation,
                            lifecycle = %sandbox.lifecycle,
                            "retired invalid Kubernetes pod create claim"
                        );
                    }
                }
                Err(err)
            }
        }
    }

    pub(super) async fn require_observed_create_claim_in_flight(
        &self,
        sandbox: &Sandbox,
        claim: &SandboxExecutionClaim,
    ) -> Result<(), SubstrateError> {
        let sandbox_id = sandbox.id;
        if self
            .store
            .active_execution_is_generation(sandbox_id, claim.row.generation)
            .await?
        {
            return Ok(());
        }

        tracing::warn!(
            sandbox_id = %sandbox.id,
            generation = claim.row.generation,
            pod_name = %claim.row.pod_name,
            "observed Kubernetes pod create claim was replaced before pod creation"
        );
        Err(SubstrateError::Timeout {
            message: format!(
                "observed Kubernetes pod create claim for sandbox {} generation {} was replaced \
                 before pod creation; retry the sandbox operation to converge",
                sandbox.id, claim.row.generation
            ),
            phase: None,
        })
    }

    /// Read-only counterpart to the repairing resolver. Missing, terminal, or
    /// replaced pods belong to recovery; a reachability probe cannot delete
    /// them or change execution tracking while its request can be cancelled.
    pub(super) async fn resolve_reachable_pod_ip(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<PodIp>, SubstrateError> {
        let Some(row) = self.store.active_execution_status(sandbox_id).await? else {
            return Ok(None);
        };
        if is_suspended_lifecycle(&row.lifecycle) || execution_pod_lookup_in_flight(row.state) {
            return Ok(None);
        }
        let Some(pod) = self
            .read_sandbox_pod(&row.pod_name)
            .await
            .map_err(|error| {
                kube_error_for_substrate("failed to read reachable sandbox pod", error)
            })?
        else {
            return Ok(None);
        };
        if pod.metadata.deletion_timestamp.is_some()
            || terminal_pod_phase(pod.status.as_ref()).is_some()
            || pod_unschedulable_message(pod.status.as_ref()).is_some()
            || row.pod_uid.is_none()
            || row.pod_uid.as_ref().map(PodUid::as_str) != pod.metadata.uid.as_deref()
        {
            return Ok(None);
        }
        let pod_ip = assigned_pod_ip(pod.status.as_ref())?;
        // Let the owned path publish changed routing evidence before returning
        // a client; otherwise ingress would repeatedly resolve stale metadata.
        if row.pod_ip != pod_ip.map(|ip| ip.to_string())
            || row.k8s_node_name.as_deref() != assigned_pod_node_name(&pod)
        {
            return Ok(None);
        }
        Ok(pod_ip)
    }

    pub(super) async fn resolve_pod_ip(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<PodIp>, SubstrateError> {
        let Some(row) = self.store.active_execution_status(sandbox_id).await? else {
            return Ok(None);
        };
        if is_suspended_lifecycle(&row.lifecycle) {
            return Ok(None);
        }

        let pod = if execution_pod_lookup_in_flight(row.state) {
            if !stale_restore_execution_create(&row.lifecycle, row.state, row.create_age_expired) {
                return Ok(None);
            }
            fetch_sandbox_pod_uncached(&self.k8s, &row.pod_name)
                .await
                .map_err(|err| {
                    kube_error_for_substrate("failed to verify stale creating restore pod", err)
                })?
        } else {
            self.read_sandbox_pod(&row.pod_name)
                .await
                .map_err(|err| kube_error_for_substrate("failed to query K8s pod", err))?
        };

        let pod = match pod {
            Some(pod) => pod,
            None => {
                // The filtered reflector store excludes terminal pods. During
                // suspend that creates a cache miss before a finalizer-blocked
                // Kubernetes object is actually absent, so a cache miss alone
                // must never authorize execution retirement.
                match fetch_sandbox_pod_uncached(&self.k8s, &row.pod_name)
                    .await
                    .map_err(|err| {
                        kube_error_for_substrate(
                            "failed to verify missing Kubernetes sandbox pod",
                            err,
                        )
                    })? {
                    Some(pod) => pod,
                    None => {
                        if row.state == ActiveExecutionState::RecoveryFailed {
                            match self
                                .store
                                .converge_failed_recovery_after_pod_absence_if_generation(
                                    sandbox_id,
                                    row.generation,
                                    row.pod_uid.as_ref().map(PodUid::as_str),
                                    row.pod_ip.as_deref(),
                                    row.k8s_node_name.as_deref(),
                                )
                                .await?
                            {
                                FailedRecoveryPodAbsenceOutcome::NotMatched => {}
                                FailedRecoveryPodAbsenceOutcome::ClaimPreserved => tracing::debug!(
                                    sandbox_id = %sandbox_id,
                                    generation = row.generation,
                                    pod_name = %row.pod_name,
                                    "failed recovery pod is absent; clearing cached route and preserving execution claim"
                                ),
                                FailedRecoveryPodAbsenceOutcome::ConvergedSuspended => {
                                    tracing::info!(
                                        sandbox_id = %sandbox_id,
                                        generation = row.generation,
                                        pod_name = %row.pod_name,
                                        "failed recovery pod is absent; converged current durable snapshot to suspended"
                                    )
                                }
                            }
                            return Ok(None);
                        }
                        if row.lifecycle == SandboxLifecycle::Suspending.as_str()
                            && let Some(pod_uid) = row.pod_uid.as_ref()
                            && self
                                .store
                                .finalize_missing_suspending_pod_with_current_snapshot(
                                    sandbox_id,
                                    row.generation,
                                    pod_uid.as_str(),
                                )
                                .await?
                        {
                            tracing::info!(
                                sandbox_id = %sandbox_id,
                                generation = row.generation,
                                pod_name = %row.pod_name,
                                "finalized suspended sandbox after its pod disappeared with a current durable snapshot"
                            );
                            return Ok(None);
                        }
                        tracing::warn!(
                            sandbox_id = %sandbox_id,
                            pod_name = %row.pod_name,
                            cached_pod_ip = ?row.pod_ip,
                            "tracked Kubernetes sandbox pod is authoritatively absent; clearing cached pod IP"
                        );
                        self.store
                            .forget_pod_tracking_if_generation(
                                sandbox_id,
                                row.generation,
                                PodTrackingCleanupCause::MissingPodObservation,
                            )
                            .await?;
                        return Ok(None);
                    }
                }
            }
        };

        let pod = if let Some(cached_phase) = terminal_pod_phase(pod.status.as_ref()) {
            let authoritative_pod = fetch_sandbox_pod_uncached(&self.k8s, &row.pod_name)
                .await
                .map_err(|err| {
                    kube_error_for_substrate(
                        "failed to verify terminal Kubernetes sandbox pod",
                        err,
                    )
                })?;
            if let Some(phase) = terminal_cleanup_phase_after_uncached_check(
                cached_phase,
                authoritative_pod.as_ref(),
            ) {
                let publication_grace_remaining = authoritative_pod
                    .as_ref()
                    .filter(|_| phase == TerminalPodPhase::Succeeded)
                    .and_then(|pod| {
                        succeeded_pod_publication_grace_remaining(pod, jiff::Timestamp::now())
                    });
                if row.lifecycle == SandboxLifecycle::Suspending.as_str()
                    && publication_grace_remaining.is_some()
                {
                    tracing::info!(
                        sandbox_id = %sandbox_id,
                        pod_name = %row.pod_name,
                        generation = row.generation,
                        pod_uid = ?row.pod_uid,
                        lifecycle = %row.lifecycle,
                        grace_remaining_ms = publication_grace_remaining
                            .map(|remaining| remaining.as_millis()),
                        "deferring successful terminal pod cleanup while suspend publication may still arrive"
                    );
                    return Err(SubstrateError::suspend_in_flight(sandbox_id));
                }
                tracing::info!(
                    sandbox_id = %sandbox_id,
                    pod_name = %row.pod_name,
                    cached_phase = %cached_phase,
                    phase = %phase,
                    cached_pod_ip = ?row.pod_ip,
                    "tracked Kubernetes sandbox pod is terminal after uncached verification; deleting it and clearing cached pod IP"
                );
                self.delete_pod_and_forget_tracking(
                    sandbox_id,
                    &row.pod_name,
                    row.generation,
                    PodTrackingCleanupCause::MissingPodObservation,
                )
                .await?;
                return Ok(None);
            }
            tracing::info!(
                sandbox_id = %sandbox_id,
                pod_name = %row.pod_name,
                cached_phase = %cached_phase,
                cached_pod_ip = ?row.pod_ip,
                "cached Kubernetes sandbox pod was terminal but uncached pod is still active; keeping tracking"
            );
            authoritative_pod.expect("non-terminal verification result requires an existing pod")
        } else {
            pod
        };

        if let Some(message) = pod_unschedulable_message(pod.status.as_ref()) {
            return Err(SubstrateError::Unavailable {
                message: format!("sandbox pod is unschedulable: {message}"),
            });
        }

        let current_node_name = assigned_pod_node_name(&pod).map(str::to_owned);
        if row.k8s_node_name.as_deref() != current_node_name.as_deref() {
            let _ = self
                .remember_pod_node_placement(
                    sandbox_id,
                    &pod,
                    PlacementRetryDisposition::RetryDuplicatesExecution,
                )
                .await?;
        }

        let pod_uid = pod.metadata.uid.clone();
        let pod_ip = assigned_pod_ip(pod.status.as_ref())?;
        let pod_ip_string = pod_ip.map(|ip| ip.to_string());
        if row.pod_uid.as_ref().map(PodUid::as_str) != pod_uid.as_deref()
            || row.pod_ip.as_deref() != pod_ip_string.as_deref()
        {
            match self
                .store
                .persist_pod_metadata(
                    sandbox_id,
                    row.generation,
                    pod_uid.as_deref(),
                    pod_ip_string.as_deref(),
                )
                .await
            {
                Ok(Some(_)) => {}
                Ok(None) => tracing::warn!(
                    sandbox_id = %sandbox_id,
                    generation = row.generation,
                    "pod metadata write no-oped after pod lookup: the execution was \
                     retired or replaced mid-flight"
                ),
                Err(err) => tracing::warn!(
                    sandbox_id = %sandbox_id,
                    generation = row.generation,
                    error = %err,
                    "failed to persist Kubernetes pod metadata after pod lookup"
                ),
            }
        }

        Ok(pod_ip)
    }

    async fn stale_pod_reason_for_execution(
        &self,
        sandbox: &Sandbox,
        claim: &SandboxExecutionClaim,
        pod: &Pod,
        suspend_restore_storage_uri: Option<&str>,
    ) -> Result<Option<StalePodReason>, SubstrateError> {
        // Pod phase and restore annotations do not identify a STOP episode.
        // A Running successor may have booted successfully before its owner
        // lost the lease or response; retain it so START can finish publication.
        if sandbox.lifecycle.parse::<SandboxLifecycle>() == Ok(SandboxLifecycle::Stopped)
            && self
                .store
                .execution_predates_current_stop(sandbox.id, claim.row.generation)
                .await?
        {
            return Ok(Some(StalePodReason::SandboxStopped));
        }
        Ok(stale_existing_pod_reason(
            sandbox,
            pod,
            suspend_restore_storage_uri,
        ))
    }

    async fn cleanup_stale_existing_pod_after_create_error(
        &self,
        sandbox: &Sandbox,
        claim: &SandboxExecutionClaim,
        existing: &Pod,
        suspend_restore_storage_uri: Option<&str>,
    ) -> Result<bool, SubstrateError> {
        let Some(reason) = self
            .stale_pod_reason_for_execution(sandbox, claim, existing, suspend_restore_storage_uri)
            .await?
        else {
            return Ok(false);
        };
        tracing::info!(
            sandbox_id = %sandbox.id,
            pod_name = %claim.row.pod_name,
            generation = claim.row.generation,
            reason = %reason,
            "pod create recovery found stale Kubernetes sandbox pod; replacing it"
        );
        if let Err(err) = delete_sandbox_pod_with_throttle_retry(
            &self.k8s,
            &claim.row.pod_name,
            "delete_stale_sandbox_pod_after_create_error",
        )
        .await
            && !is_not_found_error(&err)
        {
            return Err(kube_error_for_substrate(
                "failed to delete stale sandbox pod",
                err,
            ));
        }
        let _ = self
            .store
            .cleanup_replaced_pod_tracking_if_generation(sandbox.id, claim.row.generation)
            .await?;
        Ok(true)
    }

    pub(super) async fn ensure_pod_exists(
        &self,
        sandbox: &Sandbox,
        evacuation_placement: Option<Option<&EvacuationPlacement>>,
        suspend_restore_source_node: Option<&str>,
        suspend_restore_storage_uri: Option<&str>,
        ownership: PodMaterializationOwnership,
    ) -> Result<(), SubstrateError> {
        let PodMaterializationOwnership {
            started_recovery,
            restore_lease_token,
            activation_attempt,
        } = ownership;
        let started_recovery_owner_generation =
            started_recovery.map(StartedRecoveryOwnership::generation);
        let sandbox_id = sandbox.id;
        let requested_lifecycle = sandbox
            .lifecycle
            .parse::<SandboxLifecycle>()
            .map_err(|err| SubstrateError::Failed {
                message: format!("failed to parse sandbox pod lifecycle: {err}"),
            })?;
        let sandbox = self.store.require_sandbox(sandbox_id).await?;
        let materialization_lifecycle =
            sandbox
                .lifecycle
                .parse::<SandboxLifecycle>()
                .map_err(|err| SubstrateError::Failed {
                    message: format!("failed to parse refreshed sandbox pod lifecycle: {err}"),
                })?;
        if materialization_lifecycle == SandboxLifecycle::Suspending {
            return Err(SubstrateError::suspend_in_flight(sandbox_id));
        }
        let lifecycle_is_materializable = matches!(
            materialization_lifecycle,
            SandboxLifecycle::Created
                | SandboxLifecycle::Creating
                | SandboxLifecycle::Starting
                | SandboxLifecycle::Started
                | SandboxLifecycle::Restoring
                | SandboxLifecycle::Pausing
                | SandboxLifecycle::Paused
                | SandboxLifecycle::Resuming
                | SandboxLifecycle::Suspended
                | SandboxLifecycle::Checkpointing
        ) || (requested_lifecycle == SandboxLifecycle::Stopped
            && materialization_lifecycle == SandboxLifecycle::Stopped);
        let lifecycle_refresh_is_valid = requested_lifecycle == materialization_lifecycle
            || (requested_lifecycle == SandboxLifecycle::Started
                && materialization_lifecycle == SandboxLifecycle::Restoring);
        if !lifecycle_is_materializable || !lifecycle_refresh_is_valid {
            return Err(SubstrateError::Conflict {
                message: format!(
                    "sandbox cannot materialize an execution after lifecycle changed from \
                     {requested_lifecycle} to {materialization_lifecycle}"
                ),
            });
        }

        let claim_lifecycle = if started_recovery_owner_generation.is_some() {
            SandboxLifecycle::Started
        } else {
            materialization_lifecycle
        };
        let mut claim = self
            .store
            .ensure_active_execution_claim_for_lifecycle(
                sandbox_id,
                claim_lifecycle,
                restore_lease_token,
                activation_attempt
                    .as_ref()
                    .map(ActivationAttemptAssignment::request),
            )
            .await?;
        if let Some(activation_attempt) = activation_attempt.as_ref()
            && let Some(assigned_id) = claim.row.activation_attempt_id
        {
            activation_attempt.record_assigned(assigned_id, claim.row.generation);
        }
        let owns_started_recovery = started_recovery_owner_generation
            .is_some_and(|generation| claim.row.generation >= generation);
        let started_recovery_created_execution = started_recovery.is_some_and(|ownership| {
            ownership.created_execution() && ownership.generation() == claim.row.generation
        });
        if claim.row.state == ActiveExecutionState::Recovering && !owns_started_recovery {
            return Ok(());
        }
        let selected_plan = match if suspend_restore_source_node.is_none()
            && suspend_restore_storage_uri.is_none()
        {
            match normalize_lifecycle(&sandbox.lifecycle).as_str() {
                "suspended" | "restoring" => self
                    .suspend_restore_plan(&sandbox, evacuation_placement.is_some())
                    .await
                    .map(Some),
                "stopped" => self.stopped_restore_plan(&sandbox).await.map(Some),
                _ => Ok(None),
            }
        } else {
            Ok(None)
        } {
            Ok(plan) => plan,
            Err(err) => {
                if claim.created && execution_pod_create_in_flight(claim.row.state) {
                    let _ = self
                        .store
                        .abandon_created_unmaterialized_create_claim(sandbox.id, &claim)
                        .await?;
                }
                return Err(err);
            }
        };
        let started_recovery_restore_context = if owns_started_recovery
            && suspend_restore_source_node.is_none()
            && suspend_restore_storage_uri.is_none()
        {
            let generation = ExecutionGeneration::new(claim.row.generation).map_err(|err| {
                SubstrateError::Failed {
                    message: format!(
                        "invalid started recovery execution generation {}: {err}",
                        claim.row.generation
                    ),
                }
            })?;
            Some(
                self.store
                    .started_recovery_restore_context(sandbox_id, generation)
                    .await?,
            )
        } else {
            None
        };
        let mut started_recovery_restore_source = None;
        let (started_recovery_remote_storage_uri, started_recovery_has_restore_context) =
            match started_recovery_restore_context.as_ref() {
                Some(StartedRecoveryRestoreContext::Ready(
                    StartedRecoveryRestoreSource::Remote { storage_uri },
                )) => (Some(storage_uri.clone()), true),
                Some(StartedRecoveryRestoreContext::Ready(
                    StartedRecoveryRestoreSource::Local { source_node_name },
                )) => {
                    if started_recovery_created_execution {
                        started_recovery_restore_source =
                            Some(StartedRecoveryRestoreSource::Local {
                                source_node_name: source_node_name.clone(),
                            });
                    }
                    (None, true)
                }
                Some(StartedRecoveryRestoreContext::Pending) => (None, true),
                Some(StartedRecoveryRestoreContext::Unavailable) | None => (None, false),
            };
        let started_recovery_waits_for_restore_source =
            matches!(
                started_recovery_restore_context,
                Some(StartedRecoveryRestoreContext::Pending)
            ) || (matches!(
                started_recovery_restore_context,
                Some(StartedRecoveryRestoreContext::Unavailable)
            ) && self.store.prior_state_exists(sandbox_id).await?);
        let suspend_restore_source_node = suspend_restore_source_node.or(selected_plan
            .as_ref()
            .and_then(|plan| plan.source_node_name.as_deref()));
        let suspend_restore_storage_uri = suspend_restore_storage_uri
            .or(selected_plan
                .as_ref()
                .and_then(|plan| plan.storage_uri.as_deref()))
            .or_else(|| {
                started_recovery_remote_storage_uri
                    .as_ref()
                    .map(|storage_uri| storage_uri.as_wire())
            });

        // Materialize the evacuation toleration up front so it lives long
        // enough for `SandboxPodRequest::Restore`'s borrowed slots. The
        // outer `Option` flags "this is the evacuation path"; the inner
        // `Option` is whether a target cohort is active (a two-MNG
        // rollout in flight). With no rollout, evacuation still happens —
        // we just drop the source-node pin and emit no extra placement.
        let evacuation_toleration_slot: Option<[PodToleration; 1]> =
            evacuation_placement.and_then(|placement| placement.map(|p| [p.to_toleration()]));
        let evacuation_context = evacuation_placement.map(|placement| EvacuationContext {
            extra_tolerations: evacuation_toleration_slot
                .as_ref()
                .map(|slot| slot.as_slice())
                .unwrap_or(&[]),
            extra_required_affinity: placement.map(EvacuationPlacement::to_required_affinity),
        });

        let mut pod_name = claim.row.pod_name.clone();
        // Interrupted source-pinned restore recovery. A prior attempt can create
        // this generation's pinned pod and persist it — moving the row to
        // `created` (pod_uid set, pod_ip NULL) or leaving it `creating` — then be
        // interrupted (crash / cancel / watch error) before its in-request watch
        // re-drives. The fresh-create watch only runs on the request that
        // creates the pod, so without this a retry would reuse the stuck pod and
        // strand the restore on an unavailable source across retries.
        //
        // Detection derives entirely from evidence on the live pod — the
        // required pin in its spec — never from the current
        // `restore_source_node_hard_pin` config: a flag rollback must not
        // strand pods a previous deployment pinned. Gated on a portable
        // snapshot so the successor has a source to restore from;
        // `pod_has_required_source_pin` is false once a prior retry already
        // re-drove cross-node, so this converges without looping.
        if !claim.created && suspend_restore_storage_uri.is_some() {
            let key = ObjectRef::<Pod>::new(&pod_name).within(&self.k8s.sandbox_namespace);
            let existing_pod: Option<Pod> = match self.pod_store.get(&key) {
                Some(existing) => Some((*existing).clone()),
                // Cache miss on a *persisted* claim (`created`: pod_uid set,
                // pod_ip NULL): the pod exists on the apiserver, but a retry can
                // land on a replica whose reflector hasn't observed it yet.
                // Skipping recovery here would send the retry down the
                // POST-conflict path, which reuses the pinned pod without ever
                // running the scheduling watch (`created` is false there). One
                // authoritative GET on this rare shape closes that. A `creating`
                // claim stays cache-only: its POST may never have happened, so
                // the GET would usually 404, and the create-in-flight early
                // return below is the designed answer while the cache warms.
                None if !execution_pod_create_in_flight(claim.row.state) => {
                    fetch_sandbox_pod_uncached(&self.k8s, &pod_name)
                        .await
                        .map_err(|err| {
                            kube_error_for_substrate(
                                "failed to verify source-pinned restore pod",
                                err,
                            )
                        })?
                }
                None => None,
            };
            // An *unpinned* pod matching this execution under a still-`creating`
            // claim is a successor an interrupted re-drive created but never
            // persisted. Hydrate it now — without this the create-in-flight
            // early return below would leave the row unmaterialized until the
            // stale-create age expires. An interrupted *direct-node*
            // (`spec.nodeName`, local-only) pod takes this path too, and that
            // is deliberate: hydrating flips the row to `created`, which is
            // exactly what unlocks the cache-hit stale-pod classification
            // (`LocalOnlyRestoreNowPortable` -> replace) on the next attempt;
            // skipping it would strand that pod behind the create-in-flight
            // return for the full stale-create age instead.
            if let Some(existing) = existing_pod
                .as_ref()
                .filter(|existing| !pod_has_required_source_pin(existing))
                .filter(|existing| {
                    execution_pod_create_in_flight(claim.row.state)
                        && pod_matches_execution_generation(
                            existing,
                            sandbox.id,
                            claim.row.generation,
                        )
                })
            {
                self.remember_pod_node_placement(
                    sandbox.id,
                    existing,
                    PlacementRetryDisposition::RetryConverges,
                )
                .await?;
                let pod_ip = assigned_pod_ip(existing.status.as_ref())?.map(|ip| ip.to_string());
                if self
                    .store
                    .persist_pod_metadata(
                        sandbox.id,
                        claim.row.generation,
                        existing.metadata.uid.as_deref(),
                        pod_ip.as_deref(),
                    )
                    .await?
                    .is_none()
                {
                    tracing::warn!(
                        sandbox_id = %sandbox.id,
                        generation = claim.row.generation,
                        "successor hydration no-oped: the claimed execution was retired \
                         or replaced mid-flight"
                    );
                }
                return Ok(());
            }
            let pin_state = existing_pod.as_ref().and_then(|existing| {
                pod_has_required_source_pin(existing).then(|| scheduling_outcome_of(existing))
            });
            if let Some(cached_outcome) = pin_state {
                // Trust a settled cached decision; otherwise re-observe with the
                // bounded watch (which ends in an authoritative apiserver GET) —
                // the original request may have crashed before the Unschedulable
                // condition ever reached this replica's reflector, and skipping
                // recovery on a lagging cache would burn a full readiness
                // timeout per retry until it catches up. A pod that is
                // genuinely still undecided falls through unharmed: only an
                // explicit Unschedulable verdict re-drives.
                let outcome = match cached_outcome {
                    Some(outcome) => Some(outcome),
                    None => {
                        Box::pin(self.await_scheduling_outcome(&pod_name, SCHEDULING_WATCH_BUDGET))
                            .await?
                    }
                };
                if outcome == Some(SchedulingOutcome::Unschedulable) {
                    // Boxed so this already-huge async fn's stack frame embeds
                    // only a pointer, not the re-drive sub-future. The re-drive
                    // itself re-verifies the pod with an authoritative GET
                    // before any teardown.
                    match Box::pin(self.redrive_unschedulable_restore_cross_node(
                        &sandbox,
                        &claim,
                        claim_lifecycle,
                        suspend_restore_source_node,
                        suspend_restore_storage_uri,
                        restore_lease_token,
                    ))
                    .await?
                    {
                        RedriveOutcome::Redriven(cross_node_pod) => {
                            self.remember_pod_node_placement(
                                sandbox.id,
                                &cross_node_pod,
                                PlacementRetryDisposition::RetryConverges,
                            )
                            .await?;
                            // The interrupted original never reached the metric
                            // emission for the pinned pod, so this retry's
                            // successful fallback would otherwise be invisible
                            // to the cross-host-share signal. The successor's
                            // source annotation says whether the source still
                            // held trusted local bytes.
                            let local_snapshot_present = cross_node_pod
                                .metadata
                                .annotations
                                .as_ref()
                                .and_then(|annotations| {
                                    annotations.get(SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION)
                                })
                                .is_some_and(|value| !value.trim().is_empty());
                            pplx_space_metrics::sandbox::restore::placement_directed(
                                pplx_space_metrics::sandbox::restore::PlacementSample {
                                    placement: pplx_space_metrics::sandbox::restore::RestorePlacement::CrossNode,
                                    local_snapshot_present,
                                    pin: pplx_space_metrics::sandbox::restore::SourcePin::None,
                                },
                            );
                            return Ok(());
                        }
                        RedriveOutcome::OwnershipLost => {
                            return Ok(());
                        }
                        // The pin bound after all (our verdict was stale) or a
                        // concurrent re-drive already replaced it — fall through
                        // to the normal existing-pod handling. No metric here:
                        // whichever request created the live pod emits for it.
                        RedriveOutcome::PodRetained { .. } => {}
                    }
                }
                // Scheduled: the pin landed after all — fall through to the
                // normal existing-pod handling below.
            }
        }
        if !claim.created && execution_pod_create_in_flight(claim.row.state) {
            if !stale_restore_execution_create(
                &sandbox.lifecycle,
                claim.row.state,
                claim.row.create_age_expired,
            ) {
                let pod_create_context = self
                    .pod_create_context_for_claim(
                        &sandbox,
                        &claim,
                        evacuation_context,
                        suspend_restore_source_node,
                        suspend_restore_storage_uri,
                    )
                    .await?;
                if !claim.row.create_age_expired
                    || !matches!(pod_create_context, PodCreateContext::OnDemand)
                {
                    self.require_observed_create_claim_in_flight(&sandbox, &claim)
                        .await?;
                    return Ok(());
                }
            }

            let existing = fetch_sandbox_pod_uncached(&self.k8s, &pod_name)
                .await
                .map_err(|err| {
                    kube_error_for_substrate("failed to verify stale creating pod", err)
                })?;
            let replacement_reason = if let Some(existing) = existing.as_ref() {
                if let Some(reason) = self
                    .stale_pod_reason_for_execution(
                        &sandbox,
                        &claim,
                        existing,
                        suspend_restore_storage_uri,
                    )
                    .await?
                {
                    tracing::info!(
                        sandbox_id = %sandbox.id,
                        pod_name = %pod_name,
                        generation = claim.row.generation,
                        reason = %reason,
                        "replacing stale creating Kubernetes pod"
                    );
                    if let Err(err) = delete_sandbox_pod_with_throttle_retry(
                        &self.k8s,
                        &pod_name,
                        "delete_stale_create_pod",
                    )
                    .await
                        && !is_not_found_error(&err)
                    {
                        return Err(kube_error_for_substrate(
                            "failed to delete stale create pod",
                            err,
                        ));
                    }
                    reason.to_string()
                } else {
                    self.remember_pod_node_placement(
                        sandbox.id,
                        existing,
                        PlacementRetryDisposition::RetryConverges,
                    )
                    .await?;
                    let pod_ip =
                        assigned_pod_ip(existing.status.as_ref())?.map(|ip| ip.to_string());
                    if self
                        .store
                        .persist_pod_metadata(
                            sandbox.id,
                            claim.row.generation,
                            existing.metadata.uid.as_deref(),
                            pod_ip.as_deref(),
                        )
                        .await?
                        .is_none()
                    {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            generation = claim.row.generation,
                            "pod metadata write no-oped: the claimed execution was retired \
                             or replaced mid-flight"
                        );
                    }
                    return Ok(());
                }
            } else {
                tracing::warn!(
                    sandbox_id = %sandbox.id,
                    pod_name = %pod_name,
                    generation = claim.row.generation,
                    "stale creating Kubernetes execution has no live pod; replacing generation"
                );
                "missing".to_string()
            };

            let replaced_generation = claim.row.generation;
            let replaced_pod_name = pod_name.clone();
            let Some(replacement_claim) = self
                .store
                .replace_active_execution_claim_if_generation(
                    sandbox_id,
                    replaced_generation,
                    "replaced",
                    activation_attempt
                        .as_ref()
                        .map(ActivationAttemptAssignment::request),
                )
                .await?
            else {
                return Ok(());
            };
            SandboxEvent::new(
                sandbox_id,
                SandboxEventType::ExecutionReplaced,
                SandboxEventSource::Substrate,
            )
            .emit_with(
                SandboxEventDetails::default()
                    .operation("ensure_pod_exists")
                    .reason(replacement_reason)
                    .lifecycle_state(sandbox.lifecycle.as_str())
                    .execution_generation(replaced_generation.to_string())
                    .pod_name(replaced_pod_name),
            );
            claim = replacement_claim;
            if let Some(activation_attempt) = activation_attempt.as_ref()
                && let Some(assigned_id) = claim.row.activation_attempt_id
            {
                activation_attempt.record_assigned(assigned_id, claim.row.generation);
            }
            pod_name = claim.row.pod_name.clone();
        }

        let started_recovery_requires_handoff = owns_started_recovery
            && (started_recovery_waits_for_restore_source
                || (!started_recovery_created_execution
                    && (started_recovery_has_restore_context
                        || suspend_restore_source_node.is_some()
                        || suspend_restore_storage_uri.is_some())));

        // Cheap cache check first. Recovery with prior state confirms cache
        // misses because a missing tracked pod requires an atomic restore-source
        // handoff before creating its successor. Other cache misses proceed to
        // POST, where the 409-recovery branch confirms conflicting pods.
        if self.lookup_sandbox_pod(&pod_name).is_some() || started_recovery_requires_handoff {
            // The reflector cache is eventually consistent, so an evicted or
            // force-deleted pod could still appear alive, while a recently
            // created pod could be absent. Confirm with an authoritative GET.
            let existing = fetch_sandbox_pod_uncached(&self.k8s, &pod_name)
                .await
                .map_err(|err| {
                    kube_error_for_substrate("failed to verify cached sandbox pod", err)
                })?;
            if let Some(existing) = existing {
                if let Some(reason) = self
                    .stale_pod_reason_for_execution(
                        &sandbox,
                        &claim,
                        &existing,
                        suspend_restore_storage_uri,
                    )
                    .await?
                {
                    if started_recovery_requires_handoff
                        && let Some(pod_uid) = existing
                            .metadata
                            .uid
                            .as_deref()
                            .and_then(|uid| PodUid::parse(uid).ok())
                    {
                        let generation =
                            ExecutionGeneration::new(claim.row.generation).map_err(|error| {
                                SubstrateError::Failed {
                                    message: format!(
                                        "invalid started recovery execution generation: {error}"
                                    ),
                                }
                            })?;
                        if self
                            .store
                            .local_suspend_backup_pending(sandbox_id, generation, &pod_uid)
                            .await?
                        {
                            return Err(SubstrateError::suspend_in_flight(sandbox_id));
                        }
                    }
                    tracing::info!(
                        sandbox_id = %sandbox.id,
                        pod_name = %pod_name,
                        generation = claim.row.generation,
                        reason = %reason,
                        "replacing stale Kubernetes sandbox execution pod"
                    );
                    if let Err(err) = delete_sandbox_pod_with_throttle_retry(
                        &self.k8s,
                        &pod_name,
                        "delete_stale_sandbox_pod",
                    )
                    .await
                        && !is_not_found_error(&err)
                    {
                        return Err(kube_error_for_substrate(
                            "failed to delete stale sandbox pod",
                            err,
                        ));
                    }
                    if started_recovery_requires_handoff {
                        if !self
                            .replace_started_recovery_pod(
                                &sandbox,
                                &mut claim,
                                &mut pod_name,
                                &mut started_recovery_restore_source,
                                StartedRecoveryPodReplacement {
                                    reason: reason.to_string(),
                                    restore_context: started_recovery_restore_context.clone(),
                                    explicit_source_node: suspend_restore_source_node,
                                    explicit_storage_uri: suspend_restore_storage_uri,
                                    activation_assignment: activation_attempt.as_ref(),
                                },
                            )
                            .await?
                        {
                            return Ok(());
                        }
                    } else {
                        let replaced_generation = claim.row.generation;
                        let replaced_pod_name = pod_name.clone();
                        let Some(replacement_claim) = self
                            .store
                            .replace_active_execution_claim_if_generation(
                                sandbox_id,
                                replaced_generation,
                                "replaced",
                                activation_attempt
                                    .as_ref()
                                    .map(ActivationAttemptAssignment::request),
                            )
                            .await?
                        else {
                            return Ok(());
                        };
                        let replacement_event_details = started_recovery_replacement_event_details(
                            SandboxEventDetails::default()
                                .operation("ensure_pod_exists")
                                .reason(reason.to_string())
                                .lifecycle_state(sandbox.lifecycle.as_str())
                                .execution_generation(replaced_generation.to_string())
                                .pod_name(replaced_pod_name),
                            suspend_restore_source_node,
                            suspend_restore_storage_uri,
                            None,
                        );
                        SandboxEvent::new(
                            sandbox_id,
                            SandboxEventType::ExecutionReplaced,
                            SandboxEventSource::Substrate,
                        )
                        .emit_with(replacement_event_details);
                        claim = replacement_claim;
                        if let Some(activation_attempt) = activation_attempt.as_ref()
                            && let Some(assigned_id) = claim.row.activation_attempt_id
                        {
                            activation_attempt.record_assigned(assigned_id, claim.row.generation);
                        }
                    }
                } else {
                    let _ = self
                        .remember_pod_node_placement(
                            sandbox.id,
                            &existing,
                            PlacementRetryDisposition::RetryConverges,
                        )
                        .await?;
                    let pod_ip =
                        assigned_pod_ip(existing.status.as_ref())?.map(|ip| ip.to_string());
                    if self
                        .store
                        .persist_pod_metadata(
                            sandbox.id,
                            claim.row.generation,
                            existing.metadata.uid.as_deref(),
                            pod_ip.as_deref(),
                        )
                        .await?
                        .is_none()
                    {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            generation = claim.row.generation,
                            "pod metadata write no-oped: the claimed execution was retired \
                             or replaced mid-flight"
                        );
                    }
                    return Ok(());
                }
            } else if started_recovery_requires_handoff
                && !self
                    .replace_started_recovery_pod(
                        &sandbox,
                        &mut claim,
                        &mut pod_name,
                        &mut started_recovery_restore_source,
                        StartedRecoveryPodReplacement {
                            reason: "missing".to_string(),
                            restore_context: started_recovery_restore_context.clone(),
                            explicit_source_node: suspend_restore_source_node,
                            explicit_storage_uri: suspend_restore_storage_uri,
                            activation_assignment: activation_attempt.as_ref(),
                        },
                    )
                    .await?
            {
                return Ok(());
            }
        }

        let (suspend_restore_source_node, suspend_restore_storage_uri) =
            if suspend_restore_source_node.is_some() || suspend_restore_storage_uri.is_some() {
                (suspend_restore_source_node, suspend_restore_storage_uri)
            } else {
                match started_recovery_restore_source.as_ref() {
                    Some(StartedRecoveryRestoreSource::Local { source_node_name }) => {
                        (Some(source_node_name.as_str()), None)
                    }
                    Some(StartedRecoveryRestoreSource::Remote { storage_uri }) => {
                        (None, Some(storage_uri.as_wire()))
                    }
                    None => (None, None),
                }
            };

        if !claim.created && execution_pod_create_in_flight(claim.row.state) {
            self.require_observed_create_claim_in_flight(&sandbox, &claim)
                .await?;
            return Ok(());
        }
        if !self
            .store
            .active_execution_is_generation(sandbox_id, claim.row.generation)
            .await?
        {
            tracing::debug!(
                sandbox_id = %sandbox.id,
                pod_name = %claim.row.pod_name,
                generation = claim.row.generation,
                "skipping Kubernetes sandbox pod create for replaced execution generation"
            );
            return Ok(());
        }

        let pod_create_context = self
            .pod_create_context_for_claim(
                &sandbox,
                &claim,
                evacuation_context,
                suspend_restore_source_node,
                suspend_restore_storage_uri,
            )
            .await?;
        // Snapshot the placement decision before the context is consumed by
        // `sandbox_pod_request`; emitted once below on a confirmed fresh create,
        // after any in-request cross-node re-drive so it reflects the landing site.
        let placement_sample = restore_placement_sample(
            &pod_create_context,
            self.k8s.sandbox_pod_config.restore_source_node_hard_pin,
        );
        // Resolved before the prior-generation sweep and before any pod is
        // POSTed, so an unplaceable sandbox fails without leaving a pod behind —
        // and, for a local-only restore, before its source-node pin is
        // rendered. `pod_create_context` is consumed by `sandbox_pod_request`
        // further down, so this reads it while it is still in hand.
        //
        // The execution claim is already outstanding at this point — it is
        // bound upstream — which is exactly why this goes through the
        // claim-retiring wrapper rather than resolving directly.
        let node_group_placement = self
            .resolve_node_group_placement_for_claim(&sandbox, &claim, &pod_create_context)
            .await?;

        let auto_lifecycle = match self
            .store
            .auto_lifecycle_policy_after_claim(sandbox.id)
            .await
        {
            Ok(policy) => policy,
            Err(error) => {
                if claim.created && execution_pod_create_in_flight(claim.row.state) {
                    self.store
                        .abandon_created_unmaterialized_create_claim(sandbox.id, &claim)
                        .await?;
                }
                return Err(error);
            }
        };
        let auto_lifecycle_json =
            serde_json::to_string(&auto_lifecycle).map_err(|err| SubstrateError::Failed {
                message: format!("failed to serialize auto lifecycle policy: {err}"),
            })?;
        let network_policy = self.effective_network_policy_for_sandbox(&sandbox).await?;
        let network_policy_json =
            serde_json::to_string(&network_policy).map_err(|err| SubstrateError::Failed {
                message: format!("failed to serialize network policy: {err}"),
            })?;
        ensure_network_policy_annotation_size(&network_policy_json)?;
        let sandbox_owner = self.store.sandbox_owner_for_user(sandbox.user_id);

        // Sweep of lingering older-generation pods before a restore create — a
        // create prerequisite, not best-effort. An interrupted re-drive can
        // strand a source-pinned pod that no request ever deleted (the
        // transient-lifecycle sweeper reverts the sandbox row but never touches
        // pods); if the source later frees capacity, kube-scheduler binds that
        // stale pod and the shim starts an orphan VM for a sandbox that may
        // already be `suspended`. So each such pod must be confirmed **gone**
        // before the successor is materialized. Only pods from generations
        // **strictly older** than this claim are selected (never by name
        // exclusion alone): a concurrent request that superseded this claim has
        // a >= generation and is never touched. Deleting a strictly-older-
        // generation pod is always safe — it must not run.
        //
        // Fail-closed: an unconfirmed delete (timeout / observe-error / delete
        // error) retires this freshly-created claim and returns retryable, so
        // no successor is POSTed while an older pod might still be running, and
        // the retry re-runs the sweep instead of no-oping on the create-in-
        // flight early return. A cold pod-reflector cache cannot enumerate
        // prior-generation pods, so it takes the same retryable deferral rather
        // than materializing a successor unswept: `/readyz` gates external
        // traffic on the reflector, but that indirect invariant does not cover
        // direct/internal callers or a readiness-routing mistake, so the create
        // itself refuses until the cache is warm.
        if suspend_restore_source_node.is_some() || suspend_restore_storage_uri.is_some() {
            if !self.pod_reflector_ready.is_ready() {
                if claim.created && execution_pod_create_in_flight(claim.row.state) {
                    let _ = self
                        .store
                        .abandon_created_unmaterialized_create_claim(sandbox.id, &claim)
                        .await?;
                }
                return Err(SubstrateError::Unavailable {
                    message: format!(
                        "deferring restore: pod reflector cache not ready to verify \
                         lingering prior-generation pods for sandbox {}; retry",
                        sandbox.id
                    ),
                });
            }
            let lingering = lingering_sandbox_pod_names(
                self.pod_store.state().iter().map(|pod| pod.as_ref()),
                sandbox.id,
                claim.row.generation,
            );
            for stale_pod in lingering {
                match self.delete_lingering_prior_generation_pod(&stale_pod).await {
                    Ok(K8sBlockingDeleteOutcome::Deleted | K8sBlockingDeleteOutcome::NotFound) => {
                        tracing::info!(
                            sandbox_id = %sandbox.id,
                            stale_pod = %stale_pod,
                            successor_pod = %pod_name,
                            "confirmed lingering prior-generation restore pod gone before create"
                        );
                    }
                    outcome => {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            stale_pod = %stale_pod,
                            ?outcome,
                            "deferring restore: lingering prior-generation pod not confirmed \
                             gone; retry"
                        );
                        if claim.created && execution_pod_create_in_flight(claim.row.state) {
                            let _ = self
                                .store
                                .abandon_created_unmaterialized_create_claim(sandbox.id, &claim)
                                .await?;
                        }
                        return Err(SubstrateError::Unavailable {
                            message: format!(
                                "deferring restore: lingering prior-generation pod {stale_pod} \
                                 for sandbox {} not confirmed gone; retry",
                                sandbox.id
                            ),
                        });
                    }
                }
            }
        }

        if !self
            .store
            .claim_active_execution_pod_materialization(
                sandbox.id,
                claim.row.generation,
                claim_lifecycle,
            )
            .await?
        {
            tracing::debug!(
                sandbox_id = %sandbox.id,
                pod_name = %claim.row.pod_name,
                generation = claim.row.generation,
                "skipping Kubernetes sandbox pod create for replaced execution generation"
            );
            return Ok(());
        }

        let pod_result = create_sandbox_pod(
            &self.k8s,
            self.sandbox_pod_request(
                &sandbox,
                &claim.row,
                SandboxPodRequestInputs {
                    sandbox_owner: &sandbox_owner,
                    auto_lifecycle_json: &auto_lifecycle_json,
                    network_policy_json: &network_policy_json,
                    node_group_placement: node_group_placement.placement(),
                },
                pod_create_context,
            ),
        )
        .await;
        let (pod, created) = match pod_result {
            Ok(pod) => (pod, true),
            Err(err) if matches!(&err, kube::Error::Api(resp) if resp.code == 409) => {
                let execution = &claim.row;
                let pod_name = execution.pod_name.clone();
                // Reflector may not have observed the conflicting pod yet —
                // fall back to a direct kube apiserver GET so this rare
                // recovery path always sees authoritative state.
                let existing = match fetch_sandbox_pod_uncached(&self.k8s, &pod_name).await {
                    Ok(Some(existing)) => existing,
                    Ok(None) => {
                        return Err(SubstrateError::Failed {
                            message: format!(
                                "sandbox pod create conflicted but pod {pod_name} could not be loaded"
                            ),
                        });
                    }
                    Err(query_error) => {
                        return Err(SubstrateError::restore_in_flight(format!(
                            "sandbox pod create conflicted, but Kubernetes could not confirm pod \
                             {pod_name}: {query_error}"
                        )));
                    }
                };
                if self
                    .cleanup_stale_existing_pod_after_create_error(
                        &sandbox,
                        &claim,
                        &existing,
                        suspend_restore_storage_uri,
                    )
                    .await?
                {
                    return Ok(());
                }
                (existing, false)
            }
            Err(err) => {
                let create_error = format!("failed to create sandbox pod: {err}");
                match fetch_sandbox_pod_uncached(&self.k8s, &claim.row.pod_name).await {
                    Ok(Some(existing)) => {
                        if self
                            .cleanup_stale_existing_pod_after_create_error(
                                &sandbox,
                                &claim,
                                &existing,
                                suspend_restore_storage_uri,
                            )
                            .await?
                        {
                            return Ok(());
                        }
                        (existing, false)
                    }
                    Ok(None) => {
                        let _ = self
                            .store
                            .abandon_created_unmaterialized_create_claim(sandbox.id, &claim)
                            .await?;
                        return Err(SubstrateError::Failed {
                            message: create_error,
                        });
                    }
                    Err(query_error) => {
                        let message = format!(
                            "{create_error}; could not verify whether Kubernetes committed it: \
                             {query_error}"
                        );
                        // A failed create followed by a failed authoritative
                        // read is not a terminal failure: the apiserver may
                        // have committed the Pod before returning the error.
                        // Preserve started-recovery ownership so its failure
                        // publisher cannot roll the sandbox back while that
                        // Pod may be live.
                        return Err(SubstrateError::restore_in_flight(message));
                    }
                }
            }
        };

        let pod_ip = assigned_pod_ip(pod.status.as_ref())?.map(|ip| ip.to_string());
        match self
            .store
            .persist_materialized_pod_metadata(
                sandbox.id,
                claim.row.generation,
                claim_lifecycle,
                pod.metadata.uid.as_deref(),
                pod_ip.as_deref(),
            )
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => {
                if created {
                    tracing::info!(
                        sandbox_id = %sandbox.id,
                        pod_name = %claim.row.pod_name,
                        generation = claim.row.generation,
                        "deleting Kubernetes sandbox pod created after materialization ownership was revoked"
                    );
                    self.delete_pod_and_forget_tracking(
                        sandbox.id,
                        &claim.row.pod_name,
                        claim.row.generation,
                        PodTrackingCleanupCause::ExplicitTeardown,
                    )
                    .await?;
                }
                return Ok(());
            }
            Err(error) => {
                if !created {
                    return Err(SubstrateError::restore_in_flight(format!(
                        "{error}; existing Kubernetes pod {} may still be live because its \
                         materialization metadata was not persisted",
                        claim.row.pod_name
                    )));
                }
                let Some(pod_uid) = pod.metadata.uid.as_deref() else {
                    return Err(SubstrateError::restore_in_flight(format!(
                        "{error}; created Kubernetes pod {} had no UID, so its absence could not \
                         be confirmed",
                        claim.row.pod_name
                    )));
                };
                let pod_uid = PodUid::parse(pod_uid).map_err(|uid_error| {
                    SubstrateError::restore_in_flight(format!(
                        "{error}; created Kubernetes pod {} had an invalid UID, so its absence \
                         could not be confirmed: {uid_error}",
                        claim.row.pod_name
                    ))
                })?;
                match self
                    .k8s_blocking_delete_sandbox_pod_for_operation(
                        &claim.row.pod_name,
                        "materialization_persistence_failure",
                        Some((pod_uid.as_str(), None)),
                    )
                    .await
                {
                    Ok(K8sBlockingDeleteOutcome::Deleted | K8sBlockingDeleteOutcome::NotFound) => {
                        return Err(error);
                    }
                    Ok(delete_outcome) => {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            pod_name = %claim.row.pod_name,
                            generation = claim.row.generation,
                            delete_outcome = delete_outcome.as_str(),
                            "Kubernetes sandbox pod absence remained unconfirmed after materialization persistence failed"
                        );
                    }
                    Err(delete_error) => {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            pod_name = %claim.row.pod_name,
                            generation = claim.row.generation,
                            error = %delete_error,
                            "failed to confirm Kubernetes sandbox pod deletion after materialization persistence failed"
                        );
                    }
                }
                return Err(SubstrateError::restore_in_flight(format!(
                    "{error}; Kubernetes pod {} deletion was not confirmed",
                    claim.row.pod_name
                )));
            }
        }

        // Only `Ok(Some(_))` reaches here — materialization is committed and we
        // own the generation. `Ok(None)` / `Err` returned above.
        use pplx_space_metrics::sandbox::restore::{RestorePlacement, SourcePin};
        let mut placement_pod = pod;
        let mut placement_sample = placement_sample;

        // In-request source-pinned restore fallback.
        //
        // The required source-node pin (behind `restore_source_node_hard_pin`)
        // can leave the pod `Unschedulable` when the source is full, gone, or
        // disk-pressured. Observe the scheduler's first decision synchronously
        // and, if the source cannot take it, delete the pod and re-drive
        // cross-node in this same request.
        //
        // This needs no durable marker and no reconciler: an `Unschedulable`
        // pod is inert — never bound, no `nodeName`, no IP, no VM, no network
        // slot — so deleting it can never orphan a running workload, and a
        // crash before the re-drive commits simply replays pin -> watch ->
        // re-drive on the next attempt (the pin is re-derived from the same
        // inputs). Because the decision is re-made per attempt, a client retry
        // never races a half-torn-down pin the way an async reconciler would.
        //
        // Gate on the placement sample the classifier already produced: a
        // `RequiredAffinity` pin is exactly the source-pinned-portable case,
        // so the watch trigger, the rendered pod spec, and the emitted metric
        // all derive from `restore_placement_sample` and cannot drift apart.
        let rendered_required_source_pin = matches!(
            placement_sample.map(|sample| sample.pin),
            Some(SourcePin::RequiredAffinity)
        );
        if created && rendered_required_source_pin {
            match Box::pin(
                self.await_scheduling_outcome(&claim.row.pod_name, SCHEDULING_WATCH_BUDGET),
            )
            .await?
            {
                // Every listed node (bare pin: the source; shortlist: source +
                // fallbacks) failed filtering — tear down and re-drive
                // cross-node. Boxed so this already-large async fn's stack
                // frame embeds only a pointer; the re-drive re-verifies the pod
                // with an authoritative GET before any teardown.
                Some(SchedulingOutcome::Unschedulable) => {
                    match Box::pin(self.redrive_unschedulable_restore_cross_node(
                        &sandbox,
                        &claim,
                        claim_lifecycle,
                        suspend_restore_source_node,
                        suspend_restore_storage_uri,
                        restore_lease_token,
                    ))
                    .await?
                    {
                        RedriveOutcome::Redriven(cross_node_pod) => {
                            // Preserve `local_snapshot_present` (the source
                            // still held local bytes) so the metric
                            // distinguishes "cross-node because the source
                            // rejected it" from "cross-node because no source
                            // was known".
                            if let Some(sample) = placement_sample.as_mut() {
                                sample.placement = RestorePlacement::CrossNode;
                                sample.pin = SourcePin::None;
                            }
                            placement_pod = *cross_node_pod;
                        }
                        // Nothing torn down. A retained cross-node successor
                        // (installed by a concurrent re-drive) must still be
                        // recorded as cross-node — this request created the
                        // pinned pod, so it is the one that emits below. A
                        // retained bound-on-source pod keeps the same-node
                        // sample.
                        RedriveOutcome::PodRetained { cross_node } => {
                            if cross_node && let Some(sample) = placement_sample.as_mut() {
                                sample.placement = RestorePlacement::CrossNode;
                                sample.pin = SourcePin::None;
                            }
                        }
                        // Materialization ownership was lost mid-flight; the
                        // winner drives.
                        RedriveOutcome::OwnershipLost => {
                            return Ok(());
                        }
                    }
                }
                // Bound: record the actual landing. Under the shortlist the
                // scheduler may place the pod on a fallback in the same pass
                // (source infeasible but a listed node fit) — that is a
                // cross-node landing even though the pin/preference targeted
                // the source. `pin` stays `RequiredAffinity`: cross_node +
                // required_affinity is exactly the "shortlist fallback"
                // signature.
                Some(SchedulingOutcome::Scheduled { node_name }) => {
                    if Some(node_name.as_str()) != suspend_restore_source_node
                        && let Some(sample) = placement_sample.as_mut()
                    {
                        sample.placement = RestorePlacement::CrossNode;
                    }
                }
                // Undecided after the budget: proceed to the readiness wait —
                // only an explicit Unschedulable verdict justifies teardown.
                // But the landing node is unknown, and the pod may still bind
                // to a shortlist fallback. Recording the rendered `same_node`
                // sample would mis-count exactly the cross-node placements this
                // metric measures; the request can't block on a later bind
                // (latency budget), so emit an explicit `unknown` placement the
                // rollout share query excludes. `pin` stays `RequiredAffinity`.
                None => {
                    if let Some(sample) = placement_sample.as_mut() {
                        sample.placement = RestorePlacement::Unknown;
                    }
                }
            }
        }

        // Record the placement decision exactly once — only for a freshly
        // created restore pod (the 409 "found existing" arm sets
        // `created = false`) whose materialization ownership was just confirmed,
        // and after any in-request cross-node re-drive so the metric reflects
        // the actual landing site.
        if created && let Some(sample) = placement_sample {
            pplx_space_metrics::sandbox::restore::placement_directed(sample);
        }

        let _ = self
            .remember_pod_node_placement(
                sandbox.id,
                &placement_pod,
                PlacementRetryDisposition::RetryConverges,
            )
            .await?;
        Ok(())
    }
}
