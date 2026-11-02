use super::pod_ops::{ActivationWakeFailure, WakeDispatchStatus};
use super::support::{
    activation_wake_action, ready_wait_bounds, remaining_ready_window, wake_request_route_identity,
};
use super::wait_ready::CommandReadyPod;
use super::*;
use crate::controllers::substrate::runtime_upgrade::UpgradeActivation;
use crate::controllers::substrate::{ActivationOptions, RecoveryCharge};
use pplx_space_common_core::kubernetes::EvacuationPlacement;
use pplx_space_common_core::types::{
    SandboxActivationAttemptId, SandboxLifecycle, SandboxTransitionTargetState,
};
use pplx_space_common_work_coalescer::{
    JoinError as CoalesceJoinError, Role as CoalesceRole, WorkOutcome,
};
use pplx_space_events::{
    SandboxEvent, SandboxEventBoolField, SandboxEventDetails, SandboxEventSource, SandboxEventType,
};
use pplx_space_metrics::sandbox::api::{RestoreClaimLap, WakeCoalesceOutcome};
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
};

/// Backoff shared by both follower joins (started recovery and the user
/// restore lease): first poll after the initial delay, doubling to the max.
const RECOVERY_JOIN_INITIAL_DELAY: Duration = Duration::from_millis(100);
const RECOVERY_JOIN_MAX_DELAY: Duration = Duration::from_secs(5);
/// How much longer a coalesced-wake caller waits on the shared run beyond its
/// own readiness budget. The pod-leader's run is internally bounded by the
/// same budget, so at the boundary the caller's watch-wait races the run's
/// own deadline; without this grace the race replaces the run's typed error
/// (e.g. `RestoreInFlight` with a lease-derived retry hint) with a generic
/// budget-elapsed error. The grace only covers publish/scheduling latency —
/// the run itself never runs longer because of it.
const COALESCED_RUN_PUBLISH_GRACE: Duration = Duration::from_millis(500);
/// How long a deferring caller keeps retrying a fenced restore claim inside
/// its slot before handing the wait back.
pub(crate) const RETRY_DEFERRAL_GRACE: Duration = Duration::from_secs(2);

/// The follower-join backoff's two bounds, carried on the substrate so a
/// test can shorten a join's re-check without touching the wake path it is
/// exercising. Every production substrate takes [`Default`], which is
/// [`RECOVERY_JOIN_INITIAL_DELAY`] doubling to [`RECOVERY_JOIN_MAX_DELAY`].
#[derive(Clone, Copy, Debug)]
pub(super) struct RecoveryJoinBackoffPolicy {
    initial: Duration,
    max: Duration,
}

impl Default for RecoveryJoinBackoffPolicy {
    fn default() -> Self {
        Self {
            initial: RECOVERY_JOIN_INITIAL_DELAY,
            max: RECOVERY_JOIN_MAX_DELAY,
        }
    }
}

impl RecoveryJoinBackoffPolicy {
    #[cfg(test)]
    pub(super) fn with_bounds(initial: Duration, max: Duration) -> Self {
        Self { initial, max }
    }
}

/// User-restore follower poll delay, owned by the caller's whole
/// wake/restore attempt: scoping it to a single join would reset it on
/// every `Claimable` rejoin and let a losing herd lap the claim at the
/// initial delay indefinitely.
struct RecoveryJoinBackoff {
    delay: Duration,
    max: Duration,
}

impl RecoveryJoinBackoff {
    fn new(policy: RecoveryJoinBackoffPolicy) -> Self {
        Self {
            delay: policy.initial,
            max: policy.max,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.delay;
        self.delay = delay.saturating_mul(2).min(self.max);
        delay
    }
}

/// Outcome of a bounded follower join on the user restore lease.
enum UserRestoreJoin {
    /// The lease owner completed the restore with a routable execution.
    Completed(Box<ExecutionBinding>),
    /// The lease lapsed without a routable outcome; the caller may retry its
    /// own claim.
    Claimable,
}

enum StartedRecoveryJoin {
    Completed(Box<ExecutionBinding>),
    SupersededBySuspend(Box<Sandbox>),
}

enum StartedRecoveryClaimOutcome {
    Owned(StartedRecoveryOwnership),
    Joined(Box<ExecutionBinding>),
    SupersededBySuspend {
        sandbox: Box<Sandbox>,
        retired_generation: Option<i64>,
    },
}

pub(super) fn should_join_started_recovery(
    lifecycle: SandboxLifecycle,
    target_state: SandboxLifecycle,
) -> bool {
    lifecycle == SandboxLifecycle::Started && target_state == SandboxLifecycle::Started
}

fn parse_sandbox_lifecycle(lifecycle: &str) -> Result<SandboxLifecycle, SubstrateError> {
    normalize_lifecycle(lifecycle)
        .parse::<SandboxLifecycle>()
        .map_err(|err| SubstrateError::Conflict {
            message: format!("invalid sandbox lifecycle for recovery: {err}"),
        })
}

/// Map a `SandboxLifecycle` target into the proto `WakeTargetState`
/// wire enum that sbx-mgr's `Wake` and `Start` accept. Only `Started`
/// and `Paused` are valid restore targets today; anything else is a
/// caller bug (and the substrate-trait `restore_execution` enforces
/// `Started` at its boundary, so the user-API path is unaffected).
fn sandbox_lifecycle_to_wake_target_proto(
    lifecycle: SandboxLifecycle,
) -> Result<i32, SubstrateError> {
    use pplx_space_sbx_mgr_client::proto::WakeTargetState;
    match restore_transition_target_state(lifecycle)? {
        SandboxTransitionTargetState::Started => Ok(WakeTargetState::Started as i32),
        SandboxTransitionTargetState::Paused => Ok(WakeTargetState::Paused as i32),
        other => Err(SubstrateError::Conflict {
            message: format!("restore target {other:?} has no wake target state"),
        }),
    }
}

/// The activation-attempt target a restore to `lifecycle` records, and the one
/// definition of which restore targets exist.
/// [`sandbox_lifecycle_to_wake_target_proto`] maps through it, and
/// [`KubernetesSubstrate::restore_execution_inner`] calls that before claiming.
fn restore_transition_target_state(
    lifecycle: SandboxLifecycle,
) -> Result<SandboxTransitionTargetState, SubstrateError> {
    match lifecycle {
        SandboxLifecycle::Started => Ok(SandboxTransitionTargetState::Started),
        SandboxLifecycle::Paused => Ok(SandboxTransitionTargetState::Paused),
        other => Err(SubstrateError::Conflict {
            message: format!("restore target_state must be Started or Paused, got {other}"),
        }),
    }
}

pub(super) fn ensure_wake_recovery_lifecycle(sandbox: &Sandbox) -> Result<(), SubstrateError> {
    let lifecycle = parse_sandbox_lifecycle(&sandbox.lifecycle)?;
    if lifecycle == SandboxLifecycle::Suspending {
        Err(SubstrateError::suspend_in_flight(sandbox.id))
    } else if is_wake_recovery_lifecycle(&sandbox.lifecycle) {
        Ok(())
    } else {
        Err(SubstrateError::Conflict {
            message: format!(
                "sandbox cannot be woken while lifecycle is {}",
                sandbox.lifecycle
            ),
        })
    }
}

/// What [`KubernetesSubstrate::reclaim_lapsed_restore`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LapsedRestoreReclaim {
    /// This request drove the restore's wake under its own claim.
    Reclaimed,
    /// An owner holds the restore again; keep joining it.
    OwnerReturned,
    /// The row isn't this request's to reclaim (a drain owns it, it moved
    /// on, or too little budget is left).
    Declined,
}

/// Below this much of its readiness budget, a request joining a restore
/// whose owner lapsed doesn't reclaim it: the claim would take the lease only
/// to fail its wake at once.
const MIN_RESTORE_RECLAIM_WINDOW: Duration = Duration::from_secs(1);

/// The error for a request whose sandbox left `restoring` other than for
/// `started`: the error a wake would give a state that can't be woken, and
/// otherwise (a rollback to `suspended`, say) a retryable `RestoreInFlight`,
/// so the retry acts on the current state.
fn left_restoring_error(sandbox: &Sandbox) -> SubstrateError {
    match ensure_wake_recovery_lifecycle(sandbox) {
        Err(error) => error,
        Ok(()) => SubstrateError::restore_in_flight(format!(
            "sandbox {} went {} before its restore reached started; retry the same sandbox",
            sandbox.id, sandbox.lifecycle
        )),
    }
}

#[derive(Clone)]
struct RestoreClaimHandling {
    preclaimed: Option<RestoreClaim>,
    cleanup_on_failure: bool,
    /// Deferred activation charge the claim-winner pays. Preclaimed callers
    /// pass [`RecoveryCharge::Exempt`]: their claim was charged upstream.
    charge: RecoveryCharge,
}

/// Implicit wake always targets Started and retains its policy source through
/// detached activation and recovery; explicit evacuation can target Paused.
#[derive(Clone, Copy)]
enum WakeTarget {
    Explicit(SandboxLifecycle),
    Implicit(ExecutionActivityKind),
}
impl WakeTarget {
    fn state(self) -> SandboxLifecycle {
        match self {
            Self::Explicit(state) => state,
            Self::Implicit(_) => SandboxLifecycle::Started,
        }
    }
    fn activity(self) -> Option<ExecutionActivityKind> {
        match self {
            Self::Explicit(_) => None,
            Self::Implicit(activity) => Some(activity),
        }
    }
    fn started(activity: Option<ExecutionActivityKind>) -> Self {
        activity.map_or(Self::Explicit(SandboxLifecycle::Started), Self::Implicit)
    }
}

struct PreparedWakeDrive {
    prepared: PreparedWakeSandbox,
    timeout: Duration,
    metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
    evacuation_placement: Option<Option<EvacuationPlacement>>,
    target: WakeTarget,
    cleanup_on_failure: bool,
    join_deadline: Instant,
    charge: RecoveryCharge,
}

impl RestoreClaimHandling {
    fn preclaimed_with_cleanup(claim: RestoreClaim) -> Self {
        Self {
            preclaimed: Some(claim),
            cleanup_on_failure: true,
            charge: RecoveryCharge::Exempt,
        }
    }

    fn claim_and_cleanup_on_failure(charge: RecoveryCharge) -> Self {
        Self {
            preclaimed: None,
            cleanup_on_failure: true,
            charge,
        }
    }
}

impl KubernetesSubstrate {
    async fn publish_started_recovery_completion(
        &self,
        sandbox_id: SandboxId,
        generation: Option<i64>,
        activation_attempt_id: Option<SandboxActivationAttemptId>,
        completion: StartedRecoveryCompletion,
    ) {
        if let Some(generation) = generation
            && let Err(update_err) = self
                .store
                .complete_started_recovery(
                    sandbox_id,
                    generation,
                    activation_attempt_id,
                    completion,
                )
                .await
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                generation,
                %completion,
                error = %update_err,
                "failed to publish started sandbox recovery completion"
            );
        }
    }

    async fn publish_started_recovery_result<T>(
        &self,
        sandbox_id: SandboxId,
        generation: Option<i64>,
        activation_attempt_id: Option<SandboxActivationAttemptId>,
        result: &Result<T, SubstrateError>,
    ) {
        let completion = match result {
            Ok(_) => Some(StartedRecoveryCompletion::Succeeded),
            Err(
                SubstrateError::Timeout { .. }
                | SubstrateError::RestoreInFlight { .. }
                | SubstrateError::RestoreSourcePending { .. }
                | SubstrateError::SuspendInFlight { .. }
                | SubstrateError::ActivationInFlight { .. },
            ) => None,
            Err(_) => Some(StartedRecoveryCompletion::Failed),
        };
        if let Some(completion) = completion {
            self.publish_started_recovery_completion(
                sandbox_id,
                generation,
                activation_attempt_id,
                completion,
            )
            .await;
        }
    }

    fn cached_restore_pod(&self, pod_name: &str, generation: u64) -> Option<Arc<SandboxPodView>> {
        let pod = self.lookup_sandbox_pod(pod_name)?;
        let observed_generation = pod
            .annotation(SandboxPodKey::ExecutionGeneration)?
            .parse::<u64>()
            .ok()?;
        // Re-drives can reuse a Pod name while the reflector still holds the
        // previous object. Never link or report placement for that generation.
        (observed_generation == generation).then_some(pod)
    }

    fn record_restore_pod_placement(&self, pod_name: &str, generation: u64) {
        let Some(trace) = crate::wake_trace::current() else {
            return;
        };
        let Some(pod) = self.cached_restore_pod(pod_name, generation) else {
            return;
        };
        trace.restore_placement(
            pod.annotation(SandboxPodKey::SuspendRestoreSourceNode),
            pod.node_name(),
        );
    }

    /// One routable pod observation, as reported by either follower join,
    /// turned into the binding callers route on.
    fn binding_for_routable_pod(
        &self,
        generation: i64,
        pod_name: String,
        pod_ip: &str,
    ) -> Result<ExecutionBinding, SubstrateError> {
        let pod_ip = PodIp::parse(pod_ip)?;
        let execution_generation = execution_generation_to_u64(generation)?;
        self.record_restore_pod_placement(&pod_name, execution_generation);
        // The winning API pod stores the materialization context on the Pod.
        // Read only the reflector here: diagnostics must not add API traffic.
        if let Some(pod) = self.cached_restore_pod(&pod_name, execution_generation)
            && let Some(traceparent) = pod.annotation(SandboxPodKey::Traceparent)
        {
            pplx_space_observability::trace_propagation::link_span_from_traceparent(
                &tracing::Span::current(),
                traceparent,
            );
            tracing::info!(leader_traceparent = %traceparent, "joined materialized restore");
        }
        Ok(ExecutionBinding {
            execution_object_id: pod_name,
            runtime_endpoint: Self::endpoint_for_pod_ip(pod_ip, self.k8s.sandbox_proxy_port)?,
            execution_generation,
        })
    }

    async fn completed_user_restore_binding(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<ExecutionBinding>, SubstrateError> {
        match self.store.observe_user_restore(sandbox_id).await? {
            UserRestoreObservation::Routable {
                generation,
                pod_name,
                pod_ip,
            } => self
                .binding_for_routable_pod(generation, pod_name, &pod_ip)
                .map(Some),
            UserRestoreObservation::InFlight { .. }
            | UserRestoreObservation::Claimable
            | UserRestoreObservation::StartedUnroutable => Ok(None),
        }
    }

    /// Best-effort rollback of a failed fresh restore claim to `suspended`
    /// (see [`K8sStore::revert_restore_claim_to_suspended`]). When the
    /// rollback retired the claim's execution, its pod is deleted here too.
    /// That delete is best-effort: a pod it misses belongs to no active
    /// execution, so the orphaned-restore-pod GC and the next restore's
    /// lingering-pod sweep both reap it.
    pub(super) async fn revert_failed_restore_claim(
        &self,
        sandbox_id: SandboxId,
        restore_claim: RestoreClaim,
        activation_attempt: Option<(SandboxActivationAttemptId, i64)>,
    ) {
        let outcome = match self
            .store
            .revert_restore_claim_to_suspended(
                sandbox_id,
                restore_claim.restore_lease_token(),
                activation_attempt,
                self.k8s.abandoned_restore_retirement,
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %err,
                    "failed to restore suspended lifecycle after Kubernetes restore failure"
                );
                return;
            }
        };
        pplx_space_metrics::sandbox::restore::claim_reverted(outcome.metric_outcome());
        match outcome {
            RestoreClaimRevert::ExecutionRetired { pod_name } => {
                // A retired generation is never reused, so the name can't
                // belong to a successor.
                if let Err(err) = self.delete_tracked_pods(vec![pod_name]).await {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        error = %err,
                        "failed to delete the pod of an abandoned restore; it has no active \
                         execution, so orphan GC or the next restore removes it"
                    );
                }
            }
            RestoreClaimRevert::ExecutionLeftActive => tracing::warn!(
                sandbox_id = %sandbox_id,
                "reverted a failed restore with an execution still active: either restore \
                 selection isn't proven to choose a durable pre-restore source, or the claim \
                 reused that execution rather than creating it"
            ),
            RestoreClaimRevert::LifecycleOnly | RestoreClaimRevert::NotReverted => {}
        }
    }

    /// Best-effort release of a restore claim whose deferred admission
    /// charge was rejected: a fresh suspended claim rolls back to
    /// `suspended`; a redrive keeps the row `restoring` and only frees the
    /// lease so another caller can claim it.
    async fn release_rejected_restore_claim(
        &self,
        sandbox_id: SandboxId,
        restore_claim: RestoreClaim,
    ) {
        let result = match restore_claim {
            RestoreClaim::FreshFromSuspended { .. } => {
                return self
                    .revert_failed_restore_claim(sandbox_id, restore_claim, None)
                    .await;
            }
            RestoreClaim::RedroveRestoring {
                restore_lease_token: Some(token),
            } => {
                self.store
                    .release_user_restore_lease(sandbox_id, token)
                    .await
            }
            // Unreachable as written: only the evacuation claim produces this
            // variant, and evacuation restores are `RecoveryCharge::Exempt`,
            // so no deferred charge can reject one. If it ever is, leaving
            // the row `restoring` is the release, for the reason
            // `RestoreClaim::FreshFromStopped` gives.
            RestoreClaim::FreshFromStopped
            | RestoreClaim::RedroveRestoring {
                restore_lease_token: None,
            } => Ok(()),
        };
        if let Err(error) = result {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                error = %error,
                "failed to release restore claim after admission rejection"
            );
        }
    }

    #[tracing::instrument(name = "substrate.started_recovery.wait", skip_all, fields(
        sandbox_id = %sandbox_id, generation,
    ), err)]
    async fn wait_for_started_recovery(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        timeout: Duration,
        lease_remaining: Duration,
    ) -> Result<StartedRecoveryJoin, SubstrateError> {
        let lease_deadline = Instant::now() + lease_remaining;
        let (_, phase_deadline) = ready_wait_bounds(timeout.min(lease_remaining));
        let join = async {
            let mut backoff = RecoveryJoinBackoff::new(self.recovery_join_backoff);
            loop {
                tokio::time::sleep(backoff.next_delay()).await;
                match self
                    .store
                    .observe_started_recovery(sandbox_id, generation)
                    .await?
                {
                    StartedRecoveryObservation::Pending => {}
                    StartedRecoveryObservation::Succeeded {
                        generation,
                        pod_name,
                        pod_ip,
                    } => {
                        return self
                            .binding_for_routable_pod(generation, pod_name, &pod_ip)
                            .map(Box::new)
                            .map(StartedRecoveryJoin::Completed);
                    }
                    StartedRecoveryObservation::Failed => {
                        if let Some(sandbox) =
                            self.suspended_or_restoring_sandbox(sandbox_id).await?
                        {
                            return Ok(StartedRecoveryJoin::SupersededBySuspend(Box::new(sandbox)));
                        }
                        return Err(SubstrateError::Timeout {
                            message: format!(
                                "sandbox recovery attempt failed for sandbox {sandbox_id}; retry"
                            ),
                            phase: None,
                        });
                    }
                }
            }
        };

        match tokio::time::timeout_at(phase_deadline, join).await {
            Ok(result) => result,
            Err(_) => Err(SubstrateError::restore_in_flight_with_lease(
                format!("sandbox restore is already in flight for sandbox {sandbox_id}; retry"),
                remaining_ready_window(lease_deadline),
            )),
        }
    }

    async fn claim_or_join_started_recovery(
        &self,
        sandbox_id: SandboxId,
        timeout: Duration,
    ) -> Result<StartedRecoveryClaimOutcome, SubstrateError> {
        let claim = match self.store.claim_started_recovery(sandbox_id).await {
            Ok(claim) => claim,
            Err(error @ SubstrateError::Conflict { .. }) => {
                return match self.suspended_or_restoring_sandbox(sandbox_id).await? {
                    Some(sandbox) => Ok(StartedRecoveryClaimOutcome::SupersededBySuspend {
                        sandbox: Box::new(sandbox),
                        retired_generation: None,
                    }),
                    None => Err(error),
                };
            }
            Err(error) => return Err(error),
        };
        match claim {
            StartedRecoveryClaim::CreatedExecution { generation } => {
                Ok(StartedRecoveryClaimOutcome::Owned(
                    StartedRecoveryOwnership::CreatedExecution { generation },
                ))
            }
            StartedRecoveryClaim::ExistingExecution { generation } => {
                Ok(StartedRecoveryClaimOutcome::Owned(
                    StartedRecoveryOwnership::ExistingExecution { generation },
                ))
            }
            StartedRecoveryClaim::InFlight {
                generation,
                lease_remaining,
            } => match self
                .wait_for_started_recovery(sandbox_id, generation, timeout, lease_remaining)
                .await?
            {
                StartedRecoveryJoin::Completed(binding) => {
                    Ok(StartedRecoveryClaimOutcome::Joined(binding))
                }
                StartedRecoveryJoin::SupersededBySuspend(sandbox) => {
                    Ok(StartedRecoveryClaimOutcome::SupersededBySuspend {
                        sandbox,
                        retired_generation: Some(generation),
                    })
                }
            },
        }
    }

    async fn suspended_or_restoring_sandbox(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<Sandbox>, SubstrateError> {
        let sandbox = self.store.require_sandbox(sandbox_id).await?;
        Ok(matches!(
            parse_sandbox_lifecycle(&sandbox.lifecycle)?,
            SandboxLifecycle::Suspended | SandboxLifecycle::Restoring
        )
        .then_some(sandbox))
    }

    async fn wait_for_started_recovery_superseded_by_suspend(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Sandbox, SubstrateError> {
        let mut backoff = RecoveryJoinBackoff::new(self.recovery_join_backoff);
        loop {
            tokio::time::sleep(backoff.next_delay()).await;
            if let Some(sandbox) = self.suspended_or_restoring_sandbox(sandbox_id).await? {
                return Ok(sandbox);
            }
        }
    }

    fn redrive_started_recovery_after_suspend<'a>(
        &'a self,
        sandbox: &'a Sandbox,
        retired_generation: Option<i64>,
        join_deadline: Instant,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        charge: RecoveryCharge,
        wake_activity: Option<ExecutionActivityKind>,
    ) -> Pin<Box<dyn Future<Output = Result<ExecutionActivation, SubstrateError>> + Send + 'a>>
    {
        Box::pin(async move {
            let sandbox_id = sandbox.id;
            match retired_generation {
                Some(generation) => tracing::info!(
                    sandbox_id = %sandbox_id,
                    retired_generation = generation,
                    lifecycle = %sandbox.lifecycle,
                    recovery_path = "suspend_finalization_redrive",
                    "started recovery was superseded by suspend finalization; re-driving restore"
                ),
                None => tracing::info!(
                    sandbox_id = %sandbox_id,
                    lifecycle = %sandbox.lifecycle,
                    recovery_path = "suspend_finalization_redrive",
                    "started recovery claim was superseded by suspend finalization; re-driving restore"
                ),
            }
            self.wake_execution_inner(
                sandbox,
                remaining_ready_window(join_deadline),
                metric_identity,
                None,
                WakeTarget::started(wake_activity),
                RestoreClaimHandling::claim_and_cleanup_on_failure(charge),
            )
            .await
        })
    }

    /// Redrive the restore of a sandbox whose started recovery a suspend
    /// superseded, for an access request. The binding is handed out only once
    /// the row is `started` (see `join_restore_before_dial`): if the redrive's
    /// owner couldn't commit that, its restore may still be rolled back.
    pub(super) async fn redrive_suspended_recovery_for_access(
        &self,
        sandbox: &Sandbox,
        retired_generation: Option<i64>,
        join_deadline: Instant,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        charge: RecoveryCharge,
    ) -> Result<ExecutionBinding, ControllerError> {
        self.redrive_started_recovery_after_suspend(
            sandbox,
            retired_generation,
            join_deadline,
            metric_identity,
            charge,
            None,
        )
        .await
        .map_err(ControllerError::from)?;
        self.binding_once_started(sandbox.id, join_deadline).await
    }

    /// Bounded follower join on the user restore lease. The process-wide
    /// restore poller batch-observes every pending sandbox; this request owns
    /// only a watch receiver until the owner publishes a routable outcome,
    /// the lease lapses, or the join deadline expires.
    #[tracing::instrument(name = "substrate.restore_waiter.wait", skip_all, fields(
        sandbox_id = %sandbox_id,
        poll_interval_ms = super::restore_waiters::RESTORE_POLL_INTERVAL.as_millis() as u64,
    ), err)]
    async fn wait_for_user_restore(
        &self,
        sandbox_id: SandboxId,
        join_deadline: Instant,
        lease_remaining_hint: Option<Duration>,
        backoff: &mut RecoveryJoinBackoff,
    ) -> Result<UserRestoreJoin, SubstrateError> {
        let _wait = crate::wake_trace::RestoreWait::start();
        let mut last_lease_deadline =
            lease_remaining_hint.map(|remaining| Instant::now() + remaining);
        if tokio::time::timeout_at(join_deadline, tokio::time::sleep(backoff.next_delay()))
            .await
            .is_err()
        {
            return Err(SubstrateError::RestoreInFlight {
                message: format!(
                    "sandbox restore is already in flight for sandbox {sandbox_id}; \
                     retry the same sandbox after the restore lease expires"
                ),
                lease_remaining: last_lease_deadline.map(remaining_ready_window),
            });
        }
        let mut receiver = self.restore_waiters.subscribe(sandbox_id)?;
        let join = async {
            loop {
                match super::restore_waiters::wait_for_restore_update(&mut receiver).await? {
                    UserRestoreObservation::Routable {
                        generation,
                        pod_name,
                        pod_ip,
                    } => {
                        return Ok(UserRestoreJoin::Completed(Box::new(
                            self.binding_for_routable_pod(generation, pod_name, &pod_ip)?,
                        )));
                    }
                    UserRestoreObservation::InFlight { lease_remaining } => {
                        last_lease_deadline = Some(Instant::now() + lease_remaining);
                    }
                    UserRestoreObservation::Claimable => {
                        return Ok(UserRestoreJoin::Claimable);
                    }
                    UserRestoreObservation::StartedUnroutable => {
                        return Err(SubstrateError::restore_in_flight(format!(
                            "sandbox restore for {sandbox_id} finished without a routable \
                             execution; retry after restore state is available"
                        )));
                    }
                }
            }
        };
        // Bind before matching: the join future mutably borrows
        // `last_lease_deadline`, and scrutinee temporaries live for the whole
        // match — the borrow must end before the arms read the deadline.
        let joined = tokio::time::timeout_at(join_deadline, join).await;
        match joined {
            Ok(result) => result,
            Err(_) => Err(SubstrateError::RestoreInFlight {
                message: format!(
                    "sandbox restore is already in flight for sandbox {sandbox_id}; \
                     retry the same sandbox after the restore lease expires"
                ),
                lease_remaining: last_lease_deadline.map(remaining_ready_window),
            }),
        }
    }

    /// A request a user is waiting on never dials a sandbox's pod while the
    /// sandbox is `restoring`: the restore can still fail and be rolled back,
    /// and the rollback retires its execution and deletes the pod, discarding
    /// whatever it served. Instead this joins the in-flight restore and, once
    /// the sandbox is `started`, returns a client for the execution the row
    /// routes to. A row the caller saw `restoring` that is already `started`
    /// here is dialed the same way.
    ///
    /// It joins before it claims, waiting on the shared restore poller rather
    /// than polling the row per request. Only a row still `restoring` once its
    /// owner's lease and execution ownership have lapsed, and that no drain
    /// owns, is reclaimed: this request takes the restoring lease itself and
    /// drives the wake under that claim, whose owner commits `started`. A
    /// follower whose owner rolled back to `suspended` gets `RestoreInFlight`
    /// rather than starting a fresh restore that its caller's access-wake
    /// policy and admission never approved. Past `timeout` this fails as
    /// `RestoreInFlight` too, a retryable 503; a row that leaves for a state
    /// that can't be woken gets that state's error.
    pub(super) async fn join_restore_before_dial(
        &self,
        sandbox_id: SandboxId,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        timeout: Duration,
        charge: RecoveryCharge,
    ) -> Result<crate::controllers::substrate::RuntimeClient, ControllerError> {
        let (_, join_deadline) = ready_wait_bounds(timeout);
        let sandbox = self
            .store
            .require_sandbox(sandbox_id)
            .await
            .map_err(ControllerError::from)?;
        match parse_sandbox_lifecycle(&sandbox.lifecycle).map_err(ControllerError::from)? {
            SandboxLifecycle::Started => {}
            SandboxLifecycle::Restoring => {
                let mut backoff = RecoveryJoinBackoff::new(self.recovery_join_backoff);
                while let UserRestoreJoin::Claimable = self
                    .wait_for_user_restore(sandbox_id, join_deadline, None, &mut backoff)
                    .await
                    .map_err(ControllerError::from)?
                {
                    let reclaim = self
                        .reclaim_lapsed_restore(
                            sandbox_id,
                            join_deadline,
                            metric_identity,
                            charge.clone(),
                        )
                        .await?;
                    if reclaim != LapsedRestoreReclaim::OwnerReturned {
                        break;
                    }
                }
            }
            _ => return Err(ControllerError::from(left_restoring_error(&sandbox))),
        }
        self.dial_once_started(sandbox_id, join_deadline).await
    }

    /// Reclaim a `restoring` row whose owner's lease and execution ownership
    /// lapsed, for [`Self::join_restore_before_dial`]. The lease claim matches
    /// only a row still `restoring`, unowned and unmarked; this request then
    /// drives the wake under that claim, and the wake's owner commits
    /// `started`. A row a drain owns, or that moved on, is declined and left
    /// to the caller's `started` wait.
    async fn reclaim_lapsed_restore(
        &self,
        sandbox_id: SandboxId,
        join_deadline: Instant,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        charge: RecoveryCharge,
    ) -> Result<LapsedRestoreReclaim, ControllerError> {
        if remaining_ready_window(join_deadline) < MIN_RESTORE_RECLAIM_WINDOW {
            return Ok(LapsedRestoreReclaim::Declined);
        }
        let restore_claim = match self
            .store
            .claim_markerless_restoring_restore_lease(sandbox_id)
            .await
        {
            Ok(Some(restore_claim)) => restore_claim,
            Ok(None) | Err(SubstrateError::Conflict { .. }) => {
                return Ok(LapsedRestoreReclaim::Declined);
            }
            Err(SubstrateError::RestoreInFlight { .. }) => {
                return Ok(LapsedRestoreReclaim::OwnerReturned);
            }
            Err(error) => return Err(ControllerError::from(error)),
        };
        if let Err(error) = charge.charge_restore_claim().await {
            self.release_rejected_restore_claim(sandbox_id, restore_claim)
                .await;
            return Err(ControllerError::from(error));
        }
        let sandbox = self
            .store
            .require_sandbox(sandbox_id)
            .await
            .map_err(ControllerError::from)?;
        // Only a writer that ignores the lease (the tokenless evacuation
        // rollback) can move the row now; and the charge may have used up the
        // budget. Either way, hand the restore back rather than drive it.
        if !is_restoring_lifecycle(&sandbox.lifecycle)
            || remaining_ready_window(join_deadline) < MIN_RESTORE_RECLAIM_WINDOW
        {
            self.release_rejected_restore_claim(sandbox_id, restore_claim)
                .await;
            return Ok(LapsedRestoreReclaim::Declined);
        }
        self.wake_execution_inner(
            &sandbox,
            remaining_ready_window(join_deadline),
            metric_identity,
            None,
            WakeTarget::started(None),
            RestoreClaimHandling::preclaimed_with_cleanup(restore_claim),
        )
        .await
        .map_err(ControllerError::from)?;
        Ok(LapsedRestoreReclaim::Reclaimed)
    }

    /// Wake a dormant sandbox for a user request and dial it once it is
    /// `started`. A restore-shaped wake leaves the row `restoring` until its
    /// owner commits `started` (or, for a joined run, its owner did), so the
    /// binding the wake returns isn't dialed directly (see
    /// `join_restore_before_dial`).
    pub(super) async fn wake_and_dial_once_started(
        &self,
        sandbox: &Sandbox,
        timeout: Duration,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        charge: RecoveryCharge,
    ) -> Result<crate::controllers::substrate::RuntimeClient, ControllerError> {
        let (_, join_deadline) = ready_wait_bounds(timeout);
        self.wake_execution_with_metric_identity(sandbox, timeout, metric_identity, charge)
            .await
            .map_err(ControllerError::from)?;
        self.dial_once_started(sandbox.id, join_deadline).await
    }

    /// Dial the execution a sandbox routes to once the row is `started`,
    /// polling with the recovery-join backoff until `deadline`. Observes before
    /// each sleep, so a row that is already `started` is dialed at once. A
    /// `restoring` row that reads as unowned is still waited on: a drain's
    /// evacuation restore owns its row through the marker, not the lease, and
    /// only the drain moves it to `started`.
    async fn dial_once_started(
        &self,
        sandbox_id: SandboxId,
        deadline: Instant,
    ) -> Result<crate::controllers::substrate::RuntimeClient, ControllerError> {
        let binding = self.binding_once_started(sandbox_id, deadline).await?;
        crate::controllers::substrate::node_api_client::connect_worker(binding.runtime_endpoint)
            .await
    }

    /// The binding [`Self::dial_once_started`] dials.
    async fn binding_once_started(
        &self,
        sandbox_id: SandboxId,
        deadline: Instant,
    ) -> Result<ExecutionBinding, ControllerError> {
        let mut backoff = RecoveryJoinBackoff::new(self.recovery_join_backoff);
        loop {
            match self
                .store
                .observe_user_restore(sandbox_id)
                .await
                .map_err(ControllerError::from)?
            {
                UserRestoreObservation::Routable {
                    generation,
                    pod_name,
                    pod_ip,
                } => {
                    return self
                        .binding_for_routable_pod(generation, pod_name, &pod_ip)
                        .map_err(ControllerError::from);
                }
                UserRestoreObservation::InFlight { .. } => {}
                UserRestoreObservation::Claimable => {
                    let sandbox = self
                        .store
                        .require_sandbox(sandbox_id)
                        .await
                        .map_err(ControllerError::from)?;
                    if normalize_lifecycle(&sandbox.lifecycle) == "started" {
                        // Moved on since the observation: observe again now.
                        continue;
                    }
                    if !is_restoring_lifecycle(&sandbox.lifecycle) {
                        return Err(ControllerError::from(left_restoring_error(&sandbox)));
                    }
                }
                UserRestoreObservation::StartedUnroutable => {
                    return Err(ControllerError::from(SubstrateError::restore_in_flight(
                        format!(
                            "sandbox {sandbox_id} is started without a routable execution; \
                             retry the same sandbox"
                        ),
                    )));
                }
            }
            if tokio::time::timeout_at(deadline, tokio::time::sleep(backoff.next_delay()))
                .await
                .is_err()
            {
                return Err(ControllerError::from(SubstrateError::restore_in_flight(
                    format!(
                        "sandbox {sandbox_id}'s restore didn't reach started in time; \
                         retry the same sandbox"
                    ),
                )));
            }
        }
    }

    /// Claim or join started recovery. Followers wait without spending
    /// admission; owners charge before Pod creation and publish completion.
    /// The access path takes no caller-selected `suspend_restore_source_node`.
    /// Started recovery may still select a current local artifact internally,
    /// but it uses a direct `nodeName` and therefore cannot enter the
    /// required-affinity cross-node re-drive whose cancellation cleanup needs a
    /// user-restore lease token. Remote fallback carries storage only. The
    /// primary wake/restore path remains responsible for fenced required-pin
    /// re-drives.
    pub(super) async fn ensure_execution_ready_for_access(
        &self,
        sandbox: &Sandbox,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        timeout: Duration,
        charge: RecoveryCharge,
        suspend_restore_storage_uri: Option<&str>,
    ) -> Result<ExecutionBinding, ControllerError> {
        let sandbox_id = sandbox.id;
        let (_, join_deadline) = ready_wait_bounds(timeout);
        let lifecycle =
            parse_sandbox_lifecycle(&sandbox.lifecycle).map_err(ControllerError::from)?;
        if lifecycle == SandboxLifecycle::Suspending {
            return Err(ControllerError::from(SubstrateError::suspend_in_flight(
                sandbox_id,
            )));
        }
        let started_recovery = if should_join_started_recovery(lifecycle, SandboxLifecycle::Started)
        {
            match self
                .claim_or_join_started_recovery(sandbox_id, timeout)
                .await
                .map_err(ControllerError::from)?
            {
                StartedRecoveryClaimOutcome::Owned(ownership) => Some(ownership),
                StartedRecoveryClaimOutcome::Joined(binding) => return Ok(*binding),
                StartedRecoveryClaimOutcome::SupersededBySuspend {
                    sandbox,
                    retired_generation,
                } => {
                    return self
                        .redrive_suspended_recovery_for_access(
                            &sandbox,
                            retired_generation,
                            join_deadline,
                            metric_identity,
                            charge,
                        )
                        .await;
                }
            }
        } else {
            None
        };
        let started_recovery_generation =
            started_recovery.map(StartedRecoveryOwnership::generation);

        if (started_recovery_generation.is_some() || !is_dormant_lifecycle(&sandbox.lifecycle))
            && let Err(error) = charge.charge_cold_activation().await
        {
            self.publish_started_recovery_completion(
                sandbox_id,
                started_recovery_generation,
                None,
                StartedRecoveryCompletion::Failed,
            )
            .await;
            return Err(error.into());
        }

        let (wait_budget, phase_deadline) = ready_wait_bounds(timeout);
        let requested_attempt_id = pplx_space_common_core::types::SandboxActivationAttemptId::new();
        let activation_assignment = ActivationAttemptAssignment::new(
            requested_attempt_id,
            SandboxTransitionTargetState::Started,
        );
        let wait = async {
            // Boxed: `ensure_pod_exists` is a large future; inlining it here
            // overflows the test thread's stack.
            let (activation_attempt_id, joined_attempt_id) = match Box::pin(self.ensure_pod_exists(
                sandbox,
                None,
                // No source-node pin on the access path (see the fn doc): the
                // started-recovery lease can't fence the re-drive a required
                // pin would trigger.
                None,
                suspend_restore_storage_uri,
                PodMaterializationOwnership::new(
                    started_recovery,
                    // Access/join path holds no restore lease.
                    None,
                    Some(activation_assignment.clone()),
                ),
            ))
            .await
            {
                Ok(()) => (activation_assignment.assigned_or_requested(), None),
                Err(SubstrateError::ActivationInFlight {
                    same_target: true,
                    owner_attempt_id,
                    ..
                }) => (requested_attempt_id, Some(owner_attempt_id)),
                Err(error) => return Err(error),
            };
            let fence = self
                .store
                .reconcile_activation_fence(
                    sandbox_id,
                    activation_attempt_id,
                    joined_attempt_id,
                    SandboxTransitionTargetState::Started,
                )
                .await?;
            let fence_ownership = fence.ownership();
            let owned_attempt = fence
                .owned_attempt_id()
                .map(|attempt_id| (attempt_id, fence.execution.generation));
            let execution = fence.execution;
            let wake_action =
                activation_wake_action(&execution, fence_ownership, SandboxLifecycle::Started)?;
            let (wake_outcome, route_identity) = self
                .execute_activation_wake_action(
                    sandbox_id,
                    wake_action,
                    phase_deadline,
                    |route_identity, activation_policy| {
                        pplx_space_sbx_mgr_client::proto::WakeRequest {
                            implicit_wake_source: 0,
                            sandbox_id: sandbox.id.to_string(),
                            blocking: true,
                            start_context: Some(
                                pplx_space_sbx_mgr_client::proto::WakeStartContext {
                                    activation_auto_lifecycle: Some(activation_policy),
                                    expected_execution_generation: route_identity
                                        .execution_generation,
                                    operation_activation_attempt_id: execution
                                        .activation_attempt_id
                                        .map(|id| id.to_string()),
                                    vcpu: sandbox_vcpu_option(sandbox),
                                    memory_mib: sandbox_memory_mib_option(sandbox),
                                    netns_path: None,
                                    suspend_restore_storage_uri: suspend_restore_storage_uri
                                        .map(ToOwned::to_owned),
                                    suspend_restore_source_node: None,
                                    expected_local_artifact_id: None,
                                    route_identity: Some(wake_request_route_identity(
                                        route_identity,
                                    )),
                                },
                            ),
                            target_state: pplx_space_sbx_mgr_client::proto::WakeTargetState::Started
                                as i32,
                            runtime_upgrade: None,
                        }
                    },
                    || {},
                )
                .await
                .map_err(ActivationWakeFailure::into_error)?;
            let binding = self
                .poll_until_command_ready(
                    sandbox_id,
                    metric_identity,
                    remaining_ready_window(phase_deadline),
                    match route_identity.as_ref() {
                        Some(route_identity) => CommandReadyPod::SelectedRoute {
                            route_identity,
                            pod_ip: wake_outcome.pod_ip,
                        },
                        None => CommandReadyPod::ResolveLegacy,
                    },
                    None,
                )
                .await?;
            Ok((binding, owned_attempt))
        };
        let result = match tokio::time::timeout(wait_budget, wait).await {
            Ok(result) => result,
            Err(_) => Err(SubstrateError::Timeout {
                message: format!(
                    "sandbox access did not reach readiness within {}s; retry the same sandbox",
                    wait_budget.as_secs()
                ),
                phase: None,
            }),
        };

        if result.is_err()
            && let Some(generation) = started_recovery_generation
            && let Some(sandbox) = self.suspended_or_restoring_sandbox(sandbox_id).await?
        {
            if let Some((activation_attempt_id, generation)) = activation_assignment.assigned() {
                self.store
                    .abandon_activation_attempt_superseded_by_suspend_best_effort(
                        sandbox_id,
                        generation,
                        activation_attempt_id,
                    )
                    .await;
            }
            return self
                .redrive_suspended_recovery_for_access(
                    &sandbox,
                    Some(generation),
                    join_deadline,
                    metric_identity,
                    RecoveryCharge::Exempt,
                )
                .await;
        }

        self.publish_started_recovery_result(
            sandbox_id,
            started_recovery_generation,
            activation_assignment.assigned_id(),
            &result,
        )
        .await;
        if let Ok((_, Some((activation_attempt_id, generation)))) = &result {
            self.store
                .release_activation_attempt_best_effort(
                    sandbox_id,
                    *generation,
                    *activation_attempt_id,
                )
                .await;
        }
        result
            .map(|(binding, _)| binding)
            .map_err(ControllerError::from)
    }

    /// Variant of [`SubstrateAdapter::restore_execution`] used by the
    /// cluster-controller's drain pipeline. Identical lifecycle bookkeeping
    /// but the pod is built without source-node affinity, and when
    /// `placement` is `Some` a matching cohort toleration *and* required
    /// nodeAffinity term are rendered so the pod can only land on the
    /// active target MNG (the two-MNG blue/green rollout pattern). When
    /// `placement` is `None`, the rebuilt pod simply drops the source-node
    /// pin with no extra cohort placement.
    pub async fn restore_evacuated_execution(
        &self,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        options: ActivationOptions,
        placement: Option<&EvacuationPlacement>,
        target_state: SandboxLifecycle,
    ) -> Result<(), SubstrateError> {
        self.restore_execution_inner(
            metric_identity,
            sandbox_id,
            options,
            Some(placement),
            target_state,
            RecoveryCharge::Exempt,
        )
        .await
        .map(|_| ())
    }

    /// Shared lifecycle bookkeeping for both [`SubstrateAdapter::restore_execution`]
    /// (user-API restore, `evacuation_placement = None`) and
    /// [`Self::restore_evacuated_execution`] (cluster-controller drain
    /// evacuation, `evacuation_placement = Some(_)` — the outer `Some`
    /// flags the evacuation path; the inner `Option<&EvacuationPlacement>`
    /// is the optional cohort pin). Keeping the suspend-on-failure revert
    /// and the begin/wake sequence in one place so the two paths can't
    /// drift the next time the lifecycle bookkeeping changes.
    pub(super) async fn restore_execution_inner(
        &self,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        options: ActivationOptions,
        evacuation_placement: Option<Option<&EvacuationPlacement>>,
        target_state: SandboxLifecycle,
        charge: RecoveryCharge,
    ) -> Result<ActivationCompletion, SubstrateError> {
        if !options.blocking {
            return Err(SubstrateError::Conflict {
                message: "kubernetes substrate requires blocking=true for restore".to_string(),
            });
        }
        // Refuse a target the wake can't express before claiming. Past the
        // claim the row is already `restoring`, and a refusal there depends on
        // the failure cleanup to undo it — which a fresh stopped claim
        // deliberately doesn't do.
        sandbox_lifecycle_to_wake_target_proto(target_state)?;
        // Evacuation restore (`evacuation_placement` outer `Some`) claims the
        // suspended (or released stopped) → restoring transition atomically on the
        // `evacuation_restore_target` marker, so it can't restore a sandbox a
        // concurrent user restore just cleared the marker on. The user
        // restore path (`None`) claims without the marker — it's allowed to
        // restore regardless and is what clears the marker.
        //
        // A user restore that loses the lease CAS joins the owner's attempt
        // instead of erroring: exactly one caller drives node work while the
        // rest wait for its outcome inside the readiness budget, re-claiming
        // only if the lease lapses without a routable result.
        let (_, join_deadline) = ready_wait_bounds(options.ready_timeout);
        let mut join_backoff = RecoveryJoinBackoff::new(self.recovery_join_backoff);
        let restore_claim = loop {
            let claim = match evacuation_placement {
                Some(_) => {
                    self.store
                        .begin_evacuation_restore_lifecycle(sandbox_id, target_state)
                        .await
                }
                None => self.store.begin_restore_lifecycle(sandbox_id, false).await,
            };
            match claim {
                Ok(restore_claim) => break restore_claim,
                Err(err @ SubstrateError::RestoreInFlight { .. })
                    if evacuation_placement.is_some() =>
                {
                    return Err(err);
                }
                Err(SubstrateError::RestoreInFlight {
                    lease_remaining, ..
                }) => {
                    match self
                        .wait_for_user_restore(
                            sandbox_id,
                            join_deadline,
                            lease_remaining,
                            &mut join_backoff,
                        )
                        .await?
                    {
                        UserRestoreJoin::Completed(_) => {
                            return Ok(ActivationCompletion::Joined);
                        }
                        UserRestoreJoin::Claimable => {
                            pplx_space_metrics::sandbox::api::restore_claim_lap(
                                metric_identity,
                                RestoreClaimLap::ClaimableRejoin,
                            );
                        }
                    }
                }
                Err(err @ SubstrateError::Conflict { .. }) if evacuation_placement.is_none() => {
                    if self
                        .completed_user_restore_binding(sandbox_id)
                        .await?
                        .is_some()
                    {
                        return Ok(ActivationCompletion::Joined);
                    }
                    return Err(err);
                }
                Err(err) => return Err(err),
            }
        };
        if let Err(err) = charge.charge_restore_claim().await {
            self.release_rejected_restore_claim(sandbox_id, restore_claim)
                .await;
            return Err(err);
        }
        let sandbox = self.store.require_sandbox(sandbox_id).await?;
        let activation = self
            .wake_execution_inner(
                &sandbox,
                remaining_ready_window(join_deadline),
                metric_identity,
                evacuation_placement,
                WakeTarget::Explicit(target_state),
                RestoreClaimHandling::preclaimed_with_cleanup(restore_claim),
            )
            .await?;
        Ok(activation.completion)
    }

    pub(super) async fn wake_execution_with_metric_identity(
        &self,
        sandbox: &Sandbox,
        timeout: Duration,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        charge: RecoveryCharge,
    ) -> Result<ExecutionActivation, SubstrateError> {
        self.wake_execution_with_metric_identity_and_activity(
            sandbox,
            timeout,
            metric_identity,
            charge,
            None,
        )
        .await
    }

    pub(super) async fn wake_execution_with_metric_identity_and_activity(
        &self,
        sandbox: &Sandbox,
        timeout: Duration,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        charge: RecoveryCharge,
        wake_activity: Option<ExecutionActivityKind>,
    ) -> Result<ExecutionActivation, SubstrateError> {
        self.wake_execution_inner(
            sandbox,
            timeout,
            metric_identity,
            None,
            WakeTarget::started(wake_activity),
            RestoreClaimHandling::claim_and_cleanup_on_failure(charge),
        )
        .await
    }

    /// One coalescing gate in front of the wake state machine, for user
    /// wakes only. The restore-lease claim CAS below serializes contenders on
    /// the sandbox row's `FOR UPDATE` lock, and a query blocked on a row lock
    /// holds its checked-out pool connection for the whole wait — so a
    /// per-sandbox request herd saturates the pool at the dedup gate itself.
    /// This wrapper collapses pod-local contenders to one live run per
    /// sandbox: joiners hold a watch receiver instead of a connection, and
    /// cross-pod arbitration stays with the DB lease exactly as before.
    ///
    /// Preclaimed and evacuation callers bypass the gate: the drain owns its
    /// own retry loop and must keep fail-fast claim semantics.
    async fn wake_execution_inner(
        &self,
        sandbox: &Sandbox,
        timeout: Duration,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        evacuation_placement: Option<Option<&EvacuationPlacement>>,
        target: WakeTarget,
        restore_claim_handling: RestoreClaimHandling,
    ) -> Result<ExecutionActivation, SubstrateError> {
        // Fail terminal lifecycles fast, before joining a run some other
        // caller started for a still-wakeable row.
        ensure_wake_recovery_lifecycle(sandbox)?;
        let lifecycle = parse_sandbox_lifecycle(&sandbox.lifecycle)?;
        // Gate only the restore-lease path (`suspended`/`restoring` rows),
        // where concurrent restore requests converge. Started-lifecycle recovery
        // has its own join machinery (`claim_started_recovery` +
        // `wait_for_started_recovery`) with different follower error
        // contracts; unify it onto the coalescer separately, deliberately.
        let coalesce = matches!(
            lifecycle,
            SandboxLifecycle::Suspended | SandboxLifecycle::Restoring
        ) && restore_claim_handling.preclaimed.is_none()
            && evacuation_placement.is_none()
            && target.activity().is_none();
        if !coalesce {
            return self
                .wake_execution_uncoalesced(
                    sandbox,
                    timeout,
                    metric_identity,
                    evacuation_placement,
                    target,
                    restore_claim_handling,
                )
                .await;
        }

        let sandbox_id = sandbox.id;
        let (_, join_deadline) = ready_wait_bounds(timeout);
        let wait_deadline = join_deadline + COALESCED_RUN_PUBLISH_GRACE;
        let mut rerace_backoff = RecoveryJoinBackoff::new(self.recovery_join_backoff);
        let mut wake_sandbox = sandbox.clone();
        loop {
            let outcome = self
                .wake_coalescer
                .join_or_start(sandbox_id, wait_deadline, || {
                    let substrate = self.clone();
                    let run_sandbox = wake_sandbox.clone();
                    let handling = restore_claim_handling.clone();
                    let wake_trace = crate::wake_trace::current();
                    // The run is detached, so it re-enters the owner's retry
                    // deferral explicitly, as it does the wake trace.
                    let retry_deferral = crate::retry_deferral::current();
                    crate::wake_trace::scope(
                        wake_trace,
                        crate::retry_deferral::scope(retry_deferral, async move {
                            substrate
                                .wake_execution_uncoalesced(
                                    &run_sandbox,
                                    timeout,
                                    metric_identity,
                                    None,
                                    target,
                                    handling,
                                )
                                .await
                        }),
                    )
                })
                .await;
            // The owner already recorded its plan in the detached run. Only
            // followers need cached placement; do not replace a known no-restore
            // plan with unknown locality from a Pod without restore annotations.
            if let Ok(WorkOutcome::Joined(activation)) = &outcome {
                self.record_restore_pod_placement(
                    &activation.binding.execution_object_id,
                    activation.binding.execution_generation,
                );
            }
            match outcome {
                Ok(WorkOutcome::Owned(activation)) => {
                    // The run's own completion is authoritative: the pod
                    // leader may itself have joined a cross-pod restore.
                    pplx_space_metrics::sandbox::api::wake_coalesce_outcome(
                        metric_identity,
                        WakeCoalesceOutcome::Owned,
                    );
                    return Ok(activation);
                }
                Ok(WorkOutcome::Joined(activation)) => {
                    pplx_space_metrics::sandbox::api::wake_coalesce_outcome(
                        metric_identity,
                        WakeCoalesceOutcome::Joined,
                    );
                    return Ok(ExecutionActivation::joined(activation.binding));
                }
                Ok(WorkOutcome::Failed {
                    role: CoalesceRole::Owner,
                    error,
                }) => {
                    pplx_space_metrics::sandbox::api::wake_coalesce_outcome(
                        metric_identity,
                        WakeCoalesceOutcome::OwnerFailed,
                    );
                    return Err((*error).clone());
                }
                Ok(WorkOutcome::Failed {
                    role: CoalesceRole::Joiner,
                    ..
                })
                | Err(CoalesceJoinError::RunAbandoned) => {
                    // Mirror the pre-gate `Claimable` rejoin semantics: a
                    // joiner never inherits the failed run's error. Re-check
                    // the row and re-enter the gate, where at most one
                    // re-racer becomes the new owner — all paced inside this
                    // caller's readiness budget.
                    let outcome = if matches!(outcome, Err(CoalesceJoinError::RunAbandoned)) {
                        WakeCoalesceOutcome::RunAbandoned
                    } else {
                        WakeCoalesceOutcome::JoinerReraced
                    };
                    pplx_space_metrics::sandbox::api::wake_coalesce_outcome(
                        metric_identity,
                        outcome,
                    );
                    if tokio::time::timeout_at(
                        join_deadline,
                        tokio::time::sleep(rerace_backoff.next_delay()),
                    )
                    .await
                    .is_err()
                    {
                        return Err(SubstrateError::restore_in_flight(format!(
                            "sandbox {sandbox_id} readiness budget elapsed while coalesced \
                             wake runs kept failing; retry the same sandbox"
                        )));
                    }
                    let refreshed = self.store.require_sandbox(sandbox_id).await?;
                    ensure_wake_recovery_lifecycle(&refreshed)?;
                    wake_sandbox = refreshed;
                }
                Err(CoalesceJoinError::DeadlineElapsed) => {
                    pplx_space_metrics::sandbox::api::wake_coalesce_outcome(
                        metric_identity,
                        WakeCoalesceOutcome::DeadlineElapsed,
                    );
                    // One snapshot read so the timeout error keeps the
                    // lease-derived retry hint the pre-coalescer follower
                    // path carried: the client's Retry-After should track
                    // the live owner's lease, not a flat constant.
                    let lease_remaining = self
                        .store
                        .fetch_restore_ownership_snapshot(sandbox_id)
                        .await
                        .ok()
                        .map(|(_, _, remaining_secs)| remaining_secs)
                        .filter(|remaining_secs| *remaining_secs > 0.0)
                        .map(Duration::from_secs_f64);
                    return Err(SubstrateError::RestoreInFlight {
                        message: format!(
                            "sandbox {sandbox_id} wake did not publish within this caller's \
                             readiness budget; the shared wake run may still converge — retry \
                             the same sandbox"
                        ),
                        lease_remaining,
                    });
                }
            }
        }
    }

    async fn wake_execution_uncoalesced(
        &self,
        sandbox: &Sandbox,
        timeout: Duration,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        evacuation_placement: Option<Option<&EvacuationPlacement>>,
        target: WakeTarget,
        restore_claim_handling: RestoreClaimHandling,
    ) -> Result<ExecutionActivation, SubstrateError> {
        let sandbox_id = sandbox.id;
        ensure_wake_recovery_lifecycle(sandbox)?;
        // A user-initiated wake/restore takeover clears the
        // `evacuation_restore_target` marker atomically in
        // `prepare_wake_sandbox`'s `begin_restore_lifecycle` claim (the
        // user/`require_evacuation_marker = false` branch), and the
        // marker-clearing DB trigger is the catch-all for any other takeover
        // path (e.g. Start). No separate clear is needed here.
        //
        // A user wake that loses the restore-lease CAS joins the lease
        // owner's attempt instead of erroring. Completion hands back the
        // owner's routable binding directly — never a fresh pod, which a
        // `started` row at generation ≥ 2 without restore context would
        // otherwise refuse. Evacuation and preclaimed callers keep the
        // fail-fast shape: the drain owns its own retry loop.
        let (_, join_deadline) = ready_wait_bounds(timeout);
        let mut join_backoff = RecoveryJoinBackoff::new(self.recovery_join_backoff);
        let mut refreshed_sandbox = None;
        let mut conflict_started = None;
        let prepared = loop {
            let current_sandbox = refreshed_sandbox.as_ref().unwrap_or(sandbox);
            let lap = match self
                .store
                .prepare_wake_sandbox(
                    current_sandbox,
                    sandbox_id,
                    restore_claim_handling.preclaimed,
                )
                .await
            {
                Ok(prepared) => break prepared,
                Err(err @ SubstrateError::RestoreInFlight { .. })
                    if restore_claim_handling.preclaimed.is_some()
                        || evacuation_placement.is_some() =>
                {
                    return Err(err);
                }
                Err(SubstrateError::RestoreInFlight {
                    lease_remaining, ..
                }) => {
                    match self
                        .wait_for_user_restore(
                            sandbox_id,
                            join_deadline,
                            lease_remaining,
                            &mut join_backoff,
                        )
                        .await?
                    {
                        UserRestoreJoin::Completed(binding) => {
                            return Ok(ExecutionActivation::joined(*binding));
                        }
                        UserRestoreJoin::Claimable => {
                            // Re-fetch before retrying: `prepare_wake_sandbox`
                            // branches on the row's lifecycle, and the owner
                            // may have moved it (e.g. rolled back to
                            // `suspended`) while this caller waited.
                            let sandbox = self.store.require_sandbox(sandbox_id).await?;
                            ensure_wake_recovery_lifecycle(&sandbox)?;
                            refreshed_sandbox = Some(sandbox);
                            RestoreClaimLap::ClaimableRejoin
                        }
                    }
                }
                Err(err @ SubstrateError::Conflict { .. })
                    if restore_claim_handling.preclaimed.is_none()
                        && evacuation_placement.is_none() =>
                {
                    if let Some(binding) = self.completed_user_restore_binding(sandbox_id).await? {
                        return Ok(ExecutionActivation::joined(binding));
                    }
                    let sandbox = self.store.require_sandbox(sandbox_id).await?;
                    if !matches!(
                        parse_sandbox_lifecycle(&sandbox.lifecycle)?,
                        SandboxLifecycle::Suspended | SandboxLifecycle::Restoring
                    ) {
                        return Err(err);
                    }
                    // A fence-rejected suspended row misses its claim as
                    // `Conflict`, not `RestoreInFlight`, so this lap has no
                    // other sleep or deadline check — pace it inside the
                    // readiness budget and stop when the budget elapses.
                    // Under the dispatcher's `crate::retry_deferral` scope
                    // (never set by HTTP callers), this is the one step that
                    // changes: it keeps the fast first retries for
                    // `RETRY_DEFERRAL_GRACE`, then returns `RestoreInFlight`
                    // and records the deferral instead of holding its slot.
                    let conflicted_for =
                        conflict_started.get_or_insert_with(Instant::now).elapsed();
                    let deferring = crate::retry_deferral::current().is_some();
                    if deferring
                        && conflicted_for >= RETRY_DEFERRAL_GRACE
                        && crate::retry_deferral::defer_retry_wait("restore_claim_conflict")
                    {
                        return Err(SubstrateError::restore_in_flight(format!(
                            "sandbox {sandbox_id} restore claim is unavailable; deferring \
                             the retry to the caller"
                        )));
                    }
                    let mut delay = join_backoff.next_delay();
                    if deferring {
                        delay = delay.min(RETRY_DEFERRAL_GRACE.saturating_sub(conflicted_for));
                    }
                    if tokio::time::timeout_at(join_deadline, tokio::time::sleep(delay))
                        .await
                        .is_err()
                    {
                        return Err(SubstrateError::restore_in_flight(format!(
                            "sandbox {sandbox_id} readiness budget elapsed while its restore \
                             claim stayed unavailable; retry after restore ownership clears"
                        )));
                    }
                    refreshed_sandbox = Some(sandbox);
                    RestoreClaimLap::ConflictRedrive
                }
                Err(err) => return Err(err),
            };
            pplx_space_metrics::sandbox::api::restore_claim_lap(metric_identity, lap);
        };
        let charge = restore_claim_handling.charge.clone();
        // The claim-winner pays the deferred activation charge; a rejection
        // releases the claim it just won so another caller can take over.
        if restore_claim_handling.preclaimed.is_none()
            && let Some(won_claim) = prepared.restore_claim
            && let Err(err) = restore_claim_handling.charge.charge_restore_claim().await
        {
            self.release_rejected_restore_claim(sandbox_id, won_claim)
                .await;
            return Err(err);
        }

        // Once this request wins the durable restore claim, node work must no
        // longer be owned by the HTTP handler's cancellation. Dropping a
        // JoinHandle detaches its task, so a disconnected leader continues
        // materialization while this request (when still connected) receives
        // the exact result. Keeping the large wake future inside the spawned
        // task also means followers parked above retain only their watch
        // receiver rather than storage for the materialization state machine.
        let substrate = self.clone();
        let evacuation_placement = evacuation_placement.map(|placement| placement.cloned());
        let cleanup_on_failure = restore_claim_handling.cleanup_on_failure;
        let shutdown_token = self.shutdown_token.clone();
        let sandbox_id = prepared.sandbox.id;
        let leader_span = tracing::info_span!("substrate.wake.leader", sandbox_id = %sandbox_id);
        let wake_trace = crate::wake_trace::current();
        let leader = self.task_tracker.spawn(
            crate::wake_trace::scope(wake_trace, async move {
                let drive = substrate.drive_prepared_wake(PreparedWakeDrive {
                    prepared,
                    timeout,
                    metric_identity,
                    evacuation_placement,
                    target,
                    cleanup_on_failure,
                    join_deadline,
                    charge,
                });
                tokio::pin!(drive);
                tokio::select! {
                    result = &mut drive => result,
                    _ = shutdown_token.cancelled() => {
                        tracing::info!(
                            sandbox_id = %sandbox_id,
                            "shutdown started while a detached restore leader was active; \
                             finishing the restore inside the graceful drain"
                        );
                        let result = drive.await;
                        if let Err(error) = &result {
                            tracing::warn!(
                                sandbox_id = %sandbox_id,
                                error = %error,
                                "detached restore leader failed during graceful shutdown"
                            );
                        }
                        result
                    }
                }
            })
            .instrument(leader_span),
        );
        leader.await.map_err(|error| SubstrateError::Failed {
            message: format!("sandbox wake task stopped unexpectedly: {error}"),
        })?
    }

    async fn drive_prepared_wake(
        &self,
        drive: PreparedWakeDrive,
    ) -> Result<ExecutionActivation, SubstrateError> {
        let PreparedWakeDrive {
            prepared,
            timeout,
            metric_identity,
            evacuation_placement,
            target,
            cleanup_on_failure,
            join_deadline,
            charge,
        } = drive;
        let target_state = target.state();
        let wake_activity = target.activity();
        let evacuation_placement = evacuation_placement
            .as_ref()
            .map(|placement| placement.as_ref());
        let sandbox_id = prepared.sandbox.id;
        // A takeover consumed part of the caller's readiness window inside the
        // join; the remaining wake work gets only what is left of it, so the
        // ready_timeout contract holds across lease lapse + re-claim.
        let timeout = remaining_ready_window(join_deadline).min(timeout);
        let sandbox = prepared.sandbox;
        let lifecycle = parse_sandbox_lifecycle(&sandbox.lifecycle)?;
        let started_recovery = if should_join_started_recovery(lifecycle, target_state) {
            match self
                .claim_or_join_started_recovery(sandbox_id, timeout)
                .await?
            {
                StartedRecoveryClaimOutcome::Owned(ownership) => Some(ownership),
                StartedRecoveryClaimOutcome::Joined(binding) => {
                    return Ok(ExecutionActivation::joined(*binding));
                }
                StartedRecoveryClaimOutcome::SupersededBySuspend {
                    sandbox,
                    retired_generation,
                } => {
                    return self
                        .redrive_started_recovery_after_suspend(
                            &sandbox,
                            retired_generation,
                            join_deadline,
                            metric_identity,
                            charge,
                            wake_activity,
                        )
                        .await;
                }
            }
        } else {
            None
        };
        let started_recovery_generation =
            started_recovery.map(StartedRecoveryOwnership::generation);
        // Bound the *entire* wake/restore wait — pod (re)creation,
        // pod-proxy reachability (which uses `create_data_plane_ready_timeout`
        // internally), the wake RPC, and command-ready polling — to the
        // caller's ALB-aware budget. Without this outer cap the proxy-reachable
        // step could burn its own 120s before the command-ready poll even
        // starts, pushing wake/restore/access past the gateway ceiling.
        //
        // Clamp the outer wait below the node-RPC channel timeout so the
        // budget `SubstrateError::Timeout` fires *before* tonic severs the
        // worker channel as `Cancelled`/`DeadlineExceeded`. The
        // evacuation/drain path passes a much longer timeout (the evacuate
        // budget, ~600s), but the channel's `.timeout(NODE_RPC_TIMEOUT)`
        // already caps the effective wake at ~120s regardless — so without
        // this clamp a slow evacuation restore is cut as a channel error,
        // misclassified as a clean failure, and wrongly reverted (orphaning
        // the live restore). Clamping changes no effective wait (the channel
        // was the real bound); it only ensures the timeout is reported as a
        // non-revert `Timeout`. A retry / drain re-drive converges the same
        // way it does for the user path.
        //
        // The inner readiness waits clamp to `phase_deadline` so their
        // phase-tagged timeouts (pod_never_ready / proxy_unreachable /
        // guest_not_ready — what `node_drain.restore_converging{phase}`
        // reports for evacuation restores) fire before the outer budget swallows them
        // into a generic phase-less `Timeout`. The backstop sits one phase
        // margin above the caller's requested wait (capped at
        // `CREATE_READY_BUDGET`), so the caller's effective readiness wait
        // is never shortened by the margin.
        let (wait_budget, phase_deadline) = ready_wait_bounds(timeout);
        let requested_attempt_id = pplx_space_common_core::types::SandboxActivationAttemptId::new();
        let activation_assignment = ActivationAttemptAssignment::new(
            requested_attempt_id,
            restore_transition_target_state(target_state)?,
        );
        // Error variants alone cannot prove whether runtime work exists. Keep
        // dispatch evidence outside the timed future so outer cancellation,
        // post-ack publication failure, and later readiness failure all reach
        // the lifecycle cleanup decision without being reduced to a plain
        // `SubstrateError`.
        let wake_was_dispatched = AtomicBool::new(false);
        let wait = async {
            let restore_plan = if prepared.restore_started
                || normalize_lifecycle(&sandbox.lifecycle) == "restoring"
            {
                Some(
                    self.suspend_restore_plan(&sandbox, evacuation_placement.is_some())
                        .await?,
                )
            } else {
                None
            };
            // Decided before the pod exists, so a stranded sandbox is refused
            // before anything is placed. Every restore here lands in the
            // started lifecycle except a drain's evacuation, which keeps the
            // sandbox's lifecycle and its environment.
            let runtime_upgrade = match &restore_plan {
                Some(plan) if evacuation_placement.is_none() => {
                    self.activation_runtime_upgrade(
                        sandbox_id,
                        UpgradeActivation::Restore,
                        plan.storage_uri.as_deref(),
                    )
                    .await?
                }
                _ => None,
            };
            // Boxed: `ensure_pod_exists` is a large future; inlining it here
            // overflows the test thread's stack.
            let (activation_attempt_id, joined_attempt_id) = match Box::pin(
                self.ensure_pod_exists(
                    &sandbox,
                    evacuation_placement,
                    restore_plan
                        .as_ref()
                        .and_then(|plan| plan.source_node_name.as_deref()),
                    restore_plan
                        .as_ref()
                        .and_then(|plan| plan.storage_uri.as_deref()),
                    PodMaterializationOwnership::new(
                        started_recovery,
                        // Leader's restore-lease token: fences the re-drive
                        // cancellation guard so a lapsed leader can't retire/delete a
                        // claim a fresh follower took over. Tokenless evacuation
                        // restores collapse to `None` (marker-owned, unfenced).
                        prepared
                            .restore_claim
                            .and_then(RestoreClaim::restore_lease_token),
                        Some(activation_assignment.clone()),
                    ),
                ),
            )
            .await
            {
                Ok(()) => (activation_assignment.assigned_or_requested(), None),
                Err(SubstrateError::ActivationInFlight {
                    same_target: true,
                    owner_attempt_id,
                    ..
                }) => (requested_attempt_id, Some(owner_attempt_id)),
                Err(error) => return Err(error),
            };
            self.ensure_active_execution_pod_proxy_reachable(
                metric_identity,
                sandbox_id,
                phase_deadline,
            )
            .await?;

            let fence = self
                .store
                .reconcile_activation_fence(
                    sandbox_id,
                    activation_attempt_id,
                    joined_attempt_id,
                    restore_transition_target_state(target_state)?,
                )
                .await?;
            let fence_ownership = fence.ownership();
            let owned_attempt = fence
                .owned_attempt_id()
                .map(|attempt_id| (attempt_id, fence.execution.generation));
            let active_execution = fence.execution;
            if let Some(trace) = crate::wake_trace::current() {
                if restore_plan.is_none() {
                    trace.no_restore();
                } else if let Ok(generation) = u64::try_from(active_execution.generation) {
                    self.record_restore_pod_placement(&active_execution.pod_name, generation);
                }
            }
            let wake_action =
                activation_wake_action(&active_execution, fence_ownership, target_state)?;
            let restore_storage_uri = restore_plan
                .as_ref()
                .and_then(|plan| plan.storage_uri.as_deref());

            let emit_wake_requested = || {
                wake_was_dispatched.store(true, Ordering::Release);
                SandboxEvent::new(
                    sandbox_id,
                    SandboxEventType::WakeRequested,
                    SandboxEventSource::Substrate,
                )
                .emit_with(sandbox_template_event_details(
                    &sandbox,
                    restore_storage_uri,
                    SandboxEventDetails::default()
                        .operation("wake_execution")
                        .target_state(target_state.to_string())
                        .bool_field(SandboxEventBoolField::HasNetnsPath, false)
                        .bool_field(
                            SandboxEventBoolField::RestoreStorageUriPresent,
                            restore_storage_uri.is_some(),
                        ),
                ));
            };

            let wake_target_state = sandbox_lifecycle_to_wake_target_proto(target_state)?;
            let wake_result = self
                .execute_activation_wake_action(
                    sandbox_id,
                    wake_action,
                    phase_deadline,
                    |route_identity, activation_policy| {
                        pplx_space_sbx_mgr_client::proto::WakeRequest {
                            implicit_wake_source: wake_activity
                                .map_or(0, ExecutionActivityKind::wake_source),
                            sandbox_id: sandbox.id.to_string(),
                            blocking: true,
                            start_context: Some(
                                pplx_space_sbx_mgr_client::proto::WakeStartContext {
                                    activation_auto_lifecycle: Some(activation_policy),
                                    expected_execution_generation: route_identity
                                        .execution_generation,
                                    operation_activation_attempt_id: active_execution
                                        .activation_attempt_id
                                        .map(|id| id.to_string()),
                                    vcpu: sandbox_vcpu_option(&sandbox),
                                    memory_mib: sandbox_memory_mib_option(&sandbox),
                                    netns_path: None,
                                    suspend_restore_storage_uri: restore_plan
                                        .as_ref()
                                        .and_then(|plan| plan.storage_uri.clone()),
                                    suspend_restore_source_node: restore_plan
                                        .as_ref()
                                        .and_then(|plan| plan.source_node_name.clone()),
                                    route_identity: Some(wake_request_route_identity(
                                        route_identity,
                                    )),
                                    expected_local_artifact_id: None,
                                },
                            ),
                            target_state: wake_target_state,
                            runtime_upgrade: runtime_upgrade.clone(),
                        }
                    },
                    emit_wake_requested,
                )
                .await;
            let (wake_outcome, route_identity) = match wake_result {
                Ok(outcome) => outcome,
                Err(failure) => {
                    let dispatched = failure.was_dispatched();
                    let error = failure.into_error();
                    if dispatched {
                        SandboxEvent::new(
                            sandbox_id,
                            SandboxEventType::WakeFailed,
                            SandboxEventSource::Substrate,
                        )
                        .emit_with(sandbox_template_event_details(
                            &sandbox,
                            restore_storage_uri,
                            SandboxEventDetails::default()
                                .operation("wake_execution")
                                .target_state(target_state.to_string())
                                .error(error.to_string()),
                        ));
                    }
                    return Err(error);
                }
            };

            let emit_wake_completed = |readiness_fallback: bool| {
                let mut details = SandboxEventDetails::default()
                    .operation("wake_execution")
                    .target_state(target_state.to_string());
                if readiness_fallback {
                    details = details.reason("resident_route_unchanged_readiness_fallback");
                }
                SandboxEvent::new(
                    sandbox_id,
                    SandboxEventType::WakeCompleted,
                    SandboxEventSource::Substrate,
                )
                .emit_with(sandbox_template_event_details(
                    &sandbox,
                    restore_storage_uri,
                    details,
                ));
            };
            if wake_outcome.dispatch.was_dispatched() && wake_outcome.landed {
                emit_wake_completed(false);
            }

            let binding = match target_state {
                SandboxLifecycle::Started => {
                    self.poll_until_command_ready(
                        sandbox_id,
                        metric_identity,
                        timeout.min(remaining_ready_window(phase_deadline)),
                        match route_identity.as_ref() {
                            Some(route_identity) => CommandReadyPod::SelectedRoute {
                                route_identity,
                                pod_ip: wake_outcome.pod_ip,
                            },
                            None => CommandReadyPod::ResolveLegacy,
                        },
                        None,
                    )
                    .await
                }
                SandboxLifecycle::Paused => {
                    // Wake RPC's blocking=true already gates on
                    // snapshot-load completion; build the binding from
                    // the active execution row + already-resolved pod
                    // IP. No command-ready poll: paused guest can't
                    // respond.
                    //
                    if wake_outcome.landed {
                        let route_identity =
                            route_identity
                                .as_ref()
                                .ok_or_else(|| SubstrateError::Unavailable {
                                    message:
                                        "paused Wake completed without a selected route identity"
                                            .to_string(),
                                })?;
                        let pod_ip =
                            wake_outcome
                                .pod_ip
                                .ok_or_else(|| SubstrateError::Unavailable {
                                    message: "paused restore completed but pod IP unresolved"
                                        .to_string(),
                                })?;
                        self.selected_wake_binding(sandbox_id, route_identity, pod_ip)
                            .await
                    } else {
                        self.completed_execution_binding(
                            sandbox_id,
                            &active_execution,
                            route_identity.as_ref(),
                        )
                        .await
                    }
                }
                other => Err(SubstrateError::Conflict {
                    message: format!("restore target_state must be Started or Paused, got {other}"),
                }),
            }?;

            if wake_outcome.dispatch.was_dispatched() && !wake_outcome.landed {
                emit_wake_completed(true);
            }

            Ok((binding, owned_attempt))
        };
        let result = if let Some(generation) = started_recovery_generation
            && evacuation_placement.is_none()
        {
            tokio::select! {
                result = tokio::time::timeout(wait_budget, wait) => match result {
                    Ok(inner) => inner,
                    Err(_elapsed) => Err(SubstrateError::Timeout {
                        message: format!(
                            "sandbox wake did not reach readiness within {}s; the node is still \
                             waking/restoring — retry the same sandbox to converge",
                            wait_budget.as_secs()
                        ),
                        phase: None,
                    }),
                },
                sandbox = self.wait_for_started_recovery_superseded_by_suspend(sandbox_id) => {
                    let sandbox = sandbox?;
                    if let Some((activation_attempt_id, generation)) = activation_assignment.assigned() {
                        self.store
                            .abandon_activation_attempt_superseded_by_suspend_best_effort(
                                sandbox_id,
                                generation,
                                activation_attempt_id,
                            )
                            .await;
                    }
                    return self
                        .redrive_started_recovery_after_suspend(
                            &sandbox,
                            Some(generation),
                            join_deadline,
                            metric_identity,
                            charge.clone(),
                            wake_activity,
                        )
                        .await;
                }
            }
        } else {
            match tokio::time::timeout(wait_budget, wait).await {
                Ok(inner) => inner,
                Err(_elapsed) => Err(SubstrateError::Timeout {
                    message: format!(
                        "sandbox wake did not reach readiness within {}s; the node is still \
                         waking/restoring — retry the same sandbox to converge",
                        wait_budget.as_secs()
                    ),
                    phase: None,
                }),
            }
        };
        let wake_dispatch = if wake_was_dispatched.load(Ordering::Acquire) {
            WakeDispatchStatus::MayHaveLanded
        } else {
            WakeDispatchStatus::NotDispatched
        };

        if matches!(result, Err(SubstrateError::SuspendInFlight { .. }))
            && !wake_dispatch.was_dispatched()
            && started_recovery_generation.is_some()
            && evacuation_placement.is_none()
        {
            // The exact old Pod stopped after materialization, while its
            // local suspend backup is still publishing. Keep recovery owned
            // and wait for the existing finalizer, not another Wake to that
            // Pod. The restore below reselects under the original deadline.
            // On budget expiry retain the retryable SuspendInFlight result;
            // an ambiguous dispatch must never enter this wait-only path.
            if let Ok(observation) = tokio::time::timeout_at(
                phase_deadline,
                self.wait_for_started_recovery_superseded_by_suspend(sandbox_id),
            )
            .await
            {
                observation?;
            }
        }

        if result.is_err()
            && let Some(generation) = started_recovery_generation
            && evacuation_placement.is_none()
            && let Some(sandbox) = self.suspended_or_restoring_sandbox(sandbox_id).await?
        {
            if let Some((activation_attempt_id, generation)) = activation_assignment.assigned() {
                self.store
                    .abandon_activation_attempt_superseded_by_suspend_best_effort(
                        sandbox_id,
                        generation,
                        activation_attempt_id,
                    )
                    .await;
            }
            return self
                .redrive_started_recovery_after_suspend(
                    &sandbox,
                    Some(generation),
                    join_deadline,
                    metric_identity,
                    charge,
                    wake_activity,
                )
                .await;
        }

        self.publish_started_recovery_result(
            sandbox_id,
            started_recovery_generation,
            activation_assignment.assigned_id(),
            &result,
        )
        .await;

        if cleanup_on_failure
            && restore_failure_should_revert_to_suspended(
                prepared.restore_claim,
                result.as_ref().err(),
                wake_dispatch,
            )
            && let Some(restore_claim) = prepared.restore_claim
        {
            self.revert_failed_restore_claim(
                sandbox_id,
                restore_claim,
                activation_assignment.assigned(),
            )
            .await;
        }
        if cleanup_on_failure
            && !restore_failure_should_revert_to_suspended(
                prepared.restore_claim,
                result.as_ref().err(),
                wake_dispatch,
            )
            && wake_failure_should_revert_to_suspended(result.as_ref().err(), wake_dispatch)
            && let Some(restore_lease_token) = prepared
                .restore_claim
                .and_then(RestoreClaim::restore_lease_token)
            && let Err(update_err) = self
                .store
                .release_user_restore_lease(sandbox_id, restore_lease_token)
                .await
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                error = %update_err,
                "failed to release restore lease after Kubernetes wake failure"
            );
        }

        if let Ok((_, Some((activation_attempt_id, generation)))) = &result {
            self.store
                .release_activation_attempt_best_effort(
                    sandbox_id,
                    *generation,
                    *activation_attempt_id,
                )
                .await;
        }
        // The owner of a user restore commits `restoring -> started` itself,
        // under its own lease, before handing out the binding: a user request
        // never dials a `restoring` pod (see `join_restore_before_dial`), and
        // on the access path no lifecycle-controller completion follows. A
        // lost or failed CAS leaves the row to the controller's completion or
        // the node's published state, which those requests then wait for.
        if result.is_ok()
            && target_state == SandboxLifecycle::Started
            && evacuation_placement.is_none()
            && let Some(restore_lease_token) = prepared
                .restore_claim
                .and_then(RestoreClaim::restore_lease_token)
        {
            match self
                .store
                .complete_owned_user_restore(sandbox_id, restore_lease_token)
                .await
            {
                // Reported here, in the restore's detached task, so a caller
                // that hung up can't drop it; the node's published state
                // finds the row `started` and reports nothing. The lifecycle
                // controller's actions report their own completion too, as
                // for `WakeCompleted`.
                Ok(true) => SandboxEvent::new(
                    sandbox_id,
                    SandboxEventType::RestoreCompleted,
                    SandboxEventSource::Substrate,
                )
                .emit_with(
                    SandboxEventDetails::default()
                        .reason("restore_owner_completed")
                        .with_from_state("restoring")
                        .with_to_state("started"),
                ),
                Ok(false) => {}
                Err(error) => tracing::warn!(
                    sandbox_id = %sandbox_id,
                    %error,
                    "failed to commit started after an owned restore"
                ),
            }
        }
        result.map(|(binding, _)| ExecutionActivation::owned(binding))
    }
}

/// Whether a failed wake/restore is clean enough to be eligible for reverting
/// the optimistically-set `restoring` row back to `suspended`.
///
/// - Clean failure (`Conflict`, `Unavailable`, `Failed`, …) with
///   `NotDispatched` -> `true`: the restore definitively didn't take, so the
///   durable `suspended` state is accurate and a retry re-drives from scratch.
/// - `MayHaveLanded` -> `false` regardless of error variant: an acknowledged
///   Wake can be followed by route-publication, binding, or readiness failure.
///   Preserve `restoring` so its retry/sweeper owner converges the live runtime.
/// - `Timeout` -> `false`: the wake RPC may already have landed and the
///   pod/VM is live. `suspended` is swept by neither the orphaned-sandbox
///   reconciler nor the transient-lifecycle sweeper, so reverting would orphan
///   the live pod if the client never retries. Leaving the row `restoring`
///   keeps it accurate — the node converges and a client retry reaches it,
///   while a genuinely stuck `restoring` row is bounded by the
///   transient-lifecycle sweeper (non-evacuation) or node-drain's restore
///   re-drive (evacuation), both of which tear the pod down. Mirrors the
///   create path's preserve-don't-roll-back behavior.
/// - `LockContention` -> `false`, for the same reason as `Timeout` and this is
///   the subtle one. The statements that can raise it on this path run *after*
///   `wake_sandbox` returned success — `active_execution_status` and the
///   activation-fence reads — so the pod/VM is already live. Reverting to
///   `suspended` would orphan it, and because the answer is a retryable 503 the
///   client is being invited to start a second restore against the same
///   sandbox. Leaving the row `restoring` keeps it accurate and inside the
///   sweeper's reach, and makes the retry converge instead of duplicate.
///
///   Note this list is an *exclusion* list: a variant absent from it reverts.
///   Before contention was classified it arrived as `Failed` and reverted, so
///   the divergence predates the classification — but the 503 is what turns a
///   silent orphan into an invited retry.
/// - Success (`None`) -> `false`.
pub(super) fn wake_failure_should_revert_to_suspended(
    err: Option<&SubstrateError>,
    dispatch: WakeDispatchStatus,
) -> bool {
    dispatch.operation_release_is_safe()
        && matches!(err, Some(e) if !matches!(
                e,
                SubstrateError::Timeout { .. }
                    | SubstrateError::RestoreInFlight { .. }
                    | SubstrateError::RestoreSourcePending { .. }
                    | SubstrateError::SuspendInFlight { .. }
                    | SubstrateError::ActivationInFlight { .. }
                | SubstrateError::LockContention { .. }
        ))
}

/// Root rollback invariant for failed restores: only a fresh
/// `suspended -> restoring` claim may return to `suspended`, and only on a
/// clean failure.
pub(super) fn restore_failure_should_revert_to_suspended(
    claim: Option<RestoreClaim>,
    err: Option<&SubstrateError>,
    dispatch: WakeDispatchStatus,
) -> bool {
    matches!(claim, Some(RestoreClaim::FreshFromSuspended { .. }))
        && wake_failure_should_revert_to_suspended(err, dispatch)
}

#[cfg(test)]
mod tests;
