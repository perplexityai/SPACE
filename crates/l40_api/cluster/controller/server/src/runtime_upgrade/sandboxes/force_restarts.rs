//! The `sandbox_force_restarts` campaign driver: stopping a source
//! environment's running and paused sandboxes and cold-booting them onto the
//! cluster default, once the source's memory is retired.
//!
//! * **Plan.** Held while the campaign's target isn't the cluster default
//!   (`target_not_default`), while its source is `allowed` (`source_allowed`),
//!   or while its source's memory isn't retired (`memory_not_retired`,
//!   including when cleared mid-campaign while the source is `deprecated`).
//!   Once those clear, the start-time decision
//!   ([`pplx_space_api::controllers::substrate::runtime_upgrade::decide`])
//!   says whether the catalog certifies an upgrade edge to the target or
//!   leaves the source's sandboxes stranded.
//! * **Subjects.** Active-runtime (`started`, `paused`) and mid-transition
//!   sandboxes of the substrate (including rows whose
//!   `sandbox_observed_states` projection hasn't been published yet, degraded
//!   to status-only, or was pruned while `suspended`) that aren't already bound
//!   to another environment, attributed to the source through the catalog
//!   (including unbound executions under `legacy_unbound_allowed`). Scanning
//!   unpublished active rows and mid-transition lifecycles holds campaign
//!   completion open (`Release`) until node publication and transitions settle.
//!   No skipped item is retired: an unbound sandbox the catalog or
//!   `legacy_unbound_allowed` attributes elsewhere, or to none, keeps its
//!   remaining attempts so a later catalog or flipping `legacy_unbound_allowed`
//!   to `false` while the campaign remains open reopens it, without blocking
//!   completion if it stays off the source.
//! * **Upgrade.** Re-reads the sandbox, the campaign item's continuation state
//!   ([`store::continuing_run`]) and recorded
//!   [`SandboxUpgradeDispatcher::item_stop`], any external stop/kill/delete/rollback
//!   operation since [`StopFence::initiated_at`] ([`store::external_stop_since`]),
//!   any external start/wake/restore/rollback operation since
//!   [`StopFence::stopped_at`] ([`store::external_activation_since`]), and
//!   the policy fresh. A running or paused sandbox still on the source under a
//!   policy that still force-restarts it is stopped guarded on the source
//!   ([`SandboxUpgradeDispatcher::stop`], recording [`RecordedStop::Initiated`]
//!   before dialing the node and promoting to [`RecordedStop::Stopped`] with
//!   [`StopFence`] once the node stop settles or converges), re-checked against
//!   external operations, current policy, and memory retirement, and then
//!   started guarded on that owned stop so its start-time upgrade cold-boots it
//!   onto the target ([`SandboxUpgradeDispatcher::start`], recording
//!   [`RecordedStop::StartingOnTarget`] when claiming `'stopped' -> 'starting'`
//!   and [`RecordedStop::StartedOnTarget`] once settled). On a continuing run
//!   (retry, lapsed lease redrive, `:resume` with `retry_failed`, or controller
//!   replacement), an item whose stop is still owned (verified on Kubernetes by
//!   [`store::stopped_execution_ownership`], or on Direct at `stopped_at` with
//!   no pinned stop disk, and without external activation) and without an
//!   intervening external stop since `initiated_at` skips straight to the
//!   start; an item whose stop timed out or crashed at
//!   [`RecordedStop::Initiated`] while node-status publish converged the row to
//!   `'stopped'` re-checks node campaign-stop admission through
//!   [`SandboxUpgradeDispatcher::stop`] before adopting or declining the stop;
//!   an in-flight external operation releases the lease while preserving stop
//!   ownership; a succeeded external stop or activation skips the item; only an
//!   item whose own [`SandboxUpgradeDispatcher::start`] recorded
//!   [`RecordedStop::StartedOnTarget`] — or recorded
//!   [`RecordedStop::StartingOnTarget`] before a timeout, deadline cancellation,
//!   or controller restart and then settled `'started'` onto `campaign.to` with
//!   no intervening external stop or activation — finishes as `done` on redrive;
//!   and a temporary policy, memory-retirement, or unpublished-execution hold
//!   releases the lease without spending an attempt or stranding the item in
//!   `skipped`.

mod plan;
mod stranded;

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_node_model::SubstrateKind;
use pplx_space_runtime_environment_policy_store::RuntimeEnvironmentState;
use pplx_space_runtime_upgrade_campaign_store::{
    self as store, Campaign, CampaignId, CampaignKind, ExternalOperation, ItemError, ItemOutcome,
    LeasedItem, StoppedExecutionOwnership, SubjectSandbox,
};
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use sqlx::PgPool;
use uuid::Uuid;

use self::plan::page_force_restart_candidates;
pub(super) use self::plan::{
    ForceRestartPlan, ForceRestartStep, force_restart_plan, force_restart_step,
};
use super::dispatcher::{
    GuardedStop, InitiatedStop, RecordedStop, SandboxUpgradeDispatcher, StartingClaim,
    StartingRecovery, StopFence, TargetStart, is_mid_transition,
};
use super::{
    CampaignDriver, Inputs, NO_LONGER_ON_SOURCE, ON_NEITHER_END, Plan, ScanPlan, SubjectScan,
    error_outcome,
};

pub(super) const POLICY_STOPPED_FORCE_RESTARTING: &str =
    "the policy no longer force-restarts this campaign's sandboxes";
pub(super) const EXTERNAL_OPERATION_IN_FLIGHT: &str = "an external sandbox operation is in flight";
pub(super) const EXTERNAL_STOP_SUPERSEDED: &str =
    "an external stop, kill, delete, or rollback operation superseded this campaign's stop";
pub(super) const UNPUBLISHED_EXECUTION: &str =
    "the sandbox hasn't published its execution state yet";

/// Settled active-runtime lifecycles a `sandbox_force_restarts` pass scans
/// (including rows whose node hasn't published execution state yet).
pub(super) const SETTLED_SCANNED_LIFECYCLES: &[SandboxLifecycle] =
    &[SandboxLifecycle::Started, SandboxLifecycle::Paused];

/// Snapshot of whether an environment's memory is retired and the monotonic
/// generation that stamped it, so a sub-tick unretire/re-retire cycle changes
/// [`ScanPlan`] and restarts any open subject-scan pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryRetirement {
    pub retired: bool,
    pub generation: u64,
}

/// Reads whether a runtime environment's memory is retired in the cluster
/// policy, authorizing memory-discarding restores and force-restarts off a
/// deprecated environment before it's blocked.
pub trait MemoryRetirementSource: Send + Sync + 'static {
    /// `environment`'s current memory-retirement state and monotonic
    /// generation. Every retirement change must advance `generation`.
    fn memory_retirement(
        &self,
        environment: EnvironmentReference,
    ) -> impl Future<Output = Result<MemoryRetirement, String>> + Send;
}

/// Drives `sandbox_force_restarts` campaigns of one substrate's running and
/// paused sandboxes.
///
/// `runtime_upgrade_items` owns item phase, attempts, and continuation
/// ([`store::continuing_run`]), while [`SandboxUpgradeDispatcher::item_stop`]
/// exposes the [`RecordedStop`] stamped when an earlier attempt of the item
/// stopped (and optionally finished starting) the sandbox, so retries after a
/// dropped or timed-out call, a lapsed lease, or a controller restart finish
/// booting sandboxes this campaign stopped without booting an externally
/// stopped sandbox or crediting an external wake as `Done`.
pub struct SandboxForceRestartDriver<D, M> {
    pool: PgPool,
    /// The substrate's `sandboxes.managed_by`: only its sandboxes are found.
    managed_by: SubstrateKind,
    dispatcher: Arc<D>,
    memory_retirement: Arc<M>,
    stranded_cursors: Mutex<HashMap<CampaignId, Option<Uuid>>>,
}

struct SubjectSnapshot {
    sandbox: SubjectSandbox,
    parsed_lifecycle: Option<SandboxLifecycle>,
    owned_stop: Option<StopFence>,
    owned_stop_record: Option<RecordedStop>,
    initiated_stop: Option<InitiatedStop>,
    inputs: Inputs,
    attributed: Option<EnvironmentReference>,
}

impl<D: SandboxUpgradeDispatcher, M: MemoryRetirementSource> SandboxForceRestartDriver<D, M> {
    /// A driver for the running and paused sandboxes `managed_by` owns
    /// (`SubstrateAdapter::managed_by`), stopped and cold-booted onto the
    /// default environment through `dispatcher` while `memory_retirement`
    /// reports the campaign's source memory retired.
    pub fn new(
        pool: PgPool,
        managed_by: SubstrateKind,
        dispatcher: D,
        memory_retirement: M,
    ) -> Self {
        Self {
            pool,
            managed_by,
            dispatcher: Arc::new(dispatcher),
            memory_retirement: Arc::new(memory_retirement),
            stranded_cursors: Mutex::new(HashMap::new()),
        }
    }

    async fn read_memory_retirement(
        &self,
        environment: EnvironmentReference,
    ) -> Result<MemoryRetirement, String> {
        self.memory_retirement
            .memory_retirement(environment)
            .await
            .map_err(|error| format!("reading memory retirement failed: {error}"))
    }

    async fn read_plan(&self, campaign: &Campaign) -> Result<(Inputs, ScanPlan), PlanChange> {
        let inputs = Inputs::read(&self.pool)
            .await
            .map_err(PlanChange::ReadError)?;
        let retirement = self
            .read_memory_retirement(campaign.from)
            .await
            .map_err(PlanChange::ReadError)?;
        match force_restart_plan(
            &inputs.policy,
            inputs.catalog_entry(campaign.from),
            retirement.retired,
            campaign,
        ) {
            ForceRestartPlan::Hold(reason) => Err(PlanChange::Hold(reason)),
            ForceRestartPlan::Scan { moves } => {
                let plan = ScanPlan {
                    moves,
                    inputs: inputs.fingerprint(campaign, retirement.generation),
                };
                Ok((inputs, plan))
            }
        }
    }

    async fn still_force_restarts(
        &self,
        inputs: &Inputs,
        campaign: &Campaign,
    ) -> Result<bool, ItemOutcome> {
        let retirement = self
            .read_memory_retirement(campaign.from)
            .await
            .map_err(|error| ItemOutcome::Retry(ItemError::new(error)))?;
        Ok(force_restart_plan(
            &inputs.policy,
            inputs.catalog_entry(campaign.from),
            retirement.retired,
            campaign,
        ) == ForceRestartPlan::Scan { moves: true })
    }

    async fn mark_target_start_settled(&self, item: &LeasedItem) {
        if let Err(error) = store::mark_leased_started_on_target(&self.pool, item).await {
            tracing::warn!(
                campaign_id = %item.campaign_id,
                sandbox_id = %item.subject_id,
                error = %error,
                "failed to mark leased campaign item as started on target"
            );
        }
    }

    async fn target_start_settled(
        &self,
        item: &LeasedItem,
        recorded: &Option<RecordedStop>,
    ) -> Result<bool, ItemOutcome> {
        if matches!(recorded, Some(RecordedStop::StartedOnTarget(_))) {
            self.mark_target_start_settled(item).await;
            return Ok(true);
        }
        match store::started_on_target(&self.pool, item).await {
            Ok(Some(true)) => Ok(true),
            Ok(Some(false)) => Ok(false),
            Ok(None) => Err(ItemOutcome::Release(ItemError::new(
                "the campaign item lease is no longer active",
            ))),
            Err(error) => Err(ItemOutcome::Retry(ItemError::new(format!(
                "reading campaign item target-settled state failed: {error}"
            )))),
        }
    }

    async fn read_settled_subject(
        &self,
        sandbox_id: Uuid,
    ) -> Result<(SubjectSandbox, jiff::Timestamp, Option<SandboxLifecycle>), ItemOutcome> {
        let sandbox = match store::subject_sandbox(&self.pool, sandbox_id).await {
            Ok(Some(sandbox)) => sandbox,
            Ok(None) => {
                return Err(ItemOutcome::Skipped(ItemError::new(
                    "the sandbox was deleted",
                )));
            }
            Err(error) => {
                return Err(ItemOutcome::Retry(ItemError::new(format!(
                    "reading the sandbox failed: {error}"
                ))));
            }
        };
        let Some(lifecycle_changed_at) = sandbox_timestamp(sandbox.lifecycle_changed_at) else {
            return Err(ItemOutcome::Retry(ItemError::new(format!(
                "the sandbox's lifecycle_changed_at {} is out of jiff's timestamp range",
                sandbox.lifecycle_changed_at
            ))));
        };
        let parsed_lifecycle = sandbox.lifecycle.parse::<SandboxLifecycle>().ok();
        if let Some(parsed) = parsed_lifecycle
            && is_mid_transition(parsed)
        {
            return Err(ItemOutcome::Release(ItemError::new(format!(
                "the sandbox is {}; retrying once it settles",
                sandbox.lifecycle
            ))));
        }
        Ok((sandbox, lifecycle_changed_at, parsed_lifecycle))
    }

    async fn stop_still_owned(
        &self,
        subject_id: Uuid,
        sandbox: &SubjectSandbox,
        lifecycle_changed_at: jiff::Timestamp,
        parsed_lifecycle: Option<SandboxLifecycle>,
        stop: &StopFence,
    ) -> Result<bool, ItemOutcome> {
        if parsed_lifecycle != Some(SandboxLifecycle::Stopped)
            || lifecycle_changed_at < stop.settled_stopped_at
        {
            return Ok(false);
        }
        if self
            .external_activation_succeeded_since(subject_id, stop.stopped_at)
            .await?
        {
            return Ok(false);
        }
        self.stopped_execution_still_owned(
            subject_id,
            sandbox,
            lifecycle_changed_at,
            stop.stopped_at,
            stop.settled_stopped_at,
            stop.k8s_stopped_execution.as_ref(),
        )
        .await
    }

    async fn stopped_execution_still_owned(
        &self,
        subject_id: Uuid,
        sandbox: &SubjectSandbox,
        lifecycle_changed_at: jiff::Timestamp,
        stopped_at: jiff::Timestamp,
        settled_stopped_at: jiff::Timestamp,
        k8s_stopped_execution: Option<&super::dispatcher::StoppedK8sExecution>,
    ) -> Result<bool, ItemOutcome> {
        let (Some(since), Some(settled_since)) = (
            offset_timestamp(stopped_at),
            offset_timestamp(settled_stopped_at),
        ) else {
            return Err(ItemOutcome::Retry(ItemError::new(format!(
                "the recorded stop fence ({stopped_at}, {settled_stopped_at}) is out of time::OffsetDateTime's range"
            ))));
        };
        if let Some(exec) = k8s_stopped_execution {
            let ownership = store::stopped_execution_ownership(
                &self.pool,
                subject_id,
                since,
                settled_since,
                exec.generation,
                &exec.pod_uid,
            )
            .await
            .map_err(|error| {
                ItemOutcome::Retry(ItemError::new(format!(
                    "reading stopped execution ownership failed: {error}"
                )))
            })?;
            match ownership {
                StoppedExecutionOwnership::Owned => Ok(true),
                StoppedExecutionOwnership::InFlight => Err(ItemOutcome::Release(ItemError::new(
                    EXTERNAL_OPERATION_IN_FLIGHT,
                ))),
                StoppedExecutionOwnership::Superseded => Ok(false),
            }
        } else if lifecycle_changed_at != settled_stopped_at || sandbox.pinned_stop_disk {
            Ok(false)
        } else {
            Ok(true)
        }
    }

    async fn check_no_external_stop(
        &self,
        item: &LeasedItem,
        stop: &StopFence,
    ) -> Result<(), ItemOutcome> {
        self.check_no_external_stop_since(
            item,
            stop.initiated_at,
            stop.dispatched_at,
            RecordedStop::Stopped(stop.clone()),
        )
        .await
    }

    async fn check_no_external_stop_since(
        &self,
        item: &LeasedItem,
        initiated_at: jiff::Timestamp,
        dispatched_at: jiff::Timestamp,
        clear: RecordedStop,
    ) -> Result<(), ItemOutcome> {
        let (Some(initiated), Some(dispatched)) = (
            offset_timestamp(initiated_at),
            offset_timestamp(dispatched_at),
        ) else {
            return Err(ItemOutcome::Retry(ItemError::new(format!(
                "the recorded stop window ({initiated_at}, {dispatched_at}) is out of time::OffsetDateTime's range"
            ))));
        };
        let external_stop =
            store::external_stop_since(&self.pool, item.subject_id, initiated, dispatched)
                .await
                .map_err(|error| {
                    ItemOutcome::Retry(ItemError::new(format!(
                        "reading external stop operations failed: {error}"
                    )))
                })?;
        match external_stop {
            ExternalOperation::None => Ok(()),
            ExternalOperation::InFlight => Err(ItemOutcome::Release(ItemError::new(
                EXTERNAL_OPERATION_IN_FLIGHT,
            ))),
            ExternalOperation::Succeeded => {
                self.dispatcher
                    .clear_stopped_at(item, clear)
                    .await
                    .map_err(error_outcome)?;
                Err(ItemOutcome::Skipped(ItemError::new(
                    EXTERNAL_STOP_SUPERSEDED,
                )))
            }
        }
    }

    async fn external_activation_succeeded_since(
        &self,
        subject_id: Uuid,
        dispatched_at: jiff::Timestamp,
    ) -> Result<bool, ItemOutcome> {
        let Some(since) = offset_timestamp(dispatched_at) else {
            return Err(ItemOutcome::Retry(ItemError::new(format!(
                "the recorded stop dispatched_at {dispatched_at} is out of time::OffsetDateTime's range"
            ))));
        };
        let activation = store::external_activation_since(&self.pool, subject_id, since)
            .await
            .map_err(|error| {
                ItemOutcome::Retry(ItemError::new(format!(
                    "reading external activation operations failed: {error}"
                )))
            })?;
        match activation {
            ExternalOperation::None => Ok(false),
            ExternalOperation::InFlight => Err(ItemOutcome::Release(ItemError::new(
                EXTERNAL_OPERATION_IN_FLIGHT,
            ))),
            ExternalOperation::Succeeded => Ok(true),
        }
    }

    async fn resolve_subject_or_recover_starting(
        &self,
        item: &LeasedItem,
        check_recorded_stop: bool,
        subject: Result<(SubjectSandbox, jiff::Timestamp, Option<SandboxLifecycle>), ItemOutcome>,
        recorded: &mut Option<RecordedStop>,
        fallback_clear: Option<RecordedStop>,
    ) -> Result<(SubjectSandbox, jiff::Timestamp, Option<SandboxLifecycle>), ItemOutcome> {
        let outcome = match subject {
            Ok(settled) => return Ok(settled),
            Err(outcome) => outcome,
        };
        if matches!(outcome, ItemOutcome::Release(_))
            && check_recorded_stop
            && let Some(RecordedStop::StartingOnTarget(claim)) = recorded.clone()
        {
            match self
                .dispatcher
                .recover_undispatched_start(item, SandboxId::from_uuid(item.subject_id), claim)
                .await
                .map_err(error_outcome)?
            {
                StartingRecovery::StillStarting => return Err(outcome),
                StartingRecovery::RestoredStopped(_) | StartingRecovery::Settled => {
                    let recovered_subject = self.read_settled_subject(item.subject_id).await;
                    *recorded = self
                        .dispatcher
                        .item_stop(item)
                        .await
                        .map_err(error_outcome)?;
                    if self.target_start_settled(item, recorded).await? {
                        return Err(ItemOutcome::Done);
                    }
                    match recovered_subject {
                        Ok(settled) => return Ok(settled),
                        Err(recovered_outcome) => {
                            if matches!(recovered_outcome, ItemOutcome::Skipped(_)) {
                                let clear = match recorded.clone() {
                                    Some(
                                        current @ (RecordedStop::Initiated(_)
                                        | RecordedStop::Stopped(_)
                                        | RecordedStop::StartingOnTarget(_)),
                                    ) => Some(current),
                                    _ => fallback_clear,
                                };
                                if let Some(clear) = clear {
                                    self.dispatcher
                                        .clear_stopped_at(item, clear)
                                        .await
                                        .map_err(error_outcome)?;
                                }
                            }
                            return Err(recovered_outcome);
                        }
                    }
                }
            }
        }
        if matches!(outcome, ItemOutcome::Skipped(_)) {
            let clear = match recorded.clone() {
                Some(
                    current @ (RecordedStop::Initiated(_)
                    | RecordedStop::Stopped(_)
                    | RecordedStop::StartingOnTarget(_)),
                ) => Some(current),
                _ => fallback_clear,
            };
            if let Some(clear) = clear {
                self.dispatcher
                    .clear_stopped_at(item, clear)
                    .await
                    .map_err(error_outcome)?;
            }
        }
        Err(outcome)
    }

    async fn inspect_subject(
        &self,
        campaign: &Campaign,
        item: &LeasedItem,
        check_recorded_stop: bool,
    ) -> Result<SubjectSnapshot, ItemOutcome> {
        // Read the sandbox row before `item_stop(item)`: if a lingering
        // prior-attempt stop or start committed before `read_settled_subject`
        // ran, `item_stop(item)` is guaranteed to reflect it.
        let first_subject = self.read_settled_subject(item.subject_id).await;
        let mut recorded = if check_recorded_stop {
            self.dispatcher
                .item_stop(item)
                .await
                .map_err(error_outcome)?
        } else {
            None
        };
        if check_recorded_stop && self.target_start_settled(item, &recorded).await? {
            return Err(ItemOutcome::Done);
        }

        let (mut sandbox, mut lifecycle_changed_at, mut parsed_lifecycle) = self
            .resolve_subject_or_recover_starting(
                item,
                check_recorded_stop,
                first_subject,
                &mut recorded,
                None,
            )
            .await?;

        let mut owned_stop = match &recorded {
            Some(current @ RecordedStop::Stopped(stop))
            | Some(current @ RecordedStop::StartingOnTarget(StartingClaim { stop, .. }))
                if self
                    .stop_still_owned(
                        item.subject_id,
                        &sandbox,
                        lifecycle_changed_at,
                        parsed_lifecycle,
                        stop,
                    )
                    .await? =>
            {
                Some((stop.clone(), current.clone()))
            }
            _ => None,
        };

        // If `item_stop(item)` returned a `Stopped` or `StartingOnTarget` fence
        // that didn't match the initial sandbox read (for example, a lingering
        // stop or start committed right between `read_settled_subject` and
        // `item_stop(item)`), re-read both once so any stop or target start that
        // committed in between is visible before classifying the state.
        if let Some(fallback @ (RecordedStop::Stopped(_) | RecordedStop::StartingOnTarget(_))) =
            recorded.clone()
            && owned_stop.is_none()
        {
            let reread_subject = self.read_settled_subject(item.subject_id).await;
            recorded = self
                .dispatcher
                .item_stop(item)
                .await
                .map_err(error_outcome)?;
            if self.target_start_settled(item, &recorded).await? {
                return Err(ItemOutcome::Done);
            }
            (sandbox, lifecycle_changed_at, parsed_lifecycle) = self
                .resolve_subject_or_recover_starting(
                    item,
                    check_recorded_stop,
                    reread_subject,
                    &mut recorded,
                    Some(fallback),
                )
                .await?;
            if let Some(current @ RecordedStop::Stopped(current_stop))
            | Some(
                current @ RecordedStop::StartingOnTarget(StartingClaim {
                    stop: current_stop, ..
                }),
            ) = &recorded
                && self
                    .stop_still_owned(
                        item.subject_id,
                        &sandbox,
                        lifecycle_changed_at,
                        parsed_lifecycle,
                        current_stop,
                    )
                    .await?
            {
                owned_stop = Some((current_stop.clone(), current.clone()));
            }
        }

        let inputs = Inputs::read(&self.pool)
            .await
            .map_err(|error| ItemOutcome::Retry(ItemError::new(error)))?;
        let attributed = sandbox
            .effective_execution()
            .and_then(|execution| inputs.attributed(execution));

        let mut initiated_stop = None;
        if let Some((stop, stop_record)) = &owned_stop {
            self.check_no_external_stop_since(
                item,
                stop.initiated_at,
                stop.dispatched_at,
                stop_record.clone(),
            )
            .await?;
        } else if let Some(RecordedStop::StartingOnTarget(claim)) = recorded {
            let stop = &claim.stop;
            self.check_no_external_stop_since(
                item,
                stop.initiated_at,
                stop.dispatched_at,
                RecordedStop::StartingOnTarget(claim.clone()),
            )
            .await?;
            let externally_activated = self
                .external_activation_succeeded_since(item.subject_id, stop.stopped_at)
                .await?;
            if !externally_activated
                && matches!(
                    parsed_lifecycle,
                    Some(SandboxLifecycle::Started | SandboxLifecycle::Paused)
                )
            {
                if sandbox.execution.is_none() {
                    return Err(ItemOutcome::Release(ItemError::new(UNPUBLISHED_EXECUTION)));
                }
                if attributed == Some(campaign.to) {
                    self.mark_target_start_settled(item).await;
                    return Err(ItemOutcome::Done);
                }
            }
            self.dispatcher
                .clear_stopped_at(item, RecordedStop::StartingOnTarget(claim))
                .await
                .map_err(error_outcome)?;
        } else if let Some(RecordedStop::Stopped(stop)) = recorded {
            if parsed_lifecycle == Some(SandboxLifecycle::Stopped) {
                self.check_no_external_stop(item, &stop).await?;
            }
            // The sandbox settled off `stop.stopped_at` without this item's own
            // `dispatcher.start` recording `StartingOnTarget` or
            // `StartedOnTarget` (for example, an external wake booted it onto
            // `campaign.from` or `campaign.to`); clear the superseded stop so an
            // unverified external wake isn't credited as `done`.
            self.dispatcher
                .clear_stopped_at(item, RecordedStop::Stopped(stop))
                .await
                .map_err(error_outcome)?;
        } else if let Some(RecordedStop::Initiated(initiated)) = recorded {
            self.check_no_external_stop_since(
                item,
                initiated.initiated_at,
                initiated.dispatched_at,
                RecordedStop::Initiated(initiated.clone()),
            )
            .await?;
            let externally_activated = self
                .external_activation_succeeded_since(item.subject_id, initiated.dispatched_at)
                .await?;
            let candidate_owned = if externally_activated {
                false
            } else if parsed_lifecycle == Some(SandboxLifecycle::Stopped) {
                let floor = if sandbox.pinned_stop_disk {
                    initiated.dispatched_at.min(lifecycle_changed_at)
                } else {
                    lifecycle_changed_at
                };
                lifecycle_changed_at >= initiated.initiated_at
                    && self
                        .stopped_execution_still_owned(
                            item.subject_id,
                            &sandbox,
                            lifecycle_changed_at,
                            floor,
                            lifecycle_changed_at,
                            initiated.k8s_stopped_execution.as_ref(),
                        )
                        .await?
            } else {
                matches!(
                    parsed_lifecycle,
                    Some(SandboxLifecycle::Started | SandboxLifecycle::Paused)
                )
            };
            if candidate_owned {
                initiated_stop = Some(initiated);
            } else {
                self.dispatcher
                    .clear_stopped_at(item, RecordedStop::Initiated(initiated))
                    .await
                    .map_err(error_outcome)?;
            }
        }

        let (owned_stop, owned_stop_record) = match owned_stop {
            Some((stop, record)) => (Some(stop), Some(record)),
            None => (None, None),
        };

        Ok(SubjectSnapshot {
            sandbox,
            parsed_lifecycle,
            owned_stop,
            owned_stop_record,
            initiated_stop,
            inputs,
            attributed,
        })
    }

    async fn upgrade_running(
        &self,
        campaign: &Campaign,
        item: &LeasedItem,
        snapshot: &SubjectSnapshot,
        continuing: bool,
    ) -> ItemOutcome {
        if snapshot.sandbox.execution.is_none() {
            return ItemOutcome::Release(ItemError::new(UNPUBLISHED_EXECUTION));
        }
        if snapshot.attributed != Some(campaign.from) {
            if let Some(initiated) = &snapshot.initiated_stop
                && let Err(error) = self
                    .dispatcher
                    .clear_stopped_at(item, RecordedStop::Initiated(initiated.clone()))
                    .await
            {
                return error_outcome(error);
            }
            return ItemOutcome::Skipped(ItemError::new(NO_LONGER_ON_SOURCE));
        }
        match self.still_force_restarts(&snapshot.inputs, campaign).await {
            Ok(true) => {}
            Ok(false) => {
                // Release rather than skip while the sandbox is still on
                // `campaign.from`: a temporary hold (such as memory retirement
                // cleared on a deprecated source) shouldn't strand an item at
                // `MAX_ATTEMPTS` or force a full rescan once cleared.
                return ItemOutcome::Release(ItemError::new(POLICY_STOPPED_FORCE_RESTARTING));
            }
            Err(outcome) => return outcome,
        }
        if !continuing {
            match store::mark_run_started(&self.pool, item).await {
                Ok(true) => {}
                Ok(false) => {
                    return ItemOutcome::Release(ItemError::new(
                        "the campaign item lease is no longer active",
                    ));
                }
                Err(error) => {
                    return ItemOutcome::Retry(ItemError::new(format!(
                        "recording campaign item run start failed: {error}"
                    )));
                }
            }
        }

        let sandbox_id = SandboxId::from_uuid(item.subject_id);
        let stop = match self
            .dispatcher
            .stop(item, true, sandbox_id, campaign.from, campaign.to)
            .await
        {
            Ok(GuardedStop::Stopped(stop)) => stop,
            Ok(GuardedStop::Held(reason)) => {
                return self
                    .recover_after_declined_stop(campaign, item, true, reason)
                    .await;
            }
            Ok(GuardedStop::Declined(reason)) => {
                return self
                    .recover_after_declined_stop(campaign, item, false, reason)
                    .await;
            }
            Err(error) => return error_outcome(error),
        };

        self.finish_after_stop(campaign, item, stop).await
    }

    async fn settle_initiated_stop(
        &self,
        campaign: &Campaign,
        item: &LeasedItem,
        snapshot: &SubjectSnapshot,
        initiated: InitiatedStop,
    ) -> ItemOutcome {
        if let Err(outcome) = self
            .admit_start(campaign, &snapshot.sandbox, &snapshot.inputs)
            .await
        {
            if matches!(outcome, ItemOutcome::Skipped(_))
                && let Err(error) = self
                    .dispatcher
                    .clear_stopped_at(item, RecordedStop::Initiated(initiated))
                    .await
            {
                return error_outcome(error);
            }
            return outcome;
        }

        let sandbox_id = SandboxId::from_uuid(item.subject_id);
        match self
            .dispatcher
            .stop(item, true, sandbox_id, campaign.from, campaign.to)
            .await
        {
            Ok(GuardedStop::Stopped(stop)) => self.finish_after_stop(campaign, item, stop).await,
            Ok(GuardedStop::Held(reason)) => {
                self.recover_after_declined_stop(campaign, item, true, reason)
                    .await
            }
            Ok(GuardedStop::Declined(reason)) => {
                self.recover_after_declined_stop(campaign, item, false, reason)
                    .await
            }
            Err(error) => error_outcome(error),
        }
    }

    async fn finish_after_stop(
        &self,
        campaign: &Campaign,
        item: &LeasedItem,
        stop: StopFence,
    ) -> ItemOutcome {
        // Re-read the settled sandbox row plus policy, catalog, and memory
        // retirement after `stop` commits: if another transition already moved
        // the sandbox off this item's owned stop or authorization changed while
        // `stop` was in flight, classify the post-stop state rather than
        // starting blindly or skipping a completed cold-boot onto `campaign.to`.
        let (fresh_sandbox, fresh_changed_at, fresh_parsed) =
            match self.read_settled_subject(item.subject_id).await {
                Ok(settled) => settled,
                Err(outcome) => {
                    if matches!(outcome, ItemOutcome::Skipped(_))
                        && let Err(error) = self
                            .dispatcher
                            .clear_stopped_at(item, RecordedStop::Stopped(stop))
                            .await
                    {
                        return error_outcome(error);
                    }
                    return outcome;
                }
            };
        let still_owned = match self
            .stop_still_owned(
                item.subject_id,
                &fresh_sandbox,
                fresh_changed_at,
                fresh_parsed,
                &stop,
            )
            .await
        {
            Ok(still_owned) => still_owned,
            Err(outcome) => return outcome,
        };
        if !still_owned {
            let expected_record = RecordedStop::Stopped(stop.clone());
            return self
                .recover_after_post_stop_move(
                    campaign,
                    item,
                    stop,
                    expected_record,
                    false,
                    "the sandbox changed lifecycle since this campaign stopped it".to_owned(),
                )
                .await;
        }
        let fresh_inputs = match Inputs::read(&self.pool).await {
            Ok(inputs) => inputs,
            Err(error) => return ItemOutcome::Retry(ItemError::new(error)),
        };
        let expected_record = RecordedStop::Stopped(stop.clone());
        self.start_owned_stop(
            campaign,
            item,
            &fresh_sandbox,
            &fresh_inputs,
            stop,
            expected_record,
        )
        .await
    }

    async fn recover_after_declined_stop(
        &self,
        campaign: &Campaign,
        item: &LeasedItem,
        held: bool,
        declined_reason: String,
    ) -> ItemOutcome {
        // Always check `item_stop(item)` here even on attempt 1: `upgrade`
        // already cleared any pre-run recorded stop, and `dispatcher.stop` just
        // ran on this started run, so an internally retried `stop` whose first
        // RPC settled `('stopped', stop.stopped_at)` before its retry saw
        // `Declined` recorded `Stopped` for this run.
        let snapshot = match self.inspect_subject(campaign, item, true).await {
            Ok(snapshot) => snapshot,
            Err(outcome) => return outcome,
        };
        if let Some(stop) = snapshot.owned_stop {
            let expected_record = snapshot
                .owned_stop_record
                .unwrap_or_else(|| RecordedStop::Stopped(stop.clone()));
            return self
                .start_owned_stop(
                    campaign,
                    item,
                    &snapshot.sandbox,
                    &snapshot.inputs,
                    stop,
                    expected_record,
                )
                .await;
        }
        let initiated_stop = snapshot.initiated_stop;
        if let Some(initiated) = initiated_stop.clone()
            && snapshot.parsed_lifecycle == Some(SandboxLifecycle::Stopped)
        {
            if held {
                return ItemOutcome::Release(ItemError::new(declined_reason));
            }
            return match self
                .admit_start(campaign, &snapshot.sandbox, &snapshot.inputs)
                .await
            {
                Ok(()) | Err(ItemOutcome::Skipped(_)) => {
                    if let Err(error) = self
                        .dispatcher
                        .clear_stopped_at(item, RecordedStop::Initiated(initiated))
                        .await
                    {
                        return error_outcome(error);
                    }
                    ItemOutcome::Skipped(ItemError::new(declined_reason))
                }
                Err(outcome) => outcome,
            };
        }
        if matches!(
            snapshot.parsed_lifecycle,
            Some(SandboxLifecycle::Started | SandboxLifecycle::Paused)
        ) {
            if snapshot.sandbox.execution.is_none() {
                return ItemOutcome::Release(ItemError::new(UNPUBLISHED_EXECUTION));
            }
            if snapshot.attributed == Some(campaign.from) {
                if held {
                    return ItemOutcome::Release(ItemError::new(declined_reason));
                }
                match self.still_force_restarts(&snapshot.inputs, campaign).await {
                    Ok(false) => return ItemOutcome::Release(ItemError::new(declined_reason)),
                    Ok(true) => {}
                    Err(outcome) => return outcome,
                }
            }
        }
        if let Some(initiated) = initiated_stop
            && let Err(error) = self
                .dispatcher
                .clear_stopped_at(item, RecordedStop::Initiated(initiated))
                .await
        {
            return error_outcome(error);
        }
        ItemOutcome::Skipped(ItemError::new(declined_reason))
    }

    async fn admit_start(
        &self,
        campaign: &Campaign,
        sandbox: &SubjectSandbox,
        inputs: &Inputs,
    ) -> Result<(), ItemOutcome> {
        let effective = sandbox.effective_execution();
        let attributed = effective.and_then(|execution| inputs.attributed(execution));
        if attributed == Some(campaign.from) || effective.is_none() {
            if !self.still_force_restarts(inputs, campaign).await? {
                // Keep a campaign-stopped item pending across a temporary hold
                // so it finishes cold-booting once the hold clears, instead of
                // stranding it in `skipped` where `SETTLED_SCANNED_LIFECYCLES`
                // won't scan it again.
                return Err(ItemOutcome::Release(ItemError::new(
                    POLICY_STOPPED_FORCE_RESTARTING,
                )));
            }
            Ok(())
        } else if attributed == Some(campaign.to) {
            let retirement = self
                .read_memory_retirement(campaign.from)
                .await
                .map_err(|error| ItemOutcome::Retry(ItemError::new(error)))?;
            if inputs.policy.default_environment != Some(campaign.to)
                || inputs.policy.environments.get(&campaign.from)
                    == Some(&RuntimeEnvironmentState::Allowed)
                || !retirement.retired
            {
                return Err(ItemOutcome::Release(ItemError::new(
                    POLICY_STOPPED_FORCE_RESTARTING,
                )));
            }
            Ok(())
        } else if effective.is_some_and(Inputs::is_unbound) {
            // An unbound sandbox this campaign already attributed to
            // `campaign.from` and stopped can temporarily read as unattributed
            // or reattributed to another environment if `legacy_unbound_allowed`
            // or the catalog shifts before `start` runs; hold rather than
            // stranding it stopped in `skipped`.
            Err(ItemOutcome::Release(ItemError::new(
                POLICY_STOPPED_FORCE_RESTARTING,
            )))
        } else {
            Err(ItemOutcome::Skipped(ItemError::new(ON_NEITHER_END)))
        }
    }

    async fn start_owned_stop(
        &self,
        campaign: &Campaign,
        item: &LeasedItem,
        sandbox: &SubjectSandbox,
        inputs: &Inputs,
        stop: StopFence,
        expected_record: RecordedStop,
    ) -> ItemOutcome {
        if let Err(outcome) = self
            .check_no_external_stop_since(
                item,
                stop.initiated_at,
                stop.dispatched_at,
                expected_record.clone(),
            )
            .await
        {
            return outcome;
        }
        if let Err(outcome) = self.admit_start(campaign, sandbox, inputs).await {
            if matches!(outcome, ItemOutcome::Skipped(_))
                && let Err(error) = self
                    .dispatcher
                    .clear_stopped_at(item, expected_record)
                    .await
            {
                return error_outcome(error);
            }
            return outcome;
        }
        let sandbox_id = SandboxId::from_uuid(item.subject_id);
        match self
            .dispatcher
            .start(item, sandbox_id, stop.clone(), campaign.from, campaign.to)
            .await
        {
            Ok(TargetStart::Started | TargetStart::AlreadyOnTarget) => {
                self.mark_target_start_settled(item).await;
                ItemOutcome::Done
            }
            Ok(TargetStart::HeldStopped(reason)) => ItemOutcome::Release(ItemError::new(reason)),
            Ok(TargetStart::Declined(reason)) => {
                self.recover_after_post_stop_move(
                    campaign,
                    item,
                    stop,
                    expected_record,
                    true,
                    reason,
                )
                .await
            }
            Ok(TargetStart::NotOnTarget(reason)) => {
                self.recover_after_post_stop_move(
                    campaign,
                    item,
                    stop,
                    expected_record,
                    false,
                    reason,
                )
                .await
            }
            // `stop` (or `StartingOnTarget(stop)` once `'starting'` was claimed)
            // stays recorded on the item when `start` fails, including on the
            // final attempt: if the campaign pauses at its failure budget and an
            // operator resumes it with `retry_failed`, the continuing run still
            // finishes booting the sandbox this campaign stopped.
            Err(error) => error_outcome(error),
        }
    }

    async fn recover_after_post_stop_move(
        &self,
        campaign: &Campaign,
        item: &LeasedItem,
        stop: StopFence,
        expected_record: RecordedStop,
        declined_by_start: bool,
        reason: String,
    ) -> ItemOutcome {
        let settled = self.read_settled_subject(item.subject_id).await;
        let recorded = match self.dispatcher.item_stop(item).await {
            Ok(recorded) => recorded,
            Err(error) => return error_outcome(error),
        };
        match self.target_start_settled(item, &recorded).await {
            Ok(true) => return ItemOutcome::Done,
            Ok(false) => {}
            Err(outcome) => return outcome,
        }
        let clear_record = match &recorded {
            Some(current @ (RecordedStop::Stopped(_) | RecordedStop::StartingOnTarget(_))) => {
                current.clone()
            }
            _ => expected_record,
        };
        let (sandbox, lifecycle_changed_at, parsed_lifecycle) = match settled {
            Ok(settled) => settled,
            Err(outcome) => {
                if matches!(outcome, ItemOutcome::Skipped(_))
                    && let Err(error) = self.dispatcher.clear_stopped_at(item, clear_record).await
                {
                    return error_outcome(error);
                }
                return outcome;
            }
        };
        let inputs = match Inputs::read(&self.pool).await {
            Ok(inputs) => inputs,
            Err(error) => return ItemOutcome::Retry(ItemError::new(error)),
        };
        let attributed = sandbox
            .effective_execution()
            .and_then(|execution| inputs.attributed(execution));
        let still_owned = match self
            .stop_still_owned(
                item.subject_id,
                &sandbox,
                lifecycle_changed_at,
                parsed_lifecycle,
                &stop,
            )
            .await
        {
            Ok(still_owned) => still_owned,
            Err(outcome) => return outcome,
        };
        if still_owned {
            if let Err(outcome) = self
                .check_no_external_stop_since(
                    item,
                    stop.initiated_at,
                    stop.dispatched_at,
                    clear_record.clone(),
                )
                .await
            {
                return outcome;
            }
            if declined_by_start {
                // `dispatcher.start` returned `TargetStart::Declined` rather
                // than `HeldStopped` or `NotOnTarget` while this item still
                // owns the stop, meaning starting this sandbox was permanently
                // refused (for example, its template release is permanently
                // incompatible or its recorded execution moved off both
                // campaign ends); clear stop ownership and skip rather than
                // spinning in `Release`.
                if let Err(error) = self.dispatcher.clear_stopped_at(item, clear_record).await {
                    return error_outcome(error);
                }
                return ItemOutcome::Skipped(ItemError::new(reason));
            }
            return match self.admit_start(campaign, &sandbox, &inputs).await {
                Ok(()) => ItemOutcome::Release(ItemError::new(reason)),
                Err(ItemOutcome::Skipped(_)) => {
                    if let Err(error) = self.dispatcher.clear_stopped_at(item, clear_record).await {
                        return error_outcome(error);
                    }
                    ItemOutcome::Skipped(ItemError::new(reason))
                }
                Err(outcome) => outcome,
            };
        }
        if let Some(RecordedStop::StartingOnTarget(recorded_claim)) = &recorded {
            let recorded_stop = &recorded_claim.stop;
            if let Err(outcome) = self
                .check_no_external_stop_since(
                    item,
                    recorded_stop.initiated_at,
                    recorded_stop.dispatched_at,
                    RecordedStop::StartingOnTarget(recorded_claim.clone()),
                )
                .await
            {
                return outcome;
            }
            let externally_activated = match self
                .external_activation_succeeded_since(item.subject_id, recorded_stop.stopped_at)
                .await
            {
                Ok(activated) => activated,
                Err(outcome) => return outcome,
            };
            if !externally_activated
                && matches!(
                    parsed_lifecycle,
                    Some(SandboxLifecycle::Started | SandboxLifecycle::Paused)
                )
            {
                if sandbox.execution.is_none() {
                    return ItemOutcome::Release(ItemError::new(UNPUBLISHED_EXECUTION));
                }
                if attributed == Some(campaign.to) {
                    self.mark_target_start_settled(item).await;
                    return ItemOutcome::Done;
                }
            }
        } else if parsed_lifecycle == Some(SandboxLifecycle::Stopped)
            && let Err(outcome) = self
                .check_no_external_stop_since(
                    item,
                    stop.initiated_at,
                    stop.dispatched_at,
                    clear_record.clone(),
                )
                .await
        {
            return outcome;
        }
        if let Err(error) = self.dispatcher.clear_stopped_at(item, clear_record).await {
            return error_outcome(error);
        }
        if matches!(
            parsed_lifecycle,
            Some(SandboxLifecycle::Started | SandboxLifecycle::Paused)
        ) {
            if sandbox.execution.is_none() {
                return ItemOutcome::Release(ItemError::new(UNPUBLISHED_EXECUTION));
            }
            if attributed == Some(campaign.from) {
                match self.still_force_restarts(&inputs, campaign).await {
                    Ok(false) => return ItemOutcome::Release(ItemError::new(reason)),
                    Ok(true) => {}
                    Err(outcome) => return outcome,
                }
            }
        }
        ItemOutcome::Skipped(ItemError::new(reason))
    }
}

fn sandbox_timestamp(value: time::OffsetDateTime) -> Option<jiff::Timestamp> {
    jiff::Timestamp::from_nanosecond(value.unix_timestamp_nanos()).ok()
}

fn offset_timestamp(value: jiff::Timestamp) -> Option<time::OffsetDateTime> {
    time::OffsetDateTime::from_unix_timestamp_nanos(value.as_nanosecond()).ok()
}

enum PlanChange {
    Hold(&'static str),
    ReadError(String),
}

#[async_trait]
impl<D, M> CampaignDriver for SandboxForceRestartDriver<D, M>
where
    D: SandboxUpgradeDispatcher,
    M: MemoryRetirementSource,
{
    fn kind(&self) -> CampaignKind {
        CampaignKind::SandboxForceRestarts
    }

    async fn plan(&self, campaign: &Campaign) -> Result<Plan, String> {
        match self.read_plan(campaign).await {
            Ok((_, plan)) => Ok(Plan::Scan(plan)),
            Err(PlanChange::Hold(reason)) => Ok(Plan::Hold(reason)),
            Err(PlanChange::ReadError(error)) => Err(error),
        }
    }

    async fn find_subjects(
        &self,
        campaign: &Campaign,
        plan: ScanPlan,
        after: Option<Uuid>,
        limit: u32,
    ) -> Result<SubjectScan, String> {
        let (inputs, now) = match self.read_plan(campaign).await {
            Ok(current) => current,
            Err(PlanChange::Hold(_)) => return Ok(SubjectScan::PlanChanged),
            Err(PlanChange::ReadError(error)) => return Err(error),
        };
        if now != plan {
            return Ok(SubjectScan::PlanChanged);
        }
        page_force_restart_candidates(&inputs, &self.pool, campaign, self.managed_by, after, limit)
            .await
            .map(SubjectScan::Page)
    }

    async fn stranded_lease_candidates(
        &self,
        campaign: &Campaign,
        scan_limit: u32,
    ) -> Result<Vec<Uuid>, String> {
        self.select_stranded_candidates(campaign, scan_limit).await
    }

    fn forget_finished(&self, unfinished: &[Campaign]) {
        self.stranded_cursors
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .retain(|campaign_id, _| {
                unfinished
                    .iter()
                    .any(|campaign| campaign.id == *campaign_id)
            });
    }

    async fn upgrade(&self, campaign: &Campaign, item: &LeasedItem) -> ItemOutcome {
        let continuing = match store::continuing_run(&self.pool, item).await {
            Ok(Some(continuing)) => continuing,
            Ok(None) => {
                return ItemOutcome::Release(ItemError::new(
                    "the campaign item lease is no longer active",
                ));
            }
            Err(error) => {
                return ItemOutcome::Retry(ItemError::new(format!(
                    "reading campaign item continuation failed: {error}"
                )));
            }
        };
        if !continuing {
            match self.dispatcher.item_stop(item).await {
                Ok(Some(stale)) => {
                    if let Err(error) = self.dispatcher.clear_stopped_at(item, stale).await {
                        return error_outcome(error);
                    }
                }
                Ok(None) => {}
                Err(error) => return error_outcome(error),
            }
        }
        let snapshot = match self.inspect_subject(campaign, item, continuing).await {
            Ok(snapshot) => snapshot,
            Err(outcome) => return outcome,
        };
        match force_restart_step(
            &snapshot.sandbox.lifecycle,
            snapshot.owned_stop.clone(),
            snapshot.initiated_stop.clone(),
        ) {
            ForceRestartStep::StopThenStart => {
                self.upgrade_running(campaign, item, &snapshot, continuing)
                    .await
            }
            ForceRestartStep::SettleInitiatedStop(initiated) => {
                self.settle_initiated_stop(campaign, item, &snapshot, initiated)
                    .await
            }
            ForceRestartStep::StartStopped(stop) => {
                let expected_record = snapshot
                    .owned_stop_record
                    .unwrap_or_else(|| RecordedStop::Stopped(stop.clone()));
                self.start_owned_stop(
                    campaign,
                    item,
                    &snapshot.sandbox,
                    &snapshot.inputs,
                    stop,
                    expected_record,
                )
                .await
            }
            ForceRestartStep::Release(reason) => ItemOutcome::Release(ItemError::new(reason)),
            ForceRestartStep::Skip(reason) => ItemOutcome::Skipped(ItemError::new(reason)),
        }
    }
}
