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
//! * **Subjects.** Published `started` and `paused` sandboxes of the
//!   substrate that the catalog attributes to the source, including unbound
//!   executions attributed under `legacy_unbound_allowed`. A candidate
//!   attributed elsewhere retires any skipped item left from when it was on
//!   the source.
//! * **Upgrade.** Re-reads the substrate's durable [`ForceRestartProgress`],
//!   the sandbox, and the policy fresh. A running or paused sandbox still on
//!   the source under a policy that still force-restarts it is stopped guarded
//!   on the source ([`SandboxUpgradeDispatcher::stop`], stamping
//!   `sandboxes.lifecycle_changed_at` and recording
//!   [`ForceRestartProgress::Stopped`] in the substrate) and then started
//!   guarded on that stopped period so its start-time upgrade cold-boots it
//!   onto the target ([`SandboxUpgradeDispatcher::start`]). A retry, lapsed
//!   lease redrive, `:resume` with `retry_failed`, or replacement controller
//!   reads [`ForceRestartProgress`] from the substrate: an item still in the
//!   fenced `stopped_at` period skips straight to the start whereas a sandbox
//!   the user stopped is skipped; one an earlier attempt already started onto
//!   the target finishes as `done` even if recording that outcome failed or the
//!   controller restarted; and a temporary policy or catalog hold on an item
//!   already stopped by the campaign releases the lease without stranding it in
//!   `skipped`. A declined stop or a move to anything other than the target is
//!   a skip, and a sandbox mid-transition is released without spending an
//!   attempt.

use std::sync::Arc;

use async_trait::async_trait;
use pplx_space_runtime_upgrade_campaign_store::{
    self as store, Campaign, CampaignId, CampaignKind, ItemError, ItemOutcome, LeasedItem,
};
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use sqlx::PgPool;
use uuid::Uuid;

use super::dispatcher::{
    ForceRestartProgress, GuardedStop, SandboxUpgradeDispatcher, TargetStart, is_mid_transition,
};
use super::{
    CampaignDriver, Inputs, MemoryRetirementSource, NO_LONGER_ON_SOURCE, ON_NEITHER_END, Plan,
    PlanChange, ScanPlan, SubjectScan, error_outcome,
};
pub(super) use super::{
    RetiredMemoryPlan as ForceRestartPlan, retired_memory_plan as force_restart_plan,
};

pub(super) const POLICY_STOPPED_FORCE_RESTARTING: &str =
    "the policy no longer force-restarts this campaign's sandboxes";

const RESIDENT_LIFECYCLES: &[SandboxLifecycle] =
    &[SandboxLifecycle::Started, SandboxLifecycle::Paused];

/// Drives `sandbox_force_restarts` campaigns of one substrate's running and
/// paused sandboxes.
///
/// Stop ownership and target-start completion live in the substrate behind
/// [`SandboxUpgradeDispatcher::force_restart_progress`], so retries after a
/// dropped or timed-out call, a lapsed lease, or a controller restart finish
/// booting sandboxes this campaign stopped without ever booting one a user
/// stopped.
pub struct SandboxForceRestartDriver<D, M> {
    pool: PgPool,
    /// The substrate's `sandboxes.managed_by`: only its sandboxes are found.
    managed_by: &'static str,
    dispatcher: Arc<D>,
    memory_retirement: Arc<M>,
}

impl<D: SandboxUpgradeDispatcher, M: MemoryRetirementSource> SandboxForceRestartDriver<D, M> {
    /// A driver for the running and paused sandboxes `managed_by` owns
    /// (`SubstrateAdapter::managed_by`), stopped and cold-booted onto the
    /// default environment through `dispatcher` while `memory_retirement`
    /// reports the campaign's source memory retired.
    pub fn new(
        pool: PgPool,
        managed_by: &'static str,
        dispatcher: D,
        memory_retirement: M,
    ) -> Self {
        Self {
            pool,
            managed_by,
            dispatcher: Arc::new(dispatcher),
            memory_retirement: Arc::new(memory_retirement),
        }
    }

    async fn skip_clearing_progress(
        &self,
        campaign_id: CampaignId,
        sandbox_id: SandboxId,
        had_progress: bool,
        reason: impl AsRef<str>,
    ) -> ItemOutcome {
        if had_progress
            && let Err(error) = self
                .dispatcher
                .clear_force_restart_progress(campaign_id, sandbox_id)
                .await
        {
            return error_outcome(error);
        }
        ItemOutcome::Skipped(ItemError::new(reason))
    }

    async fn read_plan(&self, campaign: &Campaign) -> Result<(Inputs, ScanPlan), PlanChange> {
        Inputs::read_retired_memory_plan(&self.pool, &*self.memory_retirement, campaign).await
    }
}

fn sandbox_timestamp(value: time::OffsetDateTime) -> Option<jiff::Timestamp> {
    jiff::Timestamp::from_nanosecond(value.unix_timestamp_nanos()).ok()
}

/// What to do with a leased sandbox in `lifecycle`.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum ForceRestartStep {
    /// Guarded stop on the source, then start onto the target.
    StopThenStart,
    /// Start a sandbox this campaign already stopped at `stopped_at`.
    StartStopped(jiff::Timestamp),
    /// Leave the item for a later lease, without spending an attempt.
    Release(String),
    /// Not this campaign's to move.
    Skip(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActiveStep {
    StopThenStart,
    StartStopped(jiff::Timestamp),
}

pub(super) fn force_restart_step(
    lifecycle: &str,
    stopped_at: Option<jiff::Timestamp>,
) -> ForceRestartStep {
    let Ok(parsed) = lifecycle.parse::<SandboxLifecycle>() else {
        return ForceRestartStep::Skip(format!(
            "the sandbox's lifecycle {lifecycle:?} isn't upgradable"
        ));
    };
    if is_mid_transition(parsed) {
        return ForceRestartStep::Release(format!(
            "the sandbox is {lifecycle}; retrying once it settles"
        ));
    }
    match parsed {
        SandboxLifecycle::Started | SandboxLifecycle::Paused => ForceRestartStep::StopThenStart,
        SandboxLifecycle::Stopped => match stopped_at {
            Some(stopped_at) => ForceRestartStep::StartStopped(stopped_at),
            None => ForceRestartStep::Skip(format!(
                "the sandbox is {lifecycle}, which this campaign doesn't upgrade"
            )),
        },
        _ => ForceRestartStep::Skip(format!(
            "the sandbox is {lifecycle}, which this campaign doesn't upgrade"
        )),
    }
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

    fn retain_campaigns(&self, unfinished: &[Campaign]) {
        let ids: Vec<CampaignId> = unfinished.iter().map(|campaign| campaign.id).collect();
        self.dispatcher.retain_force_restart_campaigns(&ids);
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
        inputs
            .page_candidates(
                &self.pool,
                campaign,
                self.managed_by,
                RESIDENT_LIFECYCLES,
                after,
                limit,
            )
            .await
            .map(SubjectScan::Page)
    }

    async fn upgrade(&self, campaign: &Campaign, item: &LeasedItem) -> ItemOutcome {
        let sandbox_id = SandboxId::from_uuid(item.subject_id);
        let progress = match self
            .dispatcher
            .force_restart_progress(campaign.id, sandbox_id)
            .await
        {
            Ok(progress) => progress,
            Err(error) => return error_outcome(error),
        };
        // Once `dispatcher.start` succeeded on an earlier lease of this item,
        // the sandbox has already moved for this campaign; only recording
        // `Done` in Postgres remained.
        if progress == Some(ForceRestartProgress::StartedOnTarget) {
            return ItemOutcome::Done;
        }

        let sandbox = match store::subject_sandbox(&self.pool, item.subject_id).await {
            Ok(Some(sandbox)) => sandbox,
            Ok(None) => {
                return self
                    .skip_clearing_progress(
                        campaign.id,
                        sandbox_id,
                        progress.is_some(),
                        "the sandbox was deleted",
                    )
                    .await;
            }
            Err(error) => {
                return ItemOutcome::Retry(ItemError::new(format!(
                    "reading the sandbox failed: {error}"
                )));
            }
        };
        let Some(lifecycle_changed_at) = sandbox_timestamp(sandbox.lifecycle_changed_at) else {
            return ItemOutcome::Retry(ItemError::new(format!(
                "the sandbox's lifecycle_changed_at {} is out of jiff's timestamp range",
                sandbox.lifecycle_changed_at
            )));
        };
        let had_driver_stop = matches!(progress, Some(ForceRestartProgress::Stopped { .. }));
        let owned_stopped_at = match progress {
            Some(ForceRestartProgress::Stopped { stopped_at })
                if sandbox.lifecycle.parse::<SandboxLifecycle>()
                    == Ok(SandboxLifecycle::Stopped)
                    && lifecycle_changed_at == stopped_at =>
            {
                Some(stopped_at)
            }
            _ => None,
        };
        if let Ok(parsed) = sandbox.lifecycle.parse::<SandboxLifecycle>()
            && is_mid_transition(parsed)
        {
            return ItemOutcome::Release(ItemError::new(format!(
                "the sandbox is {}; retrying once it settles",
                sandbox.lifecycle
            )));
        }

        let inputs = match Inputs::read(&self.pool).await {
            Ok(inputs) => inputs,
            Err(error) => return ItemOutcome::Retry(ItemError::new(error)),
        };
        let attributed = inputs.attributed(&sandbox.execution);
        // An earlier attempt whose start landed on the target before its lease
        // lapsed (or a concurrent start after this campaign's stop) finished
        // the move, even if the sandbox was later stopped again or suspended;
        // record completion without disturbing its current state.
        if had_driver_stop
            && owned_stopped_at.is_none()
            && matches!(
                sandbox.lifecycle.parse::<SandboxLifecycle>(),
                Ok(SandboxLifecycle::Started
                    | SandboxLifecycle::Paused
                    | SandboxLifecycle::Stopped
                    | SandboxLifecycle::Suspended)
            )
            && attributed == Some(campaign.to)
        {
            return ItemOutcome::Done;
        }

        let step = match force_restart_step(&sandbox.lifecycle, owned_stopped_at) {
            ForceRestartStep::StopThenStart => ActiveStep::StopThenStart,
            ForceRestartStep::StartStopped(stopped_at) => ActiveStep::StartStopped(stopped_at),
            ForceRestartStep::Release(reason) => {
                return ItemOutcome::Release(ItemError::new(reason));
            }
            ForceRestartStep::Skip(reason) => {
                return self
                    .skip_clearing_progress(campaign.id, sandbox_id, progress.is_some(), reason)
                    .await;
            }
        };

        match step {
            ActiveStep::StopThenStart => {
                if attributed != Some(campaign.from) {
                    return self
                        .skip_clearing_progress(
                            campaign.id,
                            sandbox_id,
                            progress.is_some(),
                            NO_LONGER_ON_SOURCE,
                        )
                        .await;
                }
                let retirement = match self
                    .memory_retirement
                    .memory_retirement(campaign.from)
                    .await
                {
                    Ok(retirement) => retirement,
                    Err(error) => {
                        return ItemOutcome::Retry(ItemError::new(format!(
                            "reading memory retirement failed: {error}"
                        )));
                    }
                };
                if force_restart_plan(
                    &inputs.policy,
                    inputs.catalog_entry(campaign.from),
                    retirement.retired,
                    campaign,
                ) != (ForceRestartPlan::Scan { moves: true })
                {
                    return self
                        .skip_clearing_progress(
                            campaign.id,
                            sandbox_id,
                            progress.is_some(),
                            POLICY_STOPPED_FORCE_RESTARTING,
                        )
                        .await;
                }
            }
            ActiveStep::StartStopped(_) => {
                if attributed == Some(campaign.from) {
                    let retirement = match self
                        .memory_retirement
                        .memory_retirement(campaign.from)
                        .await
                    {
                        Ok(retirement) => retirement,
                        Err(error) => {
                            return ItemOutcome::Retry(ItemError::new(format!(
                                "reading memory retirement failed: {error}"
                            )));
                        }
                    };
                    if force_restart_plan(
                        &inputs.policy,
                        inputs.catalog_entry(campaign.from),
                        retirement.retired,
                        campaign,
                    ) != (ForceRestartPlan::Scan { moves: true })
                    {
                        // Keep a campaign-stopped item pending across a temporary
                        // hold so it finishes cold-booting once the hold clears,
                        // instead of stranding it in `skipped` where
                        // `RESIDENT_LIFECYCLES` won't scan it again.
                        return ItemOutcome::Release(ItemError::new(
                            POLICY_STOPPED_FORCE_RESTARTING,
                        ));
                    }
                } else if attributed == Some(campaign.to) {
                    if inputs.policy.default_environment != Some(campaign.to) {
                        return ItemOutcome::Release(ItemError::new(
                            POLICY_STOPPED_FORCE_RESTARTING,
                        ));
                    }
                } else if attributed.is_none() && Inputs::is_published_unbound(&sandbox.execution) {
                    // An unbound sandbox this campaign already attributed to
                    // `campaign.from` and stopped can temporarily read as
                    // unattributed if `legacy_unbound_allowed` or the catalog
                    // shifts before `start` runs; hold rather than stranding it
                    // stopped in `skipped`.
                    return ItemOutcome::Release(ItemError::new(POLICY_STOPPED_FORCE_RESTARTING));
                } else {
                    return self
                        .skip_clearing_progress(
                            campaign.id,
                            sandbox_id,
                            progress.is_some(),
                            ON_NEITHER_END,
                        )
                        .await;
                }
            }
        }

        let stopped_at = match step {
            ActiveStep::StopThenStart => {
                if progress.is_some()
                    && let Err(error) = self
                        .dispatcher
                        .clear_force_restart_progress(campaign.id, sandbox_id)
                        .await
                {
                    return error_outcome(error);
                }
                match self
                    .dispatcher
                    .stop(Some(campaign.id), sandbox_id, campaign.from)
                    .await
                {
                    Ok(GuardedStop::Stopped { stopped_at }) => stopped_at,
                    Ok(GuardedStop::Declined(reason)) => {
                        return self
                            .skip_clearing_progress(campaign.id, sandbox_id, false, reason)
                            .await;
                    }
                    Err(error) => return error_outcome(error),
                }
            }
            ActiveStep::StartStopped(stopped_at) => stopped_at,
        };
        match self
            .dispatcher
            .start(campaign.id, sandbox_id, stopped_at, campaign.to)
            .await
        {
            Ok(TargetStart::Started | TargetStart::AlreadyOnTarget) => ItemOutcome::Done,
            Ok(TargetStart::HeldStopped(reason)) => ItemOutcome::Release(ItemError::new(reason)),
            Ok(TargetStart::NotOnTarget(reason)) => {
                self.skip_clearing_progress(campaign.id, sandbox_id, true, reason)
                    .await
            }
            // Substrate keeps `ForceRestartProgress::Stopped` when `start`
            // fails, including on the final attempt: if the campaign pauses at
            // its failure budget and an operator resumes it with `retry_failed`,
            // the retried lease still finishes booting the sandbox this
            // campaign stopped.
            Err(error) => error_outcome(error),
        }
    }
}
