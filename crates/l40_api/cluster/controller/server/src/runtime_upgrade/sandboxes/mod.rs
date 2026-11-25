//! The sandbox campaign driver: moving an environment's sandboxes to the
//! cluster default, through a [`SandboxUpgradeDispatcher`].
//!
//! * **Plan.** The start-time decision
//!   ([`pplx_space_api::controllers::substrate::runtime_upgrade::decide`])
//!   for a sandbox on the campaign's source says whether the campaign moves
//!   anything. It's held while its target isn't the cluster default
//!   (`target_not_default`; the node-side upgrade only boots under the
//!   default) or its source is `allowed` again (`source_allowed`). Its
//!   sandboxes are stranded when no certified edge reaches the target.
//!   Running and paused sandboxes count only when the campaign sets
//!   `include_running` and the move is one whose authorization accepts memory
//!   loss, which only a block of the source gives.
//! * **Subjects.** The substrate's published sandboxes, stopped ones and, when
//!   the plan takes them, started and paused ones, that the catalog
//!   attributes to the source the way a start attributes them
//!   ([`store::sandbox_candidates`]). A sandbox the catalog attributes
//!   elsewhere is reported as no longer on the source, which retires a
//!   skipped item of it. A skipped item with leases left whose sandbox is
//!   still on the source is found again and made pending, so it holds the
//!   campaign open until it moves or its attempts run out. One that stops
//!   after its part of a pass was scanned is left to the start-time upgrade.
//!   The plan's fingerprint covers the catalog, the legacy unbound setting
//!   and which lifecycles count, so a change of any restarts the pass.
//! * **Upgrade.** Decided on the sandbox as it is now, read fresh. A stopped
//!   sandbox on the source or target goes to
//!   [`SandboxUpgradeDispatcher::upgrade_stopped`] and stays stopped. A
//!   started or paused one, when the plan takes it and it's still on the
//!   source under a policy that still takes it, is stopped first
//!   ([`SandboxUpgradeDispatcher::stop`]) and then takes the same path. One
//!   mid-transition is released for a later lease; anything else is skipped.
//!
//! Nothing registers this driver in [`super::cluster_drivers`] yet: it runs
//! once a production dispatcher exists.

use std::sync::Arc;

use async_trait::async_trait;
use pplx_space_api::controllers::substrate::runtime_upgrade::{
    CatalogEnvironment, EnvironmentMove, RecordedExecution, RuntimePlan, UpgradeActivation,
    attribute, decide,
};
use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_runtime_environment_policy_store::{
    RuntimeEnvironmentPolicy, RuntimeEnvironmentState,
};
use pplx_space_runtime_upgrade_campaign_store::{
    self as store, Campaign, CampaignKind, ItemError, ItemOutcome, LeasedItem, SandboxCandidate,
};
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use sqlx::PgPool;
use uuid::Uuid;

use super::{CampaignDriver, MAX_ATTEMPTS, Plan, ScanPlan, SubjectPage, SubjectScan};

mod dispatcher;
mod force_restarts;

#[cfg(test)]
mod tests;

pub use dispatcher::{
    DispatchError, ForceRestartProgress, GuardedStop, SandboxUpgradeDispatcher, StoppedUpgrade,
    TargetStart,
};
use dispatcher::{Step, step};
pub use force_restarts::{MemoryRetirement, MemoryRetirementSource, SandboxForceRestartDriver};

pub(super) const NO_LONGER_ON_SOURCE: &str = "the sandbox is no longer on the campaign's source";
pub(super) const ON_NEITHER_END: &str =
    "the sandbox is on neither the campaign's source nor its target";
pub(super) const POLICY_STOPPED_MOVING_RUNNING: &str =
    "the policy no longer moves this campaign's running sandboxes";

/// Drives `sandboxes` campaigns of one substrate's sandboxes.
pub struct SandboxDriver<D> {
    pool: PgPool,
    /// The substrate's `sandboxes.managed_by`: only its sandboxes are found.
    managed_by: &'static str,
    dispatcher: Arc<D>,
}

impl<D: SandboxUpgradeDispatcher> SandboxDriver<D> {
    /// A driver for the sandboxes `managed_by` owns
    /// (`SubstrateAdapter::managed_by`), moved through `dispatcher`.
    pub fn new(pool: PgPool, managed_by: &'static str, dispatcher: D) -> Self {
        Self {
            pool,
            managed_by,
            dispatcher: Arc::new(dispatcher),
        }
    }
}

/// What a leasing sandbox campaign does under the current policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SandboxPlan {
    /// Nothing calls for the move; the reason is the hold metric's tag.
    Hold(&'static str),
    /// Find the source's sandboxes. `moves` says whether they move or are
    /// stranded; `takes_running` whether running and paused ones count.
    Scan { moves: bool, takes_running: bool },
}

/// Decide `campaign` as a start of one of its source's sandboxes would be
/// decided. `from_catalog` is the source's catalog row.
fn sandbox_plan(
    policy: &RuntimeEnvironmentPolicy,
    from_catalog: Option<&CatalogEnvironment>,
    campaign: &Campaign,
) -> SandboxPlan {
    if policy.default_environment != Some(campaign.to) {
        return SandboxPlan::Hold("target_not_default");
    }
    // A running sandbox loses its memory to the upgrade's restart, which
    // takes the consent a suspended sandbox's memory-discarding resume
    // needs: only a block of the source gives it.
    let takes_running = |to_default: &EnvironmentMove| {
        campaign.include_running
            && to_default
                .authorization(UpgradeActivation::Restore)
                .memory_loss
    };
    match decide(
        policy,
        UpgradeActivation::Start,
        campaign.from,
        from_catalog,
    ) {
        RuntimePlan::ColdBootUpgrade(to_default) => SandboxPlan::Scan {
            moves: true,
            takes_running: takes_running(&to_default),
        },
        RuntimePlan::Stranded(to_default) => SandboxPlan::Scan {
            moves: false,
            takes_running: takes_running(&to_default),
        },
        RuntimePlan::AsRecorded
            if policy.environments.get(&campaign.from)
                == Some(&RuntimeEnvironmentState::Allowed) =>
        {
            SandboxPlan::Hold("source_allowed")
        }
        // A deprecated source without an edge: its sandboxes still execute
        // where they are, and nothing moves them. A start never restores, so
        // `RestoreOnDefault` doesn't arise.
        RuntimePlan::AsRecorded | RuntimePlan::RestoreOnDefault(_) => SandboxPlan::Scan {
            moves: false,
            takes_running: false,
        },
    }
}

/// The lifecycles a pass scans: stopped, and running and paused too when the
/// campaign takes them.
const fn scanned_lifecycles(takes_running: bool) -> &'static [SandboxLifecycle] {
    if takes_running {
        &[
            SandboxLifecycle::Stopped,
            SandboxLifecycle::Started,
            SandboxLifecycle::Paused,
        ]
    } else {
        &[SandboxLifecycle::Stopped]
    }
}

/// The policy and the unretired catalog, as every decision reads them.
pub(super) struct Inputs {
    pub(super) policy: RuntimeEnvironmentPolicy,
    pub(super) catalog: Vec<CatalogEnvironment>,
}

impl Inputs {
    pub(super) async fn read(pool: &PgPool) -> Result<Self, String> {
        let policy = pplx_space_runtime_environment_policy_store::get(pool)
            .await
            .map_err(|error| format!("reading the policy failed: {error}"))?;
        let catalog = pplx_space_runtime_environment_catalog_store::unretired_environments(pool)
            .await
            .map_err(|error| format!("reading the catalog failed: {error}"))?
            .into_iter()
            .map(CatalogEnvironment::from_row)
            .collect::<Result<_, _>>()
            .map_err(|error| format!("reading the catalog failed: {error}"))?;
        Ok(Self { policy, catalog })
    }

    pub(super) fn catalog_entry(
        &self,
        reference: EnvironmentReference,
    ) -> Option<&CatalogEnvironment> {
        self.catalog
            .iter()
            .find(|environment| environment.reference == reference)
    }

    fn plan(&self, campaign: &Campaign) -> SandboxPlan {
        sandbox_plan(&self.policy, self.catalog_entry(campaign.from), campaign)
    }

    /// The loop's plan for `campaign`, and which lifecycles it scans.
    fn scan_plan(&self, campaign: &Campaign) -> Result<(ScanPlan, bool), &'static str> {
        match self.plan(campaign) {
            SandboxPlan::Hold(reason) => Err(reason),
            SandboxPlan::Scan {
                moves,
                takes_running,
            } => Ok((
                ScanPlan {
                    moves,
                    inputs: self.fingerprint(takes_running),
                },
                takes_running,
            )),
        }
    }

    /// A fingerprint of what judges a sandbox besides `moves`: the policy
    /// generation (so a policy hold-and-unhold between ticks restarts an open
    /// pass), the catalog and the legacy unbound setting, which attribute it
    /// to an environment, and any mode-specific selector (`extra`). Within one
    /// process, equal inputs give equal fingerprints.
    pub(super) fn fingerprint(&self, extra: impl std::hash::Hash) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.policy.generation.hash(&mut hasher);
        format!("{:?}", self.catalog).hash(&mut hasher);
        self.policy.legacy_unbound_allowed.hash(&mut hasher);
        extra.hash(&mut hasher);
        hasher.finish()
    }

    fn parse_published(execution: &SandboxCandidate) -> Option<RecordedExecution> {
        // A publish that carried no execution is no evidence, as a start's
        // read of the published execution rules: it isn't a record from
        // before execution metadata existed, which the legacy setting would
        // attribute.
        if execution.execution_metadata.is_none()
            && execution.runtime_version.is_none()
            && execution.state_format_version.is_none()
        {
            return None;
        }
        Some(RecordedExecution::from_record(
            execution.execution_metadata.as_deref(),
            execution.runtime.clone(),
            execution.runtime_version.clone(),
            execution.state_format_name.clone(),
            execution.state_format_version.clone(),
        ))
    }

    /// Whether `execution` carries a published unbound record (whose
    /// attribution depends on the catalog or `legacy_unbound_allowed`).
    pub(super) fn is_published_unbound(execution: &SandboxCandidate) -> bool {
        Self::parse_published(execution).is_some_and(|recorded| recorded.environment.is_none())
    }

    /// The environment `execution` puts its sandbox on, as a start
    /// attributes it.
    pub(super) fn attributed(&self, execution: &SandboxCandidate) -> Option<EnvironmentReference> {
        let recorded = Self::parse_published(execution)?;
        attribute(&recorded, &self.catalog, self.policy.legacy_unbound_allowed)
    }

    /// Whether `execution` puts its sandbox on `campaign`'s source.
    pub(super) fn on_source(&self, execution: &SandboxCandidate, campaign: &Campaign) -> bool {
        self.attributed(execution) == Some(campaign.from)
    }

    /// Read one page of `lifecycles` candidates for `campaign` and split them
    /// into those attributed to `campaign`'s source and those attributed
    /// elsewhere.
    pub(super) async fn page_candidates(
        &self,
        pool: &PgPool,
        campaign: &Campaign,
        managed_by: &str,
        lifecycles: &[SandboxLifecycle],
        after: Option<Uuid>,
        limit: u32,
    ) -> Result<SubjectPage, String> {
        let page = store::sandbox_candidates(
            pool,
            campaign.id,
            managed_by,
            lifecycles,
            after,
            limit,
            MAX_ATTEMPTS,
        )
        .await
        .map_err(|error| format!("reading candidates failed: {error}"))?;
        let (on_source, elsewhere): (Vec<&SandboxCandidate>, Vec<&SandboxCandidate>) = page
            .iter()
            .partition(|candidate| self.on_source(candidate, campaign));
        Ok(SubjectPage {
            on_source: on_source.iter().map(|c| c.sandbox_id).collect(),
            // A candidate the catalog attributes elsewhere can still carry a
            // skipped item from when it was on the source. Retired, it
            // neither comes back as a candidate nor holds the campaign open.
            elsewhere: elsewhere.iter().map(|c| c.sandbox_id).collect(),
            last: page.last().map(|candidate| candidate.sandbox_id),
            examined: page.len(),
        })
    }
}

#[async_trait]
impl<D: SandboxUpgradeDispatcher> CampaignDriver for SandboxDriver<D> {
    fn kind(&self) -> CampaignKind {
        CampaignKind::Sandboxes
    }

    async fn plan(&self, campaign: &Campaign) -> Result<Plan, String> {
        let inputs = Inputs::read(&self.pool).await?;
        Ok(match inputs.scan_plan(campaign) {
            Ok((plan, _)) => Plan::Scan(plan),
            Err(reason) => Plan::Hold(reason),
        })
    }

    async fn find_subjects(
        &self,
        campaign: &Campaign,
        plan: ScanPlan,
        after: Option<Uuid>,
        limit: u32,
    ) -> Result<SubjectScan, String> {
        // Read again rather than trusting `plan`'s inputs: the page is
        // judged under what's read here, so it must be what `plan` was made
        // from. A change since restarts the pass next tick.
        let inputs = Inputs::read(&self.pool).await?;
        let takes_running = match inputs.scan_plan(campaign) {
            Ok((now, takes_running)) if now == plan => takes_running,
            _ => return Ok(SubjectScan::PlanChanged),
        };
        inputs
            .page_candidates(
                &self.pool,
                campaign,
                self.managed_by,
                scanned_lifecycles(takes_running),
                after,
                limit,
            )
            .await
            .map(SubjectScan::Page)
    }

    async fn upgrade(&self, campaign: &Campaign, item: &LeasedItem) -> ItemOutcome {
        let sandbox_id = SandboxId::from_uuid(item.subject_id);
        let sandbox = match store::subject_sandbox(&self.pool, item.subject_id).await {
            Ok(Some(sandbox)) => sandbox,
            Ok(None) => return ItemOutcome::Skipped(ItemError::new("the sandbox was deleted")),
            Err(error) => {
                return ItemOutcome::Retry(ItemError::new(format!(
                    "reading the sandbox failed: {error}"
                )));
            }
        };
        let step = step(&sandbox.lifecycle, campaign.include_running);
        // Both moves are decided on what holds now, read fresh, rather than
        // when the item was found or leased. Before an upgrade: a sandbox
        // started and stopped again under another environment since it was
        // found isn't this campaign's, while one on the target still goes to
        // the upgrade, which finishes an earlier lease's upgrade that
        // published the target but didn't finish its backup or retire, and
        // otherwise moves nothing. Before a stop: stopping loses the
        // sandbox's processes and memory, and no later decline gives them
        // back.
        let refusal = match step {
            Step::Upgrade => on_either_end(&self.pool, campaign, &sandbox.execution).await,
            Step::StopThenUpgrade => may_stop(&self.pool, campaign, &sandbox.execution).await,
            Step::Release(_) | Step::Skip(_) => Ok(None),
        };
        match refusal {
            Ok(None) => {}
            Ok(Some(reason)) => return ItemOutcome::Skipped(ItemError::new(reason)),
            Err(error) => return ItemOutcome::Retry(ItemError::new(error)),
        }
        match step {
            Step::Upgrade => {}
            Step::StopThenUpgrade => {
                match self.dispatcher.stop(None, sandbox_id, campaign.from).await {
                    Ok(GuardedStop::Stopped { .. }) => {}
                    Ok(GuardedStop::Declined(reason)) => {
                        return ItemOutcome::Skipped(ItemError::new(reason));
                    }
                    Err(error) => return error_outcome(error),
                }
            }
            Step::Release(reason) => return ItemOutcome::Release(ItemError::new(reason)),
            Step::Skip(reason) => return ItemOutcome::Skipped(ItemError::new(reason)),
        }
        match self
            .dispatcher
            .upgrade_stopped(sandbox_id, campaign.to)
            .await
        {
            Ok(StoppedUpgrade::Upgraded | StoppedUpgrade::AlreadyOnTarget) => ItemOutcome::Done,
            Ok(StoppedUpgrade::NotMoved(reason)) => ItemOutcome::Skipped(ItemError::new(reason)),
            Err(error) => error_outcome(error),
        }
    }
}

/// The outcome a dispatch error records. The loop fails a retry on the
/// item's last attempt.
pub(super) fn error_outcome(error: DispatchError) -> ItemOutcome {
    match error {
        DispatchError::Retryable(message) => ItemOutcome::Retry(ItemError::new(message)),
        DispatchError::Failed(message) => ItemOutcome::Failed(ItemError::new(message)),
    }
}

/// Why `campaign` may not stop a running sandbox that published
/// `execution`, read fresh, or `None` when it may: the sandbox is still on
/// the source, and the policy still blocks the source with an edge to the
/// target.
async fn may_stop(
    pool: &PgPool,
    campaign: &Campaign,
    execution: &SandboxCandidate,
) -> Result<Option<&'static str>, String> {
    let inputs = Inputs::read(pool).await?;
    if !inputs.on_source(execution, campaign) {
        return Ok(Some(NO_LONGER_ON_SOURCE));
    }
    Ok(match inputs.plan(campaign) {
        SandboxPlan::Scan {
            moves: true,
            takes_running: true,
        } => None,
        _ => Some(POLICY_STOPPED_MOVING_RUNNING),
    })
}

/// Why `campaign` may not upgrade a stopped sandbox that published
/// `execution`, read fresh, or `None` when it may: the sandbox is on the
/// source or the target.
async fn on_either_end(
    pool: &PgPool,
    campaign: &Campaign,
    execution: &SandboxCandidate,
) -> Result<Option<&'static str>, String> {
    let inputs = Inputs::read(pool).await?;
    let on_either = inputs
        .attributed(execution)
        .is_some_and(|environment| environment == campaign.from || environment == campaign.to);
    Ok((!on_either).then_some(ON_NEITHER_END))
}
