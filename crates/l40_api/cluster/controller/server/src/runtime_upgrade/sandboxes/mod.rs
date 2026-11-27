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
//!   ([`store::sandbox_candidates`], which leaves out one whose published
//!   execution names another environment). No skipped item is retired: an
//!   unbound sandbox the catalog attributes elsewhere, or to none, may be
//!   placed on the source again by a later catalog, and a skipped item
//!   doesn't hold the campaign open. A skipped item with attempts left whose
//!   sandbox is on the source, in a lifecycle the plan scans, is found again
//!   and made pending, so it holds the campaign open until it moves or its
//!   attempts run out. One in a lifecycle the plan doesn't scan (a sandbox
//!   that had started under a stopped-only campaign, say) isn't, and one that
//!   stops after its part of a pass was scanned is left to the start-time
//!   upgrade. The plan's fingerprint covers every catalog row's identity, the
//!   source row's edges, the legacy unbound setting and which lifecycles
//!   count, so a change of any restarts the pass.
//! * **Upgrade.** Decided on the sandbox as it is now, read fresh. A stopped
//!   sandbox on the source or target goes to
//!   [`SandboxUpgradeDispatcher::upgrade_stopped`] and stays stopped. A
//!   started or paused one, when the plan takes it and it's still on the
//!   source under a policy that still takes it, is stopped first
//!   ([`SandboxUpgradeDispatcher::stop`]) and then takes the same path.
//!   A finished upgrade leaves its sandbox stopped, a running one included:
//!   neither the driver nor its dispatcher starts it again. A running one
//!   whose `upgrade_stopped` call fails after its stop is left stopped on the
//!   source. An item past its deadline fails, and its sandbox is left
//!   wherever the dropped call's fenced effects leave it; an item whose lease
//!   is lost is re-driven by the lease's next holder.
//!   One mid-transition on the target (an earlier lease's upgrade may be
//!   finishing) is retried, within its attempts, since no pass finds a
//!   sandbox there again. Anything else is skipped, a sandbox mid-transition
//!   elsewhere included: it spends an attempt and holds no lease, and a
//!   later pass finds it again once it settles on the source in a lifecycle
//!   the plan scans. A release, which gives the attempt back, would let a
//!   sandbox stuck mid-transition take its lease slot on every tick, and in
//!   `canary` hold the campaign on it.
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

use super::{CampaignDriver, MAX_ATTEMPTS, Plan, ScanPlan, SubjectPage};

mod dispatcher;

#[cfg(test)]
mod tests;

pub use dispatcher::{DispatchError, GuardedStop, SandboxUpgradeDispatcher, StoppedUpgrade};
use dispatcher::{Step, step};

pub(super) const NO_LONGER_ON_SOURCE: &str = "the sandbox is no longer on the campaign's source";
pub(super) const ON_NEITHER_END: &str =
    "the sandbox is on neither the campaign's source nor its target";
pub(super) const POLICY_STOPPED_MOVING_RUNNING: &str =
    "the policy no longer moves this campaign's running sandboxes";
pub(super) const PLAN_CHANGED: &str =
    "the policy or catalog changed since the campaign was planned; retrying next tick";

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
struct Inputs {
    policy: RuntimeEnvironmentPolicy,
    catalog: Vec<CatalogEnvironment>,
}

impl Inputs {
    async fn read(pool: &PgPool) -> Result<Self, String> {
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

    fn plan(&self, campaign: &Campaign) -> SandboxPlan {
        let from_catalog = self
            .catalog
            .iter()
            .find(|environment| environment.reference == campaign.from);
        sandbox_plan(&self.policy, from_catalog, campaign)
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
                    inputs: self.fingerprint(campaign, takes_running),
                },
                takes_running,
            )),
        }
    }

    /// A fingerprint of what judges `campaign`'s sandboxes besides `moves`:
    /// every catalog row's identity, which an unbound record is matched
    /// against ([`attribute`]); the source row's edges, which the plan reads;
    /// the legacy unbound setting; and which lifecycles count. Other rows'
    /// edges judge nothing here, so an edge recompute elsewhere doesn't
    /// restart the pass. Within one process, equal inputs give equal
    /// fingerprints.
    fn fingerprint(&self, campaign: &Campaign, takes_running: bool) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for environment in &self.catalog {
            (
                environment.reference.to_string(),
                &environment.runtime,
                &environment.runtime_version,
                &environment.kernel_version,
                &environment.state_format_name,
                &environment.state_format_version,
            )
                .hash(&mut hasher);
            if environment.reference == campaign.from {
                format!(
                    "{:?}",
                    (&environment.upgrade_edges, &environment.memory_edges)
                )
                .hash(&mut hasher);
            }
        }
        self.policy.legacy_unbound_allowed.hash(&mut hasher);
        takes_running.hash(&mut hasher);
        hasher.finish()
    }

    /// The environment `execution` puts its sandbox on, as a start
    /// attributes it.
    ///
    /// Only a publish that carried an execution reaches here:
    /// [`store::sandbox_candidates`] and [`store::subject_sandbox`] apply the
    /// start's own evidence rule
    /// (`pplx_space_sandbox_teardown::observed_states::publish_carries_execution`).
    fn attributed(&self, execution: &SandboxCandidate) -> Option<EnvironmentReference> {
        let recorded = RecordedExecution::from_record(
            execution.execution_metadata.as_ref(),
            execution.runtime.clone(),
            execution.runtime_version.clone(),
            execution.state_format_name.clone(),
            execution.state_format_version.clone(),
        );
        attribute(&recorded, &self.catalog, self.policy.legacy_unbound_allowed)
    }

    /// Whether `execution` puts its sandbox on `campaign`'s source.
    fn on_source(&self, execution: &SandboxCandidate, campaign: &Campaign) -> bool {
        self.attributed(execution) == Some(campaign.from)
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
    ) -> Result<SubjectPage, String> {
        // Read again rather than trusting `plan`'s inputs: the page is
        // judged under what's read here, so it must be what `plan` was made
        // from. A change since restarts the pass next tick.
        let inputs = Inputs::read(&self.pool).await?;
        let takes_running = match inputs.scan_plan(campaign) {
            Ok((now, takes_running)) if now == plan => takes_running,
            _ => return Err(PLAN_CHANGED.to_owned()),
        };
        let page = store::sandbox_candidates(
            &self.pool,
            campaign.id,
            self.managed_by,
            scanned_lifecycles(takes_running),
            after,
            limit,
            MAX_ATTEMPTS,
        )
        .await
        .map_err(|error| format!("reading candidates failed: {error}"))?;
        let on_source = page
            .iter()
            .filter(|candidate| inputs.on_source(candidate, campaign))
            .map(|candidate| candidate.sandbox_id)
            .collect();
        Ok(SubjectPage {
            on_source,
            // Nothing is retired: a catalog that attributes a sandbox
            // elsewhere now may attribute it to the source again, and a
            // retired item is never found again.
            elsewhere: Vec::new(),
            last: page.last().map(|candidate| candidate.sandbox_id),
            examined: page.len(),
        })
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
        let execution = sandbox.execution.as_ref();
        let refusal = match step {
            Step::Upgrade => on_either_end(&self.pool, campaign, execution).await,
            Step::StopThenUpgrade => may_stop(&self.pool, campaign, execution).await,
            Step::Settling(_) | Step::Skip(_) => Ok(None),
        };
        match refusal {
            Ok(None) => {}
            Ok(Some(reason)) => return ItemOutcome::Skipped(ItemError::new(reason)),
            Err(error) => return ItemOutcome::Retry(ItemError::new(error)),
        }
        match step {
            Step::Upgrade => {}
            Step::StopThenUpgrade => match self
                .dispatcher
                .stop(sandbox_id, campaign.from, campaign.to)
                .await
            {
                Ok(GuardedStop::Stopped) => {}
                Ok(GuardedStop::Declined(reason)) => {
                    return ItemOutcome::Skipped(ItemError::new(reason));
                }
                Err(error) => return error_outcome(error),
            },
            // On the source, a later pass finds it again once it settles,
            // so it holds no lease meanwhile. On the target, an earlier
            // lease's upgrade may be finishing, and no pass finds a sandbox
            // there again: retry, within its attempts, so the upgrade's own
            // finish (backup and retire) still runs once it settles.
            Step::Settling(reason) => {
                return match on_target(&self.pool, campaign, execution).await {
                    Ok(true) => ItemOutcome::Retry(ItemError::new(format!(
                        "{reason} on the campaign's target; retrying once it settles"
                    ))),
                    Ok(false) => ItemOutcome::Skipped(ItemError::new(format!(
                        "{reason}; a later pass finds it again once it settles"
                    ))),
                    Err(error) => ItemOutcome::Retry(ItemError::new(error)),
                };
            }
            Step::Skip(reason) => return ItemOutcome::Skipped(ItemError::new(reason)),
        }
        match self
            .dispatcher
            .upgrade_stopped(sandbox_id, campaign.from, campaign.to)
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
fn error_outcome(error: DispatchError) -> ItemOutcome {
    match error {
        DispatchError::Retryable(message) => ItemOutcome::Retry(ItemError::new(message)),
        DispatchError::Failed(message) => ItemOutcome::Failed(ItemError::new(message)),
    }
}

/// Why `campaign` may not stop a running sandbox that published
/// `execution` (`None`: no execution), read fresh, or `None` when it may: the
/// sandbox is still on the source, and the policy still blocks the source
/// with an edge to the target.
async fn may_stop(
    pool: &PgPool,
    campaign: &Campaign,
    execution: Option<&SandboxCandidate>,
) -> Result<Option<&'static str>, String> {
    let inputs = Inputs::read(pool).await?;
    if !execution.is_some_and(|execution| inputs.on_source(execution, campaign)) {
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

/// Whether a sandbox that published `execution` (`None`: no execution) is on
/// `campaign`'s target, read fresh.
async fn on_target(
    pool: &PgPool,
    campaign: &Campaign,
    execution: Option<&SandboxCandidate>,
) -> Result<bool, String> {
    let inputs = Inputs::read(pool).await?;
    Ok(execution.and_then(|execution| inputs.attributed(execution)) == Some(campaign.to))
}

/// Why `campaign` may not upgrade a stopped sandbox that published
/// `execution` (`None`: no execution), read fresh, or `None` when it may: the
/// sandbox is on the source or the target.
async fn on_either_end(
    pool: &PgPool,
    campaign: &Campaign,
    execution: Option<&SandboxCandidate>,
) -> Result<Option<&'static str>, String> {
    let inputs = Inputs::read(pool).await?;
    let on_either = execution
        .and_then(|execution| inputs.attributed(execution))
        .is_some_and(|environment| environment == campaign.from || environment == campaign.to);
    Ok((!on_either).then_some(ON_NEITHER_END))
}
