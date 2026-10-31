//! Runtime upgrade campaigns: moving an environment's sandboxes to the
//! cluster default, paced per node and per cluster.
//!
//! An operator starts a campaign (`runtime_upgrade_campaigns`) naming the
//! environment to move off, the target, the pacing and a failure budget.
//! Each tick, for every unfinished sandbox campaign, this loop:
//!
//! 1. **Re-drives lapsed leases.** An item left `in_flight` past its lease
//!    (its worker died, or this controller lost leadership mid-upgrade) goes
//!    back to `pending`, or to `failed` after [`MAX_ATTEMPTS`] leases. The
//!    upgrade is idempotent, so redoing it is safe, and the lapsed worker can
//!    no longer record an outcome.
//! 2. **Guards the budget.** A campaign whose failed items reach its
//!    `failure_budget` pauses itself.
//! 3. **Decides the campaign as a start would.** The start-time decision
//!    ([`pplx_space_api::controllers::substrate::runtime_upgrade::decide`])
//!    for a sandbox on the campaign's source says whether the campaign moves
//!    anything. It's held while its target isn't the cluster default (the
//!    node-side upgrade only boots under the default) or its source is
//!    `allowed` again. Its sandboxes are stranded when no certified edge
//!    reaches the target. Running and paused sandboxes move only when the
//!    campaign sets `include_running` and the move is one whose authorization
//!    accepts memory loss, which only a block of the source gives.
//! 4. **Finds subjects.** One page per tick of the substrate's published
//!    sandboxes that the catalog attributes to the source, the way a start
//!    attributes them. They become pending items, or are counted as stranded.
//! 5. **Completes.** A rolling campaign that moves sandboxes completes on the
//!    tick a whole pass of subjects, all examined under the same plan and
//!    attribution inputs, ends having recorded nothing new, with no item
//!    pending or in flight. One whose sandboxes are stranded
//!    stays open. A sandbox that stops after its part of the pass was scanned
//!    is left to the start-time upgrade. One skipped because it had started
//!    when it was leased is found again if a later pass sees it stopped on the
//!    source before the campaign completes, up to [`MAX_ATTEMPTS`] leases.
//! 6. **Leases and dispatches.** Pending items are leased up to the
//!    campaign's caps ([`pplx_space_runtime_upgrade_campaign_store`] owns the
//!    pacing rules, the canary included, and counts items still in flight
//!    from earlier ticks) and handed to the [`SandboxUpgradeDispatcher`] on
//!    their own tasks, so a slow upgrade holds only its own slot. A stopped
//!    sandbox is upgraded and stays stopped, and a running or paused one is
//!    stopped first and then takes the same path. Each task renews its lease
//!    while the dispatcher works and drops the work once the lease is lost.
//!
//! Stopping the loop aborts the dispatches still running; their leases
//! lapse and the next leader re-drives them.
//!
//! Template campaigns are listed and reported but not driven yet: moving a
//! template release needs environment-keyed node caches first.
//!
//! Disabled by default ([`ReconcilerConfig::enabled`]). Disabled, the loop
//! never reads or writes the campaign tables.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pplx_space_api::controllers::substrate::runtime_upgrade::{
    CatalogEnvironment, EnvironmentMove, RecordedExecution, RuntimePlan, UpgradeActivation,
    attribute, decide,
};
use pplx_space_common_core::types::{SandboxId, SandboxLifecycle, SubstrateKind};
use pplx_space_metrics::cluster_reconciler::{self as reconciler_metrics, Loop};
use pplx_space_metrics::runtime_upgrade::{self as metrics, CampaignHold, RedriveResult};
use pplx_space_runtime_environment_policy_store::{
    RuntimeEnvironmentPolicy, RuntimeEnvironmentState,
};
use pplx_space_runtime_upgrade_campaign_store::{
    self as store, Campaign, CampaignId, CampaignKind, CampaignPhase, ItemPhase,
};
use sqlx::PgPool;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::env_config::EnvConfig;

mod dispatch;

#[cfg(test)]
mod tests;

pub use dispatch::{DispatchError, SandboxUpgradeDispatcher, StoppedUpgrade};

/// The switch: `1`, `true`, `yes`, `on` or `enabled` turns the loop on;
/// unset or anything else keeps it off.
pub const ENABLED_ENV: &str = "SPACE_RUNTIME_UPGRADE_RECONCILER_ENABLED";
/// Test-hook override of the tick, in seconds.
const TICK_TEST_ENV: &str = "SPACE_RUNTIME_UPGRADE_RECONCILER_TICK_SECS";

const DEFAULT_TICK: Duration = Duration::from_secs(30);
/// How long a lease lasts between renewals. Long enough to ride out a slow
/// renewal; short enough that a dead worker's items are re-driven within
/// minutes.
const DEFAULT_LEASE: Duration = Duration::from_secs(5 * 60);
/// Leases an item may take before a lapsed or retried one fails.
pub const MAX_ATTEMPTS: u64 = 5;
/// Sandboxes examined per campaign per tick.
const DEFAULT_CANDIDATE_PAGE: u32 = 500;
/// Pending items considered per lease.
const LEASE_SCAN: u32 = 1000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconcilerConfig {
    /// Master switch, off by default. Off, the loop waits for shutdown and
    /// touches no campaign table.
    pub enabled: bool,
    pub tick: Duration,
    /// Each lease's length; renewed at a third of it while an item is
    /// dispatched.
    pub lease: Duration,
    /// Sandboxes examined per campaign per tick. A page shorter than this
    /// ends a pass.
    pub candidate_page: u32,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tick: DEFAULT_TICK,
            lease: DEFAULT_LEASE,
            candidate_page: DEFAULT_CANDIDATE_PAGE,
        }
    }
}

impl ReconcilerConfig {
    pub fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let env = EnvConfig(env);
        let default = Self::default();
        Self {
            enabled: env.flag(ENABLED_ENV, default.enabled),
            tick: env.test_duration_secs(TICK_TEST_ENV, default.tick),
            ..default
        }
    }
}

/// What a leasing campaign does this tick under the current policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Plan {
    /// Nothing calls for the move; lease nothing.
    Hold(CampaignHold),
    /// Find the source's sandboxes. `moves` says whether they move or are
    /// stranded; `takes_running` whether running and paused ones count.
    Scan { moves: bool, takes_running: bool },
}

/// Decide `campaign` as a start of one of its source's sandboxes would be
/// decided. `from_catalog` is the source's catalog row.
fn plan(
    policy: &RuntimeEnvironmentPolicy,
    from_catalog: Option<&CatalogEnvironment>,
    campaign: &Campaign,
) -> Plan {
    if policy.default_environment != Some(campaign.to) {
        return Plan::Hold(CampaignHold::TargetNotDefault);
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
        RuntimePlan::ColdBootUpgrade(to_default) => Plan::Scan {
            moves: true,
            takes_running: takes_running(&to_default),
        },
        RuntimePlan::Stranded(to_default) => Plan::Scan {
            moves: false,
            takes_running: takes_running(&to_default),
        },
        RuntimePlan::AsRecorded
            if policy.environments.get(&campaign.from)
                == Some(&RuntimeEnvironmentState::Allowed) =>
        {
            Plan::Hold(CampaignHold::SourceAllowed)
        }
        // A deprecated source without an edge: its sandboxes still execute
        // where they are, and nothing moves them. A start never restores, so
        // `RestoreOnDefault` doesn't arise.
        RuntimePlan::AsRecorded | RuntimePlan::RestoreOnDefault(_) => Plan::Scan {
            moves: false,
            takes_running: false,
        },
    }
}

/// How a page of a pass left it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PassEnd {
    NotEnded,
    /// The pass ended having recorded or revived items, which a later pass
    /// must see drained before the campaign can complete.
    RecordedItems,
    /// The pass ended having found nothing new.
    Quiet,
}

/// A fingerprint of what attributes a sandbox to an environment: the
/// catalog and the legacy unbound setting. Within one process, equal inputs
/// give equal fingerprints.
fn attribution_inputs(catalog: &[CatalogEnvironment], legacy_unbound_allowed: Option<bool>) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    format!("{catalog:?}").hash(&mut hasher);
    legacy_unbound_allowed.hash(&mut hasher);
    hasher.finish()
}

/// Where one campaign's scan of subjects is.
#[derive(Clone, Copy, Debug, Default)]
struct Pass {
    /// The last sandbox examined, or `None` at the start of a pass.
    after: Option<SandboxId>,
    /// What this pass's pages were examined under: the plan (`moves`,
    /// `takes_running`) and a fingerprint of the inputs that attribute a
    /// sandbox to the source (the catalog and the legacy unbound setting). A
    /// change restarts the pass, so no page is left judged under inputs the
    /// campaign no longer runs on.
    plan: Option<(bool, bool, u64)>,
    /// Stranded sandboxes this pass has found so far.
    stranded: u64,
    /// Items this pass has recorded or made pending again.
    recorded: u64,
    /// The stranded count of the last whole pass, once one has ended since
    /// this controller started driving the campaign.
    last_stranded: Option<u64>,
}

/// What one tick did, for tests and logs.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TickReport {
    pub inserted: u64,
    pub leased: u64,
    pub requeued: u64,
    pub paused: Vec<CampaignId>,
    pub completed: Vec<CampaignId>,
}

pub struct Reconciler<D> {
    pool: PgPool,
    managed_by: SubstrateKind,
    worker: dispatch::Worker<D>,
    config: ReconcilerConfig,
    passes: Mutex<HashMap<CampaignId, Pass>>,
    /// Dispatches still running, across ticks.
    in_flight: Mutex<JoinSet<()>>,
}

impl<D: SandboxUpgradeDispatcher> Reconciler<D> {
    /// A reconciler for the sandboxes `managed_by` owns.
    pub fn new(
        pool: PgPool,
        managed_by: SubstrateKind,
        dispatcher: D,
        config: ReconcilerConfig,
    ) -> Self {
        Self {
            worker: dispatch::Worker::new(pool.clone(), Arc::new(dispatcher), config.lease),
            pool,
            managed_by,
            config,
            passes: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(JoinSet::new()),
        }
    }

    pub async fn tick(&self) -> TickReport {
        self.reap_dispatches();
        let mut report = TickReport::default();
        let campaigns = match store::list_unfinished(&self.pool).await {
            Ok(campaigns) => campaigns,
            Err(error) => {
                tracing::warn!(%error, "runtime_upgrade: listing campaigns failed; retrying next tick");
                return report;
            }
        };
        report_campaigns(&campaigns);
        self.forget_finished(&campaigns);
        let sandbox_campaigns: Vec<&Campaign> = campaigns
            .iter()
            .filter(|campaign| campaign.kind == CampaignKind::Sandboxes)
            .collect();
        if !sandbox_campaigns.is_empty() {
            self.drive(&sandbox_campaigns, &mut report).await;
        }
        for campaign in &campaigns {
            self.report_items(campaign).await;
        }
        report
    }

    /// Wait for every dispatch started so far to finish.
    #[cfg(test)]
    pub(crate) async fn settle(&self) {
        let mut running = std::mem::take(&mut *self.lock_in_flight());
        while running.join_next().await.is_some() {}
    }

    fn lock_in_flight(&self) -> std::sync::MutexGuard<'_, JoinSet<()>> {
        self.in_flight
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Collect the dispatches that finished since the last tick.
    fn reap_dispatches(&self) {
        let mut running = self.lock_in_flight();
        while let Some(finished) = running.try_join_next() {
            if let Err(error) = finished
                && error.is_panic()
            {
                tracing::error!(%error, "runtime_upgrade: a dispatch panicked; its lease will lapse and be re-driven");
            }
        }
    }

    async fn drive(&self, campaigns: &[&Campaign], report: &mut TickReport) {
        let policy = match pplx_space_runtime_environment_policy_store::get(&self.pool).await {
            Ok(policy) => policy,
            Err(error) => {
                tracing::warn!(%error, "runtime_upgrade: reading the policy failed; retrying next tick");
                return;
            }
        };
        let catalog: Vec<CatalogEnvironment> =
            match pplx_space_runtime_environment_catalog_store::list_unretired_entries(&self.pool)
                .await
            {
                Ok(entries) => entries.into_iter().map(CatalogEnvironment::from).collect(),
                Err(error) => {
                    tracing::warn!(%error, "runtime_upgrade: reading the catalog failed; retrying next tick");
                    return;
                }
            };

        let inputs = attribution_inputs(&catalog, policy.legacy_unbound_allowed);
        for campaign in campaigns {
            self.redrive(campaign, report).await;
            if !campaign.phase.leases() || self.pause_if_over_budget(campaign, report).await {
                continue;
            }
            let from_catalog = catalog
                .iter()
                .find(|environment| environment.reference == campaign.from);
            let (moves, takes_running) = match plan(&policy, from_catalog, campaign) {
                Plan::Hold(hold) => {
                    metrics::campaign_held(campaign.kind.as_str(), hold);
                    tracing::info!(
                        campaign_id = %campaign.id,
                        reason = hold.as_str(),
                        "runtime_upgrade: holding the campaign"
                    );
                    continue;
                }
                Plan::Scan {
                    moves,
                    takes_running,
                } => (moves, takes_running),
            };
            let pass = self
                .find_subjects(
                    campaign,
                    &policy,
                    &catalog,
                    (moves, takes_running, inputs),
                    report,
                )
                .await;
            // A pass that moved nothing proves nothing moved: a stranded
            // campaign stays open, reporting its stranded count, until an
            // edge is certified or an operator aborts it.
            if !moves || (pass == PassEnd::Quiet && self.complete_if_done(campaign, report).await) {
                continue;
            }
            let leased_at = tokio::time::Instant::now();
            let leased = self.lease(campaign).await;
            report.leased += leased.len() as u64;
            let mut running = self.lock_in_flight();
            for item in leased {
                running.spawn(self.worker.clone().drive_item(
                    campaign.id,
                    campaign.kind,
                    item,
                    takes_running,
                    leased_at,
                ));
            }
        }
    }

    async fn redrive(&self, campaign: &Campaign, report: &mut TickReport) {
        match store::redrive_expired(&self.pool, campaign.id, MAX_ATTEMPTS).await {
            Ok(redriven) => {
                let kind = campaign.kind.as_str();
                metrics::campaign_redriven(kind, RedriveResult::Requeued, redriven.requeued);
                metrics::campaign_redriven(kind, RedriveResult::Failed, redriven.failed);
                if redriven.requeued + redriven.failed > 0 {
                    tracing::info!(
                        campaign_id = %campaign.id,
                        requeued = redriven.requeued,
                        failed = redriven.failed,
                        "runtime_upgrade: re-drove items whose lease lapsed"
                    );
                }
                report.requeued += redriven.requeued;
            }
            Err(error) => tracing::warn!(
                campaign_id = %campaign.id,
                %error,
                "runtime_upgrade: re-driving lapsed leases failed; retrying next tick"
            ),
        }
    }

    /// Pause `campaign` if it's over its failure budget. `true` when it's
    /// paused now, or the check failed and nothing should be leased.
    async fn pause_if_over_budget(&self, campaign: &Campaign, report: &mut TickReport) -> bool {
        match store::pause_if_over_budget(&self.pool, campaign.id).await {
            Ok(true) => {
                metrics::campaign_auto_paused(campaign.kind.as_str());
                tracing::warn!(
                    campaign_id = %campaign.id,
                    failure_budget = campaign.failure_budget,
                    "runtime_upgrade: the campaign reached its failure budget and paused itself"
                );
                report.paused.push(campaign.id);
                true
            }
            Ok(false) => false,
            Err(error) => {
                tracing::warn!(
                    campaign_id = %campaign.id,
                    %error,
                    "runtime_upgrade: checking the failure budget failed; leasing nothing this tick"
                );
                true
            }
        }
    }

    /// Examine one page of sandboxes for `campaign` under `plan` (`moves`,
    /// `takes_running`, the attribution inputs): those attributed to its
    /// source become pending items when the campaign moves them, and are
    /// counted as stranded otherwise. Says whether this page ended a pass,
    /// and if so whether the whole pass recorded nothing new. A failed read or
    /// insert ends nothing.
    async fn find_subjects(
        &self,
        campaign: &Campaign,
        policy: &RuntimeEnvironmentPolicy,
        catalog: &[CatalogEnvironment],
        plan: (bool, bool, u64),
        report: &mut TickReport,
    ) -> PassEnd {
        let (moves, takes_running, _) = plan;
        let lifecycles: &[SandboxLifecycle] = if takes_running {
            &[
                SandboxLifecycle::Stopped,
                SandboxLifecycle::Started,
                SandboxLifecycle::Paused,
            ]
        } else {
            &[SandboxLifecycle::Stopped]
        };
        let after = {
            let mut passes = self
                .passes
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let pass = passes.entry(campaign.id).or_default();
            if pass.after.is_some() && pass.plan != Some(plan) {
                tracing::info!(
                    campaign_id = %campaign.id,
                    "runtime_upgrade: the campaign's plan or attribution inputs changed mid-pass; \
                     restarting the pass"
                );
                pass.after = None;
                pass.stranded = 0;
                pass.recorded = 0;
            }
            pass.after
        };
        let page = match store::sandbox_candidates(
            &self.pool,
            campaign.id,
            self.managed_by,
            lifecycles,
            after,
            self.config.candidate_page,
            MAX_ATTEMPTS,
        )
        .await
        {
            Ok(page) => page,
            Err(error) => {
                tracing::warn!(campaign_id = %campaign.id, %error, "runtime_upgrade: reading candidates failed");
                return PassEnd::NotEnded;
            }
        };
        let on_source: Vec<SandboxId> = page
            .iter()
            .filter(|candidate| {
                let recorded = RecordedExecution::from_record(
                    candidate.execution_metadata.as_deref(),
                    candidate.runtime.clone(),
                    candidate.runtime_version.clone(),
                    candidate.state_format_name.clone(),
                    candidate.state_format_version.clone(),
                );
                attribute(&recorded, catalog, policy.legacy_unbound_allowed) == Some(campaign.from)
            })
            .map(|candidate| candidate.sandbox_id)
            .collect();
        let (movable, stranded) = if moves {
            (on_source, 0)
        } else {
            (Vec::new(), on_source.len() as u64)
        };
        let recorded = match store::insert_pending_sandboxes(
            &self.pool,
            campaign.id,
            &movable,
            MAX_ATTEMPTS,
        )
        .await
        {
            Ok(recorded) => recorded,
            Err(error) => {
                tracing::warn!(campaign_id = %campaign.id, %error, "runtime_upgrade: recording items failed");
                return PassEnd::NotEnded;
            }
        };
        report.inserted += recorded;

        let ended = page.len() < self.config.candidate_page as usize;
        let (end, last_stranded) = {
            let mut passes = self
                .passes
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let pass = passes.entry(campaign.id).or_default();
            pass.stranded += stranded;
            pass.recorded += recorded;
            pass.plan = Some(plan);
            pass.after = page.last().map(|candidate| candidate.sandbox_id);
            if ended {
                let end = if pass.recorded == 0 {
                    PassEnd::Quiet
                } else {
                    PassEnd::RecordedItems
                };
                pass.last_stranded = Some(std::mem::take(&mut pass.stranded));
                pass.recorded = 0;
                pass.after = None;
                (end, pass.last_stranded)
            } else {
                (PassEnd::NotEnded, None)
            }
        };
        if let Some(stranded) = last_stranded {
            metrics::campaign_stranded(&campaign.id.to_string(), stranded);
            if stranded > 0 {
                tracing::warn!(
                    campaign_id = %campaign.id,
                    stranded,
                    "runtime_upgrade: sandboxes on the campaign's source have no certified \
                     upgrade edge to its target"
                );
            }
        }
        end
    }

    fn pass(&self, campaign_id: CampaignId) -> Pass {
        self.passes
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(&campaign_id)
            .copied()
            .unwrap_or_default()
    }

    fn forget_finished(&self, campaigns: &[Campaign]) {
        let mut passes = self
            .passes
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        passes
            .retain(|campaign_id, _| campaigns.iter().any(|campaign| campaign.id == *campaign_id));
    }

    async fn lease(&self, campaign: &Campaign) -> Vec<store::LeasedItem> {
        store::lease_sandbox_items(&self.pool, campaign.id, self.config.lease, LEASE_SCAN)
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(campaign_id = %campaign.id, %error, "runtime_upgrade: leasing items failed");
                Vec::new()
            })
    }

    /// Complete `campaign` if its items are drained. Called only on the tick
    /// a whole pass of subjects ends: only then has every sandbox the pass
    /// could find been examined, while partway through a later pass,
    /// sandboxes that stopped since the last one may still be ahead of the
    /// scan. `true` when this call completed it.
    async fn complete_if_done(&self, campaign: &Campaign, report: &mut TickReport) -> bool {
        if campaign.phase != CampaignPhase::Rolling {
            return false;
        }
        let stranded = self.pass(campaign.id).last_stranded.unwrap_or(0);
        match store::complete_if_drained(&self.pool, campaign.id).await {
            Ok(true) => {
                tracing::info!(
                    campaign_id = %campaign.id,
                    stranded,
                    "runtime_upgrade: the campaign completed"
                );
                report.completed.push(campaign.id);
                true
            }
            Ok(false) => false,
            Err(error) => {
                tracing::warn!(
                    campaign_id = %campaign.id,
                    %error,
                    "runtime_upgrade: completing the campaign failed; retrying next tick"
                );
                false
            }
        }
    }

    async fn report_items(&self, campaign: &Campaign) {
        let counts = match store::item_counts(&self.pool, campaign.id).await {
            Ok(counts) => counts,
            Err(error) => {
                tracing::warn!(campaign_id = %campaign.id, %error, "runtime_upgrade: counting items failed");
                return;
            }
        };
        let campaign_id = campaign.id.to_string();
        for phase in ItemPhase::ALL {
            metrics::campaign_items(
                &campaign_id,
                campaign.kind.as_str(),
                phase.as_str(),
                counts.get(*phase),
            );
        }
    }
}

/// Report every unfinished kind and phase, zero included.
fn report_campaigns(campaigns: &[Campaign]) {
    let mut counts: BTreeMap<(CampaignKind, CampaignPhase), u64> = BTreeMap::new();
    for campaign in campaigns {
        *counts.entry((campaign.kind, campaign.phase)).or_default() += 1;
    }
    for kind in CampaignKind::ALL {
        for phase in [
            CampaignPhase::Canary,
            CampaignPhase::Rolling,
            CampaignPhase::Paused,
        ] {
            metrics::campaigns(
                kind.as_str(),
                phase.as_str(),
                counts.get(&(*kind, phase)).copied().unwrap_or(0),
            );
        }
    }
}

/// Run the reconciler until `shutdown` fires. Disabled, it only waits.
pub async fn run<D: SandboxUpgradeDispatcher>(
    config: ReconcilerConfig,
    pool: PgPool,
    managed_by: SubstrateKind,
    dispatcher: D,
    shutdown: CancellationToken,
) {
    if !config.enabled {
        tracing::info!("runtime_upgrade disabled ({ENABLED_ENV} unset); waiting for shutdown");
        shutdown.cancelled().await;
        return;
    }
    tracing::info!(
        tick_secs = config.tick.as_secs(),
        lease_secs = config.lease.as_secs(),
        "starting runtime_upgrade"
    );
    let tick = config.tick;
    let reconciler = Reconciler::new(pool, managed_by, dispatcher, config);
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let started = Instant::now();
                let report = reconciler.tick().await;
                reconciler_metrics::loop_latency(Loop::RuntimeUpgrade, started);
                tracing::debug!(?report, "runtime_upgrade tick");
            }
            () = shutdown.cancelled() => {
                tracing::info!("runtime_upgrade stopping; aborting the dispatches still running");
                return;
            }
        }
    }
}
