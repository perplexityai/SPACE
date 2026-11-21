//! Runtime upgrade campaigns: moving an environment's template releases or
//! sandboxes to another, paced per cluster and, where subjects have a node,
//! per node.
//!
//! An operator starts a campaign (`runtime_upgrade_campaigns`, through the
//! admin API) naming the kind, the environment to move off, the target, the
//! pacing and a failure budget. What a kind's subjects are and how one is
//! upgraded belong to that kind's [`CampaignDriver`]; this loop owns what's
//! common. Each tick, for every unfinished campaign whose kind has a driver
//! in [`Drivers`]:
//!
//! 1. **Re-drives lapsed leases.** An item left `in_flight` past its lease
//!    (its worker died, or this controller lost leadership mid-upgrade) goes
//!    back to `pending`, or to `failed` after [`MAX_ATTEMPTS`] leases. The
//!    upgrade is idempotent, so redoing it is safe, and the lapsed worker can
//!    no longer record an outcome.
//! 2. **Guards the budget.** A campaign whose failed items reach its
//!    `failure_budget` pauses itself.
//! 3. **Plans.** The driver says whether the campaign is held this tick
//!    ([`Plan::Hold`]) or scans, and whether what it finds moves or is
//!    stranded ([`ScanPlan`]).
//! 4. **Finds subjects.** One page per tick of the driver's subjects on the
//!    source. They become pending items whether they move or not: a stranded
//!    campaign's pending items are its stranded count, and they're leased
//!    once a plan moves them. A plan that changes mid-pass restarts the pass.
//! 5. **Completes.** A rolling campaign that moves completes on the tick a
//!    whole pass of subjects, all examined under one plan, ends with no item
//!    pending or in flight and its failures under budget, when the pass also
//!    began with none open and leased nothing. A subject skipped mid-pass is
//!    then found and made pending again by a later pass before the campaign
//!    can complete. A canary none of whose items is done completes the same
//!    way. One whose subjects are stranded stays open. A pass examines each
//!    subject that's on the source when its page is read; one that reaches
//!    the source behind the cursor later is left to the next campaign, so a
//!    driver for whose kind that matters holds the campaign until no more
//!    arrive.
//! 6. **Leases and dispatches.** Pending items are leased up to the
//!    campaign's caps ([`pplx_space_runtime_upgrade_campaign_store`] owns the
//!    pacing rules, the canary included, and counts items still in flight
//!    from earlier ticks) and handed to the driver on their own tasks, so a
//!    slow upgrade holds only its own slot. Each task renews its lease while
//!    the driver works, drops the work once the lease is lost, and fails the
//!    item past the item deadline.
//!
//! A campaign whose kind has no driver is reported (its phase and items) but
//! not driven: nothing reads its subjects, and the only write to its items
//! is the one every aborted campaign gets, returning in-flight items whose
//! leases lapsed to pending. The admin API refuses to start one, so only a
//! campaign started some other way, or by a newer API during a rollout, can
//! be in that state. A campaign whose kind this build doesn't know at all is
//! logged and skipped by the store before the loop sees it, though an aborted
//! one's lapsed in-flight items are still returned to pending.
//!
//! An operator's abort leaves a campaign's in-flight items to drain: their
//! workers can't renew a finished campaign's lease, so each drops its work
//! at its next renewal attempt, or at its own deadline if the database is out
//! of reach, before its lease lapses ([`worker`]). No other campaign leases
//! those subjects while the leases live, and once they lapse each tick
//! returns the items to pending. The finished campaign is no longer listed,
//! so nothing more is leased.
//!
//! Stopping the loop aborts the dispatches still running; their leases
//! lapse and the next leader re-drives them.
//!
//! Disabled by default ([`ReconcilerConfig::enabled`]). Disabled, the loop
//! never reads or writes the campaign tables.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pplx_space_artifact_model::{TRUSTED_TEMPLATE_STORAGE_BUCKETS_ENV, TrustedStorageBuckets};
use pplx_space_env_vars::Environment;
use pplx_space_runtime_upgrade_campaign_store::{
    self as store, Campaign, CampaignId, CampaignKind, CampaignPhase, ItemOutcome, ItemPhase,
    LeasedItem,
};
use sqlx::PgPool;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::env_config::EnvConfig;
use crate::metrics::cluster_reconciler::{self as reconciler_metrics, Loop};
use crate::metrics::runtime_upgrade::{self as metrics, RedriveResult};

mod sandboxes;
mod templates;
mod worker;

#[cfg(test)]
mod tests;

pub use sandboxes::{
    DispatchError, GuardedStop, SandboxDriver, SandboxUpgradeDispatcher, StoppedUpgrade,
};

/// The switch: `1`, `true`, `yes`, `on` or `enabled` turns the loop on;
/// unset or anything else keeps it off.
pub const ENABLED_ENV: &str = "SPACE_RUNTIME_UPGRADE_RECONCILER_ENABLED";
/// Lets the template driver use a storage location's `https://` endpoint
/// override while [`TRUSTED_TEMPLATE_STORAGE_BUCKETS_ENV`] is empty, for
/// S3-compatible storage. Off by default. A plain `http://` endpoint needs
/// [`ALLOW_TEMPLATE_STORAGE_PLAINTEXT_ENDPOINTS_ENV`] instead.
pub const ALLOW_TEMPLATE_STORAGE_ENDPOINT_OVERRIDES_ENV: &str =
    "SPACE_TEMPLATE_STORAGE_ALLOW_ENDPOINT_OVERRIDES";
/// Lets the template driver use a storage location's plain `http://`
/// endpoint, for local S3-compatible storage such as the Kind object store.
/// Off by default; it allows endpoint overrides too, since a plaintext
/// endpoint is always one. A configured trusted list still refuses every
/// override.
pub const ALLOW_TEMPLATE_STORAGE_PLAINTEXT_ENDPOINTS_ENV: &str =
    "SPACE_TEMPLATE_STORAGE_ALLOW_PLAINTEXT_ENDPOINTS";
/// Test-hook override of the tick, in seconds.
const TICK_TEST_ENV: &str = "SPACE_RUNTIME_UPGRADE_RECONCILER_TICK_SECS";

const DEFAULT_TICK: Duration = Duration::from_secs(30);
/// How long a lease lasts between renewals. Long enough to ride out a slow
/// renewal; short enough that a dead worker's items are re-driven within
/// minutes.
const DEFAULT_LEASE: Duration = Duration::from_secs(5 * 60);
/// Leases an item may take before a lapsed or retried one fails.
pub const MAX_ATTEMPTS: u64 = 5;
/// Subjects examined per campaign per tick.
const DEFAULT_CANDIDATE_PAGE: u32 = 500;
/// How long one lease's dispatch of an item may take, in seconds, from
/// [`MIN_ITEM_DEADLINE`] to [`MAX_ITEM_DEADLINE`]; unset or `0` keeps
/// [`DEFAULT_ITEM_DEADLINE`].
pub const ITEM_DEADLINE_ENV: &str = "SPACE_RUNTIME_UPGRADE_ITEM_DEADLINE_SECS";
/// Room for a stop, a cold boot under the target and a stop again.
const DEFAULT_ITEM_DEADLINE: Duration = Duration::from_secs(30 * 60);
/// A day: a deadline any longer doesn't bound a hang.
const MAX_ITEM_DEADLINE: Duration = Duration::from_secs(24 * 60 * 60);
/// A deadline that dropped an upgrade partway could leave what it placed for
/// the boot with no owner, so the deadline leaves room for a boot, the reads
/// before it, and the backup and cleanup after it.
const MIN_ITEM_DEADLINE: Duration = Duration::from_secs(15 * 60);
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
    /// Subjects examined per campaign per tick. A page shorter than this
    /// ends a pass.
    pub candidate_page: u32,
    /// How long one item's dispatch may take in all. Past it the item fails,
    /// counting against the failure budget, and its lease is released, so a
    /// dispatch that never returns can't hold its slot.
    pub item_deadline: Duration,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tick: DEFAULT_TICK,
            lease: DEFAULT_LEASE,
            candidate_page: DEFAULT_CANDIDATE_PAGE,
            item_deadline: DEFAULT_ITEM_DEADLINE,
        }
    }
}

impl ReconcilerConfig {
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_env_vars::ProcessEnvironment)
    }

    pub fn from_environment(env: &dyn pplx_space_env_vars::Environment) -> Self {
        let env = EnvConfig(env);
        let default = Self::default();
        Self {
            enabled: env.flag(ENABLED_ENV, default.enabled),
            tick: env.test_duration_secs(TICK_TEST_ENV, default.tick),
            item_deadline: env
                .bounded_duration_secs(ITEM_DEADLINE_ENV, default.item_deadline, MAX_ITEM_DEADLINE)
                .max(MIN_ITEM_DEADLINE),
            ..default
        }
    }
}

/// What a leasing campaign does this tick, as its driver decides it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Plan {
    /// Nothing calls for the move; scan and lease nothing. The reason is a
    /// short `snake_case` label, logged and reported as the
    /// `runtime_upgrade.campaign.held` metric's `reason` tag.
    Hold(&'static str),
    /// Find the source's subjects.
    Scan(ScanPlan),
}

/// How a scanning campaign treats what it finds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanPlan {
    /// Whether the subjects found move. `false` records them as pending
    /// without leasing them: they're stranded, and the campaign stays open.
    pub moves: bool,
    /// A fingerprint of whatever else the driver judged subjects by (its
    /// catalog read, say). Within one process, equal inputs give equal
    /// fingerprints. A change of it, or of `moves`, restarts the pass, so no
    /// page is left judged under inputs the campaign no longer runs on.
    pub inputs: u64,
}

/// One page of a driver's subjects.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SubjectPage {
    /// Subjects on the campaign's source, to record as pending.
    pub on_source: Vec<uuid::Uuid>,
    /// Subjects examined that are no longer on the source. A skipped item of
    /// one is retired ([`store::retire_skipped`]), so a driver that excludes
    /// items out of attempts doesn't find it again.
    pub elsewhere: Vec<uuid::Uuid>,
    /// The last subject examined, in the driver's subject order: the cursor
    /// the next page starts after. `None` for an empty page.
    pub last: Option<uuid::Uuid>,
    /// How many subjects the page examined. Fewer than the page limit ends
    /// the pass.
    pub examined: usize,
}

/// What a campaign kind's upgrade needs: its subjects, its plan, and the
/// upgrade of one subject. Registered in [`Drivers`] by kind.
///
/// Errors are messages: the loop logs them and retries next tick, since
/// nothing but the next tick can act on them.
#[async_trait::async_trait]
pub trait CampaignDriver: Send + Sync + 'static {
    /// The kind of campaign this driver moves.
    fn kind(&self) -> CampaignKind;

    /// Decide what `campaign` does this tick.
    async fn plan(&self, campaign: &Campaign) -> Result<Plan, String>;

    /// Up to `limit` of the subjects `campaign` covers after `after`, in a
    /// stable order by subject id, judged under `plan`. Subjects that have an
    /// item already are fine to return: recording one again only makes a
    /// skipped item with attempts left pending again.
    async fn find_subjects(
        &self,
        campaign: &Campaign,
        plan: ScanPlan,
        after: Option<uuid::Uuid>,
        limit: u32,
    ) -> Result<SubjectPage, String>;

    /// Upgrade one leased item and say how it ended. Must be idempotent and
    /// safe to run again after any failure, including one that left the
    /// caller unsure whether it ran, and should decide afresh whether the
    /// subject is still the campaign's to move. The loop holds the lease and
    /// the deadline; a [`ItemOutcome::Retry`] on the item's last attempt is
    /// recorded as failed.
    ///
    /// The loop drops this future when the lease is lost or the item's
    /// deadline passes, and another worker may then lease the subject. So
    /// dropping it must stop the upgrade's effects, or the effects must be
    /// fenced where they land (a node-side operation keyed on the subject,
    /// say), so a later upgrade of the same subject can't run alongside work
    /// the dropped one started.
    async fn upgrade(&self, campaign: &Campaign, item: &LeasedItem) -> ItemOutcome;
}

/// The drivers this controller runs, by campaign kind.
#[derive(Clone, Default)]
pub struct Drivers(BTreeMap<CampaignKind, Arc<dyn CampaignDriver>>);

impl Drivers {
    /// Add `driver` for its kind, replacing any earlier one.
    #[must_use]
    pub fn with(mut self, driver: impl CampaignDriver) -> Self {
        self.0.insert(driver.kind(), Arc::new(driver));
        self
    }

    fn get(&self, kind: CampaignKind) -> Option<&Arc<dyn CampaignDriver>> {
        self.0.get(&kind)
    }

    /// The kinds with a driver, in kind order.
    pub fn kinds(&self) -> Vec<CampaignKind> {
        self.0.keys().copied().collect()
    }
}

/// The drivers this build runs: templates ([`templates::TemplateDriver`]).
/// The sandbox driver ([`SandboxDriver`]) isn't one of them until a
/// production [`SandboxUpgradeDispatcher`] exists. A driver added here adds
/// its kind to
/// `pplx_space_api::routes::admin::RUNTIME_UPGRADE_DRIVEN_KINDS` too, so the
/// admin API starts campaigns of it; a test holds the two equal. Building
/// them reads nothing: a disabled loop never asks them anything.
/// `boots_twins` is whether this cluster's pods boot the environment a
/// template twin is bound to; without it template campaigns are held.
/// `managed_by` is the substrate's nodes' `managed_by`. `origins` is which
/// storage locations the template driver uses ([`template_storage_origins`]).
pub fn cluster_drivers(
    pool: PgPool,
    credentials_pool: PgPool,
    boots_twins: bool,
    managed_by: &'static str,
    origins: TemplateStorageOrigins,
) -> Drivers {
    Drivers::default().with(templates::TemplateDriver::new(
        pool,
        credentials_pool,
        boots_twins,
        managed_by,
        origins,
    ))
}

/// Which storage locations the template driver uses.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TemplateStorageOrigins {
    /// The deployment's trusted template buckets; empty trusts every bucket.
    pub trusted: TrustedStorageBuckets,
    /// Whether, with no trusted buckets, a location's endpoint override is
    /// used rather than refused.
    pub allow_endpoint_overrides: bool,
    /// Whether a location's plain `http://` endpoint is used rather than
    /// refused. Implies `allow_endpoint_overrides`.
    pub allow_plaintext_endpoints: bool,
}

impl TemplateStorageOrigins {
    /// The S3 client for these origins. It refuses a location the trusted
    /// buckets refuse, and, unless allowed, a plaintext endpoint and an
    /// endpoint override while no trusted buckets are configured.
    pub fn client(&self) -> pplx_space_template_release_store::S3ReleaseObjects {
        if self.allow_plaintext_endpoints {
            return pplx_space_template_release_store::S3ReleaseObjects::allowing_plaintext_endpoints()
                .trusting(self.trusted.clone());
        }
        let client = pplx_space_template_release_store::S3ReleaseObjects::new()
            .trusting(self.trusted.clone());
        if self.allow_endpoint_overrides {
            client.allowing_endpoint_overrides()
        } else {
            client
        }
    }
}

/// The template driver's storage origins for a loop `config` turns on:
/// [`TRUSTED_TEMPLATE_STORAGE_BUCKETS_ENV`], the internal API's registration
/// policy, and [`ALLOW_TEMPLATE_STORAGE_ENDPOINT_OVERRIDES_ENV`]. A disabled
/// loop asks its drivers nothing, so neither is read then. An empty bucket
/// list admits every bucket but, unless overrides are allowed, no endpoint
/// override, and is logged as a warning: set the list here as well as on the
/// internal API. A list that
/// isn't UTF-8, which the internal API refuses to start with, turns the loop
/// off and is logged, rather than stopping the controller's other loops.
pub fn template_storage_origins(
    config: &mut ReconcilerConfig,
    env: &dyn Environment,
) -> TemplateStorageOrigins {
    if !config.enabled {
        return TemplateStorageOrigins::default();
    }
    let trusted = match env.var(TRUSTED_TEMPLATE_STORAGE_BUCKETS_ENV) {
        Ok(list) => TrustedStorageBuckets::from_list(&list),
        Err(std::env::VarError::NotPresent) => TrustedStorageBuckets::default(),
        Err(std::env::VarError::NotUnicode(_)) => {
            tracing::error!(
                "runtime_upgrade: {TRUSTED_TEMPLATE_STORAGE_BUCKETS_ENV} must contain UTF-8 \
                 bucket names; the loop stays off"
            );
            config.enabled = false;
            return TemplateStorageOrigins::default();
        }
    };
    let allow_plaintext_endpoints =
        EnvConfig(env).flag(ALLOW_TEMPLATE_STORAGE_PLAINTEXT_ENDPOINTS_ENV, false);
    let allow_endpoint_overrides = allow_plaintext_endpoints
        || EnvConfig(env).flag(ALLOW_TEMPLATE_STORAGE_ENDPOINT_OVERRIDES_ENV, false);
    if allow_plaintext_endpoints {
        tracing::warn!(
            "runtime_upgrade: {ALLOW_TEMPLATE_STORAGE_PLAINTEXT_ENDPOINTS_ENV} is set, so the \
             template driver sends its requests to plain http storage endpoints; use it only for \
             local S3-compatible storage"
        );
    }
    if trusted == TrustedStorageBuckets::default() {
        if allow_endpoint_overrides {
            tracing::warn!(
                "runtime_upgrade: {TRUSTED_TEMPLATE_STORAGE_BUCKETS_ENV} is empty and \
                 {ALLOW_TEMPLATE_STORAGE_ENDPOINT_OVERRIDES_ENV} is set, so the template driver \
                 uses storage locations' endpoint overrides; set the trusted buckets here as \
                 well as on the internal API"
            );
        } else {
            tracing::warn!(
                "runtime_upgrade: {TRUSTED_TEMPLATE_STORAGE_BUCKETS_ENV} is empty, so the \
                 template driver refuses storage locations that override the endpoint; set the \
                 trusted buckets here as well as on the internal API, or set \
                 {ALLOW_TEMPLATE_STORAGE_ENDPOINT_OVERRIDES_ENV} for S3-compatible storage at an \
                 https endpoint"
            );
        }
    }
    TemplateStorageOrigins {
        trusted,
        allow_endpoint_overrides,
        allow_plaintext_endpoints,
    }
}

/// Where one campaign's scan of subjects is.
#[derive(Clone, Copy, Debug, Default)]
struct Pass {
    /// The last subject examined, or `None` at the start of a pass.
    after: Option<uuid::Uuid>,
    /// The plan this pass's pages were examined under.
    plan: Option<ScanPlan>,
    /// Whether nothing changed an item during this pass: it began with no
    /// item pending or in flight, and this loop has leased nothing since.
    /// Only such a pass can complete the campaign: an item leased mid-pass
    /// may end skipped after its page was scanned, and only a later pass
    /// would find its subject on the source again.
    settled: bool,
}

/// Where a tick's page left a pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PassProgress {
    /// The pass continues next tick, or the page failed.
    Continues,
    /// This page ended the pass; `settled` as [`Pass::settled`] had it.
    Ended { settled: bool },
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

pub struct Reconciler {
    pool: PgPool,
    drivers: Drivers,
    worker: worker::Worker,
    config: ReconcilerConfig,
    passes: Mutex<HashMap<CampaignId, Pass>>,
    /// Dispatches still running, across ticks.
    in_flight: Mutex<JoinSet<()>>,
}

impl Reconciler {
    pub fn new(pool: PgPool, drivers: Drivers, config: ReconcilerConfig) -> Self {
        Self {
            worker: worker::Worker::new(pool.clone(), config.lease, config.item_deadline),
            pool,
            drivers,
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
        self.release_lapsed_in_finished().await;
        for campaign in &campaigns {
            match self.drivers.get(campaign.kind) {
                Some(driver) => self.drive(driver, campaign, &mut report).await,
                None => tracing::debug!(
                    campaign_id = %campaign.id,
                    kind = campaign.kind.as_str(),
                    "runtime_upgrade: no driver for the campaign's kind; reporting it only"
                ),
            }
        }
        self.report_items(&campaigns).await;
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

    fn lock_passes(&self) -> std::sync::MutexGuard<'_, HashMap<CampaignId, Pass>> {
        self.passes
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

    async fn drive(
        &self,
        driver: &Arc<dyn CampaignDriver>,
        campaign: &Campaign,
        report: &mut TickReport,
    ) {
        self.redrive(campaign, report).await;
        if !campaign.phase.leases() || self.pause_if_over_budget(campaign, report).await {
            return;
        }
        let plan = match driver.plan(campaign).await {
            Ok(Plan::Scan(plan)) => plan,
            Ok(Plan::Hold(reason)) => {
                metrics::campaign_held(campaign.kind.as_str(), reason);
                tracing::info!(
                    campaign_id = %campaign.id,
                    reason,
                    "runtime_upgrade: holding the campaign"
                );
                return;
            }
            Err(error) => {
                tracing::warn!(campaign_id = %campaign.id, %error, "runtime_upgrade: planning failed; retrying next tick");
                return;
            }
        };
        let progress = self.find_subjects(driver, campaign, plan, report).await;
        // A pass that moved nothing proves nothing moved: a stranded
        // campaign stays open, reporting its stranded count, until its
        // driver moves it or an operator aborts it. A pass that leased
        // anything, or began with work open, proves nothing either; the next
        // one finds any subject it left behind.
        let completed = progress == (PassProgress::Ended { settled: true })
            && plan.moves
            && self.complete_if_done(campaign, report).await;
        if !plan.moves || completed {
            return;
        }
        let leased_at = tokio::time::Instant::now();
        let leased = self.lease(campaign).await;
        if !leased.is_empty() {
            self.unsettle(campaign.id);
        }
        report.leased += leased.len() as u64;
        let mut running = self.lock_in_flight();
        for item in leased {
            running.spawn(self.worker.clone().drive_item(
                Arc::clone(driver),
                campaign.clone(),
                item,
                leased_at,
            ));
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

    /// Examine one page of `campaign`'s subjects under `plan` and record
    /// those on its source as pending. Says whether this page ended a pass;
    /// a failed read or write ends nothing.
    async fn find_subjects(
        &self,
        driver: &Arc<dyn CampaignDriver>,
        campaign: &Campaign,
        plan: ScanPlan,
        report: &mut TickReport,
    ) -> PassProgress {
        let after = {
            let mut passes = self.lock_passes();
            let pass = passes.entry(campaign.id).or_default();
            if pass.after.is_some() && pass.plan != Some(plan) {
                tracing::info!(
                    campaign_id = %campaign.id,
                    "runtime_upgrade: the campaign's plan changed mid-pass; restarting the pass"
                );
                pass.after = None;
            }
            pass.after
        };
        if after.is_none() {
            // A pass starts: it's settled while nothing is open. A failed
            // read decides nothing, so the pass can't complete.
            let settled = store::item_counts(&self.pool, campaign.id)
                .await
                .is_ok_and(|counts| counts.pending == 0 && counts.in_flight == 0);
            self.lock_passes().entry(campaign.id).or_default().settled = settled;
        }
        let page = match driver
            .find_subjects(campaign, plan, after, self.config.candidate_page)
            .await
        {
            Ok(page) => page,
            Err(error) => {
                tracing::warn!(campaign_id = %campaign.id, %error, "runtime_upgrade: finding subjects failed");
                return PassProgress::Continues;
            }
        };
        let subject_kind = campaign.kind.subject_kind();
        if let Err(error) =
            store::retire_skipped(&self.pool, campaign.id, subject_kind, &page.elsewhere).await
        {
            tracing::warn!(campaign_id = %campaign.id, %error, "runtime_upgrade: retiring items failed");
            return PassProgress::Continues;
        }
        match store::insert_pending(
            &self.pool,
            campaign.id,
            subject_kind,
            &page.on_source,
            MAX_ATTEMPTS,
        )
        .await
        {
            Ok(recorded) => report.inserted += recorded,
            Err(error) => {
                tracing::warn!(campaign_id = %campaign.id, %error, "runtime_upgrade: recording items failed");
                return PassProgress::Continues;
            }
        }
        let ended = page.examined < self.config.candidate_page as usize;
        let settled = {
            let mut passes = self.lock_passes();
            let pass = passes.entry(campaign.id).or_default();
            pass.plan = Some(plan);
            pass.after = if ended { None } else { page.last };
            pass.settled
        };
        if !ended {
            return PassProgress::Continues;
        }
        self.report_stranded(campaign, plan).await;
        PassProgress::Ended { settled }
    }

    /// Note that this loop leased items of `campaign`, which unsettles the
    /// pass in progress.
    fn unsettle(&self, campaign_id: CampaignId) {
        if let Some(pass) = self.lock_passes().get_mut(&campaign_id) {
            pass.settled = false;
        }
    }

    /// At the end of a pass, report how many of `campaign`'s subjects are
    /// stranded: its pending items, when the pass didn't move them.
    async fn report_stranded(&self, campaign: &Campaign, plan: ScanPlan) {
        let stranded = if plan.moves {
            0
        } else {
            match store::item_counts(&self.pool, campaign.id).await {
                Ok(counts) => counts.pending,
                Err(error) => {
                    tracing::warn!(campaign_id = %campaign.id, %error, "runtime_upgrade: counting stranded items failed");
                    return;
                }
            }
        };
        metrics::campaign_stranded(&campaign.id.to_string(), stranded);
        if stranded > 0 {
            tracing::warn!(
                campaign_id = %campaign.id,
                stranded,
                "runtime_upgrade: subjects on the campaign's source can't move to its target"
            );
        }
    }

    /// Return finished campaigns' in-flight items whose leases lapsed to
    /// pending. An abort leaves them in flight so no other campaign leases
    /// their subjects while their workers may still be on them.
    async fn release_lapsed_in_finished(&self) {
        match store::release_lapsed_in_finished(&self.pool).await {
            Ok(0) => {}
            Ok(released) => tracing::info!(
                released,
                "runtime_upgrade: released finished campaigns' items whose lease lapsed"
            ),
            Err(error) => tracing::warn!(
                %error,
                "runtime_upgrade: releasing finished campaigns' lapsed items failed; retrying next tick"
            ),
        }
    }

    fn forget_finished(&self, campaigns: &[Campaign]) {
        self.lock_passes()
            .retain(|campaign_id, _| campaigns.iter().any(|campaign| campaign.id == *campaign_id));
    }

    async fn lease(&self, campaign: &Campaign) -> Vec<LeasedItem> {
        store::lease_items(&self.pool, campaign.id, self.config.lease, LEASE_SCAN)
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(campaign_id = %campaign.id, %error, "runtime_upgrade: leasing items failed");
                Vec::new()
            })
    }

    /// Complete `campaign` if its items are drained. Called only on the tick
    /// a settled pass of subjects ends: only then has every subject on the
    /// source when its page was read been examined with nothing changing
    /// underneath. `true` when this call completed it.
    async fn complete_if_done(&self, campaign: &Campaign, report: &mut TickReport) -> bool {
        match store::complete_if_drained(&self.pool, campaign.id).await {
            Ok(true) => {
                tracing::info!(campaign_id = %campaign.id, "runtime_upgrade: the campaign completed");
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

    /// Report each campaign's items by phase, counted in one read.
    async fn report_items(&self, campaigns: &[Campaign]) {
        if campaigns.is_empty() {
            return;
        }
        let ids: Vec<CampaignId> = campaigns.iter().map(|campaign| campaign.id).collect();
        let counts = match store::item_counts_of(&self.pool, &ids).await {
            Ok(counts) => counts,
            Err(error) => {
                tracing::warn!(%error, "runtime_upgrade: counting items failed");
                return;
            }
        };
        for campaign in campaigns {
            let Some(counts) = counts.get(&campaign.id) else {
                continue;
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
}

/// Report every unfinished kind and phase, zero included.
fn report_campaigns(campaigns: &[Campaign]) {
    let mut counts: BTreeMap<(CampaignKind, CampaignPhase), u64> = BTreeMap::new();
    for campaign in campaigns {
        *counts.entry((campaign.kind, campaign.phase)).or_default() += 1;
    }
    for kind in CampaignKind::ALL {
        for phase in CampaignPhase::ALL.iter().filter(|phase| !phase.finished()) {
            metrics::campaigns(
                kind.as_str(),
                phase.as_str(),
                counts.get(&(*kind, *phase)).copied().unwrap_or(0),
            );
        }
    }
}

/// Run the reconciler until `shutdown` fires. Disabled, it only waits.
pub async fn run(
    config: ReconcilerConfig,
    pool: PgPool,
    drivers: Drivers,
    shutdown: CancellationToken,
) {
    if !config.enabled {
        tracing::info!("runtime_upgrade disabled ({ENABLED_ENV} unset); waiting for shutdown");
        shutdown.cancelled().await;
        return;
    }
    let kinds: Vec<&str> = drivers.kinds().iter().map(|kind| kind.as_str()).collect();
    tracing::info!(
        tick_secs = config.tick.as_secs(),
        lease_secs = config.lease.as_secs(),
        ?kinds,
        "starting runtime_upgrade"
    );
    let tick = config.tick;
    let reconciler = Reconciler::new(pool, drivers, config);
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let started = Instant::now();
                // Raced against shutdown, so a tick stuck on a slow read
                // doesn't hold up the drain.
                let report = tokio::select! {
                    report = reconciler.tick() => report,
                    () = shutdown.cancelled() => {
                        tracing::info!("runtime_upgrade stopping mid-tick; aborting the dispatches still running");
                        return;
                    }
                };
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
