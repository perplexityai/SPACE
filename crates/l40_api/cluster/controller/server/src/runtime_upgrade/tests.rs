//! The reconciler against a real Postgres and a fake driver. The fake moves
//! a template campaign, whose items have no node, so the loop's own steps
//! are what's under test; the store's tests cover per-node pacing.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_content_digest::Digest;
use pplx_space_db_test_fixtures::{TestDb, try_pg_test_db_with_max_connections};
use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_runtime_upgrade_campaign_store::test_support::{CampaignSeed, insert_campaign};
use pplx_space_runtime_upgrade_campaign_store::{ItemCounts, ItemError, SubjectKind};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::{TestEnv, TestResult};
use sqlx::PgPool;
use uuid::Uuid;

use super::*;

mod disabled;
mod leases;

fn environment(byte: u8) -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([byte; 32]))
}

async fn test_db() -> TestResult<Option<TestDb>> {
    try_pg_test_db_with_max_connections(8).await
}

/// A rolling template campaign from environment 0 to 1, two in flight at a
/// time, pausing at three failures.
fn seed() -> CampaignSeed {
    CampaignSeed {
        from: environment(0),
        to: environment(1),
        kind: CampaignKind::Templates,
        phase: CampaignPhase::Rolling,
        per_node: 1,
        per_cluster: 2,
        failure_budget: 3,
        include_running: false,
    }
}

async fn counts(pool: &PgPool, campaign: CampaignId) -> TestResult<ItemCounts> {
    Ok(store::item_counts(pool, campaign).await?)
}

async fn campaign_phase(pool: &PgPool, campaign: CampaignId) -> TestResult<String> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "SELECT phase FROM runtime_upgrade_campaigns WHERE id = $1"
    ))
    .bind(campaign)
    .fetch_one(pool)
    .await?)
}

/// An item's phase, error and attempts.
async fn item(
    pool: &PgPool,
    campaign: CampaignId,
    subject: Uuid,
) -> TestResult<(String, Option<String>, i64)> {
    Ok(sqlx::query_as::<_, (String, Option<String>, i64)>(sql!(
        "SELECT phase, last_error, attempts FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(subject)
    .fetch_one(pool)
    .await?)
}

/// Sorted subject ids, as a driver's stable order needs.
fn subjects(count: usize) -> Vec<Uuid> {
    let mut subjects: Vec<Uuid> = (0..count).map(|_| Uuid::now_v7()).collect();
    subjects.sort();
    subjects
}

struct FakeState {
    plan: Result<Plan, String>,
    /// Subjects on the source, in id order.
    on_source: Vec<Uuid>,
    /// Outcomes to give per subject, in order; `Done` once they run out.
    outcomes: HashMap<Uuid, VecDeque<ItemOutcome>>,
    /// The `after` of every `find_subjects` call.
    finds: Vec<Option<Uuid>>,
    /// Subjects in upgrade-call order.
    upgraded: Vec<Uuid>,
    running: usize,
    max_running: usize,
    /// Upgrades that returned.
    finished: usize,
    /// Upgrades running now per subject, dropped ones included until they're
    /// dropped, and the most that ever ran at once for one subject.
    running_by_subject: HashMap<Uuid, usize>,
    max_running_one_subject: usize,
    /// Subjects an upgrade moved. A later upgrade of one finds it on the
    /// target and changes nothing, as an idempotent twin insert does; those
    /// are counted in `already_on_target`.
    applied: std::collections::HashSet<Uuid>,
    already_on_target: usize,
    /// Per-call delays, in call order; [`FakeDriver::delay`] once they run out.
    delays: VecDeque<Duration>,
}

impl Default for FakeState {
    fn default() -> Self {
        Self {
            plan: Ok(Plan::Scan(ScanPlan {
                moves: true,
                inputs: 0,
            })),
            on_source: Vec::new(),
            outcomes: HashMap::new(),
            finds: Vec::new(),
            upgraded: Vec::new(),
            running: 0,
            max_running: 0,
            finished: 0,
            running_by_subject: HashMap::new(),
            max_running_one_subject: 0,
            applied: std::collections::HashSet::new(),
            already_on_target: 0,
            delays: VecDeque::new(),
        }
    }
}

#[derive(Clone, Default)]
struct FakeDriver {
    state: Arc<Mutex<FakeState>>,
    /// How long each upgrade takes.
    delay: Duration,
    /// Signalled each time an upgrade starts.
    started: Arc<tokio::sync::Notify>,
    /// When set, each upgrade waits for it after its delay before returning.
    release: Option<Arc<tokio::sync::Notify>>,
}

impl FakeDriver {
    fn with_subjects(on_source: &[Uuid]) -> Self {
        let driver = Self::default();
        driver.state().on_source = on_source.to_vec();
        driver
    }

    fn state(&self) -> std::sync::MutexGuard<'_, FakeState> {
        self.state.lock().unwrap()
    }

    fn set_plan(&self, plan: Plan) {
        self.state().plan = Ok(plan);
    }

    fn outcomes(&self, subject: Uuid, outcomes: impl IntoIterator<Item = ItemOutcome>) {
        self.state()
            .outcomes
            .insert(subject, outcomes.into_iter().collect());
    }

    fn upgraded(&self) -> Vec<Uuid> {
        self.state().upgraded.clone()
    }

    /// Wait, for at most ten seconds, until an upgrade has started.
    async fn wait_started(&self) {
        tokio::time::timeout(Duration::from_secs(10), self.started.notified())
            .await
            .expect("an upgrade started");
    }
}

#[async_trait::async_trait]
impl CampaignDriver for FakeDriver {
    fn kind(&self) -> CampaignKind {
        CampaignKind::Templates
    }

    async fn plan(&self, _campaign: &Campaign) -> Result<Plan, String> {
        self.state().plan.clone()
    }

    async fn find_subjects(
        &self,
        _campaign: &Campaign,
        _plan: ScanPlan,
        after: Option<Uuid>,
        limit: u32,
    ) -> Result<SubjectPage, String> {
        let mut state = self.state();
        state.finds.push(after);
        let page: Vec<Uuid> = state
            .on_source
            .iter()
            .copied()
            .filter(|subject| after.is_none_or(|after| *subject > after))
            .take(limit as usize)
            .collect();
        Ok(SubjectPage {
            examined: page.len(),
            last: page.last().copied(),
            on_source: page,
            elsewhere: Vec::new(),
        })
    }

    async fn upgrade(&self, _campaign: &Campaign, item: &LeasedItem) -> ItemOutcome {
        let subject = item.subject_id;
        let (outcome, delay) = {
            let mut state = self.state();
            state.upgraded.push(subject);
            state.running += 1;
            state.max_running = state.max_running.max(state.running);
            let on_subject = state.running_by_subject.entry(subject).or_default();
            *on_subject += 1;
            let on_subject = *on_subject;
            state.max_running_one_subject = state.max_running_one_subject.max(on_subject);
            let outcome = state
                .outcomes
                .get_mut(&subject)
                .and_then(VecDeque::pop_front)
                .unwrap_or(ItemOutcome::Done);
            (outcome, state.delays.pop_front().unwrap_or(self.delay))
        };
        self.started.notify_one();
        // Counts this call off its subject however it ends: returned, or
        // dropped by the worker when its lease is lost.
        let _running = SubjectRunning {
            state: Arc::clone(&self.state),
            subject,
        };
        tokio::time::sleep(delay).await;
        if let Some(release) = &self.release {
            release.notified().await;
        }
        let mut state = self.state();
        state.running -= 1;
        state.finished += 1;
        if outcome == ItemOutcome::Done && !state.applied.insert(subject) {
            state.already_on_target += 1;
        }
        outcome
    }
}

/// One upgrade running on one subject, counted off when dropped.
struct SubjectRunning {
    state: Arc<Mutex<FakeState>>,
    subject: Uuid,
}

impl Drop for SubjectRunning {
    fn drop(&mut self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some(running) = state.running_by_subject.get_mut(&self.subject) {
            *running -= 1;
        }
    }
}

fn config() -> ReconcilerConfig {
    ReconcilerConfig {
        enabled: true,
        ..ReconcilerConfig::default()
    }
}

fn reconciler(pool: &PgPool, driver: FakeDriver) -> Reconciler {
    Reconciler::new(pool.clone(), Drivers::default().with(driver), config())
}

/// One tick, and every dispatch it started.
async fn tick(reconciler: &Reconciler) -> TickReport {
    let report = reconciler.tick().await;
    reconciler.settle().await;
    report
}

#[tokio::test(flavor = "current_thread")]
async fn subjects_are_upgraded_within_the_cluster_cap_and_the_campaign_completes() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let found = subjects(5);
    let driver = FakeDriver::with_subjects(&found);
    let reconciler = reconciler(&db.pool, driver.clone());

    let first = tick(&reconciler).await;
    assert_eq!((first.inserted, first.leased), (5, 2));
    assert!(first.completed.is_empty(), "items are still pending");
    let mut ticks = 1;
    while campaign_phase(&db.pool, campaign).await? == "rolling" && ticks < 10 {
        tick(&reconciler).await;
        ticks += 1;
    }

    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    // Two, two and one, then the pass that finds nothing left completes it.
    assert_eq!(ticks, 4);
    let mut upgraded = driver.upgraded();
    upgraded.sort();
    assert_eq!(upgraded, found);
    // Each tick settles its dispatches, so the lease count bounds how many
    // run at once; whether two of them overlap is up to the scheduler.
    assert!(
        driver.state().max_running <= 2,
        "never past the cluster cap"
    );
    assert_eq!(
        counts(&db.pool, campaign).await?,
        ItemCounts {
            done: 5,
            ..ItemCounts::default()
        }
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn failures_pause_the_campaign_at_its_budget() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_cluster: 4,
            per_node: 1,
            failure_budget: 2,
            ..seed()
        },
    )
    .await?;
    let found = subjects(6);
    let driver = FakeDriver::with_subjects(&found);
    for subject in &found {
        driver.outcomes(
            *subject,
            [ItemOutcome::Failed(ItemError::new("the boot failed"))],
        );
    }
    let reconciler = reconciler(&db.pool, driver.clone());

    tick(&reconciler).await;
    // Four were in flight at once, so the failures overshoot the budget of 2.
    assert_eq!(counts(&db.pool, campaign).await?.failed, 4);
    let second = tick(&reconciler).await;

    assert_eq!(second.paused, vec![campaign]);
    assert_eq!(second.leased, 0);
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "paused");
    // Paused, the campaign leases nothing more.
    tick(&reconciler).await;
    assert_eq!(driver.upgraded().len(), 4);
    assert_eq!(counts(&db.pool, campaign).await?.pending, 2);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retries_run_until_the_attempts_are_used_up() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let found = subjects(1);
    let driver = FakeDriver::with_subjects(&found);
    driver.outcomes(
        found[0],
        std::iter::repeat_n(ItemOutcome::Retry(ItemError::new("busy")), 10),
    );
    let reconciler = reconciler(&db.pool, driver.clone());

    for _ in 0..MAX_ATTEMPTS + 2 {
        tick(&reconciler).await;
    }

    assert_eq!(driver.upgraded().len() as u64, MAX_ATTEMPTS);
    assert_eq!(
        item(&db.pool, campaign, found[0]).await?,
        (
            "failed".to_owned(),
            Some(format!("busy (gave up after {MAX_ATTEMPTS} attempts)")),
            MAX_ATTEMPTS as i64
        )
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_held_campaign_finds_and_leases_nothing() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let driver = FakeDriver::with_subjects(&subjects(3));
    driver.set_plan(Plan::Hold("target_not_default"));
    let reconciler = reconciler(&db.pool, driver.clone());

    let report = tick(&reconciler).await;

    assert_eq!(report, TickReport::default());
    assert!(driver.state().finds.is_empty());
    assert_eq!(counts(&db.pool, campaign).await?, ItemCounts::default());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stranded_subjects_are_recorded_pending_and_move_once_the_plan_does() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let found = subjects(3);
    let driver = FakeDriver::with_subjects(&found);
    driver.set_plan(Plan::Scan(ScanPlan {
        moves: false,
        inputs: 0,
    }));
    let reconciler = reconciler(&db.pool, driver.clone());

    for _ in 0..3 {
        let report = tick(&reconciler).await;
        assert_eq!(report.leased, 0);
        assert!(
            report.completed.is_empty(),
            "a stranded campaign stays open"
        );
    }
    assert!(driver.upgraded().is_empty());
    assert_eq!(counts(&db.pool, campaign).await?.pending, 3);

    driver.set_plan(Plan::Scan(ScanPlan {
        moves: true,
        inputs: 0,
    }));
    assert_eq!(tick(&reconciler).await.leased, 2);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_canary_upgrades_one_subject_and_waits_for_resume() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Canary,
            ..seed()
        },
    )
    .await?;
    let driver = FakeDriver::with_subjects(&subjects(3));
    let reconciler = reconciler(&db.pool, driver.clone());

    for _ in 0..3 {
        tick(&reconciler).await;
    }

    assert_eq!(driver.upgraded().len(), 1);
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "canary");
    assert_eq!(counts(&db.pool, campaign).await?.pending, 2);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_campaign_whose_kind_has_no_driver_is_left_alone() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let sandboxes = insert_campaign(
        &db.pool,
        CampaignSeed {
            kind: CampaignKind::Sandboxes,
            ..seed()
        },
    )
    .await?;
    // A lapsed lease that a driven campaign would re-drive.
    let subject = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO runtime_upgrade_items (
             campaign_id, subject_kind, subject_id, phase, attempts, lease_token,
             lease_expires_at
         )
         VALUES ($1, $2, $3, 'in_flight', 1, $4, now() - interval '1 second')"
    ))
    .bind(sandboxes)
    .bind(SubjectKind::Sandbox.as_str())
    .bind(subject)
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    let driver = FakeDriver::with_subjects(&subjects(2));
    let reconciler = reconciler(&db.pool, driver.clone());

    let report = tick(&reconciler).await;

    assert_eq!(report, TickReport::default());
    assert!(
        driver.state().finds.is_empty(),
        "the template driver isn't asked"
    );
    assert_eq!(item(&db.pool, sandboxes, subject).await?.0, "in_flight");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_plan_that_changes_mid_pass_restarts_the_pass() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let found = subjects(5);
    let driver = FakeDriver::with_subjects(&found);
    let reconciler = Reconciler::new(
        db.pool.clone(),
        Drivers::default().with(driver.clone()),
        ReconcilerConfig {
            candidate_page: 2,
            ..config()
        },
    );

    tick(&reconciler).await;
    tick(&reconciler).await;
    driver.set_plan(Plan::Scan(ScanPlan {
        moves: true,
        inputs: 1,
    }));
    tick(&reconciler).await;

    assert_eq!(
        driver.state().finds,
        vec![None, Some(found[1]), None],
        "the third page starts over under the new inputs"
    );
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_campaign_completes_only_on_a_tick_that_ends_a_pass() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_cluster: 10,
            ..seed()
        },
    )
    .await?;
    let found = subjects(3);
    let driver = FakeDriver::with_subjects(&found);
    let reconciler = Reconciler::new(
        db.pool.clone(),
        Drivers::default().with(driver.clone()),
        ReconcilerConfig {
            candidate_page: 2,
            ..config()
        },
    );

    // Page one records and upgrades two subjects; nothing is pending
    // afterwards, but the pass hasn't seen the third yet.
    let first = tick(&reconciler).await;
    assert_eq!((first.inserted, first.leased), (2, 2));
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");
    // Page two ends the pass with the third pending.
    assert!(tick(&reconciler).await.completed.is_empty());
    // The next pass starts over, finds nothing new and ends on page two.
    tick(&reconciler).await;
    let report = tick(&reconciler).await;
    assert_eq!(report.completed, vec![campaign]);
    assert_eq!(counts(&db.pool, campaign).await?.done, 3);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_subject_skipped_after_its_page_is_upgraded_before_the_campaign_completes() -> TestResult
{
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_cluster: 10,
            ..seed()
        },
    )
    .await?;
    let found = subjects(2);
    let driver = FakeDriver::with_subjects(&found);
    // The first subject is skipped once, for a passing reason, after its page
    // was scanned; the next pass finds it on the source again.
    driver.outcomes(
        found[0],
        [ItemOutcome::Skipped(ItemError::new("the subject was busy"))],
    );
    let reconciler = Reconciler::new(
        db.pool.clone(),
        Drivers::default().with(driver.clone()),
        ReconcilerConfig {
            candidate_page: 1,
            ..config()
        },
    );

    let mut ticks = 0;
    while campaign_phase(&db.pool, campaign).await? == "rolling" && ticks < 12 {
        tick(&reconciler).await;
        ticks += 1;
    }

    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(
        item(&db.pool, campaign, found[0]).await?.0,
        "done",
        "the skipped subject was upgraded before the campaign completed"
    );
    assert_eq!(counts(&db.pool, campaign).await?.done, 2);
    Ok(())
}

#[test]
fn the_api_starts_campaigns_of_exactly_the_kinds_this_build_drives() {
    assert_eq!(
        cluster_drivers().kinds(),
        pplx_space_api::routes::admin::RUNTIME_UPGRADE_DRIVEN_KINDS
    );
}

#[test]
fn the_reconciler_is_off_unless_switched_on() {
    let mut env = TestEnv::default();
    assert_eq!(
        ReconcilerConfig::from_environment(&env),
        ReconcilerConfig::default()
    );
    assert!(!ReconcilerConfig::default().enabled);
    for value in ["0", "false", "off", "", "yes please"] {
        env.set(ENABLED_ENV, value);
        assert!(
            !ReconcilerConfig::from_environment(&env).enabled,
            "{value:?}"
        );
    }
    for value in ["1", "yes", "ON", "enabled"] {
        env.set(ENABLED_ENV, value);
        assert!(
            ReconcilerConfig::from_environment(&env).enabled,
            "{value:?}"
        );
    }
    let default = ReconcilerConfig::default();
    assert_eq!(
        (
            default.tick,
            default.lease,
            default.candidate_page,
            default.item_deadline
        ),
        (
            Duration::from_secs(30),
            Duration::from_secs(300),
            500,
            Duration::from_secs(1800)
        )
    );
    env.set(ITEM_DEADLINE_ENV, "1200");
    assert_eq!(
        ReconcilerConfig::from_environment(&env).item_deadline,
        Duration::from_secs(1200)
    );
    env.set(ITEM_DEADLINE_ENV, "90");
    assert_eq!(
        ReconcilerConfig::from_environment(&env).item_deadline,
        Duration::from_secs(15 * 60),
        "raised to the floor, room for an upgrade's boot, backup and cleanup"
    );
    env.set(ITEM_DEADLINE_ENV, u64::MAX.to_string());
    assert_eq!(
        ReconcilerConfig::from_environment(&env).item_deadline,
        Duration::from_secs(24 * 60 * 60),
        "clamped, so a deadline never overflows the clock"
    );
    for value in ["0", "soon"] {
        env.set(ITEM_DEADLINE_ENV, value);
        assert_eq!(
            ReconcilerConfig::from_environment(&env).item_deadline,
            Duration::from_secs(1800),
            "{value:?}"
        );
    }
}
