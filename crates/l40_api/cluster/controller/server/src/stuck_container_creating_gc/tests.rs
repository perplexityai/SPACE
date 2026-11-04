use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::Pod;
use pplx_space_common_core::types::{PodUid, SandboxId};
use serde_json::json;
use uuid::Uuid;

use super::*;
use pplx_space_api::controllers::substrate::kubernetes::SandboxPodView;

const THRESHOLD_SECS: u64 = 600;

fn threshold() -> Duration {
    Duration::from_secs(THRESHOLD_SECS)
}

/// A fixed monotonic instant so age math is deterministic. Offset a day past a
/// real `Instant::now()` so the tests' `now() - 20min` subtractions can't
/// underflow the monotonic epoch on a freshly-booted host.
static TEST_NOW: LazyLock<Instant> = LazyLock::new(|| Instant::now() + Duration::from_secs(86_400));

fn now() -> Instant {
    *TEST_NOW
}

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

/// A synthetic `kube::Error` for the fake backend's error paths. The GC only
/// logs the source, so the exact status doesn't matter.
fn fake_kube_error() -> kube::Error {
    use kube::error::Status;
    kube::Error::Api(
        Status::failure("synthetic kube api error", "Synthetic")
            .with_code(500)
            .boxed(),
    )
}

/// A controllable monotonic clock the reconciler samples for age and rate-limit
/// windows.
struct TestClock(Arc<Mutex<Instant>>);

impl TestClock {
    fn new(at: Instant) -> Self {
        Self(Arc::new(Mutex::new(at)))
    }

    fn set(&self, at: Instant) {
        *self.0.lock().unwrap() = at;
    }

    fn clock(&self) -> Clock {
        let handle = self.0.clone();
        Arc::new(move || *handle.lock().unwrap())
    }
}

/// Build a reconciler over a fixed test clock (starting at `now()`), returning
/// the clock so multi-tick tests can advance it. Pre-seeds the observation
/// clock for every backend pod to 20 minutes ago so a single tick treats them
/// as already past the 10-minute threshold — the common case. Tests that
/// exercise the observation clock itself use [`build_unprimed`].
fn build(config: ReconcilerConfig, backend: FakeBackend) -> (Reconciler<FakeBackend>, TestClock) {
    let (mut reconciler, clock) = build_unprimed(config, backend);
    let uids: Vec<String> = reconciler
        .backend
        .pods
        .iter()
        .filter_map(|pod| pod.pod_uid.as_ref().map(|uid| uid.as_str().to_string()))
        .collect();
    for uid in uids {
        reconciler.observe_since(&uid, now() - secs(20 * 60));
    }
    (reconciler, clock)
}

/// Build a reconciler without seeding the observation clock, so pods start as
/// freshly-observed (age 0).
fn build_unprimed(
    config: ReconcilerConfig,
    backend: FakeBackend,
) -> (Reconciler<FakeBackend>, TestClock) {
    let clock = TestClock::new(now());
    let reconciler = Reconciler::new(config, backend).with_clock(clock.clock());
    (reconciler, clock)
}

fn test_config(max_deletes_per_minute: u32) -> ReconcilerConfig {
    ReconcilerConfig {
        enabled: true,
        interval: DEFAULT_INTERVAL,
        threshold: threshold(),
        max_deletes_per_minute,
        // Large by default so the fixed-clock tests (elapsed always 0) never
        // trip the budget; the deferral test sets its own via an advancing clock.
        tick_budget: Duration::from_secs(3600),
    }
}

/// A pod wedged in `ContainerCreating` on `node-a`. Its UID is `{name}-uid`,
/// which [`build`] pre-seeds into the observation clock as 20 minutes old.
fn stuck_snapshot(name: &str) -> PodSnapshot {
    PodSnapshot {
        pod_name: name.to_string(),
        pod_uid: Some(format!("{name}-uid").parse().unwrap()),
        resource_version: Some(format!("{name}-rv")),
        sandbox_id: Some(SandboxId::new()),
        node_name: Some("node-a".to_string()),
        is_restore: false,
        is_warm_pool: false,
        terminating: false,
        phase_pending: true,
        has_container_creating: true,
        any_container_running: false,
    }
}

#[derive(Debug, Clone)]
enum FreshOverride {
    Gone,
    Snapshot(PodSnapshot),
    Error,
}

#[derive(Debug, Clone, Copy)]
enum DeleteOverride {
    NotFound,
    StaleIdentity,
    Error,
}

#[derive(Default)]
struct FakeBackend {
    ready: bool,
    pods: Vec<PodSnapshot>,
    node_states: HashMap<String, NodeEligibility>,
    fresh_overrides: HashMap<String, FreshOverride>,
    delete_overrides: HashMap<String, DeleteOverride>,
    /// When set, `read_pod_fresh`/`delete_pod` awaits this long — used (under
    /// `start_paused`) to exercise the per-tick wall-clock deadline. Applies to
    /// every pod unless `slow_pods` is non-empty, in which case only those.
    op_delay: Option<Duration>,
    /// When non-empty, `op_delay` applies only to these pod names; other pods
    /// respond instantly. Lets a test make one candidate slow (deadline-tripping)
    /// while the rest stay fast.
    slow_pods: HashSet<String>,
    /// Simulates a node cordoning / going `NotReady` during the verify GET:
    /// once a fresh read has happened, the post-GET recheck returns this instead
    /// of the map value. `None` = stable node state.
    node_eligibility_recheck: Option<NodeEligibility>,
    /// Records (pod_name, pod_uid, resource_version) per delete call.
    deletes: Mutex<Vec<(String, String, Option<String>)>>,
    fresh_reads: Mutex<Vec<String>>,
}

impl FakeBackend {
    fn new(pods: Vec<PodSnapshot>) -> Self {
        Self {
            ready: true,
            pods,
            ..Default::default()
        }
    }

    fn with_node_state(mut self, node: &str, eligibility: NodeEligibility) -> Self {
        self.node_states.insert(node.to_string(), eligibility);
        self
    }

    fn with_node_eligibility_recheck(mut self, eligibility: NodeEligibility) -> Self {
        self.node_eligibility_recheck = Some(eligibility);
        self
    }

    fn with_slow_pod(mut self, pod_name: &str) -> Self {
        self.slow_pods.insert(pod_name.to_string());
        self
    }

    fn should_delay(&self, pod_name: &str) -> bool {
        self.slow_pods.is_empty() || self.slow_pods.contains(pod_name)
    }

    fn with_fresh_override(mut self, pod_name: &str, override_: FreshOverride) -> Self {
        self.fresh_overrides.insert(pod_name.to_string(), override_);
        self
    }

    fn with_delete_override(mut self, pod_name: &str, override_: DeleteOverride) -> Self {
        self.delete_overrides
            .insert(pod_name.to_string(), override_);
        self
    }

    fn delete_count(&self) -> usize {
        self.deletes.lock().unwrap().len()
    }

    fn deleted_names(&self) -> Vec<String> {
        self.deletes
            .lock()
            .unwrap()
            .iter()
            .map(|(name, _uid, _rv)| name.clone())
            .collect()
    }

    fn fresh_read_count(&self) -> usize {
        self.fresh_reads.lock().unwrap().len()
    }
}

impl GcBackend for FakeBackend {
    fn caches_ready(&self) -> bool {
        self.ready
    }

    async fn list_sandbox_pods(&self) -> Vec<PodSnapshot> {
        self.pods.clone()
    }

    fn node_eligibility(&self, node_name: &str) -> NodeEligibility {
        // Simulate a node cordoning / going NotReady *during* the verify GET:
        // once a fresh read has happened, the post-GET recheck sees the flipped
        // state. Before any GET (the scan-phase pause check and the pre-GET
        // filter) it reads the stable map value.
        if let Some(recheck) = self.node_eligibility_recheck
            && !self.fresh_reads.lock().unwrap().is_empty()
        {
            return recheck;
        }
        // Default to Eligible so tests that don't care about node state stay
        // terse; tests that exercise the node guard set it explicitly.
        self.node_states
            .get(node_name)
            .copied()
            .unwrap_or(NodeEligibility::Eligible)
    }

    async fn read_pod_fresh(&self, pod_name: &str) -> Result<Option<PodSnapshot>, BackendError> {
        if let Some(delay) = self.op_delay
            && self.should_delay(pod_name)
        {
            tokio::time::sleep(delay).await;
        }
        self.fresh_reads.lock().unwrap().push(pod_name.to_string());
        match self.fresh_overrides.get(pod_name) {
            Some(FreshOverride::Gone) => Ok(None),
            Some(FreshOverride::Snapshot(snapshot)) => Ok(Some(snapshot.clone())),
            Some(FreshOverride::Error) => Err(BackendError::Kube(fake_kube_error())),
            // Default: the pod is unchanged — still the same stuck object.
            None => Ok(self.pods.iter().find(|p| p.pod_name == pod_name).cloned()),
        }
    }

    async fn delete_pod(
        &self,
        pod_name: &str,
        pod_uid: &PodUid,
        resource_version: Option<&str>,
    ) -> Result<DeleteOutcome, BackendError> {
        if let Some(delay) = self.op_delay
            && self.should_delay(pod_name)
        {
            tokio::time::sleep(delay).await;
        }
        self.deletes.lock().unwrap().push((
            pod_name.to_string(),
            pod_uid.as_str().to_string(),
            resource_version.map(str::to_string),
        ));
        match self.delete_overrides.get(pod_name) {
            Some(DeleteOverride::NotFound) => Ok(DeleteOutcome::NotFound),
            Some(DeleteOverride::StaleIdentity) => Ok(DeleteOutcome::StaleIdentity),
            Some(DeleteOverride::Error) => Err(BackendError::Kube(fake_kube_error())),
            None => Ok(DeleteOutcome::Deleted),
        }
    }
}

fn skip_count(report: &TickReport, reason: SkipReason) -> u32 {
    report.skipped.get(&reason).copied().unwrap_or(0)
}

// ---- convergence prerequisite --------------------------------------------

#[test]
fn convergence_gate_requires_both_reconcilers_live() {
    // (orphaned-sandbox live, transient sweeper live).
    assert!(convergence_allows_live(true, true));
    assert!(!convergence_allows_live(true, false));
    assert!(!convergence_allows_live(false, true));
    assert!(!convergence_allows_live(false, false));
}

#[test]
fn transient_lifecycle_covered_rejects_disabled_and_practically_disabled() {
    use crate::transient_lifecycle_sweeper::LifecycleAge;
    use pplx_space_common_core::types::PositiveDuration;
    let bounded = Duration::from_secs(600);
    let year = Duration::from_secs(365 * 24 * 60 * 60);

    // `UseDefault` resolves to the sweeper's default age: a bounded default is
    // covered, but the practically-disabled one-year example is not.
    assert!(transient_lifecycle_covered(
        LifecycleAge::UseDefault,
        bounded
    ));
    assert!(!transient_lifecycle_covered(LifecycleAge::UseDefault, year));

    // An explicit bounded override is covered regardless of the default; an
    // explicit huge override is not.
    assert!(transient_lifecycle_covered(
        LifecycleAge::Override(PositiveDuration::new_panic(bounded)),
        year,
    ));
    assert!(!transient_lifecycle_covered(
        LifecycleAge::Override(PositiveDuration::new_panic(year)),
        bounded,
    ));

    // `Disabled` is never covered.
    assert!(!transient_lifecycle_covered(
        LifecycleAge::Disabled,
        bounded
    ));
}

// ---- is_stuck_shaped -----------------------------------------------------

#[test]
fn stuck_shaped_true_for_wedged_pod() {
    assert!(is_stuck_shaped(&stuck_snapshot("p")));
}

#[test]
fn not_stuck_shaped_when_container_running() {
    let mut snapshot = stuck_snapshot("p");
    snapshot.any_container_running = true;
    assert!(!is_stuck_shaped(&snapshot));
}

#[test]
fn not_stuck_shaped_when_not_pending() {
    let mut snapshot = stuck_snapshot("p");
    snapshot.phase_pending = false;
    assert!(!is_stuck_shaped(&snapshot));
}

#[test]
fn not_stuck_shaped_when_terminating() {
    let mut snapshot = stuck_snapshot("p");
    snapshot.terminating = true;
    assert!(!is_stuck_shaped(&snapshot));
}

#[test]
fn not_stuck_shaped_without_container_creating() {
    let mut snapshot = stuck_snapshot("p");
    snapshot.has_container_creating = false;
    assert!(!is_stuck_shaped(&snapshot));
}

// ---- observation clock (age = continuous ContainerCreating, not lifetime) --

/// A pod is a candidate only after it has been observed wedged for the
/// threshold — the first tick that sees it merely starts its clock. This is the
/// guard against deleting a pod whose old `startTime` predates a fresh
/// `ContainerCreating` transition.
#[tokio::test]
async fn stuck_pod_becomes_candidate_only_after_threshold_observed() {
    let backend = FakeBackend::new(vec![stuck_snapshot("p")]);
    let (mut reconciler, clock) = build_unprimed(test_config(60), backend);

    let first = reconciler.tick().await;
    assert_eq!(first.deleted, 0);
    assert_eq!(first.stuck_detected, 0);
    assert_eq!(first.too_young, 1);
    assert_eq!(reconciler.backend.delete_count(), 0);

    // Once the pod has held the stuck shape past the threshold, it's actioned.
    clock.set(now() + secs(11 * 60));
    let second = reconciler.tick().await;
    assert_eq!(second.stuck_detected, 1);
    assert_eq!(second.deleted, 1);
}

/// The observation clock resets when a pod stops being stuck-shaped: a pod that
/// recovers and later re-wedges must re-age from the re-wedge, not inherit its
/// stale pre-recovery age. If `tick` stopped dropping the recovered pod's
/// `first_seen_stuck` entry, the re-wedged pod would inherit the 20-minute-ago
/// observation and be deleted on the very first tick after re-wedging — the
/// early-delete regression this guards.
#[tokio::test]
async fn observation_clock_resets_when_pod_recovers_and_rewedges() {
    let stuck = stuck_snapshot("p");
    let mut recovered = stuck.clone();
    recovered.any_container_running = true;
    let backend = FakeBackend::new(vec![stuck.clone()]);
    let (mut reconciler, clock) = build_unprimed(test_config(60), backend);

    // Seed it as long-observed (well past threshold), then have this tick see it
    // recovered → its clock entry is dropped and it isn't a candidate.
    reconciler.observe_since("p-uid", now() - secs(20 * 60));
    reconciler.backend.pods = vec![recovered];
    let recovered_tick = reconciler.tick().await;
    assert_eq!(recovered_tick.stuck_detected, 0);
    assert_eq!(recovered_tick.deleted, 0);

    // The same UID re-wedges much later. Because the pre-recovery observation
    // was dropped, the clock restarts at the re-wedge — it's too young now, not
    // an instant delete on the stale 20-minutes-ago age.
    clock.set(now() + secs(30 * 60));
    reconciler.backend.pods = vec![stuck.clone()];
    let rewedge_tick = reconciler.tick().await;
    assert_eq!(rewedge_tick.stuck_detected, 0);
    assert_eq!(rewedge_tick.too_young, 1);
    assert_eq!(rewedge_tick.deleted, 0);
    assert_eq!(reconciler.backend.delete_count(), 0);

    // Once the *fresh* clock passes the threshold, it's actioned — proving the
    // reset re-ages forward rather than discarding the pod permanently.
    clock.set(now() + secs(30 * 60) + secs(11 * 60));
    let matured_tick = reconciler.tick().await;
    assert_eq!(matured_tick.stuck_detected, 1);
    assert_eq!(matured_tick.deleted, 1);
}

/// Age is measured only against the injected monotonic clock, so elapsed time
/// strictly below `threshold` is never eligible — and since production reads
/// `Instant::now()` (never a wall clock), a chrony/NTP step of the host clock
/// can't shortcut the guard. Regression for the wall-clock-jump premature
/// deletion: a pod first observed at `now()` stays too young at
/// `threshold - 1s` and only becomes a candidate at `threshold`.
#[tokio::test]
async fn observation_age_below_threshold_stays_too_young() {
    let backend = FakeBackend::new(vec![stuck_snapshot("p")]);
    let (mut reconciler, clock) = build_unprimed(test_config(60), backend);

    // First observed now → age 0.
    let first = reconciler.tick().await;
    assert_eq!(first.too_young, 1);
    assert_eq!(first.deleted, 0);

    // One second short of the threshold: still too young, no delete.
    clock.set(now() + secs(THRESHOLD_SECS - 1));
    let just_under = reconciler.tick().await;
    assert_eq!(just_under.too_young, 1);
    assert_eq!(just_under.stuck_detected, 0);
    assert_eq!(just_under.deleted, 0);
    assert_eq!(reconciler.backend.delete_count(), 0);

    // Exactly at the threshold: now a candidate and deleted.
    clock.set(now() + secs(THRESHOLD_SECS));
    let at_threshold = reconciler.tick().await;
    assert_eq!(at_threshold.stuck_detected, 1);
    assert_eq!(at_threshold.deleted, 1);
}

/// When the authoritative fresh GET shows a same-UID pod recovered, the
/// observation clock is reset even though the (stale) cached scan still showed
/// it stuck. Otherwise a re-wedge the reflector scan hasn't yet observed would
/// inherit the old age and be deleted immediately, bypassing the threshold.
#[tokio::test]
async fn fresh_read_progress_resets_observation_clock() {
    let pod = stuck_snapshot("p");
    let mut recovered = pod.clone();
    recovered.any_container_running = true;
    // Cache shows stuck (primed 20min ago), but the fresh GET shows recovered.
    let backend =
        FakeBackend::new(vec![pod]).with_fresh_override("p", FreshOverride::Snapshot(recovered));
    let (mut reconciler, clock) = build(test_config(60), backend);

    // Tick 1: candidate by cached age, but the fresh read shows progress → skip
    // and reset the clock.
    let first = reconciler.tick().await;
    assert_eq!(first.deleted, 0);
    assert_eq!(skip_count(&first, SkipReason::ProgressingOnRecheck), 1);

    // The pod re-wedges: the fresh read now shows it stuck again, and only a
    // little real time has passed.
    reconciler.backend.fresh_overrides.remove("p");
    clock.set(now() + secs(60));
    let second = reconciler.tick().await;
    // The clock restarted at tick 1's reset, so the re-wedge is too young — not
    // an immediate delete on the stale 20-minutes-ago age.
    assert_eq!(second.stuck_detected, 0);
    assert_eq!(second.too_young, 1);
    assert_eq!(second.deleted, 0);
    assert_eq!(reconciler.backend.delete_count(), 0);
}

// ---- tick: cache readiness ----------------------------------------------

#[tokio::test]
async fn tick_skips_when_cache_cold() {
    let mut backend = FakeBackend::new(vec![stuck_snapshot("p")]);
    backend.ready = false;
    let (mut reconciler, _clock) = build(test_config(60), backend);
    let report = reconciler.tick().await;
    assert!(report.cache_not_ready);
    assert_eq!(report.scanned, 0);
    assert_eq!(reconciler.backend.delete_count(), 0);
}

// ---- tick: happy paths ---------------------------------------------------

#[tokio::test]
async fn tick_deletes_stuck_pod_live() {
    let pod = stuck_snapshot("p");
    // The backend records the delete's UID precondition as a string.
    let uid = pod.pod_uid.clone().unwrap().as_str().to_string();
    let backend = FakeBackend::new(vec![pod]);
    let (mut reconciler, _clock) = build(test_config(60), backend);
    let report = reconciler.tick().await;
    assert_eq!(report.scanned, 1);
    assert_eq!(report.stuck_detected, 1);
    assert_eq!(report.deleted, 1);
    assert_eq!(report.delete_failed, 0);
    let deletes = reconciler.backend.deletes.lock().unwrap();
    // The delete carries the pod UID and the fresh read's resourceVersion as
    // preconditions.
    assert_eq!(
        deletes.as_slice(),
        &[("p".to_string(), uid, Some("p-rv".to_string()))]
    );
}

/// The verify GET's resourceVersion must be carried into the delete as a
/// precondition, so a same-UID status update (e.g. the pod going Running)
/// between the GET and the delete fails the delete rather than removing a
/// recovered pod. Here the fresh read reports a newer resourceVersion than the
/// cached scan, and it's that fresh value the delete is fenced on.
#[tokio::test]
async fn tick_delete_fences_on_fresh_resource_version() {
    let pod = stuck_snapshot("p");
    let mut fresh = pod.clone();
    fresh.resource_version = Some("fresh-rv".to_string());
    let backend =
        FakeBackend::new(vec![pod]).with_fresh_override("p", FreshOverride::Snapshot(fresh));
    let (mut reconciler, _clock) = build(test_config(60), backend);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 1);
    let deletes = reconciler.backend.deletes.lock().unwrap();
    assert_eq!(deletes.len(), 1);
    assert_eq!(deletes[0].2.as_deref(), Some("fresh-rv"));
}

#[tokio::test]
async fn tick_counts_too_young_and_ignores_not_stuck() {
    // `young` is stuck-shaped but only just observed → too young. `running` is
    // not stuck-shaped → ignored. `stuck` is observed past the threshold →
    // deleted.
    let young = stuck_snapshot("young");
    let mut running = stuck_snapshot("running");
    running.any_container_running = true;
    let backend = FakeBackend::new(vec![young, running, stuck_snapshot("stuck")]);
    let (mut reconciler, _clock) = build_unprimed(test_config(60), backend);
    reconciler.observe_since("stuck-uid", now() - secs(20 * 60));
    let report = reconciler.tick().await;
    assert_eq!(report.scanned, 3);
    assert_eq!(report.too_young, 1);
    assert_eq!(report.stuck_detected, 1);
    assert_eq!(report.deleted, 1);
}

// ---- tick: node-state guard ---------------------------------------------

/// A stuck pod on an ineligible node (NotReady / cordoned / unknown) is never
/// deleted: the observation clock is paused in the scan phase, so even a pod
/// primed well past the threshold is held too-young rather than actioned.
/// node-drain / node remediation owns the node.
#[tokio::test]
async fn tick_pauses_clock_on_ineligible_node() {
    for state in [
        NodeEligibility::NotReady,
        NodeEligibility::Cordoned,
        NodeEligibility::Unknown,
    ] {
        let backend = FakeBackend::new(vec![stuck_snapshot("p")]).with_node_state("node-a", state);
        // `build` primes the clock 20 minutes ago — well past threshold — but
        // the ineligible node pauses it, so the pod is still held too-young.
        let (mut reconciler, _clock) = build(test_config(60), backend);
        let report = reconciler.tick().await;
        assert_eq!(report.deleted, 0, "state {state:?}");
        assert_eq!(report.stuck_detected, 0, "state {state:?}");
        assert_eq!(report.too_young, 1, "state {state:?}");
        assert_eq!(reconciler.backend.delete_count(), 0, "state {state:?}");
    }
}

/// The observation clock pauses while the node is ineligible and restarts on
/// recovery, so a pod that sat stuck through a long node outage still gets the
/// full healthy-node threshold before deletion — it isn't deleted the instant
/// kubelet resumes the create.
#[tokio::test]
async fn node_recovery_restarts_observation_clock() {
    let backend = FakeBackend::new(vec![stuck_snapshot("p")])
        .with_node_state("node-a", NodeEligibility::NotReady);
    let (mut reconciler, clock) = build_unprimed(test_config(60), backend);

    // Node down at first observation.
    let t1 = reconciler.tick().await;
    assert_eq!(t1.too_young, 1);

    // Node stays down far past the threshold — age must not accumulate.
    clock.set(now() + secs(30 * 60));
    let t2 = reconciler.tick().await;
    assert_eq!(t2.deleted, 0);
    assert_eq!(t2.stuck_detected, 0);

    // Node recovers: the clock restarts now, so the pod is too young again — it
    // is not deleted on the age it "accrued" while the node was down.
    reconciler
        .backend
        .node_states
        .insert("node-a".to_string(), NodeEligibility::Eligible);
    let t3 = reconciler.tick().await;
    assert_eq!(t3.too_young, 1);
    assert_eq!(t3.stuck_detected, 0);
    assert_eq!(t3.deleted, 0);
    assert_eq!(reconciler.backend.delete_count(), 0);

    // A full threshold after recovery, it's finally actioned.
    clock.set(now() + secs(30 * 60) + secs(11 * 60));
    let t4 = reconciler.tick().await;
    assert_eq!(t4.deleted, 1);
}

// ---- tick: rate limit ----------------------------------------------------

#[tokio::test]
async fn tick_rate_limits_deletes() {
    let backend = FakeBackend::new(vec![
        stuck_snapshot("p1"),
        stuck_snapshot("p2"),
        stuck_snapshot("p3"),
    ]);
    let (mut reconciler, _clock) = build(test_config(2), backend);
    let report = reconciler.tick().await;
    assert_eq!(report.stuck_detected, 3);
    assert_eq!(report.deleted, 2);
    assert_eq!(skip_count(&report, SkipReason::RateLimited), 1);
    assert_eq!(reconciler.backend.delete_count(), 2);
}

/// The rate-limit budget must bound the authoritative (uncached) GETs too, not
/// just successful deletes — otherwise a population of candidates that all fail
/// the fresh recheck would fire one apiserver GET each, unbounded, defeating the
/// whole point of the loop (reducing control-plane load). Five stuck pods that
/// all recheck as progressing, with a budget of 2, must produce exactly 2 GETs.
#[tokio::test]
async fn tick_bounds_apiserver_gets_when_rechecks_fail() {
    let mut backend = FakeBackend::new(vec![
        stuck_snapshot("p1"),
        stuck_snapshot("p2"),
        stuck_snapshot("p3"),
        stuck_snapshot("p4"),
        stuck_snapshot("p5"),
    ]);
    // Every fresh re-read shows the pod recovered to Running, so no candidate
    // ever lands a delete — the failure mode where budget would never decrement
    // if it were charged only on delete.
    for name in ["p1", "p2", "p3", "p4", "p5"] {
        let mut recovered = stuck_snapshot(name);
        recovered.any_container_running = true;
        backend
            .fresh_overrides
            .insert(name.to_string(), FreshOverride::Snapshot(recovered));
    }
    let (mut reconciler, _clock) = build(test_config(2), backend);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 0);
    // Exactly `max_deletes_per_minute` candidates were acted on (GET issued);
    // the rest were rate-limited before touching the apiserver.
    assert_eq!(reconciler.backend.fresh_read_count(), 2);
    assert_eq!(reconciler.backend.delete_count(), 0);
    assert_eq!(skip_count(&report, SkipReason::ProgressingOnRecheck), 2);
    assert_eq!(skip_count(&report, SkipReason::RateLimited), 3);
}

#[tokio::test]
async fn rate_limit_window_frees_budget_after_expiry() {
    let backend = FakeBackend::new(vec![stuck_snapshot("p1")]);
    let (mut reconciler, clock) = build(test_config(1), backend);
    // First tick consumes the single-delete budget.
    let first = reconciler.tick().await;
    assert_eq!(first.deleted, 1);
    // A second tick in the same rolling minute (pod still present) is rate
    // limited because the budget hasn't recovered.
    clock.set(now() + secs(30));
    let same_window = reconciler.tick().await;
    assert_eq!(same_window.deleted, 0);
    assert_eq!(skip_count(&same_window, SkipReason::RateLimited), 1);
    // Once the window elapses, budget recovers.
    clock.set(now() + secs(90));
    let later = reconciler.tick().await;
    assert_eq!(later.deleted, 1);
}

/// A charge frees its slot at *exactly* one window (inclusive boundary), so a
/// full-budget GC sustains its configured rate instead of stalling an extra
/// tick when the tick interval equals the rolling window — the halved-throughput
/// regression.
#[tokio::test]
async fn rate_limit_budget_frees_at_exactly_one_window() {
    let backend = FakeBackend::new(vec![stuck_snapshot("p1")]);
    let (mut reconciler, clock) = build(test_config(1), backend);
    let first = reconciler.tick().await;
    assert_eq!(first.deleted, 1);
    // Exactly RATE_LIMIT_WINDOW (60s) later: the charge ages out on the boundary
    // and the budget refills the same tick.
    clock.set(now() + secs(60));
    let at_boundary = reconciler.tick().await;
    assert_eq!(at_boundary.deleted, 1);
}

/// An in-flight apiserver call that exceeds the tick's wall-clock budget must
/// be abandoned (not left to block the loop for the kube client's multi-minute
/// timeout), and the remaining candidates deferred to the next tick. Runs on a
/// paused clock so the fake's `op_delay` sleep and the deadline are virtual.
#[tokio::test(start_paused = true)]
async fn tick_times_out_slow_apiserver_call_and_defers_rest() {
    let mut config = test_config(60);
    config.tick_budget = Duration::from_secs(30);
    let mut backend = FakeBackend::new(vec![stuck_snapshot("p1"), stuck_snapshot("p2")]);
    // Each verify GET takes longer than the 30s budget, so the first candidate
    // trips the deadline and the second defers.
    backend.op_delay = Some(Duration::from_secs(120));
    let (mut reconciler, _clock) = build(config, backend);

    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 0);
    assert_eq!(skip_count(&report, SkipReason::TimedOut), 1);
    assert_eq!(report.deferred, 1);
}

/// A deadline that defers the tail after a cheap-skip prefix must advance the
/// fairness cursor past *everything handled* this tick, not just the budget it
/// committed. Otherwise a stable reflector order replays the same prefix — here
/// a restore skip (commits no budget) plus one slow candidate that trips the
/// deadline — every tick, and the deferred actionable pod behind them is never
/// reached. Regression for cursor starvation.
#[tokio::test(start_paused = true)]
async fn deadline_deferral_advances_cursor_past_processed_prefix() {
    let mut config = test_config(60);
    config.tick_budget = Duration::from_secs(30);
    let mut restore = stuck_snapshot("restore");
    restore.is_restore = true;
    let mut backend = FakeBackend::new(vec![
        restore,
        stuck_snapshot("slow"),
        stuck_snapshot("target"),
    ])
    .with_slow_pod("slow");
    backend.op_delay = Some(Duration::from_secs(120));
    let (mut reconciler, _clock) = build(config, backend);

    // Tick 1: `restore` is skipped (commits no budget), `slow` trips the 30s
    // deadline, `target` defers. Nothing deleted.
    let first = reconciler.tick().await;
    assert_eq!(first.deleted, 0);
    assert_eq!(first.deferred, 1);
    assert_eq!(reconciler.backend.delete_count(), 0);

    // Tick 2: the cursor advanced past the whole processed prefix (restore +
    // slow), so `target` is now first and is deleted — not starved behind the
    // repeatedly-timing-out slow pod.
    let second = reconciler.tick().await;
    assert_eq!(second.deleted, 1);
    assert_eq!(
        reconciler.backend.deleted_names(),
        vec!["target".to_string()]
    );
}

/// A budget-sized prefix of candidates that keep failing must not permanently
/// starve the genuinely-deletable pods behind them: the scan start rotates past
/// the committed candidates each tick.
#[tokio::test]
async fn tick_rotates_past_failing_prefix() {
    // A non-candidate (Running) pod is interleaved at the front to prove the
    // cursor indexes the *candidate* list, not the raw pod list.
    let mut not_a_candidate = stuck_snapshot("skip");
    not_a_candidate.any_container_running = true;
    let backend = FakeBackend::new(vec![
        not_a_candidate,
        stuck_snapshot("p1"),
        stuck_snapshot("p2"),
        stuck_snapshot("p3"),
        stuck_snapshot("p4"),
    ])
    .with_delete_override("p1", DeleteOverride::Error)
    .with_delete_override("p2", DeleteOverride::Error);
    let (mut reconciler, clock) = build(test_config(2), backend);

    // Tick 1: budget of 2 is spent on the p1/p2 prefix, both of which error.
    let first = reconciler.tick().await;
    assert_eq!(first.delete_failed, 2);
    assert_eq!(skip_count(&first, SkipReason::RateLimited), 2);

    // Let the window replenish, then tick again: the cursor has rotated past
    // p1/p2, so p3/p4 are the ones acted on this time.
    clock.set(now() + secs(90));
    let second = reconciler.tick().await;
    assert_eq!(second.deleted, 2);
    let deleted = reconciler.backend.deleted_names();
    assert!(deleted.contains(&"p3".to_string()));
    assert!(deleted.contains(&"p4".to_string()));
}

/// A skip-only prefix (restore pods, which commit no budget) ahead of a
/// repeatedly-failing committed candidate must not starve the actionable tail:
/// the cursor advances to just past the last commitment (the failing pod), not
/// by the commitment *count*, so the tail is reached the next tick.
#[tokio::test]
async fn cursor_advances_past_skip_prefix_to_reach_actionable_tail() {
    let mut r0 = stuck_snapshot("r0");
    r0.is_restore = true;
    let mut r1 = stuck_snapshot("r1");
    r1.is_restore = true;
    let mut r2 = stuck_snapshot("r2");
    r2.is_restore = true;
    let backend = FakeBackend::new(vec![
        r0,
        r1,
        r2,
        stuck_snapshot("fail"),
        stuck_snapshot("target"),
    ])
    .with_delete_override("fail", DeleteOverride::Error);
    let (mut reconciler, clock) = build(test_config(1), backend);

    // Tick 1: three restore skips (no budget), `fail` spends the single slot and
    // errors, `target` is rate-limited. Under a count-based cursor the advance
    // would be 1 (into the skip prefix) and `target` would stay starved.
    let t1 = reconciler.tick().await;
    assert_eq!(t1.deleted, 0);
    assert_eq!(skip_count(&t1, SkipReason::RestorePod), 3);
    assert_eq!(skip_count(&t1, SkipReason::RateLimited), 1);

    // Tick 2 after the rate window replenishes: the cursor advanced past the
    // skip prefix + the failing commit, so `target` is reached and deleted
    // rather than starved behind them.
    clock.set(now() + secs(90));
    let t2 = reconciler.tick().await;
    assert_eq!(t2.deleted, 1);
    assert!(
        reconciler
            .backend
            .deleted_names()
            .contains(&"target".to_string())
    );
}

// ---- tick: verify-before-destroy ----------------------------------------

#[tokio::test]
async fn tick_skips_when_recheck_shows_progress() {
    let pod = stuck_snapshot("p");
    let mut recovered = pod.clone();
    recovered.any_container_running = true;
    let backend =
        FakeBackend::new(vec![pod]).with_fresh_override("p", FreshOverride::Snapshot(recovered));
    let (mut reconciler, _clock) = build(test_config(60), backend);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 0);
    assert_eq!(skip_count(&report, SkipReason::ProgressingOnRecheck), 1);
    // Fresh read happened, but no delete.
    assert_eq!(reconciler.backend.fresh_read_count(), 1);
    assert_eq!(reconciler.backend.delete_count(), 0);
}

#[tokio::test]
async fn tick_skips_when_recheck_shows_gone() {
    let backend =
        FakeBackend::new(vec![stuck_snapshot("p")]).with_fresh_override("p", FreshOverride::Gone);
    let (mut reconciler, _clock) = build(test_config(60), backend);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 0);
    assert_eq!(skip_count(&report, SkipReason::AlreadyGone), 1);
    assert_eq!(reconciler.backend.delete_count(), 0);
}

#[tokio::test]
async fn tick_skips_when_recheck_uid_changed() {
    let pod = stuck_snapshot("p");
    let mut regenerated = pod.clone();
    regenerated.pod_uid = Some("different-uid".parse().unwrap());
    let backend =
        FakeBackend::new(vec![pod]).with_fresh_override("p", FreshOverride::Snapshot(regenerated));
    let (mut reconciler, _clock) = build(test_config(60), backend);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 0);
    assert_eq!(skip_count(&report, SkipReason::IdentityChanged), 1);
    assert_eq!(reconciler.backend.delete_count(), 0);
}

/// If the cached snapshot missed the restore annotation (stale) but the
/// authoritative re-read reveals a restore pod, it must not be deleted — the
/// destructive-eligibility guard is re-applied to the fresh object.
#[tokio::test]
async fn tick_skips_when_recheck_shows_restore_pod() {
    let pod = stuck_snapshot("p");
    let mut fresh = pod.clone();
    fresh.is_restore = true;
    let backend =
        FakeBackend::new(vec![pod]).with_fresh_override("p", FreshOverride::Snapshot(fresh));
    let (mut reconciler, _clock) = build(test_config(60), backend);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 0);
    assert_eq!(skip_count(&report, SkipReason::RestorePod), 1);
    assert_eq!(reconciler.backend.delete_count(), 0);
}

/// A warm-pool slot pod (`pool-role=warm`) stuck in `ContainerCreating` is
/// skipped, not deleted, before any apiserver read — the warm-pool reconciler
/// owns it and no `sandboxes` row exists for the convergence reconcilers.
#[tokio::test]
async fn warm_pool_pod_is_skipped_before_any_read() {
    let mut pod = stuck_snapshot("p");
    pod.is_warm_pool = true;
    let backend = FakeBackend::new(vec![pod]);
    let (mut reconciler, _clock) = build(test_config(60), backend);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 0);
    assert_eq!(skip_count(&report, SkipReason::WarmPool), 1);
    assert_eq!(reconciler.backend.delete_count(), 0);
    assert_eq!(reconciler.backend.fresh_read_count(), 0);
}

/// Warm-pool exclusion is re-applied to the authoritative object: a pod whose
/// pool-role label wasn't in the stale cache but appears on the fresh read is
/// still skipped, not deleted.
#[tokio::test]
async fn tick_skips_when_recheck_shows_warm_pool() {
    let pod = stuck_snapshot("p");
    let mut fresh = pod.clone();
    fresh.is_warm_pool = true;
    let backend =
        FakeBackend::new(vec![pod]).with_fresh_override("p", FreshOverride::Snapshot(fresh));
    let (mut reconciler, _clock) = build(test_config(60), backend);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 0);
    assert_eq!(skip_count(&report, SkipReason::WarmPool), 1);
    assert_eq!(reconciler.backend.delete_count(), 0);
}

/// The node-state guard is re-applied after the verify GET: if the node cordons
/// while the authoritative read is in flight, the pod is deferred rather than
/// deleted, so the GC never races node-drain / remediation.
#[tokio::test]
async fn node_recheck_after_get_defers_when_node_cordons() {
    // Node reads Eligible until the verify GET happens, then Cordoned on the
    // post-GET recheck (the fake flips once a fresh read has occurred).
    let backend = FakeBackend::new(vec![stuck_snapshot("p")])
        .with_node_eligibility_recheck(NodeEligibility::Cordoned);
    let (mut reconciler, _clock) = build(test_config(60), backend);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 0);
    assert_eq!(skip_count(&report, SkipReason::NodeCordoned), 1);
    assert_eq!(reconciler.backend.delete_count(), 0);
    // The pre-GET check passed, so the GET ran; the recheck caught the cordon.
    assert_eq!(reconciler.backend.fresh_read_count(), 1);
}

/// A candidate whose node goes ineligible *during* the verify GET is deferred
/// AND its observation clock is reset, so if the node recovers before the next
/// scan the pod re-ages from scratch — it isn't deleted on the age it held
/// before the mid-tick cordon, matching the scan-phase pause behavior.
#[tokio::test]
async fn node_reject_during_get_resets_observation_clock() {
    let backend = FakeBackend::new(vec![stuck_snapshot("p")])
        .with_node_eligibility_recheck(NodeEligibility::Cordoned);
    let (mut reconciler, clock) = build(test_config(60), backend);

    // Tick 1: eligible at scan (primed past threshold), but the post-GET recheck
    // sees the node cordoned → deferred, and the clock is reset.
    let t1 = reconciler.tick().await;
    assert_eq!(t1.deleted, 0);
    assert_eq!(skip_count(&t1, SkipReason::NodeCordoned), 1);

    // Node recovers before the next scan.
    reconciler.backend.node_eligibility_recheck = None;
    let t2 = reconciler.tick().await;
    // Clock was reset in tick 1, so the pod is too young — not deleted on its
    // pre-cordon age.
    assert_eq!(t2.deleted, 0);
    assert_eq!(t2.too_young, 1);
    assert_eq!(t2.stuck_detected, 0);
    assert_eq!(reconciler.backend.delete_count(), 0);

    // A full threshold after recovery, it's finally actioned.
    clock.set(now() + secs(11 * 60));
    let t3 = reconciler.tick().await;
    assert_eq!(t3.deleted, 1);
}

/// Dry-run runs the same authoritative path as live and diverges only at the
/// DELETE: a candidate the fresh read shows progressing is NOT counted as a
/// would-delete, so dry-run volume faithfully previews live deletions.
#[tokio::test]
async fn tick_defers_on_recheck_read_error() {
    let backend =
        FakeBackend::new(vec![stuck_snapshot("p")]).with_fresh_override("p", FreshOverride::Error);
    let (mut reconciler, _clock) = build(test_config(60), backend);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 0);
    assert_eq!(skip_count(&report, SkipReason::ReadError), 1);
    assert_eq!(reconciler.backend.delete_count(), 0);
}

// ---- tick: delete outcomes ----------------------------------------------

#[tokio::test]
async fn tick_delete_not_found_is_benign_skip() {
    let backend = FakeBackend::new(vec![stuck_snapshot("p")])
        .with_delete_override("p", DeleteOverride::NotFound);
    let (mut reconciler, _clock) = build(test_config(60), backend);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 0);
    assert_eq!(report.delete_failed, 0);
    assert_eq!(skip_count(&report, SkipReason::AlreadyGone), 1);
}

#[tokio::test]
async fn tick_delete_stale_identity_is_benign_skip() {
    let backend = FakeBackend::new(vec![stuck_snapshot("p")])
        .with_delete_override("p", DeleteOverride::StaleIdentity);
    let (mut reconciler, _clock) = build(test_config(60), backend);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 0);
    assert_eq!(report.delete_failed, 0);
    assert_eq!(skip_count(&report, SkipReason::IdentityChanged), 1);
}

#[tokio::test]
async fn tick_delete_error_counts_failure() {
    let backend = FakeBackend::new(vec![stuck_snapshot("p")])
        .with_delete_override("p", DeleteOverride::Error);
    let (mut reconciler, _clock) = build(test_config(60), backend);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 0);
    assert_eq!(report.delete_failed, 1);
}

// ---- tick: missing-metadata guards --------------------------------------

/// User-restore pods (carrying the suspend-restore annotation) are excluded:
/// their `restoring` row has no orphaned-sandbox convergence path and they hold
/// durable snapshot state, so a restore-aware path owns them, not this GC.
#[tokio::test]
async fn tick_skips_restore_pods() {
    let mut pod = stuck_snapshot("p");
    pod.is_restore = true;
    let backend = FakeBackend::new(vec![pod]);
    let (mut reconciler, _clock) = build(test_config(60), backend);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 0);
    assert_eq!(skip_count(&report, SkipReason::RestorePod), 1);
    assert_eq!(reconciler.backend.delete_count(), 0);
    assert_eq!(reconciler.backend.fresh_read_count(), 0);
}

/// `from_pod` flags restore pods by any restore annotation — remote
/// (storage-URI), node-local (source-node), or checkpoint — while a fresh
/// create (no such annotation) is not flagged.
#[test]
fn from_pod_flags_restore_pods() {
    let restore_pod = |annotations: serde_json::Value| -> PodSnapshot {
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {
                "name": "space-sbx-mno-g1",
                "uid": "pod-uid-5",
                "annotations": annotations
            },
            "spec": { "nodeName": "node-a" },
            "status": { "phase": "Pending" }
        }))
        .unwrap();
        PodSnapshot::from_pod(&SandboxPodView::from(pod.clone()))
    };

    assert!(
        restore_pod(json!({ "space.pplx.net/suspend-restore-storage-uri": "s3://b/s" })).is_restore
    );
    assert!(
        restore_pod(json!({ "space.pplx.net/suspend-restore-source-node": "node-b" })).is_restore
    );
    assert!(restore_pod(json!({ "space.pplx.net/checkpoint": "ckpt-1" })).is_restore);
    // A fresh create carries the id/template annotations but no restore marker.
    assert!(!restore_pod(json!({ "space.pplx.net/template": "seed" })).is_restore);
}

#[tokio::test]
async fn tick_skips_missing_sandbox_id() {
    let mut pod = stuck_snapshot("p");
    pod.sandbox_id = None;
    let backend = FakeBackend::new(vec![pod]);
    let (mut reconciler, _clock) = build(test_config(60), backend);
    let report = reconciler.tick().await;
    assert_eq!(skip_count(&report, SkipReason::MissingSandboxId), 1);
    assert_eq!(reconciler.backend.delete_count(), 0);
}

#[tokio::test]
async fn tick_skips_missing_node() {
    let mut pod = stuck_snapshot("p");
    pod.node_name = None;
    let backend = FakeBackend::new(vec![pod]);
    let (mut reconciler, _clock) = build(test_config(60), backend);
    let report = reconciler.tick().await;
    assert_eq!(skip_count(&report, SkipReason::MissingNode), 1);
    assert_eq!(reconciler.backend.delete_count(), 0);
}

/// A stuck pod with no UID can't be anchored to the observation clock, so its
/// age is unknowable — it's counted `too_young` and never deleted rather than
/// acted on blind.
#[tokio::test]
async fn tick_no_uid_pod_is_too_young() {
    let mut pod = stuck_snapshot("p");
    pod.pod_uid = None;
    let backend = FakeBackend::new(vec![pod]);
    let (mut reconciler, _clock) = build(test_config(60), backend);
    let report = reconciler.tick().await;
    assert_eq!(report.too_young, 1);
    assert_eq!(report.stuck_detected, 0);
    assert_eq!(report.deleted, 0);
    assert_eq!(reconciler.backend.delete_count(), 0);
}

// ---- PodSnapshot::from_pod ----------------------------------------------

#[test]
fn from_pod_extracts_stuck_container_creating() {
    let sandbox_id = SandboxId::new();
    let pod: Pod = serde_json::from_value(json!({
        "metadata": {
            "name": "space-sbx-abc-g1",
            "uid": "pod-uid-1",
            "resourceVersion": "12345",
            "labels": { "space.pplx.net/sandbox-id": sandbox_id.to_string() }
        },
        "spec": { "nodeName": "node-a" },
        "status": {
            "phase": "Pending",
            "startTime": "2023-11-14T00:00:00Z",
            "containerStatuses": [{
                "name": "sandbox",
                "ready": false,
                "restartCount": 0,
                "image": "img",
                "imageID": "",
                "state": { "waiting": { "reason": "ContainerCreating" } }
            }]
        }
    }))
    .unwrap();

    let snapshot = PodSnapshot::from_pod(&SandboxPodView::from(pod.clone()));
    assert_eq!(snapshot.pod_name, "space-sbx-abc-g1");
    assert_eq!(
        snapshot.pod_uid.as_ref().map(PodUid::as_str),
        Some("pod-uid-1")
    );
    assert_eq!(snapshot.resource_version.as_deref(), Some("12345"));
    assert_eq!(snapshot.sandbox_id, Some(sandbox_id));
    assert_eq!(snapshot.node_name.as_deref(), Some("node-a"));
    assert!(!snapshot.terminating);
    assert!(snapshot.phase_pending);
    assert!(snapshot.has_container_creating);
    assert!(!snapshot.any_container_running);
    assert!(is_stuck_shaped(&snapshot));
}

/// A pod wedged creating an *init* container is stuck too — the
/// ContainerCreating signal must be read from init container statuses, not just
/// app containers.
#[test]
fn from_pod_detects_container_creating_on_init_container() {
    let pod: Pod = serde_json::from_value(json!({
        "metadata": {
            "name": "space-sbx-pqr-g1",
            "uid": "pod-uid-6",
            "labels": { "space.pplx.net/sandbox-id": Uuid::now_v7().to_string() }
        },
        "spec": { "nodeName": "node-a" },
        "status": {
            "phase": "Pending",
            "startTime": "2023-11-14T00:00:00Z",
            "initContainerStatuses": [{
                "name": "setup",
                "ready": false,
                "restartCount": 0,
                "image": "img",
                "imageID": "",
                "state": { "waiting": { "reason": "ContainerCreating" } }
            }]
        }
    }))
    .unwrap();

    let snapshot = PodSnapshot::from_pod(&SandboxPodView::from(pod.clone()));
    assert!(snapshot.has_container_creating);
    assert!(!snapshot.any_container_running);
    assert!(is_stuck_shaped(&snapshot));
}

/// A running init container is the progress guard — the pod is still
/// initializing, not wedged.
#[test]
fn from_pod_running_init_container_is_progressing() {
    let pod: Pod = serde_json::from_value(json!({
        "metadata": {
            "name": "space-sbx-stu-g1",
            "uid": "pod-uid-7",
            "labels": { "space.pplx.net/sandbox-id": Uuid::now_v7().to_string() }
        },
        "spec": { "nodeName": "node-a" },
        "status": {
            "phase": "Pending",
            "initContainerStatuses": [{
                "name": "setup",
                "ready": false,
                "restartCount": 0,
                "image": "img",
                "imageID": "",
                "state": { "running": { "startedAt": "2023-11-14T00:00:00Z" } }
            }]
        }
    }))
    .unwrap();

    let snapshot = PodSnapshot::from_pod(&SandboxPodView::from(pod.clone()));
    assert!(snapshot.any_container_running);
    assert!(!is_stuck_shaped(&snapshot));
}

#[test]
fn from_pod_marks_running_container_as_progressing() {
    let pod: Pod = serde_json::from_value(json!({
        "metadata": {
            "name": "space-sbx-def-g1",
            "uid": "pod-uid-2",
            "labels": { "space.pplx.net/sandbox-id": Uuid::now_v7().to_string() }
        },
        "spec": { "nodeName": "node-a" },
        "status": {
            "phase": "Pending",
            "containerStatuses": [{
                "name": "sandbox",
                "ready": true,
                "restartCount": 0,
                "image": "img",
                "imageID": "",
                "state": { "running": { "startedAt": "2023-11-14T00:00:00Z" } }
            }]
        }
    }))
    .unwrap();

    let snapshot = PodSnapshot::from_pod(&SandboxPodView::from(pod.clone()));
    assert!(snapshot.any_container_running);
    assert!(!snapshot.has_container_creating);
    assert!(!is_stuck_shaped(&snapshot));
}

/// The sandbox id must be recovered when a pod carries it only as an
/// annotation (not the same-keyed label) — otherwise a real stuck pod would be
/// classified `missing_sandbox_id` and silently skipped.
#[test]
fn from_pod_reads_sandbox_id_from_annotation_when_label_absent() {
    let sandbox_id = SandboxId::new();
    let pod: Pod = serde_json::from_value(json!({
        "metadata": {
            "name": "space-sbx-jkl-g1",
            "uid": "pod-uid-4",
            "annotations": { "space.pplx.net/sandbox-id": sandbox_id.to_string() }
        },
        "spec": { "nodeName": "node-a" },
        "status": { "phase": "Pending" }
    }))
    .unwrap();

    let snapshot = PodSnapshot::from_pod(&SandboxPodView::from(pod.clone()));
    assert_eq!(snapshot.sandbox_id, Some(sandbox_id));
}

#[test]
fn from_pod_detects_terminating_and_missing_labels() {
    let pod: Pod = serde_json::from_value(json!({
        "metadata": {
            "name": "space-sbx-ghi-g1",
            "uid": "pod-uid-3",
            "deletionTimestamp": "2023-11-14T00:00:00Z"
        },
        "spec": {},
        "status": { "phase": "Pending" }
    }))
    .unwrap();

    let snapshot = PodSnapshot::from_pod(&SandboxPodView::from(pod.clone()));
    assert!(snapshot.terminating);
    assert_eq!(snapshot.sandbox_id, None);
    assert_eq!(snapshot.node_name, None);
    assert!(!is_stuck_shaped(&snapshot));
}
