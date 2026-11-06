//! Unit tests for the retired-execution pod GC: the classification matrix,
//! config parsing, and the reconciler's observe → verify → delete flow
//! against in-memory fakes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pplx_space_common_core::types::{PodUid, SandboxId};

use super::backend::{BackendError, DeleteOutcome, ExecutionStore, PodBackend};
use super::classify::{
    Disposition, ExecutionRow, PodPhase, PodSnapshot, ReapReason, SkipReason, classify,
};
use super::config::ReconcilerConfig;
use super::reconciler::Reconciler;

const GEN: i64 = 1;

fn uid(raw: &str) -> PodUid {
    raw.parse().expect("pod uid")
}

fn pod(name: &str, uid_raw: &str, sandbox_id: SandboxId, phase: PodPhase) -> PodSnapshot {
    PodSnapshot {
        pod_name: name.to_string(),
        pod_uid: Some(uid(uid_raw)),
        resource_version: Some("100".to_string()),
        sandbox_id: Some(sandbox_id),
        generation: Some(GEN),
        node_name: Some("node-1".to_string()),
        terminating: false,
        is_restore: false,
        has_pod_ip: !matches!(phase, PodPhase::Pending),
        sandbox_not_ready: matches!(phase, PodPhase::Pending),
        phase,
        all_containers_terminated: matches!(phase, PodPhase::Succeeded | PodPhase::Failed),
    }
}

fn row(sandbox_id: SandboxId, active: bool, state: &str) -> ExecutionRow {
    ExecutionRow {
        sandbox_id,
        generation: GEN,
        active,
        state: state.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Classification
// ---------------------------------------------------------------------------

#[test]
fn reaps_a_terminal_pod_of_a_suspended_execution() {
    let sid = SandboxId::new();
    let pod = pod("p", "u", sid, PodPhase::Succeeded);
    assert_eq!(
        classify(&pod, Some(&row(sid, false, "suspended"))),
        Disposition::Reap(ReapReason::TerminalRetired)
    );
    let failed = PodSnapshot {
        phase: PodPhase::Failed,
        ..pod
    };
    assert_eq!(
        classify(&failed, Some(&row(sid, false, "deleted"))),
        Disposition::Reap(ReapReason::TerminalRetired)
    );
    // A suspended sandbox's terminal restore pod is reaped too.
    let restore = PodSnapshot {
        is_restore: true,
        ..failed
    };
    assert_eq!(
        classify(&restore, Some(&row(sid, false, "suspended"))),
        Disposition::Reap(ReapReason::TerminalRetired)
    );
}

#[test]
fn reaps_a_pending_fresh_create_pod_retired_as_replaced() {
    let sid = SandboxId::new();
    let pod = pod("p", "u", sid, PodPhase::Pending);
    assert_eq!(
        classify(&pod, Some(&row(sid, false, "replaced"))),
        Disposition::Reap(ReapReason::ReplacedNeverStarted)
    );
}

#[test]
fn never_reaps_a_pod_whose_execution_is_active() {
    let sid = SandboxId::new();
    for phase in [
        PodPhase::Pending,
        PodPhase::Running,
        PodPhase::Succeeded,
        PodPhase::Failed,
        PodPhase::Other,
    ] {
        let pod = pod("p", "u", sid, phase);
        assert_eq!(
            classify(&pod, Some(&row(sid, true, "creating"))),
            Disposition::Skip(SkipReason::ActiveExecution),
            "{phase:?}"
        );
    }
}

#[test]
fn leaves_retired_pods_that_may_still_hold_a_vm() {
    let sid = SandboxId::new();
    // Pending but retired as something other than `replaced`.
    let pending = pod("p", "u", sid, PodPhase::Pending);
    assert_eq!(
        classify(&pending, Some(&row(sid, false, "suspended"))),
        Disposition::Skip(SkipReason::RetiredNotProvablyIdle)
    );
    // Terminal phase but a container still running.
    let live_container = PodSnapshot {
        all_containers_terminated: false,
        ..pod("p", "u", sid, PodPhase::Succeeded)
    };
    assert_eq!(
        classify(&live_container, Some(&row(sid, false, "suspended"))),
        Disposition::Skip(SkipReason::RetiredNotProvablyIdle)
    );
    // Restore pods in the replaced/pending shape belong to
    // orphaned_restore_pod_gc.
    let restore = PodSnapshot {
        is_restore: true,
        ..pod("p", "u", sid, PodPhase::Pending)
    };
    assert_eq!(
        classify(&restore, Some(&row(sid, false, "replaced"))),
        Disposition::Skip(SkipReason::RetiredNotProvablyIdle)
    );
    // Pending, but its sandbox came up (it has an IP), so a VM may exist.
    let sandbox_up = PodSnapshot {
        has_pod_ip: true,
        ..pod("p", "u", sid, PodPhase::Pending)
    };
    assert_eq!(
        classify(&sandbox_up, Some(&row(sid, false, "replaced"))),
        Disposition::Skip(SkipReason::RetiredNotProvablyIdle)
    );
    // No IP, but kubelet hasn't reported the sandbox as not ready.
    let sandbox_unknown = PodSnapshot {
        sandbox_not_ready: false,
        ..pod("p", "u", sid, PodPhase::Pending)
    };
    assert_eq!(
        classify(&sandbox_unknown, Some(&row(sid, false, "replaced"))),
        Disposition::Skip(SkipReason::RetiredNotProvablyIdle)
    );
    // Unknown phase.
    let unknown = pod("p", "u", sid, PodPhase::Other);
    assert_eq!(
        classify(&unknown, Some(&row(sid, false, "replaced"))),
        Disposition::Skip(SkipReason::RetiredNotProvablyIdle)
    );
}

#[test]
fn skips_pods_without_a_matching_row_or_identity() {
    let sid = SandboxId::new();
    let base = pod("p", "u", sid, PodPhase::Succeeded);
    assert_eq!(
        classify(&base, None),
        Disposition::Skip(SkipReason::NoExecutionRow)
    );
    assert_eq!(
        classify(&base, Some(&row(SandboxId::new(), false, "suspended"))),
        Disposition::Skip(SkipReason::IdentityMismatch)
    );
    let other_generation = ExecutionRow {
        generation: GEN + 1,
        ..row(sid, false, "suspended")
    };
    assert_eq!(
        classify(&base, Some(&other_generation)),
        Disposition::Skip(SkipReason::IdentityMismatch)
    );
    let no_uid = PodSnapshot {
        pod_uid: None,
        ..base.clone()
    };
    assert_eq!(
        classify(&no_uid, Some(&row(sid, false, "suspended"))),
        Disposition::Skip(SkipReason::MissingIdentity)
    );
    let terminating = PodSnapshot {
        terminating: true,
        ..base
    };
    assert_eq!(
        classify(&terminating, Some(&row(sid, false, "suspended"))),
        Disposition::Skip(SkipReason::Terminating)
    );
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

fn config_from(vars: &[(&str, &str)]) -> ReconcilerConfig {
    let mut env = pplx_space_test_env::TestEnv::default();
    for (name, value) in vars {
        env.set(name, value);
    }
    ReconcilerConfig::from_environment(&env)
}

#[test]
fn config_defaults_to_disabled_dry_run() {
    let config = config_from(&[]);
    assert!(!config.enabled);
    assert!(config.dry_run);
    assert_eq!(config, ReconcilerConfig::default());
}

#[test]
fn config_reads_overrides_and_clamps() {
    let config = config_from(&[
        ("SPACE_RETIRED_EXECUTION_POD_GC_ENABLED", "true"),
        ("SPACE_RETIRED_EXECUTION_POD_GC_DRY_RUN", "false"),
        ("SPACE_RETIRED_EXECUTION_POD_GC_THRESHOLD_SECS", "120"),
        (
            "SPACE_RETIRED_EXECUTION_POD_GC_MAX_DELETES_PER_MINUTE",
            "999999",
        ),
        ("SPACE_RETIRED_EXECUTION_POD_GC_BATCH_SIZE", "0"),
    ]);
    assert!(config.enabled);
    assert!(!config.dry_run);
    assert_eq!(config.threshold.get(), Duration::from_secs(120));
    assert_eq!(
        config.max_deletes_per_minute,
        super::config::MAX_DELETES_PER_MINUTE_CEILING
    );
    assert_eq!(config.batch_size, super::config::DEFAULT_BATCH_SIZE);
}

// ---------------------------------------------------------------------------
// Reconciler
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeleteCall {
    pod_name: String,
    pod_uid: PodUid,
    resource_version: Option<String>,
}

#[derive(Default)]
struct FakePods {
    not_ready: bool,
    listed: Mutex<Vec<PodSnapshot>>,
    /// Overrides for the fresh GET; absent means "same as listed".
    fresh: Mutex<HashMap<String, Option<PodSnapshot>>>,
    deletes: Mutex<Vec<DeleteCall>>,
}

impl PodBackend for Arc<FakePods> {
    fn caches_ready(&self) -> bool {
        !self.not_ready
    }

    async fn list_pods(&self) -> Vec<PodSnapshot> {
        self.listed.lock().expect("listed").clone()
    }

    async fn read_pod_fresh(&self, pod_name: &str) -> Result<Option<PodSnapshot>, BackendError> {
        if let Some(read) = self.fresh.lock().expect("fresh").get(pod_name) {
            return Ok(read.clone());
        }
        Ok(self
            .listed
            .lock()
            .expect("listed")
            .iter()
            .find(|pod| pod.pod_name == pod_name)
            .cloned())
    }

    async fn delete_pod(
        &self,
        pod_name: &str,
        pod_uid: &PodUid,
        resource_version: Option<&str>,
    ) -> Result<DeleteOutcome, BackendError> {
        self.deletes.lock().expect("deletes").push(DeleteCall {
            pod_name: pod_name.to_string(),
            pod_uid: pod_uid.clone(),
            resource_version: resource_version.map(str::to_string),
        });
        Ok(DeleteOutcome::Deleted)
    }
}

#[derive(Default)]
struct FakeStore {
    rows: Mutex<HashMap<String, ExecutionRow>>,
    fail: Mutex<bool>,
}

impl ExecutionStore for Arc<FakeStore> {
    async fn rows_for_pods(
        &self,
        pod_names: &[String],
    ) -> Result<HashMap<String, ExecutionRow>, sqlx::Error> {
        if *self.fail.lock().expect("fail") {
            return Err(sqlx::Error::PoolClosed);
        }
        let rows = self.rows.lock().expect("rows");
        Ok(pod_names
            .iter()
            .filter_map(|name| rows.get(name).map(|row| (name.clone(), row.clone())))
            .collect())
    }
}

struct Harness {
    pods: Arc<FakePods>,
    store: Arc<FakeStore>,
    now: Arc<Mutex<Instant>>,
    reconciler: Reconciler<Arc<FakePods>, Arc<FakeStore>>,
}

impl Harness {
    fn new(dry_run: bool, max_deletes_per_minute: u32) -> Self {
        let pods = Arc::new(FakePods::default());
        let store = Arc::new(FakeStore::default());
        let now = Arc::new(Mutex::new(Instant::now()));
        let clock = Arc::clone(&now);
        let config = ReconcilerConfig {
            enabled: true,
            dry_run,
            max_deletes_per_minute,
            ..ReconcilerConfig::default()
        };
        let reconciler = Reconciler::new(config, Arc::clone(&pods), Arc::clone(&store))
            .with_clock(move || *clock.lock().expect("clock"));
        Self {
            pods,
            store,
            now,
            reconciler,
        }
    }

    fn add(&self, pod: PodSnapshot, row: Option<ExecutionRow>) {
        if let Some(row) = row {
            self.store
                .rows
                .lock()
                .expect("rows")
                .insert(pod.pod_name.clone(), row);
        }
        self.pods.listed.lock().expect("listed").push(pod);
    }

    fn advance(&self, by: Duration) {
        *self.now.lock().expect("clock") += by;
    }

    fn deletes(&self) -> Vec<DeleteCall> {
        self.pods.deletes.lock().expect("deletes").clone()
    }
}

const PAST_THRESHOLD: Duration = Duration::from_secs(301);

#[tokio::test]
async fn deletes_only_after_the_threshold_with_uid_and_rv_preconditions() {
    let mut h = Harness::new(false, 60);
    let sid = SandboxId::new();
    h.add(
        pod("space-sbx-a-g1", "uid-a", sid, PodPhase::Succeeded),
        Some(row(sid, false, "suspended")),
    );

    let first = h.reconciler.tick().await;
    assert_eq!(first.too_young, 1);
    assert!(h.deletes().is_empty(), "nothing deleted on first sight");

    h.advance(PAST_THRESHOLD);
    let second = h.reconciler.tick().await;
    assert_eq!(second.reaped, 1);
    assert_eq!(
        h.deletes(),
        vec![DeleteCall {
            pod_name: "space-sbx-a-g1".to_string(),
            pod_uid: uid("uid-a"),
            resource_version: Some("100".to_string()),
        }]
    );
}

#[tokio::test]
async fn reaps_the_stuck_container_creating_shape() {
    let mut h = Harness::new(false, 60);
    let sid = SandboxId::new();
    h.add(
        pod("space-sbx-b-g1", "uid-b", sid, PodPhase::Pending),
        Some(row(sid, false, "replaced")),
    );
    h.reconciler.tick().await;
    h.advance(PAST_THRESHOLD);
    let report = h.reconciler.tick().await;
    assert_eq!(report.reaped, 1);
    assert_eq!(report.reapable.get("replaced_never_started"), Some(&1));
}

#[tokio::test]
async fn never_deletes_active_or_unknown_pods() {
    let mut h = Harness::new(false, 60);
    let active = SandboxId::new();
    h.add(
        pod(
            "space-sbx-active-g1",
            "uid-active",
            active,
            PodPhase::Pending,
        ),
        Some(row(active, true, "creating")),
    );
    // Unclaimed warm-pool pod: no execution row names it.
    h.add(
        pod("warm-1", "uid-warm", SandboxId::new(), PodPhase::Pending),
        None,
    );
    h.reconciler.tick().await;
    h.advance(PAST_THRESHOLD);
    let report = h.reconciler.tick().await;
    assert_eq!(report.reaped, 0);
    assert!(h.deletes().is_empty());
    assert_eq!(report.skipped.get("active_execution"), Some(&1));
    assert_eq!(report.skipped.get("no_execution_row"), Some(&1));
}

#[tokio::test]
async fn dry_run_verifies_but_never_deletes_and_reports_once() {
    let mut h = Harness::new(true, 60);
    let sid = SandboxId::new();
    h.add(
        pod("space-sbx-c-g1", "uid-c", sid, PodPhase::Succeeded),
        Some(row(sid, false, "suspended")),
    );
    h.reconciler.tick().await;
    h.advance(PAST_THRESHOLD);
    let second = h.reconciler.tick().await;
    assert_eq!(second.reaped, 1, "dry-run counts the would-reap");
    h.advance(Duration::from_secs(60));
    let third = h.reconciler.tick().await;
    assert_eq!(third.reaped, 0, "each would-reap is reported once");
    assert!(h.deletes().is_empty());
}

#[tokio::test]
async fn verify_get_that_finds_a_different_pod_does_not_delete() {
    let mut h = Harness::new(false, 60);
    let sid = SandboxId::new();
    h.add(
        pod("space-sbx-d-g1", "uid-d", sid, PodPhase::Pending),
        Some(row(sid, false, "replaced")),
    );
    h.reconciler.tick().await;
    h.advance(PAST_THRESHOLD);
    // Between the scan and the verify, the fresh GET shows a different object
    // under the same name.
    h.pods.fresh.lock().expect("fresh").insert(
        "space-sbx-d-g1".to_string(),
        Some(pod("space-sbx-d-g1", "uid-other", sid, PodPhase::Pending)),
    );
    let report = h.reconciler.tick().await;
    assert_eq!(report.stale_identity, 1);
    assert!(h.deletes().is_empty());
}

#[tokio::test]
async fn a_pod_that_stops_classifying_restarts_its_clock() {
    let mut h = Harness::new(false, 60);
    let sid = SandboxId::new();
    h.add(
        pod("space-sbx-e-g1", "uid-e", sid, PodPhase::Succeeded),
        Some(row(sid, false, "suspended")),
    );
    h.reconciler.tick().await;
    // A failed lookup breaks continuity.
    *h.store.fail.lock().expect("fail") = true;
    h.advance(PAST_THRESHOLD);
    let failed = h.reconciler.tick().await;
    assert!(failed.lookup_failed);
    *h.store.fail.lock().expect("fail") = false;
    let after = h.reconciler.tick().await;
    assert_eq!(
        after.too_young, 1,
        "the clock restarted after the failed scan"
    );

    // So does a tick on which the pod stops classifying as reapable.
    let set_active = |active: bool| {
        h.store
            .rows
            .lock()
            .expect("rows")
            .get_mut("space-sbx-e-g1")
            .expect("row")
            .active = active;
    };
    h.advance(PAST_THRESHOLD);
    set_active(true);
    h.reconciler.tick().await;
    set_active(false);
    h.advance(PAST_THRESHOLD);
    let resumed = h.reconciler.tick().await;
    assert_eq!(
        resumed.too_young, 1,
        "the clock restarted after the pod stopped classifying"
    );
    assert!(h.deletes().is_empty());
}

#[tokio::test]
async fn rate_limit_caps_deletes_per_rolling_minute() {
    let mut h = Harness::new(false, 2);
    for n in 0..5 {
        let sid = SandboxId::new();
        h.add(
            pod(
                &format!("space-sbx-{n}-g1"),
                &format!("uid-{n}"),
                sid,
                PodPhase::Succeeded,
            ),
            Some(row(sid, false, "suspended")),
        );
    }
    h.reconciler.tick().await;
    h.advance(PAST_THRESHOLD);
    let report = h.reconciler.tick().await;
    assert_eq!(report.reaped, 2);
    assert_eq!(report.rate_limited, 3);
    assert_eq!(h.deletes().len(), 2);
}

#[tokio::test]
async fn cold_cache_skips_the_tick() {
    let pods = Arc::new(FakePods {
        not_ready: true,
        ..FakePods::default()
    });
    let store = Arc::new(FakeStore::default());
    let mut reconciler = Reconciler::new(
        ReconcilerConfig {
            enabled: true,
            dry_run: false,
            ..ReconcilerConfig::default()
        },
        pods,
        store,
    );
    let report = reconciler.tick().await;
    assert!(report.cache_not_ready);
}
