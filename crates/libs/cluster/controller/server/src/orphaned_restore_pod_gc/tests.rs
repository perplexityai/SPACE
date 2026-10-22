//! Unit tests for the orphaned-restore-pod GC: the pure supersession matrix and
//! the reconciler's verify-then-delete flow, driven by in-memory fakes so no
//! kube cluster or Postgres is needed.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use pplx_space_common_core::types::{PodUid, PositiveDuration, SandboxId, SandboxLifecycle};
use uuid::Uuid;

use super::*;
use pplx_space_api::controllers::substrate::kubernetes::SandboxPodView;

/// Every `restore_pod` fixture carries this execution generation; store states
/// set `active_generation` relative to it to drive the supersession fence.
const POD_GEN: i64 = 7;

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeleteCall {
    pod_name: String,
    pod_uid: PodUid,
    resource_version: Option<String>,
}

/// How the fresh (authoritative) GET behaves for a given pod name.
#[derive(Clone)]
enum FreshRead {
    /// Return the same object the scan saw (verify passes).
    Same,
    /// The pod vanished between scan and verify.
    Gone,
    /// A different object now holds this name (e.g. a newer-generation pod).
    Replace(Box<RestorePodSnapshot>),
    /// The authoritative GET fails.
    Error,
    /// The authoritative GET never returns (a stalled apiserver).
    Hang,
}

/// A synthetic `kube::Error` for the fake backend's error path. The GC only
/// logs the source, so the exact status doesn't matter.
fn fake_kube_error() -> kube::Error {
    use kube::error::Status;
    kube::Error::Api(
        Status::failure("synthetic kube api error", "Synthetic")
            .with_code(500)
            .boxed(),
    )
}

struct FakePods {
    ready: bool,
    listed: Mutex<Vec<RestorePodSnapshot>>,
    fresh: Mutex<HashMap<String, FreshRead>>,
    delete_outcome: DeleteOutcome,
    deletes: Mutex<Vec<DeleteCall>>,
}

impl FakePods {
    fn new(listed: Vec<RestorePodSnapshot>) -> Self {
        Self {
            ready: true,
            listed: Mutex::new(listed),
            fresh: Mutex::new(HashMap::new()),
            delete_outcome: DeleteOutcome::Deleted,
            deletes: Mutex::new(Vec::new()),
        }
    }

    /// Replace what the pod cache lists (e.g. drop a deleted pod, or show it
    /// terminating), between ticks.
    fn set_listed(&self, listed: Vec<RestorePodSnapshot>) {
        *self.listed.lock().expect("listed lock") = listed;
    }

    fn with_fresh(self, pod_name: &str, read: FreshRead) -> Self {
        self.set_fresh(pod_name, read);
        self
    }

    fn set_fresh(&self, pod_name: &str, read: FreshRead) {
        self.fresh
            .lock()
            .expect("fresh lock")
            .insert(pod_name.to_string(), read);
    }

    fn with_delete_outcome(mut self, outcome: DeleteOutcome) -> Self {
        self.delete_outcome = outcome;
        self
    }

    fn deletes(&self) -> Vec<DeleteCall> {
        self.deletes.lock().expect("deletes lock").clone()
    }
}

impl PodBackend for FakePods {
    fn caches_ready(&self) -> bool {
        self.ready
    }

    async fn list_restore_pods(&self) -> Vec<RestorePodSnapshot> {
        self.listed.lock().expect("listed lock").clone()
    }

    async fn read_pod_fresh(
        &self,
        pod_name: &str,
    ) -> Result<Option<RestorePodSnapshot>, BackendError> {
        let read = self
            .fresh
            .lock()
            .expect("fresh lock")
            .get(pod_name)
            .cloned()
            .unwrap_or(FreshRead::Same);
        match read {
            FreshRead::Same => Ok(self
                .listed
                .lock()
                .expect("listed lock")
                .iter()
                .find(|pod| pod.pod_name == pod_name)
                .cloned()),
            FreshRead::Gone => Ok(None),
            FreshRead::Replace(pod) => Ok(Some(*pod)),
            FreshRead::Error => Err(BackendError::Kube(fake_kube_error())),
            FreshRead::Hang => std::future::pending().await,
        }
    }

    async fn delete_pod(
        &self,
        pod_name: &str,
        pod_uid: &PodUid,
        resource_version: Option<&str>,
    ) -> Result<DeleteOutcome, BackendError> {
        self.deletes.lock().expect("deletes lock").push(DeleteCall {
            pod_name: pod_name.to_string(),
            pod_uid: pod_uid.clone(),
            resource_version: resource_version.map(str::to_string),
        });
        Ok(self.delete_outcome)
    }
}

/// Store fake. `states` answers the first `classify_sandboxes` call for an id
/// (the scan); `recheck`, when set for that id, answers subsequent calls (the
/// verify-before-destroy recheck), so a scan→recheck race can be exercised.
#[derive(Default)]
struct FakeStore {
    states: HashMap<SandboxId, SandboxRestoreState>,
    recheck: HashMap<SandboxId, SandboxRestoreState>,
    calls: Mutex<HashMap<SandboxId, u32>>,
    /// When true, every `classify_sandboxes` call returns an error.
    fail: bool,
    /// When true, the recheck call (call index >= 1 for an id) errors.
    fail_recheck: bool,
    /// When true, the scan call (call index 0 for an id) blocks effectively
    /// forever, so a test can drive the scan's tick-budget timeout.
    block_scan: bool,
    /// When true, the recheck call (call index >= 1 for an id) blocks
    /// effectively forever, so a test can drive the tick-budget timeout on the
    /// recheck specifically (the scan call at index 0 still succeeds).
    block_recheck: bool,
}

impl FakeStore {
    fn with(mut self, id: SandboxId, state: SandboxRestoreState) -> Self {
        self.states.insert(id, state);
        self
    }

    fn with_recheck(mut self, id: SandboxId, state: SandboxRestoreState) -> Self {
        self.recheck.insert(id, state);
        self
    }

    fn failing() -> Self {
        Self {
            fail: true,
            ..Self::default()
        }
    }
}

impl SandboxStore for FakeStore {
    async fn classify_sandboxes(
        &self,
        ids: &[SandboxId],
    ) -> Result<HashMap<SandboxId, SandboxRestoreState>, sqlx::Error> {
        if self.fail {
            return Err(sqlx::Error::PoolClosed);
        }
        let block = {
            let mut calls = self.calls.lock().expect("calls lock");
            let mut out = HashMap::new();
            let mut block = false;
            for id in ids {
                let count = calls.entry(*id).or_insert(0);
                if *count >= 1 && self.fail_recheck {
                    return Err(sqlx::Error::PoolClosed);
                }
                if (*count >= 1 && self.block_recheck) || (*count == 0 && self.block_scan) {
                    block = true;
                }
                let state = if *count == 0 {
                    self.states.get(id)
                } else {
                    self.recheck.get(id).or_else(|| self.states.get(id))
                };
                *count += 1;
                if let Some(state) = state {
                    out.insert(*id, state.clone());
                }
            }
            if !block {
                return Ok(out);
            }
            block
        };
        // Blocked call: never resolve, so the caller's
        // `timeout_at(deadline, …)` must fire.
        if block {
            std::future::pending::<()>().await;
        }
        unreachable!("pending future never resolves")
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn sandbox_id() -> SandboxId {
    SandboxId::from_uuid(Uuid::now_v7())
}

fn restore_pod(
    name: &str,
    uid: &str,
    sandbox_id: SandboxId,
    node: Option<&str>,
) -> RestorePodSnapshot {
    RestorePodSnapshot {
        pod_name: name.to_string(),
        pod_uid: Some(uid.parse().expect("non-empty uid")),
        resource_version: Some("100".to_string()),
        sandbox_id: Some(sandbox_id),
        generation: Some(POD_GEN),
        node_name: node.map(str::to_string),
        terminating: false,
        is_restore: true,
    }
}

fn state(lifecycle: SandboxLifecycle, active_generation: Option<i64>) -> SandboxRestoreState {
    SandboxRestoreState {
        lifecycle: RowLifecycle::Known(lifecycle),
        active_generation,
    }
}

fn config(dry_run: bool) -> ReconcilerConfig {
    ReconcilerConfig {
        enabled: true,
        dry_run,
        interval: Duration::from_secs(60),
        threshold: PositiveDuration::new_panic(Duration::from_secs(300)),
        max_deletes_per_minute: 60,
        tick_budget: Duration::from_secs(45),
        batch_size: 500,
        teardown_stuck_after: PositiveDuration::new_panic(Duration::from_secs(900)),
    }
}

/// A clock fixed at `base + offset`.
fn fixed_clock(base: Instant, offset: Duration) -> Clock {
    std::sync::Arc::new(move || base + offset)
}

// ---------------------------------------------------------------------------
// Configuration
//
// This crate's tests compile `test-hooks` in, so a test-hook read would pass
// these tests too. `scripts/source_guards/env_test_hook_boundary.py` keeps the
// documented variables off test-hook readers in release code.
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
fn config_reads_every_variable_from_the_environment() {
    let config = config_from(&[
        ("SPACE_ORPHANED_RESTORE_POD_GC_ENABLED", "true"),
        ("SPACE_ORPHANED_RESTORE_POD_GC_DRY_RUN", "false"),
        ("SPACE_ORPHANED_RESTORE_POD_GC_INTERVAL_SECS", "30"),
        ("SPACE_ORPHANED_RESTORE_POD_GC_THRESHOLD_SECS", "600"),
        ("SPACE_ORPHANED_RESTORE_POD_GC_MAX_DELETES_PER_MINUTE", "5"),
        ("SPACE_ORPHANED_RESTORE_POD_GC_TICK_BUDGET_SECS", "20"),
        ("SPACE_ORPHANED_RESTORE_POD_GC_BATCH_SIZE", "100"),
        (
            "SPACE_ORPHANED_RESTORE_POD_GC_TEARDOWN_STUCK_AFTER_SECS",
            "120",
        ),
    ]);
    assert_eq!(
        config,
        ReconcilerConfig {
            enabled: true,
            dry_run: false,
            interval: Duration::from_secs(30),
            threshold: PositiveDuration::new_panic(Duration::from_secs(600)),
            max_deletes_per_minute: 5,
            tick_budget: Duration::from_secs(20),
            batch_size: 100,
            teardown_stuck_after: PositiveDuration::new_panic(Duration::from_secs(120)),
        }
    );
}

#[test]
fn config_keeps_defaults_for_zero_or_invalid_values() {
    for value in ["0", "-1", "ten", ""] {
        let config = config_from(&[
            ("SPACE_ORPHANED_RESTORE_POD_GC_INTERVAL_SECS", value),
            ("SPACE_ORPHANED_RESTORE_POD_GC_THRESHOLD_SECS", value),
            (
                "SPACE_ORPHANED_RESTORE_POD_GC_MAX_DELETES_PER_MINUTE",
                value,
            ),
            ("SPACE_ORPHANED_RESTORE_POD_GC_TICK_BUDGET_SECS", value),
            ("SPACE_ORPHANED_RESTORE_POD_GC_BATCH_SIZE", value),
            (
                "SPACE_ORPHANED_RESTORE_POD_GC_TEARDOWN_STUCK_AFTER_SECS",
                value,
            ),
        ]);
        assert_eq!(config, ReconcilerConfig::default(), "{value:?}");
    }
}

#[test]
fn config_clamps_values_above_their_ceilings() {
    for value in ["999999", "99999999999999999999999"] {
        let config = config_from(&[
            ("SPACE_ORPHANED_RESTORE_POD_GC_INTERVAL_SECS", value),
            ("SPACE_ORPHANED_RESTORE_POD_GC_THRESHOLD_SECS", value),
            (
                "SPACE_ORPHANED_RESTORE_POD_GC_MAX_DELETES_PER_MINUTE",
                value,
            ),
            ("SPACE_ORPHANED_RESTORE_POD_GC_TICK_BUDGET_SECS", value),
            ("SPACE_ORPHANED_RESTORE_POD_GC_BATCH_SIZE", value),
            (
                "SPACE_ORPHANED_RESTORE_POD_GC_TEARDOWN_STUCK_AFTER_SECS",
                value,
            ),
        ]);
        let day = Duration::from_secs(24 * 60 * 60);
        assert_eq!(
            config,
            ReconcilerConfig {
                interval: day,
                threshold: PositiveDuration::new_panic(day),
                max_deletes_per_minute: 600,
                tick_budget: day,
                batch_size: 5000,
                teardown_stuck_after: PositiveDuration::new_panic(day),
                ..ReconcilerConfig::default()
            },
            "{value:?}"
        );
    }
}

#[test]
fn config_leaves_dry_run_only_on_an_explicit_false() {
    for value in ["false", "0", "FALSE"] {
        let config = config_from(&[("SPACE_ORPHANED_RESTORE_POD_GC_DRY_RUN", value)]);
        assert!(!config.dry_run, "{value:?}");
    }
    for value in ["true", "fasle", "off", ""] {
        let config = config_from(&[("SPACE_ORPHANED_RESTORE_POD_GC_DRY_RUN", value)]);
        assert!(config.dry_run, "{value:?} should keep dry-run on");
    }
}

// ---------------------------------------------------------------------------
// Pure supersession matrix
// ---------------------------------------------------------------------------

#[test]
fn missing_row_is_orphaned() {
    // A restore pod whose sandbox row was deleted is an orphan: no row-driven
    // reconciler can find it, and a future restore claims a fresh sandbox — it
    // can't adopt this pod.
    assert_eq!(
        classify_disposition(None, POD_GEN),
        Disposition::Orphaned(OrphanReason::SandboxMissing)
    );
}

#[test]
fn restoring_row_is_skipped() {
    let s = state(SandboxLifecycle::Restoring, Some(POD_GEN));
    assert_eq!(
        classify_disposition(Some(&s), POD_GEN),
        Disposition::Skip(SkipClass::RowRestoring)
    );
}

#[test]
fn other_transient_rows_are_skipped() {
    for lifecycle in [
        SandboxLifecycle::Creating,
        SandboxLifecycle::Starting,
        SandboxLifecycle::Pausing,
        SandboxLifecycle::Resuming,
        SandboxLifecycle::Suspending,
        SandboxLifecycle::Stopping,
        SandboxLifecycle::Deleting,
        SandboxLifecycle::Checkpointing,
    ] {
        let s = state(lifecycle, None);
        assert_eq!(
            classify_disposition(Some(&s), POD_GEN),
            Disposition::Skip(SkipClass::RowTransient),
            "lifecycle {lifecycle:?} should defer as transient"
        );
    }
}

#[test]
fn reverted_row_with_no_active_execution_is_orphaned() {
    // A reverted restore's settle states with no active execution: whatever
    // retired the execution meant the pod to be gone, and a future restore
    // claims a strictly-higher generation.
    for lifecycle in [SandboxLifecycle::Suspended, SandboxLifecycle::Failed] {
        let s = state(lifecycle, None);
        assert_eq!(
            classify_disposition(Some(&s), POD_GEN),
            Disposition::Orphaned(OrphanReason::RowReverted),
            "lifecycle {lifecycle:?} with no active execution should be an orphan"
        );
    }
}

#[test]
fn other_resident_row_with_no_active_execution_is_skipped() {
    // These lifecycles should carry an active execution. Without one the pod
    // is unadoptable, but nothing proves it holds no unique state (a paused
    // VM lives only in its pod), so the loop leaves it alone.
    for lifecycle in [
        SandboxLifecycle::Created,
        SandboxLifecycle::Started,
        SandboxLifecycle::Paused,
        SandboxLifecycle::Stopped,
    ] {
        let s = state(lifecycle, None);
        assert_eq!(
            classify_disposition(Some(&s), POD_GEN),
            Disposition::Skip(SkipClass::NoActiveExecution),
            "lifecycle {lifecycle:?} with no active execution should be skipped"
        );
    }
}

#[test]
fn quarantined_row_is_never_reaped() {
    // Parked for an operator: skipped whatever the generations say.
    for active_generation in [None, Some(POD_GEN), Some(POD_GEN + 1)] {
        let s = state(SandboxLifecycle::Quarantined, active_generation);
        assert_eq!(
            classify_disposition(Some(&s), POD_GEN),
            Disposition::Skip(SkipClass::RowQuarantined),
            "quarantined with active {active_generation:?}"
        );
    }
}

#[test]
fn strictly_older_generation_is_orphaned() {
    // Active execution advanced past this pod's generation → superseded.
    for lifecycle in [
        SandboxLifecycle::Started,
        SandboxLifecycle::Paused,
        SandboxLifecycle::Suspended,
        SandboxLifecycle::Failed,
    ] {
        let s = state(lifecycle, Some(POD_GEN + 1));
        assert_eq!(
            classify_disposition(Some(&s), POD_GEN),
            Disposition::Orphaned(OrphanReason::RowReverted),
            "lifecycle {lifecycle:?} strictly-older pod should be an orphan"
        );
    }
}

#[test]
fn current_generation_pod_is_skipped() {
    // Same generation as the active execution → the current execution's pod
    // (materialized, or still materializing its UID) → skip.
    for lifecycle in [
        SandboxLifecycle::Started,
        SandboxLifecycle::Paused,
        SandboxLifecycle::Failed,
    ] {
        let s = state(lifecycle, Some(POD_GEN));
        assert_eq!(
            classify_disposition(Some(&s), POD_GEN),
            Disposition::Skip(SkipClass::ActiveRuntime),
            "lifecycle {lifecycle:?} current-generation pod should be skipped"
        );
    }
}

#[test]
fn same_generation_pod_is_never_reaped_even_when_row_reverted() {
    // A same-generation pod on a `suspended` row whose execution is still
    // active is left alone: a restore resuming that generation could still
    // adopt it, and a suspend that raced a wake leaves this same shape with
    // the only copy of the sandbox's state in the pod. It's tallied apart from
    // the live runtimes so the backlog is visible; once a revert retires the
    // execution, this loop reaps the pod as no-active-execution.
    let s = state(SandboxLifecycle::Suspended, Some(POD_GEN));
    assert_eq!(
        classify_disposition(Some(&s), POD_GEN),
        Disposition::Skip(SkipClass::SuspendedSameGeneration)
    );
}

#[test]
fn future_generation_is_skipped() {
    // A pod claiming a generation above the active execution is anomalous
    // (the active execution is the max); never reap on that basis.
    let s = state(SandboxLifecycle::Started, Some(POD_GEN - 1));
    assert_eq!(
        classify_disposition(Some(&s), POD_GEN),
        Disposition::Skip(SkipClass::ActiveRuntime)
    );
}

#[test]
fn unrecognized_row_is_skipped_even_when_otherwise_superseded() {
    // A row that exists but that this loop can't vouch for (non-Kubernetes
    // `managed_by`, or a lifecycle value with no `SandboxLifecycle` variant)
    // is skipped, even with no active execution — it has an owner.
    let s = SandboxRestoreState {
        lifecycle: RowLifecycle::Unrecognized,
        active_generation: None,
    };
    assert_eq!(
        classify_disposition(Some(&s), POD_GEN),
        Disposition::Skip(SkipClass::UnrecognizedRow)
    );
}

#[test]
fn from_pod_rejects_non_positive_generation() {
    use k8s_openapi::api::core::v1::Pod;
    use pplx_space_common_core::kubernetes::{
        SANDBOX_EXECUTION_GENERATION_ANNOTATION, SANDBOX_ID_ANNOTATION,
        SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION,
    };
    let pod_with = |generation: &str| {
        let mut pod = Pod::default();
        pod.metadata.name = Some("sbx-a".to_string());
        pod.metadata.uid = Some("uid-a".to_string());
        pod.metadata.annotations = Some(
            [
                (SANDBOX_ID_ANNOTATION, Uuid::now_v7().to_string()),
                (
                    SANDBOX_EXECUTION_GENERATION_ANNOTATION,
                    generation.to_string(),
                ),
                (
                    SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION,
                    "s3://b/k".to_string(),
                ),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
        );
        pod
    };
    let snap = RestorePodSnapshot::from_pod(&SandboxPodView::from(pod_with("3")));
    assert_eq!(snap.generation, Some(3));
    assert!(snap.is_restore);
    for bad in ["0", "-1", "x"] {
        assert_eq!(
            RestorePodSnapshot::from_pod(&SandboxPodView::from(pod_with(bad))).generation,
            None,
            "{bad}"
        );
    }
}

// ---------------------------------------------------------------------------
// Reconciler flow
// ---------------------------------------------------------------------------

fn reconciler(
    cfg: ReconcilerConfig,
    pods: FakePods,
    store: FakeStore,
    clock: Clock,
) -> Reconciler<FakePods, FakeStore> {
    Reconciler::new(cfg, pods, store).with_clock(clock)
}

/// A store state that makes a `POD_GEN` pod an orphan: suspended row whose
/// execution was already retired (no active execution).
fn orphan_state() -> SandboxRestoreState {
    state(SandboxLifecycle::Suspended, None)
}

#[tokio::test]
async fn reaps_orphan_past_threshold_in_live_mode() {
    let sid = sandbox_id();
    let pod = restore_pod("sbx-a", "uid-a", sid, Some("node-1"));
    let pods = FakePods::new(vec![pod]);
    let store = FakeStore::default().with(sid, orphan_state());
    let base = Instant::now();
    let mut r = reconciler(
        config(false),
        pods,
        store,
        fixed_clock(base, Duration::ZERO),
    );
    r.observe_since("uid-a", base - Duration::from_secs(301));

    let report = r.tick().await;

    assert_eq!(report.orphans_detected, 1);
    // Accepted, not yet confirmed: the pod is terminating until it leaves.
    assert_eq!(report.delete_requested, 1);
    assert_eq!(report.reaped, 0);
    let deletes = r.pod_backend.deletes();
    assert_eq!(deletes.len(), 1);
    assert_eq!(deletes[0].pod_name, "sbx-a");
    assert_eq!(deletes[0].pod_uid.as_str(), "uid-a");
    // The delete carries the fresh resourceVersion precondition.
    assert_eq!(deletes[0].resource_version.as_deref(), Some("100"));
}

#[tokio::test]
async fn dry_run_reports_but_does_not_delete() {
    let sid = sandbox_id();
    let pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, Some("node-1"))]);
    let store = FakeStore::default().with(sid, orphan_state());
    let base = Instant::now();
    let mut r = reconciler(config(true), pods, store, fixed_clock(base, Duration::ZERO));
    r.observe_since("uid-a", base - Duration::from_secs(301));

    let report = r.tick().await;

    assert_eq!(report.orphans_detected, 1);
    assert_eq!(report.reaped, 1, "dry-run counts the would-reap");
    assert!(
        r.pod_backend.deletes().is_empty(),
        "dry-run deletes nothing"
    );
}

#[tokio::test]
async fn dry_run_reports_each_orphan_once() {
    // Live mode deletes a pod once, so the preview must count it once too —
    // not once per tick while it lingers.
    let sid = sandbox_id();
    let pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, Some("node-1"))]);
    let store = FakeStore::default().with(sid, orphan_state());
    let base = Instant::now();
    let mut r = reconciler(config(true), pods, store, fixed_clock(base, Duration::ZERO));
    r.observe_since("uid-a", base - Duration::from_secs(301));

    let first = r.tick().await;
    let second = r.tick().await;

    assert_eq!(first.reaped, 1);
    assert_eq!(second.reaped, 0, "already previewed");
    assert_eq!(second.already_previewed, 1);
    assert_eq!(second.orphans_detected, 0);
}

#[tokio::test]
async fn orphan_under_threshold_is_too_young() {
    let sid = sandbox_id();
    let pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, None)]);
    let store = FakeStore::default().with(sid, orphan_state());
    let base = Instant::now();
    // No observe_since seed: first observation, so observed age is 0 < threshold.
    let mut r = reconciler(
        config(false),
        pods,
        store,
        fixed_clock(base, Duration::ZERO),
    );

    let report = r.tick().await;

    assert_eq!(report.orphans_detected, 0);
    assert_eq!(report.too_young, 1);
    assert!(r.pod_backend.deletes().is_empty());
}

#[tokio::test]
async fn restoring_row_is_never_reaped() {
    let sid = sandbox_id();
    let pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, Some("node-1"))]);
    let store = FakeStore::default().with(sid, state(SandboxLifecycle::Restoring, Some(POD_GEN)));
    let base = Instant::now();
    let mut r = reconciler(
        config(false),
        pods,
        store,
        fixed_clock(base, Duration::ZERO),
    );
    r.observe_since("uid-a", base - Duration::from_secs(301));

    let report = r.tick().await;

    assert_eq!(report.orphans_detected, 0);
    assert_eq!(
        report.skipped.get(&SkipClass::RowRestoring).copied(),
        Some(1)
    );
    assert!(r.pod_backend.deletes().is_empty());
}

#[tokio::test]
async fn current_generation_pod_is_never_reaped() {
    let sid = sandbox_id();
    let pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, Some("node-1"))]);
    // Started sandbox whose active execution is at this pod's generation.
    let store = FakeStore::default().with(sid, state(SandboxLifecycle::Started, Some(POD_GEN)));
    let base = Instant::now();
    let mut r = reconciler(
        config(false),
        pods,
        store,
        fixed_clock(base, Duration::ZERO),
    );
    r.observe_since("uid-a", base - Duration::from_secs(301));

    let report = r.tick().await;

    assert_eq!(report.orphans_detected, 0);
    assert_eq!(
        report.skipped.get(&SkipClass::ActiveRuntime).copied(),
        Some(1)
    );
    assert!(r.pod_backend.deletes().is_empty());
}

#[tokio::test]
async fn recheck_race_with_new_restore_skips_delete() {
    // Scan classifies the pod an orphan (row suspended, no active execution),
    // but between scan and the verify-before-destroy recheck a new restore
    // CAS'd the row back to `restoring`. The reaper must NOT delete it.
    let sid = sandbox_id();
    let pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, Some("node-1"))]);
    let store = FakeStore::default()
        .with(sid, orphan_state())
        .with_recheck(sid, state(SandboxLifecycle::Restoring, Some(POD_GEN)));
    let base = Instant::now();
    let mut r = reconciler(
        config(false),
        pods,
        store,
        fixed_clock(base, Duration::ZERO),
    );
    r.observe_since("uid-a", base - Duration::from_secs(301));

    let report = r.tick().await;

    assert_eq!(report.orphans_detected, 1);
    assert_eq!(report.reaped, 0);
    assert!(
        r.pod_backend.deletes().is_empty(),
        "must not delete a pod a new restore re-claimed"
    );
}

#[tokio::test]
async fn stale_identity_on_fresh_read_skips_delete() {
    let sid = sandbox_id();
    let pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, Some("node-1"))])
        // A newer-generation pod now holds this name.
        .with_fresh(
            "sbx-a",
            FreshRead::Replace(Box::new(restore_pod("sbx-a", "uid-b", sid, Some("node-1")))),
        );
    let store = FakeStore::default().with(sid, orphan_state());
    let base = Instant::now();
    let mut r = reconciler(
        config(false),
        pods,
        store,
        fixed_clock(base, Duration::ZERO),
    );
    r.observe_since("uid-a", base - Duration::from_secs(301));

    let report = r.tick().await;

    assert_eq!(report.reaped, 0);
    assert!(r.pod_backend.deletes().is_empty());
    assert_eq!(r.observed_count(), 0, "a replaced pod's clock is dropped");
}

#[tokio::test]
async fn changed_identity_on_fresh_read_skips_delete() {
    // Same UID, but the object no longer matches what the scan classified:
    // its generation moved, its owning sandbox changed, or it lost its
    // restore marker. None may fall through to the delete.
    let sid = sandbox_id();
    let other = sandbox_id();
    let variants: [fn(&mut RestorePodSnapshot, SandboxId); 3] = [
        |pod, _| pod.generation = Some(POD_GEN + 1),
        |pod, other| pod.sandbox_id = Some(other),
        |pod, _| pod.is_restore = false,
    ];
    for mutate in variants {
        let mut changed = restore_pod("sbx-a", "uid-a", sid, Some("node-1"));
        mutate(&mut changed, other);
        let pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, Some("node-1"))])
            .with_fresh("sbx-a", FreshRead::Replace(Box::new(changed)));
        let store = FakeStore::default().with(sid, orphan_state());
        let base = Instant::now();
        let mut r = reconciler(
            config(false),
            pods,
            store,
            fixed_clock(base, Duration::ZERO),
        );
        r.observe_since("uid-a", base - Duration::from_secs(301));

        let report = r.tick().await;

        assert_eq!(report.reaped, 0);
        assert!(r.pod_backend.deletes().is_empty());
        assert_eq!(r.observed_count(), 0, "a changed identity drops its clock");
    }
}

#[tokio::test]
async fn terminating_on_fresh_read_skips_delete() {
    let sid = sandbox_id();
    let mut terminating = restore_pod("sbx-a", "uid-a", sid, Some("node-1"));
    terminating.terminating = true;
    let pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, Some("node-1"))])
        .with_fresh("sbx-a", FreshRead::Replace(Box::new(terminating)));
    let store = FakeStore::default().with(sid, orphan_state());
    let base = Instant::now();
    let mut r = reconciler(
        config(false),
        pods,
        store,
        fixed_clock(base, Duration::ZERO),
    );
    r.observe_since("uid-a", base - Duration::from_secs(301));

    let report = r.tick().await;

    assert_eq!(report.reaped, 0);
    assert!(r.pod_backend.deletes().is_empty());
}

#[tokio::test]
async fn fresh_read_error_resets_the_pod_clock() {
    // An authoritative GET that fails is not verified observation: no
    // delete, and the pod re-ages a full threshold.
    let sid = sandbox_id();
    let pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, Some("node-1"))])
        .with_fresh("sbx-a", FreshRead::Error);
    let store = FakeStore::default().with(sid, orphan_state());
    let base = Instant::now();
    let mut r = reconciler(
        config(false),
        pods,
        store,
        fixed_clock(base, Duration::ZERO),
    );
    r.observe_since("uid-a", base - Duration::from_secs(301));

    let report = r.tick().await;

    assert_eq!(report.reaped, 0);
    assert!(r.pod_backend.deletes().is_empty());
    assert_eq!(
        r.observed_count(),
        0,
        "a failed fresh read must drop the clock"
    );
}

#[tokio::test]
async fn pod_gone_on_fresh_read_skips_delete() {
    let sid = sandbox_id();
    let pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, None)])
        .with_fresh("sbx-a", FreshRead::Gone);
    let store = FakeStore::default().with(sid, state(SandboxLifecycle::Failed, None));
    let base = Instant::now();
    let mut r = reconciler(
        config(false),
        pods,
        store,
        fixed_clock(base, Duration::ZERO),
    );
    r.observe_since("uid-a", base - Duration::from_secs(301));

    let report = r.tick().await;

    assert_eq!(report.reaped, 0);
    assert!(r.pod_backend.deletes().is_empty());
}

#[tokio::test]
async fn terminating_pod_is_not_a_candidate() {
    let sid = sandbox_id();
    let mut pod = restore_pod("sbx-a", "uid-a", sid, Some("node-1"));
    pod.terminating = true;
    let pods = FakePods::new(vec![pod]);
    let store = FakeStore::default().with(sid, orphan_state());
    let base = Instant::now();
    let mut r = reconciler(
        config(false),
        pods,
        store,
        fixed_clock(base, Duration::ZERO),
    );
    r.observe_since("uid-a", base - Duration::from_secs(301));

    let report = r.tick().await;

    assert_eq!(report.terminating, 1);
    assert_eq!(report.orphans_detected, 0);
    assert!(r.pod_backend.deletes().is_empty());
}

#[tokio::test]
async fn cache_outage_resets_observation_clock() {
    let sid = sandbox_id();
    let mut pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, None)]);
    pods.ready = false;
    let store = FakeStore::default().with(sid, orphan_state());
    let base = Instant::now();
    let mut r = reconciler(
        config(false),
        pods,
        store,
        fixed_clock(base, Duration::ZERO),
    );
    r.observe_since("uid-a", base - Duration::from_secs(301));

    let report = r.tick().await;

    assert!(report.cache_not_ready);
    assert_eq!(
        r.observed_count(),
        0,
        "cold-cache tick must clear the observation clock"
    );
}

#[tokio::test]
async fn classify_failure_resets_observation_clock() {
    let sid = sandbox_id();
    let pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, None)]);
    let store = FakeStore::failing();
    let base = Instant::now();
    let mut r = reconciler(
        config(false),
        pods,
        store,
        fixed_clock(base, Duration::ZERO),
    );
    r.observe_since("uid-a", base - Duration::from_secs(301));

    let report = r.tick().await;

    assert_eq!(report.orphans_detected, 0);
    assert_eq!(
        r.observed_count(),
        0,
        "a failed classify query must clear the observation clock"
    );
    assert!(r.pod_backend.deletes().is_empty());
}

#[tokio::test]
async fn classify_timeout_resets_observation_clock() {
    // The scan's classify query shares the tick's deadline, so a stalled
    // Postgres can't hold the tick past its budget.
    let sid = sandbox_id();
    let pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, None)]);
    let store = FakeStore {
        block_scan: true,
        ..FakeStore::default().with(sid, orphan_state())
    };
    let mut cfg = config(false);
    cfg.tick_budget = Duration::from_millis(100);
    let base = Instant::now();
    let mut r = reconciler(cfg, pods, store, fixed_clock(base, Duration::ZERO));
    r.observe_since("uid-a", base - Duration::from_secs(301));

    let report = r.tick().await;

    assert_eq!(report.orphans_detected, 0);
    assert_eq!(
        r.observed_count(),
        0,
        "a timed-out scan must clear the clock"
    );
    assert!(r.pod_backend.deletes().is_empty());
}

#[tokio::test]
async fn db_recheck_error_resets_the_pod_clock() {
    // A recheck that can't reach Postgres is not verified observation: the
    // pod must re-age a full threshold rather than be deleted next tick.
    let sid = sandbox_id();
    let pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, Some("node-1"))]);
    let store = FakeStore {
        fail_recheck: true,
        ..FakeStore::default().with(sid, orphan_state())
    };
    let base = Instant::now();
    let mut r = reconciler(
        config(false),
        pods,
        store,
        fixed_clock(base, Duration::ZERO),
    );
    r.observe_since("uid-a", base - Duration::from_secs(301));

    let report = r.tick().await;

    assert_eq!(report.reaped, 0);
    assert!(r.pod_backend.deletes().is_empty());
    assert_eq!(
        r.observed_count(),
        0,
        "a failed recheck must drop the clock"
    );
}

#[tokio::test]
async fn db_recheck_timeout_defers_delete() {
    // The per-candidate recheck must be bounded by the tick deadline so a
    // stalled Postgres can't hold the loop past the budget.
    let sid = sandbox_id();
    let pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, Some("node-1"))]);
    let mut store = FakeStore::default().with(sid, orphan_state());
    store.block_recheck = true;
    let mut cfg = config(false);
    cfg.tick_budget = Duration::from_millis(100);
    let base = Instant::now();
    let mut r = reconciler(cfg, pods, store, fixed_clock(base, Duration::ZERO));
    r.observe_since("uid-a", base - Duration::from_secs(301));

    let report = r.tick().await;

    assert_eq!(report.orphans_detected, 1);
    assert_eq!(report.reaped, 0);
    assert!(
        r.pod_backend.deletes().is_empty(),
        "a stalled recheck must not fall through to delete"
    );
    assert_eq!(
        r.observed_count(),
        0,
        "a timed-out recheck must drop the clock"
    );
}

#[tokio::test]
async fn rate_limit_caps_deletes_per_tick() {
    let mut listed = Vec::new();
    let mut store = FakeStore::default();
    for i in 0..5 {
        let sid = sandbox_id();
        let name = format!("sbx-{i}");
        let uid = format!("uid-{i}");
        listed.push(restore_pod(&name, &uid, sid, Some("node-1")));
        store = store.with(sid, orphan_state());
    }
    let mut cfg = config(false);
    cfg.max_deletes_per_minute = 2;
    let pods = FakePods::new(listed);
    let base = Instant::now();
    let mut r = reconciler(cfg, pods, store, fixed_clock(base, Duration::ZERO));
    for i in 0..5 {
        r.observe_since(&format!("uid-{i}"), base - Duration::from_secs(301));
    }

    let report = r.tick().await;

    assert_eq!(report.orphans_detected, 5);
    assert_eq!(
        report.delete_requested, 2,
        "only the budget is spent this tick"
    );
    assert_eq!(report.rate_limited, 3);
    assert_eq!(r.pod_backend.deletes().len(), 2);
}

#[tokio::test]
async fn rate_budget_frees_at_the_next_tick() {
    // A tick that starts just before the previous tick's charges leave the
    // window waits for them to expire rather than losing the whole tick to
    // rate limiting. The test clock runs in real time from a settable offset,
    // so the wait is observable.
    let mut listed = Vec::new();
    let mut store = FakeStore::default();
    for i in 0..5 {
        let sid = sandbox_id();
        listed.push(restore_pod(
            &format!("sbx-{i}"),
            &format!("uid-{i}"),
            sid,
            Some("node-1"),
        ));
        store = store.with(sid, orphan_state());
    }
    let mut cfg = config(false);
    cfg.max_deletes_per_minute = 2;
    let base = Instant::now();
    let offset = std::sync::Arc::new(Mutex::new(Duration::ZERO));
    let clock_offset = offset.clone();
    let real = Instant::now();
    let clock: Clock = std::sync::Arc::new(move || {
        base + *clock_offset.lock().expect("clock lock") + real.elapsed()
    });
    let mut r = reconciler(cfg, FakePods::new(listed), store, clock);
    for i in 0..5 {
        r.observe_since(&format!("uid-{i}"), base - Duration::from_secs(301));
    }

    let first = r.tick().await;
    // Land 100ms short of the first tick's charges expiring.
    *offset.lock().expect("clock lock") = Duration::from_millis(59_900) - real.elapsed();
    let second = r.tick().await;

    assert_eq!(first.delete_requested, 2);
    assert_eq!(
        second.delete_requested, 2,
        "the previous tick's charges expired"
    );
}

#[tokio::test]
async fn fairness_cursor_rotates_through_a_sorted_backlog() {
    // Budget 1 per tick, every delete conflicting (409 keeps the pod's clock),
    // pods listed out of UID order. Each tick must try the next pod in UID
    // order rather than retrying the same one.
    let mut listed = Vec::new();
    let mut store = FakeStore::default();
    for i in [2, 0, 1] {
        let sid = sandbox_id();
        listed.push(restore_pod(
            &format!("sbx-{i}"),
            &format!("uid-{i}"),
            sid,
            Some("node-1"),
        ));
        store = store.with(sid, orphan_state());
    }
    let mut cfg = config(false);
    cfg.max_deletes_per_minute = 1;
    let pods = FakePods::new(listed).with_delete_outcome(DeleteOutcome::StaleIdentity);
    let base = Instant::now();
    let offset = std::sync::Arc::new(Mutex::new(Duration::ZERO));
    let clock_offset = offset.clone();
    let clock: Clock =
        std::sync::Arc::new(move || base + *clock_offset.lock().expect("clock lock"));
    let mut r = reconciler(cfg, pods, store, clock);
    for i in 0..3 {
        r.observe_since(&format!("uid-{i}"), base - Duration::from_secs(301));
    }

    for tick in 0..3u32 {
        *offset.lock().expect("clock lock") = Duration::from_secs(61) * tick;
        r.tick().await;
    }

    let order: Vec<String> = r
        .pod_backend
        .deletes()
        .into_iter()
        .map(|call| call.pod_uid.as_str().to_string())
        .collect();
    assert_eq!(order, ["uid-0", "uid-1", "uid-2"]);
}

#[tokio::test]
async fn stale_delete_precondition_is_benign() {
    let sid = sandbox_id();
    let pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, Some("node-1"))])
        .with_delete_outcome(DeleteOutcome::StaleIdentity);
    let store = FakeStore::default().with(sid, orphan_state());
    let base = Instant::now();
    let mut r = reconciler(
        config(false),
        pods,
        store,
        fixed_clock(base, Duration::ZERO),
    );
    r.observe_since("uid-a", base - Duration::from_secs(301));

    let report = r.tick().await;

    // The delete was attempted (budget spent) but the precondition failed, so
    // nothing was reaped. A resourceVersion-only conflict leaves the same pod
    // in place, so the observation clock is preserved (retry next tick), NOT
    // reset — otherwise a just-changed pod would re-age a full threshold.
    assert_eq!(report.reaped, 0);
    assert_eq!(r.pod_backend.deletes().len(), 1);
    assert_eq!(
        r.observed_count(),
        1,
        "a stale-identity conflict must keep the observation clock for retry"
    );
}

#[tokio::test]
async fn missing_row_orphan_is_reaped() {
    // A restore pod whose sandbox row was deleted has no entry in the store —
    // no row-driven reconciler can find it, so this reaper must.
    let sid = sandbox_id();
    let pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, Some("node-1"))]);
    let store = FakeStore::default(); // no row for `sid`
    let base = Instant::now();
    let mut r = reconciler(
        config(false),
        pods,
        store,
        fixed_clock(base, Duration::ZERO),
    );
    r.observe_since("uid-a", base - Duration::from_secs(301));

    let report = r.tick().await;

    assert_eq!(report.orphans_detected, 1);
    assert_eq!(report.delete_requested, 1);
    assert_eq!(r.pod_backend.deletes().len(), 1);
    assert_eq!(r.pod_backend.deletes()[0].pod_uid.as_str(), "uid-a");
}

// ---------------------------------------------------------------------------
// Teardown confirmation
// ---------------------------------------------------------------------------

/// A reconciler whose live DELETE of `uid-a` was accepted on the first tick,
/// with a settable clock offset for the follow-up ticks.
async fn reconciler_after_accepted_delete() -> (
    Reconciler<FakePods, FakeStore>,
    std::sync::Arc<Mutex<Duration>>,
    SandboxId,
) {
    let sid = sandbox_id();
    let pods = FakePods::new(vec![restore_pod("sbx-a", "uid-a", sid, Some("node-1"))]);
    let store = FakeStore::default().with(sid, orphan_state());
    let base = Instant::now();
    let offset = std::sync::Arc::new(Mutex::new(Duration::ZERO));
    let clock_offset = offset.clone();
    let clock: Clock =
        std::sync::Arc::new(move || base + *clock_offset.lock().expect("clock lock"));
    let mut r = reconciler(config(false), pods, store, clock);
    r.observe_since("uid-a", base - Duration::from_secs(301));
    let first = r.tick().await;
    assert_eq!(first.delete_requested, 1);
    (r, offset, sid)
}

#[tokio::test]
async fn reap_is_confirmed_only_once_the_pod_is_gone() {
    let (mut r, offset, sid) = reconciler_after_accepted_delete().await;
    let mut terminating = restore_pod("sbx-a", "uid-a", sid, Some("node-1"));
    terminating.terminating = true;

    // Still terminating: not reaped, not re-deleted, not stuck yet.
    r.pod_backend.set_listed(vec![terminating]);
    *offset.lock().expect("clock lock") = Duration::from_secs(60);
    let pending = r.tick().await;
    assert_eq!(pending.reaped, 0);
    assert_eq!(pending.teardown_stuck, 0);
    assert_eq!(r.pod_backend.deletes().len(), 1, "no second DELETE");

    // Gone from the cache: that's the confirmed reap.
    r.pod_backend.set_listed(Vec::new());
    *offset.lock().expect("clock lock") = Duration::from_secs(120);
    let confirmed = r.tick().await;
    assert_eq!(confirmed.reaped, 1);

    // Confirmed once.
    let after = r.tick().await;
    assert_eq!(after.reaped, 0);
}

#[tokio::test]
async fn cached_pod_right_after_delete_is_not_redeleted() {
    // The cache can still show the pod as not terminating right after the
    // DELETE; it's in teardown, not a fresh candidate.
    let (mut r, offset, _sid) = reconciler_after_accepted_delete().await;
    *offset.lock().expect("clock lock") = Duration::from_secs(60);
    let report = r.tick().await;
    assert_eq!(report.orphans_detected, 0);
    assert_eq!(report.terminating, 1);
    assert_eq!(r.pod_backend.deletes().len(), 1);
}

#[tokio::test]
async fn lingering_teardown_is_reported_stuck_once() {
    let (mut r, offset, sid) = reconciler_after_accepted_delete().await;
    let mut terminating = restore_pod("sbx-a", "uid-a", sid, Some("node-1"));
    terminating.terminating = true;
    r.pod_backend.set_listed(vec![terminating]);

    *offset.lock().expect("clock lock") = Duration::from_secs(899);
    assert_eq!(r.tick().await.teardown_stuck, 0, "under the threshold");
    *offset.lock().expect("clock lock") = Duration::from_secs(900);
    assert_eq!(r.tick().await.teardown_stuck, 1);
    *offset.lock().expect("clock lock") = Duration::from_secs(960);
    assert_eq!(r.tick().await.teardown_stuck, 0, "reported once");

    // A stuck teardown that finally finishes is still confirmed.
    r.pod_backend.set_listed(Vec::new());
    assert_eq!(r.tick().await.reaped, 1);
}

#[tokio::test]
async fn stale_cache_teardown_is_confirmed_by_the_authoritative_read() {
    // A stalled watch can keep a deleted pod in the cache. Past the stuck
    // threshold the fresh read decides: gone means the reap completed, not a
    // stuck teardown.
    let (mut r, offset, sid) = reconciler_after_accepted_delete().await;
    let mut terminating = restore_pod("sbx-a", "uid-a", sid, Some("node-1"));
    terminating.terminating = true;
    r.pod_backend.set_listed(vec![terminating]);
    r.pod_backend.set_fresh("sbx-a", FreshRead::Gone);

    *offset.lock().expect("clock lock") = Duration::from_secs(900);
    let report = r.tick().await;
    assert_eq!(report.teardown_stuck, 0);
    assert_eq!(report.reaped, 1);
}

#[tokio::test]
async fn a_hung_stuck_check_does_not_starve_reaping() {
    // A teardown due for its stuck check whose authoritative GET hangs must not
    // eat the tick: orphan work runs first, the check gets the leftover budget.
    let stuck_sid = sandbox_id();
    let orphan_sid = sandbox_id();
    let mut terminating = restore_pod("sbx-stuck", "uid-stuck", stuck_sid, Some("node-1"));
    terminating.terminating = true;
    let pods = FakePods::new(vec![
        terminating,
        restore_pod("sbx-orphan", "uid-orphan", orphan_sid, Some("node-1")),
    ])
    .with_fresh("sbx-stuck", FreshRead::Hang);
    let store = FakeStore::default().with(orphan_sid, orphan_state());
    let mut cfg = config(false);
    cfg.tick_budget = Duration::from_millis(200);
    let base = Instant::now();
    let offset = std::sync::Arc::new(Mutex::new(Duration::ZERO));
    let clock_offset = offset.clone();
    let clock: Clock =
        std::sync::Arc::new(move || base + *clock_offset.lock().expect("clock lock"));
    let mut r = reconciler(cfg, pods, store, clock);

    // First sighting starts the stuck clock; the orphan isn't old enough yet.
    r.tick().await;
    // Past both thresholds: the stuck check is due and hangs; the orphan is
    // still reaped this tick.
    *offset.lock().expect("clock lock") = Duration::from_secs(901);
    r.observe_since("uid-orphan", base);
    let report = r.tick().await;
    assert_eq!(report.delete_requested, 1);
    assert_eq!(report.teardown_stuck, 0, "unverified, so not reported yet");
    assert_eq!(r.pod_backend.deletes()[0].pod_uid.as_str(), "uid-orphan");
}

#[tokio::test]
async fn stuck_teardown_is_rechecked_so_a_missed_disappearance_settles() {
    // After the one-time stuck report the pod keeps being re-read: if it goes
    // away while the cache stays stale, the reap is still confirmed.
    let (mut r, offset, sid) = reconciler_after_accepted_delete().await;
    let mut terminating = restore_pod("sbx-a", "uid-a", sid, Some("node-1"));
    terminating.terminating = true;
    r.pod_backend.set_listed(vec![terminating]);

    *offset.lock().expect("clock lock") = Duration::from_secs(900);
    assert_eq!(r.tick().await.teardown_stuck, 1);
    // Gone from the apiserver, but the cache still lists it.
    r.pod_backend.set_fresh("sbx-a", FreshRead::Gone);
    *offset.lock().expect("clock lock") = Duration::from_secs(1000);
    assert_eq!(r.tick().await.reaped, 0, "not due for a re-read yet");
    *offset.lock().expect("clock lock") = Duration::from_secs(1800);
    let report = r.tick().await;
    assert_eq!(report.reaped, 1);
    assert_eq!(report.teardown_stuck, 0, "not reported twice");
}

#[tokio::test]
async fn stuck_teardown_checks_run_when_classification_fails() {
    // Stuck checks only need Kubernetes: a Postgres failure doesn't stop them.
    let sid = sandbox_id();
    let mut terminating = restore_pod("sbx-a", "uid-a", sid, Some("node-1"));
    terminating.terminating = true;
    let base = Instant::now();
    let offset = std::sync::Arc::new(Mutex::new(Duration::ZERO));
    let clock_offset = offset.clone();
    let clock: Clock =
        std::sync::Arc::new(move || base + *clock_offset.lock().expect("clock lock"));
    let pods = FakePods::new(vec![
        terminating,
        restore_pod("sbx-b", "uid-b", sandbox_id(), Some("node-1")),
    ]);
    let mut r = reconciler(config(false), pods, FakeStore::failing(), clock);

    r.tick().await;
    *offset.lock().expect("clock lock") = Duration::from_secs(900);
    assert_eq!(r.tick().await.teardown_stuck, 1);
}

#[tokio::test]
async fn a_stuck_pod_still_present_at_a_re_read_is_reported_once() {
    let (mut r, offset, sid) = reconciler_after_accepted_delete().await;
    let mut terminating = restore_pod("sbx-a", "uid-a", sid, Some("node-1"));
    terminating.terminating = true;
    r.pod_backend.set_listed(vec![terminating]);

    *offset.lock().expect("clock lock") = Duration::from_secs(900);
    assert_eq!(r.tick().await.teardown_stuck, 1);
    // Due for a periodic re-read, still there: not reported again.
    *offset.lock().expect("clock lock") = Duration::from_secs(1800);
    assert_eq!(r.tick().await.teardown_stuck, 0);
    *offset.lock().expect("clock lock") = Duration::from_secs(2700);
    assert_eq!(r.tick().await.teardown_stuck, 0);
}

#[tokio::test]
async fn failing_stuck_checks_rotate_through_the_backlog() {
    // More teardowns are due than the per-tick cap and every read fails: each
    // tick must check different pods, not the same prefix forever.
    let mut listed = Vec::new();
    for i in 0..40 {
        let mut pod = restore_pod(&format!("sbx-{i}"), &format!("uid-{i}"), sandbox_id(), None);
        pod.terminating = true;
        listed.push(pod);
    }
    let pods = FakePods::new(listed);
    for i in 0..40 {
        pods.set_fresh(&format!("sbx-{i}"), FreshRead::Error);
    }
    let base = Instant::now();
    let offset = std::sync::Arc::new(Mutex::new(Duration::ZERO));
    let clock_offset = offset.clone();
    let clock: Clock =
        std::sync::Arc::new(move || base + *clock_offset.lock().expect("clock lock"));
    let mut r = reconciler(config(false), pods, FakeStore::default(), clock);
    r.tick().await;

    // Past the threshold: two ticks cover all 40 (32 then the remaining 8).
    *offset.lock().expect("clock lock") = Duration::from_secs(900);
    r.tick().await;
    *offset.lock().expect("clock lock") = Duration::from_secs(901);
    r.tick().await;
    for i in 0..40 {
        r.pod_backend
            .set_fresh(&format!("sbx-{i}"), FreshRead::Same);
    }
    // All 40 were attempted once, so none is due again until a full
    // `teardown_stuck_after` later — then all are re-read and reported.
    *offset.lock().expect("clock lock") = Duration::from_secs(902);
    assert_eq!(r.tick().await.teardown_stuck, 0);
    *offset.lock().expect("clock lock") = Duration::from_secs(1801);
    let first = r.tick().await.teardown_stuck;
    let second = r.tick().await.teardown_stuck;
    assert_eq!(first + second, 40, "every pod reached, cap-sized batches");
}

#[tokio::test]
async fn terminating_pod_seen_after_restart_is_tracked_for_stuck_teardown() {
    // A fresh reconciler (e.g. after a controller restart) that finds a restore
    // pod already terminating adopts it, so a teardown that never finishes is
    // still reported — but its disappearance isn't this loop's reap.
    let sid = sandbox_id();
    let mut terminating = restore_pod("sbx-a", "uid-a", sid, Some("node-1"));
    terminating.terminating = true;
    let pods = FakePods::new(vec![terminating]);
    let base = Instant::now();
    let offset = std::sync::Arc::new(Mutex::new(Duration::ZERO));
    let clock_offset = offset.clone();
    let clock: Clock =
        std::sync::Arc::new(move || base + *clock_offset.lock().expect("clock lock"));
    let mut r = reconciler(config(false), pods, FakeStore::default(), clock);

    assert_eq!(r.tick().await.teardown_stuck, 0);
    *offset.lock().expect("clock lock") = Duration::from_secs(900);
    assert_eq!(r.tick().await.teardown_stuck, 1);
    r.pod_backend.set_listed(Vec::new());
    assert_eq!(r.tick().await.reaped, 0, "not this loop's delete");
}

// ---------------------------------------------------------------------------
// Postgres-backed store test
//
// Exercises the real classify SQL (the `LEFT JOIN` + `ANY($1)`, the
// `managed_by` handling, the active generation, the tombstone and
// unrecognized-lifecycle rows) against a throwaway Postgres. Skips silently
// when no Docker daemon is reachable.
// ---------------------------------------------------------------------------

mod postgres_backed {
    use pplx_space_common_core::sql;
    use pplx_space_common_core::types::SandboxId;
    use pplx_space_db_test_fixtures::try_pg_test_db;
    use sqlx::PgPool;
    use uuid::Uuid;

    use crate::orphaned_restore_pod_gc::RowLifecycle;
    use crate::orphaned_restore_pod_gc::backend::SandboxStore;
    use crate::orphaned_restore_pod_gc::store::SandboxDbStore;
    use pplx_space_common_core::types::SandboxLifecycle;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

    const SEED_ORG_ID: &str = "00000000-0000-0000-0000-000000000001";
    const SEED_USER_ID: &str = "00000000-0000-0000-0000-000000000010";

    async fn insert_sandbox(
        pool: &PgPool,
        id: Uuid,
        managed_by: &str,
        lifecycle: &str,
    ) -> TestResult {
        sqlx::query(sql!(
            "INSERT INTO sandboxes
                (id, user_id, org_id, managed_by, lifecycle, placement,
                 network_policy_generation)
             VALUES ($1, $2::uuid, $3::uuid, $4, $5, 'on_node', 1)"
        ))
        .bind(id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(managed_by)
        .bind(lifecycle)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn insert_active_execution(
        pool: &PgPool,
        sandbox_id: Uuid,
        generation: i64,
        pod_uid: Option<&str>,
    ) -> TestResult {
        insert_execution(pool, sandbox_id, generation, pod_uid, true).await
    }

    async fn insert_execution(
        pool: &PgPool,
        sandbox_id: Uuid,
        generation: i64,
        pod_uid: Option<&str>,
        active: bool,
    ) -> TestResult {
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active)
             VALUES ($1, $2, $3, 'sandboxes', $4,
                     CASE WHEN $5 THEN 'running' ELSE 'replaced' END, $5)"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .bind(format!("space-sbx-{}-g{generation}", sandbox_id.simple()))
        .bind(pod_uid)
        .bind(active)
        .execute(pool)
        .await?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn classify_sandboxes_reads_lifecycle_and_generation() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let store = SandboxDbStore::new(db.pool.clone());

        let suspended = Uuid::now_v7();
        let started = Uuid::now_v7();
        let non_k8s = Uuid::now_v7();
        let missing = Uuid::now_v7();
        // A soft-delete tombstone (`lifecycle = 'deleted'`) — a value
        // `SandboxLifecycle` has no variant for. Its presence in the batch must
        // NOT abort the query's decode; it is excluded so the caller reaps its
        // pod via the row-absent (`SandboxMissing`) path.
        let deleted = Uuid::now_v7();
        // A node-local lifecycle value with no `SandboxLifecycle` variant. It
        // must come back as `Unrecognized` (skipped), not fail the batch.
        let cleanup_required = Uuid::now_v7();
        insert_sandbox(&db.pool, suspended, "k8s", "suspended").await?;
        insert_sandbox(&db.pool, started, "k8s", "started").await?;
        // A retired (inactive) execution at a higher generation must not be
        // mistaken for the active one: only `e.active` rows join.
        insert_execution(&db.pool, started, 4, Some("uid-retired"), false).await?;
        insert_active_execution(&db.pool, started, 3, Some("uid-live")).await?;
        insert_execution(&db.pool, suspended, 2, Some("uid-old"), false).await?;
        insert_sandbox(&db.pool, non_k8s, "space", "suspended").await?;
        insert_sandbox(&db.pool, deleted, "k8s", "deleted").await?;
        insert_sandbox(&db.pool, cleanup_required, "k8s", "cleanup_required").await?;

        let states = store
            .classify_sandboxes(&[
                SandboxId::from_uuid(suspended),
                SandboxId::from_uuid(started),
                SandboxId::from_uuid(non_k8s),
                SandboxId::from_uuid(missing),
                SandboxId::from_uuid(deleted),
                SandboxId::from_uuid(cleanup_required),
            ])
            .await?;

        // Suspended: present, no active execution → no generation.
        let s = states
            .get(&SandboxId::from_uuid(suspended))
            .expect("suspended sandbox classified");
        assert_eq!(
            s.lifecycle,
            RowLifecycle::Known(SandboxLifecycle::Suspended)
        );
        assert_eq!(s.active_generation, None);
        // Started: present, active execution's generation surfaced.
        let s = states
            .get(&SandboxId::from_uuid(started))
            .expect("started sandbox classified");
        assert_eq!(s.lifecycle, RowLifecycle::Known(SandboxLifecycle::Started));
        assert_eq!(s.active_generation, Some(3));
        // Non-k8s and unrecognized-lifecycle rows are present but
        // `Unrecognized` (skipped at the caller), not absent — an absent row
        // would read as `SandboxMissing` and get its pod reaped.
        for id in [non_k8s, cleanup_required] {
            let s = states
                .get(&SandboxId::from_uuid(id))
                .expect("existing row classified");
            assert_eq!(s.lifecycle, RowLifecycle::Unrecognized);
        }
        // Missing and deleted-tombstone rows are absent (SandboxMissing at the
        // caller).
        assert!(!states.contains_key(&SandboxId::from_uuid(missing)));
        assert!(!states.contains_key(&SandboxId::from_uuid(deleted)));
        Ok(())
    }
}
