//! # Test matrix
//!
//! ## C — `ReconcilerConfig`
//! - C1 `c1_default_is_disabled_dry_run_with_1h_min_age`
//!
//! ## T — Tick behavior (fake presence + fake sink)
//! - T1 `t1_no_candidates_emits_empty_report`
//! - T2 `t2_missing_pod_candidates_retired_live`
//! - T3 `t3_dry_run_counts_but_never_writes`
//! - T4 `t4_present_lingering_unknown_pods_skipped`
//! - T5 `t5_raced_outcome_counted_not_retried`
//! - T6 `t6_list_error_yields_empty_report`
//! - T7 `t7_cursor_advances_and_wraps_across_ticks`
//! - T8 `t8_terminal_succeeded_pod_is_deleted_then_retired_live`
//! - T9 `t9_terminal_deleted_requires_successful_retire`
//! - T10 `t10_terminal_succeeded_pod_is_verified_but_not_deleted_in_dry_run`
//! - T11 `t11_terminal_succeeded_cleanup_skip_blocks_retire`
//! - T12 `t12_terminal_failed_pod_is_audit_only`
//! - T13 `t13_terminal_pod_validation_requires_matching_identity`
//! - T14 `t14_terminal_pod_validation_requires_safe_succeeded_status`
//!
//! ## R — Postgres-backed sink (predicate + transaction semantics)
//! - R1 `r1_lists_only_aged_suspended_with_active_execution`
//! - R2 `r2_retire_clears_execution_and_placement_bookkeeping`
//! - R3 `r3_lifecycle_moved_off_suspended_skips`
//! - R4 `r4_execution_identity_changed_skips`
//! - R5 `r5_null_target_node_backfilled_from_active_node`
//! - R6 `r6_cursor_bounds_respected`
//! - R7 `r7_stale_prior_cycle_artifact_identity_cleared`
//! - R8 `r8_no_ready_storage_snapshot_is_not_listed_and_apply_skips`
//! - R9 `r9_activity_after_snapshot_is_not_listed_and_apply_skips`
//! - R10 `r10_newer_non_suspend_snapshot_blocks_marker_clear`

use pplx_space_common_core::types::{NodeId, SandboxId};

use std::collections::BTreeMap;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use k8s_openapi::api::core::v1::{
    ContainerState, ContainerStateRunning, ContainerStateTerminated, ContainerStatus, Pod,
    PodStatus,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use pplx_space_common_core::types::PositiveDuration;
use uuid::Uuid;

use super::*;
use pplx_space_common_core::kubernetes::{
    SANDBOX_EXECUTION_GENERATION_ANNOTATION, SANDBOX_ID_ANNOTATION, SANDBOX_MANAGED_BY_LABEL,
};

// -----------------------------------------------------------------
// Fakes
// -----------------------------------------------------------------

struct FakePresence {
    /// Status returned per pod name; defaults to `Missing` when absent.
    statuses: StdMutex<std::collections::HashMap<String, PodPresenceStatus>>,
    terminal_cleanup_outcomes:
        StdMutex<std::collections::HashMap<String, TerminalPodCleanupOutcome>>,
    terminal_cleanup_calls: StdMutex<Vec<(StaleExecutionCandidate, TerminalPodAction)>>,
}

impl FakePresence {
    fn all_missing() -> Self {
        Self {
            statuses: StdMutex::new(std::collections::HashMap::new()),
            terminal_cleanup_outcomes: StdMutex::new(std::collections::HashMap::new()),
            terminal_cleanup_calls: StdMutex::new(Vec::new()),
        }
    }

    fn with_status(self, pod_name: &str, status: PodPresenceStatus) -> Self {
        self.statuses
            .lock()
            .unwrap()
            .insert(pod_name.to_string(), status);
        self
    }

    fn with_terminal_cleanup_outcome(
        self,
        pod_name: &str,
        outcome: TerminalPodCleanupOutcome,
    ) -> Self {
        self.terminal_cleanup_outcomes
            .lock()
            .unwrap()
            .insert(pod_name.to_string(), outcome);
        self
    }

    fn terminal_cleanup_calls(&self) -> Vec<(StaleExecutionCandidate, TerminalPodAction)> {
        self.terminal_cleanup_calls.lock().unwrap().clone()
    }
}

impl PodPresence for FakePresence {
    async fn check_pod(&self, pod_name: &str) -> PodPresenceStatus {
        *self
            .statuses
            .lock()
            .unwrap()
            .get(pod_name)
            .unwrap_or(&PodPresenceStatus::Missing)
    }
}

impl TerminalSucceededPodCleaner for FakePresence {
    async fn prepare_terminal_succeeded_pod(
        &self,
        candidate: &StaleExecutionCandidate,
        action: TerminalPodAction,
    ) -> TerminalPodCleanupOutcome {
        self.terminal_cleanup_calls
            .lock()
            .unwrap()
            .push((candidate.clone(), action));
        if let Some(outcome) = self
            .terminal_cleanup_outcomes
            .lock()
            .unwrap()
            .get(&candidate.pod_name)
        {
            *outcome
        } else {
            match action {
                TerminalPodAction::VerifyOnly => TerminalPodCleanupOutcome::ReadyToRetire,
                TerminalPodAction::Delete => TerminalPodCleanupOutcome::DeletedAndReadyToRetire,
            }
        }
    }
}

#[derive(Default)]
struct FakeSinkState {
    /// Candidate pages returned by successive `list_candidates` calls;
    /// `None` entries produce a DB error.
    pages: Vec<Option<Vec<StaleExecutionCandidate>>>,
    /// `(after_sandbox_id, upper_bound)` per `list_candidates` call.
    list_calls: Vec<(Option<SandboxId>, SandboxId)>,
    /// Candidates passed to `retire_stale_execution`, in order.
    retire_calls: Vec<StaleExecutionCandidate>,
    /// Outcome returned for every retire call.
    retire_outcome: Option<RetireOutcome>,
    max_sandbox_id: Option<SandboxId>,
}

struct FakeSink(StdMutex<FakeSinkState>);

impl FakeSink {
    fn new(max_sandbox_id: Option<SandboxId>) -> Self {
        Self(StdMutex::new(FakeSinkState {
            retire_outcome: Some(RetireOutcome::Retired),
            max_sandbox_id,
            ..FakeSinkState::default()
        }))
    }

    fn push_page(&self, page: Option<Vec<StaleExecutionCandidate>>) {
        self.0.lock().unwrap().pages.push(page);
    }

    fn set_retire_outcome(&self, outcome: RetireOutcome) {
        self.0.lock().unwrap().retire_outcome = Some(outcome);
    }

    fn retire_calls(&self) -> Vec<StaleExecutionCandidate> {
        self.0.lock().unwrap().retire_calls.clone()
    }

    fn list_calls(&self) -> Vec<(Option<SandboxId>, SandboxId)> {
        self.0.lock().unwrap().list_calls.clone()
    }
}

impl RetireSink for FakeSink {
    async fn list_candidates(
        &self,
        _batch_size: u32,
        after_sandbox_id: Option<SandboxId>,
        upper_bound: SandboxId,
        _min_age: PositiveDuration,
    ) -> Result<Vec<StaleExecutionCandidate>, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.list_calls.push((after_sandbox_id, upper_bound));
        if state.pages.is_empty() {
            return Ok(Vec::new());
        }
        match state.pages.remove(0) {
            Some(page) => Ok(page),
            None => Err(sqlx::Error::PoolClosed),
        }
    }

    async fn current_max_sandbox_id(&self) -> Result<Option<SandboxId>, sqlx::Error> {
        Ok(self.0.lock().unwrap().max_sandbox_id)
    }

    async fn retire_stale_execution(
        &self,
        candidate: &StaleExecutionCandidate,
    ) -> Result<RetireOutcome, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.retire_calls.push(candidate.clone());
        Ok(state.retire_outcome.expect("outcome configured"))
    }
}

fn test_config(dry_run: bool) -> ReconcilerConfig {
    ReconcilerConfig {
        enabled: true,
        tick: DEFAULT_TICK,
        min_age: PositiveDuration::new(Duration::from_secs(DEFAULT_MIN_AGE_SECS)).unwrap(),
        batch_size: 10,
        dry_run,
    }
}

fn candidate(pod_name: &str) -> StaleExecutionCandidate {
    StaleExecutionCandidate {
        sandbox_id: SandboxId::new(),
        generation: 1,
        pod_name: pod_name.to_string(),
        pod_uid: Some(Uuid::now_v7().to_string()),
        execution_state: "running".to_string(),
        active_node_id: Some(NodeId::new()),
        current_local_artifact_id: None,
    }
}

fn terminal_pod_for(candidate: &StaleExecutionCandidate) -> Pod {
    let mut labels = BTreeMap::new();
    labels.insert(
        SANDBOX_ID_ANNOTATION.to_string(),
        candidate.sandbox_id.to_string(),
    );
    labels.insert(
        SANDBOX_EXECUTION_GENERATION_ANNOTATION.to_string(),
        candidate.generation.to_string(),
    );
    labels.insert(
        SANDBOX_MANAGED_BY_LABEL.to_string(),
        "space-api".to_string(),
    );
    Pod {
        metadata: ObjectMeta {
            name: Some(candidate.pod_name.clone()),
            uid: candidate.pod_uid.clone(),
            labels: Some(labels),
            ..Default::default()
        },
        status: Some(PodStatus {
            phase: Some("Succeeded".to_string()),
            container_statuses: Some(vec![ContainerStatus {
                state: Some(ContainerState {
                    terminated: Some(ContainerStateTerminated::default()),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

// -----------------------------------------------------------------
// C — Config
// -----------------------------------------------------------------

#[test]
fn c1_default_is_disabled_dry_run_with_1h_min_age() {
    let config = ReconcilerConfig::default();
    assert!(!config.enabled, "reaper must default to disabled");
    assert!(config.dry_run, "reaper must default to dry-run");
    assert_eq!(
        config.min_age.get(),
        Duration::from_secs(DEFAULT_MIN_AGE_SECS)
    );
    assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
}

// -----------------------------------------------------------------
// T — Tick behavior
// -----------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn t1_no_candidates_emits_empty_report() {
    let sink = FakeSink::new(Some(SandboxId::new()));
    let mut reconciler = Reconciler::new(test_config(false), FakePresence::all_missing(), sink);
    let report = reconciler.tick().await;
    assert_eq!(report, TickReport::default());
}

#[tokio::test(flavor = "current_thread")]
async fn t2_missing_pod_candidates_retired_live() {
    let sink = FakeSink::new(Some(SandboxId::new()));
    sink.push_page(Some(vec![candidate("pod-a"), candidate("pod-b")]));
    let mut reconciler = Reconciler::new(test_config(false), FakePresence::all_missing(), sink);
    let report = reconciler.tick().await;
    assert_eq!(report.scanned, 2);
    assert_eq!(report.retired, 2);
    assert_eq!(reconciler.sink.retire_calls().len(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn t3_dry_run_counts_but_never_writes() {
    let sink = FakeSink::new(Some(SandboxId::new()));
    sink.push_page(Some(vec![candidate("pod-a")]));
    let mut reconciler = Reconciler::new(test_config(true), FakePresence::all_missing(), sink);
    let report = reconciler.tick().await;
    assert_eq!(report.retired, 1);
    assert!(
        reconciler.sink.retire_calls().is_empty(),
        "dry-run must never call retire_stale_execution"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn t4_present_lingering_unknown_pods_skipped() {
    let sink = FakeSink::new(Some(SandboxId::new()));
    sink.push_page(Some(vec![
        candidate("pod-present"),
        candidate("pod-lingering"),
        candidate("pod-unknown"),
    ]));
    let presence = FakePresence::all_missing()
        .with_status("pod-present", PodPresenceStatus::Present)
        .with_status("pod-lingering", PodPresenceStatus::Unready)
        .with_status("pod-unknown", PodPresenceStatus::Unknown);
    let mut reconciler = Reconciler::new(test_config(false), presence, sink);
    let report = reconciler.tick().await;
    assert_eq!(report.scanned, 3);
    assert_eq!(report.pod_present, 1);
    assert_eq!(report.pod_lingering, 1);
    assert_eq!(report.pod_unknown, 1);
    assert_eq!(report.retired, 0);
    assert!(reconciler.sink.retire_calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn t5_raced_outcome_counted_not_retried() {
    let sink = FakeSink::new(Some(SandboxId::new()));
    sink.push_page(Some(vec![candidate("pod-a")]));
    sink.set_retire_outcome(RetireOutcome::SkippedRaced);
    let mut reconciler = Reconciler::new(test_config(false), FakePresence::all_missing(), sink);
    let report = reconciler.tick().await;
    assert_eq!(report.raced, 1);
    assert_eq!(report.retired, 0);
    assert_eq!(reconciler.sink.retire_calls().len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn t6_list_error_yields_empty_report() {
    let sink = FakeSink::new(Some(SandboxId::new()));
    sink.push_page(None);
    let mut reconciler = Reconciler::new(test_config(false), FakePresence::all_missing(), sink);
    let report = reconciler.tick().await;
    assert_eq!(report, TickReport::default());
}

#[tokio::test(flavor = "current_thread")]
async fn t7_cursor_advances_and_wraps_across_ticks() {
    let upper = SandboxId::new();
    let sink = FakeSink::new(Some(upper));
    let first = candidate("pod-a");
    let first_id = first.sandbox_id;
    // Tick 1: one page. Tick 2: the cursor passes `first_id`, gets an empty
    // page, wraps, and re-lists from the start within the same tick.
    sink.push_page(Some(vec![first]));
    sink.push_page(Some(Vec::new()));
    sink.push_page(Some(Vec::new()));
    let mut reconciler = Reconciler::new(test_config(false), FakePresence::all_missing(), sink);
    let report = reconciler.tick().await;
    assert_eq!(report.retired, 1);
    let _ = reconciler.tick().await;
    let calls = reconciler.sink.list_calls();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[0], (None, upper), "sweep starts unbounded-left");
    assert_eq!(
        calls[1].0,
        Some(first_id),
        "second list resumes after the last candidate"
    );
    assert_eq!(
        calls[2].0, None,
        "after the wrap the next sweep starts from the smallest id again"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn t8_terminal_succeeded_pod_is_deleted_then_retired_live() {
    let sink = FakeSink::new(Some(SandboxId::new()));
    let terminal = candidate("pod-terminal");
    sink.push_page(Some(vec![terminal.clone()]));
    let presence = FakePresence::all_missing()
        .with_status("pod-terminal", PodPresenceStatus::TerminalSucceeded);

    let mut reconciler = Reconciler::new(test_config(false), presence, sink);
    let report = reconciler.tick().await;

    assert_eq!(report.scanned, 1);
    assert_eq!(report.terminal_deleted, 1);
    assert_eq!(report.retired, 1);
    assert_eq!(reconciler.sink.retire_calls(), vec![terminal.clone()]);
    assert_eq!(
        reconciler.presence.terminal_cleanup_calls(),
        vec![(terminal, TerminalPodAction::Delete)]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn t9_terminal_deleted_requires_successful_retire() {
    let sink = FakeSink::new(Some(SandboxId::new()));
    let terminal = candidate("pod-terminal");
    sink.push_page(Some(vec![terminal.clone()]));
    sink.set_retire_outcome(RetireOutcome::SkippedRaced);
    let presence = FakePresence::all_missing()
        .with_status("pod-terminal", PodPresenceStatus::TerminalSucceeded);

    let mut reconciler = Reconciler::new(test_config(false), presence, sink);
    let report = reconciler.tick().await;

    assert_eq!(report.terminal_deleted, 0);
    assert_eq!(report.raced, 1);
    assert_eq!(report.retired, 0);
    assert_eq!(reconciler.sink.retire_calls(), vec![terminal]);
}

#[tokio::test(flavor = "current_thread")]
async fn t10_terminal_succeeded_pod_is_verified_but_not_deleted_in_dry_run() {
    let sink = FakeSink::new(Some(SandboxId::new()));
    let terminal = candidate("pod-terminal");
    sink.push_page(Some(vec![terminal.clone()]));
    let presence = FakePresence::all_missing()
        .with_status("pod-terminal", PodPresenceStatus::TerminalSucceeded);

    let mut reconciler = Reconciler::new(test_config(true), presence, sink);
    let report = reconciler.tick().await;

    assert_eq!(report.terminal_deleted, 0);
    assert_eq!(report.retired, 1);
    assert!(reconciler.sink.retire_calls().is_empty());
    assert_eq!(
        reconciler.presence.terminal_cleanup_calls(),
        vec![(terminal, TerminalPodAction::VerifyOnly)]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn t11_terminal_succeeded_cleanup_skip_blocks_retire() {
    let sink = FakeSink::new(Some(SandboxId::new()));
    sink.push_page(Some(vec![candidate("pod-terminal")]));
    let presence = FakePresence::all_missing()
        .with_status("pod-terminal", PodPresenceStatus::TerminalSucceeded)
        .with_terminal_cleanup_outcome(
            "pod-terminal",
            TerminalPodCleanupOutcome::Skipped(TerminalPodCleanupSkipReason::IdentityMismatch),
        );

    let mut reconciler = Reconciler::new(test_config(false), presence, sink);
    let report = reconciler.tick().await;

    assert_eq!(report.terminal_cleanup_skipped, 1);
    assert_eq!(
        report
            .terminal_cleanup_skip_reasons
            .iter()
            .find(|(reason, _)| *reason == TerminalPodCleanupSkipReason::IdentityMismatch)
            .map(|(_, count)| count),
        Some(1)
    );
    assert_eq!(
        report
            .terminal_cleanup_skip_reasons
            .iter()
            .find(|(reason, _)| *reason == TerminalPodCleanupSkipReason::DeleteFailed)
            .map(|(_, count)| count),
        Some(0)
    );
    assert_eq!(report.retired, 0);
    assert!(reconciler.sink.retire_calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn t12_terminal_failed_pod_is_audit_only() {
    let sink = FakeSink::new(Some(SandboxId::new()));
    sink.push_page(Some(vec![candidate("pod-failed")]));
    let presence =
        FakePresence::all_missing().with_status("pod-failed", PodPresenceStatus::TerminalFailed);

    let mut reconciler = Reconciler::new(test_config(false), presence, sink);
    let report = reconciler.tick().await;

    assert_eq!(report.pod_terminal_failed, 1);
    assert_eq!(report.retired, 0);
    assert!(reconciler.sink.retire_calls().is_empty());
    assert!(reconciler.presence.terminal_cleanup_calls().is_empty());
}

#[test]
fn t13_terminal_pod_validation_requires_matching_identity() {
    let candidate = candidate("pod-terminal");
    let pod = terminal_pod_for(&candidate);
    assert_eq!(
        validate_terminal_succeeded_pod(&SandboxPodView::from(pod.clone()), &candidate),
        Ok(())
    );

    let mut wrong_uid = pod.clone();
    wrong_uid.metadata.uid = Some("other-uid".to_string());
    assert_eq!(
        validate_terminal_succeeded_pod(&SandboxPodView::from(wrong_uid.clone()), &candidate),
        Err(TerminalPodCleanupSkipReason::IdentityMismatch)
    );

    let mut wrong_sandbox = pod.clone();
    wrong_sandbox.metadata.labels.as_mut().unwrap().insert(
        SANDBOX_ID_ANNOTATION.to_string(),
        Uuid::now_v7().to_string(),
    );
    assert_eq!(
        validate_terminal_succeeded_pod(&SandboxPodView::from(wrong_sandbox.clone()), &candidate),
        Err(TerminalPodCleanupSkipReason::IdentityMismatch)
    );

    let mut wrong_generation = pod;
    wrong_generation.metadata.labels.as_mut().unwrap().insert(
        SANDBOX_EXECUTION_GENERATION_ANNOTATION.to_string(),
        "2".to_string(),
    );
    assert_eq!(
        validate_terminal_succeeded_pod(
            &SandboxPodView::from(wrong_generation.clone()),
            &candidate
        ),
        Err(TerminalPodCleanupSkipReason::IdentityMismatch)
    );
}

#[test]
fn t14_terminal_pod_validation_requires_safe_succeeded_status() {
    let candidate = candidate("pod-terminal");
    let mut running = terminal_pod_for(&candidate);
    running.status.as_mut().unwrap().container_statuses = Some(vec![ContainerStatus {
        state: Some(ContainerState {
            running: Some(ContainerStateRunning::default()),
            ..Default::default()
        }),
        ..Default::default()
    }]);
    assert_eq!(
        validate_terminal_succeeded_pod(&SandboxPodView::from(running.clone()), &candidate),
        Err(TerminalPodCleanupSkipReason::NoLongerSucceeded)
    );

    let mut failed = terminal_pod_for(&candidate);
    failed.status.as_mut().unwrap().phase = Some("Failed".to_string());
    assert_eq!(
        validate_terminal_succeeded_pod(&SandboxPodView::from(failed.clone()), &candidate),
        Err(TerminalPodCleanupSkipReason::NoLongerSucceeded)
    );
}

// -----------------------------------------------------------------
// R — Postgres-backed sink
// -----------------------------------------------------------------

mod postgres_backed {

    use super::*;
    use crate::suspended_execution_reaper::store::ExecutionDbStore;
    use pplx_space_common_core::types::{LocalArtifactId, NodeId, SandboxId, SnapshotId};
    use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
    use sqlx::PgPool;

    use pplx_space_test_env::TestResult;

    const HOUR_SECS: i64 = 3_600;

    /// Seed a suspended sandbox with a stale active execution: suspended for
    /// 2h, placed on-node with equal active/target node IDs, and a running
    /// execution created 3h ago with no artifact identity.
    struct StaleSandboxSpec {
        sandbox_id: SandboxId,
        lifecycle: &'static str,
        /// `lifecycle_changed_at = NOW() - this`.
        lifecycle_age_secs: i64,
        active_node_id: Option<NodeId>,
        target_node_id: Option<NodeId>,
        generation: i64,
        execution_state: &'static str,
        execution_active: bool,
        /// `sandbox_k8s_executions.created_at = NOW() - this`.
        execution_age_secs: i64,
        pod_uid: Option<&'static str>,
        pod_ip: Option<&'static str>,
        durable_suspend_snapshot: bool,
        snapshot_storage: bool,
        last_activity_after_snapshot: bool,
        newer_non_suspend_snapshot: bool,
    }

    impl StaleSandboxSpec {
        fn stale(sandbox_id: SandboxId, node_id: NodeId) -> Self {
            Self {
                sandbox_id,
                lifecycle: "suspended",
                lifecycle_age_secs: 2 * HOUR_SECS,
                active_node_id: Some(node_id),
                target_node_id: Some(node_id),
                generation: 1,
                execution_state: "running",
                execution_active: true,
                execution_age_secs: 3 * HOUR_SECS,
                pod_uid: Some("uid-1"),
                pod_ip: Some("10.0.0.7"),
                durable_suspend_snapshot: true,
                snapshot_storage: true,
                last_activity_after_snapshot: false,
                newer_non_suspend_snapshot: false,
            }
        }

        fn pod_name(&self) -> String {
            format!(
                "space-sbx-{}-g{}",
                self.sandbox_id.as_uuid().simple(),
                self.generation
            )
        }
    }

    async fn insert_stale_sandbox(pool: &PgPool, spec: &StaleSandboxSpec) -> TestResult {
        // `network_policy_generation` is explicit because migration 034
        // drops the column DEFAULT after backfill.
        sqlx::query(
            "INSERT INTO sandboxes
                (id, user_id, org_id, managed_by, lifecycle, placement,
                 active_node_id, target_node_id, network_policy_generation)
             VALUES ($1, $2::uuid, $3::uuid, 'k8s', $4, 'on_node', $5, $6, 1)",
        )
        .bind(spec.sandbox_id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(spec.lifecycle)
        .bind(spec.active_node_id)
        .bind(spec.target_node_id)
        .execute(pool)
        .await?;
        // Backdate the suspend. A direct write survives the
        // `sandboxes_touch_lifecycle_changed_at` trigger because the
        // lifecycle column itself does not change in this UPDATE.
        sqlx::query(
            "UPDATE sandboxes
                SET lifecycle_changed_at = NOW() - make_interval(secs => $2::BIGINT)
              WHERE id = $1",
        )
        .bind(spec.sandbox_id)
        .bind(spec.lifecycle_age_secs)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip,
                 state, active, created_at)
             VALUES ($1, $2, $3, 'sandboxes', $4, $5::inet, $6, $7,
                     NOW() - make_interval(secs => $8::BIGINT))",
        )
        .bind(spec.sandbox_id)
        .bind(spec.generation)
        .bind(spec.pod_name())
        .bind(spec.pod_uid)
        .bind(spec.pod_ip)
        .bind(spec.execution_state)
        .bind(spec.execution_active)
        .bind(spec.execution_age_secs)
        .execute(pool)
        .await?;
        if spec.durable_suspend_snapshot {
            insert_snapshot(
                pool,
                spec.sandbox_id,
                "suspend",
                spec.lifecycle_age_secs.saturating_sub(60),
                spec.snapshot_storage,
            )
            .await?;
        }
        if spec.last_activity_after_snapshot {
            sqlx::query(
                "UPDATE sandboxes
                    SET last_command_at = NOW(),
                        last_access_at = NOW()
                  WHERE id = $1",
            )
            .bind(spec.sandbox_id)
            .execute(pool)
            .await?;
        }
        if spec.newer_non_suspend_snapshot {
            insert_snapshot(
                pool,
                spec.sandbox_id,
                "stop",
                spec.lifecycle_age_secs.saturating_sub(90),
                true,
            )
            .await?;
        }
        Ok(())
    }

    async fn insert_snapshot(
        pool: &PgPool,
        sandbox_id: SandboxId,
        artifact_origin: &str,
        snapshot_age_secs: i64,
        storage: bool,
    ) -> TestResult<SnapshotId> {
        let snapshot_id = SnapshotId::new();
        sqlx::query(
            "INSERT INTO snapshots
                (id, sandbox_id, org_id, type, state, timestamp,
                 local_artifact_id, artifact_kind, artifact_origin)
             VALUES ($1, $2, $3::uuid, 'full', 'ready',
                     NOW() - make_interval(secs => $4::BIGINT),
                     $5, 'full', $6)",
        )
        .bind(snapshot_id)
        .bind(sandbox_id)
        .bind(SEED_ORG_ID)
        .bind(snapshot_age_secs)
        .bind(Uuid::now_v7())
        .bind(artifact_origin)
        .execute(pool)
        .await?;
        if storage {
            sqlx::query(
                "INSERT INTO snapshot_storage_locations
                    (snapshot_id, storage_connection_string)
                 VALUES ($1, 's3://bucket/suspended-execution-reaper-test')",
            )
            .bind(snapshot_id)
            .execute(pool)
            .await?;
        }
        Ok(snapshot_id)
    }

    fn candidate_for(spec: &StaleSandboxSpec) -> StaleExecutionCandidate {
        StaleExecutionCandidate {
            sandbox_id: spec.sandbox_id,
            generation: spec.generation,
            pod_name: spec.pod_name(),
            pod_uid: spec.pod_uid.map(str::to_string),
            execution_state: spec.execution_state.to_string(),
            active_node_id: spec.active_node_id,
            current_local_artifact_id: None,
        }
    }

    fn default_min_age() -> PositiveDuration {
        PositiveDuration::new(Duration::from_secs(DEFAULT_MIN_AGE_SECS)).unwrap()
    }

    async fn list_all(store: &ExecutionDbStore) -> TestResult<Vec<StaleExecutionCandidate>> {
        let upper = store
            .current_max_sandbox_id()
            .await?
            .expect("at least one sandbox");
        Ok(store
            .list_candidates(100, None, upper, default_min_age())
            .await?)
    }

    /// Only suspended sandboxes whose suspend AND execution row are both
    /// older than `min_age` and whose execution is still active are listed.
    #[tokio::test(flavor = "current_thread")]
    async fn r1_lists_only_aged_suspended_with_active_execution() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let node_id = NodeId::new();

        let stale = StaleSandboxSpec::stale(SandboxId::new(), node_id);
        insert_stale_sandbox(&db.pool, &stale).await?;

        // Suspended only 10 minutes ago — inside the convergence window.
        insert_stale_sandbox(
            &db.pool,
            &StaleSandboxSpec {
                lifecycle_age_secs: 600,
                ..StaleSandboxSpec::stale(SandboxId::new(), node_id)
            },
        )
        .await?;

        // Execution row only 10 minutes old — a fresh claim, possibly with
        // pod creation still in flight.
        insert_stale_sandbox(
            &db.pool,
            &StaleSandboxSpec {
                execution_age_secs: 600,
                ..StaleSandboxSpec::stale(SandboxId::new(), node_id)
            },
        )
        .await?;

        // Live lifecycle — not the reaper's business.
        insert_stale_sandbox(
            &db.pool,
            &StaleSandboxSpec {
                lifecycle: "started",
                ..StaleSandboxSpec::stale(SandboxId::new(), node_id)
            },
        )
        .await?;

        // Already retired execution — converged.
        insert_stale_sandbox(
            &db.pool,
            &StaleSandboxSpec {
                execution_active: false,
                execution_state: "suspended",
                ..StaleSandboxSpec::stale(SandboxId::new(), node_id)
            },
        )
        .await?;

        let store = ExecutionDbStore::new(db.pool.clone());
        let candidates = list_all(&store).await?;
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0], candidate_for(&stale));
        Ok(())
    }

    /// The retire transaction mirrors `finish_suspended_execution` minus the
    /// artifact identity: execution retired, port exposure observations
    /// invalidated, sandbox unplaced with the wake-affinity pin preserved —
    /// and `placement_changed_at` untouched.
    #[tokio::test(flavor = "current_thread")]
    async fn r2_retire_clears_execution_and_placement_bookkeeping() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let node_id = NodeId::new();
        let spec = StaleSandboxSpec::stale(SandboxId::new(), node_id);
        insert_stale_sandbox(&db.pool, &spec).await?;
        sqlx::query(
            "UPDATE sandboxes
                SET placement_changed_at = NOW() - make_interval(secs => $2::BIGINT)
              WHERE id = $1",
        )
        .bind(spec.sandbox_id)
        .bind(4 * HOUR_SECS)
        .execute(&db.pool)
        .await?;
        sqlx::query(
            "INSERT INTO sandbox_port_exposures
                (sandbox_id, protocol, guest_port, visibility, source,
                 desired_state, apply_state, last_applied_node_id)
             VALUES ($1, 'tcp', 8080, 'public', 'user', 'open', 'applied', $2)",
        )
        .bind(spec.sandbox_id)
        .bind(node_id)
        .execute(&db.pool)
        .await?;
        let store = ExecutionDbStore::new(db.pool.clone());
        let outcome = store.retire_stale_execution(&candidate_for(&spec)).await?;
        assert_eq!(outcome, RetireOutcome::Retired);

        let (active, state, pod_ip, terminated): (bool, String, Option<String>, bool) =
            sqlx::query_as(
                "SELECT active, state, host(pod_ip), terminated_at IS NOT NULL
                   FROM sandbox_k8s_executions
                  WHERE sandbox_id = $1 AND generation = $2",
            )
            .bind(spec.sandbox_id)
            .bind(spec.generation)
            .fetch_one(&db.pool)
            .await?;
        assert!(!active, "execution must be retired");
        assert_eq!(state, "suspended");
        assert_eq!(pod_ip, None);
        assert!(terminated, "terminated_at must be stamped");

        let (placement, active_node, target_node, placement_age_secs): (
            String,
            Option<NodeId>,
            Option<NodeId>,
            f64,
        ) = sqlx::query_as(
            "SELECT placement, active_node_id, target_node_id,
                    EXTRACT(EPOCH FROM (NOW() - placement_changed_at))::FLOAT8
               FROM sandboxes
              WHERE id = $1",
        )
        .bind(spec.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(placement, "unplaced");
        assert_eq!(active_node, None, "stale active_node_id must be cleared");
        assert_eq!(
            target_node,
            Some(node_id),
            "wake-affinity pin must survive the sweep"
        );
        assert!(
            placement_age_secs > 3.0 * HOUR_SECS as f64,
            "placement_changed_at must NOT be bumped (snapshot freshness anchor), \
             but it is only {placement_age_secs}s old"
        );

        let (apply_state, last_applied_node): (String, Option<NodeId>) = sqlx::query_as(
            "SELECT apply_state, last_applied_node_id
               FROM sandbox_port_exposures
              WHERE sandbox_id = $1",
        )
        .bind(spec.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(apply_state, "pending");
        assert_eq!(last_applied_node, None);

        Ok(())
    }

    /// A lifecycle that moved off `suspended` between scan and retire (a
    /// wake claimed the sandbox) aborts the transaction untouched.
    #[tokio::test(flavor = "current_thread")]
    async fn r3_lifecycle_moved_off_suspended_skips() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let node_id = NodeId::new();
        let spec = StaleSandboxSpec::stale(SandboxId::new(), node_id);
        insert_stale_sandbox(&db.pool, &spec).await?;
        let observed = candidate_for(&spec);
        // A wake claims the row after the scan.
        sqlx::query("UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1")
            .bind(spec.sandbox_id)
            .execute(&db.pool)
            .await?;

        let store = ExecutionDbStore::new(db.pool.clone());
        let outcome = store.retire_stale_execution(&observed).await?;
        assert_eq!(outcome, RetireOutcome::SkippedRaced);

        let (active, active_node): (bool, Option<NodeId>) = sqlx::query_as(
            "SELECT e.active, s.active_node_id
               FROM sandbox_k8s_executions e
               JOIN sandboxes s ON s.id = e.sandbox_id
              WHERE e.sandbox_id = $1 AND e.generation = $2",
        )
        .bind(spec.sandbox_id)
        .bind(spec.generation)
        .fetch_one(&db.pool)
        .await?;
        assert!(active, "execution must be left to the wake's owner");
        assert_eq!(active_node, Some(node_id));
        Ok(())
    }

    /// An execution whose identity changed since the scan (a replacement
    /// bumped the generation / a recreation rewrote the pod uid) is not the
    /// row we verified — skip.
    #[tokio::test(flavor = "current_thread")]
    async fn r4_execution_identity_changed_skips() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let node_id = NodeId::new();
        let spec = StaleSandboxSpec::stale(SandboxId::new(), node_id);
        insert_stale_sandbox(&db.pool, &spec).await?;
        let observed = candidate_for(&spec);
        // Same-generation pod recreation persisted a fresh uid after our
        // scan+pod-check observed the old one.
        sqlx::query(
            "UPDATE sandbox_k8s_executions
                SET pod_uid = 'uid-2'
              WHERE sandbox_id = $1 AND generation = $2",
        )
        .bind(spec.sandbox_id)
        .bind(spec.generation)
        .execute(&db.pool)
        .await?;

        let store = ExecutionDbStore::new(db.pool.clone());
        let outcome = store.retire_stale_execution(&observed).await?;
        assert_eq!(outcome, RetireOutcome::SkippedRaced);

        let active: bool = sqlx::query_scalar(
            "SELECT active FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = $2",
        )
        .bind(spec.sandbox_id)
        .bind(spec.generation)
        .fetch_one(&db.pool)
        .await?;
        assert!(active);
        Ok(())
    }

    /// The rare candidate with `target_node_id IS NULL` gets the pin
    /// backfilled from `active_node_id` so the sweep never erases the only
    /// pointer to the node holding local state.
    #[tokio::test(flavor = "current_thread")]
    async fn r5_null_target_node_backfilled_from_active_node() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let node_id = NodeId::new();
        let spec = StaleSandboxSpec {
            target_node_id: None,
            ..StaleSandboxSpec::stale(SandboxId::new(), node_id)
        };
        insert_stale_sandbox(&db.pool, &spec).await?;

        let store = ExecutionDbStore::new(db.pool.clone());
        let outcome = store.retire_stale_execution(&candidate_for(&spec)).await?;
        assert_eq!(outcome, RetireOutcome::Retired);

        let (active_node, target_node): (Option<NodeId>, Option<NodeId>) =
            sqlx::query_as("SELECT active_node_id, target_node_id FROM sandboxes WHERE id = $1")
                .bind(spec.sandbox_id)
                .fetch_one(&db.pool)
                .await?;
        assert_eq!(active_node, None);
        assert_eq!(target_node, Some(node_id));
        Ok(())
    }

    /// A candidate that still carries `current_local_artifact_*` got it from
    /// a PRIOR suspend cycle (the current suspend never ran
    /// `finish_suspended_execution` — that's the defining property of the
    /// candidate set; wake does not clear the identity). Leaving that stale
    /// identity in place while clearing `active_node_id` would satisfy the
    /// exact-artifact portability branch (`current_local_artifact_id IS NOT
    /// NULL AND active_node_id IS NULL` reads as "suspend bookkeeping
    /// completed for this artifact") and a later restore/drain release would
    /// trust prior-cycle bytes. The retire transaction must clear all three
    /// identity columns.
    #[tokio::test(flavor = "current_thread")]
    async fn r7_stale_prior_cycle_artifact_identity_cleared() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let node_id = NodeId::new();
        let spec = StaleSandboxSpec::stale(SandboxId::new(), node_id);
        insert_stale_sandbox(&db.pool, &spec).await?;
        // Prior API-driven suspend left an artifact identity; the wake that
        // followed only re-placed the sandbox; the node-side auto-suspend
        // that produced this candidate never published a new one.
        let prior_artifact_id = Uuid::now_v7();
        sqlx::query(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1",
        )
        .bind(spec.sandbox_id)
        .bind(prior_artifact_id)
        .execute(&db.pool)
        .await?;

        let store = ExecutionDbStore::new(db.pool.clone());
        // The scan surfaces the stale identity so the event records what
        // got cleared.
        let candidates = list_all(&store).await?;
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].current_local_artifact_id,
            Some(LocalArtifactId::from_uuid(prior_artifact_id))
        );

        let outcome = store.retire_stale_execution(&candidates[0]).await?;
        assert_eq!(outcome, RetireOutcome::Retired);

        let (artifact_id, artifact_kind, artifact_origin): (
            Option<Uuid>,
            Option<String>,
            Option<String>,
        ) = sqlx::query_as(
            "SELECT current_local_artifact_id,
                    current_local_artifact_kind,
                    current_local_artifact_origin
               FROM sandboxes
              WHERE id = $1",
        )
        .bind(spec.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(
            artifact_id, None,
            "stale prior-cycle artifact identity must be cleared with active_node_id, \
             or the exact-artifact portability branch reads prior-cycle bytes as current"
        );
        assert_eq!(artifact_kind, None);
        assert_eq!(artifact_origin, None);
        Ok(())
    }

    /// The cursor bounds — `id > after`, `id <= upper_bound`, ORDER BY id —
    /// page deterministically through the candidate set.
    #[tokio::test(flavor = "current_thread")]
    async fn r6_cursor_bounds_respected() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let node_id = NodeId::new();
        // v7 ids are time-ordered, so generation order == id order.
        let mut ids = Vec::new();
        for _ in 0..3 {
            let spec = StaleSandboxSpec::stale(SandboxId::new(), node_id);
            insert_stale_sandbox(&db.pool, &spec).await?;
            ids.push(spec.sandbox_id);
        }

        let store = ExecutionDbStore::new(db.pool.clone());
        let upper = store
            .current_max_sandbox_id()
            .await?
            .expect("sandboxes inserted");
        assert_eq!(upper, ids[2]);

        let page = store
            .list_candidates(2, None, upper, default_min_age())
            .await?;
        assert_eq!(
            page.iter().map(|c| c.sandbox_id).collect::<Vec<_>>(),
            vec![ids[0], ids[1]]
        );

        let page = store
            .list_candidates(2, Some(ids[1]), upper, default_min_age())
            .await?;
        assert_eq!(
            page.iter().map(|c| c.sandbox_id).collect::<Vec<_>>(),
            vec![ids[2]]
        );

        // An upper bound below the last id excludes it.
        let page = store
            .list_candidates(10, None, ids[1], default_min_age())
            .await?;
        assert_eq!(
            page.iter().map(|c| c.sandbox_id).collect::<Vec<_>>(),
            vec![ids[0], ids[1]]
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r8_no_ready_storage_snapshot_is_not_listed_and_apply_skips() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let node_id = NodeId::new();
        let spec = StaleSandboxSpec {
            snapshot_storage: false,
            ..StaleSandboxSpec::stale(SandboxId::new(), node_id)
        };
        insert_stale_sandbox(&db.pool, &spec).await?;

        let store = ExecutionDbStore::new(db.pool.clone());
        assert!(
            list_all(&store).await?.is_empty(),
            "candidate scan must require durable snapshot storage"
        );
        let outcome = store.retire_stale_execution(&candidate_for(&spec)).await?;
        assert_eq!(outcome, RetireOutcome::SkippedSnapshotNotCurrent);

        let active: bool = sqlx::query_scalar(
            "SELECT active
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = $2",
        )
        .bind(spec.sandbox_id)
        .bind(spec.generation)
        .fetch_one(&db.pool)
        .await?;
        assert!(
            active,
            "artifact guard failure must roll back execution retire"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r9_activity_after_snapshot_is_not_listed_and_apply_skips() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let node_id = NodeId::new();
        let spec = StaleSandboxSpec {
            last_activity_after_snapshot: true,
            ..StaleSandboxSpec::stale(SandboxId::new(), node_id)
        };
        insert_stale_sandbox(&db.pool, &spec).await?;

        let store = ExecutionDbStore::new(db.pool.clone());
        assert!(
            list_all(&store).await?.is_empty(),
            "candidate scan must reject snapshots older than activity evidence"
        );
        let outcome = store.retire_stale_execution(&candidate_for(&spec)).await?;
        assert_eq!(outcome, RetireOutcome::SkippedSnapshotNotCurrent);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r10_newer_non_suspend_snapshot_blocks_marker_clear() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let node_id = NodeId::new();
        let spec = StaleSandboxSpec {
            newer_non_suspend_snapshot: true,
            ..StaleSandboxSpec::stale(SandboxId::new(), node_id)
        };
        insert_stale_sandbox(&db.pool, &spec).await?;
        let marker_id = Uuid::now_v7();
        sqlx::query(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1",
        )
        .bind(spec.sandbox_id)
        .bind(marker_id)
        .execute(&db.pool)
        .await?;

        let store = ExecutionDbStore::new(db.pool.clone());
        assert!(
            list_all(&store).await?.is_empty(),
            "clearing a marker is unsafe when markerless restore would pick a newer non-suspend full snapshot"
        );
        let outcome = store.retire_stale_execution(&candidate_for(&spec)).await?;
        assert_eq!(outcome, RetireOutcome::SkippedSnapshotNotCurrent);
        let artifact_id: Option<Uuid> =
            sqlx::query_scalar("SELECT current_local_artifact_id FROM sandboxes WHERE id = $1")
                .bind(spec.sandbox_id)
                .fetch_one(&db.pool)
                .await?;
        assert_eq!(
            artifact_id,
            Some(marker_id),
            "unsafe repair skip must leave the existing marker untouched"
        );
        Ok(())
    }
}
