//! # Test matrix
//!
//! ## C - `ReconcilerConfig`
//! - C1 `c1_default_is_enabled`
//!
//! ## T - Tick behavior
//! - T3 `t3_live_repair_applies_candidates`
//! - T4 `t4_present_lingering_unknown_pods_are_not_repaired`
//! - T5 `t5_raced_outcome_counted`
//! - T6 `t6_apply_error_counted`
//! - T7 `t7_marker_cleared_outcome_routes_to_newest_snapshot_classification`
//! - T7b `t7b_marker_current_outcome_not_counted_as_newest`
//! - T8 `t8_repair_classification_maps_marker_branches`
//! - T9 `t9_repair_event_strings_distinguish_marker_stale_branch`
//!
//! ## R - Postgres-backed sink
//! - R1 `r1_lists_only_started_rows_with_current_suspend_snapshot_no_active_and_no_pod_ip`
//! - R2 `r2_repair_flips_lifecycle_and_clears_live_placement`
//! - R3 `r3_active_execution_is_not_listed`
//! - R5 `r5_execution_pod_ip_is_not_listed`
//! - R4 `r4_lifecycle_race_skips`
//! - R7 `r7_markerless_stale_snapshot_is_not_listed`
//! - R8 `r8_marker_stale_with_newer_snapshot_is_listed_and_clears_marker`
//! - R9 `r9_marker_stale_with_writes_after_newest_snapshot_is_not_listed`
//! - R10 `r10_newer_non_suspend_snapshot_blocks_marker_clear`
//! - R11 `r11_exact_current_marker_snapshot_can_precede_lifecycle_changed_at`
//! - R12 `r12_exact_current_marker_with_later_activity_is_not_listed`
//! - R13 `r13_direct_rows_are_not_bounded_listed_or_repaired`
//!
//! - E3 `e3_execution_generation_race_skips`
//! - E4 `e4_execution_lifecycle_race_skips`
//! - E5 `e5_execution_marker_stale_clears_marker`
//! - E6 `e6_uncommitted_wake_generation_race_skips`
//! - E7 `e7_scan_selects_newest_execution_generation`

use pplx_space_common_core::types::LocalArtifactId;
use pplx_space_common_core::types::{NodeId, SandboxId, SnapshotId};

use std::sync::Mutex as StdMutex;
use std::time::Duration;

use pplx_space_common_core::types::PositiveDuration;
use uuid::Uuid;

use super::*;

type RepairedSandboxRow = (
    String,
    String,
    Option<NodeId>,
    Option<NodeId>,
    Option<time::OffsetDateTime>,
    bool,
    Option<LocalArtifactId>,
);

struct FakePresence {
    statuses: StdMutex<std::collections::HashMap<String, PodPresenceStatus>>,
    calls: StdMutex<Vec<String>>,
}

impl FakePresence {
    fn all_missing() -> Self {
        Self {
            statuses: StdMutex::new(std::collections::HashMap::new()),
            calls: StdMutex::new(Vec::new()),
        }
    }

    fn with_status(self, pod_name: &str, status: PodPresenceStatus) -> Self {
        self.statuses
            .lock()
            .unwrap()
            .insert(pod_name.to_string(), status);
        self
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl PodPresence for FakePresence {
    async fn check_pod(&self, pod_name: &str) -> PodPresenceStatus {
        self.calls.lock().unwrap().push(pod_name.to_string());
        *self
            .statuses
            .lock()
            .unwrap()
            .get(pod_name)
            .unwrap_or(&PodPresenceStatus::Missing)
    }
}

#[derive(Default)]
struct FakeSinkState {
    pages: Vec<Option<Vec<RepairCandidate>>>,
    repair_calls: Vec<RepairCandidate>,
    apply_outcome: Option<ApplyOutcome>,
    max_sandbox_id: Option<SandboxId>,
}

struct FakeSink(StdMutex<FakeSinkState>);

impl FakeSink {
    fn new(max_sandbox_id: Option<SandboxId>) -> Self {
        Self(StdMutex::new(FakeSinkState {
            apply_outcome: Some(ApplyOutcome::RevertedToSuspended),
            max_sandbox_id,
            ..FakeSinkState::default()
        }))
    }

    fn push_page(&self, page: Option<Vec<RepairCandidate>>) {
        self.0.lock().unwrap().pages.push(page);
    }

    fn set_apply_outcome(&self, outcome: ApplyOutcome) {
        self.0.lock().unwrap().apply_outcome = Some(outcome);
    }

    fn set_apply_error(&self) {
        self.0.lock().unwrap().apply_outcome = None;
    }

    fn repair_calls(&self) -> Vec<RepairCandidate> {
        self.0.lock().unwrap().repair_calls.clone()
    }
}

impl RepairSink for FakeSink {
    async fn list_candidates(
        &self,
        _batch_size: u32,
        _after_sandbox_id: Option<SandboxId>,
        _upper_bound: SandboxId,
        _min_age: PositiveDuration,
    ) -> Result<Vec<RepairCandidate>, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
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

    async fn revert_started_to_suspended(
        &self,
        candidate: &RepairCandidate,
        _min_age: PositiveDuration,
    ) -> Result<ApplyOutcome, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.repair_calls.push(candidate.clone());
        match state.apply_outcome {
            Some(outcome) => Ok(outcome),
            None => Err(sqlx::Error::PoolClosed),
        }
    }
}

fn candidate() -> RepairCandidate {
    RepairCandidate {
        sandbox_id: SandboxId::new(),
        newest_execution: None,
        current_local_artifact_id: Some(LocalArtifactId::new()),
        clear_marker: false,
    }
}

fn tracked_candidate(pod_name: &str) -> RepairCandidate {
    RepairCandidate {
        sandbox_id: SandboxId::new(),
        newest_execution: Some(NewestExecutionRow {
            pod_name: pod_name.to_string(),
            generation: 1,
        }),
        current_local_artifact_id: Some(LocalArtifactId::new()),
        clear_marker: false,
    }
}

fn marker_stale_candidate() -> RepairCandidate {
    RepairCandidate {
        sandbox_id: SandboxId::new(),
        newest_execution: None,
        current_local_artifact_id: Some(LocalArtifactId::new()),
        clear_marker: true,
    }
}

#[test]
fn c1_default_is_enabled() {
    let config = ReconcilerConfig::default();
    assert!(config.enabled);
}

/// The repair writes, so only an unset or explicitly truthy switch leaves it
/// running; a misspelled one must stop it.
#[test]
fn c2_malformed_kill_switch_disables_the_repair() {
    let env = pplx_space_test_env::TestEnv::default();
    assert!(ReconcilerConfig::from_environment(&env).enabled);
    for value in ["true", "1"] {
        let mut env = pplx_space_test_env::TestEnv::default();
        env.set("SPACE_STARTED_SANDBOX_REPAIR_ENABLED", value);
        assert!(ReconcilerConfig::from_environment(&env).enabled, "{value}");
    }
    for value in ["false", "0", "off", " false  ", "fasle", ""] {
        let mut env = pplx_space_test_env::TestEnv::default();
        env.set("SPACE_STARTED_SANDBOX_REPAIR_ENABLED", value);
        assert!(
            !ReconcilerConfig::from_environment(&env).enabled,
            "{value:?}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn t3_live_repair_applies_candidates() {
    let sink = FakeSink::new(Some(SandboxId::new()));
    sink.push_page(Some(vec![candidate(), tracked_candidate("pod-missing")]));
    let mut reconciler = Reconciler::new(FakePresence::all_missing(), sink);
    let report = reconciler.tick().await;
    assert_eq!(report.scanned, 2);
    assert_eq!(report.reverted, 2);
    assert_eq!(reconciler.sink.repair_calls().len(), 2);
    assert_eq!(reconciler.presence.calls(), vec!["pod-missing"]);
}

#[tokio::test(flavor = "current_thread")]
async fn t4_present_lingering_unknown_pods_are_not_repaired() {
    let sink = FakeSink::new(Some(SandboxId::new()));
    sink.push_page(Some(vec![
        tracked_candidate("pod-present"),
        tracked_candidate("pod-lingering"),
        tracked_candidate("pod-unknown"),
    ]));
    let presence = FakePresence::all_missing()
        .with_status("pod-present", PodPresenceStatus::Present)
        .with_status("pod-lingering", PodPresenceStatus::Unready)
        .with_status("pod-unknown", PodPresenceStatus::Unknown);
    let mut reconciler = Reconciler::new(presence, sink);
    let report = reconciler.tick().await;
    assert_eq!(report.pod_present, 1);
    assert_eq!(report.pod_lingering, 1);
    assert_eq!(report.pod_unknown, 1);
    assert_eq!(report.reverted, 0);
    assert!(reconciler.sink.repair_calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn t5_raced_outcome_counted() {
    let sink = FakeSink::new(Some(SandboxId::new()));
    sink.push_page(Some(vec![candidate()]));
    sink.set_apply_outcome(ApplyOutcome::SkippedRaced);
    let mut reconciler = Reconciler::new(FakePresence::all_missing(), sink);
    let report = reconciler.tick().await;
    assert_eq!(report.raced, 1);
    assert_eq!(report.reverted, 0);
    assert_eq!(reconciler.sink.repair_calls().len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn t7_marker_cleared_outcome_routes_to_newest_snapshot_classification() {
    // Live path: tick() must classify on the apply's ACTUAL outcome. A sink that
    // reports a cleared marker must be counted in `reverted_via_newest`.
    let sink = FakeSink::new(Some(SandboxId::new()));
    sink.push_page(Some(vec![marker_stale_candidate()]));
    sink.set_apply_outcome(ApplyOutcome::RevertedToSuspendedMarkerCleared);
    let mut reconciler = Reconciler::new(FakePresence::all_missing(), sink);
    let report = reconciler.tick().await;
    assert_eq!(report.reverted, 1);
    assert_eq!(
        report.reverted_via_newest, 1,
        "a marker-cleared apply outcome must route to the newest-snapshot branch"
    );
    assert_eq!(reconciler.sink.repair_calls().len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn t7b_marker_current_outcome_not_counted_as_newest() {
    // A plain reverted (marker-current) outcome must NOT be counted as newest.
    let sink = FakeSink::new(Some(SandboxId::new()));
    sink.push_page(Some(vec![candidate()]));
    sink.set_apply_outcome(ApplyOutcome::RevertedToSuspended);
    let mut reconciler = Reconciler::new(FakePresence::all_missing(), sink);
    let report = reconciler.tick().await;
    assert_eq!(report.reverted, 1);
    assert_eq!(report.reverted_via_newest, 0);
}

#[test]
fn t8_repair_classification_maps_marker_branches() {
    let cases = [
        (
            false,
            REPAIR_CONDITION_SET,
            RepairOutcome::RevertedToDurableSnapshot,
        ),
        (
            true,
            REPAIR_CONDITION_SET_NEWEST_SNAPSHOT,
            RepairOutcome::RevertedToNewestSnapshotMarkerCleared,
        ),
    ];
    for (clear_marker, expected_condition_set, expected_outcome) in cases {
        let (condition_set, outcome) = repair_classification(clear_marker);
        assert_eq!(condition_set, expected_condition_set);
        assert_eq!(outcome, expected_outcome);
    }
}

#[test]
fn t9_repair_event_strings_distinguish_marker_stale_branch() {
    assert_eq!(
        repair_event_strings(true),
        (
            "newest_suspend_snapshot_current_without_marker",
            "newest_suspend_snapshot",
            "reverted_to_suspended_marker_cleared",
        )
    );
    assert_eq!(
        repair_event_strings(false),
        (
            "current_suspend_snapshot_without_live_k8s_execution",
            "current_suspend_snapshot",
            "reverted_to_suspended",
        )
    );
}

#[tokio::test(flavor = "current_thread")]
async fn t6_apply_error_counted() {
    let sink = FakeSink::new(Some(SandboxId::new()));
    sink.push_page(Some(vec![candidate()]));
    sink.set_apply_error();
    let mut reconciler = Reconciler::new(FakePresence::all_missing(), sink);
    let report = reconciler.tick().await;
    assert_eq!(report.errors, 1);
    assert_eq!(report.reverted, 0);
    assert_eq!(reconciler.sink.repair_calls().len(), 1);
}

mod postgres_backed {

    use super::*;
    use crate::started_sandbox_repair::store::RepairDbStore;
    use pplx_space_common_core::types::{LocalArtifactId, NodeId, SandboxId};
    use pplx_space_db_test_fixtures::{
        SEED_ORG_ID, SEED_USER_ID, try_pg_test_db, try_pg_test_db_with_max_connections,
    };
    use sqlx::PgPool;

    use pplx_space_test_env::TestResult;

    const MIN_AGE_SECS: i64 = DEFAULT_MIN_AGE_SECS as i64;

    struct StartedSandboxSpec {
        sandbox_id: SandboxId,
        lifecycle: &'static str,
        lifecycle_age_secs: i64,
        execution_active: bool,
        execution_pod_ip: Option<&'static str>,
        current_snapshot: bool,
        stale_snapshot: bool,
        current_marker: bool,
        /// Insert an additional ready suspend snapshot with a *different*
        /// artifact id, taken after `lifecycle_changed_at`. Models the marker-
        /// stale shape: the marker points at an old snapshot while a newer
        /// durable snapshot from this cycle exists.
        newer_distinct_snapshot: bool,
        /// Set `last_command_at`/`last_access_at` to NOW(), i.e. after the newer
        /// snapshot — making the newest snapshot stale-vs-activity (unsafe).
        last_command_after_newest: bool,
        /// Insert an even-newer ready full snapshot with a NON-suspend origin
        /// (`stop`). The markerless restore selector picks the newest full
        /// snapshot regardless of origin, so this is the one restore would load
        /// — recovery must refuse rather than vouch for the suspend snapshot.
        newer_nonsuspend_snapshot: bool,
    }

    impl StartedSandboxSpec {
        fn repairable(sandbox_id: SandboxId) -> Self {
            Self {
                sandbox_id,
                lifecycle: "started",
                lifecycle_age_secs: MIN_AGE_SECS + 300,
                execution_active: false,
                execution_pod_ip: None,
                current_snapshot: true,
                stale_snapshot: false,
                current_marker: true,
                newer_distinct_snapshot: false,
                last_command_after_newest: false,
                newer_nonsuspend_snapshot: false,
            }
        }

        fn pod_name(&self) -> String {
            format!("space-sbx-{}-g1", self.sandbox_id.as_uuid().simple())
        }
    }

    async fn insert_started_sandbox(pool: &PgPool, spec: &StartedSandboxSpec) -> TestResult {
        let artifact_id = Uuid::now_v7();
        let node_id = NodeId::new();
        sqlx::query(
            "INSERT INTO sandboxes
                (id, user_id, org_id, managed_by, lifecycle, placement,
                 active_node_id, target_node_id, network_policy_generation,
                 command_ready_pending, current_local_artifact_id,
                 current_local_artifact_kind, current_local_artifact_origin)
             VALUES ($1, $2::uuid, $3::uuid, 'k8s', $4, 'on_node',
                     $5, $5, 1, TRUE, $6, 'full', 'suspend')",
        )
        .bind(spec.sandbox_id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(spec.lifecycle)
        .bind(node_id)
        .bind(artifact_id)
        .execute(pool)
        .await?;
        if !spec.current_marker {
            sqlx::query(
                "UPDATE sandboxes
                    SET current_local_artifact_id = NULL,
                        current_local_artifact_kind = NULL,
                        current_local_artifact_origin = NULL
                  WHERE id = $1",
            )
            .bind(spec.sandbox_id)
            .execute(pool)
            .await?;
        }
        sqlx::query(
            "UPDATE sandboxes
                SET lifecycle_changed_at = NOW() - make_interval(secs => $2::BIGINT),
                    placement_changed_at = NOW() - make_interval(secs => $2::BIGINT)
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
             VALUES ($1, 1, $2, 'sandboxes', 'uid-1', $3::inet,
                     'forgotten', $4, NOW() - make_interval(secs => $5::BIGINT))",
        )
        .bind(spec.sandbox_id)
        .bind(spec.pod_name())
        .bind(spec.execution_pod_ip)
        .bind(spec.execution_active)
        .bind(MIN_AGE_SECS + 300)
        .execute(pool)
        .await?;

        if spec.current_snapshot || spec.stale_snapshot {
            let snapshot_id = SnapshotId::new();
            let snapshot_age = if spec.stale_snapshot {
                spec.lifecycle_age_secs + 60
            } else {
                spec.lifecycle_age_secs.saturating_sub(60)
            };
            sqlx::query(
                "INSERT INTO snapshots
                    (id, sandbox_id, org_id, type, state, timestamp,
                     local_artifact_id, artifact_kind, artifact_origin)
                 VALUES ($1, $2, $3::uuid, 'full', 'ready',
                         NOW() - make_interval(secs => $4::BIGINT),
                         $5, 'full', 'suspend')",
            )
            .bind(snapshot_id)
            .bind(spec.sandbox_id)
            .bind(SEED_ORG_ID)
            .bind(snapshot_age)
            .bind(artifact_id)
            .execute(pool)
            .await?;
            sqlx::query(
                "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
                 VALUES ($1, 's3://bucket/snapshot')",
            )
            .bind(snapshot_id)
            .execute(pool)
            .await?;
        }

        if spec.newer_distinct_snapshot {
            // Distinct artifact id (not the marker), taken after lifecycle_changed_at.
            let newer_snapshot_id = SnapshotId::new();
            let newer_artifact_id = Uuid::now_v7();
            sqlx::query(
                "INSERT INTO snapshots
                    (id, sandbox_id, org_id, type, state, timestamp,
                     local_artifact_id, artifact_kind, artifact_origin)
                 VALUES ($1, $2, $3::uuid, 'full', 'ready',
                         NOW() - make_interval(secs => $4::BIGINT),
                         $5, 'full', 'suspend')",
            )
            .bind(newer_snapshot_id)
            .bind(spec.sandbox_id)
            .bind(SEED_ORG_ID)
            .bind(spec.lifecycle_age_secs.saturating_sub(60))
            .bind(newer_artifact_id)
            .execute(pool)
            .await?;
            sqlx::query(
                "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
                 VALUES ($1, 's3://bucket/newer')",
            )
            .bind(newer_snapshot_id)
            .execute(pool)
            .await?;
        }

        if spec.last_command_after_newest {
            sqlx::query(
                "UPDATE sandboxes SET last_command_at = NOW(), last_access_at = NOW() WHERE id = $1",
            )
            .bind(spec.sandbox_id)
            .execute(pool)
            .await?;
        }

        if spec.newer_nonsuspend_snapshot {
            // Even newer than newer_distinct_snapshot (-60s), and NON-suspend origin.
            let nonsuspend_id = Uuid::now_v7();
            let nonsuspend_artifact = Uuid::now_v7();
            sqlx::query(
                "INSERT INTO snapshots
                    (id, sandbox_id, org_id, type, state, timestamp,
                     local_artifact_id, artifact_kind, artifact_origin)
                 VALUES ($1, $2, $3::uuid, 'full', 'ready',
                         NOW() - make_interval(secs => $4::BIGINT),
                         $5, 'full', 'stop')",
            )
            .bind(nonsuspend_id)
            .bind(spec.sandbox_id)
            .bind(SEED_ORG_ID)
            .bind(spec.lifecycle_age_secs.saturating_sub(90))
            .bind(nonsuspend_artifact)
            .execute(pool)
            .await?;
            sqlx::query(
                "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
                 VALUES ($1, 's3://bucket/newer-stop')",
            )
            .bind(nonsuspend_id)
            .execute(pool)
            .await?;
        }
        Ok(())
    }

    /// A retired later-generation execution row (no active claim, no IP), as
    /// a raced start leaves behind after its own cleanup.
    async fn insert_inactive_execution(
        pool: &PgPool,
        sandbox_id: SandboxId,
        generation: i64,
    ) -> TestResult {
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES ($1, $2, $3, 'sandboxes', 'forgotten', FALSE)",
        )
        .bind(sandbox_id)
        .bind(generation)
        .bind(format!(
            "space-sbx-{}-g{generation}",
            sandbox_id.as_uuid().simple()
        ))
        .execute(pool)
        .await?;
        Ok(())
    }

    fn default_min_age() -> PositiveDuration {
        PositiveDuration::new(Duration::from_secs(DEFAULT_MIN_AGE_SECS)).unwrap()
    }

    async fn list_all(store: &RepairDbStore) -> TestResult<Vec<RepairCandidate>> {
        let upper = store
            .current_max_sandbox_id()
            .await?
            .expect("at least one sandbox");
        Ok(store
            .list_candidates(100, None, upper, default_min_age())
            .await?)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r1_lists_only_started_rows_with_current_suspend_snapshot_no_active_and_no_pod_ip()
    -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };

        let repairable = StartedSandboxSpec::repairable(SandboxId::new());
        insert_started_sandbox(&db.pool, &repairable).await?;

        insert_started_sandbox(
            &db.pool,
            &StartedSandboxSpec {
                lifecycle: "suspended",
                ..StartedSandboxSpec::repairable(SandboxId::new())
            },
        )
        .await?;
        insert_started_sandbox(
            &db.pool,
            &StartedSandboxSpec {
                lifecycle_age_secs: 60,
                ..StartedSandboxSpec::repairable(SandboxId::new())
            },
        )
        .await?;
        insert_started_sandbox(
            &db.pool,
            &StartedSandboxSpec {
                current_snapshot: false,
                ..StartedSandboxSpec::repairable(SandboxId::new())
            },
        )
        .await?;

        let store = RepairDbStore::new(db.pool.clone());
        let candidates = list_all(&store).await?;
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].sandbox_id, repairable.sandbox_id);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r2_repair_flips_lifecycle_and_clears_live_placement() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let spec = StartedSandboxSpec::repairable(SandboxId::new());
        insert_started_sandbox(&db.pool, &spec).await?;
        sqlx::query(
            "INSERT INTO sandbox_port_exposures
                (sandbox_id, protocol, guest_port, visibility, source,
                 desired_state, apply_state, last_applied_node_id)
             VALUES ($1, 'tcp', 8080, 'public', 'user', 'open', 'applied', $2)",
        )
        .bind(spec.sandbox_id)
        .bind(Uuid::now_v7())
        .execute(&db.pool)
        .await?;

        let store = RepairDbStore::new(db.pool.clone());
        let candidate = list_all(&store).await?.remove(0);
        let outcome = store
            .revert_started_to_suspended(&candidate, default_min_age())
            .await?;
        assert_eq!(outcome, ApplyOutcome::RevertedToSuspended);

        let (
            lifecycle,
            placement,
            active_node,
            target_node,
            placement_changed,
            command_ready,
            current_artifact,
        ): RepairedSandboxRow = sqlx::query_as(
            "SELECT lifecycle, placement, active_node_id, target_node_id,
                    placement_changed_at, command_ready_pending,
                    current_local_artifact_id
               FROM sandboxes
              WHERE id = $1",
        )
        .bind(spec.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(lifecycle, "suspended");
        assert_eq!(placement, "unplaced");
        assert_eq!(active_node, None);
        assert_eq!(target_node, None);
        assert_eq!(placement_changed, None);
        assert!(!command_ready);
        assert_eq!(current_artifact, candidate.current_local_artifact_id);

        let active: bool = sqlx::query_scalar(
            "SELECT active
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = 1",
        )
        .bind(spec.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert!(!active);

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

    #[tokio::test(flavor = "current_thread")]
    async fn r3_active_execution_is_not_listed() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        insert_started_sandbox(
            &db.pool,
            &StartedSandboxSpec {
                execution_active: true,
                ..StartedSandboxSpec::repairable(SandboxId::new())
            },
        )
        .await?;
        let store = RepairDbStore::new(db.pool.clone());
        assert!(list_all(&store).await?.is_empty());
        assert!(list_all(&store).await?.is_empty());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r5_execution_pod_ip_is_not_listed() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        insert_started_sandbox(
            &db.pool,
            &StartedSandboxSpec {
                execution_pod_ip: Some("10.0.0.7"),
                ..StartedSandboxSpec::repairable(SandboxId::new())
            },
        )
        .await?;
        let store = RepairDbStore::new(db.pool.clone());
        assert!(list_all(&store).await?.is_empty());
        assert!(list_all(&store).await?.is_empty());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r4_lifecycle_race_skips() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let spec = StartedSandboxSpec::repairable(SandboxId::new());
        insert_started_sandbox(&db.pool, &spec).await?;
        let store = RepairDbStore::new(db.pool.clone());
        let candidate = list_all(&store).await?.remove(0);
        sqlx::query("UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1")
            .bind(spec.sandbox_id)
            .execute(&db.pool)
            .await?;
        let outcome = store
            .revert_started_to_suspended(&candidate, default_min_age())
            .await?;
        assert_eq!(outcome, ApplyOutcome::SkippedRaced);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r7_markerless_stale_snapshot_is_not_listed() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let spec = StartedSandboxSpec {
            current_snapshot: false,
            stale_snapshot: true,
            current_marker: false,
            ..StartedSandboxSpec::repairable(SandboxId::new())
        };
        insert_started_sandbox(&db.pool, &spec).await?;
        let store = RepairDbStore::new(db.pool.clone());
        assert!(list_all(&store).await?.is_empty());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r11_exact_current_marker_snapshot_can_precede_lifecycle_changed_at() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let spec = StartedSandboxSpec {
            current_snapshot: false,
            stale_snapshot: true,
            ..StartedSandboxSpec::repairable(SandboxId::new())
        };
        insert_started_sandbox(&db.pool, &spec).await?;

        let store = RepairDbStore::new(db.pool.clone());
        let candidate = {
            let mut listed = list_all(&store).await?;
            assert_eq!(
                listed.len(),
                1,
                "exact current marker backup is durable proof even if lifecycle_changed_at moved later"
            );
            listed.remove(0)
        };
        assert!(
            !candidate.clear_marker,
            "exact current marker must be preserved"
        );

        let outcome = store
            .revert_started_to_suspended(&candidate, default_min_age())
            .await?;
        assert_eq!(outcome, ApplyOutcome::RevertedToSuspended);

        let (lifecycle, current_artifact): (String, Option<LocalArtifactId>) = sqlx::query_as(
            "SELECT lifecycle, current_local_artifact_id FROM sandboxes WHERE id = $1",
        )
        .bind(spec.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(lifecycle, "suspended");
        assert_eq!(
            current_artifact, candidate.current_local_artifact_id,
            "exact marker branch preserves the artifact identity restore will select"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r12_exact_current_marker_with_later_activity_is_not_listed() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let spec = StartedSandboxSpec {
            current_snapshot: false,
            stale_snapshot: true,
            last_command_after_newest: true,
            ..StartedSandboxSpec::repairable(SandboxId::new())
        };
        insert_started_sandbox(&db.pool, &spec).await?;

        let store = RepairDbStore::new(db.pool.clone());
        assert!(
            list_all(&store).await?.is_empty(),
            "exact-marker repair must not roll back recorded activity after the snapshot"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r8_marker_stale_with_newer_snapshot_is_listed_and_clears_marker() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        // Marker points at a pre-restore snapshot (stale), but a newer durable
        // suspend snapshot from this cycle exists and nothing ran after it.
        let spec = StartedSandboxSpec {
            current_snapshot: false,
            stale_snapshot: true,
            newer_distinct_snapshot: true,
            ..StartedSandboxSpec::repairable(SandboxId::new())
        };
        insert_started_sandbox(&db.pool, &spec).await?;

        let store = RepairDbStore::new(db.pool.clone());
        let candidate = {
            let mut listed = list_all(&store).await?;
            assert_eq!(
                listed.len(),
                1,
                "marker-stale row with a newer current snapshot must be listed"
            );
            listed.remove(0)
        };
        assert!(
            candidate.clear_marker,
            "marker-stale candidate must request the marker be cleared"
        );

        let outcome = store
            .revert_started_to_suspended(&candidate, default_min_age())
            .await?;
        assert_eq!(outcome, ApplyOutcome::RevertedToSuspendedMarkerCleared);

        let (lifecycle, current_artifact): (String, Option<LocalArtifactId>) = sqlx::query_as(
            "SELECT lifecycle, current_local_artifact_id FROM sandboxes WHERE id = $1",
        )
        .bind(spec.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(lifecycle, "suspended");
        assert_eq!(
            current_artifact, None,
            "stale marker must be cleared so restore selects the newest snapshot"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r9_marker_stale_with_writes_after_newest_snapshot_is_not_listed() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        // A newer snapshot exists, but a command ran after it: restoring the
        // newest would lose those writes, so it must not be recovered.
        let spec = StartedSandboxSpec {
            current_snapshot: false,
            stale_snapshot: true,
            newer_distinct_snapshot: true,
            last_command_after_newest: true,
            ..StartedSandboxSpec::repairable(SandboxId::new())
        };
        insert_started_sandbox(&db.pool, &spec).await?;
        let store = RepairDbStore::new(db.pool.clone());
        assert!(
            list_all(&store).await?.is_empty(),
            "must not recover when activity post-dates the newest snapshot"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r10_newer_non_suspend_snapshot_blocks_marker_clear() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        // Current suspend snapshot exists, but an even-newer NON-suspend (stop)
        // full snapshot is what the markerless restore selector would load.
        // Clearing the marker would restore a non-suspend artifact, so recovery
        // must refuse.
        let spec = StartedSandboxSpec {
            current_snapshot: false,
            stale_snapshot: true,
            newer_distinct_snapshot: true,
            newer_nonsuspend_snapshot: true,
            ..StartedSandboxSpec::repairable(SandboxId::new())
        };
        insert_started_sandbox(&db.pool, &spec).await?;
        let store = RepairDbStore::new(db.pool.clone());
        assert!(
            list_all(&store).await?.is_empty(),
            "must not recover when a newer non-suspend full snapshot would be the restore source"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn e3_execution_generation_race_skips() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let spec = StartedSandboxSpec::repairable(SandboxId::new());
        insert_started_sandbox(&db.pool, &spec).await?;

        let store = RepairDbStore::new(db.pool.clone());
        let candidate = list_all(&store).await?.remove(0);
        // A start races in between scan and apply: even one that already
        // retired its execution (inactive, no IP) bumps the newest generation
        // and must abort this apply — the next tick re-scans fresh evidence.
        insert_inactive_execution(&db.pool, spec.sandbox_id, 2).await?;
        let outcome = store
            .revert_started_to_suspended(&candidate, default_min_age())
            .await?;
        assert_eq!(outcome, ApplyOutcome::SkippedRaced);
        let lifecycle: String = sqlx::query_scalar("SELECT lifecycle FROM sandboxes WHERE id = $1")
            .bind(spec.sandbox_id)
            .fetch_one(&db.pool)
            .await?;
        assert_eq!(lifecycle, "started");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn e4_execution_lifecycle_race_skips() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let spec = StartedSandboxSpec::repairable(SandboxId::new());
        insert_started_sandbox(&db.pool, &spec).await?;
        let store = RepairDbStore::new(db.pool.clone());
        let candidate = list_all(&store).await?.remove(0);
        sqlx::query("UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1")
            .bind(spec.sandbox_id)
            .execute(&db.pool)
            .await?;
        let outcome = store
            .revert_started_to_suspended(&candidate, default_min_age())
            .await?;
        assert_eq!(outcome, ApplyOutcome::SkippedRaced);
        Ok(())
    }

    /// Wait until a backend in this test's database is blocked on a row
    /// lock — proof the repair apply started (and took its statement
    /// snapshot) before the racing writer commits.
    async fn wait_for_lock_waiter(pool: &PgPool) -> TestResult {
        for _ in 0..200 {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                     SELECT 1
                       FROM pg_stat_activity
                      WHERE datname = current_database()
                        AND wait_event_type = 'Lock'
                 )",
            )
            .fetch_one(pool)
            .await?;
            if waiting {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Err("repair apply never blocked on the sandbox row lock".into())
    }

    /// The wake/restore race the apply-side row lock exists for: a writer
    /// holds the sandbox row lock with an uncommitted new active generation
    /// while the repair applies. The repair must park on that lock and
    /// re-prove the generation CAS against the writer's committed state —
    /// not against a statement snapshot taken before the commit, which
    /// cannot see the new generation.
    #[tokio::test(flavor = "current_thread")]
    async fn e6_uncommitted_wake_generation_race_skips() -> TestResult {
        let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
            return Ok(());
        };
        let spec = StartedSandboxSpec::repairable(SandboxId::new());
        insert_started_sandbox(&db.pool, &spec).await?;
        let store = RepairDbStore::new(db.pool.clone());
        let candidate = list_all(&store).await?.remove(0);

        // The wake side: lock the sandbox row and allocate the next active
        // generation, holding both uncommitted while the repair applies.
        let mut wake_tx = db.pool.begin().await?;
        sqlx::query("SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE")
            .bind(spec.sandbox_id)
            .execute(&mut *wake_tx)
            .await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES ($1, 2, $2, 'sandboxes', 'creating', TRUE)",
        )
        .bind(spec.sandbox_id)
        .bind(format!(
            "space-sbx-{}-g2",
            spec.sandbox_id.as_uuid().simple()
        ))
        .execute(&mut *wake_tx)
        .await?;

        let repair = tokio::spawn({
            let store = RepairDbStore::new(db.pool.clone());
            let candidate = candidate.clone();
            async move {
                store
                    .revert_started_to_suspended(&candidate, default_min_age())
                    .await
            }
        });
        wait_for_lock_waiter(&db.pool).await?;
        wake_tx.commit().await?;

        let outcome = repair.await??;
        assert_eq!(
            outcome,
            ApplyOutcome::SkippedRaced,
            "repair must re-prove the generation CAS against the wake's committed state"
        );
        let (lifecycle, active_generations): (String, i64) = sqlx::query_as(
            "SELECT s.lifecycle,
                    (SELECT COUNT(*)::BIGINT
                       FROM sandbox_k8s_executions e
                      WHERE e.sandbox_id = s.id
                        AND e.active)
               FROM sandboxes s
              WHERE s.id = $1",
        )
        .bind(spec.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(lifecycle, "started", "the woken sandbox must stay started");
        assert_eq!(active_generations, 1);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn e5_execution_marker_stale_clears_marker() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let spec = StartedSandboxSpec {
            current_snapshot: false,
            stale_snapshot: true,
            newer_distinct_snapshot: true,
            ..StartedSandboxSpec::repairable(SandboxId::new())
        };
        insert_started_sandbox(&db.pool, &spec).await?;

        let store = RepairDbStore::new(db.pool.clone());
        let candidate = list_all(&store).await?.remove(0);
        assert!(candidate.clear_marker);

        let outcome = store
            .revert_started_to_suspended(&candidate, default_min_age())
            .await?;
        assert_eq!(outcome, ApplyOutcome::RevertedToSuspendedMarkerCleared);

        let (lifecycle, current_artifact): (String, Option<LocalArtifactId>) = sqlx::query_as(
            "SELECT lifecycle, current_local_artifact_id FROM sandboxes WHERE id = $1",
        )
        .bind(spec.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(lifecycle, "suspended");
        assert_eq!(current_artifact, None);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn e7_scan_selects_newest_execution_generation() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };

        let spec = StartedSandboxSpec::repairable(SandboxId::new());
        insert_started_sandbox(&db.pool, &spec).await?;
        insert_inactive_execution(&db.pool, spec.sandbox_id, 2).await?;

        let store = RepairDbStore::new(db.pool.clone());
        let candidate = list_all(&store).await?.remove(0);
        assert_eq!(
            candidate.newest_execution,
            Some(NewestExecutionRow {
                pod_name: format!("space-sbx-{}-g2", spec.sandbox_id.as_uuid().simple()),
                generation: 2,
            })
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r13_direct_rows_are_not_bounded_listed_or_repaired() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let spec = StartedSandboxSpec::repairable(SandboxId::new());
        insert_started_sandbox(&db.pool, &spec).await?;
        let set_managed_by = |managed_by: &'static str| {
            sqlx::query("UPDATE sandboxes SET managed_by = $2 WHERE id = $1")
                .bind(spec.sandbox_id)
                .bind(managed_by)
                .execute(&db.pool)
        };
        let store = RepairDbStore::new(db.pool.clone());
        let candidate = list_all(&store).await?.remove(0);

        set_managed_by("space").await?;
        assert_eq!(
            store.current_max_sandbox_id().await?,
            None,
            "a direct row does not bound the sweep"
        );
        assert!(
            store
                .list_candidates(100, None, spec.sandbox_id, default_min_age())
                .await?
                .is_empty(),
            "a direct row is not listed"
        );
        assert_eq!(
            store
                .revert_started_to_suspended(&candidate, default_min_age())
                .await?,
            ApplyOutcome::SkippedRaced,
            "the apply refuses a direct row"
        );

        set_managed_by("k8s").await?;
        assert_eq!(
            store
                .revert_started_to_suspended(&candidate, default_min_age())
                .await?,
            ApplyOutcome::RevertedToSuspended
        );
        Ok(())
    }
}
