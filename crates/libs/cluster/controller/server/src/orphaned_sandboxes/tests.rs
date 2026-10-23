use pplx_space_common_core::types::{NodeId, SnapshotId};

use super::*;
use k8s_openapi::api::core::v1::{
    ContainerState, ContainerStateRunning, ContainerStateTerminated, ContainerStateWaiting,
    ContainerStatus, Pod, PodStatus,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use pplx_space_common_core::types::{
    AutoLifecycleConditionRuntimeState, AutoLifecycleConditionSet, AutoLifecycleConditionSetId,
    AutoLifecycleIdleAction, AutoLifecyclePolicy, AutoLifecycleRuntimeState, AutoLifecycleStage,
    SandboxActivationAttemptId,
};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn classified_counter_count(
    counters: &[pplx_space_metrics::test_capture::CapturedCounter],
    outcome: &str,
    mode: &str,
) -> usize {
    counters
        .iter()
        .filter(|counter| {
            counter.name == "sandbox.api.reconciler.rows_classified"
                && counter.tags.iter().any(|tag| tag == outcome)
                && counter.tags.iter().any(|tag| tag == mode)
        })
        .count()
}

#[derive(Default)]
struct FakePresence {
    present: Vec<String>,
    unready: Vec<String>,
    unknown: Vec<String>,
}

impl FakePresence {
    fn with_present<I: IntoIterator<Item = String>>(present: I) -> Self {
        Self {
            present: present.into_iter().collect(),
            unready: Vec::new(),
            unknown: Vec::new(),
        }
    }

    fn with_unready<I: IntoIterator<Item = String>>(unready: I) -> Self {
        Self {
            present: Vec::new(),
            unready: unready.into_iter().collect(),
            unknown: Vec::new(),
        }
    }

    fn with_unknown<I: IntoIterator<Item = String>>(unknown: I) -> Self {
        Self {
            present: Vec::new(),
            unready: Vec::new(),
            unknown: unknown.into_iter().collect(),
        }
    }
}

impl PodPresence for FakePresence {
    async fn check_pod(&self, pod_name: &str) -> PodPresenceStatus {
        if self.present.iter().any(|p| p == pod_name) {
            PodPresenceStatus::Present
        } else if self.unready.iter().any(|p| p == pod_name) {
            PodPresenceStatus::Unready
        } else if self.unknown.iter().any(|p| p == pod_name) {
            PodPresenceStatus::Unknown
        } else {
            PodPresenceStatus::Missing
        }
    }
}

#[derive(Default)]
struct FakeSink {
    candidates: Mutex<Vec<OrphanCandidate>>,
    actions: Mutex<Vec<(SandboxId, ClassifyGuard)>>,
    fail_action_for: Mutex<Vec<SandboxId>>,
    quarantine_for: Mutex<Vec<SandboxId>>,
    defer_for: Mutex<Vec<SandboxId>>,
    skip_terminal_for: Mutex<Vec<SandboxId>>,
    active_quarantine_count: Mutex<u32>,
}

impl FakeSink {
    fn new(candidates: Vec<OrphanCandidate>) -> Self {
        Self {
            candidates: Mutex::new(candidates),
            ..Default::default()
        }
    }

    fn action_count(&self) -> usize {
        self.actions.lock().unwrap().len()
    }
}

impl OrphanActionSink for FakeSink {
    async fn list_candidates(
        &self,
        batch_size: u32,
        after_sandbox_id: Option<SandboxId>,
        upper_bound: SandboxId,
    ) -> Result<Vec<OrphanCandidate>, sqlx::Error> {
        let candidates = self.candidates.lock().unwrap();
        let page: Vec<_> = candidates
            .iter()
            .filter(|candidate| {
                after_sandbox_id
                    .map(|after| candidate.sandbox_id > after)
                    .unwrap_or(true)
                    && candidate.sandbox_id <= upper_bound
            })
            .take(batch_size as usize)
            .cloned()
            .collect();
        Ok(page)
    }

    async fn current_max_sandbox_id(&self) -> Result<Option<SandboxId>, sqlx::Error> {
        Ok(self
            .candidates
            .lock()
            .unwrap()
            .iter()
            .map(|c| c.sandbox_id)
            .max())
    }

    async fn classify_orphan(
        &self,
        sandbox_id: SandboxId,
        guard: ClassifyGuard,
    ) -> Result<ClassifyOutcome, OrphanClassifyError> {
        let Some(plan) = self.plan_orphan(sandbox_id, guard.clone()).await? else {
            return Ok(ClassifyOutcome::SkippedTerminal);
        };
        let outcome = match plan.disposition {
            OrphanDisposition::FinalizeDelete(reason) => ClassifyOutcome::Deleted {
                from_lifecycle: plan.from_lifecycle,
                reason,
            },
            OrphanDisposition::RecoverSuspended => ClassifyOutcome::RecoveredSuspended {
                from_lifecycle: plan.from_lifecycle,
            },
            OrphanDisposition::QuarantineAmbiguous => ClassifyOutcome::Quarantined {
                from_lifecycle: plan.from_lifecycle,
            },
            OrphanDisposition::Defer(reason) => {
                return Ok(ClassifyOutcome::Deferred {
                    from_lifecycle: plan.from_lifecycle,
                    reason,
                });
            }
        };
        self.actions.lock().unwrap().push((sandbox_id, guard));
        Ok(outcome)
    }

    async fn plan_orphan(
        &self,
        sandbox_id: SandboxId,
        guard: ClassifyGuard,
    ) -> Result<Option<OrphanPlan>, OrphanClassifyError> {
        if self.fail_action_for.lock().unwrap().contains(&sandbox_id) {
            return Err(OrphanClassifyError::Database(sqlx::Error::RowNotFound));
        }
        if self.skip_terminal_for.lock().unwrap().contains(&sandbox_id) {
            return Ok(None);
        }
        let from_lifecycle = self
            .candidates
            .lock()
            .unwrap()
            .iter()
            .find(|candidate| candidate.sandbox_id == sandbox_id)
            .map(|candidate| candidate.lifecycle)
            .expect("classified sandbox should come from the candidate scan");
        let disposition = if self.defer_for.lock().unwrap().contains(&sandbox_id) {
            OrphanDisposition::Defer(OrphanDeferReason::InFlightSnapshot)
        } else if matches!(guard, ClassifyGuard::LegacyDeleting { .. }) {
            OrphanDisposition::FinalizeDelete(OrphanDeleteReason::ExplicitDeleting)
        } else if from_lifecycle == SandboxLifecycle::Creating {
            OrphanDisposition::FinalizeDelete(OrphanDeleteReason::AbandonedCreate)
        } else if self.quarantine_for.lock().unwrap().contains(&sandbox_id) {
            OrphanDisposition::QuarantineAmbiguous
        } else {
            OrphanDisposition::RecoverSuspended
        };
        Ok(Some(OrphanPlan {
            from_lifecycle,
            disposition,
        }))
    }

    async fn active_quarantine_count(&self) -> Result<u32, OrphanClassifyError> {
        Ok(*self.active_quarantine_count.lock().unwrap())
    }
}

impl FakeSink {
    fn quarantine(&self, sandbox_id: SandboxId) {
        self.quarantine_for.lock().unwrap().push(sandbox_id);
    }

    fn defer(&self, sandbox_id: SandboxId) {
        self.defer_for.lock().unwrap().push(sandbox_id);
    }

    fn fail_action(&self, sandbox_id: SandboxId) {
        self.fail_action_for.lock().unwrap().push(sandbox_id);
    }

    fn clear_action_failures(&self) {
        self.fail_action_for.lock().unwrap().clear();
    }

    fn skip_terminal(&self, sandbox_id: SandboxId) {
        self.skip_terminal_for.lock().unwrap().push(sandbox_id);
    }

    fn set_active_quarantine_count(&self, count: u32) {
        *self.active_quarantine_count.lock().unwrap() = count;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FakePodCleanupResult {
    Outcome(K8sBlockingDeleteOutcome),
    DeleteTimeout,
}

struct FakePodCleaner {
    result: FakePodCleanupResult,
    calls: Mutex<Vec<String>>,
}

impl FakePodCleaner {
    fn new(outcome: K8sBlockingDeleteOutcome) -> Self {
        Self {
            result: FakePodCleanupResult::Outcome(outcome),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn timeout() -> Self {
        Self {
            result: FakePodCleanupResult::DeleteTimeout,
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

struct LifecycleRacePodCleaner {
    pool: PgPool,
    sandbox_id: SandboxId,
}

#[async_trait::async_trait]
impl ProjectionPodCleaner for LifecycleRacePodCleaner {
    async fn delete_projection_pod(
        &self,
        _name: &str,
    ) -> Result<K8sBlockingDeleteOutcome, K8sBlockingDeleteError> {
        sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
        ))
        .bind(self.sandbox_id)
        .execute(&self.pool)
        .await
        .expect("test lifecycle race update should succeed");
        Ok(K8sBlockingDeleteOutcome::Deleted)
    }
}

#[async_trait::async_trait]
impl ProjectionPodCleaner for FakePodCleaner {
    async fn delete_projection_pod(
        &self,
        name: &str,
    ) -> Result<K8sBlockingDeleteOutcome, K8sBlockingDeleteError> {
        self.calls.lock().unwrap().push(name.to_string());
        match self.result {
            FakePodCleanupResult::Outcome(outcome) => Ok(outcome),
            FakePodCleanupResult::DeleteTimeout => Err(K8sBlockingDeleteError::DeleteTimeout {
                pod_name: name.to_string(),
                timeout_secs: 1,
            }),
        }
    }
}

fn config(grace_secs: u64, max_per_min: u32, dry_run: bool) -> ReconcilerConfig {
    ReconcilerConfig {
        enabled: true,
        interval: Duration::from_secs(60),
        grace_period: Duration::from_secs(grace_secs),
        batch_size: 500,
        max_classifications_per_minute: max_per_min,
        max_active_quarantines: DEFAULT_MAX_ACTIVE_QUARANTINES,
        max_new_quarantines_per_sweep: DEFAULT_MAX_NEW_QUARANTINES_PER_SWEEP,
        max_quarantine_percentage: DEFAULT_MAX_QUARANTINE_PERCENTAGE,
        dry_run,
    }
}

fn candidate(pod: &str) -> OrphanCandidate {
    OrphanCandidate {
        sandbox_id: SandboxId::new(),
        pod_name: pod.to_string(),
        pod_uid: Some(PodUid::parse(format!("{pod}-uid")).unwrap()),
        kind: OrphanKind::Normal,
        lifecycle: SandboxLifecycle::Started,
    }
}

fn candidate_with_id(id: u128, pod: &str) -> OrphanCandidate {
    OrphanCandidate {
        sandbox_id: SandboxId::from_uuid(Uuid::from_u128(id)),
        pod_name: pod.to_string(),
        pod_uid: Some(PodUid::parse(format!("{pod}-uid")).unwrap()),
        kind: OrphanKind::Normal,
        lifecycle: SandboxLifecycle::Started,
    }
}

fn forgotten_candidate() -> OrphanCandidate {
    OrphanCandidate {
        sandbox_id: SandboxId::new(),
        pod_name: String::new(),
        pod_uid: None,
        kind: OrphanKind::Forgotten,
        lifecycle: SandboxLifecycle::Started,
    }
}

fn legacy_deleting_candidate() -> OrphanCandidate {
    OrphanCandidate {
        sandbox_id: SandboxId::new(),
        pod_name: String::new(),
        pod_uid: None,
        kind: OrphanKind::LegacyDeleting,
        lifecycle: SandboxLifecycle::Deleting,
    }
}

fn pod_with_phase(phase: &str, pod_ip: Option<&str>) -> Pod {
    Pod {
        metadata: ObjectMeta::default(),
        status: Some(PodStatus {
            phase: Some(phase.to_string()),
            pod_ip: pod_ip.map(ToString::to_string),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn pod_with_phase_and_container_state(phase: &str, state: ContainerState) -> Pod {
    Pod {
        metadata: ObjectMeta::default(),
        status: Some(PodStatus {
            phase: Some(phase.to_string()),
            container_statuses: Some(vec![ContainerStatus {
                state: Some(state),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn terminated_container_state() -> ContainerState {
    ContainerState {
        terminated: Some(ContainerStateTerminated::default()),
        ..Default::default()
    }
}

fn running_container_state() -> ContainerState {
    ContainerState {
        running: Some(ContainerStateRunning::default()),
        ..Default::default()
    }
}

fn waiting_container_state() -> ContainerState {
    ContainerState {
        waiting: Some(ContainerStateWaiting::default()),
        ..Default::default()
    }
}

async fn insert_node(pool: &PgPool, node_id: NodeId) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.5'::inet, 'worker', 'k8s', 'registered')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_started_sandbox_with_active_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, network_policy_generation,
             template_id, content_hash)
         VALUES ($1, $2::uuid, $3::uuid, 'k8s', 'started', 'on_node',
                 $4, $4, 1, 'app-runtime', 'sha256:test')"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, pod_uid,
             state, active, created_at)
         VALUES ($1, 1, $2, 'sandboxes', $3, 'running', TRUE, NOW() - INTERVAL '1 hour')"
    ))
    .bind(sandbox_id)
    .bind(format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple()))
    .bind(format!("{}-uid", sandbox_id.as_uuid().simple()))
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_legacy_deleting_sandbox(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, placement,
             network_policy_generation, template_id, content_hash)
         VALUES ($1, $2::uuid, $3::uuid, 'k8s', 'deleting', 'unplaced',
                 1, 'app-runtime', 'sha256:test')"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_observed_sandbox_state(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    age_secs: i64,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states
            (sandbox_id, node_id, status, runtime, sandbox_owner,
             image_hash, snapshots_enabled, template, observed_at, updated_at)
         VALUES ($1, $2, 'suspended', 'fc', 'user:test',
                 'sha256:test', FALSE, 'app-runtime',
                 NOW() - make_interval(secs => $3::BIGINT),
                 NOW() - make_interval(secs => $3::BIGINT))"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(age_secs)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_ready_full_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    artifact_id: Uuid,
    timestamp_expr: &str,
) -> TestResult {
    let snapshot_id = SnapshotId::new();
    let query = format!(
        "INSERT INTO snapshots
            (id, sandbox_id, org_id, type, state, timestamp,
             local_artifact_id, artifact_kind, artifact_origin)
         VALUES ($1, $2, $3::uuid, 'full', 'ready', {timestamp_expr},
                 $4, 'full', 'suspend')"
    );
    sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(snapshot_id)
        .bind(sandbox_id)
        .bind(SEED_ORG_ID)
        .bind(artifact_id)
        .execute(pool)
        .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
            (snapshot_id, org_id, storage_connection_string)
         VALUES ($1, $2::uuid, $3)"
    ))
    .bind(snapshot_id)
    .bind(SEED_ORG_ID)
    .bind(format!("s3://space-test/snapshots/{snapshot_id}"))
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_failed_full_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    artifact_id: Uuid,
    timestamp_expr: &str,
) -> TestResult {
    let snapshot_id = SnapshotId::new();
    let query = format!(
        "INSERT INTO snapshots
            (id, sandbox_id, org_id, type, state, timestamp,
             local_artifact_id, artifact_kind, artifact_origin)
         VALUES ($1, $2, $3::uuid, 'full', 'failed', {timestamp_expr},
                 $4, 'full', 'suspend')"
    );
    sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(snapshot_id)
        .bind(sandbox_id)
        .bind(SEED_ORG_ID)
        .bind(artifact_id)
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn split_operation_expired_stopped_row_has_no_orphan_owner_proof() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'stopped' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET route_activation_id = $2,
            activation_attempt_id = $3, activation_attempt_expires_at = NOW() - INTERVAL '1 hour',
            updated_at = NOW() - INTERVAL '1 hour'
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    for _ in 0..3 {
        let candidates =
            list_candidates_after(&db.pool, None, SandboxId::from_uuid(Uuid::max()), 100).await?;
        assert!(candidates.iter().all(|row| row.sandbox_id != sandbox_id));
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn candidate_scan_lists_missing_pod_without_node_observation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;

    let candidates =
        list_candidates_after(&db.pool, None, SandboxId::from_uuid(Uuid::max()), 10).await?;

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].sandbox_id, sandbox_id);
    assert_eq!(candidates[0].lifecycle, SandboxLifecycle::Started);
    let expected_pod_uid = format!("{}-uid", sandbox_id.as_uuid().simple());
    assert_eq!(
        candidates[0].pod_uid.as_ref().map(PodUid::as_str),
        Some(expected_pod_uid.as_str())
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn candidate_scan_waits_for_creating_attempt_expiry() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'creating' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'creating',
                updated_at = NOW() - INTERVAL '1 hour',
                activation_attempt_id = $2,
                activation_attempt_expires_at = NOW() + INTERVAL '5 minutes'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .bind(SandboxActivationAttemptId::new())
    .execute(&db.pool)
    .await?;
    insert_observed_sandbox_state(&db.pool, sandbox_id, node_id, 60).await?;

    let active_attempt =
        list_candidates_after(&db.pool, None, SandboxId::from_uuid(Uuid::max()), 10).await?;
    assert!(
        active_attempt.is_empty(),
        "a live materialization attempt must keep ownership despite an old execution clock"
    );

    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_expires_at = NOW() - INTERVAL '1 second'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let expired_attempt =
        list_candidates_after(&db.pool, None, SandboxId::from_uuid(Uuid::max()), 10).await?;
    assert_eq!(expired_attempt.len(), 1);
    assert_eq!(expired_attempt[0].sandbox_id, sandbox_id);
    assert_eq!(expired_attempt[0].lifecycle, SandboxLifecycle::Creating);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn classification_reports_lifecycle_observed_inside_guarded_transaction() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;

    let sink = SubstrateActionSink::new_without_substrate(db.pool.clone());
    let mut candidates = sink
        .list_candidates(10, None, SandboxId::from_uuid(Uuid::max()))
        .await?;
    let candidate = candidates
        .pop()
        .expect("candidate scan should find sandbox");
    assert_eq!(candidate.lifecycle, SandboxLifecycle::Started);

    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'paused' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let outcome = sink
        .classify_orphan(
            sandbox_id,
            ClassifyGuard::PodName {
                name: candidate.pod_name,
                uid: candidate.pod_uid,
            },
        )
        .await?;

    assert_eq!(
        outcome,
        ClassifyOutcome::Quarantined {
            from_lifecycle: SandboxLifecycle::Paused,
        },
        "live telemetry must use the lifecycle read under the classification lock"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn candidate_scan_skips_sandbox_with_node_observation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    insert_observed_sandbox_state(&db.pool, sandbox_id, node_id, 60).await?;

    let candidates =
        list_candidates_after(&db.pool, None, SandboxId::from_uuid(Uuid::max()), 10).await?;

    assert!(
        candidates.is_empty(),
        "node-local observation means the sandbox still exists and must not be pruned"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn candidate_scan_defers_to_projection_gc_for_observed_rows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    insert_observed_sandbox_state(&db.pool, sandbox_id, node_id, 3_600).await?;

    let candidates =
        list_candidates_after(&db.pool, None, SandboxId::from_uuid(Uuid::max()), 10).await?;

    assert!(
        candidates.is_empty(),
        "orphan reconciliation should not prune rows while the node projection still carries them"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn candidate_scan_ignores_observation_from_other_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let active_node_id = NodeId::new();
    let other_node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, active_node_id).await?;
    insert_node(&db.pool, other_node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, active_node_id).await?;
    insert_observed_sandbox_state(&db.pool, sandbox_id, other_node_id, 60).await?;

    let candidates =
        list_candidates_after(&db.pool, None, SandboxId::from_uuid(Uuid::max()), 10).await?;

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].sandbox_id, sandbox_id);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn candidate_scan_lists_legacy_deleting_row() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    insert_legacy_deleting_sandbox(&db.pool, sandbox_id).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, pod_uid,
             state, active, created_at)
         VALUES ($1, 1, 'legacy-pod', 'sandboxes', 'legacy-uid',
                 'running', TRUE, NOW() - INTERVAL '1 hour')"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let candidates =
        list_candidates_after(&db.pool, None, SandboxId::from_uuid(Uuid::max()), 10).await?;

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].sandbox_id, sandbox_id);
    assert_eq!(candidates[0].pod_name.as_deref(), Some("legacy-pod"));
    assert_eq!(candidates[0].lifecycle, SandboxLifecycle::Deleting);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn classify_missing_pod_with_current_artifact_recovers_to_suspended() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let artifact_id = Uuid::now_v7();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend',
                last_command_at = NOW() - INTERVAL '10 minutes',
                last_access_at = NOW() - INTERVAL '10 minutes'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact_id)
    .execute(&db.pool)
    .await?;
    insert_ready_full_snapshot(
        &db.pool,
        sandbox_id,
        artifact_id,
        "NOW() - INTERVAL '5 minutes'",
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_port_exposures (
             sandbox_id, protocol, guest_port, visibility, source,
             desired_state, apply_state, last_applied_node_id, last_error
         )
         VALUES ($1, 'tcp', 8080, 'private', 'api', 'open', 'failed', $2,
                 'expose port: pod network namespace is gone')"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    let desired_state_ref = ReconcileElementRef::sandbox_desired_state(sandbox_id);
    sqlx::query(sql!(
        "INSERT INTO node_reconcile_queue (node_id, facet, element, attempts, last_error)
         VALUES ($1, $2, $3, 12, 'local sandbox state unavailable')"
    ))
    .bind(node_id)
    .bind(desired_state_ref.facet_str())
    .bind(desired_state_ref.element_string())
    .execute(&db.pool)
    .await?;

    let sink = SubstrateActionSink::new_without_substrate(db.pool.clone());
    let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
    let pod_uid = Some(PodUid::parse(format!(
        "{}-uid",
        sandbox_id.as_uuid().simple()
    ))?);
    let guard = ClassifyGuard::PodName {
        name: pod_name,
        uid: pod_uid,
    };
    let outcome = sink.classify_orphan(sandbox_id, guard).await?;

    assert_eq!(
        outcome,
        ClassifyOutcome::RecoveredSuspended {
            from_lifecycle: SandboxLifecycle::Started,
        }
    );
    let lifecycle: SandboxLifecycle =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(lifecycle, SandboxLifecycle::Suspended);
    let active_execution_count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*)::BIGINT
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(active_execution_count, 0);
    let quarantine_count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*)::BIGINT FROM sandbox_quarantine_state WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(quarantine_count, 0);
    // User port intent survives the recovery with its application
    // observation reset, so the exposure re-applies after restore.
    let (desired_state, apply_state, last_applied_node_id): (String, String, Option<NodeId>) =
        sqlx::query_as(sql!(
            "SELECT desired_state, apply_state, last_applied_node_id
               FROM sandbox_port_exposures WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(desired_state, "open");
    assert_eq!(apply_state, "pending");
    assert_eq!(last_applied_node_id, None);
    // The old node's desired-state queue ref is dropped alongside the
    // node-assignment retirement, same as the quarantine branch.
    let queue_refs: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*)::BIGINT FROM node_reconcile_queue
          WHERE facet = $1 AND element = $2"
    ))
    .bind(desired_state_ref.facet_str())
    .bind(desired_state_ref.element_string())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(queue_refs, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn failed_retry_of_exact_ready_artifact_does_not_block_recovery() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let artifact_id = Uuid::now_v7();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend',
                last_command_at = NOW() - INTERVAL '10 minutes',
                last_access_at = NOW() - INTERVAL '10 minutes'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact_id)
    .execute(&db.pool)
    .await?;
    insert_ready_full_snapshot(
        &db.pool,
        sandbox_id,
        artifact_id,
        "NOW() - INTERVAL '5 minutes'",
    )
    .await?;
    insert_failed_full_snapshot(
        &db.pool,
        sandbox_id,
        artifact_id,
        "NOW() - INTERVAL '1 minute'",
    )
    .await?;

    let sink = SubstrateActionSink::new_without_substrate(db.pool.clone());
    let outcome = sink
        .classify_orphan(
            sandbox_id,
            ClassifyGuard::PodName {
                name: format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple()),
                uid: Some(PodUid::parse(format!(
                    "{}-uid",
                    sandbox_id.as_uuid().simple()
                ))?),
            },
        )
        .await?;

    assert_eq!(
        outcome,
        ClassifyOutcome::RecoveredSuspended {
            from_lifecycle: SandboxLifecycle::Started,
        }
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn failed_retry_of_different_artifact_remains_ambiguous() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let artifact_id = Uuid::now_v7();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend',
                last_command_at = NOW() - INTERVAL '10 minutes',
                last_access_at = NOW() - INTERVAL '10 minutes'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact_id)
    .execute(&db.pool)
    .await?;
    insert_ready_full_snapshot(
        &db.pool,
        sandbox_id,
        artifact_id,
        "NOW() - INTERVAL '5 minutes'",
    )
    .await?;
    insert_failed_full_snapshot(
        &db.pool,
        sandbox_id,
        Uuid::now_v7(),
        "NOW() - INTERVAL '1 minute'",
    )
    .await?;

    let sink = SubstrateActionSink::new_without_substrate(db.pool.clone());
    let plan = sink
        .plan_orphan(
            sandbox_id,
            ClassifyGuard::PodName {
                name: format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple()),
                uid: Some(PodUid::parse(format!(
                    "{}-uid",
                    sandbox_id.as_uuid().simple()
                ))?),
            },
        )
        .await?
        .expect("guarded orphan should still exist");

    assert_eq!(plan.disposition, OrphanDisposition::QuarantineAmbiguous);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn due_auto_lifecycle_delete_without_durable_claim_is_quarantined() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let policy = AutoLifecyclePolicy {
        wake: Default::default(),
        condition_sets: BTreeMap::from([(
            AutoLifecycleConditionSetId::AccessIdle,
            AutoLifecycleConditionSet::access_idle(),
        )]),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::AccessIdle,
            at: 300,
            action: AutoLifecycleIdleAction::Delete,
        }],
    };
    let state = AutoLifecycleRuntimeState {
        conditions: BTreeMap::from([(
            AutoLifecycleConditionSetId::AccessIdle,
            AutoLifecycleConditionRuntimeState {
                armed: true,
                epoch_at: Some(now.saturating_sub(600)),
                ..Default::default()
            },
        )]),
        ..Default::default()
    };
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET auto_lifecycle_json = $2,
                auto_lifecycle_state_json = $3
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(Json(policy))
    .bind(Json(state))
    .execute(&db.pool)
    .await?;

    let sink = SubstrateActionSink::new_without_substrate(db.pool.clone());
    let outcome = sink
        .classify_orphan(
            sandbox_id,
            ClassifyGuard::PodName {
                name: format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple()),
                uid: Some(PodUid::parse(format!(
                    "{}-uid",
                    sandbox_id.as_uuid().simple()
                ))?),
            },
        )
        .await?;

    assert_eq!(
        outcome,
        ClassifyOutcome::Quarantined {
            from_lifecycle: SandboxLifecycle::Started,
        }
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn abandoned_initial_create_is_finalized_instead_of_quarantined() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'creating' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'creating',
                updated_at = NOW() - INTERVAL '1 hour'
          WHERE sandbox_id = $1
            AND active"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let sink = SubstrateActionSink::new_without_substrate(db.pool.clone());
    let outcome = sink
        .classify_orphan(
            sandbox_id,
            ClassifyGuard::PodName {
                name: format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple()),
                uid: Some(PodUid::parse(format!(
                    "{}-uid",
                    sandbox_id.as_uuid().simple()
                ))?),
            },
        )
        .await?;

    assert_eq!(
        outcome,
        ClassifyOutcome::Deleted {
            from_lifecycle: SandboxLifecycle::Creating,
            reason: OrphanDeleteReason::AbandonedCreate,
        }
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn advisory_plan_does_not_wait_for_sandbox_row_lock() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    let mut lock_tx = db.pool.begin().await?;
    sqlx::query(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox_id)
        .fetch_one(&mut *lock_tx)
        .await?;
    let sink = SubstrateActionSink::new_without_substrate(db.pool.clone());

    let plan = tokio::time::timeout(
        Duration::from_secs(1),
        sink.plan_orphan(
            sandbox_id,
            ClassifyGuard::PodName {
                name: format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple()),
                uid: Some(PodUid::parse(format!(
                    "{}-uid",
                    sandbox_id.as_uuid().simple()
                ))?),
            },
        ),
    )
    .await
    .expect("advisory planning must not wait for the mutation row lock")?;

    assert!(plan.is_some());
    lock_tx.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unconfirmed_cleanup_keeps_abandoned_create_for_retry() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'creating' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'creating',
                updated_at = NOW() - INTERVAL '1 hour'
          WHERE sandbox_id = $1
            AND active"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
    let cleaner = Arc::new(FakePodCleaner::timeout());
    let sink = SubstrateActionSink::new_with_pod_cleaner(db.pool.clone(), cleaner.clone());
    let outcome = sink
        .classify_orphan(
            sandbox_id,
            ClassifyGuard::PodName {
                name: pod_name.clone(),
                uid: Some(PodUid::parse(format!(
                    "{}-uid",
                    sandbox_id.as_uuid().simple()
                ))?),
            },
        )
        .await?;

    assert!(matches!(
        outcome,
        ClassifyOutcome::Deferred {
            from_lifecycle: SandboxLifecycle::Creating,
            reason: OrphanDeferReason::PodCleanupUnconfirmed,
        }
    ));
    assert_eq!(cleaner.calls(), vec![pod_name]);
    let sandbox_count: i64 =
        sqlx::query_scalar(sql!("SELECT COUNT(*)::BIGINT FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(
        sandbox_count, 1,
        "an unconfirmed pod cleanup must leave the row eligible for retry"
    );
    let lifecycle: SandboxLifecycle =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(
        lifecycle,
        SandboxLifecycle::Deleting,
        "pod cleanup must be backed by durable deleting intent before the external call"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn lifecycle_change_during_cleanup_prevents_database_delete() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'creating' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'creating',
                updated_at = NOW() - INTERVAL '1 hour'
          WHERE sandbox_id = $1
            AND active"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let cleaner = Arc::new(LifecycleRacePodCleaner {
        pool: db.pool.clone(),
        sandbox_id,
    });
    let sink = SubstrateActionSink::new_with_pod_cleaner(db.pool.clone(), cleaner);

    let outcome = sink
        .classify_orphan(
            sandbox_id,
            ClassifyGuard::PodName {
                name: format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple()),
                uid: Some(PodUid::parse(format!(
                    "{}-uid",
                    sandbox_id.as_uuid().simple()
                ))?),
            },
        )
        .await?;

    assert_eq!(outcome, ClassifyOutcome::SkippedTerminal);
    let lifecycle: SandboxLifecycle =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(lifecycle, SandboxLifecycle::Started);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn creating_snapshot_defers_disposition() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots
            (id, sandbox_id, org_id, type, state, timestamp)
         VALUES ($1, $2, $3::uuid, 'full', 'creating', NOW())"
    ))
    .bind(Uuid::now_v7())
    .bind(sandbox_id)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;

    let sink = SubstrateActionSink::new_without_substrate(db.pool.clone());
    let outcome = sink
        .classify_orphan(
            sandbox_id,
            ClassifyGuard::PodName {
                name: format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple()),
                uid: Some(PodUid::parse(format!(
                    "{}-uid",
                    sandbox_id.as_uuid().simple()
                ))?),
            },
        )
        .await?;

    assert_eq!(
        outcome,
        ClassifyOutcome::Deferred {
            from_lifecycle: SandboxLifecycle::Started,
            reason: OrphanDeferReason::InFlightSnapshot,
        }
    );
    let lifecycle: SandboxLifecycle =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(lifecycle, SandboxLifecycle::Started);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn post_commit_pod_cleanup_failure_keeps_recovered_classification() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let artifact_id = Uuid::now_v7();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend',
                last_command_at = NOW() - INTERVAL '10 minutes',
                last_access_at = NOW() - INTERVAL '10 minutes'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact_id)
    .execute(&db.pool)
    .await?;
    insert_ready_full_snapshot(
        &db.pool,
        sandbox_id,
        artifact_id,
        "NOW() - INTERVAL '5 minutes'",
    )
    .await?;

    let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
    let cleaner = Arc::new(FakePodCleaner::timeout());
    let sink = SubstrateActionSink::new_with_pod_cleaner(db.pool.clone(), cleaner.clone());
    let outcome = sink
        .classify_orphan(
            sandbox_id,
            ClassifyGuard::PodName {
                name: pod_name.clone(),
                uid: Some(PodUid::parse(format!(
                    "{}-uid",
                    sandbox_id.as_uuid().simple()
                ))?),
            },
        )
        .await?;

    assert_eq!(
        outcome,
        ClassifyOutcome::RecoveredSuspended {
            from_lifecycle: SandboxLifecycle::Started,
        }
    );
    assert_eq!(cleaner.calls(), vec![pod_name]);
    let lifecycle: SandboxLifecycle =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(lifecycle, SandboxLifecycle::Suspended);
    let active_execution_count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*)::BIGINT
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(active_execution_count, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn same_name_new_uid_recreation_skips_classification_without_cleanup() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let artifact_id = Uuid::now_v7();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend',
                last_command_at = NOW() - INTERVAL '10 minutes',
                last_access_at = NOW() - INTERVAL '10 minutes'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact_id)
    .execute(&db.pool)
    .await?;
    insert_ready_full_snapshot(
        &db.pool,
        sandbox_id,
        artifact_id,
        "NOW() - INTERVAL '5 minutes'",
    )
    .await?;

    let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
    let observed_uid = PodUid::parse(format!("{}-uid", sandbox_id.as_uuid().simple()))?;
    let replacement_uid = format!("{}-replacement-uid", sandbox_id.as_uuid().simple());
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET pod_uid = $3
          WHERE sandbox_id = $1 AND pod_name = $2 AND active"
    ))
    .bind(sandbox_id)
    .bind(&pod_name)
    .bind(&replacement_uid)
    .execute(&db.pool)
    .await?;

    let cleaner = Arc::new(FakePodCleaner::new(K8sBlockingDeleteOutcome::Deleted));
    let sink = SubstrateActionSink::new_with_pod_cleaner(db.pool.clone(), cleaner.clone());
    let outcome = sink
        .classify_orphan(
            sandbox_id,
            ClassifyGuard::PodName {
                name: pod_name.clone(),
                uid: Some(observed_uid.clone()),
            },
        )
        .await?;

    assert_eq!(outcome, ClassifyOutcome::SkippedTerminal);
    assert!(cleaner.calls().is_empty());
    let (lifecycle, active_node_id): (String, Option<NodeId>) = sqlx::query_as(sql!(
        "SELECT lifecycle, active_node_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(lifecycle, "started");
    assert_eq!(active_node_id, Some(node_id));
    let active_execution_uid: Option<String> = sqlx::query_scalar(sql!(
        "SELECT pod_uid
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(active_execution_uid, Some(replacement_uid));
    let quarantine_count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*)::BIGINT FROM sandbox_quarantine_state WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(quarantine_count, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn fresh_materialization_lease_skips_classification_without_cleanup() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let artifact_id = Uuid::now_v7();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend',
                last_command_at = NOW() - INTERVAL '10 minutes',
                last_access_at = NOW() - INTERVAL '10 minutes'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact_id)
    .execute(&db.pool)
    .await?;
    insert_ready_full_snapshot(
        &db.pool,
        sandbox_id,
        artifact_id,
        "NOW() - INTERVAL '5 minutes'",
    )
    .await?;

    let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
    let observed_uid = PodUid::parse(format!("{}-uid", sandbox_id.as_uuid().simple()))?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'creating',
                pod_uid = NULL,
                pod_ip = NULL,
                updated_at = NOW()
          WHERE sandbox_id = $1 AND generation = 1 AND active AND pod_name = $2"
    ))
    .bind(sandbox_id)
    .bind(&pod_name)
    .execute(&db.pool)
    .await?;

    let cleaner = Arc::new(FakePodCleaner::new(K8sBlockingDeleteOutcome::Deleted));
    let sink = SubstrateActionSink::new_with_pod_cleaner(db.pool.clone(), cleaner.clone());
    let outcome = sink
        .classify_orphan(
            sandbox_id,
            ClassifyGuard::PodName {
                name: pod_name.clone(),
                uid: Some(observed_uid),
            },
        )
        .await?;

    assert_eq!(outcome, ClassifyOutcome::SkippedTerminal);
    assert!(cleaner.calls().is_empty());
    let (lifecycle, active_node_id): (String, Option<NodeId>) = sqlx::query_as(sql!(
        "SELECT lifecycle, active_node_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(lifecycle, "started");
    assert_eq!(active_node_id, Some(node_id));
    let (active_execution_state, active_execution_uid): (String, Option<String>) =
        sqlx::query_as(sql!(
            "SELECT state, pod_uid
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
        ))
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(active_execution_state, "creating");
    assert_eq!(active_execution_uid, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn lifecycle_race_to_suspending_skips_classification_without_cleanup() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let artifact_id = Uuid::now_v7();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'suspending',
                current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend',
                last_command_at = NOW() - INTERVAL '10 minutes',
                last_access_at = NOW() - INTERVAL '10 minutes'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact_id)
    .execute(&db.pool)
    .await?;
    insert_ready_full_snapshot(
        &db.pool,
        sandbox_id,
        artifact_id,
        "NOW() - INTERVAL '5 minutes'",
    )
    .await?;

    let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
    let pod_uid = Some(PodUid::parse(format!(
        "{}-uid",
        sandbox_id.as_uuid().simple()
    ))?);
    let cleaner = Arc::new(FakePodCleaner::new(K8sBlockingDeleteOutcome::Deleted));
    let sink = SubstrateActionSink::new_with_pod_cleaner(db.pool.clone(), cleaner.clone());
    let outcome = sink
        .classify_orphan(
            sandbox_id,
            ClassifyGuard::PodName {
                name: pod_name,
                uid: pod_uid,
            },
        )
        .await?;

    assert_eq!(outcome, ClassifyOutcome::SkippedTerminal);
    assert!(cleaner.calls().is_empty());
    let (lifecycle, active_node_id): (String, Option<NodeId>) = sqlx::query_as(sql!(
        "SELECT lifecycle, active_node_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(lifecycle, "suspending");
    assert_eq!(active_node_id, Some(node_id));
    let active_execution_count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*)::BIGINT
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(active_execution_count, 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn classify_missing_pod_with_stale_artifact_quarantines_and_preserves_row() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let artifact_id = Uuid::now_v7();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend',
                last_command_at = NOW(),
                last_access_at = NOW()
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact_id)
    .execute(&db.pool)
    .await?;
    insert_ready_full_snapshot(
        &db.pool,
        sandbox_id,
        artifact_id,
        "NOW() - INTERVAL '10 minutes'",
    )
    .await?;
    // Reproduce the poison-loop shape: an open exposure whose apply
    // observation failed on the (now pod-less) node, plus the node's
    // pending sandbox-desired-state queue ref that keeps retrying it.
    sqlx::query(sql!(
        "INSERT INTO sandbox_port_exposures (
             sandbox_id, protocol, guest_port, visibility, source,
             desired_state, apply_state, last_applied_node_id, last_error
         )
         VALUES ($1, 'tcp', 8080, 'private', 'api', 'open', 'failed', $2,
                 'expose port: pod network namespace is gone')"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    let desired_state_ref = ReconcileElementRef::sandbox_desired_state(sandbox_id);
    sqlx::query(sql!(
        "INSERT INTO node_reconcile_queue (node_id, facet, element, attempts, last_error)
         VALUES ($1, $2, $3, 12, 'local sandbox state unavailable')"
    ))
    .bind(node_id)
    .bind(desired_state_ref.facet_str())
    .bind(desired_state_ref.element_string())
    .execute(&db.pool)
    .await?;

    let sink = SubstrateActionSink::new_without_substrate(db.pool.clone());
    let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
    let pod_uid = Some(PodUid::parse(format!(
        "{}-uid",
        sandbox_id.as_uuid().simple()
    ))?);
    let guard = ClassifyGuard::PodName {
        name: pod_name,
        uid: pod_uid,
    };
    let outcome = sink.classify_orphan(sandbox_id, guard).await?;

    assert_eq!(
        outcome,
        ClassifyOutcome::Quarantined {
            from_lifecycle: SandboxLifecycle::Started,
        }
    );
    let lifecycle: SandboxLifecycle =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(lifecycle, SandboxLifecycle::Quarantined);
    let sandbox_count: i64 =
        sqlx::query_scalar(sql!("SELECT COUNT(*)::BIGINT FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(sandbox_count, 1);
    // The old node assignment is retired so the desired-state query stops
    // returning this sandbox (and its exposures) for the old node.
    let (placement, active_node_id, target_node_id): (String, Option<NodeId>, Option<NodeId>) =
        sqlx::query_as(sql!(
            "SELECT placement, active_node_id, target_node_id
               FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(placement, "unplaced");
    assert_eq!(active_node_id, None);
    assert_eq!(target_node_id, None);
    // Durable user intent survives; only the application observation is
    // reset so a later operator recovery re-applies the exposure.
    let (desired_state, apply_state, last_applied_node_id, last_error): (
        String,
        String,
        Option<NodeId>,
        Option<String>,
    ) = sqlx::query_as(sql!(
        "SELECT desired_state, apply_state, last_applied_node_id, last_error
           FROM sandbox_port_exposures WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(desired_state, "open");
    assert_eq!(apply_state, "pending");
    assert_eq!(last_applied_node_id, None);
    assert_eq!(last_error, None);
    // The poison queue ref is removed in the same transaction.
    let queue_refs: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*)::BIGINT FROM node_reconcile_queue
          WHERE facet = $1 AND element = $2"
    ))
    .bind(desired_state_ref.facet_str())
    .bind(desired_state_ref.element_string())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(queue_refs, 0);
    let quarantine: (String, String, Option<NodeId>) = sqlx::query_as(sql!(
        "SELECT source, reason_code, last_observed_node_id
           FROM sandbox_quarantine_state
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        quarantine.0,
        SandboxQuarantineSource::ClusterController.as_str()
    );
    assert_eq!(
        quarantine.1,
        SandboxQuarantineReasonCode::OrphanedExecutionMissingArtifact.as_str()
    );
    assert_eq!(quarantine.2, Some(node_id));
    let history_count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*)::BIGINT FROM sandbox_quarantine_history WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(history_count, 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn classify_legacy_deleting_finalizes_delete() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    insert_legacy_deleting_sandbox(&db.pool, sandbox_id).await?;

    let sink = SubstrateActionSink::new_without_substrate(db.pool.clone());
    let outcome = sink
        .classify_orphan(
            sandbox_id,
            ClassifyGuard::LegacyDeleting {
                pod_name: Some("legacy-pod".to_string()),
            },
        )
        .await?;

    assert_eq!(
        outcome,
        ClassifyOutcome::Deleted {
            from_lifecycle: SandboxLifecycle::Deleting,
            reason: OrphanDeleteReason::ExplicitDeleting,
        }
    );
    let sandbox_count: i64 =
        sqlx::query_scalar(sql!("SELECT COUNT(*)::BIGINT FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(sandbox_count, 0);
    Ok(())
}

#[test]
fn pod_presence_classifies_running_pod_with_ip_as_present() {
    let pod = pod_with_phase("Running", Some("10.90.0.8"));

    let status = classify_pod_presence(&SandboxPodView::from(pod));

    assert_eq!(status, PodPresenceStatus::Present);
}

#[test]
fn pod_presence_classifies_pending_pod_as_unready() {
    let pod = pod_with_phase("Pending", None);

    let status = classify_pod_presence(&SandboxPodView::from(pod));

    assert_eq!(status, PodPresenceStatus::Unready);
}

#[test]
fn pod_presence_classifies_succeeded_pod_with_terminated_containers() {
    let pod = pod_with_phase_and_container_state("Succeeded", terminated_container_state());

    let status = classify_pod_presence(&SandboxPodView::from(pod));

    assert_eq!(status, PodPresenceStatus::TerminalSucceeded);
}

#[test]
fn pod_presence_classifies_failed_pod_with_terminated_containers() {
    let pod = pod_with_phase_and_container_state("Failed", terminated_container_state());

    let status = classify_pod_presence(&SandboxPodView::from(pod));

    assert_eq!(status, PodPresenceStatus::TerminalFailed);
}

#[test]
fn pod_presence_requires_container_statuses_for_terminal_phase() {
    let pod = pod_with_phase("Succeeded", None);

    let status = classify_pod_presence(&SandboxPodView::from(pod));

    assert_eq!(status, PodPresenceStatus::Unready);
}

#[test]
fn pod_presence_rejects_terminal_phase_with_live_container_state() {
    let running = pod_with_phase_and_container_state("Succeeded", running_container_state());
    let waiting = pod_with_phase_and_container_state("Succeeded", waiting_container_state());

    assert_eq!(
        classify_pod_presence(&SandboxPodView::from(running)),
        PodPresenceStatus::Unready
    );
    assert_eq!(
        classify_pod_presence(&SandboxPodView::from(waiting)),
        PodPresenceStatus::Unready
    );
}

#[tokio::test]
async fn grace_period_holds_action_until_threshold() {
    let cand = candidate("ghost-1");
    let sink = FakeSink::new(vec![cand.clone()]);
    let presence = FakePresence::default();
    let mut r = Reconciler::new(config(60, 1000, false), presence, sink);

    let t0 = Instant::now();
    let r0 = r.tick(t0).await;
    assert_eq!(r0.within_grace, 1);
    assert_eq!(r0.recovered_suspended, 0);
    assert_eq!(r.sink.action_count(), 0);

    let r1 = r.tick(t0 + Duration::from_secs(30)).await;
    assert_eq!(r1.within_grace, 1);
    assert_eq!(r.sink.action_count(), 0);

    let r2 = r.tick(t0 + Duration::from_secs(70)).await;
    assert_eq!(r2.within_grace, 0);
    assert_eq!(r2.recovered_suspended, 1);
    assert_eq!(r.sink.action_count(), 1);
}

#[tokio::test]
async fn pod_returning_resets_grace_clock() {
    let cand = candidate("ghost-flap");
    let sink = FakeSink::new(vec![cand.clone()]);
    let mut r = Reconciler::new(config(60, 1000, false), FakePresence::default(), sink);

    let t0 = Instant::now();
    r.tick(t0).await;
    r.tick(t0 + Duration::from_secs(30)).await;

    // Pod comes back at t=40s.
    r.presence = FakePresence::with_present([cand.pod_name.clone()]);
    let r_back = r.tick(t0 + Duration::from_secs(40)).await;
    assert_eq!(r_back.pod_present, 1);
    assert!(r.first_missing_at.is_empty());

    // Pod disappears again at t=50s. The grace clock must restart from
    // here, so a tick at t=80s (40s after re-disappearance) is still
    // within grace.
    r.presence = FakePresence::default();
    let r_gone = r.tick(t0 + Duration::from_secs(50)).await;
    assert_eq!(r_gone.within_grace, 1);
    let r_partial = r.tick(t0 + Duration::from_secs(80)).await;
    assert_eq!(r_partial.within_grace, 1);
    assert_eq!(r.sink.action_count(), 0);

    // ...and a tick at t=120s (70s after re-disappearance) is past it.
    let r_done = r.tick(t0 + Duration::from_secs(120)).await;
    assert_eq!(r_done.recovered_suspended, 1);
    assert_eq!(r.sink.action_count(), 1);
}

#[tokio::test]
async fn dry_run_reports_candidate_but_performs_no_action() {
    let cand = candidate("ghost-dry");
    let sink = FakeSink::new(vec![cand.clone()]);
    let mut r = Reconciler::new(config(0, 1000, true), FakePresence::default(), sink);

    let r0 = r.tick(Instant::now()).await;
    assert_eq!(r0.dry_run_classified, 1);
    assert_eq!(r.sink.action_count(), 0);
}

#[tokio::test]
async fn rate_limit_caps_per_minute() {
    let cands: Vec<_> = (0..200).map(|i| candidate(&format!("ghost-{i}"))).collect();
    let sink = FakeSink::new(cands);
    let mut r = Reconciler::new(config(0, 60, false), FakePresence::default(), sink);

    let report = r.tick(Instant::now()).await;
    assert_eq!(report.recovered_suspended, 60);
    assert_eq!(report.rate_limited, 140);
    assert_eq!(r.sink.action_count(), 60);
}

#[tokio::test]
async fn quarantine_outcome_is_counted_separately() {
    let cand = candidate("ghost-quarantine");
    let sink = FakeSink::new(vec![cand.clone()]);
    sink.quarantine(cand.sandbox_id);
    let mut r = Reconciler::new(config(0, 1000, false), FakePresence::default(), sink);

    let report = r.tick(Instant::now()).await;
    assert_eq!(report.recovered_suspended, 0);
    assert_eq!(report.quarantined, 1);
    assert_eq!(r.sink.action_count(), 1);
}

#[tokio::test]
async fn quarantine_circuit_breaker_stops_mutations_at_configured_ceiling() {
    let candidates: Vec<_> = (0..25)
        .map(|index| candidate(&format!("ambiguous-{index}")))
        .collect();
    let sink = FakeSink::new(candidates.clone());
    for candidate in &candidates {
        sink.quarantine(candidate.sandbox_id);
    }
    let mut reconciler = Reconciler::new(
        ReconcilerConfig {
            max_new_quarantines_per_sweep: 3,
            ..config(0, 1000, false)
        },
        FakePresence::default(),
        sink,
    );

    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    let report = reconciler.tick(Instant::now()).await;
    let counters = counters.finish();

    assert_eq!(report.quarantined, 3);
    assert_eq!(report.circuit_breaker, 22);
    assert_eq!(reconciler.sink.action_count(), 3);
    assert!(reconciler.circuit_open);
    assert_eq!(
        classified_counter_count(&counters, "outcome:quarantine_blocked", "mode:live"),
        22
    );
    assert_eq!(
        classified_counter_count(&counters, "outcome:quarantined", "mode:live"),
        3
    );
}

#[tokio::test]
async fn fresh_reconciler_honors_persisted_active_quarantine_ceiling() {
    let candidate = candidate("ambiguous-after-restart");
    let sink = FakeSink::new(vec![candidate.clone()]);
    sink.quarantine(candidate.sandbox_id);
    sink.set_active_quarantine_count(DEFAULT_MAX_ACTIVE_QUARANTINES);
    let mut reconciler = Reconciler::new(config(0, 1000, false), FakePresence::default(), sink);

    let report = reconciler.tick(Instant::now()).await;

    assert_eq!(report.quarantined, 0);
    assert_eq!(report.circuit_breaker, 1);
    assert_eq!(reconciler.sink.action_count(), 0);
    assert!(reconciler.circuit_open);
}

#[tokio::test]
async fn active_quarantine_ceiling_does_not_block_recovery_or_deletion() {
    let mut deleting = candidate_with_id(1, "explicit-delete");
    deleting.kind = OrphanKind::LegacyDeleting;
    deleting.lifecycle = SandboxLifecycle::Deleting;
    let recovery = candidate_with_id(2, "recoverable");
    let ambiguous = candidate_with_id(3, "ambiguous");
    let sink = FakeSink::new(vec![deleting, recovery, ambiguous.clone()]);
    sink.quarantine(ambiguous.sandbox_id);
    sink.set_active_quarantine_count(DEFAULT_MAX_ACTIVE_QUARANTINES);
    let mut reconciler = Reconciler::new(config(0, 1000, false), FakePresence::default(), sink);

    let report = reconciler.tick(Instant::now()).await;

    assert_eq!(report.deleted, 1);
    assert_eq!(report.recovered_suspended, 1);
    assert_eq!(report.quarantined, 0);
    assert_eq!(report.circuit_breaker, 1);
    assert_eq!(reconciler.sink.action_count(), 2);
}

#[tokio::test]
async fn quarantine_percentage_uses_strict_threshold_and_excludes_deferrals() {
    let candidates: Vec<_> = (1..=23)
        .map(|id| candidate_with_id(id, &format!("candidate-{id}")))
        .collect();
    let sink = FakeSink::new(candidates.clone());
    for candidate in candidates.iter().skip(15).take(2) {
        sink.defer(candidate.sandbox_id);
    }
    for candidate in candidates.iter().skip(17) {
        sink.quarantine(candidate.sandbox_id);
    }
    let mut reconciler = Reconciler::new(
        ReconcilerConfig {
            max_quarantine_percentage: 25,
            ..config(0, 1000, false)
        },
        FakePresence::default(),
        sink,
    );

    let report = reconciler.tick(Instant::now()).await;

    assert_eq!(report.recovered_suspended, 15);
    assert_eq!(report.deferred, 2);
    assert_eq!(report.quarantined, 5);
    assert_eq!(report.circuit_breaker, 1);
}

#[tokio::test]
async fn dry_run_shadow_evaluates_active_quarantine_ceiling() {
    let ambiguous = candidate("dry-run-ambiguous");
    let sink = FakeSink::new(vec![ambiguous.clone()]);
    sink.quarantine(ambiguous.sandbox_id);
    sink.set_active_quarantine_count(DEFAULT_MAX_ACTIVE_QUARANTINES);
    let mut reconciler = Reconciler::new(config(0, 1000, true), FakePresence::default(), sink);

    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    let report = reconciler.tick(Instant::now()).await;
    let counters = counters.finish();

    assert_eq!(report.dry_run_classified, 1);
    assert_eq!(report.circuit_breaker, 1);
    assert_eq!(reconciler.sink.action_count(), 0);
    assert_eq!(
        classified_counter_count(&counters, "outcome:quarantine_blocked", "mode:dry_run"),
        1
    );
    assert_eq!(
        classified_counter_count(&counters, "outcome:quarantined", "mode:dry_run"),
        0
    );
}

#[tokio::test]
async fn pod_present_skips_action_and_clears_state() {
    let cand = candidate("ghost-then-back");
    let sink = FakeSink::new(vec![cand.clone()]);
    let mut r = Reconciler::new(config(60, 1000, false), FakePresence::default(), sink);

    let t0 = Instant::now();
    r.tick(t0).await;
    assert_eq!(r.first_missing_at.len(), 1);

    r.presence = FakePresence::with_present([cand.pod_name.clone()]);
    let report = r.tick(t0 + Duration::from_secs(10)).await;
    assert_eq!(report.pod_present, 1);
    assert_eq!(report.recovered_suspended, 0);
    assert!(r.first_missing_at.is_empty());
}

#[tokio::test]
async fn stale_creating_with_present_pod_is_cleaned_after_its_attempt_expires() {
    let mut cand = candidate("abandoned-create-present");
    cand.lifecycle = SandboxLifecycle::Creating;
    let sink = FakeSink::new(vec![cand.clone()]);
    let mut reconciler = Reconciler::new(
        config(0, 1000, false),
        FakePresence::with_present([cand.pod_name.clone()]),
        sink,
    );

    let report = reconciler.tick(Instant::now()).await;

    assert_eq!(report.deleted, 1);
    assert_eq!(reconciler.sink.action_count(), 1);
}

#[tokio::test]
async fn pod_unready_uses_grace_then_classifies() {
    let cand = candidate("stuck-container-creating");
    let sink = FakeSink::new(vec![cand.clone()]);
    let mut r = Reconciler::new(
        config(60, 1000, false),
        FakePresence::with_unready([cand.pod_name.clone()]),
        sink,
    );

    let t0 = Instant::now();
    let first = r.tick(t0).await;
    assert_eq!(first.pod_unready, 1);
    assert_eq!(first.within_grace, 1);
    assert_eq!(first.recovered_suspended, 0);
    assert_eq!(r.sink.action_count(), 0);

    let second = r.tick(t0 + Duration::from_secs(70)).await;
    assert_eq!(second.pod_unready, 1);
    assert_eq!(second.within_grace, 0);
    assert_eq!(second.recovered_suspended, 1);
    assert_eq!(r.sink.action_count(), 1);
}

#[tokio::test]
async fn pod_unknown_skips_action_and_keeps_state_for_retry() {
    let cand = candidate("ghost-unknown");
    let sink = FakeSink::new(vec![cand.clone()]);
    let mut r = Reconciler::new(config(60, 1000, false), FakePresence::default(), sink);

    let t0 = Instant::now();
    let missing = r.tick(t0).await;
    assert_eq!(missing.within_grace, 1);
    assert!(r.first_missing_at.contains_key(&cand.sandbox_id));

    r.presence = FakePresence::with_unknown([cand.pod_name.clone()]);
    let report = r.tick(t0 + Duration::from_secs(30)).await;
    assert_eq!(report.pod_unknown, 1);
    assert_eq!(report.recovered_suspended, 0);
    assert_eq!(r.sink.action_count(), 0);
    assert!(r.first_missing_at.contains_key(&cand.sandbox_id));
}

#[tokio::test]
async fn classification_failure_keeps_first_missing_for_retry() {
    let cand = candidate("ghost-fail");
    let sink = FakeSink::new(vec![cand.clone()]);
    sink.fail_action(cand.sandbox_id);
    let mut r = Reconciler::new(config(0, 1000, false), FakePresence::default(), sink);

    let t0 = Instant::now();
    let report = r.tick(t0).await;
    assert_eq!(report.recovered_suspended, 0);
    assert!(r.first_missing_at.contains_key(&cand.sandbox_id));
    assert!(r.action_history.is_empty());

    r.sink.clear_action_failures();
    let retry = r.tick(t0 + Duration::from_secs(1)).await;
    assert_eq!(retry.recovered_suspended, 1);
    assert_eq!(r.sink.action_count(), 1);
    assert!(r.first_missing_at.is_empty());
}

#[tokio::test]
async fn scan_cursor_advances_past_healthy_first_page() {
    let healthy = candidate_with_id(1, "healthy-first");
    let orphan = candidate_with_id(2, "orphan-second");
    let ordered = vec![healthy.clone(), orphan.clone()];
    let sink = FakeSink::new(ordered);
    let mut r = Reconciler::new(
        ReconcilerConfig {
            batch_size: 1,
            ..config(0, 1000, false)
        },
        FakePresence::with_present([healthy.pod_name.clone()]),
        sink,
    );

    let t0 = Instant::now();
    let first = r.tick(t0).await;
    assert_eq!(first.scanned, 1);
    assert_eq!(first.pod_present, 1);
    assert_eq!(r.sink.action_count(), 0);

    r.presence = FakePresence::with_present([healthy.pod_name]);
    let second = r.tick(t0 + Duration::from_secs(1)).await;
    assert_eq!(second.scanned, 1);
    assert_eq!(second.recovered_suspended, 1);
    assert_eq!(r.sink.action_count(), 1);
    assert_eq!(r.sink.actions.lock().unwrap()[0].0, orphan.sandbox_id);
}

#[tokio::test]
async fn forgotten_applies_grace_and_uses_forgotten_guard() {
    // Forgotten row: API path already cleared the active execution.
    // Reconciler has no pod_name to check_pod, so presence is skipped,
    // but grace MUST still be applied — between scan and classification a
    // legitimate state transition (e.g. into 'suspended') or recovery
    // could land. The `Forgotten` guard re-checks both inside the
    // UPDATE.
    let forgotten = forgotten_candidate();
    let sink = FakeSink::new(vec![forgotten.clone()]);
    let mut r = Reconciler::new(config(60, 1000, false), FakePresence::default(), sink);

    let t0 = Instant::now();
    let r0 = r.tick(t0).await;
    assert_eq!(r0.within_grace, 1, "first observation enters grace");
    assert_eq!(r0.recovered_suspended, 0);

    let r1 = r.tick(t0 + Duration::from_secs(70)).await;
    assert_eq!(
        r1.recovered_suspended, 1,
        "after grace, classification proceeds"
    );
    let recorded = r.sink.actions.lock().unwrap();
    assert_eq!(recorded[0].0, forgotten.sandbox_id);
    assert_eq!(recorded[0].1, ClassifyGuard::Forgotten);
}

#[tokio::test]
async fn legacy_deleting_skips_presence_but_waits_for_grace() {
    let mut deleting = legacy_deleting_candidate();
    deleting.pod_name = "legacy-pod".to_string();
    let sink = FakeSink::new(vec![deleting.clone()]);
    let presence = FakePresence::with_unknown([String::new()]);
    let mut r = Reconciler::new(config(60, 1000, false), presence, sink);

    let now = Instant::now();
    let first = r.tick(now).await;
    assert_eq!(first.deleted, 0);
    assert_eq!(first.within_grace, 1);
    assert_eq!(first.pod_unknown, 0);
    assert_eq!(r.sink.action_count(), 0);

    let after_grace = r.tick(now + Duration::from_secs(61)).await;
    assert_eq!(after_grace.deleted, 1);
    assert_eq!(after_grace.within_grace, 0);
    assert_eq!(after_grace.pod_unknown, 0);
    let recorded = r.sink.actions.lock().unwrap();
    assert_eq!(recorded[0].0, deleting.sandbox_id);
    assert_eq!(
        recorded[0].1,
        ClassifyGuard::LegacyDeleting {
            pod_name: Some("legacy-pod".to_string())
        }
    );
}

#[tokio::test]
async fn classify_orphan_passes_pod_name_for_normal_path() {
    // Normal orphan path: reconciler should pass the observed pod_name
    // via `ClassifyGuard::PodName(...)` so the sink's UPDATE can guard
    // against a concurrent wake that swapped in a new active execution
    // with a different pod_name.
    let cand = candidate("ghost-witness");
    let sink = FakeSink::new(vec![cand.clone()]);
    let mut r = Reconciler::new(config(0, 1000, false), FakePresence::default(), sink);

    let report = r.tick(Instant::now()).await;
    assert_eq!(report.recovered_suspended, 1);
    let recorded = r.sink.actions.lock().unwrap();
    assert_eq!(recorded[0].0, cand.sandbox_id);
    assert_eq!(
        recorded[0].1,
        ClassifyGuard::PodName {
            name: "ghost-witness".to_string(),
            uid: Some(PodUid::parse("ghost-witness-uid").unwrap()),
        }
    );
}

#[tokio::test]
async fn new_pod_uid_same_name_resets_grace_clock() {
    // Same-generation pod recreation: `ensure_pod_exists` reuses the
    // active execution row (same pod_name) but the new pod gets a new
    // pod_uid via `persist_pod_metadata`. If the new pod is also
    // missing on the next tick (e.g., still being scheduled), the
    // reconciler must restart its grace clock — not reuse the
    // timestamp from the prior incarnation.
    let cand_a = OrphanCandidate {
        sandbox_id: SandboxId::from_uuid(Uuid::from_u128(7)),
        pod_name: "pod-x".to_string(),
        pod_uid: Some(PodUid::parse("uid-a").unwrap()),
        kind: OrphanKind::Normal,
        lifecycle: SandboxLifecycle::Started,
    };
    let sink = FakeSink::new(vec![cand_a]);
    let mut r = Reconciler::new(config(60, 1000, false), FakePresence::default(), sink);

    let t0 = Instant::now();
    let r0 = r.tick(t0).await;
    assert_eq!(r0.within_grace, 1);
    assert_eq!(r0.recovered_suspended, 0);

    // ensure_pod_exists recreates: same pod_name, new pod_uid.
    *r.sink.candidates.lock().unwrap() = vec![OrphanCandidate {
        sandbox_id: SandboxId::from_uuid(Uuid::from_u128(7)),
        pod_name: "pod-x".to_string(),
        pod_uid: Some(PodUid::parse("uid-b").unwrap()),
        kind: OrphanKind::Normal,
        lifecycle: SandboxLifecycle::Started,
    }];

    // Long after t0 — but this is the first observation of the new
    // incarnation. Grace clock must restart, not carry t0 forward.
    let r1 = r.tick(t0 + Duration::from_secs(120)).await;
    assert_eq!(r1.within_grace, 1, "new pod_uid restarts grace");
    assert_eq!(r1.recovered_suspended, 0);
    assert_eq!(r.sink.action_count(), 0);
}

#[tokio::test]
async fn normal_path_with_null_pod_uid_still_classifies() {
    // Active execution exists with pod_name set but pod_uid still NULL
    // (pod creation never reached `persist_pod_metadata`, e.g. crashed
    // mid-create or legacy row). The reconciler must NOT silently skip
    // these — that would regress orphan classification for legacy or
    // crashed-mid-create rows. The UPDATE's NULL-safe
    // `IS NOT DISTINCT FROM` handles it:
    // we observed NULL, the UPDATE confirms NULL, the row is classified.
    let cand = OrphanCandidate {
        sandbox_id: SandboxId::new(),
        pod_name: "ghost-no-uid".to_string(),
        pod_uid: None,
        kind: OrphanKind::Normal,
        lifecycle: SandboxLifecycle::Creating,
    };
    let sink = FakeSink::new(vec![cand.clone()]);
    let mut r = Reconciler::new(config(0, 1000, false), FakePresence::default(), sink);

    let report = r.tick(Instant::now()).await;
    assert_eq!(report.deleted, 1);
    let recorded = r.sink.actions.lock().unwrap();
    assert_eq!(recorded[0].0, cand.sandbox_id);
    assert_eq!(
        recorded[0].1,
        ClassifyGuard::PodName {
            name: "ghost-no-uid".to_string(),
            uid: None,
        }
    );
}

#[test]
fn normal_pod_cleanup_uses_observed_name_after_classification() {
    assert_eq!(
        ClassifyGuard::PodName {
            name: "space-sbx-a-g1".to_string(),
            uid: Some(PodUid::parse("uid-a").unwrap()),
        }
        .observed_pod_cleanup_name(),
        Some("space-sbx-a-g1".to_string())
    );
    assert_eq!(
        ClassifyGuard::PodName {
            name: "space-sbx-a-g1".to_string(),
            uid: None,
        }
        .observed_pod_cleanup_name(),
        Some("space-sbx-a-g1".to_string())
    );
}

#[test]
fn legacy_deleting_cleanup_target_preserves_name_only_delete() {
    assert_eq!(
        ClassifyGuard::LegacyDeleting {
            pod_name: Some("space-sbx-a-g1".to_string()),
        }
        .observed_pod_cleanup_name(),
        Some("space-sbx-a-g1".to_string())
    );
    assert_eq!(
        ClassifyGuard::LegacyDeleting { pod_name: None }.observed_pod_cleanup_name(),
        None
    );
}

#[tokio::test]
async fn new_pod_name_resets_grace_clock_across_executions() {
    // Sandbox X was observed Missing on pod-a at t0 (within grace).
    // Then the user stops/suspends X (X drops out of the candidate set
    // because the active execution is cleared). Later they start X
    // again with a new active execution pod-b. The reconciler should
    // restart the grace clock for pod-b — not reuse pod-a's stale
    // timestamp from t0, which would skip grace and classify a healthy
    // sandbox on the first observation.
    let cand_a = candidate_with_id(1, "pod-a");
    let sink = FakeSink::new(vec![cand_a]);
    let mut r = Reconciler::new(config(60, 1000, false), FakePresence::default(), sink);

    let t0 = Instant::now();
    let r0 = r.tick(t0).await;
    assert_eq!(r0.within_grace, 1);
    assert_eq!(r0.recovered_suspended, 0);

    // Restart: same sandbox_id, new pod_name.
    *r.sink.candidates.lock().unwrap() = vec![candidate_with_id(1, "pod-b")];

    // Long after t0 — but pod-b has only just been observed for the
    // first time. Without the per-pod_name reset, this tick would see
    // 120s > 60s grace and immediately classify the row.
    let r1 = r.tick(t0 + Duration::from_secs(120)).await;
    assert_eq!(
        r1.within_grace, 1,
        "new pod_name must restart the grace clock"
    );
    assert_eq!(r1.recovered_suspended, 0);
    assert_eq!(r.sink.action_count(), 0);
}

#[tokio::test]
async fn high_water_mark_excludes_rows_created_mid_sweep() {
    // v7 UUID IDs are time-ordered. Without a per-sweep high-water mark,
    // continuous sandbox creation extends the cursor's right edge
    // forever and older candidates that were within grace, rate-limited,
    // or failed classification never get revisited. The bound captured at
    // sweep start excludes new rows so the sweep makes progress.
    let cands: Vec<_> = (1u128..=4)
        .map(|i| candidate_with_id(i, &format!("g-{i}")))
        .collect();
    let sink = FakeSink::new(cands);
    let mut r = Reconciler::new(
        // dry_run so the fake's candidate list isn't mutated by actions;
        // we want to observe sweep state, not action bookkeeping.
        ReconcilerConfig {
            batch_size: 2,
            ..config(0, 1000, true)
        },
        FakePresence::default(),
        sink,
    );

    let t0 = Instant::now();
    let r0 = r.tick(t0).await;
    assert_eq!(r0.scanned, 2, "first page A,B");
    assert_eq!(
        r.sweep_upper_bound,
        Some(SandboxId::from_uuid(Uuid::from_u128(4)))
    );

    // New sandbox created mid-sweep, id=5 > upper_bound=4.
    r.sink
        .candidates
        .lock()
        .unwrap()
        .push(candidate_with_id(5, "g-5"));

    let r1 = r.tick(t0 + Duration::from_secs(1)).await;
    assert_eq!(r1.scanned, 2, "second page C,D — E excluded by upper bound");

    // Cursor reached upper bound; tick wraps and retries with a fresh
    // bound that now includes id=5. So scanned > 0.
    let r2 = r.tick(t0 + Duration::from_secs(2)).await;
    assert!(
        r2.scanned > 0,
        "after wrap, fresh sweep picks up newly-created rows"
    );
    assert_eq!(
        r.sweep_upper_bound,
        Some(SandboxId::from_uuid(Uuid::from_u128(5)))
    );
}

#[tokio::test]
async fn first_missing_at_is_garbage_collected_on_sweep_wrap() {
    // Sandbox A is observed missing alongside sandbox B → first_missing_at
    // gains both entries. Then A disappears from the candidate set
    // through another path. Without GC, first_missing_at would retain A's
    // stale tuple forever. With GC, a full sweep that doesn't see A
    // drops the stale entry; B (still in the set) is retained.
    let cand_a = candidate_with_id(1, "ghost-a");
    let cand_b = candidate_with_id(2, "ghost-b");
    let sink = FakeSink::new(vec![cand_a.clone(), cand_b.clone()]);
    let mut r = Reconciler::new(
        ReconcilerConfig {
            batch_size: 10,
            ..config(120, 1000, false)
        },
        FakePresence::default(),
        sink,
    );

    // Sweep 1 — A and B both observed, sweep 1 wraps the next tick.
    let t0 = Instant::now();
    r.tick(t0).await; // page 1 returns [A, B]
    r.tick(t0 + Duration::from_secs(1)).await; // page 2 empty → wrap
    assert!(
        r.first_missing_at.contains_key(&cand_a.sandbox_id),
        "A's grace clock was set during sweep 1"
    );
    assert!(
        r.first_missing_at.contains_key(&cand_b.sandbox_id),
        "B's grace clock was set during sweep 1"
    );

    // A vanishes between sweeps.
    r.sink
        .candidates
        .lock()
        .unwrap()
        .retain(|c| c.sandbox_id != cand_a.sandbox_id);

    // Sweep 2 — only B is in the candidate set; sweep 2's wrap GCs A.
    r.tick(t0 + Duration::from_secs(2)).await; // page 1 returns [B]
    r.tick(t0 + Duration::from_secs(3)).await; // page 2 empty → wrap → GC

    assert!(
        !r.first_missing_at.contains_key(&cand_a.sandbox_id),
        "A's stale grace clock was pruned on sweep 2's wrap"
    );
    assert!(
        r.first_missing_at.contains_key(&cand_b.sandbox_id),
        "B's grace clock survives because B was seen in sweep 2"
    );
}

#[tokio::test]
async fn empty_table_returns_default_without_panic() {
    let sink = FakeSink::new(vec![]);
    let mut r = Reconciler::new(config(0, 1000, false), FakePresence::default(), sink);

    let report = r.tick(Instant::now()).await;
    assert_eq!(report, TickReport::default());
    assert!(r.sweep_upper_bound.is_none());
}

#[tokio::test]
async fn skipped_terminal_does_not_count_as_recovered_suspended() {
    // Sink reports SkippedTerminal — e.g., the predicated UPDATE
    // matched zero rows because a concurrent wake replaced the active
    // execution. The reconciler should not count this as an action or
    // consume rate-limit budget.
    let cand = candidate("ghost-raced");
    let sink = FakeSink::new(vec![cand.clone()]);
    sink.skip_terminal(cand.sandbox_id);
    let mut r = Reconciler::new(config(0, 1, false), FakePresence::default(), sink);

    let report = r.tick(Instant::now()).await;
    assert_eq!(report.skipped_terminal, 1);
    assert_eq!(report.recovered_suspended, 0);
    assert_eq!(r.sink.action_count(), 0);
    // first_missing_at cleared so the row isn't held forever in state.
    assert!(r.first_missing_at.is_empty());
}

#[test]
fn classification_rate_limit_from_env_clamps_to_a_finite_ceiling() {
    const RATE: &str = "SPACE_API_RECONCILER_MAX_CLASSIFICATIONS_PER_MINUTE";
    let rate = |env: &pplx_space_test_env::TestEnv| {
        ReconcilerConfig::from_environment(env).max_classifications_per_minute
    };
    let mut env = pplx_space_test_env::TestEnv::default();

    env.set(RATE, "10");
    assert_eq!(rate(&env), 10);

    for over in ["60000", "99999999999999999999"] {
        env.set(RATE, over);
        assert_eq!(
            rate(&env),
            MAX_CLASSIFICATIONS_PER_MINUTE_CEILING,
            "{over:?}"
        );
    }

    for invalid in ["0", "-5", "sixty", "5000000000abc"] {
        env.set(RATE, invalid);
        assert_eq!(
            rate(&env),
            DEFAULT_MAX_CLASSIFICATIONS_PER_MINUTE,
            "{invalid:?}"
        );
    }
}

#[test]
fn quarantine_breakers_from_env_clamp_to_finite_ceilings() {
    const ACTIVE: &str = "SPACE_API_RECONCILER_MAX_ACTIVE_QUARANTINES";
    const PER_SWEEP: &str = "SPACE_API_RECONCILER_MAX_NEW_QUARANTINES_PER_SWEEP";
    const PERCENTAGE: &str = "SPACE_API_RECONCILER_MAX_QUARANTINE_PERCENTAGE";
    let breakers = |env: &pplx_space_test_env::TestEnv| {
        let cfg = ReconcilerConfig::from_environment(env);
        (
            cfg.max_active_quarantines,
            cfg.max_new_quarantines_per_sweep,
            cfg.max_quarantine_percentage,
        )
    };
    let set_all = |env: &mut pplx_space_test_env::TestEnv, value: &str| {
        for var in [ACTIVE, PER_SWEEP, PERCENTAGE] {
            env.set(var, value);
        }
    };
    let mut env = pplx_space_test_env::TestEnv::default();
    let defaults = (
        DEFAULT_MAX_ACTIVE_QUARANTINES,
        DEFAULT_MAX_NEW_QUARANTINES_PER_SWEEP,
        DEFAULT_MAX_QUARANTINE_PERCENTAGE,
    );

    set_all(&mut env, "7");
    assert_eq!(breakers(&env), (7, 7, 7));

    // `0` is the kill switch, not a request for the default.
    set_all(&mut env, "0");
    assert_eq!(breakers(&env), (0, 0, 0));

    // However many digits a typo adds, a breaker can't clamp to "no limit".
    let ceilings = (
        MAX_ACTIVE_QUARANTINES_CEILING,
        MAX_NEW_QUARANTINES_PER_SWEEP_CEILING,
        MAX_QUARANTINE_PERCENTAGE_CEILING,
    );
    for over in ["5000", "5000000000", "99999999999999999999"] {
        set_all(&mut env, over);
        assert_eq!(breakers(&env), ceilings, "{over:?}");
    }

    for invalid in ["", "-5", "fifty", "50 ", "5000000000abc"] {
        set_all(&mut env, invalid);
        assert_eq!(breakers(&env), defaults, "{invalid:?}");
    }
}

#[test]
fn config_from_env_uses_defaults_when_unset() {
    let env = pplx_space_test_env::TestEnv::default();
    let cfg = ReconcilerConfig::from_environment(&env);
    assert_eq!(cfg.interval, DEFAULT_INTERVAL);
    assert_eq!(cfg.grace_period, DEFAULT_GRACE_PERIOD);
    assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
    assert_eq!(
        cfg.max_classifications_per_minute,
        DEFAULT_MAX_CLASSIFICATIONS_PER_MINUTE
    );
    assert_eq!(cfg.max_active_quarantines, DEFAULT_MAX_ACTIVE_QUARANTINES);
    assert_eq!(
        cfg.max_new_quarantines_per_sweep,
        DEFAULT_MAX_NEW_QUARANTINES_PER_SWEEP
    );
    assert_eq!(
        cfg.max_quarantine_percentage,
        DEFAULT_MAX_QUARANTINE_PERCENTAGE
    );
    // The loop is enabled by default, while fresh deployments remain dry-run.
    assert!(cfg.enabled);
    // Dry-run defaults to true so a fresh deployment doesn't write.
    assert!(cfg.dry_run);
}

#[test]
fn config_from_env_reads_classification_rate_limit() {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set("SPACE_API_RECONCILER_MAX_CLASSIFICATIONS_PER_MINUTE", "11");

    let cfg = ReconcilerConfig::from_environment(&env);
    assert_eq!(cfg.max_classifications_per_minute, 11);
}

/// The candidate scan and the sweep's upper bound both bind the Kubernetes
/// substrate: an otherwise-orphaned direct row neither bounds the sweep nor
/// is listed.
#[tokio::test(flavor = "current_thread")]
async fn direct_rows_do_not_bound_the_sweep_or_become_candidates() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    let sink = SubstrateActionSink::new_without_substrate(db.pool.clone());
    let upper = SandboxId::from_uuid(Uuid::max());
    assert_eq!(sink.current_max_sandbox_id().await?, Some(sandbox_id));
    assert_eq!(
        list_candidates_after(&db.pool, None, upper, 10)
            .await?
            .len(),
        1,
        "the k8s fixture is a candidate"
    );

    sqlx::query(sql!(
        "UPDATE sandboxes SET managed_by = 'space' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    assert_eq!(sink.current_max_sandbox_id().await?, None);
    assert!(
        list_candidates_after(&db.pool, None, upper, 10)
            .await?
            .is_empty()
    );
    Ok(())
}

/// A `creating` shell reached through the **Forgotten** guard must be claimable.
///
/// `AbandonedCreate` is decided from lifecycle and recoverability alone, so it is
/// reached through either guard, and `ACTIVE_EXECUTION_REQUIRED_LIFECYCLES`
/// documents why `creating` belongs to the Forgotten path: a failed pod creation
/// calls `forget_pod_tracking` while the row is still `creating`, leaving a
/// non-recoverable shell with no active execution.
///
/// An earlier revision's claim began `let ClassifyGuard::PodName { .. } = guard
/// else { return Ok(false) }`, so this shape was refused, the plan transaction
/// rolled back, and the sweep reported `SkippedTerminal` on every pass — forever,
/// with no other owner. The existing Forgotten test could not see it: it uses a
/// `Started` lifecycle and a fake sink, so it exercises neither `Creating` nor
/// this claim.
///
/// Asserting the claim directly, against Postgres, because that is where the
/// guard mismatch lived.
#[tokio::test]
async fn abandoned_create_claim_accepts_the_forgotten_guard() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id).await?;
    insert_started_sandbox_with_active_execution(&db.pool, sandbox_id, node_id).await?;
    // The post-`forget_pod_tracking` shape: still `creating`, newest execution
    // `forgotten`, nothing active.
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'creating' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'forgotten', active = FALSE
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let mut tx = db.pool.begin().await?;
    let claimed =
        claim_abandoned_create_delete(&mut tx, sandbox_id, &ClassifyGuard::Forgotten).await?;
    tx.commit().await?;
    assert!(
        claimed,
        "the Forgotten guard must be claimable, or this shape is skipped on every \
         sweep with nothing else owning it"
    );
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(
        lifecycle, "deleting",
        "the claim's whole purpose is durable delete intent on the row"
    );

    // The other half: a row with an *active* execution is a live create, not an
    // abandoned shell, and must not be claimed through this guard.
    let live_sandbox_id = SandboxId::new();
    insert_started_sandbox_with_active_execution(&db.pool, live_sandbox_id, node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'creating' WHERE id = $1"
    ))
    .bind(live_sandbox_id)
    .execute(&db.pool)
    .await?;
    let mut tx = db.pool.begin().await?;
    let live_claimed =
        claim_abandoned_create_delete(&mut tx, live_sandbox_id, &ClassifyGuard::Forgotten).await?;
    tx.rollback().await?;
    assert!(
        !live_claimed,
        "an active execution means a live create; claiming it would delete a \
         sandbox still being built"
    );
    Ok(())
}
