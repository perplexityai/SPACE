use pplx_space_common_core::types::{LocalArtifactId, NodeId, SandboxId, SnapshotId};

use std::time::Duration;

use pplx_space_db_test_fixtures::{TestDb, pg_test_db_with_max_connections};
use pplx_space_sandbox_portability::portable_for_restore_affinity;
use time::OffsetDateTime;

use super::*;

use pplx_space_test_env::TestResult;

mod cleanup_recovery_lifecycle_matrix_tests;
mod lifecycle_writer_tests;
mod materialization_record_tests;
mod owned_restore_completion_tests;
mod restore_revert_tests;
mod runtime_upgrade_evidence_tests;
mod started_recovery_observation_tests;
mod stopped_evacuation_claim_tests;
mod stopped_restore_selector_tests;
mod substrate_scope_tests;
mod suspend_lifecycle_matrix_tests;
use owned_restore_completion_tests::owned_restore_completion_requires_its_lease_and_a_routable_execution;
use restore_revert_tests::{
    failed_restore_revert_retires_only_its_own_proven_execution,
    without_retirement_a_failed_restore_revert_moves_only_the_lifecycle,
};
use started_recovery_observation_tests::started_recovery_followers_succeed_only_on_a_started_sandbox;
use stopped_evacuation_claim_tests::{
    a_fresh_stopped_claim_is_not_rolled_back_on_a_clean_failure,
    evacuation_claim_takes_only_a_released_stopped_evacuation,
};
use stopped_restore_selector_tests::{
    stopped_disk_selector_agrees_with_the_retention_never_prune_guard,
    stopped_disk_selector_ignores_a_full_artifact_and_a_foreign_disk_artifact,
    stopped_disk_selector_requires_the_pinned_ready_disk_artifact,
};
type ForgottenRestoreRow = (
    String,
    String,
    Option<Uuid>,
    Option<Uuid>,
    Option<OffsetDateTime>,
    Option<OffsetDateTime>,
    Option<Uuid>,
);
type ForgottenSuspendedRow = (
    String,
    String,
    Option<NodeId>,
    Option<NodeId>,
    Option<OffsetDateTime>,
    bool,
    Option<LocalArtifactId>,
);

/// Typed reader for the `sandboxes` x `sandbox_k8s_executions` join keyed by
/// `(sandbox_id, generation)`. Selecting every column the pod-absence
/// convergence behaviors touch, rather than the 2-3 columns any one test
/// asserts on, means one `insta` snapshot pins the full row shape instead of
/// a hand-picked subset.
#[derive(Debug, sqlx::FromRow, serde::Serialize)]
struct ExecutionByGenerationRow {
    lifecycle: String,
    state: String,
    active: bool,
    pod_uid: Option<String>,
}

async fn execution_by_generation_row(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
) -> TestResult<ExecutionByGenerationRow> {
    Ok(sqlx::query_as(sql!(
        "SELECT s.lifecycle, e.state, e.active, e.pod_uid
           FROM sandboxes s
           JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
          WHERE s.id = $1 AND e.generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(pool)
    .await?)
}

/// Typed reader for a `sandboxes` row's restore-lifecycle columns, keyed by
/// `id`. Covers the `begin_restore_lifecycle` / `prepare_wake_sandbox`
/// behavior chain, where individual tests each asserted 1-3 of these columns
/// by hand.
#[derive(Debug, sqlx::FromRow, serde::Serialize)]
struct SandboxLeaseRow {
    lifecycle: String,
    evacuation_restore_target: Option<String>,
    restore_lease_token: Option<Uuid>,
}

async fn sandbox_lease_row(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<SandboxLeaseRow> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, evacuation_restore_target, restore_lease_token
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// Typed reader for a `sandboxes` row's placement columns, keyed by `id`.
#[derive(Debug, sqlx::FromRow, serde::Serialize)]
struct SandboxPlacementRow {
    lifecycle: String,
    placement: String,
    active_node_id: Option<NodeId>,
}

async fn sandbox_placement_row(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<SandboxPlacementRow> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, placement, active_node_id
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

struct PostgresStore {
    store: K8sStore,
    pool: PgPool,
    _db: TestDb,
}

async fn postgres_store() -> TestResult<PostgresStore> {
    postgres_store_with_max_connections(2).await
}

async fn postgres_store_with_max_connections(max_connections: u32) -> TestResult<PostgresStore> {
    let db = pg_test_db_with_max_connections(max_connections).await?;
    let pool = db.pool.clone();
    Ok(PostgresStore {
        store: K8sStore::new(pool.clone(), "space".to_string()),
        pool,
        _db: db,
    })
}

async fn insert_sandbox(pool: &PgPool, sandbox_id: SandboxId, lifecycle: &str) -> TestResult {
    sqlx::query(
        sql!("INSERT INTO sandboxes (id, user_id, org_id, lifecycle, placement, managed_by, network_policy_override_json, network_policy_generation)
         VALUES (
            $1,
            '00000000-0000-0000-0000-000000000010',
            '00000000-0000-0000-0000-000000000001',
            $2,
            'unplaced',
            'k8s',
            $3,
            1
         )"),
    )
    .bind(sandbox_id)
    .bind(lifecycle)
    .bind(sqlx::types::Json(
        &pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// **A stopped evacuation counts as unplaced only once its local copy is gone.**
///
/// The flag exists because `restore_source_node_name` does not fail closed
/// here. After a drain's teardown clears both placement columns, the executions
/// fallback still answers with the drained node's `k8s_node_name` — retiring an
/// execution marks the row inactive without deleting it — so the plan builder
/// needs to be told to refuse rather than trust that name.
///
/// The other direction is the one that made an earlier revision wrong: scoping
/// it to the marker alone would also catch a claimed-but-not-torn-down row,
/// which still has its copy and whose marker nothing in the fleet ever clears.
/// Both are asserted on one fixture, differing only in placement.
#[tokio::test]
async fn a_stopped_evacuation_counts_as_unplaced_only_once_its_local_copy_is_gone() -> TestResult {
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "stopped").await?;

    // The executions row the fallback answers from, and the drained node it
    // names — this is the value that would become `spec.nodeName`.
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.51'::inet, 'worker', 'k8s', 'registered')"
    ))
    .bind(node_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, k8s_node_name, state, active)
         VALUES ($1, 1, $2, 'space', 'ip-10-0-0-51.ec2.internal', 'retired', FALSE)"
    ))
    .bind(sandbox_id)
    .bind(K8sStore::execution_pod_name(sandbox_id, 1))
    .execute(&fixture.pool)
    .await?;

    // Claimed but not torn down: still on its node, so a `start` is fine.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = 'stopped',
                active_node_id = $2,
                placement = 'on_node'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&fixture.pool)
    .await?;
    let plan = fixture.store.restore_source_plan(sandbox_id).await?;
    assert!(
        !plan.stopped_evacuation_unplaced,
        "a claimed-but-placed sandbox still has its rootfs on its node, and nothing clears \
         the marker, so it must not read as evacuated"
    );

    // Torn down: placement cleared, marker still set. Now the refusal is right.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = NULL, target_node_id = NULL, placement = 'unplaced'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    let plan = fixture.store.restore_source_plan(sandbox_id).await?;
    assert!(
        plan.stopped_evacuation_unplaced,
        "an evacuation that has destroyed the local copy must read as unplaced — otherwise \
         the start is planned against a node that is about to disappear"
    );
    assert_eq!(
        plan.node_name.as_deref(),
        Some("ip-10-0-0-51.ec2.internal"),
        "and the fallback still answers with the drained node, which is exactly why the \
         flag cannot be left to a missing node name"
    );

    // Mid-re-placement: `target_node_id` set, `active_node_id` still NULL. This
    // is the phase that distinguishes `active_node_id IS NULL` from
    // `COALESCE(target_node_id, active_node_id) IS NULL`, and without it nothing
    // in the suite does — every other phase moves the two columns together, so
    // "harmonising" the refusal onto the same `COALESCE` the WARN's
    // `placement_node_id` uses would look like tidying an oversight and would
    // leave this test green. It would also stop the refusal firing exactly here,
    // handing the caller a pod bound to the drained node instead of a 503.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = NULL, target_node_id = $2, placement = 'unplaced'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&fixture.pool)
    .await?;
    let plan = fixture.store.restore_source_plan(sandbox_id).await?;
    assert!(
        plan.stopped_evacuation_unplaced,
        "a re-placement in flight — target chosen, nothing active yet — must still read as \
         unplaced: the local copy is gone, and the target is not serving the sandbox yet"
    );
    assert_eq!(
        plan.node_name.as_deref(),
        Some("ip-10-0-0-51.ec2.internal"),
        "and the name must resolve through the `node_k8s` mapping for the chosen target \
         rather than the executions fallback — which also means the missing-mapping WARN \
         stays silent in this state, since a placement node id is present"
    );

    // Unmarked and unplaced: not this feature's business.
    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = NULL WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    let plan = fixture.store.restore_source_plan(sandbox_id).await?;
    assert!(
        !plan.stopped_evacuation_unplaced,
        "an unplaced sandbox with no marker is some other path's problem, and flagging it \
         here would refuse starts this feature never touched"
    );
    Ok(())
}

#[tokio::test]
async fn started_recovery_handoff_keeps_generation_when_local_source_is_not_allowed() -> TestResult
{
    // Arrange.
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let artifact_id = Uuid::now_v7();
    let activation_attempt_id = SandboxActivationAttemptId::new();
    insert_sandbox(
        &fixture.pool,
        sandbox_id,
        SandboxLifecycle::Started.as_str(),
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.10', 'worker', 'k8s', 'ready')"
    ))
    .bind(source_node_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name)
         VALUES ($1, 'source-node')"
    ))
    .bind(source_node_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots
             (sandbox_id, type, state, source_node_id, org_id,
              local_artifact_id, artifact_kind, artifact_origin)
         VALUES ($1, 'full', 'creating', $2,
                 '00000000-0000-0000-0000-000000000001',
                 $3, 'full', 'suspend')"
    ))
    .bind(sandbox_id)
    .bind(source_node_id)
    .bind(artifact_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_ip, state, active,
              activation_attempt_id, activation_attempt_expires_at,
              activation_attempt_target_state)
         VALUES ($1, 2, 'space-sbx-stale-g2', 'space', '127.0.0.1', 'recovering', TRUE,
                 $2, NOW() + INTERVAL '5 minutes', 'started')"
    ))
    .bind(sandbox_id)
    .bind(activation_attempt_id)
    .execute(&fixture.pool)
    .await?;

    // Act.
    let outcome = fixture
        .store
        .replace_started_recovery_execution(sandbox_id, ExecutionGeneration::new(2)?, None)
        .await?;

    // Assert.
    assert!(matches!(outcome, StartedRecoveryReplacement::Pending));
    let executions: Vec<(i64, String, bool, Option<Uuid>)> = sqlx::query_as(sql!(
        "SELECT generation, state, active, activation_attempt_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          ORDER BY generation"
    ))
    .bind(sandbox_id)
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(
        executions,
        vec![(
            2,
            ActiveExecutionState::Recovering.as_str().to_string(),
            true,
            Some(activation_attempt_id.as_uuid()),
        )]
    );
    Ok(())
}

#[tokio::test]
async fn started_recovery_rejects_suspend_snapshot_older_than_recorded_activity() -> TestResult {
    // Arrange.
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let artifact_id = Uuid::now_v7();
    let snapshot_id = SnapshotId::new();
    insert_sandbox(
        &fixture.pool,
        sandbox_id,
        SandboxLifecycle::Started.as_str(),
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.10', 'worker', 'k8s', 'ready')"
    ))
    .bind(source_node_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name)
         VALUES ($1, 'source-node')"
    ))
    .bind(source_node_id)
    .execute(&fixture.pool)
    .await?;
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
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, timestamp, source_node_id, org_id,
              local_artifact_id, artifact_kind, artifact_origin)
         VALUES ($1, $2, 'full', 'ready', NOW() - INTERVAL '1 minute', $3,
                 '00000000-0000-0000-0000-000000000001',
                 $4, 'full', 'suspend')"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(source_node_id)
    .bind(artifact_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, 's3://space-test/stale-suspend?region=us-east-1',
                 '00000000-0000-0000-0000-000000000001')"
    ))
    .bind(snapshot_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state, active)
         VALUES ($1, 2, 'space-sbx-stale-g2', 'space', 'recovering', TRUE)"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;

    // Act.
    let context = fixture
        .store
        .started_recovery_restore_context(sandbox_id, ExecutionGeneration::new(2)?)
        .await?;

    // Assert.
    assert!(matches!(
        context,
        StartedRecoveryRestoreContext::Unavailable
    ));
    Ok(())
}

#[tokio::test]
async fn started_recovery_treats_deleting_snapshot_as_unavailable() -> TestResult {
    // Arrange: a started sandbox whose marker-matched suspend snapshot is a
    // valid remote restore source.
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let artifact_id = Uuid::now_v7();
    let snapshot_id = SnapshotId::new();
    insert_sandbox(
        &fixture.pool,
        sandbox_id,
        SandboxLifecycle::Started.as_str(),
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.11', 'worker', 'k8s', 'ready')"
    ))
    .bind(source_node_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name)
         VALUES ($1, 'deleting-source-node')"
    ))
    .bind(source_node_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, timestamp, source_node_id, org_id,
              local_artifact_id, artifact_kind, artifact_origin)
         VALUES ($1, $2, 'full', 'ready', NOW(), $3,
                 '00000000-0000-0000-0000-000000000001',
                 $4, 'full', 'suspend')"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(source_node_id)
    .bind(artifact_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, 's3://space-test/deleting-suspend?region=us-east-1',
                 '00000000-0000-0000-0000-000000000001')"
    ))
    .bind(snapshot_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state, active)
         VALUES ($1, 2, 'space-sbx-deleting-g2', 'space', 'recovering', TRUE)"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;

    // Sanity: while the snapshot is `ready`, recovery has a remote source.
    let context = fixture
        .store
        .started_recovery_restore_context(sandbox_id, ExecutionGeneration::new(2)?)
        .await?;
    assert!(matches!(
        context,
        StartedRecoveryRestoreContext::Ready(StartedRecoveryRestoreSource::Remote { .. })
    ));

    // Act: a prune claim de-advertises the row; nothing else changes.
    sqlx::query(sql!(
        "UPDATE snapshots SET state = 'deleting' WHERE id = $1"
    ))
    .bind(snapshot_id)
    .execute(&fixture.pool)
    .await?;
    let context = fixture
        .store
        .started_recovery_restore_context(sandbox_id, ExecutionGeneration::new(2)?)
        .await?;

    // Assert: the claimed row must read as gone, not as a restore source.
    assert!(matches!(
        context,
        StartedRecoveryRestoreContext::Unavailable
    ));
    Ok(())
}

#[tokio::test]
async fn started_recovery_handoff_reports_lost_ownership() -> TestResult {
    // Arrange.
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(
        &fixture.pool,
        sandbox_id,
        SandboxLifecycle::Started.as_str(),
    )
    .await?;

    // Act.
    let outcome = fixture
        .store
        .replace_started_recovery_execution(sandbox_id, ExecutionGeneration::new(1)?, None)
        .await?;

    // Assert.
    assert!(matches!(outcome, StartedRecoveryReplacement::OwnershipLost));
    Ok(())
}

async fn finish_suspended_execution_for_test(
    fixture: &PostgresStore,
    sandbox_id: SandboxId,
) -> TestResult<super::super::super::PublishedLocalArtifact> {
    let claim = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    let pod_uid = Uuid::now_v7().to_string();
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET pod_uid = COALESCE(pod_uid, $3)
          WHERE sandbox_id = $1
            AND generation = $2
            AND active"
    ))
    .bind(sandbox_id)
    .bind(claim.generation)
    .bind(&pod_uid)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'running'
          WHERE sandbox_id = $1
            AND generation = $2
            AND active"
    ))
    .bind(sandbox_id)
    .bind(claim.generation)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspending' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    let execution = fixture
        .store
        .active_execution_status(sandbox_id)
        .await?
        .expect("test execution must remain active");
    let generation = ExecutionGeneration::new(execution.generation)?;
    let pod_uid = execution
        .pod_uid
        .expect("test execution must have a pod UID");
    let published_artifact = super::super::super::PublishedLocalArtifact {
        id: LocalArtifactId::new(),
        kind: ArtifactKind::Full,
    };
    fixture
        .store
        .persist_or_accept_suspend_commit(sandbox_id, generation, &pod_uid, published_artifact)
        .await?;
    fixture
        .store
        .finish_suspended_execution(sandbox_id, generation, &pod_uid, published_artifact)
        .await?;
    Ok(published_artifact)
}

async fn insert_ready_full_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    uri: &str,
) -> TestResult<SnapshotId> {
    let snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, timestamp, org_id)
         VALUES ($1, $2, 'full', 'ready', NOW() - INTERVAL '1 minute',
                 '00000000-0000-0000-0000-000000000001')"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, '00000000-0000-0000-0000-000000000001')"
    ))
    .bind(snapshot_id)
    .bind(uri)
    .execute(pool)
    .await?;
    Ok(snapshot_id)
}

async fn set_restore_lease(pool: &PgPool, sandbox_id: SandboxId, ttl: &str) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() + $2::interval,
                restore_lease_token = $3
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(ttl)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn observe_user_restore_classifies_join_outcomes() -> TestResult {
    let fixture = postgres_store().await?;

    // A live markerless lease on a transient lifecycle: keep waiting.
    let leased = SandboxId::new();
    insert_sandbox(&fixture.pool, leased, "restoring").await?;
    set_restore_lease(&fixture.pool, leased, "90 seconds").await?;
    let observation = fixture.store.observe_user_restore(leased).await?;
    let UserRestoreObservation::InFlight { lease_remaining } = observation else {
        panic!("live lease must observe as in-flight, got {observation:?}");
    };
    assert!(
        lease_remaining > Duration::ZERO && lease_remaining <= Duration::from_secs(90),
        "lease remaining must sit within the lease TTL, got {lease_remaining:?}"
    );

    // An expired lease on a transient lifecycle: the claim CAS adjudicates.
    let lapsed = SandboxId::new();
    insert_sandbox(&fixture.pool, lapsed, "suspended").await?;
    set_restore_lease(&fixture.pool, lapsed, "-1 seconds").await?;
    assert_eq!(
        fixture.store.observe_user_restore(lapsed).await?,
        UserRestoreObservation::Claimable
    );
    let observations = fixture
        .store
        .observe_user_restores(&[leased, lapsed])
        .await?;
    assert!(matches!(
        observations.get(&leased),
        Some(UserRestoreObservation::InFlight { .. })
    ));
    assert_eq!(
        observations.get(&lapsed),
        Some(&UserRestoreObservation::Claimable)
    );

    // Started with a routable active execution: the owner finished.
    let routable = SandboxId::new();
    insert_sandbox(&fixture.pool, routable, "started").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_ip, state, active)
         VALUES ($1, 2, 'space-sbx-routable-g2', 'space', '127.0.0.1', 'running', TRUE)"
    ))
    .bind(routable)
    .execute(&fixture.pool)
    .await?;
    assert_eq!(
        fixture.store.observe_user_restore(routable).await?,
        UserRestoreObservation::Routable {
            generation: 2,
            pod_name: "space-sbx-routable-g2".to_string(),
            pod_ip: "127.0.0.1".to_string(),
        }
    );

    // Started without a routable execution and no live lease: the
    // started-without-restore-context shape — joining cannot help.
    let phantom = SandboxId::new();
    insert_sandbox(&fixture.pool, phantom, "started").await?;
    assert_eq!(
        fixture.store.observe_user_restore(phantom).await?,
        UserRestoreObservation::StartedUnroutable
    );

    // A lease written onto a `started` row is dropped by the
    // `sandboxes_clear_restore_lease` trigger, so the phantom shape stays
    // unjoinable even after a stray lease write.
    set_restore_lease(&fixture.pool, phantom, "90 seconds").await?;
    assert_eq!(
        fixture.store.observe_user_restore(phantom).await?,
        UserRestoreObservation::StartedUnroutable
    );

    // A marker-owned restore belongs to the drain; the claim path (which
    // understands takeover semantics) adjudicates instead of the join.
    let marked = SandboxId::new();
    insert_sandbox(&fixture.pool, marked, "restoring").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = 'paused',
                restore_lease_expires_at = NOW() + INTERVAL '90 seconds',
                restore_lease_token = $2
          WHERE id = $1"
    ))
    .bind(marked)
    .bind(Uuid::now_v7())
    .execute(&fixture.pool)
    .await?;
    assert_eq!(
        fixture.store.observe_user_restore(marked).await?,
        UserRestoreObservation::Claimable
    );

    Ok(())
}

fn user_fresh_restore_token(claim: RestoreClaim) -> Uuid {
    match claim {
        RestoreClaim::FreshFromSuspended {
            restore_lease_token: Some(token),
        } => token,
        other => panic!("expected user fresh restore claim, got {other:?}"),
    }
}

fn user_redrive_restore_token(claim: RestoreClaim) -> Uuid {
    match claim {
        RestoreClaim::RedroveRestoring {
            restore_lease_token: Some(token),
        } => token,
        other => panic!("expected user redrive restore claim, got {other:?}"),
    }
}

fn assert_evacuation_fresh_restore_claim(claim: RestoreClaim) {
    assert!(matches!(
        claim,
        RestoreClaim::FreshFromSuspended {
            restore_lease_token: None
        }
    ));
}

fn assert_evacuation_redrive_restore_claim(claim: RestoreClaim) {
    assert!(matches!(
        claim,
        RestoreClaim::RedroveRestoring {
            restore_lease_token: None
        }
    ));
}

#[test]
fn active_execution_state_round_trips() {
    for state in [
        ActiveExecutionState::Creating,
        ActiveExecutionState::Created,
        ActiveExecutionState::Recovering,
        ActiveExecutionState::RecoveryFailed,
        ActiveExecutionState::Running,
        ActiveExecutionState::Suspended,
    ] {
        let value = state.to_string();

        let parsed = value.parse::<ActiveExecutionState>();

        assert_eq!(value, state.as_str());
        assert_eq!(parsed, Ok(state));
    }
    "forgotten".parse::<ActiveExecutionState>().unwrap_err();
}

#[tokio::test(flavor = "current_thread")]
async fn missing_k8s_inventory_demotes_only_stale_ready_nodes() -> TestResult {
    let fixture = postgres_store().await?;
    let missing = Uuid::now_v7();
    let observed = Uuid::now_v7();
    let recent = Uuid::now_v7();
    let draining = Uuid::now_v7();
    let missing_name = format!("space-test-missing-{}", missing.simple());
    let observed_name = format!("space-test-observed-{}", observed.simple());
    let recent_name = format!("space-test-recent-{}", recent.simple());
    let draining_name = format!("space-test-draining-{}", draining.simple());

    for (node_id, node_name) in [
        (missing, missing_name.as_str()),
        (observed, observed_name.as_str()),
        (recent, recent_name.as_str()),
        (draining, draining_name.as_str()),
    ] {
        fixture
            .store
            .persist_k8s_node_inventory(
                node_name,
                &format!("uid-{node_id}"),
                NodeId::from_uuid(node_id),
                NodeRole::Worker,
                Some("10.0.0.20"),
                NodeStateWrite::Manage(NodeLifecycleState::Ready),
            )
            .await?;
    }

    sqlx::query(sql!(
        "UPDATE nodes
         SET last_heartbeat = NOW() - INTERVAL '1 hour'
         WHERE id = ANY($1::UUID[])"
    ))
    .bind(vec![missing, observed, draining])
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE node_k8s
         SET updated_at = NOW() - INTERVAL '1 hour'
         WHERE node_id = ANY($1::UUID[])"
    ))
    .bind(vec![missing, observed, draining])
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE nodes
         SET drain_phase = 'draining', drain_started_at = NOW()
         WHERE id = $1"
    ))
    .bind(draining)
    .execute(&fixture.pool)
    .await?;

    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "suspended").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET placement = 'on_node', active_node_id = $2, target_node_id = $2
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(missing)
    .execute(&fixture.pool)
    .await?;

    let changed_from_empty_snapshot = fixture
        .store
        .demote_unobserved_k8s_nodes(&[], Duration::from_secs(60), 10)
        .await?;
    assert_eq!(
        changed_from_empty_snapshot,
        Vec::<String>::new(),
        "an empty Kubernetes snapshot must not demote any nodes"
    );

    let changed = fixture
        .store
        .demote_unobserved_k8s_nodes(
            std::slice::from_ref(&observed_name),
            Duration::from_secs(60),
            10,
        )
        .await?;

    assert_eq!(changed, vec![missing_name]);
    let states: Vec<(Uuid, String)> = sqlx::query_as(sql!(
        "SELECT id, state
         FROM nodes
         WHERE id = ANY($1::UUID[])
         ORDER BY id"
    ))
    .bind(vec![missing, observed, recent, draining])
    .fetch_all(&fixture.pool)
    .await?;
    let state = |node_id| {
        states
            .iter()
            .find_map(|(id, state)| (*id == node_id).then_some(state.as_str()))
    };
    assert_eq!(state(missing), Some("registered"));
    assert_eq!(state(observed), Some("ready"));
    assert_eq!(state(recent), Some("ready"));
    assert_eq!(state(draining), Some("ready"));

    let placement: (Option<Uuid>, Option<Uuid>) = sqlx::query_as(sql!(
        "SELECT active_node_id, target_node_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(placement, (Some(missing), Some(missing)));
    Ok(())
}

/// The admin inventory listing derives node state from node-api *pod*
/// readiness, which lags Kubernetes Node readiness during a node-api
/// rollout. It is not the lifecycle owner, so its `InsertOnly` write must
/// preserve a live `ready` node's state even when it observes an unready
/// pod — otherwise it recreates the same competing-writer oscillation the
/// placement fix removed (`kube_node_sync` writes `ready`, the listing
/// clobbers `registered`, the next sync restores `ready`).
#[tokio::test(flavor = "current_thread")]
async fn inventory_listing_insert_only_does_not_downgrade_ready_node() -> TestResult {
    let fixture = postgres_store().await?;

    // The lifecycle owner (`kube_node_sync`) reconciles the node to `ready`.
    let node_id = NodeId::new();
    let node_name = format!("space-test-inventory-node-{}", node_id.as_uuid().simple());
    fixture
        .store
        .persist_k8s_node_inventory(
            &node_name,
            "uid-inv",
            node_id,
            NodeRole::Worker,
            Some("10.0.0.50"),
            NodeStateWrite::Manage(NodeLifecycleState::Ready),
        )
        .await?;

    // The inventory listing observes an unready node-api pod (rollout) and
    // writes with insert-only semantics.
    fixture
        .store
        .persist_k8s_node_inventory(
            &node_name,
            "uid-inv",
            node_id,
            NodeRole::Worker,
            Some("10.0.0.50"),
            NodeStateWrite::InsertOnly(NodeLifecycleState::from_ready(false)),
        )
        .await?;

    let state: String = sqlx::query_scalar(sql!("SELECT state FROM nodes WHERE id = $1"))
        .bind(node_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(
        state, "ready",
        "inventory listing must not downgrade a live ready node observed with an unready pod"
    );

    Ok(())
}

/// Pod placement must not own node lifecycle `state`. A live `ready`
/// node that has a pod placed on it must stay `ready`. The placement
/// writer previously clobbered `ready` -> `registered` on every pod
/// placement, and `kube_node_sync` then restored it on its next tick,
/// oscillating the ready-worker population and amplifying inventory
/// `changed=` churn.
#[tokio::test(flavor = "current_thread")]
async fn pod_placement_does_not_downgrade_ready_node_state() -> TestResult {
    let fixture = postgres_store().await?;

    // A live worker node reconciled to `ready` by the inventory owner.
    let node_id = NodeId::new();
    let node_name = format!("space-test-ready-node-{}", node_id.as_uuid().simple());
    fixture
        .store
        .persist_k8s_node_inventory(
            &node_name,
            "uid-ready",
            node_id,
            NodeRole::Worker,
            Some("10.0.0.30"),
            NodeStateWrite::Manage(NodeLifecycleState::Ready),
        )
        .await?;

    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;

    // A pod lands on the node.
    fixture
        .store
        .persist_pod_node_placement(
            sandbox_id,
            &node_name,
            "uid-ready",
            node_id,
            NodeRole::Worker,
            Some("10.0.0.30"),
        )
        .await?;

    let state: String = sqlx::query_scalar(sql!("SELECT state FROM nodes WHERE id = $1"))
        .bind(node_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(
        state, "ready",
        "pod placement must not downgrade a live ready node to registered"
    );

    // Placement with a CHANGED address exercises the conflict-update
    // branch (address differs, so the UPDATE actually fires): the address
    // is refreshed but `InsertOnly` still preserves the owner's `ready`
    // state.
    fixture
        .store
        .persist_pod_node_placement(
            sandbox_id,
            &node_name,
            "uid-ready",
            node_id,
            NodeRole::Worker,
            Some("10.0.0.99"),
        )
        .await?;
    let (state, address): (String, String) =
        sqlx::query_as(sql!("SELECT state, host(address) FROM nodes WHERE id = $1"))
            .bind(node_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(
        state, "ready",
        "conflict-update from placement must refresh address without downgrading state"
    );
    assert_eq!(
        address, "10.0.0.99",
        "placement must refresh the node address"
    );

    // A genuinely new node discovered by placement (no prior inventory)
    // still gets an initial `registered` state on INSERT.
    let fresh_id = NodeId::new();
    let fresh_name = format!("space-test-fresh-node-{}", fresh_id.as_uuid().simple());
    let fresh_sandbox = SandboxId::new();
    insert_sandbox(&fixture.pool, fresh_sandbox, "started").await?;
    fixture
        .store
        .persist_pod_node_placement(
            fresh_sandbox,
            &fresh_name,
            "uid-fresh",
            fresh_id,
            NodeRole::Worker,
            Some("10.0.0.31"),
        )
        .await?;
    let fresh_state: String = sqlx::query_scalar(sql!("SELECT state FROM nodes WHERE id = $1"))
        .bind(fresh_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(
        fresh_state, "registered",
        "placement of a brand-new node still sets an initial registered state"
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn materialization_claim_defers_when_suspend_commits_after_refresh() -> TestResult {
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(
        &fixture.pool,
        sandbox_id,
        SandboxLifecycle::Started.as_str(),
    )
    .await?;

    let refreshed = fixture.store.require_sandbox(sandbox_id).await?;
    assert_eq!(refreshed.lifecycle, SandboxLifecycle::Started.as_str());

    let mut suspend_tx = fixture.pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspending' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&mut *suspend_tx)
    .await?;

    let store = fixture.store.clone();
    let mut claim_task = tokio::spawn(async move {
        store
            .ensure_active_execution_claim_for_lifecycle(
                sandbox_id,
                SandboxLifecycle::Started,
                None,
                None,
            )
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut claim_task)
            .await
            .is_err(),
        "materialization claim must wait for the concurrent suspend row lock"
    );

    suspend_tx.commit().await?;
    let result = tokio::time::timeout(Duration::from_secs(5), claim_task).await??;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("materialization must defer after suspend commits"),
    };
    assert!(
        matches!(error, SubstrateError::SuspendInFlight { .. }),
        "expected retryable suspend ownership, got {error:?}"
    );

    let execution_count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        execution_count, 0,
        "deferred materialization must not allocate an execution generation"
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn split_operation_stopped_start_retry_preserves_route_proof() -> TestResult {
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "stopped").await?;
    let id = sandbox_id;
    let route_a = SandboxActivationAttemptId::new();
    let claim = fixture
        .store
        .ensure_active_execution_claim_for_lifecycle(
            id,
            SandboxLifecycle::Stopped,
            None,
            Some((route_a, SandboxTransitionTargetState::Started)),
        )
        .await?;
    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            claim.row.generation,
            Some("pod-proof"),
            Some("100.64.0.7"),
        )
        .await?;
    // Caller abandonment does not complete a stopped->started lifecycle; ownership
    // becomes reclaimable at the lease deadline, without rotating the route.
    assert!(
        !fixture
            .store
            .complete_activation_attempt(id, claim.row.generation, route_a)
            .await?
    );
    sqlx::query(sql!("UPDATE sandbox_k8s_executions SET activation_attempt_expires_at = NOW() - INTERVAL '1 second' WHERE sandbox_id = $1"))
        .bind(sandbox_id).execute(&fixture.pool).await?;
    let requested_b = SandboxActivationAttemptId::new_after(route_a).unwrap();
    let operation_b = fixture
        .store
        .assign_activation_attempt(
            id,
            claim.row.generation,
            requested_b,
            SandboxTransitionTargetState::Started,
        )
        .await?;
    assert_ne!(operation_b, route_a);
    let stored: (String, Option<Uuid>, Option<Uuid>) = sqlx::query_as(sql!(
        "SELECT s.lifecycle, e.route_activation_id, e.activation_attempt_id
           FROM sandboxes s JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        stored,
        (
            "stopped".to_string(),
            Some(route_a.as_uuid()),
            Some(operation_b.as_uuid())
        )
    );
    Ok(())
}

async fn selected_route_pod_ip_refresh_is_fenced_by_complete_route(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let route = SandboxActivationAttemptId::new();
    let operation = SandboxActivationAttemptId::new_after(route).unwrap();
    let pod_uid = PodUid::parse("selected-pod-uid")?;
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip,
             state, active, activation_attempt_id, route_activation_id
         ) VALUES ($1, 7, 'selected-pod', 'space', $2, '10.90.0.7',
                   'running', TRUE, $3, $4)"
    ))
    .bind(sandbox_id)
    .bind(pod_uid.as_str())
    .bind(operation.as_uuid())
    .bind(route.as_uuid())
    .execute(&fixture.pool)
    .await?;

    assert!(
        fixture
            .store
            .persist_selected_route_pod_ip(
                sandbox_id,
                7,
                "selected-pod",
                route,
                &pod_uid,
                PodIp::parse("10.90.0.8")?,
            )
            .await?,
        "the complete selected route may refresh its own dial address"
    );

    for (generation, pod_name, candidate_route, candidate_uid) in [
        (8, "selected-pod", route, pod_uid.clone()),
        (7, "replacement-pod", route, pod_uid.clone()),
        (
            7,
            "selected-pod",
            SandboxActivationAttemptId::new_after(operation).unwrap(),
            pod_uid.clone(),
        ),
        (
            7,
            "selected-pod",
            route,
            PodUid::parse("replacement-pod-uid")?,
        ),
    ] {
        assert!(
            !fixture
                .store
                .persist_selected_route_pod_ip(
                    sandbox_id,
                    generation,
                    pod_name,
                    candidate_route,
                    &candidate_uid,
                    PodIp::parse("10.90.0.99")?,
                )
                .await?,
            "stale selected-route evidence must not update the dial address"
        );
    }

    let persisted_ip: String = sqlx::query_scalar(sql!(
        "SELECT HOST(pod_ip)::TEXT
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(persisted_ip, "10.90.0.8");

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn k8s_store_postgres_behaviors() -> TestResult {
    let fixture = postgres_store().await?;

    ensure_active_execution_row_is_idempotent(&fixture).await?;
    pod_materialization_claim_publishes_fresh_lease(&fixture).await?;
    started_recovery_claim_is_bounded_and_preserves_its_marker(&fixture).await?;
    failed_started_recovery_converges_unplaced_sandbox_with_current_snapshot(&fixture).await?;
    failed_started_recovery_converges_after_authoritative_pod_absence(&fixture).await?;
    failed_started_recovery_after_pod_absence_refuses_stale_snapshot(&fixture).await?;
    started_recovery_claim_on_lost_sandbox_mints_no_execution(&fixture).await?;
    failed_started_recovery_pod_absence_is_fenced_against_late_materialization(&fixture).await?;
    failed_started_recovery_without_current_snapshot_stays_started(&fixture).await?;
    failed_started_recovery_with_orphaned_snapshot_stays_started(&fixture).await?;
    stale_failed_started_recovery_does_not_converge_newer_execution(&fixture).await?;
    failed_started_recovery_without_attempt_does_not_steal_owned_generation(&fixture).await?;
    failed_started_recovery_follows_owned_successor_generation(&fixture).await?;
    failed_started_recovery_with_materialized_pod_stays_started(&fixture).await?;
    failed_started_recovery_with_node_placement_stays_started(&fixture).await?;
    failed_started_recovery_with_routable_pod_stays_started(&fixture).await?;
    started_recovery_follows_superseding_execution(&fixture).await?;
    stale_creating_started_recovery_claim_is_reclaimed(&fixture).await?;
    finish_active_execution_if_generation_guards_against_chained_bumps(&fixture).await?;
    abandon_created_unmaterialized_create_claim_preserves_observed_or_materialized_claim(&fixture)
        .await?;
    abandon_unmaterialized_create_claim_retires_foreign_creating_claim(&fixture).await?;
    abandon_unmaterialized_create_claim_is_fenced_by_restore_lease_token(&fixture).await?;
    reset_active_execution_to_creating_reopens_a_scheduled_pinned_claim(&fixture).await?;
    transient_create_dodge_handoff_yields_fresh_claim(&fixture).await?;
    concurrent_handler_does_not_claim_g7_while_g6_create_is_in_flight(&fixture).await?;
    activation_attempt_assignment_is_monotonic_after_lease_expiry(&fixture).await?;
    route_less_activation_retains_completion_fence(&fixture).await?;
    completed_activation_cannot_release_a_later_clock_skewed_allocation(&fixture).await?;
    activation_attempt_assignment_adopts_route_less_legacy_resident_after_ack(&fixture).await?;
    suspend_superseded_activation_abandons_only_the_canonical_attempt(&fixture).await?;
    stale_generation_cannot_complete_or_abandon_reused_attempt(&fixture).await?;
    selected_route_pod_ip_refresh_is_fenced_by_complete_route(&fixture).await?;
    replacement_handler_claims_g7_after_lagging_g6_is_replaced(&fixture).await?;
    stale_generation_pod_metadata_does_not_update_new_active_execution(&fixture).await?;
    blank_pod_uid_decodes_as_absent_for_active_execution(&fixture).await?;
    persist_pod_metadata_normalizes_blank_pod_uid_to_null(&fixture).await?;
    persist_pod_metadata_reports_noop_when_retirement_commits_mid_write(&fixture).await?;
    persist_pod_metadata_reports_recovering_state_unchanged(&fixture).await?;
    forget_pod_tracking_if_generation_does_not_forget_newer_active_execution(&fixture).await?;
    forget_pod_tracking_if_generation_does_not_forget_successor_attempt(&fixture).await?;
    forget_pod_tracking_defers_to_suspend_finalization(&fixture).await?;
    explicit_teardown_forget_retires_execution_while_suspending(&fixture).await?;
    forget_pod_tracking_recovers_markerless_restoring_with_ready_snapshot(&fixture).await?;
    forget_pod_tracking_keeps_restoring_when_recovery_is_not_safe(&fixture).await?;
    forget_pod_tracking_recovers_started_suspended_by_node_after_current_backup(&fixture).await?;
    forget_pod_tracking_keeps_started_without_current_suspend_backup(&fixture).await?;
    unchanged_pod_metadata_does_not_touch_execution_row(&fixture).await?;
    unchanged_pod_node_placement_does_not_touch_current_rows(&fixture).await?;
    finishing_active_execution_preserves_active_node_for_replacement(&fixture).await?;
    finishing_active_execution_rolls_back_port_observation_failure(&fixture).await?;
    finish_suspended_execution_rejects_uncommitted_running_execution(&fixture).await?;
    suspend_commit_rejects_artifact_claimed_for_prune(&fixture).await?;
    finish_suspended_execution_rejects_mismatched_artifact(&fixture).await?;
    suspend_commit_finishes_after_exact_execution_is_forgotten(&fixture).await?;
    finish_suspended_execution_accepts_matching_projection_race(&fixture).await?;
    finish_suspended_execution_clears_active_node_but_preserves_target_node(&fixture).await?;
    finish_suspended_execution_deletes_bound_claimed_pool_slot(&fixture).await?;
    committed_suspend_artifact_belongs_to_the_active_execution(&fixture).await?;
    durable_suspend_snapshot_artifact_requires_this_suspends_snapshot(&fixture).await?;
    finish_suspended_execution_keeps_fresh_snapshot_portable(&fixture).await?;
    creating_snapshot_does_not_select_stale_remote_uri_until_ready(&fixture).await?;
    suspend_restore_selector_ignores_orphaned_storage_locations(&fixture).await?;
    stopped_disk_selector_requires_the_pinned_ready_disk_artifact(&fixture).await?;
    stopped_disk_selector_ignores_a_full_artifact_and_a_foreign_disk_artifact(&fixture).await?;
    stopped_disk_selector_agrees_with_the_retention_never_prune_guard(&fixture).await?;
    restoring_retry_uses_pre_restore_snapshot_when_late_branch_publishes_artifact(&fixture).await?;
    restoring_retry_refuses_prior_placement_snapshot_after_late_branch_artifact(&fixture).await?;
    restoring_retry_refuses_markerless_snapshot_after_late_branch_artifact(&fixture).await?;
    restoring_sandbox_selects_newest_ready_snapshot_after_placement_bump(&fixture).await?;
    forget_pod_tracking_keeps_restoring_with_late_branch_artifact(&fixture).await?;
    forget_pod_tracking_preserves_execution_history(&fixture).await?;
    forget_pod_tracking_deletes_bound_claimed_pool_slot(&fixture).await?;
    forget_pod_tracking_recovers_started_with_current_suspend_backup(&fixture).await?;
    forget_pod_tracking_clears_stale_marker_for_newest_current_suspend_backup(&fixture).await?;
    forget_pod_tracking_keeps_started_when_suspend_backup_is_unsafe(&fixture).await?;
    forget_pod_tracking_preserve_keeps_markerless_snapshot_selectable(&fixture).await?;
    begin_restore_lifecycle_preserves_retry_and_marker_claim_rules(&fixture).await?;
    evacuation_claim_takes_only_a_released_stopped_evacuation(&fixture).await?;
    a_fresh_stopped_claim_is_not_rolled_back_on_a_clean_failure(&fixture).await?;
    user_restore_lease_blocks_concurrent_redrive_until_expiry(&fixture).await?;
    expired_restore_lease_with_live_execution_stays_in_flight(&fixture).await?;
    clean_redrive_failure_releases_restore_lease_for_immediate_retry(&fixture).await?;
    stale_restore_rollback_is_guarded_by_lease_token(&fixture).await?;
    restore_rollback_requires_fresh_claim_and_clean_failure(&fixture).await?;
    failed_restore_revert_retires_only_its_own_proven_execution(&fixture).await?;
    without_retirement_a_failed_restore_revert_moves_only_the_lifecycle(&fixture).await?;
    owned_restore_completion_requires_its_lease_and_a_routable_execution(&fixture).await?;
    started_recovery_followers_succeed_only_on_a_started_sandbox(&fixture).await?;
    prepare_wake_preserves_restore_source_boundary(&fixture).await?;
    prepare_wake_sandbox_restores_suspended_lifecycle(&fixture).await?;
    restored_generation_accepts_late_pod_metadata(&fixture).await?;
    persist_k8s_node_inventory_stores_canonical_role(&fixture).await?;
    upsert_clears_drain_state_when_k8s_node_uid_changes(&fixture).await?;
    upsert_preserves_drain_state_when_k8s_node_uid_unchanged(&fixture).await?;
    persist_k8s_node_inventory_repairs_missing_and_stale_address(&fixture).await?;
    azrebalance_variant_uid_change_does_not_leak_stale_drain_state_or_drain_started_at(&fixture)
        .await?;
    persist_k8s_node_inventory_refuses_when_tombstoned(&fixture).await?;
    clear_command_ready_pending_is_a_cas_against_the_reaper(&fixture).await?;
    wake_signal_classifies_post_wake_flip_by_recency_not_lease(&fixture).await?;
    post_wake_update_does_not_resurrect_quarantined_sandbox(&fixture).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn started_recovery_does_not_recreate_execution_after_teardown() -> TestResult {
    // Arrange: recovery observed `started`, then waits behind teardown's
    // sandbox-row lock while teardown retires the active execution.
    let fixture = postgres_store_with_max_connections(5).await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;

    let mut teardown = fixture.pool.begin().await?;
    sqlx::query(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox_id)
        .fetch_one(&mut *teardown)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'deleting' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&mut *teardown)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
         SET state = 'terminated', active = FALSE, terminated_at = NOW()
         WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&mut *teardown)
    .await?;

    let recovery_store = fixture.store.clone();
    let recovery =
        tokio::spawn(async move { recovery_store.claim_started_recovery(sandbox_id).await });
    wait_for_blocked_query(
        &fixture.pool,
        "%SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE%",
        None,
    )
    .await?;

    // Act.
    teardown.commit().await?;
    let error = tokio::time::timeout(Duration::from_secs(5), recovery)
        .await??
        .expect_err("recovery must lose to the committed teardown lifecycle");

    // Assert.
    assert!(matches!(error, SubstrateError::Conflict { .. }));
    let active_executions: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(active_executions, 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn started_recovery_maps_concurrent_suspend_to_retryable_deferral() -> TestResult {
    // Arrange: access observed `started`, then waits behind suspend's
    // sandbox-row lock while suspend claims the authoritative lifecycle.
    let fixture = postgres_store_with_max_connections(5).await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;

    let mut suspend = fixture.pool.begin().await?;
    sqlx::query(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox_id)
        .fetch_one(&mut *suspend)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspending' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&mut *suspend)
    .await?;

    let recovery_store = fixture.store.clone();
    let recovery =
        tokio::spawn(async move { recovery_store.claim_started_recovery(sandbox_id).await });
    wait_for_blocked_query(
        &fixture.pool,
        "%SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE%",
        None,
    )
    .await?;

    // Act.
    suspend.commit().await?;
    let error = tokio::time::timeout(Duration::from_secs(5), recovery)
        .await??
        .expect_err("recovery must defer to the committed suspend lifecycle");

    // Assert: the atomic claim boundary preserves retryability and does not
    // allocate a successor after the caller's stale `started` read.
    assert!(matches!(error, SubstrateError::SuspendInFlight { .. }));
    let generations: Vec<i64> = sqlx::query_scalar(sql!(
        "SELECT generation
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          ORDER BY generation"
    ))
    .bind(sandbox_id)
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(generations, vec![1]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_claim_loses_to_cleanup_recovery_transitions() -> TestResult {
    for lifecycle in [SandboxLifecycle::Restoring, SandboxLifecycle::Started] {
        concurrent_claim_loses_to_cleanup_recovery(lifecycle).await?;
    }
    Ok(())
}

async fn concurrent_claim_loses_to_cleanup_recovery(lifecycle: SandboxLifecycle) -> TestResult {
    let fixture = postgres_store_with_max_connections(5).await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, lifecycle.as_str()).await?;
    match lifecycle {
        SandboxLifecycle::Restoring => {
            insert_ready_full_snapshot(
                &fixture.pool,
                sandbox_id,
                "s3://space-test/concurrent-markerless-cleanup?region=us-east-1",
            )
            .await?;
        }
        SandboxLifecycle::Started => {
            let artifact_id = LocalArtifactId::new();
            sqlx::query(sql!(
                "UPDATE sandboxes
                 SET current_local_artifact_id = $2,
                     current_local_artifact_kind = 'full',
                     current_local_artifact_origin = 'suspend'
                 WHERE id = $1"
            ))
            .bind(sandbox_id)
            .bind(artifact_id)
            .execute(&fixture.pool)
            .await?;
            insert_ready_full_snapshot_relative_to_lifecycle(
                &fixture.pool,
                sandbox_id,
                "s3://space-test/concurrent-started-cleanup?region=us-east-1",
                1,
                Some(artifact_id),
            )
            .await?;
        }
        _ => unreachable!("test only covers cleanup recovery lifecycles"),
    }
    let execution = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;

    let mut cleanup = fixture.pool.begin().await?;
    assert!(K8sStore::lock_sandbox_for_execution_mutation_in(&mut cleanup, sandbox_id).await?);
    assert!(
        fixture
            .store
            .finish_active_execution_generation_in(
                &mut cleanup,
                sandbox_id,
                execution.generation,
                "forgotten",
            )
            .await?
    );
    fixture
        .store
        .clear_pod_tracking_in(&mut cleanup, sandbox_id, PlacementClock::Bump)
        .await?;
    fixture
        .store
        .recover_markerless_restoring_after_execution_cleanup_in(&mut cleanup, sandbox_id)
        .await?;
    fixture
        .store
        .recover_started_after_suspend_cleanup_in(&mut cleanup, sandbox_id)
        .await?;

    let claim_store = fixture.store.clone();
    let claim = tokio::spawn(async move {
        claim_store
            .ensure_active_execution_claim_for_lifecycle(sandbox_id, lifecycle, None, None)
            .await
    });
    wait_for_blocked_query(
        &fixture.pool,
        "%SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE%",
        None,
    )
    .await?;

    cleanup.commit().await?;
    let claim_error = match tokio::time::timeout(Duration::from_secs(5), claim).await?? {
        Err(error) => error,
        Ok(_) => panic!("claim must observe the committed cleanup recovery"),
    };
    assert!(matches!(claim_error, SubstrateError::Conflict { .. }));

    let (recovered_lifecycle, active_executions): (String, i64) = sqlx::query_as(sql!(
        "SELECT s.lifecycle,
                (SELECT COUNT(*)
                   FROM sandbox_k8s_executions e
                  WHERE e.sandbox_id = s.id AND e.active)
         FROM sandboxes s
         WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(recovered_lifecycle, SandboxLifecycle::Suspended.as_str());
    assert_eq!(active_executions, 0);

    Ok(())
}

/// A dropped leader A's delayed create-claim cleanup must serialize against
/// a follower B's lease takeover of the same still-`creating` generation.
/// The two touch different rows (A retires the execution; B rewrites the
/// sandbox lease), so nothing forces an ordering unless A locks the sandbox
/// row `FOR UPDATE` before reading the token. Here B holds that lock open
/// while A's stale-token cleanup runs concurrently: A must block on the
/// lock, then read B's committed token and no-op, leaving B's adopted
/// generation intact. Without the lock, A retires B's claim on a stale
/// READ COMMITTED snapshot (the sequential fenced test cannot see this —
/// there B's token is already committed before A starts).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandon_unmaterialized_create_claim_serializes_with_lease_takeover() -> TestResult {
    // Arrange: a still-`creating` generation that leader A owns via its
    // restore lease token.
    let fixture = postgres_store_with_max_connections(5).await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "restoring").await?;
    let claim = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?;
    assert!(claim.created);
    let generation = claim.row.generation;
    let token_a = Uuid::now_v7();
    let token_b = Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_token = $2,
                restore_lease_expires_at = NOW() + INTERVAL '120 seconds'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(token_a)
    .execute(&fixture.pool)
    .await?;

    // Follower B takes over: lock the sandbox row and rewrite the lease to
    // token B, holding the transaction open across A's cleanup attempt.
    let mut takeover = fixture.pool.begin().await?;
    sqlx::query(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox_id)
        .fetch_one(&mut *takeover)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_token = $2,
                restore_lease_expires_at = NOW() + INTERVAL '120 seconds'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(token_b)
    .execute(&mut *takeover)
    .await?;

    // A's stale cleanup (its OLD token) runs concurrently; it must block on
    // the sandbox row lock B holds rather than retire on a stale snapshot.
    let cleanup_store = fixture.store.clone();
    let cleanup = tokio::spawn(async move {
        cleanup_store
            .abandon_unmaterialized_create_claim_by_generation(
                sandbox_id,
                generation,
                Some(token_a),
                None,
            )
            .await
    });
    wait_for_blocked_query(
        &fixture.pool,
        "%SELECT restore_lease_token FROM sandboxes WHERE id = $1 FOR UPDATE%",
        None,
    )
    .await?;

    // Act: B commits the takeover; A unblocks and reads token B.
    takeover.commit().await?;
    let cleanup_result = tokio::time::timeout(Duration::from_secs(5), cleanup).await??;

    // Assert: A no-ops (its token no longer matches) and B's adopted
    // generation survives; the current owner B can still retire it.
    assert!(
        !cleanup_result?,
        "A's stale-token cleanup must no-op after B's committed takeover"
    );
    assert!(
        fixture
            .store
            .active_execution_status(sandbox_id)
            .await?
            .is_some(),
        "B's adopted generation must survive A's serialized cleanup"
    );
    assert!(
        fixture
            .store
            .abandon_unmaterialized_create_claim_by_generation(
                sandbox_id,
                generation,
                Some(token_b),
                None,
            )
            .await?,
        "the current lease owner B must still be able to retire the generation"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_pod_placement_and_cleanup_do_not_deadlock() -> TestResult {
    // Arrange.
    let fixture = postgres_store_with_max_connections(5).await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let execution = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    let node_id = NodeId::new();
    let mut sandbox_blocker = fixture.pool.begin().await?;
    sqlx::query(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox_id)
        .fetch_one(&mut *sandbox_blocker)
        .await?;

    let cleanup_store = fixture.store.clone();
    let cleanup = tokio::spawn(async move {
        cleanup_store
            .forget_pod_tracking_if_generation(
                sandbox_id,
                execution.generation,
                PodTrackingCleanupCause::MissingPodObservation,
            )
            .await
    });
    wait_for_blocked_query(
        &fixture.pool,
        "%SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE%",
        None,
    )
    .await?;

    let placement_store = fixture.store.clone();
    let placement = tokio::spawn(async move {
        placement_store
            .persist_pod_node_placement(
                sandbox_id,
                "worker-node-placement-race",
                "worker-node-placement-race-uid",
                node_id,
                NodeRole::Worker,
                Some("10.90.0.11"),
            )
            .await
    });
    wait_for_blocked_query_count(
        &fixture.pool,
        "%UPDATE sandboxes%active_node_id%",
        Some("%SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE%"),
        2,
    )
    .await?;

    // Act.
    sandbox_blocker.commit().await?;
    let cleanup_result = tokio::time::timeout(Duration::from_secs(5), cleanup).await??;
    let placement_result = tokio::time::timeout(Duration::from_secs(5), placement).await??;

    // Assert.
    cleanup_result?;
    placement_result?;
    let (placement, active_node_id, active_executions): (String, Option<NodeId>, i64) =
        sqlx::query_as(sql!(
            "SELECT s.placement,
                s.active_node_id,
                (SELECT COUNT(*)
                   FROM sandbox_k8s_executions e
                  WHERE e.sandbox_id = s.id AND e.active)
         FROM sandboxes s
         WHERE s.id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(placement, "unplaced");
    assert_eq!(active_node_id, None);
    assert_eq!(active_executions, 0);

    Ok(())
}

/// A pod placement that preserves node `state` (`InsertOnly`) must not
/// spuriously fail when a concurrent writer inserts the same node id.
/// Under READ COMMITTED the placement's `INSERT ... ON CONFLICT` blocks on
/// the unique index while the concurrent insert is in flight; when it
/// commits, the ON CONFLICT arm locks a row that is invisible to the
/// placement statement's snapshot and the change-detection WHERE skips the
/// now-redundant update (address and role already match). The single-query
/// fallback SELECT cannot see the row either, so without the ownership
/// re-check the placement would abort with a bogus "conflicts with a
/// non-Kubernetes node" error. The row must instead be recognized as an
/// existing Kubernetes node and its `ready` state left intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pod_placement_survives_concurrent_insert_of_same_node() -> TestResult {
    // Arrange.
    let fixture = postgres_store_with_max_connections(5).await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let node_id = NodeId::new();
    let node_name = format!("space-test-race-node-{}", node_id.as_uuid().simple());

    // A concurrent writer inserts the node as `ready` but holds the
    // transaction open, so the row is not yet visible to other snapshots.
    let mut inserter = fixture.pool.begin().await?;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, $2::INET, $3, 'k8s', 'ready')"
    ))
    .bind(node_id)
    .bind("10.0.0.40")
    .bind(NodeRole::Worker.as_str())
    .execute(&mut *inserter)
    .await?;

    // Placement for the same node id blocks on the unique index while the
    // concurrent insert is uncommitted; it writes the same address/role so
    // the change-detection WHERE will skip the update once it proceeds.
    let placement_store = fixture.store.clone();
    let placement_name = node_name.clone();
    let placement = tokio::spawn(async move {
        placement_store
            .persist_pod_node_placement(
                sandbox_id,
                &placement_name,
                "uid-race",
                node_id,
                NodeRole::Worker,
                Some("10.0.0.40"),
            )
            .await
    });
    wait_for_blocked_query(
        &fixture.pool,
        "%INSERT INTO nodes (id, address, role, managed_by, state)%",
        None,
    )
    .await?;

    // Act. Release the concurrent insert; placement's ON CONFLICT now
    // resolves against a row invisible to its statement snapshot.
    inserter.commit().await?;
    let placement_result = tokio::time::timeout(Duration::from_secs(5), placement).await??;

    // Assert. Placement succeeds and the concurrent writer's `ready` state
    // is preserved (not clobbered to `registered`).
    placement_result?;
    let state: String = sqlx::query_scalar(sql!("SELECT state FROM nodes WHERE id = $1"))
        .bind(node_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(
        state, "ready",
        "concurrent-insert race must preserve the node's ready state"
    );

    Ok(())
}

async fn wait_for_blocked_query(
    pool: &PgPool,
    query_pattern: &str,
    alternate_pattern: Option<&str>,
) -> TestResult {
    wait_for_blocked_query_count(pool, query_pattern, alternate_pattern, 1).await
}

async fn wait_for_blocked_query_count(
    pool: &PgPool,
    query_pattern: &str,
    alternate_pattern: Option<&str>,
    minimum_count: i64,
) -> TestResult {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar(sql!(
                "SELECT COUNT(*) >= $3
                 FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND pid <> pg_backend_pid()
                   AND wait_event_type = 'Lock'
                   AND (query LIKE $1 OR ($2::TEXT IS NOT NULL AND query LIKE $2))"
            ))
            .bind(query_pattern)
            .bind(alternate_pattern)
            .bind(minimum_count)
            .fetch_one(pool)
            .await?;
            if blocked {
                return Ok::<(), sqlx::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    Ok(())
}

/// The data-plane wake signal cannot rely on the restore lease surviving
/// the post-wake flip: the `sandboxes_clear_restore_lease` BEFORE UPDATE
/// trigger nulls the lease inside any write that leaves the row
/// non-`restoring`, so the flip and the lease-clear land atomically. The
/// post-flip listener-rebind window is therefore only observable through
/// `lifecycle_changed_at` recency — which is what
/// `fetch_sandbox_wake_signal` must classify on.
async fn wake_signal_classifies_post_wake_flip_by_recency_not_lease(
    fixture: &PostgresStore,
) -> TestResult {
    use crate::controllers::sandbox::{
        POST_WAKE_LISTENER_REBIND_GRACE, fetch_sandbox_wake_signal,
        update_and_fetch_post_wake_sandbox,
    };
    use pplx_space_common_core::types::SandboxLifecycle;

    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "suspended").await?;

    // Claim a user restore lease through the real claim path.
    let claim = fixture
        .store
        .begin_restore_lifecycle(sandbox_id, false)
        .await?;
    user_fresh_restore_token(claim);
    let lease_expires_at: Option<OffsetDateTime> = sqlx::query_scalar(sql!(
        "SELECT restore_lease_expires_at FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert!(
        lease_expires_at.is_some(),
        "user restore claim must install a restore lease"
    );

    // While `restoring`, the wake signal classifies as waking off the
    // lifecycle alone.
    let signal = fetch_sandbox_wake_signal(&fixture.pool, sandbox_id)
        .await?
        .expect("sandbox row must exist");
    assert_eq!(signal.lifecycle, Some(SandboxLifecycle::Restoring));
    assert!(
        signal.is_plausibly_waking(),
        "restoring row must classify as waking"
    );

    // Finalize the wake through the real post-wake update path; the
    // lease-clear and lifecycle-changed-at triggers fire inside this
    // UPDATE.
    update_and_fetch_post_wake_sandbox(&fixture.pool, sandbox_id, None).await?;

    let (lease_expires_at, lease_token): (Option<OffsetDateTime>, Option<Uuid>) =
        sqlx::query_as(sql!(
            "SELECT restore_lease_expires_at, restore_lease_token
               FROM sandboxes
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(
        (lease_expires_at, lease_token),
        (None, None),
        "the lifecycle flip to `started` must clear the restore lease atomically"
    );

    // The just-woken row still classifies as waking — via flip recency,
    // with no lease left to consult.
    let signal = fetch_sandbox_wake_signal(&fixture.pool, sandbox_id)
        .await?
        .expect("sandbox row must exist");
    assert_eq!(signal.lifecycle, Some(SandboxLifecycle::Started));
    assert!(
        signal.lifecycle_changed_recently,
        "the post-wake flip must register as a recent lifecycle change"
    );
    assert!(
        signal.is_plausibly_waking(),
        "just-woken started row must classify as waking via the recent flip"
    );

    // Once the flip ages past the listener-rebind grace, the same
    // `started` row is a genuine fault, not a wake in progress.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle_changed_at = NOW() - make_interval(secs => $1)
          WHERE id = $2"
    ))
    .bind(POST_WAKE_LISTENER_REBIND_GRACE.as_secs_f64() + 1.0)
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    let signal = fetch_sandbox_wake_signal(&fixture.pool, sandbox_id)
        .await?
        .expect("sandbox row must exist");
    assert_eq!(signal.lifecycle, Some(SandboxLifecycle::Started));
    assert!(
        !signal.lifecycle_changed_recently && !signal.is_plausibly_waking(),
        "a settled started row past the rebind grace must classify as a fault"
    );

    Ok(())
}

async fn post_wake_update_does_not_resurrect_quarantined_sandbox(
    fixture: &PostgresStore,
) -> TestResult {
    use crate::controllers::sandbox::update_and_fetch_post_wake_sandbox;

    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "suspended").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'quarantined' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;

    let err = update_and_fetch_post_wake_sandbox(&fixture.pool, sandbox_id, None)
        .await
        .expect_err("quarantine must win the post-wake CAS");
    assert!(
        err.to_string().contains("lifecycle CAS"),
        "unexpected error: {err}"
    );

    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(lifecycle, "quarantined");

    Ok(())
}

/// `clear_command_ready_pending` must only claim a still-`started` marker,
/// so it loses to the command-ready reaper's `started -> failed` flip and a
/// concurrent access can't hand back a client to a reaped (deleting) pod.
async fn clear_command_ready_pending_is_a_cas_against_the_reaper(
    fixture: &PostgresStore,
) -> TestResult {
    let set_marker = |id: SandboxId| async move {
        sqlx::query(sql!(
            "UPDATE sandboxes SET command_ready_pending = TRUE WHERE id = $1"
        ))
        .bind(id)
        .execute(&fixture.pool)
        .await
        .map(|_| ())
    };

    // A started + marker row: the clear claims it and the marker is gone.
    let started = SandboxId::new();
    insert_sandbox(&fixture.pool, started, "started").await?;
    set_marker(started).await?;
    assert!(
        fixture.store.clear_command_ready_pending(started).await?,
        "clearing a started + marker row must claim it"
    );
    let still_pending: bool = sqlx::query_scalar(sql!(
        "SELECT command_ready_pending FROM sandboxes WHERE id = $1"
    ))
    .bind(started)
    .fetch_one(&fixture.pool)
    .await?;
    assert!(!still_pending, "the marker must be cleared");
    // A second clear claims nothing (marker already gone) — benign.
    assert!(
        !fixture.store.clear_command_ready_pending(started).await?,
        "re-clearing an already-cleared row must claim nothing"
    );

    // A row the reaper already flipped to `failed` (marker still set): the
    // clear must NOT claim it — that is the lost-race signal the access path
    // uses to avoid returning a client to a doomed pod.
    let reaped = SandboxId::new();
    insert_sandbox(&fixture.pool, reaped, "failed").await?;
    set_marker(reaped).await?;
    assert!(
        !fixture.store.clear_command_ready_pending(reaped).await?,
        "a non-started row must not be claimed by the access-path clear"
    );

    Ok(())
}

async fn ensure_active_execution_row_is_idempotent(fixture: &PostgresStore) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "creating").await?;

    let first = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?;
    let second = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?;

    assert!(first.created);
    assert!(!second.created);
    assert_eq!(first.row.generation, 1);
    assert_eq!(first.row.pod_name, second.row.pod_name);
    assert_eq!(
        first.row.pod_name,
        format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple())
    );
    let status = fixture
        .store
        .active_execution_status(sandbox_id)
        .await?
        .expect("active execution should exist");
    assert_eq!(status.state, ActiveExecutionState::Creating);
    assert!(!status.create_age_expired);

    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
         SET updated_at = NOW() - make_interval(secs => $2)
         WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .bind(super::super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS + 1)
    .execute(&fixture.pool)
    .await?;
    let aged = fixture
        .store
        .active_execution_status(sandbox_id)
        .await?
        .expect("active execution should exist");
    assert!(aged.create_age_expired);

    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            first.row.generation,
            Some("pod-uid"),
            Some("10.90.0.7"),
        )
        .await?;
    let (state, pod_ip): (String, Option<String>) = sqlx::query_as(sql!(
        "SELECT state, host(pod_ip) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(state, "running");
    assert_eq!(pod_ip.as_deref(), Some("10.90.0.7"));
    Ok(())
}

async fn pod_materialization_claim_publishes_fresh_lease(fixture: &PostgresStore) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let claim = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?;

    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            claim.row.generation,
            Some("stale-pod-uid"),
            Some("10.90.0.8"),
        )
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
         SET updated_at = NOW() - make_interval(secs => $2)
         WHERE sandbox_id = $1 AND generation = $3 AND active"
    ))
    .bind(sandbox_id)
    .bind(super::super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS + 1)
    .bind(claim.row.generation)
    .execute(&fixture.pool)
    .await?;

    assert!(
        fixture
            .store
            .active_execution_status(sandbox_id)
            .await?
            .expect("active execution should exist")
            .create_age_expired
    );

    assert!(
        fixture
            .store
            .claim_active_execution_pod_materialization(
                sandbox_id,
                claim.row.generation,
                SandboxLifecycle::Started,
            )
            .await?,
        "active execution should claim pod materialization"
    );

    let status = fixture
        .store
        .active_execution_status(sandbox_id)
        .await?
        .expect("active execution should exist");
    assert_eq!(status.state, ActiveExecutionState::Creating);
    assert_eq!(status.pod_uid, None);
    assert_eq!(status.pod_ip, None);
    assert!(!status.create_age_expired);

    Ok(())
}

async fn started_recovery_claim_is_bounded_and_preserves_its_marker(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let typed = sandbox_id;
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;

    let first = fixture.store.claim_started_recovery(typed).await?;
    let concurrent_create = fixture.store.claim_started_recovery(typed).await?;
    let generation = match first {
        StartedRecoveryClaim::CreatedExecution { generation } => generation,
        other => panic!("expected a new execution claim, got {other:?}"),
    };
    assert!(matches!(
        concurrent_create,
        StartedRecoveryClaim::InFlight {
            generation: concurrent_generation,
            lease_remaining,
        } if concurrent_generation == generation && !lease_remaining.is_zero()
    ));

    fixture
        .store
        .persist_pod_metadata(sandbox_id, generation, Some("pod-uid"), Some("10.90.0.8"))
        .await?;
    let state: String = sqlx::query_scalar(sql!(
        "SELECT state FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(state, "recovering");
    assert_eq!(
        fixture
            .store
            .observe_started_recovery(typed, generation)
            .await?,
        StartedRecoveryObservation::Pending
    );
    fixture
        .store
        .complete_started_recovery(
            typed,
            generation,
            None,
            StartedRecoveryCompletion::Succeeded,
        )
        .await?;
    assert_eq!(
        fixture
            .store
            .observe_started_recovery(typed, generation)
            .await?,
        StartedRecoveryObservation::Succeeded {
            generation,
            pod_name: K8sStore::execution_pod_name(sandbox_id, generation),
            pod_ip: "10.90.0.8".to_string(),
        }
    );
    let recovery = fixture.store.claim_started_recovery(typed).await?;
    assert_eq!(
        recovery,
        StartedRecoveryClaim::ExistingExecution { generation }
    );
    fixture
        .store
        .persist_pod_metadata(sandbox_id, generation, Some("pod-uid"), Some("10.90.0.8"))
        .await?;
    let state: String = sqlx::query_scalar(sql!(
        "SELECT state FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(state, "recovering");
    assert!(matches!(
        fixture.store.claim_started_recovery(typed).await?,
        StartedRecoveryClaim::InFlight {
            generation: concurrent_generation,
            lease_remaining,
        } if concurrent_generation == generation && !lease_remaining.is_zero()
    ));

    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
         SET updated_at = NOW() - make_interval(secs => $2)
         WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .bind(recovery_lease_ttl_secs() + 1)
    .execute(&fixture.pool)
    .await?;
    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            generation,
            Some("replacement-pod-uid"),
            Some("10.90.0.8"),
        )
        .await?;
    assert_eq!(
        fixture.store.claim_started_recovery(typed).await?,
        StartedRecoveryClaim::ExistingExecution { generation }
    );

    fixture
        .store
        .complete_started_recovery(
            typed,
            generation,
            None,
            StartedRecoveryCompletion::Succeeded,
        )
        .await?;
    let state: String = sqlx::query_scalar(sql!(
        "SELECT state FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(state, "running");

    assert_eq!(
        fixture.store.claim_started_recovery(typed).await?,
        StartedRecoveryClaim::ExistingExecution { generation }
    );
    fixture
        .store
        .complete_started_recovery(typed, generation, None, StartedRecoveryCompletion::Failed)
        .await?;
    assert_eq!(
        fixture
            .store
            .observe_started_recovery(typed, generation)
            .await?,
        StartedRecoveryObservation::Failed
    );
    assert_eq!(
        fixture.store.claim_started_recovery(typed).await?,
        StartedRecoveryClaim::ExistingExecution { generation }
    );

    Ok(())
}

async fn failed_started_recovery_converges_unplaced_sandbox_with_current_snapshot(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let typed = sandbox_id;
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/failed-started-recovery?region=us-east-1",
        -1,
        None,
    )
    .await?;
    let generation = match fixture.store.claim_started_recovery(typed).await? {
        StartedRecoveryClaim::CreatedExecution { generation } => generation,
        other => panic!("expected a new execution claim, got {other:?}"),
    };

    fixture
        .store
        .complete_started_recovery(typed, generation, None, StartedRecoveryCompletion::Failed)
        .await?;

    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(
        lifecycle, "suspended",
        "terminal recovery failure should converge a recoverable started sandbox"
    );
    let (state, active): (String, bool) = sqlx::query_as(sql!(
        "SELECT state, active
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(state, "recovery_failed");
    assert!(!active, "converged failed execution must be retired");
    assert_eq!(
        fixture
            .store
            .persist_pod_metadata(
                sandbox_id,
                generation,
                Some("late-pod-uid"),
                Some("10.90.0.9"),
            )
            .await?,
        None,
        "late pod metadata must not resurrect a converged failed execution"
    );

    Ok(())
}

async fn failed_started_recovery_without_current_snapshot_stays_started(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let typed = sandbox_id;
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let generation = match fixture.store.claim_started_recovery(typed).await? {
        StartedRecoveryClaim::CreatedExecution { generation } => generation,
        other => panic!("expected a new execution claim, got {other:?}"),
    };

    fixture
        .store
        .complete_started_recovery(typed, generation, None, StartedRecoveryCompletion::Failed)
        .await?;

    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(lifecycle, "started");
    let state: String = sqlx::query_scalar(sql!(
        "SELECT state
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(state, "recovery_failed");

    Ok(())
}

async fn failed_started_recovery_converges_after_authoritative_pod_absence(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let typed = sandbox_id;
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/pod-absence-current?region=us-east-1",
        -1,
        None,
    )
    .await?;
    let generation = match fixture.store.claim_started_recovery(typed).await? {
        StartedRecoveryClaim::CreatedExecution { generation } => generation,
        other => panic!("expected a new execution claim, got {other:?}"),
    };
    fixture
        .store
        .persist_pod_metadata(sandbox_id, generation, Some("failed-pod-uid"), None)
        .await?;
    fixture
        .store
        .complete_started_recovery(typed, generation, None, StartedRecoveryCompletion::Failed)
        .await?;

    assert_eq!(
        sqlx::query_scalar::<_, String>(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&fixture.pool)
            .await?,
        "started",
        "pod evidence must initially block failed-restore convergence"
    );

    assert_eq!(
        fixture
            .store
            .converge_failed_recovery_after_pod_absence_if_generation(
                sandbox_id,
                generation,
                Some("failed-pod-uid"),
                None,
                None,
            )
            .await?,
        FailedRecoveryPodAbsenceOutcome::ConvergedSuspended
    );

    // The failed execution must be retired (`suspended`/`recovery_failed`/
    // `active = false`) with its pod identity cleared.
    insta::assert_yaml_snapshot!(
        execution_by_generation_row(&fixture.pool, sandbox_id, generation).await?
    );

    Ok(())
}

/// A sandbox given up as lost is `failed` with its execution retired. A wake
/// that still reaches started recovery must be refused without allocating a
/// generation, so a retrying client stops minting executions.
async fn started_recovery_claim_on_lost_sandbox_mints_no_execution(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "failed").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, state, active)
         VALUES ($1, 7, $2, 'sandboxes', 'forgotten', FALSE)"
    ))
    .bind(sandbox_id)
    .bind(format!("space-sbx-{}-g7", sandbox_id.as_uuid().simple()))
    .execute(&fixture.pool)
    .await?;

    let claim = fixture.store.claim_started_recovery(sandbox_id).await;
    assert!(
        matches!(claim, Err(SubstrateError::Conflict { .. })),
        "a lost sandbox must not be recovered, got {claim:?}"
    );
    let (executions, newest): (i64, i64) = sqlx::query_as(sql!(
        "SELECT COUNT(*), MAX(generation)
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!((executions, newest), (1, 7));
    Ok(())
}

async fn failed_started_recovery_after_pod_absence_refuses_stale_snapshot(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let typed = sandbox_id;
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/pod-absence-stale?region=us-east-1",
        -1,
        None,
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET last_command_at = NOW() WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    let generation = match fixture.store.claim_started_recovery(typed).await? {
        StartedRecoveryClaim::CreatedExecution { generation } => generation,
        other => panic!("expected a new execution claim, got {other:?}"),
    };
    fixture
        .store
        .persist_pod_metadata(sandbox_id, generation, Some("failed-pod-uid"), None)
        .await?;
    fixture
        .store
        .complete_started_recovery(typed, generation, None, StartedRecoveryCompletion::Failed)
        .await?;

    assert_eq!(
        fixture
            .store
            .converge_failed_recovery_after_pod_absence_if_generation(
                sandbox_id,
                generation,
                Some("failed-pod-uid"),
                None,
                None,
            )
            .await?,
        FailedRecoveryPodAbsenceOutcome::ClaimPreserved
    );
    // Stale bytes must never be restored: lifecycle stays `started`, the
    // failed claim remains owned (`active`), and the authoritatively absent
    // pod metadata stays cleared.
    insta::assert_yaml_snapshot!(
        execution_by_generation_row(&fixture.pool, sandbox_id, generation).await?
    );

    // This is the reachable pod-less, route-less RecoveryFailed row. Neither
    // allocation entry point may silently treat it as a resident. Both roll
    // back without publishing a route or retaining a new operation lease.
    let candidate = SandboxActivationAttemptId::new();
    for direct_assignment in [true, false] {
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let result = if direct_assignment {
            fixture
                .store
                .assign_activation_attempt(
                    typed,
                    generation,
                    candidate,
                    SandboxTransitionTargetState::Started,
                )
                .await
                .map(|_| ())
        } else {
            fixture
                .store
                .begin_activation_execution_row(
                    typed,
                    candidate,
                    SandboxTransitionTargetState::Started,
                )
                .await
                .map(|_| ())
        };
        let counters = capture.finish();
        assert!(matches!(result, Err(SubstrateError::Unavailable { .. })));
        assert_eq!(
            counters
                .iter()
                .filter(
                    |counter| counter.name == "sandbox.api.k8s.route_identity.decision"
                        && counter
                            .tags
                            .iter()
                            .any(|tag| tag == "reason:rotation_required_unclassified")
                )
                .count(),
            1
        );
        let row = fixture
            .store
            .active_execution_status(sandbox_id)
            .await?
            .unwrap();
        assert_eq!(row.activation_attempt_id, None);
        assert_eq!(row.route_activation_id, None);
        assert_eq!(row.state, ActiveExecutionState::RecoveryFailed);
    }
    // A subsequent Started access owns recovery through this bounded lease;
    // it changes the state before assignment, without restoring stale bytes.
    assert_eq!(
        fixture.store.claim_started_recovery(typed).await?,
        StartedRecoveryClaim::ExistingExecution { generation }
    );
    let assigned = fixture
        .store
        .assign_activation_attempt(
            typed,
            generation,
            candidate,
            SandboxTransitionTargetState::Started,
        )
        .await?;
    let row = fixture
        .store
        .active_execution_status(sandbox_id)
        .await?
        .unwrap();
    assert_eq!(row.state, ActiveExecutionState::Recovering);
    assert_eq!(row.route_activation_id, Some(assigned));
    assert_eq!(row.activation_attempt_id, Some(assigned));

    Ok(())
}

async fn failed_started_recovery_pod_absence_is_fenced_against_late_materialization(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let typed = sandbox_id;
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/pod-absence-race?region=us-east-1",
        -1,
        None,
    )
    .await?;
    let generation = match fixture.store.claim_started_recovery(typed).await? {
        StartedRecoveryClaim::CreatedExecution { generation } => generation,
        other => panic!("expected a new execution claim, got {other:?}"),
    };
    fixture
        .store
        .persist_pod_metadata(sandbox_id, generation, Some("observed-pod-uid"), None)
        .await?;
    fixture
        .store
        .complete_started_recovery(typed, generation, None, StartedRecoveryCompletion::Failed)
        .await?;

    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET pod_uid = 'late-pod-uid', updated_at = NOW()
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .execute(&fixture.pool)
    .await?;

    assert_eq!(
        fixture
            .store
            .converge_failed_recovery_after_pod_absence_if_generation(
                sandbox_id,
                generation,
                Some("observed-pod-uid"),
                None,
                None,
            )
            .await?,
        FailedRecoveryPodAbsenceOutcome::NotMatched
    );
    // The race must leave the claim untouched: `started`/`active`, with the
    // late-materialized pod uid intact.
    insta::assert_yaml_snapshot!(
        execution_by_generation_row(&fixture.pool, sandbox_id, generation).await?
    );

    Ok(())
}

async fn failed_started_recovery_with_orphaned_snapshot_stays_started(
    fixture: &PostgresStore,
) -> TestResult {
    for artifact_id in [None, Some(Uuid::now_v7())] {
        let sandbox_id = SandboxId::new();
        let typed = sandbox_id;
        insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
        if let Some(artifact_id) = artifact_id {
            sqlx::query(sql!(
                "UPDATE sandboxes
                    SET current_local_artifact_id = $2,
                        current_local_artifact_kind = 'full',
                        current_local_artifact_origin = 'suspend'
                  WHERE id = $1"
            ))
            .bind(sandbox_id)
            .bind(artifact_id)
            .execute(&fixture.pool)
            .await?;
        }
        let snapshot_id = insert_ready_full_snapshot_relative_to_lifecycle(
            &fixture.pool,
            sandbox_id,
            "s3://space-test/orphaned-failed-started-recovery?region=us-east-1",
            1,
            artifact_id.map(LocalArtifactId::from_uuid),
        )
        .await?;
        sqlx::query(sql!(
            "UPDATE snapshot_storage_locations
                SET orphaned_at = NOW()
              WHERE snapshot_id = $1"
        ))
        .bind(snapshot_id)
        .execute(&fixture.pool)
        .await?;
        let generation = match fixture.store.claim_started_recovery(typed).await? {
            StartedRecoveryClaim::CreatedExecution { generation } => generation,
            other => panic!("expected a new execution claim, got {other:?}"),
        };

        fixture
            .store
            .complete_started_recovery(typed, generation, None, StartedRecoveryCompletion::Failed)
            .await?;

        let lifecycle: String =
            sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
                .bind(sandbox_id)
                .fetch_one(&fixture.pool)
                .await?;
        assert_eq!(lifecycle, "started");
    }

    Ok(())
}

async fn stale_failed_started_recovery_does_not_converge_newer_execution(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let typed = sandbox_id;
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/stale-failed-started-recovery?region=us-east-1",
        1,
        None,
    )
    .await?;
    let stale_generation = match fixture.store.claim_started_recovery(typed).await? {
        StartedRecoveryClaim::CreatedExecution { generation } => generation,
        other => panic!("expected a new execution claim, got {other:?}"),
    };
    let successor = fixture
        .store
        .replace_active_execution_claim_if_generation(typed, stale_generation, "replaced", None)
        .await?
        .expect("active recovery generation should be replaced");

    fixture
        .store
        .complete_started_recovery(
            typed,
            stale_generation,
            None,
            StartedRecoveryCompletion::Failed,
        )
        .await?;

    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(lifecycle, "started");
    let active: (i64, String) = sqlx::query_as(sql!(
        "SELECT generation, state
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        active,
        (
            successor.row.generation,
            ActiveExecutionState::Recovering.as_str().to_string(),
        )
    );

    Ok(())
}

async fn failed_started_recovery_follows_owned_successor_generation(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let typed = sandbox_id;
    let activation_attempt_id = SandboxActivationAttemptId::new();
    let artifact_id = LocalArtifactId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact_id)
    .execute(&fixture.pool)
    .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/owned-successor-failed-recovery?region=us-east-1",
        1,
        Some(artifact_id),
    )
    .await?;
    let initial_generation = match fixture.store.claim_started_recovery(typed).await? {
        StartedRecoveryClaim::CreatedExecution { generation } => generation,
        other => panic!("expected a new execution claim, got {other:?}"),
    };
    let materialization = fixture
        .store
        .ensure_active_execution_claim_for_lifecycle(
            typed,
            SandboxLifecycle::Started,
            None,
            Some((activation_attempt_id, SandboxTransitionTargetState::Started)),
        )
        .await?;
    assert_eq!(materialization.row.generation, initial_generation);
    let replacement = match fixture
        .store
        .replace_started_recovery_execution(
            typed,
            ExecutionGeneration::new(initial_generation)?,
            None,
        )
        .await?
    {
        StartedRecoveryReplacement::Replaced { claim, .. } => claim,
        other => panic!("expected an owned successor execution, got {other:?}"),
    };

    let preserved_owner: (Option<Uuid>, Option<Uuid>) = sqlx::query_as(sql!(
        "SELECT activation_attempt_id, route_activation_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2 AND active"
    ))
    .bind(sandbox_id)
    .bind(replacement.row.generation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        preserved_owner,
        (
            Some(activation_attempt_id.as_uuid()),
            Some(activation_attempt_id.as_uuid()),
        )
    );
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_expires_at = NOW() - INTERVAL '1 second'
          WHERE sandbox_id = $1 AND generation = $2 AND active"
    ))
    .bind(sandbox_id)
    .bind(replacement.row.generation)
    .execute(&fixture.pool)
    .await?;
    let successor_attempt_id = fixture
        .store
        .assign_activation_attempt(
            typed,
            replacement.row.generation,
            activation_attempt_id,
            SandboxTransitionTargetState::Started,
        )
        .await?;
    assert!(
        successor_attempt_id > activation_attempt_id,
        "a stale proposal after handoff must advance beyond the preserved route owner"
    );

    fixture
        .store
        .complete_started_recovery(
            typed,
            initial_generation,
            Some(successor_attempt_id),
            StartedRecoveryCompletion::Failed,
        )
        .await?;

    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(lifecycle, "suspended");
    let successor: (String, bool, Option<Uuid>, Option<Uuid>) = sqlx::query_as(sql!(
        "SELECT state, active, activation_attempt_id, route_activation_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(replacement.row.generation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        successor,
        (
            "recovery_failed".to_string(),
            false,
            None,
            Some(activation_attempt_id.as_uuid()),
        ),
        "terminal recovery must release the lease while preserving the route high-water mark"
    );

    Ok(())
}

async fn failed_started_recovery_without_attempt_does_not_steal_owned_generation(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let typed = sandbox_id;
    let activation_attempt_id = SandboxActivationAttemptId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/same-generation-owner?region=us-east-1",
        -1,
        None,
    )
    .await?;
    let generation = match fixture.store.claim_started_recovery(typed).await? {
        StartedRecoveryClaim::CreatedExecution { generation } => generation,
        other => panic!("expected a new execution claim, got {other:?}"),
    };
    let materialization = fixture
        .store
        .ensure_active_execution_claim_for_lifecycle(
            typed,
            SandboxLifecycle::Started,
            None,
            Some((activation_attempt_id, SandboxTransitionTargetState::Started)),
        )
        .await?;
    assert_eq!(materialization.row.generation, generation);

    fixture
        .store
        .complete_started_recovery(typed, generation, None, StartedRecoveryCompletion::Failed)
        .await?;

    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(lifecycle, "started");
    let execution: (String, bool, Option<Uuid>) = sqlx::query_as(sql!(
        "SELECT state, active, activation_attempt_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        execution,
        (
            ActiveExecutionState::Recovering.as_str().to_string(),
            true,
            Some(activation_attempt_id.as_uuid()),
        )
    );

    Ok(())
}

async fn failed_started_recovery_with_materialized_pod_stays_started(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let typed = sandbox_id;
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/materialized-failed-started-recovery?region=us-east-1",
        -1,
        None,
    )
    .await?;
    let generation = match fixture.store.claim_started_recovery(typed).await? {
        StartedRecoveryClaim::CreatedExecution { generation } => generation,
        other => panic!("expected a new execution claim, got {other:?}"),
    };
    fixture
        .store
        .persist_pod_metadata(sandbox_id, generation, Some("pending-pod-uid"), None)
        .await?;

    fixture
        .store
        .complete_started_recovery(typed, generation, None, StartedRecoveryCompletion::Failed)
        .await?;

    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(
        lifecycle, "started",
        "a materialized Kubernetes pod must block lifecycle convergence"
    );

    Ok(())
}

async fn failed_started_recovery_with_node_placement_stays_started(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let typed = sandbox_id;
    let node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/placed-failed-started-recovery?region=us-east-1",
        -1,
        None,
    )
    .await?;
    let generation = match fixture.store.claim_started_recovery(typed).await? {
        StartedRecoveryClaim::CreatedExecution { generation } => generation,
        other => panic!("expected a new execution claim, got {other:?}"),
    };
    fixture
        .store
        .persist_pod_node_placement(
            sandbox_id,
            "placed-recovery-node",
            "placed-recovery-node-uid",
            node_id,
            NodeRole::Worker,
            Some("10.90.0.10"),
        )
        .await?;

    fixture
        .store
        .complete_started_recovery(typed, generation, None, StartedRecoveryCompletion::Failed)
        .await?;

    // A materialized pod placement blocks lifecycle convergence: the row
    // must stay `started`/`on_node` and pin exactly the placed node.
    insta::assert_yaml_snapshot!(
        sandbox_placement_row(&fixture.pool, sandbox_id).await?,
        {
            ".active_node_id" => insta::dynamic_redaction(move |value, _path| {
                assert_eq!(
                    value.as_str(),
                    Some(node_id.to_string().as_str()),
                    "placement must pin the recovery's placed node"
                );
                "[node_id]"
            }),
        }
    );

    Ok(())
}

async fn failed_started_recovery_with_routable_pod_stays_started(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let typed = sandbox_id;
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/routable-failed-started-recovery?region=us-east-1",
        1,
        None,
    )
    .await?;
    let generation = match fixture.store.claim_started_recovery(typed).await? {
        StartedRecoveryClaim::CreatedExecution { generation } => generation,
        other => panic!("expected a new execution claim, got {other:?}"),
    };
    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            generation,
            Some("routable-pod-uid"),
            Some("10.90.0.8"),
        )
        .await?;

    fixture
        .store
        .complete_started_recovery(typed, generation, None, StartedRecoveryCompletion::Failed)
        .await?;

    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(lifecycle, "started");
    let state: String = sqlx::query_scalar(sql!(
        "SELECT state
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(state, "recovery_failed");

    Ok(())
}

async fn stale_creating_started_recovery_claim_is_reclaimed(fixture: &PostgresStore) -> TestResult {
    let sandbox_id = SandboxId::new();
    let typed = sandbox_id;
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let created = fixture.store.ensure_active_execution_claim(typed).await?;
    assert!(created.created);
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
         SET updated_at = NOW() - make_interval(secs => $2)
         WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .bind(recovery_lease_ttl_secs() + 1)
    .execute(&fixture.pool)
    .await?;

    let reclaimed = fixture.store.claim_started_recovery(typed).await?;

    assert_eq!(
        reclaimed,
        StartedRecoveryClaim::ExistingExecution {
            generation: created.row.generation,
        }
    );
    let state: String = sqlx::query_scalar(sql!(
        "SELECT state FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(state, "recovering");
    Ok(())
}

async fn started_recovery_follows_superseding_execution(fixture: &PostgresStore) -> TestResult {
    let sandbox_id = SandboxId::new();
    let typed = sandbox_id;
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let first_generation = match fixture.store.claim_started_recovery(typed).await? {
        StartedRecoveryClaim::CreatedExecution { generation } => generation,
        other => panic!("expected a new execution claim, got {other:?}"),
    };
    let replacement = fixture
        .store
        .replace_active_execution_claim_if_generation(typed, first_generation, "replaced", None)
        .await?
        .expect("active recovery generation should be replaced");

    let pending = fixture
        .store
        .observe_started_recovery(typed, first_generation)
        .await?;

    assert_eq!(pending, StartedRecoveryObservation::Pending);

    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            replacement.row.generation,
            Some("replacement-pod-uid"),
            Some("10.90.0.9"),
        )
        .await?;

    let still_pending = fixture
        .store
        .observe_started_recovery(typed, first_generation)
        .await?;

    assert_eq!(still_pending, StartedRecoveryObservation::Pending);

    fixture
        .store
        .complete_started_recovery(
            typed,
            first_generation,
            None,
            StartedRecoveryCompletion::Succeeded,
        )
        .await?;

    let succeeded = fixture
        .store
        .observe_started_recovery(typed, first_generation)
        .await?;

    assert_eq!(
        succeeded,
        StartedRecoveryObservation::Succeeded {
            generation: replacement.row.generation,
            pod_name: replacement.row.pod_name,
            pod_ip: "10.90.0.9".to_string(),
        }
    );
    Ok(())
}

/// The wake path's transient-create dodge hands its finished generation
/// off WITHOUT pre-inserting the successor claim. A successor row in
/// `creating` state that the next `ensure_pod_exists` round did not
/// insert itself trips the concurrent-create dedupe guard
/// (`claim.created == false`), so the replacement pod would never be
/// created and the wake would wedge until timeout.
async fn transient_create_dodge_handoff_yields_fresh_claim(fixture: &PostgresStore) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let typed = sandbox_id;

    let first = fixture.store.ensure_active_execution_claim(typed).await?;
    assert!(first.created);

    // The trap shape: replace_… pre-inserts the successor in `creating`,
    // so a later claim attempt dedupes against it instead of inserting.
    let replaced = fixture
        .store
        .replace_active_execution_claim_if_generation(typed, first.row.generation, "replaced", None)
        .await?
        .expect("replacement claim");
    let reacquired = fixture.store.ensure_active_execution_claim(typed).await?;
    assert!(!reacquired.created);
    assert_eq!(reacquired.row.state, ActiveExecutionState::Creating);
    assert_eq!(reacquired.row.generation, replaced.row.generation);

    // The dodge contract: finishing the generation without a successor
    // makes the next claim a fresh insert (`created == true`), which is
    // what lets the wake loop's second round create the replacement pod.
    assert!(
        fixture
            .store
            .finish_active_execution_if_generation(sandbox_id, replaced.row.generation, "replaced",)
            .await?
    );
    let fresh = fixture.store.ensure_active_execution_claim(typed).await?;
    assert!(fresh.created);
    assert_eq!(fresh.row.state, ActiveExecutionState::Creating);
    assert!(fresh.row.generation > replaced.row.generation);

    Ok(())
}

async fn finish_active_execution_if_generation_guards_against_chained_bumps(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;

    let first = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    assert_eq!(first.generation, 1);

    // Arrange: one restorer retires generation 1 and allocates generation 2.
    assert!(
        fixture
            .store
            .finish_active_execution_if_generation(sandbox_id, first.generation, "replaced")
            .await?
    );
    let second = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    assert_eq!(second.generation, 2);

    // Act: a lagging restorer still holding generation 1 tries to finish.
    assert!(
        !fixture
            .store
            .finish_active_execution_if_generation(sandbox_id, first.generation, "replaced")
            .await?
    );

    // Assert: generation 2 remains active.
    let converged = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    assert_eq!(converged.generation, 2);
    assert_eq!(converged.pod_name, second.pod_name);

    Ok(())
}

async fn abandon_created_unmaterialized_create_claim_preserves_observed_or_materialized_claim(
    fixture: &PostgresStore,
) -> TestResult {
    let observed_sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, observed_sandbox_id, "started").await?;
    let winner = fixture
        .store
        .ensure_active_execution_claim(observed_sandbox_id)
        .await?;
    assert!(winner.created);
    let observed = fixture
        .store
        .ensure_active_execution_claim(observed_sandbox_id)
        .await?;
    assert!(!observed.created);

    let abandoned_observed = fixture
        .store
        .abandon_created_unmaterialized_create_claim(observed_sandbox_id, &observed)
        .await?;

    assert!(!abandoned_observed);
    let active_observed = fixture
        .store
        .active_execution_status(observed_sandbox_id)
        .await?
        .expect("observed execution should remain active");
    assert_eq!(active_observed.generation, winner.row.generation);

    let materialized_sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, materialized_sandbox_id, "started").await?;
    let materialized = fixture
        .store
        .ensure_active_execution_claim(materialized_sandbox_id)
        .await?;
    fixture
        .store
        .persist_pod_metadata(
            materialized_sandbox_id,
            materialized.row.generation,
            Some("materialized-pod-uid"),
            None,
        )
        .await?;

    let abandoned_materialized = fixture
        .store
        .abandon_created_unmaterialized_create_claim(materialized_sandbox_id, &materialized)
        .await?;

    assert!(!abandoned_materialized);
    let active_materialized = fixture
        .store
        .active_execution_status(materialized_sandbox_id)
        .await?
        .expect("materialized execution should remain active");
    assert_eq!(active_materialized.generation, materialized.row.generation);
    assert_eq!(
        active_materialized.pod_uid.as_ref().map(PodUid::as_str),
        Some("materialized-pod-uid")
    );

    let unmaterialized_sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, unmaterialized_sandbox_id, "started").await?;
    let unmaterialized = fixture
        .store
        .ensure_active_execution_claim(unmaterialized_sandbox_id)
        .await?;

    let abandoned_unmaterialized = fixture
        .store
        .abandon_created_unmaterialized_create_claim(unmaterialized_sandbox_id, &unmaterialized)
        .await?;

    assert!(abandoned_unmaterialized);
    assert!(
        fixture
            .store
            .active_execution_status(unmaterialized_sandbox_id)
            .await?
            .is_none()
    );
    let terminal_state: String = sqlx::query_scalar(sql!(
        "SELECT state FROM sandbox_k8s_executions
         WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(unmaterialized_sandbox_id)
    .bind(unmaterialized.row.generation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(terminal_state, "replaced");

    Ok(())
}

/// The re-drive failure path retires a `creating` claim it did NOT create
/// (`claim.created == false` on a retry): the created-guarded wrapper must
/// refuse it, the no-guard variant must retire it, and a materialized row
/// must stay protected by the SQL CAS either way.
async fn abandon_unmaterialized_create_claim_retires_foreign_creating_claim(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "restoring").await?;
    let winner = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?;
    assert!(winner.created);
    let retry = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?;
    assert!(!retry.created);

    // The created-guarded wrapper refuses a claim this request didn't make.
    assert!(
        !fixture
            .store
            .abandon_created_unmaterialized_create_claim(sandbox_id, &retry)
            .await?
    );
    // The no-guard variant retires it: nothing is in flight behind it once
    // the re-drive deleted the pinned pod.
    assert!(
        fixture
            .store
            .abandon_unmaterialized_create_claim(sandbox_id, &retry)
            .await?
    );
    assert!(
        fixture
            .store
            .active_execution_status(sandbox_id)
            .await?
            .is_none()
    );

    // A materialized row is protected by the SQL CAS even without the
    // created guard.
    let materialized_sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, materialized_sandbox_id, "restoring").await?;
    let materialized = fixture
        .store
        .ensure_active_execution_claim(materialized_sandbox_id)
        .await?;
    fixture
        .store
        .persist_pod_metadata(
            materialized_sandbox_id,
            materialized.row.generation,
            Some("redrive-materialized-pod-uid"),
            None,
        )
        .await?;
    assert!(
        !fixture
            .store
            .abandon_unmaterialized_create_claim(materialized_sandbox_id, &materialized)
            .await?
    );
    assert!(
        fixture
            .store
            .active_execution_status(materialized_sandbox_id)
            .await?
            .is_some()
    );

    Ok(())
}

/// The re-drive cancellation guard's background retirement is fenced on the
/// leader's restore-lease token: once a follower B has taken over the same
/// still-`creating` generation with a fresh token, A's stale cleanup must
/// no-op (and thus never delete B's successor pod).
async fn abandon_unmaterialized_create_claim_is_fenced_by_restore_lease_token(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "restoring").await?;
    let claim = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?;
    assert!(claim.created);
    let generation = claim.row.generation;

    // Follower B took over: the row now holds a NEW live lease token.
    let token_a = Uuid::now_v7();
    let token_b = Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_token = $2,
                restore_lease_expires_at = NOW() + INTERVAL '120 seconds'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(token_b)
    .execute(&fixture.pool)
    .await?;

    // A's stale cleanup (its OLD token) must no-op — B's adopted claim survives.
    assert!(
        !fixture
            .store
            .abandon_unmaterialized_create_claim_by_generation(
                sandbox_id,
                generation,
                Some(token_a),
                None,
            )
            .await?
    );
    assert!(
        fixture
            .store
            .active_execution_status(sandbox_id)
            .await?
            .is_some(),
        "a stale-token retirement must not retire the follower's adopted claim"
    );

    // The current owner's token retires it.
    assert!(
        fixture
            .store
            .abandon_unmaterialized_create_claim_by_generation(
                sandbox_id,
                generation,
                Some(token_b),
                None,
            )
            .await?
    );
    assert!(
        fixture
            .store
            .active_execution_status(sandbox_id)
            .await?
            .is_none()
    );

    Ok(())
}

/// A source-pinned pod's UID is persisted before the scheduler's verdict,
/// so an `Unschedulable` pin leaves the row `created` (pod_ip NULL). The
/// re-drive's `creating`-scoped cancellation cleanup can't match that, which
/// would strand the cross-node successor on a client disconnect. The re-
/// drive resets the row to `creating` after deleting the pinned pod; this
/// proves that reset reopens the claim so retirement matches again, and that
/// it is lease-fenced.
async fn reset_active_execution_to_creating_reopens_a_scheduled_pinned_claim(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "restoring").await?;
    let claim = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?;
    assert!(claim.created);
    let generation = claim.row.generation;
    // The pinned pod's UID persists with no IP (Unschedulable), advancing
    // the row to `created`.
    fixture
        .store
        .persist_pod_metadata(sandbox_id, generation, Some("pinned-pod-uid"), None)
        .await?;
    let state: String = sqlx::query_scalar(sql!(
        "SELECT state FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        state, "created",
        "a pinned pod with no IP advances to created"
    );

    // The `creating`-scoped retirement cannot match the `created` row — the
    // orphan-successor gap.
    assert!(
        !fixture
            .store
            .abandon_unmaterialized_create_claim(sandbox_id, &claim)
            .await?
    );
    assert!(
        fixture
            .store
            .active_execution_status(sandbox_id)
            .await?
            .is_some()
    );

    // Reset (as the re-drive does after deleting the pinned pod) reopens the
    // claim so the guard's retirement matches and clears it.
    assert!(
        fixture
            .store
            .reset_active_execution_to_creating_if_generation(sandbox_id, generation, None, None,)
            .await?
    );
    assert!(
        fixture
            .store
            .abandon_unmaterialized_create_claim(sandbox_id, &claim)
            .await?
    );
    assert!(
        fixture
            .store
            .active_execution_status(sandbox_id)
            .await?
            .is_none()
    );

    // Lease fence: a foreign token must no-op the reset.
    let fenced_id = SandboxId::new();
    insert_sandbox(&fixture.pool, fenced_id, "restoring").await?;
    let fenced_claim = fixture
        .store
        .ensure_active_execution_claim(fenced_id)
        .await?;
    let fenced_generation = fenced_claim.row.generation;
    let owner_token = Uuid::now_v7();
    let foreign_token = Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_token = $2,
                restore_lease_expires_at = NOW() + INTERVAL '120 seconds'
          WHERE id = $1"
    ))
    .bind(fenced_id)
    .bind(owner_token)
    .execute(&fixture.pool)
    .await?;
    fixture
        .store
        .persist_pod_metadata(fenced_id, fenced_generation, Some("pinned-pod-uid"), None)
        .await?;
    assert!(
        !fixture
            .store
            .reset_active_execution_to_creating_if_generation(
                fenced_id,
                fenced_generation,
                Some(foreign_token),
                None,
            )
            .await?,
        "a foreign lease token must not reset another owner's claim"
    );
    let fenced_state: String = sqlx::query_scalar(sql!(
        "SELECT state FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(fenced_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        fenced_state, "created",
        "the foreign-token reset must not apply"
    );
    assert!(
        fixture
            .store
            .reset_active_execution_to_creating_if_generation(
                fenced_id,
                fenced_generation,
                Some(owner_token),
                None,
            )
            .await?,
        "the current lease owner can reset its own claim"
    );

    Ok(())
}

async fn concurrent_handler_does_not_claim_g7_while_g6_create_is_in_flight(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;

    let mut handler_a_claim = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?;
    while handler_a_claim.row.generation < 6 {
        assert!(
            fixture
                .store
                .finish_active_execution_if_generation(
                    sandbox_id,
                    handler_a_claim.row.generation,
                    "replaced",
                )
                .await?
        );
        handler_a_claim = fixture
            .store
            .ensure_active_execution_claim(sandbox_id)
            .await?;
    }
    assert!(handler_a_claim.created);
    assert_eq!(handler_a_claim.row.state, ActiveExecutionState::Creating);
    assert_eq!(handler_a_claim.row.generation, 6);
    assert_eq!(
        handler_a_claim.row.pod_name,
        format!("space-sbx-{}-g6", sandbox_id.as_uuid().simple())
    );

    let attempt_a = pplx_space_common_core::types::SandboxActivationAttemptId::new();
    let initialized_claim = fixture
        .store
        .ensure_active_execution_claim_for_lifecycle(
            sandbox_id,
            SandboxLifecycle::Started,
            None,
            Some((attempt_a, SandboxTransitionTargetState::Started)),
        )
        .await?;
    assert_eq!(
        initialized_claim.row.generation,
        handler_a_claim.row.generation
    );
    let attempt_b = pplx_space_common_core::types::SandboxActivationAttemptId::new();
    let concurrent_error = fixture
        .store
        .assign_activation_attempt(
            sandbox_id,
            handler_a_claim.row.generation,
            attempt_b,
            SandboxTransitionTargetState::Started,
        )
        .await
        .expect_err("a second request must not adopt another activation attempt");
    assert!(matches!(
        concurrent_error,
        SubstrateError::ActivationInFlight {
            same_target: true,
            lease_remaining,
            ..
        } if lease_remaining > Duration::ZERO
    ));
    let different_target_error = fixture
        .store
        .assign_activation_attempt(
            sandbox_id,
            handler_a_claim.row.generation,
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
            SandboxTransitionTargetState::Paused,
        )
        .await
        .expect_err("a different target must not join the live activation lease");
    assert!(matches!(
        different_target_error,
        SubstrateError::ActivationInFlight {
            same_target: false,
            ..
        }
    ));
    let persisted_attempt: Uuid = sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(persisted_attempt, attempt_a.as_uuid());
    let route_attempt: Uuid = sqlx::query_scalar(sql!(
        "SELECT route_activation_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(route_attempt, attempt_a.as_uuid());

    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_expires_at = NOW() - INTERVAL '1 second'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    let assigned_attempt = fixture
        .store
        .assign_activation_attempt(
            sandbox_id,
            handler_a_claim.row.generation,
            attempt_b,
            SandboxTransitionTargetState::Started,
        )
        .await?;
    assert_eq!(assigned_attempt, attempt_b);
    assert!(
        !fixture
            .store
            .complete_activation_attempt(sandbox_id, handler_a_claim.row.generation, attempt_a,)
            .await?,
        "an expired owner must not release its successor's lease"
    );
    let persisted_attempt: Uuid = sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(persisted_attempt, attempt_b.as_uuid());
    let route_attempt: Uuid = sqlx::query_scalar(sql!(
        "SELECT route_activation_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        route_attempt,
        attempt_a.as_uuid(),
        "activation ownership transfer must preserve the resident route incarnation"
    );

    let handler_b_claim = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?;
    assert!(!handler_b_claim.created);
    assert_eq!(
        handler_b_claim.row.generation,
        handler_a_claim.row.generation
    );
    assert_eq!(handler_b_claim.row.pod_name, handler_a_claim.row.pod_name);
    assert_eq!(handler_b_claim.row.state, ActiveExecutionState::Creating);

    let generations: Vec<i64> = sqlx::query_scalar(sql!(
        "SELECT generation
         FROM sandbox_k8s_executions
         WHERE sandbox_id = $1
         ORDER BY generation"
    ))
    .bind(sandbox_id)
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(generations, vec![1, 2, 3, 4, 5, 6]);

    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    assert!(
        !fixture
            .store
            .complete_activation_attempt(sandbox_id, handler_a_claim.row.generation, attempt_b,)
            .await?,
        "an API fast-path completion must retain the receipt fence until lifecycle converges"
    );
    assert_eq!(
        fixture
            .store
            .active_execution_status(sandbox_id)
            .await?
            .unwrap()
            .activation_attempt_id,
        Some(attempt_b)
    );

    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    assert!(
        fixture
            .store
            .complete_activation_attempt(sandbox_id, handler_a_claim.row.generation, attempt_b,)
            .await?
    );
    let disappeared_join = fixture
        .store
        .reconcile_activation_fence(
            sandbox_id,
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
            Some(attempt_b),
            SandboxTransitionTargetState::Started,
        )
        .await;
    assert!(
        matches!(disappeared_join, Err(SubstrateError::Unavailable { .. })),
        "a join racing attempt release must remain retryable"
    );

    Ok(())
}

async fn activation_attempt_assignment_is_monotonic_after_lease_expiry(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let attempt_a = pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(
        Uuid::parse_str("019f8000-0000-7000-8000-000000000001")?,
    );
    let claim = fixture
        .store
        .begin_activation_execution_row(
            sandbox_id,
            attempt_a,
            SandboxTransitionTargetState::Started,
        )
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_expires_at = NOW() - INTERVAL '1 second'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;

    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = $2,
                activation_attempt_expires_at = NOW() - INTERVAL '1 second'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .bind(Uuid::parse_str("019f8000-0000-7000-8000-000000000003")?)
    .execute(&fixture.pool)
    .await?;

    let attempt_b = pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(
        Uuid::parse_str("019f8000-0000-7000-8000-000000000003")?,
    );
    let delayed_proposal = pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(
        Uuid::parse_str("019f8000-0000-7000-8000-000000000002")?,
    );
    let attempt_c = fixture
        .store
        .assign_activation_attempt(
            sandbox_id,
            claim.generation,
            delayed_proposal,
            SandboxTransitionTargetState::Started,
        )
        .await?;
    assert!(
        attempt_c > attempt_b,
        "Postgres must canonicalize a non-monotonic proposal into a successor dispatch"
    );
    let route_attempt: Uuid = sqlx::query_scalar(sql!(
        "SELECT route_activation_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        route_attempt,
        attempt_a.as_uuid(),
        "successor operation leases must not rotate the resident route"
    );

    Ok(())
}

async fn completed_activation_cannot_release_a_later_clock_skewed_allocation(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let route: SandboxActivationAttemptId = "ffffffff-0000-7000-8000-000000000001".parse()?;
    let execution = fixture
        .store
        .begin_activation_execution_row(sandbox_id, route, SandboxTransitionTargetState::Started)
        .await?;
    assert!(
        fixture
            .store
            .complete_activation_attempt(sandbox_id, execution.generation, route)
            .await?
    );
    let mut completed = vec![route];
    for _ in 0..3 {
        let operation = fixture
            .store
            .assign_activation_attempt(
                sandbox_id,
                execution.generation,
                SandboxActivationAttemptId::new(),
                SandboxTransitionTargetState::Started,
            )
            .await?;
        for stale in &completed {
            assert!(
                !fixture
                    .store
                    .complete_activation_attempt(sandbox_id, execution.generation, *stale)
                    .await?,
                "an old completion must not release a fresh operation after the allocator forgets completed owners"
            );
        }
        assert!(
            fixture
                .store
                .complete_activation_attempt(sandbox_id, execution.generation, operation)
                .await?
        );
        completed.push(operation);
    }
    assert_eq!(
        fixture
            .store
            .active_execution_status(sandbox_id)
            .await?
            .unwrap()
            .route_activation_id,
        Some(route)
    );
    Ok(())
}

async fn activation_attempt_assignment_adopts_route_less_legacy_resident_after_ack(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let stale_proposal = pplx_space_common_core::types::SandboxActivationAttemptId::new();
    let legacy_owner =
        pplx_space_common_core::types::SandboxActivationAttemptId::new_after(stale_proposal)
            .expect("test activation identity space must have a successor");
    let execution = fixture
        .store
        .begin_activation_execution_row(
            sandbox_id,
            legacy_owner,
            SandboxTransitionTargetState::Started,
        )
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET route_activation_id = NULL,
                pod_uid = 'legacy-pod-uid',
                state = 'running',
                activation_attempt_expires_at = NOW() - INTERVAL '1 second'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;

    let assigned = fixture
        .store
        .assign_activation_attempt(
            sandbox_id,
            execution.generation,
            stale_proposal,
            SandboxTransitionTargetState::Started,
        )
        .await?;
    assert!(assigned > legacy_owner);
    let unpublished_route: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT route_activation_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        unpublished_route, None,
        "legacy adoption must not publish before the typed Wake acknowledgement"
    );

    let wrong_pod = fixture
        .store
        .publish_legacy_route_identity_after_ack(
            sandbox_id,
            execution.generation,
            assigned,
            "replacement-pod-uid",
        )
        .await;
    assert!(
        matches!(wrong_pod, Err(SubstrateError::Conflict { .. })),
        "acknowledgement from a replaced pod must not publish its route"
    );

    fixture
        .store
        .publish_legacy_route_identity_after_ack(
            sandbox_id,
            execution.generation,
            assigned,
            "legacy-pod-uid",
        )
        .await?;
    let published_route: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT route_activation_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(published_route, Some(assigned.as_uuid()));

    Ok(())
}

async fn route_less_activation_retains_completion_fence(fixture: &PostgresStore) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let claim = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?;
    let attempt = SandboxActivationAttemptId::new();
    fixture
        .store
        .assign_activation_attempt(
            sandbox_id,
            claim.row.generation,
            attempt,
            SandboxTransitionTargetState::Started,
        )
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET route_activation_id = NULL
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;

    assert!(
        !fixture
            .store
            .complete_activation_attempt(sandbox_id, claim.row.generation, attempt,)
            .await?,
        "a completion must not clear the operation that fences unpublished route adoption"
    );
    let execution = fixture
        .store
        .active_execution_status(sandbox_id)
        .await?
        .unwrap();
    assert_eq!(execution.activation_attempt_id, Some(attempt));
    assert_eq!(execution.route_activation_id, None);

    Ok(())
}

async fn suspend_superseded_activation_abandons_only_the_canonical_attempt(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let execution = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    let requested_attempt = SandboxActivationAttemptId::new();
    let prior_route_owner = SandboxActivationAttemptId::new_after(requested_attempt)
        .expect("a fresh activation ID has a successor");
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET route_activation_id = $2
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .bind(prior_route_owner)
    .execute(&fixture.pool)
    .await?;

    let assigned_attempt = fixture
        .store
        .assign_activation_attempt(
            sandbox_id,
            execution.generation,
            requested_attempt,
            SandboxTransitionTargetState::Started,
        )
        .await?;
    assert!(assigned_attempt > prior_route_owner);
    assert_ne!(assigned_attempt, requested_attempt);

    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspended' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    assert!(
        !fixture
            .store
            .complete_activation_attempt(sandbox_id, execution.generation, assigned_attempt,)
            .await?,
        "ordinary completion must not clear a lease after suspend wins the lifecycle race"
    );
    assert!(
        !fixture
            .store
            .abandon_activation_attempt_superseded_by_suspend(
                sandbox_id,
                execution.generation,
                requested_attempt,
            )
            .await?,
        "the caller's stale proposal must not clear Postgres' canonical owner"
    );
    assert!(
        fixture
            .store
            .abandon_activation_attempt_superseded_by_suspend(
                sandbox_id,
                execution.generation,
                assigned_attempt,
            )
            .await?,
        "the exact canonical owner must be abandoned after suspend supersedes recovery"
    );
    let persisted_attempt: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(persisted_attempt, None);

    Ok(())
}

async fn stale_generation_cannot_complete_or_abandon_reused_attempt(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let attempt = SandboxActivationAttemptId::new();
    let first = fixture
        .store
        .begin_activation_execution_row(sandbox_id, attempt, SandboxTransitionTargetState::Started)
        .await?;
    let successor_generation = first.generation + 1;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active,
             activation_attempt_id, route_activation_id,
             activation_attempt_expires_at, activation_attempt_target_state
         ) VALUES (
             $1, $2, $3, 'space', 'successor-pod', 'running', TRUE,
             $4, $4, NOW() + INTERVAL '5 minutes', 'started'
         )"
    ))
    .bind(sandbox_id)
    .bind(successor_generation)
    .bind(format!("space-sbx-{sandbox_id}-g{successor_generation}"))
    .bind(attempt)
    .execute(&fixture.pool)
    .await?;

    assert!(
        !fixture
            .store
            .complete_activation_attempt(sandbox_id, first.generation, attempt,)
            .await?,
        "generation-N completion must not clear generation N+1"
    );
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspended' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    assert!(
        !fixture
            .store
            .abandon_activation_attempt_superseded_by_suspend(
                sandbox_id,
                first.generation,
                attempt,
            )
            .await?,
        "generation-N abandonment must not clear generation N+1"
    );
    let persisted: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2 AND active"
    ))
    .bind(sandbox_id)
    .bind(successor_generation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(persisted, Some(attempt.as_uuid()));

    Ok(())
}

async fn replacement_handler_claims_g7_after_lagging_g6_is_replaced(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;

    let mut active_claim = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?;
    while active_claim.row.generation < 6 {
        assert!(
            fixture
                .store
                .finish_active_execution_if_generation(
                    sandbox_id,
                    active_claim.row.generation,
                    "replaced",
                )
                .await?
        );
        active_claim = fixture
            .store
            .ensure_active_execution_claim(sandbox_id)
            .await?;
    }
    let handler_a_claim = active_claim;
    assert_eq!(handler_a_claim.row.generation, 6);
    assert_eq!(
        handler_a_claim.row.pod_name,
        format!("space-sbx-{}-g6", sandbox_id.as_uuid().simple())
    );
    assert!(
        fixture
            .store
            .active_execution_is_generation(sandbox_id, handler_a_claim.row.generation,)
            .await?
    );

    let replacement_attempt = SandboxActivationAttemptId::new();
    let handler_b_claim = fixture
        .store
        .replace_active_execution_claim_if_generation(
            sandbox_id,
            handler_a_claim.row.generation,
            "replaced",
            Some((replacement_attempt, SandboxTransitionTargetState::Started)),
        )
        .await?
        .expect("handler B should replace the stale generation");
    assert!(handler_b_claim.created);
    assert_eq!(handler_b_claim.row.generation, 7);
    assert_eq!(
        handler_b_claim.row.pod_name,
        format!("space-sbx-{}-g7", sandbox_id.as_uuid().simple())
    );
    assert_eq!(
        handler_b_claim.row.activation_attempt_id,
        Some(replacement_attempt)
    );
    let replacement_route: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT route_activation_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2 AND active"
    ))
    .bind(sandbox_id)
    .bind(handler_b_claim.row.generation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        replacement_route,
        Some(replacement_attempt.as_uuid()),
        "a replacement execution initializes its first route in the replacement transaction"
    );

    let lagging_handler_a_replacement = fixture
        .store
        .replace_active_execution_claim_if_generation(
            sandbox_id,
            handler_a_claim.row.generation,
            "replaced",
            None,
        )
        .await?;
    assert!(
        lagging_handler_a_replacement.is_none(),
        "handler A must not replace the new active generation with its stale g6 claim"
    );
    assert!(
        !fixture
            .store
            .active_execution_is_generation(sandbox_id, handler_a_claim.row.generation,)
            .await?,
        "handler A's lagging g6 claim must not create a pod after handler B replaces it"
    );
    assert!(
        fixture
            .store
            .active_execution_is_generation(sandbox_id, handler_b_claim.row.generation,)
            .await?
    );
    Ok(())
}

async fn stale_generation_pod_metadata_does_not_update_new_active_execution(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;

    let stale = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?;
    assert_eq!(stale.row.generation, 1);

    assert!(
        fixture
            .store
            .finish_active_execution_if_generation(sandbox_id, stale.row.generation, "replaced",)
            .await?
    );
    let active = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?;
    assert_eq!(active.row.generation, 2);

    let stale_write_landed = fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            stale.row.generation,
            Some("stale-pod-uid"),
            Some("10.90.0.7"),
        )
        .await?;
    assert!(
        stale_write_landed.is_none(),
        "a stale-generation pod metadata write must report itself as a no-op — \
         callers that treat 'metadata persisted' as evidence branch on this"
    );

    let active_after_stale_write = fixture
        .store
        .active_execution_status(sandbox_id)
        .await?
        .expect("replacement execution should remain active");
    assert_eq!(active_after_stale_write.generation, active.row.generation);
    assert_eq!(active_after_stale_write.pod_uid, None);
    assert_eq!(active_after_stale_write.pod_ip, None);

    let active_write_landed = fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            active.row.generation,
            Some("active-pod-uid"),
            Some("10.90.0.8"),
        )
        .await?;
    assert_eq!(
        active_write_landed,
        Some(ActiveExecutionState::Running),
        "a current-generation pod metadata write with an IP must report the \
         promoted running state"
    );
    let active_after_active_write = fixture
        .store
        .active_execution_status(sandbox_id)
        .await?
        .expect("replacement execution should remain active");
    assert_eq!(active_after_active_write.generation, active.row.generation);
    assert_eq!(
        active_after_active_write
            .pod_uid
            .as_ref()
            .map(PodUid::as_str),
        Some("active-pod-uid")
    );
    assert_eq!(
        active_after_active_write.pod_ip.as_deref(),
        Some("10.90.0.8")
    );

    Ok(())
}

async fn blank_pod_uid_decodes_as_absent_for_active_execution(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let execution = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
         SET pod_uid = '   '
         WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(execution.generation)
    .execute(&fixture.pool)
    .await?;

    let active = fixture
        .store
        .active_execution_status(sandbox_id)
        .await?
        .expect("active execution should exist");
    assert_eq!(active.pod_uid, None);

    let attempt = SandboxActivationAttemptId::new();
    let initialized = fixture
        .store
        .ensure_active_execution_claim_for_lifecycle(
            sandbox_id,
            SandboxLifecycle::Started,
            None,
            Some((attempt, SandboxTransitionTargetState::Started)),
        )
        .await?;
    assert_eq!(initialized.row.activation_attempt_id, Some(attempt));
    let route: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT route_activation_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(execution.generation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(route, Some(attempt.as_uuid()));

    Ok(())
}

/// A `recovering` execution keeps its state through a pod metadata write
/// (the node-side recovery flow owns the promote to `running`), and the
/// persist reports that resulting state so callers cannot mistake
/// "pod IP recorded" for "execution is routable". The sweeper's
/// hydration claims `Hydrated` only on a resulting `running` state — a
/// `recovering` row defers instead of emitting a repair that the
/// converge CAS's `state = 'running'` predicate would refuse anyway.
async fn persist_pod_metadata_reports_recovering_state_unchanged(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let execution = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
         SET state = 'recovering'
         WHERE sandbox_id = $1 AND generation = $2 AND active"
    ))
    .bind(sandbox_id)
    .bind(execution.generation)
    .execute(&fixture.pool)
    .await?;

    let resulting_state = fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            execution.generation,
            Some("pod-uid"),
            Some("10.90.0.13"),
        )
        .await?;
    assert_eq!(
        resulting_state,
        Some(ActiveExecutionState::Recovering),
        "a recovering execution must keep its state and report it — not a \
         routable running result"
    );
    let (state, pod_ip): (String, Option<String>) = sqlx::query_as(sql!(
        "SELECT state, host(pod_ip)
         FROM sandbox_k8s_executions
         WHERE sandbox_id = $1 AND generation = $2 AND active"
    ))
    .bind(sandbox_id)
    .bind(execution.generation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(state, "recovering");
    assert_eq!(pod_ip.as_deref(), Some("10.90.0.13"));
    Ok(())
}

/// A retirement that commits while the metadata persist is waiting on
/// the row lock must make the persist report a no-op. The old
/// data-modifying-CTE + `SELECT EXISTS` shape got this wrong: the UPDATE
/// re-checked `active` via EvalPlanQual and matched zero rows, but the
/// trailing EXISTS read the statement-start snapshot — which still held
/// the pre-retirement active version — and reported success.
async fn persist_pod_metadata_reports_noop_when_retirement_commits_mid_write(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let execution = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;

    // Conn A: retire the active row and hold the transaction open, so
    // the persist blocks on the row lock.
    let mut retiring_tx = fixture.pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
         SET active = FALSE, state = 'suspended'
         WHERE sandbox_id = $1 AND generation = $2 AND active"
    ))
    .bind(sandbox_id)
    .bind(execution.generation)
    .execute(&mut *retiring_tx)
    .await?;

    // Conn B: the persist, which must block behind A's row lock.
    let store = K8sStore::new(fixture.pool.clone(), "space".to_string());
    let generation = execution.generation;
    let persist = tokio::spawn(async move {
        store
            .persist_pod_metadata(sandbox_id, generation, Some("pod-uid"), Some("10.90.0.12"))
            .await
    });
    // Give the persist time to reach (and block on) the locked row, then
    // commit the retirement out from under it.
    tokio::time::sleep(Duration::from_millis(200)).await;
    retiring_tx.commit().await?;

    let persisted = persist.await??;
    assert!(
        persisted.is_none(),
        "a persist that lost to a concurrently-committed retirement must \
         report itself as a no-op, not a successful write"
    );
    let (pod_uid, pod_ip): (Option<String>, Option<String>) = sqlx::query_as(sql!(
        "SELECT pod_uid, host(pod_ip)
         FROM sandbox_k8s_executions
         WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!((pod_uid, pod_ip), (None, None), "nothing may be written");
    Ok(())
}

async fn persist_pod_metadata_normalizes_blank_pod_uid_to_null(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let execution = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;

    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            execution.generation,
            Some("   "),
            Some("10.90.0.8"),
        )
        .await?;

    let active_pod_uid: Option<String> = sqlx::query_scalar(sql!(
        "SELECT pod_uid
         FROM sandbox_k8s_executions
         WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(execution.generation)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(active_pod_uid, None);

    Ok(())
}

async fn forget_pod_tracking_if_generation_does_not_forget_newer_active_execution(
    fixture: &PostgresStore,
) -> TestResult {
    // Arrange.
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;

    let first = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    assert!(
        fixture
            .store
            .finish_active_execution_if_generation(sandbox_id, first.generation, "replaced")
            .await?
    );
    let second = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;

    // Act.
    let forgotten = fixture
        .store
        .forget_pod_tracking_if_generation(
            sandbox_id,
            first.generation,
            PodTrackingCleanupCause::MissingPodObservation,
        )
        .await?;

    // Assert.
    assert!(!forgotten);
    let active = fixture
        .store
        .active_execution_status(sandbox_id)
        .await?
        .expect("newer execution should remain active");
    assert_eq!(active.generation, second.generation);
    assert_eq!(active.pod_name, second.pod_name);

    Ok(())
}

async fn forget_pod_tracking_if_generation_does_not_forget_successor_attempt(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let execution = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    let stale_attempt = pplx_space_common_core::types::SandboxActivationAttemptId::new();
    let successor_attempt = pplx_space_common_core::types::SandboxActivationAttemptId::new();
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = $2,
                activation_attempt_expires_at = NOW() + INTERVAL '5 minutes',
                activation_attempt_target_state = 'started'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .bind(successor_attempt)
    .execute(&fixture.pool)
    .await?;

    assert!(
        !fixture
            .store
            .forget_pod_tracking_if_generation_and_attempt(
                sandbox_id,
                execution.generation,
                stale_attempt,
            )
            .await?,
        "a stale activation must not retire a successor that reused its generation"
    );
    let active_attempt: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(active_attempt, Some(successor_attempt.as_uuid()));

    Ok(())
}

async fn forget_pod_tracking_defers_to_suspend_finalization(fixture: &PostgresStore) -> TestResult {
    // A pod death observed while the row is `suspending` belongs to the
    // suspend flow: finalization retires the execution as `suspended`.
    // Forgetting it here clears placement, which makes the publish
    // ingest drop the node's final `suspended` record and strands the
    // row in `suspending`.
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "suspending").await?;
    let execution = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;

    let forgotten = fixture
        .store
        .forget_pod_tracking_if_generation(
            sandbox_id,
            execution.generation,
            PodTrackingCleanupCause::MissingPodObservation,
        )
        .await?;

    assert!(
        !forgotten,
        "missing-pod cleanup must defer while the sandbox is suspending"
    );
    let active = fixture
        .store
        .active_execution_status(sandbox_id)
        .await?
        .expect("suspending sandbox must keep its active execution for finalization");
    assert_eq!(active.generation, execution.generation);
    assert_eq!(
        active.state,
        ActiveExecutionState::Creating,
        "the deferred execution must stay in its pre-suspend state for finalization to retire"
    );

    // Once the suspend resolves, the same cleanup must work again.
    sqlx::query("UPDATE sandboxes SET lifecycle = 'stopped' WHERE id = $1")
        .bind(sandbox_id)
        .execute(&fixture.pool)
        .await?;
    let forgotten = fixture
        .store
        .forget_pod_tracking_if_generation(
            sandbox_id,
            execution.generation,
            PodTrackingCleanupCause::MissingPodObservation,
        )
        .await?;
    assert!(
        forgotten,
        "the deferral must end with the suspending lifecycle"
    );

    Ok(())
}

async fn explicit_teardown_forget_retires_execution_while_suspending(
    fixture: &PostgresStore,
) -> TestResult {
    // Explicit delete/retire teardown deletes the pod itself and has no
    // suspend finalizer behind it: it must retire tracking even while
    // the sandbox row is `suspending`, or the row lingers with stale
    // tracking until the sweeper.
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "suspending").await?;
    fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;

    fixture
        .store
        .forget_pod_tracking(sandbox_id, PlacementClock::Bump)
        .await?;

    assert!(
        fixture
            .store
            .active_execution_status(sandbox_id)
            .await?
            .is_none(),
        "explicit teardown must retire the execution even while suspending"
    );
    let retired_state: String = sqlx::query_scalar(
        "SELECT state FROM sandbox_k8s_executions WHERE sandbox_id = $1 ORDER BY generation DESC LIMIT 1",
    )
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        retired_state, "forgotten",
        "explicit teardown must retire the execution as forgotten"
    );

    Ok(())
}

async fn forget_pod_tracking_recovers_markerless_restoring_with_ready_snapshot(
    fixture: &PostgresStore,
) -> TestResult {
    // Arrange.
    let sandbox_id = SandboxId::new();
    let uri = "s3://space-test/recovered-markerless-restore?region=us-east-1";
    insert_sandbox(&fixture.pool, sandbox_id, "restoring").await?;
    insert_ready_full_snapshot(&fixture.pool, sandbox_id, uri).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET placement = 'on_node',
             placement_changed_at = NOW(),
             restore_lease_expires_at = NOW() + INTERVAL '2 minutes',
             restore_lease_token = $2
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .execute(&fixture.pool)
    .await?;
    let execution = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;

    // Act.
    let forgotten = fixture
        .store
        .forget_pod_tracking_if_generation(
            sandbox_id,
            execution.generation,
            PodTrackingCleanupCause::MissingPodObservation,
        )
        .await?;

    // Assert.
    assert!(forgotten);
    let (
        lifecycle,
        placement,
        active_node_id,
        target_node_id,
        placement_changed_at,
        restore_lease_expires_at,
        restore_lease_token,
    ): ForgottenRestoreRow = sqlx::query_as(sql!(
        "SELECT lifecycle,
                placement,
                active_node_id,
                target_node_id,
                placement_changed_at,
                restore_lease_expires_at,
                restore_lease_token
         FROM sandboxes
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(lifecycle, "suspended");
    assert_eq!(placement, "unplaced");
    assert_eq!(active_node_id, None);
    assert_eq!(target_node_id, None);
    assert_eq!(placement_changed_at, None);
    assert_eq!(restore_lease_expires_at, None);
    assert_eq!(restore_lease_token, None);

    let active_count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(active_count, 0);

    let sandbox = fixture.store.require_sandbox(sandbox_id).await?;
    let selected_uri = fixture
        .store
        .selected_suspend_restore_storage_uri(&sandbox, false)
        .await?;
    assert_eq!(selected_uri.as_deref(), Some(uri));

    Ok(())
}

async fn forget_pod_tracking_keeps_restoring_when_recovery_is_not_safe(
    fixture: &PostgresStore,
) -> TestResult {
    // Arrange.
    let no_snapshot = SandboxId::new();
    insert_sandbox(&fixture.pool, no_snapshot, "restoring").await?;
    let no_snapshot_execution = fixture
        .store
        .ensure_active_execution_row(no_snapshot)
        .await?;

    let marked = SandboxId::new();
    insert_sandbox(&fixture.pool, marked, "restoring").await?;
    insert_ready_full_snapshot(
        &fixture.pool,
        marked,
        "s3://space-test/marked-restore?region=us-east-1",
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET evacuation_restore_target = 'started'
         WHERE id = $1"
    ))
    .bind(marked)
    .execute(&fixture.pool)
    .await?;
    let marked_execution = fixture.store.ensure_active_execution_row(marked).await?;

    // Act.
    assert!(
        fixture
            .store
            .forget_pod_tracking_if_generation(
                no_snapshot,
                no_snapshot_execution.generation,
                PodTrackingCleanupCause::MissingPodObservation
            )
            .await?
    );
    assert!(
        fixture
            .store
            .forget_pod_tracking_if_generation(
                marked,
                marked_execution.generation,
                PodTrackingCleanupCause::MissingPodObservation
            )
            .await?
    );

    // Assert.
    let no_snapshot_lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(no_snapshot)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(no_snapshot_lifecycle, "restoring");

    let (marked_lifecycle, marker): (String, Option<String>) = sqlx::query_as(sql!(
        "SELECT lifecycle, evacuation_restore_target FROM sandboxes WHERE id = $1"
    ))
    .bind(marked)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(marked_lifecycle, "restoring");
    assert_eq!(marker.as_deref(), Some("started"));

    Ok(())
}

async fn forget_pod_tracking_keeps_restoring_with_late_branch_artifact(
    fixture: &PostgresStore,
) -> TestResult {
    // Arrange.
    let sandbox_id = SandboxId::new();
    let source_artifact_id = LocalArtifactId::new();
    let late_branch_artifact_id = LocalArtifactId::new();
    let source_uri = "s3://space-test/g2-source?region=us-east-1";

    insert_sandbox(&fixture.pool, sandbox_id, "restoring").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET current_local_artifact_id = $2,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(late_branch_artifact_id)
    .execute(&fixture.pool)
    .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        source_uri,
        -1,
        Some(source_artifact_id),
    )
    .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/g3-late-branch?region=us-east-1",
        1,
        Some(late_branch_artifact_id),
    )
    .await?;
    let execution = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;

    // Act.
    let forgotten = fixture
        .store
        .forget_pod_tracking_if_generation(
            sandbox_id,
            execution.generation,
            PodTrackingCleanupCause::MissingPodObservation,
        )
        .await?;

    // Assert.
    assert!(forgotten);
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(
        lifecycle, "restoring",
        "cleanup must not flip to suspended when suspended selection would choose a late branch artifact"
    );

    let active_count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(active_count, 0);

    let sandbox = fixture.store.require_sandbox(sandbox_id).await?;
    let selected_uri = fixture
        .store
        .selected_suspend_restore_storage_uri(&sandbox, false)
        .await?;
    assert_eq!(selected_uri.as_deref(), Some(source_uri));

    Ok(())
}

async fn insert_ready_full_snapshot_relative_to_lifecycle(
    pool: &PgPool,
    sandbox_id: SandboxId,
    uri: &str,
    timestamp_offset_minutes: i32,
    local_artifact_id: Option<LocalArtifactId>,
) -> TestResult<SnapshotId> {
    let snapshot_id = SnapshotId::new();
    let artifact_kind = local_artifact_id.map(|_| "full");
    let artifact_origin = local_artifact_id.map(|_| "suspend");
    sqlx::query(sql!(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, timestamp, org_id,
              local_artifact_id, artifact_kind, artifact_origin)
         VALUES ($1, $2, 'full', 'ready',
                 (SELECT lifecycle_changed_at + ($3 * INTERVAL '1 minute')
                    FROM sandboxes WHERE id = $2),
                 '00000000-0000-0000-0000-000000000001',
                 $4, $5, $6)"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(timestamp_offset_minutes)
    .bind(local_artifact_id)
    .bind(artifact_kind)
    .bind(artifact_origin)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, '00000000-0000-0000-0000-000000000001')"
    ))
    .bind(snapshot_id)
    .bind(uri)
    .execute(pool)
    .await?;
    Ok(snapshot_id)
}

async fn forget_pod_tracking_recovers_started_suspended_by_node_after_current_backup(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let uri = "s3://space-test/current-access-idle-suspend?region=us-east-1";
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let execution = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    fixture
        .store
        .persist_pod_node_placement(
            sandbox_id,
            "worker-node-a",
            "worker-node-a-uid",
            node_id,
            NodeRole::Worker,
            Some("10.90.0.10"),
        )
        .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET command_ready_pending = TRUE
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    let current_artifact_id = LocalArtifactId::new();
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET current_local_artifact_id = $2,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(current_artifact_id)
    .execute(&fixture.pool)
    .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        uri,
        1,
        Some(current_artifact_id),
    )
    .await?;

    let forgotten = fixture
        .store
        .forget_pod_tracking_if_generation(
            sandbox_id,
            execution.generation,
            PodTrackingCleanupCause::MissingPodObservation,
        )
        .await?;

    assert!(forgotten);
    let (
        lifecycle,
        placement,
        active_node_id,
        target_node_id,
        placement_changed_at,
        command_ready_pending,
    ): (
        String,
        String,
        Option<NodeId>,
        Option<NodeId>,
        Option<OffsetDateTime>,
        bool,
    ) = sqlx::query_as(sql!(
        "SELECT lifecycle,
                placement,
                active_node_id,
                target_node_id,
                placement_changed_at,
                command_ready_pending
         FROM sandboxes
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(lifecycle, "suspended");
    assert_eq!(placement, "unplaced");
    assert_eq!(active_node_id, None);
    assert_eq!(target_node_id, None);
    assert_eq!(placement_changed_at, None);
    assert!(!command_ready_pending);

    let sandbox = fixture.store.require_sandbox(sandbox_id).await?;
    let selected_uri = fixture
        .store
        .selected_suspend_restore_storage_uri(&sandbox, false)
        .await?;
    assert_eq!(selected_uri.as_deref(), Some(uri));
    let source_node = fixture.store.restore_source_node_name(&sandbox).await?;
    assert_eq!(source_node.as_deref(), Some("worker-node-a"));

    Ok(())
}

async fn forget_pod_tracking_keeps_started_without_current_suspend_backup(
    fixture: &PostgresStore,
) -> TestResult {
    let stale_backup = SandboxId::new();
    insert_sandbox(&fixture.pool, stale_backup, "started").await?;
    let stale_execution = fixture
        .store
        .ensure_active_execution_row(stale_backup)
        .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        stale_backup,
        "s3://space-test/stale-access-idle-suspend?region=us-east-1",
        -1,
        None,
    )
    .await?;

    let no_backup = SandboxId::new();
    insert_sandbox(&fixture.pool, no_backup, "started").await?;
    let no_backup_execution = fixture.store.ensure_active_execution_row(no_backup).await?;

    assert!(
        fixture
            .store
            .forget_pod_tracking_if_generation(
                stale_backup,
                stale_execution.generation,
                PodTrackingCleanupCause::MissingPodObservation
            )
            .await?
    );
    assert!(
        fixture
            .store
            .forget_pod_tracking_if_generation(
                no_backup,
                no_backup_execution.generation,
                PodTrackingCleanupCause::MissingPodObservation
            )
            .await?
    );

    let stale_lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(stale_backup)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(stale_lifecycle, "started");

    let no_backup_lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(no_backup)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(no_backup_lifecycle, "started");

    Ok(())
}

async fn unchanged_pod_metadata_does_not_touch_execution_row(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let execution = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;

    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            execution.generation,
            Some("pod-uid"),
            Some("10.90.0.7"),
        )
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
         SET updated_at = TIMESTAMPTZ '2024-01-01 00:00:00+00'
         WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            execution.generation,
            Some("pod-uid"),
            Some("10.90.0.7"),
        )
        .await?;

    let execution_unchanged: bool = sqlx::query_scalar(sql!(
        "SELECT updated_at = TIMESTAMPTZ '2024-01-01 00:00:00+00'
         FROM sandbox_k8s_executions
         WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert!(execution_unchanged);

    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            execution.generation,
            Some("pod-uid"),
            Some("10.90.0.8"),
        )
        .await?;
    let (state, execution_ip, execution_touched): (String, Option<String>, bool) =
        sqlx::query_as(sql!(
            "SELECT state, host(pod_ip),
                    updated_at <> TIMESTAMPTZ '2024-01-01 00:00:00+00'
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND active"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(state, "running");
    assert_eq!(execution_ip.as_deref(), Some("10.90.0.8"));
    assert!(execution_touched);

    Ok(())
}

async fn unchanged_pod_node_placement_does_not_touch_current_rows(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let node_name = format!("space-test-node-{}", node_id.as_uuid().simple());
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;

    fixture
        .store
        .persist_pod_node_placement(
            sandbox_id,
            &node_name,
            "node-uid",
            node_id,
            NodeRole::Worker,
            Some("10.0.0.9"),
        )
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
         SET updated_at = TIMESTAMPTZ '2024-01-01 00:00:00+00'
         WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET placement_changed_at = TIMESTAMPTZ '2024-01-01 00:00:00+00'
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;

    fixture
        .store
        .persist_pod_node_placement(
            sandbox_id,
            &node_name,
            "node-uid",
            node_id,
            NodeRole::Worker,
            Some("10.0.0.9"),
        )
        .await?;

    let (execution_unchanged, sandbox_unchanged): (bool, bool) = sqlx::query_as(sql!(
        "SELECT
                (
                    SELECT updated_at = TIMESTAMPTZ '2024-01-01 00:00:00+00'
                    FROM sandbox_k8s_executions
                    WHERE sandbox_id = $1 AND active
                ),
                (
                    SELECT placement_changed_at = TIMESTAMPTZ '2024-01-01 00:00:00+00'
                    FROM sandboxes
                    WHERE id = $1
                )"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert!(execution_unchanged);
    assert!(sandbox_unchanged);
    Ok(())
}

async fn finishing_active_execution_preserves_active_node_for_replacement(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET active_node_id = $1,
             target_node_id = $1,
             placement = 'on_node'
         WHERE id = $2"
    ))
    .bind(node_id)
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;

    let first = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    fixture
        .store
        .finish_active_execution_if_generation(sandbox_id, first.generation, "replaced")
        .await?;
    let second = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;

    assert_eq!(first.generation, 1);
    assert_eq!(second.generation, 2);
    assert_eq!(
        second.pod_name,
        format!("space-sbx-{}-g2", sandbox_id.as_uuid().simple())
    );

    let active_count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(active_count, 1);

    let (placement, active_node_id, target_node_id): (String, Option<NodeId>, Option<NodeId>) =
        sqlx::query_as(sql!(
            "SELECT placement, active_node_id, target_node_id
             FROM sandboxes
             WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(placement, "on_node");
    assert_eq!(active_node_id, Some(node_id));
    assert_eq!(target_node_id, Some(node_id));
    Ok(())
}

async fn finishing_active_execution_rolls_back_port_observation_failure(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET active_node_id = $1,
             target_node_id = $1,
             placement = 'on_node'
         WHERE id = $2"
    ))
    .bind(node_id)
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    let execution = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_port_exposures (
             sandbox_id, protocol, guest_port, visibility, source,
             desired_state, apply_state, last_bridge_status, last_observed_at
         )
         VALUES (
             $1, 'tcp', 9012, 'private', 'api',
             'open', 'applied', 'loopback_forwarded', NOW()
         )"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;

    sqlx::query(sql!(
        "DROP TRIGGER IF EXISTS fail_port_exposure_observation_update ON sandbox_port_exposures"
    ))
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "DROP FUNCTION IF EXISTS fail_port_exposure_observation_update()"
    ))
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "CREATE FUNCTION fail_port_exposure_observation_update()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             RAISE EXCEPTION 'forced port exposure observation update failure';
         END;
         $$"
    ))
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TRIGGER fail_port_exposure_observation_update
         BEFORE UPDATE ON sandbox_port_exposures
         FOR EACH ROW
         EXECUTE FUNCTION fail_port_exposure_observation_update();"
    ))
    .execute(&fixture.pool)
    .await?;

    let result = fixture
        .store
        .finish_active_execution_if_generation(sandbox_id, execution.generation, "replaced")
        .await;
    sqlx::query(sql!(
        "DROP TRIGGER fail_port_exposure_observation_update ON sandbox_port_exposures"
    ))
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "DROP FUNCTION fail_port_exposure_observation_update()"
    ))
    .execute(&fixture.pool)
    .await?;
    result.unwrap_err();

    let active_count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(active_count, 1);

    let (apply_state, bridge_status, has_observation): (String, Option<String>, bool) =
        sqlx::query_as(sql!(
            "SELECT apply_state, last_bridge_status, last_observed_at IS NOT NULL
             FROM sandbox_port_exposures
             WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(apply_state, "applied");
    assert_eq!(bridge_status.as_deref(), Some("loopback_forwarded"));
    assert!(has_observation);

    let (placement, active_node_id, target_node_id): (String, Option<NodeId>, Option<NodeId>) =
        sqlx::query_as(sql!(
            "SELECT placement, active_node_id, target_node_id
             FROM sandboxes
             WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(placement, "on_node");
    assert_eq!(active_node_id, Some(node_id));
    assert_eq!(target_node_id, Some(node_id));
    Ok(())
}

async fn finish_suspended_execution_clears_active_node_but_preserves_target_node(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "suspending").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET active_node_id = $1,
             target_node_id = $1,
             placement = 'on_node',
             command_ready_pending = TRUE
         WHERE id = $2"
    ))
    .bind(node_id)
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_port_exposures (
             sandbox_id, protocol, guest_port, visibility, source,
             desired_state, apply_state, last_applied_node_id, last_error,
             last_bridge_status, last_applied_bridge_generation, last_observed_at
         )
         VALUES (
             $1, 'tcp', 9013, 'private', 'api',
             'open', 'applied', $2, 'stale error',
             'loopback_forwarded', 'bridge-generation-1', NOW()
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&fixture.pool)
    .await?;
    fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;

    finish_suspended_execution_for_test(fixture, sandbox_id).await?;

    let (lifecycle, placement, active_node_id, target_node_id, command_ready_pending): (
        String,
        String,
        Option<NodeId>,
        Option<NodeId>,
        bool,
    ) = sqlx::query_as(sql!(
        "SELECT lifecycle,
                placement,
                active_node_id,
                target_node_id,
                command_ready_pending
             FROM sandboxes
             WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(lifecycle, "suspended");
    assert_eq!(placement, "unplaced");
    assert_eq!(active_node_id, None);
    assert_eq!(target_node_id, Some(node_id));
    assert!(!command_ready_pending);

    let active_execution_exists: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS(
             SELECT 1 FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND active
         )"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert!(!active_execution_exists);

    let (
        apply_state,
        last_applied_node_id,
        last_error,
        last_bridge_status,
        last_applied_bridge_generation,
        has_last_observed_at,
    ): (
        String,
        Option<NodeId>,
        Option<String>,
        Option<String>,
        Option<String>,
        bool,
    ) = sqlx::query_as(sql!(
        "SELECT apply_state,
                last_applied_node_id,
                last_error,
                last_bridge_status,
                last_applied_bridge_generation,
                last_observed_at IS NOT NULL
           FROM sandbox_port_exposures
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(apply_state, "pending");
    assert_eq!(last_applied_node_id, None);
    assert_eq!(last_error, None);
    assert_eq!(last_bridge_status, None);
    assert_eq!(last_applied_bridge_generation, None);
    assert!(!has_last_observed_at);
    Ok(())
}

async fn finish_suspended_execution_rejects_uncommitted_running_execution(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "suspending").await?;
    let claim = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    let pod_uid = PodUid::parse(Uuid::now_v7().to_string())?;
    fixture
        .store
        .persist_pod_metadata(sandbox_id, claim.generation, Some(pod_uid.as_str()), None)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'running'
          WHERE sandbox_id = $1
            AND generation = $2
            AND active"
    ))
    .bind(sandbox_id)
    .bind(claim.generation)
    .execute(&fixture.pool)
    .await?;

    let error = fixture
        .store
        .finish_suspended_execution(
            sandbox_id,
            ExecutionGeneration::new(claim.generation)?,
            &pod_uid,
            super::super::super::PublishedLocalArtifact {
                id: LocalArtifactId::new(),
                kind: ArtifactKind::Full,
            },
        )
        .await
        .expect_err("running execution without durable evidence must stay active");
    assert!(matches!(error, SubstrateError::Unavailable { .. }));

    let execution = fixture
        .store
        .active_execution_status(sandbox_id)
        .await?
        .expect("uncommitted execution must remain active");
    assert_eq!(execution.state, ActiveExecutionState::Running);
    Ok(())
}

async fn suspend_commit_rejects_artifact_claimed_for_prune(fixture: &PostgresStore) -> TestResult {
    let sandbox_id = SandboxId::new();
    let typed_sandbox_id = sandbox_id;
    insert_sandbox(&fixture.pool, sandbox_id, "suspending").await?;
    let claim = fixture
        .store
        .ensure_active_execution_row(typed_sandbox_id)
        .await?;
    let generation = ExecutionGeneration::new(claim.generation)?;
    let pod_uid = PodUid::parse(Uuid::now_v7().to_string())?;
    fixture
        .store
        .persist_pod_metadata(sandbox_id, claim.generation, Some(pod_uid.as_str()), None)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'running'
          WHERE sandbox_id = $1
            AND generation = $2
            AND active"
    ))
    .bind(sandbox_id)
    .bind(claim.generation)
    .execute(&fixture.pool)
    .await?;

    let published_artifact = super::super::super::PublishedLocalArtifact {
        id: LocalArtifactId::new(),
        kind: ArtifactKind::Full,
    };
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, org_id,
             local_artifact_id, artifact_kind, artifact_origin
         )
         VALUES (
             $1, $2, 'full', 'deleting',
             '00000000-0000-0000-0000-000000000001',
             $3, 'full', 'suspend'
         )"
    ))
    .bind(Uuid::now_v7())
    .bind(sandbox_id)
    .bind(published_artifact.id.as_uuid())
    .execute(&fixture.pool)
    .await?;

    let error = fixture
        .store
        .persist_or_accept_suspend_commit(
            typed_sandbox_id,
            generation,
            &pod_uid,
            published_artifact,
        )
        .await
        .expect_err("a prune claim must fence a matching suspend completion retry");
    assert!(matches!(error, SubstrateError::Unavailable { .. }));

    let current_artifact: Option<LocalArtifactId> = sqlx::query_scalar(sql!(
        "SELECT current_local_artifact_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(current_artifact, None);
    let execution = fixture
        .store
        .active_execution_status(sandbox_id)
        .await?
        .expect("rejected completion must leave the execution active");
    assert_eq!(execution.state, ActiveExecutionState::Running);
    Ok(())
}

async fn finish_suspended_execution_rejects_mismatched_artifact(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let typed_sandbox_id = sandbox_id;
    insert_sandbox(&fixture.pool, sandbox_id, "suspending").await?;
    let claim = fixture
        .store
        .ensure_active_execution_row(typed_sandbox_id)
        .await?;
    let generation = ExecutionGeneration::new(claim.generation)?;
    let pod_uid = PodUid::parse(Uuid::now_v7().to_string())?;
    fixture
        .store
        .persist_pod_metadata(sandbox_id, claim.generation, Some(pod_uid.as_str()), None)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'running'
          WHERE sandbox_id = $1
            AND generation = $2
            AND active"
    ))
    .bind(sandbox_id)
    .bind(claim.generation)
    .execute(&fixture.pool)
    .await?;
    let committed_artifact = super::super::super::PublishedLocalArtifact {
        id: LocalArtifactId::new(),
        kind: ArtifactKind::Full,
    };
    fixture
        .store
        .persist_or_accept_suspend_commit(
            typed_sandbox_id,
            generation,
            &pod_uid,
            committed_artifact,
        )
        .await?;

    let mismatched_artifact = super::super::super::PublishedLocalArtifact {
        id: LocalArtifactId::new(),
        kind: ArtifactKind::Full,
    };
    assert!(matches!(
        fixture
            .store
            .finish_suspended_execution(sandbox_id, generation, &pod_uid, mismatched_artifact,)
            .await,
        Err(SubstrateError::Unavailable { .. })
    ));
    let execution = fixture
        .store
        .active_execution_status(sandbox_id)
        .await?
        .expect("artifact mismatch must keep the execution active");
    assert_eq!(execution.state, ActiveExecutionState::Suspended);
    Ok(())
}

async fn finish_suspended_execution_accepts_matching_projection_race(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id =
        seed_bound_claimed_pool_slot(fixture, "suspending", "pool-projection-race").await?;
    let execution = fixture
        .store
        .active_execution_status(sandbox_id)
        .await?
        .expect("seeded execution must be active");
    let generation = ExecutionGeneration::new(execution.generation)?;
    let pod_uid = execution
        .pod_uid
        .expect("seeded execution must have a pod UID");
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'running'
          WHERE sandbox_id = $1
            AND generation = $2
            AND active"
    ))
    .bind(sandbox_id)
    .bind(generation.get())
    .execute(&fixture.pool)
    .await?;
    let published_artifact = super::super::super::PublishedLocalArtifact {
        id: LocalArtifactId::new(),
        kind: ArtifactKind::Full,
    };
    fixture
        .store
        .persist_or_accept_suspend_commit(sandbox_id, generation, &pod_uid, published_artifact)
        .await?;

    let mut tx = fixture.pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE,
                state = 'suspended',
                pod_ip = NULL,
                terminated_at = NOW(),
                updated_at = NOW()
          WHERE sandbox_id = $1
            AND generation = $2
            AND pod_uid = $3
            AND active"
    ))
    .bind(sandbox_id)
    .bind(generation.get())
    .bind(pod_uid.as_str())
    .execute(&mut *tx)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'suspended',
                placement = 'unplaced',
                active_node_id = NULL,
                command_ready_pending = FALSE
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    assert_eq!(
        fixture
            .store
            .committed_suspend_artifact(sandbox_id, generation, &pod_uid,)
            .await?,
        Some(published_artifact),
        "the retirement handoff must accept the exact artifact after projection finalizes"
    );

    let mismatched_artifact = super::super::super::PublishedLocalArtifact {
        id: LocalArtifactId::new(),
        kind: ArtifactKind::Full,
    };
    assert!(matches!(
        fixture
            .store
            .finish_suspended_execution(sandbox_id, generation, &pod_uid, mismatched_artifact,)
            .await,
        Err(SubstrateError::Unavailable { .. })
    ));
    assert_eq!(
        claimed_slot_count(fixture, sandbox_id).await?,
        1,
        "artifact mismatch must not consume remaining finalization cleanup"
    );

    fixture
        .store
        .finish_suspended_execution(sandbox_id, generation, &pod_uid, published_artifact)
        .await?;
    assert_eq!(
        claimed_slot_count(fixture, sandbox_id).await?,
        0,
        "the API finalizer must complete cleanup after an exact projection race"
    );
    assert_eq!(
        fixture
            .store
            .claim_suspend_execution(sandbox_id, generation, &pod_uid)
            .await?,
        SuspendExecutionClaim::Completed(published_artifact),
        "a concurrent pause that read the exact execution before finalization must receive its completed suspend"
    );

    let successor = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    assert!(matches!(
        fixture
            .store
            .claim_suspend_execution(sandbox_id, generation, &pod_uid)
            .await,
        Err(SubstrateError::Unavailable { .. })
    ));
    assert!(matches!(
        fixture
            .store
            .committed_suspend_artifact(sandbox_id, generation, &pod_uid,)
            .await,
        Err(SubstrateError::Unavailable { .. })
    ));
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE
          WHERE sandbox_id = $1
            AND generation = $2
            AND active"
    ))
    .bind(sandbox_id)
    .bind(successor.generation)
    .execute(&fixture.pool)
    .await?;

    let successor_artifact = super::super::super::PublishedLocalArtifact {
        id: LocalArtifactId::new(),
        kind: ArtifactKind::Full,
    };
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = $3,
                current_local_artifact_origin = $4
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(successor_artifact.id.as_uuid())
    .bind(successor_artifact.kind.as_str())
    .bind(ArtifactOrigin::Suspend.as_str())
    .execute(&fixture.pool)
    .await?;

    assert!(matches!(
        fixture
            .store
            .committed_suspend_artifact(sandbox_id, generation, &pod_uid,)
            .await,
        Err(SubstrateError::Unavailable { .. })
    ));
    assert!(matches!(
        fixture
            .store
            .finish_suspended_execution(sandbox_id, generation, &pod_uid, successor_artifact,)
            .await,
        Err(SubstrateError::Unavailable { .. })
    ));
    Ok(())
}

/// Seed a pooled sandbox in the shape `persist_pooled_execution_claim`
/// leaves behind after bind: an active execution plus a `claimed` slot
/// whose reserved sandbox id and pod name match it.
/// `template_name` must be unique per call — it doubles as the release
/// hash and the slot's template columns.
async fn seed_bound_claimed_pool_slot(
    fixture: &PostgresStore,
    lifecycle: &str,
    template_name: &str,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let template_release_id = Uuid::now_v7();
    let config_id = Uuid::now_v7();
    insert_sandbox(&fixture.pool, sandbox_id, lifecycle).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET active_node_id = $1,
             target_node_id = $1,
             placement = 'on_node'
         WHERE id = $2"
    ))
    .bind(node_id)
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    let execution = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            execution.generation,
            Some("pod-uid"),
            Some("10.90.0.9"),
        )
        .await?;

    sqlx::query(sql!(
        "INSERT INTO templates (id, name, description, org_id)
         VALUES ($1, $2, NULL, NULL)
         ON CONFLICT (name) DO NOTHING"
    ))
    .bind(Uuid::now_v7())
    .bind(template_name)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash)
         VALUES ($1, $2, 'fc', 'sha256:' || $2)"
    ))
    .bind(template_release_id)
    .bind(template_name)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled
         )
         VALUES ($1, $2, 2, 1024, 1, 1, 1, 'space', true)"
    ))
    .bind(config_id)
    .bind(template_release_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_slots (
             config_id, template_release_id, reserved_sandbox_id, template_name,
             content_hash, vcpu, memory_mib, pod_name, pod_namespace, pod_uid,
             pod_ip, k8s_node_name, state, claim_id, ready_at, claimed_at
         )
         VALUES ($1, $2, $3, $4, 'sha256:' || $4,
                 2, 1024, $5, 'space', 'pod-uid', '10.90.0.9'::inet, 'worker-1',
                 'claimed', $6, NOW(), NOW())"
    ))
    .bind(config_id)
    .bind(template_release_id)
    .bind(sandbox_id)
    .bind(template_name)
    .bind(&execution.pod_name)
    .bind(Uuid::now_v7())
    .execute(&fixture.pool)
    .await?;
    Ok(sandbox_id)
}

async fn claimed_slot_count(fixture: &PostgresStore, sandbox_id: SandboxId) -> TestResult<i64> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM kubernetes_pool_slots WHERE reserved_sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?)
}

async fn finish_suspended_execution_deletes_bound_claimed_pool_slot(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id =
        seed_bound_claimed_pool_slot(fixture, "suspending", "pool-suspended-slot").await?;
    finish_suspended_execution_for_test(fixture, sandbox_id).await?;
    assert_eq!(
        claimed_slot_count(fixture, sandbox_id).await?,
        0,
        "suspend finalization must delete the bound claimed pool slot"
    );
    Ok(())
}

async fn suspend_commit_finishes_after_exact_execution_is_forgotten(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let typed_sandbox_id = sandbox_id;
    let pod_uid = PodUid::parse("forgotten-suspend-pod-uid")?;
    let published_artifact = super::super::super::PublishedLocalArtifact {
        id: LocalArtifactId::new(),
        kind: ArtifactKind::Full,
    };

    insert_sandbox(&fixture.pool, sandbox_id, "suspending").await?;
    let execution = fixture
        .store
        .ensure_active_execution_row(typed_sandbox_id)
        .await?;
    let generation = ExecutionGeneration::new(execution.generation)?;
    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            execution.generation,
            Some(pod_uid.as_str()),
            Some("100.64.101.40"),
        )
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'running'
          WHERE sandbox_id = $1
            AND generation = $2
            AND active"
    ))
    .bind(sandbox_id)
    .bind(execution.generation)
    .execute(&fixture.pool)
    .await?;

    assert!(
        fixture
            .store
            .forget_pod_tracking_if_generation(
                sandbox_id,
                execution.generation,
                PodTrackingCleanupCause::ExplicitTeardown,
            )
            .await?,
        "terminal cleanup must retire the execution used by this regression"
    );

    fixture
        .store
        .persist_or_accept_suspend_commit(
            typed_sandbox_id,
            generation,
            &pod_uid,
            published_artifact,
        )
        .await?;

    let (lifecycle, placement, marker_id, marker_kind): (
        String,
        String,
        Option<Uuid>,
        Option<String>,
    ) = sqlx::query_as(sql!(
        "SELECT lifecycle,
                placement,
                current_local_artifact_id,
                current_local_artifact_kind
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(lifecycle, "suspended");
    assert_eq!(placement, "unplaced");
    assert_eq!(marker_id, Some(published_artifact.id.as_uuid()));
    assert_eq!(marker_kind.as_deref(), Some("full"));

    let (active, state): (bool, String) = sqlx::query_as(sql!(
        "SELECT active, state
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
            AND generation = $2
            AND pod_uid = $3"
    ))
    .bind(sandbox_id)
    .bind(generation.get())
    .bind(pod_uid.as_str())
    .fetch_one(&fixture.pool)
    .await?;
    assert!(!active);
    assert_eq!(state, "suspended");

    fixture
        .store
        .finish_suspended_execution(sandbox_id, generation, &pod_uid, published_artifact)
        .await?;
    Ok(())
}

async fn durable_suspend_snapshot_artifact_requires_this_suspends_snapshot(
    fixture: &PostgresStore,
) -> TestResult {
    let node = NodeId::new();
    let other_node = NodeId::new();
    for (id, name) in [(node, "suspend-node"), (other_node, "other-node")] {
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '10.0.0.30', 'worker', 'k8s', 'ready')"
        ))
        .bind(id)
        .execute(&fixture.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
        ))
        .bind(id)
        .bind(name)
        .execute(&fixture.pool)
        .await?;
    }
    // (case, snapshot node, taken before the suspend began, live location, accepted)
    let cases = [
        ("this suspend's snapshot", node, false, true, true),
        ("an earlier suspend's snapshot", node, true, true, false),
        ("another node's snapshot", other_node, false, true, false),
        ("an orphaned snapshot", node, false, false, false),
    ];
    for (case, source_node, before_suspend, live_location, accepted) in cases {
        let sandbox_id = SandboxId::new();
        let artifact = super::super::super::PublishedLocalArtifact {
            id: LocalArtifactId::new(),
            kind: ArtifactKind::Full,
        };
        insert_sandbox(&fixture.pool, sandbox_id, "suspending").await?;
        sqlx::query(sql!(
            "UPDATE sandboxes SET placement = 'on_node', active_node_id = $2 WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(node)
        .execute(&fixture.pool)
        .await?;
        let execution = fixture
            .store
            .ensure_active_execution_row(sandbox_id)
            .await?;
        let generation = ExecutionGeneration::new(execution.generation)?;
        let pod_uid = PodUid::parse("suspend-pod-uid")?;
        fixture
            .store
            .persist_pod_metadata(
                sandbox_id,
                execution.generation,
                Some(pod_uid.as_str()),
                Some("100.64.101.30"),
            )
            .await?;
        let snapshot_id = Uuid::now_v7();
        sqlx::query(sql!(
            "INSERT INTO snapshots
                 (id, sandbox_id, type, state, timestamp, source_node_id, org_id,
                  local_artifact_id, artifact_kind, artifact_origin)
             VALUES ($1, $2, 'full', 'ready',
                     CASE WHEN $5 THEN NOW() - INTERVAL '1 hour' ELSE NOW() + INTERVAL '1 second' END,
                     $3, '00000000-0000-0000-0000-000000000001', $4, 'full', 'suspend')"
        ))
        .bind(snapshot_id)
        .bind(sandbox_id)
        .bind(source_node)
        .bind(artifact.id.as_uuid())
        .bind(before_suspend)
        .execute(&fixture.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations
                 (snapshot_id, storage_connection_string, org_id, orphaned_at)
             VALUES ($1, 's3://space-test/suspend?region=us-east-1',
                     '00000000-0000-0000-0000-000000000001',
                     CASE WHEN $2 THEN NULL ELSE NOW() END)"
        ))
        .bind(snapshot_id)
        .bind(live_location)
        .execute(&fixture.pool)
        .await?;

        let found = fixture
            .store
            .durable_suspend_snapshot_artifact(sandbox_id, generation, &pod_uid)
            .await?;
        assert_eq!(found, accepted.then_some(artifact), "{case}");
        if accepted {
            // The handoff then runs as for the node's published identity:
            // nothing is committed yet, the commit takes the recovered
            // artifact, and finishing leaves the sandbox suspended.
            assert_eq!(
                fixture
                    .store
                    .committed_suspend_artifact(sandbox_id, generation, &pod_uid)
                    .await?,
                None,
                "{case}"
            );
            fixture
                .store
                .persist_or_accept_suspend_commit(sandbox_id, generation, &pod_uid, artifact)
                .await?;
            assert_eq!(
                fixture
                    .store
                    .committed_suspend_artifact(sandbox_id, generation, &pod_uid)
                    .await?,
                Some(artifact),
                "{case}"
            );
            fixture
                .store
                .finish_suspended_execution(sandbox_id, generation, &pod_uid, artifact)
                .await?;
            let (lifecycle, placement, marker, execution_active, execution_state): (
                String,
                String,
                Option<Uuid>,
                bool,
                String,
            ) = sqlx::query_as(sql!(
                "SELECT s.lifecycle, s.placement, s.current_local_artifact_id, e.active, e.state
                   FROM sandboxes s
                   JOIN sandbox_k8s_executions e
                     ON e.sandbox_id = s.id AND e.generation = $2
                  WHERE s.id = $1"
            ))
            .bind(sandbox_id)
            .bind(generation.get())
            .fetch_one(&fixture.pool)
            .await?;
            assert_eq!(
                (
                    lifecycle.as_str(),
                    placement.as_str(),
                    marker,
                    execution_active,
                    execution_state.as_str()
                ),
                (
                    "suspended",
                    "unplaced",
                    Some(artifact.id.as_uuid()),
                    false,
                    "suspended"
                ),
                "{case}"
            );
        }
    }
    Ok(())
}

async fn committed_suspend_artifact_belongs_to_the_active_execution(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let typed_sandbox_id = sandbox_id;
    let stale_artifact_id = LocalArtifactId::new();
    let current_artifact = super::super::super::PublishedLocalArtifact {
        id: LocalArtifactId::new(),
        kind: ArtifactKind::Full,
    };

    insert_sandbox(&fixture.pool, sandbox_id, "suspending").await?;
    let prior_execution = fixture
        .store
        .ensure_active_execution_row(typed_sandbox_id)
        .await?;
    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            prior_execution.generation,
            Some("prior-pod-uid"),
            Some("100.64.101.20"),
        )
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
         SET active = FALSE, state = 'suspended'
         WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(prior_execution.generation)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET current_local_artifact_id = $2,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(stale_artifact_id.as_uuid())
    .execute(&fixture.pool)
    .await?;

    let current_execution = fixture
        .store
        .ensure_active_execution_row(typed_sandbox_id)
        .await?;
    let current_generation = ExecutionGeneration::new(current_execution.generation)?;
    let current_pod_uid = PodUid::parse("current-pod-uid")?;
    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            current_execution.generation,
            Some(current_pod_uid.as_str()),
            Some("100.64.101.21"),
        )
        .await?;

    assert_eq!(
        fixture
            .store
            .committed_suspend_artifact(typed_sandbox_id, current_generation, &current_pod_uid,)
            .await?,
        None,
        "a prior execution's artifact pointer must not commit the current execution's suspend"
    );

    fixture
        .store
        .persist_or_accept_suspend_commit(
            typed_sandbox_id,
            current_generation,
            &current_pod_uid,
            current_artifact,
        )
        .await?;
    assert_eq!(
        fixture
            .store
            .committed_suspend_artifact(typed_sandbox_id, current_generation, &current_pod_uid,)
            .await?,
        Some(current_artifact)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stale_worker_suspend_result_cannot_retire_replacement() -> TestResult {
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    let typed_sandbox_id = sandbox_id;
    let stale_artifact = super::super::super::PublishedLocalArtifact {
        id: LocalArtifactId::new(),
        kind: ArtifactKind::Full,
    };
    let stale_pod_uid = PodUid::parse("old-worker-pod-uid")?;
    let replacement_pod_uid = PodUid::parse("new-worker-pod-uid")?;

    insert_sandbox(&fixture.pool, sandbox_id, "suspending").await?;
    let stale_execution = fixture
        .store
        .ensure_active_execution_row(typed_sandbox_id)
        .await?;
    let stale_generation = ExecutionGeneration::new(stale_execution.generation)?;
    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            stale_execution.generation,
            Some(stale_pod_uid.as_str()),
            Some("100.64.101.30"),
        )
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE,
                state = 'forgotten'
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(stale_execution.generation)
    .execute(&fixture.pool)
    .await?;

    let replacement_execution = fixture
        .store
        .ensure_active_execution_row(typed_sandbox_id)
        .await?;
    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            replacement_execution.generation,
            Some(replacement_pod_uid.as_str()),
            Some("100.64.101.31"),
        )
        .await?;

    assert!(matches!(
        fixture
            .store
            .persist_or_accept_suspend_commit(
                typed_sandbox_id,
                stale_generation,
                &stale_pod_uid,
                stale_artifact,
            )
            .await,
        Err(SubstrateError::Unavailable { .. })
    ));
    assert!(matches!(
        fixture
            .store
            .finish_suspended_execution(
                sandbox_id,
                stale_generation,
                &stale_pod_uid,
                stale_artifact,
            )
            .await,
        Err(SubstrateError::Unavailable { .. })
    ));

    let active: (i64, Option<String>) = sqlx::query_as(sql!(
        "SELECT generation, pod_uid
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        active,
        (
            replacement_execution.generation,
            Some(replacement_pod_uid.to_string())
        )
    );
    Ok(())
}

async fn finish_suspended_execution_keeps_fresh_snapshot_portable(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let snapshot_id = SnapshotId::new();
    let prior_placement = OffsetDateTime::from_unix_timestamp(1_704_067_200)?;

    insert_sandbox(&fixture.pool, sandbox_id, "suspending").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET active_node_id = $1,
             target_node_id = $1,
             placement = 'on_node',
             placement_changed_at = $2
         WHERE id = $3"
    ))
    .bind(node_id)
    .bind(prior_placement)
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;

    sqlx::query(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, source_node_id, org_id)
         VALUES ($1, $2, 'full', 'ready', $3, '00000000-0000-0000-0000-000000000001')",
    )
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, 's3://space-test/suspend-snapshot', '00000000-0000-0000-0000-000000000001')",
    )
    .bind(snapshot_id)
    .execute(&fixture.pool)
    .await?;

    let published_artifact = finish_suspended_execution_for_test(fixture, sandbox_id).await?;
    sqlx::query(sql!(
        "UPDATE snapshots
            SET local_artifact_id = $1,
                artifact_kind = 'full',
                artifact_origin = 'suspend'
          WHERE id = $2"
    ))
    .bind(published_artifact.id.as_uuid())
    .bind(snapshot_id)
    .execute(&fixture.pool)
    .await?;

    let placement_changed_at: OffsetDateTime =
        sqlx::query_scalar("SELECT placement_changed_at FROM sandboxes WHERE id = $1")
            .bind(sandbox_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(placement_changed_at, prior_placement);

    let portability_predicate = portable_for_restore_affinity("s");
    let portable_query = format!(
        "SELECT EXISTS (
             SELECT 1
               FROM sandboxes s
              WHERE s.id = $1
                AND {portability_predicate}
         )"
    );
    let portable: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(portable_query.as_str())))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert!(
        portable,
        "fresh ready S3 snapshot should drop restore node affinity after suspend cleanup"
    );

    Ok(())
}

async fn creating_snapshot_does_not_select_stale_remote_uri_until_ready(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let previous_snapshot_id = SnapshotId::new();
    let current_snapshot_id = SnapshotId::new();

    insert_sandbox(&fixture.pool, sandbox_id, "suspended").await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, timestamp, org_id)
         VALUES ($1, $2, 'full', 'ready', NOW() - INTERVAL '2 hours',
                 '00000000-0000-0000-0000-000000000001')"
    ))
    .bind(previous_snapshot_id)
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        sql!("INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, 's3://space-test/previous-suspend?region=us-east-1', '00000000-0000-0000-0000-000000000001')"),
    )
    .bind(previous_snapshot_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET placement_changed_at = NOW() - INTERVAL '1 hour'
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, timestamp, org_id)
         VALUES ($1, $2, 'full', 'creating', NOW(),
                 '00000000-0000-0000-0000-000000000001')"
    ))
    .bind(current_snapshot_id)
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        sql!("INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, 's3://space-test/current-suspend?region=us-east-1', '00000000-0000-0000-0000-000000000001')"),
    )
    .bind(current_snapshot_id)
    .execute(&fixture.pool)
    .await?;
    let sandbox = fixture.store.require_sandbox(sandbox_id).await?;

    let uri_while_creating = fixture
        .store
        .selected_suspend_restore_storage_uri(&sandbox, false)
        .await?;

    assert_eq!(uri_while_creating, None);

    // Evacuation mode must ALSO refuse the stale prior-cycle snapshot
    // while the current suspend upload is still `creating`. The newest
    // full snapshot is the `creating` one, so `no_newer_full_snapshot`
    // excludes the older `ready` row — restoring it would lose every
    // change since the previous wake (the data-loss bug this guards).
    let evac_uri_while_creating = fixture
        .store
        .selected_suspend_restore_storage_uri(&sandbox, true)
        .await?;
    assert_eq!(
        evac_uri_while_creating, None,
        "evacuation restore must not select a stale snapshot while the current upload is in flight"
    );

    sqlx::query(sql!("UPDATE snapshots SET state = 'ready' WHERE id = $1"))
        .bind(current_snapshot_id)
        .execute(&fixture.pool)
        .await?;

    let uri_after_ready = fixture
        .store
        .selected_suspend_restore_storage_uri(&sandbox, false)
        .await?;

    assert_eq!(
        uri_after_ready.as_deref(),
        Some("s3://space-test/current-suspend?region=us-east-1")
    );

    // Once the current upload finalizes, the newest full snapshot is
    // `ready`, so the evacuation path selects it (not the stale one).
    let evac_uri_after_ready = fixture
        .store
        .selected_suspend_restore_storage_uri(&sandbox, true)
        .await?;
    assert_eq!(
        evac_uri_after_ready.as_deref(),
        Some("s3://space-test/current-suspend?region=us-east-1"),
        "evacuation restore selects the current snapshot once it is the newest ready one"
    );
    Ok(())
}

async fn restoring_sandbox_selects_newest_ready_snapshot_after_placement_bump(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let snapshot_id = SnapshotId::new();

    insert_sandbox(&fixture.pool, sandbox_id, "restoring").await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, timestamp, org_id)
         VALUES ($1, $2, 'full', 'ready', NOW() - INTERVAL '1 second',
                 '00000000-0000-0000-0000-000000000001')"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, 's3://space-test/restoring-suspend?region=us-east-1',
                 '00000000-0000-0000-0000-000000000001')"
    ))
    .bind(snapshot_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET placement_changed_at = NOW()
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    let sandbox = fixture.store.require_sandbox(sandbox_id).await?;

    let uri = fixture
        .store
        .selected_suspend_restore_storage_uri(&sandbox, false)
        .await?;

    assert_eq!(
        uri.as_deref(),
        Some("s3://space-test/restoring-suspend?region=us-east-1"),
        "restoring retries should not strand a durable snapshot just because restore placement bumped placement_changed_at"
    );
    Ok(())
}

async fn suspend_restore_selector_ignores_orphaned_storage_locations(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "suspended").await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/live-older?region=us-east-1",
        1,
        None,
    )
    .await?;
    let newer_snapshot_id = insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/orphaned-newer?region=us-east-1",
        2,
        None,
    )
    .await?;
    // Production orphaning also detaches snapshot_id atomically. Keep it
    // attached through this assertion to cover the selector's defense against
    // legacy or manually repaired rows, then model the production detach below.
    sqlx::query(sql!(
        "UPDATE snapshot_storage_locations
            SET orphaned_at = NOW()
          WHERE snapshot_id = $1"
    ))
    .bind(newer_snapshot_id)
    .execute(&fixture.pool)
    .await?;
    let sandbox = fixture.store.require_sandbox(sandbox_id).await?;

    let fallback = fixture
        .store
        .selected_suspend_restore_storage_uri(&sandbox, false)
        .await?;
    assert_eq!(
        fallback.as_deref(),
        Some("s3://space-test/live-older?region=us-east-1"),
        "an orphaned newer location must not hide the newest live snapshot"
    );

    sqlx::query(sql!(
        "UPDATE snapshot_storage_locations
            SET snapshot_id = NULL
          WHERE snapshot_id = $1"
    ))
    .bind(newer_snapshot_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, '00000000-0000-0000-0000-000000000001')"
    ))
    .bind(newer_snapshot_id)
    .bind("s3://space-test/live-newer?region=us-east-1")
    .execute(&fixture.pool)
    .await?;
    let newest_live = fixture
        .store
        .selected_suspend_restore_storage_uri(&sandbox, false)
        .await?;
    assert_eq!(
        newest_live.as_deref(),
        Some("s3://space-test/live-newer?region=us-east-1"),
        "a replacement live location must make the newer snapshot selectable while the detached orphan remains"
    );

    Ok(())
}

async fn restoring_retry_uses_pre_restore_snapshot_when_late_branch_publishes_artifact(
    fixture: &PostgresStore,
) -> TestResult {
    // Arrange.
    let sandbox_id = SandboxId::new();
    let source_artifact_id = LocalArtifactId::new();
    let late_branch_artifact_id = LocalArtifactId::new();

    insert_sandbox(&fixture.pool, sandbox_id, "restoring").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET current_local_artifact_id = $2,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(late_branch_artifact_id)
    .execute(&fixture.pool)
    .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/g2-source?region=us-east-1",
        -1,
        Some(source_artifact_id),
    )
    .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/g3-late-branch?region=us-east-1",
        1,
        Some(late_branch_artifact_id),
    )
    .await?;
    let claim = fixture
        .store
        .begin_restore_lifecycle(sandbox_id, false)
        .await?;
    user_redrive_restore_token(claim);
    let sandbox = fixture.store.require_sandbox(sandbox_id).await?;

    // Act.
    let uri = fixture
        .store
        .selected_suspend_restore_storage_uri(&sandbox, false)
        .await?;

    // Assert.
    assert_eq!(
        uri.as_deref(),
        Some("s3://space-test/g2-source?region=us-east-1")
    );
    Ok(())
}

async fn restoring_retry_refuses_prior_placement_snapshot_after_late_branch_artifact(
    fixture: &PostgresStore,
) -> TestResult {
    // Arrange.
    let sandbox_id = SandboxId::new();
    let late_branch_artifact_id = LocalArtifactId::new();

    insert_sandbox(&fixture.pool, sandbox_id, "restoring").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET placement_changed_at = lifecycle_changed_at - INTERVAL '30 minutes',
             current_local_artifact_id = $2,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(late_branch_artifact_id)
    .execute(&fixture.pool)
    .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/g1-prior-placement?region=us-east-1",
        -60,
        None,
    )
    .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/g3-late-branch?region=us-east-1",
        1,
        Some(late_branch_artifact_id),
    )
    .await?;
    let sandbox = fixture.store.require_sandbox(sandbox_id).await?;

    // Act.
    let uri = fixture
        .store
        .selected_suspend_restore_storage_uri(&sandbox, false)
        .await?;

    // Assert.
    assert_eq!(uri, None);
    Ok(())
}

async fn restoring_retry_refuses_markerless_snapshot_after_late_branch_artifact(
    fixture: &PostgresStore,
) -> TestResult {
    // Arrange.
    let sandbox_id = SandboxId::new();
    let late_branch_artifact_id = LocalArtifactId::new();

    insert_sandbox(&fixture.pool, sandbox_id, "restoring").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET current_local_artifact_id = $2,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(late_branch_artifact_id)
    .execute(&fixture.pool)
    .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/markerless-prior?region=us-east-1",
        -60,
        None,
    )
    .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/g3-late-branch?region=us-east-1",
        1,
        Some(late_branch_artifact_id),
    )
    .await?;
    let sandbox = fixture.store.require_sandbox(sandbox_id).await?;

    // Act.
    let uri = fixture
        .store
        .selected_suspend_restore_storage_uri(&sandbox, false)
        .await?;

    // Assert.
    assert_eq!(uri, None);
    Ok(())
}

async fn forget_pod_tracking_preserves_execution_history(fixture: &PostgresStore) -> TestResult {
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET active_node_id = $1,
             target_node_id = $1,
             placement = 'on_node'
         WHERE id = $2"
    ))
    .bind(node_id)
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;

    let execution = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            execution.generation,
            Some("pod-uid"),
            Some("10.90.0.9"),
        )
        .await?;
    fixture
        .store
        .forget_pod_tracking(sandbox_id, PlacementClock::Bump)
        .await?;

    let (generation, pod_name, active, state, pod_ip): (i64, String, bool, String, Option<String>) =
        sqlx::query_as(sql!(
            "SELECT generation, pod_name, active, state, host(pod_ip)
         FROM sandbox_k8s_executions
         WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(generation, execution.generation);
    assert_eq!(pod_name, execution.pod_name);
    assert!(!active);
    assert_eq!(state, "forgotten");
    assert_eq!(pod_ip, None);

    let (lifecycle, placement, active_node_id, target_node_id): (
        String,
        String,
        Option<NodeId>,
        Option<NodeId>,
    ) = sqlx::query_as(sql!(
        "SELECT lifecycle, placement, active_node_id, target_node_id
             FROM sandboxes
             WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(lifecycle, "started");
    assert_eq!(placement, "unplaced");
    assert_eq!(active_node_id, None);
    assert_eq!(target_node_id, None);
    Ok(())
}

async fn forget_pod_tracking_deletes_bound_claimed_pool_slot(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = seed_bound_claimed_pool_slot(fixture, "started", "pool-retired-slot").await?;
    fixture
        .store
        .forget_pod_tracking(sandbox_id, PlacementClock::Bump)
        .await?;
    assert_eq!(
        claimed_slot_count(fixture, sandbox_id).await?,
        0,
        "pod-tracking teardown must delete the bound claimed pool slot"
    );
    Ok(())
}

async fn forget_pod_tracking_recovers_started_with_current_suspend_backup(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let artifact_id = LocalArtifactId::new();
    let uri = "s3://space-test/generic-current-suspend?region=us-east-1";

    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET active_node_id = $2,
             target_node_id = $2,
             placement = 'on_node',
             command_ready_pending = TRUE,
             current_local_artifact_id = $3,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(artifact_id)
    .execute(&fixture.pool)
    .await?;
    let execution = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            execution.generation,
            Some("pod-uid"),
            Some("10.90.0.9"),
        )
        .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        uri,
        1,
        Some(artifact_id),
    )
    .await?;

    fixture
        .store
        .forget_pod_tracking(sandbox_id, PlacementClock::Bump)
        .await?;

    let (
        lifecycle,
        placement,
        active_node_id,
        target_node_id,
        placement_changed_at,
        command_ready_pending,
        current_artifact_id,
    ): ForgottenSuspendedRow = sqlx::query_as(sql!(
        "SELECT lifecycle,
                placement,
                active_node_id,
                target_node_id,
                placement_changed_at,
                command_ready_pending,
                current_local_artifact_id
         FROM sandboxes
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(lifecycle, "suspended");
    assert_eq!(placement, "unplaced");
    assert_eq!(active_node_id, None);
    assert_eq!(target_node_id, None);
    assert_eq!(placement_changed_at, None);
    assert!(!command_ready_pending);
    assert_eq!(current_artifact_id, Some(artifact_id));

    let active_count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(active_count, 0);

    let sandbox = fixture.store.require_sandbox(sandbox_id).await?;
    let selected_uri = fixture
        .store
        .selected_suspend_restore_storage_uri(&sandbox, false)
        .await?;
    assert_eq!(selected_uri.as_deref(), Some(uri));
    Ok(())
}

async fn forget_pod_tracking_clears_stale_marker_for_newest_current_suspend_backup(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let stale_artifact_id = LocalArtifactId::new();
    let current_artifact_id = LocalArtifactId::new();
    let current_uri = "s3://space-test/generic-newest-suspend?region=us-east-1";

    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET current_local_artifact_id = $2,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(stale_artifact_id)
    .execute(&fixture.pool)
    .await?;
    let execution = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            execution.generation,
            Some("pod-uid"),
            Some("10.90.0.9"),
        )
        .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        "s3://space-test/generic-stale-suspend?region=us-east-1",
        -1,
        Some(stale_artifact_id),
    )
    .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        current_uri,
        1,
        Some(current_artifact_id),
    )
    .await?;

    fixture
        .store
        .forget_pod_tracking(sandbox_id, PlacementClock::Bump)
        .await?;

    let (lifecycle, current_artifact_id): (String, Option<LocalArtifactId>) = sqlx::query_as(sql!(
        "SELECT lifecycle, current_local_artifact_id
         FROM sandboxes
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(lifecycle, "suspended");
    assert_eq!(
        current_artifact_id, None,
        "stale marker must be cleared so restore selects the newest current snapshot"
    );

    let sandbox = fixture.store.require_sandbox(sandbox_id).await?;
    let selected_uri = fixture
        .store
        .selected_suspend_restore_storage_uri(&sandbox, false)
        .await?;
    assert_eq!(selected_uri.as_deref(), Some(current_uri));
    Ok(())
}

async fn forget_pod_tracking_keeps_started_when_suspend_backup_is_unsafe(
    fixture: &PostgresStore,
) -> TestResult {
    let stale_activity = SandboxId::new();
    insert_sandbox(&fixture.pool, stale_activity, "started").await?;
    let stale_activity_execution = fixture
        .store
        .ensure_active_execution_row(stale_activity)
        .await?;
    fixture
        .store
        .persist_pod_metadata(
            stale_activity,
            stale_activity_execution.generation,
            Some("pod-uid"),
            Some("10.90.0.9"),
        )
        .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        stale_activity,
        "s3://space-test/generic-stale-vs-activity?region=us-east-1",
        1,
        Some(LocalArtifactId::new()),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET last_command_at = lifecycle_changed_at + INTERVAL '2 minutes',
             last_access_at = lifecycle_changed_at + INTERVAL '2 minutes'
         WHERE id = $1"
    ))
    .bind(stale_activity)
    .execute(&fixture.pool)
    .await?;

    let newer_non_suspend = SandboxId::new();
    insert_sandbox(&fixture.pool, newer_non_suspend, "started").await?;
    let newer_non_suspend_execution = fixture
        .store
        .ensure_active_execution_row(newer_non_suspend)
        .await?;
    fixture
        .store
        .persist_pod_metadata(
            newer_non_suspend,
            newer_non_suspend_execution.generation,
            Some("pod-uid"),
            Some("10.90.0.10"),
        )
        .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        newer_non_suspend,
        "s3://space-test/generic-current-suspend-blocked?region=us-east-1",
        1,
        Some(LocalArtifactId::new()),
    )
    .await?;
    let stop_snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, timestamp, org_id,
              local_artifact_id, artifact_kind, artifact_origin)
         VALUES ($1, $2, 'full', 'ready',
                 (SELECT lifecycle_changed_at + INTERVAL '2 minutes'
                    FROM sandboxes WHERE id = $2),
                 '00000000-0000-0000-0000-000000000001',
                 $3, 'full', 'stop')"
    ))
    .bind(stop_snapshot_id)
    .bind(newer_non_suspend)
    .bind(Uuid::now_v7())
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, '00000000-0000-0000-0000-000000000001')"
    ))
    .bind(stop_snapshot_id)
    .bind("s3://space-test/generic-newer-stop?region=us-east-1")
    .execute(&fixture.pool)
    .await?;

    fixture
        .store
        .forget_pod_tracking(stale_activity, PlacementClock::Bump)
        .await?;
    fixture
        .store
        .forget_pod_tracking(newer_non_suspend, PlacementClock::Bump)
        .await?;

    let stale_activity_lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(stale_activity)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(stale_activity_lifecycle, "started");

    let newer_non_suspend_lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(newer_non_suspend)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(newer_non_suspend_lifecycle, "started");
    Ok(())
}

/// A `PlacementClock::Preserve` forget (the `retire_execution` path the
/// failed-sandbox cleanup uses) must not bump `placement_changed_at`, or a
/// markerless (`current_local_artifact_id IS NULL`) failed row's ready
/// snapshot — selectable before the retire under the legacy freshness gate
/// — would flip to "stale" afterward and strand a recoverable sandbox even
/// though the snapshot rows are untouched.
async fn forget_pod_tracking_preserve_keeps_markerless_snapshot_selectable(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    // An old placement timestamp so the snapshot (created moments ago) is
    // unambiguously fresh relative to it before the retire runs.
    let prior_placement = OffsetDateTime::from_unix_timestamp(1_704_067_200)?;

    insert_sandbox(&fixture.pool, sandbox_id, "failed").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET active_node_id = $1,
             target_node_id = $1,
             placement = 'on_node',
             placement_changed_at = $2
         WHERE id = $3"
    ))
    .bind(node_id)
    .bind(prior_placement)
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    // Markerless on purpose: `insert_sandbox` leaves
    // `current_local_artifact_id` NULL, so portability falls to the legacy
    // `sn.timestamp >= placement_changed_at` branch this test guards.
    insert_ready_full_snapshot(&fixture.pool, sandbox_id, "s3://space-test/failed-snapshot")
        .await?;
    fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    fixture
        .store
        .persist_pod_metadata(sandbox_id, 1, Some("pod-uid"), Some("10.90.0.9"))
        .await?;

    let fresh_query = format!(
        "SELECT EXISTS (
             SELECT 1 FROM sandboxes s WHERE s.id = $1 AND {}
         )",
        fresh_ready_snapshot_exists("s")
    );
    let selectable_before: bool =
        sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(fresh_query.as_str())))
            .bind(sandbox_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert!(
        selectable_before,
        "the ready snapshot must be selectable before the retire"
    );

    fixture
        .store
        .forget_pod_tracking(sandbox_id, PlacementClock::Preserve)
        .await?;

    let placement_changed_at: OffsetDateTime = sqlx::query_scalar(sql!(
        "SELECT placement_changed_at FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        placement_changed_at, prior_placement,
        "a preserving retire must not bump placement_changed_at"
    );

    let selectable_after: bool =
        sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(fresh_query.as_str())))
            .bind(sandbox_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert!(
        selectable_after,
        "the ready snapshot must stay selectable after a preserving retire — \
         the failed-cleanup reap must not strand a recoverable snapshot"
    );
    Ok(())
}

async fn begin_restore_lifecycle_preserves_retry_and_marker_claim_rules(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;

    let err = fixture
        .store
        .begin_restore_lifecycle(sandbox_id, false)
        .await
        .expect_err("started sandbox should not enter restore lifecycle");
    assert!(matches!(err, SubstrateError::Conflict { .. }));

    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspended' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    let claim = fixture
        .store
        .begin_restore_lifecycle(sandbox_id, false)
        .await?;
    let first_token = user_fresh_restore_token(claim);

    let (lifecycle, lease_active, lease_token): (String, bool, Option<Uuid>) =
        sqlx::query_as(sql!(
            "SELECT lifecycle,
                COALESCE(restore_lease_expires_at > NOW(), FALSE),
                restore_lease_token
           FROM sandboxes
          WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(lifecycle, "restoring");
    assert!(lease_active, "user restore claim must acquire a live lease");
    assert_eq!(lease_token, Some(first_token));

    // Age the row's staleness clock to simulate a slow restore that has
    // been `restoring` long enough to be near the sweeper's reap window.
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle_changed_at = NOW() - INTERVAL '1 hour' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;

    let err = fixture
        .store
        .begin_restore_lifecycle(sandbox_id, false)
        .await
        .expect_err("a live restore lease must block a concurrent redrive");
    let SubstrateError::RestoreInFlight {
        lease_remaining, ..
    } = err
    else {
        panic!("expected RestoreInFlight, got {err:?}");
    };
    let lease_remaining =
        lease_remaining.expect("a collision against a live lease must carry lease remaining");
    assert!(
        lease_remaining > Duration::ZERO
            && lease_remaining <= Duration::from_secs(u64::try_from(recovery_lease_ttl_secs())?),
        "lease remaining must sit within the lease TTL, got {lease_remaining:?}"
    );

    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;

    let original_source_boundary: OffsetDateTime = sqlx::query_scalar(sql!(
        "SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;

    // User retry after the lease expires: a timed-out restore intentionally
    // stays `restoring`; retrying must preserve the original source boundary
    // while taking ownership with a fresh lease.
    let claim = fixture
        .store
        .begin_restore_lifecycle(sandbox_id, false)
        .await?;
    let second_token = user_redrive_restore_token(claim);
    assert_ne!(
        first_token, second_token,
        "a redrive must take ownership with a fresh lease token"
    );
    let (lifecycle, source_boundary, lease_token): (String, OffsetDateTime, Option<Uuid>) =
        sqlx::query_as(sql!(
            "SELECT lifecycle, lifecycle_changed_at, restore_lease_token
           FROM sandboxes
          WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(
        lifecycle, "restoring",
        "user restore retry should idempotently keep restoring lifecycle"
    );
    assert_eq!(
        source_boundary, original_source_boundary,
        "re-driving a restoring row must not widen the pre-restore source window"
    );
    assert_eq!(lease_token, Some(second_token));

    // Marker-required claim (the cluster-controller's deferred cohort
    // restore). A suspended row with NO `evacuation_restore_target` must
    // lose the claim — this is what stops the drain re-drive from
    // restoring a sandbox a concurrent user restore cleared the marker
    // on. With the marker set, the claim succeeds.
    let marker_id = SandboxId::new();
    insert_sandbox(&fixture.pool, marker_id, "suspended").await?;
    let err = fixture
        .store
        .begin_restore_lifecycle(marker_id, true)
        .await
        .expect_err("marker-required claim must fail when the marker is NULL");
    // The marker-loss CAS surfaces the dedicated `EvacuationClaimLost`
    // variant (NOT a generic `Conflict`), so the cohort-restore caller can
    // treat it as a clean deferral without swallowing real `Conflict`s
    // like a wake-RPC failure on the rebuilt pod.
    assert!(matches!(err, SubstrateError::EvacuationClaimLost { .. }));
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(marker_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(lifecycle, "suspended", "lost claim must not flip lifecycle");

    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = 'started' WHERE id = $1"
    ))
    .bind(marker_id)
    .execute(&fixture.pool)
    .await?;
    let claim = fixture
        .store
        .begin_restore_lifecycle(marker_id, true)
        .await?;
    assert_evacuation_fresh_restore_claim(claim);
    // Evacuation restore claims must not acquire user restore leases: the row
    // flips to `restoring` with no `restore_lease_token`, and the marker
    // that gated the claim is still visible.
    insta::assert_yaml_snapshot!(sandbox_lease_row(&fixture.pool, marker_id).await?);

    // The user restore path must NOT grab a `restoring` row whose
    // evacuation marker is still set: that is a drain restore in flight,
    // and claiming it would double-drive a second wake. (A *markerless*
    // `restoring` row — the user's own timed-out restore — is re-drivable;
    // see the user-retry case earlier.) The claim loses, and neither the
    // lifecycle nor the marker changes.
    let err = fixture
        .store
        .begin_restore_lifecycle(marker_id, false)
        .await
        .expect_err("user restore must not take over a marker-set restoring row");
    assert!(matches!(err, SubstrateError::Conflict { .. }));
    // A rejected user claim must not flip lifecycle or clear the in-flight
    // drain marker.
    insta::assert_yaml_snapshot!(sandbox_lease_row(&fixture.pool, marker_id).await?);

    // Crash-recovery re-drive: the evacuation claim accepts an already-
    // `restoring` row (marker still set) so the recovery sweep can resume
    // a restore whose controller died before finalize. Idempotent — the
    // row stays `restoring`, and the marker remains required.
    let claim = fixture
        .store
        .begin_restore_lifecycle(marker_id, true)
        .await?;
    assert_evacuation_redrive_restore_claim(claim);
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(marker_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(
        lifecycle, "restoring",
        "recovery re-claim of a restoring row is idempotent"
    );

    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = NULL WHERE id = $1"
    ))
    .bind(marker_id)
    .execute(&fixture.pool)
    .await?;
    let err = fixture
        .store
        .begin_restore_lifecycle(marker_id, true)
        .await
        .expect_err("marker-required claim must fail for restoring rows without the marker");
    assert!(matches!(err, SubstrateError::EvacuationClaimLost { .. }));
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(marker_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(
        lifecycle, "restoring",
        "lost restoring claim must not change lifecycle"
    );

    // User-takeover claim (`require_evacuation_marker = false`) clears the
    // marker atomically in the same CAS, so a later user suspend can't
    // leave a stale marker for the restore sweep — and so the drain claim
    // can't double-drive a user-claimed `restoring` row.
    let user_sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, user_sandbox_id, "suspended").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = 'started' WHERE id = $1"
    ))
    .bind(user_sandbox_id)
    .execute(&fixture.pool)
    .await?;
    let claim = fixture
        .store
        .begin_restore_lifecycle(user_sandbox_id, false)
        .await?;
    let user_token = user_fresh_restore_token(claim);
    // The user-takeover claim must clear the evacuation marker atomically
    // and acquire exactly the fresh lease token the claim returned — the
    // redaction below asserts that exact match inline rather than erasing
    // it, since `restore_lease_token` is otherwise a run-varying UUID.
    insta::assert_yaml_snapshot!(
        sandbox_lease_row(&fixture.pool, user_sandbox_id).await?,
        {
            ".restore_lease_token" => insta::dynamic_redaction(move |value, _path| {
                assert_eq!(
                    value.as_str(),
                    Some(user_token.to_string().as_str()),
                    "user-takeover claim must acquire the fresh lease token"
                );
                "[lease_token]"
            }),
        }
    );
    Ok(())
}

async fn user_restore_lease_blocks_concurrent_redrive_until_expiry(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "suspended").await?;

    let claim = fixture
        .store
        .begin_restore_lifecycle(sandbox_id, false)
        .await?;
    let first_token = user_fresh_restore_token(claim);

    let err = fixture
        .store
        .begin_restore_lifecycle(sandbox_id, false)
        .await
        .expect_err("concurrent user restore must not re-drive a live lease");
    assert!(
        matches!(err, SubstrateError::RestoreInFlight { .. }),
        "active restore lease should surface as retryable in-flight work, not a conflict"
    );
    // A denied redrive must not steal the active lease: lifecycle stays
    // `restoring` and the token is exactly the still-live first claim's.
    insta::assert_yaml_snapshot!(
        sandbox_lease_row(&fixture.pool, sandbox_id).await?,
        {
            ".restore_lease_token" => insta::dynamic_redaction(move |value, _path| {
                assert_eq!(
                    value.as_str(),
                    Some(first_token.to_string().as_str()),
                    "denied redrive must not steal the active lease"
                );
                "[lease_token]"
            }),
        }
    );

    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    let claim = fixture
        .store
        .begin_restore_lifecycle(sandbox_id, false)
        .await?;
    let second_token = user_redrive_restore_token(claim);
    assert_ne!(first_token, second_token);

    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    let lease_token: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT restore_lease_token FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        lease_token, None,
        "successful restore finalization must clear the user restore lease"
    );
    Ok(())
}

async fn expired_restore_lease_with_live_execution_stays_in_flight(
    fixture: &PostgresStore,
) -> TestResult {
    // Arrange.
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "suspended").await?;
    let first_claim = fixture
        .store
        .begin_restore_lifecycle(sandbox_id, false)
        .await?;
    let first_token = user_fresh_restore_token(first_claim);
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state, active)
         VALUES ($1, 1, $2, 'space', 'creating', TRUE)"
    ))
    .bind(sandbox_id)
    .bind(format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple()))
    .execute(&fixture.pool)
    .await?;

    // Act.
    let error = fixture
        .store
        .begin_restore_lifecycle(sandbox_id, false)
        .await
        .expect_err("a live execution must keep an expired restore lease in flight");

    // Assert.
    let SubstrateError::RestoreInFlight {
        lease_remaining: Some(lease_remaining),
        ..
    } = error
    else {
        panic!("expected RestoreInFlight with execution-derived retry hint")
    };
    assert!(lease_remaining > Duration::from_secs(150));
    assert!(matches!(
        fixture.store.observe_user_restore(sandbox_id).await?,
        UserRestoreObservation::InFlight { .. }
    ));
    let token: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT restore_lease_token FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(token, Some(first_token));
    Ok(())
}

async fn clean_redrive_failure_releases_restore_lease_for_immediate_retry(
    fixture: &PostgresStore,
) -> TestResult {
    use super::super::execution::{
        restore_failure_should_revert_to_suspended, wake_failure_should_revert_to_suspended,
    };
    use super::super::pod_ops::WakeDispatchStatus;

    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "suspended").await?;

    let claim = fixture
        .store
        .begin_restore_lifecycle(sandbox_id, false)
        .await?;
    user_fresh_restore_token(claim);
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;

    let claim = fixture
        .store
        .begin_restore_lifecycle(sandbox_id, false)
        .await?;
    let redrive_token = user_redrive_restore_token(claim);
    let clean_err = SubstrateError::Conflict {
        message: "wake failed before live work landed".to_string(),
    };
    assert!(!restore_failure_should_revert_to_suspended(
        Some(claim),
        Some(&clean_err),
        WakeDispatchStatus::NotDispatched,
    ));
    assert!(wake_failure_should_revert_to_suspended(
        Some(&clean_err),
        WakeDispatchStatus::NotDispatched,
    ));
    fixture
        .store
        .release_user_restore_lease(sandbox_id, redrive_token)
        .await?;
    let (lease_token, lease_expiry): (Option<Uuid>, Option<time::OffsetDateTime>) =
        sqlx::query_as(sql!(
            "SELECT restore_lease_token, restore_lease_expires_at
               FROM sandboxes
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(
        lease_token,
        Some(redrive_token),
        "clean redrive failure expires the owner pair without reverting"
    );
    let lease_expiry = lease_expiry.expect("release retains the restore-attempt clock");
    let now = time::OffsetDateTime::now_utc();
    assert!(lease_expiry <= now + time::Duration::seconds(1));
    assert!(
        lease_expiry >= now - time::Duration::seconds(10),
        "release starts the sweeper quiet window at the last restore activity"
    );

    let claim = fixture
        .store
        .begin_restore_lifecycle(sandbox_id, false)
        .await?;
    let retry_token = user_redrive_restore_token(claim);
    assert_ne!(redrive_token, retry_token);

    fixture
        .store
        .release_user_restore_lease(sandbox_id, redrive_token)
        .await?;
    let lease_token: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT restore_lease_token FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        lease_token,
        Some(retry_token),
        "late cleanup from an old attempt must not clear a newer retry's lease"
    );

    Ok(())
}

/// A stale clean-failure rollback (delayed past the lease TTL, carrying an
/// OLD token) must not flip a row that a retry has already re-claimed with a
/// NEW token back to `suspended` — that would orphan the newer live restore.
/// The token-guarded `revert_restore_claim_to_suspended` is a CAS on the
/// claim's token; `None` (the tokenless evacuation claim) is unguarded.
async fn stale_restore_rollback_is_guarded_by_lease_token(fixture: &PostgresStore) -> TestResult {
    let id = SandboxId::new();
    insert_sandbox(&fixture.pool, id, "restoring").await?;
    // A retry has taken over: the row holds a NEW live lease token.
    let new_token = Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_token = $2,
                restore_lease_expires_at = NOW() + INTERVAL '120 seconds'
          WHERE id = $1"
    ))
    .bind(id)
    .bind(new_token)
    .execute(&fixture.pool)
    .await?;

    // A stale rollback from the earlier attempt (its OLD token) must no-op.
    let old_token = Uuid::now_v7();
    fixture
        .store
        .revert_restore_claim_to_suspended(
            id,
            Some(old_token),
            None,
            crate::controllers::substrate::AbandonedRestoreRetirement::Enabled,
        )
        .await?;
    // A stale-token rollback must not flip the newer retry to `suspended`
    // nor clear its lease — the redaction asserts the token is exactly the
    // newer retry's, since it is otherwise a run-varying UUID.
    insta::assert_yaml_snapshot!(
        sandbox_lease_row(&fixture.pool, id).await?,
        {
            ".restore_lease_token" => insta::dynamic_redaction(move |value, _path| {
                assert_eq!(
                    value.as_str(),
                    Some(new_token.to_string().as_str()),
                    "a stale-token rollback must not clear the newer retry's lease"
                );
                "[lease_token]"
            }),
        }
    );

    // The rollback whose token matches the current holder DOES revert (and
    // the clear-trigger drops the lease as the row leaves `restoring`).
    fixture
        .store
        .revert_restore_claim_to_suspended(
            id,
            Some(new_token),
            None,
            crate::controllers::substrate::AbandonedRestoreRetirement::Enabled,
        )
        .await?;
    // A matching-token rollback reverts, and leaving `restoring` clears the
    // lease (no run-varying value survives here, so a plain snapshot pins
    // both facts).
    insta::assert_yaml_snapshot!(sandbox_lease_row(&fixture.pool, id).await?);

    Ok(())
}

async fn restore_rollback_requires_fresh_claim_and_clean_failure(
    fixture: &PostgresStore,
) -> TestResult {
    use super::super::execution::restore_failure_should_revert_to_suspended;
    use super::super::pod_ops::WakeDispatchStatus;

    let clean_err = SubstrateError::Conflict {
        message: "wake failed before live work landed".to_string(),
    };
    let timeout_err = SubstrateError::Timeout {
        message: "wake budget elapsed".to_string(),
        phase: None,
    };

    let fresh_clean_id = SandboxId::new();
    insert_sandbox(&fixture.pool, fresh_clean_id, "suspended").await?;
    let claim = fixture
        .store
        .begin_restore_lifecycle(fresh_clean_id, false)
        .await?;
    user_fresh_restore_token(claim);
    assert!(restore_failure_should_revert_to_suspended(
        Some(claim),
        Some(&clean_err),
        WakeDispatchStatus::NotDispatched,
    ));
    if restore_failure_should_revert_to_suspended(
        Some(claim),
        Some(&clean_err),
        WakeDispatchStatus::NotDispatched,
    ) {
        fixture
            .store
            .revert_restore_claim_to_suspended(
                fresh_clean_id,
                None,
                None,
                crate::controllers::substrate::AbandonedRestoreRetirement::Enabled,
            )
            .await?;
    }
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(fresh_clean_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(
        lifecycle, "suspended",
        "fresh suspended claim should revert on clean restore failure"
    );
    let lease_token: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT restore_lease_token FROM sandboxes WHERE id = $1"
    ))
    .bind(fresh_clean_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        lease_token, None,
        "fresh clean rollback should clear the restore lease"
    );

    let fresh_dispatched_id = SandboxId::new();
    insert_sandbox(&fixture.pool, fresh_dispatched_id, "suspended").await?;
    let claim = fixture
        .store
        .begin_restore_lifecycle(fresh_dispatched_id, false)
        .await?;
    user_fresh_restore_token(claim);
    assert!(
        !restore_failure_should_revert_to_suspended(
            Some(claim),
            Some(&clean_err),
            WakeDispatchStatus::MayHaveLanded,
        ),
        "post-dispatch publication/readiness failure must retain restoring"
    );
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(fresh_dispatched_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(
        lifecycle, "restoring",
        "a dispatched Wake must stay visible to its retry and sweeper owners"
    );

    let redrove_clean_id = SandboxId::new();
    insert_sandbox(&fixture.pool, redrove_clean_id, "suspended").await?;
    let claim = fixture
        .store
        .begin_restore_lifecycle(redrove_clean_id, false)
        .await?;
    user_fresh_restore_token(claim);
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
          WHERE id = $1"
    ))
    .bind(redrove_clean_id)
    .execute(&fixture.pool)
    .await?;
    let claim = fixture
        .store
        .begin_restore_lifecycle(redrove_clean_id, false)
        .await?;
    user_redrive_restore_token(claim);
    assert!(!restore_failure_should_revert_to_suspended(
        Some(claim),
        Some(&clean_err),
        WakeDispatchStatus::NotDispatched,
    ));
    if restore_failure_should_revert_to_suspended(
        Some(claim),
        Some(&clean_err),
        WakeDispatchStatus::NotDispatched,
    ) {
        fixture
            .store
            .revert_restore_claim_to_suspended(
                redrove_clean_id,
                None,
                None,
                crate::controllers::substrate::AbandonedRestoreRetirement::Enabled,
            )
            .await?;
    }
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(redrove_clean_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(
        lifecycle, "restoring",
        "redriven restore should stay restoring after clean failure"
    );

    let fresh_timeout_id = SandboxId::new();
    insert_sandbox(&fixture.pool, fresh_timeout_id, "suspended").await?;
    let claim = fixture
        .store
        .begin_restore_lifecycle(fresh_timeout_id, false)
        .await?;
    user_fresh_restore_token(claim);
    assert!(!restore_failure_should_revert_to_suspended(
        Some(claim),
        Some(&timeout_err),
        WakeDispatchStatus::NotDispatched,
    ));
    if restore_failure_should_revert_to_suspended(
        Some(claim),
        Some(&timeout_err),
        WakeDispatchStatus::NotDispatched,
    ) {
        fixture
            .store
            .revert_restore_claim_to_suspended(
                fresh_timeout_id,
                None,
                None,
                crate::controllers::substrate::AbandonedRestoreRetirement::Enabled,
            )
            .await?;
    }
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(fresh_timeout_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(
        lifecycle, "restoring",
        "timeout should not revert a fresh restore claim"
    );

    let redrove_timeout_id = SandboxId::new();
    insert_sandbox(&fixture.pool, redrove_timeout_id, "suspended").await?;
    let claim = fixture
        .store
        .begin_restore_lifecycle(redrove_timeout_id, false)
        .await?;
    user_fresh_restore_token(claim);
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
          WHERE id = $1"
    ))
    .bind(redrove_timeout_id)
    .execute(&fixture.pool)
    .await?;
    let claim = fixture
        .store
        .begin_restore_lifecycle(redrove_timeout_id, false)
        .await?;
    user_redrive_restore_token(claim);
    assert!(!restore_failure_should_revert_to_suspended(
        Some(claim),
        Some(&timeout_err),
        WakeDispatchStatus::NotDispatched,
    ));
    if restore_failure_should_revert_to_suspended(
        Some(claim),
        Some(&timeout_err),
        WakeDispatchStatus::NotDispatched,
    ) {
        fixture
            .store
            .revert_restore_claim_to_suspended(
                redrove_timeout_id,
                None,
                None,
                crate::controllers::substrate::AbandonedRestoreRetirement::Enabled,
            )
            .await?;
    }
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(redrove_timeout_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(
        lifecycle, "restoring",
        "timeout should not revert a redriven restore claim"
    );

    Ok(())
}

/// A wake/access that re-drives an already-`restoring` row through
/// `prepare_wake_sandbox` must preserve the lifecycle timestamp that bounds
/// the original restore source. The renewed restore lease, not a widened
/// source window, protects a markerless active re-drive from recovery.
async fn prepare_wake_preserves_restore_source_boundary(fixture: &PostgresStore) -> TestResult {
    let age_clock = |id: SandboxId| async move {
        sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle_changed_at = NOW() - INTERVAL '1 hour' WHERE id = $1"
        ))
        .bind(id)
        .execute(&fixture.pool)
        .await
        .map(|_| ())
    };
    let source_boundary = |id: SandboxId| async move {
        sqlx::query_scalar::<_, OffsetDateTime>(sql!(
            "SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1"
        ))
        .bind(id)
        .fetch_one(&fixture.pool)
        .await
    };

    // Markerless restoring (a timed-out user restore being re-driven): the
    // wake/access redrive renews its lease without moving the source window.
    let markerless = SandboxId::new();
    insert_sandbox(&fixture.pool, markerless, "restoring").await?;
    age_clock(markerless).await?;
    let markerless_boundary = source_boundary(markerless).await?;
    let sandbox = fixture.store.require_sandbox(markerless).await?;
    let prepared = fixture
        .store
        .prepare_wake_sandbox(&sandbox, markerless, None)
        .await?;
    assert!(
        !prepared.restore_started,
        "an already-restoring row does not make a suspended-to-restoring claim"
    );
    let redrive_token = user_redrive_restore_token(
        prepared
            .restore_claim
            .expect("markerless restoring wake should claim the restore lease"),
    );
    assert_eq!(
        source_boundary(markerless).await?,
        markerless_boundary,
        "markerless restoring redrive must preserve the restore source boundary"
    );
    let lease_token: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT restore_lease_token FROM sandboxes WHERE id = $1"
    ))
    .bind(markerless)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(lease_token, Some(redrive_token));

    let active = SandboxId::new();
    let active_token = Uuid::now_v7();
    insert_sandbox(&fixture.pool, active, "restoring").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() + INTERVAL '1 minute',
                restore_lease_token = $2
          WHERE id = $1"
    ))
    .bind(active)
    .bind(active_token)
    .execute(&fixture.pool)
    .await?;
    let sandbox = fixture.store.require_sandbox(active).await?;
    let err = match fixture
        .store
        .prepare_wake_sandbox(&sandbox, active, None)
        .await
    {
        Ok(_) => panic!("wake redrive must wait for the active restore lease"),
        Err(err) => err,
    };
    assert!(matches!(err, SubstrateError::RestoreInFlight { .. }));

    // Marker-set restoring (an evacuation restore the drain owns) preserves
    // the same boundary without taking a user restore lease.
    let marked = SandboxId::new();
    insert_sandbox(&fixture.pool, marked, "restoring").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = 'started' WHERE id = $1"
    ))
    .bind(marked)
    .execute(&fixture.pool)
    .await?;
    age_clock(marked).await?;
    let marked_boundary = source_boundary(marked).await?;
    let sandbox = fixture.store.require_sandbox(marked).await?;
    let prepared = fixture
        .store
        .prepare_wake_sandbox(&sandbox, marked, None)
        .await?;
    assert_eq!(prepared.restore_claim, None);
    assert_eq!(
        source_boundary(marked).await?,
        marked_boundary,
        "marker-set evacuation restoring boundary must be left alone"
    );

    Ok(())
}

async fn prepare_wake_sandbox_restores_suspended_lifecycle(fixture: &PostgresStore) -> TestResult {
    let suspended_id = SandboxId::new();
    insert_sandbox(&fixture.pool, suspended_id, "suspended").await?;

    let suspended = fixture.store.require_sandbox(suspended_id).await?;
    let prepared = fixture
        .store
        .prepare_wake_sandbox(&suspended, suspended_id, None)
        .await?;

    assert!(prepared.restore_started);
    let restore_token = user_fresh_restore_token(
        prepared
            .restore_claim
            .expect("suspended wake should claim restore"),
    );
    assert_eq!(prepared.sandbox.lifecycle, "restoring");
    // The persisted row must carry exactly the claim's fresh lease token.
    insta::assert_yaml_snapshot!(
        sandbox_lease_row(&fixture.pool, suspended_id).await?,
        {
            ".restore_lease_token" => insta::dynamic_redaction(move |value, _path| {
                assert_eq!(
                    value.as_str(),
                    Some(restore_token.to_string().as_str()),
                    "suspended wake must persist the claim's fresh lease token"
                );
                "[lease_token]"
            }),
        }
    );

    let started_id = SandboxId::new();
    insert_sandbox(&fixture.pool, started_id, "started").await?;

    let started = fixture.store.require_sandbox(started_id).await?;
    let prepared = fixture
        .store
        .prepare_wake_sandbox(&started, started_id, None)
        .await?;

    assert!(!prepared.restore_started);
    assert_eq!(prepared.restore_claim, None);
    assert_eq!(prepared.sandbox.lifecycle, "started");
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(started_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(lifecycle, "started");
    Ok(())
}

async fn restored_generation_accepts_late_pod_metadata(fixture: &PostgresStore) -> TestResult {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "suspended").await?;

    let first = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    finish_suspended_execution_for_test(fixture, sandbox_id).await?;
    fixture
        .store
        .begin_restore_lifecycle(sandbox_id, false)
        .await?;
    let restored = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;

    assert_eq!(first.generation, 1);
    assert_eq!(restored.generation, 2);

    let before = fixture
        .store
        .active_execution_status(sandbox_id)
        .await?
        .expect("restored execution should be active");
    assert_eq!(before.generation, restored.generation);
    assert_eq!(before.pod_name, restored.pod_name);
    assert_eq!(before.pod_ip, None);

    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            restored.generation,
            Some("restore-pod-uid"),
            Some("100.64.101.240"),
        )
        .await?;

    let after = fixture
        .store
        .active_execution_status(sandbox_id)
        .await?
        .expect("restored execution should remain active");
    assert_eq!(after.generation, restored.generation);
    assert_eq!(after.pod_ip.as_deref(), Some("100.64.101.240"));

    let (state, pod_uid, pod_ip): (String, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT state, pod_uid, host(pod_ip)
         FROM sandbox_k8s_executions
         WHERE sandbox_id = $1 AND active",
    )
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(state, "running");
    assert_eq!(pod_uid.as_deref(), Some("restore-pod-uid"));
    assert_eq!(pod_ip.as_deref(), Some("100.64.101.240"));

    Ok(())
}

/// Kube inventory callers pass typed roles; the DB stores the canonical label.
async fn persist_k8s_node_inventory_stores_canonical_role(fixture: &PostgresStore) -> TestResult {
    let node_id = NodeId::new();
    let node_name = format!("space-test-node-role-{}", node_id.as_uuid().simple());

    fixture
        .store
        .persist_k8s_node_inventory(
            &node_name,
            "uid-A",
            node_id,
            NodeRole::Builder,
            Some("10.0.0.12"),
            NodeStateWrite::Manage(NodeLifecycleState::Ready),
        )
        .await?;

    let role: String = sqlx::query_scalar(sql!("SELECT role FROM nodes WHERE id = $1"))
        .bind(node_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(role, NodeRole::Builder.as_str());

    Ok(())
}

/// When the inventory reflector observes a same-name kube Node with
/// a different UID, the existing `node_k8s` row's UID is
/// overwritten by the upsert. That UID was the only signal the
/// drain reconciler had to detect a replacement node — without
/// clearing `drain_phase` atomically, the reconciler would resolve
/// the providerID of the REPLACEMENT instance and call ASG
/// terminate on it while the operator's `:terminate` was issued
/// against the original. Solution B: the inventory upsert clears
/// drain state when the UID changes; in-progress drains are
/// abandoned (visible in tracing) and the operator can re-drain.
async fn upsert_clears_drain_state_when_k8s_node_uid_changes(
    fixture: &PostgresStore,
) -> TestResult {
    let node_id = NodeId::new();
    let node_name = format!("space-test-node-{}", node_id.as_uuid().simple());

    // Register the node, then put it into a mid-drain state.
    fixture
        .store
        .persist_k8s_node_inventory(
            &node_name,
            "uid-A",
            node_id,
            NodeRole::Worker,
            Some("10.0.0.1"),
            NodeStateWrite::Manage(NodeLifecycleState::Ready),
        )
        .await?;
    sqlx::query(sql!(
        "UPDATE nodes
         SET drain_phase = 'draining',
             drain_mode = 'gentle',
             drain_started_at = NOW(),
             drain_last_error = 'pre-existing operator note'
         WHERE id = $1"
    ))
    .bind(node_id)
    .execute(&fixture.pool)
    .await?;

    // Sanity: drain state is what we set.
    let (phase, mode, started_some, err): (Option<String>, Option<String>, bool, Option<String>) =
        sqlx::query_as(sql!(
            "SELECT drain_phase, drain_mode, drain_started_at IS NOT NULL, drain_last_error
         FROM nodes WHERE id = $1"
        ))
        .bind(node_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(phase.as_deref(), Some("draining"));
    assert_eq!(mode.as_deref(), Some("gentle"));
    assert!(started_some);
    assert_eq!(err.as_deref(), Some("pre-existing operator note"));

    // The replacement node arrives: same name, new UID. This call
    // is the inventory reflector's normal upsert path.
    fixture
        .store
        .persist_k8s_node_inventory(
            &node_name,
            "uid-B",
            node_id,
            NodeRole::Worker,
            Some("10.0.0.1"),
            NodeStateWrite::Manage(NodeLifecycleState::Ready),
        )
        .await?;

    // node_k8s now holds the new UID.
    let stored_uid: String =
        sqlx::query_scalar(sql!("SELECT k8s_node_uid FROM node_k8s WHERE node_id = $1"))
            .bind(node_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(stored_uid, "uid-B");

    // Drain state must be cleared atomically with the UID change — *all* of it.
    let (phase, mode, started_some, err): (Option<String>, Option<String>, bool, Option<String>) =
        sqlx::query_as(sql!(
            "SELECT drain_phase, drain_mode, drain_started_at IS NOT NULL, drain_last_error
         FROM nodes WHERE id = $1"
        ))
        .bind(node_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert!(
        phase.is_none(),
        "drain_phase must be NULL after same-name UID replacement; got {phase:?}"
    );
    assert!(
        mode.is_none(),
        "drain_mode must be NULL after same-name UID replacement; got {mode:?}. \
         A mode outliving its phase makes the admin API and CLI report a node as \
         draining gently when it is not draining at all"
    );
    assert!(
        !started_some,
        "drain_started_at must be NULL after same-name UID replacement"
    );
    assert!(
        err.is_none(),
        "drain_last_error must be NULL after same-name UID replacement; got {err:?}"
    );
    Ok(())
}

/// Idempotency sanity check: an inventory upsert that observes the
/// SAME UID (the steady-state case, fired many times per minute)
/// must NOT clear drain state. Without this guarantee a drain
/// would be reset by every reflector tick.
async fn upsert_preserves_drain_state_when_k8s_node_uid_unchanged(
    fixture: &PostgresStore,
) -> TestResult {
    let node_id = NodeId::new();
    let node_name = format!("space-test-node-{}", node_id.as_uuid().simple());

    fixture
        .store
        .persist_k8s_node_inventory(
            &node_name,
            "uid-A",
            node_id,
            NodeRole::Worker,
            Some("10.0.0.2"),
            NodeStateWrite::Manage(NodeLifecycleState::Ready),
        )
        .await?;
    sqlx::query(sql!(
        "UPDATE nodes
         SET drain_phase = 'terminating',
             drain_started_at = NOW()
         WHERE id = $1"
    ))
    .bind(node_id)
    .execute(&fixture.pool)
    .await?;

    // Re-observation with the same UID (every reflector tick).
    fixture
        .store
        .persist_k8s_node_inventory(
            &node_name,
            "uid-A",
            node_id,
            NodeRole::Worker,
            Some("10.0.0.2"),
            NodeStateWrite::Manage(NodeLifecycleState::Ready),
        )
        .await?;

    let phase: Option<String> =
        sqlx::query_scalar(sql!("SELECT drain_phase FROM nodes WHERE id = $1"))
            .bind(node_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(
        phase.as_deref(),
        Some("terminating"),
        "same-UID upsert must NOT touch drain_phase"
    );
    Ok(())
}

async fn persist_k8s_node_inventory_repairs_missing_and_stale_address(
    fixture: &PostgresStore,
) -> TestResult {
    let node_id = NodeId::new();
    let node_name = format!("space-test-address-repair-{}", node_id.as_uuid().simple());

    let changed = fixture
        .store
        .persist_k8s_node_inventory(
            &node_name,
            "uid-A",
            node_id,
            NodeRole::Worker,
            None,
            NodeStateWrite::Manage(NodeLifecycleState::Registered),
        )
        .await?;
    assert!(changed, "initial inventory insert must report changed");

    let address: Option<String> =
        sqlx::query_scalar(sql!("SELECT HOST(address) FROM nodes WHERE id = $1"))
            .bind(node_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(address, None);

    let changed = fixture
        .store
        .persist_k8s_node_inventory(
            &node_name,
            "uid-A",
            node_id,
            NodeRole::Worker,
            Some("10.0.0.33"),
            NodeStateWrite::Manage(NodeLifecycleState::Ready),
        )
        .await?;
    assert!(changed, "address and state backfill must report changed");

    let (address, state): (Option<String>, Option<String>) =
        sqlx::query_as(sql!("SELECT HOST(address), state FROM nodes WHERE id = $1"))
            .bind(node_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(address.as_deref(), Some("10.0.0.33"));
    assert_eq!(state.as_deref(), Some("ready"));

    let stable_updated_at = OffsetDateTime::UNIX_EPOCH;
    sqlx::query(sql!(
        "UPDATE node_k8s SET updated_at = $2 WHERE node_id = $1"
    ))
    .bind(node_id)
    .bind(stable_updated_at)
    .execute(&fixture.pool)
    .await?;

    let changed = fixture
        .store
        .persist_k8s_node_inventory(
            &node_name,
            "uid-A",
            node_id,
            NodeRole::Worker,
            Some("10.0.0.33"),
            NodeStateWrite::Manage(NodeLifecycleState::Ready),
        )
        .await?;
    assert!(
        !changed,
        "unchanged inventory tick must report no DB changes"
    );

    let updated_at: OffsetDateTime =
        sqlx::query_scalar(sql!("SELECT updated_at FROM node_k8s WHERE node_id = $1"))
            .bind(node_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(
        updated_at, stable_updated_at,
        "unchanged inventory ticks must not churn node_k8s rows"
    );

    let changed = fixture
        .store
        .persist_k8s_node_inventory(
            &node_name,
            "uid-A",
            node_id,
            NodeRole::Worker,
            Some("10.0.0.44"),
            NodeStateWrite::Manage(NodeLifecycleState::Ready),
        )
        .await?;
    assert!(changed, "stale address repair must report changed");

    let address: Option<String> =
        sqlx::query_scalar(sql!("SELECT HOST(address) FROM nodes WHERE id = $1"))
            .bind(node_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(address.as_deref(), Some("10.0.0.44"));
    Ok(())
}

/// AZRebalance variant of the admin-row reuse bug.
///
/// The SPACE node id is derived from `(k8s_node_name, role)`, so when
/// AZRebalance launches a fresh EC2 with a recycled private IP, the
/// resulting kube Node carries the same name but a new UID. The
/// inventory reflector calls `persist_k8s_node_inventory` against the
/// existing `nodes` row. Without the UID-change clearing in the
/// upsert, the fresh instance would inherit the previous instance's
/// drain state — `drain_phase` set, `drain_started_at` from the
/// already-terminated EC2 — so the operator sees a "draining" node
/// that has none of the original sandboxes and the drain reconciler
/// would try to converge it against the replacement EC2. This test
/// pins the partial-fix #1 contract: the reused row reflects the new
/// instance's identity and no longer carries the prior counter set.
async fn azrebalance_variant_uid_change_does_not_leak_stale_drain_state_or_drain_started_at(
    fixture: &PostgresStore,
) -> TestResult {
    let node_id = NodeId::new();
    let node_name = format!("space-test-azrebalance-{}", node_id.as_uuid().simple());

    fixture
        .store
        .persist_k8s_node_inventory(
            &node_name,
            "instance-A-uid",
            node_id,
            NodeRole::Worker,
            Some("10.0.0.20"),
            NodeStateWrite::Manage(NodeLifecycleState::Ready),
        )
        .await?;
    sqlx::query(sql!(
        "UPDATE nodes
         SET drain_phase = 'draining',
             drain_started_at = NOW(),
             drain_last_error = 'pre-replacement note'
         WHERE id = $1"
    ))
    .bind(node_id)
    .execute(&fixture.pool)
    .await?;

    fixture
        .store
        .persist_k8s_node_inventory(
            &node_name,
            "instance-B-uid",
            node_id,
            NodeRole::Worker,
            Some("10.0.0.20"),
            NodeStateWrite::Manage(NodeLifecycleState::Ready),
        )
        .await?;

    let stored_uid: String =
        sqlx::query_scalar(sql!("SELECT k8s_node_uid FROM node_k8s WHERE node_id = $1"))
            .bind(node_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(stored_uid, "instance-B-uid");

    let (phase, started_at, last_error): (Option<String>, Option<OffsetDateTime>, Option<String>) =
        sqlx::query_as(sql!(
            "SELECT drain_phase, drain_started_at, drain_last_error
         FROM nodes
         WHERE id = $1"
        ))
        .bind(node_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert!(
        phase.is_none(),
        "drain_phase must reset to NULL; got {phase:?}"
    );
    assert!(
        started_at.is_none(),
        "drain_started_at must reset to NULL; got {started_at:?}"
    );
    assert!(
        last_error.is_none(),
        "drain_last_error must reset to NULL; got {last_error:?}"
    );
    Ok(())
}

/// The bootstrap gate alone is not enough: `kube_node_sync` and the
/// late-pod-placement path also reach the upsert and would otherwise
/// re-create the row under the same deterministic id. Asserts that
/// `persist_k8s_node_inventory` refuses when a tombstone exists.
async fn persist_k8s_node_inventory_refuses_when_tombstoned(fixture: &PostgresStore) -> TestResult {
    let node_id = NodeId::new();
    let node_name = format!(
        "space-test-tombstoned-inventory-{}",
        node_id.as_uuid().simple()
    );
    sqlx::query(sql!(
        "INSERT INTO node_k8s_tombstones (node_id, k8s_node_name, role, reason)
         VALUES ($1, $2, 'worker', 'admin-terminate')"
    ))
    .bind(node_id)
    .bind(&node_name)
    .execute(&fixture.pool)
    .await?;

    let err = fixture
        .store
        .persist_k8s_node_inventory(
            &node_name,
            "instance-uid",
            node_id,
            NodeRole::Worker,
            Some("10.0.0.21"),
            NodeStateWrite::Manage(NodeLifecycleState::Ready),
        )
        .await
        .expect_err("inventory upsert must refuse tombstoned node");
    assert!(
        err.to_string().contains("tombstoned"),
        "error must mention tombstone: {err}",
    );

    let nodes_count: i64 = sqlx::query_scalar(sql!("SELECT COUNT(*) FROM nodes WHERE id = $1"))
        .bind(node_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(
        nodes_count, 0,
        "tombstoned upsert must not leave a nodes row"
    );
    Ok(())
}

/// A store call whose `SELECT ... FOR UPDATE` guard loses the row lock must
/// tell the caller to retry, not that the operation failed.
///
/// A real `55P03` from a genuinely held row lock rather than an injected
/// SQLSTATE, so the whole path is under test: the pool's `lock_timeout`, the
/// `BEGIN`, the guard's own statement, and the mapping. An injected SQLSTATE
/// only ever exercises the last of those. The disposition asserted is the one
/// an operator sees — the `ControllerError` status and its attribution — not
/// the intermediate variant.
///
/// `claim_started_recovery` is the shape the whole file shares: `BEGIN`, lock
/// the `sandboxes` row, then mutate the execution rows. The production peer
/// that holds that row is another lifecycle transaction on the same sandbox —
/// a concurrent suspend, delete, or restore claim — which is why every guard
/// here takes the row before touching the execution rows at all.
#[tokio::test]
async fn a_contended_execution_guard_answers_retryable_not_failed() -> TestResult {
    let db = pg_test_db_with_max_connections(2).await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&db.pool, sandbox_id, SandboxLifecycle::Started.as_str()).await?;

    // A separate pool so `lock_timeout` is set at connect time and applies to
    // the store's statements only. Without a bound the guard below would block
    // until the blocker commits instead of aborting, which is the
    // pre-`lock_timeout` behavior this classification exists for.
    let store_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::query("SET lock_timeout = '150ms'")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&db.database_url)
        .await?;
    let store = K8sStore::new(store_pool, "space".to_string());
    let typed = sandbox_id;

    let mut blocker = db.pool.begin().await?;
    sqlx::query(sql!(
        "SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE"
    ))
    .bind(sandbox_id)
    .fetch_one(&mut *blocker)
    .await?;

    let error = store
        .claim_started_recovery(typed)
        .await
        .expect_err("the guard must abort rather than wait out the held row lock");
    assert!(
        matches!(error, SubstrateError::LockContention { .. }),
        "a held row lock must classify as contention, got {error:?}"
    );
    let surfaced: crate::controllers::ControllerError = error.into();
    assert_eq!(
        surfaced.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "a lost lock race must not be a 500"
    );
    assert!(
        surfaced.is_lock_contention(),
        "an unmarked 503 is re-labelled as a fleet condition by the surface translators"
    );

    // The identical call succeeds once the blocker releases, so the abort was
    // the bound firing rather than a missing precondition.
    blocker.rollback().await?;
    store
        .claim_started_recovery(typed)
        .await
        .expect("the same claim succeeds once the row lock is released");
    Ok(())
}

/// A contended post-wake update must NOT be retryable, even though every rule
/// in this PR says to classify a contended `UPDATE`.
///
/// It is the documented exception: by the time this statement runs the warm
/// slot is claimed, bound and prepared and the pod is running, and `create`
/// mints a fresh `SandboxId` per request with no idempotency key — so a 503
/// here has the caller claim a second slot and start a second sandbox while
/// the first stays `started` under an id they never see.
///
/// Asserted because nothing else protects it. The exception is carried by a
/// comment, and a comment is exactly what a later reader "fixes" when every
/// sibling statement around it is classified — this is the arm whose own
/// if/else twin was already documented and which got classified anyway.
#[tokio::test]
async fn a_contended_post_wake_update_is_not_retryable() -> TestResult {
    use crate::controllers::sandbox::update_and_fetch_post_wake_sandbox;

    let db = pg_test_db_with_max_connections(2).await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&db.pool, sandbox_id, SandboxLifecycle::Suspended.as_str()).await?;

    // A bounded pool so the held row lock aborts rather than blocking.
    let bounded = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::query("SET lock_timeout = '150ms'")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&db.database_url)
        .await?;

    let mut blocker = db.pool.begin().await?;
    sqlx::query(sql!(
        "SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE"
    ))
    .bind(sandbox_id)
    .fetch_one(&mut *blocker)
    .await?;

    let error = update_and_fetch_post_wake_sandbox(&bounded, sandbox_id, None)
        .await
        .expect_err("the held row lock must abort the post-wake update");
    assert_eq!(
        error.status(),
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        "a retryable 503 here provisions a second sandbox: {error}"
    );
    assert!(
        !error.is_lock_contention(),
        "the contention marking would reach the surface as a retryable 503"
    );

    blocker.rollback().await?;
    Ok(())
}

/// A create and a wake that hit the identical lock race while recording where
/// their pod landed must get opposite answers.
///
/// The wake is told to retry: its retry re-drives the sandbox row it already
/// names, so it converges. The create is not: `create` mints a fresh
/// `SandboxId` and carries no idempotency key, so telling it to retry buys a
/// second sandbox and leaves this one's pod live under an id the caller never
/// learns. Same statement, same abort, opposite disposition — which is why the
/// disposition is the caller's parameter and not a property of the statement.
#[tokio::test]
async fn placement_retryability_follows_the_operation_not_the_statement() -> TestResult {
    let db = pg_test_db_with_max_connections(2).await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&db.pool, sandbox_id, SandboxLifecycle::Started.as_str()).await?;

    // A separate bounded pool, as in `a_contended_execution_guard_answers_
    // retryable_not_failed`: without `lock_timeout` the placement transaction
    // would wait out the blocker instead of aborting.
    let store_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::query("SET lock_timeout = '150ms'")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&db.database_url)
        .await?;
    let store = K8sStore::new(store_pool, "space".to_string());
    let node_id = NodeId::new();

    // Hold the row `lock_sandbox_for_execution_mutation_in` takes.
    let mut blocker = db.pool.begin().await?;
    sqlx::query(sql!(
        "SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE"
    ))
    .bind(sandbox_id)
    .fetch_one(&mut *blocker)
    .await?;

    let persist = |retry| {
        store.persist_pod_node_placement_after_pod_exists(
            PodNodePlacement {
                sandbox_id,
                node_name: "ip-10-0-0-1.ec2.internal",
                node_uid: "11111111-1111-1111-1111-111111111111",
                node_id,
                role: NodeRole::Worker,
                address: Some("10.0.0.1"),
            },
            retry,
        )
    };

    let converging = persist(PlacementRetryDisposition::RetryConverges)
        .await
        .expect_err("the placement transaction must abort on the held row lock");
    assert!(
        matches!(converging, SubstrateError::LockContention { .. }),
        "a wake's placement write must stay classified as contention, got {converging:?}"
    );
    let surfaced: crate::controllers::ControllerError = converging.into();
    assert_eq!(
        surfaced.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "a wake retry converges on the same sandbox, so its lost lock race is a 503"
    );
    assert!(
        surfaced.is_lock_contention(),
        "without the marker the surface translators re-label this as a fleet condition"
    );

    let duplicating = persist(PlacementRetryDisposition::RetryDuplicatesExecution)
        .await
        .expect_err("the placement transaction must abort on the held row lock");
    assert!(
        matches!(duplicating, SubstrateError::Failed { .. }),
        "a create's placement write must be demoted, not reported retryable, got {duplicating:?}"
    );
    let surfaced: crate::controllers::ControllerError = duplicating.into();
    assert_eq!(
        surfaced.status(),
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        "a retryable answer here would buy a second sandbox"
    );
    assert!(
        !surfaced.is_lock_contention(),
        "the marker must be cleared too — the translators rebuild a retryable \
         answer from the kind alone, so leaving it set defeats the demotion"
    );

    // Both aborts were the bound firing, not a missing precondition: the same
    // call succeeds once the blocker releases.
    blocker.rollback().await?;
    persist(PlacementRetryDisposition::RetryConverges)
        .await
        .expect("the same placement write succeeds once the row lock is released");
    Ok(())
}

#[tokio::test]
async fn retired_pool_reserved_generation_moves_cold_create_past_the_pool_pod() -> TestResult {
    // A create that could not adopt its warm-pool slot builds cold under the
    // slot's reserved ID. The slot's pod used generation 1 of that ID, so the
    // cold execution, and every later restore or wake, must start past it.
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "creating").await?;

    fixture
        .store
        .retire_pool_reserved_generation(sandbox_id)
        .await?;
    // Idempotent for a retried create.
    fixture
        .store
        .retire_pool_reserved_generation(sandbox_id)
        .await?;

    let claim = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?;
    assert!(claim.created);
    assert_eq!(claim.row.generation, 2);
    assert_eq!(
        claim.row.pod_name,
        pplx_space_common_core::kubernetes::sandbox_pod_name(sandbox_id, 2)
    );
    assert_ne!(
        claim.row.pod_name,
        pplx_space_common_core::kubernetes::sandbox_pod_name(sandbox_id, 1)
    );

    // Once the sandbox has executions, retiring is a no-op.
    fixture
        .store
        .retire_pool_reserved_generation(sandbox_id)
        .await?;
    let rows: Vec<(i64, bool, String)> = sqlx::query_as(sql!(
        "SELECT generation, active, state
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          ORDER BY generation"
    ))
    .bind(sandbox_id)
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, false, "replaced".to_string()));
    assert_eq!(rows[1].0, 2);
    assert!(rows[1].1);

    // A later activation (restore, wake) continues from generation 2.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE, state = 'replaced', terminated_at = NOW()
          WHERE sandbox_id = $1 AND generation = 2"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    let next = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?;
    assert_eq!(next.row.generation, 3);
    assert_eq!(
        next.row.pod_name,
        pplx_space_common_core::kubernetes::sandbox_pod_name(sandbox_id, 3)
    );
    Ok(())
}
