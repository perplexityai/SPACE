//! Sandbox publication behavior through the public internal API. Keep private
//! implementation tests in the library so this target reuses its normal rlib.

#![cfg(test)]

use pplx_space_common_core::types::{LocalArtifactId, NodeId, SnapshotId};
use pplx_space_common_core::{
    sql,
    types::{
        AbandonedRestoreRetirement, Arch, AutoLifecycleConditionSet, AutoLifecycleConditionSetId,
        AutoLifecycleDeleteReceiptId, AutoLifecycleIdleAction, AutoLifecyclePolicy,
        AutoLifecycleStage, ExecutionMetadata, PolicyAuthority, SandboxActivationAttemptId,
        SandboxId, SandboxLifecycle, SandboxQuarantineHistoryAction, SandboxQuarantineNextAction,
        SandboxQuarantineReasonCode, SandboxQuarantineSource,
    },
};
use pplx_space_common_svc::types::{
    AutoLifecyclePolicyFingerprint, FACET_NETWORK_POLICY, FACET_SANDBOX_DESIRED_STATE,
    NetworkPolicyTarget, SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY, SANDBOX_FAILURE_EPISODE_KEY,
    SANDBOX_FAILURE_EXECUTION_GENERATION_KEY, SANDBOX_LOCAL_SUSPEND_KEY,
};
use pplx_space_db_test_fixtures::{
    insert_pool_slot_receipt, insert_ready_worker_node, pg_test_db, pool_slot_receipt_state,
    publish_ready_disk_artifact, try_pg_test_db, try_pg_test_db_with_max_connections,
};
use pplx_space_internal_api_server::{
    ClusterApiService,
    proto::{
        GetQuarantineRecoveryPlanRequest, NodeRole as ProtoNodeRole, PublishNodeStateRequest,
        PublishedSandboxState, cluster_api_server::ClusterApi,
    },
    publish_sandbox_states, publish_sandbox_states_with_retirement,
};
use pplx_space_net_common::proto::NetworkPolicySource;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use tonic::Request;
use uuid::Uuid;

#[path = "support/published_state.rs"]
mod support;
use support::*;

/// Typed reader for the placement columns a suspend finalization clears,
/// alongside the execution's own active/state columns.
#[derive(Debug, sqlx::FromRow, serde::Serialize)]
struct SandboxExecutionPlacementRow {
    lifecycle: String,
    placement: String,
    active_node_id: Option<NodeId>,
    active: bool,
    state: String,
}

async fn sandbox_execution_placement_row(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<SandboxExecutionPlacementRow> {
    Ok(sqlx::query_as(sql!(
        "SELECT s.lifecycle, s.placement, s.active_node_id, e.active, e.state
           FROM sandboxes s
           JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

async fn insert_ready_legacy_suspend_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
) -> TestResult<SnapshotId> {
    let snapshot_id = SnapshotId::new();
    sqlx::query(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, source_node_id, org_id
         )
         VALUES (
             $1, $2, 'full', 'ready', $3,
             '00000000-0000-0000-0000-000000000001'
         )",
    )
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES (
             $1,
             's3://space-test/legacy-suspend-snapshot',
             '00000000-0000-0000-0000-000000000001'
         )",
    )
    .bind(snapshot_id)
    .execute(pool)
    .await?;
    Ok(snapshot_id)
}

fn quarantine_recovery_plan_request(
    node_id: NodeId,
    sandbox_id: SandboxId,
) -> Request<GetQuarantineRecoveryPlanRequest> {
    let mut request = Request::new(GetQuarantineRecoveryPlanRequest {
        node_id: node_id.to_string(),
        sandbox_id: sandbox_id.to_string(),
        role: Some(ProtoNodeRole::Worker as i32),
    });
    request
        .extensions_mut()
        .insert(tonic::transport::server::TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(std::net::SocketAddr::from(([127, 0, 0, 1], 49000))),
        });
    request
}

fn assert_quarantine_recovery_rejection_cause(
    captured: Vec<pplx_space_metrics::test_capture::CapturedCounter>,
    expected: &str,
) {
    let rejected = captured
        .into_iter()
        .filter(|counter| counter.name == "internal_api.quarantine_recovery_plan.rejected")
        .collect::<Vec<_>>();
    assert_eq!(
        rejected.len(),
        1,
        "expected one quarantine-recovery rejection counter: {rejected:?}"
    );
    assert_eq!(rejected[0].tags, [format!("cause:{expected}")]);
}

async fn pin_suspend_snapshot_artifact(
    pool: &PgPool,
    sandbox_id: SandboxId,
    snapshot_id: SnapshotId,
) -> TestResult {
    sqlx::query(
        "UPDATE sandboxes s
            SET current_local_artifact_id = snapshot.local_artifact_id,
                current_local_artifact_kind = snapshot.artifact_kind,
                current_local_artifact_origin = snapshot.artifact_origin
           FROM snapshots snapshot
          WHERE s.id = $1
            AND snapshot.id = $2
            AND snapshot.sandbox_id = s.id",
    )
    .bind(sandbox_id)
    .bind(snapshot_id)
    .execute(pool)
    .await?;
    Ok(())
}

fn failed_sandbox_state(sandbox_id: SandboxId) -> PublishedSandboxState {
    PublishedSandboxState {
        sandbox_id: sandbox_id.to_string(),
        present: true,
        status: Some("failed".to_string()),
        template: Some("app-runtime".to_string()),
        content_hash: Some("sha256:test".to_string()),
        runtime: Some("fc".to_string()),
        snapshots_enabled: true,
        failure_reason: Some("guest bridge health probe timed out".to_string()),
        ..PublishedSandboxState::default()
    }
}

fn quarantined_sandbox_state(sandbox_id: SandboxId) -> PublishedSandboxState {
    PublishedSandboxState {
        sandbox_id: sandbox_id.to_string(),
        present: true,
        status: Some("quarantined".to_string()),
        template: Some("app-runtime".to_string()),
        content_hash: Some("sha256:test".to_string()),
        runtime: Some("fc".to_string()),
        snapshots_enabled: true,
        failure_reason: Some("sync path found conflicting local artifacts".to_string()),
        quarantine_reason_code: Some("suspend_snapshot_publication_failed".to_string()),
        ..PublishedSandboxState::default()
    }
}

fn recovered_suspended_state(
    sandbox_id: SandboxId,
    receipt_id: Uuid,
    execution_generation: i64,
    activation_attempt_id: Option<Uuid>,
    reason_code: SandboxQuarantineReasonCode,
    local_artifact_id: LocalArtifactId,
) -> PublishedSandboxState {
    PublishedSandboxState {
        sandbox_id: sandbox_id.to_string(),
        present: true,
        status: Some("suspended".to_string()),
        transition_receipt: Some(pplx_space_sbx_mgr_client::proto::TransitionReceipt {
            id: receipt_id.to_string(),
            kind: "quarantine_recovery".to_string(),
            target_state: "suspended".to_string(),
            execution_generation,
            reason_code: reason_code.to_string(),
            activation_attempt_id: activation_attempt_id.map(|id| id.to_string()),
        }),
        current_local_artifact_id: Some(local_artifact_id.to_string()),
        current_local_artifact_kind: Some(
            pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32,
        ),
        current_local_artifact_origin: Some(
            pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32,
        ),
        ..PublishedSandboxState::default()
    }
}

fn recovered_legacy_suspended_state(
    sandbox_id: SandboxId,
    receipt_id: Uuid,
    execution_generation: i64,
) -> PublishedSandboxState {
    PublishedSandboxState {
        sandbox_id: sandbox_id.to_string(),
        present: true,
        status: Some("suspended".to_string()),
        transition_receipt: Some(pplx_space_sbx_mgr_client::proto::TransitionReceipt {
            id: receipt_id.to_string(),
            kind: "quarantine_recovery".to_string(),
            target_state: "suspended".to_string(),
            execution_generation,
            reason_code: SandboxQuarantineReasonCode::OrphanedSandboxRowMissing.to_string(),
            activation_attempt_id: None,
        }),
        ..PublishedSandboxState::default()
    }
}

fn activation_completed_state(
    sandbox_id: SandboxId,
    receipt_id: Uuid,
    target_state: &str,
    execution_generation: i64,
    activation_attempt_id: Uuid,
) -> PublishedSandboxState {
    PublishedSandboxState {
        sandbox_id: sandbox_id.to_string(),
        present: true,
        status: Some(target_state.to_string()),
        transition_receipt: Some(pplx_space_sbx_mgr_client::proto::TransitionReceipt {
            id: receipt_id.to_string(),
            kind: "activation_completed".to_string(),
            target_state: target_state.to_string(),
            execution_generation,
            reason_code: String::new(),
            activation_attempt_id: Some(activation_attempt_id.to_string()),
        }),
        ..PublishedSandboxState::default()
    }
}

fn activation_rollback_state(
    sandbox_id: SandboxId,
    receipt_id: Uuid,
    target_state: &str,
    execution_generation: i64,
    activation_attempt_id: Uuid,
    reason_code: SandboxQuarantineReasonCode,
) -> PublishedSandboxState {
    PublishedSandboxState {
        sandbox_id: sandbox_id.to_string(),
        present: true,
        status: Some(target_state.to_string()),
        transition_receipt: Some(pplx_space_sbx_mgr_client::proto::TransitionReceipt {
            id: receipt_id.to_string(),
            kind: "activation_rollback".to_string(),
            target_state: target_state.to_string(),
            execution_generation,
            reason_code: reason_code.to_string(),
            activation_attempt_id: Some(activation_attempt_id.to_string()),
        }),
        ..PublishedSandboxState::default()
    }
}

fn published_sandbox_state_with_network_policy(
    sandbox_id: SandboxId,
    generation: u64,
) -> PublishedSandboxState {
    let mut state = published_sandbox_state(sandbox_id, "started");
    state.observed_network_policy = Some(pplx_space_net_common::proto::NetworkPolicyV1 {
        version: pplx_space_net_common::proto::NetworkPolicyVersion::V1 as i32,
        egress_default_action: pplx_space_net_common::proto::EgressDefaultAction::Allow as i32,
        ..pplx_space_net_common::proto::NetworkPolicyV1::default()
    });
    state.observed_network_policy_generation = generation;
    state.observed_network_policy_source = NetworkPolicySource::Override as i32;
    state.observed_network_policy_authority = PolicyAuthority::Cluster.as_proto_i32();
    state
}

fn absent_sandbox_state(sandbox_id: SandboxId) -> PublishedSandboxState {
    PublishedSandboxState {
        sandbox_id: sandbox_id.to_string(),
        present: false,
        ..PublishedSandboxState::default()
    }
}

async fn sandbox_lifecycle(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<String> {
    Ok(
        sqlx::query_scalar::<_, String>("SELECT lifecycle FROM sandboxes WHERE id = $1")
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

async fn sandbox_exists(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<bool> {
    Ok(
        sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM sandboxes WHERE id = $1)")
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

async fn sandbox_last_used_at(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<Option<time::OffsetDateTime>> {
    Ok(
        sqlx::query_scalar("SELECT last_used_at FROM sandboxes WHERE id = $1")
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

async fn sandbox_activity_at(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<(
    Option<time::OffsetDateTime>,
    Option<time::OffsetDateTime>,
    Option<time::OffsetDateTime>,
)> {
    Ok(sqlx::query_as(
        "SELECT last_used_at, last_access_at, last_command_at
         FROM sandboxes
         WHERE id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

async fn sandbox_updated_at(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<time::OffsetDateTime> {
    Ok(
        sqlx::query_scalar("SELECT updated_at FROM sandboxes WHERE id = $1")
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

async fn network_policy_facet_count(
    pool: &PgPool,
    node_id: NodeId,
    sandbox_id: SandboxId,
) -> TestResult<i64> {
    Ok(sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM node_facet_observed_status
         WHERE node_id = $1
           AND facet = $2
           AND element = $3",
    )
    .bind(node_id)
    .bind(FACET_NETWORK_POLICY)
    .bind(NetworkPolicyTarget::sandbox(sandbox_id).as_element_string())
    .fetch_one(pool)
    .await?)
}

async fn set_sandbox_updated_at_for_test(
    pool: &PgPool,
    sandbox_id: SandboxId,
    updated_at: time::OffsetDateTime,
) -> TestResult {
    let mut tx = pool.begin().await?;
    sqlx::query("ALTER TABLE sandboxes DISABLE TRIGGER sandboxes_set_updated_at")
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE sandboxes SET updated_at = $2 WHERE id = $1")
        .bind(sandbox_id)
        .bind(updated_at)
        .execute(&mut *tx)
        .await?;
    sqlx::query("ALTER TABLE sandboxes ENABLE TRIGGER sandboxes_set_updated_at")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn sandbox_failure_episode_provenance_is_recorded_and_cleared() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "failed").await?;

    let k8s_attempt_id = SandboxActivationAttemptId::new();
    let mut failed = published_sandbox_state(sandbox_id, "failed");
    failed.execution_generation = Some(7);
    failed.pod_uid = Some("pod-attempt-a".to_string());
    failed.activation_attempt_id = Some(k8s_attempt_id.to_string());
    failed.failure_reason = Some("internal runtime diagnostics".to_string());
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[failed]).await?;
    drop(conn);

    let sqlx::types::Json(payload): sqlx::types::Json<serde_json::Value> =
        sqlx::query_scalar(sql!(
            "SELECT observed_payload
         FROM node_facet_observed_status
         WHERE node_id = $1 AND facet = $2 AND element = $3"
        ))
        .bind(node_id)
        .bind(FACET_SANDBOX_DESIRED_STATE)
        .bind(sandbox_id.to_string())
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(
        payload,
        serde_json::json!({
            "failure_episode": {
                "execution_generation": 7,
                "pod_uid": "pod-attempt-a",
                "activation_attempt_id": k8s_attempt_id.to_string()
            }
        })
    );

    let mut degraded = published_sandbox_state(sandbox_id, "failed");
    degraded.state_degraded = true;
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[degraded]).await?;
    drop(conn);

    let preserved_generation: Option<i64> = sqlx::query_scalar(sql!(
        "SELECT jsonb_extract_path_text(observed_payload, $4::TEXT, $5::TEXT)::BIGINT
         FROM node_facet_observed_status
         WHERE node_id = $1 AND facet = $2 AND element = $3"
    ))
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .bind(SANDBOX_FAILURE_EPISODE_KEY)
    .bind(SANDBOX_FAILURE_EXECUTION_GENERATION_KEY)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(preserved_generation, Some(7));

    let mut malformed = published_sandbox_state(sandbox_id, "failed");
    malformed.execution_generation = Some(8);
    malformed.pid = Some(u32::MAX);
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[malformed.clone()]).await?;
    drop(conn);

    let preserved_generation: Option<i64> = sqlx::query_scalar(sql!(
        "SELECT jsonb_extract_path_text(observed_payload, $4::TEXT, $5::TEXT)::BIGINT
         FROM node_facet_observed_status
         WHERE node_id = $1 AND facet = $2 AND element = $3"
    ))
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .bind(SANDBOX_FAILURE_EPISODE_KEY)
    .bind(SANDBOX_FAILURE_EXECUTION_GENERATION_KEY)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(preserved_generation, Some(7));

    malformed.status = Some("started".to_string());
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[malformed]).await?;
    drop(conn);

    let has_episode: bool = sqlx::query_scalar(sql!(
        "SELECT observed_payload ? $4::TEXT
         FROM node_facet_observed_status
         WHERE node_id = $1 AND facet = $2 AND element = $3"
    ))
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .bind(SANDBOX_FAILURE_EPISODE_KEY)
    .fetch_one(&db.pool)
    .await?;
    assert!(!has_episode);

    let stale_sandbox_node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, stale_sandbox_node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET managed_by = 'space', active_node_id = $2
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(stale_sandbox_node_id)
    .execute(&db.pool)
    .await?;
    let direct_attempt_id = SandboxActivationAttemptId::new();
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_identities (
             sandbox_id, route_generation, current_node_id, substrate_kind,
             execution_generation, activation_attempt_id
         ) VALUES ($1, 1, $2, 'space', 9, $3)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(direct_attempt_id.as_uuid())
    .execute(&db.pool)
    .await?;

    let mut direct_failed = published_sandbox_state(sandbox_id, "failed");
    direct_failed.execution_generation = Some(9);
    direct_failed.activation_attempt_id = Some(direct_attempt_id.to_string());
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[direct_failed.clone()]).await?;
    drop(conn);

    let (direct_generation, observed_attempt): (Option<i64>, Option<String>) =
        sqlx::query_as(sql!(
            "SELECT jsonb_extract_path_text(observed_payload, $4::TEXT, $5::TEXT)::BIGINT,
                jsonb_extract_path_text(observed_payload, $4::TEXT, $6::TEXT)
         FROM node_facet_observed_status
         WHERE node_id = $1 AND facet = $2 AND element = $3"
        ))
        .bind(node_id)
        .bind(FACET_SANDBOX_DESIRED_STATE)
        .bind(sandbox_id.to_string())
        .bind(SANDBOX_FAILURE_EPISODE_KEY)
        .bind(SANDBOX_FAILURE_EXECUTION_GENERATION_KEY)
        .bind(SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(direct_generation, Some(9));
    assert_eq!(observed_attempt, Some(direct_attempt_id.to_string()));
    let direct_status: Option<String> = sqlx::query_scalar(sql!(
        "SELECT status
         FROM sandbox_observed_states
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(direct_status.as_deref(), Some("failed"));

    direct_failed.status = Some("started".to_string());
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[direct_failed]).await?;
    drop(conn);

    let has_episode: bool = sqlx::query_scalar(sql!(
        "SELECT observed_payload ? $4::TEXT
         FROM node_facet_observed_status
         WHERE node_id = $1 AND facet = $2 AND element = $3"
    ))
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .bind(SANDBOX_FAILURE_EPISODE_KEY)
    .fetch_one(&db.pool)
    .await?;
    assert!(!has_episode);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn quarantine_recovery_observation_is_validated_stored_and_cleared() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let local_quarantine_id = Uuid::now_v7();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "restoring").await?;
    sqlx::query(
        "INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_generation, observed_payload,
             observed_at, updated_at
         )
         VALUES ($1, $2, $3, 42, '{\"other\": {\"value\": 1}}'::JSONB, NOW(), NOW())",
    )
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .execute(&db.pool)
    .await?;

    let mut state = published_sandbox_state(sandbox_id, "restoring");
    let local_artifact_id = LocalArtifactId::new();
    state.execution_generation = Some(7);
    state.current_local_artifact_id = Some(local_artifact_id.to_string());
    state.current_local_artifact_kind =
        Some(pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32);
    state.current_local_artifact_origin =
        Some(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32);
    state.quarantine_recovery = Some(
        pplx_space_sbx_mgr_client::proto::QuarantineRecoveryObservation {
            local_quarantine_id: local_quarantine_id.to_string(),
            observed_owner:
                pplx_space_sbx_mgr_client::proto::QuarantineRecoveryObservedOwner::LegacyRecovery
                    as i32,
            attempt_count: 3,
            next_attempt_at: Some(timestamp(1_786_800_000)),
            schema_version: 1,
            ..Default::default()
        },
    );
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[state.clone()]).await?;
    drop(conn);

    let (payload, observed_generation, first_observed_at, first_updated_at): (
        serde_json::Value,
        Option<i64>,
        time::OffsetDateTime,
        time::OffsetDateTime,
    ) = sqlx::query_as(
        "SELECT observed_payload, observed_generation, observed_at, updated_at
           FROM node_facet_observed_status
          WHERE node_id = $1
            AND facet = $2
            AND element = $3",
    )
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .fetch_one(&db.pool)
    .await?;
    let observed = &payload["quarantine_recovery"];
    assert_eq!(observed["schema_version"], 1);
    assert_eq!(
        observed["local_quarantine_id"],
        local_quarantine_id.to_string()
    );
    assert_eq!(observed["observed_owner"], "legacy_recovery");
    assert_eq!(observed["attempt_count"], 3);
    assert_eq!(observed["next_attempt_at"], "2026-08-15T13:20:00Z");
    assert!(observed.get("observed_at").is_none());
    assert_eq!(payload["other"]["value"], 1);
    assert_eq!(observed_generation, Some(42));

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[state.clone()]).await?;
    drop(conn);
    let (unchanged_payload, unchanged_observed_at, unchanged_updated_at): (
        serde_json::Value,
        time::OffsetDateTime,
        time::OffsetDateTime,
    ) = sqlx::query_as(
        "SELECT observed_payload, observed_at, updated_at
           FROM node_facet_observed_status
          WHERE node_id = $1
            AND facet = $2
            AND element = $3",
    )
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(unchanged_payload, payload);
    assert_eq!(unchanged_observed_at, first_observed_at);
    assert_eq!(unchanged_updated_at, first_updated_at);

    sqlx::query(
        "UPDATE node_facet_observed_status
            SET observed_at = NOW() - INTERVAL '2 hours',
                updated_at = NOW() - INTERVAL '2 hours'
          WHERE node_id = $1
            AND facet = $2
            AND element = $3",
    )
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .execute(&db.pool)
    .await?;
    let stale_observed_at: time::OffsetDateTime = sqlx::query_scalar(
        "SELECT observed_at
           FROM node_facet_observed_status
          WHERE node_id = $1
            AND facet = $2
            AND element = $3",
    )
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .fetch_one(&db.pool)
    .await?;
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[state.clone()]).await?;
    drop(conn);
    let (refreshed_payload, refreshed_observed_at): (serde_json::Value, time::OffsetDateTime) =
        sqlx::query_as(
            "SELECT observed_payload, observed_at
               FROM node_facet_observed_status
              WHERE node_id = $1
                AND facet = $2
                AND element = $3",
        )
        .bind(node_id)
        .bind(FACET_SANDBOX_DESIRED_STATE)
        .bind(sandbox_id.to_string())
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(refreshed_payload, payload);
    assert!(refreshed_observed_at > stale_observed_at);

    state.quarantine_recovery = Some(
        pplx_space_sbx_mgr_client::proto::QuarantineRecoveryObservation {
            local_quarantine_id: "not-a-uuid".to_string(),
            schema_version: 1,
            ..Default::default()
        },
    );
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[state.clone()]).await?;
    drop(conn);
    let preserved: Option<String> = sqlx::query_scalar(
        "SELECT observed_payload #>> '{quarantine_recovery,local_quarantine_id}'
           FROM node_facet_observed_status
          WHERE node_id = $1
            AND facet = $2
            AND element = $3",
    )
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(preserved, Some(local_quarantine_id.to_string()));

    state.network_tx_bytes = Some(8_675_309);
    state.quarantine_recovery = Some(
        pplx_space_sbx_mgr_client::proto::QuarantineRecoveryObservation {
            degraded: true,
            ..Default::default()
        },
    );
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[state.clone()]).await?;
    drop(conn);
    let (preserved_after_degraded_worker_pass, updated_network_tx_bytes): (
        Option<String>,
        Option<i64>,
    ) = sqlx::query_as(
        "SELECT nfos.observed_payload #>> '{quarantine_recovery,local_quarantine_id}',
                sos.network_tx_bytes
           FROM node_facet_observed_status nfos
           JOIN sandbox_observed_states sos
             ON sos.sandbox_id = $3::UUID
            AND sos.node_id = nfos.node_id
          WHERE nfos.node_id = $1
            AND nfos.facet = $2
            AND nfos.element = $3",
    )
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        preserved_after_degraded_worker_pass,
        Some(local_quarantine_id.to_string())
    );
    assert_eq!(updated_network_tx_bytes, Some(8_675_309));

    state.quarantine_recovery = None;
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[state]).await?;
    drop(conn);
    let cleared: bool = sqlx::query_scalar(
        "SELECT observed_payload ? 'quarantine_recovery'
           FROM node_facet_observed_status
          WHERE node_id = $1
            AND facet = $2
            AND element = $3",
    )
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .fetch_one(&db.pool)
    .await?;
    assert!(!cleared);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn quarantine_recovery_observation_is_node_scoped_not_canonical_state() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let publishing_node_id = NodeId::new();
    let active_node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let local_quarantine_id = Uuid::now_v7();
    insert_ready_worker_node(&db.pool, publishing_node_id).await?;
    insert_ready_worker_node(&db.pool, active_node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, active_node_id, "restoring").await?;
    sqlx::query(
        "INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_generation, observed_payload,
             observed_at, updated_at
         )
         VALUES ($1, $2, $3, 42, '{\"other\": {\"value\": 1}}'::JSONB, NOW(), NOW())",
    )
    .bind(publishing_node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .execute(&db.pool)
    .await?;

    let mut state = published_sandbox_state(sandbox_id, "restoring");
    state.quarantine_recovery = Some(
        pplx_space_sbx_mgr_client::proto::QuarantineRecoveryObservation {
            local_quarantine_id: local_quarantine_id.to_string(),
            observed_owner:
                pplx_space_sbx_mgr_client::proto::QuarantineRecoveryObservedOwner::LegacyRecovery
                    as i32,
            schema_version: 1,
            ..Default::default()
        },
    );
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, publishing_node_id, &[state]).await?;
    drop(conn);

    let observed_local_quarantine_id: Option<String> = sqlx::query_scalar(
        "SELECT observed_payload #>> '{quarantine_recovery,local_quarantine_id}'
           FROM node_facet_observed_status
          WHERE node_id = $1
            AND facet = $2
            AND element = $3",
    )
    .bind(publishing_node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        observed_local_quarantine_id,
        Some(local_quarantine_id.to_string())
    );

    let canonical_node_id: Option<NodeId> =
        sqlx::query_scalar("SELECT node_id FROM sandbox_observed_states WHERE sandbox_id = $1")
            .bind(sandbox_id)
            .fetch_optional(&db.pool)
            .await?;
    assert_ne!(canonical_node_id, Some(publishing_node_id));

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(
        &mut conn,
        publishing_node_id,
        &[absent_sandbox_state(sandbox_id)],
    )
    .await?;
    drop(conn);

    let (payload, observed_generation): (serde_json::Value, Option<i64>) = sqlx::query_as(
        "SELECT observed_payload, observed_generation
           FROM node_facet_observed_status
          WHERE node_id = $1
            AND facet = $2
            AND element = $3",
    )
    .bind(publishing_node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .fetch_one(&db.pool)
    .await?;
    assert!(
        !payload
            .as_object()
            .unwrap()
            .contains_key("quarantine_recovery")
    );
    assert_eq!(payload["other"]["value"], 1);
    assert_eq!(observed_generation, Some(42));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn quarantine_recovery_observation_persists_and_clears_without_central_sandbox() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let local_quarantine_id = Uuid::now_v7();
    insert_ready_worker_node(&db.pool, node_id).await?;

    let mut state = published_sandbox_state(sandbox_id, "restoring");
    state.quarantine_recovery = Some(
        pplx_space_sbx_mgr_client::proto::QuarantineRecoveryObservation {
            local_quarantine_id: local_quarantine_id.to_string(),
            observed_owner:
                pplx_space_sbx_mgr_client::proto::QuarantineRecoveryObservedOwner::LegacyRecovery
                    as i32,
            schema_version: 1,
            ..Default::default()
        },
    );
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[state.clone()]).await?;
    drop(conn);

    let observed_local_quarantine_id: Option<String> = sqlx::query_scalar(
        "SELECT observed_payload #>> '{quarantine_recovery,local_quarantine_id}'
           FROM node_facet_observed_status
          WHERE node_id = $1
            AND facet = $2
            AND element = $3",
    )
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .fetch_optional(&db.pool)
    .await?
    .flatten();
    assert_eq!(
        observed_local_quarantine_id,
        Some(local_quarantine_id.to_string())
    );

    state.quarantine_recovery = None;
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[state]).await?;
    drop(conn);

    let recovery_key_present: Option<bool> = sqlx::query_scalar(
        "SELECT observed_payload ? 'quarantine_recovery'
           FROM node_facet_observed_status
          WHERE node_id = $1
            AND facet = $2
            AND element = $3",
    )
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .fetch_optional(&db.pool)
    .await?;
    assert_eq!(recovery_key_present, Some(false));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publish_updates_observed_state_without_writing_component_rows() -> TestResult {
    use pplx_space_common_core::types::{
        ArtifactComponentKind, ArtifactComponentMetadata, ArtifactComponentRole, ArtifactFormat,
        ArtifactVersion, Digest,
    };

    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;

    let mut initial = published_sandbox_state(sandbox_id, "started");
    initial.execution = Some((&test_execution()).into());
    let mut conn = db.pool.acquire().await?;
    let accepted = publish_sandbox_states(&mut conn, node_id, &[initial]).await?;
    assert_eq!(accepted.accepted, 1);
    drop(conn);

    let mut suspended = published_sandbox_state(sandbox_id, "suspended");
    suspended.execution = Some(
        (&ExecutionMetadata::initial_firecracker(
            Arch::Aarch64,
            vec![ArtifactComponentMetadata {
                kind: ArtifactComponentKind::Rootfs,
                role: Some(ArtifactComponentRole::parse("system").unwrap()),
                version: Some(ArtifactVersion::parse("v2").unwrap()),
                digest: Some(Digest::sha256([0x24; 32])),
                format: Some(ArtifactFormat::parse("ext4").unwrap()),
            }],
        ))
            .into(),
    );
    let mut conn = db.pool.acquire().await?;
    let accepted = publish_sandbox_states(&mut conn, node_id, &[suspended]).await?;
    assert_eq!(accepted.accepted, 1);
    drop(conn);

    let (status, guest_arch, execution_metadata): (String, String, serde_json::Value) =
        sqlx::query_as(
            "SELECT status, guest_arch, execution_metadata
             FROM sandbox_observed_states
             WHERE sandbox_id = $1",
        )
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(status, "suspended");
    assert_eq!(guest_arch, "aarch64");
    assert_ne!(execution_metadata, serde_json::Value::Null);

    // The retired per-component projection must stay unwritten even when the
    // publish payload carries execution components; component facts travel in
    // execution_metadata (and on snapshots) instead. The existence check keeps
    // the assertion valid across the table's separate drop migration, and
    // after the drop it guards against the projection being reintroduced with
    // a writer.
    let component_table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('sandbox_observed_components') IS NOT NULL")
            .fetch_one(&db.pool)
            .await?;
    if component_table_exists {
        let component_rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sandbox_observed_components WHERE sandbox_id = $1",
        )
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(component_rows, 0);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_sandbox_publish_does_not_enqueue_desired_state() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;

    let mut conn = db.pool.acquire().await?;
    let accepted = publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state(sandbox_id, "suspended")],
    )
    .await?;
    assert_eq!(accepted.accepted, 1);
    drop(conn);

    let observed_node_id: NodeId = sqlx::query_scalar(
        "SELECT node_id
         FROM sandbox_observed_states
         WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(observed_node_id, node_id);

    let queued: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM node_reconcile_queue
         WHERE node_id = $1",
    )
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(queued, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_spaced_version_does_not_fail_sandbox_publish_batch() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let invalid_version_sandbox_id = SandboxId::new();
    let valid_version_sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, invalid_version_sandbox_id, node_id, "started").await?;
    insert_active_sandbox(&db.pool, valid_version_sandbox_id, node_id, "started").await?;

    let mut invalid_version_state = published_sandbox_state(invalid_version_sandbox_id, "started");
    invalid_version_state.spaced_version = Some("not-semver".to_string());
    let mut valid_version_state = published_sandbox_state(valid_version_sandbox_id, "started");
    valid_version_state.spaced_version = Some("0.1.2".to_string());

    let mut conn = db.pool.acquire().await?;
    let accepted = publish_sandbox_states(
        &mut conn,
        node_id,
        &[invalid_version_state, valid_version_state],
    )
    .await?;
    assert_eq!(accepted.accepted, 2);
    drop(conn);

    let invalid_stored_version: Option<String> = sqlx::query_scalar(
        "SELECT spaced_version
         FROM sandbox_observed_states
         WHERE sandbox_id = $1",
    )
    .bind(invalid_version_sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(invalid_stored_version, None);

    let valid_stored_version: Option<String> = sqlx::query_scalar(
        "SELECT spaced_version
         FROM sandbox_observed_states
         WHERE sandbox_id = $1",
    )
    .bind(valid_version_sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(valid_stored_version.as_deref(), Some("0.1.2"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn assigned_sandbox_publish_enqueues_desired_state_once() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;

    let mut conn = db.pool.acquire().await?;
    let accepted = publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state(sandbox_id, "started")],
    )
    .await?;
    assert_eq!(accepted.accepted, 1);
    drop(conn);

    let desired_state_queued: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM node_reconcile_queue
         WHERE node_id = $1
           AND facet = $2
           AND element = $3",
    )
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(desired_state_queued, 1);

    let network_policy_queued: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM node_reconcile_queue
         WHERE node_id = $1
           AND facet = $2
           AND element = $3",
    )
    .bind(node_id)
    .bind(FACET_NETWORK_POLICY)
    .bind(NetworkPolicyTarget::sandbox(sandbox_id).as_element_string())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(network_policy_queued, 1);

    let mut conn = db.pool.acquire().await?;
    let accepted = publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state(sandbox_id, "started")],
    )
    .await?;
    assert_eq!(accepted.accepted, 1);
    drop(conn);

    let queue_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM node_reconcile_queue
         WHERE node_id = $1",
    )
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(queue_rows, 2);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_sandbox_publish_does_not_rewrite_observed_state() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;

    let mut state = published_sandbox_state(sandbox_id, "started");
    state.execution = Some((&test_execution()).into());
    state.state_changed_at = Some(timestamp(1_234_567));
    let mut conn = db.pool.acquire().await?;
    let accepted = publish_sandbox_states(&mut conn, node_id, &[state.clone()]).await?;
    assert_eq!(accepted.accepted, 1);
    assert_eq!(accepted.post_commit_state_convergence.len(), 1);
    drop(conn);

    sqlx::query(
        "UPDATE sandbox_observed_states
         SET updated_at = '2026-01-01 00:00:00+00'::timestamptz
         WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let mut conn = db.pool.acquire().await?;
    let accepted = publish_sandbox_states(&mut conn, node_id, &[state]).await?;
    assert_eq!(accepted.accepted, 1);
    assert!(accepted.post_commit_state_convergence.is_empty());
    drop(conn);

    let updated_at: String = sqlx::query_scalar(
        "SELECT updated_at::TEXT
         FROM sandbox_observed_states
         WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert!(
        updated_at.starts_with("2026-01-01 00:00:00"),
        "unchanged publish should not touch updated_at, got {updated_at}"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn sandbox_publish_without_disk_usage_preserves_last_measurement() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;

    let mut measured = published_sandbox_state(sandbox_id, "started");
    measured.disk_usage_bytes = Some(42);
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[measured]).await?;
    drop(conn);

    sqlx::query(
        "UPDATE sandbox_observed_states
         SET updated_at = '2026-01-01 00:00:00+00'::timestamptz
         WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state(sandbox_id, "started")],
    )
    .await?;
    drop(conn);

    let unchanged: (Option<i64>, String) = sqlx::query_as(
        "SELECT disk_usage_bytes, updated_at::TEXT
         FROM sandbox_observed_states
         WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(unchanged.0, Some(42));
    assert!(unchanged.1.starts_with("2026-01-01 00:00:00"));

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state(sandbox_id, "suspended")],
    )
    .await?;
    drop(conn);

    let observed: (Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT disk_usage_bytes, status
         FROM sandbox_observed_states
         WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(observed, (Some(42), Some("suspended".to_string())));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn inventory_publish_without_policy_does_not_write_network_policy_facet() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;

    let mut conn = db.pool.acquire().await?;
    let accepted = publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state(sandbox_id, "started")],
    )
    .await?;
    assert_eq!(accepted.accepted, 1);
    drop(conn);

    assert_eq!(
        network_policy_facet_count(&db.pool, node_id, sandbox_id).await?,
        0
    );
    let observed_authority: Option<String> = sqlx::query_scalar(
        "SELECT network_policy_authority
         FROM sandbox_observed_states
         WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(observed_authority, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn inventory_publish_preserves_existing_network_policy_projection() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;
    let element = NetworkPolicyTarget::sandbox(sandbox_id).as_element_string();

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state_with_network_policy(sandbox_id, 7)],
    )
    .await?;
    drop(conn);

    sqlx::query(
        "UPDATE node_facet_observed_status
         SET updated_at = '2026-01-01 00:00:00+00'::timestamptz
         WHERE node_id = $1
           AND facet = $2
           AND element = $3",
    )
    .bind(node_id)
    .bind(FACET_NETWORK_POLICY)
    .bind(&element)
    .execute(&db.pool)
    .await?;

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state(sandbox_id, "started")],
    )
    .await?;
    drop(conn);

    let (generation, updated_at, observed_authority): (Option<i64>, String, Option<String>) =
        sqlx::query_as(
            "SELECT nfos.observed_generation,
                    nfos.updated_at::TEXT,
                    sos.network_policy_authority
             FROM node_facet_observed_status nfos
             JOIN sandbox_observed_states sos ON sos.sandbox_id = $4
             WHERE nfos.node_id = $1
               AND nfos.facet = $2
               AND nfos.element = $3",
        )
        .bind(node_id)
        .bind(FACET_NETWORK_POLICY)
        .bind(&element)
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(generation, Some(7));
    assert!(
        updated_at.starts_with("2026-01-01 00:00:00"),
        "inventory publish should not rewrite policy facet updated_at, got {updated_at}"
    );
    assert_eq!(observed_authority.as_deref(), Some("cluster"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn absent_sandbox_publish_removes_network_policy_facet() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state_with_network_policy(sandbox_id, 7)],
    )
    .await?;
    drop(conn);
    assert_eq!(
        network_policy_facet_count(&db.pool, node_id, sandbox_id).await?,
        1
    );

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[absent_sandbox_state(sandbox_id)]).await?;
    drop(conn);

    assert_eq!(
        network_policy_facet_count(&db.pool, node_id, sandbox_id).await?,
        0
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn changed_sandbox_publish_updates_observed_state() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;

    let state = published_sandbox_state(sandbox_id, "started");
    let mut conn = db.pool.acquire().await?;
    let accepted = publish_sandbox_states(&mut conn, node_id, &[state]).await?;
    assert_eq!(accepted.accepted, 1);
    drop(conn);

    sqlx::query(
        "UPDATE sandbox_observed_states
         SET updated_at = '2026-01-01 00:00:00+00'::timestamptz
         WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let mut changed = published_sandbox_state(sandbox_id, "started");
    changed.content_hash = Some("sha256:changed".to_string());
    let mut conn = db.pool.acquire().await?;
    let accepted = publish_sandbox_states(&mut conn, node_id, &[changed]).await?;
    assert_eq!(accepted.accepted, 1);
    drop(conn);

    let (image_hash, updated_at): (Option<String>, String) = sqlx::query_as(
        "SELECT image_hash, updated_at::TEXT
         FROM sandbox_observed_states
         WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(image_hash.as_deref(), Some("sha256:changed"));
    assert!(
        !updated_at.starts_with("2026-01-01 00:00:00"),
        "changed publish should touch updated_at"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn sandbox_publish_claims_observed_row_from_previous_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let previous_node_id = NodeId::new();
    let active_node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, previous_node_id).await?;
    insert_ready_worker_node(&db.pool, active_node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, active_node_id, "started").await?;
    sqlx::query(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, image_hash)
         VALUES ($1, $2, 'started', 'sha256:old')",
    )
    .bind(sandbox_id)
    .bind(previous_node_id)
    .execute(&db.pool)
    .await?;

    let mut conn = db.pool.acquire().await?;
    let accepted = publish_sandbox_states(
        &mut conn,
        active_node_id,
        &[published_sandbox_state(sandbox_id, "started")],
    )
    .await?;
    assert_eq!(accepted.accepted, 1);
    drop(conn);

    let observed_node_id: NodeId = sqlx::query_scalar(
        "SELECT node_id
         FROM sandbox_observed_states
         WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(observed_node_id, active_node_id);

    let queued_refs: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM node_reconcile_queue
         WHERE node_id = $1",
    )
    .bind(active_node_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(queued_refs, 2);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_unknown_sandbox_publish_applies_losing_insert_as_update() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(4).await? else {
        return Ok(());
    };
    let fast_node_id = NodeId::new();
    let slow_node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, fast_node_id).await?;
    insert_ready_worker_node(&db.pool, slow_node_id).await?;

    sqlx::query(
        "CREATE OR REPLACE FUNCTION block_observed_insert_for_test()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             PERFORM pg_advisory_lock(4242, 2414);
             PERFORM pg_advisory_unlock(4242, 2414);
             IF NEW.image_hash = 'sha256:slow' THEN
                 PERFORM pg_sleep(0.2);
             END IF;
             RETURN NEW;
         END;
         $$",
    )
    .execute(&db.pool)
    .await?;
    sqlx::query(
        "CREATE TRIGGER block_observed_insert_for_test
         BEFORE INSERT ON sandbox_observed_states
         FOR EACH ROW
         EXECUTE FUNCTION block_observed_insert_for_test()",
    )
    .execute(&db.pool)
    .await?;

    let mut lock_conn = db.pool.acquire().await?;
    sqlx::query("SELECT pg_advisory_lock(4242, 2414)")
        .execute(&mut *lock_conn)
        .await?;

    let fast_pool = db.pool.clone();
    let fast_publish = tokio::spawn(async move {
        let mut state = published_sandbox_state(sandbox_id, "started");
        state.content_hash = Some("sha256:fast".to_string());
        let mut conn = fast_pool.acquire().await?;
        publish_sandbox_states(&mut conn, fast_node_id, &[state]).await?;
        TestResult::Ok(())
    });
    let slow_pool = db.pool.clone();
    let slow_publish = tokio::spawn(async move {
        let mut state = published_sandbox_state(sandbox_id, "started");
        state.content_hash = Some("sha256:slow".to_string());
        let mut conn = slow_pool.acquire().await?;
        publish_sandbox_states(&mut conn, slow_node_id, &[state]).await?;
        TestResult::Ok(())
    });

    let mut waiters_blocked = false;
    for _ in 0..100 {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)
             FROM pg_locks
             WHERE locktype = 'advisory'
               AND classid = 4242
               AND objid = 2414
               AND NOT granted",
        )
        .fetch_one(&db.pool)
        .await?;
        if waiting >= 2 {
            waiters_blocked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        waiters_blocked,
        "concurrent publishes did not both reach the insert trigger"
    );

    sqlx::query("SELECT pg_advisory_unlock(4242, 2414)")
        .execute(&mut *lock_conn)
        .await?;
    fast_publish.await??;
    slow_publish.await??;

    let (observed_node_id, image_hash): (NodeId, Option<String>) = sqlx::query_as(
        "SELECT node_id, image_hash
         FROM sandbox_observed_states
         WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(observed_node_id, slow_node_id);
    assert_eq!(image_hash.as_deref(), Some("sha256:slow"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_owned_suspended_sandbox_without_durable_snapshot_preserves_central_lifecycle()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, "running", Some("100.64.0.1")).await?;

    let mut conn = db.pool.acquire().await?;
    let accepted = publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state(sandbox_id, "suspended")],
    )
    .await?;

    assert_eq!(accepted.accepted, 1);
    let (sandbox_lifecycle, placement, active_node_id, observed_status) =
        sqlx::query_as::<_, (String, String, Option<NodeId>, String)>(
            "SELECT s.lifecycle, s.placement, s.active_node_id, sos.status
         FROM sandboxes s
         JOIN sandbox_observed_states sos ON sos.sandbox_id = s.id
         WHERE s.id = $1",
        )
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;

    assert_eq!(observed_status, "suspended");
    assert_eq!(sandbox_lifecycle, "started");
    assert_eq!(placement, "on_node");
    assert_eq!(active_node_id, Some(node_id));
    let active_execution_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
                AND active
                AND state = 'running'
                AND pod_ip IS NOT NULL
         )",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert!(active_execution_exists);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_owned_suspended_sandbox_with_current_durable_snapshot_projects_suspended()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, "running", Some("100.64.0.2")).await?;
    sqlx::query(
        "INSERT INTO sandbox_port_exposures (
             sandbox_id, protocol, guest_port, visibility, source,
             desired_state, apply_state, last_applied_node_id, last_error,
             last_bridge_status, last_applied_bridge_generation, last_observed_at
         )
         VALUES (
             $1, 'tcp', 9014, 'private', 'api',
             'open', 'applied', $2, 'stale error',
             'loopback_forwarded', 'bridge-generation-1', NOW()
         )",
    )
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;

    let mut conn = db.pool.acquire().await?;
    let accepted = publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state(sandbox_id, "suspended")],
    )
    .await?;

    assert_eq!(accepted.accepted, 1);
    let (sandbox_lifecycle, placement, active_node_id, target_node_id, observed_status) =
        sqlx::query_as::<_, (String, String, Option<NodeId>, Option<NodeId>, String)>(
            "SELECT s.lifecycle, s.placement, s.active_node_id, s.target_node_id, sos.status
         FROM sandboxes s
         JOIN sandbox_observed_states sos ON sos.sandbox_id = s.id
         WHERE s.id = $1",
        )
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;

    assert_eq!(observed_status, "suspended");
    assert_eq!(sandbox_lifecycle, "suspended");
    assert_eq!(placement, "unplaced");
    assert_eq!(active_node_id, None);
    assert_eq!(target_node_id, Some(node_id));
    let (execution_active, execution_state, execution_pod_ip) =
        sqlx::query_as::<_, (bool, String, Option<String>)>(
            "SELECT active, state, pod_ip::TEXT
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
              ORDER BY generation DESC
              LIMIT 1",
        )
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
    assert!(!execution_active);
    assert_eq!(execution_state, "suspended");
    assert_eq!(execution_pod_ip, None);
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
    ) = sqlx::query_as(
        "SELECT apply_state,
                last_applied_node_id,
                last_error,
                last_bridge_status,
                last_applied_bridge_generation,
                last_observed_at IS NOT NULL
           FROM sandbox_port_exposures
          WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(apply_state, "pending");
    assert_eq!(last_applied_node_id, None);
    assert_eq!(last_error, None);
    assert_eq!(last_bridge_status, None);
    assert_eq!(last_applied_bridge_generation, None);
    assert!(!has_last_observed_at);
    Ok(())
}

/// The node-published suspend finalization retires the warm-pool slot
/// receipt in the same transaction: once `suspended` commits the claimed pod
/// is gone and the receipt is dead weight that would otherwise wait for the
/// warm-pool reconciler's fossil drain. A `cleanup_pending` slot still owes
/// a pod kill, so the same publish must leave it untouched.
#[tokio::test(flavor = "current_thread")]
async fn publishing_owned_suspended_sandbox_retires_warm_pool_receipt() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let retired_sandbox_id = SandboxId::new();
    let cleanup_pending_sandbox_id = SandboxId::new();
    for (sandbox_id, pod_ip) in [
        (retired_sandbox_id, "100.64.0.2"),
        (cleanup_pending_sandbox_id, "100.64.0.3"),
    ] {
        insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;
        insert_k8s_execution(&db.pool, sandbox_id, 1, "running", Some(pod_ip)).await?;
        insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;
    }

    insert_pool_slot_receipt(&db.pool, retired_sandbox_id, "bound").await?;
    insert_pool_slot_receipt(&db.pool, cleanup_pending_sandbox_id, "cleanup_pending").await?;

    let mut conn = db.pool.acquire().await?;
    let accepted = publish_sandbox_states(
        &mut conn,
        node_id,
        &[
            published_sandbox_state(retired_sandbox_id, "suspended"),
            published_sandbox_state(cleanup_pending_sandbox_id, "suspended"),
        ],
    )
    .await?;
    assert_eq!(accepted.accepted, 2);

    let lifecycles: Vec<String> = sqlx::query_scalar(
        "SELECT lifecycle
           FROM sandboxes
          WHERE id = ANY($1)",
    )
    .bind(vec![retired_sandbox_id, cleanup_pending_sandbox_id])
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(lifecycles, vec!["suspended".to_string(); 2]);
    let receipts: Vec<(SandboxId, String)> = sqlx::query_as(
        "SELECT reserved_sandbox_id, state
           FROM kubernetes_pool_slots
          WHERE reserved_sandbox_id = ANY($1)",
    )
    .bind(vec![retired_sandbox_id, cleanup_pending_sandbox_id])
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(
        receipts,
        vec![(cleanup_pending_sandbox_id, "cleanup_pending".to_string())]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_suspended_node_state_does_not_finalize_api_owned_suspend() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "suspending").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, "running", Some("100.64.0.3")).await?;
    insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state(sandbox_id, "suspended")],
    )
    .await?;
    drop(conn);

    // A crash-suspend publish without an accompanying durable snapshot must
    // not finalize: lifecycle stays `suspending` and the execution stays
    // `active`/`running`.
    insta::assert_yaml_snapshot!(sandbox_execution_row(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_suspended_node_state_cannot_pin_a_deleting_snapshot() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "suspending").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, "running", Some("100.64.0.30")).await?;
    let snapshot_id = insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;
    let local_artifact_id = snapshot_local_artifact_id(&db.pool, snapshot_id).await?;
    sqlx::query("UPDATE snapshots SET state = 'deleting' WHERE id = $1")
        .bind(snapshot_id)
        .execute(&db.pool)
        .await?;
    let mut published = published_sandbox_state(sandbox_id, "suspended");
    published.execution_generation = Some(1);
    published.pod_uid = Some("pod-uid-1".to_string());
    published.current_local_artifact_id = Some(local_artifact_id.to_string());
    published.current_local_artifact_kind =
        Some(pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32);
    published.current_local_artifact_origin =
        Some(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32);

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[published]).await?;
    drop(conn);

    let (lifecycle, execution_active, execution_state, current_artifact): (
        String,
        bool,
        String,
        Option<LocalArtifactId>,
    ) = sqlx::query_as(
        "SELECT s.lifecycle, e.active, e.state, s.current_local_artifact_id
           FROM sandboxes s
           JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
          WHERE s.id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(lifecycle, "suspending");
    assert!(execution_active);
    assert_eq!(execution_state, "running");
    assert_eq!(current_artifact, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_suspended_node_state_accepts_a_ready_copy_of_a_deleting_artifact() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "suspending").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, "running", Some("100.64.0.30")).await?;
    let ready_snapshot_id = insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;
    let local_artifact_id = snapshot_local_artifact_id(&db.pool, ready_snapshot_id).await?;
    sqlx::query(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, source_node_id, org_id,
             local_artifact_id, artifact_kind, artifact_origin
         )
         SELECT $2, sandbox_id, type, 'deleting', source_node_id, org_id,
                local_artifact_id, artifact_kind, artifact_origin
           FROM snapshots
          WHERE id = $1",
    )
    .bind(ready_snapshot_id)
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    let mut published = published_sandbox_state(sandbox_id, "suspended");
    published.execution_generation = Some(1);
    published.pod_uid = Some("pod-uid-1".to_string());
    published.current_local_artifact_id = Some(local_artifact_id.to_string());
    published.current_local_artifact_kind =
        Some(pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32);
    published.current_local_artifact_origin =
        Some(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32);

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[published]).await?;
    drop(conn);

    let row = sandbox_execution_artifact_row(&db.pool, sandbox_id).await?;
    assert_eq!(row.lifecycle, "suspended");
    assert!(!row.active);
    assert_eq!(row.state, "suspended");
    assert_eq!(
        row.current_local_artifact_id,
        Some(LocalArtifactId::from_uuid(local_artifact_id))
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_suspended_node_state_with_mismatched_execution_identity_preserves_suspend()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "suspending").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, "running", Some("100.64.0.31")).await?;
    let snapshot_id = insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;
    let local_artifact_id = snapshot_local_artifact_id(&db.pool, snapshot_id).await?;
    let mut published = published_sandbox_state(sandbox_id, "suspended");
    published.execution_generation = Some(2);
    published.pod_uid = Some("pod-uid-2".to_string());
    published.current_local_artifact_id = Some(local_artifact_id.to_string());
    published.current_local_artifact_kind =
        Some(pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32);
    published.current_local_artifact_origin =
        Some(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32);

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[published]).await?;
    drop(conn);

    // A mismatched execution identity must not finalize: lifecycle stays
    // `suspending` and the execution stays owned (`active`).
    insta::assert_yaml_snapshot!(sandbox_execution_row(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_local_suspend_handoff_records_only_kubernetes_rows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let mut published = Vec::new();
    let mut sandboxes = Vec::new();
    for managed_by in ["k8s", "space"] {
        let sandbox_id = SandboxId::new();
        insert_active_sandbox_with_managed_by(&db.pool, sandbox_id, node_id, "started", managed_by)
            .await?;
        insert_k8s_execution(&db.pool, sandbox_id, 1, "running", None).await?;
        let mut state = published_sandbox_state(sandbox_id, "suspended");
        state.execution_generation = Some(1);
        state.pod_uid = Some("pod-uid-1".to_string());
        state.current_local_artifact_id = Some(Uuid::now_v7().to_string());
        state.current_local_artifact_kind =
            Some(pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32);
        state.current_local_artifact_origin =
            Some(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32);
        published.push(state);
        sandboxes.push((sandbox_id, managed_by == "k8s"));
    }

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &published).await?;
    drop(conn);

    for (sandbox_id, recorded) in sandboxes {
        let handoff: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM node_facet_observed_status
                  WHERE node_id = $1 AND facet = $2 AND element = $3
                    AND observed_payload ? $4
             )"
        ))
        .bind(node_id)
        .bind(FACET_SANDBOX_DESIRED_STATE)
        .bind(sandbox_id.to_string())
        .bind(SANDBOX_LOCAL_SUSPEND_KEY)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(handoff, recorded, "sandbox {sandbox_id}");
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_local_suspend_handoff_clears_once_node_reports_running() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, "running", None).await?;
    let mut suspended = published_sandbox_state(sandbox_id, "suspended");
    suspended.execution_generation = Some(1);
    suspended.pod_uid = Some("pod-uid-1".to_string());
    suspended.current_local_artifact_id = Some(Uuid::now_v7().to_string());
    suspended.current_local_artifact_kind =
        Some(pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32);
    suspended.current_local_artifact_origin =
        Some(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32);
    let mut running = published_sandbox_state(sandbox_id, "running");
    running.execution_generation = Some(1);
    running.pod_uid = Some("pod-uid-1".to_string());

    let mut handoffs = Vec::new();
    for published in [suspended, running] {
        let mut conn = db.pool.acquire().await?;
        publish_sandbox_states(&mut conn, node_id, &[published]).await?;
        drop(conn);
        let handoff: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM node_facet_observed_status
                  WHERE node_id = $1 AND facet = $2 AND element = $3
                    AND observed_payload ? $4
             )"
        ))
        .bind(node_id)
        .bind(FACET_SANDBOX_DESIRED_STATE)
        .bind(sandbox_id.to_string())
        .bind(SANDBOX_LOCAL_SUSPEND_KEY)
        .fetch_one(&db.pool)
        .await?;
        handoffs.push(handoff);
    }
    assert_eq!(handoffs, [true, false]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_exact_suspend_recovers_execution_with_cleared_pod_uid() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "suspending").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, "running", Some("100.64.0.33")).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET pod_uid = NULL
          WHERE sandbox_id = $1 AND generation = 1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let snapshot_id = insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;
    let local_artifact_id = snapshot_local_artifact_id(&db.pool, snapshot_id).await?;
    let mut published = published_sandbox_state(sandbox_id, "suspended");
    published.execution_generation = Some(1);
    published.pod_uid = Some("pod-uid-1".to_string());
    published.current_local_artifact_id = Some(local_artifact_id.to_string());
    published.current_local_artifact_kind =
        Some(pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32);
    published.current_local_artifact_origin =
        Some(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32);

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[published]).await?;
    drop(conn);

    // The exact-generation match recovers and finalizes the execution:
    // `suspended`/inactive/`suspended` state, with the (now cleared) pod uid
    // no longer blocking convergence.
    insta::assert_yaml_snapshot!(sandbox_execution_row(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_suspended_node_state_finalizes_committed_suspend() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "suspending").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, "suspended", Some("100.64.0.4")).await?;
    let snapshot_id = insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;
    pin_suspend_snapshot_artifact(&db.pool, sandbox_id, snapshot_id).await?;
    let local_artifact_id = snapshot_local_artifact_id(&db.pool, snapshot_id).await?;
    let mut published = published_sandbox_state(sandbox_id, "suspended");
    published.current_local_artifact_id = Some(local_artifact_id.to_string());
    published.current_local_artifact_kind =
        Some(pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32);
    published.current_local_artifact_origin =
        Some(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32);

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[published]).await?;
    drop(conn);

    // A committed suspend must finalize AND release placement: `suspended`,
    // `unplaced`, no `active_node_id`, execution inactive/`suspended`. None
    // of these columns are run-varying (the finalization clears the node),
    // so a plain snapshot pins every value.
    insta::assert_yaml_snapshot!(sandbox_execution_placement_row(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_suspended_node_state_with_mismatched_artifact_preserves_suspend() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    for (kind, origin) in [
        (
            pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
            pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Stop,
        ),
        (
            pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk,
            pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
        ),
    ] {
        let sandbox_id = SandboxId::new();
        insert_active_sandbox(&db.pool, sandbox_id, node_id, "suspending").await?;
        insert_k8s_execution(&db.pool, sandbox_id, 1, "suspended", Some("100.64.0.5")).await?;
        let snapshot_id = insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;
        pin_suspend_snapshot_artifact(&db.pool, sandbox_id, snapshot_id).await?;
        let local_artifact_id = snapshot_local_artifact_id(&db.pool, snapshot_id).await?;
        let mut published = published_sandbox_state(sandbox_id, "suspended");
        published.current_local_artifact_id = Some(local_artifact_id.to_string());
        published.current_local_artifact_kind = Some(kind as i32);
        published.current_local_artifact_origin = Some(origin as i32);

        let mut conn = db.pool.acquire().await?;
        publish_sandbox_states(&mut conn, node_id, &[published]).await?;
        drop(conn);

        // A mismatched artifact (wrong kind or origin) must preserve the
        // in-flight suspend: `suspending`/still-`active` execution, for
        // both mismatch shapes the loop drives.
        insta::assert_yaml_snapshot!(sandbox_execution_row(&db.pool, sandbox_id).await?);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_suspended_node_state_without_committed_artifact_preserves_suspend() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "suspending").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, "suspended", Some("100.64.0.5")).await?;
    insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state(sandbox_id, "suspended")],
    )
    .await?;
    drop(conn);

    // Without a committed artifact, the suspend cannot finalize: lifecycle
    // stays `suspending` with the execution still `active`.
    insta::assert_yaml_snapshot!(sandbox_execution_row(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_stopped_node_state_does_not_override_api_owned_suspend() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "suspending").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, "running", Some("100.64.0.6")).await?;

    let mut conn = db.pool.acquire().await?;
    let outcome = publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state(sandbox_id, "stopped")],
    )
    .await?;
    drop(conn);

    assert_eq!(outcome.accepted, 1);
    // A `stopped` node publish must not override an API-owned suspend:
    // lifecycle stays `suspending` and the execution stays `active`/`running`.
    insta::assert_yaml_snapshot!(sandbox_execution_row(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_owned_stopped_sandbox_promotes_central_lifecycle() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox_with_managed_by(&db.pool, sandbox_id, node_id, "suspended", "space")
        .await?;

    let mut conn = db.pool.acquire().await?;
    let accepted = publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state(sandbox_id, "stopped")],
    )
    .await?;

    assert_eq!(accepted.accepted, 1);
    assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "stopped");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_absent_stopped_space_sandbox_deletes_central_row() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    // Otherwise identical, but Kubernetes-managed: absence does not delete it.
    let k8s_sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox_with_managed_by(&db.pool, sandbox_id, node_id, "stopped", "space")
        .await?;
    insert_active_sandbox_with_managed_by(&db.pool, k8s_sandbox_id, node_id, "stopped", "k8s")
        .await?;

    insert_pool_slot_receipt(&db.pool, sandbox_id, "bound").await?;
    insert_pool_slot_receipt(&db.pool, k8s_sandbox_id, "bound").await?;

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(
        &mut conn,
        node_id,
        &[
            published_sandbox_state(sandbox_id, "stopped"),
            published_sandbox_state(k8s_sandbox_id, "stopped"),
        ],
    )
    .await?;
    let accepted = publish_sandbox_states(
        &mut conn,
        node_id,
        &[
            absent_sandbox_state(sandbox_id),
            absent_sandbox_state(k8s_sandbox_id),
        ],
    )
    .await?;

    assert_eq!(accepted.accepted, 2);
    assert!(!sandbox_exists(&db.pool, sandbox_id).await?);
    assert_eq!(pool_slot_receipt_state(&db.pool, sandbox_id).await?, None);
    assert_eq!(
        sandbox_lifecycle(&db.pool, k8s_sandbox_id).await?,
        "stopped"
    );
    assert_eq!(
        pool_slot_receipt_state(&db.pool, k8s_sandbox_id).await?,
        Some("bound".to_owned())
    );
    Ok(())
}

/// A stopped sandbox released from its node once its disk was durable must
/// survive that node reporting it absent, and keep the storage location that
/// makes it restorable. A stopped sandbox with no durable copy is still
/// deleted, so the exemption can't be mistaken for the delete not running.
#[tokio::test(flavor = "current_thread")]
async fn publishing_absent_released_stopped_sandbox_keeps_it_and_its_artifact() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let released_id = SandboxId::new();
    let lost_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    for sandbox_id in [released_id, lost_id] {
        insert_active_sandbox_with_managed_by(&db.pool, sandbox_id, node_id, "stopped", "space")
            .await?;
    }
    publish_ready_disk_artifact(
        &db.pool,
        released_id,
        LocalArtifactId::new(),
        &format!("s3://space-test/{released_id}/disk"),
    )
    .await?;

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(
        &mut conn,
        node_id,
        &[
            absent_sandbox_state(released_id),
            absent_sandbox_state(lost_id),
        ],
    )
    .await?;
    drop(conn);

    assert_eq!(sandbox_lifecycle(&db.pool, released_id).await?, "stopped");
    let live_locations: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
           FROM snapshot_storage_locations ssl
           JOIN snapshots sn ON sn.id = ssl.snapshot_id
          WHERE sn.sandbox_id = $1
            AND ssl.orphaned_at IS NULL",
    )
    .bind(released_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(live_locations, 1);
    assert!(!sandbox_exists(&db.pool, lost_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publish_sandbox_state_updates_central_last_used_at() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let original_updated_at = time::OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;
    set_sandbox_updated_at_for_test(&db.pool, sandbox_id, original_updated_at).await?;

    let mut state = published_sandbox_state(sandbox_id, "started");
    state.last_used_at = Some(timestamp(1_800_000_000));

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[state]).await?;
    drop(conn);

    let last_used_at = sandbox_last_used_at(&db.pool, sandbox_id)
        .await?
        .expect("last_used_at should be populated");
    assert_eq!(last_used_at.unix_timestamp(), 1_800_000_000);
    let updated_at = sandbox_updated_at(&db.pool, sandbox_id).await?;
    assert_eq!(
        updated_at.unix_timestamp(),
        original_updated_at.unix_timestamp(),
        "publishing access-only last_used_at must not make the sandbox look structurally updated"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publish_sandbox_state_updates_activity_clocks_without_touching_updated_at() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let original_updated_at = time::OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;
    set_sandbox_updated_at_for_test(&db.pool, sandbox_id, original_updated_at).await?;

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state(sandbox_id, "started")],
    )
    .await?;
    drop(conn);
    assert_eq!(
        sandbox_activity_at(&db.pool, sandbox_id).await?,
        (None, None, None),
        "a report with no activity must leave all three clocks unset"
    );

    let mut state = published_sandbox_state(sandbox_id, "started");
    state.last_access_at = Some(timestamp(1_800_000_000));
    state.last_command_at = Some(timestamp(1_800_000_120));

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[state]).await?;
    drop(conn);

    let (last_used_at, last_access_at, last_command_at) =
        sandbox_activity_at(&db.pool, sandbox_id).await?;
    assert_eq!(
        last_used_at
            .expect("last_used_at should be populated")
            .unix_timestamp(),
        1_800_000_120
    );
    assert_eq!(
        last_access_at
            .expect("last_access_at should be populated")
            .unix_timestamp(),
        1_800_000_000
    );
    assert_eq!(
        last_command_at
            .expect("last_command_at should be populated")
            .unix_timestamp(),
        1_800_000_120
    );
    let foreign_node = NodeId::new();
    insert_ready_worker_node(&db.pool, foreign_node).await?;
    let mut foreign_state = published_sandbox_state(sandbox_id, "started");
    foreign_state.last_used_at = Some(timestamp(1_900_000_000));
    foreign_state.last_access_at = Some(timestamp(1_900_000_000));
    foreign_state.last_command_at = Some(timestamp(1_900_000_000));
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, foreign_node, &[foreign_state]).await?;
    drop(conn);
    assert_eq!(
        sandbox_activity_at(&db.pool, sandbox_id).await?,
        (last_used_at, last_access_at, last_command_at),
        "a foreign publisher cannot advance any of the sandbox's activity clocks"
    );

    let updated_at = sandbox_updated_at(&db.pool, sandbox_id).await?;
    assert_eq!(
        updated_at.unix_timestamp(),
        original_updated_at.unix_timestamp(),
        "publishing activity clocks must not make the sandbox look structurally updated"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publish_sandbox_state_throttles_central_last_used_at() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;

    let mut initial = published_sandbox_state(sandbox_id, "started");
    initial.last_used_at = Some(timestamp(1_800_000_000));
    let mut fresh = published_sandbox_state(sandbox_id, "started");
    fresh.last_used_at = Some(timestamp(1_800_000_060));
    let mut newer = published_sandbox_state(sandbox_id, "started");
    newer.last_used_at = Some(timestamp(1_800_000_061));
    let mut older = published_sandbox_state(sandbox_id, "started");
    older.last_used_at = Some(timestamp(1_799_999_000));

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[initial]).await?;
    publish_sandbox_states(&mut conn, node_id, &[fresh]).await?;
    publish_sandbox_states(&mut conn, node_id, &[newer]).await?;
    publish_sandbox_states(&mut conn, node_id, &[older]).await?;
    drop(conn);

    let last_used_at = sandbox_last_used_at(&db.pool, sandbox_id)
        .await?
        .expect("last_used_at should be populated");
    assert_eq!(last_used_at.unix_timestamp(), 1_800_000_061);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_owned_failed_sandbox_marks_central_lifecycle_failed() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;

    let mut conn = db.pool.acquire().await?;
    let accepted =
        publish_sandbox_states(&mut conn, node_id, &[failed_sandbox_state(sandbox_id)]).await?;

    assert_eq!(accepted.accepted, 1);
    let (sandbox_lifecycle, observed_status, failure_reason) =
        sqlx::query_as::<_, (String, String, Option<String>)>(
            "SELECT s.lifecycle, sos.status, sos.failure_reason
             FROM sandboxes s
             JOIN sandbox_observed_states sos ON sos.sandbox_id = s.id
             WHERE s.id = $1",
        )
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;

    assert_eq!(observed_status, "failed");
    assert_eq!(
        failure_reason.as_deref(),
        Some("guest bridge health probe timed out")
    );
    assert_eq!(sandbox_lifecycle, "failed");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_owned_quarantined_sandbox_marks_central_lifecycle_and_records_reason()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;

    let mut conn = db.pool.acquire().await?;
    let accepted =
        publish_sandbox_states(&mut conn, node_id, &[quarantined_sandbox_state(sandbox_id)])
            .await?;

    assert_eq!(accepted.accepted, 1);
    let (
        sandbox_lifecycle,
        observed_status,
        current_reason,
        current_source,
        current_reason_code,
        current_next_action,
        history_action,
        history_reason,
    ) = sqlx::query_as::<
        _,
        (
            String,
            String,
            Option<String>,
            String,
            String,
            String,
            String,
            Option<String>,
        ),
    >(
        "SELECT s.lifecycle,
                sos.status,
                qs.reason_message,
                qs.source,
                qs.reason_code,
                qs.next_action,
                qh.action,
                qh.message
         FROM sandboxes s
         JOIN sandbox_observed_states sos ON sos.sandbox_id = s.id
         JOIN sandbox_quarantine_state qs ON qs.sandbox_id = s.id
         JOIN sandbox_quarantine_history qh ON qh.sandbox_id = s.id
         WHERE s.id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;

    assert_eq!(observed_status, "quarantined");
    assert_eq!(sandbox_lifecycle, "quarantined");
    assert_eq!(
        current_reason.as_deref(),
        Some("sync path found conflicting local artifacts")
    );
    assert_eq!(
        current_reason_code,
        SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed.as_str()
    );
    assert_eq!(current_source, SandboxQuarantineSource::SbxManager.as_str());
    assert_eq!(
        current_next_action,
        SandboxQuarantineNextAction::RequiresOperatorAttention.as_str()
    );
    assert_eq!(
        history_action,
        SandboxQuarantineHistoryAction::EnteredQuarantine.as_str()
    );
    assert_eq!(
        history_reason.as_deref(),
        Some("sync path found conflicting local artifacts")
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_unknown_quarantine_reason_downgrades_without_rejecting_batch() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;

    let mut state = quarantined_sandbox_state(sandbox_id);
    state.quarantine_reason_code = Some("reason_added_by_newer_node".to_string());
    let mut conn = db.pool.acquire().await?;
    let accepted = publish_sandbox_states(&mut conn, node_id, &[state]).await?;

    assert_eq!(accepted.accepted, 1);
    let (lifecycle, source, reason_code) = sqlx::query_as::<_, (String, String, String)>(
        "SELECT s.lifecycle, qs.source, qs.reason_code
           FROM sandboxes s
           JOIN sandbox_quarantine_state qs ON qs.sandbox_id = s.id
          WHERE s.id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(lifecycle, "quarantined");
    assert_eq!(source, SandboxQuarantineSource::NodeStatePublish.as_str());
    assert_eq!(
        reason_code,
        SandboxQuarantineReasonCode::NodePublishedQuarantined.as_str()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn node_inventory_recovers_pause_failed_quarantine_to_reported_lifecycle() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;

    for target_state in ["started", "paused"] {
        let sandbox_id = SandboxId::new();
        insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;
        let mut quarantined = quarantined_sandbox_state(sandbox_id);
        quarantined.quarantine_reason_code =
            Some(SandboxQuarantineReasonCode::PauseFailed.to_string());
        let mut conn = db.pool.acquire().await?;
        publish_sandbox_states(&mut conn, node_id, &[quarantined]).await?;
        drop(conn);

        let mut tx = db.pool.begin().await?;
        let outcome = publish_sandbox_states(
            &mut tx,
            node_id,
            &[published_sandbox_state(sandbox_id, target_state)],
        )
        .await?;
        tx.commit().await?;
        assert_eq!(outcome.post_commit_events.len(), 1);

        let mut tx = db.pool.begin().await?;
        let replay = publish_sandbox_states(
            &mut tx,
            node_id,
            &[published_sandbox_state(sandbox_id, target_state)],
        )
        .await?;
        tx.commit().await?;
        assert!(replay.post_commit_events.is_empty());

        let recovered = sqlx::query_as::<
            _,
            (
                String,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
                bool,
            ),
        >(sql!(
            "SELECT s.lifecycle,
                    observed.failure_reason,
                    history.action,
                    history.source,
                    history.reason_code,
                    history.next_action,
                    NOT EXISTS (
                        SELECT 1
                          FROM sandbox_quarantine_state
                         WHERE sandbox_id = s.id
                    )
               FROM sandboxes s
               JOIN sandbox_observed_states observed ON observed.sandbox_id = s.id
               LEFT JOIN sandbox_quarantine_history history
                 ON history.sandbox_id = s.id
                AND history.action IN ($2, $3)
              WHERE s.id = $1"
        ))
        .bind(sandbox_id)
        .bind(SandboxQuarantineHistoryAction::RecoveredToStarted.as_str())
        .bind(SandboxQuarantineHistoryAction::RecoveredToPaused.as_str())
        .fetch_one(&db.pool)
        .await?;
        let expected_action = match target_state {
            "started" => SandboxQuarantineHistoryAction::RecoveredToStarted.as_str(),
            "paused" => SandboxQuarantineHistoryAction::RecoveredToPaused.as_str(),
            _ => unreachable!(),
        };
        assert_eq!(
            recovered,
            (
                target_state.to_string(),
                None,
                Some(expected_action.to_string()),
                Some(SandboxQuarantineSource::NodeStatePublish.to_string()),
                Some(SandboxQuarantineReasonCode::PauseFailed.to_string()),
                None,
                true,
            )
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn node_inventory_only_recovers_sbx_manager_pause_failed_quarantines() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let other_node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_ready_worker_node(&db.pool, other_node_id).await?;

    for (source, reason_code, owner_node_id, reported_status) in [
        (
            SandboxQuarantineSource::SbxManager,
            SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed,
            node_id,
            "started",
        ),
        (
            SandboxQuarantineSource::NodeStatePublish,
            SandboxQuarantineReasonCode::PauseFailed,
            node_id,
            "started",
        ),
        (
            SandboxQuarantineSource::SbxManager,
            SandboxQuarantineReasonCode::PauseFailed,
            other_node_id,
            "started",
        ),
    ] {
        let sandbox_id = SandboxId::new();
        insert_active_sandbox(&db.pool, sandbox_id, owner_node_id, "started").await?;
        let mut quarantined = quarantined_sandbox_state(sandbox_id);
        quarantined.quarantine_reason_code = Some(reason_code.to_string());
        let mut conn = db.pool.acquire().await?;
        publish_sandbox_states(&mut conn, owner_node_id, &[quarantined]).await?;
        sqlx::query(sql!(
            "UPDATE sandbox_quarantine_state
                SET source = $2
              WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .bind(source.as_str())
        .execute(&mut *conn)
        .await?;
        drop(conn);
        let mut tx = db.pool.begin().await?;
        let outcome = publish_sandbox_states(
            &mut tx,
            node_id,
            &[published_sandbox_state(sandbox_id, reported_status)],
        )
        .await?;
        tx.commit().await?;
        assert!(outcome.post_commit_events.is_empty());

        assert_eq!(
            sandbox_lifecycle(&db.pool, sandbox_id).await?,
            "quarantined"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn recovery_receipt_converges_each_safe_pre_resume_failure_to_suspended() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    for reason_code in [
        SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
        SandboxQuarantineReasonCode::RestorePrepareFailed,
        SandboxQuarantineReasonCode::RestoreSpacedHostLaunchFailed,
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
    ] {
        let sandbox_id = SandboxId::new();
        insert_active_sandbox(&db.pool, sandbox_id, node_id, "restoring").await?;
        insert_k8s_execution(&db.pool, sandbox_id, 7, "running", Some("100.64.0.7")).await?;
        let snapshot_id = insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;
        let local_artifact_id = snapshot_local_artifact_id(&db.pool, snapshot_id).await?;
        if reason_code != SandboxQuarantineReasonCode::OrphanedSandboxRowMissing {
            pin_suspend_snapshot_artifact(&db.pool, sandbox_id, snapshot_id).await?;
        }

        let mut quarantined = quarantined_sandbox_state(sandbox_id);
        quarantined.quarantine_reason_code = Some(reason_code.to_string());
        let mut conn = db.pool.acquire().await?;
        publish_sandbox_states(&mut conn, node_id, &[quarantined]).await?;
        let receipt_id = sqlx::query_scalar::<_, Uuid>(
            "SELECT quarantine_id FROM sandbox_quarantine_state WHERE sandbox_id = $1",
        )
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        publish_sandbox_states(
            &mut conn,
            node_id,
            &[recovered_suspended_state(
                sandbox_id,
                receipt_id,
                7,
                None,
                reason_code,
                LocalArtifactId::from_uuid(local_artifact_id),
            )],
        )
        .await?;

        assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "suspended");
        let quarantine_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM sandbox_quarantine_state WHERE sandbox_id = $1)",
        )
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert!(!quarantine_exists);
        let history_action = sqlx::query_scalar::<_, String>(
            "SELECT action FROM sandbox_quarantine_history WHERE id = $1",
        )
        .bind(receipt_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(
            history_action,
            SandboxQuarantineHistoryAction::RecoveredToSuspended.as_str()
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn recovery_plan_binds_node_episode_generation_and_exact_artifact() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "restoring").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 11, "running", Some("100.64.0.11")).await?;
    let snapshot_id = insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;
    let local_artifact_id = snapshot_local_artifact_id(&db.pool, snapshot_id).await?;
    pin_suspend_snapshot_artifact(&db.pool, sandbox_id, snapshot_id).await?;

    let reason_code = SandboxQuarantineReasonCode::RestorePrepareFailed;
    let mut quarantined = quarantined_sandbox_state(sandbox_id);
    quarantined.quarantine_reason_code = Some(reason_code.to_string());
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[quarantined]).await?;
    let quarantine_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT quarantine_id FROM sandbox_quarantine_state WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;

    let service = ClusterApiService::new(db.pool.clone());
    let plan = service
        .get_quarantine_recovery_plan(quarantine_recovery_plan_request(node_id, sandbox_id))
        .await?
        .into_inner();
    let source = plan.suspend_snapshot.expect("suspend snapshot plan");

    assert_eq!(plan.recovery_id, quarantine_id.to_string());
    assert_eq!(plan.execution_generation, 11);
    assert_eq!(plan.reason_code, reason_code.as_str());
    assert_eq!(source.snapshot_id, snapshot_id.to_string());
    assert_eq!(source.local_artifact_id, local_artifact_id.to_string());

    sqlx::query(
        "UPDATE sandboxes
            SET current_local_artifact_id = NULL,
                current_local_artifact_kind = NULL,
                current_local_artifact_origin = NULL,
                last_command_at = NOW()
          WHERE id = $1",
    )
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let orphan_reason = SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    sqlx::query("UPDATE sandbox_quarantine_state SET reason_code = $1 WHERE sandbox_id = $2")
        .bind(orphan_reason.as_str())
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
    let stale = service
        .get_quarantine_recovery_plan(quarantine_recovery_plan_request(node_id, sandbox_id))
        .await
        .expect_err("fallback must not rewind activity newer than the snapshot");
    assert_eq!(stale.code(), tonic::Code::FailedPrecondition);
    sqlx::query("UPDATE sandboxes SET last_command_at = NULL WHERE id = $1")
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
    let fallback = service
        .get_quarantine_recovery_plan(quarantine_recovery_plan_request(node_id, sandbox_id))
        .await?
        .into_inner();
    let fallback_source = fallback.suspend_snapshot.expect("fallback snapshot plan");
    assert_eq!(fallback.recovery_id, quarantine_id.to_string());
    assert_eq!(fallback.reason_code, orphan_reason.as_str());
    assert_eq!(fallback_source.snapshot_id, snapshot_id.to_string());
    assert_eq!(
        fallback_source.local_artifact_id,
        local_artifact_id.to_string()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn recovery_plan_preserves_known_rejection_distinctions() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let other_node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_ready_worker_node(&db.pool, other_node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "restoring").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 11, "running", Some("100.64.0.11")).await?;
    let snapshot_id = insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;
    pin_suspend_snapshot_artifact(&db.pool, sandbox_id, snapshot_id).await?;
    let service = ClusterApiService::new(db.pool.clone());

    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    let no_quarantine = service
        .get_quarantine_recovery_plan(quarantine_recovery_plan_request(node_id, sandbox_id))
        .await
        .expect_err("recovery requires an active central quarantine");
    assert_eq!(no_quarantine.code(), tonic::Code::FailedPrecondition);
    assert!(
        no_quarantine
            .message()
            .contains("has no active quarantine on this node")
    );
    assert_quarantine_recovery_rejection_cause(counters.finish(), "no_active_quarantine");

    let mut quarantined = quarantined_sandbox_state(sandbox_id);
    quarantined.quarantine_reason_code =
        Some(SandboxQuarantineReasonCode::RestorePrepareFailed.to_string());
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[quarantined]).await?;

    sqlx::query("UPDATE sandboxes SET active_node_id = NULL WHERE id = $1")
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    let no_active_node = service
        .get_quarantine_recovery_plan(quarantine_recovery_plan_request(node_id, sandbox_id))
        .await
        .expect_err("an unplaced sandbox must not receive recovery authority");
    assert_eq!(no_active_node.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        no_active_node.message(),
        no_quarantine.message(),
        "an unplaced sandbox must be indistinguishable on the wire from one with no \
         quarantine at all; only the rejection-cause counter may tell them apart"
    );
    assert_quarantine_recovery_rejection_cause(counters.finish(), "no_active_node");
    sqlx::query("UPDATE sandboxes SET active_node_id = $1 WHERE id = $2")
        .bind(node_id)
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;

    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    let wrong_node = service
        .get_quarantine_recovery_plan(quarantine_recovery_plan_request(other_node_id, sandbox_id))
        .await
        .expect_err("a different node must not receive recovery authority");
    assert_eq!(wrong_node.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        wrong_node.message(),
        no_quarantine.message(),
        "the lookup is not scoped to the requesting node, so a node probing sandbox \
         ids must not be able to tell a real quarantine on another node from a \
         sandbox with no quarantine; only the cause counter may"
    );
    assert_quarantine_recovery_rejection_cause(counters.finish(), "wrong_node");

    sqlx::query("UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1")
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    let no_execution = service
        .get_quarantine_recovery_plan(quarantine_recovery_plan_request(node_id, sandbox_id))
        .await
        .expect_err("recovery requires a current execution generation fence");
    assert_eq!(no_execution.code(), tonic::Code::FailedPrecondition);
    assert!(no_execution.message().contains("has no active execution"));
    assert_quarantine_recovery_rejection_cause(counters.finish(), "no_active_execution");

    // Composed: no active execution *and* a foreign caller. Every rejection after
    // the ownership check may carry its own message, and that is only safe because
    // ownership is checked first — so a foreign node must still see the uniform
    // message here, not learn that this sandbox exists and lost its execution.
    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    let wrong_node_without_execution = service
        .get_quarantine_recovery_plan(quarantine_recovery_plan_request(other_node_id, sandbox_id))
        .await
        .expect_err("a different node must not receive recovery authority");
    assert_eq!(
        wrong_node_without_execution.message(),
        no_quarantine.message(),
        "the ownership check has to run before every rejection that carries its own \
         message, or those messages become the oracle the uniform one closes"
    );
    assert_quarantine_recovery_rejection_cause(counters.finish(), "wrong_node");

    sqlx::query("UPDATE sandbox_k8s_executions SET active = TRUE WHERE sandbox_id = $1")
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
    sqlx::query(
        "UPDATE sandbox_quarantine_state SET reason_code = 'future_reason' WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    let unsupported_reason = service
        .get_quarantine_recovery_plan(quarantine_recovery_plan_request(node_id, sandbox_id))
        .await
        .expect_err("unknown quarantine reasons must not receive recovery authority");
    assert_eq!(unsupported_reason.code(), tonic::Code::FailedPrecondition);
    assert_quarantine_recovery_rejection_cause(counters.finish(), "unsupported_reason");

    sqlx::query("UPDATE sandbox_quarantine_state SET reason_code = $1 WHERE sandbox_id = $2")
        .bind(SandboxQuarantineReasonCode::PauseFailed.as_str())
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    let not_recoverable = service
        .get_quarantine_recovery_plan(quarantine_recovery_plan_request(node_id, sandbox_id))
        .await
        .expect_err("non-recoverable reasons must not receive recovery authority");
    assert_eq!(not_recoverable.code(), tonic::Code::FailedPrecondition);
    assert_quarantine_recovery_rejection_cause(counters.finish(), "not_automatically_recoverable");

    sqlx::query("UPDATE sandbox_quarantine_state SET reason_code = $1 WHERE sandbox_id = $2")
        .bind(SandboxQuarantineReasonCode::RestorePrepareFailed.as_str())
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
    sqlx::query(
        "UPDATE sandboxes
            SET current_local_artifact_id = NULL,
                current_local_artifact_kind = NULL,
                current_local_artifact_origin = NULL
          WHERE id = $1",
    )
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    let no_exact_artifact = service
        .get_quarantine_recovery_plan(quarantine_recovery_plan_request(node_id, sandbox_id))
        .await
        .expect_err("exact-artifact policy requires an exact local identity");
    assert_eq!(no_exact_artifact.code(), tonic::Code::FailedPrecondition);
    assert!(
        no_exact_artifact
            .message()
            .contains("has no exact suspend artifact identity")
    );
    assert_quarantine_recovery_rejection_cause(counters.finish(), "no_exact_suspend_artifact");

    pin_suspend_snapshot_artifact(&db.pool, sandbox_id, snapshot_id).await?;
    sqlx::query("DELETE FROM snapshot_storage_locations WHERE snapshot_id = $1")
        .bind(snapshot_id)
        .execute(&db.pool)
        .await?;
    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    let no_snapshot = service
        .get_quarantine_recovery_plan(quarantine_recovery_plan_request(node_id, sandbox_id))
        .await
        .expect_err("an exact artifact without a registered location is not recoverable");
    assert_eq!(no_snapshot.code(), tonic::Code::FailedPrecondition);
    assert_quarantine_recovery_rejection_cause(counters.finish(), "no_compatible_snapshot");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn pre_quarantine_recovery_ignores_only_failed_retry_of_same_artifact() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "restoring").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 12, "running", Some("100.64.0.12")).await?;
    let ready_snapshot_id = insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;
    let local_artifact_id = snapshot_local_artifact_id(&db.pool, ready_snapshot_id).await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, source_node_id, org_id, timestamp,
             local_artifact_id, artifact_kind, artifact_origin
         )
         VALUES (
             $1, $2, 'full', 'failed', $3,
             '00000000-0000-0000-0000-000000000001',
             NOW() + INTERVAL '1 second', $4, 'full', 'suspend'
         )",
    ))
    .bind(Uuid::now_v7())
    .bind(sandbox_id)
    .bind(node_id)
    .bind(local_artifact_id)
    .execute(&db.pool)
    .await?;

    let reason_code = SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let mut quarantined = quarantined_sandbox_state(sandbox_id);
    quarantined.quarantine_reason_code = Some(reason_code.to_string());
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[quarantined]).await?;
    let service = ClusterApiService::new(db.pool.clone());

    let plan = service
        .get_quarantine_recovery_plan(quarantine_recovery_plan_request(node_id, sandbox_id))
        .await?
        .into_inner();
    assert_eq!(
        plan.suspend_snapshot
            .expect("recovery snapshot")
            .snapshot_id,
        ready_snapshot_id.to_string()
    );

    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, source_node_id, org_id, timestamp,
             local_artifact_id, artifact_kind, artifact_origin
         )
         VALUES (
             $1, $2, 'full', 'failed', $3,
             '00000000-0000-0000-0000-000000000001',
             NOW() + INTERVAL '2 seconds', $4, 'full', 'suspend'
         )",
    ))
    .bind(Uuid::now_v7())
    .bind(sandbox_id)
    .bind(node_id)
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;

    let error = service
        .get_quarantine_recovery_plan(quarantine_recovery_plan_request(node_id, sandbox_id))
        .await
        .expect_err("a newer failed snapshot for different bytes must remain ambiguous");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn recovery_plan_supports_current_legacy_snapshot_without_artifact_identity() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "restoring").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 12, "running", Some("100.64.0.12")).await?;
    let snapshot_id = insert_ready_legacy_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;

    let reason_code = SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let mut quarantined = quarantined_sandbox_state(sandbox_id);
    quarantined.quarantine_reason_code = Some(reason_code.to_string());
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[quarantined]).await?;

    let service = ClusterApiService::new(db.pool.clone());
    let plan = service
        .get_quarantine_recovery_plan(quarantine_recovery_plan_request(node_id, sandbox_id))
        .await?
        .into_inner();
    let source = plan.suspend_snapshot.expect("legacy suspend snapshot plan");

    assert_eq!(plan.execution_generation, 12);
    assert_eq!(plan.reason_code, reason_code.as_str());
    assert_eq!(source.snapshot_id, snapshot_id.to_string());
    assert_eq!(source.local_artifact_id, "");
    assert_eq!(
        source.storage_uri,
        "s3://space-test/legacy-suspend-snapshot"
    );

    sqlx::query("UPDATE sandboxes SET last_command_at = NOW() WHERE id = $1")
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
    let stale = service
        .get_quarantine_recovery_plan(quarantine_recovery_plan_request(node_id, sandbox_id))
        .await
        .expect_err("legacy recovery must not rewind activity newer than the snapshot");
    assert_eq!(stale.code(), tonic::Code::FailedPrecondition);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn legacy_recovery_receipt_converges_orphan_quarantine_to_suspended() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "restoring").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 13, "running", Some("100.64.0.13")).await?;
    insert_ready_legacy_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;

    let mut quarantined = quarantined_sandbox_state(sandbox_id);
    quarantined.quarantine_reason_code =
        Some(SandboxQuarantineReasonCode::OrphanedSandboxRowMissing.to_string());
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[quarantined]).await?;
    let receipt_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT quarantine_id FROM sandbox_quarantine_state WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;

    let accepted = publish_sandbox_states(
        &mut conn,
        node_id,
        &[recovered_legacy_suspended_state(sandbox_id, receipt_id, 13)],
    )
    .await?;

    assert_eq!(accepted.accepted, 1);
    let (lifecycle, current_local_artifact_id): (String, Option<LocalArtifactId>) = sqlx::query_as(
        "SELECT lifecycle, current_local_artifact_id
           FROM sandboxes
          WHERE id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(lifecycle, SandboxLifecycle::Suspended.as_str());
    assert_eq!(current_local_artifact_id, None);
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                 SELECT 1 FROM sandbox_quarantine_state WHERE sandbox_id = $1
             )",
        )
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unfenced_recovery_receipt_preserves_new_restore_without_blocking_ordinary_suspend()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "restoring").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 7, "creating", None).await?;
    sqlx::query(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = $2,
                activation_attempt_expires_at = NOW() + INTERVAL '5 minutes',
                activation_attempt_target_state = 'started'
          WHERE sandbox_id = $1 AND generation = 7",
    )
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    let snapshot_id = insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;
    let local_artifact_id = snapshot_local_artifact_id(&db.pool, snapshot_id).await?;
    pin_suspend_snapshot_artifact(&db.pool, sandbox_id, snapshot_id).await?;

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(
        &mut conn,
        node_id,
        &[recovered_suspended_state(
            sandbox_id,
            Uuid::now_v7(),
            7,
            None,
            SandboxQuarantineReasonCode::RestorePrepareFailed,
            LocalArtifactId::from_uuid(local_artifact_id),
        )],
    )
    .await?;

    assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "restoring");
    let active_generation = sqlx::query_scalar::<_, i64>(
        "SELECT generation FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(active_generation, 7);

    let missed_quarantine_id = SandboxId::new();
    let missed_receipt_id = Uuid::now_v7();
    insert_active_sandbox(&db.pool, missed_quarantine_id, node_id, "restoring").await?;
    insert_k8s_execution(
        &db.pool,
        missed_quarantine_id,
        9,
        "running",
        Some("100.64.0.9"),
    )
    .await?;
    let snapshot_id =
        insert_ready_suspend_snapshot(&db.pool, missed_quarantine_id, node_id).await?;
    let missed_local_artifact_id = snapshot_local_artifact_id(&db.pool, snapshot_id).await?;
    pin_suspend_snapshot_artifact(&db.pool, missed_quarantine_id, snapshot_id).await?;
    publish_sandbox_states(
        &mut conn,
        node_id,
        &[recovered_suspended_state(
            missed_quarantine_id,
            missed_receipt_id,
            9,
            None,
            SandboxQuarantineReasonCode::RestorePrepareFailed,
            LocalArtifactId::from_uuid(missed_local_artifact_id),
        )],
    )
    .await?;
    assert_eq!(
        sandbox_lifecycle(&db.pool, missed_quarantine_id).await?,
        "restoring",
        "a recovery receipt without an exact central quarantine authority must be ignored"
    );

    let ordinary_id = SandboxId::new();
    insert_active_sandbox(&db.pool, ordinary_id, node_id, "started").await?;
    insert_k8s_execution(&db.pool, ordinary_id, 3, "running", Some("100.64.0.8")).await?;
    let ordinary_snapshot_id =
        insert_ready_suspend_snapshot(&db.pool, ordinary_id, node_id).await?;
    let ordinary_local_artifact_id =
        snapshot_local_artifact_id(&db.pool, ordinary_snapshot_id).await?;
    publish_sandbox_states(
        &mut conn,
        node_id,
        &[recovered_suspended_state(
            ordinary_id,
            Uuid::now_v7(),
            3,
            None,
            SandboxQuarantineReasonCode::RestorePrepareFailed,
            LocalArtifactId::from_uuid(ordinary_local_artifact_id),
        )],
    )
    .await?;

    assert_eq!(sandbox_lifecycle(&db.pool, ordinary_id).await?, "suspended");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn forward_compatible_transition_reason_does_not_wedge_rollback_or_healthy_sibling()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let malformed_id = SandboxId::new();
    let healthy_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, malformed_id, node_id, "stopped").await?;
    insert_active_sandbox(&db.pool, healthy_id, node_id, "started").await?;
    insert_k8s_execution(&db.pool, malformed_id, 7, "creating", None).await?;

    let activation_attempt_id = Uuid::now_v7();
    sqlx::query(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = $2
          WHERE sandbox_id = $1 AND generation = 7",
    )
    .bind(malformed_id)
    .bind(activation_attempt_id)
    .execute(&db.pool)
    .await?;
    let mut malformed = activation_rollback_state(
        malformed_id,
        Uuid::now_v7(),
        "stopped",
        7,
        activation_attempt_id,
        SandboxQuarantineReasonCode::ColdActivationInterrupted,
    );
    malformed.transition_receipt.as_mut().unwrap().reason_code =
        "reason_added_by_newer_worker".to_string();
    let mut malformed_core = published_sandbox_state(SandboxId::new(), "started");
    malformed_core.pid = Some(u32::MAX);
    let healthy = published_sandbox_state(healthy_id, "started");
    let mut conn = db.pool.acquire().await?;
    let outcome =
        publish_sandbox_states(&mut conn, node_id, &[malformed, malformed_core, healthy]).await?;

    assert_eq!(outcome.accepted, 3);
    let still_active = sqlx::query_scalar::<_, bool>(
        "SELECT active
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = 7",
    )
    .bind(malformed_id)
    .fetch_one(&db.pool)
    .await?;
    assert!(
        !still_active,
        "an unknown diagnostic reason must not make an otherwise fenced rollback unconsumable"
    );
    assert_eq!(sandbox_lifecycle(&db.pool, healthy_id).await?, "started");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn degraded_publication_updates_presence_and_state_without_erasing_richer_observation()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;

    let initial = published_sandbox_state(sandbox_id, "started");
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[initial]).await?;

    let degraded = PublishedSandboxState {
        sandbox_id: sandbox_id.to_string(),
        present: true,
        status: Some("suspended".to_string()),
        state_degraded: true,
        ..PublishedSandboxState::default()
    };
    let outcome = publish_sandbox_states(&mut conn, node_id, &[degraded]).await?;
    assert_eq!(outcome.accepted, 1);

    let observed =
        sqlx::query_as::<_, (Option<String>, Option<String>, Option<String>, bool, i32)>(
            "SELECT status, template, image_hash, snapshots_enabled, missing_publish_count
           FROM sandbox_observed_states
          WHERE sandbox_id = $1",
        )
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(
        observed,
        (
            Some("suspended".to_string()),
            Some("app-runtime".to_string()),
            Some("sha256:test".to_string()),
            true,
            0,
        )
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn activation_rollback_receipts_converge_stable_targets_with_attempt_fence() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;

    // A rollback to `suspended` retires its execution as `replaced`, which the
    // backup fence refuses a capture of, only with abandoned-restore
    // retirement enabled, for a row still `restoring` that isn't an evacuation
    // restore, whose execution the restore created, and whose restore
    // selection is proven to choose a durable pre-restore snapshot; otherwise
    // a capture may be the sandbox's only copy, so the execution reads
    // `suspended`. A row another writer already moved off `restoring` no
    // longer dates the restore, so its snapshot proves nothing.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Source {
        /// No snapshot: nothing proves a pre-restore source.
        Missing,
        /// A durable pre-restore snapshot, and an execution and pod placement
        /// the restore made after entering `restoring`.
        Durable,
        /// As `Durable`, but the execution predates the restore, which
        /// reused it.
        DurableReusedExecution,
        /// As `Durable`, on an evacuation restore.
        DurableEvacuation,
        /// As `Durable`, with the row's artifact marker naming the snapshot's
        /// artifact, so the exact-artifact branch of the proof decides.
        DurableMarked,
        /// As `Durable`, but the marker names a newer artifact the control
        /// plane holds no durable snapshot of. The older durable snapshot
        /// isn't that artifact, so it proves nothing.
        MarkedWithoutBackup,
    }
    const REPLACED_IF_ENABLED: &str = "replaced if retirement is enabled";
    for retirement in [
        AbandonedRestoreRetirement::Disabled,
        AbandonedRestoreRetirement::Enabled,
    ] {
        for (target, central_lifecycle, reason_code, source, execution_state_after) in [
            (
                "created",
                "created",
                SandboxQuarantineReasonCode::CreateInterrupted,
                Source::Missing,
                "replaced",
            ),
            (
                "stopped",
                "stopped",
                SandboxQuarantineReasonCode::ColdActivationInterrupted,
                Source::Missing,
                "replaced",
            ),
            (
                "suspended",
                "restoring",
                SandboxQuarantineReasonCode::RestoreActivationInterrupted,
                Source::Durable,
                REPLACED_IF_ENABLED,
            ),
            (
                "suspended",
                "restoring",
                SandboxQuarantineReasonCode::RestorePrepareFailed,
                Source::Missing,
                "suspended",
            ),
            (
                "suspended",
                "suspended",
                SandboxQuarantineReasonCode::RestoreActivationInterrupted,
                Source::Durable,
                "suspended",
            ),
            (
                "suspended",
                "restoring",
                SandboxQuarantineReasonCode::RestoreActivationInterrupted,
                Source::DurableReusedExecution,
                "suspended",
            ),
            (
                "suspended",
                "restoring",
                SandboxQuarantineReasonCode::RestoreActivationInterrupted,
                Source::DurableEvacuation,
                "suspended",
            ),
            (
                "suspended",
                "restoring",
                SandboxQuarantineReasonCode::RestoreActivationInterrupted,
                Source::DurableMarked,
                REPLACED_IF_ENABLED,
            ),
            (
                "suspended",
                "restoring",
                SandboxQuarantineReasonCode::RestoreActivationInterrupted,
                Source::MarkedWithoutBackup,
                "suspended",
            ),
        ] {
            let sandbox_id = SandboxId::new();
            insert_active_sandbox(&db.pool, sandbox_id, node_id, central_lifecycle).await?;
            if source != Source::Missing {
                let snapshot_id =
                    insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;
                sqlx::query(
                    "UPDATE snapshots
                    SET timestamp = NOW() - INTERVAL '1 minute'
                  WHERE id = $1",
                )
                .bind(snapshot_id)
                .execute(&db.pool)
                .await?;
                // The restore's transition, then the placement of its pod.
                sqlx::query(
                    "UPDATE sandboxes
                        SET lifecycle_changed_at = NOW() - INTERVAL '30 seconds',
                            placement_changed_at = NOW() - INTERVAL '20 seconds',
                            evacuation_restore_target = CASE WHEN $2 THEN 'started' END
                      WHERE id = $1",
                )
                .bind(sandbox_id)
                .bind(source == Source::DurableEvacuation)
                .execute(&db.pool)
                .await?;
                let marker = match source {
                    Source::DurableMarked => {
                        Some(snapshot_local_artifact_id(&db.pool, snapshot_id).await?)
                    }
                    Source::MarkedWithoutBackup => Some(Uuid::now_v7()),
                    _ => None,
                };
                if let Some(marker) = marker {
                    sqlx::query(
                        "UPDATE sandboxes
                            SET current_local_artifact_id = $2,
                                current_local_artifact_kind = 'full',
                                current_local_artifact_origin = 'suspend'
                          WHERE id = $1",
                    )
                    .bind(sandbox_id)
                    .bind(marker)
                    .execute(&db.pool)
                    .await?;
                }
            }
            insert_k8s_execution(&db.pool, sandbox_id, 7, "creating", Some("100.64.0.7")).await?;
            if source == Source::DurableReusedExecution {
                // The execution predates the restore that reused it.
                sqlx::query(
                    "UPDATE sandbox_k8s_executions
                        SET created_at = NOW() - INTERVAL '1 hour'
                      WHERE sandbox_id = $1 AND generation = 7",
                )
                .bind(sandbox_id)
                .execute(&db.pool)
                .await?;
            }

            let activation_attempt_id = Uuid::now_v7();
            sqlx::query(
                "UPDATE sandbox_k8s_executions
                SET activation_attempt_id = $2
              WHERE sandbox_id = $1 AND generation = 7",
            )
            .bind(sandbox_id)
            .bind(activation_attempt_id)
            .execute(&db.pool)
            .await?;
            let receipt = activation_rollback_state(
                sandbox_id,
                Uuid::now_v7(),
                target,
                7,
                activation_attempt_id,
                reason_code,
            );
            let mut conn = db.pool.acquire().await?;
            let outcome = publish_sandbox_states_with_retirement(
                &mut conn,
                node_id,
                std::slice::from_ref(&receipt),
                retirement,
            )
            .await?;
            assert_eq!(outcome.post_commit_events.len(), 1);
            assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, target);
            let (placement, active_node_id, target_node_id) =
                sqlx::query_as::<_, (String, Option<NodeId>, Option<NodeId>)>(
                    "SELECT placement, active_node_id, target_node_id
                   FROM sandboxes WHERE id = $1",
                )
                .bind(sandbox_id)
                .fetch_one(&db.pool)
                .await?;
            assert_eq!(placement, "unplaced");
            assert!(active_node_id.is_none());
            assert_eq!(target_node_id, Some(node_id));
            let (active, execution_state, pod_ip) =
                sqlx::query_as::<_, (bool, String, Option<String>)>(
                    "SELECT active, state, pod_ip::TEXT
                   FROM sandbox_k8s_executions
                  WHERE sandbox_id = $1 AND generation = 7",
                )
                .bind(sandbox_id)
                .fetch_one(&db.pool)
                .await?;
            assert!(!active);
            let expected_state = match execution_state_after {
                REPLACED_IF_ENABLED if retirement.is_enabled() => "replaced",
                REPLACED_IF_ENABLED => "suspended",
                state => state,
            };
            assert_eq!(
                execution_state, expected_state,
                "{reason_code} {source:?} {retirement:?}"
            );
            // A retirement clears the placement clock the restore's pod set,
            // so the suspended selector still takes the pre-restore snapshot.
            if source != Source::Missing {
                let placement_cleared = sqlx::query_scalar::<_, bool>(
                    "SELECT placement_changed_at IS NULL FROM sandboxes WHERE id = $1",
                )
                .bind(sandbox_id)
                .fetch_one(&db.pool)
                .await?;
                assert_eq!(
                    placement_cleared,
                    expected_state == "replaced",
                    "{reason_code} {source:?} {retirement:?}"
                );
            }
            assert!(pod_ip.is_none());

            let replay =
                publish_sandbox_states_with_retirement(&mut conn, node_id, &[receipt], retirement)
                    .await?;
            assert!(replay.post_commit_events.is_empty());
        }
    }

    let stale_id = SandboxId::new();
    insert_active_sandbox(&db.pool, stale_id, node_id, "stopped").await?;
    insert_k8s_execution(&db.pool, stale_id, 7, "creating", None).await?;
    let current_attempt_id = Uuid::now_v7();
    sqlx::query(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = $2
          WHERE sandbox_id = $1 AND generation = 7",
    )
    .bind(stale_id)
    .bind(current_attempt_id)
    .execute(&db.pool)
    .await?;
    let stale_receipt = activation_rollback_state(
        stale_id,
        Uuid::now_v7(),
        "stopped",
        7,
        Uuid::now_v7(),
        SandboxQuarantineReasonCode::ColdActivationInterrupted,
    );
    let mut conn = db.pool.acquire().await?;
    let outcome = publish_sandbox_states(&mut conn, node_id, &[stale_receipt]).await?;
    assert!(outcome.post_commit_events.is_empty());
    assert_eq!(sandbox_lifecycle(&db.pool, stale_id).await?, "stopped");
    let newer_execution_active = sqlx::query_scalar::<_, bool>(
        "SELECT active FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = 7",
    )
    .bind(stale_id)
    .fetch_one(&db.pool)
    .await?;
    assert!(newer_execution_active);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn node_publish_applies_completion_after_same_transaction_lifecycle_projection() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let activation_attempt_id = Uuid::now_v7();
    let route_activation_id = Uuid::now_v7();
    let receipt_id = AutoLifecycleDeleteReceiptId::generate();
    let policy = AutoLifecyclePolicy {
        condition_sets: [(
            AutoLifecycleConditionSetId::AccessIdle,
            AutoLifecycleConditionSet::access_idle(),
        )]
        .into(),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::AccessIdle,
            at: 900,
            action: AutoLifecycleIdleAction::Delete,
        }],
        ..Default::default()
    };
    insert_ready_worker_node(&db.pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(format!("node-{node_id}"))
    .execute(&db.pool)
    .await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "restoring").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET auto_lifecycle_json = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(sqlx::types::Json(&policy))
    .execute(&db.pool)
    .await?;
    insert_k8s_execution(&db.pool, sandbox_id, 7, "creating", Some("100.64.0.7")).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = $2,
                route_activation_id = $3,
                k8s_node_name = $4
          WHERE sandbox_id = $1 AND generation = 7"
    ))
    .bind(sandbox_id)
    .bind(activation_attempt_id)
    .bind(route_activation_id)
    .bind(format!("node-{node_id}"))
    .execute(&db.pool)
    .await?;
    insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;

    let projected_state = activation_rollback_state(
        sandbox_id,
        Uuid::now_v7(),
        "suspended",
        7,
        activation_attempt_id,
        SandboxQuarantineReasonCode::RestoreActivationInterrupted,
    );
    let completion = pplx_space_internal_api_server::proto::AutoLifecycleDeleteCompletion {
        receipt_id: receipt_id.to_string(),
        sandbox_id: sandbox_id.to_string(),
        execution_generation: 7,
        route_activation_id: route_activation_id.to_string(),
        auto_lifecycle_policy_fingerprint: AutoLifecyclePolicyFingerprint::for_policy(&policy)
            .to_string(),
    };
    let mut request = Request::new(PublishNodeStateRequest {
        node_id: node_id.to_string(),
        sandboxes: vec![projected_state],
        role: Some(ProtoNodeRole::Worker as i32),
        auto_lifecycle_delete_completions: vec![completion],
        ..PublishNodeStateRequest::default()
    });
    request
        .extensions_mut()
        .insert(tonic::transport::server::TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(std::net::SocketAddr::from(([127, 0, 0, 1], 49000))),
        });

    let response = ClusterApiService::new(db.pool.clone())
        .publish_node_state(request)
        .await?
        .into_inner();

    assert_eq!(
        response.acknowledged_auto_lifecycle_delete_receipt_ids,
        vec![receipt_id.to_string()]
    );
    assert!(
        !sqlx::query_scalar::<_, bool>(sql!(
            "SELECT EXISTS(SELECT 1 FROM sandboxes WHERE id = $1)"
        ))
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn node_publish_rejects_delete_completions_from_a_builder_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'builder', 'k8s', 'ready')"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    // Builders publish sandbox inventory for their build sandboxes, so the
    // inventory role check admits them, but only workers run the cluster-owned
    // auto-lifecycle deletes a receipt proves. A builder that omits the role
    // field never authenticates against the worker role hint; one that declares
    // it has to be turned away by the receipt gate.
    let mut request = Request::new(PublishNodeStateRequest {
        node_id: node_id.to_string(),
        role: Some(ProtoNodeRole::Builder as i32),
        auto_lifecycle_delete_completions: vec![
            pplx_space_internal_api_server::proto::AutoLifecycleDeleteCompletion {
                receipt_id: AutoLifecycleDeleteReceiptId::generate().to_string(),
                sandbox_id: Uuid::now_v7().to_string(),
                execution_generation: 7,
                route_activation_id: Uuid::now_v7().to_string(),
                auto_lifecycle_policy_fingerprint: AutoLifecyclePolicyFingerprint::for_policy(
                    &AutoLifecyclePolicy::disabled(),
                )
                .to_string(),
            },
        ],
        ..PublishNodeStateRequest::default()
    });
    request
        .extensions_mut()
        .insert(tonic::transport::server::TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(std::net::SocketAddr::from(([127, 0, 0, 1], 49000))),
        });

    let status = ClusterApiService::new(db.pool.clone())
        .publish_node_state(request)
        .await
        .expect_err("builder receipts must be rejected");

    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status.message().contains("worker nodes"),
        "unexpected message: {}",
        status.message()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn node_publish_refuses_a_name_registered_under_another_role() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    // A Kubernetes node name resolves against (k8s_node_name, role). Declaring
    // a role the name is not registered under is a miss: registration belongs
    // to the controller's node sync, so the publication is NotFound and leaves
    // no second row carrying the asserted role and the caller's address.
    let builder_node_id = NodeId::new();
    let k8s_node_name = format!("node-{builder_node_id}");
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'builder', 'k8s', 'ready')"
    ))
    .bind(builder_node_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(builder_node_id)
    .bind(&k8s_node_name)
    .execute(&db.pool)
    .await?;

    let mut request = Request::new(PublishNodeStateRequest {
        node_id: k8s_node_name.clone(),
        role: Some(ProtoNodeRole::Worker as i32),
        auto_lifecycle_delete_completions: vec![
            pplx_space_internal_api_server::proto::AutoLifecycleDeleteCompletion {
                receipt_id: AutoLifecycleDeleteReceiptId::generate().to_string(),
                sandbox_id: Uuid::now_v7().to_string(),
                execution_generation: 7,
                route_activation_id: Uuid::now_v7().to_string(),
                auto_lifecycle_policy_fingerprint: AutoLifecyclePolicyFingerprint::for_policy(
                    &AutoLifecyclePolicy::disabled(),
                )
                .to_string(),
            },
        ],
        ..PublishNodeStateRequest::default()
    });
    request
        .extensions_mut()
        .insert(tonic::transport::server::TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(std::net::SocketAddr::from(([127, 0, 0, 1], 49000))),
        });

    let status = ClusterApiService::new(db.pool.clone())
        .publish_node_state(request)
        .await
        .expect_err("a name registered under another role must not resolve");

    assert_eq!(status.code(), tonic::Code::NotFound);
    let rows_for_name: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM nodes n
         JOIN node_k8s nk ON nk.node_id = n.id
         WHERE nk.k8s_node_name = $1"
    ))
    .bind(&k8s_node_name)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        rows_for_name, 1,
        "the publication must not register a second row for the name"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn split_operation_receipt_expiry_does_not_converge_proof() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    for rollback in [false, true] {
        let sandbox_id = SandboxId::new();
        let route_a = Uuid::now_v7();
        let operation_b = Uuid::now_v7();
        insert_active_sandbox(&db.pool, sandbox_id, node_id, "stopped").await?;
        insert_k8s_execution(&db.pool, sandbox_id, 7, "running", Some("100.64.0.7")).await?;
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET route_activation_id = $2, activation_attempt_id = $3,
                    activation_attempt_expires_at = NOW() - INTERVAL '1 hour',
                    activation_attempt_target_state = 'started'
              WHERE sandbox_id = $1 AND generation = 7"
        ))
        .bind(sandbox_id)
        .bind(route_a)
        .bind(operation_b)
        .execute(&db.pool)
        .await?;
        let receipt = |attempt| {
            if rollback {
                activation_rollback_state(
                    sandbox_id,
                    Uuid::now_v7(),
                    "stopped",
                    7,
                    attempt,
                    SandboxQuarantineReasonCode::ColdActivationInterrupted,
                )
            } else {
                activation_completed_state(sandbox_id, Uuid::now_v7(), "started", 7, attempt)
            }
        };
        // Repeated inventory and expired ownership do not turn route A into evidence for B.
        for _ in 0..3 {
            let mut conn = db.pool.acquire().await?;
            publish_sandbox_states(&mut conn, node_id, &[receipt(route_a)]).await?;
        }
        let read = || {
            sqlx::query_as::<_, (bool, Option<Uuid>, Option<Uuid>)>(sql!(
                "SELECT active, route_activation_id, activation_attempt_id
               FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND generation = 7"
            ))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
        };
        assert_eq!(read().await?, (true, Some(route_a), Some(operation_b)));
        assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "stopped");
        let mut conn = db.pool.acquire().await?;
        publish_sandbox_states(&mut conn, node_id, &[receipt(operation_b)]).await?;
        let (active, route, operation) = read().await?;
        assert_eq!(route, Some(route_a));
        if rollback {
            assert!(
                !active,
                "exact rollback retires the execution expiry left active"
            );
            let placement: String =
                sqlx::query_scalar(sql!("SELECT placement FROM sandboxes WHERE id = $1"))
                    .bind(sandbox_id)
                    .fetch_one(&db.pool)
                    .await?;
            assert_eq!(placement, "unplaced");
        } else {
            assert!(active);
            assert_eq!(operation, None);
            assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "started");
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn activation_completion_receipt_converges_and_releases_only_its_attempt() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let attempt_a = Uuid::now_v7();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "stopped").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 7, "creating", None).await?;
    sqlx::query(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = $2,
                activation_attempt_expires_at = NOW() + INTERVAL '5 minutes',
                activation_attempt_target_state = 'started'
          WHERE sandbox_id = $1 AND generation = 7",
    )
    .bind(sandbox_id)
    .bind(attempt_a)
    .execute(&db.pool)
    .await?;

    let mut receipt =
        activation_completed_state(sandbox_id, Uuid::now_v7(), "started", 7, attempt_a);
    receipt.transition_receipt.as_mut().unwrap().reason_code =
        "diagnostic_added_by_newer_worker".to_string();
    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(&mut conn, node_id, &[receipt.clone()]).await?;
    assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "stopped");
    let still_owned = sqlx::query_scalar::<_, Option<Uuid>>(
        "SELECT activation_attempt_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = 7",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(still_owned, Some(attempt_a));

    sqlx::query(
        "UPDATE sandbox_k8s_executions
            SET state = 'running', pod_ip = '100.64.0.7'
          WHERE sandbox_id = $1 AND generation = 7",
    )
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let fenced_ready = sqlx::query_as::<_, (String, Option<String>, Option<Uuid>, Option<String>)>(
        "SELECT state, host(pod_ip), activation_attempt_id, activation_attempt_target_state
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = 7",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        fenced_ready,
        (
            "running".to_string(),
            Some("100.64.0.7".to_string()),
            Some(attempt_a),
            Some("started".to_string()),
        )
    );
    // A legacy route-less resident must retain its operation until the API's
    // post-ack route publication CAS can still match that operation owner.
    publish_sandbox_states(&mut conn, node_id, &[receipt.clone()]).await?;
    let unpublished_owner: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND generation = 7"
    )).bind(sandbox_id).fetch_one(&db.pool).await?;
    assert_eq!(unpublished_owner, Some(attempt_a));
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET route_activation_id = $2
          WHERE sandbox_id = $1 AND generation = 7 AND activation_attempt_id = $2"
    ))
    .bind(sandbox_id)
    .bind(attempt_a)
    .execute(&db.pool)
    .await?;
    publish_sandbox_states(&mut conn, node_id, &[receipt.clone()]).await?;
    assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "started");
    let released = sqlx::query_scalar::<_, Option<Uuid>>(
        "SELECT activation_attempt_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = 7",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert!(released.is_none());

    let attempt_b = Uuid::now_v7();
    sqlx::query(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = $2,
                activation_attempt_expires_at = NOW() + INTERVAL '5 minutes',
                activation_attempt_target_state = 'started'
          WHERE sandbox_id = $1 AND generation = 7",
    )
    .bind(sandbox_id)
    .bind(attempt_b)
    .execute(&db.pool)
    .await?;
    publish_sandbox_states(&mut conn, node_id, &[receipt]).await?;
    let current = sqlx::query_scalar::<_, Option<Uuid>>(
        "SELECT activation_attempt_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = 7",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(current, Some(attempt_b));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn activation_completion_receipt_cannot_overwrite_a_newer_suspend_claim() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    for suspend_lifecycle in ["suspending", "suspended"] {
        let sandbox_id = SandboxId::new();
        let activation_attempt_id = Uuid::now_v7();
        insert_active_sandbox(&db.pool, sandbox_id, node_id, suspend_lifecycle).await?;
        insert_k8s_execution(&db.pool, sandbox_id, 7, "running", Some("100.64.0.8")).await?;
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET activation_attempt_id = $2,
                    route_activation_id = $2,
                    activation_attempt_expires_at = NOW() + INTERVAL '5 minutes',
                    activation_attempt_target_state = 'started'
              WHERE sandbox_id = $1 AND generation = 7"
        ))
        .bind(sandbox_id)
        .bind(activation_attempt_id)
        .execute(&db.pool)
        .await?;

        let receipt = activation_completed_state(
            sandbox_id,
            Uuid::now_v7(),
            "started",
            7,
            activation_attempt_id,
        );
        let mut conn = db.pool.acquire().await?;
        publish_sandbox_states(&mut conn, node_id, &[receipt]).await?;
        drop(conn);

        assert_eq!(
            sandbox_lifecycle(&db.pool, sandbox_id).await?,
            suspend_lifecycle,
            "the node's stale activation completion must defer to suspend ownership"
        );
        let remaining_attempt = sqlx::query_scalar::<_, Option<Uuid>>(sql!(
            "SELECT activation_attempt_id
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = 7"
        ))
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(
            remaining_attempt, None,
            "the exact completed attempt should still be released instead of being republished forever"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_failed_sandbox_from_non_active_node_does_not_mark_central_lifecycle_failed()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let active_node_id = NodeId::new();
    let publishing_node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, active_node_id).await?;
    insert_ready_worker_node(&db.pool, publishing_node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, active_node_id, "started").await?;

    let mut conn = db.pool.acquire().await?;
    let accepted = publish_sandbox_states(
        &mut conn,
        publishing_node_id,
        &[failed_sandbox_state(sandbox_id)],
    )
    .await?;

    assert_eq!(accepted.accepted, 0);
    let sandbox_lifecycle =
        sqlx::query_scalar::<_, String>("SELECT lifecycle FROM sandboxes WHERE id = $1")
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;

    assert_eq!(sandbox_lifecycle, "started");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_direct_failed_sandbox_does_not_mark_central_lifecycle_failed() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox_with_managed_by(&db.pool, sandbox_id, node_id, "started", "space")
        .await?;

    let mut conn = db.pool.acquire().await?;
    let accepted =
        publish_sandbox_states(&mut conn, node_id, &[failed_sandbox_state(sandbox_id)]).await?;

    assert_eq!(accepted.accepted, 1);
    let (sandbox_lifecycle, observed_status) = sqlx::query_as::<_, (String, String)>(
        "SELECT s.lifecycle, sos.status
         FROM sandboxes s
         JOIN sandbox_observed_states sos ON sos.sandbox_id = s.id
         WHERE s.id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;

    assert_eq!(observed_status, "failed");
    assert_eq!(sandbox_lifecycle, "started");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_failed_sandbox_does_not_override_deleting() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "deleting").await?;

    let mut conn = db.pool.acquire().await?;
    let accepted =
        publish_sandbox_states(&mut conn, node_id, &[failed_sandbox_state(sandbox_id)]).await?;

    assert_eq!(accepted.accepted, 1);
    let (sandbox_lifecycle, observed_status) = sqlx::query_as::<_, (String, String)>(
        "SELECT s.lifecycle, sos.status
         FROM sandboxes s
         JOIN sandbox_observed_states sos ON sos.sandbox_id = s.id
         WHERE s.id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;

    assert_eq!(observed_status, "failed");
    assert_eq!(sandbox_lifecycle, "deleting");
    Ok(())
}

/// A node-local restore can finish after the API caller timed out, but a
/// published `started` status cannot safely complete the control-plane row
/// until the active Kubernetes execution is routable. An otherwise identical
/// Direct (`'space'`) row has no such contract and converges.
#[tokio::test(flavor = "current_thread")]
async fn publishing_owned_started_sandbox_leaves_unroutable_restoring_lifecycle() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let direct_sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    for (id, managed_by) in [(sandbox_id, "k8s"), (direct_sandbox_id, "space")] {
        insert_active_sandbox_with_managed_by(&db.pool, id, node_id, "restoring", managed_by)
            .await?;
        insert_k8s_execution(&db.pool, id, 7, "creating", None).await?;
    }

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(
        &mut conn,
        node_id,
        &[
            published_sandbox_state(sandbox_id, "started"),
            published_sandbox_state(direct_sandbox_id, "started"),
        ],
    )
    .await?;
    drop(conn);

    assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "restoring");
    assert_eq!(
        sandbox_lifecycle(&db.pool, direct_sandbox_id).await?,
        "started"
    );
    Ok(())
}

/// Once the blocking path has already hydrated the active Kubernetes
/// execution, node-published `started` may converge a stale `restoring`
/// lifecycle.
#[tokio::test(flavor = "current_thread")]
async fn publishing_owned_started_sandbox_converges_restoring_lifecycle() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "restoring").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 7, "running", Some("100.64.10.7")).await?;

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state(sandbox_id, "started")],
    )
    .await?;
    drop(conn);

    assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "started");
    Ok(())
}

/// A sandbox that restored and then idle-auto-suspended while the
/// control plane still read `restoring` publishes `suspended`. The
/// suspended promotion must accept `restoring` as a prior state or the
/// row stays wedged with a ready artifact it will never acknowledge.
#[tokio::test(flavor = "current_thread")]
async fn publishing_owned_suspended_sandbox_promotes_restoring_lifecycle() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "restoring").await?;
    insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state(sandbox_id, "suspended")],
    )
    .await?;
    drop(conn);

    assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "suspended");
    Ok(())
}

/// Only the node the control plane placed the sandbox on may converge
/// it — a stale report from a previous node must not race a cross-node
/// restore.
#[tokio::test(flavor = "current_thread")]
async fn publishing_started_sandbox_from_non_active_node_does_not_converge() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let active_node_id = NodeId::new();
    let publishing_node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, active_node_id).await?;
    insert_ready_worker_node(&db.pool, publishing_node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, active_node_id, "restoring").await?;

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(
        &mut conn,
        publishing_node_id,
        &[published_sandbox_state(sandbox_id, "started")],
    )
    .await?;
    drop(conn);

    assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "restoring");
    Ok(())
}

/// An evacuation-marked `restoring` row is owned by the node-drain
/// redrive (mirrors the transient-lifecycle sweeper's exemption); the
/// publish-side convergence must leave it alone.
#[tokio::test(flavor = "current_thread")]
async fn publishing_started_sandbox_spares_evacuation_marked_restoring_row() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "restoring").await?;
    sqlx::query("UPDATE sandboxes SET evacuation_restore_target = 'started' WHERE id = $1")
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;

    let mut conn = db.pool.acquire().await?;
    publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state(sandbox_id, "started")],
    )
    .await?;
    drop(conn);

    assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "restoring");
    Ok(())
}

/// `sandbox_inventory_pruned_at` is the census-freshness signal the drain gates
/// read, and it has to record the server's prune verdict rather than the node's
/// `sandbox_inventory_complete` claim. Walk one node through the four
/// publication shapes in sequence, because the sequence is the behavior: the
/// status upsert runs on *every* publication, so a stamp that came from the wire
/// flag — or a preserve half written as `= EXCLUDED.x` — would certify a census
/// the server rejected, or blank a good one on the next facet's tick.
#[tokio::test(flavor = "current_thread")]
async fn only_a_pruned_inventory_stamps_the_census_timestamp() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'k8s', 'ready')"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    let sandbox_id = SandboxId::new();
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;

    let service = ClusterApiService::new(db.pool.clone());
    let present = |id: String| PublishedSandboxState {
        sandbox_id: id,
        present: true,
        status: Some("started".to_string()),
        ..PublishedSandboxState::default()
    };
    let publish = async |complete: bool, sandboxes: Vec<PublishedSandboxState>| {
        let mut request = Request::new(PublishNodeStateRequest {
            node_id: node_id.to_string(),
            role: Some(ProtoNodeRole::Worker as i32),
            sandbox_inventory_complete: complete,
            sandboxes,
            ..PublishNodeStateRequest::default()
        });
        request
            .extensions_mut()
            .insert(tonic::transport::server::TcpConnectInfo {
                local_addr: None,
                remote_addr: Some(std::net::SocketAddr::from(([127, 0, 0, 1], 49000))),
            });
        service.publish_node_state(request).await
    };
    let pruned_at = async || {
        sqlx::query_scalar::<_, Option<time::OffsetDateTime>>(sql!(
            "SELECT sandbox_inventory_pruned_at FROM node_status_snapshots WHERE node_id = $1"
        ))
        .bind(node_id)
        .fetch_one(&db.pool)
        .await
    };

    // A complete inventory the server can parse prunes, so it stamps.
    publish(true, vec![present(sandbox_id.to_string())]).await?;
    assert!(
        pruned_at().await?.is_some(),
        "an accepted complete inventory must record when the census was pruned"
    );

    // Back-date it so "preserved" and "re-stamped" cannot be confused: two
    // consecutive transactions differ only by microseconds otherwise.
    sqlx::query(sql!(
        "UPDATE node_status_snapshots
            SET sandbox_inventory_pruned_at = NOW() - make_interval(hours => 1)
          WHERE node_id = $1"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    let backdated = pruned_at()
        .await?
        .expect("the back-dated timestamp must still be set");

    // The status, network-policy, and template facets all publish with the flag
    // false, on the same cadence as the census. They must leave it alone.
    publish(false, Vec::new()).await?;
    assert_eq!(
        pruned_at().await?,
        Some(backdated),
        "a facet publishing no complete inventory must neither stamp nor blank \
         the census timestamp"
    );

    // Flagged complete, but one sandbox id the server cannot parse: the prune is
    // skipped to preserve existing associations, so the census is not current
    // and must not be certified as such.
    publish(
        true,
        vec![
            present(sandbox_id.to_string()),
            present("not-a-sandbox-uuid".to_string()),
        ],
    )
    .await?;
    assert_eq!(
        pruned_at().await?,
        Some(backdated),
        "an inventory the server refused to prune against must not stamp the \
         timestamp its own completeness flag claims"
    );

    // And the good shape still advances it, so the preserve half above is not
    // simply a column that never moves.
    publish(true, vec![present(sandbox_id.to_string())]).await?;
    let advanced = pruned_at().await?.expect("a pruned census must stamp");
    assert!(
        advanced > backdated,
        "a later accepted inventory must advance the timestamp: \
         {advanced} is not after {backdated}"
    );
    Ok(())
}
