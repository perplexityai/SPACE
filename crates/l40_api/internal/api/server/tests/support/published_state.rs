//! Fixtures shared by public publication behavior tests and private unit tests.

use pplx_space_common_core::types::{LocalArtifactId, NodeId, SandboxId, SnapshotId};
use pplx_space_common_core::{
    sql,
    types::{Arch, ExecutionMetadata},
};
use pplx_space_node_api_client::proto::PublishedSandboxState;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

pub(super) async fn insert_active_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    lifecycle: &str,
) -> TestResult {
    insert_active_sandbox_with_managed_by(pool, sandbox_id, node_id, lifecycle, "k8s").await
}

pub(super) async fn insert_active_sandbox_with_managed_by(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    lifecycle: &str,
    managed_by: &str,
) -> TestResult {
    sqlx::query(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, active_node_id,
             target_node_id, managed_by, network_policy_override_json,
             network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $2,
             'on_node',
             $3,
             $3,
             $4,
             $5,
             1
         )",
    )
    .bind(sandbox_id)
    .bind(lifecycle)
    .bind(node_id)
    .bind(managed_by)
    .bind(sqlx::types::Json(
        &pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(pool)
    .await?;
    Ok(())
}

pub(super) async fn insert_k8s_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    state: &str,
    pod_ip: Option<&str>,
) -> TestResult {
    let pod_name = format!("space-sbx-{}-g{generation}", sandbox_id.as_uuid().simple());
    sqlx::query(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, pod_uid,
             pod_ip, state, active
         )
         VALUES ($1, $2, $3, 'sandboxes', $4, $5::INET, $6, TRUE)",
    )
    .bind(sandbox_id)
    .bind(generation)
    .bind(pod_name)
    .bind(format!("pod-uid-{generation}"))
    .bind(pod_ip)
    .bind(state)
    .execute(pool)
    .await?;
    Ok(())
}

/// Typed reader for the `sandboxes` x `sandbox_k8s_executions` join keyed by
/// `sandboxes.id`, covering the publish-outcome columns the
/// `publish_sandbox_states` behavior suite checks most often. Deliberately
/// excludes `active_node_id` and `current_local_artifact_id`: both are set
/// to run-varying UUIDs by most fixtures here, and most of these call sites
/// never asserted on them, so including them would make the snapshot flap
/// across runs without pinning any signal the old probes actually checked.
/// The two sites that do care about those columns use their own readers
/// below with an explicit redaction (or a deterministic-`None` assertion)
/// instead.
#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub(super) struct SandboxExecutionRow {
    pub(super) lifecycle: String,
    pub(super) active: bool,
    pub(super) state: String,
}

pub(super) async fn sandbox_execution_row(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<SandboxExecutionRow> {
    Ok(sqlx::query_as(sql!(
        "SELECT s.lifecycle, e.active, e.state
           FROM sandboxes s
           JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// Typed reader adding the current local artifact id to
/// [`SandboxExecutionRow`], for the one behavior (a finalized suspend) that
/// asserts an exact match against a run-varying artifact id.
#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub(super) struct SandboxExecutionArtifactRow {
    pub(super) lifecycle: String,
    pub(super) active: bool,
    pub(super) state: String,
    pub(super) current_local_artifact_id: Option<LocalArtifactId>,
}

pub(super) async fn sandbox_execution_artifact_row(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<SandboxExecutionArtifactRow> {
    Ok(sqlx::query_as(sql!(
        "SELECT s.lifecycle, e.active, e.state, s.current_local_artifact_id
           FROM sandboxes s
           JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

pub(super) async fn insert_ready_suspend_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
) -> TestResult<SnapshotId> {
    let snapshot_id = SnapshotId::new();
    let local_artifact_id = LocalArtifactId::new();
    sqlx::query(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, source_node_id, org_id,
             local_artifact_id, artifact_kind, artifact_origin
         )
         VALUES (
             $1, $2, 'full', 'ready', $3,
             '00000000-0000-0000-0000-000000000001',
             $4, 'full', 'suspend'
         )",
    )
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(node_id)
    .bind(local_artifact_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES (
             $1,
             's3://space-test/suspend-snapshot',
             '00000000-0000-0000-0000-000000000001'
         )",
    )
    .bind(snapshot_id)
    .execute(pool)
    .await?;
    Ok(snapshot_id)
}

pub(super) async fn snapshot_local_artifact_id(
    pool: &PgPool,
    snapshot_id: SnapshotId,
) -> TestResult<Uuid> {
    Ok(
        sqlx::query_scalar("SELECT local_artifact_id FROM snapshots WHERE id = $1")
            .bind(snapshot_id)
            .fetch_one(pool)
            .await?,
    )
}

pub(super) fn published_sandbox_state(
    sandbox_id: SandboxId,
    status: &str,
) -> PublishedSandboxState {
    PublishedSandboxState {
        sandbox_id: sandbox_id.to_string(),
        present: true,
        status: Some(status.to_string()),
        template: Some("app-runtime".to_string()),
        content_hash: Some("sha256:test".to_string()),
        runtime: Some("fc".to_string()),
        snapshots_enabled: true,
        ..PublishedSandboxState::default()
    }
}

pub(super) fn test_execution() -> ExecutionMetadata {
    use pplx_space_common_core::types::{
        ArtifactComponentKind, ArtifactComponentMetadata, ArtifactFormat, ArtifactVersion, Digest,
    };

    ExecutionMetadata::initial_firecracker(
        Arch::X86_64,
        vec![ArtifactComponentMetadata {
            kind: ArtifactComponentKind::Kernel,
            role: None,
            version: Some(ArtifactVersion::parse("6.10.14").unwrap()),
            digest: Some(Digest::sha256([0x42; 32])),
            format: Some(ArtifactFormat::parse("vmlinux").unwrap()),
        }],
    )
}

pub(super) fn timestamp(seconds: i64) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds, nanos: 0 }
}
