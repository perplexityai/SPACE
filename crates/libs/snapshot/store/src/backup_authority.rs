//! Historical publisher evidence. Inventory can veto a claim, never create it.
use super::*;
use pplx_space_common_core::types::{K8sNodeName, SubstrateKind};

#[derive(sqlx::FromRow)]
pub struct BackupPlacement {
    pub source_node_id: Option<NodeId>,
}

/// Serializes registration with placement changes and execution retirement.
pub async fn lock_placement(
    conn: &mut sqlx::PgConnection,
    sandbox: SandboxId,
) -> Result<Option<BackupPlacement>, sqlx::Error> {
    sqlx::query_as(sql!("SELECT COALESCE(active_node_id,target_node_id) AS source_node_id FROM sandboxes WHERE id=$1 FOR UPDATE"))
        .bind(sandbox).fetch_optional(conn).await
}

#[derive(sqlx::FromRow)]
pub struct Registration {
    pub id: SnapshotId,
    pub source_node_id: NodeId,
    pub source_node_uid: K8sNodeUid,
}

/// Only a registration carrying original verified identity grants retry authority.
/// A legacy attempt remains unknown even if today's inventory has a Node UID.
pub async fn prior_registration(
    conn: &mut sqlx::PgConnection,
    sandbox: SandboxId,
    artifact: LocalArtifactId,
) -> Result<Option<Registration>, sqlx::Error> {
    sqlx::query_as(sql!("SELECT id,source_node_id,source_node_uid FROM snapshots
        WHERE recovery_grant_id IS NULL AND sandbox_id=$1 AND local_artifact_id=$2 AND source_node_id IS NOT NULL AND source_node_uid IS NOT NULL
        ORDER BY timestamp,id LIMIT 1"))
        .bind(sandbox).bind(artifact).fetch_optional(conn).await
}

pub async fn provenance_conflicts(
    conn: &mut sqlx::PgConnection,
    sandbox: SandboxId,
    artifact: &SnapshotArtifact,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!("SELECT EXISTS(SELECT 1 FROM snapshots WHERE sandbox_id=$1 AND local_artifact_id=$2
        AND (artifact_kind IS DISTINCT FROM $3 OR artifact_origin IS DISTINCT FROM $4 OR artifact_snapshot_name IS DISTINCT FROM $5))
        OR EXISTS(SELECT 1 FROM sandboxes WHERE id=$1 AND current_local_artifact_id=$2
        AND (current_local_artifact_kind IS DISTINCT FROM $3 OR current_local_artifact_origin IS DISTINCT FROM $4 OR $5::TEXT IS NOT NULL))"))
        .bind(sandbox).bind(artifact.local_artifact_id()).bind(artifact.kind().as_str()).bind(artifact.origin().as_str())
        .bind(artifact.snapshot_name().map(SnapshotName::as_str)).fetch_one(conn).await
}

#[derive(sqlx::FromRow)]
pub struct NodeIncarnation {
    pub managed_by: SubstrateKind,
    pub node_name: Option<K8sNodeName>,
    pub node_uid: Option<K8sNodeUid>,
}

pub async fn node_incarnation(
    conn: &mut sqlx::PgConnection,
    node: NodeId,
) -> Result<Option<NodeIncarnation>, sqlx::Error> {
    // Lock each relation separately: an outer join cannot lock its nullable side.
    let managed_by = sqlx::query_scalar(sql!(
        "SELECT managed_by FROM nodes WHERE id=$1 AND role='worker' AND state<>'deleted' FOR SHARE"
    ))
    .bind(node)
    .fetch_optional(&mut *conn)
    .await?;
    let Some(managed_by) = managed_by else {
        return Ok(None);
    };
    let binding: Option<(Option<K8sNodeName>, Option<K8sNodeUid>)> = sqlx::query_as(sql!("SELECT k8s_node_name,NULLIF(BTRIM(k8s_node_uid),'') FROM node_k8s WHERE node_id=$1 FOR SHARE"))
        .bind(node).fetch_optional(conn).await?;
    let (node_name, node_uid) = binding.unwrap_or_default();
    Ok(Some(NodeIncarnation {
        managed_by,
        node_name,
        node_uid,
    }))
}

/// Retirement alone does not erase the source's authority. The caller must
/// also apply the generation supersession fence and locked placement check.
#[derive(sqlx::FromRow)]
pub struct ExecutionIncarnation {
    pub node_name: Option<K8sNodeName>,
    pub node_uid: Option<K8sNodeUid>,
}

pub async fn execution_incarnation(
    conn: &mut sqlx::PgConnection,
    sandbox: SandboxId,
    generation: ExecutionGeneration,
) -> Result<Option<ExecutionIncarnation>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT k8s_node_name AS node_name, NULLIF(BTRIM(k8s_node_uid),'') AS node_uid FROM sandbox_k8s_executions
        WHERE sandbox_id=$1 AND generation=$2 AND state<>'replaced' FOR SHARE"
    ))
    .bind(sandbox).bind(generation.get()).fetch_optional(conn).await
}

/// Called only for the row this transaction inserted after proving its source.
/// Preserve capture freshness across attempts; heartbeat time describes liveness.
pub async fn certify(
    conn: &mut sqlx::PgConnection,
    attempt: SnapshotId,
    node: NodeId,
    uid: &K8sNodeUid,
    prior: Option<SnapshotId>,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!("UPDATE snapshots SET source_node_uid=$3,
        timestamp=COALESCE((SELECT timestamp FROM snapshots WHERE id=$4),timestamp), last_attempt_at=NOW()
        WHERE id=$1 AND source_node_id=$2 AND source_node_uid IS NULL AND state='creating'"))
        .bind(attempt).bind(node).bind(uid).bind(prior).execute(conn).await?.rows_affected()==1)
}

/// Authentication and incarnation checks run before this guarded heartbeat.
pub async fn touch(
    conn: &mut sqlx::PgConnection,
    attempt: SnapshotId,
    node: NodeId,
    uid: Option<&K8sNodeUid>,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE snapshots SET last_attempt_at=NOW() WHERE id=$1 AND state='creating'
        AND source_node_id=$2 AND source_node_uid IS NOT DISTINCT FROM $3"
    ))
    .bind(attempt)
    .bind(node)
    .bind(uid)
    .execute(conn)
    .await?;
    Ok(())
}

/// Grant-backed bytes never become ordinary retry authority after expiry/consumption.
pub async fn has_recovery_provenance(
    conn: &mut sqlx::PgConnection,
    sandbox: SandboxId,
    artifact: LocalArtifactId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!("SELECT EXISTS(SELECT 1 FROM snapshots WHERE sandbox_id=$1 AND local_artifact_id=$2 AND recovery_grant_id IS NOT NULL) OR EXISTS(SELECT 1 FROM backup_recovery_grants WHERE sandbox_id=$1 AND local_artifact_id=$2)"))
        .bind(sandbox).bind(artifact).fetch_one(conn).await
}
