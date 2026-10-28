//! Deterministic seed operations for isolated synthetic publication workloads.

pub mod outcome;

use pplx_space_common_core::types::{
    NodeId, SandboxId, SandboxLifecycle, SandboxQuarantineSource, SubstrateKind,
};
use pplx_space_sql_locator::sql;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::{SEED_ORG_ID, SEED_USER_ID, TestDbResult};

pub const TEMPLATE_NAME: &str = "synthetic-template";

pub struct PublishSandboxFixture {
    pub id: SandboxId,
    pub node_id: NodeId,
    pub lifecycle: SandboxLifecycle,
    pub activation_attempt_id: Uuid,
}

/// Insert a fresh template; collisions fail rather than modifying existing rows.
pub async fn insert_template(pool: &PgPool, id: Uuid) -> TestDbResult<()> {
    sqlx::query(sql!("INSERT INTO templates (id, name) VALUES ($1, $2)"))
        .bind(id)
        .bind(TEMPLATE_NAME)
        .execute(pool)
        .await?;
    Ok(())
}

/// Insert a new sandbox and its Kubernetes execution and routing identity in the
/// caller's transaction. Foreign keys take their usual key-share locks;
/// duplicate fixture IDs fail instead of mutating an existing sandbox.
pub async fn insert_sandbox(
    conn: &mut PgConnection,
    fixture: &PublishSandboxFixture,
) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, active_node_id,
             target_node_id, managed_by, network_policy_override_json,
             network_policy_generation
         ) VALUES ($1, $2, $3, $4, 'on_node', $5, $5, $6, '{}'::jsonb, 1)"
    ))
    .bind(fixture.id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(fixture.lifecycle.as_str())
    .bind(fixture.node_id)
    .bind(SubstrateKind::Kubernetes)
    .execute(&mut *conn)
    .await?;
    let creating = fixture.lifecycle == SandboxLifecycle::Stopped;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid,
              pod_ip, state, active, activation_attempt_id, route_activation_id)
         VALUES ($1, 1, $2, 'synthetic', $2, '127.0.0.1', $3, TRUE, $4, $5)"
    ))
    .bind(fixture.id)
    .bind(format!("synthetic-{}", fixture.id))
    .bind(if creating { "creating" } else { "running" })
    .bind(fixture.activation_attempt_id)
    // A cold activation has not established routing yet; its rollback claim
    // must exercise that fence rather than a previously routable execution.
    .bind((!creating).then_some(fixture.id.as_uuid()))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Seed one pause-failure episode on an already quarantined fixture sandbox.
/// Inserts only, with normal FK locks; an existing episode is an error.
pub async fn insert_pause_failed(
    conn: &mut PgConnection,
    sandbox_id: SandboxId,
    node_id: NodeId,
    episode_id: Uuid,
) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_state
             (sandbox_id, quarantine_id, entered_from_lifecycle,
              source, reason_code, next_action, last_observed_node_id)
         VALUES ($1, $2, 'started', $3, 'pause_failed', 'none', $4)"
    ))
    .bind(sandbox_id)
    .bind(episode_id)
    .bind(SandboxQuarantineSource::SbxManager.as_str())
    .bind(node_id)
    .execute(conn)
    .await?;
    Ok(())
}

/// Seed durable suspend evidence atomically, with no access to object storage.
/// Both rows are new, and collisions roll back rather than replacing evidence.
pub async fn insert_ready_suspend(
    pool: &PgPool,
    snapshot_id: Uuid,
    sandbox_id: SandboxId,
    node_id: NodeId,
    artifact_id: Uuid,
) -> TestDbResult<()> {
    let mut tx = pool.begin().await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, source_node_id, org_id,
              local_artifact_id, artifact_kind, artifact_origin)
         VALUES ($1, $2, 'full', 'ready', $3, $4, $5, 'full', 'suspend')"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(node_id)
    .bind(SEED_ORG_ID)
    .bind(artifact_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, 's3://space-publish-synthetic/suspend-snapshot', $2)"
    ))
    .bind(snapshot_id)
    .bind(SEED_ORG_ID)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}
