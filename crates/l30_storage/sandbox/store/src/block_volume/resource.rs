//! Independent resource lifetime, distinct from a sandbox's logical reservation
//! and from its physical attachment. All mutations share the existing authority.
use super::{VolumePhase, VolumeStoreError};
use pplx_space_accounts::OrgId;
use pplx_space_common_core::{sql, types::SandboxId};
use pplx_space_execution_spec::BlockVolumeMount;
use sqlx::{PgExecutor, Postgres, Transaction, types::Json};
use uuid::Uuid;

pub fn controller_scope(managed_by: &str) -> String {
    format!("{managed_by}/independent-volumes-v1")
}

#[derive(Debug, sqlx::FromRow)]
pub struct Resource {
    pub id: Uuid,
    pub org_id: OrgId,
    pub sandbox_id: Option<SandboxId>,
    pub mount_spec: Option<Json<BlockVolumeMount>>,
    pub availability_zone: String,
    pub aws_volume_id: Option<String>,
    pub size_gib: i32,
    pub phase: VolumePhase,
    pub delete_requested: bool,
    pub retained: bool,
    pub revision: i64,
}

/// Includes legacy resources so management does not change their ownership.
pub async fn get(
    executor: impl PgExecutor<'_>,
    org: OrgId,
    id: Uuid,
) -> Result<Option<Resource>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT id, org_id, sandbox_id, mount_spec, availability_zone, aws_volume_id,
                COALESCE(size_gib, (mount_spec->>'size_gib')::integer) AS size_gib,
                phase, delete_requested, retained, revision
         FROM sandbox_block_volumes WHERE id = $1 AND org_id = $2"
    ))
    .bind(id)
    .bind(org)
    .fetch_optional(executor)
    .await
}

pub async fn list(
    executor: impl PgExecutor<'_>,
    org: OrgId,
    after: Option<Uuid>,
) -> Result<Vec<Resource>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT id, org_id, sandbox_id, mount_spec, availability_zone, aws_volume_id,
                size_gib, phase, delete_requested, retained, revision
         FROM sandbox_block_volumes
         WHERE org_id = $1 AND retained AND ($2::uuid IS NULL OR id > $2)
         ORDER BY id LIMIT 100"
    ))
    .bind(org)
    .bind(after)
    .fetch_all(executor)
    .await
}

pub struct NewResource<'a> {
    pub id: Uuid,
    pub org_id: OrgId,
    pub managed_by: &'a str,
    pub availability_zone: &'a str,
    pub resource_limit: u32,
}

pub async fn reserve(
    tx: &mut Transaction<'_, Postgres>,
    input: NewResource<'_>,
) -> Result<(), VolumeStoreError> {
    sqlx::query(sql!("SELECT pg_advisory_xact_lock(791471913282021)"))
        .execute(&mut **tx)
        .await?;
    let count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_block_volumes WHERE phase != 'deleted'"
    ))
    .fetch_one(&mut **tx)
    .await?;
    if count >= i64::from(input.resource_limit) {
        return Err(VolumeStoreError::Capacity);
    }
    sqlx::query(sql!(
        "INSERT INTO sandbox_block_volumes
         (id, org_id, managed_by, availability_zone, filesystem_uuid, retained, size_gib)
         VALUES ($1, $2, $3, $4, $5, TRUE, 128)"
    ))
    .bind(input.id)
    .bind(input.org_id)
    .bind(controller_scope(input.managed_by))
    .bind(input.availability_zone)
    .bind(Uuid::now_v7())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Called in the sandbox insertion transaction. A suspended sandbox retains
/// sandbox_id even after physical detach, and therefore cannot lose its disk.
pub async fn bind(
    tx: &mut Transaction<'_, Postgres>,
    org: OrgId,
    id: Uuid,
    sandbox: SandboxId,
    managed_by: &str,
    zone: &str,
    mount: &BlockVolumeMount,
) -> Result<(), VolumeStoreError> {
    let changed = sqlx::query(sql!(
        "UPDATE sandbox_block_volumes SET sandbox_id = $3, mount_spec = $4,
             revision = revision + 1, updated_at = NOW()
         WHERE id = $1 AND org_id = $2 AND retained AND managed_by = $5
           AND availability_zone = $6 AND size_gib = ($4->>'size_gib')::integer
           AND NOT delete_requested AND phase = 'available'
           AND node_id IS NULL AND checkpoint_id IS NULL AND activation_id IS NULL
           AND (sandbox_id IS NULL OR (sandbox_id = $3 AND mount_spec = $4))"
    ))
    .bind(id)
    .bind(org)
    .bind(sandbox)
    .bind(Json(mount))
    .bind(controller_scope(managed_by))
    .bind(zone)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    if changed != 1 {
        return Err(VolumeStoreError::Conflict);
    }
    Ok(())
}

/// Only an absent sandbox releases the logical reservation. Never infer that
/// from phase=available: suspend deliberately preserves the memory dependency.
pub async fn release_deleted_owner(
    executor: impl PgExecutor<'_>,
    id: Uuid,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_block_volumes v
         SET sandbox_id = NULL, mount_spec = NULL, checkpoint_id = NULL,
             checkpoint_generation = NULL, activation_id = NULL,
             revision = revision + 1, updated_at = NOW()
         WHERE id = $1 AND retained AND sandbox_id IS NOT NULL
           AND phase = 'available' AND node_id IS NULL
           AND NOT EXISTS (SELECT 1 FROM sandboxes s WHERE s.id = v.sandbox_id)"
    ))
    .bind(id)
    .execute(executor)
    .await?
    .rows_affected()
        == 1)
}

pub async fn request_delete(
    tx: &mut Transaction<'_, Postgres>,
    org: OrgId,
    id: Uuid,
    operation: Uuid,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_block_volumes SET delete_requested = TRUE, delete_operation_id = $3,
             revision = revision + 1, updated_at = NOW()
         WHERE id = $1 AND org_id = $2 AND retained AND sandbox_id IS NULL
           AND node_id IS NULL AND checkpoint_id IS NULL AND phase = 'available'"
    ))
    .bind(id)
    .bind(org)
    .bind(operation)
    .execute(&mut **tx)
    .await?
    .rows_affected()
        == 1)
}

pub async fn lock_next(
    tx: &mut Transaction<'_, Postgres>,
    managed_by: &str,
) -> Result<Option<Resource>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT id, org_id, sandbox_id, mount_spec, availability_zone, aws_volume_id,
                size_gib, phase, delete_requested, retained, revision
         FROM sandbox_block_volumes
         WHERE managed_by = $1 AND retained AND sandbox_id IS NULL AND phase != 'deleted'
         ORDER BY updated_at, id LIMIT 1 FOR UPDATE SKIP LOCKED"
    ))
    .bind(controller_scope(managed_by))
    .fetch_optional(&mut **tx)
    .await
}

pub async fn observe(
    executor: impl PgExecutor<'_>,
    row: &Resource,
    phase: VolumePhase,
    cloud_id: Option<&str>,
    error: Option<&str>,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_block_volumes
         SET phase = $3, aws_volume_id = COALESCE(aws_volume_id, $4), last_error = $5,
             revision = revision + 1, updated_at = NOW()
         WHERE id = $1 AND revision = $2
           AND ($4::text IS NULL OR aws_volume_id IS NULL OR aws_volume_id = $4)"
    ))
    .bind(row.id)
    .bind(row.revision)
    .bind(phase)
    .bind(cloud_id)
    .bind(error)
    .execute(executor)
    .await?
    .rows_affected()
        == 1)
}
