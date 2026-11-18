//! Durable EBS authority. Cloud identifiers survive sandbox-row deletion.
//! The caller retains its transaction and lifecycle locks across guarded steps.

use pplx_space_accounts::{NodeId, OrgId};
use pplx_space_common_core::{sql, types::SandboxId};
use pplx_space_execution_spec::BlockVolumeMount;
use sqlx::{PgExecutor, Postgres, Transaction, types::Json};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
pub enum VolumePhase {
    Requested,
    Provisioning,
    Available,
    Attaching,
    Attached,
    Releasing,
    Detaching,
    Deleting,
    Deleted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
pub enum Initialization {
    Fresh,
    Claimed,
    Ready,
}

#[derive(Clone, Debug, sqlx::FromRow)]
pub struct BlockVolume {
    pub id: Uuid,
    pub sandbox_id: SandboxId,
    pub org_id: OrgId,
    pub managed_by: String,
    pub mount_spec: Json<BlockVolumeMount>,
    pub availability_zone: String,
    pub aws_volume_id: Option<String>,
    pub phase: VolumePhase,
    pub delete_requested: bool,
    pub attachment_epoch: i64,
    pub node_id: Option<NodeId>,
    pub instance_id: Option<String>,
    pub device_name: Option<String>,
    pub execution_id: Option<Uuid>,
    pub released_epoch: Option<i64>,
    pub filesystem_uuid: Uuid,
    pub initialization: Initialization,
    pub initialization_claim: Option<Uuid>,
    pub ever_exposed: bool,
    pub disk_generation: i64,
    pub checkpoint_id: Option<Uuid>,
    pub checkpoint_generation: Option<i64>,
    pub activation_id: Option<Uuid>,
    pub activated_epoch: Option<i64>,
    pub revision: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum VolumeStoreError {
    #[error("block-volume capacity is exhausted, including retained and cleanup-pending disks")]
    Capacity,
    #[error("block-volume ownership or immutable mount intent conflicts with the existing record")]
    Conflict,
    #[error("block-volume database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}

pub struct NewBlockVolume<'a> {
    pub sandbox_id: SandboxId,
    pub org_id: OrgId,
    pub managed_by: &'a str,
    pub mount_spec: &'a BlockVolumeMount,
    pub availability_zone: &'a str,
    pub resource_limit: u32,
}

#[derive(Debug, sqlx::FromRow)]
pub struct CurrentExecution {
    pub execution_generation: Option<i64>,
    pub execution_id: Option<Uuid>,
    pub active_node_id: Option<NodeId>,
    pub target_node_id: Option<NodeId>,
}

/// The resident execution survives completion of its temporary activation
/// claim. Disk authority follows its durable route identity until retirement.
pub async fn current_execution(
    executor: impl PgExecutor<'_>,
    sandbox: SandboxId,
) -> Result<Option<CurrentExecution>, sqlx::Error> {
    sqlx::query_as::<_, CurrentExecution>(sql!(
        "SELECT COALESCE(e.generation, r.execution_generation) AS execution_generation,
                COALESCE(e.route_activation_id, r.activation_attempt_id) AS execution_id,
                s.active_node_id, s.target_node_id
         FROM sandboxes s
         LEFT JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
         LEFT JOIN sandbox_direct_route_identities r ON r.sandbox_id = s.id
         WHERE s.id = $1"
    ))
    .bind(sandbox)
    .fetch_optional(executor)
    .await
}

/// The advisory lock serializes admissions, including creates whose AWS result
/// is still unknown. Deleted tombstones do not consume capacity; all others do.
/// This must share the transaction that records the sandbox's creation intent.
pub async fn reserve(
    tx: &mut Transaction<'_, Postgres>,
    input: NewBlockVolume<'_>,
) -> Result<Uuid, VolumeStoreError> {
    sqlx::query(sql!("SELECT pg_advisory_xact_lock(791471913282021)"))
        .execute(&mut **tx)
        .await?;
    if let Some(existing) = get(&mut **tx, input.sandbox_id).await? {
        return if existing.org_id == input.org_id
            && existing.managed_by == input.managed_by
            && existing.mount_spec.0 == *input.mount_spec
            && existing.availability_zone == input.availability_zone
            && !existing.delete_requested
            && existing.phase != VolumePhase::Deleted
        {
            Ok(existing.id)
        } else {
            Err(VolumeStoreError::Conflict)
        };
    }
    let (count,): (i64,) = sqlx::query_as(sql!(
        "SELECT COUNT(*) FROM sandbox_block_volumes WHERE phase != 'deleted'"
    ))
    .fetch_one(&mut **tx)
    .await?;
    if count >= i64::from(input.resource_limit) {
        return Err(VolumeStoreError::Capacity);
    }
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandbox_block_volumes
         (id, sandbox_id, org_id, managed_by, mount_spec, availability_zone, filesystem_uuid)
         VALUES ($1, $2, $3, $4, $5, $6, $7)"
    ))
    .bind(id)
    .bind(input.sandbox_id)
    .bind(input.org_id)
    .bind(input.managed_by)
    .bind(Json(input.mount_spec))
    .bind(input.availability_zone)
    .bind(Uuid::now_v7())
    .execute(&mut **tx)
    .await?;
    Ok(id)
}

pub async fn get(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<BlockVolume>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT id, sandbox_id, org_id, managed_by, mount_spec, availability_zone,
                aws_volume_id, phase, delete_requested, attachment_epoch,
                node_id, instance_id, device_name, execution_id, released_epoch,
                filesystem_uuid, initialization, initialization_claim, ever_exposed,
                disk_generation, checkpoint_id, checkpoint_generation, activation_id,
                activated_epoch, revision
         FROM sandbox_block_volumes WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor)
    .await
}

pub async fn live_reservations(executor: impl PgExecutor<'_>) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT id FROM sandbox_block_volumes WHERE phase != 'deleted' ORDER BY id"
    ))
    .fetch_all(executor)
    .await
}

/// Resolve a tenant-visible identifier without exposing provider identity.
pub async fn sandbox_for_volume(
    executor: impl PgExecutor<'_>,
    org: OrgId,
    volume: Uuid,
) -> Result<Option<SandboxId>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT sandbox_id FROM sandbox_block_volumes WHERE id = $1 AND org_id = $2"
    ))
    .bind(volume)
    .bind(org)
    .fetch_optional(executor)
    .await
}

/// The sandbox lock serializes this admission with execution activation.
/// A detached disk is necessary but not sufficient: saved RAM still owns its
/// filesystem view, even when there is no live device attachment.
pub async fn request_detached_delete(
    tx: &mut Transaction<'_, Postgres>,
    org: OrgId,
    volume: Uuid,
) -> Result<bool, sqlx::Error> {
    let Some(sandbox) = sandbox_for_volume(&mut **tx, org, volume).await? else {
        return Ok(false);
    };
    let stopped: Option<bool> = sqlx::query_scalar(sql!(
        "SELECT lifecycle = 'stopped' FROM sandboxes WHERE id = $1 AND org_id = $2 FOR UPDATE"
    ))
    .bind(sandbox)
    .bind(org)
    .fetch_optional(&mut **tx)
    .await?;
    // A new operation cannot join sandbox/expiry cleanup while execution is
    // still live or uncertain. Exact operation replays bypass this admission.
    if stopped != Some(true) {
        return Ok(false);
    }
    let deleted: bool = sqlx::query_scalar(sql!(
        "SELECT delete_requested FROM sandbox_block_volumes WHERE id = $1 FOR UPDATE"
    ))
    .bind(volume)
    .fetch_one(&mut **tx)
    .await?;
    if deleted {
        return Ok(true);
    }
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_block_volumes
         SET delete_requested = TRUE, revision = revision + 1, updated_at = NOW()
         WHERE id = $1 AND org_id = $2 AND phase = 'available'
           AND node_id IS NULL AND checkpoint_id IS NULL"
    ))
    .bind(volume)
    .bind(org)
    .execute(&mut **tx)
    .await?
    .rows_affected()
        == 1)
}

#[derive(sqlx::FromRow)]
pub struct BlockVolumeStatus {
    pub id: Uuid,
    pub sandbox_id: SandboxId,
    pub mount_spec: Json<BlockVolumeMount>,
    pub phase: VolumePhase,
}

/// Tenant-scoped status projection. Cloud IDs and attachment authority never
/// enter a public response, and a page is hydrated with one bounded query.
pub async fn statuses(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    sandboxes: &[SandboxId],
) -> Result<Vec<BlockVolumeStatus>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT id, sandbox_id, mount_spec, phase FROM sandbox_block_volumes
         WHERE org_id = $1 AND sandbox_id = ANY($2)"
    ))
    .bind(org_id)
    .bind(sandboxes)
    .fetch_all(executor)
    .await
}

/// A per-row reconciliation lock outlives each AWS request. Leaders changing
/// while an AWS call is in flight cannot issue contradictory commands.
pub async fn lock_next(
    tx: &mut Transaction<'_, Postgres>,
    managed_by: &str,
) -> Result<Option<BlockVolume>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT id, sandbox_id, org_id, managed_by, mount_spec, availability_zone,
                aws_volume_id, phase, delete_requested, attachment_epoch,
                node_id, instance_id, device_name, execution_id, released_epoch,
                filesystem_uuid, initialization, initialization_claim, ever_exposed,
                disk_generation, checkpoint_id, checkpoint_generation, activation_id,
                activated_epoch, revision
         FROM sandbox_block_volumes
         WHERE managed_by = $1 AND phase != 'deleted'
         ORDER BY updated_at, id LIMIT 1 FOR UPDATE SKIP LOCKED"
    ))
    .bind(managed_by)
    .fetch_optional(&mut **tx)
    .await
}

/// Record exactly one cloud observation under the reconciler's row lock.
/// A missing cloud ID never overwrites an existing durable creation receipt.
pub async fn observe(
    executor: impl PgExecutor<'_>,
    volume: &BlockVolume,
    phase: VolumePhase,
    cloud_id: Option<&str>,
    error: Option<&str>,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_block_volumes
         SET phase = $3, aws_volume_id = COALESCE(aws_volume_id, $4),
             last_error = $5, revision = revision + 1, updated_at = NOW()
         WHERE id = $1 AND revision = $2
           AND ($4::text IS NULL OR aws_volume_id IS NULL OR aws_volume_id = $4)"
    ))
    .bind(volume.id)
    .bind(volume.revision)
    .bind(phase)
    .bind(cloud_id)
    .bind(error)
    .execute(executor)
    .await?
    .rows_affected()
        == 1)
}

pub struct AttachmentTarget<'a> {
    pub node_id: NodeId,
    pub instance_id: &'a str,
    pub device_name: &'a str,
    pub execution_id: Uuid,
    pub availability_zone: &'a str,
    pub host_attachment_limit: u32,
}

/// Reserve an attachment slot after placement has verified the host's identity.
/// The same global lock as admission prevents concurrent host overbooking.
pub async fn reserve_attachment(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    target: AttachmentTarget<'_>,
) -> Result<bool, sqlx::Error> {
    sqlx::query(sql!("SELECT pg_advisory_xact_lock(791471913282021)"))
        .execute(&mut **tx)
        .await?;
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_block_volumes
         SET phase = 'attaching', node_id = $2, instance_id = $3,
             device_name = $4, execution_id = $5,
             attachment_epoch = attachment_epoch + 1, released_epoch = NULL,
             revision = revision + 1, updated_at = NOW()
         WHERE sandbox_id = $1 AND phase = 'available' AND NOT delete_requested
           AND node_id IS NULL AND availability_zone = $6
           AND (SELECT COUNT(*) FROM sandbox_block_volumes
                WHERE node_id = $2 AND phase != 'deleted') < $7"
    ))
    .bind(sandbox_id)
    .bind(target.node_id)
    .bind(target.instance_id)
    .bind(target.device_name)
    .bind(target.execution_id)
    .bind(target.availability_zone)
    .bind(i64::from(target.host_attachment_limit))
    .execute(&mut **tx)
    .await?
    .rows_affected()
        == 1)
}

/// Only the first fenced preparation obtains a format claim. A retry that
/// observes `claimed` must inspect the UUID or block, never run mkfs again.
pub async fn claim_initialization(
    executor: impl PgExecutor<'_>,
    volume_id: Uuid,
    execution_id: Uuid,
    epoch: i64,
    claim: Uuid,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_block_volumes
         SET initialization = 'claimed', initialization_claim = $4,
             revision = revision + 1, updated_at = NOW()
         WHERE id = $1 AND execution_id = $2 AND attachment_epoch = $3
           AND phase = 'attached' AND NOT delete_requested
           AND initialization = 'fresh' AND NOT ever_exposed"
    ))
    .bind(volume_id)
    .bind(execution_id)
    .bind(epoch)
    .bind(claim)
    .execute(executor)
    .await?
    .rows_affected()
        == 1)
}

pub async fn complete_initialization(
    executor: impl PgExecutor<'_>,
    volume_id: Uuid,
    execution_id: Uuid,
    epoch: i64,
    claim: Uuid,
    filesystem_uuid: Uuid,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_block_volumes
         SET initialization = 'ready', revision = revision + 1, updated_at = NOW()
         WHERE id = $1 AND execution_id = $2 AND attachment_epoch = $3
           AND initialization_claim = $4 AND filesystem_uuid = $5
           AND phase = 'attached' AND NOT delete_requested
           AND initialization IN ('claimed', 'ready')"
    ))
    .bind(volume_id)
    .bind(execution_id)
    .bind(epoch)
    .bind(claim)
    .bind(filesystem_uuid)
    .execute(executor)
    .await?
    .rows_affected()
        == 1)
}

/// Consume the exact latest retained-disk checkpoint BEFORE starting writable
/// execution. A retry of the same activation is idempotent, not a new replay.
/// Existing execution admission still prevents loading a second VM for it.
pub async fn activate(
    executor: impl PgExecutor<'_>,
    volume_id: Uuid,
    execution_id: Uuid,
    epoch: i64,
    activation_id: Uuid,
    checkpoint_id: Option<Uuid>,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_block_volumes
         SET disk_generation = CASE WHEN activation_id = $4 AND activated_epoch = $3
                                    THEN disk_generation
                                    ELSE disk_generation + 1 END,
             activation_id = $4, activated_epoch = attachment_epoch, ever_exposed = TRUE,
             phase = 'attached',
             checkpoint_id = NULL, checkpoint_generation = NULL,
             revision = revision + 1, updated_at = NOW()
         WHERE id = $1 AND execution_id = $2 AND attachment_epoch = $3
           AND phase IN ('attached', 'releasing')
           AND released_epoch IS DISTINCT FROM attachment_epoch
           AND initialization = 'ready' AND NOT delete_requested
           AND ((activation_id = $4 AND activated_epoch = attachment_epoch)
                OR (phase = 'attached' AND (
                    (NOT ever_exposed AND $5::uuid IS NULL)
                    OR (checkpoint_id = $5 AND checkpoint_generation = disk_generation))))"
    ))
    .bind(volume_id)
    .bind(execution_id)
    .bind(epoch)
    .bind(activation_id)
    .bind(checkpoint_id)
    .execute(executor)
    .await?
    .rows_affected()
        == 1)
}

/// Publication alone does not authorize detach. The exact owner must separately
/// prove that its VM has been reaped and all local device references released.
pub async fn publish_checkpoint(
    executor: impl PgExecutor<'_>,
    volume_id: Uuid,
    execution_id: Uuid,
    epoch: i64,
    generation: i64,
    checkpoint_id: Uuid,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_block_volumes
         SET checkpoint_id = $5, checkpoint_generation = disk_generation,
             phase = 'releasing', revision = revision + 1, updated_at = NOW()
         WHERE id = $1 AND execution_id = $2 AND attachment_epoch = $3
           AND disk_generation = $4 AND phase IN ('attached', 'releasing')
           AND initialization = 'ready' AND ever_exposed AND NOT delete_requested
           AND (checkpoint_id IS NULL OR checkpoint_id = $5)"
    ))
    .bind(volume_id)
    .bind(execution_id)
    .bind(epoch)
    .bind(generation)
    .bind(checkpoint_id)
    .execute(executor)
    .await?
    .rows_affected()
        == 1)
}

pub async fn acknowledge_release(
    executor: impl PgExecutor<'_>,
    volume_id: Uuid,
    execution_id: Uuid,
    epoch: i64,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_block_volumes
         SET phase = 'detaching', released_epoch = attachment_epoch,
             revision = revision + 1, updated_at = NOW()
         WHERE id = $1 AND execution_id = $2 AND attachment_epoch = $3
           AND phase IN ('attached', 'releasing', 'detaching')"
    ))
    .bind(volume_id)
    .bind(execution_id)
    .bind(epoch)
    .execute(executor)
    .await?
    .rows_affected()
        == 1)
}

/// The controller calls this only after observing AWS attachment absence.
pub async fn finish_detach(
    executor: impl PgExecutor<'_>,
    volume: &BlockVolume,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_block_volumes
         SET phase = 'available', node_id = NULL, instance_id = NULL,
             device_name = NULL, execution_id = NULL,
             revision = revision + 1, updated_at = NOW()
         WHERE id = $1 AND revision = $2 AND phase = 'detaching'
           AND released_epoch = attachment_epoch"
    ))
    .bind(volume.id)
    .bind(volume.revision)
    .execute(executor)
    .await?
    .rows_affected()
        == 1)
}

pub async fn request_delete(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandbox_block_volumes
         SET delete_requested = TRUE, checkpoint_id = NULL, checkpoint_generation = NULL,
             revision = revision + 1, updated_at = NOW()
         WHERE sandbox_id = $1 AND phase != 'deleted'"
    ))
    .bind(sandbox_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Failed-create and legacy delete writers may remove the central row without
/// calling the canonical teardown helper. Reservations commit with their owner,
/// so an absent owner is deletion intent, never evidence of device release.
pub async fn request_orphan_deletion(
    executor: impl PgExecutor<'_>,
    volume_id: Uuid,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_block_volumes v
         SET delete_requested = TRUE, checkpoint_id = NULL, checkpoint_generation = NULL,
             revision = revision + 1, updated_at = NOW()
         WHERE id = $1 AND NOT delete_requested AND phase != 'deleted'
           AND NOT EXISTS (SELECT 1 FROM sandboxes s WHERE s.id = v.sandbox_id)"
    ))
    .bind(volume_id)
    .execute(executor)
    .await?
    .rows_affected()
        == 1)
}

#[cfg(test)]
mod tests;
