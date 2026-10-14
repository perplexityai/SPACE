use std::collections::BTreeMap;

use pplx_space_common_core::{
    sql,
    types::{AutoLifecyclePolicy, DiskIoLimits, NodeId, OrgId, SandboxId, UserId},
};
use sqlx::types::JsonValue;
use uuid::Uuid;

use crate::{SandboxMetadata, SandboxStoreError};

/// Values shared by cold creation and warm-pool adoption.
#[derive(Debug)]
pub struct SandboxCreate<'a> {
    pub id: SandboxId,
    pub name: Option<&'a str>,
    pub user_id: UserId,
    pub org_id: OrgId,
    pub managed_by: &'a str,
    pub template: &'a str,
    pub content_hash: &'a str,
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
    pub disk_mib: Option<i32>,
    pub disk_io_limits: DiskIoLimits,
    pub env: &'a BTreeMap<String, String>,
    pub metadata: &'a SandboxMetadata,
    pub auto_lifecycle: &'a AutoLifecyclePolicy,
    pub network_policy_override_json: Option<&'a JsonValue>,
}

/// Insert an unplaced sandbox. A restore token starts it in `restoring` and
/// sets the lease against the database clock; otherwise it starts in `creating`.
pub async fn insert_unplaced(
    executor: impl sqlx::PgExecutor<'_>,
    input: &SandboxCreate<'_>,
    volume_encryption: bool,
    restore_lease_token: Option<Uuid>,
    restore_lease_seconds: i64,
) -> Result<(), SandboxStoreError> {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
            id, name, user_id, org_id, managed_by, lifecycle, placement,
            template_id, content_hash, vcpu, memory_mib, disk_mib,
            disk_io_max_operations_per_sec,
            disk_io_max_bandwidth_mib_per_sec, env,
            metadata, auto_lifecycle_json, network_policy_override_json,
            network_policy_generation, volume_encryption,
            restore_lease_token, restore_lease_expires_at,
            preserve_local_artifact_on_restore_failure
        ) VALUES (
            $1, $2, $3, $4, $5,
            CASE WHEN $18::uuid IS NULL THEN 'creating' ELSE 'restoring' END,
            'unplaced', $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, 1, $17,
            $18,
            CASE WHEN $18::uuid IS NULL
                 THEN NULL
                 ELSE NOW() + make_interval(secs => $19::double precision)
            END,
            $18::uuid IS NOT NULL
        )"
    ))
    .bind(input.id)
    .bind(input.name)
    .bind(input.user_id)
    .bind(input.org_id)
    .bind(input.managed_by)
    .bind(input.template)
    .bind(input.content_hash)
    .bind(input.vcpu)
    .bind(input.memory_mib)
    .bind(input.disk_mib)
    .bind(i64::try_from(input.disk_io_limits.max_operations_per_sec()).unwrap_or(i64::MAX))
    .bind(i64::try_from(input.disk_io_limits.max_bandwidth_mib_per_sec()).unwrap_or(i64::MAX))
    .bind(sqlx::types::Json(input.env))
    .bind(sqlx::types::Json(input.metadata))
    .bind(sqlx::types::Json(input.auto_lifecycle))
    .bind(input.network_policy_override_json)
    .bind(volume_encryption)
    .bind(restore_lease_token)
    .bind(restore_lease_seconds)
    .execute(executor)
    .await?;
    Ok(())
}

/// Insert a sandbox adopted from a warm pool, paused on its existing node.
/// The caller owns the transaction that also records the pool claim.
pub async fn insert_pooled(
    executor: impl sqlx::PgExecutor<'_>,
    input: &SandboxCreate<'_>,
    active_node_id: Option<NodeId>,
) -> Result<(), SandboxStoreError> {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
            id, name, user_id, org_id, managed_by, lifecycle, placement,
            active_node_id, target_node_id, placement_changed_at,
            template_id, content_hash, vcpu, memory_mib, disk_mib,
            disk_io_max_operations_per_sec,
            disk_io_max_bandwidth_mib_per_sec, env, metadata,
            auto_lifecycle_json, network_policy_override_json, network_policy_generation
        )
        VALUES (
            $1, $2, $3, $4, $5, 'paused', 'on_node',
            $6, $6, NOW(),
            $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, 1
        )"
    ))
    .bind(input.id)
    .bind(input.name)
    .bind(input.user_id)
    .bind(input.org_id)
    .bind(input.managed_by)
    .bind(active_node_id)
    .bind(input.template)
    .bind(input.content_hash)
    .bind(input.vcpu)
    .bind(input.memory_mib)
    .bind(input.disk_mib)
    .bind(i64::try_from(input.disk_io_limits.max_operations_per_sec()).unwrap_or(i64::MAX))
    .bind(i64::try_from(input.disk_io_limits.max_bandwidth_mib_per_sec()).unwrap_or(i64::MAX))
    .bind(sqlx::types::Json(input.env))
    .bind(sqlx::types::Json(input.metadata))
    .bind(sqlx::types::Json(input.auto_lifecycle))
    .bind(input.network_policy_override_json)
    .execute(executor)
    .await?;
    Ok(())
}
