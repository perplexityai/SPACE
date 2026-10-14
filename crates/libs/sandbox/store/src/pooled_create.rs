use super::*;
use pplx_space_common_core::types::{AutoLifecyclePolicy, DiskIoLimits};

pub struct PooledSandboxCreate<'a> {
    pub id: SandboxId,
    pub name: Option<&'a str>,
    pub user_id: UserId,
    pub org_id: OrgId,
    pub managed_by: &'a str,
    pub node_id: Option<NodeId>,
    pub template: &'a str,
    pub content_hash: &'a str,
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
    pub disk_mib: Option<i32>,
    pub disk_io_limits: DiskIoLimits,
    pub env: &'a BTreeMap<String, String>,
    pub metadata: &'a SandboxMetadata,
    pub auto_lifecycle: &'a AutoLifecyclePolicy,
    pub network_policy_override: Option<JsonValue>,
    pub volume_encryption: bool,
}

/// Record ownership and encryption intent in the pool-claim transaction,
/// before the node can request the organization's volume key.
pub async fn insert_pooled(
    executor: impl sqlx::PgExecutor<'_>,
    create: PooledSandboxCreate<'_>,
) -> Result<(), SandboxStoreError> {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, name, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, placement_changed_at,
             template_id, content_hash, vcpu, memory_mib, disk_mib,
             disk_io_max_operations_per_sec,
             disk_io_max_bandwidth_mib_per_sec, env, metadata,
             auto_lifecycle_json, network_policy_override_json, network_policy_generation,
             volume_encryption
         )
         VALUES (
             $1, $2, $3, $4, $5, 'paused', 'on_node',
             $6, $6, NOW(),
             $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, 1, $18
         )"
    ))
    .bind(create.id)
    .bind(create.name)
    .bind(create.user_id)
    .bind(create.org_id)
    .bind(create.managed_by)
    .bind(create.node_id)
    .bind(create.template)
    .bind(create.content_hash)
    .bind(create.vcpu)
    .bind(create.memory_mib)
    .bind(create.disk_mib)
    .bind(i64::try_from(create.disk_io_limits.max_operations_per_sec()).unwrap_or(i64::MAX))
    .bind(i64::try_from(create.disk_io_limits.max_bandwidth_mib_per_sec()).unwrap_or(i64::MAX))
    .bind(sqlx::types::Json(create.env))
    .bind(sqlx::types::Json(create.metadata))
    .bind(sqlx::types::Json(create.auto_lifecycle))
    .bind(create.network_policy_override)
    .bind(create.volume_encryption)
    .execute(executor)
    .await?;
    Ok(())
}
