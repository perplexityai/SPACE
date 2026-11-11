use pplx_space_common_core::types::SandboxId;

use pplx_space_common_core::sql;
use sqlx::PgPool;

use crate::routes::debug::{response::SandboxRecord, sources::begin_read_only};

pub(in crate::routes::debug) async fn fetch(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<Option<SandboxRecord>, sqlx::Error> {
    let mut tx = begin_read_only(pool).await?;
    let sandbox = sqlx::query_as::<_, SandboxRecord>(sql!(
        "SELECT s.id,
                s.name,
                s.user_id,
                s.org_id,
                s.managed_by,
                s.lifecycle,
                s.placement,
                s.active_node_id,
                host(active_node.address) AS active_node_address,
                active_node.role AS active_node_role,
                active_node.managed_by AS active_node_managed_by,
                active_k8s.k8s_node_name AS active_k8s_node_name,
                s.target_node_id,
                host(target_node.address) AS target_node_address,
                target_node.role AS target_node_role,
                target_node.managed_by AS target_node_managed_by,
                target_k8s.k8s_node_name AS target_k8s_node_name,
                s.placement_changed_at,
                s.template_id,
                s.content_hash,
                s.vcpu,
                s.memory_mib,
                s.disk_mib,
                s.env,
                s.metadata,
                s.auto_lifecycle_json AS auto_lifecycle,
                s.auto_lifecycle_state_json AS auto_lifecycle_state,
                s.next_auto_event_at,
                s.network_policy_override_json,
                s.network_policy_generation,
                s.snapshots_enabled_desired,
                s.last_used_at,
                s.created_at,
                s.updated_at
           FROM sandboxes s
      LEFT JOIN nodes active_node ON active_node.id = s.active_node_id
      LEFT JOIN node_k8s active_k8s ON active_k8s.node_id = active_node.id
      LEFT JOIN nodes target_node ON target_node.id = s.target_node_id
      LEFT JOIN node_k8s target_k8s ON target_k8s.node_id = target_node.id
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(sandbox)
}
