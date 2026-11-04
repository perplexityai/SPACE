use pplx_space_common_core::types::SandboxId;

use pplx_space_common_core::sql;
use sqlx::PgPool;

use crate::routes::debug::{response::ObservedResources, sources::begin_read_only};

pub(in crate::routes::debug) async fn fetch(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<Option<ObservedResources>, sqlx::Error> {
    let mut tx = begin_read_only(pool).await?;
    let observed = sqlx::query_as::<_, ObservedResources>(sql!(
        "SELECT sos.sandbox_id,
                sos.node_id,
                host(n.address) AS node_address,
                n.role AS node_role,
                n.managed_by AS node_managed_by,
                nk.k8s_node_name,
                sos.status,
                sos.pid,
                sos.namespace,
                sos.uptime,
                sos.sandbox_owner AS sandbox_user,
                sos.image_hash,
                sos.snapshots_enabled,
                sos.disk_usage_bytes,
                sos.pool_template,
                sos.failure_reason,
                sos.observed_created_at,
                sos.network_tx_bytes,
                sos.network_rx_bytes,
                sos.missing_publish_count,
                sos.template,
                sos.network_policy_authority,
                sos.auto_lifecycle_authority,
                sos.observed_at,
                sos.updated_at
           FROM sandbox_observed_states sos
      LEFT JOIN nodes n ON n.id = sos.node_id
      LEFT JOIN node_k8s nk ON nk.node_id = n.id
          WHERE sos.sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(observed)
}
