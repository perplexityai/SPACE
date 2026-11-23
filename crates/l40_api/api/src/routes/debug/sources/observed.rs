use pplx_space_sandbox_model::SandboxId;

use pplx_space_sandbox_store::observed_states::{self, ObservedStateWithNode};
use sqlx::PgPool;

use crate::routes::debug::{response::ObservedResources, sources::begin_read_only};

pub(in crate::routes::debug) async fn fetch(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<Option<ObservedResources>, sqlx::Error> {
    let mut tx = begin_read_only(pool).await?;
    let observed = observed_states::get_with_node(&mut *tx, sandbox_id).await?;
    tx.commit().await?;
    Ok(observed.map(ObservedResources::from))
}

impl From<ObservedStateWithNode> for ObservedResources {
    fn from(row: ObservedStateWithNode) -> Self {
        let ObservedStateWithNode {
            sandbox_id,
            node_id,
            node_address,
            node_role,
            node_managed_by,
            k8s_node_name,
            status,
            pid,
            namespace,
            uptime,
            sandbox_user,
            image_hash,
            snapshots_enabled,
            disk_usage_bytes,
            pool_template,
            failure_reason,
            observed_created_at,
            network_tx_bytes,
            network_rx_bytes,
            missing_publish_count,
            template,
            network_policy_authority,
            auto_lifecycle_authority,
            observed_at,
            updated_at,
        } = row;
        Self {
            sandbox_id,
            node_id,
            node_address,
            node_role,
            node_managed_by,
            k8s_node_name,
            status,
            pid,
            namespace,
            uptime,
            sandbox_user,
            image_hash,
            snapshots_enabled,
            disk_usage_bytes,
            pool_template,
            failure_reason,
            observed_created_at,
            network_tx_bytes,
            network_rx_bytes,
            missing_publish_count,
            template,
            network_policy_authority,
            auto_lifecycle_authority,
            observed_at,
            updated_at,
        }
    }
}
