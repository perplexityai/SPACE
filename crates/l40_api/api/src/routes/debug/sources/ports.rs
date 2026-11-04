use pplx_space_common_core::types::SandboxId;

use pplx_space_common_core::sql;
use sqlx::PgPool;

use crate::routes::debug::{response::PortExposure, sources::begin_read_only};

pub(in crate::routes::debug) async fn fetch(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<Vec<PortExposure>, sqlx::Error> {
    let mut tx = begin_read_only(pool).await?;
    let ports = sqlx::query_as::<_, PortExposure>(sql!(
        "SELECT sandbox_id,
                protocol,
                guest_port,
                visibility,
                source,
                desired_state,
                apply_state,
                last_applied_node_id,
                last_error,
                last_bridge_status,
                last_applied_bridge_generation,
                last_observed_at,
                created_at,
                updated_at
           FROM sandbox_port_exposures
          WHERE sandbox_id = $1
          ORDER BY guest_port ASC, protocol ASC"
    ))
    .bind(sandbox_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(ports)
}
