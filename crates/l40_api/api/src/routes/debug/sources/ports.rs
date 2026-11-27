use pplx_space_sandbox_model::SandboxId;

use pplx_space_sandbox_store::port_exposures::{self, PortExposureRecord};
use sqlx::PgPool;

use crate::routes::debug::{response::PortExposure, sources::begin_read_only};

pub(in crate::routes::debug) async fn fetch(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<Vec<PortExposure>, sqlx::Error> {
    let mut tx = begin_read_only(pool).await?;
    let ports = port_exposures::list(&mut *tx, sandbox_id).await?;
    tx.commit().await?;
    Ok(ports.into_iter().map(PortExposure::from).collect())
}

impl From<PortExposureRecord> for PortExposure {
    fn from(record: PortExposureRecord) -> Self {
        let PortExposureRecord {
            sandbox_id,
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
            updated_at,
        } = record;
        Self {
            sandbox_id,
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
            updated_at,
        }
    }
}
