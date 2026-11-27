use pplx_space_sandbox_model::SandboxId;

use pplx_space_sandbox_store::debug_reads::{self, ExecutionRow};
use sqlx::PgPool;

use crate::routes::debug::{response::PodExecution, sources::begin_read_only};

pub(in crate::routes::debug) async fn fetch(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<Vec<PodExecution>, sqlx::Error> {
    let mut tx = begin_read_only(pool).await?;
    let pods = debug_reads::executions(&mut *tx, sandbox_id).await?;
    tx.commit().await?;
    Ok(pods.into_iter().map(PodExecution::from).collect())
}

impl From<ExecutionRow> for PodExecution {
    fn from(row: ExecutionRow) -> Self {
        let ExecutionRow {
            id,
            sandbox_id,
            generation,
            pod_name,
            pod_namespace,
            pod_uid,
            pod_ip,
            k8s_node_name,
            state,
            active,
            created_at,
            updated_at,
            terminated_at,
        } = row;
        Self {
            id,
            sandbox_id,
            generation,
            pod_name,
            pod_namespace,
            pod_uid,
            pod_ip,
            k8s_node_name,
            state,
            active,
            created_at,
            updated_at,
            terminated_at,
        }
    }
}

#[cfg(test)]
mod tests;
