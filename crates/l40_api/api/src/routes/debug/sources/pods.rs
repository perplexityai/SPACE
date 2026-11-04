use pplx_space_common_core::types::SandboxId;

use pplx_space_common_core::sql;
use sqlx::PgPool;

use crate::routes::debug::{response::PodExecution, sources::begin_read_only};

pub(in crate::routes::debug) async fn fetch(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<Vec<PodExecution>, sqlx::Error> {
    let mut tx = begin_read_only(pool).await?;
    let pods = sqlx::query_as::<_, PodExecution>(sql!(
        "SELECT id,
                sandbox_id,
                generation,
                pod_name,
                pod_namespace,
                NULLIF(BTRIM(pod_uid), '') AS pod_uid,
                host(pod_ip) AS pod_ip,
                k8s_node_name,
                state,
                active,
                created_at,
                updated_at,
                terminated_at
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          ORDER BY created_at DESC, generation DESC"
    ))
    .bind(sandbox_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(pods)
}

#[cfg(test)]
mod tests;
