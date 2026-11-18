//! What a forced v2 drain writes of the executions that still name its node:
//! a released row's execution on the node is retired
//! ([`retire_source_execution`]).

use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

/// Retire `execution_id`, the row's source execution, as `forgotten`, the
/// pod-tracking write-off state: its pod died with the machine. Only that
/// execution, and only while it is active; returns whether it changed.
pub async fn retire_source_execution(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    execution_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let retired = sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE,
                state = 'forgotten',
                pod_ip = NULL,
                terminated_at = COALESCE(terminated_at, NOW()),
                updated_at = NOW()
          WHERE id = $2
            AND sandbox_id = $1
            AND active"
    ))
    .bind(sandbox_id)
    .bind(execution_id)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    Ok(retired == 1)
}

#[cfg(test)]
mod tests;
