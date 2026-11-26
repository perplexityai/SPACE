//! The execution a warm-pool claim binds to the sandbox it creates: the
//! claimed slot's pod becomes that sandbox's first generation.
//!
//! The caller keeps its transaction, the slot's bind stamp and row lock it
//! takes first, and the read of the slot's pod this insert records.
//!
//! [`k8s_executions::claim::retire_pool_reserved_generation`](super::claim::retire_pool_reserved_generation)
//! is the other generation-1 insert.

use pplx_space_cluster_pool::ClaimedSlotBindSource;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use uuid::Uuid;

/// Insert generation `1` of `sandbox_id`, active and `running`, tracking the
/// claimed slot's pod as `source` reads it, with the slot's id as its route
/// activation. A pod UID stamps `pod_materialized_at`.
///
/// Runs in the caller's transaction, after it bound the slot to the sandbox.
/// It refuses a pool because the write is correct only inside that
/// transaction, and the caller must pass its open transaction. The sandbox
/// is new, so no other execution exists: an existing generation `1` or
/// active execution fails the insert on the table's unique indexes instead.
pub async fn insert_first_generation<C>(
    conn: &mut C,
    sandbox_id: SandboxId,
    source: &ClaimedSlotBindSource,
    route_activation_id: Uuid,
) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip,
             k8s_node_name, state, active, route_activation_id, pod_materialized_at
         )
         VALUES (
             $1, 1, $2, $3, $4, $5::inet, $6, 'running', TRUE, $7,
             CASE WHEN NULLIF(BTRIM($4::TEXT), '') IS NOT NULL THEN NOW() END
         )"
    ))
    .bind(sandbox_id)
    .bind(&source.pod_name)
    .bind(&source.pod_namespace)
    .bind(source.pod_uid.as_deref())
    .bind(source.pod_ip.as_deref())
    .bind(source.k8s_node_name.as_deref())
    .bind(route_activation_id)
    .execute(conn.into_sqlx())
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
