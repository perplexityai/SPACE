//! The ECW service route's reset: a client that could not reach a sandbox's
//! service port through the route it was handed returns the port to
//! `pending`, so the node applies it again.

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{RouteGeneration, SandboxId};
use pplx_space_sql_locator::sql;

#[cfg(test)]
mod tests;

/// Guarded transition: returns the open TCP exposure of `guest_port` from
/// `applied` to `pending`, clearing what the node last applied and recording
/// `diagnostic` as its last error. Returns the sandbox's active node, which
/// the caller enqueues the desired-state reconcile for, or `None` when the
/// guard matched no row and nothing changed.
///
/// Guarded in the statement, as one snapshot: the row must have an active
/// node and its route generation must be `route_generation`, the active
/// Kubernetes execution's generation, else the Direct route identity's (a
/// Direct sandbox has no execution row); and the exposure must be `applied`
/// on that active node. The caller keeps its transaction, the commit of a
/// no-op and the reconcile enqueue.
///
/// It resets the exposure columns
/// [`crate::port_exposures::invalidate_open_observations`] resets,
/// recording `diagnostic` where that clears `last_error`, so change them
/// together. The caller must pass its open transaction.
pub async fn reset_applied_service_port<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
    guest_port: i32,
    route_generation: RouteGeneration,
    diagnostic: &str,
) -> Result<Option<NodeId>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_scalar::<_, NodeId>(sql!(
        "WITH current_route AS (
             SELECT s.id AS sandbox_id, s.active_node_id
             FROM sandboxes s
             LEFT JOIN sandbox_k8s_executions e
               ON e.sandbox_id = s.id
              AND e.active
             LEFT JOIN sandbox_direct_route_identities direct_route
               ON direct_route.sandbox_id = s.id
             WHERE s.id = $1
               AND s.active_node_id IS NOT NULL
               AND COALESCE(e.generation, direct_route.execution_generation) = $3
         ),
         updated AS (
             UPDATE sandbox_port_exposures spe
             SET apply_state = 'pending',
                 last_applied_node_id = NULL,
                 last_error = $4,
                 last_bridge_status = NULL,
                 last_applied_bridge_generation = NULL,
                 last_observed_at = NULL,
                 updated_at = NOW()
             FROM current_route
             WHERE spe.sandbox_id = current_route.sandbox_id
               AND spe.protocol = 'tcp'
               AND spe.guest_port = $2
               AND spe.desired_state = 'open'
               AND spe.apply_state = 'applied'
               AND spe.last_applied_node_id = current_route.active_node_id
             RETURNING current_route.active_node_id
         )
         SELECT active_node_id FROM updated"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(guest_port)
    .bind(route_generation)
    .bind(diagnostic)
    .fetch_optional(tx.into_sqlx())
    .await?)
}
