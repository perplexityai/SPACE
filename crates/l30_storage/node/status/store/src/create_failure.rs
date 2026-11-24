//! The Direct substrate's write after a worker refused or failed a create:
//! the node's snapshot says it can't take work until its next publication or
//! a short window passes.

use pplx_space_accounts::NodeId;
use pplx_space_sql_locator::sql;

/// Insert or update `node_id`'s snapshot after a create on it failed: observed
/// now, stale `stale_after_secs` from now, with `reachable`, the node's role,
/// `scheduling_status`, and `reason` as the scheduling reason and, when
/// unreachable, as the error; a reachable failure leaves no error. An
/// unreachable node is unhealthy. On an existing snapshot, a status of `full`,
/// `draining` or `unavailable` zeroes the ready network slots, a reachable
/// failure keeps the stored health, and the other columns keep their stored
/// values. A first snapshot has no ready-slot count, an empty service list
/// and, after a reachable failure, no health. Writes nothing when the node
/// isn't registered.
pub async fn record_create_failure(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    reachable: bool,
    reason: &str,
    stale_after_secs: i64,
    scheduling_status: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id,
             observed_at,
             stale_after,
             reachable,
             error,
             role,
             scheduling_status,
             scheduling_reason,
             health_healthy,
             services,
             updated_at
         )
         SELECT n.id,
                NOW(),
                NOW() + ($4::TEXT || ' seconds')::INTERVAL,
                $2,
                CASE WHEN $2 THEN NULL ELSE $3 END,
                n.role,
                $5,
                $3,
                CASE WHEN $2 THEN NULL ELSE FALSE END,
                '[]'::JSONB,
                NOW()
         FROM nodes n
         WHERE n.id = $1
         ON CONFLICT (node_id) DO UPDATE
         SET observed_at = EXCLUDED.observed_at,
             stale_after = EXCLUDED.stale_after,
             reachable = EXCLUDED.reachable,
             error = EXCLUDED.error,
             role = EXCLUDED.role,
             scheduling_status = EXCLUDED.scheduling_status,
             scheduling_reason = EXCLUDED.scheduling_reason,
             network_slots_ready = CASE
                 WHEN EXCLUDED.scheduling_status IN ('full', 'draining', 'unavailable')
                 THEN 0
                 ELSE node_status_snapshots.network_slots_ready
             END,
             health_healthy = CASE
                 WHEN EXCLUDED.reachable THEN node_status_snapshots.health_healthy
                 ELSE FALSE
             END,
             updated_at = NOW()"
    ))
    .bind(node_id)
    .bind(reachable)
    .bind(reason)
    .bind(stale_after_secs)
    .bind(scheduling_status)
    .execute(executor)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
