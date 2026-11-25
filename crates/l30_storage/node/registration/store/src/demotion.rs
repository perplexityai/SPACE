//! Demoting `ready` Kubernetes nodes that a full-cluster membership snapshot
//! no longer lists back to `registered`.

use pplx_space_node_model::SubstrateKind;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;

/// Demote up to `max_per_tick` `ready` Kubernetes nodes outside any drain
/// whose Kubernetes node name is not in `live_node_names`, and whose
/// heartbeat (if any) and `node_k8s.updated_at` are both older than
/// `missing_grace_secs` on the database clock, oldest registration first.
/// Returns the Kubernetes node names it demoted, unordered.
///
/// The `UPDATE` re-checks, on the `nodes` row it writes, each candidate's
/// substrate, state and drain phase, and that its heartbeat is still the one
/// the scan read. A candidate another writer moved to another substrate, out
/// of `ready`, into a drain or to another heartbeat in between is left as it
/// is and not returned. Node and sandbox placement rows are never deleted.
pub async fn demote_unobserved_ready_nodes(
    executor: impl PgExecutor<'_>,
    live_node_names: &[String],
    missing_grace_secs: i64,
    max_per_tick: i64,
) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>(sql!(
        "WITH candidates AS MATERIALIZED (
             SELECT candidate.id,
                    candidate.last_heartbeat,
                    candidate_k8s.updated_at AS k8s_updated_at
             FROM nodes AS candidate
             JOIN node_k8s AS candidate_k8s
               ON candidate_k8s.node_id = candidate.id
             WHERE candidate.managed_by = $4
               AND candidate.state = 'ready'
               AND candidate.drain_phase IS NULL
               AND NOT (candidate_k8s.k8s_node_name = ANY($1::TEXT[]))
               AND (
                    candidate.last_heartbeat IS NULL
                    OR candidate.last_heartbeat
                       < NOW() - ($2 * INTERVAL '1 second')
               )
               AND candidate_k8s.updated_at
                   < NOW() - ($2 * INTERVAL '1 second')
             ORDER BY candidate_k8s.updated_at, candidate.id
             LIMIT $3
         )
         UPDATE nodes AS n
         SET state = 'registered'
         FROM node_k8s AS nk
         JOIN candidates AS candidate
           ON candidate.id = nk.node_id
         WHERE n.id = candidate.id
           AND n.managed_by = $4
           AND n.state = 'ready'
           AND n.drain_phase IS NULL
           AND n.last_heartbeat IS NOT DISTINCT FROM candidate.last_heartbeat
           AND nk.updated_at = candidate.k8s_updated_at
         RETURNING nk.k8s_node_name"
    ))
    .bind(live_node_names)
    .bind(missing_grace_secs)
    .bind(max_per_tick)
    .bind(SubstrateKind::Kubernetes)
    .fetch_all(executor)
    .await
}

#[cfg(test)]
mod tests;
