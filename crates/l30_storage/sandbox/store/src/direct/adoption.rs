//! The reads a Direct live-execution adoption decides from: whether an
//! unplaced row's route identity trails the execution a worker still runs,
//! and the candidate row the adoption locks before
//! [`super::adopt_live_execution`] places it. Both read the row with its
//! route identity (`sandbox_direct_route_identities`) and provenance
//! (`sandbox_direct_route_provenance`) in one statement, so they come from
//! one snapshot.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use pplx_space_sql_locator::sql;

#[cfg(test)]
mod tests;

/// Whether the Direct row with no active node, still in `read_lifecycle`
/// (compared case- and whitespace-insensitively) and holding no
/// restore-lease token, has a route identity on `node_id` older than
/// `record_generation`, a generation its provenance has already reserved.
/// Takes no lock.
pub async fn route_identity_trails_record(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    node_id: NodeId,
    record_generation: i64,
    read_lifecycle: SandboxLifecycle,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT TRUE
               FROM sandboxes s
               JOIN sandbox_direct_route_identities route ON route.sandbox_id = s.id
               JOIN sandbox_direct_route_provenance provenance
                 ON provenance.sandbox_id = s.id
              WHERE s.id = $1
                AND s.managed_by = $4
                AND s.active_node_id IS NULL
                AND LOWER(TRIM(s.lifecycle)) = $5
                AND s.restore_lease_token IS NULL
                AND route.current_node_id = $2
                AND route.execution_generation < $3
                AND provenance.last_execution_generation >= $3"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(node_id.as_uuid())
    .bind(record_generation)
    .bind(SubstrateKind::Space)
    .bind(read_lifecycle.as_str())
    .fetch_optional(executor.into_sqlx())
    .await?
    .is_some())
}

/// The generations [`lock_live_adoption_candidate`] read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::FromRow)]
pub struct LiveAdoptionGenerations {
    /// The route identity's execution generation.
    pub execution_generation: i64,
    /// The provenance's last reserved execution generation.
    pub last_execution_generation: i64,
}

/// Locks the `sandboxes` row a live execution on `node_id` at
/// `execution_generation` may be adopted into, and returns its route
/// identity's and provenance's generations; `None` when the row fails the
/// guard. The lock (`FOR NO KEY UPDATE OF s`) lasts until the caller's
/// transaction ends.
///
/// Guarded in the statement: Direct's row, no active node, still in
/// `read_lifecycle` (compared case- and whitespace-insensitively), no live
/// restore lease, a route identity on `node_id` no newer than
/// `execution_generation`, a provenance that has reserved it, and either no
/// restore-lease token or all three at that one generation. A row that
/// fails the guard in the statement's snapshot returns no row without
/// waiting, so a row another restore owns under a live lease is not waited
/// on. A row that passes it after a wait is re-checked against its new
/// version.
///
/// Only the `sandboxes` row is locked. `NO KEY UPDATE` is the lock an update
/// of the row takes; `FOR UPDATE` would also block the foreign-key check of
/// an insert into the route tables, which a route writer can reach while
/// holding its route row. The route rows take no lock: locking them would
/// order the adoption against writers that take the route identity and the
/// provenance in different orders. The caller must pass its open
/// transaction.
pub async fn lock_live_adoption_candidate<C>(
    transaction: &mut C,
    sandbox_id: SandboxId,
    read_lifecycle: SandboxLifecycle,
    node_id: NodeId,
    execution_generation: i64,
) -> Result<Option<LiveAdoptionGenerations>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_as::<_, LiveAdoptionGenerations>(sql!(
        "SELECT route.execution_generation,
                    provenance.last_execution_generation
               FROM sandboxes s
               JOIN sandbox_direct_route_identities route ON route.sandbox_id = s.id
               JOIN sandbox_direct_route_provenance provenance ON provenance.sandbox_id = s.id
              WHERE s.id = $1
                AND s.managed_by = $2
                AND s.active_node_id IS NULL
                AND LOWER(TRIM(s.lifecycle)) = $3
                AND (s.restore_lease_expires_at IS NULL OR s.restore_lease_expires_at <= NOW())
                AND route.current_node_id = $4
                AND route.execution_generation <= $5
                AND provenance.last_execution_generation >= $5
                AND (
                    s.restore_lease_token IS NULL
                    OR (route.execution_generation = $5
                        AND provenance.last_execution_generation = $5)
                )
              FOR NO KEY UPDATE OF s"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(SubstrateKind::Space)
    .bind(read_lifecycle.as_str())
    .bind(node_id)
    .bind(execution_generation)
    .fetch_optional(transaction.into_sqlx())
    .await?)
}
