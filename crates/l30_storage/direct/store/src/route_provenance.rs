//! `sandbox_direct_route_provenance`: the highest route and execution
//! generations a Direct sandbox has reserved, one row per sandbox. It
//! outlives the route identity, so a later activation allocates past every
//! generation the sandbox ever reserved.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{ExecutionGeneration, RouteGeneration, SandboxId};
use pplx_space_sql_locator::sql;
use sqlx::FromRow;

/// The generations [`advance_generations`] reserved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromRow)]
pub struct ReservedGenerations {
    pub last_route_generation: i64,
    pub last_execution_generation: i64,
}

/// Raises the sandbox's provenance to `route_generation` and
/// `execution_generation`, creating it when absent. Each column keeps the
/// higher of its stored and given values, so it never moves back.
pub async fn retain_generations(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    route_generation: RouteGeneration,
    execution_generation: ExecutionGeneration,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_provenance (
             sandbox_id,
             last_route_generation,
             last_execution_generation,
             updated_at
         )
         VALUES ($1, $2, $3, NOW())
         ON CONFLICT (sandbox_id) DO UPDATE
         SET last_route_generation = GREATEST(
                 sandbox_direct_route_provenance.last_route_generation,
                 EXCLUDED.last_route_generation
             ),
             last_execution_generation = GREATEST(
                 sandbox_direct_route_provenance.last_execution_generation,
                 EXCLUDED.last_execution_generation
             ),
             updated_at = NOW()"
    ))
    .bind(sandbox_id)
    .bind(route_generation)
    .bind(execution_generation)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Raises the sandbox's provenance to the generations of its current route
/// identity, the same way [`retain_generations`] does. Returns `false` when
/// the sandbox has no route identity, and then writes nothing.
pub async fn retain_route_identity_generations(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, DbError> {
    let retained = sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_provenance (
             sandbox_id,
             last_route_generation,
             last_execution_generation,
             updated_at
         )
         SELECT sandbox_id,
                route_generation,
                execution_generation,
                NOW()
         FROM sandbox_direct_route_identities
         WHERE sandbox_id = $1
         ON CONFLICT (sandbox_id) DO UPDATE
         SET last_route_generation = GREATEST(
                 sandbox_direct_route_provenance.last_route_generation,
                 EXCLUDED.last_route_generation
             ),
             last_execution_generation = GREATEST(
                 sandbox_direct_route_provenance.last_execution_generation,
                 EXCLUDED.last_execution_generation
             ),
             updated_at = NOW()"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(executor.into_sqlx())
    .await?;
    Ok(retained.rows_affected() > 0)
}

/// Creates the sandbox's provenance at generation 1 for both columns.
/// Returns `false`, and changes nothing, when the sandbox already has one.
pub async fn initialize(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, DbError> {
    let inserted = sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_provenance (
             sandbox_id,
             last_route_generation,
             last_execution_generation
         )
         VALUES ($1, 1, 1)
         ON CONFLICT (sandbox_id) DO NOTHING"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(executor.into_sqlx())
    .await?;
    Ok(inserted.rows_affected() == 1)
}

/// Reserves the next generation for an activation: sets both columns to one
/// past the higher of the two and returns them. Returns `None` when the
/// sandbox has no provenance.
pub async fn advance_generations(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<ReservedGenerations>, DbError> {
    Ok(sqlx::query_as::<_, ReservedGenerations>(sql!(
        "UPDATE sandbox_direct_route_provenance
         SET last_route_generation = GREATEST(
                 sandbox_direct_route_provenance.last_route_generation,
                 sandbox_direct_route_provenance.last_execution_generation
             ) + 1,
             last_execution_generation = GREATEST(
                 sandbox_direct_route_provenance.last_route_generation,
                 sandbox_direct_route_provenance.last_execution_generation
             ) + 1,
             updated_at = NOW()
         WHERE sandbox_id = $1
         RETURNING last_route_generation, last_execution_generation"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_optional(executor.into_sqlx())
    .await?)
}
