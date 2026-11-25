//! The internal API's `BeginArtifactBackup` execution-generation fence: the
//! share lock of the executions at and above a capture's generation, with
//! the columns the fence's supersession predicate reads.
//!
//! The caller keeps its transaction, the snapshot insert that already holds
//! the sandbox row's key-share lock, and the supersession decision
//! (`pplx_space_sandbox_portability::execution_supersession`).

use pplx_space_sandbox_model::{ExecutionGeneration, SandboxId};
use pplx_space_sql_locator::sql;
use sqlx::{Postgres, Transaction};

/// One execution row as [`share_lock_from_generation`] reads it, decoded by
/// column name so a reordered `SELECT` can't swap two of the flags.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct FenceRow {
    pub generation: i64,
    pub active: bool,
    /// The execution was retired as `replaced`.
    pub replaced: bool,
    /// The execution recorded a pod: a materialization time or a non-blank
    /// pod UID.
    pub materialized: bool,
}

/// Lock the sandbox's executions at and above `generation` `FOR SHARE` and
/// read them. The lock lasts until the caller's transaction ends: it holds
/// those rows against a concurrent retire, and a retire that commits while
/// this waits is re-read.
pub async fn share_lock_from_generation(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    generation: ExecutionGeneration,
) -> Result<Vec<FenceRow>, sqlx::Error> {
    sqlx::query_as::<_, FenceRow>(sql!(
        "SELECT generation, active, state = 'replaced' AS replaced,
                pod_materialized_at IS NOT NULL OR NULLIF(BTRIM(pod_uid), '') IS NOT NULL
                    AS materialized
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
            AND generation >= $2
            FOR SHARE"
    ))
    .bind(sandbox_id)
    .bind(generation.get())
    .fetch_all(&mut **tx)
    .await
}

#[cfg(test)]
mod tests;
