//! Which sandbox rows and execution generations own a set of restore pods.

use pplx_space_postgres_pool::Executor;
use pplx_space_sql_locator::sql;
use uuid::Uuid;

use crate::SandboxStoreError;

/// One sandbox row and one of its execution rows, or the row alone (with
/// `NULL` execution columns) when it has none in range. Decoded by column
/// name so a reordered `SELECT` can't swap two of the flags. `managed_by` and
/// `lifecycle` are read as text, for the caller to parse per row so one
/// unrecognized value can't fail the batch. When the LEFT JOIN found no
/// execution, `generation`, `active` and `replaced` are `NULL` and
/// `materialized` reads `FALSE`.
#[derive(Clone, Debug, PartialEq, Eq, sqlx::FromRow)]
pub struct RestorePodOwnerRow {
    pub id: Uuid,
    pub managed_by: String,
    pub lifecycle: String,
    pub generation: Option<i64>,
    pub active: Option<bool>,
    /// The execution's state is `replaced`.
    pub replaced: Option<bool>,
    /// The execution's pod was materialized or bound a pod UID.
    pub materialized: Option<bool>,
}

/// For each of `sandbox_ids`, paired by position with the lowest pod
/// generation scanned for it in `floor_generations`: one row per execution
/// that is active or at or above its floor, or one with `NULL` execution
/// columns when there is none. A single statement, so the active execution
/// and the rows above the floor come from one snapshot. Read-only and
/// unlocked.
///
/// `lifecycle <> 'deleted'` excludes soft-delete **tombstone** rows, which the
/// control plane retains until tombstone retention hard-deletes them, so a
/// deleted sandbox's pod reads as having no owning row.
pub async fn list(
    executor: impl Executor<'_>,
    sandbox_ids: &[Uuid],
    floor_generations: &[i64],
) -> Result<Vec<RestorePodOwnerRow>, SandboxStoreError> {
    Ok(sqlx::query_as::<_, RestorePodOwnerRow>(sql!(
        "SELECT s.id, s.managed_by, s.lifecycle,
                    e.generation, e.active, e.state = 'replaced' AS replaced,
                    e.pod_materialized_at IS NOT NULL OR NULLIF(BTRIM(e.pod_uid), '') IS NOT NULL
                        AS materialized
               FROM UNNEST($1::uuid[], $2::bigint[]) AS p(sandbox_id, floor_generation)
               JOIN sandboxes s ON s.id = p.sandbox_id
               LEFT JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id
                AND (e.active OR e.generation >= p.floor_generation)
              WHERE s.lifecycle <> 'deleted'"
    ))
    .bind(sandbox_ids)
    .bind(floor_generations)
    .fetch_all(executor.into_sqlx())
    .await?)
}
