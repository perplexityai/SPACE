//! What a sandbox's active execution records about its pod once the pod
//! exists: its UID and IP, and the Kubernetes node it runs on.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;

/// Record `pod_uid` and `pod_ip` on `sandbox_id`'s active execution at
/// `generation`, moving it to `running` once it has an IP or to `created`
/// once it has a UID. A `recovering` execution keeps its state and
/// `updated_at`, and a `suspended` one is not written.
///
/// Stamps `pod_materialized_at` when the row records a pod UID, or held one
/// before this write replaces or clears it, and never clears a stamp. An
/// otherwise unchanged row is still written when it carries a UID and no
/// stamp; that write leaves `state` and `updated_at` as they were.
///
/// `false` when nothing was written: no active execution at `generation`, a
/// suspended one, or one already current. The two read alike, so a caller that must tell them
/// apart holds the execution's row lock before this write, in the same
/// transaction.
pub async fn record_pod_metadata(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    pod_uid: Option<&str>,
    pod_ip: Option<&str>,
) -> Result<bool, DbError> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
             SET pod_uid = $1::TEXT,
                 pod_ip = $2::INET,
                 pod_materialized_at = COALESCE(
                     pod_materialized_at,
                     CASE
                         WHEN NULLIF(BTRIM(pod_uid), '') IS NOT NULL
                           OR NULLIF(BTRIM($1::TEXT), '') IS NOT NULL
                         THEN NOW()
                     END
                 ),
                 state = CASE
                     WHEN state = 'recovering' THEN state
                     WHEN NOT (
                         pod_uid IS DISTINCT FROM $1::TEXT
                         OR pod_ip IS DISTINCT FROM $2::INET
                         OR (
                             state NOT IN ('running', 'recovering')
                             AND (
                                 $2::TEXT IS NOT NULL
                                 OR ($1::TEXT IS NOT NULL AND state <> 'created')
                             )
                         )
                     ) THEN state
                     WHEN $2::TEXT IS NOT NULL THEN 'running'
                     WHEN $1::TEXT IS NOT NULL THEN 'created'
                     ELSE state
                 END,
                 updated_at = CASE
                     WHEN state = 'recovering' THEN updated_at
                     WHEN NOT (
                         pod_uid IS DISTINCT FROM $1::TEXT
                         OR pod_ip IS DISTINCT FROM $2::INET
                         OR (
                             state NOT IN ('running', 'recovering')
                             AND (
                                 $2::TEXT IS NOT NULL
                                 OR ($1::TEXT IS NOT NULL AND state <> 'created')
                             )
                         )
                     ) THEN updated_at
                     ELSE NOW()
                 END
             WHERE sandbox_id = $3 AND generation = $4 AND active
               AND state <> 'suspended'
               AND (
                 pod_uid IS DISTINCT FROM $1::TEXT
                 OR pod_ip IS DISTINCT FROM $2::INET
                 OR (
                     state NOT IN ('running', 'recovering')
                     AND (
                         $2::TEXT IS NOT NULL
                         OR ($1::TEXT IS NOT NULL AND state <> 'created')
                     )
                 )
                 OR (
                     pod_materialized_at IS NULL
                     AND (
                         NULLIF(BTRIM(pod_uid), '') IS NOT NULL
                         OR NULLIF(BTRIM($1::TEXT), '') IS NOT NULL
                     )
                 )
               )"
    ))
    .bind(pod_uid)
    .bind(pod_ip)
    .bind(sandbox_id)
    .bind(generation)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected()
        > 0)
}

/// Repair a missing `pod_uid` on `sandbox_id`'s active `'creating'` execution
/// at `generation`, promoting it to `'created'` and stamping
/// `pod_materialized_at` while leaving `pod_ip` `NULL`.
///
/// Applies only when `pod_uid` is non-blank and the execution is still
/// `active`, at `generation`, in state `'creating'`, with no recorded `pod_uid`,
/// `pod_ip`, or `pod_materialized_at`, and `updated_at` still equals
/// `observed_updated_at`. If a concurrent materializer or same-generation reset
/// has recorded or cleared a pod, stamped `pod_materialized_at`, or advanced
/// `updated_at`, nothing changes and `false` is returned. The caller must pass
/// its open transaction (`conn`).
pub async fn repair_creating_pod_uid<C>(
    conn: &mut C,
    sandbox_id: SandboxId,
    generation: i64,
    pod_uid: &str,
    observed_updated_at: time::OffsetDateTime,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET pod_uid = NULLIF(BTRIM($3::TEXT), ''),
                state = 'created',
                pod_materialized_at = COALESCE(pod_materialized_at, NOW()),
                updated_at = NOW()
          WHERE sandbox_id = $1
            AND generation = $2
            AND active
            AND state = 'creating'
            AND NULLIF(BTRIM(pod_uid), '') IS NULL
            AND pod_ip IS NULL
            AND pod_materialized_at IS NULL
            AND updated_at = $4
            AND NULLIF(BTRIM($3::TEXT), '') IS NOT NULL"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(pod_uid)
    .bind(observed_updated_at)
    .execute(conn.into_sqlx())
    .await?
    .rows_affected()
        > 0)
}

/// Record `node_name` as the Kubernetes node of `sandbox_id`'s active
/// execution when it records another, touching `updated_at` unless the
/// execution is `recovering`. Returns whether the sandbox had an active
/// execution when the statement started, whether or not this wrote it;
/// `false` means there was none to record the node on.
pub async fn record_node_name(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    node_name: &str,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "WITH updated AS (
                UPDATE sandbox_k8s_executions
                SET k8s_node_name = $1::TEXT,
                    updated_at = CASE WHEN state = 'recovering' THEN updated_at ELSE NOW() END
                WHERE sandbox_id = $2 AND active
                  AND k8s_node_name IS DISTINCT FROM $1::TEXT
                RETURNING 1
             )
             SELECT EXISTS (
                SELECT 1
                FROM sandbox_k8s_executions
                WHERE sandbox_id = $2 AND active
             )"
    ))
    .bind(node_name)
    .bind(sandbox_id)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// Record `node_name` on `sandbox_id`'s active execution at `generation` while
/// its pod identity still matches, returning the row's post-write `updated_at`
/// (`None` when the execution was retired, suspended, moved to another
/// generation, or no longer matches the expected pod identity).
///
/// When `expected_pod_uid` is `Some(uid)`, requires `pod_uid = uid`. When
/// `None`, requires `NULLIF(BTRIM(pod_uid), '') IS NULL` and
/// `pod_materialized_at IS NULL` so an unmaterialized create never writes
/// placement onto a row another attempt already materialized.
pub async fn record_node_name_fenced(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    expected_pod_uid: Option<&str>,
    node_name: &str,
) -> Result<Option<time::OffsetDateTime>, DbError> {
    Ok(sqlx::query_scalar::<_, time::OffsetDateTime>(sql!(
        "WITH matched AS (
             SELECT id, k8s_node_name, updated_at
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $2
                AND generation = $3
                AND active
                AND state <> 'suspended'
                AND CASE
                    WHEN $4::TEXT IS NOT NULL THEN pod_uid = $4::TEXT
                    ELSE NULLIF(BTRIM(pod_uid), '') IS NULL AND pod_materialized_at IS NULL
                END
              FOR NO KEY UPDATE
         ),
         updated AS (
             UPDATE sandbox_k8s_executions AS e
                SET k8s_node_name = $1::TEXT,
                    updated_at = CASE
                        WHEN e.state = 'recovering' THEN e.updated_at
                        ELSE GREATEST(NOW(), e.updated_at + INTERVAL '1 microsecond')
                    END
               FROM matched
              WHERE e.id = matched.id
                AND matched.k8s_node_name IS DISTINCT FROM $1::TEXT
             RETURNING e.updated_at
         )
         SELECT COALESCE((SELECT updated_at FROM updated), matched.updated_at)
           FROM matched"
    ))
    .bind(node_name)
    .bind(sandbox_id)
    .bind(generation)
    .bind(expected_pod_uid)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

#[cfg(test)]
mod tests;
