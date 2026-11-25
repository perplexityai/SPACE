//! What a sandbox's active execution records about its pod once the pod
//! exists: its UID and IP, and the Kubernetes node it runs on.

use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;

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
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    pod_uid: Option<&str>,
    pod_ip: Option<&str>,
) -> Result<bool, sqlx::Error> {
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
    .execute(executor)
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
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    node_name: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(sql!(
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
    .fetch_one(executor)
    .await
}

#[cfg(test)]
mod tests;
