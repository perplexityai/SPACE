//! The execution-row lock the Kubernetes substrate takes on the active
//! execution before it writes the row's pod metadata, or decides whether
//! the sandbox's record of whose activation created its runtime has to
//! change.
//!
//! The caller keeps its transaction, whether it took the `sandboxes` row
//! lock first, any read of the sandbox it runs after this lock, and the
//! decision.

use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;

/// The pod an active execution row tracks, blanks read as `None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackedPodRow {
    pub pod_uid: Option<String>,
    pub pod_ip: Option<String>,
}

/// Lock the active execution row of `sandbox_id` at `generation`
/// (`FOR UPDATE`) and read the pod it tracks. The lock lasts until the
/// caller's transaction ends. `None` when no such active row exists, and
/// nothing is locked.
pub async fn lock_tracked_pod(
    conn: &mut sqlx::PgConnection,
    sandbox_id: SandboxId,
    generation: i64,
) -> Result<Option<TrackedPodRow>, sqlx::Error> {
    let row = sqlx::query_as::<_, (Option<String>, Option<String>)>(sql!(
        "SELECT NULLIF(BTRIM(pod_uid), ''), HOST(pod_ip)::TEXT
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = $2 AND active
              FOR UPDATE"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|(pod_uid, pod_ip)| TrackedPodRow { pod_uid, pod_ip }))
}

#[cfg(test)]
mod tests;
