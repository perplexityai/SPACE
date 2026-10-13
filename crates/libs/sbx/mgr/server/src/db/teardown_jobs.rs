//! Generation-bound teardown jobs have no generic worker lease. Terminal local
//! transitions close them atomically; backup failure also preserves the exact
//! artifact's eviction evidence. A recovered resident may start a new attempt
//! for the same execution while retaining failed history. None of these writes
//! proves runtime release. Periodic reconciliation closes at most 128 terminal
//! active teardown jobs per tick, excluding unrelated and retained job history.

use super::{active_teardown_suspend_operation, immediate_transaction};
use pplx_space_common_core::{
    sql,
    types::{ExecutionGeneration, JobId, LocalArtifactId},
};
use rusqlite::{Connection, Result, params};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TeardownSuspendFailure {
    Missing,
    Deleted,
    Deleting,
    Quarantined,
    Failed,
    BackupUnavailable,
    ArtifactUnavailable,
    Reconciled,
}

impl TeardownSuspendFailure {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "sandbox record no longer exists",
            Self::Deleted => "sandbox was deleted",
            Self::Deleting => "sandbox is deleting",
            Self::Quarantined => "sandbox is quarantined",
            Self::Failed => "generation suspend permanently failed",
            Self::BackupUnavailable => "backup cannot complete: cluster-api sandbox not found",
            Self::ArtifactUnavailable => {
                "backup cannot complete: local suspend artifact unavailable"
            }
            Self::Reconciled => {
                "teardown suspend reconciled: sandbox missing, deleting or quarantined"
            }
        }
    }
}

impl std::fmt::Display for TeardownSuspendFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Caller holds the local database lock through job admission. Checking the
/// captured artifact as well as the operation prevents a late retry from
/// publishing the next execution's bytes.
pub(crate) fn teardown_suspend_owns_backup_source(
    conn: &Connection,
    id: &str,
    generation: ExecutionGeneration,
    artifact_id: LocalArtifactId,
    local_path: &str,
) -> Result<bool> {
    use crate::state::SandboxState;
    if super::runtime_execution_generation(conn, id)?.is_some_and(|current| current > generation) {
        return Ok(false);
    }
    Ok(
        super::teardown_suspend_status(conn, id)?.is_some_and(|status| {
            matches!(
                status.state,
                SandboxState::Suspending
                    | SandboxState::Suspended
                    | SandboxState::CleanupRequired
                    | SandboxState::CleanupFailed
            ) && status.operation.is_some_and(|operation| {
                operation.status.is_active() && operation.execution_generation == generation
            }) && status.artifact.is_some_and(|artifact| {
                artifact.local_artifact_id == artifact_id
                    && artifact.sandbox_id == id
                    && artifact.local_path == local_path
            })
        }),
    )
}

/// A recovered resident may start a fresh attempt; retained artifacts and
/// in-flight cleanup cannot supply that authority, even for the same generation.
pub(crate) fn teardown_suspend_retry_admissible(
    conn: &Connection,
    id: &str,
    generation: ExecutionGeneration,
) -> Result<bool> {
    conn.query_row(
        sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM sandboxes s
                  WHERE s.id = ?1 AND s.state IN ('started', 'paused')
                    AND s.space_api_route_execution_generation = ?2
                    AND s.cleanup_completion IS NULL AND s.cleanup_keep_filesystem IS NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM sandbox_cleanup_episodes e WHERE e.sandbox_id = s.id
                    )
             )"
        ),
        params![id, generation.get()],
        |row| row.get(0),
    )
}

pub(crate) fn fail_teardown_suspend_operation(
    conn: &Connection,
    id: &str,
    generation: ExecutionGeneration,
    reason: TeardownSuspendFailure,
) -> Result<bool> {
    fail_operation(conn, id, generation, reason, None)
}

pub(crate) fn fail_teardown_suspend_backup_operation(
    conn: &Connection,
    id: &str,
    generation: ExecutionGeneration,
    failed_artifact: LocalArtifactId,
) -> Result<bool> {
    fail_operation(
        conn,
        id,
        generation,
        TeardownSuspendFailure::BackupUnavailable,
        Some(failed_artifact),
    )
}

pub(crate) fn fail_teardown_suspend_artifact_unavailable(
    conn: &Connection,
    id: &str,
    generation: ExecutionGeneration,
    failed_artifact: LocalArtifactId,
) -> Result<bool> {
    fail_operation(
        conn,
        id,
        generation,
        TeardownSuspendFailure::ArtifactUnavailable,
        Some(failed_artifact),
    )
}

fn fail_operation(
    conn: &Connection,
    id: &str,
    generation: ExecutionGeneration,
    reason: TeardownSuspendFailure,
    failed_artifact: Option<LocalArtifactId>,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = if let Some(operation) = active_teardown_suspend_operation(&tx, id)? {
        if operation.execution_generation == generation {
            // Closing the only driver also closes its opportunity to publish
            // eviction evidence. Keep that evidence atomic with job failure.
            if let Some(failed_artifact) = failed_artifact {
                let owns_artifact = super::teardown_suspend_status(&tx, id)?
                    .and_then(|status| status.artifact)
                    .is_some_and(|artifact| artifact.local_artifact_id == failed_artifact);
                if !owns_artifact {
                    return Ok(false);
                }
                super::mark_teardown_suspend_backup_permanently_failed(&tx, id, failed_artifact)?;
            }
            pplx_space_common_jobs::db::fail_job_permanent(
                &tx,
                &operation.job_id,
                reason.as_str(),
                None,
            )?;
            true
        } else {
            false
        }
    } else {
        false
    };
    tx.commit()?;
    Ok(changed)
}

/// Called within the transaction that revokes the sandbox's ability to suspend.
pub(super) fn fail_active(
    conn: &Connection,
    id: &str,
    reason: TeardownSuspendFailure,
) -> Result<()> {
    if let Some(operation) = active_teardown_suspend_operation(conn, id)? {
        pplx_space_common_jobs::db::fail_job_permanent(
            conn,
            &operation.job_id,
            reason.as_str(),
            None,
        )?;
    }
    Ok(())
}

/// Use the active-kind index to avoid retained history. Limit closures per tick
/// so a backlog cannot monopolize SQLite. Recheck candidates after taking the
/// write lock; the read-only fast path takes no write transaction when idle.
pub(crate) fn reconcile_terminal_teardown_suspend_jobs(conn: &Connection) -> Result<usize> {
    // Include the complete partial-index predicate, then exclude cancellation:
    // SQLite does not infer that the narrower two-status IN implies this index.
    let has_candidates: bool = conn.query_row(
        sql!(
            "SELECT EXISTS (
             SELECT 1 FROM jobs j LEFT JOIN sandboxes s ON s.id = j.subject_id
              WHERE j.kind = ?1 AND j.status IN ('pending', 'running', 'cancelling')
                AND j.status != 'cancelling'
                AND (s.id IS NULL OR s.state IN ('quarantined', 'deleting'))
         )"
        ),
        params![super::TEARDOWN_SUSPEND_JOB_KIND],
        |row| row.get(0),
    )?;
    if !has_candidates {
        return Ok(0);
    }
    let tx = immediate_transaction(conn)?;
    let mut statement = tx.prepare(sql!(
        "SELECT j.job_id FROM jobs j LEFT JOIN sandboxes s ON s.id = j.subject_id
          WHERE j.kind = ?1 AND j.status IN ('pending', 'running', 'cancelling')
            AND j.status != 'cancelling'
            AND (s.id IS NULL OR s.state IN ('quarantined', 'deleting'))
          LIMIT 128"
    ))?;
    let jobs = statement
        .query_map(params![super::TEARDOWN_SUSPEND_JOB_KIND], |row| {
            row.get::<_, JobId>(0)
        })?
        .collect::<Result<Vec<_>>>()?;
    drop(statement);
    for job in &jobs {
        pplx_space_common_jobs::db::fail_job_permanent(
            &tx,
            job,
            TeardownSuspendFailure::Reconciled.as_str(),
            None,
        )?;
    }
    tx.commit()?;
    Ok(jobs.len())
}

/// Persist retry pacing on the generation's existing job, so daemon restarts
/// and frequent shim polls cannot turn a blocked backup into a tight loop.
/// These jobs have no BackgroundWorker; after the initial running claim,
/// attempts counts failed driver runs.
pub(crate) fn defer_teardown_suspend_retry(
    conn: &Connection,
    id: &str,
    generation: ExecutionGeneration,
    error: &str,
) -> Result<()> {
    if let Some(operation) = active_teardown_suspend_operation(conn, id)?
        && operation.execution_generation == generation
    {
        conn.execute(
            sql!(
                "UPDATE jobs SET
                 next_attempt_at = datetime('now', '+' || min(900, 30 * (1 << min(max(attempts - 1, 0), 5))) || ' seconds'),
                 attempts = min(attempts + 1, 30), error = ?2
              WHERE job_id = ?1 AND status IN ('pending', 'running')"
            ),
            params![operation.job_id, error],
        )?;
    }
    Ok(())
}

pub(crate) fn teardown_suspend_retry_due(
    conn: &Connection,
    id: &str,
    generation: ExecutionGeneration,
) -> Result<bool> {
    let Some(operation) = active_teardown_suspend_operation(conn, id)? else {
        return Ok(true);
    };
    if operation.execution_generation != generation {
        return Ok(true);
    }
    conn.query_row(
        sql!(
            "SELECT next_attempt_at IS NULL OR next_attempt_at <= datetime('now')
           FROM jobs WHERE job_id = ?1"
        ),
        params![operation.job_id],
        |row| row.get(0),
    )
}

/// Bounded discovery for retries whose original RPC/shim has disappeared.
/// Running drivers are deduplicated by the service's existing driver slots.
pub(crate) fn due_teardown_suspend_redrives(
    conn: &Connection,
) -> Result<Vec<(String, ExecutionGeneration)>> {
    let mut statement = conn.prepare(sql!(
        "SELECT subject_id FROM jobs
         WHERE kind = ?1 AND status IN ('pending', 'running')
           AND (next_attempt_at IS NULL OR next_attempt_at <= datetime('now'))
         ORDER BY next_attempt_at, created_at LIMIT 128"
    ))?;
    let ids = statement
        .query_map([super::TEARDOWN_SUSPEND_JOB_KIND], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<Result<Vec<_>>>()?;
    ids.into_iter()
        .filter_map(|id| match active_teardown_suspend_operation(conn, &id) {
            Ok(Some(operation)) => Some(Ok((id, operation.execution_generation))),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}
