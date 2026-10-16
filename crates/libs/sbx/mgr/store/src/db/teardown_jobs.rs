//! Generation-bound teardown jobs have no generic worker lease. Terminal local
//! transitions close them atomically; backup failure also preserves the exact
//! artifact's eviction evidence. A recovered resident may start a new attempt
//! for the same execution while retaining failed history. None of these writes
//! proves runtime release. Periodic reconciliation closes at most 128 terminal
//! active teardown jobs per tick, excluding unrelated and retained job history.

use super::{active_teardown_suspend_operation, immediate_transaction};
use crate::db::{Connection, Result};
use pplx_space_common_core::{
    sql,
    types::{ExecutionGeneration, JobId, LocalArtifactId},
};
use rusqlite::params;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeardownSuspendFailure {
    Missing,
    Deleted,
    Deleting,
    Quarantined,
    Failed,
    BackupUnavailable,
    Reconciled,
}

impl TeardownSuspendFailure {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "sandbox record no longer exists",
            Self::Deleted => "sandbox was deleted",
            Self::Deleting => "sandbox is deleting",
            Self::Quarantined => "sandbox is quarantined",
            Self::Failed => "generation suspend permanently failed",
            Self::BackupUnavailable => "backup cannot complete: cluster-api sandbox not found",
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

/// A recovered resident may start a fresh attempt; retained artifacts and
/// in-flight cleanup cannot supply that authority, even for the same generation.
pub fn teardown_suspend_retry_admissible(
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

pub fn fail_teardown_suspend_operation(
    conn: &Connection,
    id: &str,
    generation: ExecutionGeneration,
    reason: TeardownSuspendFailure,
) -> Result<bool> {
    fail_operation(conn, id, generation, reason, None)
}

pub fn fail_teardown_suspend_backup_operation(
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
                tx.raw(),
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
            conn.raw(),
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
pub fn reconcile_terminal_teardown_suspend_jobs(conn: &Connection) -> Result<usize> {
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
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    for job in &jobs {
        pplx_space_common_jobs::db::fail_job_permanent(
            tx.raw(),
            job,
            TeardownSuspendFailure::Reconciled.as_str(),
            None,
        )?;
    }
    tx.commit()?;
    Ok(jobs.len())
}

const TEARDOWN_SUSPEND_RETRY_DELAY_SECS: i64 = 30;

/// Persist retry pacing on the generation's existing job, so daemon restarts
/// and frequent shim polls cannot turn a blocked backup into a tight loop.
pub fn defer_teardown_suspend_retry(
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
                "UPDATE jobs SET next_attempt_at = datetime('now', '+' || ?3 || ' seconds'), error = ?2
              WHERE job_id = ?1 AND status IN ('pending', 'running')"
            ),
            params![operation.job_id, error, TEARDOWN_SUSPEND_RETRY_DELAY_SECS],
        )?;
    }
    Ok(())
}

pub fn teardown_suspend_retry_due(
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
