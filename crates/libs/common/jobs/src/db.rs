use pplx_space_common_core::sql;
use pplx_space_common_core::types::JobId;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, params};

use crate::{Job, JobFailure, JobFailureKind, JobFailureReason, JobPriority, SubjectConcurrency};

/// Canonical `jobs` table columns, in SQLite table order.
pub const JOB_TABLE_COLUMNS: &[&str] = &[
    "job_id",
    "kind",
    "status",
    "payload",
    "subject_id",
    "dedup_key",
    "error_kind",
    "attempts",
    "max_attempts",
    "bytes_processed",
    "error",
    "created_at",
    "started_at",
    "completed_at",
    "next_attempt_at",
    "traceparent",
    "lease_owner",
    "lease_expires_at",
    "heartbeat_at",
    "priority",
    "failure_reason",
];

/// Columns selected by shared job readers, in the order `map_job` expects.
pub const JOB_SELECT_COLUMNS: &str = "
    job_id, kind, status, payload, subject_id, dedup_key,
    error_kind, failure_reason, attempts, max_attempts, bytes_processed, error,
    created_at, started_at, completed_at, traceparent, lease_owner, priority";

/// Columns selected by shared job readers.
pub fn selected_job_columns() -> Vec<&'static str> {
    JOB_SELECT_COLUMNS
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .collect()
}

#[derive(Debug, thiserror::Error)]
#[error("persisted retriable or absent job failure has a domain reason")]
struct InvalidPersistedJobFailure;

fn persisted_failure(
    kind: Option<JobFailureKind>,
    reason: Option<JobFailureReason>,
) -> rusqlite::Result<Option<JobFailure>> {
    match (kind, reason) {
        (None, None) => Ok(None),
        (Some(JobFailureKind::Permanent), reason) => Ok(Some(JobFailure::Permanent { reason })),
        (Some(JobFailureKind::Retriable), None) => Ok(Some(JobFailure::Retriable)),
        (None | Some(JobFailureKind::Retriable), Some(_)) => {
            Err(rusqlite::Error::FromSqlConversionFailure(
                7,
                rusqlite::types::Type::Text,
                Box::new(InvalidPersistedJobFailure),
            ))
        }
    }
}

fn map_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<Job> {
    let failure = persisted_failure(row.get(6)?, row.get(7)?)?;
    Ok(Job {
        job_id: row.get(0)?,
        kind: row.get(1)?,
        status: row.get(2)?,
        payload: row.get(3)?,
        subject_id: row.get(4)?,
        dedup_key: row.get(5)?,
        failure,
        attempts: row.get(8)?,
        max_attempts: row.get(9)?,
        bytes_processed: row.get(10)?,
        error: row.get(11)?,
        created_at: row.get(12)?,
        started_at: row.get(13)?,
        completed_at: row.get(14)?,
        traceparent: row.get(15)?,
        lease_owner: row.get(16)?,
        priority: JobPriority::new(row.get(17)?),
    })
}

pub(crate) fn nonnegative_i64_to_u64(value: i64) -> u64 {
    value.max(0).cast_unsigned()
}

/// Returns the job_id of a non-failed job with the same `kind` and `dedup_key`.
/// Returns `None` if no dedup_key is given or no matching job exists.
pub fn find_active_job(
    conn: &Connection,
    kind: &str,
    dedup_key: &str,
) -> rusqlite::Result<Option<JobId>> {
    conn.query_row(
        "SELECT job_id FROM jobs
         WHERE kind = ?1
           AND dedup_key = ?2
           AND status IN ('pending', 'running', 'cancelling')
         LIMIT 1",
        params![kind, dedup_key],
        |row| row.get(0),
    )
    .optional()
}

/// Returns the newest job with the given `kind` and `dedup_key`, regardless of
/// status. Unlike [`find_active_job`] this also matches `completed` and
/// `failed` rows. A caller retrying an idempotent batch after the original job
/// already reached a terminal state recovers the *same* durable record — e.g.
/// the wrapped DEK persisted in the job payload — instead of minting a fresh
/// one. Terminal rows remain queryable until the shared jobs retention pruner
/// removes them. `ORDER BY created_at DESC` (UUIDv7 job_ids are monotonic, so
/// the secondary key keeps ties stable) returns the most recent attempt when
/// more than one row shares the key.
pub fn find_job_by_dedup_key(
    conn: &Connection,
    kind: &str,
    dedup_key: &str,
) -> rusqlite::Result<Option<Job>> {
    conn.query_row(
        &format!(
            "SELECT {JOB_SELECT_COLUMNS} FROM jobs
             WHERE kind = ?1 AND dedup_key = ?2
             ORDER BY created_at DESC, job_id DESC
             LIMIT 1"
        ),
        params![kind, dedup_key],
        map_job,
    )
    .optional()
}

pub struct InsertJob<'a> {
    pub job_id: &'a JobId,
    pub kind: &'a str,
    pub payload: &'a str,
    pub subject_id: Option<&'a str>,
    pub dedup_key: Option<&'a str>,
    pub max_attempts: i32,
    pub traceparent: Option<&'a str>,
    pub priority: JobPriority,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InsertJobOutcome {
    Inserted,
    AtActiveLimit,
}

pub fn insert_job(conn: &Connection, job: InsertJob<'_>) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO jobs (
             job_id, kind, payload, subject_id, dedup_key, max_attempts, traceparent, priority
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            job.job_id,
            job.kind,
            job.payload,
            job.subject_id,
            job.dedup_key,
            job.max_attempts,
            job.traceparent,
            job.priority.get(),
        ],
    )?;
    Ok(())
}

/// Change an active job's execution parameters while the caller holds its
/// domain serialization fence. This does not authorize the domain mutation.
pub fn update_active_job_payload(
    conn: &Connection,
    job_id: &JobId,
    payload: &str,
    priority: JobPriority,
) -> rusqlite::Result<bool> {
    Ok(conn.execute(
        sql!(
            "UPDATE jobs SET payload = ?2, priority = ?3,
             next_attempt_at = CASE WHEN priority < ?3 THEN NULL ELSE next_attempt_at END
         WHERE job_id = ?1 AND status IN ('pending', 'running')"
        ),
        params![job_id, payload, priority.get()],
    )? == 1)
}

/// Restart recovery for a single service-owned kind. Unlike inventory recovery,
/// this does not materialize all pending jobs or reset another worker's jobs.
pub fn reset_running_kind(conn: &Connection, kind: &str) -> rusqlite::Result<usize> {
    conn.execute(
        sql!(
            "UPDATE jobs SET status = 'pending', started_at = NULL
         WHERE kind = ?1 AND status = 'running'"
        ),
        [kind],
    )
}

/// Repair a single-process dispatcher's claims after completed/panicked tasks.
/// The active set must include every task that can still mutate its subject.
pub fn reset_inactive_running_kind(
    conn: &Connection,
    kind: &str,
    active_subjects: &[String],
) -> rusqlite::Result<usize> {
    let excluded = serde_json::to_string(active_subjects)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        sql!(
            "UPDATE jobs SET status = 'pending', next_attempt_at = datetime('now', '+1 seconds')
         WHERE kind = ?1 AND status = 'running'
           AND subject_id NOT IN (SELECT value FROM json_each(?2))"
        ),
        params![kind, excluded],
    )
}

pub(crate) fn insert_job_with_priority_band_active_limit(
    conn: &Connection,
    job: InsertJob<'_>,
    min_priority: Option<JobPriority>,
    max_priority: Option<JobPriority>,
    active_limit: usize,
) -> rusqlite::Result<InsertJobOutcome> {
    let active_limit = i64::try_from(active_limit)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let inserted = conn.execute(
        "INSERT INTO jobs (
             job_id, kind, payload, subject_id, dedup_key, max_attempts, traceparent, priority
         ) SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8
         WHERE (
             SELECT COUNT(*) FROM jobs
             WHERE kind = ?2
               AND status IN ('pending', 'running', 'cancelling')
               AND (?9 IS NULL OR priority >= ?9)
               AND (?10 IS NULL OR priority < ?10)
         ) < ?11",
        params![
            job.job_id,
            job.kind,
            job.payload,
            job.subject_id,
            job.dedup_key,
            job.max_attempts,
            job.traceparent,
            job.priority.get(),
            min_priority.map(JobPriority::get),
            max_priority.map(JobPriority::get),
            active_limit,
        ],
    )?;
    Ok(if inserted == 1 {
        InsertJobOutcome::Inserted
    } else {
        InsertJobOutcome::AtActiveLimit
    })
}

pub(crate) fn raise_pending_job_priority(
    conn: &Connection,
    job_id: &JobId,
    priority: JobPriority,
) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE jobs
         SET priority = ?2
         WHERE job_id = ?1 AND status = 'pending' AND priority < ?2",
        params![job_id, priority.get()],
    )?;
    Ok(())
}

pub fn prune_terminal_jobs(
    conn: &Connection,
    completed_retention: Duration,
    failed_retention: Duration,
) -> rusqlite::Result<usize> {
    conn.execute(
        "DELETE FROM jobs
         WHERE (status IN ('completed', 'cancelled')
                AND COALESCE(completed_at, created_at) < datetime('now', ?1))
            OR (status = 'failed'
                AND COALESCE(completed_at, created_at) < datetime('now', ?2))",
        params![
            sqlite_past_duration_modifier(completed_retention),
            sqlite_past_duration_modifier(failed_retention),
        ],
    )
}

pub fn get_job(conn: &Connection, job_id: &JobId) -> rusqlite::Result<Option<Job>> {
    conn.query_row(
        &format!("SELECT {JOB_SELECT_COLUMNS} FROM jobs WHERE job_id = ?1"),
        params![job_id],
        map_job,
    )
    .optional()
}

pub fn list_jobs(
    conn: &Connection,
    subject_id: Option<&str>,
    status_filter: Option<&str>,
    limit: u32,
) -> rusqlite::Result<Vec<Job>> {
    let limit = if limit == 0 { 100i64 } else { limit as i64 };
    match (subject_id, status_filter) {
        (Some(sid), Some(st)) => {
            let mut stmt = conn.prepare(&format!(
                "SELECT {JOB_SELECT_COLUMNS} FROM jobs
                 WHERE subject_id = ?1 AND status = ?2
                 ORDER BY created_at DESC LIMIT ?3"
            ))?;
            stmt.query_map(params![sid, st, limit], map_job)?.collect()
        }
        (Some(sid), None) => {
            let mut stmt = conn.prepare(&format!(
                "SELECT {JOB_SELECT_COLUMNS} FROM jobs
                 WHERE subject_id = ?1
                 ORDER BY created_at DESC LIMIT ?2"
            ))?;
            stmt.query_map(params![sid, limit], map_job)?.collect()
        }
        (None, Some(st)) => {
            let mut stmt = conn.prepare(&format!(
                "SELECT {JOB_SELECT_COLUMNS} FROM jobs
                 WHERE status = ?1
                 ORDER BY created_at DESC LIMIT ?2"
            ))?;
            stmt.query_map(params![st, limit], map_job)?.collect()
        }
        (None, None) => {
            let mut stmt = conn.prepare(&format!(
                "SELECT {JOB_SELECT_COLUMNS} FROM jobs
                 ORDER BY created_at DESC LIMIT ?1"
            ))?;
            stmt.query_map(params![limit], map_job)?.collect()
        }
    }
}

pub fn mark_job_running(conn: &Connection, job_id: &JobId) -> rusqlite::Result<bool> {
    let changed = conn.execute(
        "UPDATE jobs
         SET status = 'running', started_at = datetime('now'), attempts = attempts + 1
         WHERE job_id = ?1 AND status = 'pending'",
        params![job_id],
    )?;
    Ok(changed == 1)
}

/// Guard appended to claim/list queries under
/// [`SubjectConcurrency::Serialized`]. A pending job is held back while
/// another job with the same `(kind, subject_id)` is running (mutual
/// exclusion) or while an older pending job for the same subject exists
/// (FIFO; `created_at` has second resolution, so ties break on the
/// time-ordered UUIDv7 `job_id`). NULL subjects are never serialized. An
/// older pending job in retry backoff still holds its FIFO slot until it
/// reaches a terminal state.
const SERIALIZED_SUBJECT_GUARD: &str = "
           AND (
             jobs.subject_id IS NULL
             OR NOT EXISTS (
               SELECT 1 FROM jobs other
               WHERE other.kind = jobs.kind
                 AND other.subject_id = jobs.subject_id
                 AND (
                   other.status = 'running'
                   OR (other.status = 'pending'
                       AND (other.created_at < jobs.created_at
                            OR (other.created_at = jobs.created_at
                                AND other.job_id < jobs.job_id)))
                 )
             )
           )";

pub fn try_claim_pending_job(
    conn: &Connection,
    job_id: &JobId,
    worker_id: &str,
    lease_ttl: Duration,
    subject_concurrency: SubjectConcurrency,
) -> rusqlite::Result<Option<Job>> {
    let lease_modifier = sqlite_duration_modifier(lease_ttl);
    let mut sql = String::from(
        "UPDATE jobs
         SET status = 'running',
             started_at = datetime('now'),
             attempts = attempts + 1,
             lease_owner = ?2,
             lease_expires_at = datetime('now', ?3),
             heartbeat_at = datetime('now')
         WHERE job_id = ?1
           AND status = 'pending'
           AND (next_attempt_at IS NULL OR next_attempt_at <= datetime('now'))",
    );
    if subject_concurrency == SubjectConcurrency::Serialized {
        sql.push_str(SERIALIZED_SUBJECT_GUARD);
    }
    let rows = conn.execute(&sql, params![job_id, worker_id, lease_modifier])?;
    if rows == 0 {
        return Ok(None);
    }
    get_job(conn, job_id)
}

pub fn list_due_pending_jobs(
    conn: &Connection,
    limit: u32,
    subject_concurrency: SubjectConcurrency,
) -> rusqlite::Result<Vec<(JobId, String, JobPriority)>> {
    let limit = if limit == 0 { 100i64 } else { limit as i64 };
    let mut sql = String::from(
        "SELECT job_id, kind, priority
         FROM jobs
         WHERE status = 'pending'
           AND (next_attempt_at IS NULL OR next_attempt_at <= datetime('now'))",
    );
    if subject_concurrency == SubjectConcurrency::Serialized {
        // Filtering blocked subjects out of the due list keeps them from
        // consuming per-kind spawn slots every poll tick, which would starve
        // runnable jobs for other subjects when permits are scarce. The claim
        // guard above remains the atomic correctness check.
        sql.push_str(SERIALIZED_SUBJECT_GUARD);
    }
    sql.push_str(
        "
         ORDER BY priority DESC, created_at ASC, job_id ASC
         LIMIT ?1",
    );
    let mut stmt = conn.prepare(&sql)?;
    stmt.query_map(params![limit], |row| {
        Ok((row.get(0)?, row.get(1)?, JobPriority::new(row.get(2)?)))
    })?
    .collect()
}

pub fn requeue_expired_running_jobs(conn: &Connection) -> rusqlite::Result<usize> {
    conn.execute(
        "UPDATE jobs
         SET status = 'pending',
             started_at = NULL,
             lease_owner = NULL,
             lease_expires_at = NULL,
             heartbeat_at = NULL
         WHERE status = 'running'
           AND lease_expires_at IS NOT NULL
           AND lease_expires_at <= datetime('now')",
        [],
    )
}

pub fn heartbeat_job_lease(
    conn: &Connection,
    job_id: &JobId,
    worker_id: &str,
    lease_ttl: Duration,
) -> rusqlite::Result<bool> {
    let lease_modifier = sqlite_duration_modifier(lease_ttl);
    let rows = conn.execute(
        "UPDATE jobs
         SET lease_expires_at = datetime('now', ?3),
             heartbeat_at = datetime('now')
         WHERE job_id = ?1
           AND lease_owner = ?2
           AND status = 'running'",
        params![job_id, worker_id, lease_modifier],
    )?;
    Ok(rows > 0)
}

pub fn complete_leased_job(
    conn: &Connection,
    job_id: &JobId,
    worker_id: &str,
    bytes: i64,
) -> rusqlite::Result<bool> {
    complete_leased_job_with_payload(conn, job_id, worker_id, None, bytes, None)
}

pub fn complete_leased_job_with_payload(
    conn: &Connection,
    job_id: &JobId,
    worker_id: &str,
    expected_attempt: Option<i32>,
    bytes: i64,
    payload: Option<&str>,
) -> rusqlite::Result<bool> {
    let rows = conn.execute(
        "UPDATE jobs
         SET status = 'completed',
             bytes_processed = ?3,
             payload = COALESCE(?4, payload),
             error = NULL,
             error_kind = NULL,
             failure_reason = NULL,
             completed_at = datetime('now'),
             lease_owner = NULL,
             lease_expires_at = NULL,
             heartbeat_at = NULL
         WHERE job_id = ?1
           AND lease_owner = ?2
           AND status = 'running'
           AND (?5 IS NULL OR attempts = ?5)",
        params![job_id, worker_id, bytes, payload, expected_attempt],
    )?;
    Ok(rows > 0)
}

pub fn fail_leased_job_permanent(
    conn: &Connection,
    job_id: &JobId,
    worker_id: &str,
    error: &str,
    failure_reason: Option<JobFailureReason>,
) -> rusqlite::Result<bool> {
    let rows = conn.execute(
        "UPDATE jobs
         SET status = 'failed',
             error_kind = 'permanent',
             error = ?3,
             failure_reason = ?4,
             completed_at = datetime('now'),
             lease_owner = NULL,
             lease_expires_at = NULL,
             heartbeat_at = NULL
         WHERE job_id = ?1
           AND lease_owner = ?2
           AND status = 'running'",
        params![
            job_id,
            worker_id,
            error,
            failure_reason.map(JobFailureReason::as_str)
        ],
    )?;
    Ok(rows > 0)
}

pub fn fail_leased_job_retriable_exhausted(
    conn: &Connection,
    job_id: &JobId,
    worker_id: &str,
    error: &str,
) -> rusqlite::Result<bool> {
    let rows = conn.execute(
        "UPDATE jobs
         SET status = 'failed',
             error_kind = 'retriable',
             error = ?3,
             failure_reason = NULL,
             completed_at = datetime('now'),
             lease_owner = NULL,
             lease_expires_at = NULL,
             heartbeat_at = NULL
         WHERE job_id = ?1
           AND lease_owner = ?2
           AND status = 'running'",
        params![job_id, worker_id, error],
    )?;
    Ok(rows > 0)
}

pub fn retry_leased_job(
    conn: &Connection,
    job_id: &JobId,
    worker_id: &str,
    error: &str,
    delay_secs: u64,
) -> rusqlite::Result<bool> {
    let rows = conn.execute(
        "UPDATE jobs
         SET status = 'pending',
             error = ?3,
             failure_reason = NULL,
             next_attempt_at = datetime('now', ?4),
             lease_owner = NULL,
             lease_expires_at = NULL,
             heartbeat_at = NULL
         WHERE job_id = ?1
           AND lease_owner = ?2
           AND status = 'running'",
        params![job_id, worker_id, error, format!("+{delay_secs} seconds")],
    )?;
    Ok(rows > 0)
}

/// Complete a job while the caller holds its domain serialization fence.
/// Executor persistence retries use [`complete_job_attempt`] or the leased
/// completion path to check their execution's claim as well.
pub fn complete_job(conn: &Connection, job_id: &JobId, bytes: i64) -> rusqlite::Result<bool> {
    let changed = conn.execute(
        "UPDATE jobs
         SET status = 'completed', bytes_processed = ?2,
             error = NULL, error_kind = NULL, failure_reason = NULL,
             completed_at = datetime('now')
         WHERE job_id = ?1 AND status = 'running'",
        params![job_id, bytes],
    )?;
    Ok(changed == 1)
}

/// Reject an unleased executor's late success after restart recovery has
/// reclaimed its job with a different attempt count. Attempts are a retry
/// budget, not an ownership generation: yield/refund paths can reuse a count,
/// so the service must still prove quiescence before releasing ownership.
pub fn complete_job_attempt(
    conn: &Connection,
    job_id: &JobId,
    expected_attempts: i32,
    bytes: i64,
) -> rusqlite::Result<bool> {
    complete_job_attempt_with_payload(conn, job_id, expected_attempts, bytes, None)
}

pub fn complete_job_attempt_with_payload(
    conn: &Connection,
    job_id: &JobId,
    expected_attempts: i32,
    bytes: i64,
    payload: Option<&str>,
) -> rusqlite::Result<bool> {
    let changed = conn.execute(
        "UPDATE jobs
         SET status = 'completed', bytes_processed = ?2, payload = COALESCE(?4, payload),
             error = NULL, error_kind = NULL, failure_reason = NULL,
             completed_at = datetime('now')
         WHERE job_id = ?1 AND status = 'running' AND attempts = ?3",
        params![job_id, bytes, expected_attempts, payload],
    )?;
    Ok(changed == 1)
}

pub fn request_job_cancellation(
    conn: &Connection,
    job_id: &JobId,
) -> rusqlite::Result<Option<Job>> {
    conn.execute(
        "UPDATE jobs
         SET status = 'cancelling',
             error = 'cancellation requested',
             next_attempt_at = NULL
         WHERE job_id = ?1 AND status IN ('pending', 'running')",
        params![job_id],
    )?;
    get_job(conn, job_id)
}

pub fn mark_job_cancelled(conn: &Connection, job_id: &JobId) -> rusqlite::Result<bool> {
    let changed = conn.execute(
        "UPDATE jobs
         SET status = 'cancelled',
             error = NULL,
             error_kind = NULL,
             failure_reason = NULL,
             completed_at = datetime('now'),
             lease_owner = NULL,
             lease_expires_at = NULL,
             heartbeat_at = NULL
         WHERE job_id = ?1 AND status = 'cancelling'",
        params![job_id],
    )?;
    Ok(changed == 1)
}

pub fn fail_cancelling_job(
    conn: &Connection,
    job_id: &JobId,
    error: &str,
    error_kind: JobFailureKind,
    failure_reason: Option<JobFailureReason>,
) -> rusqlite::Result<bool> {
    let changed = conn.execute(
        "UPDATE jobs
         SET status = 'failed',
             error = ?2,
             error_kind = ?3,
             failure_reason = ?4,
             completed_at = datetime('now'),
             lease_owner = NULL,
             lease_expires_at = NULL,
             heartbeat_at = NULL
         WHERE job_id = ?1 AND status = 'cancelling'",
        params![
            job_id,
            error,
            error_kind.as_str(),
            failure_reason.map(JobFailureReason::as_str)
        ],
    )?;
    Ok(changed == 1)
}

pub fn fail_job_permanent(
    conn: &Connection,
    job_id: &JobId,
    error: &str,
    failure_reason: Option<JobFailureReason>,
) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE jobs
         SET status = 'failed', error_kind = 'permanent',
             error = ?2, failure_reason = ?3, completed_at = datetime('now')
         WHERE job_id = ?1",
        params![job_id, error, failure_reason.map(JobFailureReason::as_str)],
    )?;
    Ok(())
}

pub fn fail_job_retriable_exhausted(
    conn: &Connection,
    job_id: &JobId,
    error: &str,
) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE jobs
         SET status = 'failed', error_kind = 'retriable',
             error = ?2, failure_reason = NULL, completed_at = datetime('now')
         WHERE job_id = ?1",
        params![job_id, error],
    )?;
    Ok(())
}

pub fn fail_running_job_permanent(
    conn: &Connection,
    job_id: &JobId,
    error: &str,
    failure_reason: Option<JobFailureReason>,
) -> rusqlite::Result<bool> {
    let changed = conn.execute(
        "UPDATE jobs
         SET status = 'failed', error_kind = 'permanent',
             error = ?2, failure_reason = ?3, completed_at = datetime('now')
         WHERE job_id = ?1 AND status = 'running'",
        params![job_id, error, failure_reason.map(JobFailureReason::as_str)],
    )?;
    Ok(changed == 1)
}

pub fn fail_running_job_retriable_exhausted(
    conn: &Connection,
    job_id: &JobId,
    error: &str,
) -> rusqlite::Result<bool> {
    let changed = conn.execute(
        "UPDATE jobs
         SET status = 'failed', error_kind = 'retriable',
             error = ?2, failure_reason = NULL, completed_at = datetime('now')
         WHERE job_id = ?1 AND status = 'running'",
        params![job_id, error],
    )?;
    Ok(changed == 1)
}

pub fn retry_running_job(
    conn: &Connection,
    job_id: &JobId,
    error: &str,
    delay_secs: u64,
) -> rusqlite::Result<bool> {
    let changed = conn.execute(
        "UPDATE jobs
         SET status = 'pending', error = ?2,
             failure_reason = NULL,
             next_attempt_at = datetime('now', ?3)
         WHERE job_id = ?1 AND status = 'running'",
        params![job_id, error, format!("+{delay_secs} seconds")],
    )?;
    Ok(changed == 1)
}

pub fn yield_running_job(conn: &Connection, job_id: &JobId) -> rusqlite::Result<bool> {
    let changed = conn.execute(
        "UPDATE jobs
         SET status = 'pending', attempts = MAX(attempts - 1, 0),
             started_at = NULL, next_attempt_at = NULL, error = NULL,
             error_kind = NULL, failure_reason = NULL,
             lease_owner = NULL, lease_expires_at = NULL, heartbeat_at = NULL
         WHERE job_id = ?1 AND status = 'running'",
        params![job_id],
    )?;
    Ok(changed == 1)
}

pub fn yield_leased_job(
    conn: &Connection,
    job_id: &JobId,
    worker_id: &str,
) -> rusqlite::Result<bool> {
    let changed = conn.execute(
        "UPDATE jobs
         SET status = 'pending', attempts = MAX(attempts - 1, 0),
             started_at = NULL, next_attempt_at = NULL, error = NULL,
             error_kind = NULL, failure_reason = NULL,
             lease_owner = NULL, lease_expires_at = NULL, heartbeat_at = NULL
         WHERE job_id = ?1 AND status = 'running' AND lease_owner = ?2",
        params![job_id, worker_id],
    )?;
    Ok(changed == 1)
}

/// Reset a retriable job to `pending` with an exponential-backoff delay.
pub fn retry_job(
    conn: &Connection,
    job_id: &JobId,
    error: &str,
    delay_secs: u64,
) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE jobs
         SET status = 'pending', error = ?2,
             failure_reason = NULL,
             next_attempt_at = datetime('now', ?3)
         WHERE job_id = ?1",
        params![job_id, error, format!("+{delay_secs} seconds")],
    )?;
    Ok(())
}

/// On startup: reset selected `running` jobs (interrupted by crash) back to
/// `pending` and return `(job_id, kind, priority, delay_secs)` for every
/// selected `pending` job. An empty `job_kinds` slice selects every kind.
/// `delay_secs` is how long the worker must wait before re-spawning the job:
/// the difference between `next_attempt_at` and `now`, clamped to 0. Jobs
/// scheduled in the past or without a scheduled time come back as `0`.
pub fn recover_interrupted_jobs(
    conn: &Connection,
    job_kinds: &[String],
) -> rusqlite::Result<Vec<(JobId, String, JobPriority, i64)>> {
    let kind_placeholders = (1..=job_kinds.len())
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let kind_filter = if job_kinds.is_empty() {
        String::new()
    } else {
        format!(" AND kind IN ({kind_placeholders})")
    };
    conn.execute(
        &format!(
            "UPDATE jobs SET status = 'pending', started_at = NULL
             WHERE status = 'running'{kind_filter}"
        ),
        rusqlite::params_from_iter(job_kinds),
    )?;
    let mut stmt = conn.prepare(&format!(
        "SELECT job_id, kind, priority,
                COALESCE(
                    CAST(strftime('%s', next_attempt_at) AS INTEGER)
                  - CAST(strftime('%s', 'now')         AS INTEGER),
                    0
                ) AS delay_secs
         FROM jobs
         WHERE status = 'pending'{kind_filter}
         ORDER BY priority DESC, created_at ASC, job_id ASC"
    ))?;
    stmt.query_map(rusqlite::params_from_iter(job_kinds), |row| {
        Ok((
            row.get(0)?,
            row.get(1)?,
            JobPriority::new(row.get(2)?),
            row.get(3)?,
        ))
    })?
    .collect()
}

pub fn cancelling_jobs(conn: &Connection, job_kinds: &[String]) -> rusqlite::Result<Vec<Job>> {
    let kind_placeholders = (1..=job_kinds.len())
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let kind_filter = if job_kinds.is_empty() {
        String::new()
    } else {
        format!(" AND kind IN ({kind_placeholders})")
    };
    let mut statement = conn.prepare(&format!(
        "SELECT {JOB_SELECT_COLUMNS} FROM jobs
         WHERE status = 'cancelling'{kind_filter}
         ORDER BY created_at ASC, job_id ASC"
    ))?;
    statement
        .query_map(rusqlite::params_from_iter(job_kinds), map_job)?
        .collect()
}

/// Return at most `limit` permanently failed jobs (status=failed,
/// error_kind=permanent), newest first. Callers use [`count_permanent_failures`]
/// to learn the full total when they need it.
pub fn list_permanent_failures(conn: &Connection, limit: u32) -> rusqlite::Result<Vec<Job>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {JOB_SELECT_COLUMNS} FROM jobs
         WHERE status = 'failed' AND error_kind = 'permanent'
         ORDER BY completed_at DESC
         LIMIT ?1"
    ))?;
    stmt.query_map(params![limit as i64], map_job)?.collect()
}

/// Count of permanently failed jobs (status=failed, error_kind=permanent).
pub fn count_permanent_failures(conn: &Connection) -> rusqlite::Result<u64> {
    conn.query_row(
        "SELECT COUNT(*) FROM jobs WHERE status = 'failed' AND error_kind = 'permanent'",
        [],
        |row| row.get::<_, i64>(0),
    )
    .map(nonnegative_i64_to_u64)
}

/// Count jobs that have not yet reached a terminal state.
pub fn count_active_jobs(conn: &Connection) -> rusqlite::Result<u64> {
    conn.query_row(
        "SELECT COUNT(*) FROM jobs WHERE status IN ('pending', 'running', 'cancelling')",
        [],
        |row| row.get::<_, i64>(0),
    )
    .map(nonnegative_i64_to_u64)
}

/// `(kind, status, count)` rows for active jobs, suitable for emitting one
/// gauge sample per group. Terminal statuses are excluded — `failed` rows are
/// reported separately via [`count_permanent_failures_by_kind`].
pub fn count_active_jobs_by_kind_and_status(
    conn: &Connection,
) -> rusqlite::Result<Vec<(String, String, i64)>> {
    let mut stmt = conn.prepare(
        "SELECT kind, status, COUNT(*) AS n
         FROM jobs
         WHERE status IN ('pending', 'running', 'cancelling')
         GROUP BY kind, status",
    )?;
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect()
}

/// `(kind, age_seconds)` for the oldest `pending` row per kind. A queue that
/// is stuck because its dependency is unhealthy shows up here long before
/// individual jobs time out.
pub fn oldest_pending_age_secs_by_kind(conn: &Connection) -> rusqlite::Result<Vec<(String, i64)>> {
    let mut stmt = conn.prepare(
        "SELECT kind,
                CAST(strftime('%s', 'now') AS INTEGER)
              - CAST(strftime('%s', MIN(created_at)) AS INTEGER) AS age_secs
         FROM jobs
         WHERE status = 'pending'
         GROUP BY kind",
    )?;
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect()
}

/// `(kind, count)` for permanently failed rows per kind. Operators need this
/// between restarts; the existing summary log line only fires at shutdown.
pub fn count_permanent_failures_by_kind(conn: &Connection) -> rusqlite::Result<Vec<(String, i64)>> {
    let mut stmt = conn.prepare(
        "SELECT kind, COUNT(*) AS n
         FROM jobs
         WHERE status = 'failed' AND error_kind = 'permanent'
         GROUP BY kind",
    )?;
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect()
}

fn sqlite_duration_modifier(duration: Duration) -> String {
    format!("+{} seconds", duration.as_secs().max(1))
}

fn sqlite_past_duration_modifier(duration: Duration) -> String {
    format!("-{} seconds", duration.as_secs().max(1))
}

#[cfg(test)]
mod tests;
