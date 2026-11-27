//! build-manager's node-local SQLite store: `build_job_results`, what each
//! finished build job produced, and build-manager's reads of its shared
//! `jobs` table and its expired-lease give-up there.
//!
//! [`database`] opens the database for `pplx-space-build-mgr-server` and
//! holds the connection types callers name; `pplx-space-db-migrate` applies
//! its migrations. The job queue itself is read and written by the
//! common-jobs worker, which build-manager reaches only through
//! [`jobs::JobWorker`]. This crate's `AGENTS.md` states the rules for
//! extending either. The lease-guarded result write and the expired-lease
//! give-up are verbs here with their `WHERE` whole. The result write's guard
//! checks that the job the caller names is `running` under the lease owner
//! the caller passes. The give-up's guard compares each running build job's
//! lease expiry with SQLite's clock, and a job with no expiry counts as
//! expired. Matching no row is an outcome, not an error. Each function takes
//! a [`database::Connection`], so it runs under the caller's held lock, and
//! fails with [`database::Error`], whose `Display` and `source` are
//! rusqlite's, unchanged.

#![forbid(unsafe_code)]

pub mod database;
pub mod jobs;

use pplx_space_job_id::JobId;
use pplx_space_sql_locator::sql;
use rusqlite::{OptionalExtension, params};

use crate::database::{Connection, Error};

/// The `jobs.kind` of every build job.
pub const BUILD_JOB_KIND: &str = "template_build";

#[derive(Clone, Debug)]
pub struct BuildJobResult {
    pub template_id: String,
    pub state: BuildTerminalState,
    pub content_hash: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildTerminalState {
    Succeeded,
    Failed,
    PublishFailed,
}

impl BuildTerminalState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::PublishFailed => "publish_failed",
        }
    }
}

impl std::fmt::Display for BuildTerminalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for BuildTerminalState {
    type Err = ParseBuildTerminalStateError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "publish_failed" => Ok(Self::PublishFailed),
            other => Err(ParseBuildTerminalStateError {
                value: other.to_string(),
            }),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unknown build terminal state: {value}")]
pub struct ParseBuildTerminalStateError {
    value: String,
}

pub fn get_build_result(
    conn: &Connection,
    job_id: &JobId,
) -> Result<Option<BuildJobResult>, Error> {
    let conn = &conn.0;
    conn.query_row(
        sql!(
            "SELECT template_id, state, content_hash, error
         FROM build_job_results
         WHERE job_id = ?1"
        ),
        params![job_id],
        |row| {
            let state: String = row.get(1)?;
            let state = state.parse().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
            Ok(BuildJobResult {
                template_id: row.get(0)?,
                state,
                content_hash: row.get(2)?,
                error: row.get(3)?,
            })
        },
    )
    .optional()
    .map_err(Error::from)
}

/// Record a job's result, replacing anything recorded for it before.
pub fn upsert_build_result(
    conn: &Connection,
    job_id: &JobId,
    result: &BuildJobResult,
) -> Result<(), Error> {
    conn.0.execute(
        sql!(
            "INSERT INTO build_job_results (job_id, template_id, state, content_hash, error)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(job_id) DO UPDATE SET
             template_id = excluded.template_id,
             state = excluded.state,
             content_hash = excluded.content_hash,
             error = excluded.error,
             completed_at = datetime('now')"
        ),
        params![
            job_id,
            result.template_id,
            result.state.as_str(),
            result.content_hash,
            result.error,
        ],
    )?;
    Ok(())
}

/// Record a job's result, replacing anything recorded for it before, only
/// while the job is `running` and its `lease_owner` is `lease_owner`. Returns
/// whether the row was written; `false` means the job is not running under
/// `lease_owner`, and nothing changed.
pub fn record_build_result_as_lease_owner(
    conn: &Connection,
    job_id: &JobId,
    lease_owner: &str,
    result: &BuildJobResult,
) -> Result<bool, Error> {
    let rows = conn.0.execute(
        sql!(
            "INSERT INTO build_job_results (job_id, template_id, state, content_hash, error)
         SELECT ?1, ?3, ?4, ?5, ?6
         WHERE EXISTS (
             SELECT 1 FROM jobs
             WHERE job_id = ?1
               AND lease_owner = ?2
               AND status = 'running'
         )
         ON CONFLICT(job_id) DO UPDATE SET
             template_id = excluded.template_id,
             state = excluded.state,
             content_hash = excluded.content_hash,
             error = excluded.error,
             completed_at = datetime('now')
         WHERE EXISTS (
             SELECT 1 FROM jobs
             WHERE job_id = ?1
               AND lease_owner = ?2
               AND status = 'running'
         )"
        ),
        params![
            job_id,
            lease_owner,
            result.template_id,
            result.state.as_str(),
            result.content_hash,
            result.error,
        ],
    )?;
    Ok(rows > 0)
}

/// Permanently fails running build jobs whose lease expired (or that predate
/// lease tracking) and returns the failed `(job_id, subject_id)` pairs so the
/// caller can reap each job's staging directory — the owning process is gone
/// and won't clean up after itself. An empty result means no running build
/// job had an expired or missing lease.
///
/// A single `UPDATE … RETURNING` keeps the expiry check and the row capture
/// atomic: a select-then-update split would let a live worker's heartbeat
/// land between the two, failing a healthy build and deleting its staging
/// dir out from under it. The expiry compares against SQLite's clock.
pub fn fail_expired_running_builds(
    conn: &Connection,
    error: &str,
) -> Result<Vec<(JobId, Option<String>)>, Error> {
    let mut stmt = conn.0.prepare(sql!(
        "UPDATE jobs
         SET status = 'failed',
             error_kind = 'permanent',
             error = ?2,
             completed_at = datetime('now'),
             lease_owner = NULL,
             lease_expires_at = NULL,
             heartbeat_at = NULL
         WHERE kind = ?1
           AND status = 'running'
           AND (
               lease_expires_at IS NULL
               OR lease_expires_at <= datetime('now')
           )
         RETURNING job_id, subject_id"
    ))?;
    stmt.query_map(params![BUILD_JOB_KIND, error], |row| {
        Ok((row.get(0)?, row.get(1)?))
    })?
    .collect::<rusqlite::Result<_>>()
    .map_err(Error::from)
}

/// Number of build jobs for `subject_id` (the template) that are pending or
/// running, excluding `exclude_job_id`. Gates the post-publish local prune:
/// any active sibling may be mid-register/mid-publish on an artifact the
/// prune would delete.
pub fn count_other_active_builds_for_subject(
    conn: &Connection,
    subject_id: &str,
    exclude_job_id: &JobId,
) -> Result<usize, Error> {
    let conn = &conn.0;
    conn.query_row(
        sql!(
            "SELECT COUNT(*) FROM jobs
         WHERE kind = ?1
           AND subject_id = ?2
           AND job_id != ?3
           AND status IN ('pending', 'running')"
        ),
        params![BUILD_JOB_KIND, subject_id, exclude_job_id],
        |row| row.get::<_, i64>(0),
    )
    .map(sqlite_count_to_usize)
    .map_err(Error::from)
}

/// Number of build jobs that are pending or running.
pub fn count_active_build_jobs(conn: &Connection) -> Result<usize, Error> {
    let conn = &conn.0;
    conn.query_row(
        sql!(
            "SELECT COUNT(*) FROM jobs
         WHERE kind = ?1 AND status IN ('pending', 'running')"
        ),
        params![BUILD_JOB_KIND],
        |row| row.get::<_, i64>(0),
    )
    .map(sqlite_count_to_usize)
    .map_err(Error::from)
}

/// Number of running build jobs leased to `lease_owner`.
pub fn count_running_build_jobs_by_lease_owner(
    conn: &Connection,
    lease_owner: &str,
) -> Result<usize, Error> {
    let conn = &conn.0;
    conn.query_row(
        sql!(
            "SELECT COUNT(*) FROM jobs
         WHERE kind = ?1
           AND status = 'running'
           AND lease_owner = ?2"
        ),
        params![BUILD_JOB_KIND, lease_owner],
        |row| row.get::<_, i64>(0),
    )
    .map(sqlite_count_to_usize)
    .map_err(Error::from)
}

fn sqlite_count_to_usize(count: i64) -> usize {
    usize::try_from(count).unwrap_or(0)
}

#[cfg(test)]
mod tests;
