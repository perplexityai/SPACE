//! build-manager's node-local SQLite store: `build_job_results`, what each
//! finished build job produced, and build-manager's reads of its shared `jobs`
//! table.
//!
//! The database's opening and PRAGMAs stay in `pplx-space-build-mgr-server`,
//! and `pplx-space-db-migrate` applies its migrations. The job queue itself is
//! read and written through `pplx-space-common-jobs`. The lease-guarded result
//! write and the expired-lease give-up stay in the server: their `WHERE`
//! re-checks the job's lease. Each function takes a `rusqlite::Connection`, so
//! a caller can pass its own open transaction, and returns rusqlite's error
//! unchanged.

#![forbid(unsafe_code)]

use pplx_space_job_id::JobId;
use pplx_space_sql_locator::sql;
use rusqlite::{Connection, OptionalExtension, params};

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
) -> rusqlite::Result<Option<BuildJobResult>> {
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
}

/// Record a job's result, replacing anything recorded for it before.
pub fn upsert_build_result(
    conn: &Connection,
    job_id: &JobId,
    result: &BuildJobResult,
) -> rusqlite::Result<()> {
    conn.execute(
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

/// Number of build jobs for `subject_id` (the template) that are pending or
/// running, excluding `exclude_job_id`. Gates the post-publish local prune:
/// any active sibling may be mid-register/mid-publish on an artifact the
/// prune would delete.
pub fn count_other_active_builds_for_subject(
    conn: &Connection,
    subject_id: &str,
    exclude_job_id: &JobId,
) -> rusqlite::Result<usize> {
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
}

/// Number of build jobs that are pending or running.
pub fn count_active_build_jobs(conn: &Connection) -> rusqlite::Result<usize> {
    conn.query_row(
        sql!(
            "SELECT COUNT(*) FROM jobs
         WHERE kind = ?1 AND status IN ('pending', 'running')"
        ),
        params![BUILD_JOB_KIND],
        |row| row.get::<_, i64>(0),
    )
    .map(sqlite_count_to_usize)
}

/// Number of running build jobs leased to `lease_owner`.
pub fn count_running_build_jobs_by_lease_owner(
    conn: &Connection,
    lease_owner: &str,
) -> rusqlite::Result<usize> {
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
}

fn sqlite_count_to_usize(count: i64) -> usize {
    usize::try_from(count).unwrap_or(0)
}

#[cfg(test)]
mod tests;
