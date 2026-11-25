//! Owning store for runtime-manager's `operation_results`, in its node-local
//! SQLite database: what each install or uninstall job did on disk.
//!
//! [`database`] migrates and opens the database for
//! `pplx-space-node-runtime-manager`, holds the connection types callers
//! name, and gives `pplx-space-db-migrate` its migration catalog. Its shared
//! `jobs` table is read and written by the common-jobs worker, which
//! runtime-manager reaches only through [`jobs::JobWorker`]. This crate's
//! `AGENTS.md` states the rules for extending either.

pub mod database;
pub mod jobs;

use std::path::{Path, PathBuf};
use std::str::FromStr;

use pplx_space_content_digest::Digest;
use pplx_space_job_id::JobId;
use pplx_space_sql_locator::sql;
use rusqlite::{OptionalExtension as _, params};

use crate::database::Connection;

/// The on-disk step a job is part-way through.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobPhase {
    Publishing,
    Removing,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unknown operation phase '{0}'")]
pub struct JobPhaseParseError(String);

impl JobPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Publishing => "publishing",
            Self::Removing => "removing",
        }
    }
}

impl FromStr for JobPhase {
    type Err = JobPhaseParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "publishing" => Ok(Self::Publishing),
            "removing" => Ok(Self::Removing),
            other => Err(JobPhaseParseError(other.to_string())),
        }
    }
}

impl std::fmt::Display for JobPhase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// What a finished operation achieved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobOutcome {
    Installed,
    AlreadyInstalled,
    Uninstalled,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unknown operation outcome '{0}'")]
pub struct JobOutcomeParseError(String);

impl JobOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Installed => "installed",
            Self::AlreadyInstalled => "already-installed",
            Self::Uninstalled => "uninstalled",
        }
    }
}

impl FromStr for JobOutcome {
    type Err = JobOutcomeParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "installed" => Ok(Self::Installed),
            "already-installed" => Ok(Self::AlreadyInstalled),
            "uninstalled" => Ok(Self::Uninstalled),
            other => Err(JobOutcomeParseError(other.to_string())),
        }
    }
}

impl std::fmt::Display for JobOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// What an operation did on disk. A job with no result has not reached a
/// node-manager mutation yet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JobResult {
    /// About to call node-manager, or awaiting its reply: the artifact digest.
    InProgress {
        phase: JobPhase,
        path: PathBuf,
        digest: Digest,
    },
    /// Finished: the kernel's directory and image SHA-256.
    Done {
        outcome: JobOutcome,
        path: PathBuf,
        digest: Digest,
    },
}

impl JobResult {
    pub fn path(&self) -> &Path {
        match self {
            Self::InProgress { path, .. } | Self::Done { path, .. } => path,
        }
    }

    pub fn digest(&self) -> Digest {
        match self {
            Self::InProgress { digest, .. } | Self::Done { digest, .. } => *digest,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum JobResultError {
    #[error("runtime-manager database")]
    Sqlite(#[from] database::Error),
    #[error("runtime-manager database holds an unreadable {column} value '{value}'")]
    Corrupt { column: &'static str, value: String },
}

fn parsed<T: FromStr>(column: &'static str, value: &str) -> Result<T, JobResultError> {
    value.parse().map_err(|_| JobResultError::Corrupt {
        column,
        value: value.to_string(),
    })
}

pub fn get_job_result(
    conn: &Connection,
    job_id: &JobId,
) -> Result<Option<JobResult>, JobResultError> {
    let row = conn
        .0
        .query_row(
            sql!("SELECT phase, outcome, path, digest FROM operation_results WHERE job_id = ?1"),
            params![job_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()
        .map_err(database::Error::from)?;
    let Some((phase, outcome, path, digest)) = row else {
        return Ok(None);
    };
    let path = PathBuf::from(path);
    let digest = parsed("digest", &digest)?;
    // The table's CHECK keeps exactly one of the two set.
    Ok(Some(match (phase, outcome) {
        (Some(phase), None) => JobResult::InProgress {
            phase: parsed("phase", &phase)?,
            path,
            digest,
        },
        (None, Some(outcome)) => JobResult::Done {
            outcome: parsed("outcome", &outcome)?,
            path,
            digest,
        },
        _ => {
            return Err(JobResultError::Corrupt {
                column: "phase",
                value: job_id.to_string(),
            });
        }
    }))
}

/// Record what a job did, replacing anything recorded for it before.
pub fn put_job_result(
    conn: &Connection,
    job_id: &JobId,
    result: &JobResult,
    at: jiff::Timestamp,
) -> Result<(), JobResultError> {
    let (phase, outcome) = match result {
        JobResult::InProgress { phase, .. } => (Some(phase.as_str()), None),
        JobResult::Done { outcome, .. } => (None, Some(outcome.as_str())),
    };
    conn.0
        .execute(
            sql!(
                "INSERT INTO operation_results (job_id, phase, outcome, path, digest, updated_at_unix_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT (job_id) DO UPDATE SET phase = excluded.phase,
                 outcome = excluded.outcome, path = excluded.path, digest = excluded.digest,
                 updated_at_unix_ms = excluded.updated_at_unix_ms"
            ),
            params![
                job_id,
                phase,
                outcome,
                path_text(result.path()),
                result.digest().to_string(),
                at.as_millisecond(),
            ],
        )
        .map_err(database::Error::from)?;
    Ok(())
}

/// Drop results once their terminal jobs have expired. A later request is
/// idempotent at node-manager; it needs no provenance from an earlier job.
pub fn delete_orphaned_job_results(conn: &Connection) -> Result<usize, JobResultError> {
    Ok(conn
        .0
        .execute(
            sql!(
                "DELETE FROM operation_results
             WHERE job_id NOT IN (SELECT job_id FROM jobs)"
            ),
            [],
        )
        .map_err(database::Error::from)?)
}

fn path_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests;
