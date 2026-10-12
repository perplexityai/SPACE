//! Durable operation intent and outcomes alongside the shared jobs table.
//! Artifact presence belongs to the node store, never to an ownership ledger.

use std::path::{Path, PathBuf};
use std::str::FromStr;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{Digest, JobId};
use rusqlite::{Connection, OptionalExtension as _, params};

static COMMON_SCHEMA_MIGRATOR: sqlx::migrate::Migrator =
    sqlx::migrate!("../../../../../../db/common");

pub(crate) const DATABASE_FILE: &str = "runtime-manager.db";
const DATABASE_NAME: &str = "runtime-manager";

/// The on-disk step a job is part-way through.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JobPhase {
    Publishing,
    Removing,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unknown operation phase '{0}'")]
pub(crate) struct JobPhaseParseError(String);

impl JobPhase {
    pub(crate) const fn as_str(self) -> &'static str {
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
pub(crate) enum JobOutcome {
    Installed,
    AlreadyInstalled,
    Uninstalled,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unknown operation outcome '{0}'")]
pub(crate) struct JobOutcomeParseError(String);

impl JobOutcome {
    pub(crate) const fn as_str(self) -> &'static str {
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
pub(crate) enum JobResult {
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
    pub(crate) fn path(&self) -> &Path {
        match self {
            Self::InProgress { path, .. } | Self::Done { path, .. } => path,
        }
    }

    pub(crate) fn digest(&self) -> Digest {
        match self {
            Self::InProgress { digest, .. } | Self::Done { digest, .. } => *digest,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("runtime-manager database")]
    Sqlite(#[from] rusqlite::Error),
    #[error("runtime-manager database migration")]
    Migrate(#[source] pplx_space_common_svc::sql::sqlite::SqliteError),
    #[error("cannot open the runtime-manager database")]
    Open(#[source] pplx_space_common_svc::sql::sqlite::SqliteError),
    #[error("runtime-manager database holds an unreadable {column} value '{value}'")]
    Corrupt { column: &'static str, value: String },
}

/// Migrate and open the database in `state_dir`, with the shared jobs schema.
/// The database is restricted to 0600 inside the already private directory.
pub(crate) async fn open(state_dir: &Path) -> Result<Connection, StoreError> {
    let path = state_dir.join(DATABASE_FILE);
    pplx_space_common_svc::sql::sqlite::migrate_embedded(
        DATABASE_NAME,
        &path,
        pplx_space_common_svc::sql::sqlite::EmbeddedMigrators {
            service: pplx_space_common_svc::sql::sqlite::runtime_manager::catalog(&path)
                .map_err(StoreError::Migrate)?,
            common: Some(&COMMON_SCHEMA_MIGRATOR),
        },
    )
    .await
    .map_err(StoreError::Migrate)?;
    pplx_space_common_svc::sql::sqlite::open_rusqlite(
        &path,
        pplx_space_common_svc::sql::sqlite::RusqliteOpenOptions::default(),
    )
    .map_err(StoreError::Open)
}

fn parsed<T: FromStr>(column: &'static str, value: &str) -> Result<T, StoreError> {
    value.parse().map_err(|_| StoreError::Corrupt {
        column,
        value: value.to_string(),
    })
}

pub(crate) fn get_job_result(
    conn: &Connection,
    job_id: &JobId,
) -> Result<Option<JobResult>, StoreError> {
    let row = conn
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
        .optional()?;
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
            return Err(StoreError::Corrupt {
                column: "phase",
                value: job_id.to_string(),
            });
        }
    }))
}

/// Record what a job did, replacing anything recorded for it before.
pub(crate) fn put_job_result(
    conn: &Connection,
    job_id: &JobId,
    result: &JobResult,
    at: jiff::Timestamp,
) -> Result<(), StoreError> {
    let (phase, outcome) = match result {
        JobResult::InProgress { phase, .. } => (Some(phase.as_str()), None),
        JobResult::Done { outcome, .. } => (None, Some(outcome.as_str())),
    };
    conn.execute(
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
    )?;
    Ok(())
}

/// Drop results once their terminal jobs have expired. A later request is
/// idempotent at node-manager; it needs no provenance from an earlier job.
pub(crate) fn delete_orphaned_job_results(conn: &Connection) -> Result<usize, StoreError> {
    Ok(conn.execute(
        sql!(
            "DELETE FROM operation_results
             WHERE job_id NOT IN (SELECT job_id FROM jobs)"
        ),
        [],
    )?)
}

fn path_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests;
