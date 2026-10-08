//! The runtime manager's durable kernel records, beside the shared `jobs`
//! table that carries each kernel operation's request and lifecycle: one row
//! per kernel it installed, and one per job recording what the job did on
//! disk.
//!
//! Install rows are the only source of space-managed ownership, so the store
//! fails closed: a migration failure stops the service rather than recreating
//! an empty database that would disown every install.
//!
//! Verbs take a `&Connection` so callers compose them inside a transaction
//! (`rusqlite::Transaction` derefs to one).

use std::path::{Path, PathBuf};
use std::str::FromStr;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{Digest, JobId, StorageUri};
use rusqlite::{Connection, OptionalExtension as _, params};

use super::source::{ContentMd5, KernelVersion, RuntimeLine};

pub(crate) static MIGRATOR: sqlx::migrate::Migrator =
    sqlx::migrate!("../../../../../../db/runtime-manager");
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
#[error("unknown kernel job phase '{0}'")]
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

/// What a finished kernel job achieved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JobOutcome {
    Installed,
    AlreadyInstalled,
    ProvidedExternally,
    Uninstalled,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unknown kernel job outcome '{0}'")]
pub(crate) struct JobOutcomeParseError(String);

impl JobOutcome {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Installed => "installed",
            Self::AlreadyInstalled => "already-installed",
            Self::ProvidedExternally => "provided-externally",
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
            "provided-externally" => Ok(Self::ProvidedExternally),
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

/// What a kernel job did on disk. A job with no result has not reached a
/// rename yet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum JobResult {
    /// About to rename, or renaming: the directory, and the image's SHA-256.
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct KernelInstall {
    pub(crate) version: KernelVersion,
    pub(crate) path: PathBuf,
    /// The runtime line the kernel was installed for.
    pub(crate) line: RuntimeLine,
    /// The source object it was downloaded from.
    pub(crate) location: StorageUri,
    pub(crate) size: u64,
    pub(crate) md5: ContentMd5,
    /// The installed image's SHA-256.
    pub(crate) digest: Digest,
    /// The job that installed it; `None` for a kernel seeded from the node
    /// image's manifest.
    pub(crate) job_id: Option<JobId>,
    pub(crate) installed_at: jiff::Timestamp,
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
            service: &MIGRATOR,
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
            sql!("SELECT phase, outcome, path, digest FROM kernel_job_results WHERE job_id = ?1"),
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
            "INSERT INTO kernel_job_results (job_id, phase, outcome, path, digest, updated_at_unix_ms)
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

/// Forget a job's result: it never renamed anything after all.
pub(crate) fn clear_job_result(conn: &Connection, job_id: &JobId) -> Result<(), StoreError> {
    conn.execute(
        sql!("DELETE FROM kernel_job_results WHERE job_id = ?1"),
        params![job_id],
    )?;
    Ok(())
}

/// Drop outcomes whose job the shared worker has pruned. An unfinished step
/// is kept: it records a rename that may have landed, which a later install
/// of the same bytes adopts.
pub(crate) fn delete_orphaned_job_results(conn: &Connection) -> Result<usize, StoreError> {
    Ok(conn.execute(
        sql!(
            "DELETE FROM kernel_job_results
             WHERE outcome IS NOT NULL AND job_id NOT IN (SELECT job_id FROM jobs)"
        ),
        [],
    )?)
}

/// One job's recorded rename that it has not resolved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UnfinishedStep {
    pub(crate) job_id: JobId,
    pub(crate) phase: JobPhase,
    pub(crate) path: PathBuf,
    pub(crate) digest: Digest,
}

/// Every job's unfinished step.
pub(crate) fn list_unfinished_steps(conn: &Connection) -> Result<Vec<UnfinishedStep>, StoreError> {
    let mut statement = conn.prepare(sql!(
        "SELECT job_id, phase, path, digest FROM kernel_job_results
         WHERE phase IS NOT NULL ORDER BY job_id"
    ))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, JobId>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|(job_id, phase, path, digest)| {
            Ok(UnfinishedStep {
                job_id,
                phase: parsed("phase", &phase)?,
                path: PathBuf::from(path),
                digest: parsed("digest", &digest)?,
            })
        })
        .collect()
}

/// The job, if any, that recorded publishing `digest` to `path` and has not
/// resolved it.
pub(crate) fn find_unfinished_publication(
    conn: &Connection,
    path: &Path,
    digest: Digest,
) -> Result<Option<JobId>, StoreError> {
    Ok(conn
        .query_row(
            sql!(
                "SELECT job_id FROM kernel_job_results
                 WHERE phase = ?1 AND path = ?2 AND digest = ?3
                 ORDER BY job_id LIMIT 1"
            ),
            params![
                JobPhase::Publishing.as_str(),
                path_text(path),
                digest.to_string()
            ],
            |row| row.get(0),
        )
        .optional()?)
}

/// Record an install. A version already recorded is left as it is, and the
/// call returns `false`: one record per version, enforced by the key.
pub(crate) fn insert_kernel_install(
    conn: &Connection,
    install: &KernelInstall,
) -> Result<bool, StoreError> {
    let inserted = conn.execute(
        sql!(
            "INSERT INTO kernel_installs (kernel_version, path, runtime, runtime_line, source,
                size, source_md5, digest, job_id, installed_at_unix_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT (kernel_version) DO NOTHING"
        ),
        params![
            install.version.as_str(),
            path_text(&install.path),
            install.line.runtime.as_str(),
            install.line.release.to_string(),
            install.location.to_string(),
            i64::try_from(install.size).map_err(|_| StoreError::Corrupt {
                column: "size",
                value: install.size.to_string(),
            })?,
            install.md5.to_string(),
            install.digest.to_string(),
            install.job_id,
            install.installed_at.as_millisecond(),
        ],
    )?;
    Ok(inserted == 1)
}

pub(crate) fn get_kernel_install(
    conn: &Connection,
    version: &KernelVersion,
) -> Result<Option<KernelInstall>, StoreError> {
    let row = conn
        .query_row(
            sql!(
                "SELECT kernel_version, path, runtime, runtime_line, source, size, source_md5, digest,
                    job_id, installed_at_unix_ms
                 FROM kernel_installs WHERE kernel_version = ?1"
            ),
            params![version.as_str()],
            InstallRow::read,
        )
        .optional()?;
    row.map(InstallRow::parse).transpose()
}

pub(crate) fn list_kernel_installs(conn: &Connection) -> Result<Vec<KernelInstall>, StoreError> {
    let mut statement = conn.prepare(sql!(
        "SELECT kernel_version, path, runtime, runtime_line, source, size, source_md5, digest,
            job_id, installed_at_unix_ms
         FROM kernel_installs ORDER BY kernel_version"
    ))?;
    let rows = statement
        .query_map([], InstallRow::read)?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter().map(InstallRow::parse).collect()
}

/// Returns whether a row was removed.
pub(crate) fn delete_kernel_install(
    conn: &Connection,
    version: &KernelVersion,
) -> Result<bool, StoreError> {
    let removed = conn.execute(
        sql!("DELETE FROM kernel_installs WHERE kernel_version = ?1"),
        params![version.as_str()],
    )?;
    Ok(removed == 1)
}

/// Paths are written by this service from UTF-8 configuration, so a lossy
/// conversion never alters one.
fn path_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

struct InstallRow {
    version: String,
    path: String,
    runtime: String,
    runtime_line: String,
    location: String,
    size: i64,
    md5: String,
    digest: String,
    job_id: Option<JobId>,
    installed_at_unix_ms: i64,
}

impl InstallRow {
    fn read(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            version: row.get(0)?,
            path: row.get(1)?,
            runtime: row.get(2)?,
            runtime_line: row.get(3)?,
            location: row.get(4)?,
            size: row.get(5)?,
            md5: row.get(6)?,
            digest: row.get(7)?,
            job_id: row.get(8)?,
            installed_at_unix_ms: row.get(9)?,
        })
    }

    fn parse(self) -> Result<KernelInstall, StoreError> {
        let corrupt = |column, value: String| StoreError::Corrupt { column, value };
        Ok(KernelInstall {
            version: parsed("kernel_version", &self.version)?,
            path: PathBuf::from(self.path),
            line: RuntimeLine::new(
                parsed("runtime", &self.runtime)?,
                parsed("runtime_line", &self.runtime_line)?,
            ),
            location: parsed("source", &self.location)?,
            size: u64::try_from(self.size).map_err(|_| corrupt("size", self.size.to_string()))?,
            md5: parsed("source_md5", &self.md5)?,
            digest: parsed("digest", &self.digest)?,
            job_id: self.job_id,
            installed_at: jiff::Timestamp::from_millisecond(self.installed_at_unix_ms).map_err(
                |_| {
                    corrupt(
                        "installed_at_unix_ms",
                        self.installed_at_unix_ms.to_string(),
                    )
                },
            )?,
        })
    }
}

#[cfg(test)]
mod tests;
