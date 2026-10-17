use std::sync::{Arc, Mutex, MutexGuard};

use pplx_space_build_mgr::{SystemPublicationGrantId, TemplateOrg, TenantPublicationGrantId};
use pplx_space_build_mgr_client::proto::BuildState;
use pplx_space_metrics::template::TemplateBuildResult;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

pub const BUILD_JOB_KIND: &str = "template_build";

// The job kind remains stable for queue accounting and scheduling. Nest the
// legacy fields so a downgraded worker cannot interpret selection as E0.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "schema", deny_unknown_fields)]
pub enum VersionedBuildJobPayload {
    #[serde(rename = "environment_v1")]
    Environment {
        environment: pplx_space_common_core::types::EnvironmentReference,
        build: BuildJobPayload,
    },
}

pub fn decode_payload(
    job: &pplx_space_common_jobs::Job,
) -> Result<
    (
        BuildJobPayload,
        Option<pplx_space_common_core::types::EnvironmentReference>,
    ),
    pplx_space_common_jobs::JobError,
> {
    if job.kind != BUILD_JOB_KIND {
        return Err(pplx_space_common_jobs::JobError::permanent(
            "unsupported build job kind",
        ));
    }
    // Dispatch on the envelope before decoding its body so malformed or future
    // envelopes cannot fall back to an ordinary build or erase the parse cause.
    let value = job.payload_as::<serde_json::Value>()?;
    if value.get("schema").is_some() {
        let VersionedBuildJobPayload::Environment { environment, build } = job.payload_as()?;
        Ok((build, Some(environment)))
    } else {
        Ok((job.payload_as()?, None))
    }
}

fn db_lock(db: &Arc<Mutex<Connection>>) -> rusqlite::Result<MutexGuard<'_, Connection>> {
    db.lock().map_err(|error| {
        rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(format!(
            "build-manager db mutex poisoned: {error}"
        ))))
    })
}

pub(super) fn sqlite_count_to_usize(count: i64) -> usize {
    usize::try_from(count).unwrap_or(0)
}

/// Legacy flat jobs retain additive compatibility. New execution-critical
/// semantics belong in a versioned envelope rather than an ignorable flat field.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BuildJobPayload {
    pub recipe_proto: Vec<u8>,
    pub recipe_label: String,
    pub template_id: String,
    pub force: bool,
    pub publish_tags: Vec<String>,
    pub template_org: Option<TemplateOrg>,
    #[serde(default)]
    pub system_publication_grant_id: Option<SystemPublicationGrantId>,
    /// Absent for jobs queued before tenant grant preparation.
    #[serde(default)]
    pub tenant_publication_grant_id: Option<TenantPublicationGrantId>,
}

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

    pub fn as_metric_result(self) -> TemplateBuildResult {
        match self {
            Self::Succeeded => TemplateBuildResult::Success,
            Self::Failed => TemplateBuildResult::Failed,
            Self::PublishFailed => TemplateBuildResult::PublishFailed,
        }
    }

    pub fn as_build_state(self) -> BuildState {
        match self {
            Self::Succeeded => BuildState::Succeeded,
            Self::Failed => BuildState::Failed,
            Self::PublishFailed => BuildState::PublishFailed,
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
    job_id: &pplx_space_common_jobs::JobId,
) -> rusqlite::Result<Option<BuildJobResult>> {
    conn.query_row(
        "SELECT template_id, state, content_hash, error
         FROM build_job_results
         WHERE job_id = ?1",
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

pub fn store_build_result(
    db: &Arc<Mutex<Connection>>,
    job_id: &pplx_space_common_jobs::JobId,
    result: &BuildJobResult,
) -> rusqlite::Result<()> {
    let conn = db_lock(db)?;
    conn.execute(
        "INSERT INTO build_job_results (job_id, template_id, state, content_hash, error)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(job_id) DO UPDATE SET
             template_id = excluded.template_id,
             state = excluded.state,
             content_hash = excluded.content_hash,
             error = excluded.error,
             completed_at = datetime('now')",
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

pub fn store_build_result_if_lease_owner(
    db: &Arc<Mutex<Connection>>,
    job_id: &pplx_space_common_jobs::JobId,
    lease_owner: &str,
    result: &BuildJobResult,
) -> rusqlite::Result<bool> {
    let conn = db_lock(db)?;
    let rows = conn.execute(
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
         )",
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
/// and won't clean up after itself.
///
/// A single `UPDATE … RETURNING` keeps the expiry check and the row capture
/// atomic: a select-then-update split would let a live worker's heartbeat
/// land between the two, failing a healthy build and deleting its staging
/// dir out from under it.
pub fn fail_expired_running_builds(
    conn: &Connection,
    error: &str,
) -> rusqlite::Result<Vec<(pplx_space_common_jobs::JobId, Option<String>)>> {
    let mut stmt = conn.prepare(
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
         RETURNING job_id, subject_id",
    )?;
    stmt.query_map(params![BUILD_JOB_KIND, error], |row| {
        Ok((row.get(0)?, row.get(1)?))
    })?
    .collect()
}

/// Number of build jobs for `subject_id` (the template) that are pending or
/// running, excluding `exclude_job_id`. Gates the post-publish local prune:
/// any active sibling may be mid-register/mid-publish on an artifact the
/// prune would delete.
pub fn count_other_active_builds_for_subject(
    conn: &Connection,
    subject_id: &str,
    exclude_job_id: &pplx_space_common_jobs::JobId,
) -> rusqlite::Result<usize> {
    conn.query_row(
        "SELECT COUNT(*) FROM jobs
         WHERE kind = ?1
           AND subject_id = ?2
           AND job_id != ?3
           AND status IN ('pending', 'running')",
        params![BUILD_JOB_KIND, subject_id, exclude_job_id],
        |row| row.get::<_, i64>(0),
    )
    .map(sqlite_count_to_usize)
}

#[cfg(test)]
mod tests;
