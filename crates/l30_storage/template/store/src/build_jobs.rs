//! The builds a builder accepted and the specs requested for them
//! (`template_build_jobs`), keyed by template and build id.
//!
//! A row records the organization that wrote it. A write to a row another
//! organization recorded matches nothing and reports `false`; a row with no
//! organization is anyone's.

use pplx_space_accounts::{NodeId, OrgId};
use pplx_space_sql_locator::sql;
use pplx_space_template_model::{BuildRunId, TemplateName};
use sqlx::{FromRow, PgExecutor};
use time::OffsetDateTime;

/// A build a builder accepted, for [`record_accepted`].
#[derive(Clone, Copy, Debug)]
pub struct AcceptedBuild<'a> {
    pub template: &'a TemplateName,
    pub build_id: &'a str,
    pub daemon_job_id: BuildRunId,
    /// The builder that accepted the job, when the job was pinned to one.
    pub builder_node_id: Option<NodeId>,
    pub org_id: OrgId,
}

/// Record `build`'s job and builder, creating its row or updating the one
/// the spec wrote. `false` when another organization recorded the row.
pub async fn record_accepted(
    executor: impl PgExecutor<'_>,
    build: AcceptedBuild<'_>,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!(
        "INSERT INTO template_build_jobs (
                 template_id,
                 build_id,
                 daemon_job_id,
                 builder_node_id,
                 org_id
             )
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (template_id, build_id) DO UPDATE
             SET daemon_job_id = EXCLUDED.daemon_job_id,
                 builder_node_id = EXCLUDED.builder_node_id
             WHERE template_build_jobs.org_id IS NULL OR template_build_jobs.org_id = EXCLUDED.org_id"
    ))
    .bind(build.template.as_str())
    .bind(build.build_id)
    .bind(build.daemon_job_id.to_string())
    .bind(build.builder_node_id.map(NodeId::as_uuid))
    .bind(build.org_id.as_uuid())
    .execute(executor)
    .await?;
    Ok(result.rows_affected() != 0)
}

/// The requested shape of a build, for [`record_spec`].
#[derive(Clone, Copy, Debug)]
pub struct BuildSpec<'a> {
    pub tags: &'a [String],
    pub cpu_count: Option<i32>,
    pub memory_mb: Option<i32>,
}

/// Record `spec` for `org_id`'s build `build_id` of `template`, creating its
/// row or updating its spec. `false` when another organization recorded the
/// row.
pub async fn record_spec(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
    build_id: &str,
    org_id: OrgId,
    spec: BuildSpec<'_>,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!(
        "INSERT INTO template_build_jobs (template_id, build_id, cpu_count, memory_mb, tags, org_id)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (template_id, build_id) DO UPDATE
             SET cpu_count = EXCLUDED.cpu_count,
                 memory_mb = EXCLUDED.memory_mb,
                 tags = EXCLUDED.tags
             WHERE template_build_jobs.org_id IS NULL OR template_build_jobs.org_id = EXCLUDED.org_id"
    ))
    .bind(template.as_str())
    .bind(build_id)
    .bind(spec.cpu_count)
    .bind(spec.memory_mb)
    .bind(spec.tags)
    .bind(org_id.as_uuid())
    .execute(executor)
    .await?;
    Ok(result.rows_affected() != 0)
}

/// The stored job id of build `build_id` of `template`, unparsed, when a
/// builder accepted it and the row is `org_id`'s or no organization's.
pub async fn daemon_job_id(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
    build_id: &str,
    org_id: OrgId,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>(sql!(
        "SELECT daemon_job_id
             FROM template_build_jobs
             WHERE template_id = $1
               AND build_id = $2
               AND (org_id = $3 OR org_id IS NULL)
               AND daemon_job_id IS NOT NULL"
    ))
    .bind(template.as_str())
    .bind(build_id)
    .bind(org_id.as_uuid())
    .fetch_optional(executor)
    .await
}

/// The builder that accepted job `daemon_job_id`, or `None` when no row
/// records the job, it was routed without a pinned builder, or the builder's
/// node row has since been deleted.
pub async fn builder_node(
    executor: impl PgExecutor<'_>,
    daemon_job_id: BuildRunId,
) -> Result<Option<NodeId>, sqlx::Error> {
    sqlx::query_as::<_, (Option<uuid::Uuid>,)>(sql!(
        "SELECT builder_node_id
             FROM template_build_jobs
             WHERE daemon_job_id = $1"
    ))
    .bind(daemon_job_id.to_string())
    .fetch_optional(executor)
    .await
    .map(|row| row.and_then(|(builder_node_id,)| builder_node_id.map(NodeId::from_uuid)))
}

/// A build a builder accepted, as [`recent_accepted`] reads it.
#[derive(Debug, FromRow)]
pub struct RecordedBuild {
    /// The job id, unparsed.
    pub daemon_job_id: String,
    pub builder_node_id: Option<uuid::Uuid>,
    pub created_at: OffsetDateTime,
}

/// The newest `limit` builds of `template` that a builder accepted, newest
/// first. Rows recorded before submission carry no job id and are left out.
pub async fn recent_accepted(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
    limit: u32,
) -> Result<Vec<RecordedBuild>, sqlx::Error> {
    sqlx::query_as::<_, RecordedBuild>(sql!(
        "SELECT daemon_job_id, builder_node_id, created_at
             FROM template_build_jobs
             WHERE template_id = $1
               AND daemon_job_id IS NOT NULL
             ORDER BY created_at DESC, build_id DESC
             LIMIT $2"
    ))
    .bind(template.as_str())
    .bind(i64::from(limit))
    .fetch_all(executor)
    .await
}

/// A build's stored spec, as [`spec`] reads it.
#[derive(Debug, FromRow)]
pub struct StoredBuildSpec {
    pub tags: Vec<String>,
    pub cpu_count: Option<i32>,
    pub memory_mb: Option<i32>,
}

/// The spec of build `build_id` of `template`, when the row is `org_id`'s or
/// no organization's.
pub async fn spec(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
    build_id: &str,
    org_id: OrgId,
) -> Result<Option<StoredBuildSpec>, sqlx::Error> {
    sqlx::query_as::<_, StoredBuildSpec>(sql!(
        "SELECT tags, cpu_count, memory_mb
             FROM template_build_jobs
             WHERE template_id = $1
               AND build_id = $2
               AND (org_id = $3 OR org_id IS NULL)"
    ))
    .bind(template.as_str())
    .bind(build_id)
    .bind(org_id.as_uuid())
    .fetch_optional(executor)
    .await
}
