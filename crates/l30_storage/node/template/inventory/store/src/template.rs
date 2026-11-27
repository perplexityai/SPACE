//! Reads of what the nodes cache of one template, or of a few.

use pplx_space_execution_spec::RuntimeKind;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use pplx_space_template_model::{TemplateDesiredState, TemplateName};
use serde::de::DeserializeOwned;
use sqlx::FromRow;
use sqlx::types::Json;

/// The VM shape a node reports a build of a template was built for, each
/// value as stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromRow)]
pub struct ReleaseShape {
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
}

/// A template's warm pool as its nodes report it.
#[derive(Debug, Clone, FromRow)]
pub struct TemplatePoolObserved {
    pub pool_target: Option<i32>,
    pub pool_current: i32,
    pub pool_pending: i32,
    pub pool_sandbox_ids: Json<Vec<String>>,
}

/// One cached build variant (template, hash, runtime) of a template, from
/// the node that reported it last.
#[derive(Debug, Clone, FromRow)]
pub struct TemplateVariant<Snapshots> {
    pub template_name: String,
    pub content_hash: String,
    pub runtime: Option<String>,
    pub arch: Option<String>,
    pub built_at: Option<String>,
    pub artifact_size_bytes: Option<i64>,
    pub has_base_snapshot: bool,
    pub base_snapshot_size_bytes: Option<i64>,
    pub snapshots: Json<Snapshots>,
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
    pub disk_size_mb: Option<i32>,
}

/// How many cached builds of `template_name` nodes report, across nodes.
pub async fn template_inventory_rows(
    executor: impl Executor<'_>,
    template_name: &str,
) -> Result<i64, DbError> {
    Ok(sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COUNT(*)::BIGINT FROM node_template_inventory WHERE template_name = $1"
    ))
    .bind(template_name)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// The shape a present template's builds report, preferring a worker's row,
/// then a builder's, then the latest observed, built and lowest hash. `None`
/// when no node reports either value.
pub async fn template_snapshot_shape(
    executor: impl Executor<'_>,
    template_name: &TemplateName,
) -> Result<Option<ReleaseShape>, DbError> {
    Ok(sqlx::query_as::<_, ReleaseShape>(sql!(
        "SELECT nti.vcpu, nti.memory_mib
         FROM node_template_inventory nti
         JOIN nodes n ON n.id = nti.node_id
         JOIN templates t ON t.name = nti.template_name
         WHERE nti.template_name = $1
           AND t.desired_state = $2
           AND (nti.vcpu IS NOT NULL OR nti.memory_mib IS NOT NULL)
         ORDER BY CASE n.role WHEN 'worker' THEN 0 WHEN 'builder' THEN 1 ELSE 2 END,
                  nti.observed_at DESC,
                  nti.built_at DESC NULLS LAST,
                  nti.content_hash ASC
         LIMIT 1"
    ))
    .bind(template_name.as_str())
    .bind(TemplateDesiredState::Present.as_str())
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The shape of build `content_hash` of `template` for `runtime` (or no
/// runtime), from a node reporting both values positive: the builder's row
/// first, then the latest observed. `None` when no node reports one.
pub async fn release_build_shape(
    executor: impl Executor<'_>,
    template: &TemplateName,
    content_hash: &str,
    runtime: RuntimeKind,
) -> Result<Option<ReleaseShape>, DbError> {
    Ok(sqlx::query_as::<_, ReleaseShape>(sql!(
        "SELECT nti.vcpu, nti.memory_mib
         FROM node_template_inventory nti
         JOIN nodes n ON n.id = nti.node_id
         WHERE nti.template_name = $1
           AND nti.content_hash = $2
           AND (nti.runtime IS NULL OR nti.runtime = '' OR nti.runtime = $3)
           AND nti.vcpu > 0
           AND nti.memory_mib > 0
         ORDER BY (n.role = 'builder') DESC, nti.observed_at DESC
         LIMIT 1"
    ))
    .bind(template.as_str())
    .bind(content_hash)
    .bind(runtime.alias())
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The execution environment every node holding `content_hash` (either
/// spelling of the digest) of `template` with a binding outside `excluded`
/// reports it bound to, unparsed. `None` when none does, or when they
/// disagree.
pub async fn agreed_build_environment(
    executor: impl Executor<'_>,
    template: &str,
    content_hash: &str,
    excluded: &[String],
) -> Result<Option<String>, DbError> {
    Ok(sqlx::query_scalar::<_, Option<String>>(sql!(
        "SELECT CASE WHEN COUNT(DISTINCT bindings.environment) = 1
                     THEN MIN(bindings.environment) END
         FROM (
             SELECT nti.execution_metadata->>'environment' AS environment
             FROM node_template_inventory nti
             WHERE nti.template_name = $1
               AND nti.content_hash IN (replace($2, 'sha256:', ''),
                                        'sha256:' || replace($2, 'sha256:', ''))
               AND nti.execution_metadata->>'environment' IS NOT NULL
               AND NOT (nti.execution_metadata->>'environment' = ANY($3))
         ) bindings"
    ))
    .bind(template)
    .bind(content_hash)
    .bind(excluded)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// The warm pool `template_name`'s nodes report, restricted to the build the
/// newest builder row names when there is one. `None` when no node reports a
/// pool target.
pub async fn template_pool_observed(
    executor: impl Executor<'_>,
    template_name: &TemplateName,
) -> Result<Option<TemplatePoolObserved>, DbError> {
    Ok(sqlx::query_as::<_, TemplatePoolObserved>(sql!(
        "WITH builder_metadata AS (
             SELECT nti.content_hash
             FROM node_template_inventory nti
             JOIN nodes n ON n.id = nti.node_id
             WHERE nti.template_name = $1
               AND n.role = 'builder'
             ORDER BY nti.observed_at DESC, nti.built_at DESC NULLS LAST, nti.content_hash ASC
             LIMIT 1
         ),
         observed AS (
             SELECT SUM(nti.pool_target)::INTEGER AS pool_target,
                    COALESCE(SUM(nti.pool_current), 0)::INTEGER AS pool_current,
                    COALESCE(SUM(nti.pool_pending), 0)::INTEGER AS pool_pending
             FROM node_template_inventory nti
             LEFT JOIN builder_metadata bm ON TRUE
             WHERE nti.template_name = $1
               AND nti.pool_target IS NOT NULL
               AND (NULLIF(bm.content_hash, '') IS NULL OR nti.content_hash = bm.content_hash)
         )
         SELECT observed.pool_target,
                observed.pool_current,
                observed.pool_pending,
                COALESCE(
                    (
                        SELECT JSONB_AGG(DISTINCT sandbox_id)
                        FROM node_template_inventory nti2
                        LEFT JOIN builder_metadata bm ON TRUE
                        LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS_TEXT(nti2.pool_sandbox_ids) AS sandbox_id ON TRUE
                        WHERE nti2.template_name = $1
                          AND (NULLIF(bm.content_hash, '') IS NULL OR nti2.content_hash = bm.content_hash)
                          AND sandbox_id IS NOT NULL
                    ),
                    '[]'::JSONB
                ) AS pool_sandbox_ids
         FROM observed
         WHERE observed.pool_target IS NOT NULL"
    ))
    .bind(template_name.as_str())
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Every cached build variant of `template_names`, each from its latest
/// report, by template then newest build.
pub async fn template_variants<Snapshots>(
    executor: impl Executor<'_>,
    template_names: &[String],
) -> Result<Vec<TemplateVariant<Snapshots>>, DbError>
where
    Snapshots: DeserializeOwned + Send + Unpin + 'static,
{
    Ok(sqlx::query_as::<_, TemplateVariant<Snapshots>>(sql!(
        "SELECT *
         FROM (
             SELECT DISTINCT ON (template_name, content_hash, COALESCE(runtime, ''))
                    template_name,
                    content_hash,
                    runtime,
                    arch,
                    built_at,
                    artifact_size_bytes,
                    has_base_snapshot,
                    base_snapshot_size_bytes,
                    snapshots,
                    vcpu,
                    memory_mib,
                    disk_size_mb
             FROM node_template_inventory
             WHERE template_name = ANY($1)
               AND content_hash <> ''
             ORDER BY template_name,
                      content_hash,
                      COALESCE(runtime, ''),
                      observed_at DESC
         ) variants
         ORDER BY template_name ASC, built_at DESC NULLS LAST, content_hash ASC"
    ))
    .bind(template_names)
    .fetch_all(executor.into_sqlx())
    .await?)
}

#[cfg(test)]
mod inventory_rows_tests;
