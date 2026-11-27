//! The auto-lifecycle policy a template's sandboxes default to
//! (`template_default_auto_lifecycle_policies`).

use pplx_space_auto_lifecycle::AutoLifecyclePolicy;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use pplx_space_template_model::{TemplateDesiredState, TemplateName};
use sqlx::types::Json;

/// Set `template`'s default auto-lifecycle policy to `policy`, whatever the
/// template's desired state. The update route's write, gated on a present
/// template, is [`upsert_if_present`].
pub async fn upsert(
    executor: impl Executor<'_>,
    template: &TemplateName,
    policy: &AutoLifecyclePolicy,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "INSERT INTO template_default_auto_lifecycle_policies
                 (template, default_auto_lifecycle_json, updated_at)
             VALUES ($1, $2, NOW())
             ON CONFLICT (template) DO UPDATE
             SET default_auto_lifecycle_json = EXCLUDED.default_auto_lifecycle_json,
                 updated_at = NOW()"
    ))
    .bind(template.as_str())
    .bind(Json(policy))
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// The auto-lifecycle policy a create of `template` at `content_hash` on
/// `runtime` defaults to: `template`'s own default, else the one the
/// builder last reported for that build (a report without a runtime
/// matches any), or `None` when neither is recorded.
pub async fn for_create(
    executor: impl Executor<'_>,
    template: &str,
    content_hash: &str,
    runtime: &str,
) -> Result<Option<Json<AutoLifecyclePolicy>>, DbError> {
    Ok(sqlx::query_scalar::<_, Json<AutoLifecyclePolicy>>(sql!(
        "WITH desired AS (
             SELECT default_auto_lifecycle_json
             FROM template_default_auto_lifecycle_policies
             WHERE template = $1
         ),
         observed AS (
             SELECT nti.default_auto_lifecycle_json
             FROM node_template_inventory nti
             JOIN nodes n ON n.id = nti.node_id
             WHERE nti.template_name = $1
               AND nti.content_hash = $2
               AND (nti.runtime IS NULL OR nti.runtime = '' OR nti.runtime = $3)
               AND nti.default_auto_lifecycle_json IS NOT NULL
               AND n.role = 'builder'
             ORDER BY nti.observed_at DESC
             LIMIT 1
         )
         SELECT default_auto_lifecycle_json FROM desired
         UNION ALL
         SELECT default_auto_lifecycle_json FROM observed
         WHERE NOT EXISTS (SELECT 1 FROM desired)
         LIMIT 1"
    ))
    .bind(template)
    .bind(content_hash)
    .bind(runtime)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// `template`'s own default auto-lifecycle policy, or `None` when it has
/// none. Unlike [`for_create`], a builder's report never stands in for it.
pub async fn desired(
    executor: impl Executor<'_>,
    template: &TemplateName,
) -> Result<Option<AutoLifecyclePolicy>, DbError> {
    Ok(sqlx::query_scalar::<_, Json<AutoLifecyclePolicy>>(sql!(
        "SELECT default_auto_lifecycle_json
         FROM template_default_auto_lifecycle_policies
         WHERE template = $1"
    ))
    .bind(template.as_str())
    .fetch_optional(executor.into_sqlx())
    .await?
    .map(|Json(policy)| policy))
}

/// Delete `template`'s default auto-lifecycle policy, if it has one.
pub async fn delete(executor: impl Executor<'_>, template: &TemplateName) -> Result<(), DbError> {
    sqlx::query(sql!(
        "DELETE FROM template_default_auto_lifecycle_policies WHERE template = $1"
    ))
    .bind(template.as_str())
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Set a present `template`'s default auto-lifecycle policy to `policy`.
/// `false` when the template isn't present, which writes nothing.
pub async fn upsert_if_present(
    executor: impl Executor<'_>,
    template: &TemplateName,
    policy: &AutoLifecyclePolicy,
) -> Result<bool, DbError> {
    let result = sqlx::query(sql!(
        "INSERT INTO template_default_auto_lifecycle_policies
                     (template, default_auto_lifecycle_json, updated_at)
                 SELECT name, $2, NOW()
                 FROM templates
                 WHERE name = $1
                   AND desired_state = $3
                 ON CONFLICT (template) DO UPDATE
                 SET default_auto_lifecycle_json = EXCLUDED.default_auto_lifecycle_json,
                     updated_at = NOW()"
    ))
    .bind(template.as_str())
    .bind(Json(policy))
    .bind(TemplateDesiredState::Present.as_str())
    .execute(executor.into_sqlx())
    .await?;
    Ok(result.rows_affected() != 0)
}
