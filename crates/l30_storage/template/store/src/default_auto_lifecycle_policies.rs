//! The auto-lifecycle policy a template's sandboxes default to
//! (`template_default_auto_lifecycle_policies`).

use pplx_space_auto_lifecycle::AutoLifecyclePolicy;
use pplx_space_sql_locator::sql;
use pplx_space_template_model::TemplateName;
use sqlx::{PgExecutor, types::Json};

/// Set `template`'s default auto-lifecycle policy to `policy`, whatever the
/// template's desired state. The update route's write, gated on a present
/// template, stays at its call site.
pub async fn upsert(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
    policy: &AutoLifecyclePolicy,
) -> Result<(), sqlx::Error> {
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
    .execute(executor)
    .await?;
    Ok(())
}

/// Delete `template`'s default auto-lifecycle policy, if it has one.
pub async fn delete(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM template_default_auto_lifecycle_policies WHERE template = $1"
    ))
    .bind(template.as_str())
    .execute(executor)
    .await?;
    Ok(())
}
