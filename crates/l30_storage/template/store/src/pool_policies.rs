//! A template's warm-pool policy (`template_pool_policies`).

use pplx_space_sql_locator::sql;
use pplx_space_template_model::{TemplateDesiredState, TemplateName};
use sqlx::{FromRow, PgExecutor};

/// The pool `template` asks for.
#[derive(Debug, FromRow)]
pub struct PoolPolicy {
    pub target_count: i32,
    pub boot_mode: String,
}

/// `template`'s pool policy, or `None` when it has none.
pub async fn get(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
) -> Result<Option<PoolPolicy>, sqlx::Error> {
    sqlx::query_as::<_, PoolPolicy>(sql!(
        "SELECT target_count, boot_mode
         FROM template_pool_policies
         WHERE template = $1"
    ))
    .bind(template.as_str())
    .fetch_optional(executor)
    .await
}

/// Delete `template`'s pool policy, if it has one.
pub async fn delete(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM template_pool_policies WHERE template = $1"
    ))
    .bind(template.as_str())
    .execute(executor)
    .await?;
    Ok(())
}

/// Set a present `template`'s pool policy. `false` when the template isn't
/// present, which writes nothing.
pub async fn upsert_if_present(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
    target_count: i32,
    boot_mode: &str,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!(
        "INSERT INTO template_pool_policies (template, target_count, boot_mode, updated_at)
             SELECT name, $2, $3, NOW()
             FROM templates
             WHERE name = $1
               AND desired_state = $4
             ON CONFLICT (template) DO UPDATE
             SET target_count = EXCLUDED.target_count,
                 boot_mode = EXCLUDED.boot_mode,
                 updated_at = NOW()"
    ))
    .bind(template.as_str())
    .bind(target_count)
    .bind(boot_mode)
    .bind(TemplateDesiredState::Present.as_str())
    .execute(executor)
    .await?;
    Ok(result.rows_affected() != 0)
}
