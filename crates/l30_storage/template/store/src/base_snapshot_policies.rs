//! Whether a template keeps a base snapshot, and at what size
//! (`template_base_snapshot_policies`).

use pplx_space_sql_locator::sql;
use pplx_space_template_model::{TemplateDesiredState, TemplateName};
use sqlx::{FromRow, PgExecutor};
use time::OffsetDateTime;

/// A template's configured base snapshot policy.
#[derive(Debug, FromRow)]
pub struct BaseSnapshotPolicy {
    pub template: String,
    pub ensure_base_snapshot: bool,
    pub snapshot_vcpu: i32,
    pub snapshot_memory_mib: i32,
    pub updated_at: OffsetDateTime,
}

/// `template`'s policy, or `None` when it has none or the template is not
/// present.
pub async fn get_present(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
) -> Result<Option<BaseSnapshotPolicy>, sqlx::Error> {
    sqlx::query_as::<_, BaseSnapshotPolicy>(
        sql!("SELECT p.template, p.ensure_base_snapshot, p.snapshot_vcpu, p.snapshot_memory_mib, p.updated_at
         FROM template_base_snapshot_policies p
         JOIN templates t ON t.name = p.template
         WHERE p.template = $1
           AND t.desired_state = $2"),
    )
    .bind(template.as_str())
    .bind(TemplateDesiredState::Present.as_str())
    .fetch_optional(executor)
    .await
}

/// Delete `template`'s policy, if it has one.
pub async fn delete(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM template_base_snapshot_policies WHERE template = $1"
    ))
    .bind(template.as_str())
    .execute(executor)
    .await?;
    Ok(())
}
