//! One build submission's authority to read its base template
//! (`template_build_base_grants`).
//!
//! Each verb touches only the grant it names. The sweep of expired grants
//! compares against the database clock and runs at its call site.

use pplx_space_accounts::{NodeId, OrgId};
use pplx_space_sql_locator::sql;
use pplx_space_template_model::TemplateName;
use sqlx::PgExecutor;
use uuid::Uuid;

/// Record grant `id`, authorizing `org_id`'s build of `template` to read
/// `base`.
pub async fn insert(
    executor: impl PgExecutor<'_>,
    id: Uuid,
    template: &TemplateName,
    base: &TemplateName,
    org_id: OrgId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO template_build_base_grants (id, template_id, base_template_name, org_id)
             VALUES ($1, $2, $3, $4)"
    ))
    .bind(id)
    .bind(template.as_str())
    .bind(base.as_str())
    .bind(org_id.as_uuid())
    .execute(executor)
    .await?;
    Ok(())
}

/// Bind grant `id` to the builder that accepted its build, the only builder
/// that may use it from then on.
pub async fn pin_to_builder(
    executor: impl PgExecutor<'_>,
    id: Uuid,
    builder_node_id: NodeId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE template_build_base_grants
             SET builder_node_id = $2
             WHERE id = $1"
    ))
    .bind(id)
    .bind(builder_node_id.as_uuid())
    .execute(executor)
    .await?;
    Ok(())
}
