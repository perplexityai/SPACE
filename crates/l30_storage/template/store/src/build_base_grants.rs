//! One build submission's authority to read its base template
//! (`template_build_base_grants`).
//!
//! Each verb touches only the grant it names, except [`delete_expired`],
//! the sweep of expired grants, which compares against the database clock
//! in its SQL. The node-facing template policy reads that check a grant is
//! fresh are [`crate::templates`]'s, as they read the template it covers;
//! the node-scoped release lookups check it in their own statements.

use pplx_space_accounts::{NodeId, OrgId};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use pplx_space_template_model::TemplateName;
use uuid::Uuid;

/// Record grant `id`, authorizing `org_id`'s build of `template` to read
/// `base`.
pub async fn insert(
    executor: impl Executor<'_>,
    id: Uuid,
    template: &TemplateName,
    base: &TemplateName,
    org_id: OrgId,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "INSERT INTO template_build_base_grants (id, template_id, base_template_name, org_id)
             VALUES ($1, $2, $3, $4)"
    ))
    .bind(id)
    .bind(template.as_str())
    .bind(base.as_str())
    .bind(org_id.as_uuid())
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Bind grant `id` to the builder that accepted its build, the only builder
/// that may use it from then on.
pub async fn pin_to_builder(
    executor: impl Executor<'_>,
    id: Uuid,
    builder_node_id: NodeId,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "UPDATE template_build_base_grants
             SET builder_node_id = $2
             WHERE id = $1"
    ))
    .bind(id)
    .bind(builder_node_id.as_uuid())
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Delete every grant recorded more than seven days before the database's
/// `NOW()`, the longest validity any reader honours.
pub async fn delete_expired(executor: impl Executor<'_>) -> Result<(), DbError> {
    sqlx::query(sql!(
        "DELETE FROM template_build_base_grants
             WHERE granted_at < NOW() - INTERVAL '7 days'"
    ))
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}
