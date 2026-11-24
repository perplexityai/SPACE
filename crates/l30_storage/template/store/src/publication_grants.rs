//! One build submission's authority to publish its template: as a system
//! template (`template_system_publication_grants`) or for an organization
//! (`template_tenant_publication_grants`).
//!
//! Each verb touches only the grant it names, never another submission's.
//! Freshness is the database clock's: the sweeps of expired grants, and the
//! reads and consumption that check a grant is still fresh, run at their
//! call sites.

use pplx_space_accounts::{NodeId, OrgId, SystemPublicationGrantId, TenantPublicationGrantId};
use pplx_space_sql_locator::sql;
use pplx_space_template_model::TemplateName;
use sqlx::PgExecutor;

/// Record `grant`, authorizing one build to publish `template` as a system
/// template.
pub async fn insert_system(
    executor: impl PgExecutor<'_>,
    grant: SystemPublicationGrantId,
    template: &TemplateName,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO template_system_publication_grants (id, template_name)
             VALUES ($1, $2)"
    ))
    .bind(grant.as_uuid())
    .bind(template.as_str())
    .execute(executor)
    .await?;
    Ok(())
}

/// Withdraw `grant`, if it is still recorded.
pub async fn delete_system(
    executor: impl PgExecutor<'_>,
    grant: SystemPublicationGrantId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM template_system_publication_grants WHERE id = $1"
    ))
    .bind(grant.as_uuid())
    .execute(executor)
    .await?;
    Ok(())
}

/// Record `grant`, authorizing one build to publish `template` for
/// `org_id`.
pub async fn insert_tenant(
    executor: impl PgExecutor<'_>,
    grant: TenantPublicationGrantId,
    template: &TemplateName,
    org_id: OrgId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO template_tenant_publication_grants (id, template_name, org_id)
             VALUES ($1, $2, $3)"
    ))
    .bind(grant.as_uuid())
    .bind(template.as_str())
    .bind(org_id.as_uuid())
    .execute(executor)
    .await?;
    Ok(())
}

/// Withdraw `grant`, if it is still recorded.
pub async fn delete_tenant(
    executor: impl PgExecutor<'_>,
    grant: TenantPublicationGrantId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM template_tenant_publication_grants WHERE id = $1"
    ))
    .bind(grant.as_uuid())
    .execute(executor)
    .await?;
    Ok(())
}

/// Bind `grant` to the builder that accepted its build, the only builder
/// that may use it from then on.
pub async fn pin_tenant_to_builder(
    executor: impl PgExecutor<'_>,
    grant: TenantPublicationGrantId,
    builder_node_id: NodeId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE template_tenant_publication_grants
             SET builder_node_id = $2
             WHERE id = $1"
    ))
    .bind(grant.as_uuid())
    .bind(builder_node_id.as_uuid())
    .execute(executor)
    .await?;
    Ok(())
}
