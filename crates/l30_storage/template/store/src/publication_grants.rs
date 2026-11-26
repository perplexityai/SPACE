//! One build submission's authority to publish its template: as a system
//! template (`template_system_publication_grants`) or for an organization
//! (`template_tenant_publication_grants`).
//!
//! Each verb touches only the grant it names, never another submission's,
//! except the sweeps of expired grants. Freshness is the database clock's:
//! the sweeps, and the reads and consumption that check a grant is still
//! fresh, compare against `NOW()` in their SQL, so each runs in the
//! executor its caller passes, and a check and a consumption in one
//! transaction compare against one instant.

use pplx_space_accounts::{NodeId, OrgId, SystemPublicationGrantId, TenantPublicationGrantId};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use pplx_space_template_model::TemplateName;
use uuid::Uuid;

/// Record `grant`, authorizing one build to publish `template` as a system
/// template.
pub async fn insert_system(
    executor: impl Executor<'_>,
    grant: SystemPublicationGrantId,
    template: &TemplateName,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "INSERT INTO template_system_publication_grants (id, template_name)
             VALUES ($1, $2)"
    ))
    .bind(grant.as_uuid())
    .bind(template.as_str())
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Withdraw `grant`, if it is still recorded.
pub async fn delete_system(
    executor: impl Executor<'_>,
    grant: SystemPublicationGrantId,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "DELETE FROM template_system_publication_grants WHERE id = $1"
    ))
    .bind(grant.as_uuid())
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Record `grant`, authorizing one build to publish `template` for
/// `org_id`.
pub async fn insert_tenant(
    executor: impl Executor<'_>,
    grant: TenantPublicationGrantId,
    template: &TemplateName,
    org_id: OrgId,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "INSERT INTO template_tenant_publication_grants (id, template_name, org_id)
             VALUES ($1, $2, $3)"
    ))
    .bind(grant.as_uuid())
    .bind(template.as_str())
    .bind(org_id.as_uuid())
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Withdraw `grant`, if it is still recorded.
pub async fn delete_tenant(
    executor: impl Executor<'_>,
    grant: TenantPublicationGrantId,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "DELETE FROM template_tenant_publication_grants WHERE id = $1"
    ))
    .bind(grant.as_uuid())
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Bind `grant` to the builder that accepted its build, the only builder
/// that may use it from then on.
pub async fn pin_tenant_to_builder(
    executor: impl Executor<'_>,
    grant: TenantPublicationGrantId,
    builder_node_id: NodeId,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "UPDATE template_tenant_publication_grants
             SET builder_node_id = $2
             WHERE id = $1"
    ))
    .bind(grant.as_uuid())
    .bind(builder_node_id.as_uuid())
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Delete every system grant recorded more than seven days before the
/// database's `NOW()`, after which none is fresh.
pub async fn delete_expired_system(executor: impl Executor<'_>) -> Result<(), DbError> {
    sqlx::query(sql!(
        "DELETE FROM template_system_publication_grants
             WHERE granted_at < NOW() - INTERVAL '7 days'"
    ))
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// The same statement as [`delete_expired_system`] apart from whitespace in
/// its SQL text, kept as its own verb so the operator grant issue runs its
/// statement unchanged apart from the `sql!` locator. Change the two together.
pub async fn delete_expired_system_before_issue(
    executor: impl Executor<'_>,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "DELETE FROM template_system_publication_grants
        WHERE granted_at < NOW() - INTERVAL '7 days'"
    ))
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Whether `grant` names `template` and was recorded less than seven days
/// before the database's `NOW()`.
pub async fn system_grant_covers(
    executor: impl Executor<'_>,
    template: &TemplateName,
    grant: SystemPublicationGrantId,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1 FROM template_system_publication_grants
             WHERE id = $1 AND template_name = $2
               AND granted_at > NOW() - INTERVAL '7 days')"
    ))
    .bind(grant)
    .bind(template.as_str())
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// Consume `grant` under the conditions [`system_grant_covers`] checks.
/// `false` when it doesn't cover `template` (it names another, is stale,
/// or is gone), which leaves every grant in place. Concurrent consumers of
/// one grant commit at most once.
pub async fn consume_system(
    executor: impl Executor<'_>,
    template: &TemplateName,
    grant: SystemPublicationGrantId,
) -> Result<bool, DbError> {
    let consumed = sqlx::query_scalar::<_, Uuid>(sql!(
        "DELETE FROM template_system_publication_grants
         WHERE id = $1 AND template_name = $2
           AND granted_at > NOW() - INTERVAL '7 days'
         RETURNING id"
    ))
    .bind(grant)
    .bind(template.as_str())
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(consumed.is_some())
}

/// Delete every tenant grant recorded more than seven days before the
/// database's `NOW()`, the longest validity any reader honours.
pub async fn delete_expired_tenant(executor: impl Executor<'_>) -> Result<(), DbError> {
    sqlx::query(sql!(
        "DELETE FROM template_tenant_publication_grants
             WHERE granted_at < NOW() - INTERVAL '7 days'"
    ))
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// One tenant publication's claim on a grant: the build's grant, the target
/// and owner it publishes, and the registered nodes its publisher
/// authenticated as.
#[derive(Clone, Copy, Debug)]
pub struct TenantPublication<'a> {
    pub grant: TenantPublicationGrantId,
    pub template: &'a TemplateName,
    pub org_id: OrgId,
    pub caller_nodes: &'a [NodeId],
}

/// Whether `publication`'s grant names its target and owner, and one of its
/// caller nodes is a builder the grant still covers by the database's
/// `NOW()`: the pinned builder for seven days, or, while none is pinned,
/// any builder for one hour.
pub async fn tenant_grant_covers(
    executor: impl Executor<'_>,
    publication: &TenantPublication<'_>,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
             FROM template_tenant_publication_grants g
             JOIN nodes bn ON bn.id = ANY($4)
             WHERE g.id = $1 AND g.template_name = $2 AND g.org_id = $3
               AND bn.role = 'builder'
               AND ((g.builder_node_id IS NULL
                     AND g.granted_at > NOW() - INTERVAL '1 hour')
                    OR (g.builder_node_id = bn.id
                        AND g.granted_at > NOW() - INTERVAL '7 days')))"
    ))
    .bind(publication.grant)
    .bind(publication.template.as_str())
    .bind(publication.org_id.as_uuid())
    .bind(publication.caller_nodes)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// Consume `publication`'s grant under the conditions
/// [`tenant_grant_covers`] checks. `false` when the grant doesn't cover
/// it, which leaves every grant in place. Concurrent consumers of one grant
/// commit at most once.
pub async fn consume_tenant(
    executor: impl Executor<'_>,
    publication: &TenantPublication<'_>,
) -> Result<bool, DbError> {
    let consumed = sqlx::query_scalar::<_, Uuid>(sql!(
        "DELETE FROM template_tenant_publication_grants g
         WHERE g.id = $1 AND g.template_name = $2 AND g.org_id = $3
           AND EXISTS (
               SELECT 1 FROM nodes bn
               WHERE bn.id = ANY($4)
                 AND bn.role = 'builder'
                 AND ((g.builder_node_id IS NULL
                       AND g.granted_at > NOW() - INTERVAL '1 hour')
                      OR (g.builder_node_id = bn.id
                          AND g.granted_at > NOW() - INTERVAL '7 days')))
         RETURNING g.id"
    ))
    .bind(publication.grant)
    .bind(publication.template.as_str())
    .bind(publication.org_id.as_uuid())
    .bind(publication.caller_nodes)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(consumed.is_some())
}
