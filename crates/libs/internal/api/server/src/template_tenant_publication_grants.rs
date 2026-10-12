//! Tenant publication grants are matched to the authorization carried by one
//! build and to the builder that took it, never selected from other
//! submissions for the same template or organization.
//!
//! A grant covers a publisher authenticated as a registered builder node: the
//! pinned builder for seven days, or, until the API records which builder
//! accepted the build, any builder for one hour. These are the windows
//! `template_build_base_grants` honours for the same submission.

use pplx_space_common_core::{
    sql,
    types::{NodeId, OrgId, TemplateName, TenantPublicationGrantId},
};
use sqlx::PgExecutor;

#[derive(Debug, thiserror::Error)]
#[error("tenant publication grant storage failed: {0}")]
pub(super) struct GrantStorageError(#[from] sqlx::Error);

impl GrantStorageError {
    pub(super) fn as_sql(&self) -> &sqlx::Error {
        &self.0
    }
}

/// One publication's claim: the build's grant, the target and owner it
/// publishes, and the publisher’s authenticated registered nodes.
pub(super) struct TenantPublication<'a> {
    pub(super) grant: TenantPublicationGrantId,
    pub(super) template: &'a TemplateName,
    pub(super) org_id: OrgId,
    pub(super) caller_nodes: &'a [NodeId],
}

/// Whether this build's grant is fresh, names this target and owner, and
/// is bound to the authenticated builder.
pub(super) async fn covers(
    executor: impl PgExecutor<'_>,
    publication: &TenantPublication<'_>,
) -> Result<bool, GrantStorageError> {
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
    .fetch_one(executor)
    .await?)
}

/// Consume only this build's grant in the publication transaction, under the
/// same conditions as [`covers`]. Rollback restores it; concurrent uses of
/// the same grant can commit at most once.
pub(super) async fn consume(
    executor: impl PgExecutor<'_>,
    publication: &TenantPublication<'_>,
) -> Result<bool, GrantStorageError> {
    let consumed = sqlx::query_scalar::<_, uuid::Uuid>(sql!(
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
    .fetch_optional(executor)
    .await?;
    Ok(consumed.is_some())
}
