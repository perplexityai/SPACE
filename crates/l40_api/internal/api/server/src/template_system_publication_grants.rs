//! System publication grants are matched to the authorization carried by one
//! build, never selected from other submissions for the same template.

use pplx_space_common_core::{
    sql,
    types::{SystemPublicationGrantId, TemplateName},
};
use sqlx::PgExecutor;

#[derive(Debug, thiserror::Error)]
#[error("system publication grant storage failed: {0}")]
pub(super) struct GrantStorageError(#[from] sqlx::Error);

impl GrantStorageError {
    pub(super) fn as_sql(&self) -> &sqlx::Error {
        &self.0
    }
}

/// Whether this build's grant is fresh and names its publication target.
pub(super) async fn covers(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
    grant: SystemPublicationGrantId,
) -> Result<bool, GrantStorageError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1 FROM template_system_publication_grants
             WHERE id = $1 AND template_name = $2
               AND granted_at > NOW() - INTERVAL '7 days')"
    ))
    .bind(grant)
    .bind(template.as_str())
    .fetch_one(executor)
    .await?)
}

/// Consume only this build's grant in the publication transaction. Rollback
/// restores it; concurrent uses of the same grant can commit at most once.
pub(super) async fn consume(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
    grant: SystemPublicationGrantId,
) -> Result<bool, GrantStorageError> {
    let consumed = sqlx::query_scalar::<_, uuid::Uuid>(sql!(
        "DELETE FROM template_system_publication_grants
         WHERE id = $1 AND template_name = $2
           AND granted_at > NOW() - INTERVAL '7 days'
         RETURNING id"
    ))
    .bind(grant)
    .bind(template.as_str())
    .fetch_optional(executor)
    .await?;
    Ok(consumed.is_some())
}

/// Issue a fresh authorization under independently verified operator authority.
pub(super) async fn issue(
    tx: &mut sqlx::PgConnection,
    template: &TemplateName,
) -> Result<SystemPublicationGrantId, GrantStorageError> {
    sqlx::query(sql!(
        "DELETE FROM template_system_publication_grants
        WHERE granted_at < NOW() - INTERVAL '7 days'"
    ))
    .execute(&mut *tx)
    .await?;
    let grant = SystemPublicationGrantId::new();
    sqlx::query(sql!(
        "INSERT INTO template_system_publication_grants (id, template_name)
        VALUES ($1, $2)"
    ))
    .bind(grant)
    .bind(template.as_str())
    .execute(&mut *tx)
    .await?;
    Ok(grant)
}
