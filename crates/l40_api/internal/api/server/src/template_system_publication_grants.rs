//! System publication grants are matched to the authorization carried by one
//! build, never selected from other submissions for the same template.

use pplx_space_accounts::SystemPublicationGrantId;
use pplx_space_template_model::TemplateName;
use pplx_space_template_store::publication_grants;
use sqlx::PgExecutor;

#[derive(Debug, thiserror::Error)]
#[error("system publication grant storage failed: {0}")]
pub(super) struct GrantStorageError(#[from] sqlx::Error);

impl GrantStorageError {
    pub(super) fn as_sql(&self) -> &sqlx::Error {
        &self.0
    }
}

/// Consume only this build's grant in the publication transaction. Rollback
/// restores it; concurrent uses of the same grant can commit at most once.
pub(super) async fn consume(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
    grant: SystemPublicationGrantId,
) -> Result<bool, GrantStorageError> {
    Ok(publication_grants::consume_system(executor, template, grant).await?)
}

/// Issue a fresh authorization under independently verified operator authority.
pub(super) async fn issue(
    tx: &mut sqlx::PgConnection,
    template: &TemplateName,
) -> Result<SystemPublicationGrantId, GrantStorageError> {
    publication_grants::delete_expired_system_before_issue(&mut *tx).await?;
    let grant = SystemPublicationGrantId::new();
    publication_grants::insert_system(&mut *tx, grant, template).await?;
    Ok(grant)
}
