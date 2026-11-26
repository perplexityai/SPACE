//! System publication grants are matched to the authorization carried by one
//! build, never selected from other submissions for the same template.

use pplx_space_accounts::SystemPublicationGrantId;
use pplx_space_template_model::TemplateName;
use pplx_space_template_store::publication_grants;

#[derive(Debug, thiserror::Error)]
#[error("system publication grant storage failed: {0}")]
pub(super) struct GrantStorageError(#[from] sqlx::Error);

impl From<pplx_space_postgres_pool::DbError> for GrantStorageError {
    fn from(error: pplx_space_postgres_pool::DbError) -> Self {
        Self::from(sqlx::Error::from(error))
    }
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
