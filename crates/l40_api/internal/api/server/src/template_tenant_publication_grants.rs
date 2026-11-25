//! Tenant publication grants are matched to the authorization carried by one
//! build and to the builder that took it, never selected from other
//! submissions for the same template or organization.
//!
//! A grant covers a publisher authenticated as a registered builder node: the
//! pinned builder for seven days, or, until the API records which builder
//! accepted the build, any builder for one hour. These are the windows
//! `template_build_base_grants` honours for the same submission.

use pplx_space_template_store::publication_grants;
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
pub(super) use pplx_space_template_store::publication_grants::TenantPublication;

/// Consume only this build's grant in the publication transaction, under the
/// same conditions as [`publication_grants::tenant_grant_covers`]. Rollback
/// restores it; concurrent uses of the same grant can commit at most once.
pub(super) async fn consume(
    executor: impl PgExecutor<'_>,
    publication: &TenantPublication<'_>,
) -> Result<bool, GrantStorageError> {
    Ok(publication_grants::consume_tenant(executor, publication).await?)
}
