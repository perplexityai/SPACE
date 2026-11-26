//! Owning store for `org_storage_vend_identities`: the identity, per
//! organization, under which scoped storage credentials are vended for that
//! organization's storage, and the provider that vends them.
//!
//! The table is credential-fenced: only the credentials roles can read it, so
//! run these verbs on a credentials pool. The provider is returned as text for
//! the caller to parse against the providers it supports.

use pplx_space_accounts::OrgId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;

/// An organization's `org_storage_vend_identities` row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageVendIdentityRecord {
    pub provider: String,
    pub identity: String,
}

/// The organization's storage vend identity, or `None` when it has none.
pub async fn org_vend_identity(
    executor: impl Executor<'_>,
    organization_id: OrgId,
) -> Result<Option<StorageVendIdentityRecord>, DbError> {
    let row: Option<(String, String)> = sqlx::query_as(sql!(
        "SELECT provider, identity
         FROM org_storage_vend_identities
         WHERE org_id = $1"
    ))
    .bind(organization_id.as_uuid())
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(row.map(|(provider, identity)| StorageVendIdentityRecord { provider, identity }))
}
