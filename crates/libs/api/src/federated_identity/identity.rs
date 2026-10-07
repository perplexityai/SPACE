//! Resolve validated upstream owners without coupling authentication to sandbox creation.

use super::{FederatedOwner, store};
use crate::models::{Organization, User};
use pplx_space_common_core::types::{OrgId, UserId};
use sqlx::PgPool;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FederatedOwnerRecord {
    pub(crate) org_id: OrgId,
    pub(crate) user_id: UserId,
    pub(crate) email: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum MaterializeOwnerError {
    #[error("failed to start federated sandbox owner transaction: {0}")]
    Begin(#[source] sqlx::Error),
    #[error("failed to resolve federated organization owner: {0}")]
    Organization(#[source] sqlx::Error),
    #[error("failed to resolve federated user owner: {0}")]
    User(#[source] sqlx::Error),
    #[error("failed to commit federated sandbox owner transaction: {0}")]
    Commit(#[source] sqlx::Error),
}

impl MaterializeOwnerError {
    pub(crate) fn database_error(&self) -> &sqlx::Error {
        match self {
            Self::Begin(error)
            | Self::Organization(error)
            | Self::User(error)
            | Self::Commit(error) => error,
        }
    }

    pub(crate) fn context(&self) -> &'static str {
        match self {
            Self::Begin(_) => "failed to start federated sandbox owner transaction",
            Self::Organization(_) => "failed to resolve federated organization owner",
            Self::User(_) => "failed to resolve federated user owner",
            Self::Commit(_) => "failed to commit federated sandbox owner transaction",
        }
    }

    pub(crate) fn is_lock_contention(&self) -> bool {
        pplx_space_common_svc::sql::contention::is_lock_contention(self.database_error())
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ResolvePrincipalError {
    #[error("failed to look up federated principal: {0}")]
    Lookup(#[source] sqlx::Error),
    #[error(transparent)]
    Materialize(#[from] MaterializeOwnerError),
    #[error("failed to reload federated principal: {0}")]
    Reload(#[source] sqlx::Error),
}

/// Resolve the effective principal. Existing mappings take a read-only path:
/// repeated authentication must not rewrite or lock the tenant's identity rows.
pub(crate) async fn resolve_principal(
    pool: &PgPool,
    owner: &FederatedOwner,
) -> Result<(User, Organization), ResolvePrincipalError> {
    if let Some(found) = store::find_federated_user_org(pool, owner)
        .await
        .map_err(ResolvePrincipalError::Lookup)?
    {
        return Ok(found);
    }
    let effective = materialize_owner(pool, owner).await?;
    store::get_user_org(pool, effective.user_id)
        .await
        .map_err(ResolvePrincipalError::Reload)
}

/// Read-only lookup of the record [`materialize_owner`] would return for an
/// owner that is already materialized. `None` when the owner's organization
/// or subject row does not exist yet. Never writes or locks a row, so a
/// caller can ask before deciding anything that depends on the owner.
pub(crate) async fn find_owner_record(
    pool: &PgPool,
    owner: &FederatedOwner,
) -> Result<Option<FederatedOwnerRecord>, sqlx::Error> {
    Ok(store::find_federated_user_org(pool, owner)
        .await?
        .map(|(user, org)| FederatedOwnerRecord {
            org_id: org.id,
            user_id: user.id,
            email: user.email,
        }))
}

/// Atomically materialize the organization and subject. This transaction commits
/// independently of any later sandbox creation; it never persists grant material.
pub(crate) async fn materialize_owner(
    pool: &PgPool,
    owner: &FederatedOwner,
) -> Result<FederatedOwnerRecord, MaterializeOwnerError> {
    let mut tx = pool.begin().await.map_err(MaterializeOwnerError::Begin)?;
    let result = store::materialize_owner_in_tx(&mut tx, owner).await?;
    tx.commit().await.map_err(MaterializeOwnerError::Commit)?;
    Ok(result)
}

#[cfg(test)]
mod tests;
