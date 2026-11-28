//! Durable owner mappings. Transactions are supplied by the identity module.

use super::{
    FederatedOwner,
    identity::{FederatedOwnerRecord, MaterializeOwnerError},
};
use crate::models::{Organization, User};
use pplx_space_accounts::{OrgId, UserId};
use pplx_space_accounts_store::{FederatedOrganization, FederatedUser, organizations, users};
use sqlx::PgPool;

/// Load the durable effective principal after federated owner materialization.
///
/// One statement: a user whose org row is missing is the same `RowNotFound`
/// the former user-then-org pair returned from its second read.
pub(super) async fn get_user_org(
    pool: &PgPool,
    user_id: UserId,
) -> Result<(User, Organization), sqlx::Error> {
    users::user_with_organization(pool, user_id)
        .await
        .map(|found| (found.user, found.org))
        .map_err(sqlx::Error::from)
}

/// Read-only lookup of an already-materialized federated owner. A miss means
/// the caller must take the race-safe materialization path.
///
/// The org and subject predicates are the ones the former org-then-user pair
/// applied, joined on the subject's org, so a missing org or a missing subject
/// is still the same `None`.
pub(super) async fn find_federated_user_org(
    pool: &PgPool,
    owner: &FederatedOwner,
) -> Result<Option<(User, Organization)>, sqlx::Error> {
    organizations::federated_organization_user(
        pool,
        owner.issuer.as_str(),
        owner.org_id.as_str(),
        owner.subject_id.as_str(),
    )
    .await
    .map(|found| found.map(|found| (found.user, found.org)))
    .map_err(sqlx::Error::from)
}

pub(super) async fn materialize_owner_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    owner: &FederatedOwner,
) -> Result<FederatedOwnerRecord, MaterializeOwnerError> {
    let proposed_org_id = OrgId::new();
    let org_id = organizations::upsert_federated_organization(
        &mut **tx,
        FederatedOrganization {
            proposed_id: proposed_org_id,
            name: &format!(
                "Federated {} org {}",
                owner.issuer.as_str(),
                owner.org_id.as_str()
            ),
            slug: &format!("federated-{proposed_org_id}"),
            upstream_issuer: owner.issuer.as_str(),
            upstream_org_id: owner.org_id.as_str(),
        },
    )
    .await
    .map_err(sqlx::Error::from)
    .map_err(MaterializeOwnerError::Organization)?;

    let proposed_user_id = UserId::new();
    let email = format!("federated-{proposed_user_id}@space.invalid");
    let user = users::upsert_federated_user(
        &mut **tx,
        FederatedUser {
            proposed_id: proposed_user_id,
            proposed_email: &email,
            display_name: &format!(
                "Federated {} user {}",
                owner.issuer.as_str(),
                owner.subject_id.as_str()
            ),
            org_id,
            upstream_issuer: owner.issuer.as_str(),
            upstream_subject_id: owner.subject_id.as_str(),
        },
    )
    .await
    .map_err(sqlx::Error::from)
    .map_err(MaterializeOwnerError::User)?;

    Ok(FederatedOwnerRecord {
        user_id: user.id,
        org_id,
        email: user.email,
    })
}
