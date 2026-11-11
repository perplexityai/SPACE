use pplx_space_common_core::types::SandboxId;

use sqlx::{PgPool, Postgres};

use super::{FederatedOwner, FederatedServiceGrant};

pub use pplx_space_federated_grant_store::FederatedGrantStoreError;
use pplx_space_federated_grant_store::{FederatedGrantOwnerRef, FederatedServiceGrantRef};
pub use pplx_space_federated_identity::{
    DEFAULT_FEDERATED_GRANT_AUDIENCE, FEDERATED_OWNER_GRANT_PURPOSE,
};

pub async fn upsert_sandbox_federated_service_grant(
    pool: &PgPool,
    sandbox_id: SandboxId,
    owner: &FederatedOwner,
    grant: &FederatedServiceGrant,
    grant_audience: &str,
    grant_purpose: &str,
) -> Result<(), FederatedGrantStoreError> {
    pplx_space_federated_grant_store::upsert_sandbox_federated_service_grant(
        pool,
        sandbox_id,
        owner_ref(owner),
        grant_ref(grant),
        grant_audience,
        grant_purpose,
    )
    .await
}

pub async fn upsert_sandbox_federated_service_grant_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    owner: &FederatedOwner,
    grant: &FederatedServiceGrant,
    grant_audience: &str,
    grant_purpose: &str,
) -> Result<(), FederatedGrantStoreError> {
    pplx_space_federated_grant_store::upsert_sandbox_federated_service_grant_tx(
        tx,
        sandbox_id,
        owner_ref(owner),
        grant_ref(grant),
        grant_audience,
        grant_purpose,
    )
    .await
}

fn owner_ref(owner: &FederatedOwner) -> FederatedGrantOwnerRef<'_> {
    FederatedGrantOwnerRef {
        issuer: owner.issuer.as_str(),
        org_id: owner.org_id.as_str(),
        subject_id: owner.subject_id.as_str(),
    }
}

fn grant_ref(grant: &FederatedServiceGrant) -> FederatedServiceGrantRef<'_> {
    FederatedServiceGrantRef {
        bytes: grant.as_bytes(),
        expires_at: grant.expires_at(),
        issued_at: grant.issued_at(),
    }
}
