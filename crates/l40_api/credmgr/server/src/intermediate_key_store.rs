//! Postgres-backed [`WrappedIntermediateKeyStore`]: reads the broker-wrapped
//! intermediate-key blobs persisted in the `org_branching_keys` sidecar,
//! resolved through the org's `org_master_keys` rows so a blob can never be
//! served across organizations. Rows are written by the provisioning path
//! ([`crate::kek_provisioning`]); this store is read-only.

use pplx_space_accounts::OrgId;
use pplx_space_byok_model::MasterKeyIdentifier;

use crate::intermediate_key_broker::{WrappedIntermediateKeyStore, WrappedKeyStoreError};

pub struct PgWrappedIntermediateKeyStore {
    pool: sqlx::PgPool,
}

impl PgWrappedIntermediateKeyStore {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[tonic::async_trait]
impl WrappedIntermediateKeyStore for PgWrappedIntermediateKeyStore {
    async fn wrapped_key(
        &self,
        org: OrgId,
        key_id: &MasterKeyIdentifier,
    ) -> Result<Option<Vec<u8>>, WrappedKeyStoreError> {
        // key_identifier is the provider's per-version key id; the store pins
        // both rows to the asserted org so a cross-org claim reads as absent.
        pplx_space_byok_store::branching_key(&self.pool, org, key_id)
            .await
            .map_err(WrappedKeyStoreError::Lookup)
    }
}
