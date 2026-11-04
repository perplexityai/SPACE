//! Postgres-backed [`WrappedIntermediateKeyStore`]: reads the broker-wrapped
//! intermediate-key blobs persisted in the `org_branching_keys` sidecar,
//! resolved through the org's `org_master_keys` rows so a blob can never be
//! served across organizations. Rows are written by the provisioning path
//! ([`crate::kek_provisioning`]); this store is read-only.

use pplx_space_common_svc::core::types::{MasterKeyIdentifier, OrgId};
use pplx_space_common_svc::sql;

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
        // key_identifier is the provider's per-version key id; the join pins
        // both rows to the asserted org so a cross-org claim reads as absent.
        sqlx::query_scalar(sql!(
            "SELECT b.wrapped_key FROM org_branching_keys b \
             JOIN org_master_keys k ON k.id = b.cmk_id AND k.org_id = b.org_id \
             WHERE k.org_id = $1 AND k.key_identifier = $2 \
             ORDER BY b.created_at DESC LIMIT 1"
        ))
        .bind(org.as_uuid())
        .bind(key_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(WrappedKeyStoreError::Lookup)
    }
}
