//! Resolution of `cmk_id` → KEK row for the `Encrypt`/`Decrypt` RPCs.
//!
//! Callers of those RPCs name the KEK by its `org_master_keys.id` instead of
//! supplying `(provider, key_identifier)` themselves; credmgr resolves the
//! row and rejects the request when it does not belong to the asserted
//! organization — server-side validation in front of the selected backend's
//! cryptographic context binding. The Postgres connection uses the dedicated
//! read-only `space_credmgr` role.

use pplx_space_common_svc::core::types::{KekProvider, MasterKeyIdentifier, OrgId};
use pplx_space_common_svc::sql;
use uuid::Uuid;

/// A resolved `org_master_keys` row, narrowed to what the key wrapper needs.
#[derive(Debug, Clone)]
pub struct KekRecord {
    /// `org_master_keys.id`. Recorded inside the wrapped-DEK envelope so
    /// Decrypt can find its way back without the caller naming a key.
    pub cmk_id: Uuid,
    pub provider: KekProvider,
    pub key_identifier: MasterKeyIdentifier,
    /// Retired KEKs may still decrypt existing data but must not wrap new
    /// DEKs.
    pub retired: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum KekStoreError {
    #[error("KEK row {cmk_id} holds an invalid provider value: {message}")]
    InvalidProvider { cmk_id: Uuid, message: String },
    #[error("KEK row {cmk_id} holds an invalid key identifier: {message}")]
    InvalidKeyIdentifier { cmk_id: Uuid, message: String },
    #[error("KEK lookup query failed: {0}")]
    Query(#[from] sqlx::Error),
}

/// Lookup of a KEK row scoped to an organization. A trait so the service
/// handlers can be exercised without a live Postgres.
#[tonic::async_trait]
pub trait KekStore: Send + Sync + 'static {
    /// Resolve the organization's active (non-retired) KEK, used by
    /// Encrypt. At most one row is active per org (enforced by a partial
    /// unique index); `None` means the org is not provisioned for
    /// encryption.
    async fn active_kek_for_org(&self, org: OrgId) -> Result<Option<KekRecord>, KekStoreError>;

    /// Resolve `cmk_id` (read back from a wrapped-DEK envelope) for `org`,
    /// used by Decrypt. Returns `None` when no row with that id belongs to
    /// that organization — deliberately indistinguishable from the row not
    /// existing at all, so one org cannot probe another's KEK ids.
    async fn kek_for_org(
        &self,
        cmk_id: Uuid,
        org: OrgId,
    ) -> Result<Option<KekRecord>, KekStoreError>;
}

pub struct PgKekStore {
    pool: sqlx::PgPool,
}

impl PgKekStore {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[tonic::async_trait]
impl KekStore for PgKekStore {
    async fn active_kek_for_org(&self, org: OrgId) -> Result<Option<KekRecord>, KekStoreError> {
        let row: Option<(Uuid, String, String)> = sqlx::query_as(sql!(
            "SELECT id, provider, key_identifier \
             FROM org_master_keys WHERE org_id = $1 AND retired_at IS NULL"
        ))
        .bind(org.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        let Some((cmk_id, provider, key_identifier)) = row else {
            return Ok(None);
        };
        parse_record(cmk_id, provider, key_identifier, false).map(Some)
    }

    async fn kek_for_org(
        &self,
        cmk_id: Uuid,
        org: OrgId,
    ) -> Result<Option<KekRecord>, KekStoreError> {
        let row: Option<(String, String, bool)> = sqlx::query_as(sql!(
            "SELECT provider, key_identifier, retired_at IS NOT NULL \
             FROM org_master_keys WHERE id = $1 AND org_id = $2"
        ))
        .bind(cmk_id)
        .bind(org.as_uuid())
        .fetch_optional(&self.pool)
        .await?;

        let Some((provider, key_identifier, retired)) = row else {
            return Ok(None);
        };
        parse_record(cmk_id, provider, key_identifier, retired).map(Some)
    }
}

fn parse_record(
    cmk_id: Uuid,
    provider: String,
    key_identifier: String,
    retired: bool,
) -> Result<KekRecord, KekStoreError> {
    let provider: KekProvider = provider
        .parse()
        .map_err(|e| KekStoreError::InvalidProvider {
            cmk_id,
            message: format!("{e}"),
        })?;
    let key_identifier: MasterKeyIdentifier =
        key_identifier
            .parse()
            .map_err(|e| KekStoreError::InvalidKeyIdentifier {
                cmk_id,
                message: format!("{e}"),
            })?;
    Ok(KekRecord {
        cmk_id,
        provider,
        key_identifier,
        retired,
    })
}
