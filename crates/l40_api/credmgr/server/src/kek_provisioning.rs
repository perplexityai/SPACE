//! Authority-side KEK provisioning.
//!
//! Enrolling an org onto the intermediate-key broker requires an
//! `IssueIntermediateKey` round-trip that only the control-plane authority can
//! make (it alone has the provider egress path), so this policy lives here.
//! The development-only in-memory provider follows the same ownership rule:
//! API services request encryption and never write key inventory. On the first
//! wrap for an org with no active KEK, the service asks
//! [`KekProvisioner::provision_active_kek`] to mint a key and persist its
//! inventory, including the broker-wrapped blob when applicable, race-safe via
//! the partial unique active-KEK index.

use pplx_space_common_svc::sql;
use std::sync::Arc;

use pplx_space_common_svc::core::types::{KekProvider, MasterKeyIdentifier, OrgId, SandboxId};
use uuid::Uuid;

use crate::intermediate_key_broker::{IntermediateKeyBrokerApi, ProviderError};
use crate::kek_store::{KekRecord, KekStore, KekStoreError, PgKekStore};

#[derive(Debug, thiserror::Error)]
pub enum ProvisionError {
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error(transparent)]
    KekStore(#[from] KekStoreError),
    #[error("provisioning transaction failed: {0}")]
    Db(#[from] sqlx::Error),
    #[error("provisioning raced a concurrent writer and no active KEK is visible; retry")]
    LostRaceWithoutWinner,
}

/// Provision an active KEK for an org that has none. A trait so the service
/// handlers can be exercised without a live Postgres or provider.
#[tonic::async_trait]
pub trait KekProvisioner: Send + Sync + 'static {
    async fn provision_active_kek(
        &self,
        org: OrgId,
        requesting_sandbox: Option<SandboxId>,
    ) -> Result<KekRecord, ProvisionError>;
}

/// [`KekProvisioner`] for [`KekProvider::IntermediateKeyBroker`]: mints a
/// intermediate key at the broker and persists the KEK row + wrapped blob.
pub struct IntermediateKeyProvisioner {
    pool: sqlx::PgPool,
    kek_store: PgKekStore,
    provider: Arc<dyn IntermediateKeyBrokerApi>,
}

impl IntermediateKeyProvisioner {
    pub fn new(pool: sqlx::PgPool, provider: Arc<dyn IntermediateKeyBrokerApi>) -> Self {
        Self {
            kek_store: PgKekStore::new(pool.clone()),
            pool,
            provider,
        }
    }
}

#[tonic::async_trait]
impl KekProvisioner for IntermediateKeyProvisioner {
    async fn provision_active_kek(
        &self,
        org: OrgId,
        requesting_sandbox: Option<SandboxId>,
    ) -> Result<KekRecord, ProvisionError> {
        // Narrow the race window before paying the provider round-trip.
        if let Some(existing) = self.kek_store.active_kek_for_org(org).await? {
            return Ok(existing);
        }

        // Issue before the transaction so the tx stays short. Losing the
        // insert race orphans this key at the broker — harmless: it is
        // never persisted, never unwrapped, and the provider may GC it.
        let issued = self
            .provider
            .issue_intermediate_key(org, requesting_sandbox)
            .await?;

        let mut tx = self.pool.begin().await?;
        let inserted: Option<Uuid> = sqlx::query_scalar(sql!(
            "INSERT INTO org_master_keys (org_id, provider, key_identifier) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (org_id) WHERE retired_at IS NULL DO NOTHING \
             RETURNING id"
        ))
        .bind(org.as_uuid())
        .bind(KekProvider::IntermediateKeyBroker.as_str())
        .bind(issued.key_id.as_str())
        .fetch_optional(&mut *tx)
        .await?;

        match inserted {
            Some(cmk_id) => {
                sqlx::query(sql!(
                    "INSERT INTO org_branching_keys (cmk_id, org_id, wrapped_key) \
                     VALUES ($1, $2, $3)"
                ))
                .bind(cmk_id)
                .bind(org.as_uuid())
                .bind(&issued.wrapped_key)
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
                tracing::info!(
                    org = %org.as_uuid(),
                    %cmk_id,
                    key_id = %issued.key_id,
                    "provisioned intermediate-key-broker KEK"
                );
                Ok(KekRecord {
                    cmk_id,
                    provider: KekProvider::IntermediateKeyBroker,
                    key_identifier: issued.key_id,
                    retired: false,
                })
            }
            None => {
                // A concurrent provisioner won the partial unique index;
                // adopt its row. Absence here means the winner retired or
                // deleted it in the same instant — surface for retry.
                tx.rollback().await?;
                self.kek_store
                    .active_kek_for_org(org)
                    .await?
                    .ok_or(ProvisionError::LostRaceWithoutWinner)
            }
        }
    }
}

/// Development-only provisioner for the in-process key wrapper.
///
/// The identifier selects one authority-process-local key. The binary only
/// installs this provisioner together with the explicit in-memory backend
/// opt-in; keeping the database write here ensures API services never become
/// key authorities, even in local development.
pub struct InMemoryKekProvisioner {
    pool: sqlx::PgPool,
    kek_store: PgKekStore,
    key_identifier: MasterKeyIdentifier,
}

impl InMemoryKekProvisioner {
    pub fn new(pool: sqlx::PgPool, key_identifier: MasterKeyIdentifier) -> Self {
        Self {
            kek_store: PgKekStore::new(pool.clone()),
            pool,
            key_identifier,
        }
    }
}

#[tonic::async_trait]
impl KekProvisioner for InMemoryKekProvisioner {
    async fn provision_active_kek(
        &self,
        org: OrgId,
        _requesting_sandbox: Option<SandboxId>,
    ) -> Result<KekRecord, ProvisionError> {
        if let Some(existing) = self.kek_store.active_kek_for_org(org).await? {
            return Ok(existing);
        }

        let inserted: Option<Uuid> = sqlx::query_scalar(sql!(
            "INSERT INTO org_master_keys (org_id, provider, key_identifier) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (org_id) WHERE retired_at IS NULL DO NOTHING \
             RETURNING id"
        ))
        .bind(org.as_uuid())
        .bind(KekProvider::InMemory.as_str())
        .bind(self.key_identifier.as_str())
        .fetch_optional(&self.pool)
        .await?;

        match inserted {
            Some(cmk_id) => Ok(KekRecord {
                cmk_id,
                provider: KekProvider::InMemory,
                key_identifier: self.key_identifier.clone(),
                retired: false,
            }),
            None => self
                .kek_store
                .active_kek_for_org(org)
                .await?
                .ok_or(ProvisionError::LostRaceWithoutWinner),
        }
    }
}

#[cfg(test)]
mod postgres_tests;
