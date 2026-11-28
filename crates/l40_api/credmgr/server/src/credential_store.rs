//! Sandbox-owner resolution, read through `pplx-space-sandbox-store`. Runs as
//! the read-only `space_credmgr` role.

use pplx_space_accounts::{OrgId, UserId};
use pplx_space_postgres_pool::{Db, DbError};
use pplx_space_sandbox_model::SandboxId;

/// The owner identity a sandbox's credentials are resolved against.
#[derive(Debug, Clone, Copy)]
pub struct SandboxOwner {
    pub user_id: UserId,
    pub org_id: OrgId,
    /// The org's `organizations.byok_enabled` flag. The volume-encryption gate
    /// requires this in addition to an active KEK: an org may have a KEK for
    /// credential encryption without being a BYOK org, and toggling BYOK off
    /// in the dashboard must take effect even if a key row is left behind.
    pub byok_enabled: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum CredentialStoreError {
    #[error("credential query failed: {0}")]
    Query(#[from] DbError),
}

/// Sandbox-owner lookup. A trait so the handlers can be exercised without a
/// live Postgres.
#[tonic::async_trait]
pub trait CredentialStore: Send + Sync + 'static {
    /// Resolve the sandbox's owner, or `None` when the sandbox does not
    /// exist.
    async fn sandbox_owner(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<SandboxOwner>, CredentialStoreError>;
}

pub struct PgCredentialStore {
    pool: Db,
}

impl PgCredentialStore {
    pub fn new(pool: Db) -> Self {
        Self { pool }
    }
}

#[tonic::async_trait]
impl CredentialStore for PgCredentialStore {
    async fn sandbox_owner(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<SandboxOwner>, CredentialStoreError> {
        let row = pplx_space_sandbox_store::row_reads::owner(&self.pool, sandbox_id).await?;
        Ok(row.map(|row| SandboxOwner {
            user_id: row.user_id,
            org_id: row.org_id,
            byok_enabled: row.byok_enabled,
        }))
    }
}
