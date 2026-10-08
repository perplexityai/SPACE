//! Direct Postgres reads for sandbox-owner resolution. Runs as the read-only
//! `space_credmgr` role.

use pplx_space_common_core::types::{OrgId, SandboxId, UserId};

use pplx_space_common_svc::sql;

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
    Query(#[from] sqlx::Error),
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
    pool: sqlx::PgPool,
}

impl PgCredentialStore {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[tonic::async_trait]
impl CredentialStore for PgCredentialStore {
    async fn sandbox_owner(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<SandboxOwner>, CredentialStoreError> {
        let row: Option<(UserId, OrgId, bool)> = sqlx::query_as(sql!(
            "SELECT s.user_id, s.org_id, o.byok_enabled \
             FROM sandboxes s JOIN organizations o ON o.id = s.org_id \
             WHERE s.id = $1"
        ))
        .bind(sandbox_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(user_id, org_id, byok_enabled)| SandboxOwner {
            user_id,
            org_id,
            byok_enabled,
        }))
    }
}
