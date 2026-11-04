//! Credential Authority's reader delegates its atomic snapshot to the owning store.
use async_trait::async_trait;
use pplx_space_common_svc::core::types::SandboxId;
pub(super) use pplx_space_credential_injection_store::resolution::{
    NetworkPolicyResolutionCandidate, ResolutionState, ResolutionStateError,
};

/// Internal state seam used by the authority and its tests.
#[async_trait]
pub(super) trait ResolutionStateReader: Send + Sync + 'static {
    /// Read all state that authorizes one resolution in a single statement.
    /// `None` means no complete current candidate exists.
    async fn load_network_policy_candidate(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<NetworkPolicyResolutionCandidate>, ResolutionStateError>;
}

pub(super) struct PgResolutionStateReader {
    pool: sqlx::PgPool,
}

impl PgResolutionStateReader {
    pub(super) fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ResolutionStateReader for PgResolutionStateReader {
    async fn load_network_policy_candidate(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<NetworkPolicyResolutionCandidate>, ResolutionStateError> {
        pplx_space_credential_injection_store::resolution::load_network_policy_candidate(
            &self.pool, sandbox_id,
        )
        .await
    }
}
