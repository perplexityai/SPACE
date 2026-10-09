//! Direct Postgres reads for sandbox-owner and placement resolution. Runs as
//! the read-only `space_credmgr` role.

use pplx_space_common_core::types::{OrgId, SandboxId, UserId};

use pplx_space_common_svc::core::types::{K8sNodeName, K8sNodeUid};
use pplx_space_common_svc::sql;

use crate::storage_vend_store::SandboxNodeIdentity;

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
    #[error("stored sandbox node identity is invalid")]
    InvalidNodeIdentity,
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

    /// Every node durably assigned to run the sandbox's work: its active and
    /// target placement and the node of its latest Kubernetes execution. More
    /// than one can be live during handoff. Empty when the sandbox does not
    /// exist or has no placement.
    async fn sandbox_placement_nodes(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Vec<SandboxNodeIdentity>, CredentialStoreError>;
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

    async fn sandbox_placement_nodes(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Vec<SandboxNodeIdentity>, CredentialStoreError> {
        let rows: Vec<(String, String)> = sqlx::query_as(sql!(
            "WITH latest_execution AS (
                 SELECT k8s_node_name
                 FROM sandbox_k8s_executions
                 WHERE sandbox_id = $1 AND k8s_node_name IS NOT NULL
                 ORDER BY generation DESC
                 LIMIT 1
             ), candidate_nodes AS (
                 SELECT active_node_id AS node_id FROM sandboxes WHERE id = $1
                 UNION
                 SELECT target_node_id AS node_id FROM sandboxes WHERE id = $1
                 UNION
                 SELECT k.node_id
                 FROM latest_execution e
                 JOIN node_k8s k ON k.k8s_node_name = e.k8s_node_name
             )
             SELECT k.k8s_node_name, k.k8s_node_uid
             FROM candidate_nodes candidates
             JOIN node_k8s k ON k.node_id = candidates.node_id
             WHERE k.k8s_node_uid IS NOT NULL"
        ))
        .bind(sandbox_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(name, uid)| {
                Ok(SandboxNodeIdentity {
                    name: K8sNodeName::parse(name)
                        .map_err(|_| CredentialStoreError::InvalidNodeIdentity)?,
                    uid: K8sNodeUid::parse(uid)
                        .map_err(|_| CredentialStoreError::InvalidNodeIdentity)?,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod postgres_tests;
