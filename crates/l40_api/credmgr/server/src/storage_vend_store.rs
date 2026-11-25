//! Durable authorization context for scoped storage credential exchange.

use pplx_space_snapshot_model::SnapshotId;

use std::str::FromStr;

use pplx_space_accounts::OrgId;
use pplx_space_artifact_model::StorageUri;
use pplx_space_content_digest::ContentHash;
use pplx_space_k8s_ids::{K8sNodeName, K8sNodeUid};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_template_model::TemplateName;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxStorageContext {
    pub organization_id: OrgId,
    /// Every durable node attribution that can legitimately be carrying work
    /// for this sandbox/snapshot: active or target placement, the snapshot's
    /// source node, and the latest Kubernetes execution. More than one can be
    /// live during handoff.
    pub nodes: Vec<SandboxNodeIdentity>,
    /// Current organization storage plus the URI recorded with this exact
    /// snapshot. The latter keeps old snapshots readable after an org changes
    /// its configured base URI.
    pub snapshot_storage: Vec<StorageUri>,
    /// A malformed configured or recorded URI is not allowed to turn into an
    /// unconstrained bucket fallback. Any other valid URI can still authorize
    /// the exact historical state; otherwise authorization fails closed.
    pub snapshot_storage_configuration_invalid: bool,
    /// Fork copies are deliberately executable on any ready worker. While the
    /// prepared destination snapshot is still `creating`, its exact recorded
    /// URI may therefore be written by an authenticated volume manager even
    /// though that worker is not the source sandbox's placement.
    pub portable_snapshot_write: Option<StorageUri>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxNodeIdentity {
    pub name: K8sNodeName,
    pub uid: K8sNodeUid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageVendProvider {
    AwsSts,
}

impl FromStr for StorageVendProvider {
    type Err = ParseStorageVendProviderError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "aws-sts" => Ok(Self::AwsSts),
            _ => Err(ParseStorageVendProviderError),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("unsupported storage vend provider")]
pub struct ParseStorageVendProviderError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageVendIdentity {
    pub provider: StorageVendProvider,
    pub identity: String,
}

#[derive(Debug, thiserror::Error)]
pub enum StorageVendStoreError {
    #[error("storage credential authorization query failed: {0}")]
    Query(#[from] sqlx::Error),
    #[error("stored storage vend provider is unsupported")]
    UnsupportedProvider,
}

#[tonic::async_trait]
pub trait StorageVendStore: Send + Sync + 'static {
    async fn sandbox_context(
        &self,
        sandbox_id: SandboxId,
        snapshot_id: SnapshotId,
    ) -> Result<Option<SandboxStorageContext>, StorageVendStoreError>;

    /// `Some(None)` is a system template; `Some(Some(org))` is tenant-owned.
    async fn template_owner(
        &self,
        template: &TemplateName,
    ) -> Result<Option<Option<OrgId>>, StorageVendStoreError>;

    /// True once a storage location is registered for the release
    /// `templates/<template>/<hash>`. A published release is immutable.
    async fn template_release_published(
        &self,
        template: &TemplateName,
        hash: &ContentHash,
    ) -> Result<bool, StorageVendStoreError>;

    async fn org_vend_identity(
        &self,
        organization_id: OrgId,
    ) -> Result<Option<StorageVendIdentity>, StorageVendStoreError>;
}

pub struct PgStorageVendStore {
    pool: sqlx::PgPool,
}

impl PgStorageVendStore {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[tonic::async_trait]
impl StorageVendStore for PgStorageVendStore {
    async fn sandbox_context(
        &self,
        sandbox_id: SandboxId,
        snapshot_id: SnapshotId,
    ) -> Result<Option<SandboxStorageContext>, StorageVendStoreError> {
        let Some(pplx_space_sandbox_store::org_storage::SandboxOrgStorage {
            organization_id,
            snapshot_base_object_storage_uri: current_snapshot_storage,
        }) = pplx_space_sandbox_store::org_storage::sandbox_org_storage(&self.pool, sandbox_id)
            .await?
        else {
            return Ok(None);
        };

        let nodes = pplx_space_sandbox_store::k8s_node_attribution::attributed_k8s_nodes(
            &self.pool,
            sandbox_id,
            snapshot_id,
        )
        .await?
        .into_iter()
        .map(|node| SandboxNodeIdentity {
            name: node.k8s_node_name,
            uid: node.k8s_node_uid,
        })
        .collect();

        let recorded_snapshot = pplx_space_snapshot_store::org_storage_location(
            &self.pool,
            snapshot_id,
            organization_id,
        )
        .await?;
        let (recorded_snapshot_storage, portable_snapshot_write) = recorded_snapshot
            .map(|location| {
                (
                    Some(location.storage_connection_string),
                    location.creating_fork_copy,
                )
            })
            .unwrap_or((None, false));

        let mut snapshot_storage = Vec::with_capacity(2);
        let mut snapshot_storage_configuration_invalid = false;
        let mut parsed_recorded_snapshot_storage = None;
        for (source, raw) in [
            ("organization", current_snapshot_storage),
            ("snapshot", recorded_snapshot_storage),
        ] {
            let Some(raw) = raw else { continue };
            match StorageUri::parse(&raw) {
                Ok(uri) => {
                    if source == "snapshot" {
                        parsed_recorded_snapshot_storage = Some(uri.clone());
                    }
                    if !snapshot_storage.contains(&uri) {
                        snapshot_storage.push(uri);
                    }
                }
                Err(error) => {
                    snapshot_storage_configuration_invalid = true;
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        %snapshot_id,
                        source,
                        %error,
                        "ignoring malformed stored snapshot URI during credential authorization"
                    );
                }
            }
        }

        Ok(Some(SandboxStorageContext {
            organization_id,
            nodes,
            snapshot_storage,
            snapshot_storage_configuration_invalid,
            portable_snapshot_write: portable_snapshot_write
                .then_some(parsed_recorded_snapshot_storage)
                .flatten(),
        }))
    }

    async fn template_owner(
        &self,
        template: &TemplateName,
    ) -> Result<Option<Option<OrgId>>, StorageVendStoreError> {
        Ok(pplx_space_template_store::templates::owner(&self.pool, template).await?)
    }

    async fn template_release_published(
        &self,
        template: &TemplateName,
        hash: &ContentHash,
    ) -> Result<bool, StorageVendStoreError> {
        Ok(pplx_space_template_release_store::release_published(
            &self.pool,
            template.as_str(),
            hash,
        )
        .await?)
    }

    async fn org_vend_identity(
        &self,
        organization_id: OrgId,
    ) -> Result<Option<StorageVendIdentity>, StorageVendStoreError> {
        pplx_space_storage_vend_store::org_vend_identity(&self.pool, organization_id)
            .await?
            .map(|record| {
                Ok(StorageVendIdentity {
                    provider: record
                        .provider
                        .parse()
                        .map_err(|_| StorageVendStoreError::UnsupportedProvider)?,
                    identity: record.identity,
                })
            })
            .transpose()
    }
}

#[cfg(test)]
mod postgres_tests;
