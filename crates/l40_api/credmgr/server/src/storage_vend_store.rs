//! Durable authorization context for scoped storage credential exchange.

use pplx_space_common_core::types::SnapshotId;

use std::str::FromStr;

use pplx_space_common_svc::core::types::{
    ContentHash, K8sNodeName, K8sNodeUid, OrgId, SandboxId, StorageUri, TemplateName,
};
use pplx_space_common_svc::sql;

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
    #[error("stored sandbox node identity is invalid")]
    InvalidNodeIdentity,
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
        let row: Option<(uuid::Uuid, Option<String>)> = sqlx::query_as(sql!(
            "SELECT s.org_id, o.snapshot_base_object_storage_uri
             FROM sandboxes s
             JOIN organizations o ON o.id = s.org_id
             WHERE s.id = $1"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        let Some((organization_id, current_snapshot_storage)) = row else {
            return Ok(None);
        };

        let node_rows: Vec<(String, String)> = sqlx::query_as(sql!(
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
                 SELECT snapshot.source_node_id AS node_id
                 FROM snapshots snapshot
                 JOIN sandboxes subject ON subject.id = $1
                 WHERE snapshot.id = $2
                   AND snapshot.source_node_id IS NOT NULL
                   AND (
                       snapshot.sandbox_id = $1
                       OR (
                           snapshot.artifact_origin = 'fork'
                           AND snapshot.org_id = subject.org_id
                       )
                   )
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
        .bind(sandbox_id.as_uuid())
        .bind(snapshot_id)
        .fetch_all(&self.pool)
        .await?;
        let nodes = node_rows
            .into_iter()
            .map(|(name, uid)| {
                Ok(SandboxNodeIdentity {
                    name: K8sNodeName::parse(name)
                        .map_err(|_| StorageVendStoreError::InvalidNodeIdentity)?,
                    uid: K8sNodeUid::parse(uid)
                        .map_err(|_| StorageVendStoreError::InvalidNodeIdentity)?,
                })
            })
            .collect::<Result<Vec<_>, StorageVendStoreError>>()?;

        let recorded_snapshot: Option<(String, Option<bool>)> = sqlx::query_as(sql!(
            "SELECT location.storage_connection_string,
                    snapshot.artifact_origin = 'fork' AND snapshot.state = 'creating'
             FROM snapshot_storage_locations location
             JOIN snapshots snapshot ON snapshot.id = location.snapshot_id
             WHERE location.snapshot_id = $1
               AND location.orphaned_at IS NULL
               AND COALESCE(snapshot.org_id, $2) = $2"
        ))
        .bind(snapshot_id)
        .bind(organization_id)
        .fetch_optional(&self.pool)
        .await?;
        let (recorded_snapshot_storage, portable_snapshot_write) = recorded_snapshot
            .map(|(uri, portable): (String, Option<bool>)| (Some(uri), portable.unwrap_or(false)))
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
            organization_id: OrgId::from_uuid(organization_id),
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
        let owner: Option<Option<uuid::Uuid>> =
            sqlx::query_scalar(sql!("SELECT org_id FROM templates WHERE name = $1"))
                .bind(template.as_str())
                .fetch_optional(&self.pool)
                .await?;
        Ok(owner.map(|organization_id| organization_id.map(OrgId::from_uuid)))
    }

    async fn template_release_published(
        &self,
        template: &TemplateName,
        hash: &ContentHash,
    ) -> Result<bool, StorageVendStoreError> {
        // `template_releases.hash` holds the canonical `sha256:<hex>` form
        // and, for releases registered before the prefix was required, the
        // bare hex. A published release must be found under either spelling.
        let canonical = hash.to_string();
        let bare = canonical
            .strip_prefix("sha256:")
            .unwrap_or(&canonical)
            .to_string();
        let published: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1
                 FROM template_release_storage_locations location
                 JOIN template_releases release
                   ON release.id = location.template_release_id
                 WHERE release.template_name = $1
                   AND release.hash IN ($2, $3))"
        ))
        .bind(template.as_str())
        .bind(canonical)
        .bind(bare)
        .fetch_one(&self.pool)
        .await?;
        Ok(published)
    }

    async fn org_vend_identity(
        &self,
        organization_id: OrgId,
    ) -> Result<Option<StorageVendIdentity>, StorageVendStoreError> {
        let row: Option<(String, String)> = sqlx::query_as(sql!(
            "SELECT provider, identity
             FROM org_storage_vend_identities
             WHERE org_id = $1"
        ))
        .bind(organization_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|(provider, identity)| {
            Ok(StorageVendIdentity {
                provider: provider
                    .parse()
                    .map_err(|_| StorageVendStoreError::UnsupportedProvider)?,
                identity,
            })
        })
        .transpose()
    }
}

#[cfg(test)]
mod postgres_tests;
