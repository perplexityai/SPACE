#![doc = include_str!("../README.md")]

use std::str::FromStr;

use jiff::Timestamp;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    ArtifactKind, ArtifactKindParseError, ArtifactOrigin, ArtifactOriginParseError,
    ExactStorageUri, ExecutionGeneration, ExecutionGenerationParseError, LocalArtifactId, NodeId,
    OrgId, SandboxId, SnapshotContentHash, SnapshotExecutionMetadata, SnapshotId, SnapshotKind,
    SnapshotKindParseError, SnapshotName, SnapshotParseError, SnapshotState,
    SnapshotStateParseError, TemplateName, TemplateParseError,
};
use pplx_space_common_core::types::{
    Digest, DigestParseError, SnapshotArtifact as SnapshotArtifactFile, SnapshotArtifactParseError,
};
use sqlx::FromRow;
use sqlx::types::Json;
use time::OffsetDateTime;
use uuid::Uuid;

/// Reading a snapshot catalog row failed.
#[derive(Debug, thiserror::Error)]
pub enum GetSnapshotError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),

    #[error("snapshot {snapshot_id} has an invalid kind")]
    InvalidKind {
        snapshot_id: SnapshotId,
        #[source]
        source: SnapshotKindParseError,
    },

    #[error("snapshot {snapshot_id} has an invalid state")]
    InvalidState {
        snapshot_id: SnapshotId,
        #[source]
        source: SnapshotStateParseError,
    },

    #[error("snapshot {snapshot_id} has an invalid artifact kind")]
    InvalidArtifactKind {
        snapshot_id: SnapshotId,
        #[source]
        source: ArtifactKindParseError,
    },

    #[error("snapshot {snapshot_id} has an invalid artifact origin")]
    InvalidArtifactOrigin {
        snapshot_id: SnapshotId,
        #[source]
        source: ArtifactOriginParseError,
    },

    #[error("snapshot {snapshot_id} has an invalid artifact snapshot name")]
    InvalidArtifactSnapshotName {
        snapshot_id: SnapshotId,
        #[source]
        source: SnapshotParseError,
    },

    #[error("snapshot {snapshot_id} has an invalid template name")]
    InvalidTemplateName {
        snapshot_id: SnapshotId,
        #[source]
        source: TemplateParseError,
    },

    #[error("snapshot {snapshot_id} has an inconsistent artifact identity")]
    InvalidArtifactIdentity { snapshot_id: SnapshotId },

    #[error("snapshot {snapshot_id} has invalid execution metadata")]
    InvalidExecutionMetadata {
        snapshot_id: SnapshotId,
        #[source]
        source: serde_json::Error,
    },

    #[error("snapshot {snapshot_id} has an invalid execution generation")]
    InvalidGeneration {
        snapshot_id: SnapshotId,
        #[source]
        source: ExecutionGenerationParseError,
    },

    #[error("snapshot {snapshot_id} has an invalid {column} timestamp")]
    InvalidTimestamp {
        snapshot_id: SnapshotId,
        column: &'static str,
        #[source]
        source: jiff::Error,
    },

    #[error("snapshot {snapshot_id} has an inconsistent storage location row")]
    InvalidStorageLocation { snapshot_id: SnapshotId },
}

/// Inserting a snapshot catalog aggregate failed.
#[derive(Debug, thiserror::Error)]
pub enum InsertSnapshotError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),

    #[error("failed to load snapshot {snapshot_id} after insert")]
    Load {
        snapshot_id: SnapshotId,
        #[source]
        source: GetSnapshotError,
    },

    #[error("snapshot {snapshot_id} disappeared after insert")]
    Missing { snapshot_id: SnapshotId },

    #[error("snapshot {snapshot_id} already exists with a different identity")]
    IdentityConflict { snapshot_id: SnapshotId },

    #[error("snapshot {snapshot_id} already exists with a different storage location")]
    StorageLocationConflict { snapshot_id: SnapshotId },

    #[error("snapshot {snapshot_id} has no storage location after insert")]
    MissingStorageLocation { snapshot_id: SnapshotId },
}

/// Values required to insert one snapshot and its durable storage location.
///
/// Database-generated identity and timestamps for the location row remain
/// private to the store. The duplicated scalar execution columns are derived
/// from [`SnapshotExecutionMetadata`] rather than accepted as separate inputs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewSnapshot {
    id: SnapshotId,
    sandbox_id: SandboxId,
    kind: SnapshotKind,
    state: SnapshotState,
    source_node_id: Option<NodeId>,
    vcpu: Option<i32>,
    memory_mib: Option<i32>,
    size_bytes: Option<i64>,
    template_id: Option<TemplateName>,
    content_hash: Option<SnapshotContentHash>,
    org_id: OrgId,
    artifact: Option<SnapshotArtifact>,
    execution_metadata: Option<SnapshotExecutionMetadata>,
    storage_uri: ExactStorageUri,
}

impl NewSnapshot {
    /// Construct the required snapshot and storage-location identity.
    pub fn new(
        id: SnapshotId,
        sandbox_id: SandboxId,
        kind: SnapshotKind,
        state: SnapshotState,
        org_id: OrgId,
        storage_uri: ExactStorageUri,
    ) -> Self {
        Self {
            id,
            sandbox_id,
            kind,
            state,
            source_node_id: None,
            vcpu: None,
            memory_mib: None,
            size_bytes: None,
            template_id: None,
            content_hash: None,
            org_id,
            artifact: None,
            execution_metadata: None,
            storage_uri,
        }
    }

    /// Set the node from which the snapshot bytes are uploaded.
    pub fn with_source_node_id(mut self, source_node_id: Option<NodeId>) -> Self {
        self.source_node_id = source_node_id;
        self
    }

    /// Set the captured VM compute shape.
    pub fn with_resources(mut self, vcpu: Option<i32>, memory_mib: Option<i32>) -> Self {
        self.vcpu = vcpu;
        self.memory_mib = memory_mib;
        self
    }

    /// Set the captured snapshot size.
    pub fn with_size_bytes(mut self, size_bytes: Option<i64>) -> Self {
        self.size_bytes = size_bytes;
        self
    }

    /// Set the template identity captured with the snapshot.
    pub fn with_template(
        mut self,
        template_id: Option<TemplateName>,
        content_hash: Option<SnapshotContentHash>,
    ) -> Self {
        self.template_id = template_id;
        self.content_hash = content_hash;
        self
    }

    /// Set artifact-backup identity for a snapshot backed by a local artifact.
    pub fn with_artifact(mut self, artifact: SnapshotArtifact) -> Self {
        self.artifact = Some(artifact);
        self
    }

    /// Set the canonical persisted execution environment.
    pub fn with_execution_metadata(
        mut self,
        execution_metadata: SnapshotExecutionMetadata,
    ) -> Self {
        self.execution_metadata = Some(execution_metadata);
        self
    }
}

/// The common read model for one durable `snapshots` row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotRecord {
    id: SnapshotId,
    sandbox_id: SandboxId,
    kind: SnapshotKind,
    state: SnapshotState,
    timestamp: Timestamp,
    source_node_id: Option<NodeId>,
    label: Option<String>,
    retention_tier: Option<String>,
    vcpu: Option<i32>,
    memory_mib: Option<i32>,
    size_bytes: Option<i64>,
    template_id: Option<TemplateName>,
    content_hash: Option<SnapshotContentHash>,
    org_id: Option<OrgId>,
    failure_reason: Option<String>,
    last_attempt_at: Option<Timestamp>,
    artifact: Option<SnapshotArtifact>,
    execution_metadata: Option<SnapshotExecutionMetadata>,
    generation: Option<ExecutionGeneration>,
    storage_location: Option<SnapshotStorageLocationRecord>,
}

impl SnapshotRecord {
    pub const fn id(&self) -> SnapshotId {
        self.id
    }

    pub const fn sandbox_id(&self) -> SandboxId {
        self.sandbox_id
    }

    pub const fn kind(&self) -> SnapshotKind {
        self.kind
    }

    pub const fn state(&self) -> SnapshotState {
        self.state
    }

    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    pub const fn source_node_id(&self) -> Option<NodeId> {
        self.source_node_id
    }

    pub fn label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    pub fn retention_tier(&self) -> Option<&str> {
        self.retention_tier.as_deref()
    }

    pub const fn vcpu(&self) -> Option<i32> {
        self.vcpu
    }

    pub const fn memory_mib(&self) -> Option<i32> {
        self.memory_mib
    }

    pub const fn size_bytes(&self) -> Option<i64> {
        self.size_bytes
    }

    pub const fn template_id(&self) -> Option<&TemplateName> {
        self.template_id.as_ref()
    }

    pub const fn content_hash(&self) -> Option<&SnapshotContentHash> {
        self.content_hash.as_ref()
    }

    pub const fn org_id(&self) -> Option<OrgId> {
        self.org_id
    }

    pub fn failure_reason(&self) -> Option<&str> {
        self.failure_reason.as_deref()
    }

    pub const fn last_attempt_at(&self) -> Option<Timestamp> {
        self.last_attempt_at
    }

    pub const fn artifact(&self) -> Option<&SnapshotArtifact> {
        self.artifact.as_ref()
    }

    pub const fn execution_metadata(&self) -> Option<&SnapshotExecutionMetadata> {
        self.execution_metadata.as_ref()
    }

    pub const fn generation(&self) -> Option<ExecutionGeneration> {
        self.generation
    }

    pub const fn storage_location(&self) -> Option<&SnapshotStorageLocationRecord> {
        self.storage_location.as_ref()
    }

    /// A full snapshot without artifact-backup identity is a suspend snapshot.
    pub const fn is_suspend_snapshot(&self) -> bool {
        matches!(self.kind, SnapshotKind::Full) && self.artifact.is_none()
    }
}

/// The linked base `snapshot_storage_locations` row for a snapshot.
///
/// The base `storage_connection_string` is stored verbatim. It is not a
/// resolved URI: when an encrypted-fields sidecar exists, this value is only a
/// non-secret stand-in and the encrypted sidecar remains the source of truth.
/// Orphan rows have `snapshot_id = NULL`, cannot join to an owning snapshot,
/// and are therefore intentionally unreachable through [`get`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotStorageLocationRecord {
    id: Uuid,
    snapshot_id: SnapshotId,
    storage_connection_string: String,
    created_at: Timestamp,
    org_id: Option<OrgId>,
    orphaned_at: Option<Timestamp>,
}

impl SnapshotStorageLocationRecord {
    pub const fn id(&self) -> Uuid {
        self.id
    }

    pub const fn snapshot_id(&self) -> SnapshotId {
        self.snapshot_id
    }

    /// The verbatim base-table value, which may be an encrypted-row stand-in.
    pub fn storage_connection_string(&self) -> &str {
        &self.storage_connection_string
    }

    pub const fn created_at(&self) -> Timestamp {
        self.created_at
    }

    pub const fn org_id(&self) -> Option<OrgId> {
        self.org_id
    }

    pub const fn orphaned_at(&self) -> Option<Timestamp> {
        self.orphaned_at
    }
}

/// Typed artifact-backup identity attached to a snapshot record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotArtifact {
    local_artifact_id: Option<LocalArtifactId>,
    kind: ArtifactKind,
    origin: ArtifactOrigin,
    snapshot_name: Option<SnapshotName>,
}

/// A snapshot artifact identity violated the catalog schema contract.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SnapshotArtifactIdentityError {
    #[error("snapshot origin requires a snapshot name")]
    MissingSnapshotName,

    #[error("snapshot name is only valid for snapshot origin")]
    UnexpectedSnapshotName,

    #[error("artifact origin requires a local artifact ID unless it is fork")]
    MissingLocalArtifactId,
}

impl SnapshotArtifact {
    /// Construct a schema-valid artifact identity.
    pub fn new(
        local_artifact_id: Option<LocalArtifactId>,
        kind: ArtifactKind,
        origin: ArtifactOrigin,
        snapshot_name: Option<SnapshotName>,
    ) -> Result<Self, SnapshotArtifactIdentityError> {
        match (origin, snapshot_name.is_some()) {
            (ArtifactOrigin::Snapshot, false) => {
                return Err(SnapshotArtifactIdentityError::MissingSnapshotName);
            }
            (ArtifactOrigin::Snapshot, true) | (_, false) => {}
            (_, true) => return Err(SnapshotArtifactIdentityError::UnexpectedSnapshotName),
        }
        if local_artifact_id.is_none() && origin != ArtifactOrigin::Fork {
            return Err(SnapshotArtifactIdentityError::MissingLocalArtifactId);
        }
        Ok(Self {
            local_artifact_id,
            kind,
            origin,
            snapshot_name,
        })
    }

    pub const fn local_artifact_id(&self) -> Option<LocalArtifactId> {
        self.local_artifact_id
    }

    pub const fn kind(&self) -> ArtifactKind {
        self.kind
    }

    pub const fn origin(&self) -> ArtifactOrigin {
        self.origin
    }

    pub const fn snapshot_name(&self) -> Option<&SnapshotName> {
        self.snapshot_name.as_ref()
    }
}

#[derive(Debug, FromRow)]
struct SnapshotRow {
    id: SnapshotId,
    sandbox_id: SandboxId,
    snapshot_kind: String,
    state: String,
    timestamp: OffsetDateTime,
    source_node_id: Option<NodeId>,
    label: Option<String>,
    retention_tier: Option<String>,
    vcpu: Option<i32>,
    memory_mib: Option<i32>,
    size_bytes: Option<i64>,
    template_id: Option<String>,
    content_hash: Option<String>,
    org_id: Option<OrgId>,
    failure_reason: Option<String>,
    last_attempt_at: Option<OffsetDateTime>,
    local_artifact_id: Option<LocalArtifactId>,
    artifact_kind: Option<String>,
    artifact_origin: Option<String>,
    artifact_snapshot_name: Option<String>,
    execution_metadata: Option<Json<serde_json::Value>>,
    generation: Option<i64>,
    storage_location_id: Option<Uuid>,
    storage_location_snapshot_id: Option<SnapshotId>,
    storage_connection_string: Option<String>,
    storage_location_created_at: Option<OffsetDateTime>,
    storage_location_org_id: Option<OrgId>,
    storage_location_orphaned_at: Option<OffsetDateTime>,
}

/// Deleting a snapshot catalog aggregate failed.
#[derive(Debug, thiserror::Error)]
pub enum DeleteSnapshotCatalogError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Migration scaffolding for callers whose surrounding workflow still uses
/// `sqlx::Error`. This conversion stays lossless while the store error has one
/// transparent variant.
impl From<DeleteSnapshotCatalogError> for sqlx::Error {
    fn from(error: DeleteSnapshotCatalogError) -> Self {
        match error {
            DeleteSnapshotCatalogError::Database(source) => source,
        }
    }
}

/// Delete one snapshot and its optional storage location from the durable catalog.
///
/// This is unguarded catalog CRUD. It does not delete bytes from S3 or another
/// storage backend. The caller owns authorization and row locks; requiring its
/// open transaction keeps the child-before-parent deletion atomic with those
/// workflow checks. Returns `false` when the snapshot did not exist.
pub async fn delete_snapshot_catalog(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    snapshot_id: SnapshotId,
) -> Result<bool, DeleteSnapshotCatalogError> {
    // The catalog contract permits at most one row. Keep the predicate-based
    // delete defensive so legacy corruption cannot strand the parent row.
    sqlx::query(sql!(
        "DELETE FROM snapshot_storage_locations WHERE snapshot_id = $1"
    ))
    .bind(snapshot_id.as_uuid())
    .execute(&mut **tx)
    .await?;
    let result = sqlx::query(sql!("DELETE FROM snapshots WHERE id = $1"))
        .bind(snapshot_id.as_uuid())
        .execute(&mut **tx)
        .await?;
    Ok(result.rows_affected() == 1)
}

/// Content digest of one artifact file in a snapshot attempt, as reported by
/// the node that uploaded it. Restore compares the bytes it pulls against this
/// instead of trusting the object store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotArtifactDigest {
    pub artifact: SnapshotArtifactFile,
    pub digest: Digest,
    pub size_bytes: u64,
}

/// Recording artifact digests failed.
#[derive(Debug, thiserror::Error)]
pub enum RecordArtifactDigestsError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),

    #[error("snapshot {snapshot_id} already records a different digest for {artifact}")]
    Conflict {
        snapshot_id: SnapshotId,
        artifact: SnapshotArtifactFile,
    },

    #[error("snapshot {snapshot_id} reports a size for {artifact} beyond the catalog range")]
    SizeOutOfRange {
        snapshot_id: SnapshotId,
        artifact: SnapshotArtifactFile,
    },
}

/// Reading artifact digests failed.
#[derive(Debug, thiserror::Error)]
pub enum GetArtifactDigestsError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),

    #[error("snapshot {snapshot_id} records an unknown artifact")]
    InvalidArtifact {
        snapshot_id: SnapshotId,
        #[source]
        source: SnapshotArtifactParseError,
    },

    #[error("snapshot {snapshot_id} records an invalid digest for {artifact}")]
    InvalidDigest {
        snapshot_id: SnapshotId,
        artifact: SnapshotArtifactFile,
        #[source]
        source: DigestParseError,
    },

    #[error("snapshot {snapshot_id} records a negative size for {artifact}")]
    InvalidSize {
        snapshot_id: SnapshotId,
        artifact: SnapshotArtifactFile,
    },
}

/// Record the digests the uploading node reported for `snapshot_id`.
///
/// Digests are write-once: a replay that repeats them is a no-op and one that
/// disagrees is refused, so a later finalize cannot swap what restore verifies.
pub async fn record_artifact_digests(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    snapshot_id: SnapshotId,
    digests: &[SnapshotArtifactDigest],
) -> Result<(), RecordArtifactDigestsError> {
    for entry in digests {
        let size_bytes = i64::try_from(entry.size_bytes).map_err(|_| {
            RecordArtifactDigestsError::SizeOutOfRange {
                snapshot_id,
                artifact: entry.artifact,
            }
        })?;
        // The no-op update keeps an identical replay at one affected row while
        // a differing one touches nothing.
        let result = sqlx::query(sql!(
            "INSERT INTO snapshot_artifact_digests
                 (snapshot_id, artifact, digest, size_bytes)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (snapshot_id, artifact) DO UPDATE
                 SET digest = EXCLUDED.digest
               WHERE snapshot_artifact_digests.digest = EXCLUDED.digest
                 AND snapshot_artifact_digests.size_bytes = EXCLUDED.size_bytes"
        ))
        .bind(snapshot_id.as_uuid())
        .bind(entry.artifact.file_name())
        .bind(entry.digest.to_string())
        .bind(size_bytes)
        .execute(&mut **tx)
        .await?;
        if result.rows_affected() != 1 {
            return Err(RecordArtifactDigestsError::Conflict {
                snapshot_id,
                artifact: entry.artifact,
            });
        }
    }
    Ok(())
}

/// The digests recorded for `snapshot_id`, ordered by artifact file name.
/// Empty for attempts finalized before digests were recorded.
pub async fn artifact_digests(
    executor: impl sqlx::PgExecutor<'_>,
    snapshot_id: SnapshotId,
) -> Result<Vec<SnapshotArtifactDigest>, GetArtifactDigestsError> {
    let rows: Vec<(String, String, i64)> = sqlx::query_as(sql!(
        "SELECT artifact, digest, size_bytes
         FROM snapshot_artifact_digests
         WHERE snapshot_id = $1
         ORDER BY artifact"
    ))
    .bind(snapshot_id.as_uuid())
    .fetch_all(executor)
    .await?;

    rows.into_iter()
        .map(|(artifact, digest, size_bytes)| {
            let artifact = SnapshotArtifactFile::from_str(&artifact).map_err(|source| {
                GetArtifactDigestsError::InvalidArtifact {
                    snapshot_id,
                    source,
                }
            })?;
            let digest = Digest::from_str(&digest).map_err(|source| {
                GetArtifactDigestsError::InvalidDigest {
                    snapshot_id,
                    artifact,
                    source,
                }
            })?;
            let size_bytes =
                u64::try_from(size_bytes).map_err(|_| GetArtifactDigestsError::InvalidSize {
                    snapshot_id,
                    artifact,
                })?;
            Ok(SnapshotArtifactDigest {
                artifact,
                digest,
                size_bytes,
            })
        })
        .collect()
}

/// Reading a ready artifact's storage location failed. A malformed stored
/// row surfaces as a `sqlx::Error` decode failure, like every other typed
/// column.
#[derive(Debug, thiserror::Error)]
pub enum ReadyArtifactLocationError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Where a `ready` snapshot's bytes live, as a restoring node needs it: the
/// catalog row, the storage connection string of its location, and the byte
/// count the upload reported.
#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct ReadyArtifactLocation {
    pub snapshot_id: SnapshotId,
    pub storage_connection_string: String,
    pub size_bytes: Option<i64>,
}

/// The newest `ready` suspend snapshot of `sandbox_id`, when that sandbox is
/// placed on one of `node_ids`: active there, or the target of a pending
/// migration or restore. A sandbox placed elsewhere reads as absent, not as
/// an error: the caller is a node asking about its own set.
///
/// `timestamp` is the row's immutable creation time, so the newest row is the
/// suspend attempt that started most recently and reached `ready`. Rows in
/// `creating`, `failed` or `deleting` are invisible, which keeps a mid-flight
/// upload, or a row claimed for prune, out of restore.
pub async fn latest_ready_suspend_location_placed_on(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    node_ids: &[NodeId],
) -> Result<Option<ReadyArtifactLocation>, ReadyArtifactLocationError> {
    let row = sqlx::query_as::<_, ReadyArtifactLocation>(sql!(
        "SELECT s.id AS snapshot_id,
                ssl.storage_connection_string,
                s.size_bytes
         FROM snapshots s
         JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = s.id
         JOIN sandboxes sb ON sb.id = s.sandbox_id
         WHERE s.sandbox_id = $1
           AND (sb.active_node_id = ANY($2) OR sb.target_node_id = ANY($2))
           AND s.type = 'full'
           AND (s.artifact_origin IS NULL OR s.artifact_origin = 'suspend')
           AND s.state = 'ready'
         ORDER BY s.timestamp DESC
         LIMIT 1"
    ))
    .bind(sandbox_id)
    .bind(node_ids)
    .fetch_optional(executor)
    .await?;
    Ok(row)
}

/// The newest `ready` backup of the exact local artifact
/// (`sandbox_id`, `local_artifact_id`, `artifact_kind`), when that sandbox is
/// placed on one of `node_ids`. Same placement and absence semantics as
/// [`latest_ready_suspend_location_placed_on`].
pub async fn ready_backup_location_placed_on(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    local_artifact_id: LocalArtifactId,
    artifact_kind: ArtifactKind,
    node_ids: &[NodeId],
) -> Result<Option<ReadyArtifactLocation>, ReadyArtifactLocationError> {
    let row = sqlx::query_as::<_, ReadyArtifactLocation>(sql!(
        "SELECT s.id AS snapshot_id,
                ssl.storage_connection_string,
                s.size_bytes
         FROM snapshots s
         JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = s.id
         JOIN sandboxes sb ON sb.id = s.sandbox_id
         WHERE s.sandbox_id = $1
           AND (sb.active_node_id = ANY($4) OR sb.target_node_id = ANY($4))
           AND s.local_artifact_id = $2
           AND s.artifact_kind = $3
           AND s.state = 'ready'
         ORDER BY s.timestamp DESC
         LIMIT 1"
    ))
    .bind(sandbox_id)
    .bind(local_artifact_id.as_uuid())
    .bind(artifact_kind.as_str())
    .bind(node_ids)
    .fetch_optional(executor)
    .await?;
    Ok(row)
}

#[derive(Debug, thiserror::Error)]
pub enum CheckBackupError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Whether these exact immutable bytes have a usable ready backup. This is
/// placement-independent so a former hosting node can release its local copy;
/// it discloses neither the backup identity nor its storage credentials.
pub async fn artifact_backup_ready(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    local_artifact_id: LocalArtifactId,
    artifact_kind: ArtifactKind,
) -> Result<bool, CheckBackupError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
             FROM snapshots s
             JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = s.id
             WHERE s.sandbox_id = $1
               AND s.local_artifact_id = $2
               AND s.artifact_kind = $3
               AND s.state = 'ready'
               AND ssl.orphaned_at IS NULL
         )"
    ))
    .bind(sandbox_id)
    .bind(local_artifact_id.as_uuid())
    .bind(artifact_kind.as_str())
    .fetch_one(executor)
    .await?)
}

#[derive(Debug, thiserror::Error)]
pub enum CheckArtifactRegistrationError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Whether an attempt other than `except` already registered the local
/// artifact `local_artifact_id` of `sandbox_id`, in any state: some other
/// committed attempt for these bytes was admitted, fenced or not. Only as durable as that
/// row: once retention deletes it, this answers `false`.
pub async fn artifact_registered_by_another_attempt(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    local_artifact_id: LocalArtifactId,
    except: SnapshotId,
) -> Result<bool, CheckArtifactRegistrationError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
             FROM snapshots
             WHERE sandbox_id = $1
               AND local_artifact_id = $2
               AND id <> $3
         )"
    ))
    .bind(sandbox_id)
    .bind(local_artifact_id.as_uuid())
    .bind(except)
    .fetch_one(executor)
    .await?)
}

/// Whether `snapshot_id` is still the latest ready suspend attempt of
/// `sandbox_id`, with a live storage location. This is the proof an unpinned
/// local cache needs: a ready backup of an older attempt cannot show which
/// generation that cache contains.
pub async fn is_latest_ready_suspend(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    snapshot_id: SnapshotId,
) -> Result<bool, CheckBackupError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT COALESCE((
             SELECT s.id = $2 AND ssl.orphaned_at IS NULL
             FROM snapshots s
             JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = s.id
             WHERE s.sandbox_id = $1
               AND s.type = 'full'
               AND (s.artifact_origin IS NULL OR s.artifact_origin = 'suspend')
               AND s.state = 'ready'
             ORDER BY s.timestamp DESC
             LIMIT 1
         ), FALSE)"
    ))
    .bind(sandbox_id)
    .bind(snapshot_id)
    .fetch_one(executor)
    .await?)
}

/// Fetch one snapshot catalog row by id.
///
/// Returns the same typed model for every read. `None` means no row has this id.
pub async fn get(
    executor: impl sqlx::PgExecutor<'_>,
    snapshot_id: SnapshotId,
) -> Result<Option<SnapshotRecord>, GetSnapshotError> {
    let row: Option<SnapshotRow> = sqlx::query_as(sql!(
        "SELECT sn.id, sn.sandbox_id, sn.type AS snapshot_kind, sn.state,
                sn.timestamp, sn.source_node_id, sn.label, sn.retention_tier,
                sn.vcpu, sn.memory_mib, sn.size_bytes, sn.template_id,
                sn.content_hash, sn.org_id, sn.failure_reason,
                sn.last_attempt_at, sn.local_artifact_id, sn.artifact_kind,
                sn.artifact_origin, sn.artifact_snapshot_name,
                sn.execution_metadata, sn.generation,
                ssl.id AS storage_location_id,
                ssl.snapshot_id AS storage_location_snapshot_id,
                ssl.storage_connection_string,
                ssl.created_at AS storage_location_created_at,
                ssl.org_id AS storage_location_org_id,
                ssl.orphaned_at AS storage_location_orphaned_at
         FROM snapshots sn
         LEFT JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
         WHERE sn.id = $1"
    ))
    .bind(snapshot_id.as_uuid())
    .fetch_optional(executor)
    .await?;

    row.map(SnapshotRecord::try_from).transpose()
}

impl TryFrom<SnapshotRow> for SnapshotRecord {
    type Error = GetSnapshotError;

    fn try_from(row: SnapshotRow) -> Result<Self, Self::Error> {
        let snapshot_id = row.id;
        let kind = SnapshotKind::from_str(&row.snapshot_kind).map_err(|source| {
            GetSnapshotError::InvalidKind {
                snapshot_id,
                source,
            }
        })?;
        let state = SnapshotState::from_str(&row.state).map_err(|source| {
            GetSnapshotError::InvalidState {
                snapshot_id,
                source,
            }
        })?;
        let storage_location = snapshot_storage_location(snapshot_id, &row)?;
        let timestamp = snapshot_timestamp(snapshot_id, "timestamp", row.timestamp)?;
        let template_id = row
            .template_id
            .map(|value| {
                TemplateName::try_from(value).map_err(|source| {
                    GetSnapshotError::InvalidTemplateName {
                        snapshot_id,
                        source,
                    }
                })
            })
            .transpose()?;
        let content_hash = row.content_hash.map(SnapshotContentHash::from_stored);
        let last_attempt_at = row
            .last_attempt_at
            .map(|value| snapshot_timestamp(snapshot_id, "last_attempt_at", value))
            .transpose()?;
        let artifact = snapshot_artifact(
            snapshot_id,
            row.local_artifact_id,
            row.artifact_kind,
            row.artifact_origin,
            row.artifact_snapshot_name,
        )?;
        let execution_metadata = row
            .execution_metadata
            .map(|value| {
                serde_json::from_value(value.0).map_err(|source| {
                    GetSnapshotError::InvalidExecutionMetadata {
                        snapshot_id,
                        source,
                    }
                })
            })
            .transpose()?;
        let generation = row
            .generation
            .map(|value| {
                ExecutionGeneration::new(value).map_err(|source| {
                    GetSnapshotError::InvalidGeneration {
                        snapshot_id,
                        source,
                    }
                })
            })
            .transpose()?;
        Ok(Self {
            id: snapshot_id,
            sandbox_id: row.sandbox_id,
            kind,
            state,
            timestamp,
            source_node_id: row.source_node_id,
            label: row.label,
            retention_tier: row.retention_tier,
            vcpu: row.vcpu,
            memory_mib: row.memory_mib,
            size_bytes: row.size_bytes,
            template_id,
            content_hash,
            org_id: row.org_id,
            failure_reason: row.failure_reason,
            last_attempt_at,
            artifact,
            execution_metadata,
            generation,
            storage_location,
        })
    }
}

fn snapshot_timestamp(
    snapshot_id: SnapshotId,
    column: &'static str,
    value: OffsetDateTime,
) -> Result<Timestamp, GetSnapshotError> {
    Timestamp::from_nanosecond(value.unix_timestamp_nanos()).map_err(|source| {
        GetSnapshotError::InvalidTimestamp {
            snapshot_id,
            column,
            source,
        }
    })
}

fn snapshot_storage_location(
    snapshot_id: SnapshotId,
    row: &SnapshotRow,
) -> Result<Option<SnapshotStorageLocationRecord>, GetSnapshotError> {
    let location = match (
        row.storage_location_id,
        row.storage_location_snapshot_id,
        row.storage_connection_string.as_deref(),
        row.storage_location_created_at,
    ) {
        (None, None, None, None) => return Ok(None),
        (Some(id), Some(location_snapshot_id), Some(connection_string), Some(created_at)) => {
            SnapshotStorageLocationRecord {
                id,
                snapshot_id: location_snapshot_id,
                storage_connection_string: connection_string.to_owned(),
                created_at: snapshot_timestamp(
                    snapshot_id,
                    "storage_location.created_at",
                    created_at,
                )?,
                org_id: row.storage_location_org_id,
                orphaned_at: row
                    .storage_location_orphaned_at
                    .map(|value| {
                        snapshot_timestamp(snapshot_id, "storage_location.orphaned_at", value)
                    })
                    .transpose()?,
            }
        }
        _ => return Err(GetSnapshotError::InvalidStorageLocation { snapshot_id }),
    };
    if location.snapshot_id != snapshot_id {
        return Err(GetSnapshotError::InvalidStorageLocation { snapshot_id });
    }
    Ok(Some(location))
}

fn snapshot_artifact(
    snapshot_id: SnapshotId,
    local_artifact_id: Option<LocalArtifactId>,
    artifact_kind: Option<String>,
    artifact_origin: Option<String>,
    snapshot_name: Option<String>,
) -> Result<Option<SnapshotArtifact>, GetSnapshotError> {
    let (artifact_kind, artifact_origin) = match (artifact_kind, artifact_origin) {
        (None, None) if local_artifact_id.is_none() && snapshot_name.is_none() => return Ok(None),
        (Some(kind), Some(origin)) => (kind, origin),
        _ => return Err(GetSnapshotError::InvalidArtifactIdentity { snapshot_id }),
    };
    let kind = ArtifactKind::from_str(&artifact_kind).map_err(|source| {
        GetSnapshotError::InvalidArtifactKind {
            snapshot_id,
            source,
        }
    })?;
    let origin = ArtifactOrigin::from_str(&artifact_origin).map_err(|source| {
        GetSnapshotError::InvalidArtifactOrigin {
            snapshot_id,
            source,
        }
    })?;
    let snapshot_name = snapshot_name
        .map(|value| {
            SnapshotName::try_from(value).map_err(|source| {
                GetSnapshotError::InvalidArtifactSnapshotName {
                    snapshot_id,
                    source,
                }
            })
        })
        .transpose()?;

    SnapshotArtifact::new(local_artifact_id, kind, origin, snapshot_name)
        .map(Some)
        .map_err(|_| GetSnapshotError::InvalidArtifactIdentity { snapshot_id })
}

/// Whether [`insert`] wrote the snapshot row or found the same identity already
/// registered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InsertOutcome {
    /// This call's transaction wrote the row.
    Created,
    /// A committed row with the same identity already existed.
    AlreadyRegistered,
}

/// Insert one snapshot and its single durable storage location.
///
/// The operation is idempotent for the same durable identity and exact URI,
/// and reports which case it took so a caller can admit a new registration
/// under conditions a replay doesn't have to meet again. Reusing the snapshot
/// id for different content or a different location is a domain conflict. The
/// caller owns the transaction so the two-table mutation cannot be committed
/// partially.
pub async fn insert(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    snapshot: &NewSnapshot,
) -> Result<(SnapshotRecord, InsertOutcome), InsertSnapshotError> {
    let artifact = snapshot.artifact.as_ref();
    let execution = snapshot
        .execution_metadata
        .as_ref()
        .map(SnapshotExecutionMetadata::execution);
    let state_format = execution.and_then(|metadata| metadata.state_format());
    let content_hash = snapshot
        .content_hash
        .as_ref()
        .map(SnapshotContentHash::as_stored);

    // Keep the conflict target implicit: snapshots has both a primary key on
    // id and a unique constraint on (id, org_id), and concurrent idempotent
    // inserts may collide on either one.
    let inserted_id: Option<Uuid> = sqlx::query_scalar(sql!(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, source_node_id, vcpu, memory_mib,
              size_bytes, template_id, content_hash, org_id,
              local_artifact_id, artifact_kind, artifact_origin,
              artifact_snapshot_name, runtime, runtime_version, guest_os,
              guest_arch, state_format_name, state_format_version,
              execution_metadata)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                 $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
         ON CONFLICT DO NOTHING
         RETURNING id"
    ))
    .bind(snapshot.id.as_uuid())
    .bind(snapshot.sandbox_id.as_uuid())
    .bind(snapshot.kind.as_str())
    .bind(snapshot.state.as_str())
    .bind(snapshot.source_node_id.map(NodeId::as_uuid))
    .bind(snapshot.vcpu)
    .bind(snapshot.memory_mib)
    .bind(snapshot.size_bytes)
    .bind(snapshot.template_id.as_ref().map(TemplateName::as_str))
    .bind(content_hash)
    .bind(snapshot.org_id)
    .bind(
        artifact
            .and_then(SnapshotArtifact::local_artifact_id)
            .map(LocalArtifactId::as_uuid),
    )
    .bind(artifact.map(|artifact| artifact.kind().as_str()))
    .bind(artifact.map(|artifact| artifact.origin().as_str()))
    .bind(
        artifact
            .and_then(SnapshotArtifact::snapshot_name)
            .map(SnapshotName::as_str),
    )
    .bind(execution.map(|metadata| metadata.runtime().kind.alias()))
    .bind(execution.map(|metadata| metadata.runtime().version.as_str()))
    .bind(execution.map(|metadata| metadata.guest().os.as_str()))
    .bind(execution.map(|metadata| metadata.guest().architecture.as_str()))
    .bind(state_format.map(|identity| identity.format.as_str()))
    .bind(state_format.map(|identity| identity.version.as_str()))
    .bind(snapshot.execution_metadata.as_ref().map(Json))
    .fetch_optional(&mut **tx)
    .await?;

    if inserted_id.is_none() {
        let persisted = load_after_insert(tx, snapshot.id).await?;
        if !same_snapshot_identity(&persisted, snapshot) {
            return Err(InsertSnapshotError::IdentityConflict {
                snapshot_id: snapshot.id,
            });
        }
    }

    // `UNIQUE (snapshot_id)` is the concurrency backstop: after a conflicting
    // insert finishes, the aggregate reload observes the winning transaction's
    // value without requiring a parent-row or location-row lock.
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, $3)
         ON CONFLICT (snapshot_id) DO NOTHING"
    ))
    .bind(snapshot.id.as_uuid())
    .bind(snapshot.storage_uri.as_wire())
    .bind(snapshot.org_id)
    .execute(&mut **tx)
    .await?;

    let persisted = load_after_insert(tx, snapshot.id).await?;
    let Some(location) = persisted.storage_location() else {
        return Err(InsertSnapshotError::MissingStorageLocation {
            snapshot_id: snapshot.id,
        });
    };
    if location.storage_connection_string() != snapshot.storage_uri.as_wire() {
        return Err(InsertSnapshotError::StorageLocationConflict {
            snapshot_id: snapshot.id,
        });
    }
    let outcome = if inserted_id.is_some() {
        InsertOutcome::Created
    } else {
        InsertOutcome::AlreadyRegistered
    };
    Ok((persisted, outcome))
}

async fn load_after_insert(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    snapshot_id: SnapshotId,
) -> Result<SnapshotRecord, InsertSnapshotError> {
    get(&mut **tx, snapshot_id)
        .await
        .map_err(|source| InsertSnapshotError::Load {
            snapshot_id,
            source,
        })?
        .ok_or(InsertSnapshotError::Missing { snapshot_id })
}

fn same_snapshot_identity(persisted: &SnapshotRecord, expected: &NewSnapshot) -> bool {
    persisted.sandbox_id() == expected.sandbox_id
        && persisted.kind() == expected.kind
        && persisted.org_id() == Some(expected.org_id)
        && persisted.artifact() == expected.artifact.as_ref()
        && persisted.execution_metadata() == expected.execution_metadata.as_ref()
}

#[cfg(test)]
mod tests;
