use std::path::{Path, PathBuf};

use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, ContentHash, DEFAULT_GUEST_USER, ExecutionGeneration,
    ExecutionMetadata, LocalArtifactId, RuntimeKind, SandboxId, SandboxOwner, SandboxRuntimeLabel,
    TemplateName,
};

use crate::retained_source::{
    RetainedSource, RetainedSourceCheck, RetainedSourceVerdicts, RootfsArtifact,
};
use crate::snapshot_restore::RecoveredSandboxRowFieldsError;
use crate::state::SandboxState;

pub(super) const LOCAL_SNAPSHOT_DIR: &str = "snapshot";
const SUSPEND_STAGING_PARENT_DIR: &str = "snapshots";
pub(super) const SUSPEND_STAGING_PREFIX: &str = ".suspend-publish-";

pub(super) fn canonical_suspend_snapshot_dir(sandbox_dir: &Path) -> PathBuf {
    sandbox_dir.join(LOCAL_SNAPSHOT_DIR)
}

pub(super) fn suspend_snapshot_staging_parent_dir(sandbox_dir: &Path) -> PathBuf {
    sandbox_dir.join(SUSPEND_STAGING_PARENT_DIR)
}

pub(super) fn suspend_snapshot_staging_dir(
    sandbox_dir: &Path,
    local_artifact_id: LocalArtifactId,
) -> PathBuf {
    suspend_snapshot_staging_parent_dir(sandbox_dir)
        .join(format!("{SUSPEND_STAGING_PREFIX}{local_artifact_id}"))
}

/// Written into an encrypted suspend's staging directory once the capture
/// completed, before its volume is sealed. It carries the sidecar metadata of
/// that capture, so a later seal publishes exactly it after the runtime is
/// gone. Its absence means the staging holds no finished encrypted capture.
pub(super) const ENCRYPTED_CAPTURE_RECORD: &str = "encrypted-capture.json";

/// How long recovery keeps retrying an unsealed encrypted capture. After it,
/// the staging directory is treated as incomplete and the row quarantines as
/// an only-copy filesystem, which deletion still retains.
pub(super) const ENCRYPTED_SEAL_RETRY_WINDOW: std::time::Duration =
    std::time::Duration::from_secs(6 * 60 * 60);

/// The record, written durably before the capture under a name recovery
/// ignores. [`commit_encrypted_capture_record`] gives it the record's name
/// once the capture has finished.
const ENCRYPTED_CAPTURE_INTENT: &str = ".encrypted-capture.json.intent";

/// Write the capture's record before the capture, while a failure can still
/// roll back to paused. Nothing treats it as a record until it is committed,
/// so an interrupted capture can never be sealed and published.
pub(super) fn prepare_encrypted_capture_record(
    staging_dir: &Path,
    metadata: &crate::snapshot_upload::SnapshotMeta,
) -> std::io::Result<()> {
    let json = serde_json::to_vec_pretty(metadata).map_err(std::io::Error::other)?;
    crate::snapshot_upload::replace_file_durably(
        staging_dir,
        ".encrypted-capture.json.tmp",
        ENCRYPTED_CAPTURE_INTENT,
        &json,
    )?;
    // The record is the only copy of the capture's identity: make the
    // staging directory's own entry durable too, up to the sandbox directory.
    let mut directory = staging_dir.parent();
    for _ in 0..2 {
        let Some(parent) = directory else { break };
        std::fs::File::open(parent)?.sync_all()?;
        directory = parent.parent();
    }
    Ok(())
}

/// Mark the capture finished: a same-directory rename of the already
/// durable record, then a directory sync. It writes no data, so it can fail
/// only if the staging filesystem itself is failing.
pub(super) fn commit_encrypted_capture_record(staging_dir: &Path) -> std::io::Result<()> {
    std::fs::rename(
        staging_dir.join(ENCRYPTED_CAPTURE_INTENT),
        staging_dir.join(ENCRYPTED_CAPTURE_RECORD),
    )?;
    std::fs::File::open(staging_dir)?.sync_all()
}

pub(super) fn read_encrypted_capture_record(
    staging_dir: &Path,
) -> Result<Option<crate::snapshot_upload::SnapshotMeta>, SuspendSnapshotPublicationError> {
    let error = |source| SuspendSnapshotPublicationError::EncryptedCaptureRecord {
        staging_dir: path_label(staging_dir),
        source,
    };
    match std::fs::read(staging_dir.join(ENCRYPTED_CAPTURE_RECORD)) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|source| error(std::io::Error::other(source))),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(error(source)),
    }
}

/// The sealed artifacts and sidecar are in place; the record has done its job.
pub(super) fn remove_encrypted_capture_record(staging_dir: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(staging_dir.join(ENCRYPTED_CAPTURE_RECORD)) {
        Ok(()) => std::fs::File::open(staging_dir)?.sync_all(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Staging directories of `sandbox_dir` holding a finished encrypted capture
/// whose volume has not sealed yet and may still be sealed, in name order.
/// Every seal retry selects through this, so a record past its retry window is
/// never sealed: classification quarantines it instead.
pub(super) fn encrypted_captures_awaiting_seal(
    sandbox_dir: &Path,
) -> Result<Vec<StagedSuspendSnapshotArtifact>, SuspendSnapshotPublicationError> {
    let staging_parent = suspend_snapshot_staging_parent_dir(sandbox_dir);
    let entries = match std::fs::read_dir(&staging_parent) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(SuspendSnapshotPublicationError::ListStagingParent {
                staging_parent: path_label(&staging_parent),
                source,
            });
        }
    };
    let now = std::time::SystemTime::now();
    let mut pending = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter_map(|path| {
            let local_artifact_id = path
                .file_name()?
                .to_str()?
                .strip_prefix(SUSPEND_STAGING_PREFIX)?
                .parse()
                .ok()?;
            encrypted_seal_pending(&path, now).then_some(StagedSuspendSnapshotArtifact {
                local_artifact_id,
                staging_dir: path,
            })
        })
        .collect::<Vec<_>>();
    pending.sort_by(|left, right| left.staging_dir.cmp(&right.staging_dir));
    Ok(pending)
}

/// The capture a seal retry should seal: the newest pending record, unless a
/// complete staged capture already awaits publication, which classification
/// publishes without a seal.
pub(super) fn recorded_encrypted_capture_to_seal(
    sandbox_dir: &Path,
) -> Result<Option<StagedSuspendSnapshotArtifact>, SuspendSnapshotPublicationError> {
    match find_complete_suspend_snapshot_staging_dir(
        sandbox_dir,
        &RetainedSourceVerdicts::default(),
    ) {
        Ok(Some(_)) => return Ok(None),
        Ok(None) | Err(SuspendSnapshotPublicationError::EncryptedSealPending { .. }) => {}
        Err(error) => return Err(error),
    }
    Ok(encrypted_captures_awaiting_seal(sandbox_dir)?.pop())
}

/// An unsealed capture is pending evidence while recovery may still seal it.
fn encrypted_seal_pending(staging_dir: &Path, now: std::time::SystemTime) -> bool {
    std::fs::metadata(staging_dir.join(ENCRYPTED_CAPTURE_RECORD))
        .and_then(|metadata| metadata.modified())
        .is_ok_and(|recorded| match now.duration_since(recorded) {
            Ok(age) => age < ENCRYPTED_SEAL_RETRY_WINDOW,
            // Small clock skew keeps a fresh record pending; a record dated
            // further ahead than the window itself cannot be trusted to expire.
            Err(ahead) => ahead.duration() < ENCRYPTED_SEAL_RETRY_WINDOW,
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CanonicalSuspendSnapshotArtifact {
    pub(super) local_artifact_id: LocalArtifactId,
    pub(super) path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StagedSuspendSnapshotArtifact {
    pub(super) local_artifact_id: LocalArtifactId,
    pub(super) staging_dir: PathBuf,
}

pub(super) struct RecoveredSandboxRowFields {
    pub(super) template: TemplateName,
    pub(super) template_hash: Option<ContentHash>,
    pub(super) user: SandboxOwner,
    pub(super) runtime: RuntimeKind,
    pub(super) snapshots_enabled: bool,
    pub(super) rootfs_size_mb: Option<u32>,
    pub(super) disk_io_limits: pplx_space_common_core::types::DiskIoLimits,
    // The artifact marker carries the immutable encryption setting across nodes.
    pub(super) volume_encryption: bool,
    pub(super) process_env: std::collections::BTreeMap<String, String>,
    pub(super) auto_lifecycle: pplx_space_common_core::types::AutoLifecyclePolicy,
}

impl RecoveredSandboxRowFields {
    pub(super) fn with_insert_params<R>(
        &self,
        id: &str,
        uuid8: &SandboxRuntimeLabel,
        state: SandboxState,
        rootfs_size_fallback_mb: Option<u32>,
        bind: impl FnOnce(crate::db::InsertSandboxParams<'_>) -> R,
    ) -> R {
        let template_hash = self.template_hash.map(|hash| hash.to_string());
        bind(crate::db::InsertSandboxParams {
            guest_image: Default::default(),
            id,
            uuid8,
            state,
            template: self.template.as_str(),
            template_hash: template_hash.as_deref(),
            user: &self.user,
            runtime: self.runtime.alias(),
            snapshots_enabled: self.snapshots_enabled,
            rootfs_size_mb: self.rootfs_size_mb.or(rootfs_size_fallback_mb),
            disk_io_limits: self.disk_io_limits,
            pool_template: None,
            volume_encryption: self.volume_encryption,
            process_env: &self.process_env,
            auto_lifecycle: &self.auto_lifecycle,
            network_policy_override: None,
            network_policy_generation: 0,
            network_policy_authority: crate::db::PolicyAuthority::Cluster,
            auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
        })
    }
}

pub(super) struct ValidatedSuspendSnapshotArtifact {
    pub(super) execution: ExecutionMetadata,
    pub(super) row: RecoveredSandboxRowFields,
    pub(super) local_artifact_id: LocalArtifactId,
    pub(super) snapshot_dir: PathBuf,
}

pub(super) struct ValidatedSuspendSnapshotMetadata {
    metadata: crate::snapshot_upload::SnapshotMeta,
    pub(super) execution: ExecutionMetadata,
    pub(super) row: RecoveredSandboxRowFields,
    pub(super) local_artifact_id: LocalArtifactId,
    pub(super) snapshot_dir: PathBuf,
}

impl ValidatedSuspendSnapshotMetadata {
    /// Local structural validation of this artifact's files; see
    /// [`validate_suspend_snapshot_artifact_files`].
    pub(super) fn validate_artifact_files(
        &self,
        verdicts: &RetainedSourceVerdicts,
    ) -> Result<(), SuspendSnapshotValidationError> {
        validate_suspend_snapshot_artifact_files(&self.metadata, &self.snapshot_dir, verdicts)
    }

    pub(super) fn into_artifact(self) -> ValidatedSuspendSnapshotArtifact {
        ValidatedSuspendSnapshotArtifact {
            execution: self.execution,
            row: self.row,
            local_artifact_id: self.local_artifact_id,
            snapshot_dir: self.snapshot_dir,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SuspendSnapshotPublicationDecision {
    CanonicalReady(CanonicalSuspendSnapshotArtifact),
    StagedComplete(StagedSuspendSnapshotArtifact),
    RuntimeLiveWithoutUsableArtifact,
    RuntimeGoneWithoutUsableArtifact,
}

#[derive(Debug, thiserror::Error)]
pub enum SuspendSnapshotValidationError {
    #[error("read snapshot metadata from {snapshot_dir} failed: {source}")]
    ReadMetadata {
        snapshot_dir: String,
        #[source]
        source: std::io::Error,
    },
    #[error("parse snapshot metadata from {snapshot_dir} failed: {source}")]
    ParseMetadata {
        snapshot_dir: String,
        #[source]
        source: serde_json::Error,
    },
    #[error(
        "snapshot metadata in {snapshot_dir} names sandbox {actual_sandbox_id} instead of {expected_sandbox_id}"
    )]
    SandboxIdentityMismatch {
        snapshot_dir: String,
        expected_sandbox_id: SandboxId,
        actual_sandbox_id: String,
    },
    #[error("snapshot directory {snapshot_dir} has no local artifact id")]
    MissingArtifactIdentity { snapshot_dir: String },
    #[error("snapshot directory {snapshot_dir} is not a full suspend artifact")]
    ArtifactShapeMismatch { snapshot_dir: String },
    #[error("invalid recovered sandbox row fields in {snapshot_dir}: {source}")]
    InvalidRecoveredRowFields {
        snapshot_dir: String,
        #[source]
        source: RecoveredSandboxRowFieldsError,
    },
    #[error("snapshot directory {snapshot_dir} is missing required artifact file {artifact}")]
    MissingArtifact {
        snapshot_dir: String,
        artifact: &'static str,
    },
    #[error("stat suspend artifact {path} failed: {source}")]
    StatArtifact {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("suspend artifact {path} is not a regular file")]
    ArtifactNotRegular { path: PathBuf },
    #[error("suspend artifact size overflowed u64")]
    ArtifactSizeOverflow,
    #[error(
        "snapshot metadata in {snapshot_dir} declares {declared_bytes} bytes but required files contain {actual_bytes}"
    )]
    ArtifactSizeMismatch {
        snapshot_dir: String,
        declared_bytes: u64,
        actual_bytes: u64,
    },
    #[error(transparent)]
    HostArchitecture(#[from] super::tail_support::RuntimeArtifactReadError),
    #[error("invalid snapshot execution metadata in {snapshot_dir}: {source}")]
    InvalidExecutionMetadata {
        snapshot_dir: String,
        #[source]
        source: crate::snapshot_upload::SnapshotRuntimeMetadataError,
    },
    #[error(
        "snapshot source runtime {declared_runtime} disagrees with execution runtime {execution_runtime}"
    )]
    RuntimeMismatch {
        declared_runtime: RuntimeKind,
        execution_runtime: RuntimeKind,
    },
    #[error("retained send source {source_id} in {snapshot_dir} is unusable: {reason}")]
    RetainedSourceInvalid {
        snapshot_dir: String,
        source_id: uuid::Uuid,
        reason: String,
    },
    #[error("retained send source {source_id} in {snapshot_dir} is not yet verified: {reason}")]
    RetainedSourcePending {
        snapshot_dir: String,
        source_id: uuid::Uuid,
        reason: String,
    },
}

impl SuspendSnapshotValidationError {
    /// Whether the failure proves the artifact unusable, rather than merely
    /// unverified. Exhaustive so that a new variant must choose.
    pub(super) fn is_unsafe_evidence(&self) -> bool {
        match self {
            Self::ReadMetadata { source, .. } | Self::StatArtifact { source, .. } => {
                source.kind() == std::io::ErrorKind::NotFound
            }
            Self::HostArchitecture(_) | Self::RetainedSourcePending { .. } => false,
            Self::ParseMetadata { .. }
            | Self::SandboxIdentityMismatch { .. }
            | Self::MissingArtifactIdentity { .. }
            | Self::ArtifactShapeMismatch { .. }
            | Self::InvalidRecoveredRowFields { .. }
            | Self::MissingArtifact { .. }
            | Self::ArtifactNotRegular { .. }
            | Self::ArtifactSizeOverflow
            | Self::ArtifactSizeMismatch { .. }
            | Self::InvalidExecutionMetadata { .. }
            | Self::RuntimeMismatch { .. }
            | Self::RetainedSourceInvalid { .. } => true,
        }
    }

    /// Awaiting node-manager.
    pub(super) fn is_pending(&self) -> bool {
        matches!(self, Self::RetainedSourcePending { .. })
    }

    /// No conclusion about the artifact: awaiting node-manager, or a read
    /// that failed for a reason other than absence. A lifecycle decision must
    /// retry rather than skip the artifact, or it could conclude that none is
    /// usable. A host-architecture mismatch is a conclusion, not uncertainty.
    /// Exhaustive so that a new variant must choose.
    pub(super) fn is_inconclusive(&self) -> bool {
        match self {
            Self::RetainedSourcePending { .. } => true,
            Self::ReadMetadata { source, .. } | Self::StatArtifact { source, .. } => {
                source.kind() != std::io::ErrorKind::NotFound
            }
            Self::HostArchitecture(_)
            | Self::ParseMetadata { .. }
            | Self::SandboxIdentityMismatch { .. }
            | Self::MissingArtifactIdentity { .. }
            | Self::ArtifactShapeMismatch { .. }
            | Self::InvalidRecoveredRowFields { .. }
            | Self::MissingArtifact { .. }
            | Self::ArtifactNotRegular { .. }
            | Self::ArtifactSizeOverflow
            | Self::ArtifactSizeMismatch { .. }
            | Self::InvalidExecutionMetadata { .. }
            | Self::RuntimeMismatch { .. }
            | Self::RetainedSourceInvalid { .. } => false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SuspendSnapshotPublicationError {
    #[error(transparent)]
    Validation(#[from] SuspendSnapshotValidationError),
    #[error("snapshot directory {snapshot_dir} has no parent")]
    MissingParent { snapshot_dir: String },
    #[error("create snapshot parent {parent} failed: {source}")]
    CreateParent {
        parent: String,
        #[source]
        source: std::io::Error,
    },
    #[error("retire existing snapshot directory {snapshot_dir} failed: {source}")]
    RetireExisting {
        snapshot_dir: String,
        #[source]
        source: std::io::Error,
    },
    #[error("snapshot directory {snapshot_dir} is not the expected artifact {local_artifact_id}")]
    ArtifactIdMismatch {
        snapshot_dir: String,
        local_artifact_id: LocalArtifactId,
    },
    #[error("staged snapshot directory {staging_dir} is missing")]
    MissingStaged { staging_dir: String },
    #[error("list suspend staging parent {staging_parent} failed: {source}")]
    ListStagingParent {
        staging_parent: String,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "retired snapshot {retired_dir} remains beside a canonical snapshot of another artifact"
    )]
    RetiredResidue { retired_dir: String },
    #[error("retire duplicate staged snapshot directory {staging_dir} failed: {source}")]
    RetireDuplicateStaged {
        staging_dir: String,
        #[source]
        source: std::io::Error,
    },
    #[error("publish staged snapshot {staging_dir} as {snapshot_dir} failed: {source}")]
    PublishStaged {
        staging_dir: String,
        snapshot_dir: String,
        #[source]
        source: std::io::Error,
    },
    #[error("encrypted capture in {staging_dir} is waiting for its volume to seal")]
    EncryptedSealPending { staging_dir: String },
    #[error("access encrypted capture record in {staging_dir} failed: {source}")]
    EncryptedCaptureRecord {
        staging_dir: String,
        #[source]
        source: std::io::Error,
    },
}

impl SuspendSnapshotPublicationError {
    /// The artifact exists but a step owned elsewhere has not finished.
    pub(super) fn is_pending(&self) -> bool {
        match self {
            Self::EncryptedSealPending { .. } => true,
            Self::Validation(error) => error.is_pending(),
            _ => false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum CanonicalSuspendSnapshotRecoveryError {
    #[error("interrupted suspend publication staging remains at {staging_dir}")]
    StagingResidue { staging_dir: String },
    #[error(transparent)]
    Publication(#[from] SuspendSnapshotPublicationError),
    #[error(transparent)]
    Validation(#[from] SuspendSnapshotValidationError),
}

impl CanonicalSuspendSnapshotRecoveryError {
    pub(super) fn is_unsafe_evidence(&self) -> bool {
        match self {
            Self::StagingResidue { .. } => true,
            Self::Publication(_) => false,
            Self::Validation(error) => error.is_unsafe_evidence(),
        }
    }
}

fn path_label(path: &Path) -> String {
    path.display().to_string()
}

fn suspended_snapshot_dir_metadata(
    snapshot_dir: &Path,
) -> Result<crate::snapshot_upload::SnapshotMeta, SuspendSnapshotValidationError> {
    let bytes =
        std::fs::read(snapshot_dir.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME))
            .map_err(|source| SuspendSnapshotValidationError::ReadMetadata {
                snapshot_dir: path_label(snapshot_dir),
                source,
            })?;
    serde_json::from_slice(&bytes).map_err(|source| SuspendSnapshotValidationError::ParseMetadata {
        snapshot_dir: path_label(snapshot_dir),
        source,
    })
}

pub(super) fn recovered_sandbox_row_fields(
    metadata: &crate::snapshot_upload::SnapshotMeta,
) -> Result<RecoveredSandboxRowFields, RecoveredSandboxRowFieldsError> {
    let (user, snapshots_enabled, process_env, auto_lifecycle) = match metadata.sandbox.as_ref() {
        Some(sandbox) => (
            SandboxOwner::parse(sandbox.owner.clone())?,
            sandbox.snapshots_enabled,
            sandbox.process_env.clone(),
            sandbox.auto_lifecycle.clone(),
        ),
        None => (
            SandboxOwner::parse(DEFAULT_GUEST_USER)?,
            true,
            std::collections::BTreeMap::new(),
            pplx_space_common_core::types::AutoLifecyclePolicy::disabled(),
        ),
    };
    Ok(RecoveredSandboxRowFields {
        template: TemplateName::parse(metadata.source.template_id.clone())?,
        template_hash: metadata
            .source
            .content_hash
            .as_deref()
            .map(str::parse)
            .transpose()?,
        user,
        runtime: RuntimeKind::parse(&metadata.source.runtime)?,
        snapshots_enabled,
        rootfs_size_mb: metadata.source.rootfs_size_mib,
        disk_io_limits: pplx_space_common_core::types::DiskIoLimits::try_from_optional(
            metadata.source.disk_io_max_operations_per_sec,
            metadata.source.disk_io_max_bandwidth_mib_per_sec,
        )?,
        volume_encryption: metadata.encrypted,
        process_env,
        auto_lifecycle,
    })
}

pub(super) fn validate_suspend_snapshot_metadata(
    snapshot_dir: &Path,
    expected_sandbox_id: Option<SandboxId>,
) -> Result<ValidatedSuspendSnapshotMetadata, SuspendSnapshotValidationError> {
    let metadata = suspended_snapshot_dir_metadata(snapshot_dir)?;
    if let Some(expected_sandbox_id) = expected_sandbox_id
        && metadata.sandbox_id != expected_sandbox_id.to_string()
    {
        return Err(SuspendSnapshotValidationError::SandboxIdentityMismatch {
            snapshot_dir: path_label(snapshot_dir),
            expected_sandbox_id,
            actual_sandbox_id: metadata.sandbox_id,
        });
    }
    let local_artifact_id = metadata.local_artifact_id.ok_or_else(|| {
        SuspendSnapshotValidationError::MissingArtifactIdentity {
            snapshot_dir: path_label(snapshot_dir),
        }
    })?;
    if metadata.artifact_kind != Some(ArtifactKind::Full)
        || metadata.artifact_origin != Some(ArtifactOrigin::Suspend)
    {
        return Err(SuspendSnapshotValidationError::ArtifactShapeMismatch {
            snapshot_dir: path_label(snapshot_dir),
        });
    }
    let row = recovered_sandbox_row_fields(&metadata).map_err(|source| {
        SuspendSnapshotValidationError::InvalidRecoveredRowFields {
            snapshot_dir: path_label(snapshot_dir),
            source,
        }
    })?;

    let execution = metadata
        .execution_metadata(super::tail_support::host_architecture()?)
        .map_err(
            |source| SuspendSnapshotValidationError::InvalidExecutionMetadata {
                snapshot_dir: path_label(snapshot_dir),
                source,
            },
        )?
        .into_execution();
    if row.runtime != execution.runtime().kind {
        return Err(SuspendSnapshotValidationError::RuntimeMismatch {
            declared_runtime: row.runtime,
            execution_runtime: execution.runtime().kind,
        });
    }

    Ok(ValidatedSuspendSnapshotMetadata {
        metadata,
        execution,
        row,
        local_artifact_id,
        snapshot_dir: snapshot_dir.to_path_buf(),
    })
}

/// Local structural validation. A retained send source takes its verdict from
/// `verdicts`, gathered beforehand by the async caller; without one the
/// artifact is pending, not incomplete.
pub(super) fn validate_suspend_snapshot_artifact_files(
    metadata: &crate::snapshot_upload::SnapshotMeta,
    snapshot_dir: &Path,
    verdicts: &RetainedSourceVerdicts,
) -> Result<(), SuspendSnapshotValidationError> {
    let artifacts = if metadata.encrypted {
        crate::snapshot_upload::encrypted_vm_artifacts()
    } else {
        crate::snapshot_upload::vm_artifacts_for_format(crate::snapshot_upload::format_from_meta(
            metadata,
        ))
    };
    let mut actual_bytes = 0_u64;
    // A stream beside a retained source: the sidecar may count either, so a
    // mismatch on the stream is rechecked against the retained image below.
    let mut cached_stream: Option<(RetainedSource, u64)> = None;
    for artifact in artifacts {
        if artifact.kind == pplx_space_common_core::types::SnapshotArtifact::RootfsBtrfsSend {
            match RootfsArtifact::resolve_meta(metadata, snapshot_dir).map_err(|error| {
                SuspendSnapshotValidationError::StatArtifact {
                    path: error.path,
                    source: error.source,
                }
            })? {
                Some(RootfsArtifact::RetainedSource(source)) => {
                    let rootfs_bytes = retained_rootfs_bytes(snapshot_dir, &source, verdicts)?;
                    actual_bytes = actual_bytes
                        .checked_add(rootfs_bytes)
                        .ok_or(SuspendSnapshotValidationError::ArtifactSizeOverflow)?;
                    continue;
                }
                Some(RootfsArtifact::SendFile { .. }) => {
                    if let Some(source_id) = metadata.rootfs_send_source_id {
                        cached_stream = Some((
                            RetainedSource {
                                sandbox_id: metadata.sandbox_id.clone(),
                                artifact_dir: snapshot_dir.to_path_buf(),
                                source_id,
                            },
                            0,
                        ));
                    }
                }
                None => {}
            }
        }
        let path = snapshot_dir.join(artifact.local_name);
        let file = match std::fs::symlink_metadata(&path) {
            Ok(file) => file,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                return Err(SuspendSnapshotValidationError::MissingArtifact {
                    snapshot_dir: path_label(snapshot_dir),
                    artifact: artifact.local_name,
                });
            }
            Err(source) => {
                return Err(SuspendSnapshotValidationError::StatArtifact { path, source });
            }
        };
        if !file.file_type().is_file() {
            return Err(SuspendSnapshotValidationError::ArtifactNotRegular { path });
        }
        if artifact.kind == pplx_space_common_core::types::SnapshotArtifact::RootfsBtrfsSend
            && let Some((_, stream_len)) = cached_stream.as_mut()
        {
            *stream_len = file.len();
        }
        actual_bytes = actual_bytes
            .checked_add(file.len())
            .ok_or(SuspendSnapshotValidationError::ArtifactSizeOverflow)?;
    }
    if actual_bytes != metadata.size_bytes {
        // Reclaim rewrites the sidecar to count the retained image before it
        // removes the stream, and older sidecars count the stream while it
        // exists. Both are verified quantities for the same artifact.
        if let Some((source, stream_len)) = cached_stream {
            let image = retained_rootfs_bytes(snapshot_dir, &source, verdicts)?;
            let by_image = actual_bytes
                .checked_sub(stream_len)
                .and_then(|rest| rest.checked_add(image))
                .ok_or(SuspendSnapshotValidationError::ArtifactSizeOverflow)?;
            if by_image == metadata.size_bytes {
                return Ok(());
            }
        }
        return Err(SuspendSnapshotValidationError::ArtifactSizeMismatch {
            snapshot_dir: path_label(snapshot_dir),
            declared_bytes: metadata.size_bytes,
            actual_bytes,
        });
    }

    Ok(())
}

fn retained_rootfs_bytes(
    snapshot_dir: &Path,
    source: &RetainedSource,
    verdicts: &RetainedSourceVerdicts,
) -> Result<u64, SuspendSnapshotValidationError> {
    match verdicts.check(source) {
        RetainedSourceCheck::Present { rootfs_bytes } => Ok(rootfs_bytes),
        RetainedSourceCheck::Absent => {
            // Node-manager reports a bundle without its manifest as invalid,
            // so absent means it found no bundle at this path. A bundle entry
            // here now means the answer predates a rename that brought this
            // artifact back, from a publication hold or staging: pending
            // another inspection, not missing. The entry is readable without
            // traversing the bundle, and only its definite absence is
            // evidence.
            let bundle = snapshot_dir.join(crate::retained_source::BUNDLE_DIRECTORY);
            match bundle.symlink_metadata() {
                Ok(_) => Err(SuspendSnapshotValidationError::RetainedSourcePending {
                    snapshot_dir: path_label(snapshot_dir),
                    source_id: source.source_id,
                    reason: "reported absent while its bundle directory exists; re-inspect"
                        .to_owned(),
                }),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    Err(SuspendSnapshotValidationError::MissingArtifact {
                        snapshot_dir: path_label(snapshot_dir),
                        artifact: crate::retained_source::BUNDLE_DIRECTORY,
                    })
                }
                Err(source) => Err(SuspendSnapshotValidationError::StatArtifact {
                    path: bundle,
                    source,
                }),
            }
        }
        RetainedSourceCheck::Invalid { reason } => {
            Err(SuspendSnapshotValidationError::RetainedSourceInvalid {
                snapshot_dir: path_label(snapshot_dir),
                source_id: source.source_id,
                reason,
            })
        }
        RetainedSourceCheck::Unavailable { reason } => {
            Err(SuspendSnapshotValidationError::RetainedSourcePending {
                snapshot_dir: path_label(snapshot_dir),
                source_id: source.source_id,
                reason,
            })
        }
    }
}

/// Retained sources named by the canonical and staged suspend artifacts in
/// `sandbox_dir`, for an async caller to inspect before validating. Unreadable
/// metadata is skipped here; validation reports it.
pub(super) fn retained_sources_in_sandbox_dir(sandbox_dir: &Path) -> Vec<RetainedSource> {
    retained_sources_in_snapshot_dirs(suspend_snapshot_dirs_in(sandbox_dir))
}

/// The retained sources the canonical artifact alone needs inspected, for a
/// caller that validates only it; staging is another owner's concern.
pub(super) fn retained_sources_in_canonical_snapshot(sandbox_dir: &Path) -> Vec<RetainedSource> {
    retained_sources_in_snapshot_dirs(vec![canonical_suspend_snapshot_dir(sandbox_dir)])
}

/// The canonical snapshot directory and every staging directory of
/// `sandbox_dir`, existing or not.
fn suspend_snapshot_dirs_in(sandbox_dir: &Path) -> Vec<PathBuf> {
    let mut directories = vec![canonical_suspend_snapshot_dir(sandbox_dir)];
    if let Ok(entries) = std::fs::read_dir(suspend_snapshot_staging_parent_dir(sandbox_dir)) {
        directories.extend(
            entries
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .filter(|path| {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.starts_with(SUSPEND_STAGING_PREFIX))
                }),
        );
    }
    directories
}

fn retained_sources_in_snapshot_dirs(directories: Vec<PathBuf>) -> Vec<RetainedSource> {
    directories
        .into_iter()
        .filter_map(|directory| {
            let metadata = suspended_snapshot_dir_metadata(&directory).ok()?;
            if metadata.encrypted
                || crate::snapshot_upload::format_from_meta(&metadata)
                    != pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::BtrfsSend
            {
                return None;
            }
            // Exactly the sources the validator needs: without verdicts it
            // reports those, and only those, as pending. That covers a
            // deferred artifact and one whose sidecar already counts the
            // image while its stream still exists; a stream the sidecar
            // counts needs no inspection.
            let source_id = metadata.rootfs_send_source_id?;
            match validate_suspend_snapshot_artifact_files(
                &metadata,
                &directory,
                &RetainedSourceVerdicts::default(),
            ) {
                Err(SuspendSnapshotValidationError::RetainedSourcePending { .. }) => {
                    Some(RetainedSource {
                        sandbox_id: metadata.sandbox_id.clone(),
                        artifact_dir: directory,
                        source_id,
                    })
                }
                _ => None,
            }
        })
        .collect()
}

pub(super) fn validate_recoverable_suspend_snapshot_dir(
    snapshot_dir: &Path,
    expected_sandbox_id: Option<SandboxId>,
    verdicts: &RetainedSourceVerdicts,
) -> Result<ValidatedSuspendSnapshotArtifact, SuspendSnapshotValidationError> {
    let metadata = validate_suspend_snapshot_metadata(snapshot_dir, expected_sandbox_id)?;
    metadata.validate_artifact_files(verdicts)?;
    Ok(metadata.into_artifact())
}

/// Validate the one canonical artifact that may safely anchor a recovery.
/// Staging residue is deliberately rejected: it represents an unresolved
/// publication generation, so choosing either directory would destroy evidence.
pub(super) fn validate_canonical_suspend_snapshot_for_recovery(
    cache_dir: &Path,
    sandbox_id: SandboxId,
    verdicts: &RetainedSourceVerdicts,
) -> Result<ValidatedSuspendSnapshotArtifact, CanonicalSuspendSnapshotRecoveryError> {
    let sandbox_dir = cache_dir.join(sandbox_id.to_string());
    if let Some(staging_dir) = find_suspend_snapshot_staging_residue(&sandbox_dir)? {
        return Err(CanonicalSuspendSnapshotRecoveryError::StagingResidue {
            staging_dir: path_label(&staging_dir),
        });
    }
    let snapshot_dir = canonical_suspend_snapshot_dir(&sandbox_dir);
    Ok(validate_recoverable_suspend_snapshot_dir(
        &snapshot_dir,
        Some(sandbox_id),
        verdicts,
    )?)
}

pub(super) fn validate_complete_suspend_snapshot_dir(
    snapshot_dir: &Path,
    verdicts: &RetainedSourceVerdicts,
) -> Result<LocalArtifactId, SuspendSnapshotValidationError> {
    Ok(validate_recoverable_suspend_snapshot_dir(snapshot_dir, None, verdicts)?.local_artifact_id)
}

pub(super) fn ensure_complete_suspend_snapshot_dir(
    snapshot_dir: &Path,
    local_artifact_id: LocalArtifactId,
    verdicts: &RetainedSourceVerdicts,
) -> Result<(), SuspendSnapshotPublicationError> {
    let actual = validate_complete_suspend_snapshot_dir(snapshot_dir, verdicts)?;
    if actual != local_artifact_id {
        return Err(SuspendSnapshotPublicationError::ArtifactIdMismatch {
            snapshot_dir: path_label(snapshot_dir),
            local_artifact_id,
        });
    }
    Ok(())
}

/// A superseded snapshot waits here, outside the staging sweep's garbage lane,
/// until its replacement is committed. Only publication and recovery read it.
pub(super) const RETIRED_HOLD_PREFIX: &str = ".suspend-retired-";

/// The hold for the snapshot a publication of `local_artifact_id` replaces.
pub(super) fn retired_suspend_snapshot_dir(
    sandbox_dir: &Path,
    local_artifact_id: LocalArtifactId,
) -> PathBuf {
    suspend_snapshot_staging_parent_dir(sandbox_dir)
        .join(format!("{RETIRED_HOLD_PREFIX}{local_artifact_id}"))
}

/// A fresh name in the staging sweep's lane. Deferred capture leaves read-only
/// subvolumes inside retired snapshots that this service cannot delete; the
/// sweep removes them through node-manager.
pub(super) fn garbage_suspend_snapshot_dir(sandbox_dir: &Path) -> PathBuf {
    suspend_snapshot_staging_parent_dir(sandbox_dir).join(format!(
        "{}{}",
        super::recovery::staging_cleanup::RETIRED_PREFIX,
        LocalArtifactId::new()
    ))
}

/// Hand a hold to the sweep once the publication it belonged to is committed.
pub(super) fn release_retired_suspend_snapshot(
    sandbox_dir: &Path,
    hold: &Path,
) -> std::io::Result<Option<PathBuf>> {
    let garbage = garbage_suspend_snapshot_dir(sandbox_dir);
    match std::fs::rename(hold, &garbage) {
        Ok(()) => Ok(Some(garbage)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Why a staged capture wasn't published by
/// [`publish_recorded_suspend_snapshot_dir`].
#[derive(Debug, thiserror::Error)]
pub(super) enum RecordedPublicationError {
    #[error("recording the capture's execution failed: {0}")]
    Record(#[source] std::io::Error),
    #[error(transparent)]
    Publish(#[from] SuspendSnapshotPublicationError),
}

/// Publish a staged capture as [`publish_suspend_snapshot_dir`] does. A
/// teardown suspend's capture, the one with an `execution_generation`, first
/// records the execution that took it ([`crate::capture_record`]) in its
/// staging directory, so the record is published with the bytes and lasts as
/// long as they do. A capture whose record can't be written isn't published.
pub(super) fn publish_recorded_suspend_snapshot_dir(
    staging_dir: &Path,
    snapshot_dir: &Path,
    local_artifact_id: LocalArtifactId,
    execution_generation: Option<ExecutionGeneration>,
    verdicts: &RetainedSourceVerdicts,
) -> Result<Vec<PathBuf>, RecordedPublicationError> {
    if let Some(execution_generation) = execution_generation {
        crate::capture_record::write(staging_dir, local_artifact_id, execution_generation)
            .map_err(RecordedPublicationError::Record)?;
    }
    Ok(publish_suspend_snapshot_dir(
        staging_dir,
        snapshot_dir,
        local_artifact_id,
        verdicts,
    )?)
}

/// Publish `staging_dir` as the canonical snapshot. Returns the lane entries
/// this publication retired (the displaced snapshot, a duplicate staging
/// directory), already renamed into the staging sweep's lane: the caller
/// reclaims them at once and the sweep is the retry owner.
pub(super) fn publish_suspend_snapshot_dir(
    staging_dir: &Path,
    snapshot_dir: &Path,
    local_artifact_id: LocalArtifactId,
    verdicts: &RetainedSourceVerdicts,
) -> Result<Vec<PathBuf>, SuspendSnapshotPublicationError> {
    // A seal interrupted after its sidecar leaves the record behind; it is
    // not part of the artifact.
    remove_encrypted_capture_record(staging_dir).map_err(|source| {
        SuspendSnapshotPublicationError::EncryptedCaptureRecord {
            staging_dir: path_label(staging_dir),
            source,
        }
    })?;
    let mut garbage = Vec::new();
    let parent =
        snapshot_dir
            .parent()
            .ok_or_else(|| SuspendSnapshotPublicationError::MissingParent {
                snapshot_dir: path_label(snapshot_dir),
            })?;
    let staging_parent = suspend_snapshot_staging_parent_dir(parent);
    std::fs::create_dir_all(&staging_parent).map_err(|source| {
        SuspendSnapshotPublicationError::CreateParent {
            parent: path_label(&staging_parent),
            source,
        }
    })?;

    // A hold exists only while a publication of this artifact is between its
    // two renames or before its release. Resume it rather than stack on it:
    // without a canonical snapshot the replacement never committed, so the
    // held snapshot comes back; with this artifact canonical, it is garbage.
    let hold = retired_suspend_snapshot_dir(parent, local_artifact_id);
    if hold.exists() {
        if !snapshot_dir.exists() {
            std::fs::rename(&hold, snapshot_dir).map_err(|source| {
                SuspendSnapshotPublicationError::RetireExisting {
                    snapshot_dir: path_label(snapshot_dir),
                    source,
                }
            })?;
        } else {
            match validate_complete_suspend_snapshot_dir(snapshot_dir, verdicts) {
                Ok(existing) if existing == local_artifact_id => {}
                // Not checked yet is not residue: the caller retries once
                // node-manager has answered for the canonical artifact.
                Err(error) if error.is_pending() => return Err(error.into()),
                Ok(_) | Err(_) => {
                    return Err(SuspendSnapshotPublicationError::RetiredResidue {
                        retired_dir: path_label(&hold),
                    });
                }
            }
        }
    }

    if snapshot_dir.exists() {
        match validate_complete_suspend_snapshot_dir(snapshot_dir, verdicts) {
            Ok(existing_artifact_id) if existing_artifact_id == local_artifact_id => {
                garbage.extend(release_retired_suspend_snapshot(parent, &hold).map_err(
                    |source| SuspendSnapshotPublicationError::RetireExisting {
                        snapshot_dir: path_label(&hold),
                        source,
                    },
                )?);
                if staging_dir.exists() {
                    ensure_complete_suspend_snapshot_dir(staging_dir, local_artifact_id, verdicts)?;
                    let duplicate = garbage_suspend_snapshot_dir(parent);
                    std::fs::rename(staging_dir, &duplicate).map_err(|source| {
                        SuspendSnapshotPublicationError::RetireDuplicateStaged {
                            staging_dir: path_label(staging_dir),
                            source,
                        }
                    })?;
                    garbage.push(duplicate);
                }
                return Ok(garbage);
            }
            Ok(_) | Err(_) => {}
        }
    }

    if !staging_dir.exists() {
        return Err(SuspendSnapshotPublicationError::MissingStaged {
            staging_dir: path_label(staging_dir),
        });
    }
    ensure_complete_suspend_snapshot_dir(staging_dir, local_artifact_id, verdicts)?;

    let retired_existing = match std::fs::rename(snapshot_dir, &hold) {
        Ok(()) => true,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
        Err(err) => {
            return Err(SuspendSnapshotPublicationError::RetireExisting {
                snapshot_dir: path_label(snapshot_dir),
                source: err,
            });
        }
    };

    if let Err(err) = std::fs::rename(staging_dir, snapshot_dir) {
        if retired_existing && let Err(restore_err) = std::fs::rename(&hold, snapshot_dir) {
            tracing::error!(
                old_snapshot_dir = %hold.display(),
                snapshot_dir = %snapshot_dir.display(),
                error = %restore_err,
                "failed to restore prior local snapshot after publication error"
            );
        }
        return Err(SuspendSnapshotPublicationError::PublishStaged {
            staging_dir: path_label(staging_dir),
            snapshot_dir: path_label(snapshot_dir),
            source: err,
        });
    }

    // Committed. The hold is garbage now; the sweep also releases a hold
    // whose artifact is canonical, so a failure here only delays reclamation.
    if retired_existing {
        match release_retired_suspend_snapshot(parent, &hold) {
            Ok(released) => garbage.extend(released),
            Err(err) => tracing::warn!(
                retired_snapshot_dir = %hold.display(),
                error = %err,
                "failed to release retired local snapshot after publication"
            ),
        }
    }
    Ok(garbage)
}

pub(super) fn find_complete_suspend_snapshot_staging_dir(
    sandbox_dir: &Path,
    verdicts: &RetainedSourceVerdicts,
) -> Result<Option<StagedSuspendSnapshotArtifact>, SuspendSnapshotPublicationError> {
    let staging_parent = suspend_snapshot_staging_parent_dir(sandbox_dir);
    let entries = match std::fs::read_dir(&staging_parent) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(SuspendSnapshotPublicationError::ListStagingParent {
                staging_parent: path_label(&staging_parent),
                source,
            });
        }
    };
    let mut paths = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(SUSPEND_STAGING_PREFIX))
        })
        .collect::<Vec<_>>();
    paths.sort();
    let now = std::time::SystemTime::now();
    let mut awaiting_seal = None;
    for path in paths {
        match validate_complete_suspend_snapshot_dir(&path, verdicts) {
            Ok(local_artifact_id) => {
                return Ok(Some(StagedSuspendSnapshotArtifact {
                    local_artifact_id,
                    staging_dir: path,
                }));
            }
            // Skipping an unverified staging generation could pick a sibling
            // or conclude that no usable artifact exists.
            Err(error) if error.is_inconclusive() => return Err(error.into()),
            // A finished capture whose seal can still succeed is not missing,
            // but a later complete capture supersedes it.
            Err(_) if encrypted_seal_pending(&path, now) => {
                awaiting_seal.get_or_insert(path);
            }
            Err(error) => {
                tracing::warn!(
                    staging_dir = %path.display(),
                    %error,
                    "skipping incomplete suspend publication staging directory"
                );
            }
        }
    }
    match awaiting_seal {
        Some(path) => Err(SuspendSnapshotPublicationError::EncryptedSealPending {
            staging_dir: path_label(&path),
        }),
        None => Ok(None),
    }
}

pub(super) fn find_suspend_snapshot_staging_residue(
    sandbox_dir: &Path,
) -> Result<Option<PathBuf>, SuspendSnapshotPublicationError> {
    let staging_parent = suspend_snapshot_staging_parent_dir(sandbox_dir);
    let entries = match std::fs::read_dir(&staging_parent) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(SuspendSnapshotPublicationError::ListStagingParent {
                staging_parent: path_label(&staging_parent),
                source,
            });
        }
    };
    for entry in entries {
        let path = entry
            .map_err(
                |source| SuspendSnapshotPublicationError::ListStagingParent {
                    staging_parent: path_label(&staging_parent),
                    source,
                },
            )?
            .path();
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(SUSPEND_STAGING_PREFIX))
        {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

/// An inconclusive validation is an error, not a decision: the caller retries
/// once node-manager answers or the filesystem read succeeds.
pub(super) fn classify_suspend_snapshot_publication(
    sandbox_dir: &Path,
    expected_canonical_artifact_id: Option<LocalArtifactId>,
    runtime_live: bool,
    verdicts: &RetainedSourceVerdicts,
) -> Result<SuspendSnapshotPublicationDecision, SuspendSnapshotPublicationError> {
    if runtime_live {
        return Ok(SuspendSnapshotPublicationDecision::RuntimeLiveWithoutUsableArtifact);
    }

    let snapshot_dir = canonical_suspend_snapshot_dir(sandbox_dir);
    if snapshot_dir.exists()
        && let Some(expected) = expected_canonical_artifact_id
    {
        match validate_complete_suspend_snapshot_dir(&snapshot_dir, verdicts) {
            Ok(local_artifact_id) if expected == local_artifact_id => {
                return Ok(SuspendSnapshotPublicationDecision::CanonicalReady(
                    CanonicalSuspendSnapshotArtifact {
                        local_artifact_id,
                        path: snapshot_dir,
                    },
                ));
            }
            Ok(local_artifact_id) => {
                tracing::warn!(
                    snapshot_dir = %snapshot_dir.display(),
                    expected_local_artifact_id = %expected,
                    found_local_artifact_id = %local_artifact_id,
                    "refusing interrupted suspend recovery with mismatched canonical artifact"
                );
            }
            Err(error) if error.is_inconclusive() => return Err(error.into()),
            Err(error) => {
                tracing::warn!(
                    snapshot_dir = %snapshot_dir.display(),
                    %error,
                    "refusing interrupted suspend recovery with invalid canonical artifact"
                );
            }
        }
    }

    if let Some(staged) = find_complete_suspend_snapshot_staging_dir(sandbox_dir, verdicts)? {
        return Ok(SuspendSnapshotPublicationDecision::StagedComplete(staged));
    }

    if expected_canonical_artifact_id.is_none() && snapshot_dir.exists() {
        match validate_complete_suspend_snapshot_dir(&snapshot_dir, verdicts) {
            Ok(local_artifact_id) => {
                return Ok(SuspendSnapshotPublicationDecision::CanonicalReady(
                    CanonicalSuspendSnapshotArtifact {
                        local_artifact_id,
                        path: snapshot_dir,
                    },
                ));
            }
            Err(error) if error.is_inconclusive() => return Err(error.into()),
            Err(error) => {
                tracing::warn!(
                    snapshot_dir = %snapshot_dir.display(),
                    %error,
                    "refusing interrupted suspend recovery with invalid unpinned canonical artifact"
                );
            }
        }
    }

    Ok(SuspendSnapshotPublicationDecision::RuntimeGoneWithoutUsableArtifact)
}

#[cfg(test)]
pub(super) fn write_full_suspend_artifact(
    snapshot_dir: &Path,
    sandbox_id: &str,
    local_artifact_id: LocalArtifactId,
    payload: &[u8],
) -> pplx_space_common_core::types::ExecutionMetadata {
    let architecture = std::env::consts::ARCH.parse().unwrap();
    let execution = pplx_space_common_core::types::SnapshotExecutionMetadata::new(
        pplx_space_common_core::types::ExecutionMetadata::initial_firecracker(architecture, vec![]),
    )
    .unwrap();
    std::fs::create_dir_all(snapshot_dir).unwrap();
    std::fs::write(snapshot_dir.join("vmstate"), payload).unwrap();
    std::fs::write(snapshot_dir.join("memory"), payload).unwrap();
    std::fs::write(snapshot_dir.join("rootfs.ext4"), payload).unwrap();
    let meta = serde_json::json!({
        "version": crate::snapshot_upload::SNAPSHOT_METADATA_VERSION,
        "execution": execution.execution(),
        "sandbox_id": sandbox_id,
        "size_bytes": payload.len() * 3,
        "created_at": "2026-05-25T00:00:00Z",
        "source": {
            "runtime": "fc",
            "template_id": "seed"
        },
        "local_artifact_id": local_artifact_id,
        "artifact_kind": "full",
        "artifact_origin": "suspend"
    });
    std::fs::write(
        snapshot_dir.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME),
        serde_json::to_vec_pretty(&meta).unwrap(),
    )
    .unwrap();
    execution.into_execution()
}

/// Makes a retained bundle untraversable, as the sandbox jail's ownership does
/// for sandbox-manager, and restores access on drop for cleanup.
#[cfg(test)]
pub(super) struct InaccessibleBundle(PathBuf);

#[cfg(test)]
impl InaccessibleBundle {
    fn new(snapshot_dir: &Path) -> Self {
        use std::os::unix::fs::PermissionsExt;
        let bundle = snapshot_dir.join(crate::retained_source::BUNDLE_DIRECTORY);
        std::fs::create_dir_all(bundle.join("source")).unwrap();
        std::fs::write(bundle.join("source/rootfs.ext4"), b"jail-owned rootfs").unwrap();
        std::fs::set_permissions(&bundle, std::fs::Permissions::from_mode(0o000)).unwrap();
        Self(bundle)
    }
}

#[cfg(test)]
impl Drop for InaccessibleBundle {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o750));
    }
}

/// A deferred suspend artifact: VM state, a retained source this process
/// cannot read, and no `rootfs.btrfs-send`.
#[cfg(test)]
pub(super) fn write_deferred_suspend_artifact(
    snapshot_dir: &Path,
    sandbox_id: &str,
    local_artifact_id: LocalArtifactId,
    source_id: uuid::Uuid,
    rootfs_bytes: u64,
) -> InaccessibleBundle {
    write_full_suspend_artifact(snapshot_dir, sandbox_id, local_artifact_id, b"state");
    std::fs::remove_file(snapshot_dir.join("rootfs.ext4")).unwrap();
    let path = snapshot_dir.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME);
    let mut meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    meta["rootfs_diff_format"] = crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1.into();
    meta["rootfs_send_source_id"] = source_id.to_string().into();
    meta["size_bytes"] = (2 * b"state".len() as u64 + rootfs_bytes).into();
    std::fs::write(&path, serde_json::to_vec_pretty(&meta).unwrap()).unwrap();
    InaccessibleBundle::new(snapshot_dir)
}

#[cfg(test)]
mod tests;
