//! Pull suspend-snapshot files from object storage via vol-mgr, leaving
//! the sandbox cache dir in the same shape an in-place suspend produces.
//!
//! After [`prepare_restore_dir`] returns:
//!
//! ```text
//! /space/cache/sandboxes/<sandbox_id>/
//!   instance/
//!     rootfs.ext4                   <- reflinked from snapshot/rootfs.ext4
//!   snapshot/
//!     vmstate                       <- s3://…/suspend_snapshots/<id>/binfile
//!     memory                        <- s3://…/suspend_snapshots/<id>/memfile
//!     rootfs.ext4                   <- s3://…/suspend_snapshots/<id>/rootfs
//!     snapshot.json                 <- s3://…/suspend_snapshots/<id>/metadata.json
//!   snapshots/                      (empty; rolling btrfs snapshots dir)
//! ```
//!
//! The reflink from `snapshot/rootfs.ext4` into `instance/rootfs.ext4` is
//! the signal that `prepare_sandbox` uses to skip the template clone.
//!
//! Local file names match [`crate::snapshot_upload::VM_ARTIFACTS`].
//! Sqlite row repopulation is performed by the caller, not here.

use pplx_space_common_core::types::{JobId, SnapshotId};

use std::path::{Path, PathBuf};
use std::time::Duration;

use futures_util::future::{BoxFuture, join_all};
use pplx_space_common_core::host::linux::paths;
use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, ExactStorageUri, LocalArtifactId, SandboxCleanupEpisodeId,
    SandboxId, SnapshotArtifact,
};
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventType, emit_sandbox_event,
};
use pplx_space_volmgr_client::{
    EnsureFileReplicaError, EnsureFileReplicaWaitConfig, RESTORE_PRIORITY, VOLMGR_SOCKET,
    ensure_file_replica_with_config, proto::PullFileRequest, read_verified_replica_provenance,
    snapshot_artifact_to_proto, verified_replica_provenance_path,
};
use tracing::Instrument;

use crate::snapshot_upload::{SnapshotUploadConfig, build_artifact_uri};

// Vol-mgr workers default to 1800s per transfer attempt and 5 attempts
// (~9000s total). Three hours gives retry/backoff headroom while still
// bounding jobs that never reach a pull lane.
pub(crate) const RESTORE_TRANSFER_WAIT_MAX_TOTAL: Duration = Duration::from_secs(10_800);

const CANONICAL_RESTORE_EXCHANGE_RECEIPT_FILENAME: &str = ".canonical-restore-exchange.json";

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct CanonicalRestoreExchangeReceipt {
    sandbox_id: String,
    transfer_scope_id: String,
    execution_generation: i64,
    activation_attempt_id: String,
    old_source: String,
    replacement_source: String,
}

/// Outcome of a successful [`prepare_restore_dir`] call.
#[derive(Clone, Debug)]
pub struct RestorePrepared {
    /// Directory where the artifacts were placed
    /// (`/space/cache/sandboxes/<id>/snapshot`).
    pub snapshot_dir: PathBuf,
    /// `s3://` base location the artifacts were pulled from. Same shape as
    /// the URI stored in `snapshot_storage_locations`.
    pub storage_uri: String,
    /// `local_artifact_id` recorded inside the restored snapshot metadata.
    /// Legacy snapshots may not carry this field.
    pub local_artifact_id: Option<LocalArtifactId>,
    /// Artifact kind paired with `local_artifact_id` in the snapshot metadata.
    pub artifact_kind: Option<ArtifactKind>,
}

/// Outcome of [`restore_pull_metadata_scoped`] — phase 1 of cross-node restore.
///
/// All values are owned because the caller commits a sandbox row between
/// phase 1 and phase 2 (closing the race against the periodic inventory
/// reconciler) and the borrow checker won't let us hold a `&` across the
/// DB write without inventing a self-referential struct.
#[derive(Debug)]
pub struct RestoreMetadataStaged {
    pub sandbox_dir: PathBuf,
    pub snapshot_dir: PathBuf,
    pub instance_dir: PathBuf,
    pub config: SnapshotUploadConfig,
    pub storage_uri: String,
    pub meta: crate::snapshot_upload::SnapshotMeta,
    pub codec: RestoreCodec,
    pub transfer_scope_id: Option<pplx_space_common_core::types::TransferScopeId>,
    /// Sandbox identity that owns the durable object path. Restore-as-new
    /// stages into the target sandbox's filesystem while credentials remain
    /// scoped to the source artifact owner.
    credential_subject_id: String,
    filesystem_scope_id: Option<pplx_space_common_core::types::TransferScopeId>,
}

#[derive(Clone, Copy)]
struct RestoreTransferIdentity<'a> {
    sandbox_id: &'a str,
    credential_subject_id: &'a str,
}

#[derive(Debug)]
pub(crate) struct PublishedRemoteRestore {
    pub storage_uri: String,
    pub meta: crate::snapshot_upload::SnapshotMeta,
}

#[derive(Debug, thiserror::Error)]
pub enum RecoveredSandboxRowFieldsError {
    #[error("invalid template in snapshot metadata: {0}")]
    Template(#[from] pplx_space_common_core::types::TemplateParseError),
    #[error("invalid runtime in snapshot metadata: {0}")]
    Runtime(#[from] pplx_space_common_core::types::RuntimeKindParseError),
    #[error("invalid template content hash in snapshot metadata: {0}")]
    ContentHash(#[from] pplx_space_common_core::types::DigestParseError),
    #[error("invalid sandbox owner in snapshot metadata: {0}")]
    Owner(#[from] pplx_space_common_core::types::AuthParseError),
    #[error("invalid disk I/O limits in snapshot metadata: {0}")]
    DiskIoLimits(#[from] pplx_space_common_core::types::DiskIoLimitsError),
}

/// Errors returned from [`prepare_restore_dir`].
#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    /// Cluster API is not configured on this node — restore-from-S3 can't
    /// run. Falls through to normal `NotFound` handling for the caller.
    #[error("cluster-api not configured: {0}")]
    ClusterApiUnconfigured(String),
    /// Cluster API has no suspend snapshot recorded for this sandbox. The
    /// caller should surface this as a normal `NotFound`.
    #[error("no suspend snapshot registered for sandbox {0}")]
    SnapshotNotRegistered(String),
    /// Restore lock admission or volume-manager refused new work.
    #[error("{0}")]
    AdmissionRejected(#[source] tonic::Status),
    /// Another operation already owns this sandbox's cleanup or restore
    /// serialization.
    #[error("sandbox {sandbox_id} cleanup or restore operation is already in progress; retry")]
    RestoreOperationBusy { sandbox_id: SandboxId },
    #[error("snapshot metadata names sandbox {actual} instead of requested sandbox {expected}")]
    SnapshotIdentityMismatch { expected: SandboxId, actual: String },
    #[error("invalid sandbox ID {value:?}: {source}")]
    InvalidSandboxId {
        value: String,
        #[source]
        source: pplx_space_common_core::types::SandboxIdParseError,
    },
    #[error("invalid restore source URI {uri}: {source}")]
    InvalidRestoreSource {
        uri: String,
        #[source]
        source: pplx_space_common_core::types::StorageUriParseError,
    },
    #[error("artifact snapshot {expected} does not match storage URI {storage_uri}")]
    ArtifactSnapshotUriMismatch {
        expected: SnapshotId,
        storage_uri: String,
    },
    #[error("artifact source sandbox {expected} does not match storage URI {storage_uri}")]
    ArtifactSourceUriMismatch {
        expected: SandboxId,
        storage_uri: String,
    },
    /// A `WAKE_TARGET_STATE_STOPPED` restore staged metadata that does not
    /// describe an evacuated stopped sandbox's disk image. Carries all three
    /// observed fields rather than only the one that failed, because the usual
    /// cause is that the caller selected the wrong artifact entirely and the
    /// other two say which one it did select.
    #[error(
        "materializing a stopped sandbox requires a stop-origin disk artifact naming its local \
         artifact; staged metadata has kind {kind:?}, origin {origin:?}, local artifact \
         {local_artifact_id:?}"
    )]
    StoppedMaterializationArtifactUnsuitable {
        kind: Option<ArtifactKind>,
        origin: Option<ArtifactOrigin>,
        local_artifact_id: Option<LocalArtifactId>,
    },
    /// The staged artifact is a well-formed evacuated disk, but not the one the
    /// caller said this URI resolves to. Caught before the rootfs transfer and
    /// before the row is committed: no rootfs is pulled and no row is written.
    /// On a fresh materialization the metadata pull's transfer scope has no row
    /// and is left for the rowless-scope sweep.
    #[error(
        "materializing a stopped sandbox expected artifact {expected} but the staged metadata \
         names {staged}"
    )]
    StoppedMaterializationArtifactMismatch {
        expected: LocalArtifactId,
        staged: LocalArtifactId,
    },
    #[error("artifact snapshot is missing sandbox ownership metadata")]
    MissingSandboxOwnershipMetadata,
    #[error("restored rootfs capacity is not permitted: {0}")]
    RootfsPolicy(#[source] pplx_space_common_core::sandbox_storage::RootfsSizeError),
    #[error("create directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("remove file {path}: {source}")]
    RemoveFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("remove directory {path}: {source}")]
    RemoveDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("rename {from} to {to}: {source}")]
    RenamePath {
        from: PathBuf,
        to: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("read sandbox row before remote restore: {source}")]
    ReadSandboxRow {
        #[source]
        source: crate::db::Error,
    },
    #[error("persist restoring sandbox row: {source}")]
    PersistRestoringRow {
        #[source]
        source: crate::db::RestoreAdoptOrphanInsertError,
    },
    #[error("invalid recovered sandbox row: {source}")]
    InvalidRecoveredSandboxRow {
        #[source]
        source: RecoveredSandboxRowFieldsError,
    },
    #[error("advance remote restore to artifact staging: {source}")]
    AdvanceRestorePhase {
        #[source]
        source: crate::db::Error,
    },
    #[error("sandbox {sandbox_id} left restoring before artifact staging was recorded")]
    RestorePhaseTransitionLost { sandbox_id: SandboxId },
    #[error("quarantine recovery claim for sandbox {sandbox_id} was superseded")]
    QuarantineRecoveryClaimSuperseded { sandbox_id: SandboxId },
    #[error("read quarantine recovery claim: {source}")]
    ReadQuarantineRecoveryClaim {
        #[source]
        source: crate::db::Error,
    },
    #[error("renew quarantine recovery claim: {source}")]
    RenewQuarantineRecoveryClaim {
        #[source]
        source: crate::db::Error,
    },
    #[error("persist orphan quarantine recovery: {source}")]
    PersistOrphanQuarantineRecovery {
        #[source]
        source: crate::db::Error,
    },
    /// A `GetQuarantineRecoveryPlan` call failed with a status this release does
    /// not map. The code and the server's message are separate fields on purpose:
    /// the code is a bounded value safe to log, the message is server-controlled
    /// text that is not.
    #[error("cluster-api GetQuarantineRecoveryPlan ({code}): {message}")]
    QuarantineRecoveryPlanRpc { code: tonic::Code, message: String },
    #[error("quarantine recovery for sandbox {sandbox_id} was abandoned at shutdown")]
    QuarantineRecoveryAbandonedAtShutdown { sandbox_id: SandboxId },
    #[error("quarantine recovery for sandbox {sandbox_id} timed out after {timeout:?}")]
    QuarantineRecoveryTimedOut {
        sandbox_id: SandboxId,
        timeout: Duration,
    },
    #[error("sandbox database mutex poisoned")]
    SandboxDatabaseMutexPoisoned,
    #[error(
        "canonical cache root for sandbox {sandbox_id} belongs to a different durable restore source"
    )]
    CanonicalRootConflict { sandbox_id: SandboxId },
    /// template-manager could not provide the parent template a btrfs-send
    /// stream is received against: it is missing and not pullable, or the
    /// lookup itself failed. `template` and `content_hash` come from the
    /// downloaded snapshot metadata.
    #[error("tmplmgr GetTemplate({template}@{content_hash}) failed: {source}")]
    ParentTemplateLookup {
        template: String,
        content_hash: String,
        #[source]
        source: tonic::Status,
    },
    /// node-manager's privileged receive into the restore scope failed: the
    /// local template was missing or carried the wrong received UUID, node-manager
    /// was unreachable, or `btrfs receive` rejected the stream.
    #[error("nodemgr BtrfsReceiveInstance: {source}")]
    BtrfsReceive {
        #[source]
        source: crate::btrfs_via_nodemgr::BtrfsViaNodemgrError,
    },
    #[error("replace retained restore instance: {source}")]
    ReplaceInstance {
        #[source]
        source: crate::btrfs_via_nodemgr::BtrfsViaNodemgrError,
    },
    /// Anything else — transport blips, S3 read failures, filesystem
    /// errors, malformed registered URIs.
    #[error("{0}")]
    Other(String),
}

impl RestoreError {
    /// Whether this is a stopped materialization refused because the staged
    /// artifact isn't the evacuated disk the caller named. Retrying can't
    /// help — the same request stages the same artifact — so callers report
    /// it as `FailedPrecondition` rather than a retryable `Unavailable`.
    pub(crate) fn is_stopped_materialization_refusal(&self) -> bool {
        matches!(
            self,
            Self::StoppedMaterializationArtifactUnsuitable { .. }
                | Self::StoppedMaterializationArtifactMismatch { .. }
        )
    }

    /// What failed, for a stage-in failure reported as `stage_failed`. Every
    /// variant is listed so a new one is classified rather than defaulted.
    /// The variants a restore reports under a reason of their own map to
    /// `Other`: they never reach that label.
    pub(crate) fn stage_error_kind(&self) -> pplx_space_metrics::snapshot::RestoreStageErrorKind {
        use pplx_space_metrics::snapshot::RestoreStageErrorKind as Kind;
        match self {
            Self::RestoreOperationBusy { .. } => Kind::Busy,
            Self::SnapshotIdentityMismatch { .. }
            | Self::InvalidSandboxId { .. }
            | Self::InvalidRestoreSource { .. }
            | Self::ArtifactSnapshotUriMismatch { .. }
            | Self::ArtifactSourceUriMismatch { .. }
            | Self::StoppedMaterializationArtifactUnsuitable { .. }
            | Self::StoppedMaterializationArtifactMismatch { .. }
            | Self::MissingSandboxOwnershipMetadata => Kind::SourceMismatch,
            Self::CreateDirectory { .. }
            | Self::RemoveFile { .. }
            | Self::RemoveDirectory { .. }
            | Self::RenamePath { .. } => Kind::Filesystem,
            Self::ReadSandboxRow { .. }
            | Self::PersistRestoringRow { .. }
            | Self::InvalidRecoveredSandboxRow { .. }
            | Self::AdvanceRestorePhase { .. }
            | Self::SandboxDatabaseMutexPoisoned => Kind::SandboxDb,
            Self::RestorePhaseTransitionLost { .. } => Kind::TransitionLost,
            Self::QuarantineRecoveryClaimSuperseded { .. }
            | Self::ReadQuarantineRecoveryClaim { .. }
            | Self::RenewQuarantineRecoveryClaim { .. }
            | Self::PersistOrphanQuarantineRecovery { .. }
            | Self::QuarantineRecoveryPlanRpc { .. }
            | Self::QuarantineRecoveryAbandonedAtShutdown { .. } => Kind::QuarantineRecovery,
            Self::QuarantineRecoveryTimedOut { .. } => Kind::QuarantineRecoveryTimeout,
            Self::ParentTemplateLookup { .. } => Kind::ParentTemplate,
            Self::BtrfsReceive { .. } => Kind::BtrfsReceive,
            Self::ReplaceInstance { .. } => Kind::Filesystem,
            Self::ClusterApiUnconfigured(_)
            | Self::SnapshotNotRegistered(_)
            | Self::AdmissionRejected(_)
            | Self::RootfsPolicy(_)
            | Self::CanonicalRootConflict { .. }
            | Self::Other(_) => Kind::Other,
        }
    }

    /// The error's own message, for the variants that carry no external text.
    ///
    /// Redact-by-default, because the alternative was tried and leaked. A
    /// catch-all `other => Some(self.to_string())` looked safe next to three
    /// explicitly-redacted URI-bearing variants, but `Other` is an opaque bag and
    /// the storage-URI parse errors funnel straight into it — and every variant of
    /// `StorageUriParseError` prints the URI it rejected. So a plan URI with an
    /// endpoint override reached the log through the one accessor added to keep it
    /// out.
    ///
    /// Every variant is therefore listed. There is no catch-all: a new variant
    /// does not compile until it has been placed in the redacted group or the safe
    /// one, which is the only mechanism that stops the next URI-bearing variant
    /// inheriting a leaking rendering.
    ///
    /// The cost falls on `Other`, and it is real: about a hundred construction
    /// sites render as nothing, of which only three carry a URI. An opaque
    /// `String` cannot be classified, so the way to get their detail back is to
    /// give those sites typed variants — the direction `docs/rust/error-handling.md`
    /// already prescribes — not to render the bag and hope. Until then, `kind_label`
    /// plus the sandbox id is what a reader gets from an `Other`.
    ///
    /// This is a narrow property, not "every rendering of a `RestoreError` is
    /// classified". Only the two quarantine-recovery log sites in
    /// `service/recovery.rs` call this. On the same path, `run_cross_node_restore`
    /// renders the full `Display` three other ways when staging fails: into a
    /// `SnapshotRestoreFailed` sandbox event, into the sandbox row's
    /// `failure_reason` — which the node state snapshot publishes on the wire —
    /// and into an INFO log. The cross-node path also logs `storage_uri` at INFO
    /// on every *successful* staging.
    ///
    /// Those are pre-existing and shared with the ordinary cross-node restore, so
    /// routing them through a classification changes durable event content and a
    /// published field for callers that have nothing to do with quarantine
    /// recovery. That is a separate change; a log-only fix would not close it, and
    /// this accessor should not be read as having closed it.
    pub fn redacted_detail(&self) -> Option<String> {
        match self {
            // Redacted: each of these renders a storage URI or free-form text this
            // process did not build. No path-bearing variant is here — the four
            // that carry one construct it locally and sit in the safe group. `Other` is the important one — it is an opaque bag, and the
            // storage-URI parse errors funnel into it, every variant of which
            // prints the URI it rejected.
            Self::InvalidRestoreSource { .. }
            | Self::ArtifactSnapshotUriMismatch { .. }
            | Self::ArtifactSourceUriMismatch { .. }
            | Self::ClusterApiUnconfigured(..)
            | Self::AdmissionRejected(..)
            // These three carry strings that came from outside this process, not
            // payloads it built. `SnapshotIdentityMismatch.actual` is
            // `SnapshotMeta.sandbox_id`, deserialized from a `snapshot.json` in
            // object storage and deliberately left unparsed because comparing it
            // is the point; `InvalidSandboxId.value` is whatever a caller or that
            // same metadata supplied; `InvalidRecoveredSandboxRow` wraps parse
            // failures over metadata-supplied template, runtime, digest and owner
            // values. Unbounded external text has no place in a log line whose
            // premise is that it cannot disclose anything.
            | Self::SnapshotIdentityMismatch { .. }
            | Self::InvalidSandboxId { .. }
            | Self::InvalidRecoveredSandboxRow { .. }
            // A peer's status message, and for the template lookup the
            // metadata-supplied template identity: text this process did not
            // build, as when these two were rendered through `Other`.
            | Self::ParentTemplateLookup { .. }
            | Self::BtrfsReceive { .. }
            | Self::ReplaceInstance { .. }
            | Self::Other(..) => None,
            // The one place a server-supplied failure is worth reporting: the
            // status code is a bounded vocabulary, so it is logged while the
            // accompanying message is not.
            Self::QuarantineRecoveryPlanRpc { code, .. } => {
                Some(format!("plan_rpc_status={code:?}"))
            }
            // Safe: payloads this process built — typed ids, generations,
            // timeouts, paths it constructed, and wrapped SQLite/io sources.
            // Nothing here holds a string supplied by a caller, a peer, or
            // downloaded metadata.
            Self::SnapshotNotRegistered(..)
            | Self::RootfsPolicy(..)
            | Self::RestoreOperationBusy { .. }
            | Self::MissingSandboxOwnershipMetadata
            | Self::CreateDirectory { .. }
            | Self::RemoveFile { .. }
            | Self::RemoveDirectory { .. }
            | Self::RenamePath { .. }
            | Self::ReadSandboxRow { .. }
            | Self::PersistRestoringRow { .. }
            | Self::AdvanceRestorePhase { .. }
            | Self::RestorePhaseTransitionLost { .. }
            | Self::QuarantineRecoveryClaimSuperseded { .. }
            | Self::ReadQuarantineRecoveryClaim { .. }
            | Self::RenewQuarantineRecoveryClaim { .. }
            | Self::PersistOrphanQuarantineRecovery { .. }
            | Self::QuarantineRecoveryAbandonedAtShutdown { .. }
            | Self::QuarantineRecoveryTimedOut { .. }
            | Self::SandboxDatabaseMutexPoisoned
            | Self::StoppedMaterializationArtifactUnsuitable { .. }
            | Self::StoppedMaterializationArtifactMismatch { .. }
            | Self::CanonicalRootConflict { .. } => Some(self.to_string()),
        }
    }

    /// A bounded label naming which failure this is, with no payload at all.
    ///
    /// Always safe to log. Pair it with [`Self::redacted_detail`] when the detail
    /// is wanted too; this one carries no data from any variant, so it cannot
    /// disclose anything regardless of what a future variant holds.
    ///
    /// Exhaustive on purpose: a new variant has to choose its label before it
    /// compiles.
    pub fn kind_label(&self) -> &'static str {
        match self {
            Self::ClusterApiUnconfigured(..) => "cluster_api_unconfigured",
            Self::SnapshotNotRegistered(..) => "snapshot_not_registered",
            Self::AdmissionRejected(..) => "admission_rejected",
            Self::RestoreOperationBusy { .. } => "restore_operation_busy",
            Self::SnapshotIdentityMismatch { .. } => "snapshot_identity_mismatch",
            Self::InvalidSandboxId { .. } => "invalid_sandbox_id",
            Self::InvalidRestoreSource { .. } => "invalid_restore_source",
            Self::ArtifactSnapshotUriMismatch { .. } => "artifact_snapshot_uri_mismatch",
            Self::ArtifactSourceUriMismatch { .. } => "artifact_source_uri_mismatch",
            Self::MissingSandboxOwnershipMetadata => "missing_sandbox_ownership_metadata",
            Self::RootfsPolicy(..) => "rootfs_policy",
            Self::CreateDirectory { .. } => "create_directory",
            Self::RemoveFile { .. } => "remove_file",
            Self::RemoveDirectory { .. } => "remove_directory",
            Self::RenamePath { .. } => "rename_path",
            Self::ReadSandboxRow { .. } => "read_sandbox_row",
            Self::PersistRestoringRow { .. } => "persist_restoring_row",
            Self::InvalidRecoveredSandboxRow { .. } => "invalid_recovered_sandbox_row",
            Self::AdvanceRestorePhase { .. } => "advance_restore_phase",
            Self::RestorePhaseTransitionLost { .. } => "restore_phase_transition_lost",
            Self::QuarantineRecoveryClaimSuperseded { .. } => {
                "quarantine_recovery_claim_superseded"
            }
            Self::ReadQuarantineRecoveryClaim { .. } => "read_quarantine_recovery_claim",
            Self::RenewQuarantineRecoveryClaim { .. } => "renew_quarantine_recovery_claim",
            Self::PersistOrphanQuarantineRecovery { .. } => "persist_orphan_quarantine_recovery",
            Self::QuarantineRecoveryPlanRpc { .. } => "quarantine_recovery_plan_rpc",
            Self::QuarantineRecoveryAbandonedAtShutdown { .. } => {
                "quarantine_recovery_abandoned_at_shutdown"
            }
            Self::QuarantineRecoveryTimedOut { .. } => "quarantine_recovery_timed_out",
            Self::SandboxDatabaseMutexPoisoned => "sandbox_database_mutex_poisoned",
            Self::CanonicalRootConflict { .. } => "canonical_root_conflict",
            Self::ParentTemplateLookup { .. } => "parent_template_lookup",
            Self::BtrfsReceive { .. } => "btrfs_receive",
            Self::ReplaceInstance { .. } => "replace_instance",
            Self::StoppedMaterializationArtifactUnsuitable { .. } => {
                "stopped_materialization_artifact_unsuitable"
            }
            Self::StoppedMaterializationArtifactMismatch { .. } => {
                "stopped_materialization_artifact_mismatch"
            }
            Self::Other(..) => "other",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RestoreArtifactLocationError {
    #[error("snapshot artifact URI {uri:?} does not identify {expected_artifact}")]
    ArtifactMismatch {
        uri: String,
        expected_artifact: &'static str,
    },
    #[error("snapshot artifact URI {uri:?} has no object-store query")]
    MissingQuery { uri: String },
    #[error("snapshot artifact URI {uri:?} has an invalid path shape for {expected_artifact}")]
    InvalidPathShape {
        uri: String,
        expected_artifact: &'static str,
    },
    #[error("snapshot artifact URI {uri:?} has an invalid snapshot base: {source}")]
    InvalidBase {
        uri: String,
        #[source]
        source: pplx_space_common_core::types::StorageUriParseError,
    },
}

#[derive(Debug)]
struct LatestReadySnapshot {
    config: SnapshotUploadConfig,
    storage_uri: String,
}

#[derive(Clone, Copy, Debug)]
enum RestoreMetadataDestination {
    InPlace(pplx_space_common_core::types::TransferScopeId),
    Scoped(pplx_space_common_core::types::TransferScopeId),
}

#[derive(Clone, Debug)]
pub(crate) struct QuarantineRecoveryPlan {
    pub(crate) recovery_id: SandboxCleanupEpisodeId,
    pub(crate) snapshot_id: SnapshotId,
    pub(crate) local_artifact_id: Option<LocalArtifactId>,
    pub(crate) storage_uri: String,
    pub(crate) execution_generation: pplx_space_common_core::types::ExecutionGeneration,
    pub(crate) reason_code: pplx_space_common_core::types::SandboxQuarantineReasonCode,
}

fn parse_recovery_local_artifact_id(value: &str) -> Result<Option<LocalArtifactId>, RestoreError> {
    (!value.trim().is_empty())
        .then(|| {
            value.parse().map_err(|error| {
                RestoreError::Other(format!("invalid recovery local artifact id: {error}"))
            })
        })
        .transpose()
}

/// Drop guard that wipes partially-created restore staging on early return.
pub(crate) struct SandboxCacheDirGuard {
    path: PathBuf,
    armed: bool,
}

impl SandboxCacheDirGuard {
    pub(crate) fn for_restore_staging(
        path: &Path,
        transfer_scope_id: Option<pplx_space_common_core::types::TransferScopeId>,
    ) -> Self {
        Self {
            path: path.to_path_buf(),
            // Scoped staging can contain btrfs subvolumes from an interrupted
            // phase-2 attempt. Its durable scope row owns cleanup through
            // BtrfsDeleteSandboxInstance with a restore-scope target; plain remove_dir_all is only safe for
            // legacy metadata-only staging.
            armed: transfer_scope_id.is_none(),
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SandboxCacheDirGuard {
    fn drop(&mut self) {
        if self.armed
            && let Err(error) = std::fs::remove_dir_all(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %self.path.display(),
                %error,
                "failed to remove abandoned snapshot restore staging"
            );
        }
    }
}

/// Cross-node metadata is staged outside the inventory namespace until its
/// Restoring row commits. Inventory only recognizes UUID-named directories,
/// so a process crash before the row exists cannot manufacture an orphan.
pub(crate) fn remote_restore_metadata_staging_sandbox_id(
    name: &std::ffi::OsStr,
) -> Option<SandboxId> {
    let name = name.to_str()?;
    let sandbox_id = name.strip_prefix('.')?.strip_suffix(".restore-metadata")?;
    sandbox_id.parse().ok()
}

pub(crate) fn remote_restore_metadata_staging_dir(
    sandbox_id: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
) -> PathBuf {
    PathBuf::from(paths::sandbox_restore_staging_dir(
        sandbox_id,
        &transfer_scope_id.to_string(),
    ))
}

/// Publish metadata staging after the Restoring row owns it.
///
/// A canonical cache root is never merged with an attempt: doing so could mix
/// artifacts from different restore generations. The caller must retire the
/// conflicting local execution before publishing this attempt.
pub(crate) fn publish_remote_restore_metadata(
    stage: &mut RestoreMetadataStaged,
    sandbox_id: SandboxId,
) -> Result<(), RestoreError> {
    let canonical_sandbox_dir = PathBuf::from(paths::sandbox_cache_dir(&sandbox_id.to_string()));
    publish_remote_restore_metadata_to(stage, sandbox_id, canonical_sandbox_dir)
}

/// Atomically publish a fully-staged replacement while moving the prior
/// canonical root under this attempt's existing transfer-scope path. A crash
/// can therefore leave only two recoverable states: the old root is still
/// canonical, or the new root is canonical and the old root remains owned by
/// the durable transfer scope.
pub(crate) fn exchange_remote_restore_metadata(
    stage: &mut RestoreMetadataStaged,
    sandbox_id: SandboxId,
) -> Result<(), RestoreError> {
    let canonical_sandbox_dir = PathBuf::from(paths::sandbox_cache_dir(&sandbox_id.to_string()));
    exchange_remote_restore_metadata_to(stage, sandbox_id, canonical_sandbox_dir)
}

pub(crate) fn write_canonical_restore_exchange_receipt(
    stage: &RestoreMetadataStaged,
    sandbox_id: SandboxId,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
    execution_generation: pplx_space_common_core::types::ExecutionGeneration,
    activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
    old_source: &ExactStorageUri,
    replacement_source: &ExactStorageUri,
) -> Result<(), RestoreError> {
    let receipt = CanonicalRestoreExchangeReceipt {
        sandbox_id: sandbox_id.to_string(),
        transfer_scope_id: transfer_scope_id.to_string(),
        execution_generation: execution_generation.get(),
        activation_attempt_id: activation_attempt_id.to_string(),
        old_source: old_source.as_wire().to_string(),
        replacement_source: replacement_source.as_wire().to_string(),
    };
    let bytes = serde_json::to_vec(&receipt).map_err(|source| {
        RestoreError::Other(format!(
            "encode canonical-root exchange receipt for {sandbox_id}: {source}"
        ))
    })?;
    crate::snapshot_scheduler::write_file_atomic(
        stage
            .sandbox_dir
            .join(CANONICAL_RESTORE_EXCHANGE_RECEIPT_FILENAME),
        &bytes,
    )
    .map_err(|source| {
        RestoreError::Other(format!(
            "persist canonical-root exchange receipt for {sandbox_id}: {source}"
        ))
    })
}

pub(crate) fn canonical_restore_exchange_old_source_at(
    canonical_sandbox_dir: &Path,
    sandbox_id: SandboxId,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
    execution_generation: pplx_space_common_core::types::ExecutionGeneration,
    activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
    replacement_source: &ExactStorageUri,
) -> Result<Option<ExactStorageUri>, RestoreError> {
    let path = canonical_sandbox_dir.join(CANONICAL_RESTORE_EXCHANGE_RECEIPT_FILENAME);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(RestoreError::Other(format!(
                "read canonical-root exchange receipt {}: {error}",
                path.display()
            )));
        }
    };
    let receipt: CanonicalRestoreExchangeReceipt =
        serde_json::from_slice(&bytes).map_err(|source| {
            RestoreError::Other(format!(
                "parse canonical-root exchange receipt {}: {source}",
                path.display()
            ))
        })?;
    let receipt_generation =
        pplx_space_common_core::types::ExecutionGeneration::try_from(receipt.execution_generation)
            .map_err(|source| {
                RestoreError::Other(format!(
                    "canonical-root exchange receipt has an invalid execution generation for {sandbox_id}: {source}"
                ))
            })?;
    let receipt_attempt: pplx_space_common_core::types::SandboxActivationAttemptId = receipt
        .activation_attempt_id
        .parse()
        .map_err(|source| {
            RestoreError::Other(format!(
                "canonical-root exchange receipt has an invalid activation attempt for {sandbox_id}: {source}"
            ))
        })?;
    if receipt.sandbox_id != sandbox_id.to_string()
        || receipt.transfer_scope_id != transfer_scope_id.to_string()
        || receipt.replacement_source != replacement_source.as_wire()
        || execution_generation.get() < receipt_generation.get()
        || (execution_generation == receipt_generation && activation_attempt_id < receipt_attempt)
    {
        return Err(RestoreError::Other(format!(
            "canonical-root exchange receipt was produced by an attempt newer than the current cleanup owner for {sandbox_id}"
        )));
    }
    let old_source = receipt.old_source.parse().map_err(|source| {
        RestoreError::Other(format!(
            "canonical-root exchange receipt has an invalid old source for {sandbox_id}: {source}"
        ))
    })?;
    // A partial canonical publication can be replaced from the same durable
    // source. The receipt's scope, generation, activation attempt, and exact
    // replacement URI still fence cleanup; the caller separately verifies
    // that the exchanged old root carries this recorded source.
    Ok(Some(old_source))
}

#[cfg(target_os = "linux")]
fn exchange_remote_restore_metadata_to(
    stage: &mut RestoreMetadataStaged,
    sandbox_id: SandboxId,
    canonical_sandbox_dir: PathBuf,
) -> Result<(), RestoreError> {
    use std::os::unix::ffi::OsStrExt as _;

    if stage.sandbox_dir == canonical_sandbox_dir
        || !stage.sandbox_dir.exists()
        || !canonical_sandbox_dir.exists()
    {
        return Err(RestoreError::Other(format!(
            "cannot exchange restore scope {:?} for sandbox {sandbox_id}: both distinct roots must exist",
            stage.transfer_scope_id
        )));
    }
    let staging =
        std::ffi::CString::new(stage.sandbox_dir.as_os_str().as_bytes()).map_err(|_| {
            RestoreError::Other(format!(
                "restore scope path contains a NUL byte for sandbox {sandbox_id}"
            ))
        })?;
    let canonical =
        std::ffi::CString::new(canonical_sandbox_dir.as_os_str().as_bytes()).map_err(|_| {
            RestoreError::Other(format!(
                "canonical restore path contains a NUL byte for sandbox {sandbox_id}"
            ))
        })?;
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        staging.as_c_str(),
        rustix::fs::CWD,
        canonical.as_c_str(),
        rustix::fs::RenameFlags::EXCHANGE,
    )
    .map_err(|error| {
        RestoreError::Other(format!(
            "exchange restore scope {:?} with the canonical root for sandbox {sandbox_id}: {error}",
            stage.transfer_scope_id,
        ))
    })?;

    if let Some(parent) = canonical_sandbox_dir.parent() {
        std::fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| {
                RestoreError::Other(format!(
                    "sync canonical-root exchange for sandbox {sandbox_id}: {source}"
                ))
            })?;
    }

    stage.sandbox_dir = canonical_sandbox_dir;
    stage.snapshot_dir = stage.sandbox_dir.join("snapshot");
    stage.instance_dir = stage.sandbox_dir.join("instance");
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn exchange_remote_restore_metadata_to(
    stage: &mut RestoreMetadataStaged,
    sandbox_id: SandboxId,
    _canonical_sandbox_dir: PathBuf,
) -> Result<(), RestoreError> {
    Err(RestoreError::Other(format!(
        "atomic canonical-root exchange is unsupported on this platform for restore scope {:?} of sandbox {sandbox_id}",
        stage.transfer_scope_id
    )))
}

fn publish_remote_restore_metadata_to(
    stage: &mut RestoreMetadataStaged,
    sandbox_id: SandboxId,
    canonical_sandbox_dir: PathBuf,
) -> Result<(), RestoreError> {
    if stage.sandbox_dir == canonical_sandbox_dir {
        return Ok(());
    }

    if canonical_sandbox_dir.exists() {
        return Err(RestoreError::Other(format!(
            "refusing to publish restore scope {:?} for sandbox {sandbox_id}: canonical cache root {} already exists",
            stage.transfer_scope_id,
            canonical_sandbox_dir.display()
        )));
    } else {
        std::fs::rename(&stage.sandbox_dir, &canonical_sandbox_dir).map_err(|source| {
            RestoreError::RenamePath {
                from: stage.sandbox_dir.clone(),
                to: canonical_sandbox_dir.clone(),
                source,
            }
        })?;
    }

    stage.sandbox_dir = canonical_sandbox_dir;
    stage.snapshot_dir = stage.sandbox_dir.join("snapshot");
    stage.instance_dir = stage.sandbox_dir.join("instance");
    Ok(())
}

pub(crate) async fn recover_published_remote_restore(
    sandbox_id: SandboxId,
    expected_storage_uri: &ExactStorageUri,
    restore_as_new: Option<(SandboxId, ArtifactKind)>,
) -> Result<Option<PublishedRemoteRestore>, RestoreError> {
    let canonical_sandbox_dir = PathBuf::from(paths::sandbox_cache_dir(&sandbox_id.to_string()));
    let expected_storage_uri = expected_storage_uri.clone();
    tokio::task::spawn_blocking(move || {
        recover_published_remote_restore_at(
            &canonical_sandbox_dir,
            sandbox_id,
            &expected_storage_uri,
            restore_as_new,
        )
    })
    .await
    .map_err(|source| {
        RestoreError::Other(format!(
            "join restore publication recovery task for {sandbox_id}: {source}"
        ))
    })?
}

pub(crate) async fn reclaimable_published_remote_restore_source(
    sandbox_id: SandboxId,
) -> Result<Option<ExactStorageUri>, RestoreError> {
    let canonical_sandbox_dir = PathBuf::from(paths::sandbox_cache_dir(&sandbox_id.to_string()));
    tokio::task::spawn_blocking(move || {
        reclaimable_published_remote_restore_source_at(&canonical_sandbox_dir, sandbox_id)
    })
    .await
    .map_err(|source| {
        RestoreError::Other(format!(
            "join reclaimable restore provenance inspection task for {sandbox_id}: {source}"
        ))
    })?
}

fn recover_published_remote_restore_at(
    canonical_sandbox_dir: &Path,
    sandbox_id: SandboxId,
    expected_storage_uri: &ExactStorageUri,
    restore_as_new: Option<(SandboxId, ArtifactKind)>,
) -> Result<Option<PublishedRemoteRestore>, RestoreError> {
    let metadata_path = canonical_sandbox_dir
        .join("snapshot")
        .join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME);
    let Some(storage_uri) = published_remote_restore_source_at(canonical_sandbox_dir, sandbox_id)?
    else {
        return Ok(None);
    };
    if &storage_uri != expected_storage_uri {
        return Err(RestoreError::CanonicalRootConflict { sandbox_id });
    }
    let metadata = std::fs::read(&metadata_path).map_err(|source| {
        RestoreError::Other(format!(
            "read published restore metadata {}: {source}",
            metadata_path.display()
        ))
    })?;
    let meta: crate::snapshot_upload::SnapshotMeta =
        serde_json::from_slice(&metadata).map_err(|source| {
            RestoreError::Other(format!(
                "parse published restore metadata {}: {source}",
                metadata_path.display()
            ))
        })?;
    if restore_as_new
        .is_some_and(|(source_sandbox_id, _)| meta.sandbox_id == source_sandbox_id.to_string())
    {
        return Ok(None);
    }
    if meta.sandbox_id != sandbox_id.to_string() {
        return Err(RestoreError::SnapshotIdentityMismatch {
            expected: sandbox_id,
            actual: meta.sandbox_id,
        });
    }
    if let Some((_, artifact_kind)) = restore_as_new
        && !published_remote_restore_artifacts_complete(
            canonical_sandbox_dir,
            sandbox_id,
            expected_storage_uri,
            &meta,
            artifact_kind,
        )
    {
        return Ok(None);
    }
    if restore_as_new.is_none()
        && !published_remote_restore_artifacts_complete(
            canonical_sandbox_dir,
            sandbox_id,
            expected_storage_uri,
            &meta,
            crate::snapshot_upload::artifact_kind_from_meta(&meta),
        )
    {
        // The canonical root belongs to this source, but publication did not
        // finish. Route it through the same fenced stage-and-exchange path as
        // a conflicting source so activation cannot consume partial bytes.
        return Err(RestoreError::CanonicalRootConflict { sandbox_id });
    }

    Ok(Some(PublishedRemoteRestore {
        storage_uri: storage_uri.as_wire().to_string(),
        meta,
    }))
}

pub(crate) fn published_remote_restore_source_at(
    canonical_sandbox_dir: &Path,
    sandbox_id: SandboxId,
) -> Result<Option<ExactStorageUri>, RestoreError> {
    let metadata_path = canonical_sandbox_dir
        .join("snapshot")
        .join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME);
    let provenance = match read_verified_replica_provenance(&metadata_path) {
        Ok(provenance) => provenance,
        Err(error) if error.io_error_kind() == Some(std::io::ErrorKind::NotFound) => {
            return Ok(None);
        }
        Err(error) => {
            return Err(RestoreError::Other(format!(
                "read published restore provenance: {error}"
            )));
        }
    };
    let expected_subject = pplx_space_common_core::types::SubjectId::Sandbox(sandbox_id);
    if provenance.subject_id != expected_subject {
        return Err(RestoreError::Other(format!(
            "published restore provenance names subject {} instead of {expected_subject}",
            provenance.subject_id
        )));
    }
    let storage_uri =
        restore_source_from_artifact_location(&provenance.storage_uri, SnapshotArtifact::Metadata)
            .map_err(|error| {
                RestoreError::Other(format!(
                    "recover published restore source from provenance: {error}"
                ))
            })?;
    Ok(Some(storage_uri))
}

pub(crate) fn reclaimable_published_remote_restore_source_at(
    canonical_sandbox_dir: &Path,
    sandbox_id: SandboxId,
) -> Result<Option<ExactStorageUri>, RestoreError> {
    let source = published_remote_restore_source_at(canonical_sandbox_dir, sandbox_id)?;
    let Some(source) = source else {
        return Ok(None);
    };
    let snapshot_metadata_path = canonical_sandbox_dir.join("snapshot").join("snapshot.json");
    let snapshot_metadata = std::fs::read(&snapshot_metadata_path).map_err(|source| {
        RestoreError::Other(format!(
            "read reclaimable restore metadata {}: {source}",
            snapshot_metadata_path.display()
        ))
    })?;
    let snapshot_metadata: crate::snapshot_upload::SnapshotMeta =
        serde_json::from_slice(&snapshot_metadata).map_err(|source| {
            RestoreError::Other(format!(
                "parse reclaimable restore metadata {}: {source}",
                snapshot_metadata_path.display()
            ))
        })?;
    if snapshot_metadata.sandbox_id != sandbox_id.to_string() {
        return Err(RestoreError::SnapshotIdentityMismatch {
            expected: sandbox_id,
            actual: snapshot_metadata.sandbox_id,
        });
    }
    Ok(Some(source))
}

fn published_remote_restore_artifacts_complete(
    canonical_sandbox_dir: &Path,
    sandbox_id: SandboxId,
    expected_storage_uri: &ExactStorageUri,
    meta: &crate::snapshot_upload::SnapshotMeta,
    artifact_kind: ArtifactKind,
) -> bool {
    if crate::snapshot_upload::artifact_kind_from_meta(meta) != artifact_kind {
        return false;
    }
    let Ok(config) = SnapshotUploadConfig::from_uri(expected_storage_uri.as_wire()) else {
        return false;
    };
    let expected_subject = pplx_space_common_core::types::SubjectId::Sandbox(sandbox_id);
    let snapshot_dir = canonical_sandbox_dir.join("snapshot");
    let artifacts = if meta.encrypted {
        crate::snapshot_upload::encrypted_artifacts(artifact_kind)
    } else {
        crate::snapshot_upload::artifact_files_for_kind(
            artifact_kind,
            crate::snapshot_upload::format_from_meta(meta),
        )
    };
    let snapshot_complete = artifacts.into_iter().all(|artifact| {
        let path = snapshot_dir.join(artifact.local_name);
        path.is_file()
            && read_verified_replica_provenance(&path).is_ok_and(|provenance| {
                provenance.subject_id == expected_subject
                    && provenance.storage_uri.as_wire()
                        == restore_artifact_uri(&config, artifact.kind.file_name())
            })
    });
    snapshot_complete
        && (meta.encrypted || canonical_sandbox_dir.join("instance/rootfs.ext4").is_file())
}

/// Filename prefix shared by [`PullReplacementGuard`]'s replacement
/// temps for `local_path` (`.{name}.restore-<uuidv5-of-source-uri>`)
/// and the sweep that reclaims superseded ones.
fn pull_replacement_prefix(local_path: &Path) -> Option<String> {
    let name = local_path.file_name()?;
    Some(format!(".{}.restore-", name.to_string_lossy()))
}

fn replacement_artifact_basename(name: &str) -> &str {
    name.strip_prefix('.')
        .and_then(|name| name.strip_suffix(".volmgr-replica.json"))
        .unwrap_or(name)
}

/// Best-effort removal of replacement temps left by pull attempts for
/// *other source URIs* of `local_path`.
///
/// The download itself runs in vol-mgr as a durable background job that
/// is detached from the gRPC request, so an abandoned restore attempt
/// can strand its temp (guest-memory-sized for the memfile) next to
/// the artifact. Temp names are deterministic per source URI: the temp
/// for the URI currently being restored (`spare_basename`, plus its
/// `.partial` / `.dec.tmp` derivatives) is deliberately spared — it is
/// either a finished download the current attempt commits without
/// re-pulling, or the working file of an in-flight job the current
/// attempt deduped onto. Everything else matching the prefix belongs
/// to a superseded snapshot generation and is removed.
///
/// This deliberately races detached vol-mgr jobs still pulling a
/// *superseded* URI (sbx-mgr's per-sandbox restore serialization does
/// not extend to jobs an abandoned attempt left running): unlinking a
/// zombie's working file kills that job cleanly mid-pipeline, but a
/// zombie between pipeline stages can re-materialize its temp after
/// this sweep. The post-commit re-sweep in [`pull_artifact`] reclaims
/// that within the same restore cycle, so the standing residue bound
/// is one temp per artifact, transiently two.
fn sweep_stale_pull_replacements(
    sandbox_id: &str,
    local_path: &Path,
    spare_basename: Option<&str>,
) {
    let Some(prefix) = pull_replacement_prefix(local_path) else {
        return;
    };
    let Some(parent) = local_path.parent() else {
        return;
    };
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    let mut removed_files: u64 = 0;
    let mut removed_bytes: u64 = 0;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let artifact_basename = replacement_artifact_basename(name);
        if !artifact_basename.starts_with(prefix.as_str()) {
            continue;
        }
        if spare_basename.is_some_and(|spare| artifact_basename.starts_with(spare)) {
            continue;
        }
        // Size feeds the summary log line only; 0 on a stat race is fine.
        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        match std::fs::remove_file(entry.path()) {
            Ok(()) => {
                removed_files += 1;
                removed_bytes = removed_bytes.saturating_add(size);
                tracing::debug!(
                    sandbox_id,
                    path = %entry.path().display(),
                    "removed superseded snapshot artifact pull temp"
                );
            }
            Err(error) => tracing::warn!(
                sandbox_id,
                path = %entry.path().display(),
                %error,
                "failed to remove superseded snapshot artifact pull temp"
            ),
        }
    }
    // One summary line per sweep instead of one line per leaked file —
    // a long-lived sandbox can accumulate one temp per superseded
    // snapshot generation.
    if removed_files > 0 {
        tracing::info!(
            sandbox_id,
            artifact = %local_path.display(),
            removed_files,
            removed_bytes,
            "swept superseded snapshot artifact pull temps"
        );
    }
}

/// Frozen namespace for the deterministic replacement-temp names.
/// Changing it only renames future temps (a one-time cache miss for
/// in-flight reuse) — it has no wire-format or cross-node impact.
const PULL_REPLACEMENT_TEMP_NS: uuid::Uuid =
    uuid::Uuid::from_u128(0x3b1f_8e52_9d4a_4c6e_8f2b_5a7d_90c4_1e83);

struct PullReplacementGuard {
    path: Option<PathBuf>,
}

impl PullReplacementGuard {
    /// Build the replacement temp for `local_path`, named
    /// **deterministically** from the artifact's source URI:
    /// `.{name}.restore-<uuidv5(s3_uri)>`.
    ///
    /// Determinism is what lets retried restores cooperate instead of
    /// duplicating work. vol-mgr dedups active transfer jobs on a key
    /// that includes `local_path`, so two attempts pulling the same
    /// `s3_uri` into the same temp collapse onto one in-flight
    /// download (`wait_for_job` attaches), and an attempt that finds a
    /// *completed* temp on disk takes vol-mgr's skip-present path —
    /// one HEAD instead of a full re-download. A temp left by an
    /// abandoned attempt is therefore a same-bytes cache entry keyed
    /// by source URI, not garbage: the URI names an immutable snapshot
    /// artifact, and vol-mgr re-downloads if it finds the temp in the
    /// half-decompressed (zstd-magic) intermediate state. Temps for
    /// *other* source URIs are removed by
    /// [`sweep_stale_pull_replacements`].
    fn for_existing(local_path: &Path, s3_uri: &str) -> Self {
        let path = pull_replacement_prefix(local_path).map(|prefix| {
            local_path.with_file_name(format!(
                "{prefix}{}",
                uuid::Uuid::new_v5(&PULL_REPLACEMENT_TEMP_NS, s3_uri.as_bytes())
            ))
        });
        Self { path }
    }

    fn download_path(&self, local_path: &Path) -> PathBuf {
        self.path
            .as_ref()
            .cloned()
            .unwrap_or_else(|| local_path.to_path_buf())
    }

    /// Basename the superseded-temp sweep must spare: this attempt's
    /// temp and its `.partial` / `.dec.tmp` derivatives, which a
    /// deduped in-flight vol-mgr job may still be writing.
    fn spare_basename(&self) -> Option<String> {
        self.path
            .as_ref()
            .and_then(|path| path.file_name())
            .map(|name| name.to_string_lossy().into_owned())
    }

    fn commit(&mut self, local_path: &Path) -> Result<(), RestoreError> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        move_verified_replica(path, local_path)?;
        self.path = None;
        Ok(())
    }
}

fn move_verified_replica(source: &Path, destination: &Path) -> Result<(), RestoreError> {
    let source_provenance = verified_replica_provenance_path(source);
    let destination_provenance = verified_replica_provenance_path(destination);
    match std::fs::remove_file(&destination_provenance) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(RestoreError::RemoveFile {
                path: destination_provenance,
                source,
            });
        }
    }
    std::fs::rename(source, destination).map_err(|source_error| RestoreError::RenamePath {
        from: source.to_path_buf(),
        to: destination.to_path_buf(),
        source: source_error,
    })?;
    match std::fs::rename(&source_provenance, &destination_provenance) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(RestoreError::RenamePath {
            from: source_provenance,
            to: destination_provenance,
            source,
        }),
    }
}

// Deliberately no Drop: an uncommitted temp is kept on disk. It is
// either still being written by the detached vol-mgr job (which a
// retry attaches to via the dedup key) or a completed download the
// retry commits without re-pulling. The sweep removes temps whose
// source URI is no longer the one being restored.

/// If `local_path` is missing but a deterministic replacement temp for
/// `s3_uri` exists (a prior abandoned attempt finished its download
/// and was never committed), adopt it by renaming it into place —
/// atomic within the directory — instead of re-downloading. Returns
/// whether adoption happened; [`replacement_for_pull`] then keeps the
/// follow-up pull on the canonical path so vol-mgr's skip-present
/// check (S3 HEAD plus the zstd-magic check that catches a
/// half-decompressed intermediate and forces a re-download) validates
/// the adopted bytes in place rather than re-fetching them.
///
/// A zombie job racing this rename can at worst re-materialize the
/// temp afterwards — the superseded-temp sweeps and the same
/// validation converge on correct bytes either way.
fn adopt_completed_replacement_temp(sandbox_id: &str, local_path: &Path, s3_uri: &str) -> bool {
    if local_path.exists() {
        return false;
    }
    let temp = PullReplacementGuard::for_existing(local_path, s3_uri).download_path(local_path);
    if temp == local_path || !temp.exists() {
        return false;
    }
    match move_verified_replica(&temp, local_path) {
        Ok(()) => {
            tracing::info!(
                sandbox_id,
                path = %local_path.display(),
                "adopted completed pull temp for missing artifact"
            );
            true
        }
        Err(error) => {
            tracing::warn!(
                sandbox_id,
                path = %temp.display(),
                %error,
                "failed to adopt completed pull temp; re-downloading"
            );
            false
        }
    }
}

/// Choose where the vol-mgr pull lands for this artifact.
///
/// An artifact that exists *because adoption just renamed a completed
/// temp into place* must be pulled at its canonical path: vol-mgr's
/// skip-present check inspects the request's `local_path`, so only a
/// pull targeting the adopted file validates it in place (S3 HEAD +
/// the zstd-magic intermediate check) — a replacement temp here would
/// point skip-present at the path adoption just emptied and force the
/// full re-download adoption exists to avoid. Any other pre-existing
/// artifact is possibly-stale local state and is replaced through a
/// temp so it is never observed torn.
fn replacement_for_pull(local_path: &Path, s3_uri: &str, adopted: bool) -> PullReplacementGuard {
    if !adopted && local_path.exists() {
        PullReplacementGuard::for_existing(local_path, s3_uri)
    } else {
        PullReplacementGuard { path: None }
    }
}

/// Look up the latest suspend-snapshot storage location for `sandbox_id` and
/// pull each artifact into `/space/cache/sandboxes/<sandbox_id>/snapshot/`.
///
/// This is the single-call wrapper for callers where the sandbox row
/// already exists in sbxmgr's SQLite (in-place restage after local-file
/// loss). The cross-node restore path uses the two-phase API
/// ([`restore_pull_metadata_scoped`] + [`restore_pull_rootfs`]) instead. Metadata is
/// first pulled under a hidden, non-inventory name; the caller commits a
/// `Restoring` row and publishes that directory before the slow rootfs phase.
pub async fn prepare_restore_dir(
    sandbox_id: &str,
    owner: &pplx_space_common_core::types::SandboxOwner,
    execution_fence: pplx_space_node_mgr_client::execution_fence::ExecutionFence,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
    templates: &dyn crate::template_resolver::TemplateResolver,
) -> Result<RestorePrepared, RestoreError> {
    let latest = latest_ready_snapshot(sandbox_id).await?;
    let staged = restore_pull_metadata_from_source(
        sandbox_id,
        latest,
        RestoreMetadataDestination::InPlace(transfer_scope_id),
    )
    .await?;
    restore_pull_rootfs(&staged, owner, execution_fence, node_manager, templates).await?;
    Ok(RestorePrepared {
        snapshot_dir: staged.snapshot_dir,
        storage_uri: staged.storage_uri,
        local_artifact_id: staged.meta.local_artifact_id,
        artifact_kind: staged.meta.artifact_kind,
    })
}

pub async fn prepare_restore_dir_from_uri(
    sandbox_id: &str,
    owner: &pplx_space_common_core::types::SandboxOwner,
    execution_fence: pplx_space_node_mgr_client::execution_fence::ExecutionFence,
    storage_uri: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
    templates: &dyn crate::template_resolver::TemplateResolver,
) -> Result<RestorePrepared, RestoreError> {
    let config = SnapshotUploadConfig::from_uri(storage_uri)
        .map_err(|e| RestoreError::Other(format!("parse storage_uri: {e}")))?;
    let staged = restore_pull_metadata_from_source(
        sandbox_id,
        LatestReadySnapshot {
            config,
            storage_uri: storage_uri.to_string(),
        },
        RestoreMetadataDestination::InPlace(transfer_scope_id),
    )
    .await?;
    restore_pull_rootfs(&staged, owner, execution_fence, node_manager, templates).await?;
    Ok(RestorePrepared {
        snapshot_dir: staged.snapshot_dir,
        storage_uri: staged.storage_uri,
        local_artifact_id: staged.meta.local_artifact_id,
        artifact_kind: staged.meta.artifact_kind,
    })
}

fn restore_error_from_cluster_connect(
    error: pplx_space_node_api_client::cluster::ConnectClusterApiError,
) -> RestoreError {
    use pplx_space_node_api_client::cluster::ConnectClusterApiError;
    match error {
        error @ (ConnectClusterApiError::MissingEndpoint
        | ConnectClusterApiError::MissingToken(_)) => {
            RestoreError::ClusterApiUnconfigured(error.to_string())
        }
        error => RestoreError::Other(format!("cluster-api connect: {error}")),
    }
}

async fn get_suspend_snapshot_location(
    sandbox_id: &str,
) -> Result<pplx_space_node_api_client::proto::GetSuspendSnapshotLocationResponse, RestoreError> {
    let mut cluster = pplx_space_node_api_client::cluster::connect_from_env()
        .await
        .map_err(restore_error_from_cluster_connect)?;

    cluster
        .get_suspend_snapshot_location(
            pplx_space_node_api_client::proto::GetSuspendSnapshotLocationRequest {
                sandbox_id: sandbox_id.to_string(),
            },
        )
        .await
        .map_err(|status| match status.code() {
            tonic::Code::NotFound => RestoreError::SnapshotNotRegistered(sandbox_id.to_string()),
            _ => RestoreError::Other(format!(
                "cluster-api GetSuspendSnapshotLocation ({:?}): {}",
                status.code(),
                status.message()
            )),
        })
        .map(tonic::Response::into_inner)
}

pub(crate) enum QuarantineRecoveryPlanResolution {
    Available(QuarantineRecoveryPlan),
    NoProgress(crate::db::QuarantineRecoveryNoProgressReason),
}

/// The read-only plan lookup runs before the claim, and it is still bounded:
/// an unbounded connect-and-call would hold the serial inventory reconciler for
/// as long as the control plane stays unreachable. A small fraction of the
/// claim lease keeps a slow plan lookup a retryable no-progress result rather
/// than a stalled pass.
const QUARANTINE_RECOVERY_PLAN_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) async fn quarantine_recovery_plan(
    sandbox_id: &str,
) -> Result<QuarantineRecoveryPlanResolution, RestoreError> {
    match tokio::time::timeout(
        QUARANTINE_RECOVERY_PLAN_TIMEOUT,
        quarantine_recovery_plan_inner(sandbox_id),
    )
    .await
    {
        Ok(resolution) => resolution,
        Err(_elapsed) => Ok(QuarantineRecoveryPlanResolution::NoProgress(
            crate::db::QuarantineRecoveryNoProgressReason::PlanLookupTimedOut,
        )),
    }
}

/// Bounds a malformed-plan log detail.
///
/// What this bounds is the line, not the allocation: a caller composes the detail with
/// `format!` first, so the value — and the parse error, which renders it again — are
/// materialized in full and then truncated. That is deliberate rather than overlooked.
/// The value is already resident, decoded from the plan RPC and therefore bounded by
/// the transport's message limit, so the transient cost is a copy of something already
/// held. Bounding the allocation too would mean errors whose `Display` cannot embed
/// their input, which is a change to the error types rather than to this function.
///
/// A malformed plan is logged once per attempt per episode, and every value in
/// the detail arrives from the control plane unvalidated and unlengthed — the
/// same reason `redacted_detail` withholds externally-sourced text. Bounding the
/// whole rendered detail rather than the individual fields is deliberate: the
/// parse errors interpolated beside those fields render the offending value
/// again themselves (`unknown {kind} '{value}'`), so a per-field bound is
/// defeated by the `{error}` next to it. Version skew is still diagnosable,
/// because the prefix and the real length survive.
fn bounded_log_detail(detail: &str) -> String {
    const MAX_LOGGED_CHARS: usize = 200;
    if detail.chars().count() <= MAX_LOGGED_CHARS {
        return detail.to_owned();
    }
    let head: String = detail.chars().take(MAX_LOGGED_CHARS).collect();
    format!("{head}… ({} bytes)", detail.len())
}

async fn quarantine_recovery_plan_inner(
    sandbox_id: &str,
) -> Result<QuarantineRecoveryPlanResolution, RestoreError> {
    let request = quarantine_recovery_plan_request(
        sandbox_id,
        &crate::service::NodeIdentitySource::from_process(),
        std::env::var("SPACE_NODE_ROLE").ok().as_deref(),
    )?;
    let mut cluster = pplx_space_node_api_client::cluster::connect_from_env()
        .await
        .map_err(restore_error_from_cluster_connect)?;
    let plan = match cluster.get_quarantine_recovery_plan(request).await {
        Ok(response) => response.into_inner(),
        Err(status) if status.code() == tonic::Code::NotFound => {
            return Ok(QuarantineRecoveryPlanResolution::NoProgress(
                crate::db::QuarantineRecoveryNoProgressReason::PlanNotFound,
            ));
        }
        Err(status) if status.code() == tonic::Code::FailedPrecondition => {
            return Ok(QuarantineRecoveryPlanResolution::NoProgress(
                crate::db::QuarantineRecoveryNoProgressReason::PlanIneligible,
            ));
        }
        Err(status) if status.code() == tonic::Code::Unimplemented => {
            return Ok(QuarantineRecoveryPlanResolution::NoProgress(
                crate::db::QuarantineRecoveryNoProgressReason::PlanRpcUnsupported,
            ));
        }
        Err(status) => {
            return Err(RestoreError::QuarantineRecoveryPlanRpc {
                code: status.code(),
                message: status.message().to_string(),
            });
        }
    };
    resolve_quarantine_recovery_plan_response(sandbox_id, plan)
}

fn quarantine_recovery_plan_request(
    sandbox_id: &str,
    node_identity: &crate::service::NodeIdentitySource,
    node_role: Option<&str>,
) -> Result<pplx_space_node_api_client::proto::GetQuarantineRecoveryPlanRequest, RestoreError> {
    let node_id = node_identity.configured_node_id().ok_or_else(|| {
        RestoreError::ClusterApiUnconfigured(
            "neither SPACE_NODE_ID nor the node-id file names this node".to_string(),
        )
    })?;
    let role = match node_role {
        Some("worker") | None => Some(pplx_space_node_api_client::proto::NodeRole::Worker as i32),
        Some("builder") => Some(pplx_space_node_api_client::proto::NodeRole::Builder as i32),
        Some(other) => {
            return Err(RestoreError::Other(format!(
                "unsupported SPACE_NODE_ROLE for quarantine recovery: {other}"
            )));
        }
    };
    Ok(
        pplx_space_node_api_client::proto::GetQuarantineRecoveryPlanRequest {
            node_id,
            sandbox_id: sandbox_id.to_string(),
            role,
        },
    )
}

/// Turn a plan response into a resolution.
///
/// Split from the RPC so every rejection branch is reachable without a control
/// plane. These branches are what let a caller tell "no plan exists" from "a plan
/// we must not act on", and they were entirely untested while they sat behind
/// `connect_from_env`.
pub(crate) fn resolve_quarantine_recovery_plan_response(
    sandbox_id: &str,
    plan: pplx_space_node_api_client::proto::GetQuarantineRecoveryPlanResponse,
) -> Result<QuarantineRecoveryPlanResolution, RestoreError> {
    // Validating the target state belongs with the rest of the response checks,
    // not on the RPC side: a control plane that asks for a state this release
    // cannot honor is version skew, and the node has to decline it distinctly
    // from a corrupt field.
    if plan.target_state
        != pplx_space_node_api_client::proto::QuarantineRecoveryTargetState::Suspended as i32
    {
        return Ok(QuarantineRecoveryPlanResolution::NoProgress(
            crate::db::QuarantineRecoveryNoProgressReason::UnsupportedTargetState,
        ));
    }
    let malformed = |detail: &str| {
        tracing::warn!(
            sandbox_id,
            detail = bounded_log_detail(detail),
            "received malformed quarantine recovery plan"
        );
        QuarantineRecoveryPlanResolution::NoProgress(
            crate::db::QuarantineRecoveryNoProgressReason::PlanMalformed,
        )
    };
    let Some(source) = plan.suspend_snapshot else {
        return Ok(malformed("missing suspend snapshot"));
    };
    let recovery_id = match plan.recovery_id.parse() {
        Ok(value) => value,
        Err(error) => {
            return Ok(malformed(&format!(
                "invalid recovery id {:?}: {error}",
                plan.recovery_id
            )));
        }
    };
    let snapshot_id = match source.snapshot_id.parse() {
        Ok(value) => value,
        Err(error) => {
            return Ok(malformed(&format!(
                "invalid recovery snapshot id {:?}: {error}",
                source.snapshot_id
            )));
        }
    };
    let local_artifact_id = match parse_recovery_local_artifact_id(&source.local_artifact_id) {
        Ok(value) => value,
        Err(error) => {
            return Ok(malformed(&format!(
                "invalid local artifact id {:?}: {error}",
                source.local_artifact_id
            )));
        }
    };
    let execution_generation =
        match pplx_space_common_core::types::ExecutionGeneration::new(plan.execution_generation) {
            Ok(value) => value,
            Err(error) => {
                return Ok(malformed(&format!(
                    "invalid recovery generation {}: {error}",
                    plan.execution_generation
                )));
            }
        };
    let reason_code: pplx_space_common_core::types::SandboxQuarantineReasonCode =
        match plan.reason_code.parse() {
            Ok(value) => value,
            Err(error) => {
                return Ok(malformed(&format!(
                    "invalid recovery reason {:?}: {error}",
                    plan.reason_code
                )));
            }
        };
    if reason_code.automatic_recovery_policy().is_none() {
        return Ok(QuarantineRecoveryPlanResolution::NoProgress(
            crate::db::QuarantineRecoveryNoProgressReason::PlanReasonIneligible,
        ));
    }
    Ok(QuarantineRecoveryPlanResolution::Available(
        QuarantineRecoveryPlan {
            recovery_id,
            snapshot_id,
            local_artifact_id,
            storage_uri: source.storage_uri,
            execution_generation,
            reason_code,
        },
    ))
}

async fn latest_ready_snapshot(sandbox_id: &str) -> Result<LatestReadySnapshot, RestoreError> {
    let location = get_suspend_snapshot_location(sandbox_id).await?;

    let config = SnapshotUploadConfig::from_uri(&location.storage_uri)
        .map_err(|e| RestoreError::Other(format!("parse storage_uri: {e}")))?;
    Ok(LatestReadySnapshot {
        config,
        storage_uri: location.storage_uri,
    })
}

pub async fn restore_pull_metadata_scoped(
    sandbox_id: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
) -> Result<RestoreMetadataStaged, RestoreError> {
    let latest = latest_ready_snapshot(sandbox_id).await?;
    restore_pull_metadata_from_source(
        sandbox_id,
        latest,
        RestoreMetadataDestination::Scoped(transfer_scope_id),
    )
    .await
}

pub async fn restore_pull_metadata_from_uri_scoped(
    sandbox_id: &str,
    storage_uri: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
) -> Result<RestoreMetadataStaged, RestoreError> {
    let config = SnapshotUploadConfig::from_uri(storage_uri)
        .map_err(|e| RestoreError::Other(format!("parse storage_uri: {e}")))?;
    restore_pull_metadata_from_source(
        sandbox_id,
        LatestReadySnapshot {
            config,
            storage_uri: storage_uri.to_string(),
        },
        RestoreMetadataDestination::Scoped(transfer_scope_id),
    )
    .await
}

pub async fn restore_pull_metadata_from_uri_as_new(
    target_sandbox_id: &str,
    source_sandbox_id: &str,
    storage_uri: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
) -> Result<RestoreMetadataStaged, RestoreError> {
    restore_pull_metadata_from_uri_as_new_at_destination(
        target_sandbox_id,
        source_sandbox_id,
        storage_uri,
        RestoreMetadataDestination::Scoped(transfer_scope_id),
    )
    .await
}

pub async fn restore_pull_metadata_from_uri_as_new_in_place(
    target_sandbox_id: &str,
    source_sandbox_id: &str,
    storage_uri: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
) -> Result<RestoreMetadataStaged, RestoreError> {
    restore_pull_metadata_from_uri_as_new_at_destination(
        target_sandbox_id,
        source_sandbox_id,
        storage_uri,
        RestoreMetadataDestination::InPlace(transfer_scope_id),
    )
    .await
}

async fn restore_pull_metadata_from_uri_as_new_at_destination(
    target_sandbox_id: &str,
    source_sandbox_id: &str,
    storage_uri: &str,
    destination: RestoreMetadataDestination,
) -> Result<RestoreMetadataStaged, RestoreError> {
    let config = SnapshotUploadConfig::from_uri(storage_uri)
        .map_err(|e| RestoreError::Other(format!("parse storage_uri: {e}")))?;
    restore_pull_metadata_from_source_for_identity(
        target_sandbox_id,
        source_sandbox_id,
        LatestReadySnapshot {
            config,
            storage_uri: storage_uri.to_string(),
        },
        destination,
    )
    .await
}

async fn restore_pull_metadata_from_source(
    sandbox_id: &str,
    latest: LatestReadySnapshot,
    destination: RestoreMetadataDestination,
) -> Result<RestoreMetadataStaged, RestoreError> {
    restore_pull_metadata_from_source_for_identity(sandbox_id, sandbox_id, latest, destination)
        .await
}

pub(crate) fn validate_restore_metadata_identity(
    meta: &crate::snapshot_upload::SnapshotMeta,
    target_sandbox_id: &str,
    expected_source_sandbox_id: &str,
) -> Result<(), RestoreError> {
    if let Some(metadata) = meta.execution.as_ref() {
        // Snapshot transport does not carry non-legacy image semantics.
        pplx_space_node_runtime_core::boot::GuestProfile::resolve(
            metadata.execution(),
            Default::default(),
        )
        .map_err(|error| RestoreError::Other(error.to_string()))?;
    }
    let expected_identity = if meta.artifact_origin == Some(ArtifactOrigin::Fork) {
        target_sandbox_id
    } else {
        expected_source_sandbox_id
    };
    let expected_sandbox_id: SandboxId =
        expected_identity
            .parse()
            .map_err(|source| RestoreError::InvalidSandboxId {
                value: expected_identity.to_string(),
                source,
            })?;
    if meta.sandbox_id != expected_sandbox_id.to_string() {
        return Err(RestoreError::SnapshotIdentityMismatch {
            expected: expected_sandbox_id,
            actual: meta.sandbox_id.clone(),
        });
    }
    Ok(())
}

async fn restore_pull_metadata_from_source_for_identity(
    sandbox_id: &str,
    expected_source_sandbox_id: &str,
    latest: LatestReadySnapshot,
    destination: RestoreMetadataDestination,
) -> Result<RestoreMetadataStaged, RestoreError> {
    let config = latest.config;

    let canonical_sandbox_dir = PathBuf::from(paths::sandbox_cache_dir(sandbox_id));
    let (sandbox_dir, transfer_scope_id, filesystem_scope_id, sandbox_row_exists) =
        match destination {
            RestoreMetadataDestination::InPlace(scope) => {
                #[allow(
                    clippy::expect_used,
                    reason = "sandbox_cache_dir always joins a sandbox to the absolute cache root"
                )]
                let legacy_staging_dir = canonical_sandbox_dir
                    .parent()
                    .expect("sandbox cache path has a parent")
                    .join(format!(".{sandbox_id}.restore-metadata"));
                match std::fs::remove_dir_all(&legacy_staging_dir) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => tracing::warn!(
                        sandbox_id,
                        path = %legacy_staging_dir.display(),
                        %error,
                        "failed to remove stale pre-row restore metadata staging"
                    ),
                }
                (canonical_sandbox_dir, Some(scope), None, true)
            }
            RestoreMetadataDestination::Scoped(scope) => (
                remote_restore_metadata_staging_dir(sandbox_id, scope),
                Some(scope),
                Some(scope),
                false,
            ),
        };
    let snapshot_dir = sandbox_dir.join("snapshot");
    let instance_dir = sandbox_dir.join("instance");
    // `snapshots/` (plural) is the rolling-btrfs-snapshots directory. The
    // in-place create path lays it down via node-mgr's `prepare_sandbox`
    // alongside the `instance/` subvolume; cross-node restore re-creates it
    // here so the directory shape matches what `start_from_checkpoint`'s
    // downstream callers expect to find.
    let snapshots_dir = sandbox_dir.join("snapshots");
    std::fs::create_dir_all(&sandbox_dir).map_err(|source| RestoreError::CreateDirectory {
        path: sandbox_dir.clone(),
        source,
    })?;
    // Before a sandbox row exists this is a hidden, non-inventory directory.
    // A retry with an existing Restoring row keeps attributed files when
    // admission rejects metadata; the next attempt can reuse them safely.
    let mut sandbox_dir_guard =
        SandboxCacheDirGuard::for_restore_staging(&sandbox_dir, transfer_scope_id);
    std::fs::create_dir_all(&snapshot_dir).map_err(|source| RestoreError::CreateDirectory {
        path: snapshot_dir.clone(),
        source,
    })?;
    std::fs::create_dir_all(&snapshots_dir).map_err(|source| RestoreError::CreateDirectory {
        path: snapshots_dir.clone(),
        source,
    })?;

    // Phase 1: pull `metadata.json` *before* the rootfs so we can
    // dispatch on the sidecar's codec marker. The metadata wire-format
    // is the same regardless of codec — the only difference is the
    // rootfs filename and its bytes.
    let pull_result = pull_artifact(
        RestoreTransferIdentity {
            sandbox_id,
            credential_subject_id: expected_source_sandbox_id,
        },
        &config,
        snapshot_dir.join("snapshot.json"),
        SnapshotArtifact::Metadata,
        transfer_scope_id,
    )
    .await;
    preserve_attributed_metadata_retry(&mut sandbox_dir_guard, sandbox_row_exists, pull_result)?;

    // Parse the sidecar once and derive the codec from it. Read/parse
    // failures here are corruption or schema-incompatibility signals
    // and surface as `RestoreError::Other` so callers see a precise
    // diagnostic — historically this path collapsed parse failures into
    // `RestoreCodec::Full`, which then 404'd in S3 on the wrong artifact
    // key and turned a clear "sidecar broken" into a confusing
    // wrong-blob fetch.
    let meta_path = snapshot_dir.join("snapshot.json");
    let meta_bytes = std::fs::read(&meta_path)
        .map_err(|e| RestoreError::Other(format!("read {}: {e}", meta_path.display())))?;
    let mut meta: crate::snapshot_upload::SnapshotMeta = serde_json::from_slice(&meta_bytes)
        .map_err(|e| RestoreError::Other(format!("parse {}: {e}", meta_path.display())))?;
    // A retained source identity is node-local. Uploads no longer carry one,
    // but a restored artifact never has the bundle it would name, and its
    // local sidecar must describe the pulled stream. Volume-manager wrote the
    // pulled file under its own identity, so replace it through a sibling and
    // a rename rather than opening it for writing.
    if meta.rootfs_send_source_id.take().is_some() {
        // The durable replacement syncs a file and its directory, so it runs
        // off the async workers. The staging guard travels with it: a
        // cancelled restore cannot remove the directory mid-replacement.
        let dir = snapshot_dir.clone();
        let (guard, rewritten) = tokio::task::spawn_blocking(move || {
            let rewritten = replace_restored_sidecar(&dir, &meta).map(|()| meta);
            (sandbox_dir_guard, rewritten)
        })
        .await
        .map_err(|source| {
            RestoreError::Other(format!("join rewrite of {}: {source}", meta_path.display()))
        })?;
        sandbox_dir_guard = guard;
        meta = rewritten
            .map_err(|e| RestoreError::Other(format!("rewrite {}: {e}", meta_path.display())))?;
    }
    validate_restore_metadata_identity(&meta, sandbox_id, expected_source_sandbox_id)?;
    let codec = codec_from_meta(&meta).map_err(|error| RestoreError::Other(error.to_string()))?;
    tracing::info!(
        sandbox_id,
        codec = ?codec,
        snapshot_dir = %snapshot_dir.display(),
        "cross-node restore: dispatching on rootfs codec"
    );

    sandbox_dir_guard.disarm();
    Ok(RestoreMetadataStaged {
        sandbox_dir,
        snapshot_dir,
        instance_dir,
        config,
        storage_uri: latest.storage_uri,
        meta,
        codec,
        transfer_scope_id,
        credential_subject_id: expected_source_sandbox_id.to_string(),
        filesystem_scope_id,
    })
}

/// Replace a pulled `snapshot.json` with `meta` through a sibling and a
/// rename. The pulled file belongs to volume-manager's identity and is not
/// writable by this service; the snapshot directory is.
pub(crate) fn replace_restored_sidecar(
    snapshot_dir: &Path,
    meta: &crate::snapshot_upload::SnapshotMeta,
) -> std::io::Result<()> {
    let bytes = serde_json::to_vec_pretty(meta)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    crate::snapshot_upload::replace_file_durably(
        snapshot_dir,
        ".snapshot.json.restore",
        crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME,
        &bytes,
    )
}

fn preserve_attributed_metadata_retry(
    sandbox_dir_guard: &mut SandboxCacheDirGuard,
    sandbox_row_exists: bool,
    result: Result<(), RestoreError>,
) -> Result<(), RestoreError> {
    if sandbox_row_exists && matches!(result, Err(RestoreError::AdmissionRejected(_))) {
        sandbox_dir_guard.disarm();
    }
    result
}

/// Phase 2 of cross-node restore: pull the rootfs artifact (full image
/// or btrfs-send stream) and reconstruct `<sandbox>/instance/`. For the
/// btrfs-send codec this calls into node-manager's
/// `BtrfsReceiveInstance` RPC (sbxmgr lacks `CAP_SYS_ADMIN`); for the
/// Full codec it pulls the rootfs blob and reflinks it.
///
/// Ordinary failures tear down partial state. Admission rejection preserves
/// completed, verified files for the next retry.
pub async fn restore_pull_rootfs(
    stage: &RestoreMetadataStaged,
    owner: &pplx_space_common_core::types::SandboxOwner,
    execution_fence: pplx_space_node_mgr_client::execution_fence::ExecutionFence,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
    templates: &dyn crate::template_resolver::TemplateResolver,
) -> Result<(), RestoreError> {
    use pplx_space_node_mgr_client::execution_fence::InstanceTarget;
    let target = stage.filesystem_scope_id.map_or(
        InstanceTarget::Execution(execution_fence),
        InstanceTarget::RestoreScope,
    );
    if let Some(size_mib) = stage.meta.source.rootfs_size_mib {
        pplx_space_common_core::sandbox_storage::validate_rootfs_mib(size_mib)
            .map_err(RestoreError::RootfsPolicy)?;
    }
    let sandbox_id = stage.meta.sandbox_id.as_str();
    let transfer_identity = RestoreTransferIdentity {
        sandbox_id,
        credential_subject_id: &stage.credential_subject_id,
    };

    // BYOK encrypted snapshot: pull the two sealed ciphertext artifacts
    // (snapshot.luks = vmstate+memory, cow.luks = rootfs delta) into the
    // snapshot dir. The plaintext origin is cloned from the local template by
    // `prepare_sandbox`; the launch path (create.rs) reconstructs the dm-crypt
    // rootfs from cow.luks and opens the snapshot volume from snapshot.luks.
    if stage.meta.encrypted {
        let stage_result = stage_encrypted_restore(
            transfer_identity,
            &stage.config,
            &stage.snapshot_dir,
            stage.transfer_scope_id,
            crate::snapshot_upload::artifact_kind_from_meta(&stage.meta),
        )
        .await;
        if let Err(ref e) = stage_result {
            handle_restore_stage_failure(stage, target, e, node_manager).await;
        }
        return stage_result;
    }

    let stage_result = match stage.codec {
        RestoreCodec::BtrfsSendV1 => {
            stage_btrfs_send_restore(
                transfer_identity,
                owner,
                &stage.config,
                &stage.snapshot_dir.join("snapshot.json"),
                &stage.snapshot_dir,
                crate::snapshot_upload::artifact_kind_from_meta(&stage.meta),
                stage.transfer_scope_id,
                target,
                node_manager,
                templates,
            )
            .await
        }
        // `RestoreCodec::Full` covers both legacy sidecars that have
        // no `rootfs_diff_format` field at all and the post-PR ones
        // explicitly tagged as Full. Either lands in the historical
        // download-rootfs + reflink path for backwards compatibility.
        RestoreCodec::Full => {
            stage_full_codec_artifacts(transfer_identity, stage, target, node_manager).await
        }
    };

    if let Err(ref e) = stage_result {
        handle_restore_stage_failure(stage, target, e, node_manager).await;
    }
    stage_result
}

async fn handle_restore_stage_failure(
    stage: &RestoreMetadataStaged,
    target: pplx_space_node_mgr_client::execution_fence::InstanceTarget,
    error: &RestoreError,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
) {
    if matches!(error, RestoreError::AdmissionRejected(_)) {
        preserve_completed_restore_artifacts(stage);
        tracing::info!(
            sandbox_id = %stage.meta.sandbox_id,
            snapshot_dir = %stage.snapshot_dir.display(),
            "restore admission rejected; preserved completed artifacts for retry"
        );
        return;
    }
    tracing::warn!(
        sandbox_id = %stage.meta.sandbox_id,
        error = %error,
        snapshot_dir = %stage.snapshot_dir.display(),
        "cross-node restore staging failed; removing partial artifacts"
    );
    cleanup_partial_restore_artifacts(
        &stage.sandbox_dir,
        &stage.meta.sandbox_id,
        target,
        node_manager,
    )
    .await;
}

fn preserve_completed_restore_artifacts(stage: &RestoreMetadataStaged) {
    for (artifact, local_name) in [
        (SnapshotArtifact::Metadata, "snapshot.json"),
        (SnapshotArtifact::VmState, "vmstate"),
        (SnapshotArtifact::Memory, "memory"),
        (SnapshotArtifact::Rootfs, "rootfs.ext4"),
        (
            SnapshotArtifact::RootfsBtrfsSend,
            crate::snapshot_upload::ROOTFS_BTRFS_SEND_FILENAME,
        ),
        (SnapshotArtifact::EncryptedSnapshotVolume, "snapshot.luks"),
        (SnapshotArtifact::EncryptedRootfsCow, "cow.luks"),
    ] {
        let local_path = stage.snapshot_dir.join(local_name);
        if !local_path.exists() {
            continue;
        }
        let s3_uri = restore_artifact_uri(&stage.config, artifact.file_name());
        let replacement = PullReplacementGuard::for_existing(&local_path, &s3_uri);
        let retry_path = replacement.download_path(&local_path);
        if retry_path.exists() {
            continue;
        }
        if let Err(error) = move_verified_replica(&local_path, &retry_path) {
            tracing::warn!(
                sandbox_id = %stage.meta.sandbox_id,
                artifact = local_name,
                error = %error,
                "failed to preserve completed restore artifact for retry"
            );
        }
    }
}

/// Delegate failed restore cleanup to the node-manager's artifact owner.
/// Scoped transfers retain staging until the durable recovery owner has
/// cancelled volume-manager work. In-place restores remove only instance
/// targets; snapshot inputs and unrelated files remain for retry or lifecycle
/// cleanup. The Restoring row remains available for recovery.
async fn cleanup_partial_restore_artifacts(
    sandbox_dir: &Path,
    sandbox_id: &str,
    target: pplx_space_node_mgr_client::execution_fence::InstanceTarget,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
) {
    use pplx_space_node_mgr_client::execution_fence::InstanceTarget;
    let scope = match target {
        InstanceTarget::Execution(_) => None,
        InstanceTarget::RestoreScope(scope) => Some(scope),
    };
    cleanup_partial_restore_artifacts_with(
        sandbox_dir,
        sandbox_id,
        scope,
        crate::btrfs_via_nodemgr::delete_sandbox_instance(sandbox_id, target, node_manager),
    )
    .await;
}

async fn cleanup_partial_restore_artifacts_with(
    sandbox_dir: &Path,
    sandbox_id: &str,
    transfer_scope_id: Option<pplx_space_common_core::types::TransferScopeId>,
    cleanup: impl std::future::Future<
        Output = Result<(), crate::btrfs_via_nodemgr::BtrfsViaNodemgrError>,
    >,
) {
    if let Some(scope) = transfer_scope_id {
        // The recovery owner cancels volume-manager transfers and confirms
        // node-manager cleanup before removing the scope. A failed pull is not
        // proof that every accepted download stopped writing.
        tracing::info!(sandbox_id, %scope, "retaining failed restore scope for owned recovery cleanup");
        return;
    }
    if let Err(error) = cleanup.await {
        // An RPC response never transfers a recursive deletion capability back
        // to this process. The owning daemon retains exclusion through cleanup.
        tracing::warn!(sandbox_id, path = %sandbox_dir.display(), %error, "restore artifact cleanup incomplete; retained for retry");
    }
}

/// Codec dispatch for the restore-side rootfs artifact. Encodes the
/// decision drawn from the on-disk sidecar's `rootfs_diff_format`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreCodec {
    /// Full `rootfs.ext4`, identified by an absent `rootfs_diff_format`.
    /// Both retained historical snapshots and current full captures use it.
    Full,
    /// `btrfs send -p` stream (`rootfs.btrfs-send` on the wire).
    BtrfsSendV1,
}

impl RestoreCodec {
    fn as_str(self) -> &'static str {
        match self {
            Self::Full => pplx_space_metrics::snapshot::CODEC_FULL,
            Self::BtrfsSendV1 => pplx_space_metrics::snapshot::CODEC_BTRFS_SEND_V1,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("snapshot rootfs codec is unsupported")]
struct UnsupportedRestoreCodec;

/// An absent marker means full rootfs bytes. An unknown marker cannot prove
/// that representation; reject it before fetching or applying the wrong file.
fn codec_from_meta(
    meta: &crate::snapshot_upload::SnapshotMeta,
) -> Result<RestoreCodec, UnsupportedRestoreCodec> {
    match meta.rootfs_diff_format.as_deref() {
        Some(crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1) => {
            Ok(RestoreCodec::BtrfsSendV1)
        }
        None => Ok(RestoreCodec::Full),
        Some(_) => Err(UnsupportedRestoreCodec),
    }
}

/// Pull a single suspend-snapshot artifact from S3 via vol-mgr into
/// `local_path`. Pull path is the same for every codec — only the
/// orchestration around which artifacts to fetch varies.
async fn pull_artifact(
    identity: RestoreTransferIdentity<'_>,
    config: &SnapshotUploadConfig,
    local_path: PathBuf,
    kind: SnapshotArtifact,
    transfer_scope_id: Option<pplx_space_common_core::types::TransferScopeId>,
) -> Result<(), RestoreError> {
    let file_name = kind.file_name();
    pull_artifact_inner(
        identity,
        config,
        local_path,
        kind,
        file_name,
        snapshot_pull_priority(kind),
        transfer_scope_id,
    )
    .instrument(tracing::info_span!(
        "sbxmgr.start.stage",
        sandbox_id = identity.sandbox_id,
        stage = "restore.snapshot.artifact_pull",
        otel.name = %format!("restore.snapshot.artifact_pull:{file_name}"),
        artifact = file_name,
    ))
    .await
}

fn snapshot_pull_priority(kind: SnapshotArtifact) -> i32 {
    match kind {
        SnapshotArtifact::Rootfs
        | SnapshotArtifact::RootfsBtrfsSend
        | SnapshotArtifact::Memory
        | SnapshotArtifact::VmState
        | SnapshotArtifact::EncryptedSnapshotVolume
        | SnapshotArtifact::EncryptedRootfsCow
        | SnapshotArtifact::Metadata => RESTORE_PRIORITY,
    }
}

async fn pull_artifact_inner(
    identity: RestoreTransferIdentity<'_>,
    config: &SnapshotUploadConfig,
    local_path: PathBuf,
    kind: SnapshotArtifact,
    file_name: &'static str,
    priority: i32,
    transfer_scope_id: Option<pplx_space_common_core::types::TransferScopeId>,
) -> Result<(), RestoreError> {
    let sandbox_id = identity.sandbox_id;
    let parent = local_path
        .parent()
        .ok_or_else(|| RestoreError::Other("snapshot pull destination has no parent".to_owned()))?;
    let prepare_parent = parent.to_owned();
    tokio::task::spawn_blocking(move || crate::transfer_directory::prepare(&prepare_parent))
        .await
        .map_err(|source| {
            RestoreError::Other(format!("join transfer directory preparation: {source}"))
        })?
        .map_err(|source| RestoreError::CreateDirectory {
            path: parent.to_owned(),
            source,
        })?;
    let s3_uri = restore_artifact_uri(config, file_name);
    let local_path_str = local_path.to_string_lossy().into_owned();
    let adopted = adopt_completed_replacement_temp(sandbox_id, &local_path, &s3_uri);
    let mut replacement = replacement_for_pull(&local_path, &s3_uri, adopted);
    sweep_stale_pull_replacements(
        sandbox_id,
        &local_path,
        replacement.spare_basename().as_deref(),
    );
    let download_path = replacement.download_path(&local_path);

    emit_sandbox_event(
        sandbox_id,
        SandboxEventType::SnapshotArtifactPullStarted,
        SandboxEventSource::SbxManager,
        SandboxEventDetails::default()
            .artifact(file_name)
            .local_path(local_path_str.clone())
            .object_uri(s3_uri.clone()),
    );
    let volmgr = match pplx_space_volmgr_client::connect(VOLMGR_SOCKET).await {
        Ok(volmgr) => volmgr,
        Err(error) => {
            emit_sandbox_event(
                sandbox_id,
                SandboxEventType::SnapshotArtifactPullFailed,
                SandboxEventSource::SbxManager,
                SandboxEventDetails::default()
                    .artifact(file_name)
                    .local_path(local_path_str.clone())
                    .object_uri(s3_uri)
                    .reason("vol_mgr_connect")
                    .error(error.to_string()),
            );
            return Err(RestoreError::Other(format!("vol-mgr connect: {error}")));
        }
    };
    let output = match ensure_file_replica_with_config(
        volmgr,
        PullFileRequest {
            subject_id: format!("sandbox:{}", identity.credential_subject_id),
            s3_uri: s3_uri.clone(),
            // The sibling temp is deterministic per source URI, so a
            // retried attempt submits the identical verified PullFile:
            // vol-mgr dedups in-flight work and only adopts an existing
            // completed temp when its provenance matches this object and
            // transfer contract.
            local_path: download_path.to_string_lossy().into_owned(),
            // Snapshot files round-trip through vol-mgr's symmetric
            // compress-on-push / decompress-on-pull path, so we want
            // the transparent decompress that `raw=false` selects.
            raw: false,
            file_mode: None,
            priority,
            snapshot_artifact: snapshot_artifact_to_proto(kind) as i32,
            transfer_scope_id: transfer_scope_id.map(|scope| scope.to_string()),
            ..Default::default()
        },
        EnsureFileReplicaWaitConfig {
            max_total: RESTORE_TRANSFER_WAIT_MAX_TOTAL,
        },
    )
    .await
    {
        Ok(output) => output,
        Err(error) => {
            let (reason, job_id) = ensure_file_replica_error_details(&error);
            let mut details = SandboxEventDetails::default()
                .artifact(file_name)
                .local_path(local_path_str.clone())
                .object_uri(s3_uri.clone())
                .reason(reason)
                .error(error.to_string());
            if let Some(job_id) = job_id {
                details = details.transfer_job_id(job_id.to_string());
            }
            emit_sandbox_event(
                sandbox_id,
                SandboxEventType::SnapshotArtifactPullFailed,
                SandboxEventSource::SbxManager,
                details,
            );
            return Err(restore_error_from_ensure_file_replica(file_name, error));
        }
    };
    let bytes_downloaded = output.bytes_present;
    replacement.commit(&local_path)?;
    // Re-sweep after the commit: a zombie job for a superseded URI that
    // was between pipeline stages during the pre-pull sweep may have
    // re-materialized its temp since. Nothing is spared — the current
    // URI's temp was just consumed by the commit rename, and a
    // late-landing duplicate of it is equally dead.
    sweep_stale_pull_replacements(sandbox_id, &local_path, None);

    // Log the *on-disk* filename (basename of `local_path`) rather
    // than the wire `file_name`. Operators reading this line correlate
    // with what they see in the snapshot dir; xnode-snapshot-full.sh
    // and other harness tests grep for the on-disk shape, e.g.
    // `artifact="rootfs.ext4"` rather than the wire-side `rootfs`.
    let on_disk = local_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(file_name);
    tracing::info!(
        sandbox_id,
        artifact = on_disk,
        bytes = bytes_downloaded,
        local_path = %local_path_str,
        s3_uri = %s3_uri,
        "snapshot artifact restored from S3",
    );
    emit_sandbox_event(
        sandbox_id,
        SandboxEventType::SnapshotArtifactPullCompleted,
        SandboxEventSource::SbxManager,
        SandboxEventDetails::default()
            .artifact(on_disk)
            .bytes_transferred(bytes_downloaded.to_string())
            .local_path(local_path_str)
            .object_uri(s3_uri),
    );
    Ok(())
}

fn restore_artifact_uri(config: &SnapshotUploadConfig, file_name: &str) -> String {
    let dir_prefix = config.key_prefix.trim_matches('/');
    let key = if dir_prefix.is_empty() {
        file_name.to_string()
    } else {
        format!("{dir_prefix}/{file_name}")
    };
    build_artifact_uri(&key, config)
}

/// Invert [`restore_artifact_uri`] for a verified artifact location while
/// preserving the exact validated query string used by volume-manager.
pub(crate) fn restore_source_from_artifact_location(
    artifact_uri: &ExactStorageUri,
    artifact: SnapshotArtifact,
) -> Result<ExactStorageUri, RestoreArtifactLocationError> {
    let artifact_uri_wire = artifact_uri.as_wire();
    let expected_artifact = artifact.file_name();
    if artifact_uri.parsed().key_prefix.rsplit('/').next() != Some(expected_artifact) {
        return Err(RestoreArtifactLocationError::ArtifactMismatch {
            uri: artifact_uri_wire.to_string(),
            expected_artifact,
        });
    }
    let (object_path, query) = artifact_uri_wire.split_once('?').ok_or_else(|| {
        RestoreArtifactLocationError::MissingQuery {
            uri: artifact_uri_wire.to_string(),
        }
    })?;
    let suffix = format!("/{expected_artifact}");
    let base_path = object_path.strip_suffix(&suffix).ok_or_else(|| {
        RestoreArtifactLocationError::InvalidPathShape {
            uri: artifact_uri_wire.to_string(),
            expected_artifact,
        }
    })?;
    let base_uri = format!("{base_path}?{query}");
    base_uri
        .parse()
        .map_err(|source| RestoreArtifactLocationError::InvalidBase {
            uri: artifact_uri_wire.to_string(),
            source,
        })
}

fn restore_error_from_ensure_file_replica(
    file_name: &str,
    error: EnsureFileReplicaError,
) -> RestoreError {
    let message = format!("vol-mgr ensure_file_replica {file_name}: {error}");
    match error {
        EnsureFileReplicaError::Submit(status)
            if status.code() == tonic::Code::ResourceExhausted =>
        {
            let details = tonic::codegen::Bytes::copy_from_slice(status.details());
            let metadata = status.metadata().clone();
            RestoreError::AdmissionRejected(tonic::Status::with_details_and_metadata(
                tonic::Code::ResourceExhausted,
                message,
                details,
                metadata,
            ))
        }
        _ => RestoreError::Other(message),
    }
}

fn ensure_file_replica_error_details(
    error: &EnsureFileReplicaError,
) -> (&'static str, Option<&JobId>) {
    match error {
        EnsureFileReplicaError::InvalidJobId(_) => ("vol_mgr_invalid_job_id", None),
        EnsureFileReplicaError::Submit(_) => ("vol_mgr_submit_pull", None),
        EnsureFileReplicaError::Wait(_) => ("vol_mgr_job_wait", None),
        EnsureFileReplicaError::EmptyJob { job_id } => ("vol_mgr_job_missing", Some(job_id)),
        EnsureFileReplicaError::Failed { job_id, .. } => ("vol_mgr_job_failed", Some(job_id)),
        EnsureFileReplicaError::Cancelled { job_id } => ("vol_mgr_job_cancelled", Some(job_id)),
        EnsureFileReplicaError::DeadlineExceeded { job_id, .. } => {
            ("vol_mgr_job_wait_deadline_exceeded", Some(job_id))
        }
        EnsureFileReplicaError::UnknownStatus { job_id, .. } => {
            ("vol_mgr_job_unknown_status", Some(job_id))
        }
    }
}

/// Run restore-staging transfers concurrently: an optional template
/// materialization plus the codec's artifact pulls.
///
/// Every future runs to completion regardless of sibling failures — no
/// cancel-on-first-error. Cancelling an in-flight pull would strand its
/// deterministic replacement temp in an arbitrary state and lose the
/// temp-adoption/dedupe cooperation retried restores rely on; letting
/// each transfer finish (or fail) on its own keeps retries cheap.
/// Errors surface with template-ensure first (it gates the receive/clone
/// step), then pulls in submission order, so diagnostics stay stable
/// across runs regardless of completion timing.
async fn run_staging_transfers(
    template_ensure: Option<BoxFuture<'_, Result<(), RestoreError>>>,
    pulls: Vec<BoxFuture<'_, Result<(), RestoreError>>>,
) -> Result<(), RestoreError> {
    let template = async {
        match template_ensure {
            Some(ensure) => ensure.await,
            None => Ok(()),
        }
    };
    let (template_result, pull_results) = tokio::join!(template, join_all(pulls));
    template_result?;
    pull_results.into_iter().collect()
}

/// Restore-side staging for the historical rootfs codec. Full artifacts pull
/// vmstate, memory, and rootfs; disk artifacts pull only rootfs. Both reflink
/// the rootfs into the instance directory.
async fn stage_full_codec_artifacts(
    identity: RestoreTransferIdentity<'_>,
    stage: &RestoreMetadataStaged,
    target: pplx_space_node_mgr_client::execution_fence::InstanceTarget,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
) -> Result<(), RestoreError> {
    let sandbox_id = identity.sandbox_id;
    let config = &stage.config;
    let snapshot_dir = &stage.snapshot_dir;
    let instance_rootfs = stage.instance_dir.join("rootfs.ext4");
    let instance_rootfs = instance_rootfs.as_path();
    let transfer_scope_id = stage.transfer_scope_id;
    let include_runtime_state =
        crate::snapshot_upload::artifact_kind_from_meta(&stage.meta) == ArtifactKind::Full;
    let artifacts = full_codec_artifacts(include_runtime_state);
    let pulls = artifacts
        .into_iter()
        .map(|(artifact, local_name)| -> BoxFuture<'_, _> {
            Box::pin(pull_artifact(
                identity,
                config,
                snapshot_dir.join(local_name),
                artifact,
                transfer_scope_id,
            ))
        })
        .collect();
    run_staging_transfers(None, pulls).await?;

    // `prepare_sandbox` skips the template clone when
    // `instance/rootfs.ext4` is present, so seed it with a btrfs reflink
    // of the snapshot rootfs to preserve CoW semantics.
    let snapshot_rootfs = snapshot_dir.join("rootfs.ext4");
    let rootfs_metadata = tokio::fs::symlink_metadata(&snapshot_rootfs)
        .await
        .map_err(|error| RestoreError::Other(format!("inspect restored rootfs: {error}")))?;
    if !rootfs_metadata.file_type().is_file() {
        return Err(RestoreError::Other(
            "restored rootfs must be a regular file".to_string(),
        ));
    }
    pplx_space_common_core::sandbox_storage::validate_rootfs_bytes(rootfs_metadata.len())
        .map_err(RestoreError::RootfsPolicy)?;
    // Retained instances are root-owned after execution cleanup. Replace them
    // through their fenced owner only after every source artifact is staged and
    // validated. Never grant sandbox-manager write access to an active instance.
    crate::btrfs_via_nodemgr::delete_sandbox_instance(sandbox_id, target, node_manager)
        .await
        .map_err(|source| RestoreError::ReplaceInstance { source })?;
    let instance_dir = instance_rootfs
        .parent()
        .ok_or_else(|| RestoreError::Other("restored rootfs has no instance directory".into()))?;
    std::fs::create_dir_all(instance_dir).map_err(|error| {
        RestoreError::Other(format!("create restored instance directory: {error}"))
    })?;
    {
        let span = tracing::info_span!(
            "sbxmgr.start.stage",
            sandbox_id,
            stage = "restore.snapshot.reflink_rootfs",
            otel.name = "restore.snapshot.reflink_rootfs",
            codec = RestoreCodec::Full.as_str(),
        );
        let _entered = span.enter();
        pplx_space_node_runtime_core::reflink_copy(&snapshot_rootfs, instance_rootfs).map_err(
            |e| {
                RestoreError::Other(format!(
                    "reflink {} -> {}: {e}",
                    snapshot_rootfs.display(),
                    instance_rootfs.display(),
                ))
            },
        )?;
    }

    Ok(())
}

fn full_codec_artifacts(include_runtime_state: bool) -> Vec<(SnapshotArtifact, &'static str)> {
    let mut artifacts = Vec::with_capacity(if include_runtime_state { 3 } else { 1 });
    if include_runtime_state {
        artifacts.extend([
            (SnapshotArtifact::VmState, "vmstate"),
            (SnapshotArtifact::Memory, "memory"),
        ]);
    }
    artifacts.push((SnapshotArtifact::Rootfs, "rootfs.ext4"));
    artifacts
}

/// Restore-side staging for a BYOK encrypted snapshot.
///
/// Pulls the two sealed ciphertext artifacts into the snapshot dir:
/// `snapshot.luks` (the vmstate+memory volume) and `cow.luks` (the encrypted
/// rootfs delta). Both stay in the sbx-mgr-accessible snapshot dir; the launch
/// path hands their paths to vol-mgr, which (privileged) lays the COW down at
/// `instance/cow.luks` and opens the snapshot volume from `snapshot.luks`. The
/// plaintext dm-snapshot origin is cloned from the local template by
/// `prepare_sandbox`, exactly as on a fresh create — so unlike the Full/
/// btrfs-send codecs there is no rootfs artifact to download here.
async fn stage_encrypted_restore(
    identity: RestoreTransferIdentity<'_>,
    config: &SnapshotUploadConfig,
    snapshot_dir: &Path,
    transfer_scope_id: Option<pplx_space_common_core::types::TransferScopeId>,
    artifact_kind: ArtifactKind,
) -> Result<(), RestoreError> {
    std::fs::create_dir_all(snapshot_dir)
        .map_err(|e| RestoreError::Other(format!("mkdir {}: {e}", snapshot_dir.display())))?;
    let pulls = crate::snapshot_upload::encrypted_artifacts(artifact_kind)
        .into_iter()
        .map(|artifact| -> BoxFuture<'_, _> {
            Box::pin(pull_artifact(
                identity,
                config,
                snapshot_dir.join(artifact.local_name),
                artifact.kind,
                transfer_scope_id,
            ))
        });
    run_staging_transfers(None, pulls.collect()).await
}

/// Restore-side staging for the `btrfs-send-v1` codec.
///
/// Reconstructs the sandbox `instance/` subvolume by piping a
/// `rootfs.btrfs-send` stream pulled from S3 through `btrfs receive`
/// against the local template subvolume — the same template
/// `(name, content_hash)` the source-node suspend handler ran
/// `btrfs send -p` against. Cross-node portability rides on the
/// deterministic `received_uuid` stamped on every node's local
/// template subvolume by the tmplmgr install path.
///
/// Preconditions: the snapshot sidecar names a template with a
/// `content_hash`; that template can be on-demand-pulled by the local
/// and the local copy is a btrfs subvolume with the expected
/// deterministic `received_uuid`. Failure on any of these surfaces as
/// `RestoreError::Other` with a precise diagnostic — the caller is
/// responsible for cleanup of the partial staging directory.
#[expect(
    clippy::too_many_arguments,
    reason = "each argument is an independent restore input; the node-manager and template-manager clients are injected"
)]
async fn stage_btrfs_send_restore(
    identity: RestoreTransferIdentity<'_>,
    owner: &pplx_space_common_core::types::SandboxOwner,
    config: &SnapshotUploadConfig,
    sidecar_path: &Path,
    snapshot_dir: &Path,
    artifact_kind: ArtifactKind,
    transfer_scope_id: Option<pplx_space_common_core::types::TransferScopeId>,
    target: pplx_space_node_mgr_client::execution_fence::InstanceTarget,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
    templates: &dyn crate::template_resolver::TemplateResolver,
) -> Result<(), RestoreError> {
    let sandbox_id = identity.sandbox_id;
    let sidecar_bytes = std::fs::read(sidecar_path).map_err(|e| {
        RestoreError::Other(format!("read sidecar {}: {e}", sidecar_path.display()))
    })?;
    let sidecar: crate::snapshot_upload::SnapshotMeta = serde_json::from_slice(&sidecar_bytes)
        .map_err(|e| {
            RestoreError::Other(format!("parse sidecar {}: {e}", sidecar_path.display()))
        })?;

    let template_name = &sidecar.source.template_id;
    let content_hash = sidecar.source.content_hash.as_deref().ok_or_else(|| {
        RestoreError::Other(format!(
            "btrfs-send snapshot for sandbox {sandbox_id} has no source.content_hash; \
             cannot identify the parent template subvolume"
        ))
    })?;

    // Template and rootfs availability gate receive; runtime-state downloads
    // are independent. Keep them running while node-manager reconstructs the
    // filesystem, then join all work before publishing or cleaning staging.
    //
    // tmplmgr's `GetTemplate` transparently triggers an on-demand S3
    // pull when the template isn't cached, so this works the first time
    // a node sees the template just as well as on the cache-warm path.
    // tmplmgr's pull path goes through node-manager for the privileged
    // subvolume + received_uuid stamping; on completion the template is
    // a btrfs subvolume with the deterministic UUIDv5 stamped.
    //
    // The btrfs-send codec always carries rootfs.btrfs-send. Full artifacts
    // additionally carry binfile + memfile; disk artifacts cold-boot and must
    // not require runtime-state blobs. metadata.json was pulled by the
    // dispatcher.
    let mut rootfs_pulls = Vec::with_capacity(1);
    let mut runtime_pulls = Vec::with_capacity(2);
    for (artifact, local_name) in btrfs_restore_artifacts(artifact_kind) {
        let pull: BoxFuture<'_, _> = Box::pin(pull_artifact(
            identity,
            config,
            snapshot_dir.join(local_name),
            artifact,
            transfer_scope_id,
        ));
        if artifact == SnapshotArtifact::RootfsBtrfsSend {
            rootfs_pulls.push(pull);
        } else {
            runtime_pulls.push(pull);
        }
    }
    run_btrfs_staging(
        Box::pin(async {
            ensure_local_template(templates, sandbox_id, owner, template_name, content_hash)
                .await
                .map(drop)
        }),
        rootfs_pulls,
        runtime_pulls,
        Box::pin(async {
            // Node-manager retains privileged receive and execution fencing.
            crate::btrfs_via_nodemgr::receive_instance(
                sandbox_id,
                template_name,
                content_hash,
                target,
                node_manager,
            )
            .instrument(tracing::info_span!(
                "sbxmgr.start.stage",
                sandbox_id,
                stage = "restore.snapshot.btrfs_receive",
                otel.name = "restore.snapshot.btrfs_receive",
                codec = RestoreCodec::BtrfsSendV1.as_str(),
            ))
            .await
            .map_err(|source| RestoreError::BtrfsReceive { source })
        }),
    )
    .await?;

    tracing::info!(
        sandbox_id,
        template = %template_name,
        content_hash,
        "btrfs-send codec: instance subvolume reconstructed (via nodemgr)",
    );
    Ok(())
}

fn btrfs_restore_artifacts(artifact_kind: ArtifactKind) -> Vec<(SnapshotArtifact, &'static str)> {
    let mut artifacts = Vec::with_capacity(if artifact_kind == ArtifactKind::Full {
        3
    } else {
        1
    });
    if artifact_kind == ArtifactKind::Full {
        artifacts.extend([
            (SnapshotArtifact::VmState, "vmstate"),
            (SnapshotArtifact::Memory, "memory"),
        ]);
    }
    artifacts.push((
        SnapshotArtifact::RootfsBtrfsSend,
        crate::snapshot_upload::ROOTFS_BTRFS_SEND_FILENAME,
    ));
    artifacts
}

async fn run_btrfs_staging<'a>(
    template: BoxFuture<'a, Result<(), RestoreError>>,
    rootfs_pulls: Vec<BoxFuture<'a, Result<(), RestoreError>>>,
    runtime_pulls: Vec<BoxFuture<'a, Result<(), RestoreError>>>,
    receive: BoxFuture<'a, Result<(), RestoreError>>,
) -> Result<(), RestoreError> {
    let rootfs = async {
        let (template_result, pulls) = tokio::join!(template, join_all(rootfs_pulls));
        let rootfs_result: Result<(), RestoreError> = pulls.into_iter().collect();
        let receive_result = if template_result.is_ok() && rootfs_result.is_ok() {
            receive.await
        } else {
            Ok(())
        };
        (template_result, rootfs_result, receive_result)
    };
    // Do not cancel sibling downloads or receive on failure: cleanup must only
    // run after all staging work finishes. Preserve the previous error order,
    // including which failure decides whether completed files survive retry.
    let ((template, rootfs, receive), runtime) = tokio::join!(rootfs, join_all(runtime_pulls));
    template?;
    runtime.into_iter().collect::<Result<(), RestoreError>>()?;
    rootfs?;
    receive
}

/// Ensure `(template_name, content_hash)` is locally cached by asking
/// tmplmgr — same RPC the sandbox-create path uses. Returns the local
/// directory the template lives at.
async fn ensure_local_template(
    templates: &dyn crate::template_resolver::TemplateResolver,
    sandbox_id: &str,
    owner: &pplx_space_common_core::types::SandboxOwner,
    template_name: &str,
    content_hash: &str,
) -> Result<String, RestoreError> {
    use crate::template_resolver::TemplateLookupError;
    async {
        let mut request =
            tonic::Request::new(pplx_space_template_mgr_client::proto::GetTemplateRequest {
                caller_org_id: None,
                template: Some(pplx_space_template_mgr_client::proto::TemplateRef {
                    name: template_name.to_string(),
                    content_hash: Some(content_hash.to_string()),
                    tag: None,
                }),
                vcpu: None,
                memory_mib: None,
            });
        let scope = crate::service::template_lookup_scope(owner, Some(sandbox_id))
            .map_err(|error| RestoreError::Other(error.to_string()))?;
        scope.attach(&mut request);
        let resp = templates
            .get_template(request)
            .await
            .map_err(|error| match error {
                TemplateLookupError::Connect(e) => RestoreError::Other(format!(
                    "tmplmgr connect for GetTemplate({template_name}): {e}"
                )),
                TemplateLookupError::Rpc(error) => {
                    template_pull_error(template_name, content_hash, error)
                }
            })?;
        scope
            .observe_response(&resp)
            .map_err(|error| template_pull_error(template_name, content_hash, error))?;
        let resp = resp.into_inner();
        if resp.template_dir.is_empty() {
            return Err(RestoreError::Other(format!(
                "tmplmgr returned no local template dir for {template_name}@{content_hash}"
            )));
        }
        Ok(resp.template_dir)
    }
    .instrument(tracing::info_span!(
        "sbxmgr.start.stage",
        sandbox_id,
        stage = "restore.snapshot.ensure_template",
        otel.name = "restore.snapshot.ensure_template",
        template = template_name,
        codec = RestoreCodec::BtrfsSendV1.as_str(),
    ))
    .await
}

fn template_pull_error(
    template_name: &str,
    content_hash: &str,
    error: tonic::Status,
) -> RestoreError {
    if error.code() == tonic::Code::ResourceExhausted {
        let message =
            format!("tmplmgr GetTemplate({template_name}@{content_hash}) failed: {error}");
        RestoreError::AdmissionRejected(tonic::Status::resource_exhausted(message))
    } else {
        RestoreError::ParentTemplateLookup {
            template: template_name.to_string(),
            content_hash: content_hash.to_string(),
            source: error,
        }
    }
}

#[cfg(test)]
mod tests;
