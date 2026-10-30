use pplx_space_common_core::types::LocalArtifactId;

#[cfg(test)]
use super::cleanup_support::CleanupLockGuard;
use super::cleanup_support::CleanupLockTryResult;
use super::*;

use pplx_space_common_core::types::SandboxQuarantineReasonCode;
use pplx_space_metrics::sandbox::worker::{
    InventoryReconcileOperation, InventoryReconcileOutcome, InventoryReconcileReason,
    inventory_reconcile,
};
use pplx_space_node_mgr_client::execution_fence::ExecutionFence;

mod periodic;
#[cfg(test)]
mod tests;
pub(super) use periodic::PeriodicInventoryScan;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ReconcileSource {
    Startup,
    Cleanup,
    Periodic,
    #[cfg(test)]
    Test,
}

impl ReconcileSource {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Cleanup => "cleanup",
            Self::Periodic => "periodic",
            #[cfg(test)]
            Self::Test => "reconcile-test",
        }
    }
}

impl std::fmt::Display for ReconcileSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Default)]
pub(super) struct SandboxInventoryReconcileReport {
    changed_ids: BTreeSet<SandboxId>,
    removed_ids: BTreeSet<SandboxId>,
}

impl SandboxInventoryReconcileReport {
    fn record_changed(&mut self, sandbox_id: SandboxId) {
        self.changed_ids.insert(sandbox_id);
    }

    fn record_removed(&mut self, sandbox_id: SandboxId) {
        self.changed_ids.insert(sandbox_id);
        self.removed_ids.insert(sandbox_id);
    }

    pub(super) fn changed_ids(&self) -> Vec<SandboxId> {
        self.changed_ids.iter().copied().collect()
    }
}

fn durable_inventory_reason(
    reason: crate::db::InventoryOwnershipReason,
) -> InventoryReconcileReason {
    match reason {
        crate::db::InventoryOwnershipReason::DurableCleanupEpisode => {
            InventoryReconcileReason::DurableCleanupEpisode
        }
        crate::db::InventoryOwnershipReason::RowCleanupIntent => {
            InventoryReconcileReason::RowCleanupIntent
        }
        crate::db::InventoryOwnershipReason::MalformedCleanupIntent => {
            InventoryReconcileReason::MalformedCleanupIntent
        }
        crate::db::InventoryOwnershipReason::LifecycleOwned => {
            InventoryReconcileReason::LifecycleOwned
        }
        crate::db::InventoryOwnershipReason::RestoreTransferScope => {
            InventoryReconcileReason::RestoreTransferScope
        }
    }
}

pub(super) fn ensure_sandbox_cache_root_in(
    cache_dir: &Path,
    sandbox_id: &str,
) -> Result<(), Status> {
    let sandbox_root = cache_dir.join(sandbox_id);
    std::fs::create_dir_all(&sandbox_root).map_err(|error| {
        Status::internal(format!(
            "cannot create sandbox cache root {}: {error}",
            sandbox_root.display()
        ))
    })
}

/// States whose row is not stale merely because it has no cache root or
/// runtime directory. `Quarantined` is a hold whose local evidence may be
/// gone; skipping it here also keeps every pass from taking its cleanup fence
/// and a write transaction. The guarded delete refuses it independently.
pub(super) fn sandbox_state_may_lack_cache_root(state: SandboxState) -> bool {
    matches!(
        state,
        SandboxState::Creating
            | SandboxState::Created
            | SandboxState::Suspended
            | SandboxState::Restoring
            | SandboxState::CleanupRequired
            | SandboxState::CleanupFailed
            | SandboxState::Quarantined
    )
}

fn sandbox_ids_to_strings(ids: impl IntoIterator<Item = SandboxId>) -> Vec<String> {
    ids.into_iter().map(|id| id.to_string()).collect()
}

/// Scan the sandbox cache directory for existing sandbox IDs.
/// A sandbox exists once its cache root exists. `Create` establishes that root;
/// `Start` later creates `instance/rootfs.ext4`.
pub(super) fn list_sandbox_dirs() -> Vec<SandboxId> {
    list_sandbox_dirs_in(Path::new(CACHE_DIR))
}

pub(super) fn list_sandbox_dirs_in(cache_dir: &Path) -> Vec<SandboxId> {
    let Ok(entries) = std::fs::read_dir(cache_dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter(|e| e.path().is_dir())
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            SandboxId::from_str(&name).ok()
        })
        .collect()
}

pub(crate) fn list_live_sandbox_run_dirs_in(run_dir: &Path) -> Vec<SandboxId> {
    let Ok(entries) = std::fs::read_dir(run_dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            let sandbox_id = SandboxId::from_str(&name).ok()?;
            is_sandbox_running_in(run_dir, &name).then_some(sandbox_id)
        })
        .collect()
}

fn list_live_sandbox_run_dirs() -> Vec<SandboxId> {
    list_live_sandbox_run_dirs_in(Path::new(RUN_DIR))
}

fn list_sandbox_run_dirs_in(run_dir: &Path) -> Vec<SandboxId> {
    let Ok(entries) = std::fs::read_dir(run_dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            SandboxId::from_str(&name).ok()
        })
        .collect()
}

fn list_sandbox_run_dirs() -> Vec<SandboxId> {
    list_sandbox_run_dirs_in(Path::new(RUN_DIR))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SandboxInventoryPresence {
    has_cache_root: bool,
    has_live_runtime: bool,
    has_runtime_artifact: bool,
}

#[derive(Clone, Copy)]
pub(super) struct SandboxInventoryPaths<'a> {
    cache_dir: &'a Path,
    run_dir: &'a Path,
}

impl<'a> SandboxInventoryPaths<'a> {
    pub(super) fn new(cache_dir: &'a Path, run_dir: &'a Path) -> Self {
        Self { cache_dir, run_dir }
    }
}

impl SandboxInventoryPresence {
    fn inspect(paths: SandboxInventoryPaths<'_>, sandbox_id: SandboxId) -> std::io::Result<Self> {
        let sandbox_id = sandbox_id.to_string();
        Ok(Self {
            // Unexpected files also count as retained evidence. Neither a failed
            // stat nor an unrecognized object is proof of sandbox absence.
            has_cache_root: paths.cache_dir.join(&sandbox_id).try_exists()?,
            has_live_runtime: paths
                .run_dir
                .join(&sandbox_id)
                .join("fc.pid")
                .try_exists()?,
            has_runtime_artifact: paths.run_dir.join(sandbox_id).try_exists()?,
        })
    }
}

/// Check if a sandbox's Firecracker process is alive via the fc.pid symlink.
/// The symlink points to /proc/{pid} — if the target exists, the process is alive.
pub(super) fn is_sandbox_running(sandbox_id: &str) -> bool {
    is_sandbox_running_in(Path::new(RUN_DIR), sandbox_id)
}

pub(super) fn is_sandbox_running_in(run_dir: &Path, sandbox_id: &str) -> bool {
    let link_path = run_dir.join(sandbox_id).join("fc.pid");
    // Path::exists() follows the symlink — returns true only if /proc/{pid} exists
    link_path.exists()
}

fn rootfs_size_mb(cache_dir: &Path, sandbox_id: &str) -> Option<u32> {
    let rootfs = cache_dir
        .join(sandbox_id)
        .join("instance")
        .join("rootfs.ext4");
    let bytes = std::fs::metadata(rootfs).ok()?.len();
    let mib = bytes.div_ceil(1024 * 1024);
    u32::try_from(mib).ok().filter(|mib| *mib > 0)
}

#[derive(Debug, thiserror::Error)]
pub(super) enum OrphanedSandboxRowMaterializationError {
    #[error("cache-only orphan inspection must precede database materialization")]
    MissingCacheInspection,
    #[error("cannot inspect sandbox filesystem evidence: {0}")]
    Filesystem(#[from] std::io::Error),
    #[error(transparent)]
    Sqlite(#[from] crate::db::Error),
    #[error(transparent)]
    Quarantine(#[from] crate::db::MarkQuarantinedError),
    #[error(transparent)]
    RecoveredSuspended(#[from] crate::db::InsertRecoveredSuspendedSandboxError),
    #[error(transparent)]
    RecoveredRestoring(#[from] crate::db::RestoreAdoptOrphanInsertError),
    #[error(transparent)]
    Publication(#[from] super::suspend_snapshot_publication::SuspendSnapshotPublicationError),
    #[error(transparent)]
    Validation(#[from] super::suspend_snapshot_publication::SuspendSnapshotValidationError),
    #[error(transparent)]
    RestoreProvenance(#[from] pplx_space_volmgr_client::ReadVerifiedReplicaProvenanceError),
}

impl OrphanedSandboxRowMaterializationError {
    fn is_database_error(&self) -> bool {
        match self {
            Self::Sqlite(_)
            | Self::Quarantine(crate::db::MarkQuarantinedError::Db(_))
            | Self::RecoveredSuspended(crate::db::InsertRecoveredSuspendedSandboxError::Db(_))
            | Self::RecoveredRestoring(crate::db::RestoreAdoptOrphanInsertError::Sqlite(_)) => true,
            Self::Quarantine(
                crate::db::MarkQuarantinedError::UnknownCurrentState { .. }
                | crate::db::MarkQuarantinedError::IllegalTransition(_)
                | crate::db::MarkQuarantinedError::NonQuarantinedInsert { .. },
            )
            | Self::RecoveredSuspended(
                crate::db::InsertRecoveredSuspendedSandboxError::NonSuspendedInsert { .. }
                | crate::db::InsertRecoveredSuspendedSandboxError::ArtifactPinInvariant { .. },
            )
            | Self::RecoveredRestoring(
                crate::db::RestoreAdoptOrphanInsertError::NonRestoringState { .. }
                | crate::db::RestoreAdoptOrphanInsertError::ExistingRowNotSyntheticOrphan { .. },
            )
            | Self::Publication(_)
            | Self::Validation(_)
            | Self::RestoreProvenance(_)
            | Self::MissingCacheInspection
            | Self::Filesystem(_) => false,
        }
    }
}

enum RemoteRestoreProvenanceInspection {
    Confirmed(pplx_space_common_core::types::ExactStorageUri),
    Absent,
    Unsafe { detail: String },
}

fn inspect_remote_restore_provenance(
    snapshot_dir: &Path,
    sandbox_id: SandboxId,
) -> Result<RemoteRestoreProvenanceInspection, OrphanedSandboxRowMaterializationError> {
    let metadata_path = snapshot_dir.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME);
    let provenance =
        match pplx_space_volmgr_client::read_verified_replica_provenance(&metadata_path) {
            Ok(provenance) => provenance,
            Err(error) if error.io_error_kind() == Some(std::io::ErrorKind::NotFound) => {
                return Ok(RemoteRestoreProvenanceInspection::Absent);
            }
            Err(error) if error.io_error_kind().is_some() => return Err(error.into()),
            Err(error) => {
                return Ok(RemoteRestoreProvenanceInspection::Unsafe {
                    detail: error.to_string(),
                });
            }
        };
    let expected_subject = pplx_space_common_core::types::SubjectId::Sandbox(sandbox_id);
    if provenance.subject_id != expected_subject {
        return Ok(RemoteRestoreProvenanceInspection::Unsafe {
            detail: format!(
                "restore provenance names subject {} instead of {expected_subject}",
                provenance.subject_id
            ),
        });
    }
    match crate::snapshot_restore::restore_source_from_artifact_location(
        &provenance.storage_uri,
        pplx_space_common_core::types::SnapshotArtifact::Metadata,
    ) {
        Ok(source) => Ok(RemoteRestoreProvenanceInspection::Confirmed(source)),
        Err(error) => Ok(RemoteRestoreProvenanceInspection::Unsafe {
            detail: error.to_string(),
        }),
    }
}

pub(super) enum CanonicalSuspendArtifactInspection {
    Recoverable(Box<super::suspend_snapshot_publication::ValidatedSuspendSnapshotArtifact>),
    InterruptedRestore {
        metadata: Box<super::suspend_snapshot_publication::ValidatedSuspendSnapshotMetadata>,
        detail: String,
        restore_source: pplx_space_common_core::types::ExactStorageUri,
    },
    Unsafe {
        detail: String,
    },
}

/// An unverified retained source is an error, so the orphan waits for a later
/// pass instead of being classified.
pub(super) fn inspect_canonical_suspend_artifact(
    cache_dir: &Path,
    sandbox_id: SandboxId,
    verdicts: &crate::retained_source::RetainedSourceVerdicts,
) -> Result<CanonicalSuspendArtifactInspection, OrphanedSandboxRowMaterializationError> {
    let sandbox_id_string = sandbox_id.to_string();
    let sandbox_dir = cache_dir.join(&sandbox_id_string);
    if let Some(staging_dir) =
        super::suspend_snapshot_publication::find_suspend_snapshot_staging_residue(&sandbox_dir)?
    {
        return Ok(CanonicalSuspendArtifactInspection::Unsafe {
            detail: format!(
                "interrupted suspend publication staging remains at {}",
                staging_dir.display()
            ),
        });
    }

    let snapshot_dir =
        super::suspend_snapshot_publication::canonical_suspend_snapshot_dir(&sandbox_dir);
    let metadata = match super::suspend_snapshot_publication::validate_suspend_snapshot_metadata(
        &snapshot_dir,
        Some(sandbox_id),
    ) {
        Ok(metadata) => metadata,
        Err(error) if error.is_unsafe_evidence() => {
            return Ok(CanonicalSuspendArtifactInspection::Unsafe {
                detail: error.to_string(),
            });
        }
        Err(error) => return Err(error.into()),
    };
    match metadata.validate_artifact_files(verdicts) {
        Ok(()) => match inspect_remote_restore_provenance(&snapshot_dir, sandbox_id)? {
            RemoteRestoreProvenanceInspection::Confirmed(restore_source) => {
                Ok(CanonicalSuspendArtifactInspection::InterruptedRestore {
                    metadata: Box::new(metadata),
                    detail: "complete artifact retains verified remote restore provenance"
                        .to_string(),
                    restore_source,
                })
            }
            RemoteRestoreProvenanceInspection::Absent => Ok(
                CanonicalSuspendArtifactInspection::Recoverable(Box::new(
                    metadata.into_artifact(),
                )),
            ),
            RemoteRestoreProvenanceInspection::Unsafe { detail } => {
                Ok(CanonicalSuspendArtifactInspection::Unsafe { detail })
            }
        },
        // Volume-manager downloads into a `.partial` sibling and atomically
        // renames only a complete file into its final path. Under that
        // contract an interrupted pull can produce a missing final artifact,
        // but not a non-regular or wrong-sized final artifact. If a future
        // transfer codec writes final paths in place, its incomplete shape
        // must be added here before it can be classified as retryable.
        Err(error @ super::suspend_snapshot_publication::SuspendSnapshotValidationError::MissingArtifact { .. }) => {
            let detail = error.to_string();
            match inspect_remote_restore_provenance(&snapshot_dir, sandbox_id)? {
                RemoteRestoreProvenanceInspection::Confirmed(restore_source) => {
                    Ok(CanonicalSuspendArtifactInspection::InterruptedRestore {
                        metadata: Box::new(metadata),
                        detail,
                        restore_source,
                    })
                }
                RemoteRestoreProvenanceInspection::Absent => {
                    Ok(CanonicalSuspendArtifactInspection::Unsafe {
                        detail: format!(
                            "{detail}; no verified remote restore provenance is present"
                        ),
                    })
                }
                RemoteRestoreProvenanceInspection::Unsafe { detail } => {
                    Ok(CanonicalSuspendArtifactInspection::Unsafe { detail })
                }
            }
        }
        Err(error) if error.is_unsafe_evidence() => {
            Ok(CanonicalSuspendArtifactInspection::Unsafe {
                detail: error.to_string(),
            })
        }
        Err(error) => Err(error.into()),
    }
}

enum OrphanRowMaterializationEvent {
    RecoveredSuspended {
        local_artifact_id: LocalArtifactId,
    },
    RecoveredRestoring {
        detail: String,
        local_artifact_id: LocalArtifactId,
    },
    Quarantined {
        reason_code: SandboxQuarantineReasonCode,
    },
}

struct CommittedOrphanRowMaterialization {
    disposition: pplx_space_metrics::sandbox::worker::OrphanRowDisposition,
    event: Option<OrphanRowMaterializationEvent>,
}

impl CommittedOrphanRowMaterialization {
    fn emit_event(&self, sandbox_id: SandboxId) {
        let sandbox_id = sandbox_id.to_string();
        match &self.event {
            Some(OrphanRowMaterializationEvent::RecoveredSuspended { local_artifact_id }) => {
                pplx_space_events::emit_sandbox_event(
                    &sandbox_id,
                    pplx_space_events::SandboxEventType::LifecycleConverged,
                    pplx_space_events::SandboxEventSource::SbxManager,
                    pplx_space_events::SandboxEventDetails::default()
                        .reason("recovered_local_suspend_artifact")
                        .operation("inventory_reconcile")
                        .trigger(pplx_space_events::SandboxEventTrigger::System)
                        .with_to_state(SandboxState::Suspended.to_string())
                        .local_artifact_id(local_artifact_id.to_string()),
                );
            }
            Some(OrphanRowMaterializationEvent::RecoveredRestoring {
                detail,
                local_artifact_id,
            }) => {
                pplx_space_events::emit_sandbox_event(
                    &sandbox_id,
                    pplx_space_events::SandboxEventType::SnapshotRestoreStarted,
                    pplx_space_events::SandboxEventSource::SbxManager,
                    super::create_support::mark_restore_storage_uri_present(
                        pplx_space_events::SandboxEventDetails::default()
                            .reason("recovered_interrupted_restore_inventory")
                            .operation("inventory_reconcile")
                            .trigger(pplx_space_events::SandboxEventTrigger::System)
                            .result("restoring_row_reconstructed")
                            .error(detail)
                            .with_to_state(SandboxState::Restoring.to_string())
                            .restored_snapshot_local_artifact_id(local_artifact_id.to_string()),
                    ),
                );
            }
            Some(OrphanRowMaterializationEvent::Quarantined { reason_code }) => {
                crate::db::emit_insert_quarantined_sandbox_event(&sandbox_id, *reason_code);
            }
            None => {}
        }
    }
}

fn insert_orphaned_sandbox_row_in_transaction(
    conn: &crate::db::Connection,
    sandbox_id: SandboxId,
    has_cache_root: bool,
    has_live_runtime: bool,
    rootfs_size_mb: Option<u32>,
    cache_inspection: Option<CanonicalSuspendArtifactInspection>,
    source: ReconcileSource,
) -> Result<CommittedOrphanRowMaterialization, OrphanedSandboxRowMaterializationError> {
    let sandbox_id_string = sandbox_id.to_string();
    let uuid8 = SandboxRuntimeLabel::from_sandbox_id(sandbox_id);
    if has_cache_root && !has_live_runtime {
        return match cache_inspection
            .ok_or(OrphanedSandboxRowMaterializationError::MissingCacheInspection)?
        {
            CanonicalSuspendArtifactInspection::Recoverable(artifact) => {
                artifact.row.with_insert_params(
                    &sandbox_id_string,
                    &uuid8,
                    SandboxState::Suspended,
                    rootfs_size_mb,
                    |params| {
                        crate::db::insert_recovered_suspended_sandbox_in_transaction(
                            conn,
                            params,
                            &artifact.execution,
                            artifact.local_artifact_id,
                            &artifact.snapshot_dir.to_string_lossy(),
                        )
                    },
                )?;
                Ok(CommittedOrphanRowMaterialization {
                    disposition: pplx_space_metrics::sandbox::worker::OrphanRowDisposition::RecoveredSuspended,
                    event: Some(OrphanRowMaterializationEvent::RecoveredSuspended {
                        local_artifact_id: artifact.local_artifact_id,
                    }),
                })
            }
            CanonicalSuspendArtifactInspection::InterruptedRestore {
                metadata,
                detail,
                restore_source,
            } => {
                let local_artifact_id = metadata.local_artifact_id;
                let restore_operation =
                    crate::restore_operation::RemoteRestoreOperation::artifact_staging(
                        restore_source,
                    );
                metadata.row.with_insert_params(
                    &sandbox_id_string,
                    &uuid8,
                    SandboxState::Restoring,
                    rootfs_size_mb,
                    |params| {
                        crate::db::restore_adopt_orphan_or_insert_sandbox_in_transaction(
                            conn,
                            params,
                            &restore_operation,
                        )
                    },
                )?;
                Ok(CommittedOrphanRowMaterialization {
                    disposition: pplx_space_metrics::sandbox::worker::OrphanRowDisposition::RecoveredRestoring,
                    event: Some(OrphanRowMaterializationEvent::RecoveredRestoring {
                        detail,
                        local_artifact_id,
                    }),
                })
            }
            CanonicalSuspendArtifactInspection::Unsafe { detail } => {
                let reason_code = SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
                let reason = format!(
                    "{source}: sandbox cache exists without an sbx-manager metadata row or live runtime and cannot be safely recovered: {detail}"
                );
                crate::db::insert_quarantined_sandbox_in_transaction(
                    conn,
                    crate::db::InsertSandboxParams {
                        guest_image: Default::default(),
                        id: &sandbox_id_string,
                        uuid8: &uuid8,
                        state: SandboxState::Quarantined,
                        template: ORPHANED_SANDBOX_TEMPLATE,
                        template_hash: None,
                        user: &ORPHANED_SANDBOX_OWNER,
                        runtime: RuntimeKind::Firecracker.alias(),
                        snapshots_enabled: false,
                        rootfs_size_mb,
                        disk_io_limits: Default::default(),
                        pool_template: None,
                        volume_encryption: false,
                        process_env: &BTreeMap::new(),
                        auto_lifecycle: &AutoLifecyclePolicy::disabled(),
                        network_policy_override: None,
                        network_policy_generation: 0,
                        network_policy_authority: crate::db::PolicyAuthority::Cluster,
                        auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
                    },
                    pplx_space_common_core::types::SandboxQuarantineSource::SbxManager,
                    reason_code,
                    &reason,
                    pplx_space_common_core::types::SandboxQuarantineNextAction::RequiresOperatorAttention,
                )?;
                Ok(CommittedOrphanRowMaterialization {
                    disposition:
                        pplx_space_metrics::sandbox::worker::OrphanRowDisposition::QuarantinedRow,
                    event: Some(OrphanRowMaterializationEvent::Quarantined { reason_code }),
                })
            }
        };
    }

    let runtime = RuntimeKind::Firecracker;
    crate::db::insert_sandbox(
        conn,
        crate::db::InsertSandboxParams {
            guest_image: Default::default(),
            id: &sandbox_id_string,
            uuid8: &uuid8,
            state: SandboxState::CleanupRequired,
            template: ORPHANED_SANDBOX_TEMPLATE,
            template_hash: None,
            user: &ORPHANED_SANDBOX_OWNER,
            runtime: runtime.alias(),
            snapshots_enabled: false,
            rootfs_size_mb,
            disk_io_limits: Default::default(),
            pool_template: None,
            volume_encryption: false,
            process_env: &BTreeMap::new(),
            auto_lifecycle: &AutoLifecyclePolicy::disabled(),
            network_policy_override: None,
            network_policy_generation: 0,
            network_policy_authority: crate::db::PolicyAuthority::Cluster,
            auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
        },
    )?;
    let reason = format!(
        "{source}: sandbox exists on disk/runtime but sbx-manager metadata row was missing"
    );
    let disposition = if has_live_runtime {
        crate::db::mark_cleanup_required(
            conn,
            &sandbox_id_string,
            true,
            crate::db::CleanupCompletion::Quarantine,
            &reason,
        )?;
        pplx_space_metrics::sandbox::worker::OrphanRowDisposition::CleanupQuarantine
    } else if !has_cache_root {
        crate::db::mark_cleanup_required(
            conn,
            &sandbox_id_string,
            false,
            crate::db::CleanupCompletion::DeleteRow,
            &reason,
        )?;
        pplx_space_metrics::sandbox::worker::OrphanRowDisposition::DeleteRow
    } else {
        unreachable!("cache-only orphans return through the quarantine branch")
    };
    Ok(CommittedOrphanRowMaterialization {
        disposition,
        event: None,
    })
}

fn sandbox_has_import_staging(cache_dir: &Path, sandbox_id: SandboxId) -> bool {
    let sandbox_root = cache_dir.join(sandbox_id.to_string());
    sandbox_root.join("instance.importing").exists()
        || sandbox_root.join("snapshot.importing").exists()
        || sandbox_root.join("instance.new").exists()
}

pub(super) fn reclaim_abandoned_restore_metadata_staging<StagingGuard>(
    cache_dir: &Path,
    source: ReconcileSource,
    try_restore_staging_guard: &impl Fn(SandboxId) -> Option<StagingGuard>,
) {
    let Ok(entries) = std::fs::read_dir(cache_dir) else {
        return;
    };
    for entry in entries.flatten() {
        if !entry.path().is_dir() {
            continue;
        }
        let Some(sandbox_id) =
            crate::snapshot_restore::remote_restore_metadata_staging_sandbox_id(&entry.file_name())
        else {
            continue;
        };
        let Some(_staging_guard) = try_restore_staging_guard(sandbox_id) else {
            continue;
        };
        match std::fs::remove_dir_all(entry.path()) {
            Ok(()) => tracing::info!(
                source = %source,
                sandbox_id = %sandbox_id,
                "removed abandoned pre-row restore metadata staging"
            ),
            Err(error) => tracing::warn!(
                source = %source,
                sandbox_id = %sandbox_id,
                %error,
                "failed to remove abandoned pre-row restore metadata staging"
            ),
        }
    }
}

pub(super) trait InventoryDbAccess {
    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&crate::db::Connection) -> T,
    ) -> Result<T, InventoryError>;
}

#[derive(Debug, thiserror::Error)]
pub enum InventoryError {
    #[error("inventory database mutex is poisoned")]
    DatabaseMutexPoisoned,
    #[error(transparent)]
    Database(#[from] crate::db::Error),
}

enum OrphanRowMaterialization {
    Materialized(CommittedOrphanRowMaterialization),
    DatabaseRowAppeared,
    InventoryChanged,
    RestoreTransferScope,
}

enum StaleRowInventoryMutation {
    FilesystemChanged(SandboxInventoryPresence),
    InspectionFailed(std::io::Error),
    Database(crate::db::InventoryStaleRowMutation),
}

impl InventoryDbAccess for crate::db::Connection {
    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&crate::db::Connection) -> T,
    ) -> Result<T, InventoryError> {
        Ok(operation(self))
    }
}

impl InventoryDbAccess for crate::db::SharedConnection {
    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&crate::db::Connection) -> T,
    ) -> Result<T, InventoryError> {
        let conn = self
            .lock()
            .map_err(|_| InventoryError::DatabaseMutexPoisoned)?;
        Ok(operation(&conn))
    }
}

pub(super) struct InventoryOwnershipGuards<
    'a,
    CleanupGuard,
    StagingGuard,
    Retirement = fn(&str) -> Option<fn()>,
> {
    try_cleanup_guard: CleanupGuard,
    try_restore_staging_guard: StagingGuard,
    try_retirement: Retirement,
    observe_execution: &'a dyn Fn(SandboxId) -> Result<Option<ExecutionFence>, Status>,
    /// Node-manager verdicts gathered before this pass. Without them, orphans
    /// with retained sources wait for a pass that has them.
    retained_sources: Option<&'a crate::retained_source::RetainedSourceVerdicts>,
}

impl<'a, CleanupGuard, StagingGuard> InventoryOwnershipGuards<'a, CleanupGuard, StagingGuard> {
    pub(super) fn new(
        try_cleanup_guard: CleanupGuard,
        try_restore_staging_guard: StagingGuard,
        observe_execution: &'a dyn Fn(SandboxId) -> Result<Option<ExecutionFence>, Status>,
    ) -> Self {
        Self {
            try_cleanup_guard,
            try_restore_staging_guard,
            try_retirement: |_| Some(|| {}),
            observe_execution,
            retained_sources: None,
        }
    }

    fn with_retirement<R>(
        self,
        try_retirement: R,
    ) -> InventoryOwnershipGuards<'a, CleanupGuard, StagingGuard, R> {
        InventoryOwnershipGuards {
            try_cleanup_guard: self.try_cleanup_guard,
            try_restore_staging_guard: self.try_restore_staging_guard,
            try_retirement,
            observe_execution: self.observe_execution,
            retained_sources: self.retained_sources,
        }
    }
}

impl<'a, CleanupGuard, StagingGuard, Retirement>
    InventoryOwnershipGuards<'a, CleanupGuard, StagingGuard, Retirement>
{
    pub(super) fn with_retained_sources(
        mut self,
        verdicts: &'a crate::retained_source::RetainedSourceVerdicts,
    ) -> Self {
        self.retained_sources = Some(verdicts);
        self
    }
}

pub(super) fn reconcile_sandbox_inventory_ids<
    Db,
    CleanupGuard,
    StagingGuard,
    RestoreStagingGuard,
    Retirement,
    RetirementGuard,
>(
    db: &Db,
    paths: SandboxInventoryPaths<'_>,
    disk_ids: Vec<SandboxId>,
    live_ids: Vec<SandboxId>,
    runtime_artifact_ids: Vec<SandboxId>,
    source: ReconcileSource,
    guards: InventoryOwnershipGuards<'_, CleanupGuard, StagingGuard, Retirement>,
) -> Result<SandboxInventoryReconcileReport, InventoryError>
where
    Db: InventoryDbAccess,
    CleanupGuard: Fn(&str) -> CleanupLockTryResult,
    StagingGuard: Fn(SandboxId) -> Option<RestoreStagingGuard>,
    Retirement: Fn(&str) -> Option<RetirementGuard>,
    RetirementGuard: FnOnce(),
{
    let rows = db.with_connection(|conn| {
        crate::db::scan::all_sandboxes(conn, crate::db::scan::SkipUndecodableRows)
    })??;
    reconcile_inventory_batch(
        db,
        paths,
        InventoryDiscovery {
            rows,
            disk_ids,
            live_ids,
            runtime_artifact_ids,
        },
        source,
        guards,
    )
}

struct InventoryDiscovery {
    rows: Vec<crate::db::SandboxRow>,
    disk_ids: Vec<SandboxId>,
    live_ids: Vec<SandboxId>,
    runtime_artifact_ids: Vec<SandboxId>,
}

fn reconcile_inventory_batch<
    Db,
    CleanupGuard,
    StagingGuard,
    RestoreStagingGuard,
    Retirement,
    RetirementGuard,
>(
    db: &Db,
    paths: SandboxInventoryPaths<'_>,
    discovery: InventoryDiscovery,
    source: ReconcileSource,
    guards: InventoryOwnershipGuards<'_, CleanupGuard, StagingGuard, Retirement>,
) -> Result<SandboxInventoryReconcileReport, InventoryError>
where
    Db: InventoryDbAccess,
    CleanupGuard: Fn(&str) -> CleanupLockTryResult,
    StagingGuard: Fn(SandboxId) -> Option<RestoreStagingGuard>,
    Retirement: Fn(&str) -> Option<RetirementGuard>,
    RetirementGuard: FnOnce(),
{
    let InventoryDiscovery {
        rows,
        disk_ids,
        live_ids,
        runtime_artifact_ids,
    } = discovery;
    let disk_ids = disk_ids.into_iter().collect::<BTreeSet<_>>();
    let live_ids = live_ids.into_iter().collect::<BTreeSet<_>>();
    let runtime_artifact_ids = runtime_artifact_ids.into_iter().collect::<BTreeSet<_>>();
    let present_ids = disk_ids
        .iter()
        .copied()
        .chain(live_ids.iter().copied())
        .chain(runtime_artifact_ids.iter().copied())
        .collect::<BTreeSet<_>>();
    let db_ids = rows
        .iter()
        .filter_map(|row| SandboxId::from_str(&row.id).ok())
        .collect::<BTreeSet<_>>();

    let mut report = SandboxInventoryReconcileReport::default();

    for row in &rows {
        let row_id = SandboxId::from_str(&row.id).ok();
        if row_id.is_none() {
            tracing::warn!(sandbox_id = %row.id, "preserving inventory row with an invalid sandbox identity");
            continue;
        }
        if row_id
            .as_ref()
            .is_some_and(|sandbox_id| present_ids.contains(sandbox_id))
            || sandbox_state_may_lack_cache_root(row.state)
        {
            continue;
        }
        let CleanupLockTryResult::Acquired(_cleanup_guard) = (guards.try_cleanup_guard)(&row.id)
        else {
            inventory_reconcile(
                InventoryReconcileOperation::StaleRowRemoval,
                InventoryReconcileOutcome::Deferred,
                InventoryReconcileReason::LiveLockBusy,
            );
            tracing::debug!(
                source = %source,
                sandbox_id = %row.id,
                "deferring stale sandbox-row removal while cleanup owns the sandbox"
            );
            continue;
        };
        let _restore_staging_guard = if let Some(sandbox_id) = row_id {
            let Some(guard) = (guards.try_restore_staging_guard)(sandbox_id) else {
                inventory_reconcile(
                    InventoryReconcileOperation::StaleRowRemoval,
                    InventoryReconcileOutcome::Deferred,
                    InventoryReconcileReason::RestoreStagingBusy,
                );
                tracing::debug!(
                    source = %source,
                    sandbox_id = %row.id,
                    "deferring stale sandbox-row removal while restore staging owns the sandbox"
                );
                continue;
            };
            Some(guard)
        } else {
            None
        };
        // Reserve all registries before deleting the only durable retry witness.
        // Busy maps defer deletion to the next inventory sweep, without tombstones.
        let Some(retire_registrations) = (guards.try_retirement)(&row.id) else {
            tracing::debug!(sandbox_id = %row.id, "deferring stale row removal while runtime registries are busy");
            continue;
        };
        let mutation = db.with_connection(|conn| {
            if let Some(sandbox_id) = row_id {
                let current_presence = match SandboxInventoryPresence::inspect(paths, sandbox_id) {
                    Ok(presence) => presence,
                    Err(error) => return Ok(StaleRowInventoryMutation::InspectionFailed(error)),
                };
                if current_presence
                    != (SandboxInventoryPresence {
                        has_cache_root: false,
                        has_live_runtime: false,
                        has_runtime_artifact: false,
                    })
                {
                    return Ok(StaleRowInventoryMutation::FilesystemChanged(
                        current_presence,
                    ));
                }
            }
            crate::db::delete_sandbox_if_inventory_witness_matches(conn, row).map(|mutation| {
                if matches!(mutation, crate::db::InventoryStaleRowMutation::Removed) {
                    // Keep same-ID creation excluded until the previous
                    // identity's policy obligations have been retired.
                    retire_registrations();
                }
                StaleRowInventoryMutation::Database(mutation)
            })
        })?;
        match mutation {
            Ok(StaleRowInventoryMutation::InspectionFailed(error)) => {
                tracing::warn!(sandbox_id = %row.id, %error, "preserving sandbox whose filesystem evidence could not be read");
            }
            Ok(StaleRowInventoryMutation::FilesystemChanged(current_presence)) => {
                inventory_reconcile(
                    InventoryReconcileOperation::StaleRowRemoval,
                    InventoryReconcileOutcome::Deferred,
                    InventoryReconcileReason::FilesystemChanged,
                );
                tracing::debug!(
                    source = %source,
                    sandbox_id = %row.id,
                    ?current_presence,
                    "deferring stale sandbox-row removal because filesystem inventory changed"
                );
            }
            Ok(StaleRowInventoryMutation::Database(
                crate::db::InventoryStaleRowMutation::Removed,
            )) => {
                inventory_reconcile(
                    InventoryReconcileOperation::StaleRowRemoval,
                    InventoryReconcileOutcome::Applied,
                    InventoryReconcileReason::Removed,
                );
                tracing::warn!(
                    source = %source,
                    sandbox_id = %row.id,
                    state = %row.state,
                    "removing stale sandbox row absent from disk/runtime inventory"
                );
                if let Some(sandbox_id) = row_id {
                    report.record_removed(sandbox_id);
                }
            }
            Ok(StaleRowInventoryMutation::Database(
                crate::db::InventoryStaleRowMutation::Missing,
            )) => {
                inventory_reconcile(
                    InventoryReconcileOperation::StaleRowRemoval,
                    InventoryReconcileOutcome::Noop,
                    InventoryReconcileReason::AlreadyMissing,
                );
                tracing::debug!(
                    source = %source,
                    sandbox_id = %row.id,
                    "stale sandbox row was already absent"
                );
            }
            Ok(StaleRowInventoryMutation::Database(
                crate::db::InventoryStaleRowMutation::WitnessChanged,
            )) => {
                inventory_reconcile(
                    InventoryReconcileOperation::StaleRowRemoval,
                    InventoryReconcileOutcome::Deferred,
                    InventoryReconcileReason::WitnessChanged,
                );
                tracing::debug!(
                    source = %source,
                    sandbox_id = %row.id,
                    "skipping stale sandbox-row removal because lifecycle ownership changed"
                );
            }
            Ok(StaleRowInventoryMutation::Database(
                crate::db::InventoryStaleRowMutation::Deferred(reason),
            )) => {
                inventory_reconcile(
                    InventoryReconcileOperation::StaleRowRemoval,
                    InventoryReconcileOutcome::Deferred,
                    durable_inventory_reason(reason),
                );
                if reason == crate::db::InventoryOwnershipReason::MalformedCleanupIntent {
                    tracing::debug!(
                        source = %source,
                        sandbox_id = %row.id,
                        ?reason,
                        "deferring stale sandbox-row removal because cleanup intent is incomplete"
                    );
                } else {
                    tracing::debug!(
                        source = %source,
                        sandbox_id = %row.id,
                        ?reason,
                        "deferring stale sandbox-row removal to durable lifecycle ownership"
                    );
                }
            }
            Err(error) => {
                inventory_reconcile(
                    InventoryReconcileOperation::StaleRowRemoval,
                    InventoryReconcileOutcome::Failed,
                    InventoryReconcileReason::DatabaseError,
                );
                tracing::warn!(
                    source = %source,
                    sandbox_id = %row.id,
                    %error,
                    "failed to remove stale sandbox row during inventory reconcile"
                );
            }
        }
    }

    for sandbox_id in present_ids.difference(&db_ids) {
        let CleanupLockTryResult::Acquired(_cleanup_guard) =
            (guards.try_cleanup_guard)(&sandbox_id.to_string())
        else {
            inventory_reconcile(
                InventoryReconcileOperation::OrphanMaterialization,
                InventoryReconcileOutcome::Deferred,
                InventoryReconcileReason::LiveLockBusy,
            );
            tracing::debug!(
                source = %source,
                sandbox_id = %sandbox_id,
                "deferring orphan materialization while cleanup owns the sandbox"
            );
            continue;
        };
        let Some(_restore_staging_guard) = (guards.try_restore_staging_guard)(*sandbox_id) else {
            inventory_reconcile(
                InventoryReconcileOperation::OrphanMaterialization,
                InventoryReconcileOutcome::Deferred,
                InventoryReconcileReason::RestoreStagingBusy,
            );
            tracing::debug!(
                source = %source,
                sandbox_id = %sandbox_id,
                "skipping in-progress snapshot restore during inventory reconcile"
            );
            continue;
        };
        if sandbox_has_import_staging(paths.cache_dir, *sandbox_id) {
            inventory_reconcile(
                InventoryReconcileOperation::OrphanMaterialization,
                InventoryReconcileOutcome::Deferred,
                InventoryReconcileReason::ImportStaging,
            );
            tracing::debug!(
                source = %source,
                sandbox_id = %sandbox_id,
                "skipping in-progress sandbox import during inventory reconcile"
            );
            continue;
        }
        // Periodic DB and directory cursors advance independently: absence from
        // this DB page is not absence from SQLite. Avoid receiver RPCs for rows
        // owned by another page; recheck again in the insertion transaction.
        if db.with_connection(|conn| {
            crate::db::read::sandbox_view(conn, &sandbox_id.to_string()).exists()
        })?? {
            continue;
        }
        let observed_presence = SandboxInventoryPresence {
            has_cache_root: disk_ids.contains(sandbox_id),
            has_live_runtime: live_ids.contains(sandbox_id),
            has_runtime_artifact: runtime_artifact_ids.contains(sandbox_id),
        };
        let current_presence = match SandboxInventoryPresence::inspect(paths, *sandbox_id) {
            Ok(presence) => presence,
            Err(error) => {
                tracing::warn!(%sandbox_id, %error, "deferring orphan materialization until filesystem evidence is readable");
                continue;
            }
        };
        if current_presence != observed_presence {
            inventory_reconcile(
                InventoryReconcileOperation::OrphanMaterialization,
                InventoryReconcileOutcome::Deferred,
                InventoryReconcileReason::FilesystemChanged,
            );
            tracing::debug!(
                source = %source,
                sandbox_id = %sandbox_id,
                ?observed_presence,
                ?current_presence,
                "skipping orphan materialization because filesystem inventory changed"
            );
            continue;
        }
        let has_cache_root = observed_presence.has_cache_root;
        let has_live_runtime = observed_presence.has_live_runtime;
        let rootfs_size = rootfs_size_mb(paths.cache_dir, &sandbox_id.to_string());
        let cache_inspection = if has_cache_root && !has_live_runtime {
            match inspect_canonical_suspend_artifact(
                paths.cache_dir,
                *sandbox_id,
                guards.retained_sources.unwrap_or(&Default::default()),
            ) {
                Ok(inspection) => Some(inspection),
                // Startup and cleanup reconciles carry no verdicts; periodic
                // inventory inspects this orphan before its own pass.
                Err(OrphanedSandboxRowMaterializationError::Validation(error))
                    if error.is_pending() =>
                {
                    inventory_reconcile(
                        InventoryReconcileOperation::OrphanMaterialization,
                        InventoryReconcileOutcome::Deferred,
                        InventoryReconcileReason::RetainedSourcePending,
                    );
                    tracing::debug!(
                        source = %source,
                        sandbox_id = %sandbox_id,
                        %error,
                        "deferring orphan materialization until its retained source is inspected"
                    );
                    continue;
                }
                Err(error) => {
                    inventory_reconcile(
                        InventoryReconcileOperation::OrphanMaterialization,
                        InventoryReconcileOutcome::Failed,
                        InventoryReconcileReason::FilesystemInspectionFailed,
                    );
                    tracing::warn!(
                        source = %source,
                        sandbox_id = %sandbox_id,
                        %error,
                        "failed to inspect orphaned sandbox artifact"
                    );
                    continue;
                }
            }
        } else {
            None
        };
        // Capture authority once while local creation/cleanup is excluded and
        // before locking SQLite. A later receiver replacement must reject this
        // captured fence, not cause cleanup to adopt a newer execution on retry.
        let execution = (guards.observe_execution)(*sandbox_id).and_then(|fence| {
            if fence.is_none() && has_live_runtime {
                Err(Status::unavailable(
                    "live orphan has no receiver execution authority",
                ))
            } else {
                Ok(fence)
            }
        });
        let execution = match execution {
            Ok(execution) => execution,
            Err(error) => {
                inventory_reconcile(
                    InventoryReconcileOperation::OrphanMaterialization,
                    InventoryReconcileOutcome::Deferred,
                    InventoryReconcileReason::MaterializationError,
                );
                tracing::warn!(%source, %sandbox_id, %error,
                    "deferring orphan materialization until receiver authority can be confirmed");
                continue;
            }
        };
        let sandbox_id_string = sandbox_id.to_string();
        let materialized: Result<OrphanRowMaterialization, OrphanedSandboxRowMaterializationError> =
            db.with_connection(|conn| {
                if SandboxInventoryPresence::inspect(paths, *sandbox_id)? != observed_presence {
                    return Ok(OrphanRowMaterialization::InventoryChanged);
                }
                let tx = crate::db::Transaction::immediate(conn)?;
                if crate::db::read::sandbox(&tx, &sandbox_id_string)?.is_some() {
                    return Ok(OrphanRowMaterialization::DatabaseRowAppeared);
                }
                if crate::db::read::sandbox_view(&tx, &sandbox_id_string)
                    .restore_scope()
                    .scope()?
                    .is_some()
                {
                    return Ok(OrphanRowMaterialization::RestoreTransferScope);
                }
                let committed = insert_orphaned_sandbox_row_in_transaction(
                    &tx,
                    *sandbox_id,
                    has_cache_root,
                    has_live_runtime,
                    rootfs_size,
                    cache_inspection,
                    source,
                )?;
                if let Some(fence) = execution {
                    crate::db::inherit_orphan_node_execution(&tx, &sandbox_id_string, fence)?;
                }
                tx.commit()?;
                Ok(OrphanRowMaterialization::Materialized(committed))
            })?;
        match materialized {
            Ok(OrphanRowMaterialization::Materialized(committed)) => {
                inventory_reconcile(
                    InventoryReconcileOperation::OrphanMaterialization,
                    InventoryReconcileOutcome::Applied,
                    InventoryReconcileReason::Materialized,
                );
                committed.emit_event(*sandbox_id);
                pplx_space_metrics::sandbox::worker::orphan_row_materialized(
                    source.as_str(),
                    committed.disposition,
                );
                tracing::warn!(
                    source = %source,
                    sandbox_id = %sandbox_id,
                    has_cache_root,
                    has_live_runtime,
                    disposition = %committed.disposition,
                    "materialized orphaned sandbox artifact into inventory row"
                );
                report.record_changed(*sandbox_id);
            }
            Ok(OrphanRowMaterialization::DatabaseRowAppeared) => {
                inventory_reconcile(
                    InventoryReconcileOperation::OrphanMaterialization,
                    InventoryReconcileOutcome::Noop,
                    InventoryReconcileReason::DatabaseRowAppeared,
                );
                tracing::debug!(
                    source = %source,
                    sandbox_id = %sandbox_id,
                    "skipping orphan materialization because a database row appeared"
                );
            }
            Ok(OrphanRowMaterialization::InventoryChanged) => {
                inventory_reconcile(
                    InventoryReconcileOperation::OrphanMaterialization,
                    InventoryReconcileOutcome::Deferred,
                    InventoryReconcileReason::FilesystemChanged,
                );
                tracing::debug!(
                    source = %source,
                    sandbox_id = %sandbox_id,
                    "skipping orphan materialization because filesystem inventory changed while waiting for SQLite"
                );
            }
            Ok(OrphanRowMaterialization::RestoreTransferScope) => {
                inventory_reconcile(
                    InventoryReconcileOperation::OrphanMaterialization,
                    InventoryReconcileOutcome::Deferred,
                    InventoryReconcileReason::RestoreTransferScope,
                );
                tracing::debug!(
                    source = %source,
                    sandbox_id = %sandbox_id,
                    "deferring orphan materialization to a durable restore transfer"
                );
            }
            Err(error) => {
                let reason = if error.is_database_error() {
                    InventoryReconcileReason::DatabaseError
                } else {
                    InventoryReconcileReason::MaterializationError
                };
                inventory_reconcile(
                    InventoryReconcileOperation::OrphanMaterialization,
                    InventoryReconcileOutcome::Failed,
                    reason,
                );
                tracing::warn!(
                    source = %source,
                    sandbox_id = %sandbox_id,
                    %error,
                    "failed to materialize orphaned sandbox artifact"
                );
            }
        }
    }

    Ok(report)
}

#[cfg(test)]
pub(super) fn inventory_cleanup_guard_for_test(sandbox_id: &str) -> CleanupLockTryResult {
    // The default inventory fixture models an uncontended fence. Contention
    // coverage supplies a shared lock map explicitly.
    let locks = Arc::new(Mutex::new(BTreeMap::new()));
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    locks
        .lock()
        .expect("cleanup lock map poisoned")
        .insert(sandbox_id.to_string(), Arc::clone(&lock));
    CleanupLockGuard::new(locks, sandbox_id.to_string(), lock).try_acquire_owned()
}

#[cfg(test)]
pub(super) fn inventory_execution_for_test(_: SandboxId) -> Result<Option<ExecutionFence>, Status> {
    Ok(Some(ExecutionFence::new(
        pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        None,
    )))
}

#[cfg(test)]
pub(super) fn reconcile_sandbox_inventory_in(
    conn: &crate::db::Connection,
    cache_dir: &Path,
    run_dir: &Path,
    source: ReconcileSource,
) -> Result<SandboxInventoryReconcileReport, InventoryError> {
    reclaim_abandoned_restore_metadata_staging(cache_dir, source, &|_| Some(()));
    let disk_ids = list_sandbox_dirs_in(cache_dir);
    let live_ids = list_live_sandbox_run_dirs_in(run_dir);
    let runtime_artifact_ids = list_sandbox_run_dirs_in(run_dir);
    reconcile_sandbox_inventory_ids(
        conn,
        SandboxInventoryPaths::new(cache_dir, run_dir),
        disk_ids,
        live_ids,
        runtime_artifact_ids,
        source,
        InventoryOwnershipGuards::new(
            inventory_cleanup_guard_for_test,
            |_: SandboxId| Some(()),
            &inventory_execution_for_test,
        ),
    )
}

impl SandboxManagerService {
    /// Inventory runs on blocking workers. Receiver reads hold only per-sandbox
    /// guards, never SQLite. All reads in one pass share ten seconds; unavailable
    /// authority leaves the orphan for the next periodic scan, including shutdown.
    fn orphan_execution_observer(
        &self,
    ) -> impl Fn(SandboxId) -> Result<Option<ExecutionFence>, Status> + use<> {
        orphan_execution_observer(self.node_manager.lazy(), self.shutdown_token.clone())
    }

    pub(super) fn reconcile_sandbox_inventory(
        &self,
        source: ReconcileSource,
    ) -> Result<SandboxInventoryReconcileReport, InventoryError> {
        let disk_ids = list_sandbox_dirs();
        let live_ids = list_live_sandbox_run_dirs();
        let runtime_artifact_ids = list_sandbox_run_dirs();
        let auto_lifecycle = self.auto_lifecycle.clone();
        reclaim_abandoned_restore_metadata_staging(Path::new(CACHE_DIR), source, &|sandbox_id| {
            auto_lifecycle.try_staging_guard(&sandbox_id.to_string())
        });
        let reconcile_result = self.apply_route_identity_mutations("inventory_reconcile", || {
            let observe_execution = self.orphan_execution_observer();
            let report = reconcile_sandbox_inventory_ids(
                &self.db,
                SandboxInventoryPaths::new(Path::new(CACHE_DIR), Path::new(RUN_DIR)),
                disk_ids,
                live_ids,
                runtime_artifact_ids,
                source,
                InventoryOwnershipGuards::new(
                    |sandbox_id: &str| self.try_cleanup_fence(sandbox_id),
                    |sandbox_id: SandboxId| {
                        auto_lifecycle.try_staging_guard(&sandbox_id.to_string())
                    },
                    &observe_execution,
                )
                .with_retirement(|sandbox_id: &str| self.try_registration_retirement(sandbox_id)),
            )?;
            let changed_ids = report.changed_ids();
            Ok((report, changed_ids))
        });
        let report = match reconcile_result {
            Ok(report) => report,
            Err(error) => {
                if matches!(error, InventoryError::DatabaseMutexPoisoned) {
                    self.fail_poisoned_database();
                }
                let error_detail = error.to_string();
                tracing::warn!(
                    error = %error_detail,
                    source = %source,
                    "failed to reconcile sandbox inventory"
                );
                return Err(error);
            }
        };
        let changed_ids = report.changed_ids();
        if !changed_ids.is_empty() {
            mark_sandboxes_dirty(&self.db, changed_ids.iter().map(ToString::to_string));
        }
        Ok(report)
    }

    fn try_registration_retirement<'a>(
        &'a self,
        sandbox_id: &str,
    ) -> Option<impl FnOnce() + use<'a>> {
        let watchdog = self.watchdog.try_retirement(sandbox_id)?;
        let scheduler = self.snapshot_scheduler.try_retirement(sandbox_id)?;
        let sandbox_id = sandbox_id.to_owned();
        Some(move || {
            watchdog();
            scheduler();
            self.auto_lifecycle.remove_sandbox(&sandbox_id);
            self.network_policy_reconcile_queue.retire(&sandbox_id);
        })
    }

    pub(super) async fn retire_sandbox_registrations(&self, sandbox_id: &str) {
        self.watchdog.remove_sandbox(sandbox_id).await;
        self.snapshot_scheduler.remove_sandbox(sandbox_id).await;
        self.auto_lifecycle.remove_sandbox(sandbox_id);
    }

    pub(super) fn retire_absent_sandbox_policy(
        &self,
        db: &crate::db::Connection,
        sandbox_id: &str,
    ) -> crate::db::Result<()> {
        // The caller keeps SQLite locked through retirement, excluding a
        // same-ID insertion and its new policy obligation.
        if !crate::db::read::sandbox_view(db, sandbox_id).exists()? {
            self.network_policy_reconcile_queue.retire(sandbox_id);
        }
        Ok(())
    }
}

fn orphan_execution_observer(
    client: NodeManagerGrpcClient,
    shutdown: CancellationToken,
) -> impl Fn(SandboxId) -> Result<Option<ExecutionFence>, Status> {
    let runtime = tokio::runtime::Handle::current();
    let deadline = std::cell::Cell::new(None);
    move |id| {
        let deadline = deadline.get().unwrap_or_else(|| {
            let end = tokio::time::Instant::now() + Duration::from_secs(10);
            deadline.set(Some(end));
            end
        });
        let mut client = client.clone();
        if tokio::time::Instant::now() >= deadline {
            return Err(Status::deadline_exceeded(
                "inventory execution inspection budget exhausted",
            ));
        }
        runtime.block_on(async {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => Err(Status::cancelled("inventory shutdown")),
                result = tokio::time::timeout_at(deadline,
                    pplx_space_node_mgr_client::execution_fence::inspect_execution(&mut client, id)) => {
                    result.unwrap_or_else(|_| Err(Status::deadline_exceeded("inventory execution inspection budget exhausted")))
                }
            }
        })
    }
}

/// Remove run-dir junk that cannot be addressed as a sandbox cleanup target.
/// Valid sandbox-shaped run dirs are reconciled into cleanup intents above and
/// removed by node-manager, which owns privileged runtime cleanup.
/// Returns list of successfully cleaned items for logging.
pub(super) fn cleanup_orphaned_dirs(source: ReconcileSource) -> Vec<String> {
    cleanup_orphaned_dirs_in(Path::new(CACHE_DIR), Path::new(RUN_DIR), source)
}

pub(super) fn cleanup_orphaned_dirs_in(
    cache_dir: &Path,
    run_dir: &Path,
    source: ReconcileSource,
) -> Vec<String> {
    let mut known: std::collections::HashSet<String> =
        sandbox_ids_to_strings(list_sandbox_dirs_in(cache_dir))
            .into_iter()
            .collect();

    // Also consider sandboxes with live FC processes as "known" — they may be
    // mid-creation/restore and don't have a rootfs yet but are actively in use.
    if let Ok(entries) = std::fs::read_dir(run_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if is_sandbox_running_in(run_dir, &name) {
                known.insert(name);
            }
        }
    }

    let mut cleaned = Vec::new();

    // Invalid run dirs
    if let Ok(entries) = std::fs::read_dir(run_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if !known.contains(&name) && SandboxId::from_str(&name).is_err() {
                match std::fs::remove_dir_all(entry.path()) {
                    Ok(()) => {
                        tracing::debug!(
                            source = %source,
                            run_dir = %name,
                            "removed orphaned run directory"
                        );
                        cleaned.push(format!("run:{name}"));
                    }
                    Err(error) => {
                        tracing::warn!(
                            source = %source,
                            run_dir = %name,
                            error = %error,
                            "failed to remove orphaned run directory"
                        );
                    }
                }
            }
        }
    }

    // Jail dirs are owned by sandbox UID — sbx-manager can't remove them directly.
    // Orphaned jails are cleaned up by node-manager's cleanup_sandbox RPC during
    // normal stop/delete flows, and by the Cleanup RPC for startup recovery.

    cleaned
}
