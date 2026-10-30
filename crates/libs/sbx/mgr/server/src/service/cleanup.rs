use pplx_space_common_core::types::{JobId, LinuxPid, LocalArtifactId};

use super::cleanup_executor::CleanupWait;
use super::cleanup_support::{
    ActivationCleanupSpec, CLEANUP_HANDOFF_RETRY, CleanupCompletion, CleanupFenceAuthorization,
    CleanupLock, CleanupLockGuard, CleanupLockMap, CleanupLockTryResult, NetGatewayStatsSnapshot,
    NodeCleanupOptions, OwnedCleanupFenceGuard, admit_prepared_clone_disposal_attempt,
    service_cleanup_via_node_with_options,
};
use super::start_support::{ActivationAdmissionPermit, ActivationCleanupGuard};
use super::tail_support::*;
use super::*;
use crate::db::cleanup_jobs::CleanupOutcome;
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
    emit_sandbox_event,
};

pub(super) const AUTO_LIFECYCLE_DELETE_CAS_LOST_METADATA: &str =
    "x-space-auto-lifecycle-delete-cas-lost";
pub(super) const AUTO_LIFECYCLE_DELETE_BLOCKED_METADATA: &str =
    "x-space-auto-lifecycle-delete-blocked";
pub(super) const AUTO_LIFECYCLE_DELETE_RECEIPT_CONFLICT_METADATA: &str =
    "x-space-auto-lifecycle-delete-receipt-conflict";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AutoLifecycleDeleteCasLostReason {
    PolicyChanged,
    AuthorityChanged,
}

impl AutoLifecycleDeleteCasLostReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::PolicyChanged => "policy_changed",
            Self::AuthorityChanged => "authority_changed",
        }
    }
}

impl std::fmt::Display for AutoLifecycleDeleteCasLostReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub(super) struct AsyncCleanupRequest {
    pub(super) sandbox_id: SandboxId,
    pub(super) uuid8: SandboxRuntimeLabel,
    pub(super) keep_filesystem: bool,
    pub(super) context: &'static str,
    pub(super) completion: CleanupCompletion,
    pub(super) immediate_kill: bool,
    pub(super) guest_flush: crate::db::cleanup_jobs::GuestFlush,
    pub(super) persist_intent: bool,
    pub(super) cleanup_id: Option<SandboxCleanupEpisodeId>,
}

pub(super) struct SynchronousCleanupRequest<'a> {
    pub(super) sandbox_id: &'a str,
    pub(super) uuid8: &'a str,
    pub(super) keep_filesystem: bool,
    pub(super) context: &'static str,
    pub(super) completion: CleanupCompletion,
    pub(super) published_artifact: Option<(LocalArtifactId, String)>,
    pub(super) immediate_kill: bool,
    pub(super) guest_flush: crate::db::cleanup_jobs::GuestFlush,
}

pub(super) enum CleanupAdmission {
    Persisted {
        sandbox_id: SandboxId,
        expected_execution_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
        dispatch_job_id: Option<JobId>,
        auto_lifecycle_delete_receipt:
            Option<crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt>,
        fence: OwnedCleanupFenceGuard,
    },
    Unpersisted {
        sandbox_id: SandboxId,
        expected_execution_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
        persistence_error: Option<String>,
        fence: OwnedCleanupFenceGuard,
    },
}

#[derive(Debug)]
pub(super) enum CleanupAdmissionError {
    QuarantineRecoveryInProgress {
        sandbox_id: SandboxId,
    },
    SuspendPublicationPending {
        sandbox_id: SandboxId,
        state: SandboxState,
    },
    AutoLifecycleDeleteBlocked {
        sandbox_id: SandboxId,
        reason: crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteBlockReason,
    },
    AutoLifecycleDeleteCasLost {
        sandbox_id: SandboxId,
        reason: AutoLifecycleDeleteCasLostReason,
    },
    AutoLifecycleDeleteReceiptConflict {
        sandbox_id: SandboxId,
    },
    RetainedFilesystemOnlyCopy {
        sandbox_id: SandboxId,
    },
    Status(Status),
}

impl CleanupAdmissionError {
    pub(super) fn into_status(self) -> Status {
        match self {
            Self::QuarantineRecoveryInProgress { sandbox_id } => Status::unavailable(format!(
                "sandbox {sandbox_id} is completing quarantine recovery; retry cleanup"
            )),
            Self::SuspendPublicationPending { sandbox_id, state } => {
                pplx_space_sbx_mgr_client::suspend_publication_pending_status(format!(
                    "sandbox {sandbox_id} is {state}; suspend completion has not reached the cluster yet; retry the delete"
                ))
            }
            Self::AutoLifecycleDeleteBlocked { sandbox_id, reason } => {
                let mut status = Status::failed_precondition(format!(
                    "sandbox {sandbox_id} auto-lifecycle delete is blocked: {reason}"
                ));
                status.metadata_mut().insert(
                    AUTO_LIFECYCLE_DELETE_BLOCKED_METADATA,
                    tonic::metadata::MetadataValue::from_static("1"),
                );
                status
            }
            Self::AutoLifecycleDeleteCasLost { sandbox_id, reason } => {
                let mut status = Status::aborted(format!(
                    "sandbox {sandbox_id} auto-lifecycle delete lost its {reason} comparison"
                ));
                status.metadata_mut().insert(
                    AUTO_LIFECYCLE_DELETE_CAS_LOST_METADATA,
                    tonic::metadata::MetadataValue::from_static("1"),
                );
                status
            }
            Self::AutoLifecycleDeleteReceiptConflict { sandbox_id } => {
                let mut status = Status::aborted(format!(
                    "sandbox {sandbox_id} auto-lifecycle delete conflicts with durable receipt evidence"
                ));
                status.metadata_mut().insert(
                    AUTO_LIFECYCLE_DELETE_RECEIPT_CONFLICT_METADATA,
                    tonic::metadata::MetadataValue::from_static("1"),
                );
                status
            }
            Self::RetainedFilesystemOnlyCopy { sandbox_id } => {
                pplx_space_sbx_mgr_client::retained_filesystem_only_copy_status(format!(
                    "sandbox {sandbox_id} has no published suspend artifact; its retained filesystem may be the only copy; recover it before retrying the delete"
                ))
            }
            Self::Status(status) => status,
        }
    }
}

impl From<Status> for CleanupAdmissionError {
    fn from(status: Status) -> Self {
        Self::Status(status)
    }
}

impl CleanupAdmission {
    pub(super) fn persisted(
        sandbox_id: SandboxId,
        expected_execution_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
        auto_lifecycle_delete_receipt: Option<
            crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt,
        >,
        fence: OwnedCleanupFenceGuard,
    ) -> Self {
        Self::Persisted {
            sandbox_id,
            expected_execution_generation,
            dispatch_job_id: None,
            auto_lifecycle_delete_receipt,
            fence,
        }
    }

    fn unpersisted(
        sandbox_id: SandboxId,
        expected_execution_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
        persistence_error: Option<String>,
        fence: OwnedCleanupFenceGuard,
    ) -> Self {
        Self::Unpersisted {
            sandbox_id,
            expected_execution_generation,
            persistence_error,
            fence,
        }
    }

    pub(super) fn is_unpersisted_for_sandbox(&self, sandbox_id: SandboxId) -> Result<bool, Status> {
        match self {
            Self::Persisted {
                sandbox_id: admitted_sandbox_id,
                ..
            }
            | Self::Unpersisted {
                sandbox_id: admitted_sandbox_id,
                ..
            } if *admitted_sandbox_id != sandbox_id => Err(Status::internal(
                "cleanup admission does not match cleanup sandbox",
            )),
            Self::Persisted { .. } => Ok(false),
            Self::Unpersisted { .. } => Ok(true),
        }
    }

    pub(super) fn fence_for_sandbox(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<&OwnedCleanupFenceGuard, Status> {
        let (admitted_sandbox_id, fence) = match self {
            Self::Persisted {
                sandbox_id, fence, ..
            }
            | Self::Unpersisted {
                sandbox_id, fence, ..
            } => (sandbox_id, fence),
        };
        if *admitted_sandbox_id != sandbox_id {
            return Err(Status::internal(
                "cleanup admission does not match cleanup sandbox",
            ));
        }
        Ok(fence)
    }

    pub(super) fn into_unpersisted_error(
        self,
        sandbox_id: SandboxId,
    ) -> Result<Option<String>, Status> {
        match self {
            Self::Unpersisted {
                sandbox_id: admitted_sandbox_id,
                persistence_error,
                ..
            } if admitted_sandbox_id == sandbox_id => Ok(persistence_error),
            Self::Unpersisted { .. } => Err(Status::internal(
                "cleanup admission does not match cleanup sandbox",
            )),
            Self::Persisted { .. } => Err(Status::internal(
                "persisted cleanup admission cannot authorize persistence-failure cleanup",
            )),
        }
    }

    pub(super) fn expected_execution_generation(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<pplx_space_common_core::types::ExecutionGeneration>, Status> {
        match self {
            Self::Persisted {
                sandbox_id: admitted_sandbox_id,
                expected_execution_generation,
                ..
            }
            | Self::Unpersisted {
                sandbox_id: admitted_sandbox_id,
                expected_execution_generation,
                ..
            } if *admitted_sandbox_id == sandbox_id => Ok(*expected_execution_generation),
            Self::Persisted { .. } | Self::Unpersisted { .. } => Err(Status::internal(
                "cleanup admission does not match cleanup sandbox",
            )),
        }
    }

    pub(super) fn auto_lifecycle_delete_receipt(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt>, Status>
    {
        match self {
            Self::Persisted {
                sandbox_id: admitted_sandbox_id,
                auto_lifecycle_delete_receipt,
                ..
            } if *admitted_sandbox_id == sandbox_id => Ok(*auto_lifecycle_delete_receipt),
            Self::Unpersisted {
                sandbox_id: admitted_sandbox_id,
                ..
            } if *admitted_sandbox_id == sandbox_id => Ok(None),
            Self::Persisted { .. } | Self::Unpersisted { .. } => Err(Status::internal(
                "cleanup admission does not match cleanup sandbox",
            )),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum CleanupPersistenceError {
    #[error("database mutex poisoned")]
    DatabaseMutexPoisoned,
    #[error("failed to persist cleanup requirement")]
    Database(#[source] crate::db::RequestCleanupEpisodeError),
}

#[derive(Debug, thiserror::Error)]
enum ApplyCleanupCompletionError {
    #[error("database mutex poisoned")]
    DatabaseMutexPoisoned,
    #[error(transparent)]
    RecoveryArtifact(#[from] QuarantineRecoveryArtifactError),
    #[error(transparent)]
    CompleteEpisode(#[from] crate::db::CompleteCleanupEpisodeError),
    #[error("quarantine recovery artifact validation task failed: {0}")]
    ArtifactValidationTask(String),
}

enum CleanupFinalizationOutcome {
    Applied,
    Superseded,
    UnsafeEvidenceQuarantined(String),
    Retry(String),
}

#[derive(Debug, thiserror::Error)]
enum QuarantineRecoveryArtifactError {
    #[error("database mutex poisoned")]
    DatabaseMutexPoisoned,
    #[error("load cleanup episode failed: {0}")]
    Database(#[from] crate::db::Error),
    #[error("quarantine recovery episode is missing its artifact/generation fence")]
    MissingRecoveryFence,
    #[error(transparent)]
    InvalidSandboxId(#[from] pplx_space_common_core::types::SandboxIdParseError),
    #[error(transparent)]
    Inspection(#[from] super::suspend_snapshot_publication::CanonicalSuspendSnapshotRecoveryError),
    #[error("quarantine recovery artifact changed from {expected} to {observed}")]
    ArtifactChanged {
        expected: LocalArtifactId,
        observed: LocalArtifactId,
    },
}

impl QuarantineRecoveryArtifactError {
    fn is_unsafe_evidence(&self) -> bool {
        match self {
            Self::DatabaseMutexPoisoned | Self::Database(_) => false,
            Self::Inspection(error) => error.is_unsafe_evidence(),
            Self::MissingRecoveryFence
            | Self::InvalidSandboxId(_)
            | Self::ArtifactChanged { .. } => true,
        }
    }
}

impl ApplyCleanupCompletionError {
    fn is_unsafe_evidence(&self) -> bool {
        match self {
            Self::RecoveryArtifact(error) => error.is_unsafe_evidence(),
            Self::CompleteEpisode(
                crate::db::CompleteCleanupEpisodeError::MissingSuspendArtifact,
            ) => true,
            Self::DatabaseMutexPoisoned
            | Self::CompleteEpisode(_)
            | Self::ArtifactValidationTask(_) => false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum CleanupCompletionError {
    #[error("database mutex poisoned")]
    DatabaseMutexPoisoned,
    #[error("failed to persist cleanup completion")]
    Database(#[from] crate::db::Error),
    #[error(transparent)]
    RowIntent(#[from] crate::db::CompleteRowIntentCleanupError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CleanupCompletionDisposition {
    Applied,
    Quarantined,
    Superseded,
}

struct RowCleanupCompletion<'a> {
    sandbox_id: &'a str,
    completion: CleanupCompletion,
    expected_execution_generation: Option<ExecutionGeneration>,
    auto_lifecycle_delete_receipt:
        Option<crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt>,
    published_artifact: Option<(LocalArtifactId, String)>,
    job_id: Option<&'a JobId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CleanupDispatchDisposition {
    Dispatched,
    Superseded,
}

pub(super) struct ObservedCleanupClaim {
    pub(super) observed_state: SandboxState,
    pub(super) target_state: SandboxState,
    pub(super) keep_filesystem: bool,
    pub(super) completion: CleanupCompletion,
    pub(super) persist_intent: bool,
    pub(super) observed_execution_generation:
        Option<Option<pplx_space_common_core::types::ExecutionGeneration>>,
}

struct CleanupAdmissionSpec {
    mode: CleanupAdmissionMode,
    template_filter: Option<pplx_space_common_core::types::TemplateName>,
    require_template_owner: bool,
    target_state: SandboxState,
    keep_filesystem: bool,
    completion: CleanupCompletion,
    persist_intent: bool,
    immediate_kill: bool,
    guest_flush: crate::db::cleanup_jobs::GuestFlush,
}

#[derive(Debug, Clone, Copy)]
enum CleanupAdmissionMode {
    Unfenced,
    ObservedState {
        state: SandboxState,
        execution_generation: Option<Option<pplx_space_common_core::types::ExecutionGeneration>>,
    },
    ObservedExecution {
        execution_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
    },
    /// Only while the sandbox is in `state` and still running the node
    /// execution `execution` began: a later activation is someone else's.
    ObservedActivation {
        state: SandboxState,
        execution: pplx_space_common_core::types::SandboxActivationAttemptId,
    },
    AutoLifecycleDelete {
        expectation: crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteExpectation,
    },
}

impl CleanupAdmissionMode {
    const fn observed_state(self) -> Option<SandboxState> {
        match self {
            Self::ObservedState { state, .. } | Self::ObservedActivation { state, .. } => {
                Some(state)
            }
            Self::Unfenced | Self::ObservedExecution { .. } | Self::AutoLifecycleDelete { .. } => {
                None
            }
        }
    }

    const fn observed_activation(
        self,
    ) -> Option<pplx_space_common_core::types::SandboxActivationAttemptId> {
        match self {
            Self::ObservedActivation { execution, .. } => Some(execution),
            Self::Unfenced
            | Self::ObservedState { .. }
            | Self::ObservedExecution { .. }
            | Self::AutoLifecycleDelete { .. } => None,
        }
    }

    const fn observed_execution_generation(
        self,
    ) -> Option<Option<pplx_space_common_core::types::ExecutionGeneration>> {
        match self {
            Self::ObservedState {
                execution_generation,
                ..
            } => execution_generation,
            Self::ObservedExecution {
                execution_generation,
            } => Some(execution_generation),
            Self::Unfenced | Self::ObservedActivation { .. } | Self::AutoLifecycleDelete { .. } => {
                None
            }
        }
    }

    const fn requires_generation_inspection(self) -> bool {
        match self {
            Self::Unfenced => false,
            Self::ObservedState {
                execution_generation,
                ..
            } => execution_generation.is_some(),
            // An activation-fenced stop must never fall back to unpersisted
            // cleanup, which would skip the execution check.
            Self::ObservedExecution { .. }
            | Self::ObservedActivation { .. }
            | Self::AutoLifecycleDelete { .. } => true,
        }
    }

    const fn auto_lifecycle_delete_expectation(
        self,
    ) -> Option<crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteExpectation> {
        match self {
            Self::AutoLifecycleDelete { expectation } => Some(expectation),
            Self::Unfenced
            | Self::ObservedState { .. }
            | Self::ObservedExecution { .. }
            | Self::ObservedActivation { .. } => None,
        }
    }

    const fn requires_atomic_admission(self) -> bool {
        matches!(self, Self::AutoLifecycleDelete { .. })
    }

    const fn requires_immediate_transaction(self) -> bool {
        matches!(self, Self::AutoLifecycleDelete { .. })
    }
}

impl CleanupDispatchDisposition {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Dispatched => "dispatched",
            Self::Superseded => "superseded",
        }
    }
}

impl std::fmt::Display for CleanupDispatchDisposition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl CleanupCompletionDisposition {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Quarantined => "quarantined",
            Self::Superseded => "superseded",
        }
    }
}

impl std::fmt::Display for CleanupCompletionDisposition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl SandboxManagerService {
    pub(super) async fn cancel_restore_transfer_scope_and_remove_staging(
        &self,
        sandbox_id: &str,
        scope: &crate::db::RestoreTransferScopeRow,
    ) -> Result<(), Status> {
        let transfer_scope_id = scope.transfer_scope_id;
        let id = SandboxId::from_str(sandbox_id).map_err(|error| {
            Status::failed_precondition(format!("invalid restore scope sandbox: {error}"))
        })?;
        let staging_dir = crate::snapshot_restore::remote_restore_metadata_staging_dir(
            &id.to_string(),
            transfer_scope_id,
        );
        if scope.staging_dir != staging_dir.to_string_lossy() {
            return Err(Status::failed_precondition(
                "restore staging path does not match sandbox and transfer scope; preserving evidence",
            ));
        }

        let client = pplx_space_volmgr_client::connect(pplx_space_volmgr_client::VOLMGR_SOCKET)
            .await
            .map_err(|error| Status::unavailable(format!("connect volume-manager: {error}")))?;
        pplx_space_volmgr_client::cancel_transfer_scope(client, transfer_scope_id)
            .await
            .map_err(|error| {
                Status::unavailable(format!(
                    "cancel restore transfer scope {transfer_scope_id}: {error}"
                ))
            })?;

        crate::btrfs_via_nodemgr::delete_sandbox_instance(
            sandbox_id,
            pplx_space_node_mgr_client::execution_fence::InstanceTarget::RestoreScope(
                transfer_scope_id,
            ),
            &self.node_manager,
        )
        .await
        .map_err(|error| {
            Status::unavailable(format!(
                "remove restore staging subvolume for scope {transfer_scope_id}: {error}"
            ))
        })?;

        // This disposable scope is distinct from the canonical sandbox root.
        // Its durable owner excludes new work; both volume-manager cancellation
        // and node-manager's writer-excluding acknowledgement must precede removal.
        self.blocking_artifact_io(move |_| match std::fs::remove_dir_all(&staging_dir) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Status::internal(format!(
                "remove restore staging {}: {error}",
                staging_dir.display()
            ))),
        })
        .await
    }

    pub(super) async fn cancel_restore_transfers_and_remove_staging(
        &self,
        sandbox_id: &str,
    ) -> Result<(), Status> {
        let id = sandbox_id.to_owned();
        let scope = self
            .storage(move |db| {
                crate::db::read::sandbox_view(db, &id)
                    .restore_scope()
                    .scope()
                    .map_err(|error| {
                        Status::internal(format!("read restore transfer scope: {error}"))
                    })
            })
            .await?;
        let Some(scope) = scope else {
            return Ok(());
        };
        self.cancel_restore_transfer_scope_and_remove_staging(sandbox_id, &scope)
            .await
    }

    pub(super) async fn retire_rowless_restore_transfer_scope(
        &self,
        sandbox_id: &str,
        scope: crate::db::RestoreTransferScopeRow,
    ) -> Result<bool, Status> {
        let still_rowless = {
            let db = self.db_conn()?;
            crate::db::read::sandbox(&db, sandbox_id)
                .map_err(|error| Status::internal(format!("read sandbox row: {error}")))?
                .is_none()
                && crate::db::read::sandbox_view(&db, sandbox_id)
                    .restore_scope()
                    .scope()
                    .map_err(|error| {
                        Status::internal(format!("read restore transfer scope: {error}"))
                    })?
                    .is_some_and(|current| current.transfer_scope_id == scope.transfer_scope_id)
        };
        if !still_rowless {
            return Ok(false);
        }

        self.cancel_restore_transfer_scope_and_remove_staging(sandbox_id, &scope)
            .await?;

        let retired = {
            let db = self.db_conn()?;
            crate::db::retire_rowless_restore_transfer_scope(
                &db,
                sandbox_id,
                scope.transfer_scope_id,
            )
            .map_err(|error| {
                Status::internal(format!("retire rowless restore transfer scope: {error}"))
            })?
        };
        if retired {
            tracing::info!(
                sandbox_id,
                transfer_scope_id = %scope.transfer_scope_id,
                "retired rowless restore transfer scope"
            );
        }
        Ok(retired)
    }

    pub(super) async fn retire_orphan_quarantine_restore_transfer_scope(
        &self,
        sandbox_id: &str,
        scope: crate::db::RestoreTransferScopeRow,
    ) -> Result<bool, Status> {
        self.cancel_restore_transfer_scope_and_remove_staging(sandbox_id, &scope)
            .await?;

        let retired = {
            let db = self.db_conn()?;
            crate::db::retire_orphan_quarantine_restore_transfer_scope(
                &db,
                sandbox_id,
                scope.transfer_scope_id,
            )
            .map_err(|error| {
                Status::internal(format!(
                    "retire orphan quarantine restore transfer scope: {error}"
                ))
            })?
        };
        if retired {
            tracing::info!(
                sandbox_id,
                transfer_scope_id = %scope.transfer_scope_id,
                "retired orphan quarantine restore transfer scope"
            );
        }
        Ok(retired)
    }

    pub(super) async fn retire_rowless_restore_transfer_scopes(&self) {
        let scopes = {
            let Ok(db) = self.db_conn() else {
                return;
            };
            match crate::db::scan::rowless_restore_scopes(
                &db,
                crate::db::scan::RowlessRestoreScopes::All,
            ) {
                Ok(scopes) => scopes,
                Err(error) => {
                    tracing::warn!(%error, "failed to list rowless restore transfer scopes");
                    return;
                }
            }
        };
        for (sandbox_id, scope) in scopes {
            let Some(_staging_guard) = self.auto_lifecycle.try_staging_guard(&sandbox_id) else {
                continue;
            };
            if let Err(error) = self
                .retire_rowless_restore_transfer_scope(&sandbox_id, scope)
                .await
            {
                tracing::warn!(
                    sandbox_id,
                    %error,
                    "failed to retire rowless restore transfer scope"
                );
            }
        }
    }

    pub(super) async fn retire_rowless_restore_transfer_scope_batch(
        &self,
        scan: &mut crate::db::inventory::RestoreTransferScopeScan,
        shutdown: &CancellationToken,
    ) {
        let mut cursor = std::mem::take(scan);
        let batch = self
            .storage(move |db| {
                let batch = crate::db::scan::rowless_restore_scopes(
                    db,
                    crate::db::scan::RowlessRestoreScopes::Cursor(&mut cursor),
                )
                .map_err(|error| {
                    Status::internal(format!("discover orphan transfer scopes: {error}"))
                })?;
                Ok((cursor, batch))
            })
            .await;
        let scopes = match batch {
            Ok((cursor, scopes)) => {
                *scan = cursor;
                scopes
            }
            Err(error) => {
                tracing::warn!(%error, "orphan transfer discovery failed; will retry");
                return;
            }
        };
        for (sandbox_id, scope) in scopes {
            if shutdown.is_cancelled() {
                break;
            }
            let Some(_staging_guard) = self.auto_lifecycle.try_staging_guard(&sandbox_id) else {
                continue;
            };
            if let Err(error) = self
                .retire_rowless_restore_transfer_scope(&sandbox_id, scope)
                .await
            {
                tracing::warn!(sandbox_id, %error, "orphan transfer retirement failed; will retry");
            }
        }
    }

    /// Only positive process-exit evidence may stop a Started sandbox. Hold the
    /// cleanup fence from re-reading its execution through durable admission;
    /// an absent socket, unreadable /proc, or a reused live PID is not death.
    pub(super) async fn admit_exited_runtime(
        &self,
        observed: crate::db::SandboxRow,
    ) -> Result<Option<CleanupAdmission>, CleanupAdmissionError> {
        let sandbox_id: SandboxId = observed
            .id
            .parse()
            .map_err(|error| Status::internal(format!("sandbox id: {error}")))?;
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            // A healthy process must not acquire a cleanup fence or contend
            // with filesystem cleanup permits. Recheck the same witness under
            // the fence after positive exit evidence.
            let Some(pid) = observed.pid else { return Ok(None); };
            let exited = || -> Result<bool, Status> {
                match pplx_space_node_runtime_core::launch::ProcessHandle::open(pid) {
                    Ok(process) => process.has_exited().map_err(|error| Status::unavailable(format!("observe runtime exit: {error}"))),
                    Err(error) if error.is_not_found() => Ok(true),
                    Err(error) => Err(Status::unavailable(format!("pin runtime: {error}"))),
                }
            };
            if !exited()? { return Ok(None); }
            let fence = match service.try_cleanup_fence(&observed.id) {
                CleanupLockTryResult::Acquired(fence) => fence,
                CleanupLockTryResult::Busy => return Ok(None),
            };
            let _writer = service.state_writer_lease("RuntimeExitRecovery")?;
            let current = service.lookup_sandbox(&observed.id)?;
            if current.state != SandboxState::Started || current.node_execution_id.is_none()
                || current.node_execution_id != observed.node_execution_id
                || current.pid != observed.pid {
                return Ok(None);
            }
            if !exited()? { return Ok(None); }
            let admission = service.admit_lifecycle_cleanup_fenced(sandbox_id, CleanupAdmissionSpec {
                mode: CleanupAdmissionMode::ObservedState {
                    state: SandboxState::Started,
                    execution_generation: Some(current.space_api_route_identity.execution_generation),
                },
                template_filter: None,
                require_template_owner: false,
                target_state: SandboxState::Stopping,
                keep_filesystem: true,
                completion: CleanupCompletion::MarkStopped,
                immediate_kill: true,
                guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                persist_intent: true,
            }, fence)?;
            service.ensure_cleanup_executor();
            tracing::warn!(sandbox_id = %observed.id, pid, "runtime exited; admitted fenced cleanup preserving sandbox disk");
            Ok(Some(admission))
        }).await.map_err(|error| Status::internal(format!("runtime recovery: {error}")))?
    }

    pub(super) async fn admit_lifecycle_cleanup_from_observed_state(
        &self,
        sandbox_id: SandboxId,
        claim: ObservedCleanupClaim,
    ) -> Result<CleanupAdmission, CleanupAdmissionError> {
        self.admit_lifecycle_cleanup_inner(
            sandbox_id,
            CleanupAdmissionSpec {
                mode: CleanupAdmissionMode::ObservedState {
                    state: claim.observed_state,
                    execution_generation: claim.observed_execution_generation,
                },
                template_filter: None,
                require_template_owner: false,
                target_state: claim.target_state,
                keep_filesystem: claim.keep_filesystem,
                completion: claim.completion,
                immediate_kill: Self::cleanup_immediate_kill(claim.completion),
                guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                persist_intent: claim.persist_intent,
            },
        )
        .await
    }

    /// A graceful blocking stop, admitted only while `sandbox_id` is in
    /// `state`, started or paused, and still running the node execution
    /// `execution` began.
    pub(super) async fn admit_stop_for_activation(
        &self,
        sandbox_id: SandboxId,
        state: SandboxState,
        execution: pplx_space_common_core::types::SandboxActivationAttemptId,
    ) -> Result<CleanupAdmission, CleanupAdmissionError> {
        // The claim checks that the sandbox is still in `state`, so admitting
        // only a running state leaves an execution already at rest, such as
        // one the user suspended, alone.
        if !matches!(state, SandboxState::Started | SandboxState::Paused) {
            return Err(
                Status::aborted(format!("the execution to stop is {state}, not running")).into(),
            );
        }
        self.admit_lifecycle_cleanup_inner(
            sandbox_id,
            CleanupAdmissionSpec {
                mode: CleanupAdmissionMode::ObservedActivation { state, execution },
                template_filter: None,
                require_template_owner: false,
                target_state: SandboxState::Stopping,
                keep_filesystem: true,
                completion: CleanupCompletion::MarkStopped,
                immediate_kill: false,
                guest_flush: crate::db::cleanup_jobs::GuestFlush::BeforeTermination,
                persist_intent: false,
            },
        )
        .await
    }

    pub(super) async fn admit_lifecycle_cleanup_for_observed_execution(
        &self,
        sandbox_id: SandboxId,
        target_state: SandboxState,
        keep_filesystem: bool,
        completion: CleanupCompletion,
        persist_intent: bool,
        observed_execution_generation: Option<
            Option<pplx_space_common_core::types::ExecutionGeneration>,
        >,
    ) -> Result<CleanupAdmission, CleanupAdmissionError> {
        let mode = observed_execution_generation.map_or(
            CleanupAdmissionMode::Unfenced,
            |execution_generation| CleanupAdmissionMode::ObservedExecution {
                execution_generation,
            },
        );
        self.admit_lifecycle_cleanup_inner(
            sandbox_id,
            CleanupAdmissionSpec {
                mode,
                template_filter: None,
                require_template_owner: false,
                target_state,
                keep_filesystem,
                completion,
                immediate_kill: Self::cleanup_immediate_kill(completion),
                guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                persist_intent,
            },
        )
        .await
    }

    pub(super) async fn admit_auto_lifecycle_delete(
        &self,
        sandbox_id: SandboxId,
        expectation: crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteExpectation,
    ) -> Result<CleanupAdmission, CleanupAdmissionError> {
        self.admit_lifecycle_cleanup_inner(
            sandbox_id,
            CleanupAdmissionSpec {
                mode: CleanupAdmissionMode::AutoLifecycleDelete { expectation },
                template_filter: None,
                require_template_owner: false,
                target_state: SandboxState::Deleting,
                keep_filesystem: false,
                completion: CleanupCompletion::DeleteRow,
                immediate_kill: false,
                guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                persist_intent: true,
            },
        )
        .await
    }

    pub(super) fn check_cleanup_selection(
        db: &crate::db::Connection,
        sandbox_id: &str,
        expected_template: Option<&str>,
        require_template_owner: bool,
    ) -> Result<(), Status> {
        let target = crate::db::read::sandbox_view(db, sandbox_id)
            .cleanup_target()
            .map_err(|error| Status::unavailable(format!("inspect cleanup template: {error}")))?;
        if require_template_owner
            && target
                .as_ref()
                .is_none_or(|target| target.user.kind() != SandboxOwnerKind::TemplateManager)
        {
            return Err(Status::permission_denied(
                "space-tmplmgr may only clean up template-owned sandboxes",
            ));
        }
        if expected_template.is_some_and(|expected| {
            target
                .as_ref()
                .is_none_or(|target| target.template != expected)
        }) {
            return Err(Status::aborted(
                "cleanup target no longer matches template filter",
            ));
        }
        Ok(())
    }

    pub(super) async fn admit_selected_cleanup(
        &self,
        sandbox_id: SandboxId,
        completion: CleanupCompletion,
        immediate_kill: bool,
        persist_intent: bool,
        template_filter: Option<&str>,
        require_template_owner: bool,
    ) -> Result<CleanupAdmission, CleanupAdmissionError> {
        let template_filter = template_filter
            .map(pplx_space_common_core::types::TemplateName::parse)
            .transpose()
            .map_err(|error| {
                Status::invalid_argument(format!("invalid template filter: {error}"))
            })?;
        let stop = match completion {
            CleanupCompletion::MarkStopped => true,
            CleanupCompletion::DeleteRow => false,
            _ => return Err(Status::internal("invalid selected cleanup completion").into()),
        };
        let guest_flush = if !stop {
            crate::db::cleanup_jobs::GuestFlush::Unrequested
        } else if immediate_kill {
            crate::db::cleanup_jobs::GuestFlush::Forbidden
        } else {
            crate::db::cleanup_jobs::GuestFlush::BeforeTermination
        };
        self.admit_lifecycle_cleanup_inner(
            sandbox_id,
            CleanupAdmissionSpec {
                mode: CleanupAdmissionMode::Unfenced,
                template_filter,
                require_template_owner,
                target_state: if stop {
                    SandboxState::Stopping
                } else {
                    SandboxState::Deleting
                },
                keep_filesystem: stop,
                completion,
                persist_intent,
                immediate_kill,
                guest_flush,
            },
        )
        .await
    }

    async fn admit_lifecycle_cleanup_inner(
        &self,
        sandbox_id: SandboxId,
        spec: CleanupAdmissionSpec,
    ) -> Result<CleanupAdmission, CleanupAdmissionError> {
        let id = sandbox_id.to_string();
        let fence = CleanupLockGuard::new(
            Arc::clone(&self.cleanup_locks),
            id.clone(),
            self.cleanup_lock(&id),
        )
        .lock_owned()
        .await;
        let permit = Arc::clone(&self.cleanup_io)
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("cleanup storage closed"))?;
        let service = self.clone();
        // The caller's span, so the admission's events and the cleanup job it
        // persists belong to the caller's trace, as `run_blocking_io`'s do.
        let span = tracing::Span::current();
        let result = tokio::task::spawn_blocking(move || {
            let _entered = span.enter();
            let _permit = permit;
            let _writer = service.state_writer_lease("CleanupAdmission")?;
            // Ownership travels with the transaction if its caller disappears.
            let result = service.admit_lifecycle_cleanup_fenced(sandbox_id, spec, fence);
            service.ensure_cleanup_executor();
            result
        })
        .await
        .map_err(|error| Status::internal(format!("cleanup admission task: {error}")))?;
        #[cfg(test)]
        {
            let barrier = self
                .cleanup_executor
                .admission_barrier
                .lock()
                .unwrap()
                .clone();
            if let Some(barrier) = barrier {
                barrier.entered.notify_one();
                let _permit = barrier.release.acquire().await.unwrap();
            }
        }
        result
    }

    fn admit_lifecycle_cleanup_fenced(
        &self,
        sandbox_id: SandboxId,
        spec: CleanupAdmissionSpec,
        fence: OwnedCleanupFenceGuard,
    ) -> Result<CleanupAdmission, CleanupAdmissionError> {
        let CleanupAdmissionSpec {
            mode,
            template_filter,
            require_template_owner,
            target_state,
            keep_filesystem,
            completion,
            persist_intent,
            immediate_kill,
            guest_flush,
        } = spec;
        // Fenced callers depend on readable generation lineage. Auto-lifecycle
        // deletion requires the lifecycle claim and intent write to be atomic;
        // observed-generation cleanup retains its best-effort local cleanup
        // fallback when those writes fail.
        let require_selection_check = template_filter.is_some() || require_template_owner;
        let require_generation_inspection =
            mode.requires_generation_inspection() || require_selection_check;
        let require_atomic_admission = mode.requires_atomic_admission() || require_selection_check;
        let sandbox_id_string = sandbox_id.to_string();
        let db = self
            .db
            .lock()
            .map_err(|_| Status::internal("database mutex poisoned during cleanup admission"))?;
        // An unfamiliar durable intent is not a persistence failure that can
        // authorize best-effort pathname cleanup. Check before any admission
        // branch can return Unpersisted, including transaction-begin failure.
        crate::db::read::sandbox_view(&db, &sandbox_id_string)
            .cleanup_episode()
            .strict()
            .map_err(|_| {
                Status::unavailable("cleanup ownership cannot be decoded; retain resources")
            })?;
        let transaction = match if mode.requires_immediate_transaction() || require_selection_check
        {
            crate::db::Transaction::immediate(&db)
        } else {
            crate::db::Transaction::deferred(&db)
        } {
            Ok(transaction) => transaction,
            Err(error) => {
                if require_generation_inspection {
                    return Err(Status::unavailable(format!(
                        "begin fenced cleanup admission failed: {error}"
                    ))
                    .into());
                }
                return Ok(CleanupAdmission::unpersisted(
                    sandbox_id,
                    None,
                    Some(format!("begin cleanup admission failed: {error}")),
                    fence,
                ));
            }
        };

        if require_selection_check {
            Self::check_cleanup_selection(
                &transaction,
                &sandbox_id_string,
                template_filter.as_ref().map(|template| template.as_str()),
                require_template_owner,
            )?;
        }

        if crate::db::read::sandbox_view(&transaction, &sandbox_id_string)
            .cleanup_episode()
            .strict()
            .map_err(|e| Status::internal(format!("read cleanup ownership: {e}")))?
            .is_some_and(|episode| episode.completion == CleanupCompletion::DeletePoolClone)
        {
            return Err(
                Status::failed_precondition("prepared clone disposal owns this sandbox").into(),
            );
        }
        if let Some(observed_state) = mode.observed_state() {
            let current_state = crate::db::read::sandbox(&transaction, &sandbox_id_string)
                .map_err(|error| {
                    Status::internal(format!(
                        "inspect lifecycle state during cleanup admission failed: {error}"
                    ))
                })?
                .map(|row| row.state);
            if current_state != Some(observed_state) {
                return Err(Status::aborted(format!(
                    "cleanup lifecycle changed from {observed_state} to {current_state:?}"
                ))
                .into());
            }
        }
        // Read under the cleanup fence every activation claims under, so no
        // restart can slip in between this check and the claim below.
        if let Some(observed_execution) = mode.observed_activation() {
            let current_execution = crate::db::read::sandbox(&transaction, &sandbox_id_string)
                .map_err(|error| {
                    Status::internal(format!(
                        "inspect node execution during cleanup admission failed: {error}"
                    ))
                })?
                .and_then(|row| row.node_execution_id);
            if current_execution != Some(observed_execution) {
                return Err(Status::aborted(format!(
                    "cleanup execution changed from {observed_execution} to {current_execution:?}"
                ))
                .into());
            }
        }
        if target_state == SandboxState::Deleting && completion.deletes_row() {
            let erase_safety = crate::db::read::sandbox(&transaction, &sandbox_id_string)
                .map_err(|error| {
                    Status::internal(format!(
                        "inspect suspend publication during delete admission failed: {error}"
                    ))
                })?
                .map(|row| (row.state, row.erase_safety()));
            match erase_safety {
                Some((state, crate::db::SandboxEraseSafety::SuspendPublicationPending)) => {
                    tracing::info!(
                        sandbox_id = %sandbox_id,
                        state = %state,
                        "deferring delete: suspend state awaits publication to the cluster"
                    );
                    return Err(CleanupAdmissionError::SuspendPublicationPending {
                        sandbox_id,
                        state,
                    });
                }
                Some((_, crate::db::SandboxEraseSafety::RetainedFilesystemOnlyCopy)) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        "deferring delete: failed suspend filesystem may be the only copy"
                    );
                    return Err(CleanupAdmissionError::RetainedFilesystemOnlyCopy { sandbox_id });
                }
                Some((_, crate::db::SandboxEraseSafety::Safe)) | None => {}
            }
        }
        let current_execution_generation =
            match crate::db::read::sandbox_view(&transaction, &sandbox_id_string)
                .runtime_execution_generation()
            {
                Ok(generation) => generation,
                Err(error) if require_generation_inspection => {
                    return Err(Status::unavailable(format!(
                        "inspect runtime generation during fenced cleanup admission failed: {error}"
                    ))
                    .into());
                }
                Err(error) => {
                    return Ok(CleanupAdmission::unpersisted(
                        sandbox_id,
                        None,
                        Some(format!(
                            "inspect runtime generation during cleanup admission failed: {error}"
                        )),
                        fence,
                    ));
                }
            };
        if let Some(observed_execution_generation) = mode.observed_execution_generation()
            && current_execution_generation != observed_execution_generation
        {
            return Err(Status::aborted(format!(
                "cleanup execution changed from {observed_execution_generation:?} to {current_execution_generation:?}"
            ))
            .into());
        }
        if completion.deletes_row()
            && mode.auto_lifecycle_delete_expectation().is_none()
            && crate::db::read::typed_sandbox_view(&transaction, sandbox_id)
                .delete_receipts()
                .pending()
                .map_err(|error| {
                    Status::unavailable(format!(
                        "inspect pending auto-lifecycle delete receipt: {error}"
                    ))
                })?
                .is_some()
        {
            return Err(Status::unavailable(
                "pending auto-lifecycle delete receipt owns sandbox deletion",
            )
            .into());
        }
        let auto_lifecycle_delete_admission = if let Some(expectation) =
            mode.auto_lifecycle_delete_expectation()
        {
            let admission = match
                crate::db::auto_lifecycle_delete_receipts::classify_and_record_admission(
                    &transaction,
                    sandbox_id,
                    self.node_role,
                    self.node_managed_by,
                    expectation,
                )
            {
                Ok(admission) => admission,
                Err(error @ crate::db::auto_lifecycle_delete_receipts::AutoLifecycleDeleteEvidenceError::PolicyChanged) => {
                    pplx_space_metrics::sandbox::worker::auto_lifecycle_delete_admission(
                        pplx_space_metrics::sandbox::worker::AutoLifecycleDeleteAdmissionDisposition::Failed,
                        pplx_space_metrics::sandbox::worker::AutoLifecycleDeleteAdmissionReason::PolicyChanged,
                    );
                    emit_sandbox_event(
                        &sandbox_id_string,
                        SandboxEventType::LifecycleTransitionCasLost,
                        SandboxEventSource::SbxManager,
                        SandboxEventDetails::default()
                            .trigger(SandboxEventTrigger::AutoLifecycle)
                            .operation("auto_lifecycle_delete")
                            .reason("policy_changed"),
                    );
                    tracing::debug!(sandbox_id = %sandbox_id, %error, "auto-lifecycle delete policy changed before admission");
                    return Err(CleanupAdmissionError::AutoLifecycleDeleteCasLost {
                        sandbox_id,
                        reason: AutoLifecycleDeleteCasLostReason::PolicyChanged,
                    });
                }
                Err(error @ crate::db::auto_lifecycle_delete_receipts::AutoLifecycleDeleteEvidenceError::AuthorityChanged { .. }) => {
                    pplx_space_metrics::sandbox::worker::auto_lifecycle_delete_admission(
                        pplx_space_metrics::sandbox::worker::AutoLifecycleDeleteAdmissionDisposition::Failed,
                        pplx_space_metrics::sandbox::worker::AutoLifecycleDeleteAdmissionReason::AuthorityChanged,
                    );
                    emit_sandbox_event(
                        &sandbox_id_string,
                        SandboxEventType::LifecycleTransitionCasLost,
                        SandboxEventSource::SbxManager,
                        SandboxEventDetails::default()
                            .trigger(SandboxEventTrigger::AutoLifecycle)
                            .operation("auto_lifecycle_delete")
                            .reason("authority_changed"),
                    );
                    tracing::debug!(sandbox_id = %sandbox_id, %error, "auto-lifecycle delete authority changed before admission");
                    return Err(CleanupAdmissionError::AutoLifecycleDeleteCasLost {
                        sandbox_id,
                        reason: AutoLifecycleDeleteCasLostReason::AuthorityChanged,
                    });
                }
                Err(error) => {
                    return Err(Status::unavailable(format!(
                        "inspect auto-lifecycle delete authority: {error}"
                    ))
                    .into());
                }
            };
            if let crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteAdmission::Blocked {
                reason,
                changed,
            } = admission
            {
                transaction.commit().map_err(|error| {
                    Status::unavailable(format!("persist auto-lifecycle delete block: {error}"))
                })?;
                if changed {
                    emit_sandbox_event(
                        &sandbox_id_string,
                        SandboxEventType::DeleteFailed,
                        SandboxEventSource::SbxManager,
                        SandboxEventDetails::default()
                            .trigger(SandboxEventTrigger::AutoLifecycle)
                            .operation("auto_lifecycle_delete")
                            .reason(reason.as_str())
                            .error("auto-lifecycle delete lacks durable publication authority"),
                    );
                }
                pplx_space_metrics::sandbox::worker::auto_lifecycle_delete_admission(
                    pplx_space_metrics::sandbox::worker::AutoLifecycleDeleteAdmissionDisposition::Blocked,
                    reason.metric_reason(),
                );
                return Err(CleanupAdmissionError::AutoLifecycleDeleteBlocked {
                    sandbox_id,
                    reason,
                });
            }
            Some(admission)
        } else {
            None
        };
        let guest_flush = guest_flush.for_superseded_state(
            crate::db::read::sandbox_view(&transaction, &sandbox_id_string)
                .current_state_label()
                .ok()
                .flatten()
                .and_then(|state| state.parse().ok()),
        );
        match crate::db::update_state_superseding_activation_in_transaction(
            &transaction,
            &sandbox_id_string,
            target_state,
        ) {
            Ok(()) => {}
            Err(crate::db::SupersedeActivationError::QuarantineRecoveryInProgress) => {
                return Err(CleanupAdmissionError::QuarantineRecoveryInProgress { sandbox_id });
            }
            Err(crate::db::SupersedeActivationError::StateWriteNotApplied(
                crate::db::SandboxStateWriteNotApplied::Missing,
            )) => {
                if mode.auto_lifecycle_delete_expectation().is_some() {
                    return Err(Status::unavailable(
                        "sandbox disappeared during auto-lifecycle delete admission",
                    )
                    .into());
                }
                return Ok(CleanupAdmission::unpersisted(
                    sandbox_id,
                    current_execution_generation,
                    None,
                    fence,
                ));
            }
            Err(crate::db::SupersedeActivationError::StateWriteNotApplied(error)) => {
                return Err(Status::unavailable(format!(
                    "cleanup lifecycle claim was superseded: {error}"
                ))
                .into());
            }
            Err(error) => {
                if require_atomic_admission {
                    return Err(Status::unavailable(format!(
                        "claim atomic cleanup admission failed: {error}"
                    ))
                    .into());
                }
                return Ok(CleanupAdmission::unpersisted(
                    sandbox_id,
                    current_execution_generation,
                    Some(format!("claim cleanup lifecycle failed: {error}")),
                    fence,
                ));
            }
        }
        if mode.auto_lifecycle_delete_expectation().is_some()
            && crate::db::read::sandbox_view(&transaction, &sandbox_id_string)
                .cleanup_episode()
                .strict()
                .map_err(|error| {
                    Status::unavailable(format!(
                        "inspect cleanup episode after auto-lifecycle delete claim: {error}"
                    ))
                })?
                .is_some()
        {
            return Err(Status::unavailable(
                "auto-lifecycle delete claim did not supersede the cleanup episode",
            )
            .into());
        }
        if persist_intent
            && let Err(error) = crate::db::set_cleanup_intent(
                &transaction,
                &sandbox_id_string,
                keep_filesystem,
                completion,
            )
        {
            if require_atomic_admission {
                return Err(Status::unavailable(format!(
                    "persist atomic cleanup intent failed: {error}"
                ))
                .into());
            }
            return Ok(CleanupAdmission::unpersisted(
                sandbox_id,
                current_execution_generation,
                Some(format!("persist cleanup intent failed: {error}")),
                fence,
            ));
        }
        let (auto_lifecycle_delete_receipt, admission_metric) =
            match auto_lifecycle_delete_admission {
                Some(
                    crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteAdmission::Receipt(
                        evidence,
                    ),
                ) => {
                    let expectation = mode.auto_lifecycle_delete_expectation().ok_or_else(|| {
                        Status::internal(
                            "receipt admission is missing its auto-lifecycle delete expectation",
                        )
                    })?;
                    let receipt = match crate::db::auto_lifecycle_delete_receipts::insert_pending_receipt(
                            &transaction,
                            crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt {
                            receipt_id: pplx_space_common_core::types::AutoLifecycleDeleteReceiptId::generate(),
                            sandbox_id,
                            execution_generation: evidence.execution_generation,
                        route_activation_id: evidence.activation_attempt_id,
                        policy_fingerprint: expectation.policy_fingerprint,
                            },
                        ) {
                            Ok(receipt) => receipt,
                            Err(crate::db::auto_lifecycle_delete_receipts::InsertPendingReceiptError::Conflict(error)) => {
                                pplx_space_metrics::sandbox::worker::auto_lifecycle_delete_admission(
                                    pplx_space_metrics::sandbox::worker::AutoLifecycleDeleteAdmissionDisposition::Failed,
                                    pplx_space_metrics::sandbox::worker::AutoLifecycleDeleteAdmissionReason::ReceiptConflict,
                                );
                                tracing::debug!(sandbox_id = %sandbox_id, %error, "auto-lifecycle delete receipt evidence conflicts with the current claim");
                                return Err(CleanupAdmissionError::AutoLifecycleDeleteReceiptConflict {
                                    sandbox_id,
                                });
                            }
                            Err(crate::db::auto_lifecycle_delete_receipts::InsertPendingReceiptError::Database(error)) => {
                                return Err(Status::unavailable(format!(
                                    "persist auto-lifecycle delete receipt: {error}"
                                ))
                                .into());
                            }
                        };
                    (
                    Some(receipt),
                    Some((
                        pplx_space_metrics::sandbox::worker::AutoLifecycleDeleteAdmissionDisposition::ReceiptClaimed,
                        pplx_space_metrics::sandbox::worker::AutoLifecycleDeleteAdmissionReason::ClusterFenced,
                    )),
                )
                }
                Some(
                    crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteAdmission::Ordinary(
                        reason,
                    ),
                ) => {
                    tracing::info!(
                        sandbox_id = %sandbox_id,
                        reason = reason.as_str(),
                        "auto-lifecycle delete proceeds without a central completion receipt"
                    );
                    (
                    None,
                    Some((
                        pplx_space_metrics::sandbox::worker::AutoLifecycleDeleteAdmissionDisposition::Receiptless,
                        reason.metric_reason(),
                    )),
                )
                }
                Some(
                    crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteAdmission::Blocked {
                        ..
                    },
                ) => {
                    return Err(Status::internal(
                        "blocked auto-lifecycle delete admission reached lifecycle mutation",
                    )
                    .into());
                }
                None => (None, None),
            };
        if let Some(uuid8) = crate::db::read::sandbox_view(&transaction, &sandbox_id_string)
            .cleanup_runtime_label()
            .map_err(|error| {
                Status::unavailable(format!("inspect cleanup scheduling identity: {error}"))
            })?
            && let Err(error) = crate::db::cleanup_jobs::persist(
                &transaction,
                crate::db::cleanup_jobs::CleanupWork {
                    version: 1,
                    sandbox_id,
                    uuid8,
                    execution_generation: current_execution_generation,
                    cleanup_id: None,
                    completion,
                    keep_filesystem,
                    immediate_kill,
                    guest_flush,
                    published_artifact: None,
                },
            )
        {
            if require_atomic_admission {
                return Err(
                    Status::unavailable(format!("persist cleanup scheduling: {error}")).into(),
                );
            }
            return Ok(CleanupAdmission::unpersisted(
                sandbox_id,
                current_execution_generation,
                Some(format!("persist cleanup scheduling: {error}")),
                fence,
            ));
        }
        if let Err(error) = transaction.commit() {
            if require_atomic_admission {
                return Err(Status::unavailable(format!(
                    "commit atomic cleanup admission failed: {error}"
                ))
                .into());
            }
            return Ok(CleanupAdmission::unpersisted(
                sandbox_id,
                current_execution_generation,
                Some(format!("commit cleanup admission failed: {error}")),
                fence,
            ));
        }
        if let Some((disposition, reason)) = admission_metric {
            pplx_space_metrics::sandbox::worker::auto_lifecycle_delete_admission(
                disposition,
                reason,
            );
        }
        Ok(CleanupAdmission::persisted(
            sandbox_id,
            current_execution_generation,
            auto_lifecycle_delete_receipt,
            fence,
        ))
    }

    pub(super) async fn dispatch_cleanup_if_generation_current<F, Fut>(
        db: &crate::db::SharedConnection,
        sandbox_id: &str,
        expected_execution_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
        cleanup_fence: &(impl CleanupFenceAuthorization + ?Sized),
        dispatch: F,
    ) -> Result<CleanupDispatchDisposition, Status>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<(), Status>>,
    {
        if !cleanup_fence.authorizes(sandbox_id) {
            return Err(Status::internal(
                "cleanup fence does not match dispatch sandbox",
            ));
        }
        let current_execution_generation = {
            let db = db
                .lock()
                .map_err(|_| Status::internal("database mutex poisoned"))?;
            crate::db::read::sandbox_view(&db, sandbox_id)
                .runtime_execution_generation()
                .map_err(|error| {
                    Status::internal(format!("inspect runtime generation failed: {error}"))
                })?
        };
        if current_execution_generation != expected_execution_generation {
            return Ok(CleanupDispatchDisposition::Superseded);
        }
        dispatch().await?;
        Ok(CleanupDispatchDisposition::Dispatched)
    }

    pub(super) async fn dispatch_node_action_using_admission<F, Fut>(
        &self,
        sandbox_id: SandboxId,
        admission: &CleanupAdmission,
        dispatch: F,
    ) -> Result<CleanupDispatchDisposition, Status>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<(), Status>>,
    {
        let sandbox_id_string = sandbox_id.to_string();
        let expected_execution_generation = admission.expected_execution_generation(sandbox_id)?;
        let fence = admission.fence_for_sandbox(sandbox_id)?;
        Self::dispatch_cleanup_if_generation_current(
            &self.db,
            &sandbox_id_string,
            expected_execution_generation,
            fence,
            dispatch,
        )
        .await
    }

    pub(super) fn activation_cleanup_guard(
        &self,
        sandbox_id: &str,
        uuid8: &SandboxRuntimeLabel,
        spec: ActivationCleanupSpec,
    ) -> Result<ActivationCleanupGuard, Status> {
        let ActivationCleanupSpec {
            cleanup_id,
            completion,
            context,
            interrupted_reason_code,
            interrupted_reason,
        } = spec;
        let (sandbox_id, uuid8) = Self::cleanup_identity(sandbox_id, Some(uuid8))?;
        let service = self.clone();
        Ok(ActivationCleanupGuard::with_interruption_reason(
            uuid8,
            interrupted_reason_code,
            interrupted_reason,
            move |uuid8, permit, reason_code, reason| {
                service.schedule_activation_cleanup(
                    AsyncCleanupRequest {
                        sandbox_id,
                        uuid8,
                        keep_filesystem: !completion.deletes_row(),
                        context,
                        completion,
                        immediate_kill: SandboxManagerService::cleanup_immediate_kill(completion),
                        guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                        persist_intent: false,
                        cleanup_id: Some(cleanup_id),
                    },
                    cleanup_id,
                    reason_code,
                    reason,
                    permit,
                );
            },
        ))
    }

    #[allow(
        clippy::expect_used,
        reason = "Private lock map executes only string-key/Arc operations, never caller code or fallible callbacks"
    )]
    pub(super) fn cleanup_lock(&self, sandbox_id: &str) -> CleanupLock {
        let mut locks = self.cleanup_locks.lock().expect("mutex poisoned");
        locks
            .entry(sandbox_id.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    pub(super) fn try_cleanup_fence(&self, sandbox_id: &str) -> CleanupLockTryResult {
        CleanupLockGuard::new(
            Arc::clone(&self.cleanup_locks),
            sandbox_id.to_string(),
            self.cleanup_lock(sandbox_id),
        )
        .try_acquire_owned()
    }

    #[allow(
        clippy::expect_used,
        reason = "Private lock map executes only string-key/Arc operations, never caller code or fallible callbacks"
    )]
    pub(super) fn prune_cleanup_lock(locks: &CleanupLockMap, sandbox_id: &str, lock: &CleanupLock) {
        let mut locks = locks.lock().expect("mutex poisoned");
        if Arc::strong_count(lock) == 2
            && locks
                .get(sandbox_id)
                .is_some_and(|stored| Arc::ptr_eq(stored, lock))
        {
            locks.remove(sandbox_id);
        }
    }

    pub(super) async fn collect_net_gateway_stats(
        &self,
        sandbox_id: &str,
        probe_net_gateway: bool,
        gateway_pid: Option<u32>,
        fallback_tx_bytes: u64,
        fallback_rx_bytes: u64,
    ) -> NetGatewayStatsSnapshot {
        let fallback = NetGatewayStatsSnapshot::persisted(fallback_tx_bytes, fallback_rx_bytes);
        if !probe_net_gateway {
            return fallback;
        }

        // A sandbox row's id is the UUID create assigned; one that does not
        // parse fails the probe as an absent gateway socket would.
        let kept_gateway = sandbox_id.parse::<SandboxId>().map(|id| {
            (
                id,
                gateway_pid.and_then(|pid| LinuxPid::try_from_u32(pid).ok()),
            )
        });
        let result = tokio::time::timeout(NET_GATEWAY_FOREGROUND_STATS_TIMEOUT, async {
            let (id, gateway_pid) = kept_gateway.clone().map_err(|error| error.to_string())?;
            let mut client = crate::net_gateway_clients::client(id, gateway_pid)
                .await
                .map_err(|error| error.to_string())?;
            client
                .get_stats(pplx_space_net_gateway_client::proto::GetStatsRequest {})
                .await
                .map(Response::into_inner)
                .map_err(|error| error.to_string())
        })
        .await;
        if !matches!(result, Ok(Ok(_)))
            && let Ok((id, gateway_pid)) = kept_gateway
        {
            crate::net_gateway_clients::discard(id, gateway_pid);
        }

        let stats = match result {
            Ok(Ok(stats)) => stats,
            Ok(Err(error)) => {
                tracing::debug!(
                    sandbox_id = %sandbox_id,
                    %error,
                    "live net-gateway stats unavailable"
                );
                return fallback;
            }
            Err(_) => {
                tracing::debug!(
                    sandbox_id = %sandbox_id,
                    timeout = ?NET_GATEWAY_FOREGROUND_STATS_TIMEOUT,
                    "live net-gateway stats probe timed out"
                );
                return fallback;
            }
        };

        let network_tx_bytes = stats.network_tx_bytes.max(fallback_tx_bytes);
        let network_rx_bytes = stats.network_rx_bytes.max(fallback_rx_bytes);

        self.watchdog
            .update_network_counters(sandbox_id, network_tx_bytes, network_rx_bytes)
            .await;

        {
            let Ok(db) = self.db_conn() else {
                return fallback;
            };
            if let Err(error) = crate::db::update_network_counters(
                &db,
                sandbox_id,
                network_tx_bytes,
                network_rx_bytes,
            ) {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    %error,
                    "failed to persist live net-gateway counters"
                );
            }
        }

        NetGatewayStatsSnapshot {
            reachable: true,
            tcp_active_connections: stats.tcp_active_connections,
            tcp_total_connections: stats.tcp_total_connections,
            tcp_denied_connections: stats.tcp_denied_connections,
            udp_active_mappings: stats.udp_active_mappings,
            network_tx_bytes,
            network_rx_bytes,
            bytes_forwarded: stats
                .bytes_forwarded
                .max(network_tx_bytes.saturating_add(network_rx_bytes)),
        }
    }

    pub(super) fn cleanup_row_matches_completion(
        row: &crate::db::SandboxRow,
        completion: CleanupCompletion,
    ) -> bool {
        completion.owns_row_intent(row)
    }

    pub(super) fn cleanup_attempt_still_current(
        db: &crate::db::SharedConnection,
        sandbox_id: &str,
        completion: CleanupCompletion,
    ) -> Result<bool, String> {
        let db = db
            .lock()
            .map_err(|error| format!("database mutex poisoned: {error}"))?;

        if completion.deletes_row() {
            let sandbox_id = sandbox_id
                .parse::<SandboxId>()
                .map_err(|error| error.to_string())?;
            if !crate::db::read::typed_sandbox_view(&db, sandbox_id)
                .delete_receipts()
                .pending_matches_observed_execution()
                .map_err(|error| error.to_string())?
            {
                return Ok(false);
            }
        }
        crate::db::read::sandbox_view(&db, sandbox_id)
            .cleanup_intent_current(completion)
            .map_err(|error| error.to_string())
    }

    fn async_cleanup_attempt_still_current(
        db: &crate::db::SharedConnection,
        sandbox_id: &str,
        completion: CleanupCompletion,
        cleanup_id: Option<SandboxCleanupEpisodeId>,
    ) -> Result<bool, String> {
        if let Some(cleanup_id) = cleanup_id {
            let db = db
                .lock()
                .map_err(|error| format!("database mutex poisoned: {error}"))?;

            return crate::db::read::sandbox_view(&db, sandbox_id)
                .cleanup_episode()
                .current(cleanup_id)
                .map_err(|error| error.to_string());
        }
        Self::cleanup_attempt_still_current(db, sandbox_id, completion)
    }

    fn validate_quarantine_recovery_artifact(
        db: &crate::db::SharedConnection,
        sandbox_id: &str,
        cleanup_id: SandboxCleanupEpisodeId,
        verdicts: &crate::retained_source::RetainedSourceVerdicts,
    ) -> Result<(), QuarantineRecoveryArtifactError> {
        let expected_artifact_id = {
            let db = db
                .lock()
                .map_err(|_| QuarantineRecoveryArtifactError::DatabaseMutexPoisoned)?;
            let episode = crate::db::read::sandbox_view(&db, sandbox_id)
                .cleanup_episode()
                .strict()?;
            let Some(episode) = episode.filter(|episode| episode.id == cleanup_id) else {
                return Ok(());
            };
            if episode.entered_from_state != Some(SandboxState::Quarantined) {
                return Ok(());
            }
            episode
                .quarantine_recovery
                .map(|recovery| recovery.local_artifact_id)
                .ok_or(QuarantineRecoveryArtifactError::MissingRecoveryFence)?
        };
        let sandbox_id_typed = sandbox_id.parse::<SandboxId>()?;
        let artifact =
            super::suspend_snapshot_publication::validate_canonical_suspend_snapshot_for_recovery(
                Path::new(CACHE_DIR),
                sandbox_id_typed,
                verdicts,
            )?;
        if artifact.local_artifact_id == expected_artifact_id {
            Ok(())
        } else {
            Err(QuarantineRecoveryArtifactError::ArtifactChanged {
                expected: expected_artifact_id,
                observed: artifact.local_artifact_id,
            })
        }
    }

    async fn finalize_successful_cleanup(
        &self,
        sandbox_id: &str,
        cleanup_id: SandboxCleanupEpisodeId,
        job_id: Option<&JobId>,
    ) -> CleanupFinalizationOutcome {
        let artifact_guard = self
            .auto_lifecycle
            .artifact_lock(sandbox_id)
            .lock_owned()
            .await;
        let owned_sandbox_id = sandbox_id.to_owned();
        // Inspected under the artifact lock, which the completion below keeps.
        let verdicts = self
            .inspect_retained_sources_in(
                vec![Path::new(CACHE_DIR).join(sandbox_id)],
                &mut Default::default(),
            )
            .await;
        let recovery_validation = match self
            .blocking_io(move |service| {
                Ok(Self::validate_quarantine_recovery_artifact(
                    &service.db,
                    &owned_sandbox_id,
                    cleanup_id,
                    &verdicts,
                ))
            })
            .await
        {
            Ok(result) => result.map_err(ApplyCleanupCompletionError::RecoveryArtifact),
            Err(error) => Err(ApplyCleanupCompletionError::ArtifactValidationTask(
                error.to_string(),
            )),
        };
        let route_stream_sandbox_id = sandbox_id.parse().ok();
        let completion_result: Result<Option<()>, ApplyCleanupCompletionError> = self
            .apply_route_identity_mutations("cleanup_completion", || {
                let result = recovery_validation.and_then(|()| {
                    let db = self
                        .db
                        .lock()
                        .map_err(|_| ApplyCleanupCompletionError::DatabaseMutexPoisoned)?;
                    let applied = crate::db::complete_cleanup_episode_for_job(
                        &db, sandbox_id, cleanup_id, job_id,
                    )?;
                    if applied.is_some()
                        && let Err(error) = self.retire_absent_sandbox_policy(&db, sandbox_id)
                    {
                        tracing::warn!(sandbox_id, %error, "retaining policy evidence after episode cleanup");
                    }
                    Ok(applied)
                });
                result.map(|applied| {
                    let changed_ids = applied
                        .is_some()
                        .then_some(route_stream_sandbox_id)
                        .flatten()
                        .into_iter()
                        .collect();
                    (applied, changed_ids)
                })
            });

        let outcome = match completion_result {
            Ok(Some(())) => CleanupFinalizationOutcome::Applied,
            Ok(None) => CleanupFinalizationOutcome::Superseded,
            Err(error) if error.is_unsafe_evidence() => {
                let error_message = error.to_string();
                let quarantine_result = self
                    .db
                    .lock()
                    .map_err(|lock_error| lock_error.to_string())
                    .and_then(|db| {
                        crate::db::quarantine_cleanup_episode_for_job(
                            &db,
                            sandbox_id,
                            cleanup_id,
                            &error_message,
                            job_id,
                        )
                        .map_err(|quarantine_error| quarantine_error.to_string())
                    });
                match quarantine_result {
                    Ok(true) => {
                        CleanupFinalizationOutcome::UnsafeEvidenceQuarantined(error_message)
                    }
                    Ok(false) => CleanupFinalizationOutcome::Superseded,
                    Err(quarantine_error) => CleanupFinalizationOutcome::Retry(format!(
                        "{error}; failed to quarantine unsafe evidence: {quarantine_error}"
                    )),
                }
            }
            Err(error) => CleanupFinalizationOutcome::Retry(error.to_string()),
        };
        if matches!(
            outcome,
            CleanupFinalizationOutcome::Applied
                | CleanupFinalizationOutcome::UnsafeEvidenceQuarantined(_)
        ) {
            self.retire_sandbox_registrations(sandbox_id).await;
        }
        drop(artifact_guard);
        outcome
    }

    #[cfg(test)]
    pub(super) fn complete_async_cleanup(
        db: &crate::db::SharedConnection,
        sandbox_id: &str,
        completion: CleanupCompletion,
        published_artifact: Option<(LocalArtifactId, String)>,
    ) -> Result<(), CleanupCompletionError> {
        let conn = db
            .lock()
            .map_err(|_| CleanupCompletionError::DatabaseMutexPoisoned)?;
        crate::db::complete_row_intent_cleanup(
            &conn,
            sandbox_id,
            completion,
            published_artifact
                .as_ref()
                .map(|(artifact_id, path)| (*artifact_id, path.as_str())),
        )
        .map_err(CleanupCompletionError::from)
    }

    pub(super) fn complete_cleanup_after_node_release(
        db: &crate::db::SharedConnection,
        sandbox_id: &str,
        completion: CleanupCompletion,
        expected_execution_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
        auto_lifecycle_delete_receipt: Option<
            crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt,
        >,
        published_artifact: Option<(LocalArtifactId, String)>,
    ) -> Result<CleanupCompletionDisposition, CleanupCompletionError> {
        Self::complete_cleanup_after_node_release_with_hook(
            db,
            sandbox_id,
            completion,
            expected_execution_generation,
            auto_lifecycle_delete_receipt,
            published_artifact,
            || {},
        )
    }

    pub(super) fn complete_cleanup_after_node_release_with_hook(
        db: &crate::db::SharedConnection,
        sandbox_id: &str,
        completion: CleanupCompletion,
        expected_execution_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
        auto_lifecycle_delete_receipt: Option<
            crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt,
        >,
        published_artifact: Option<(LocalArtifactId, String)>,
        after_generation_check: impl FnOnce(),
    ) -> Result<CleanupCompletionDisposition, CleanupCompletionError> {
        let conn = db
            .lock()
            .map_err(|_| CleanupCompletionError::DatabaseMutexPoisoned)?;
        Self::complete_fenced_row_cleanup(
            &conn,
            RowCleanupCompletion {
                sandbox_id,
                completion,
                expected_execution_generation,
                auto_lifecycle_delete_receipt,
                published_artifact,
                job_id: None,
            },
            after_generation_check,
        )
    }

    fn complete_fenced_row_cleanup(
        conn: &crate::db::Connection,
        request: RowCleanupCompletion<'_>,
        after_generation_check: impl FnOnce(),
    ) -> Result<CleanupCompletionDisposition, CleanupCompletionError> {
        let RowCleanupCompletion {
            sandbox_id,
            completion,
            expected_execution_generation,
            auto_lifecycle_delete_receipt,
            published_artifact,
            job_id,
        } = request;
        if crate::db::read::sandbox_view(conn, sandbox_id).runtime_execution_generation()?
            != expected_execution_generation
        {
            return Ok(CleanupCompletionDisposition::Superseded);
        }
        after_generation_check();
        let disposition = crate::db::complete_row_intent_cleanup_for_job(
            conn,
            sandbox_id,
            completion,
            published_artifact
                .as_ref()
                .map(|(artifact_id, path)| (*artifact_id, path.as_str())),
            auto_lifecycle_delete_receipt,
            job_id,
        )?;
        Ok(match disposition {
            crate::db::CompleteRowIntentCleanupDisposition::Applied => {
                CleanupCompletionDisposition::Applied
            }
            crate::db::CompleteRowIntentCleanupDisposition::Quarantined => {
                CleanupCompletionDisposition::Quarantined
            }
            crate::db::CompleteRowIntentCleanupDisposition::Superseded => {
                CleanupCompletionDisposition::Superseded
            }
        })
    }

    pub(super) fn mark_async_cleanup_failed(
        db: &crate::db::SharedConnection,
        sandbox_id: &str,
        keep_filesystem: bool,
        completion: CleanupCompletion,
        reason: &str,
    ) {
        let updated = {
            let Ok(db) = db.lock() else {
                tracing::error!(
                    sandbox_id,
                    "cannot persist cleanup failure: database mutex poisoned"
                );
                return;
            };
            let row = match crate::db::read::sandbox(&db, sandbox_id) {
                Ok(row) => row,
                Err(error) => {
                    tracing::warn!(
                        sandbox_id,
                        %error,
                        "failed to load sandbox after async cleanup failure"
                    );
                    return;
                }
            };
            if row.is_some_and(|row| Self::cleanup_row_matches_completion(&row, completion)) {
                match crate::db::mark_cleanup_failed(
                    &db,
                    sandbox_id,
                    keep_filesystem,
                    completion,
                    reason,
                ) {
                    Ok(updated) => updated,
                    Err(error) => {
                        tracing::warn!(
                            sandbox_id,
                            %error,
                            "failed to persist sandbox cleanup failure"
                        );
                        false
                    }
                }
            } else {
                false
            }
        };
        if updated {
            mark_sandbox_dirty(db, sandbox_id);
        }
    }

    pub(super) fn persist_and_retry_cleanup_failure(
        &self,
        sandbox_id: SandboxId,
        uuid8: SandboxRuntimeLabel,
        keep_filesystem: bool,
        completion: CleanupCompletion,
        reason: &str,
    ) {
        Self::mark_async_cleanup_failed(
            &self.db,
            &sandbox_id.to_string(),
            keep_filesystem,
            completion,
            reason,
        );
        self.schedule_async_cleanup(AsyncCleanupRequest {
            sandbox_id,
            uuid8,
            keep_filesystem,
            context: "retry blocking cleanup failure",
            completion,
            immediate_kill: Self::cleanup_immediate_kill(completion),
            guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
            persist_intent: false,
            cleanup_id: None,
        });
    }

    pub(super) fn should_mark_async_cleanup_failed(error: &Status) -> bool {
        error.code() != tonic::Code::ResourceExhausted
    }

    pub(super) fn cleanup_return_after_slot_release(
        keep_filesystem: bool,
        completion: CleanupCompletion,
    ) -> bool {
        keep_filesystem
            && matches!(
                completion,
                CleanupCompletion::MarkCreated
                    | CleanupCompletion::MarkStopped
                    | CleanupCompletion::MarkSuspended
            )
    }

    pub(super) fn cleanup_immediate_kill(completion: CleanupCompletion) -> bool {
        matches!(
            completion,
            CleanupCompletion::MarkCreated
                | CleanupCompletion::MarkStopped
                | CleanupCompletion::MarkSuspended
        )
    }

    pub(super) fn cleanup_identity(
        sandbox_id: &str,
        uuid8: Option<&SandboxRuntimeLabel>,
    ) -> Result<(SandboxId, SandboxRuntimeLabel), Status> {
        let sandbox_id = SandboxId::from_str(sandbox_id)
            .map_err(|error| Status::invalid_argument(format!("invalid sandbox id: {error}")))?;
        let uuid8 = uuid8
            .cloned()
            .unwrap_or_else(|| SandboxRuntimeLabel::from_sandbox_id(sandbox_id));
        Ok((sandbox_id, uuid8))
    }

    pub(super) async fn run_cleanup_to_completion(
        &self,
        request: SynchronousCleanupRequest<'_>,
    ) -> Result<(), Status> {
        self.run_cleanup_to_completion_using_admission(request, None, None, CleanupWait::Attempt)
            .await
    }

    pub(super) async fn run_cleanup_episode_to_completion(
        &self,
        request: SynchronousCleanupRequest<'_>,
        cleanup_id: SandboxCleanupEpisodeId,
    ) -> Result<(), Status> {
        self.run_cleanup_to_completion_using_admission(
            request,
            None,
            Some(cleanup_id),
            CleanupWait::Attempt,
        )
        .await
    }

    pub(super) async fn run_admitted_cleanup_to_completion(
        &self,
        request: SynchronousCleanupRequest<'_>,
        admission: CleanupAdmission,
    ) -> Result<(), Status> {
        self.run_cleanup_to_completion_using_admission(
            request,
            Some(admission),
            None,
            CleanupWait::Attempt,
        )
        .await
    }

    pub(super) async fn run_admitted_stop_to_completion(
        &self,
        request: SynchronousCleanupRequest<'_>,
        admission: CleanupAdmission,
    ) -> Result<(), Status> {
        self.run_cleanup_to_completion_using_admission(
            request,
            Some(admission),
            None,
            CleanupWait::Stop,
        )
        .await
    }

    pub(super) async fn execute_cleanup_attempt(
        &self,
        request: SynchronousCleanupRequest<'_>,
        admission: Option<CleanupAdmission>,
        cleanup_id: Option<SandboxCleanupEpisodeId>,
    ) -> Result<CleanupOutcome, Status> {
        #[cfg(test)]
        {
            let dispatch = self.cleanup_executor.dispatch.lock().unwrap().clone();
            if let Some(dispatch) = dispatch {
                let id = request.sandbox_id.to_owned();
                let kill = request.immediate_kill;
                return self
                    .run_cleanup_to_completion_with_node_dispatch_and_admission(
                        request,
                        admission,
                        cleanup_id,
                        || async { Ok(()) },
                        |_| dispatch(id, kill),
                    )
                    .await;
            }
        }
        let sandbox_id = request.sandbox_id;
        let keep_filesystem = request.keep_filesystem;
        let context = request.context;
        let completion = request.completion;
        let immediate_kill = request.immediate_kill;
        let guest_flush = request.guest_flush;
        let return_after_slot_release =
            Self::cleanup_return_after_slot_release(keep_filesystem, completion);
        let job_id = match admission.as_ref() {
            Some(CleanupAdmission::Persisted {
                dispatch_job_id, ..
            }) => *dispatch_job_id,
            _ => None,
        };
        self.run_cleanup_to_completion_with_node_dispatch_and_admission(
            request,
            admission,
            cleanup_id,
            || async move {
                connect_node(&self.node_manager).await.map_err(|error| {
                    Status::internal(format!(
                        "{context}: cannot connect to node-manager: {error}"
                    ))
                })
            },
            |mut node_client| async move {
                // Before every termination attempt: writes can land between
                // a flush and a retried termination.
                let row = match self.lookup_sandbox(sandbox_id) {
                    Ok(row) => Some(row),
                    Err(error) if error.code() == tonic::Code::NotFound => None,
                    Err(error) => return Err(error),
                };
                use pplx_space_node_runtime_core::boot::{GuestProfile, GuestShutdown};
                let shutdown_policy = match row.as_ref() {
                    Some(row) => match row.execution_metadata.as_ref() {
                        Some(execution) => match GuestProfile::resolve(execution, row.guest_image) {
                            Ok(profile) => Some(profile.shutdown()),
                            Err(error) => {
                                tracing::warn!(sandbox_id, %error, "cannot resolve graceful shutdown; proceeding with fenced termination");
                                None
                            }
                        },
                        None if row.guest_image.legacy_transport() => Some(GuestShutdown::AgentFlush),
                        None => None,
                    },
                    None => Some(GuestShutdown::AgentFlush),
                };
                let shutdown = match shutdown_policy {
                    Some(GuestShutdown::Acpi) => Some(
                        self.stop_firmware_guest(
                            sandbox_id,
                            job_id.as_ref(),
                            immediate_kill
                                || guest_flush
                                    != crate::db::cleanup_jobs::GuestFlush::BeforeTermination,
                        )
                        .await?,
                    ),
                    Some(GuestShutdown::AgentFlush) => {
                        self.flush_guest_before_termination(sandbox_id, guest_flush)
                            .await;
                        None
                    }
                    None => Some(super::guest_image::ShutdownOutcome::Unclassified),
                };
                if shutdown.is_some_and(|outcome| {
                    !matches!(
                        outcome,
                        super::guest_image::ShutdownOutcome::GuestExitObserved
                    )
                }) {
                    tracing::warn!(
                        sandbox_id,
                        ?job_id,
                        ?shutdown,
                        "terminating guest without observed exit"
                    );
                }
                service_cleanup_via_node_with_options(
                    &self.db,
                    &mut node_client,
                    sandbox_id,
                    NodeCleanupOptions {
                        job_id: job_id.as_ref(),
                        keep_filesystem,
                        return_after_slot_release,
                        immediate_kill,
                    },
                    context,
                )
                .await?;
                if let Some(outcome) = shutdown {
                    let mut details = SandboxEventDetails::default().operation("runtime_termination").reason(outcome.as_str());
                    if let Some(row) = row
                        && let Some(generation) = row.space_api_route_identity.execution_generation
                    {
                        details = details.execution_generation(generation.to_string());
                    }
                    emit_sandbox_event(
                        sandbox_id,
                        SandboxEventType::CleanupCompleted,
                        SandboxEventSource::SbxManager,
                        details,
                    );
                }
                Ok(())
            },
        )
        .await
    }

    pub(super) async fn run_cleanup_to_completion_with_node_dispatch_and_admission<
        P,
        PFut,
        D,
        F,
        Fut,
    >(
        &self,
        request: SynchronousCleanupRequest<'_>,
        admission: Option<CleanupAdmission>,
        cleanup_id: Option<SandboxCleanupEpisodeId>,
        prepare_dispatch: P,
        node_dispatch: F,
    ) -> Result<CleanupOutcome, Status>
    where
        P: FnOnce() -> PFut,
        PFut: std::future::Future<Output = Result<D, Status>>,
        F: FnOnce(D) -> Fut,
        Fut: std::future::Future<Output = Result<(), Status>>,
    {
        let SynchronousCleanupRequest {
            sandbox_id,
            uuid8: _,
            keep_filesystem,
            context: _,
            completion,
            published_artifact,
            immediate_kill: _,
            guest_flush: _,
        } = request;
        // Cheap currentness pre-check *before* allocating the per-sandbox lock
        // entry: an already-stale or gone cleanup has no node-mgr work, so
        // return immediately rather than take the lock for a no-op (and avoid
        // creating a map entry just to prune it again). This reads only the db
        // mutex, not the per-sandbox lock, so it introduces no lock-ordering
        // hazard; the authoritative re-check happens under the lock below. A
        // transient inspect error falls through to that check.
        if admission.is_none()
            && matches!(
                Self::async_cleanup_attempt_still_current(
                    &self.db, sandbox_id, completion, cleanup_id,
                ),
                Ok(false)
            )
        {
            return Ok(CleanupOutcome::Superseded);
        }
        // The dispatcher owns capacity and the domain fence through completion.
        // Foreground kills have reserved capacity, and retries release their
        // slot between attempts without giving up their durable obligation.
        let parsed_sandbox_id = SandboxId::from_str(sandbox_id)
            .map_err(|error| Status::invalid_argument(format!("invalid sandbox id: {error}")))?;
        let acquired_fence;
        let fence: &dyn CleanupFenceAuthorization = match admission.as_ref() {
            Some(admission) => admission.fence_for_sandbox(parsed_sandbox_id)?,
            None => {
                let lock = self.cleanup_lock(sandbox_id);
                acquired_fence = CleanupLockGuard::new(
                    Arc::clone(&self.cleanup_locks),
                    sandbox_id.to_string(),
                    lock,
                )
                .lock_owned()
                .await;
                &acquired_fence
            }
        };
        let expected_execution_generation = match admission.as_ref() {
            Some(admission) => admission.expected_execution_generation(parsed_sandbox_id)?,
            None => {
                let db = self.db_conn()?;
                crate::db::read::sandbox_view(&db, sandbox_id)
                    .runtime_execution_generation()
                    .map_err(|error| {
                        Status::internal(format!("inspect runtime generation failed: {error}"))
                    })?
            }
        };
        let auto_lifecycle_delete_receipt = match admission.as_ref() {
            Some(admission) => admission.auto_lifecycle_delete_receipt(parsed_sandbox_id)?,
            None => None,
        };
        let _restore_staging_guard = if completion.deletes_row() {
            Some(
                self.auto_lifecycle
                    .staging_lock(sandbox_id)
                    .lock_owned()
                    .await,
            )
        } else {
            None
        };

        let job_id = match admission.as_ref() {
            Some(CleanupAdmission::Persisted {
                dispatch_job_id, ..
            }) => dispatch_job_id.as_ref(),
            _ => None,
        };
        let result = async {
            match Self::async_cleanup_attempt_still_current(
                &self.db, sandbox_id, completion, cleanup_id,
            ) {
                Ok(true) => {}
                Ok(false) => return Ok(CleanupOutcome::Superseded),
                Err(error) => {
                    return Err(Status::internal(format!(
                        "inspect cleanup target failed: {error}"
                    )));
                }
            }
            self.cancel_route_streams_for_generation(
                parsed_sandbox_id,
                expected_execution_generation,
            )?;

            if completion.deletes_row()
                && let Err(error) = self
                    .cancel_restore_transfers_and_remove_staging(sandbox_id)
                    .await
            {
                Self::mark_async_cleanup_failed(
                    &self.db,
                    sandbox_id,
                    keep_filesystem,
                    completion,
                    error.message(),
                );
                return Err(error);
            }

            if completion == CleanupCompletion::DeletePoolClone {
                admit_prepared_clone_disposal_attempt(&self.db, sandbox_id, job_id)?;
            }
            let dispatch_target = prepare_dispatch().await?;

            let dispatch = Self::dispatch_cleanup_if_generation_current(
                &self.db,
                sandbox_id,
                expected_execution_generation,
                fence,
                || node_dispatch(dispatch_target),
            )
            .await;
            match dispatch {
                Ok(CleanupDispatchDisposition::Dispatched) => {}
                Ok(CleanupDispatchDisposition::Superseded) => {
                    return Ok(CleanupOutcome::Superseded);
                }
                Err(error) => {
                    if Self::should_mark_async_cleanup_failed(&error) {
                        if let Some(cleanup_id) = cleanup_id {
                            let db = self
                                .db
                                .lock()
                                .map_err(|_| Status::internal("cleanup database poisoned"))?;
                            crate::db::record_cleanup_episode_failure(
                                &db,
                                sandbox_id,
                                cleanup_id,
                                error.message(),
                            )
                            .map_err(|error| {
                                Status::internal(format!("record cleanup failure: {error}"))
                            })?;
                        } else {
                            Self::mark_async_cleanup_failed(
                                &self.db,
                                sandbox_id,
                                keep_filesystem,
                                completion,
                                error.message(),
                            );
                        }
                    }
                    return Err(error);
                }
            }

            if let Some(cleanup_id) = cleanup_id {
                match self
                    .finalize_successful_cleanup(sandbox_id, cleanup_id, job_id)
                    .await
                {
                    CleanupFinalizationOutcome::Applied => {}
                    CleanupFinalizationOutcome::Superseded => {
                        return Ok(CleanupOutcome::Superseded);
                    }
                    CleanupFinalizationOutcome::UnsafeEvidenceQuarantined(error) => {
                        tracing::warn!(sandbox_id, %error, "cleanup evidence quarantined");
                        return Ok(CleanupOutcome::Quarantined(error));
                    }
                    CleanupFinalizationOutcome::Retry(error) => {
                        return Err(Status::internal(error));
                    }
                }
            } else {
                let id = sandbox_id.to_owned();
                let job_id = job_id.copied();
                let disposition = self
                    .blocking_io(move |service| {
                        service
                            .apply_route_identity_mutation(
                                parsed_sandbox_id,
                                "async_cleanup_completion",
                                || {
                                    let db = service.db.lock().map_err(|_| {
                                        CleanupCompletionError::DatabaseMutexPoisoned
                                    })?;
                                    Self::complete_fenced_row_cleanup(
                                        &db,
                                        RowCleanupCompletion {
                                            sandbox_id: &id,
                                            completion,
                                            expected_execution_generation,
                                            auto_lifecycle_delete_receipt,
                                            published_artifact,
                                            job_id: job_id.as_ref(),
                                        },
                                        || {},
                                    )
                                    .map(|disposition| {
                                        if matches!(disposition, CleanupCompletionDisposition::Applied)
                                            && completion.deletes_row()
                                            && let Err(error) = service.retire_absent_sandbox_policy(&db, &id)
                                        {
                                            tracing::warn!(sandbox_id = %id, %error, "retaining policy evidence after row cleanup");
                                        }
                                        let identity_changed = matches!(
                                            disposition,
                                            CleanupCompletionDisposition::Applied
                                                | CleanupCompletionDisposition::Quarantined
                                        );
                                        (disposition, identity_changed)
                                    })
                                },
                            )
                            .map_err(|error| {
                                Status::internal(format!("complete cleanup failed: {error}"))
                            })
                    })
                    .await?;
                match disposition {
                    CleanupCompletionDisposition::Applied => {}
                    CleanupCompletionDisposition::Superseded => {
                        return Ok(CleanupOutcome::Superseded);
                    }
                    CleanupCompletionDisposition::Quarantined => {
                        return Ok(CleanupOutcome::Quarantined(
                            crate::db::SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE.to_owned(),
                        ));
                    }
                }
            }
            Ok(CleanupOutcome::Applied)
        }
        .await;

        if let Some(CleanupAdmission::Persisted {
            dispatch_job_id: Some(job_id),
            ..
        }) = admission.as_ref()
        {
            self.record_cleanup_attempt(job_id, &result).await?;
        }
        if result.is_ok() {
            mark_sandbox_dirty(&self.db, sandbox_id);
        }
        result
    }

    /// Preserve stop/delete as runtime escape hatches when SQLite cannot
    /// persist their lifecycle handoff. The node mutation is still serialized
    /// with normal cleanup, but deliberately performs no state completion: the
    /// caller returns the persistence error so a later retry can converge the
    /// durable row after storage recovers.
    pub(super) async fn run_cleanup_after_persistence_failure(
        &self,
        request: SynchronousCleanupRequest<'_>,
        admission: CleanupAdmission,
    ) -> Result<Option<String>, Status> {
        let sandbox_id = SandboxId::from_str(request.sandbox_id)
            .map_err(|error| Status::invalid_argument(format!("invalid sandbox id: {error}")))?;
        let dispatch_sandbox_id = request.sandbox_id;
        let dispatch_keep_filesystem = request.keep_filesystem;
        let dispatch_completion = request.completion;
        let dispatch_immediate_kill = request.immediate_kill;
        let dispatch_guest_flush = request.guest_flush;
        let dispatch_context = request.context;
        self.run_cleanup_after_persistence_failure_with_node_dispatch(
            request,
            &admission,
            || async move {
                self.flush_guest_before_termination(dispatch_sandbox_id, dispatch_guest_flush)
                    .await;
                let mut node_client = connect_node(&self.node_manager).await.map_err(|error| {
                    Status::internal(format!(
                        "{dispatch_context}: cannot connect to node-manager: {error}"
                    ))
                })?;
                service_cleanup_via_node_with_options(
                    &self.db,
                    &mut node_client,
                    dispatch_sandbox_id,
                    NodeCleanupOptions {
                        job_id: None,
                        keep_filesystem: dispatch_keep_filesystem,
                        return_after_slot_release: Self::cleanup_return_after_slot_release(
                            dispatch_keep_filesystem,
                            dispatch_completion,
                        ),
                        immediate_kill: dispatch_immediate_kill,
                    },
                    dispatch_context,
                )
                .await
            },
        )
        .await?;
        admission.into_unpersisted_error(sandbox_id)
    }

    pub(super) async fn run_cleanup_after_persistence_failure_with_node_dispatch<F, Fut>(
        &self,
        request: SynchronousCleanupRequest<'_>,
        admission: &CleanupAdmission,
        node_dispatch: F,
    ) -> Result<(), Status>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<(), Status>>,
    {
        let SynchronousCleanupRequest { sandbox_id, .. } = request;
        let parsed_sandbox_id = SandboxId::from_str(sandbox_id)
            .map_err(|error| Status::invalid_argument(format!("invalid sandbox id: {error}")))?;
        if !admission.is_unpersisted_for_sandbox(parsed_sandbox_id)? {
            return Err(Status::internal(
                "persisted cleanup admission cannot authorize persistence-failure cleanup",
            ));
        }
        let fence = admission.fence_for_sandbox(parsed_sandbox_id)?;
        if !fence.authorizes(sandbox_id) {
            return Err(Status::internal(
                "cleanup fence does not match persistence-failure cleanup",
            ));
        }
        // Recheck immediately before the persistence-failure escape hatch. A
        // failed/unknown ownership read must never become permission to delete.
        let row_missing = {
            let db = self
                .db
                .lock()
                .map_err(|_| Status::unavailable("cleanup ownership is unavailable"))?;

            crate::db::read::sandbox_view(&db, sandbox_id)
                .cleanup_episode()
                .strict()
                .map_err(|_| {
                    Status::unavailable("cleanup ownership cannot be decoded; retain resources")
                })?;
            request.completion == CleanupCompletion::DeleteRow
                && crate::db::read::sandbox_view(&db, sandbox_id)
                    .current_state_label()
                    .map_err(|error| {
                        Status::unavailable(format!(
                            "inspect sandbox after unpersisted delete: {error}"
                        ))
                    })?
                    .is_none()
        };
        self.cancel_route_streams_for_generation(
            parsed_sandbox_id,
            admission.expected_execution_generation(parsed_sandbox_id)?,
        )?;
        if row_missing {
            // A late legacy Delete can arrive after durable deletion removed
            // the row. Durable DeleteRow completes only after node-manager
            // verified the canonical cache directory absent, so a missing row
            // with no cache directory is completed deletion: not authority to
            // send an unfenced node cleanup or acknowledge it. A rowless
            // sandbox whose cache directory remains was selected from disk and
            // is not deleted; keep it retryable rather than reporting success.
            let cache_dir = self.sandbox_cache_root().join(sandbox_id);
            return match std::fs::symlink_metadata(&cache_dir) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Ok(_) => Err(Status::unavailable(format!(
                    "sandbox {sandbox_id} has no row but {} remains; retain resources",
                    cache_dir.display()
                ))),
                Err(error) => Err(Status::unavailable(format!(
                    "inspect rowless sandbox cache {}: {error}; retain resources",
                    cache_dir.display()
                ))),
            };
        }
        node_dispatch().await
    }

    pub(super) fn schedule_async_cleanup(&self, request: AsyncCleanupRequest) {
        self.schedule_async_cleanup_using_admission(request, None);
    }

    pub(super) async fn schedule_admitted_async_cleanup(
        &self,
        request: AsyncCleanupRequest,
        admission: CleanupAdmission,
    ) {
        let permit = match Arc::clone(&self.cleanup_io).acquire_owned().await {
            Ok(permit) => permit,
            Err(error) => {
                tracing::warn!(%error, "cleanup submission deferred to recovery");
                return;
            }
        };
        let service = self.clone();
        if let Err(error) = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            service.schedule_async_cleanup_using_admission(request, Some(admission));
        })
        .await
        {
            tracing::warn!(%error, "cleanup submission task failed");
        }
    }

    fn schedule_async_cleanup_using_admission(
        &self,
        request: AsyncCleanupRequest,
        admission: Option<CleanupAdmission>,
    ) {
        if let Err(error) = self.persist_cleanup_request(request, admission) {
            tracing::warn!(%error, "cleanup dispatch deferred to durable recovery");
        }
        self.ensure_cleanup_executor();
    }

    pub(super) fn schedule_activation_cleanup(
        &self,
        request: AsyncCleanupRequest,
        cleanup_id: SandboxCleanupEpisodeId,
        reason_code: SandboxQuarantineReasonCode,
        reason: String,
        permit: Option<ActivationAdmissionPermit>,
    ) {
        let persist = || {
            let db = self
                .db
                .lock()
                .map_err(|_| CleanupPersistenceError::DatabaseMutexPoisoned)?;
            crate::db::request_cleanup_episode(
                &db,
                &request.sandbox_id.to_string(),
                cleanup_id,
                reason_code,
                &reason,
            )
            .map_err(CleanupPersistenceError::Database)
        };

        match persist() {
            Ok(crate::db::CleanupEpisodeRequestOutcome::Requested) => {
                pplx_space_metrics::sandbox::worker::activation_cleanup(
                    pplx_space_metrics::sandbox::worker::ActivationCleanupOutcome::Requested,
                );
                self.schedule_async_cleanup(request);
            }
            Ok(crate::db::CleanupEpisodeRequestOutcome::AlreadyRequested) => {
                tracing::debug!(
                    sandbox_id = %request.sandbox_id,
                    %cleanup_id,
                    "activation cleanup episode was already requested"
                );
            }
            Ok(crate::db::CleanupEpisodeRequestOutcome::Superseded) => {
                pplx_space_metrics::sandbox::worker::activation_cleanup(
                    pplx_space_metrics::sandbox::worker::ActivationCleanupOutcome::Superseded,
                );
                tracing::warn!(
                    sandbox_id = %request.sandbox_id,
                    %cleanup_id,
                    "activation cleanup episode was no longer current"
                );
            }
            Err(error) => {
                let service = self.clone();
                let shutdown_token = self.shutdown_token.clone();
                self.task_tracker.spawn(async move {
                    let _permit = permit;
                    loop {
                        tokio::select! {
                            _ = shutdown_token.cancelled() => return,
                            _ = tokio::time::sleep(CLEANUP_HANDOFF_RETRY) => {}
                        }
                        let persisted = service
                            .db
                            .lock()
                            .map_err(|_| CleanupPersistenceError::DatabaseMutexPoisoned)
                            .and_then(|db| {
                                crate::db::request_cleanup_episode(
                                    &db,
                                    &request.sandbox_id.to_string(),
                                    cleanup_id,
                                    reason_code,
                                    &reason,
                                )
                                .map_err(CleanupPersistenceError::Database)
                            });
                        match persisted {
                            Ok(crate::db::CleanupEpisodeRequestOutcome::Requested) => {
                                pplx_space_metrics::sandbox::worker::activation_cleanup(
                                    pplx_space_metrics::sandbox::worker::ActivationCleanupOutcome::Requested,
                                );
                                service.schedule_async_cleanup(request);
                                return;
                            }
                            Ok(
                                crate::db::CleanupEpisodeRequestOutcome::AlreadyRequested
                                | crate::db::CleanupEpisodeRequestOutcome::Superseded,
                            ) => return,
                            Err(retry_error) => tracing::error!(
                                sandbox_id = %request.sandbox_id,
                                %cleanup_id,
                                %retry_error,
                                initial_error = %error,
                                "failed to persist activation cleanup episode; retrying"
                            ),
                        }
                    }
                });
            }
        }
    }
}

impl SandboxManagerService {
    /// Attempt the guest flush an ordinary stop asked for, immediately before
    /// terminating its VM. A failed flush is recorded, and the stop proceeds.
    async fn flush_guest_before_termination(
        &self,
        sandbox_id: &str,
        guest_flush: crate::db::cleanup_jobs::GuestFlush,
    ) {
        if guest_flush != crate::db::cleanup_jobs::GuestFlush::BeforeTermination {
            return;
        }
        if self
            .lookup_sandbox(sandbox_id)
            .is_ok_and(|sandbox| !sandbox.guest_image.expects_agent())
        {
            return;
        }
        let flushed = within_stop_flush_budget(
            crate::guest_filesystem_flush::GUEST_FILESYSTEM_FLUSH_TIMEOUT,
            self.resume_paused_vm_before_flush(sandbox_id),
            || crate::guest_filesystem_flush::flush_guest_root_filesystem_before_stop(sandbox_id),
        )
        .await;
        match flushed {
            Ok(()) => pplx_space_metrics::sandbox::worker::stop_guest_flush("flushed"),
            Err(error) => {
                pplx_space_metrics::sandbox::worker::stop_guest_flush("failed");
                tracing::warn!(
                    %sandbox_id,
                    %error,
                    "guest filesystem flush before stop failed; stopping anyway, so writes the guest has not committed can be lost"
                );
                emit_sandbox_event(
                    sandbox_id,
                    SandboxEventType::StopGuestFlushFailed,
                    SandboxEventSource::SbxManager,
                    SandboxEventDetails::default()
                        .error(error.to_string().as_str())
                        .logged_by_caller(),
                );
            }
        }
    }

    /// Resume a paused VM so the stop's flush can reach its guest; see
    /// [`BeforeStopFlush`].
    async fn resume_paused_vm_before_flush(&self, sandbox_id: &str) -> Result<(), StopFlushError> {
        let id = SandboxId::from_str(sandbox_id).map_err(StopFlushError::SandboxId)?;
        let state = crate::runtime_via_nodemgr::get_vm_state(id, &self.node_manager).await;
        if let Err(error) = &state {
            tracing::debug!(%sandbox_id, %error, "VM state unreadable before the stop flush");
        }
        match BeforeStopFlush::for_vm_state(state.ok()) {
            BeforeStopFlush::Flush => Ok(()),
            BeforeStopFlush::NotRunning => Err(StopFlushError::VmNotRunning),
            BeforeStopFlush::ResumeThenFlush => {
                let execution_fence = {
                    let db = self.db_conn().map_err(StopFlushError::Database)?;
                    crate::db::read::sandbox_view(&db, sandbox_id)
                        .node_execution_fence()
                        .map_err(StopFlushError::Fence)?
                        .map(Into::into)
                };
                let mut node_client = connect_node(&self.node_manager)
                    .await
                    .map_err(StopFlushError::NodeManager)?;
                node_client
                    .resume_sandbox(ResumeSandboxRequest {
                        execution_fence,
                        sandbox_id: sandbox_id.to_owned(),
                    })
                    .await
                    .map(drop)
                    .map_err(StopFlushError::Resume)
            }
        }
    }
}

/// Why a stop's guest flush did not complete. The stop proceeds either way.
#[derive(Debug, thiserror::Error)]
pub(super) enum StopFlushError {
    #[error("invalid sandbox id: {0}")]
    SandboxId(#[source] pplx_space_common_core::types::SandboxIdParseError),
    #[error("the VM is not running")]
    VmNotRunning,
    #[error("cannot read the execution fence: {0}")]
    Database(#[source] Status),
    #[error("cannot read the execution fence: {0}")]
    Fence(#[source] crate::db::Error),
    #[error("cannot connect to node-manager: {0}")]
    NodeManager(#[source] NodeManagerConnectError),
    #[error("cannot resume the paused VM to flush it: {0}")]
    Resume(#[source] Status),
    #[error(transparent)]
    Flush(#[from] crate::guest_filesystem_flush::GuestFlushError),
    #[error("guest flush before stop timed out after {0:?}")]
    TimedOut(std::time::Duration),
}

/// Prepare the guest and flush it within one `budget`. Every step can stall
/// (a resume that never answers, a guest that never acknowledges), and the
/// stop holds its cleanup fence meanwhile, which a forced stop also waits on;
/// so the whole attempt, not each step, is bounded.
pub(super) async fn within_stop_flush_budget<Flush, FlushError>(
    budget: std::time::Duration,
    prepare: impl std::future::Future<Output = Result<(), StopFlushError>>,
    flush: impl FnOnce() -> Flush,
) -> Result<(), StopFlushError>
where
    Flush: std::future::Future<Output = Result<(), FlushError>>,
    FlushError: Into<StopFlushError>,
{
    tokio::time::timeout(budget, async {
        prepare.await?;
        flush().await.map_err(Into::into)
    })
    .await
    .unwrap_or(Err(StopFlushError::TimedOut(budget)))
}

/// What a stop's flush needs first, from the VM's state. A paused VM cannot
/// answer a flush, so it is resumed: pause flushes the guest before it
/// freezes the VM, but a write still in progress then can complete after that
/// flush, and stopping the paused VM would discard it. A running VM, or one
/// whose state cannot be read, goes straight to the flush, which has its own
/// budget.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum BeforeStopFlush {
    Flush,
    ResumeThenFlush,
    NotRunning,
}

impl BeforeStopFlush {
    pub(super) fn for_vm_state(
        state: Option<pplx_space_node_runtime_core::vm_state::VmState>,
    ) -> Self {
        use pplx_space_node_runtime_core::vm_state::VmState;
        match state {
            Some(VmState::Running) | None => Self::Flush,
            Some(VmState::Paused) => Self::ResumeThenFlush,
            Some(VmState::NotStarted | VmState::Shutdown) => Self::NotRunning,
        }
    }
}

#[cfg(test)]
mod stop_admission_tests;
