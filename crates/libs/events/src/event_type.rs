use std::fmt;

use crate::phase::SandboxEventPhase;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SandboxEventType {
    CreateRequested,
    CreateCompleted,
    CreateFailed,
    /// Create accepted and the sandbox pod is scheduled and running on a node,
    /// but the API-side readiness wait exhausted its budget before the runtime
    /// was command-ready. The in-flight node work is preserved (pod left
    /// running) and the sandbox id is returned so a client retry converges on
    /// the same sandbox. The row is set to `started` (non-sweepable, reads as
    /// Running) — NOT `creating` — while the runtime finishes converging.
    CreatePending,
    StartRequested,
    StartCompleted,
    StartFailed,
    WakeRequested,
    WakeCompleted,
    WakeFailed,
    ResumeRequested,
    ResumeCompleted,
    ResumeFailed,
    RestoreRequested,
    RestoreCompleted,
    RestoreFailed,
    ForkRequested,
    ForkArtifactReady,
    ForkTargetActivated,
    ForkCompleted,
    ForkFailed,
    SuspendRequested,
    SuspendCompleted,
    SuspendFailed,
    /// An exact-execution suspend request was conclusively skipped because a
    /// newer execution generation already owns the sandbox. No local runtime
    /// transition is performed for the stale execution.
    SuspendSuperseded,
    StopRequested,
    StopCompleted,
    StopFailed,
    /// An ordinary stop could not flush the guest's root filesystem before
    /// terminating the VM, and stopped it anyway: writes the guest had not
    /// committed can be lost.
    StopGuestFlushFailed,
    KillRequested,
    KillCompleted,
    KillFailed,
    DeleteRequested,
    DeleteCompleted,
    DeleteFailed,
    DeleteSuperseded,
    PauseRequested,
    PauseCompleted,
    PauseFailed,
    CheckpointRequested,
    CheckpointCompleted,
    CheckpointFailed,
    SnapshotPolicyUpdateRequested,
    SnapshotPolicyUpdateCompleted,
    SnapshotPolicyUpdateFailed,
    LifecycleTransitionCasLost,
    LifecycleTransitionTimedOut,
    /// A reconciler gave up on a lifecycle transition and forced the row to
    /// `failed` because the sandbox's node-local state had already diverged to
    /// `failed` (the guest died) while the DB still read `started`/`paused`.
    /// Distinct from [`Self::LifecycleTransitionTimedOut`]: this is an
    /// immediate divergence give-up, not an age-out, so it carries no timeout
    /// semantics. Emitted by the node-drain failed-on-node give-up path.
    LifecycleTransitionFailedOnNode,
    /// The sandbox's lifecycle/state row transitioned into the terminal
    /// `failed` state. More specific events emitted next to this one explain
    /// the operation that caused the failure; this event is the single
    /// timeline marker for alerting and incident queries that only need to
    /// know when the sandbox became failed.
    SandboxFailed,
    /// The sandbox's lifecycle/state row transitioned into the inert
    /// `quarantined` state. The current quarantine control row records the
    /// resumable next action; this event is the timeline marker.
    SandboxQuarantined,
    /// A quarantined sandbox returned to a healthy lifecycle state after the
    /// node reported definitive runtime state and completed any local cleanup.
    QuarantineRecovered,
    /// A sweep verified that a row stuck in a transient lifecycle has a live
    /// VM on its node and converged the row to the running state the VM
    /// already reached (the wake target), instead of failing it. The Postgres
    /// row was stale, not the sandbox.
    LifecycleConverged,
    /// A sweep verified that a row stuck in a transient lifecycle has no live
    /// VM but does have a ready durable snapshot, and reverted the row to
    /// `suspended` so a later restore recovers it, instead of failing it.
    LifecycleRevertedToSuspended,
    /// A sweep rolled back a lifecycle transition it had just made, because
    /// the teardown that transition existed to enable was refused on pod
    /// identity. The row is back at the lifecycle it held before the sweep
    /// touched it, and whatever changed under the sweep owns the sandbox.
    LifecycleTransitionRolledBack,
    /// A sweep aged a row out but could not prove destroying it is safe (the
    /// VM is alive in a non-convergeable lifecycle, the node holds the only
    /// copy of the sandbox's state, or the probe was inconclusive), so it
    /// took no action and left the row for a later tick.
    SweepDeferred,
    ExecutionEnsured,
    ExecutionReplaced,
    /// A stale active execution record was retired without a lifecycle
    /// change: the sandbox's lifecycle had already converged to a dormant
    /// state (e.g. a node-side auto-suspend published `suspended`), but the
    /// execution row stayed `active` while its backing pod disappeared.
    /// Emitted by the cluster-controller suspended-execution reaper when it
    /// retires the row and clears the stale placement bookkeeping.
    ExecutionRetired,
    PoolSlotClaimed,
    PoolSlotMissed,
    CleanupRequested,
    CleanupStarted,
    CleanupCompleted,
    /// Node-local activation compensation was projected into the central
    /// lifecycle and exact execution claim.
    ActivationRollbackCompleted,
    CleanupFailed,
    GuestProcessStartFailed,
    SnapshotRestoreStarted,
    SnapshotRestoreCompleted,
    SnapshotRestoreFailed,
    NetworkPolicyRefreshStarted,
    NetworkPolicyRefreshCompleted,
    NetworkPolicyRefreshFailed,
    NetworkPolicyUpdateRequested,
    NetworkPolicyUpdateCompleted,
    NetworkPolicyUpdateFailed,
    NetworkPolicyEnforcementStarted,
    NetworkPolicyEnforcementCompleted,
    NetworkPolicyEnforcementFailed,
    /// An operator asked for the sandbox's net-gateway sidecar to be cycled in
    /// place (killed and relaunched from the node's current on-disk binary)
    /// while the VM keeps running. Used to retire gateways whose launch-time
    /// binary predates the node's current deploy.
    NetGatewayCycleRequested,
    /// The in-place net-gateway cycle finished: the old gateway process is
    /// gone, a replacement is running from the current binary, and a policy
    /// reconcile was enqueued to re-hydrate it.
    NetGatewayCycleCompleted,
    /// The in-place net-gateway cycle failed partway. The watchdog's dead-
    /// sidecar restart remains the recovery backstop if the old process was
    /// already killed.
    NetGatewayCycleFailed,
    PortExposureOpenRequested,
    PortExposureOpenCompleted,
    PortExposureOpenFailed,
    PortExposureCloseRequested,
    PortExposureCloseCompleted,
    PortExposureCloseFailed,
    PortExposureApplyStarted,
    PortExposureApplyCompleted,
    PortExposureApplyFailed,
    PortExposureRemoveStarted,
    PortExposureRemoveCompleted,
    PortExposureRemoveFailed,
    SnapshotArtifactPullStarted,
    SnapshotArtifactPullCompleted,
    SnapshotArtifactPullFailed,
    LocalArtifactPublished,
    LocalArtifactPublishFailed,
    ArtifactBackupQueued,
    ArtifactBackupCompleted,
    ArtifactBackupFailed,
    /// The node-drain reconciler gave up promoting (backing up) a suspended
    /// sandbox's local snapshot from a draining node — the on-node reupload was
    /// permanently refused because the sandbox was wedged in `restoring` — and
    /// instead adopted the current suspend cycle's ready remote snapshot as the
    /// canonical recovery source so the node could finish draining. The sandbox
    /// stays `suspended` and recoverable from the adopted snapshot; the
    /// un-uploadable local state the stuck restore left behind is abandoned.
    /// Records the immutable drain-progress decision, not a runtime transition.
    LocalSnapshotPromotionAbandoned,
    /// A reconciler determined that no recoverable copy of a suspended
    /// sandbox's state exists anywhere — its local suspended artifact is gone
    /// (pruned from the node's disk, or the node's backing instance is
    /// confirmed terminated) and no ready remote snapshot exists — and flipped
    /// the row to `failed`. Terminal: the user's next wake/restore gets a
    /// clean lifecycle error instead of a confusing snapshot-not-found.
    SandboxLost,
    /// A node's cold-sandbox eviction loop removed a suspended sandbox's
    /// entire local presence (cache directory including the rootfs
    /// instance, plus node-local DB rows) because the sandbox's state is
    /// durably restorable from a ready remote snapshot. The sandbox stays
    /// `suspended` at cluster level; a later resume restores from the
    /// remote snapshot. Carries the remote snapshot id it relied on.
    LocalEvictionCompleted,
    /// The cold-sandbox eviction loop claimed a sandbox for eviction but
    /// the local cleanup failed; the local row is left in `deleting` for
    /// the cleanup machinery to retry/converge.
    LocalEvictionFailed,
    /// The local rootfs diff stream (`rootfs.btrfs-send`) of a suspended
    /// sandbox's current artifact was reclaimed after its backup reached
    /// the durable ready state. Same-node wake is preserved: the snapshot's
    /// `memory`/`vmstate` files and the `instance/` rootfs subvolume stay;
    /// only the upload-format stream — which no restore path reads locally
    /// (cross-node restore pulls it from durable storage) — is removed.
    RootfsStreamReclaimed,
    /// A retention sweep pruned an old durable snapshot of a still-live
    /// sandbox: the snapshot fell outside the newest-N `ready` full
    /// snapshots the cluster keeps per sandbox, so its S3 prefix and DB
    /// rows were deleted. The sandbox stays alive and restorable from a
    /// newer snapshot; this records the immutable durable-artifact
    /// deletion, not a runtime lifecycle transition. Carries the pruned
    /// `snapshot_id` and its `age_secs`.
    SnapshotPruned,
    SnapshotArtifactUploadQueued,
    SnapshotArtifactUploadCompleted,
    SnapshotArtifactUploadFailed,
    MemoryAdmissionFailed,
    RuntimeActivated,
    GuestBridgeReady,
    GuestBridgeFailed,
    GuestReady,
    GuestReadyFailed,
    NetworkPathStopped,
    FilesystemRemoved,
    NetworkAllocationCompleted,
    NetworkAllocationFailed,
    NetworkReleaseCompleted,
    NetworkReleaseFailed,
    SandboxProcessLaunchCompleted,
    SandboxProcessLaunchFailed,
    SandboxProcessPidPersistFailed,
}

pub const SANDBOX_EVENT_TYPES: &[SandboxEventType] = &[
    SandboxEventType::CreateRequested,
    SandboxEventType::CreateCompleted,
    SandboxEventType::CreateFailed,
    SandboxEventType::CreatePending,
    SandboxEventType::StartRequested,
    SandboxEventType::StartCompleted,
    SandboxEventType::StartFailed,
    SandboxEventType::WakeRequested,
    SandboxEventType::WakeCompleted,
    SandboxEventType::WakeFailed,
    SandboxEventType::ResumeRequested,
    SandboxEventType::ResumeCompleted,
    SandboxEventType::ResumeFailed,
    SandboxEventType::RestoreRequested,
    SandboxEventType::RestoreCompleted,
    SandboxEventType::RestoreFailed,
    SandboxEventType::ForkRequested,
    SandboxEventType::ForkArtifactReady,
    SandboxEventType::ForkTargetActivated,
    SandboxEventType::ForkCompleted,
    SandboxEventType::ForkFailed,
    SandboxEventType::SuspendRequested,
    SandboxEventType::SuspendCompleted,
    SandboxEventType::SuspendFailed,
    SandboxEventType::SuspendSuperseded,
    SandboxEventType::StopRequested,
    SandboxEventType::StopCompleted,
    SandboxEventType::StopFailed,
    SandboxEventType::StopGuestFlushFailed,
    SandboxEventType::KillRequested,
    SandboxEventType::KillCompleted,
    SandboxEventType::KillFailed,
    SandboxEventType::DeleteRequested,
    SandboxEventType::DeleteCompleted,
    SandboxEventType::DeleteFailed,
    SandboxEventType::DeleteSuperseded,
    SandboxEventType::PauseRequested,
    SandboxEventType::PauseCompleted,
    SandboxEventType::PauseFailed,
    SandboxEventType::CheckpointRequested,
    SandboxEventType::CheckpointCompleted,
    SandboxEventType::CheckpointFailed,
    SandboxEventType::SnapshotPolicyUpdateRequested,
    SandboxEventType::SnapshotPolicyUpdateCompleted,
    SandboxEventType::SnapshotPolicyUpdateFailed,
    SandboxEventType::LifecycleTransitionCasLost,
    SandboxEventType::LifecycleTransitionTimedOut,
    SandboxEventType::LifecycleTransitionFailedOnNode,
    SandboxEventType::SandboxFailed,
    SandboxEventType::SandboxQuarantined,
    SandboxEventType::QuarantineRecovered,
    SandboxEventType::LifecycleConverged,
    SandboxEventType::LifecycleRevertedToSuspended,
    SandboxEventType::LifecycleTransitionRolledBack,
    SandboxEventType::SweepDeferred,
    SandboxEventType::ExecutionEnsured,
    SandboxEventType::ExecutionReplaced,
    SandboxEventType::ExecutionRetired,
    SandboxEventType::PoolSlotClaimed,
    SandboxEventType::PoolSlotMissed,
    SandboxEventType::CleanupRequested,
    SandboxEventType::CleanupStarted,
    SandboxEventType::CleanupCompleted,
    SandboxEventType::ActivationRollbackCompleted,
    SandboxEventType::CleanupFailed,
    SandboxEventType::GuestProcessStartFailed,
    SandboxEventType::SnapshotRestoreStarted,
    SandboxEventType::SnapshotRestoreCompleted,
    SandboxEventType::SnapshotRestoreFailed,
    SandboxEventType::NetworkPolicyRefreshStarted,
    SandboxEventType::NetworkPolicyRefreshCompleted,
    SandboxEventType::NetworkPolicyRefreshFailed,
    SandboxEventType::NetworkPolicyUpdateRequested,
    SandboxEventType::NetworkPolicyUpdateCompleted,
    SandboxEventType::NetworkPolicyUpdateFailed,
    SandboxEventType::NetworkPolicyEnforcementStarted,
    SandboxEventType::NetworkPolicyEnforcementCompleted,
    SandboxEventType::NetworkPolicyEnforcementFailed,
    SandboxEventType::NetGatewayCycleRequested,
    SandboxEventType::NetGatewayCycleCompleted,
    SandboxEventType::NetGatewayCycleFailed,
    SandboxEventType::PortExposureOpenRequested,
    SandboxEventType::PortExposureOpenCompleted,
    SandboxEventType::PortExposureOpenFailed,
    SandboxEventType::PortExposureCloseRequested,
    SandboxEventType::PortExposureCloseCompleted,
    SandboxEventType::PortExposureCloseFailed,
    SandboxEventType::PortExposureApplyStarted,
    SandboxEventType::PortExposureApplyCompleted,
    SandboxEventType::PortExposureApplyFailed,
    SandboxEventType::PortExposureRemoveStarted,
    SandboxEventType::PortExposureRemoveCompleted,
    SandboxEventType::PortExposureRemoveFailed,
    SandboxEventType::SnapshotArtifactPullStarted,
    SandboxEventType::SnapshotArtifactPullCompleted,
    SandboxEventType::SnapshotArtifactPullFailed,
    SandboxEventType::LocalArtifactPublished,
    SandboxEventType::LocalArtifactPublishFailed,
    SandboxEventType::ArtifactBackupQueued,
    SandboxEventType::ArtifactBackupCompleted,
    SandboxEventType::ArtifactBackupFailed,
    SandboxEventType::LocalSnapshotPromotionAbandoned,
    SandboxEventType::SandboxLost,
    SandboxEventType::LocalEvictionCompleted,
    SandboxEventType::LocalEvictionFailed,
    SandboxEventType::RootfsStreamReclaimed,
    SandboxEventType::SnapshotPruned,
    SandboxEventType::SnapshotArtifactUploadQueued,
    SandboxEventType::SnapshotArtifactUploadCompleted,
    SandboxEventType::SnapshotArtifactUploadFailed,
    SandboxEventType::MemoryAdmissionFailed,
    SandboxEventType::RuntimeActivated,
    SandboxEventType::GuestBridgeReady,
    SandboxEventType::GuestBridgeFailed,
    SandboxEventType::GuestReady,
    SandboxEventType::GuestReadyFailed,
    SandboxEventType::NetworkPathStopped,
    SandboxEventType::FilesystemRemoved,
    SandboxEventType::NetworkAllocationCompleted,
    SandboxEventType::NetworkAllocationFailed,
    SandboxEventType::NetworkReleaseCompleted,
    SandboxEventType::NetworkReleaseFailed,
    SandboxEventType::SandboxProcessLaunchCompleted,
    SandboxEventType::SandboxProcessLaunchFailed,
    SandboxEventType::SandboxProcessPidPersistFailed,
];

impl SandboxEventType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CreateRequested => "create_requested",
            Self::CreateCompleted => "create_completed",
            Self::CreateFailed => "create_failed",
            Self::CreatePending => "create_pending",
            Self::StartRequested => "start_requested",
            Self::StartCompleted => "start_completed",
            Self::StartFailed => "start_failed",
            Self::WakeRequested => "wake_requested",
            Self::WakeCompleted => "wake_completed",
            Self::WakeFailed => "wake_failed",
            Self::ResumeRequested => "resume_requested",
            Self::ResumeCompleted => "resume_completed",
            Self::ResumeFailed => "resume_failed",
            Self::RestoreRequested => "restore_requested",
            Self::RestoreCompleted => "restore_completed",
            Self::RestoreFailed => "restore_failed",
            Self::ForkRequested => "fork_requested",
            Self::ForkArtifactReady => "fork_artifact_ready",
            Self::ForkTargetActivated => "fork_target_activated",
            Self::ForkCompleted => "fork_completed",
            Self::ForkFailed => "fork_failed",
            Self::SuspendRequested => "suspend_requested",
            Self::SuspendCompleted => "suspend_completed",
            Self::SuspendFailed => "suspend_failed",
            Self::SuspendSuperseded => "suspend_superseded",
            Self::StopRequested => "stop_requested",
            Self::StopCompleted => "stop_completed",
            Self::StopFailed => "stop_failed",
            Self::StopGuestFlushFailed => "stop_guest_flush_failed",
            Self::KillRequested => "kill_requested",
            Self::KillCompleted => "kill_completed",
            Self::KillFailed => "kill_failed",
            Self::DeleteRequested => "delete_requested",
            Self::DeleteCompleted => "delete_completed",
            Self::DeleteFailed => "delete_failed",
            Self::DeleteSuperseded => "delete_superseded",
            Self::PauseRequested => "pause_requested",
            Self::PauseCompleted => "pause_completed",
            Self::PauseFailed => "pause_failed",
            Self::CheckpointRequested => "checkpoint_requested",
            Self::CheckpointCompleted => "checkpoint_completed",
            Self::CheckpointFailed => "checkpoint_failed",
            Self::SnapshotPolicyUpdateRequested => "snapshot_policy_update_requested",
            Self::SnapshotPolicyUpdateCompleted => "snapshot_policy_update_completed",
            Self::SnapshotPolicyUpdateFailed => "snapshot_policy_update_failed",
            Self::LifecycleTransitionCasLost => "lifecycle_transition_cas_lost",
            Self::LifecycleTransitionTimedOut => "lifecycle_transition_timed_out",
            Self::LifecycleTransitionFailedOnNode => "lifecycle_transition_failed_on_node",
            Self::SandboxFailed => "sandbox_failed",
            Self::SandboxQuarantined => "sandbox_quarantined",
            Self::QuarantineRecovered => "quarantine_recovered",
            Self::LifecycleConverged => "lifecycle_converged",
            Self::LifecycleRevertedToSuspended => "lifecycle_reverted_to_suspended",
            Self::LifecycleTransitionRolledBack => "lifecycle_transition_rolled_back",
            Self::SweepDeferred => "sweep_deferred",
            Self::ExecutionEnsured => "execution_ensured",
            Self::ExecutionReplaced => "execution_replaced",
            Self::ExecutionRetired => "execution_retired",
            Self::PoolSlotClaimed => "pool_slot_claimed",
            Self::PoolSlotMissed => "pool_slot_missed",
            Self::CleanupRequested => "cleanup_requested",
            Self::CleanupStarted => "cleanup_started",
            Self::CleanupCompleted => "cleanup_completed",
            Self::ActivationRollbackCompleted => "activation_rollback_completed",
            Self::CleanupFailed => "cleanup_failed",
            Self::GuestProcessStartFailed => "guest_process_start_failed",
            Self::SnapshotRestoreStarted => "snapshot_restore_started",
            Self::SnapshotRestoreCompleted => "snapshot_restore_completed",
            Self::SnapshotRestoreFailed => "snapshot_restore_failed",
            Self::NetworkPolicyRefreshStarted => "network_policy_refresh_started",
            Self::NetworkPolicyRefreshCompleted => "network_policy_refresh_completed",
            Self::NetworkPolicyRefreshFailed => "network_policy_refresh_failed",
            Self::NetworkPolicyUpdateRequested => "network_policy_update_requested",
            Self::NetworkPolicyUpdateCompleted => "network_policy_update_completed",
            Self::NetworkPolicyUpdateFailed => "network_policy_update_failed",
            Self::NetworkPolicyEnforcementStarted => "network_policy_enforcement_started",
            Self::NetworkPolicyEnforcementCompleted => "network_policy_enforcement_completed",
            Self::NetworkPolicyEnforcementFailed => "network_policy_enforcement_failed",
            Self::NetGatewayCycleRequested => "net_gateway_cycle_requested",
            Self::NetGatewayCycleCompleted => "net_gateway_cycle_completed",
            Self::NetGatewayCycleFailed => "net_gateway_cycle_failed",
            Self::PortExposureOpenRequested => "port_exposure_open_requested",
            Self::PortExposureOpenCompleted => "port_exposure_open_completed",
            Self::PortExposureOpenFailed => "port_exposure_open_failed",
            Self::PortExposureCloseRequested => "port_exposure_close_requested",
            Self::PortExposureCloseCompleted => "port_exposure_close_completed",
            Self::PortExposureCloseFailed => "port_exposure_close_failed",
            Self::PortExposureApplyStarted => "port_exposure_apply_started",
            Self::PortExposureApplyCompleted => "port_exposure_apply_completed",
            Self::PortExposureApplyFailed => "port_exposure_apply_failed",
            Self::PortExposureRemoveStarted => "port_exposure_remove_started",
            Self::PortExposureRemoveCompleted => "port_exposure_remove_completed",
            Self::PortExposureRemoveFailed => "port_exposure_remove_failed",
            Self::SnapshotArtifactPullStarted => "snapshot_artifact_pull_started",
            Self::SnapshotArtifactPullCompleted => "snapshot_artifact_pull_completed",
            Self::SnapshotArtifactPullFailed => "snapshot_artifact_pull_failed",
            Self::LocalArtifactPublished => "local_artifact_published",
            Self::LocalArtifactPublishFailed => "local_artifact_publish_failed",
            Self::ArtifactBackupQueued => "artifact_backup_queued",
            Self::ArtifactBackupCompleted => "artifact_backup_completed",
            Self::ArtifactBackupFailed => "artifact_backup_failed",
            Self::LocalSnapshotPromotionAbandoned => "local_snapshot_promotion_abandoned",
            Self::SandboxLost => "sandbox_lost",
            Self::LocalEvictionCompleted => "local_eviction_completed",
            Self::LocalEvictionFailed => "local_eviction_failed",
            Self::RootfsStreamReclaimed => "rootfs_stream_reclaimed",
            Self::SnapshotPruned => "snapshot_pruned",
            Self::SnapshotArtifactUploadQueued => "snapshot_artifact_upload_queued",
            Self::SnapshotArtifactUploadCompleted => "snapshot_artifact_upload_completed",
            Self::SnapshotArtifactUploadFailed => "snapshot_artifact_upload_failed",
            Self::MemoryAdmissionFailed => "memory_admission_failed",
            Self::RuntimeActivated => "runtime_activated",
            Self::GuestBridgeReady => "guest_bridge_ready",
            Self::GuestBridgeFailed => "guest_bridge_failed",
            Self::GuestReady => "guest_ready",
            Self::GuestReadyFailed => "guest_ready_failed",
            Self::NetworkPathStopped => "network_path_stopped",
            Self::FilesystemRemoved => "filesystem_removed",
            Self::NetworkAllocationCompleted => "network_allocation_completed",
            Self::NetworkAllocationFailed => "network_allocation_failed",
            Self::NetworkReleaseCompleted => "network_release_completed",
            Self::NetworkReleaseFailed => "network_release_failed",
            Self::SandboxProcessLaunchCompleted => "sandbox_process_launch_completed",
            Self::SandboxProcessLaunchFailed => "sandbox_process_launch_failed",
            Self::SandboxProcessPidPersistFailed => "sandbox_process_pid_persist_failed",
        }
    }

    pub const fn phase(self) -> SandboxEventPhase {
        match self {
            Self::CreateRequested
            | Self::StartRequested
            | Self::WakeRequested
            | Self::ResumeRequested
            | Self::RestoreRequested
            | Self::ForkRequested
            | Self::SuspendRequested
            | Self::StopRequested
            | Self::KillRequested
            | Self::DeleteRequested
            | Self::PauseRequested
            | Self::CheckpointRequested
            | Self::SnapshotPolicyUpdateRequested
            | Self::CleanupRequested
            | Self::NetworkPolicyUpdateRequested
            | Self::NetGatewayCycleRequested
            | Self::PortExposureOpenRequested
            | Self::PortExposureCloseRequested => SandboxEventPhase::Requested,
            Self::CreatePending
            | Self::CleanupStarted
            | Self::SnapshotRestoreStarted
            | Self::NetworkPolicyRefreshStarted
            | Self::NetworkPolicyEnforcementStarted
            | Self::PortExposureApplyStarted
            | Self::PortExposureRemoveStarted
            | Self::SnapshotArtifactPullStarted => SandboxEventPhase::Started,
            // `SweepDeferred` is queued-shaped: the sweep took no action and
            // the row stays eligible for a later tick.
            Self::ArtifactBackupQueued
            | Self::SnapshotArtifactUploadQueued
            | Self::SweepDeferred => SandboxEventPhase::Queued,
            Self::CreateCompleted
            | Self::ForkArtifactReady
            | Self::ForkTargetActivated
            | Self::ForkCompleted
            | Self::StartCompleted
            | Self::WakeCompleted
            | Self::ResumeCompleted
            | Self::RestoreCompleted
            | Self::SuspendCompleted
            | Self::SuspendSuperseded
            | Self::StopCompleted
            | Self::KillCompleted
            | Self::DeleteCompleted
            | Self::DeleteSuperseded
            | Self::PauseCompleted
            | Self::CheckpointCompleted
            | Self::SnapshotPolicyUpdateCompleted
            | Self::ExecutionEnsured
            | Self::ExecutionReplaced
            | Self::ExecutionRetired
            | Self::PoolSlotClaimed
            | Self::PoolSlotMissed
            | Self::CleanupCompleted
            | Self::ActivationRollbackCompleted
            | Self::SnapshotRestoreCompleted
            | Self::NetworkPolicyRefreshCompleted
            | Self::NetworkPolicyUpdateCompleted
            | Self::NetworkPolicyEnforcementCompleted
            | Self::NetGatewayCycleCompleted
            | Self::PortExposureOpenCompleted
            | Self::PortExposureCloseCompleted
            | Self::PortExposureApplyCompleted
            | Self::PortExposureRemoveCompleted
            | Self::SnapshotArtifactPullCompleted
            | Self::LocalArtifactPublished
            | Self::ArtifactBackupCompleted
            | Self::LocalSnapshotPromotionAbandoned
            | Self::LifecycleConverged
            | Self::QuarantineRecovered
            | Self::LifecycleRevertedToSuspended
            | Self::LifecycleTransitionRolledBack
            | Self::LocalEvictionCompleted
            | Self::RootfsStreamReclaimed
            | Self::SnapshotPruned
            | Self::SnapshotArtifactUploadCompleted
            | Self::RuntimeActivated
            | Self::GuestBridgeReady
            | Self::GuestReady
            | Self::NetworkPathStopped
            | Self::FilesystemRemoved
            | Self::NetworkAllocationCompleted
            | Self::NetworkReleaseCompleted
            | Self::SandboxProcessLaunchCompleted => SandboxEventPhase::Completed,
            Self::CreateFailed
            | Self::ForkFailed
            | Self::StartFailed
            | Self::WakeFailed
            | Self::ResumeFailed
            | Self::RestoreFailed
            | Self::SuspendFailed
            | Self::StopFailed
            | Self::StopGuestFlushFailed
            | Self::KillFailed
            | Self::DeleteFailed
            | Self::PauseFailed
            | Self::CheckpointFailed
            | Self::SnapshotPolicyUpdateFailed
            | Self::LifecycleTransitionCasLost
            | Self::LifecycleTransitionTimedOut
            | Self::LifecycleTransitionFailedOnNode
            | Self::SandboxFailed
            | Self::SandboxQuarantined
            | Self::SandboxLost
            | Self::CleanupFailed
            | Self::GuestProcessStartFailed
            | Self::SnapshotRestoreFailed
            | Self::NetworkPolicyRefreshFailed
            | Self::NetworkPolicyUpdateFailed
            | Self::NetworkPolicyEnforcementFailed
            | Self::NetGatewayCycleFailed
            | Self::PortExposureOpenFailed
            | Self::PortExposureCloseFailed
            | Self::PortExposureApplyFailed
            | Self::PortExposureRemoveFailed
            | Self::SnapshotArtifactPullFailed
            | Self::LocalArtifactPublishFailed
            | Self::ArtifactBackupFailed
            | Self::LocalEvictionFailed
            | Self::SnapshotArtifactUploadFailed
            | Self::MemoryAdmissionFailed
            | Self::GuestBridgeFailed
            | Self::GuestReadyFailed
            | Self::NetworkAllocationFailed
            | Self::NetworkReleaseFailed
            | Self::SandboxProcessLaunchFailed
            | Self::SandboxProcessPidPersistFailed => SandboxEventPhase::Failed,
        }
    }
}

impl fmt::Display for SandboxEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
