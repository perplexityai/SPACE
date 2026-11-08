use std::fmt;

/// A sandbox row's lifecycle, stored in `sandboxes.state`. A cleanup that owns
/// the row is not a lifecycle state: the row records it apart, as its
/// [`CleanupState`], over the teardown transition its intent names, and
/// [`SandboxStatus`] carries the two together.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SandboxState {
    Creating,
    Created,
    Starting,
    Started,
    Stopping,
    Stopped,
    Deleting,
    Pausing,
    Paused,
    Resuming,
    Checkpointing,
    Suspending,
    Suspended,
    Restoring,
    Quarantined,
    Failed,
}

/// A cleanup that owns a sandbox row, stored in `sandboxes.cleanup_state`
/// over the row's lifecycle. It is not a lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CleanupState {
    Required,
    Failed,
}

impl CleanupState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Required => "required",
            Self::Failed => "failed",
        }
    }

    /// The cleanup a `sandboxes.cleanup_state` value names.
    pub(crate) fn from_column(value: &str) -> Option<Self> {
        match value {
            "required" => Some(Self::Required),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

impl SandboxState {
    /// Wait completion is not evidence that a VM or its resources were removed.
    /// Quarantine completes the wait while preserving uncertain resources.
    pub const fn completes_wait(self) -> bool {
        matches!(
            self,
            Self::Stopped | Self::Failed | Self::Suspended | Self::Quarantined
        )
    }

    /// The RPC-admission machine, over lifecycle states. A row a cleanup owns
    /// admits only Delete and quarantine ([`SandboxStatus::can_transition_to`]),
    /// and a cleanup moving between required and failed is a cleanup edge
    /// (`db::edges::CLEANUP_EDGES`), not a lifecycle transition.
    pub(crate) fn can_transition_to(&self, target: SandboxState) -> bool {
        if target == SandboxState::Quarantined {
            return *self != SandboxState::Failed;
        }

        use SandboxState::*;
        matches!(
            (self, target),
            // Create/Delete lifecycle
            (Creating, Created)
            | (Created, Starting)
            | (Created, Deleting)
            // Start/Stop lifecycle
            | (Starting, Started)
            | (Started, Stopping)
            | (Stopping, Stopped)
            | (Stopped, Starting)
            | (Stopped, Deleting)
            // Pause/Resume lifecycle
            | (Started, Pausing)
            | (Pausing, Paused)
            | (Paused, Resuming)
            | (Resuming, Started)
            | (Paused, Stopping)      // stop/kill from paused
            // Checkpoint lifecycle (VM keeps running, or from paused for migration)
            | (Started, Checkpointing)
            | (Paused, Checkpointing)
            | (Checkpointing, Started)
            | (Checkpointing, Paused)
            // Suspend lifecycle (snapshot + stop)
            | (Started, Suspending)    // auto-pause + snapshot + kill
            | (Paused, Suspending)     // snapshot + kill
            | (Suspending, Suspended)
            // Restore via Start (suspended -> starting -> started)
            | (Suspended, Starting)    // Start(checkpoint=...) or fresh start
            | (Suspended, Restoring)   // internal: Start routes through Restoring
            | (Restoring, Started)
            | (Restoring, Paused)      // restore-to-paused (drain lifecycle-preserving evacuation)
            | (Suspended, Deleting)    // delete snapshot + rootfs
            // Delete from non-transient states not yet covered above
            | (Started, Deleting)
            | (Paused, Deleting)
            // Delete from transient states (force-delete while operation in progress)
            | (Starting, Deleting)
            | (Stopping, Deleting)
            | (Resuming, Deleting)
            | (Pausing, Deleting)
            | (Suspending, Deleting)
            | (Restoring, Deleting)
            | (Checkpointing, Deleting)
            // Recovery
            | (Failed, Deleting)
            | (Quarantined, Deleting)
        )
    }

    pub fn transition(self, target: SandboxState) -> Result<SandboxState, StateTransitionError> {
        if self.can_transition_to(target) {
            Ok(target)
        } else if self.is_transient() {
            Err(StateTransitionError::Transient {
                action: target.action_name(),
                state: self.into(),
                operation: self.operation_name(),
            })
        } else {
            Err(StateTransitionError::Invalid {
                action: target.action_name(),
                state: self.into(),
                required: target.required_states(),
            })
        }
    }

    /// Whether a host-control caller matching this row's registered
    /// `(uid, netgtw_pid)` may be authorized as the sandbox's net-gateway.
    ///
    /// True only for the launch-through-teardown window in which the gateway
    /// sidecar is legitimately a live, policy-pulling process (launches happen
    /// in Starting/Restoring; the gateway stays resident across pause/resume
    /// and is killed during stop/suspend teardown). Outside that window the
    /// registration is stale or condemned — failure-marked rows keep the
    /// `(uid, netgtw_pid)` of processes cleanup already killed, and because
    /// uids are slot-derived and pids recycle, a stale registration can
    /// collide with a later sandbox's gateway and serve it the wrong policy.
    /// Deleting/cleanup states are excluded even though the process may
    /// briefly outlive the transition: a condemned gateway must not be served
    /// policy.
    pub fn can_authorize_net_gateway(&self) -> bool {
        use SandboxState::*;
        match self {
            Starting | Started | Stopping | Pausing | Paused | Resuming | Checkpointing
            | Suspending | Restoring => true,
            Creating | Created | Stopped | Suspended | Deleting | Quarantined | Failed => false,
        }
    }

    /// Whether the VM runtime may be resident outside any suspend, so a
    /// suspend backup that becomes durable now doesn't make the sandbox
    /// suspended. A suspend rolled back to `Paused` lands here, as does a
    /// sandbox started or restored since the capture. So does `Quarantined`:
    /// quarantine preserves resources it can't account for, a running VM
    /// included, and nothing settles them until an operator or a recovery
    /// does. `Suspending` and `Suspended` answer `false`, because whether a
    /// durable capture is theirs depends on the artifact they pin, not on the
    /// state. The remaining states never ran a VM, released it, or hand it to
    /// a cleanup of their own.
    pub fn holds_runtime_outside_suspend(self) -> bool {
        use SandboxState::*;
        match self {
            Starting | Started | Stopping | Pausing | Paused | Resuming | Checkpointing
            | Restoring | Quarantined => true,
            Creating | Created | Stopped | Deleting | Suspending | Suspended | Failed => false,
        }
    }

    /// Both `Started` and `Paused` sandboxes may have associated network gateways.
    pub const fn can_cycle_net_gateway(self) -> bool {
        matches!(self, Self::Started | Self::Paused)
    }

    /// Whether this sandbox still holds an on-disk `instance` subvolume whose
    /// btrfs parent lineage points at its template build — the reference that
    /// the incremental suspend codec (`btrfs send -p <template>`) and the
    /// incremental restore receive both depend on. While this is true, the
    /// parent build must not be garbage-collected or manually deleted: doing
    /// so dangles `instance.parent_uuid`, forcing suspend onto the whole-rootfs
    /// Full codec and breaking incremental restore.
    ///
    /// The `instance` subvolume is created at launch and removed only by a
    /// full teardown (`keep_filesystem = false`), which runs on the way to
    /// `Deleting`. `Stopped` and `Suspended` are explicitly kept
    /// (`MarkStopped` / `MarkSuspended` run with `keep_filesystem = true`), so
    /// they still reference the parent. Every non-`Deleting` state is therefore
    /// treated as holding the reference — fail-closed: an ambiguous or stale
    /// row over-protects (bounded, cleared when the row is finally deleted)
    /// rather than under-protects (the incremental-suspend degradation this
    /// guards against — a dangled parent forces the whole-rootfs Full codec).
    /// Only `Deleting` — where the instance subvolume is actively being torn
    /// down — is excluded.
    pub fn holds_parent_referencing_instance_subvolume(self) -> bool {
        use SandboxState::*;
        match self {
            Creating | Created | Starting | Started | Stopping | Stopped | Pausing | Paused
            | Resuming | Checkpointing | Suspending | Suspended | Restoring | Quarantined
            | Failed => true,
            Deleting => false,
        }
    }

    pub fn is_transient(&self) -> bool {
        use SandboxState::*;
        matches!(
            self,
            Creating
                | Starting
                | Stopping
                | Deleting
                | Pausing
                | Resuming
                | Checkpointing
                | Suspending
                | Restoring
        )
    }

    /// Whether the guest VM may be live (running or thawing) and spaced-host
    /// reachable, so in-guest bridge RPCs — loopback-bridge open/close — can
    /// land.
    ///
    /// This deliberately includes the wake-in-flight states (`Starting`,
    /// `Resuming`, `Restoring`): the guest answers bridge RPCs as soon as the
    /// VM resumes, long before the wake's readiness contract and
    /// network-policy gates finish. Gating these RPCs on `Started` holds the
    /// in-guest dynamic-port listener hostage to full wake completion — every
    /// exposure re-apply fails with a state-gate error while a slow wake
    /// stalls, and exposed ports answer 502 the whole time. If the call lands
    /// before spaced-host is actually up, the spaced-host connect fails with
    /// `Unavailable` and the caller (the node-ctl exposure reconciler)
    /// retries on its next tick.
    ///
    /// Frozen-or-dying states (`Pausing`, `Paused`, `Checkpointing`,
    /// `Suspending`, and everything without a VM) are excluded: a bridge RPC
    /// to a frozen guest can only hang on vsock, and there is nothing useful
    /// to re-arm in a guest that is leaving liveness.
    pub fn guest_bridge_reachable(self) -> bool {
        use SandboxState::*;
        match self {
            Starting | Started | Resuming | Restoring => true,
            Creating | Created | Stopping | Stopped | Deleting | Pausing | Paused
            | Checkpointing | Suspending | Suspended | Quarantined | Failed => false,
        }
    }

    /// A not-yet-[`Self::guest_bridge_reachable`] state that still precedes the
    /// sandbox's first running state, so it progresses toward reachability
    /// (`Started`) through its own create/start lifecycle — no separate,
    /// cluster-driven wake is required. Only the pre-run states qualify:
    /// `Creating` (create in flight) and `Created` (created, first start
    /// pending).
    ///
    /// This is the one carve-out from "every non-reachable state is terminal
    /// for a loopback-bridge desired-state apply": a sandbox created with
    /// exposed ports whose bridge op lands during create/start must stay
    /// retryable so the reconcile arms the bridge within seconds of `Started`
    /// rather than waiting for the periodic backstop. The frozen states
    /// (`Suspended`/`Paused`/`Stopped`) and the transitions into them
    /// (`Suspending`/`Pausing`/`Stopping`) do NOT qualify — they settle into a
    /// non-reachable state that becomes reachable only via a separate wake this
    /// ref does not drive, so they stay terminal.
    pub fn precedes_first_run(self) -> bool {
        matches!(self, SandboxState::Creating | SandboxState::Created)
    }

    /// The user action name that leads to this target state.
    fn action_name(&self) -> &'static str {
        use SandboxState::*;
        match self {
            Creating => "create",
            Starting => "start",
            Started => "start",
            Stopping => "stop",
            Stopped => "stop",
            Pausing => "pause",
            Paused => "pause",
            Resuming => "resume",
            Checkpointing => "checkpoint",
            Suspending => "suspend",
            Suspended => "suspend",
            Restoring => "restore",
            Quarantined => "quarantine",
            Deleting => "delete",
            _ => "transition",
        }
    }

    /// Human-readable name of the operation in progress (for transient states).
    fn operation_name(&self) -> &'static str {
        use SandboxState::*;
        match self {
            Creating => "creation",
            Starting => "start",
            Stopping => "stop",
            Pausing => "pause",
            Resuming => "resume",
            Checkpointing => "checkpoint",
            Suspending => "suspend",
            Restoring => "restore",
            Quarantined => "quarantine",
            Deleting => "deletion",
            _ => "operation",
        }
    }

    /// Which states are required for this target transition.
    fn required_states(&self) -> &'static str {
        use SandboxState::*;
        match self {
            Starting | Started => "created, stopped, or suspended",
            Stopping | Stopped => "started or paused",
            Pausing | Paused => "started",
            Resuming => "paused",
            Checkpointing => "started or paused",
            Suspending | Suspended => "started or paused",
            Restoring => "suspended",
            Deleting => {
                "created, started, stopped, paused, suspended, failed, quarantined, cleanup failed, or any transient state"
            }
            _ => "valid state",
        }
    }

    pub fn as_str(&self) -> &'static str {
        use SandboxState::*;
        match self {
            Creating => "creating",
            Created => "created",
            Starting => "starting",
            Started => "started",
            Stopping => "stopping",
            Stopped => "stopped",
            Deleting => "deleting",
            Pausing => "pausing",
            Paused => "paused",
            Resuming => "resuming",
            Checkpointing => "checkpointing",
            Suspending => "suspending",
            Suspended => "suspended",
            Restoring => "restoring",
            Quarantined => "quarantined",
            Failed => "failed",
        }
    }

    /// Every variant, for exhaustive iteration — coverage cross-checks that
    /// must visit the whole set (SQL-filter-vs-predicate parity tests, etc.).
    /// Kept next to the enum so a newly-added variant is added here once
    /// rather than re-listed at each call site.
    pub fn all() -> [Self; 16] {
        use SandboxState::*;
        [
            Creating,
            Created,
            Starting,
            Started,
            Stopping,
            Stopped,
            Deleting,
            Pausing,
            Paused,
            Resuming,
            Checkpointing,
            Suspending,
            Suspended,
            Restoring,
            Quarantined,
            Failed,
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("unrecognized sandbox lifecycle")]
pub struct ParseSandboxStateError;

impl std::str::FromStr for SandboxState {
    type Err = ParseSandboxStateError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        use SandboxState::*;
        match value {
            "creating" => Ok(Creating),
            "created" => Ok(Created),
            "starting" => Ok(Starting),
            "started" => Ok(Started),
            "stopping" => Ok(Stopping),
            "stopped" => Ok(Stopped),
            "deleting" => Ok(Deleting),
            "pausing" => Ok(Pausing),
            "paused" => Ok(Paused),
            "resuming" => Ok(Resuming),
            "checkpointing" => Ok(Checkpointing),
            "suspending" => Ok(Suspending),
            "suspended" => Ok(Suspended),
            "restoring" => Ok(Restoring),
            "quarantined" => Ok(Quarantined),
            "failed" => Ok(Failed),
            _ => Err(ParseSandboxStateError),
        }
    }
}

impl From<SandboxState> for pplx_space_sbx_mgr_protocol::proto::SandboxLifecycleState {
    fn from(state: SandboxState) -> Self {
        match state {
            SandboxState::Creating => Self::Creating,
            SandboxState::Created => Self::Created,
            SandboxState::Starting => Self::Starting,
            SandboxState::Started => Self::Started,
            SandboxState::Stopping => Self::Stopping,
            SandboxState::Stopped => Self::Stopped,
            SandboxState::Deleting => Self::Deleting,
            SandboxState::Pausing => Self::Pausing,
            SandboxState::Paused => Self::Paused,
            SandboxState::Resuming => Self::Resuming,
            SandboxState::Checkpointing => Self::Checkpointing,
            SandboxState::Suspending => Self::Suspending,
            SandboxState::Suspended => Self::Suspended,
            SandboxState::Restoring => Self::Restoring,
            SandboxState::Quarantined => Self::Quarantined,
            SandboxState::Failed => Self::Failed,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StateTransitionError {
    #[error("cannot {action}: sandbox is {state} — wait for {operation} to complete")]
    Transient {
        action: &'static str,
        state: SandboxStatus,
        operation: &'static str,
    },
    #[error("cannot {action}: sandbox is {state}, not {required}")]
    Invalid {
        action: &'static str,
        state: SandboxStatus,
        required: &'static str,
    },
}

impl From<StateTransitionError> for tonic::Status {
    fn from(error: StateTransitionError) -> Self {
        match error {
            StateTransitionError::Transient { .. } => Self::unavailable(error.to_string()),
            StateTransitionError::Invalid { .. } => Self::failed_precondition(error.to_string()),
        }
    }
}

impl fmt::Display for SandboxState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Sets a sandbox event's lifecycle together with the cleanup that owns the
/// row, so `from_state` and `from_cleanup_state` (or the `to_` pair) always
/// describe one status.
pub trait SandboxStatusEventDetails {
    fn with_from_status(self, status: impl Into<SandboxStatus>) -> Self;
    fn with_to_status(self, status: impl Into<SandboxStatus>) -> Self;
}

impl SandboxStatusEventDetails for pplx_space_events::SandboxEventDetails {
    fn with_from_status(self, status: impl Into<SandboxStatus>) -> Self {
        let status = status.into();
        self.with_from_state(status.lifecycle().as_str())
            .from_cleanup_state(status.event_cleanup_state())
    }

    fn with_to_status(self, status: impl Into<SandboxStatus>) -> Self {
        let status = status.into();
        self.with_to_state(status.lifecycle().as_str())
            .to_cleanup_state(status.event_cleanup_state())
    }
}

/// A teardown transition a cleanup can own a row over. The triggers allow a
/// cleanup only over these.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Teardown {
    Stopping,
    Deleting,
    Suspending,
}

impl Teardown {
    pub const fn lifecycle(self) -> SandboxState {
        match self {
            Self::Stopping => SandboxState::Stopping,
            Self::Deleting => SandboxState::Deleting,
            Self::Suspending => SandboxState::Suspending,
        }
    }

    /// The teardown transition `lifecycle` is, if it is one.
    pub const fn of(lifecycle: SandboxState) -> Option<Self> {
        match lifecycle {
            SandboxState::Stopping => Some(Self::Stopping),
            SandboxState::Deleting => Some(Self::Deleting),
            SandboxState::Suspending => Some(Self::Suspending),
            _ => None,
        }
    }
}

impl From<Teardown> for SandboxState {
    fn from(teardown: Teardown) -> Self {
        teardown.lifecycle()
    }
}

/// A sandbox row's status: the lifecycle of a row no cleanup owns, or the
/// cleanup that owns it and the teardown transition it is over. A row stores
/// the lifecycle in `sandboxes.state` and the cleanup in `cleanup_state`.
///
/// A status is not a lifecycle and doesn't compare equal to one. Ask
/// [`Self::is_unowned`] or [`Self::unowned`] whether a row is in a lifecycle;
/// [`Self::lifecycle`] is the stored lifecycle, a cleanup's teardown
/// transition included.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SandboxStatus {
    /// No cleanup owns the row.
    Unowned(SandboxState),
    /// `cleanup` owns the row over `teardown`.
    Owned {
        teardown: Teardown,
        cleanup: CleanupState,
    },
}

impl From<SandboxState> for SandboxStatus {
    fn from(lifecycle: SandboxState) -> Self {
        Self::Unowned(lifecycle)
    }
}

impl SandboxStatus {
    /// A required cleanup over `stopping`, as an intent-less cleanup row is
    /// stored.
    #[cfg(any(test, feature = "test-support"))]
    pub const CLEANUP_REQUIRED: Self = Self::owned(Teardown::Stopping, CleanupState::Required);

    /// A failed cleanup over `stopping`, as an intent-less cleanup row is
    /// stored.
    #[cfg(any(test, feature = "test-support"))]
    pub const CLEANUP_FAILED: Self = Self::owned(Teardown::Stopping, CleanupState::Failed);

    /// Each lifecycle no cleanup owns and each cleanup over `stopping`, in the
    /// order the combined status listed them before the split, so a table
    /// keyed by position keeps its rows.
    #[cfg(any(test, feature = "test-support"))]
    pub fn sweep() -> [Self; 18] {
        use SandboxState::*;
        [
            Creating.into(),
            Created.into(),
            Starting.into(),
            Started.into(),
            Stopping.into(),
            Stopped.into(),
            Deleting.into(),
            Pausing.into(),
            Paused.into(),
            Resuming.into(),
            Checkpointing.into(),
            Suspending.into(),
            Suspended.into(),
            Restoring.into(),
            Self::CLEANUP_REQUIRED,
            Self::CLEANUP_FAILED,
            Quarantined.into(),
            Failed.into(),
        ]
    }

    /// A row `cleanup` owns over `teardown`.
    pub const fn owned(teardown: Teardown, cleanup: CleanupState) -> Self {
        Self::Owned { teardown, cleanup }
    }

    /// The stored lifecycle: what `sandboxes.state` holds and publication
    /// reports, which is a cleanup's teardown transition while one owns the
    /// row. To decide whether a row is in a lifecycle, ask
    /// [`Self::is_unowned`].
    pub const fn lifecycle(self) -> SandboxState {
        match self {
            Self::Unowned(lifecycle) => lifecycle,
            Self::Owned { teardown, .. } => teardown.lifecycle(),
        }
    }

    /// The cleanup that owns the row, if one does.
    pub const fn cleanup(self) -> Option<CleanupState> {
        match self {
            Self::Unowned(_) => None,
            Self::Owned { cleanup, .. } => Some(cleanup),
        }
    }

    /// The lifecycle, when no cleanup owns the row.
    pub const fn unowned(self) -> Option<SandboxState> {
        match self {
            Self::Unowned(lifecycle) => Some(lifecycle),
            Self::Owned { .. } => None,
        }
    }

    /// Whether the row is in `lifecycle` and no cleanup owns it.
    pub fn is_unowned(self, lifecycle: SandboxState) -> bool {
        self.unowned() == Some(lifecycle)
    }

    /// Whether a cleanup owns the row.
    pub const fn is_cleanup_owned(self) -> bool {
        matches!(self, Self::Owned { .. })
    }

    /// The lifecycle a wake answers the row as: a required cleanup is waited
    /// out like a transition, and a failed one refused like a terminal state.
    pub const fn wake_as(self) -> SandboxState {
        match self {
            Self::Unowned(lifecycle) => lifecycle,
            Self::Owned {
                cleanup: CleanupState::Required,
                ..
            } => SandboxState::Stopping,
            Self::Owned {
                cleanup: CleanupState::Failed,
                ..
            } => SandboxState::Failed,
        }
    }

    /// Wait completes for a failed cleanup, never a required one.
    pub const fn completes_wait(self) -> bool {
        match self {
            Self::Unowned(lifecycle) => lifecycle.completes_wait(),
            Self::Owned { cleanup, .. } => matches!(cleanup, CleanupState::Failed),
        }
    }

    /// A cleanup-owned row admits only Delete and quarantine.
    pub(crate) fn can_transition_to(self, target: SandboxState) -> bool {
        match self {
            Self::Unowned(lifecycle) => lifecycle.can_transition_to(target),
            Self::Owned { .. } => {
                matches!(target, SandboxState::Deleting | SandboxState::Quarantined)
            }
        }
    }

    pub fn transition(self, target: SandboxState) -> Result<SandboxState, StateTransitionError> {
        match self {
            Self::Unowned(lifecycle) => lifecycle.transition(target),
            Self::Owned { .. } if self.can_transition_to(target) => Ok(target),
            Self::Owned { .. } => Err(StateTransitionError::Invalid {
                action: target.action_name(),
                state: self,
                required: target.required_states(),
            }),
        }
    }

    /// See [`SandboxState::can_authorize_net_gateway`]: a cleanup's gateway is
    /// condemned.
    pub fn can_authorize_net_gateway(self) -> bool {
        self.unowned()
            .is_some_and(|lifecycle| lifecycle.can_authorize_net_gateway())
    }

    /// See [`SandboxState::holds_runtime_outside_suspend`]: a cleanup holds a
    /// runtime of its own.
    pub fn holds_runtime_outside_suspend(self) -> bool {
        self.unowned()
            .is_some_and(|lifecycle| lifecycle.holds_runtime_outside_suspend())
    }

    /// See [`SandboxState::holds_parent_referencing_instance_subvolume`]: a
    /// cleanup may still keep the instance subvolume.
    pub fn holds_parent_referencing_instance_subvolume(self) -> bool {
        self.unowned()
            .is_none_or(|lifecycle| lifecycle.holds_parent_referencing_instance_subvolume())
    }

    /// See [`SandboxState::is_transient`]: a cleanup is not an operation in
    /// progress.
    pub fn is_transient(&self) -> bool {
        self.unowned()
            .is_some_and(|lifecycle| lifecycle.is_transient())
    }

    /// See [`SandboxState::guest_bridge_reachable`].
    pub fn guest_bridge_reachable(self) -> bool {
        self.unowned()
            .is_some_and(|lifecycle| lifecycle.guest_bridge_reachable())
    }

    /// See [`SandboxState::can_cycle_net_gateway`].
    pub fn can_cycle_net_gateway(self) -> bool {
        self.unowned()
            .is_some_and(|lifecycle| lifecycle.can_cycle_net_gateway())
    }

    /// See [`SandboxState::precedes_first_run`].
    pub fn precedes_first_run(self) -> bool {
        self.unowned()
            .is_some_and(|lifecycle| lifecycle.precedes_first_run())
    }

    /// The cleanup as sandbox events carry it in
    /// `from_cleanup_state`/`to_cleanup_state`.
    pub fn event_cleanup_state(self) -> &'static str {
        self.cleanup().map_or("none", CleanupState::as_str)
    }

    /// The label `sandbox.worker.lifecycle.count` groups by, which predates
    /// the split: `cleanup_required`/`cleanup_failed` for a cleanup-owned
    /// row, otherwise the lifecycle.
    pub fn legacy_label(self) -> &'static str {
        match self {
            Self::Unowned(lifecycle) => lifecycle.as_str(),
            Self::Owned {
                cleanup: CleanupState::Required,
                ..
            } => "cleanup_required",
            Self::Owned {
                cleanup: CleanupState::Failed,
                ..
            } => "cleanup_failed",
        }
    }

    /// The status a row whose `state` and `cleanup_state` hold these values
    /// has, or `None` when either names nothing or the cleanup is over a
    /// lifecycle no cleanup can be over.
    pub fn from_columns(state: &str, cleanup_state: Option<&str>) -> Option<Self> {
        let lifecycle = state.parse::<SandboxState>().ok()?;
        match cleanup_state {
            None => Some(lifecycle.into()),
            Some(value) => Some(Self::owned(
                Teardown::of(lifecycle)?,
                CleanupState::from_column(value)?,
            )),
        }
    }

    /// Every lifecycle without a cleanup, then each cleanup over each
    /// teardown transition.
    pub fn all() -> [Self; 22] {
        let mut all = [Self::from(SandboxState::Creating); 22];
        for (slot, lifecycle) in all.iter_mut().zip(SandboxState::all()) {
            *slot = lifecycle.into();
        }
        let owned = [
            Self::owned(Teardown::Stopping, CleanupState::Required),
            Self::owned(Teardown::Deleting, CleanupState::Required),
            Self::owned(Teardown::Suspending, CleanupState::Required),
            Self::owned(Teardown::Stopping, CleanupState::Failed),
            Self::owned(Teardown::Deleting, CleanupState::Failed),
            Self::owned(Teardown::Suspending, CleanupState::Failed),
        ];
        all[16..].copy_from_slice(&owned);
        all
    }
}

/// The lifecycle, followed by the cleanup that owns the row.
impl fmt::Display for SandboxStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unowned(lifecycle) => f.write_str(lifecycle.as_str()),
            Self::Owned { teardown, cleanup } => {
                write!(f, "{}, cleanup {}", teardown.lifecycle(), cleanup.as_str())
            }
        }
    }
}

#[cfg(test)]
mod tests;
