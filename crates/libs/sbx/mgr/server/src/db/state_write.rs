//! The transitions sbx-manager persists in the node-local SQLite
//! `sandboxes.state` column, and `update_state_from`, the generic writer that
//! compares against the caller's expected states. Every other writer of the
//! column is a named specialized writer in `src/db`.
//!
//! `SandboxState::can_transition_to` is the RPC-admission machine. [`EDGES`]
//! is a separate inventory of the transitions the database writers implement:
//! one entry per writer purpose, naming the writer functions and their
//! callers. Constructors are private, and `update_state_from` takes a
//! `&'static Edge`, so its call sites cannot invent one. Nothing checks the
//! table against the specialized writers.
//!
//! The table follows these conventions:
//! - A specialized compare-and-set writer's from-set is its SQL predicate,
//!   narrowed to the states its production callers reach.
//! - An `update_state_from` edge's from-set is exactly what its callers
//!   legitimately see.
//! - An empty from-set is an insert: no row existed.
//! - A write over a row already in the target state changes no state and has
//!   no edge.
//! - Deleting the row is not a state write and has no edge.
//! - Writers with only test callers are excluded.

use super::*;

/// A set of lifecycle states, as a bitset in `SandboxState::all()` order.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct StateSet(u32);

impl StateSet {
    pub const EMPTY: Self = Self(0);
    pub const ALL: Self = Self((1 << 18) - 1);

    pub const fn of(states: &[SandboxState]) -> Self {
        let mut set = Self::EMPTY;
        let mut rest = states;
        while let [state, tail @ ..] = rest {
            set = set.with(*state);
            rest = tail;
        }
        set
    }

    pub const fn with(self, state: SandboxState) -> Self {
        Self(self.0 | bit(state))
    }

    pub const fn without(self, state: SandboxState) -> Self {
        Self(self.0 & !bit(state))
    }

    pub const fn contains(self, state: SandboxState) -> bool {
        self.0 & bit(state) != 0
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// The members in `SandboxState::all()` order.
    pub fn iter(self) -> impl Iterator<Item = SandboxState> {
        SandboxState::all()
            .into_iter()
            .filter(move |state| self.contains(*state))
    }
}

impl fmt::Debug for StateSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set()
            .entries(self.iter().map(|state| state.as_str()))
            .finish()
    }
}

/// Wildcard-free, so a new lifecycle state does not compile until it has a
/// bit, and `StateSet::ALL` must then grow with it.
const fn bit(state: SandboxState) -> u32 {
    1 << match state {
        SandboxState::Creating => 0,
        SandboxState::Created => 1,
        SandboxState::Starting => 2,
        SandboxState::Started => 3,
        SandboxState::Stopping => 4,
        SandboxState::Stopped => 5,
        SandboxState::Deleting => 6,
        SandboxState::Pausing => 7,
        SandboxState::Paused => 8,
        SandboxState::Resuming => 9,
        SandboxState::Checkpointing => 10,
        SandboxState::Suspending => 11,
        SandboxState::Suspended => 12,
        SandboxState::Restoring => 13,
        SandboxState::CleanupRequired => 14,
        SandboxState::CleanupFailed => 15,
        SandboxState::Quarantined => 16,
        SandboxState::Failed => 17,
    }
}

/// Why a writer implements an edge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EdgeKind {
    /// Admission of an operation: a client RPC, or the node's own scheduler,
    /// auto-lifecycle or eviction acting as one.
    Request,
    /// An in-flight operation finished.
    Completion,
    /// Rollback to the operation's entry state after a failure.
    Compensation,
    /// Startup, inventory, quarantine or cleanup repair.
    Recovery,
}

/// One family of persisted transitions: every state in `from` to `to`.
#[derive(Debug)]
pub struct Edge {
    name: &'static str,
    from: StateSet,
    to: SandboxState,
    kind: EdgeKind,
}

impl Edge {
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Empty for an insert.
    pub const fn from(&self) -> StateSet {
        self.from
    }

    pub const fn to(&self) -> SandboxState {
        self.to
    }

    pub const fn kind(&self) -> EdgeKind {
        self.kind
    }
}

macro_rules! edge_table {
    ($($(#[doc = $doc:literal])+ $name:ident: $kind:ident, $from:expr => $to:ident;)+) => {
        /// The persisted-edge constants. Each names its writer functions.
        pub mod edges {
            use super::{Edge, EdgeKind, StateSet};
            use crate::state::SandboxState::*;

            const fn of(states: &[crate::state::SandboxState]) -> StateSet {
                StateSet::of(states)
            }

            const INSERT: StateSet = StateSet::EMPTY;

            $(
                $(#[doc = $doc])+
                pub const $name: Edge = Edge {
                    name: stringify!($name),
                    from: $from,
                    to: $to,
                    kind: EdgeKind::$kind,
                };
            )+

            /// Every persisted edge, in declaration order.
            pub const EDGES: &[Edge] = &[$($name),+];
        }
    };
}

edge_table! {
    // Create, pool clone and import.

    /// `insert_sandbox_with_cleanup_episode_and_route_evidence` (create,
    /// pool clone) and `transfer::reserve_import` insert a `creating` row with
    /// an armed cleanup episode.
    CREATE_INSERT: Request, INSERT => Creating;
    /// `create_with_id` through `write_create_completed`.
    CREATE_COMPLETE: Completion, of(&[Creating]) => Created;
    /// `create_with_id` in base-snapshot mode, through
    /// `write_base_snapshot_create_completed` after its own `created` write.
    CREATE_BASE_SNAPSHOT_COMPLETE: Completion, of(&[Created]) => Suspended;
    /// `complete_pool_clone` and `complete_import`, under their armed
    /// episodes.
    CLONE_OR_IMPORT_COMPLETE_SUSPENDED: Completion, of(&[Creating]) => Suspended;
    /// `complete_import` of a stopped export.
    IMPORT_COMPLETE_STOPPED: Completion, of(&[Creating]) => Stopped;

    // Cold start and restore activation.

    /// `begin_activation_with_route_evidence(ColdStart)` from
    /// `start_sandbox_with_authority`.
    COLD_START_BEGIN: Request, of(&[Created, Stopped, Suspended]) => Starting;
    /// `begin_activation_with_route_evidence(Restore)` from
    /// `start_from_checkpoint`.
    RESTORE_BEGIN: Request, of(&[Suspended]) => Restoring;
    /// `try_start_from_warm_snapshot` through `write_warm_start_restaged`
    /// when the row is not already `suspended`, before the cold path.
    WARM_START_RESTAGE: Request, of(&[Created, Stopped]) => Suspended;
    /// `insert_restoring_sandbox_row_from_snapshot` through
    /// `restore_adopt_orphan_or_insert_sandbox_in_transaction`: a
    /// cross-node restore for a sandbox this node has no row for.
    CROSS_NODE_RESTORE_INSERT: Request, INSERT => Restoring;
    /// `mark_start_succeeded`.
    START_COMPLETE: Completion, of(&[Starting]) => Started;
    /// `mark_restore_succeeded`.
    RESTORE_COMPLETE_STARTED: Completion, of(&[Restoring]) => Started;
    /// `mark_restore_succeeded` with a paused target.
    RESTORE_COMPLETE_PAUSED: Completion, of(&[Restoring]) => Paused;
    /// `publish_restored_suspended_artifact` and
    /// `publish_restored_suspended_without_artifact`: cross-node restore
    /// staging published a suspended row.
    CROSS_NODE_RESTORE_PUBLISHED: Completion, of(&[Restoring]) => Suspended;
    /// `publish_restored_disk_as_new`: a restore-as-new published a stopped
    /// disk.
    RESTORED_DISK_AS_NEW: Completion, of(&[Restoring]) => Stopped;
    /// `rollback_starting_episode` to a `created` entry state.
    START_ROLLBACK_CREATED: Compensation, of(&[Starting]) => Created;
    /// `rollback_starting_episode` to a `stopped` entry state.
    START_ROLLBACK_STOPPED: Compensation, of(&[Starting]) => Stopped;
    /// `rollback_starting_episode` to a `suspended` entry state.
    START_ROLLBACK_SUSPENDED: Compensation, of(&[Starting]) => Suspended;
    /// `rollback_restoring_episode` from `start_from_checkpoint`.
    RESTORE_ROLLBACK: Compensation, of(&[Restoring]) => Suspended;

    // Pause and resident resume.

    /// `pause` after `transition(Pausing)` on the read, and the suspend
    /// auto-pause after reading `started`.
    PAUSE_BEGIN: Request, of(&[Started]) => Pausing;
    /// `pause` and the suspend auto-pause, after node-manager confirms the
    /// pause.
    PAUSE_COMPLETE: Completion, of(&[Pausing]) => Paused;
    /// `pause` and the suspend auto-pause after a failed guest preparation;
    /// `pause` after a node connect failure, and `pause_with_reconciliation`,
    /// best effort.
    PAUSE_ROLLBACK: Compensation, of(&[Pausing]) => Started;
    /// `begin_resident_resume_with_policy`'s `update_state_from` inside its
    /// policy transaction, and `begin_resident_resume`'s without an activation
    /// policy.
    RESUME_BEGIN: Request, of(&[Paused]) => Resuming;
    /// `restore_resident_resume_state(Started)` through
    /// `rollback_activation_state`.
    RESUME_COMPLETE: Completion, of(&[Resuming]) => Started;
    /// `restore_resident_resume_state(Paused)` after a failed resume.
    RESUME_ROLLBACK: Compensation, of(&[Resuming]) => Paused;

    // Checkpoints and template base snapshots.

    /// `begin_checkpoint_state` from `do_checkpoint`, checked and written under
    /// one guard.
    CHECKPOINT_BEGIN: Request, of(&[Started, Paused]) => Checkpointing;
    /// `begin_template_base_snapshot_state` from
    /// `create_template_base_snapshot`, checked and written under one guard.
    TEMPLATE_SNAPSHOT_BEGIN: Request, of(&[Paused]) => Checkpointing;
    /// `restore_checkpoint_state` from `finish_checkpoint_success`.
    CHECKPOINT_COMPLETE_STARTED: Completion, of(&[Checkpointing]) => Started;
    /// `restore_checkpoint_state` from `finish_checkpoint_success`, and
    /// `complete_template_base_snapshot_state`.
    CHECKPOINT_COMPLETE_PAUSED: Completion, of(&[Checkpointing]) => Paused;
    /// `restore_checkpoint_state_best_effort` after a failed checkpoint.
    CHECKPOINT_ROLLBACK_STARTED: Compensation, of(&[Checkpointing]) => Started;
    /// `restore_checkpoint_state_best_effort`, and
    /// `rollback_template_base_snapshot_state` after a failed template base
    /// snapshot.
    CHECKPOINT_ROLLBACK_PAUSED: Compensation, of(&[Checkpointing]) => Paused;

    // Suspend.

    /// `begin_suspend` and `begin_teardown_suspend`.
    SUSPEND_BEGIN: Request, of(&[Paused]) => Suspending;
    /// `complete_row_intent_cleanup_for_job(MarkSuspended)` with a published
    /// suspend artifact.
    SUSPEND_COMPLETE: Completion, of(&[Suspending]) => Suspended;
    /// `rollback_suspending_to_paused` from the suspend snapshot path.
    SUSPEND_ROLLBACK: Compensation, of(&[Suspending]) => Paused;
    /// `restage_rolled_back_full_snapshot`: the full process-snapshot rollback
    /// restages its stopped row, so `start_from_checkpoint` can restore the
    /// snapshot. A row that is already `suspended` is left as it is.
    PROCESS_SNAPSHOT_ROLLBACK_RESTAGE: Completion, of(&[Stopped]) => Suspended;

    // Stop and delete.

    /// Stop admission: `admit_lifecycle_cleanup_fenced` through
    /// `update_state_superseding_activation_in_transaction(Stopping)` and
    /// `write_cleanup_admission_state`. Stop does not consult `transition`,
    /// but resolves its target only while the runtime is live (`fc.pid`), and
    /// admission does not re-check it. A settled `created`, `stopped` or
    /// `suspended` source is therefore reached only in the window between
    /// that gate and the cleanup fence, or with a leaked runtime; a `creating`
    /// row has none. Kill admits only `started` and `paused` through the same
    /// writer, and the generic guards refuse `deleting` and `quarantined`.
    STOP_ADMISSION: Request,
        StateSet::ALL
            .without(Creating)
            .without(Stopping)
            .without(Deleting)
            .without(Quarantined) => Stopping;
    /// `complete_row_intent_cleanup_for_job(MarkStopped)` and
    /// `publish_stopped_artifact`.
    STOP_COMPLETE: Completion, of(&[Stopping]) => Stopped;
    /// `run_due_auto_lifecycle`'s idle Stop of a `suspended` row through
    /// `stop_suspended_for_auto_lifecycle`, which bypasses every RPC.
    AUTO_LIFECYCLE_STOP_SUSPENDED: Request, of(&[Suspended]) => Stopped;
    /// Delete admission: `admit_lifecycle_cleanup_fenced` through
    /// `update_state_superseding_activation_in_transaction(Deleting)` and
    /// `write_cleanup_admission_state`, from the Delete RPC and auto-lifecycle
    /// Delete.
    DELETE_ADMISSION: Request, StateSet::ALL.without(Deleting) => Deleting;
    /// The eviction claims: `claim_stopped_sandbox_for_eviction`,
    /// `claim_suspended_sandbox_for_eviction`,
    /// `claim_failed_restoring_sandbox_for_eviction`,
    /// `claim_teardown_suspend_failed_sandbox_for_eviction` and
    /// `claim_quarantined_sandbox_for_eviction`.
    EVICTION_CLAIM: Request,
        of(&[Stopped, Suspended, Restoring, Suspending, Quarantined]) => Deleting;

    // Cleanup hand-off and completion.

    /// `request_cleanup_episode`: the activation cleanup guard
    /// (`schedule_activation_cleanup`), startup recovery and cleanup recovery
    /// hand an armed episode to cleanup. Create's guard can fire after its own
    /// `created` write, or after the base-snapshot `suspended` write when
    /// `complete_create` fails.
    ACTIVATION_CLEANUP_REQUEST: Recovery,
        of(&[Creating, Created, Starting, Suspended, Restoring]) => CleanupRequired;
    /// `mark_cleanup_required` from startup recovery (interrupted
    /// `starting`, `stopping`, `deleting`, `suspending` and `restoring` rows),
    /// cleanup recovery, and a restore that finds a pending `mark_suspended`
    /// intent; `mark_cleanup_required_if_no_intent` for an ambiguous restore
    /// retry; `mark_cleanup_required_if_no_intent_in_state` from cleanup
    /// recovery.
    CLEANUP_HANDOFF: Recovery,
        of(&[Starting, Stopping, Deleting, Suspending, Restoring, CleanupFailed])
            => CleanupRequired;
    /// `pin_suspended_artifact_for_cleanup`: startup suspend recovery pins a
    /// staged artifact and hands the row to cleanup.
    SUSPEND_ARTIFACT_CLEANUP: Recovery, of(&[Suspending]) => CleanupRequired;
    /// `mark_cleanup_failed` (from `mark_async_cleanup_failed`, for rows whose
    /// intent the completion owns) and `record_cleanup_episode_failure`.
    CLEANUP_FAILURE: Recovery,
        of(&[Stopping, Suspending, Deleting, CleanupRequired]) => CleanupFailed;
    /// `complete_cleanup_episode_for_job` and
    /// `complete_row_intent_cleanup_for_job` through `apply_cleanup_state`
    /// for a `mark_created` completion.
    CLEANUP_COMPLETE_CREATED: Recovery, of(&[CleanupRequired, CleanupFailed]) => Created;
    /// As above for `mark_stopped`, and `publish_stopped_artifact` for a
    /// cleanup row with a `mark_stopped` intent.
    CLEANUP_COMPLETE_STOPPED: Recovery, of(&[CleanupRequired, CleanupFailed]) => Stopped;
    /// As above for `mark_suspended`.
    CLEANUP_COMPLETE_SUSPENDED: Recovery, of(&[CleanupRequired, CleanupFailed]) => Suspended;
    /// `quarantine_cleanup_episode_for_job`, and `apply_cleanup_state` for a
    /// `quarantine` completion or a `mark_suspended` completion without a
    /// suspend artifact.
    CLEANUP_QUARANTINE: Recovery, of(&[CleanupRequired, CleanupFailed]) => Quarantined;

    // Quarantine entry and exit.

    /// `mark_quarantined_best_effort` and `mark_quarantined_from_state`: an
    /// unconfirmed pause (`pause_with_reconciliation`, startup pause
    /// recovery), a failed checkpoint, a failed process-snapshot stop, and a
    /// failed resident resume.
    IN_FLIGHT_QUARANTINE: Recovery,
        of(&[Pausing, Stopping, Resuming, Checkpointing]) => Quarantined;
    /// `mark_suspending_quarantined` (suspend snapshot path and startup
    /// suspend recovery), and
    /// `complete_row_intent_cleanup_for_job(MarkSuspended)` without a suspend
    /// artifact.
    SUSPEND_QUARANTINE: Recovery, of(&[Suspending]) => Quarantined;
    /// `settle_startup_resident(Quarantined)`: a `started` row with no live
    /// VM, and resident rows whose VM contradicts or cannot prove the state.
    STARTUP_RESIDENT_QUARANTINE: Recovery,
        of(&[Started, Paused, Resuming, Checkpointing]) => Quarantined;
    /// `insert_quarantined_sandbox_in_transaction` from orphan
    /// materialization.
    ORPHAN_QUARANTINE_INSERT: Recovery, INSERT => Quarantined;
    /// `recover_pause_failed_quarantine` from the watchdog.
    PAUSE_FAILED_QUARANTINE_RECOVERY_STARTED: Recovery, of(&[Quarantined]) => Started;
    /// `recover_pause_failed_quarantine` with a paused VM.
    PAUSE_FAILED_QUARANTINE_RECOVERY_PAUSED: Recovery, of(&[Quarantined]) => Paused;
    /// `adopt_quarantine_cleanup_episode`: quarantine recovery hands a pinned
    /// suspend artifact to cleanup.
    QUARANTINE_CLEANUP_ADOPTION: Recovery, of(&[Quarantined]) => CleanupRequired;
    /// `rearm_prepared_clone_disposal`: Delete of a quarantined pool clone
    /// re-requests its disposal episode instead of writing `deleting`.
    POOL_CLONE_DISPOSAL_REARM: Request, of(&[Quarantined]) => CleanupRequired;
    /// `restore_adopt_orphan_or_insert_sandbox_in_transaction`'s conflict
    /// branch: orphan materialization or a cross-node restore insert adopts
    /// the synthetic orphan row an earlier pass quarantined.
    ORPHAN_RESTORE_ADOPTION: Recovery, of(&[Quarantined]) => Restoring;

    // Startup and inventory repair.

    /// `recover_interrupted_create`, through `require_startup_state_write`.
    INTERRUPTED_CREATE_RECOVERY: Recovery, of(&[Creating]) => Created;
    /// `recover_unlaunched_start`, through `require_startup_state_write`,
    /// when a rootfs exists.
    UNLAUNCHED_START_RECOVERY_STOPPED: Recovery, of(&[Starting]) => Stopped;
    /// `recover_unlaunched_start` otherwise.
    UNLAUNCHED_START_RECOVERY_CREATED: Recovery, of(&[Starting]) => Created;
    /// `recover_interrupted_pause_state` from startup pause recovery.
    INTERRUPTED_PAUSE_RECOVERY_STARTED: Recovery, of(&[Pausing]) => Started;
    /// `recover_interrupted_pause_state` with a paused VM.
    INTERRUPTED_PAUSE_RECOVERY_PAUSED: Recovery, of(&[Pausing]) => Paused;
    /// `settle_startup_resident(Started)`: the VM runs.
    STARTUP_RESIDENT_STARTED: Recovery, of(&[Resuming, Checkpointing]) => Started;
    /// `settle_startup_resident(Paused)`: an interrupted resume left the VM
    /// paused.
    STARTUP_RESIDENT_PAUSED: Recovery, of(&[Resuming]) => Paused;
    /// `rollback_suspending_to_paused` from startup suspend recovery when the
    /// VM is live without a usable artifact.
    STARTUP_SUSPEND_ROLLBACK: Recovery, of(&[Suspending]) => Paused;
    /// `recover_interrupted_startup_states`, and
    /// `publish_orphan_quarantine_recovered_artifact`.
    INTERRUPTED_RESTORE_RECOVERY: Recovery, of(&[Restoring]) => Suspended;
    /// Orphan materialization: `insert_sandbox` of a `cleanup_required` row,
    /// which `mark_cleanup_required` then gives its intent.
    ORPHAN_CLEANUP_INSERT: Recovery, INSERT => CleanupRequired;
    /// `insert_recovered_suspended_sandbox_in_transaction` from orphan
    /// materialization.
    ORPHAN_SUSPENDED_INSERT: Recovery, INSERT => Suspended;
    /// `restore_adopt_orphan_or_insert_sandbox_in_transaction`'s insert
    /// branch from orphan materialization of an interrupted restore.
    ORPHAN_RESTORE_INSERT: Recovery, INSERT => Restoring;
}

pub use edges::EDGES;

/// A generic guard that refused a write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateWriteGuard {
    /// Only deletion leaves `quarantined`.
    Quarantined,
    /// Cleanup owns the row.
    CleanupOwned,
}

/// Why [`update_state_from`] did not write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateWriteRefusal {
    /// The row is not in any of the edge's source states.
    FromState,
    Guard(StateWriteGuard),
}

/// A guard refusal or a missing row, in the text clients receive.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SandboxStateWriteNotApplied {
    #[error("sandbox state write was not applied: {}", guard_label(.0))]
    Guard(StateWriteGuard),
    #[error("sandbox state write was not applied: missing")]
    Missing,
}

fn guard_label(guard: &StateWriteGuard) -> &'static str {
    match guard {
        StateWriteGuard::Quarantined => "blocked_by_quarantine",
        StateWriteGuard::CleanupOwned => "superseded_by_cleanup_episode",
    }
}

/// The guards every generic write of the node-local SQLite `sandboxes.state`
/// shares. Quarantine answers first: only deletion leaves `quarantined`.
/// Cleanup then owns the row while an episode that keeps the row exists or has
/// been requested, while a cleanup state carries a current `delete_row` intent
/// (except for deletion itself), and against any target that would revive a
/// `stopping` or cleanup row. An armed `delete_row` create episode
/// deliberately does not block: create completion writes through it.
pub(super) fn generic_state_write_refusal(
    conn: &Connection,
    id: &str,
    previous: Option<SandboxState>,
    target: SandboxState,
) -> Result<Option<StateWriteGuard>> {
    if generic_writer_transition_blocked(previous, target) {
        return Ok(Some(StateWriteGuard::Quarantined));
    }
    let cleanup_owned = cleanup_episode(conn, id)?
        .is_some_and(|episode| !episode.completion.deletes_row() || episode.requested_at.is_some())
        || (matches!(
            previous,
            Some(
                SandboxState::Deleting
                    | SandboxState::CleanupRequired
                    | SandboxState::CleanupFailed
            )
        ) && target != SandboxState::Deleting
            && cleanup_intent_current(conn, id, CleanupCompletion::DeleteRow)?)
        || (matches!(
            previous,
            Some(
                SandboxState::Stopping
                    | SandboxState::CleanupRequired
                    | SandboxState::CleanupFailed
            )
        ) && matches!(
            target,
            SandboxState::Started
                | SandboxState::Paused
                | SandboxState::Pausing
                | SandboxState::Resuming
                | SandboxState::Checkpointing
        ));
    Ok(cleanup_owned.then_some(StateWriteGuard::CleanupOwned))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "callers must distinguish an applied state write from a refused one"]
pub enum StateWriteOutcome {
    Applied {
        from: SandboxState,
    },
    Refused {
        found: SandboxState,
        refusal: StateWriteRefusal,
    },
    Missing,
}

/// Write `edge.to()` only if the row is in one of `edge.from()`.
///
/// The expected-state predicate is part of the `UPDATE` itself. The generic
/// guards answer first, so a guard refusal does not say whether the row was
/// also outside the from-set: a caller that must tell the two apart checks
/// `from.contains(found)`, not the refusal.
///
/// The failure reason, restore columns and receipts for another target are
/// cleared, and a `deleting` write fails the active teardown jobs. Inside a
/// caller's transaction this runs in it. In autocommit it opens its own
/// `IMMEDIATE` transaction, so the guard reads, the write and the job failure
/// commit together; it never nests one.
///
/// Inserts and `quarantined` or `failed` targets are `InvalidQuery`. A write
/// that matches no row although the row was read in a source state is
/// `StatementChangedRows(0)`.
pub fn update_state_from(
    conn: &Connection,
    id: &str,
    edge: &'static Edge,
) -> Result<StateWriteOutcome> {
    if edge.from.is_empty() {
        return Err(rusqlite::Error::InvalidQuery);
    }
    write_state_from(conn, id, edge.from, edge.to)
}

/// [`update_state_from`] over an arbitrary from-set, for tests that sweep
/// from-sets no edge has.
#[cfg(test)]
pub(crate) fn update_state_from_states(
    conn: &Connection,
    id: &str,
    from: StateSet,
    to: SandboxState,
) -> Result<StateWriteOutcome> {
    write_state_from(conn, id, from, to)
}

/// Stop and Delete admission's write, for
/// `update_state_superseding_activation_in_transaction` alone. It writes over
/// every state the generic guards admit, so it is not an `update_state_from`
/// edge: admission also re-admits a row already in its target, a `stopping`
/// row for a repeated Stop of a live runtime and a `deleting` row for a
/// repeated Delete, and no edge lists its own target. The guards refuse a Stop
/// of a `deleting` or `quarantined` row. [`edges::STOP_ADMISSION`] and
/// [`edges::DELETE_ADMISSION`] list the transitions it makes.
pub(super) fn write_cleanup_admission_state(
    conn: &Connection,
    id: &str,
    target: SandboxState,
) -> Result<std::result::Result<(), SandboxStateWriteNotApplied>> {
    if !matches!(target, SandboxState::Stopping | SandboxState::Deleting) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    match write_state_from(conn, id, StateSet::ALL, target)? {
        StateWriteOutcome::Applied { .. } => Ok(Ok(())),
        StateWriteOutcome::Refused {
            refusal: StateWriteRefusal::Guard(guard),
            ..
        } => {
            match guard {
                StateWriteGuard::Quarantined => tracing::warn!(
                    sandbox_id = %id,
                    target_state = %target,
                    "ignored generic sandbox state update because sandbox is quarantined"
                ),
                StateWriteGuard::CleanupOwned => tracing::warn!(
                    sandbox_id = %id,
                    target_state = %target,
                    "ignored generic sandbox state update because cleanup owns the transition"
                ),
            }
            Ok(Err(SandboxStateWriteNotApplied::Guard(guard)))
        }
        // `ALL` holds the row's state, so a write that missed it is the
        // invariant error `write_state_from` returns, never this refusal.
        StateWriteOutcome::Refused {
            refusal: StateWriteRefusal::FromState,
            ..
        } => Err(rusqlite::Error::StatementChangedRows(0)),
        StateWriteOutcome::Missing => Ok(Err(SandboxStateWriteNotApplied::Missing)),
    }
}

fn write_state_from(
    conn: &Connection,
    id: &str,
    from: StateSet,
    to: SandboxState,
) -> Result<StateWriteOutcome> {
    if matches!(to, SandboxState::Quarantined | SandboxState::Failed) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    if conn.is_autocommit() {
        let tx = immediate_transaction(conn)?;
        let outcome = write_state_from_in_transaction(&tx, id, from, to)?;
        tx.commit()?;
        return Ok(outcome);
    }
    write_state_from_in_transaction(conn, id, from, to)
}

fn write_state_from_in_transaction(
    conn: &Connection,
    id: &str,
    from: StateSet,
    to: SandboxState,
) -> Result<StateWriteOutcome> {
    let Some(found) = current_state_label(conn, id)? else {
        return Ok(StateWriteOutcome::Missing);
    };
    let found = found.parse::<SandboxState>().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
    })?;
    if let Some(guard) = generic_state_write_refusal(conn, id, Some(found), to)? {
        return Ok(StateWriteOutcome::Refused {
            found,
            refusal: StateWriteRefusal::Guard(guard),
        });
    }
    let from_labels =
        serde_json::to_string(&from.iter().map(|state| state.as_str()).collect::<Vec<_>>())
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = ?1,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
             failure_reason = NULL,
             restore_origin = CASE WHEN ?1 = 'restoring' THEN restore_origin ELSE NULL END,
             restore_phase = CASE WHEN ?1 = 'restoring' THEN restore_phase ELSE NULL END,
             restore_storage_uri = CASE WHEN ?1 = 'restoring' THEN restore_storage_uri ELSE NULL END,
             transition_receipt_id = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_id
                 ELSE NULL
             END,
             transition_receipt_kind = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_kind
                 ELSE NULL
             END,
             transition_receipt_target_state = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_target_state
                 ELSE NULL
             END,
             transition_receipt_execution_generation = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_execution_generation
                 ELSE NULL
             END,
             transition_receipt_activation_attempt_id = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_activation_attempt_id
                 ELSE NULL
             END,
             transition_receipt_reason_code = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_reason_code
                 ELSE NULL
             END,
             updated_at = ?2
         WHERE id = ?3
           AND state IN (SELECT value FROM json_each(?4))",
        params![to.as_str(), now()?, id, from_labels],
    )?;
    if changed == 0 {
        // The row was read as `found` in this transaction, so only a source
        // set that excludes it can miss.
        if from.contains(found) {
            return Err(rusqlite::Error::StatementChangedRows(0));
        }
        return Ok(StateWriteOutcome::Refused {
            found,
            refusal: StateWriteRefusal::FromState,
        });
    }
    if to == SandboxState::Deleting {
        teardown_jobs::fail_active(conn, id, TeardownSuspendFailure::Deleting)?;
    }
    Ok(StateWriteOutcome::Applied { from: found })
}
