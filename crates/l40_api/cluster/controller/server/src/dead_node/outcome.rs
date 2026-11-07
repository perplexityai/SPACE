//! What reconciling one sandbox row of a lost node would do: one rule for
//! every row the node holds.
//!
//! The rule reads only [`DeadNodeRow`]'s facts, which
//! `pplx_space_sandbox_store::dead_node::rows_on_node` reads with the shared
//! portability predicates, so "covered by a snapshot" here is the same
//! question the drain gates and restore selection ask.

use pplx_space_common_core::types::SandboxLifecycle;

use std::fmt;

use pplx_space_sandbox_store::StoredSandboxLifecycle;
use pplx_space_sandbox_store::dead_node::DeadNodeRow;

/// The outcome of one row. A dry run counts these; nothing applies them yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RowOutcome {
    /// `deleting`: the delete finishes; nothing on the node is needed.
    FinishDelete,
    /// `suspended` and covered by a durable snapshot: stays `suspended`,
    /// unpinned from the node, and restores elsewhere.
    StaysSuspended,
    /// `stopped` with its pinned stop artifact durable: stays `stopped`,
    /// unpinned from the node.
    StaysStopped,
    /// `quarantined` with a durable copy: keeps its lifecycle, placement
    /// cleared, no lost event.
    QuarantineKept,
    /// `quarantined` without one: keeps its lifecycle, placement cleared,
    /// with a lost event.
    QuarantineLost,
    /// Lost, with a `ready` snapshot that is kept. Such a snapshot never
    /// reverts the row: it predates the state the node held.
    LostSnapshotRetained,
    /// Lost, with no snapshot; references to the node are cleared.
    Lost,
    /// `failed` or the `deleted` tombstone: already given up, so nothing is
    /// lost; references to the node are cleared and any snapshot is kept.
    ReferencesCleared,
    /// A full upload may still finalize: decide once it has terminalized, or,
    /// from the node, once it has been quiet for the settle window.
    Wait,
    /// An active execution off the node, placed or pending elsewhere, owns
    /// the row: nothing is lost with the node.
    HeldByExecution,
}

impl RowOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FinishDelete => "finish_delete",
            Self::StaysSuspended => "stays_suspended",
            Self::StaysStopped => "stays_stopped",
            Self::QuarantineKept => "quarantine_kept",
            Self::QuarantineLost => "quarantine_lost",
            Self::LostSnapshotRetained => "lost_snapshot_retained",
            Self::Lost => "lost",
            Self::ReferencesCleared => "references_cleared",
            Self::Wait => "wait",
            Self::HeldByExecution => "held_by_execution",
        }
    }
}

impl fmt::Display for RowOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The outcome of `row` on a lost node.
///
/// - `deleting` finishes its delete, whatever else holds.
/// - A row with an active execution off the node, including one whose pod is
///   not scheduled yet, is held by that execution: `held_by_execution`, not
///   lost. A restore placed or pending elsewhere owns the row.
/// - Any other row waits while a `creating` full upload may still finalize
///   `ready` and change the answer: one from this node attempted inside the
///   settle window, or one from another node or no recorded node for the
///   row's current suspend.
/// - `suspended` covered by the portability predicate stays `suspended`;
///   otherwise it is lost, keeping any `ready` snapshot.
/// - `stopped` with its pinned stop artifact durable stays `stopped`;
///   otherwise it is lost.
/// - `quarantined` keeps its lifecycle either way, and is reported lost
///   unless the portability predicate covers it.
/// - `failed` and the `deleted` tombstone have their references cleared:
///   they were given up before, so the node takes nothing with it.
/// - Every other state is lost: those that may have run a VM (`creating`
///   through `checkpointing`) held their only current state on the node, and
///   `created` and text outside the vocabulary have nothing to recover. Any
///   `ready` snapshot is kept.
pub fn row_outcome(row: &DeadNodeRow) -> RowOutcome {
    use SandboxLifecycle as L;
    let lost = if row.ready_snapshot {
        RowOutcome::LostSnapshotRetained
    } else {
        RowOutcome::Lost
    };
    if row.lifecycle == Some(StoredSandboxLifecycle::Live(L::Deleting)) {
        return RowOutcome::FinishDelete;
    }
    if row.execution_elsewhere {
        return RowOutcome::HeldByExecution;
    }
    if row.upload_pending {
        return RowOutcome::Wait;
    }
    let lifecycle = match row.lifecycle {
        Some(StoredSandboxLifecycle::Live(lifecycle)) => lifecycle,
        Some(StoredSandboxLifecycle::Deleted) => return RowOutcome::ReferencesCleared,
        None => return lost,
    };
    match lifecycle {
        L::Suspended if row.covered_by_snapshot => RowOutcome::StaysSuspended,
        L::Stopped if row.stop_artifact_durable => RowOutcome::StaysStopped,
        L::Quarantined if row.covered_by_snapshot => RowOutcome::QuarantineKept,
        L::Quarantined => RowOutcome::QuarantineLost,
        L::Deleting => RowOutcome::FinishDelete,
        L::Failed => RowOutcome::ReferencesCleared,
        L::Suspended
        | L::Stopped
        | L::Created
        | L::Creating
        | L::Starting
        | L::Started
        | L::Restoring
        | L::Pausing
        | L::Paused
        | L::Resuming
        | L::Suspending
        | L::Stopping
        | L::Checkpointing => lost,
    }
}

#[cfg(test)]
mod tests;
