//! The persisted-edge table of the node-local SQLite `sandboxes.state` against
//! `SandboxState::can_transition_to`, the RPC-admission machine.
//!
//! Every pair on which the two disagree is listed in [`ALLOWLIST`] with a
//! reason, so a new writer edge, a changed from-set, or a changed state
//! machine fails here until the disagreement is recorded.

use std::collections::BTreeMap;

use crate::db::{EDGES, EdgeKind};
use crate::state::SandboxState::{self, *};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Disagreement {
    /// A request edge writes the pair; `can_transition_to` forbids it.
    RequestForbidden,
    /// Only completion, compensation or recovery edges write the pair;
    /// `can_transition_to` forbids it.
    WriteForbidden,
    /// `can_transition_to` allows the pair, but only completion,
    /// compensation or recovery edges write it.
    AllowedNotRequested,
    /// `can_transition_to` allows the pair; no writer implements it.
    AllowedUnwritten,
}

use Disagreement::*;

/// Stop resolves its target only while the runtime is live; admission does not
/// re-check it, and Stop does not consult `can_transition_to`.
const STOP_LIVE_RUNTIME: &str = "Stop admits a live runtime in this transient state";
const STOP_SETTLED_SOURCE: &str =
    "Stop admission, only in the runtime gate-to-fence window or with a leaked runtime";
const STOP_RUNTIME_OWNER: &str = "Stop admits a row in this state while its runtime is live";
/// `can_transition_to` lists an operation's completion; only its completion
/// writer implements it.
const COMPLETION_ONLY: &str = "an in-flight operation's completion, not an admission";
const RECOVERY_QUARANTINE: &str = "only recovery writers enter quarantine";
const NO_QUARANTINE_WRITER: &str = "no production writer quarantines a row in this state";
const CLEANUP_SETTLES_INTENT: &str = "cleanup completion settles the persisted intent's target";

#[rustfmt::skip]
const ALLOWLIST: &[(SandboxState, SandboxState, Disagreement, &str)] = &[
    (Creating,        Created,         AllowedNotRequested, COMPLETION_ONLY),
    (Creating,        Stopped,         WriteForbidden,      "import completion of a stopped export"),
    (Creating,        Deleting,        RequestForbidden,    "Delete admission supersedes the create episode"),
    (Creating,        Suspended,       WriteForbidden,      "pool clone and import completion"),
    (Creating,        CleanupRequired, WriteForbidden,      "an interrupted create is handed to cleanup"),
    (Creating,        Quarantined,     AllowedUnwritten,    NO_QUARANTINE_WRITER),
    (Created,         Stopping,        RequestForbidden,    STOP_SETTLED_SOURCE),
    (Created,         Suspended,       RequestForbidden,    "warm start restages a created row as suspended"),
    (Created,         CleanupRequired, WriteForbidden,      "create's cleanup guard fires after its created write"),
    (Created,         Quarantined,     AllowedUnwritten,    NO_QUARANTINE_WRITER),
    (Starting,        Created,         WriteForbidden,      "cold-start rollback and startup recovery restore the entry state"),
    (Starting,        Started,         AllowedNotRequested, COMPLETION_ONLY),
    (Starting,        Stopping,        RequestForbidden,    STOP_LIVE_RUNTIME),
    (Starting,        Stopped,         WriteForbidden,      "cold-start rollback and startup recovery restore the entry state"),
    (Starting,        Suspended,       WriteForbidden,      "cold-start rollback restores a suspended entry state"),
    (Starting,        CleanupRequired, WriteForbidden,      "an interrupted cold start is handed to cleanup"),
    (Starting,        Quarantined,     AllowedUnwritten,    NO_QUARANTINE_WRITER),
    (Started,         Suspending,      AllowedUnwritten,    "suspend pauses a started row first"),
    (Started,         Quarantined,     AllowedNotRequested, RECOVERY_QUARANTINE),
    (Stopping,        Stopped,         AllowedNotRequested, COMPLETION_ONLY),
    (Stopping,        CleanupRequired, WriteForbidden,      "an interrupted stop is handed to cleanup"),
    (Stopping,        CleanupFailed,   WriteForbidden,      "stop cleanup failed"),
    (Stopping,        Quarantined,     AllowedNotRequested, RECOVERY_QUARANTINE),
    (Stopped,         Stopping,        RequestForbidden,    STOP_SETTLED_SOURCE),
    (Stopped,         Suspended,       RequestForbidden,    "warm start and process-snapshot rollback restage a stopped row"),
    (Stopped,         Quarantined,     AllowedUnwritten,    NO_QUARANTINE_WRITER),
    (Deleting,        CleanupRequired, WriteForbidden,      "an interrupted delete is handed to cleanup"),
    (Deleting,        CleanupFailed,   WriteForbidden,      "delete cleanup failed"),
    (Deleting,        Quarantined,     AllowedUnwritten,    NO_QUARANTINE_WRITER),
    (Pausing,         Started,         WriteForbidden,      "pause rollback and startup pause recovery"),
    (Pausing,         Stopping,        RequestForbidden,    STOP_LIVE_RUNTIME),
    (Pausing,         Paused,          AllowedNotRequested, COMPLETION_ONLY),
    (Pausing,         Quarantined,     AllowedNotRequested, RECOVERY_QUARANTINE),
    (Paused,          Quarantined,     AllowedNotRequested, RECOVERY_QUARANTINE),
    (Resuming,        Started,         AllowedNotRequested, COMPLETION_ONLY),
    (Resuming,        Stopping,        RequestForbidden,    STOP_LIVE_RUNTIME),
    (Resuming,        Paused,          WriteForbidden,      "resume rollback and startup resident recovery"),
    (Resuming,        Quarantined,     AllowedNotRequested, RECOVERY_QUARANTINE),
    (Checkpointing,   Started,         AllowedNotRequested, COMPLETION_ONLY),
    (Checkpointing,   Stopping,        RequestForbidden,    STOP_LIVE_RUNTIME),
    (Checkpointing,   Paused,          AllowedNotRequested, COMPLETION_ONLY),
    (Checkpointing,   Quarantined,     AllowedNotRequested, RECOVERY_QUARANTINE),
    (Suspending,      Stopping,        RequestForbidden,    STOP_LIVE_RUNTIME),
    (Suspending,      Paused,          WriteForbidden,      "suspend rollback and startup suspend recovery"),
    (Suspending,      Suspended,       AllowedNotRequested, COMPLETION_ONLY),
    (Suspending,      CleanupRequired, WriteForbidden,      "an interrupted suspend is handed to cleanup"),
    (Suspending,      CleanupFailed,   WriteForbidden,      "suspend cleanup failed"),
    (Suspending,      Quarantined,     AllowedNotRequested, RECOVERY_QUARANTINE),
    (Suspended,       Stopping,        RequestForbidden,    STOP_SETTLED_SOURCE),
    (Suspended,       Stopped,         RequestForbidden,    "auto-lifecycle idle Stop of a suspended row bypasses every RPC"),
    (Suspended,       CleanupRequired, WriteForbidden,      "base-snapshot create's cleanup guard fires after its suspended write"),
    (Suspended,       Quarantined,     AllowedUnwritten,    NO_QUARANTINE_WRITER),
    (Restoring,       Started,         AllowedNotRequested, COMPLETION_ONLY),
    (Restoring,       Stopping,        RequestForbidden,    STOP_LIVE_RUNTIME),
    (Restoring,       Stopped,         WriteForbidden,      "restore-as-new publishes a stopped disk"),
    (Restoring,       Paused,          AllowedNotRequested, COMPLETION_ONLY),
    (Restoring,       Suspended,       WriteForbidden,      "restore rollback, cross-node publication, restore recovery"),
    (Restoring,       CleanupRequired, WriteForbidden,      "an interrupted or ambiguous restore is handed to cleanup"),
    (Restoring,       Quarantined,     AllowedUnwritten,    NO_QUARANTINE_WRITER),
    (CleanupRequired, Created,         WriteForbidden,      CLEANUP_SETTLES_INTENT),
    (CleanupRequired, Stopping,        RequestForbidden,    STOP_RUNTIME_OWNER),
    (CleanupRequired, Stopped,         WriteForbidden,      CLEANUP_SETTLES_INTENT),
    (CleanupRequired, Suspended,       WriteForbidden,      "cleanup completion settles a mark_suspended intent"),
    (CleanupRequired, CleanupFailed,   AllowedNotRequested, "only cleanup records its own failure"),
    (CleanupRequired, Quarantined,     AllowedNotRequested, RECOVERY_QUARANTINE),
    (CleanupFailed,   Created,         WriteForbidden,      CLEANUP_SETTLES_INTENT),
    (CleanupFailed,   Stopping,        RequestForbidden,    STOP_RUNTIME_OWNER),
    (CleanupFailed,   Stopped,         WriteForbidden,      CLEANUP_SETTLES_INTENT),
    (CleanupFailed,   Suspended,       WriteForbidden,      "cleanup completion settles a mark_suspended intent"),
    (CleanupFailed,   CleanupRequired, AllowedNotRequested, "only cleanup recovery re-requests cleanup"),
    (CleanupFailed,   Quarantined,     AllowedNotRequested, RECOVERY_QUARANTINE),
    (Quarantined,     Started,         WriteForbidden,      "pause-failed quarantine recovery"),
    (Quarantined,     Paused,          WriteForbidden,      "pause-failed quarantine recovery"),
    (Quarantined,     Restoring,       WriteForbidden,      "a restore adopts a quarantined synthetic orphan"),
    (Quarantined,     CleanupRequired, RequestForbidden,    "Delete of a quarantined pool clone re-arms its disposal"),
    (Failed,          Stopping,        RequestForbidden,    STOP_RUNTIME_OWNER),
];

fn position(state: SandboxState) -> usize {
    SandboxState::all()
        .into_iter()
        .position(|candidate| candidate == state)
        .unwrap()
}

fn classify(from: SandboxState, to: SandboxState) -> Option<Disagreement> {
    let kinds: Vec<EdgeKind> = EDGES
        .iter()
        .filter(|edge| edge.to() == to && edge.from().contains(from))
        .map(|edge| edge.kind())
        .collect();
    let requested = kinds.contains(&EdgeKind::Request);
    match (from.can_transition_to(to), requested, !kinds.is_empty()) {
        (true, true, _) | (false, false, false) => None,
        (true, false, true) => Some(AllowedNotRequested),
        (true, false, false) => Some(AllowedUnwritten),
        (false, true, _) => Some(RequestForbidden),
        (false, false, true) => Some(WriteForbidden),
    }
}

#[test]
fn persisted_edges_disagree_with_the_admission_machine_only_as_allowlisted() {
    let mut actual = BTreeMap::new();
    for from in SandboxState::all() {
        for to in SandboxState::all() {
            if from == to {
                continue;
            }
            if let Some(disagreement) = classify(from, to) {
                actual.insert((position(from), position(to)), (from, to, disagreement));
            }
        }
    }
    let mut listed = BTreeMap::new();
    for &(from, to, disagreement, reason) in ALLOWLIST {
        assert!(!reason.trim().is_empty(), "{from} -> {to} needs a reason");
        assert!(
            listed
                .insert((position(from), position(to)), (from, to, disagreement))
                .is_none(),
            "{from} -> {to} is listed twice"
        );
    }
    let unlisted: Vec<String> = actual
        .iter()
        .filter(|(key, entry)| listed.get(*key) != Some(*entry))
        .map(|(_, (from, to, disagreement))| format!("({from:?}, {to:?}, {disagreement:?}, \"\"),"))
        .collect();
    let stale: Vec<String> = listed
        .iter()
        .filter(|(key, entry)| actual.get(*key) != Some(*entry))
        .map(|(_, (from, to, disagreement))| format!("{from:?} -> {to:?} ({disagreement:?})"))
        .collect();
    assert!(
        unlisted.is_empty() && stale.is_empty(),
        "the persisted-edge table and can_transition_to disagree outside the allowlist.\n\
         Unlisted disagreements:\n{}\nAllowlist entries that no longer hold:\n{}",
        unlisted.join("\n"),
        stale.join("\n"),
    );
}

#[test]
fn no_edge_lists_its_own_target_as_a_source() {
    for edge in EDGES {
        assert!(
            !edge.from().contains(edge.to()),
            "{}: rewriting a row in its target state is not a transition",
            edge.name()
        );
    }
}
