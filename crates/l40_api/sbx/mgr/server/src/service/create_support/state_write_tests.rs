//! Create completion's and warm-snapshot Start's node-local SQLite
//! `sandboxes.state` writes, which write only over the states they expect.
//!
//! Each writer is driven directly: past the durable insert, Create and a warm
//! Start reach these writes only after creating directories under the fixed
//! host sandbox cache. The expected outcomes are written out here, not derived
//! from the edge table.

use pplx_space_common_core::types::SandboxQuarantineReasonCode;

use super::*;
use crate::db::test_fixtures::state_write_fixtures::{
    admit_stop, assert_unchanged, backdate, force_state, insert_creating, insert_row, seed_receipt,
    set_failure_reason, snapshot,
};
use crate::db::{
    ActivationKind, CleanupCompletion, CleanupEpisodeRequestOutcome,
    CompleteRowIntentCleanupDisposition,
};

fn state(conn: &crate::db::Connection, id: &str) -> SandboxState {
    crate::db::get_sandbox(conn, id).unwrap().unwrap().state
}

type Write = fn(&crate::db::Connection, &str) -> Result<(), Status>;

/// A writer's expected answer for the row with an id in each source state.
type Outcomes = fn(&str, SandboxState) -> Expected;

const CREATE: &str = "complete sandbox create";
const BASE_SNAPSHOT_CREATE: &str = "complete restored sandbox create";
const WARM_START: &str = "prepare recovered sandbox restore";

fn superseded(context: &str, found: SandboxState) -> String {
    format!("{context}: sandbox state write was superseded: sandbox is {found}")
}

fn not_applied(context: &str, outcome: &str) -> String {
    format!("{context}: sandbox state write was not applied: {outcome}")
}

/// Create holds no lock between its insert and its completion writes, so a
/// Stop admitted in between leaves the row `stopping`. The completion write
/// must refuse it and leave the row to the Stop, whose completion then
/// settles `stopped`. A completion that overwrote `stopping` would leave the
/// row behind a failed create with a stale `mark_stopped` intent, which
/// refuses every activation. The Stop RPC cannot reach a row during create,
/// which has no runtime, so this is the admission transaction itself.
#[test]
fn create_completion_leaves_a_row_a_stop_admitted_to_the_stop() {
    struct Case {
        name: &'static str,
        /// The create's own writes before the Stop is admitted.
        before_stop: &'static [Write],
        write: Write,
        context: &'static str,
    }
    for case in [
        Case {
            name: "created completion",
            before_stop: &[],
            write: write_create_completed,
            context: CREATE,
        },
        Case {
            name: "base-snapshot completion",
            before_stop: &[write_create_completed],
            write: write_base_snapshot_create_completed,
            context: BASE_SNAPSHOT_CREATE,
        },
    ] {
        let conn = crate::db::test_conn();
        let (id, cleanup_id) = insert_creating(&conn, CleanupCompletion::DeleteRow);
        for write in case.before_stop {
            write(&conn, &id).unwrap();
        }
        admit_stop(&conn, &id);

        let before = snapshot(&conn);
        let status = (case.write)(&conn, &id).expect_err(case.name);
        assert_eq!(status.code(), tonic::Code::Aborted, "{}", case.name);
        assert_eq!(
            status.message(),
            superseded(case.context, SandboxState::Stopping),
            "{}",
            case.name
        );
        assert_unchanged(case.name, &before, &conn);

        // Create then fails through its cleanup guard, which requests the
        // create episode the Stop superseded, and changes nothing.
        assert_eq!(
            crate::db::request_cleanup_episode(
                &conn,
                &id,
                cleanup_id,
                SandboxQuarantineReasonCode::CreateInterrupted,
                status.message(),
            )
            .unwrap(),
            CleanupEpisodeRequestOutcome::Superseded,
            "{}",
            case.name
        );
        assert_unchanged(case.name, &before, &conn);

        // The Stop still owns its intent and settles the row.
        assert_eq!(
            crate::db::complete_row_intent_cleanup_for_job(
                &conn,
                &id,
                CleanupCompletion::MarkStopped,
                None,
                None,
                None,
            )
            .unwrap(),
            CompleteRowIntentCleanupDisposition::Applied,
            "{}",
            case.name
        );
        let row = crate::db::get_sandbox(&conn, &id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Stopped, "{}", case.name);
        assert_eq!(row.cleanup_completion, None, "{}", case.name);
    }
}

/// What one writer answers for a decorated plain row in a given state.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Expected {
    /// The row moves to the state.
    Writes(SandboxState),
    /// The writer succeeds and changes nothing.
    AlreadyThere,
    /// Nothing is written; the status code and the exact message.
    Refused(tonic::Code, String),
}

fn aborted(message: String) -> Expected {
    Expected::Refused(tonic::Code::Aborted, message)
}

/// Create completion: only `creating` is written. A `deleting` row's delete
/// owns it, so the cleanup guard answers. Wildcard-free, so a new lifecycle
/// state does not compile until it is placed.
fn create_completed(_: &str, state: SandboxState) -> Expected {
    match state {
        SandboxState::Creating => Expected::Writes(SandboxState::Created),
        SandboxState::Quarantined => aborted(not_applied(CREATE, "blocked_by_quarantine")),
        SandboxState::Deleting => aborted(not_applied(CREATE, "superseded_by_cleanup_episode")),
        SandboxState::Created
        | SandboxState::Starting
        | SandboxState::Started
        | SandboxState::Stopping
        | SandboxState::Stopped
        | SandboxState::Pausing
        | SandboxState::Paused
        | SandboxState::Resuming
        | SandboxState::Checkpointing
        | SandboxState::Suspending
        | SandboxState::Suspended
        | SandboxState::Restoring
        | SandboxState::CleanupRequired
        | SandboxState::CleanupFailed
        | SandboxState::Failed => aborted(superseded(CREATE, state)),
    }
}

/// Base-snapshot create completion: only `created` is written.
fn base_snapshot_create_completed(_: &str, state: SandboxState) -> Expected {
    const CONTEXT: &str = BASE_SNAPSHOT_CREATE;
    match state {
        SandboxState::Created => Expected::Writes(SandboxState::Suspended),
        SandboxState::Quarantined => aborted(not_applied(CONTEXT, "blocked_by_quarantine")),
        SandboxState::Deleting => aborted(not_applied(CONTEXT, "superseded_by_cleanup_episode")),
        SandboxState::Creating
        | SandboxState::Starting
        | SandboxState::Started
        | SandboxState::Stopping
        | SandboxState::Stopped
        | SandboxState::Pausing
        | SandboxState::Paused
        | SandboxState::Resuming
        | SandboxState::Checkpointing
        | SandboxState::Suspending
        | SandboxState::Suspended
        | SandboxState::Restoring
        | SandboxState::CleanupRequired
        | SandboxState::CleanupFailed
        | SandboxState::Failed => aborted(superseded(CONTEXT, state)),
    }
}

fn warm_start_refused(id: &str, found: SandboxState) -> Expected {
    Expected::Refused(
        tonic::Code::FailedPrecondition,
        format!(
            "cannot start sandbox {id} from a warm snapshot while local state is {found}; \
             expected suspended, created or stopped"
        ),
    )
}

/// Warm Start restaging: `created` and `stopped` are written, and `suspended`
/// needs no write. Any other state another transition moved the row to is
/// refused like the read-state check before staging refuses it: a warm Start
/// must not restage a `started` row for a checkpoint restore. A `deleting` or
/// quarantined row answers its generic guard. Start refuses a `failed` row
/// before warm staging, so that row is the writer's contract only.
fn warm_start_restaged(id: &str, state: SandboxState) -> Expected {
    match state {
        SandboxState::Created | SandboxState::Stopped => Expected::Writes(SandboxState::Suspended),
        SandboxState::Suspended => Expected::AlreadyThere,
        SandboxState::Quarantined => aborted(not_applied(WARM_START, "blocked_by_quarantine")),
        SandboxState::Deleting => aborted(not_applied(WARM_START, "superseded_by_cleanup_episode")),
        SandboxState::Creating
        | SandboxState::Starting
        | SandboxState::Started
        | SandboxState::Stopping
        | SandboxState::Pausing
        | SandboxState::Paused
        | SandboxState::Resuming
        | SandboxState::Checkpointing
        | SandboxState::Suspending
        | SandboxState::Restoring
        | SandboxState::CleanupRequired
        | SandboxState::CleanupFailed
        | SandboxState::Failed => warm_start_refused(id, state),
    }
}

fn check(case: &str, conn: &crate::db::Connection, id: &str, write: Write, expected: Expected) {
    let before = snapshot(conn);
    let result = write(conn, id);
    match expected {
        Expected::Writes(target) => {
            result.unwrap_or_else(|status| panic!("{case}: {status:?}"));
            assert_eq!(state(conn, id), target, "{case}");
        }
        Expected::AlreadyThere => {
            result.unwrap_or_else(|status| panic!("{case}: {status:?}"));
            assert_unchanged(case, &before, conn);
        }
        Expected::Refused(code, message) => {
            let status = result.expect_err(case);
            assert_eq!(status.code(), code, "{case}: {status:?}");
            assert_eq!(status.message(), message, "{case}");
            assert_unchanged(case, &before, conn);
        }
    }
}

/// Every row carries the side columns a same-state generic write would
/// change: a failure reason, a receipt for another target and backdated
/// timestamps. So a writer that rewrites a row it should leave alone, such as
/// an already-`suspended` warm-start row, is visible in the row.
#[test]
fn completion_and_warm_start_writes_answer_every_source_state() {
    let writers: [(&str, Write, Outcomes); 3] = [
        (
            "created completion",
            write_create_completed,
            create_completed,
        ),
        (
            "base-snapshot completion",
            write_base_snapshot_create_completed,
            base_snapshot_create_completed,
        ),
        ("warm start", write_warm_start_restaged, warm_start_restaged),
    ];
    for (name, write, expected) in writers {
        for from in SandboxState::all() {
            let case = format!("{name} from {from}");
            let conn = crate::db::test_conn();
            let id = insert_row(&conn, from);
            force_state(&conn, &id, from);
            set_failure_reason(&conn, &id, "an earlier attempt failed");
            seed_receipt(&conn, &id, SandboxState::Started);
            backdate(&conn, &id);
            check(&case, &conn, &id, write, expected(&id, from));
        }
    }
}

/// A row that moved while the warm Start staged its snapshot and is now owned
/// by cleanup answers the generic guard before the from-set, and keeps the
/// guard's `Aborted`: only a row outside the from-set without a guard refusal
/// is `FailedPrecondition`. A concurrent cold Start or restore claims the row
/// with an activation episode. The `stopped` and `suspended` rows owned by an
/// episode are rewound by hand; they are the writer's contract only.
#[test]
fn warm_start_write_keeps_a_guard_refusal_aborted() {
    struct Case {
        name: &'static str,
        entry: SandboxState,
        kind: ActivationKind,
        /// Where the row is put back after the activation began, if anywhere.
        rewind: Option<SandboxState>,
    }
    for case in [
        Case {
            name: "cold start claimed the row",
            entry: SandboxState::Stopped,
            kind: ActivationKind::ColdStart,
            rewind: None,
        },
        Case {
            name: "restore claimed the row",
            entry: SandboxState::Suspended,
            kind: ActivationKind::Restore,
            rewind: None,
        },
        Case {
            name: "stopped row owned by a cleanup episode",
            entry: SandboxState::Stopped,
            kind: ActivationKind::ColdStart,
            rewind: Some(SandboxState::Stopped),
        },
        Case {
            name: "suspended row owned by a cleanup episode",
            entry: SandboxState::Suspended,
            kind: ActivationKind::Restore,
            rewind: Some(SandboxState::Suspended),
        },
    ] {
        let conn = crate::db::test_conn();
        let id = insert_row(&conn, case.entry);
        assert!(
            crate::db::begin_activation(&conn, &id, case.entry, case.kind, None, None)
                .unwrap()
                .is_some(),
            "{}",
            case.name
        );
        if let Some(rewind) = case.rewind {
            force_state(&conn, &id, rewind);
        }
        check(
            case.name,
            &conn,
            &id,
            write_warm_start_restaged,
            aborted(not_applied(WARM_START, "superseded_by_cleanup_episode")),
        );
    }
}

/// What the read-state check before warm staging answers for a row in each
/// state: production warm Starts target `suspended` rows, and an operator's
/// `created` or `stopped` row is restaged. Wildcard-free, so a new lifecycle
/// state does not compile until it is placed.
fn warm_start_admitted(state: SandboxState) -> bool {
    match state {
        SandboxState::Created | SandboxState::Stopped | SandboxState::Suspended => true,
        SandboxState::Creating
        | SandboxState::Starting
        | SandboxState::Started
        | SandboxState::Stopping
        | SandboxState::Deleting
        | SandboxState::Pausing
        | SandboxState::Paused
        | SandboxState::Resuming
        | SandboxState::Checkpointing
        | SandboxState::Suspending
        | SandboxState::Restoring
        | SandboxState::CleanupRequired
        | SandboxState::CleanupFailed
        | SandboxState::Quarantined
        | SandboxState::Failed => false,
    }
}

#[test]
fn warm_start_admits_only_its_sources_and_suspended_rows() {
    for from in SandboxState::all() {
        let case = format!("warm start admission of {from}");
        let conn = crate::db::test_conn();
        let id = insert_row(&conn, from);
        force_state(&conn, &id, from);
        let sandbox = crate::db::get_sandbox(&conn, &id).unwrap().unwrap();
        let result = admit_warm_start(&sandbox);
        if warm_start_admitted(from) {
            result.unwrap_or_else(|status| panic!("{case}: {status:?}"));
        } else {
            let status = result.expect_err(&case);
            assert_eq!(
                Expected::Refused(status.code(), status.message().to_owned()),
                warm_start_refused(&id, from),
                "{case}"
            );
        }
    }
}
