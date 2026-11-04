//! Every outcome of the generic node-local SQLite `sandboxes.state` writer,
//! `update_state_from` with every state expected, over all 18×18 `(from, to)`
//! lifecycle pairs and each cleanup-ownership shape that changes its answer,
//! once with a transition receipt for the target and once with a receipt for
//! another state. With every state expected only the generic guards refuse, so
//! the grids pin those guards.
//!
//! The expected outcomes are written out below as one grid per shape, not
//! derived from `SandboxState::can_transition_to`: the grids record which
//! writes the guards admit, including edges the state machine forbids.

use crate::db::{Connection, Transaction};
use pplx_space_common_core::types::{
    SandboxCleanupEpisodeId, SandboxId, SandboxQuarantineReasonCode,
};

use super::state_write_fixtures::{
    Cell, ExpectedDiff, NULL, Snapshot, assert_diff, assert_unchanged, backdate, force_state,
    generation, insert_row, receipt_cleared, seed_receipt, set_failure_reason, snapshot,
    test_start, text,
};
use crate::db::{
    CleanupCompletion, CleanupEpisodeRequestOutcome, CleanupEpisodeSpec, StateSet, StateWriteGuard,
    StateWriteOutcome, StateWriteRefusal, begin_teardown_suspend, insert_cleanup_episode,
    mark_suspending_quarantined, request_cleanup_episode, set_cleanup_intent, test_conn,
    update_state_from_states,
};
use crate::state::SandboxState;

/// The grids' row and column order. Wildcard-free, so a new lifecycle state
/// does not compile until every grid has a row and a column for it.
pub(super) const fn index(state: SandboxState) -> usize {
    match state {
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

/// `A` applied, `Q` blocked by quarantine, `S` superseded by cleanup
/// ownership, `X` rejected before reading the row (`InvalidQuery`).
pub(super) type Grid = [&'static str; 18];

/// No cleanup intent and no cleanup episode.
pub(super) const PLAIN: Grid = [
    // to:         cre crd stg std stp spd del pau psd res chk ssg ssd rst crq crf qua fai
    /* creating */
    "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* created  */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* starting */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* started  */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* stopping */ "A   A   A   S   A   A   A   S   S   S   S   A   A   A   A   A   X   X",
    /* stopped  */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* deleting */ "S   S   S   S   S   S   A   S   S   S   S   S   S   S   S   S   X   X",
    /* pausing  */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* paused   */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* resuming */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* checkpt  */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* susping  */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* susped   */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* restorng */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* clean_rq */ "A   A   A   S   A   A   A   S   S   S   S   A   A   A   A   A   X   X",
    /* clean_fl */ "A   A   A   S   A   A   A   S   S   S   S   A   A   A   A   A   X   X",
    /* quarant  */ "Q   Q   Q   Q   Q   Q   A   Q   Q   Q   Q   Q   Q   Q   Q   Q   X   X",
    /* failed   */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
];

/// A `delete_row` row intent with no episode. The armed create episode, which
/// also owns a `delete_row` intent, deliberately answers identically: create
/// completion relies on writing through it.
const DELETE_ROW_INTENT: Grid = [
    // to:         cre crd stg std stp spd del pau psd res chk ssg ssd rst crq crf qua fai
    /* creating */
    "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* created  */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* starting */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* started  */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* stopping */ "A   A   A   S   A   A   A   S   S   S   S   A   A   A   A   A   X   X",
    /* stopped  */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* deleting */ "S   S   S   S   S   S   A   S   S   S   S   S   S   S   S   S   X   X",
    /* pausing  */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* paused   */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* resuming */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* checkpt  */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* susping  */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* susped   */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* restorng */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
    /* clean_rq */ "S   S   S   S   S   S   A   S   S   S   S   S   S   S   S   S   X   X",
    /* clean_fl */ "S   S   S   S   S   S   A   S   S   S   S   S   S   S   S   S   X   X",
    /* quarant  */ "Q   Q   Q   Q   Q   Q   A   Q   Q   Q   Q   Q   Q   Q   Q   Q   X   X",
    /* failed   */ "A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   A   X   X",
];

/// An episode that does not delete the row (an armed cold-start `mark_stopped`
/// episode) or that has been requested: cleanup owns every target, deletion
/// included, except that quarantine answers first.
const CLEANUP_OWNED: Grid = [
    // to:         cre crd stg std stp spd del pau psd res chk ssg ssd rst crq crf qua fai
    /* creating */
    "S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   X   X",
    /* created  */ "S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   X   X",
    /* starting */ "S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   X   X",
    /* started  */ "S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   X   X",
    /* stopping */ "S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   X   X",
    /* stopped  */ "S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   X   X",
    /* deleting */ "S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   X   X",
    /* pausing  */ "S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   X   X",
    /* paused   */ "S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   X   X",
    /* resuming */ "S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   X   X",
    /* checkpt  */ "S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   X   X",
    /* susping  */ "S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   X   X",
    /* susped   */ "S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   X   X",
    /* restorng */ "S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   X   X",
    /* clean_rq */ "S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   X   X",
    /* clean_fl */ "S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   X   X",
    /* quarant  */ "Q   Q   Q   Q   Q   Q   S   Q   Q   Q   Q   Q   Q   Q   Q   Q   X   X",
    /* failed   */ "S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   S   X   X",
];

/// A row the production quarantine writer moved to `quarantined`, with its
/// `sandbox_quarantine_*` episode: only deletion passes.
const QUARANTINE_EPISODE: &str =
    "Q   Q   Q   Q   Q   Q   A   Q   Q   Q   Q   Q   Q   Q   Q   Q   X   X";

/// No row at all.
const MISSING: &str = "M   M   M   M   M   M   M   M   M   M   M   M   M   M   M   M   X   X";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Expected {
    Applied,
    Quarantine,
    Superseded,
    Missing,
    Rejected,
}

pub(super) fn cell(row: &str, to: SandboxState) -> Expected {
    let cells: Vec<&str> = row.split_whitespace().collect();
    assert_eq!(cells.len(), 18, "grid row {row:?}");
    match cells[index(to)] {
        "A" => Expected::Applied,
        "Q" => Expected::Quarantine,
        "S" => Expected::Superseded,
        "M" => Expected::Missing,
        "X" => Expected::Rejected,
        other => panic!("unknown grid cell {other:?}"),
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Shape {
    Plain,
    DeleteRowIntent,
    ArmedCreateEpisode,
    ArmedActivationEpisode,
    RequestedCreateEpisode,
}

impl Shape {
    const ALL: [Self; 5] = [
        Self::Plain,
        Self::DeleteRowIntent,
        Self::ArmedCreateEpisode,
        Self::ArmedActivationEpisode,
        Self::RequestedCreateEpisode,
    ];

    fn grid(self) -> &'static Grid {
        match self {
            Self::Plain => &PLAIN,
            Self::DeleteRowIntent | Self::ArmedCreateEpisode => &DELETE_ROW_INTENT,
            Self::ArmedActivationEpisode | Self::RequestedCreateEpisode => &CLEANUP_OWNED,
        }
    }

    /// The cleanup intent and episode, installed while the row is still
    /// `suspending` so the production writers that create them accept it.
    fn install(self, conn: &Connection, id: &str) {
        let create_episode = CleanupEpisodeSpec {
            id: SandboxCleanupEpisodeId::new(),
            completion: CleanupCompletion::DeleteRow,
            keep_filesystem: false,
            entered_from_state: None,
            activation_state: SandboxState::Creating,
            execution_generation: None,
            activation_attempt_id: None,
        };
        match self {
            Self::Plain => {}
            Self::DeleteRowIntent => {
                assert!(set_cleanup_intent(conn, id, false, CleanupCompletion::DeleteRow).unwrap());
            }
            Self::ArmedCreateEpisode => insert_cleanup_episode(conn, id, create_episode).unwrap(),
            Self::ArmedActivationEpisode => insert_cleanup_episode(
                conn,
                id,
                CleanupEpisodeSpec {
                    id: SandboxCleanupEpisodeId::new(),
                    completion: CleanupCompletion::MarkStopped,
                    keep_filesystem: true,
                    entered_from_state: Some(SandboxState::Stopped),
                    activation_state: SandboxState::Starting,
                    execution_generation: None,
                    activation_attempt_id: None,
                },
            )
            .unwrap(),
            Self::RequestedCreateEpisode => {
                let cleanup_id = create_episode.id;
                insert_cleanup_episode(conn, id, create_episode).unwrap();
                assert_eq!(
                    request_cleanup_episode(
                        conn,
                        id,
                        cleanup_id,
                        SandboxQuarantineReasonCode::CreateInterrupted,
                        "create interrupted",
                    )
                    .unwrap(),
                    CleanupEpisodeRequestOutcome::Requested
                );
            }
        }
    }
}

/// Which transition receipt a seeded row carries relative to the target.
#[derive(Clone, Copy, Debug)]
pub(super) enum Receipt {
    /// A receipt for `to` itself when the table admits one, which an applied
    /// write keeps; otherwise a `started` receipt, which it clears.
    ForTarget,
    /// A receipt for another receipt-capable state, which an applied write
    /// clears.
    ForAnotherState,
}

impl Receipt {
    pub(super) const ALL: [Self; 2] = [Self::ForTarget, Self::ForAnotherState];

    pub(super) fn target(self, to: SandboxState) -> SandboxState {
        let receipt_capable = match to {
            SandboxState::Created
            | SandboxState::Started
            | SandboxState::Paused
            | SandboxState::Stopped
            | SandboxState::Suspended => true,
            SandboxState::Creating
            | SandboxState::Starting
            | SandboxState::Stopping
            | SandboxState::Deleting
            | SandboxState::Pausing
            | SandboxState::Resuming
            | SandboxState::Checkpointing
            | SandboxState::Suspending
            | SandboxState::Restoring
            | SandboxState::CleanupRequired
            | SandboxState::CleanupFailed
            | SandboxState::Quarantined
            | SandboxState::Failed => false,
        };
        match self {
            Self::ForTarget if receipt_capable => to,
            Self::ForTarget | Self::ForAnotherState if to != SandboxState::Started => {
                SandboxState::Started
            }
            Self::ForTarget | Self::ForAnotherState => SandboxState::Stopped,
        }
    }
}

/// A row in `from` carrying `shape`, an active teardown-suspend job (which only
/// a deletion fails), a failure reason, a transition receipt for `receipt`, a
/// remote restore's columns when `from` is `restoring`, and backdated
/// timestamps.
pub(super) fn seed(
    conn: &Connection,
    shape: Shape,
    from: SandboxState,
    receipt: SandboxState,
) -> String {
    let id = insert_row(conn, SandboxState::Paused);
    begin_teardown_suspend(conn, &id, generation(7)).unwrap();
    shape.install(conn, &id);
    force_state(conn, &id, from);
    set_failure_reason(conn, &id, "seeded failure");
    seed_receipt(conn, &id, receipt);
    backdate(conn, &id);
    id
}

/// The row diff an applied generic write of `to` makes to a [`seed`] row whose
/// receipt targets `receipt`. The teardown job's failure is the deletion side
/// effect; every other change is to the sandbox row.
pub(super) fn applied_diff(
    id: &str,
    from: SandboxState,
    to: SandboxState,
    receipt: SandboxState,
) -> ExpectedDiff {
    let mut cells: Vec<(&'static str, Cell)> =
        vec![("updated_at", Cell::NowText), ("failure_reason", NULL)];
    if from != to {
        cells.push(("state", text(to.as_str())));
        cells.push(("state_changed_at", Cell::NowText));
    }
    if from == SandboxState::Restoring && to != SandboxState::Restoring {
        cells.push(("restore_origin", NULL));
        cells.push(("restore_phase", NULL));
        cells.push(("restore_storage_uri", NULL));
    }
    if receipt != to {
        cells.extend(receipt_cleared(receipt));
    }
    let diff = ExpectedDiff::new().sandbox(id, cells);
    if to != SandboxState::Deleting {
        return diff;
    }
    diff.updated(
        "jobs",
        ("subject_id", id),
        [
            ("status", text("failed")),
            ("error_kind", text("permanent")),
            ("error", text("sandbox is deleting")),
            ("completed_at", Cell::Fresh),
        ],
    )
}

/// `update_state_from` with every state expected, standalone or inside a
/// caller's transaction.
#[derive(Clone, Copy, Debug)]
pub(super) struct Writer {
    pub(super) in_transaction: bool,
}

impl Writer {
    /// Write `to` and name the outcome in the grids' terms. `seeded` is the
    /// row's state, which the writer must report as found or as the applied
    /// write's origin.
    fn write(
        self,
        case: &str,
        conn: &Connection,
        id: &str,
        seeded: Option<SandboxState>,
        to: SandboxState,
    ) -> Expected {
        let (observed, expected) =
            match write_from(conn, id, StateSet::ALL, to, self.in_transaction) {
                Ok(StateWriteOutcome::Applied { from }) => (Some(from), Expected::Applied),
                Ok(StateWriteOutcome::Refused {
                    found,
                    refusal: StateWriteRefusal::Guard(StateWriteGuard::Quarantined),
                }) => (Some(found), Expected::Quarantine),
                Ok(StateWriteOutcome::Refused {
                    found,
                    refusal: StateWriteRefusal::Guard(StateWriteGuard::CleanupOwned),
                }) => (Some(found), Expected::Superseded),
                Ok(StateWriteOutcome::Refused {
                    found,
                    refusal: StateWriteRefusal::FromState,
                }) => panic!("{case}: every state is expected, yet {found} was refused"),
                Ok(StateWriteOutcome::Missing) => (None, Expected::Missing),
                Err(error) if error.is_invalid_query() => return Expected::Rejected,
                Err(error) => panic!("{case}: {error}"),
            };
        assert_eq!(observed, seeded, "{case}: the outcome names the seeded row");
        expected
    }
}

/// `update_state_from` over `from`, standalone or inside a caller's
/// transaction that commits afterwards.
pub(super) fn write_from(
    conn: &Connection,
    id: &str,
    from: StateSet,
    to: SandboxState,
    in_transaction: bool,
) -> crate::db::Result<StateWriteOutcome> {
    if !in_transaction {
        return update_state_from_states(conn, id, from, to);
    }
    let tx = Transaction::deferred(conn)?;
    let outcome = update_state_from_states(&tx, id, from, to)?;
    tx.commit()?;
    Ok(outcome)
}

/// Run one write and assert both its outcome and its row diff: an applied
/// write changes exactly `applied`, any other outcome changes nothing.
fn check(
    writer: Writer,
    case: &str,
    conn: &Connection,
    id: &str,
    (seeded, to): (Option<SandboxState>, SandboxState),
    expected: Expected,
    applied: ExpectedDiff,
) {
    let case = format!("{writer:?}, {case}");
    let before: Snapshot = snapshot(conn);
    let start = test_start();
    let outcome = writer.write(&case, conn, id, seeded, to);
    assert_eq!(outcome, expected, "{case}");
    if expected == Expected::Applied {
        assert_diff(&case, &before, &snapshot(conn), start, &applied);
    } else {
        assert_unchanged(&case, &before, conn);
    }
}

pub(super) fn sweep_lifecycle_pairs_and_cleanup_shapes(writer: Writer) {
    for (shape, receipt_kind) in Shape::ALL
        .into_iter()
        .flat_map(|shape| Receipt::ALL.map(|receipt| (shape, receipt)))
    {
        for from in SandboxState::all() {
            for to in SandboxState::all() {
                let receipt = receipt_kind.target(to);
                let case = format!("{shape:?}, {receipt} receipt: {from} -> {to}");
                let conn = test_conn();
                let id = seed(&conn, shape, from, receipt);
                let expected = cell(shape.grid()[index(from)], to);
                let applied = applied_diff(&id, from, to, receipt);
                check(
                    writer,
                    &case,
                    &conn,
                    &id,
                    (Some(from), to),
                    expected,
                    applied,
                );
            }
        }
    }
}

pub(super) fn sweep_quarantine_episode_and_missing_row(writer: Writer) {
    for to in SandboxState::all() {
        let case = format!("quarantine episode: quarantined -> {to}");
        let conn = test_conn();
        let id = insert_row(&conn, SandboxState::Paused);
        begin_teardown_suspend(&conn, &id, generation(7)).unwrap();
        assert!(mark_suspending_quarantined(&conn, &id, "suspend publication failed").unwrap());
        backdate(&conn, &id);
        // Entering quarantine already failed the teardown job, so a deletion
        // changes only the row and keeps the quarantine episode.
        let applied = ExpectedDiff::new().sandbox(
            &id,
            [
                ("state", text("deleting")),
                ("state_changed_at", Cell::NowText),
                ("updated_at", Cell::NowText),
                ("failure_reason", NULL),
            ],
        );
        check(
            writer,
            &case,
            &conn,
            &id,
            (Some(SandboxState::Quarantined), to),
            cell(QUARANTINE_EPISODE, to),
            applied,
        );

        let case = format!("missing row -> {to}");
        let conn = test_conn();
        let id = SandboxId::new().to_string();
        check(
            writer,
            &case,
            &conn,
            &id,
            (None, to),
            cell(MISSING, to),
            ExpectedDiff::new(),
        );
    }
}

#[test]
fn every_lifecycle_state_has_one_grid_position() {
    let mut seen = [false; 18];
    for state in SandboxState::all() {
        assert!(!seen[index(state)], "{state} shares a grid position");
        seen[index(state)] = true;
    }
    assert!(seen.iter().all(|seen| *seen));
}
