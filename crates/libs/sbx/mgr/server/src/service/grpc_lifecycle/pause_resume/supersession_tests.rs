//! The Pause RPC against an operation that moves the node-local SQLite
//! `sandboxes.state` row while pause is between its state writes.
//!
//! Pause holds only the artifact lock while it awaits guest preparation and
//! node-manager. Stop takes the cleanup fence and admits a `pausing` row, so a
//! Stop can settle `stopped` in that window (plan finding 1). Every rollback,
//! completion and quarantine write pause makes then applies only over
//! `pausing`: any other row belongs to the operation that moved it, and Pause
//! answers `Aborted` without writing.
//!
//! [`GUEST_PREPARATION`] replaces guest preparation, which would dial the
//! host's spaced-host socket, and runs each test's interleaving at the
//! preparation call that precedes the write under test. node-manager is
//! unbound or a test-served fake.

use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::db::tests::state_write_fixtures::{
    Snapshot, assert_unchanged, force_state, snapshot, stop,
};
use crate::guest_filesystem_flush::GuestFlushError;
use crate::node_manager_transport::fake::{FakeNodeManager, ServedNodeManager};
use crate::service::activation_support::internal_request;
use crate::service::tests::service_tests::insert_test_sandbox_with_runtime;
use pplx_space_node_mgr_client::proto::{
    FirecrackerVmState as WireFirecrackerVmState, GetFirecrackerVmStateRequest,
    GetFirecrackerVmStateResponse, PauseSandboxRequest, PauseSandboxResponse,
};

const WAIT: Duration = Duration::from_secs(30);

/// The pause write a case reaches after `begin pause`.
#[derive(Clone, Copy, Debug)]
enum Exit {
    /// Guest preparation fails, and the RPC rolls back to `started`.
    PreparationFails,
    /// node-manager is unreachable, and the RPC rolls back, best effort.
    NodeUnreachable,
    /// node-manager confirms the pause, and the RPC completes `paused`.
    NodeConfirms,
    /// The node pause fails with the VM running and the retry's guest
    /// preparation fails: `pause_with_reconciliation` rolls back, best effort.
    RetryPreparationFails,
    /// Every node pause attempt fails with the VM running:
    /// `pause_with_reconciliation` rolls back, best effort.
    VmStaysRunning,
    /// Every node pause attempt fails and the VM state cannot be read, as
    /// when a Stop killed the VM: `pause_with_reconciliation` quarantines.
    VmUnobservable,
}

const EXITS: [Exit; 6] = [
    Exit::PreparationFails,
    Exit::NodeUnreachable,
    Exit::NodeConfirms,
    Exit::RetryPreparationFails,
    Exit::VmStaysRunning,
    Exit::VmUnobservable,
];

impl Exit {
    /// The guest preparation call after which the exit's write runs: pause's
    /// own is the first, and each reconciliation retry adds one.
    fn interleaved_preparation(self) -> usize {
        match self {
            Self::PreparationFails
            | Self::NodeUnreachable
            | Self::NodeConfirms
            | Self::VmUnobservable => 1,
            Self::RetryPreparationFails => 2,
            Self::VmStaysRunning => 3,
        }
    }

    fn preparation_fails(self) -> bool {
        match self {
            Self::PreparationFails | Self::RetryPreparationFails => true,
            Self::NodeUnreachable
            | Self::NodeConfirms
            | Self::VmStaysRunning
            | Self::VmUnobservable => false,
        }
    }

    fn node_manager(self) -> Option<ServedNodeManager> {
        match self {
            Self::PreparationFails | Self::NodeUnreachable => None,
            Self::NodeConfirms => Some(
                FakeNodeManager::default()
                    .unary("PauseSandbox", |_: PauseSandboxRequest| {
                        Ok(PauseSandboxResponse {})
                    })
                    .serve(),
            ),
            Self::RetryPreparationFails | Self::VmStaysRunning => Some(
                FakeNodeManager::default()
                    .unary("PauseSandbox", |_: PauseSandboxRequest| {
                        Err::<PauseSandboxResponse, _>(Status::unavailable("test pause failed"))
                    })
                    .unary(
                        "GetFirecrackerVmState",
                        |_: GetFirecrackerVmStateRequest| {
                            Ok(GetFirecrackerVmStateResponse {
                                state: WireFirecrackerVmState::Running as i32,
                            })
                        },
                    )
                    .serve(),
            ),
            Self::VmUnobservable => Some(
                FakeNodeManager::default()
                    .unary("PauseSandbox", |_: PauseSandboxRequest| {
                        Err::<PauseSandboxResponse, _>(Status::unavailable("test pause failed"))
                    })
                    .unary(
                        "GetFirecrackerVmState",
                        |_: GetFirecrackerVmStateRequest| {
                            Err::<GetFirecrackerVmStateResponse, _>(Status::unavailable(
                                "test VM state unavailable",
                            ))
                        },
                    )
                    .serve(),
            ),
        }
    }

    /// The status context of the exit's write.
    fn context(self) -> &'static str {
        match self {
            Self::PreparationFails => "roll back failed pause preparation",
            Self::NodeUnreachable => "roll back pause after node-manager connect failure",
            Self::NodeConfirms => "complete pause",
            Self::RetryPreparationFails | Self::VmStaysRunning | Self::VmUnobservable => {
                "reconcile unconfirmed pause"
            }
        }
    }
}

/// What happens to the row at the interleaved guest preparation.
#[derive(Clone, Copy, Debug)]
enum Interleave {
    /// Plan finding 1's Stop: cleanup admission's transaction for a
    /// non-blocking Stop, then its cleanup completion.
    Stop,
    /// The row moves to the state with no other column changed. `pausing`
    /// leaves it as pause wrote it.
    Force(SandboxState),
}

impl Interleave {
    fn apply(self, conn: &crate::db::Connection, id: &str) {
        match self {
            Self::Stop => stop(conn, id),
            Self::Force(SandboxState::Pausing) => {}
            Self::Force(state) => force_state(conn, id, state),
        }
    }
}

/// What Pause answers, and the row it leaves.
#[derive(Debug, Eq, PartialEq)]
enum Expected {
    /// `Ok`, with the row `paused`.
    Paused,
    /// `Internal` with this message prefix, with the row rolled back to
    /// `started`.
    RolledBack(&'static str),
    /// `Internal` reporting the quarantine, with the row `quarantined`.
    Quarantined,
    /// `Aborted` with this message, with every row as the interleaving left
    /// it.
    Rejected(String),
}

/// The generic guard that refuses a `started` or `paused` write over `found`,
/// as the generic guards' message names it, or `None` when only the from-set
/// refuses. Written out per state from #372's `PLAIN` grid for a row with no
/// cleanup intent or episode, not derived from the writer.
fn guard_refusal(found: SandboxState) -> Option<&'static str> {
    match found {
        SandboxState::Stopping
        | SandboxState::Deleting
        | SandboxState::CleanupRequired
        | SandboxState::CleanupFailed => Some("superseded_by_cleanup_episode"),
        SandboxState::Quarantined => Some("blocked_by_quarantine"),
        SandboxState::Creating
        | SandboxState::Created
        | SandboxState::Starting
        | SandboxState::Started
        | SandboxState::Stopped
        | SandboxState::Pausing
        | SandboxState::Paused
        | SandboxState::Resuming
        | SandboxState::Checkpointing
        | SandboxState::Suspending
        | SandboxState::Suspended
        | SandboxState::Restoring
        | SandboxState::Failed => None,
    }
}

fn superseded(context: &str, found: SandboxState) -> Expected {
    Expected::Rejected(format!(
        "{context}: sandbox state write was superseded: sandbox is {found}"
    ))
}

/// Pause's answer when its write finds the row in `found`: only `pausing` is
/// pause's own row.
fn expected(exit: Exit, found: SandboxState) -> Expected {
    if found == SandboxState::Pausing {
        return match exit {
            Exit::PreparationFails => {
                Expected::RolledBack("guest snapshot preparation before pause failed: ")
            }
            Exit::NodeUnreachable => Expected::RolledBack("cannot connect to node-manager: "),
            Exit::NodeConfirms => Expected::Paused,
            Exit::RetryPreparationFails | Exit::VmStaysRunning => {
                Expected::RolledBack("pause failed; sandbox reconciled to started")
            }
            Exit::VmUnobservable => Expected::Quarantined,
        };
    }
    match exit {
        // The quarantine's compare-and-set reports only that it refused;
        // pause re-reads the row and reports the state it found, including a
        // quarantine another writer made.
        Exit::VmUnobservable => match found {
            SandboxState::Creating
            | SandboxState::Created
            | SandboxState::Starting
            | SandboxState::Started
            | SandboxState::Stopping
            | SandboxState::Stopped
            | SandboxState::Deleting
            | SandboxState::Pausing
            | SandboxState::Paused
            | SandboxState::Resuming
            | SandboxState::Checkpointing
            | SandboxState::Suspending
            | SandboxState::Suspended
            | SandboxState::Restoring
            | SandboxState::CleanupRequired
            | SandboxState::CleanupFailed
            | SandboxState::Quarantined
            | SandboxState::Failed => superseded(exit.context(), found),
        },
        Exit::PreparationFails
        | Exit::NodeUnreachable
        | Exit::NodeConfirms
        | Exit::RetryPreparationFails
        | Exit::VmStaysRunning => match guard_refusal(found) {
            Some(outcome) => Expected::Rejected(format!(
                "{}: sandbox state write was not applied: {outcome}",
                exit.context()
            )),
            None => superseded(exit.context(), found),
        },
    }
}

/// Pause a `started` sandbox with a live runtime, running `interleave` at the
/// exit's guest preparation, and check the answer and the database.
async fn check_pause(exit: Exit, interleave: Interleave, found: SandboxState) {
    let case = format!("{exit:?}, {interleave:?}");
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    insert_test_sandbox_with_runtime(&conn, &id, SandboxState::Started);
    let scratch = tempfile::tempdir().unwrap();
    let node_manager = exit.node_manager();
    let mut options = SandboxManagerServiceOptions::for_test_in(scratch.path());
    if let Some(node_manager) = &node_manager {
        options.node_manager = node_manager.connector();
    }
    let service = SandboxManagerService::new_with_options(conn, options);

    let calls = Arc::new(AtomicUsize::new(0));
    let interleaved = Arc::new(Mutex::new(None::<Snapshot>));
    let prepare: GuestPreparation = {
        let (calls, interleaved) = (Arc::clone(&calls), Arc::clone(&interleaved));
        let db = service.db.clone();
        Arc::new(move |sandbox_id| {
            let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
            let last = call == exit.interleaved_preparation();
            if last {
                let db = db.lock().unwrap();
                interleave.apply(&db, &sandbox_id);
                *interleaved.lock().unwrap() = Some(snapshot(&db));
            }
            let fails = last && exit.preparation_fails();
            Box::pin(async move {
                if fails {
                    Err(GuestFlushError::Guest(Status::unavailable(
                        "test guest preparation failed",
                    )))
                } else {
                    Ok(())
                }
            })
        })
    };

    let result = tokio::time::timeout(
        WAIT,
        GUEST_PREPARATION.scope(
            prepare,
            service.pause(internal_request(
                pplx_space_sbx_mgr_client::proto::PauseRequest {
                    sandbox_id: id.clone(),
                },
            )),
        ),
    )
    .await
    .unwrap_or_else(|_| panic!("{case}: Pause waited longer than {WAIT:?}"));

    assert_eq!(
        calls.load(Ordering::SeqCst),
        exit.interleaved_preparation(),
        "{case}: guest preparation calls"
    );
    let interleaved = interleaved
        .lock()
        .unwrap()
        .take()
        .unwrap_or_else(|| panic!("{case}: the interleaving never ran"));
    assert_outcome(
        &case,
        expected(exit, found),
        result,
        &service.db_conn().unwrap(),
        &id,
        &interleaved,
    );
    service.shutdown_token.cancel();
    service.task_tracker.close();
    tokio::time::timeout(WAIT, service.task_tracker.wait())
        .await
        .unwrap_or_else(|_| panic!("{case}: service tasks outlived shutdown"));
}

/// Check Pause's answer and the database against `expected`.
fn assert_outcome(
    case: &str,
    expected: Expected,
    result: Result<Response<PauseResponse>, Status>,
    db: &crate::db::Connection,
    id: &str,
    interleaved: &Snapshot,
) {
    let state = crate::db::get_sandbox(db, id).unwrap().unwrap().state;
    match expected {
        Expected::Paused => {
            assert_eq!(state, SandboxState::Paused, "{case}");
            assert!(result.is_ok(), "{case}: {result:?}");
        }
        Expected::RolledBack(prefix) => {
            assert_eq!(state, SandboxState::Started, "{case}");
            let status = result.expect_err(case);
            assert_eq!(status.code(), tonic::Code::Internal, "{case}: {status:?}");
            assert!(status.message().starts_with(prefix), "{case}: {status:?}");
        }
        Expected::Quarantined => {
            assert_eq!(state, SandboxState::Quarantined, "{case}");
            let status = result.expect_err(case);
            assert_eq!(status.code(), tonic::Code::Internal, "{case}: {status:?}");
            assert_eq!(
                status.message(),
                "pause failed; sandbox reconciled to quarantined",
                "{case}"
            );
        }
        Expected::Rejected(message) => {
            assert_unchanged(case, interleaved, db);
            let status = result.expect_err(case);
            assert_eq!(status.code(), tonic::Code::Aborted, "{case}: {status:?}");
            assert_eq!(status.message(), message, "{case}");
        }
    }
}

/// Plan finding 1: a Stop that settles `stopped` while pause awaits guest
/// preparation or node-manager keeps its row. Before pause wrote through its
/// edges, every exit overwrote it with `started` or `paused`, leaving a row
/// with no VM.
#[tokio::test]
async fn pause_leaves_a_row_a_concurrent_stop_settled() {
    for exit in EXITS {
        check_pause(exit, Interleave::Stop, SandboxState::Stopped).await;
    }
}

/// Every state the row can be in at each of pause's writes. Only `pausing`
/// is written; it is the accepting control for each exit.
#[tokio::test]
async fn pause_writes_only_over_its_own_pausing_row() {
    for exit in EXITS {
        for state in SandboxState::all() {
            check_pause(exit, Interleave::Force(state), state).await;
        }
    }
}
