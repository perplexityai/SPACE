//! Suspend's auto-pause against an operation that moves the node-local SQLite
//! `sandboxes.state` row between suspend's `started` read and its writes.
//!
//! The auto-pause holds the cleanup fence and the wake and artifact locks, so
//! only a writer that takes none of them can move the row. Warm-snapshot
//! Start takes none, but restages only a `created` or `stopped` row as
//! `suspended`, so no production writer is known to reach these writes; the
//! test moves the row itself. Each auto-pause write applies only over the
//! state the auto-pause left: any other row belongs to the operation that
//! moved it.
//!
//! [`GUEST_PREPARATION`] replaces guest preparation, which would dial the
//! host's spaced-host socket. node-manager is a test-served fake.

use std::sync::atomic::{AtomicUsize, Ordering};

use super::super::pause_resume::{GUEST_PREPARATION, GuestPreparation};
use super::*;
use crate::db::tests::state_write_fixtures::{Snapshot, assert_unchanged, force_state, snapshot};
use crate::guest_filesystem_flush::GuestFlushError;
use crate::node_manager_transport::fake::FakeNodeManager;
use crate::node_manager_transport::{
    ConnectError, NodeManagerConnector, NodeManagerGrpcClient, NodeManagerTransport,
};
use crate::service::SandboxManagerServiceOptions;
use crate::service::activation_support::internal_request;
use pplx_space_node_mgr_client::proto::{PauseSandboxRequest, PauseSandboxResponse};
use pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat;

const WAIT: Duration = Duration::from_secs(30);

/// Rejects the `suspending` write that begins the suspend, so no case reaches
/// snapshot work: the completion control stops there once it has written
/// `paused`, and any other case that got that far fails on it.
const TRIPWIRE: &str = "suspend reached begin_suspend";

/// The auto-pause write a case interleaves with.
#[derive(Clone, Copy, Debug)]
enum Exit {
    /// The row moves while suspend connects to node-manager, between its
    /// `started` read and its `pausing` write. Guest preparation fails.
    Begin,
    /// The row moves during guest preparation, which then fails: the
    /// rollback writes `started`.
    Rollback,
    /// The row moves during guest preparation, which succeeds. node-manager
    /// confirms the pause, and the completion writes `paused`.
    Complete,
}

const EXITS: [Exit; 3] = [Exit::Begin, Exit::Rollback, Exit::Complete];

/// A node-manager transport whose `connect` holds until the test releases it.
/// Suspend's auto-pause connects after it reads the row as `started` and
/// before it writes `pausing`, so a write the test makes while `connect` holds
/// lands in that gap.
struct GatedConnect {
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
    client: NodeManagerGrpcClient,
}

#[async_trait::async_trait]
impl NodeManagerTransport for GatedConnect {
    async fn connect(&self) -> Result<NodeManagerGrpcClient, ConnectError> {
        self.entered.notify_one();
        let _permit = self.release.acquire().await.unwrap();
        Ok(self.client.clone())
    }

    async fn connect_once(&self) -> Result<NodeManagerGrpcClient, ConnectError> {
        self.connect().await
    }

    fn lazy(&self) -> NodeManagerGrpcClient {
        self.client.clone()
    }
}

/// What Suspend answers, and the row it leaves.
#[derive(Debug, Eq, PartialEq)]
enum Expected {
    /// `Internal` for the failed guest preparation, with the row rolled back
    /// to `started`.
    RolledBack,
    /// `Internal` for the failed guest preparation, with every row as the
    /// interleaving left it: the rollback was refused.
    PreparationFailed,
    /// The row `paused`, and `Internal` from the tripwire on the next write.
    Paused,
    /// `Aborted` with this message, with every row as the interleaving left
    /// it.
    Rejected(String),
}

/// The generic guard that refuses a `pausing`, `started` or `paused` write
/// over `found`, as the generic guards' message names it, or `None` when
/// only the from-set refuses. Written out per state from #372's `PLAIN` grid for a
/// row with no cleanup intent or episode, not derived from the writer.
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

fn rejected(context: &str, found: SandboxState) -> Expected {
    Expected::Rejected(match guard_refusal(found) {
        Some(outcome) => format!("{context}: sandbox state write was not applied: {outcome}"),
        None => format!("{context}: sandbox state write was superseded: sandbox is {found}"),
    })
}

/// Suspend's answer when the exit's write finds the row in `found`. Only the
/// state the auto-pause expects there is written: `started` at the begin, and
/// its own `pausing` after it.
fn expected(exit: Exit, found: SandboxState) -> Expected {
    match exit {
        Exit::Begin if found == SandboxState::Started => Expected::RolledBack,
        Exit::Begin => rejected("begin checkpoint pause", found),
        Exit::Rollback if found == SandboxState::Pausing => Expected::RolledBack,
        Exit::Rollback => Expected::PreparationFailed,
        Exit::Complete if found == SandboxState::Pausing => Expected::Paused,
        Exit::Complete => rejected("complete checkpoint pause", found),
    }
}

/// Move the row to `state` with no other column changed, and install the
/// tripwire. The state the auto-pause left at the exit is left as it is.
fn interleave(conn: &crate::db::Connection, id: &str, exit: Exit, state: SandboxState) -> Snapshot {
    let left = match exit {
        Exit::Begin => SandboxState::Started,
        Exit::Rollback | Exit::Complete => SandboxState::Pausing,
    };
    if state != left {
        force_state(conn, id, state);
    }
    conn.execute_batch(&format!(
        "CREATE TEMP TRIGGER begin_suspend_tripwire
         BEFORE UPDATE OF state ON sandboxes WHEN NEW.state = 'suspending'
         BEGIN SELECT RAISE(ABORT, '{TRIPWIRE}'); END"
    ))
    .unwrap();
    snapshot(conn)
}

/// Suspend a `started` sandbox, moving its row to `state` at the exit's
/// interleaving, and check the answer and the database.
async fn check_suspend(exit: Exit, state: SandboxState) {
    let case = format!("{exit:?}, {state:?}");
    let scratch = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Started);
    // An encrypted volume skips suspend's plaintext-COW probe of the host
    // cache directory.
    conn.execute(
        "UPDATE sandboxes SET volume_encryption = 1 WHERE id = ?1",
        [&id],
    )
    .unwrap();
    let node_manager = FakeNodeManager::default()
        .unary("PauseSandbox", |_: PauseSandboxRequest| {
            Ok(PauseSandboxResponse {})
        })
        .serve();
    let gate = Arc::new(GatedConnect {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
        client: node_manager.connector().lazy(),
    });
    let service = SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            node_manager: NodeManagerConnector::new(gate.clone()),
            ..SandboxManagerServiceOptions::for_test_in(scratch.path())
        },
    );

    let calls = Arc::new(AtomicUsize::new(0));
    let interleaved = Arc::new(Mutex::new(None::<Snapshot>));
    let prepare: GuestPreparation = {
        let (calls, interleaved) = (Arc::clone(&calls), Arc::clone(&interleaved));
        let db = service.db.clone();
        Arc::new(move |sandbox_id| {
            calls.fetch_add(1, Ordering::SeqCst);
            if !matches!(exit, Exit::Begin) {
                let db = db.lock().unwrap();
                *interleaved.lock().unwrap() = Some(interleave(&db, &sandbox_id, exit, state));
            }
            Box::pin(async move {
                match exit {
                    Exit::Begin | Exit::Rollback => Err(GuestFlushError::Guest(
                        Status::unavailable("test guest preparation failed"),
                    )),
                    Exit::Complete => Ok(()),
                }
            })
        })
    };

    let rpc_service = service.clone();
    let rpc_id = id.clone();
    let rpc = tokio::spawn(GUEST_PREPARATION.scope(prepare, async move {
        rpc_service
            .suspend(internal_request(SuspendRequest {
                sandbox_id: rpc_id,
                blocking: true,
                format: SuspendSnapshotFormat::Unspecified as i32,
            }))
            .await
    }));
    tokio::time::timeout(WAIT, gate.entered.notified())
        .await
        .unwrap_or_else(|_| panic!("{case}: suspend never connected to node-manager"));
    if let Exit::Begin = exit {
        let db = service.db.lock().unwrap();
        *interleaved.lock().unwrap() = Some(interleave(&db, &id, exit, state));
    }
    gate.release.add_permits(1);
    let result = tokio::time::timeout(WAIT, rpc)
        .await
        .unwrap_or_else(|_| panic!("{case}: Suspend waited longer than {WAIT:?}"))
        .unwrap();

    let interleaved = interleaved
        .lock()
        .unwrap()
        .take()
        .unwrap_or_else(|| panic!("{case}: the interleaving never ran"));
    let expected = expected(exit, state);
    let preparations = match (exit, &expected) {
        (Exit::Begin, Expected::Rejected(_)) => 0,
        _ => 1,
    };
    assert_eq!(
        calls.load(Ordering::SeqCst),
        preparations,
        "{case}: guest preparation calls"
    );
    let node_calls: &[&str] = match exit {
        Exit::Begin | Exit::Rollback => &[],
        Exit::Complete => &["PauseSandbox"],
    };
    assert_eq!(
        node_manager.calls(),
        node_calls,
        "{case}: node-manager calls"
    );
    {
        let db = service.db_conn().unwrap();
        let row = crate::db::get_sandbox(&db, &id).unwrap().unwrap();
        let status = result.expect_err(&case);
        match expected {
            Expected::RolledBack => {
                assert_eq!(row.state, SandboxState::Started, "{case}");
                assert_eq!(status.code(), tonic::Code::Internal, "{case}: {status:?}");
                assert!(
                    status
                        .message()
                        .starts_with("guest snapshot preparation before suspend failed: "),
                    "{case}: {status:?}"
                );
            }
            Expected::PreparationFailed => {
                assert_unchanged(&case, &interleaved, &db);
                assert_eq!(status.code(), tonic::Code::Internal, "{case}: {status:?}");
                assert!(
                    status
                        .message()
                        .starts_with("guest snapshot preparation before suspend failed: "),
                    "{case}: {status:?}"
                );
            }
            Expected::Paused => {
                assert_eq!(row.state, SandboxState::Paused, "{case}");
                assert_eq!(status.code(), tonic::Code::Internal, "{case}: {status:?}");
                assert_eq!(status.message(), format!("db error: {TRIPWIRE}"), "{case}");
            }
            Expected::Rejected(message) => {
                assert_unchanged(&case, &interleaved, &db);
                assert_eq!(status.code(), tonic::Code::Aborted, "{case}: {status:?}");
                assert_eq!(status.message(), message, "{case}");
            }
        }
    }
    assert!(
        matches!(
            service.try_cleanup_fence(&id),
            crate::service::cleanup_support::CleanupLockTryResult::Acquired(_)
        ),
        "{case}: suspend releases the cleanup fence"
    );
    service.shutdown_token.cancel();
    service.task_tracker.close();
    tokio::time::timeout(WAIT, service.task_tracker.wait())
        .await
        .unwrap_or_else(|_| panic!("{case}: service tasks outlived shutdown"));
}

/// Every state the row can be in at each auto-pause write. Only the state
/// the auto-pause expects there is written: `started` at the begin and
/// `pausing` at the rollback and the completion are the accepting controls.
#[tokio::test]
async fn suspend_auto_pause_writes_only_over_the_state_it_expects() {
    for exit in EXITS {
        for state in SandboxState::all() {
            check_suspend(exit, state).await;
        }
    }
}
