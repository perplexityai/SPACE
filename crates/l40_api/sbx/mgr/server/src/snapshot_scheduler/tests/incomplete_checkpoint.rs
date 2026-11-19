//! A checkpoint whose VM state capture fails after it created its snapshot
//! directory deletes that directory: with no VM state it restores nothing,
//! and with no row nothing else would remove it. A delete refused as
//! `Unavailable` is asked again a bounded number of times. One whose create
//! fails deletes nothing, since the name may belong to an existing snapshot.

use std::sync::{Arc, Mutex};

use pplx_space_node_mgr_client::proto::{
    CaptureVmStateResponse, CreateBtrfsSnapshotRequest, CreateBtrfsSnapshotResponse,
    DeleteBtrfsSnapshotRequest, DeleteBtrfsSnapshotResponse, SyncGuestResponse,
};

use super::*;

const NAME: &str = "f-manual-20261107T120000Z";

/// Checkpoint a paused sandbox against a node-manager whose create answers
/// `create`, whose capture fails and whose deletes answer `deletes` in turn,
/// then succeed. Returns the error and the names node-manager was asked to
/// delete.
async fn checkpoint_failing(create: Result<(), tonic::Status>) -> (String, Vec<String>) {
    checkpoint_failing_with_deletes(create, Vec::new()).await
}

async fn checkpoint_failing_with_deletes(
    create: Result<(), tonic::Status>,
    deletes: Vec<tonic::Status>,
) -> (String, Vec<String>) {
    let deleted = Arc::new(Mutex::new(Vec::new()));
    let deletes = Arc::new(Mutex::new(std::collections::VecDeque::from(deletes)));
    let node = crate::node_manager_transport::fake::FakeNodeManager::default()
        .unary("SyncGuest", |_: SyncGuestRequest| Ok(SyncGuestResponse {}))
        .unary(
            "CreateBtrfsSnapshot",
            move |_: CreateBtrfsSnapshotRequest| {
                create.clone().map(|()| CreateBtrfsSnapshotResponse {})
            },
        )
        .unary("CaptureVmState", |_: CaptureVmStateRequest| {
            Err::<CaptureVmStateResponse, _>(tonic::Status::internal("injected capture failure"))
        })
        .unary("DeleteBtrfsSnapshot", {
            let deleted = Arc::clone(&deleted);
            move |request: DeleteBtrfsSnapshotRequest| {
                deleted.lock().unwrap().push(request.snapshot_name);
                match deletes.lock().unwrap().pop_front() {
                    Some(status) => Err(status),
                    None => Ok(DeleteBtrfsSnapshotResponse {}),
                }
            }
        })
        .serve();
    let (mut ctx, sandbox_id) = checkpoint_context(SandboxState::Paused);
    ctx.node_manager = node.connector();

    let error = do_checkpoint(&ctx, &sandbox_id, NAME).await.unwrap_err();

    let row = crate::db::get_sandbox(&ctx.db.lock().unwrap(), &sandbox_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        row.state,
        SandboxStatus::Unowned(SandboxState::Paused),
        "{error}"
    );
    let deleted = deleted.lock().unwrap().clone();
    (error, deleted)
}

#[tokio::test]
async fn a_failed_capture_deletes_the_directory_it_created() {
    let (error, deleted) = checkpoint_failing(Ok(())).await;

    assert!(error.contains("injected capture failure"), "{error}");
    assert_eq!(deleted, vec![NAME.to_string()]);
}

#[tokio::test]
async fn a_failed_create_deletes_nothing() {
    let (error, deleted) =
        checkpoint_failing(Err(tonic::Status::already_exists("snapshot exists"))).await;

    assert!(error.contains("snapshot exists"), "{error}");
    assert!(deleted.is_empty(), "{deleted:?}");
}

/// Node-manager refuses a delete as `Unavailable` while another operation
/// holds the sandbox's mutation claim; the next ask deletes the directory.
#[tokio::test]
async fn a_delete_refused_as_unavailable_is_asked_again() {
    let (error, deleted) = checkpoint_failing_with_deletes(
        Ok(()),
        vec![tonic::Status::unavailable("mutation in progress")],
    )
    .await;

    assert_eq!(deleted, vec![NAME.to_string(), NAME.to_string()]);
    assert!(
        !error.contains("delete the incomplete checkpoint"),
        "{error}"
    );
}

/// A delete still refused after every attempt gives up and says so.
#[tokio::test]
async fn a_delete_refused_on_every_attempt_gives_up() {
    let (error, deleted) = checkpoint_failing_with_deletes(
        Ok(()),
        vec![
            tonic::Status::unavailable("mutation in progress");
            INCOMPLETE_CHECKPOINT_DELETE_ATTEMPTS
        ],
    )
    .await;

    assert_eq!(deleted.len(), INCOMPLETE_CHECKPOINT_DELETE_ATTEMPTS);
    assert!(
        error.contains("delete the incomplete checkpoint"),
        "{error}"
    );
}

/// Only `Unavailable` is asked again: another error is reported at once.
#[tokio::test]
async fn another_delete_error_is_not_asked_again() {
    let (error, deleted) =
        checkpoint_failing_with_deletes(Ok(()), vec![tonic::Status::internal("delete failed")])
            .await;

    assert_eq!(deleted, vec![NAME.to_string()]);
    assert!(
        error.contains("delete the incomplete checkpoint"),
        "{error}"
    );
}
