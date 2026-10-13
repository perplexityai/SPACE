//! Lifecycle handlers driven against an in-process node-manager.
//!
//! The service is built from [`SandboxManagerServiceOptions::for_test`], with
//! every host dependency redirected:
//!
//! - the node-manager connector reaches a [`FakeNodeManager`];
//! - every template lookup goes to a template-manager fake whose socket is
//!   unbound, so a lookup fails to connect (`TemplateLookupError::Connect`,
//!   which create reports as `Internal` "cannot connect to template-manager");
//! - the credential-manager socket is a test-owned path nothing binds;
//! - the host-control socket, which sandbox-manager only passes through to
//!   node-manager's `Launch`, names no live host service;
//! - no runtime is ever live.

use super::service_tests::{insert_test_sandbox, runtime_fields};
use super::*;
use crate::node_manager_transport::fake::FakeNodeManager;
use crate::service::activation_support::internal_request;
use pplx_space_common_core::types::{ExecutionGeneration, LocalArtifactId};
use pplx_space_node_mgr_client::proto::{
    CleanupSandboxRequest, CleanupSandboxResponse, StopProcessRequest, StopProcessResponse,
};

/// A test service whose node-manager clients all come from `node_manager`.
pub(crate) fn service_with_node_manager(
    conn: rusqlite::Connection,
    node_manager: NodeManagerConnector,
) -> SandboxManagerService {
    SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            node_manager,
            ..SandboxManagerServiceOptions::for_test()
        },
    )
}

#[tokio::test]
async fn fatal_kill_signal_failure_still_schedules_admitted_cleanup() {
    let stops = Arc::new(Mutex::new(Vec::new()));
    let cleanups = Arc::new(Mutex::new(Vec::new()));
    let node_manager = FakeNodeManager::default()
        .unary("StopProcess", {
            let stops = Arc::clone(&stops);
            move |request: StopProcessRequest| {
                stops.lock().unwrap().push(request);
                Err::<StopProcessResponse, _>(Status::unavailable("injected StopProcess refusal"))
            }
        })
        .unary("CleanupSandbox", {
            let cleanups = Arc::clone(&cleanups);
            move |request: CleanupSandboxRequest| {
                cleanups.lock().unwrap().push(request);
                Ok(CleanupSandboxResponse::default())
            }
        })
        .serve();
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
    let service = service_with_node_manager(conn, node_manager.connector());
    service
        .update_runtime_fields(
            sandbox_id,
            runtime_fields(ExecutionGeneration::new(1).unwrap(), 7),
        )
        .unwrap();
    let execution_fence = service
        .lookup_sandbox(&sandbox_id_string)
        .unwrap()
        .node_execution_fence();
    assert!(execution_fence.is_some());

    let error = service
        .kill(internal_request(KillRequest {
            sandbox_id: sandbox_id_string.clone(),
            signal: 9,
            blocking: false,
        }))
        .await
        .expect_err("node-manager's refusal must fail fatal signal dispatch");
    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(
        error.message().contains("send fatal signal failed")
            && error.message().contains("injected StopProcess refusal"),
        "the caller must see node-manager's refusal: {}",
        error.message()
    );
    // The signal reached node-manager once, fenced to the observed execution.
    assert_eq!(
        *stops.lock().unwrap(),
        [StopProcessRequest {
            execution_fence: execution_fence.clone(),
            sandbox_id: sandbox_id_string.clone(),
            signal: 9,
            pid: None,
        }]
    );

    // The cleanup admitted before the signal still runs: node-manager stops
    // the same execution, keeping its filesystem, and the row ends stopped.
    tokio::time::timeout(Duration::from_secs(5), async {
        while service.lookup_sandbox(&sandbox_id_string).unwrap().state != SandboxState::Stopped {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("fatal signal failure must still run the admitted cleanup to completion");
    assert_eq!(
        *cleanups.lock().unwrap(),
        [CleanupSandboxRequest {
            execution_fence,
            sandbox_id: sandbox_id_string,
            force: true,
            keep_filesystem: true,
            return_after_slot_release: true,
            immediate_kill: true,
        }]
    );
    assert_eq!(node_manager.calls(), ["StopProcess", "CleanupSandbox"]);
}

#[tokio::test]
async fn expired_encrypted_capture_is_quarantined_without_a_seal_attempt() {
    use crate::service::suspend_snapshot_publication as publication;
    use pplx_space_node_mgr_client::proto::{
        ReleaseEncryptedVolumeRequest, ReleaseEncryptedVolumeResponse,
    };
    // The holder is gone: a seal attempted now would stage the ciphertext.
    let node_manager = FakeNodeManager::default()
        .unary(
            "ReleaseEncryptedVolume",
            |request: ReleaseEncryptedVolumeRequest| {
                for name in ["snapshot.luks", "cow.luks"] {
                    std::fs::write(Path::new(&request.stage_dir).join(name), b"ciphertext")
                        .unwrap();
                }
                Ok(ReleaseEncryptedVolumeResponse::default())
            },
        )
        .serve();
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &id, SandboxState::Suspending);
    conn.execute(
        "UPDATE sandboxes SET volume_encryption = 1 WHERE id = ?1",
        [&id],
    )
    .unwrap();
    let cache = tempfile::tempdir().unwrap();
    let capture = LocalArtifactId::new();
    let staging = publication::suspend_snapshot_staging_dir(&cache.path().join(&id), capture);
    publication::write_full_suspend_artifact(&staging, &id, capture, b"captured");
    let sidecar = staging.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME);
    let mut metadata: crate::snapshot_upload::SnapshotMeta =
        serde_json::from_slice(&std::fs::read(&sidecar).unwrap()).unwrap();
    metadata.encrypted = true;
    std::fs::remove_file(sidecar).unwrap();
    publication::write_encrypted_capture_record(&staging, &metadata).unwrap();
    std::fs::File::options()
        .write(true)
        .open(staging.join(publication::ENCRYPTED_CAPTURE_RECORD))
        .unwrap()
        .set_modified(
            std::time::SystemTime::now()
                - publication::ENCRYPTED_SEAL_RETRY_WINDOW
                - std::time::Duration::from_secs(60),
        )
        .unwrap();
    let sockets = tempfile::tempdir().unwrap();
    let service = service_with_node_manager(conn, node_manager.connector(), sockets.path());

    service
        .recover_deferred_suspend_publication_batch(
            cache.path(),
            vec![service.lookup_sandbox(&id).unwrap()],
            &CancellationToken::new(),
            &mut Default::default(),
            (),
        )
        .await;

    assert!(
        node_manager.calls().is_empty(),
        "an expired capture must not be sealed"
    );
    let row = service.lookup_sandbox(&id).unwrap();
    assert_eq!(row.state, SandboxState::Quarantined);
    assert_eq!(row.current_local_artifact_id, None);
    assert!(staging.exists(), "the unsealed capture must be retained");
}
