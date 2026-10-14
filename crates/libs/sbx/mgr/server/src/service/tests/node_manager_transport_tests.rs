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

/// A suspending encrypted row whose capture was recorded `age` ago and never
/// sealed: its staging holds only the record, built from real metadata.
fn record_encrypted_capture(
    conn: &rusqlite::Connection,
    cache: &Path,
    id: &str,
    age: std::time::Duration,
    committed: bool,
) -> (LocalArtifactId, std::path::PathBuf) {
    use crate::service::suspend_snapshot_publication as publication;
    insert_test_sandbox(conn, id, SandboxState::Suspending);
    conn.execute(
        "UPDATE sandboxes SET volume_encryption = 1 WHERE id = ?1",
        [id],
    )
    .unwrap();
    let capture = LocalArtifactId::new();
    let staging = publication::suspend_snapshot_staging_dir(&cache.join(id), capture);
    publication::write_full_suspend_artifact(&staging, id, capture, b"captured");
    let mut metadata: crate::snapshot_upload::SnapshotMeta = serde_json::from_slice(
        &std::fs::read(staging.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME)).unwrap(),
    )
    .unwrap();
    metadata.encrypted = true;
    std::fs::remove_dir_all(&staging).unwrap();
    std::fs::create_dir_all(&staging).unwrap();
    publication::prepare_encrypted_capture_record(&staging, &metadata).unwrap();
    if !committed {
        return (capture, staging);
    }
    publication::commit_encrypted_capture_record(&staging).unwrap();
    std::fs::File::options()
        .write(true)
        .open(staging.join(publication::ENCRYPTED_CAPTURE_RECORD))
        .unwrap()
        .set_modified(std::time::SystemTime::now() - age)
        .unwrap();
    (capture, staging)
}

/// A node-manager whose release refuses `refusals` times, then seals by
/// staging both ciphertext artifacts.
fn sealing_node_manager(refusals: usize) -> crate::node_manager_transport::fake::ServedNodeManager {
    use pplx_space_node_mgr_client::proto::{
        ReleaseEncryptedVolumeRequest, ReleaseEncryptedVolumeResponse,
    };
    let remaining = Arc::new(Mutex::new(refusals));
    FakeNodeManager::default()
        .unary(
            "ReleaseEncryptedVolume",
            move |request: ReleaseEncryptedVolumeRequest| {
                let mut remaining = remaining.lock().unwrap();
                if *remaining > 0 {
                    *remaining -= 1;
                    return Err(Status::internal("device or resource busy"));
                }
                for name in ["snapshot.luks", "cow.luks"] {
                    std::fs::write(Path::new(&request.stage_dir).join(name), b"ciphertext")
                        .unwrap();
                }
                Ok(ReleaseEncryptedVolumeResponse::default())
            },
        )
        .serve()
}

/// One periodic publication-recovery pass over the row, including the seal
/// task it spawns, which finishes before the next tick in these tests.
async fn recover_batch(service: &SandboxManagerService, cache: &Path, id: &str) {
    service
        .recover_deferred_suspend_publication_batch(
            cache,
            vec![service.lookup_sandbox(id).unwrap()],
            &CancellationToken::new(),
            &mut Default::default(),
            (),
        )
        .await;
    // The seal task holds the row's wake lock until it finishes.
    let wake = service.auto_lifecycle.wake_lock(id);
    tokio::time::timeout(std::time::Duration::from_secs(10), wake.lock())
        .await
        .expect("the seal task finishes");
}

#[tokio::test]
async fn unsealed_encrypted_capture_survives_restart_and_publishes_once_sealed() {
    use crate::service::suspend_snapshot_publication as publication;
    let node_manager = sealing_node_manager(1);
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    let cache = tempfile::tempdir().unwrap();
    let (capture, staging) =
        record_encrypted_capture(&conn, cache.path(), &id, std::time::Duration::ZERO, true);
    let service = service_with_node_manager(conn, node_manager.connector());

    // The live seal failure, a restart and a refused retry all keep it.
    let disposition = service.record_suspend_snapshot_publication_failure_after_staging(
        &id,
        &cache.path().join(&id),
        &staging,
        "seal/stage encrypted snapshot volume failed: device busy",
        &Default::default(),
    );
    assert_eq!(
        disposition,
        crate::service::grpc_lifecycle::SuspendSnapshotStagingDisposition::Preserve
    );
    let startup = service
        .recover_prelaunch_startup_states_in(cache.path(), &Default::default())
        .unwrap();
    assert_eq!(
        startup[0].outcome,
        crate::db::StartupRecoveryOutcome::Deferred
    );
    recover_batch(&service, cache.path(), &id).await;
    let row = service.lookup_sandbox(&id).unwrap();
    assert_eq!(row.state, SandboxState::Suspending);
    assert!(staging.join(publication::ENCRYPTED_CAPTURE_RECORD).exists());

    // Once the holder exits, the next pass seals it and the one after
    // publishes that capture.
    recover_batch(&service, cache.path(), &id).await;
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxState::Suspending
    );
    recover_batch(&service, cache.path(), &id).await;
    assert_eq!(
        node_manager.calls(),
        ["ReleaseEncryptedVolume", "ReleaseEncryptedVolume"]
    );
    let row = service.lookup_sandbox(&id).unwrap();
    assert_eq!(row.state, SandboxState::CleanupRequired);
    assert_eq!(row.current_local_artifact_id, Some(capture));
    let published = publication::canonical_suspend_snapshot_dir(&cache.path().join(&id));
    assert!(
        !published
            .join(publication::ENCRYPTED_CAPTURE_RECORD)
            .exists()
    );
    let sidecar: crate::snapshot_upload::SnapshotMeta = serde_json::from_slice(
        &std::fs::read(published.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME)).unwrap(),
    )
    .unwrap();
    assert_eq!(sidecar.local_artifact_id, Some(capture));
    assert!(sidecar.encrypted);
}

#[tokio::test]
async fn expired_encrypted_capture_is_quarantined_without_a_seal_attempt() {
    use crate::service::suspend_snapshot_publication as publication;
    // The holder is gone: a seal attempted now would succeed.
    let node_manager = sealing_node_manager(0);
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    let cache = tempfile::tempdir().unwrap();
    let (_, staging) = record_encrypted_capture(
        &conn,
        cache.path(),
        &id,
        publication::ENCRYPTED_SEAL_RETRY_WINDOW + std::time::Duration::from_secs(60),
        true,
    );
    let service = service_with_node_manager(conn, node_manager.connector());

    recover_batch(&service, cache.path(), &id).await;

    assert!(
        node_manager.calls().is_empty(),
        "an expired capture must not be sealed"
    );
    let row = service.lookup_sandbox(&id).unwrap();
    assert_eq!(row.state, SandboxState::Quarantined);
    // This reason is what makes deletion keep the only copy.
    assert_eq!(
        row.failure_reason.as_deref(),
        Some(crate::db::SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE)
    );
    assert!(staging.exists(), "the unsealed capture must be retained");
}

#[tokio::test]
async fn complete_staged_capture_publishes_past_an_older_unsealed_record() {
    use crate::service::suspend_snapshot_publication as publication;
    let node_manager = sealing_node_manager(usize::MAX);
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    let cache = tempfile::tempdir().unwrap();
    record_encrypted_capture(&conn, cache.path(), &id, std::time::Duration::ZERO, true);
    let newer = LocalArtifactId::new();
    publication::write_full_suspend_artifact(
        &publication::suspend_snapshot_staging_dir(&cache.path().join(&id), newer),
        &id,
        newer,
        b"newer",
    );
    let service = service_with_node_manager(conn, node_manager.connector());

    recover_batch(&service, cache.path(), &id).await;

    assert!(
        node_manager.calls().is_empty(),
        "a complete capture needs no seal"
    );
    let row = service.lookup_sandbox(&id).unwrap();
    assert_eq!(row.state, SandboxState::CleanupRequired);
    assert_eq!(row.current_local_artifact_id, Some(newer));
}

#[tokio::test]
async fn uncommitted_encrypted_capture_record_is_never_sealed() {
    // The capture never finished: only the record written before it exists.
    let node_manager = sealing_node_manager(0);
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    let cache = tempfile::tempdir().unwrap();
    let (_, staging) =
        record_encrypted_capture(&conn, cache.path(), &id, std::time::Duration::ZERO, false);
    let service = service_with_node_manager(conn, node_manager.connector());

    recover_batch(&service, cache.path(), &id).await;

    assert!(
        node_manager.calls().is_empty(),
        "an unfinished capture must not be sealed"
    );
    let row = service.lookup_sandbox(&id).unwrap();
    assert_eq!(row.state, SandboxState::Quarantined);
    assert_eq!(
        row.failure_reason.as_deref(),
        Some(crate::db::SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE)
    );
    assert!(staging.exists());
}
