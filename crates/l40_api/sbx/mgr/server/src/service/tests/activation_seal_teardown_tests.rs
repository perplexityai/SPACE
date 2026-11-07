//! Teardown and durability work on a sealed node.
//!
//! The seal refuses VM activations only. Each teardown or durability RPC runs
//! twice against the same fixture, once on an unsealed node and once on a
//! sealed one, and must answer the same both times without asking for
//! activation admission. Node-manager is a fake that confirms cleanup,
//! signals, pauses and the backup's disk snapshot, and the guest's snapshot
//! preparation is stubbed. The fake's disk snapshot writes no file, so the
//! backup stops at its upload's local-file check, under a snapshot name that
//! differs between the runs; answers are compared with UUIDs redacted.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_common_core::types::SandboxId;
use pplx_space_node_mgr_client::proto::{
    CleanupSandboxRequest, CleanupSandboxResponse, CreateBtrfsSnapshotRequest,
    CreateBtrfsSnapshotResponse, PauseSandboxRequest, PauseSandboxResponse, StopProcessRequest,
    StopProcessResponse,
};
use tonic::Status;

use crate::node_manager_transport::fake::FakeNodeManager;
use crate::proto::{
    ArtifactKind, CurrentInstanceSource, DeleteRequest, EnqueueArtifactBackupRequest, KillRequest,
    StopRequest, SuspendRequest, SuspendSnapshotFormat, enqueue_artifact_backup_request::Source,
};
use crate::service::activation_support::internal_request;
use crate::service::grpc_lifecycle::{GUEST_PREPARATION, GuestPreparation};
use crate::service::start_support::REQUESTED_VM_ACTIVATIONS;
use crate::service::tests::service_tests::insert_test_sandbox;
use crate::service::{SandboxManagerService, SandboxManagerServiceOptions};
use crate::state::SandboxState;

#[derive(Clone, Copy, Debug)]
enum Teardown {
    Suspend,
    Stop,
    Delete,
    Kill,
    ArtifactBackup,
}

impl Teardown {
    const ALL: [Self; 5] = [
        Self::Suspend,
        Self::Stop,
        Self::Delete,
        Self::Kill,
        Self::ArtifactBackup,
    ];

    /// The row each call is made against.
    fn state(self) -> SandboxState {
        match self {
            Self::Suspend | Self::Stop | Self::Delete | Self::Kill => SandboxState::Started,
            Self::ArtifactBackup => SandboxState::Stopped,
        }
    }

    /// What each call answers on an unsealed node: an error's code and a
    /// substring of its message. Only the backup fails, at its upload's
    /// check for the disk snapshot's file, which the fake does not write.
    fn unsealed(self) -> Result<(), (tonic::Code, &'static str)> {
        match self {
            Self::Suspend | Self::Stop | Self::Delete | Self::Kill => Ok(()),
            Self::ArtifactBackup => Err((
                tonic::Code::FailedPrecondition,
                "cannot back up artifact: local file is missing",
            )),
        }
    }

    async fn call(self, service: &SandboxManagerService, id: &str) -> Result<(), Status> {
        let sandbox_id = id.to_owned();
        match self {
            Self::Suspend => service
                .suspend(internal_request(SuspendRequest {
                    sandbox_id,
                    blocking: false,
                    format: SuspendSnapshotFormat::Unspecified as i32,
                }))
                .await
                .map(drop),
            Self::Stop => service
                .stop(internal_request(StopRequest {
                    sandbox_id,
                    template_filter: None,
                    blocking: false,
                    immediate_kill: true,
                }))
                .await
                .map(drop),
            Self::Delete => service
                .delete(internal_request(DeleteRequest {
                    sandbox_id,
                    template_filter: None,
                    blocking: false,
                }))
                .await
                .map(drop),
            Self::Kill => service
                .kill(internal_request(KillRequest {
                    sandbox_id,
                    signal: 9,
                    blocking: false,
                }))
                .await
                .map(drop),
            Self::ArtifactBackup => service
                .enqueue_artifact_backup(internal_request(EnqueueArtifactBackupRequest {
                    sandbox_id,
                    expected_artifact_kind: ArtifactKind::Disk as i32,
                    source: Some(Source::CurrentInstance(CurrentInstanceSource {})),
                }))
                .await
                .map(drop),
        }
    }
}

fn redact_uuids(message: &str) -> String {
    let mut redacted = String::with_capacity(message.len());
    let mut rest = message;
    while let Some(next) = rest.chars().next() {
        let uuid = rest
            .get(..36)
            .filter(|candidate| uuid::Uuid::try_parse(candidate).is_ok())
            .and_then(|_| rest.get(36..));
        if let Some(after) = uuid {
            redacted.push_str("<uuid>");
            rest = after;
        } else {
            redacted.push(next);
            rest = rest.get(next.len_utf8()..).unwrap_or_default();
        }
    }
    redacted
}

/// What a call answered, and whether it asked for activation admission.
async fn run(
    teardown: Teardown,
    id: &str,
    sealed: bool,
) -> (Result<(), (tonic::Code, String)>, bool) {
    let conn = crate::db::test_conn();
    insert_test_sandbox(&conn, id, teardown.state());
    // An encrypted volume skips suspend's plaintext-COW probe of the host
    // cache directory.
    conn.execute(
        "UPDATE sandboxes SET volume_encryption = 1 WHERE id = ?1",
        [id],
    )
    .unwrap();
    let node_manager = FakeNodeManager::default()
        .unary("CleanupSandbox", |_: CleanupSandboxRequest| {
            Ok(CleanupSandboxResponse::default())
        })
        .unary("PauseSandbox", |_: PauseSandboxRequest| {
            Ok(PauseSandboxResponse {})
        })
        .unary("StopProcess", |_: StopProcessRequest| {
            Ok(StopProcessResponse::default())
        })
        .unary("CreateBtrfsSnapshot", |_: CreateBtrfsSnapshotRequest| {
            Ok(CreateBtrfsSnapshotResponse {})
        })
        .serve();
    let run_dir = tempfile::tempdir().unwrap();
    // Stop selects only a live runtime: link its `fc.pid`.
    std::fs::create_dir(run_dir.path().join(id)).unwrap();
    std::os::unix::fs::symlink(run_dir.path(), run_dir.path().join(id).join("fc.pid")).unwrap();
    let service = SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            node_manager: node_manager.connector(),
            ..SandboxManagerServiceOptions::for_test_in(run_dir.path())
        },
    );
    if sealed {
        service.activation_seal.seal(&service.db, 1).unwrap();
    }
    let requested = Arc::new(Mutex::new(Vec::new()));
    let prepared: GuestPreparation = Arc::new(|_| Box::pin(async { Ok(()) }));
    let answer = tokio::time::timeout(
        Duration::from_secs(30),
        GUEST_PREPARATION.scope(
            prepared,
            REQUESTED_VM_ACTIVATIONS.scope(Arc::clone(&requested), teardown.call(&service, id)),
        ),
    )
    .await
    .unwrap_or_else(|_| panic!("{teardown:?} waited longer than 30 s"))
    .map_err(|status| (status.code(), redact_uuids(status.message())));
    service.shutdown_token.cancel();
    service.task_tracker.close();
    tokio::time::timeout(Duration::from_secs(30), service.task_tracker.wait())
        .await
        .expect("service tasks stop within 30 seconds of shutdown");
    let requested_admission = !requested.lock().unwrap().is_empty();
    (answer, requested_admission)
}

#[tokio::test]
async fn teardown_and_durability_work_answers_the_same_on_a_sealed_node() {
    if !crate::service::tests::enter_route_test_process(&[
        ("EPPO_SDK_KEY", None),
        ("SPACE_FLAGD_ENABLED", None),
        // A backup needs the cluster-api wiring its upload registers with.
        (
            "SPACE_INTERNAL_API_ENDPOINT",
            Some(std::ffi::OsStr::new("http://127.0.0.1:9")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("activation-seal-test")),
        ),
    ]) {
        return;
    }
    let mut mismatches = Vec::new();
    for teardown in Teardown::ALL {
        // A new teardown fails to compile until it gets a match arm here,
        // and must also be added to `ALL`.
        match teardown {
            Teardown::Suspend
            | Teardown::Stop
            | Teardown::Delete
            | Teardown::Kill
            | Teardown::ArtifactBackup => {}
        }
        let id = SandboxId::new().to_string();
        let unsealed = run(teardown, &id, false).await;
        let expected = match (&unsealed.0, teardown.unsealed()) {
            (Ok(()), Ok(())) => true,
            (Err((code, message)), Err((expected_code, expected_message))) => {
                *code == expected_code && message.contains(expected_message)
            }
            _ => false,
        };
        if !expected {
            mismatches.push(format!(
                "{teardown:?}: unsealed answered {unsealed:?}, expected {:?}",
                teardown.unsealed()
            ));
        }
        let sealed = run(teardown, &id, true).await;
        let refused_by_seal = sealed.0.as_ref().is_err_and(|(_, message)| {
            message.contains(
                pplx_space_common_core::activation_seal_signals::ACTIVATIONS_SEALED_MARKER,
            )
        });
        if sealed != unsealed || sealed.1 || refused_by_seal {
            mismatches.push(format!(
                "{teardown:?}: unsealed answered {unsealed:?}, sealed answered {sealed:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
}
