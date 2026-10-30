//! A teardown suspend whose backup the cluster refuses because it retired the
//! execution — a reaped orphaned restore pod's VM — releases the runtime and
//! discards the capture instead of leaving the pod terminating.

use std::sync::Arc;
use std::time::Duration;

use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, ExecutionGeneration, JobId, LocalArtifactId, SandboxId,
};
use pplx_space_sbx_mgr_client::proto::{
    SuspendSnapshotFormat, TeardownSuspendAction, TeardownSuspendOutcome,
};

use super::{OrgSnapshotConfigResolution, TeardownSuspendBackup};
use crate::service::SandboxManagerService;
use crate::service::activation_support::internal_request;
use crate::service::tests::service_tests::{
    insert_stranded_teardown_suspend_sandbox, prepare_teardown_suspend_request,
    service_with_runtime_probe,
};
use crate::snapshot_upload_jobs::RegistrationProbe;
use crate::state::SandboxState;
pub(super) use cluster_api_stub_tests::{
    FinalizedBackup, finalized_backups, record_finalized_backup, with_cluster_api,
};
use cluster_api_stub_tests::{
    RETIRING, RetiringClusterApi, admitted_registrations, bound_upload_payload,
};

const GENERATION: i64 = 7;

fn generation(value: i64) -> ExecutionGeneration {
    ExecutionGeneration::new(value).unwrap()
}

fn insert_upload_job(
    conn: &crate::db::Connection,
    sandbox_id: &str,
    staged: LocalArtifactId,
    payload: &str,
) -> JobId {
    let job_id = JobId::new();
    pplx_space_common_jobs::db::insert_job(
        conn.raw(),
        pplx_space_common_jobs::db::InsertJob {
            job_id: &job_id,
            kind: crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
            payload,
            subject_id: Some(sandbox_id),
            dedup_key: Some(&staged.to_string()),
            max_attempts: 1,
            traceparent: None,
            priority: pplx_space_common_jobs::JobPriority::default(),
        },
    )
    .unwrap();
    job_id
}

/// The stranded row a refused teardown backup leaves: `suspending`, its
/// capture staged and never backed up, and the upload job permanently
/// failed. Returns the service with a recording cleanup dispatcher, the error
/// the drive failed with, and the staged artifact.
struct RefusedTeardown {
    service: SandboxManagerService,
    sandbox_id: SandboxId,
    staged: LocalArtifactId,
    error: tonic::Status,
    dispatched: Dispatched,
}

async fn refused_teardown() -> RefusedTeardown {
    refused_teardown_with(|_| "{}".to_string()).await
}

/// [`refused_teardown`] whose failed upload job carries the payload `payload`
/// builds for the staged artifact.
async fn refused_teardown_with(payload: impl FnOnce(LocalArtifactId) -> String) -> RefusedTeardown {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let staged =
        insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation(GENERATION));
    let job_id = insert_upload_job(&conn, &sandbox_id_string, staged, &payload(staged));
    // The worker records a refusal naming a retired execution as the
    // artifact's own.
    crate::snapshot_upload_jobs::record_failure_scope(
        &conn,
        &job_id,
        crate::snapshot_upload::UploadFailureScope::Artifact,
    )
    .unwrap();
    pplx_space_common_jobs::db::fail_job_permanent(
        conn.raw(),
        &job_id,
        "cluster-api rejected begin_artifact_backup (FailedPrecondition): execution retired",
        None,
    )
    .unwrap();
    let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
        .unwrap()
        .unwrap();
    let (service, dispatched) = service_recording_cleanup(conn);

    let artifact = crate::db::LocalArtifactRow {
        local_artifact_id: staged,
        sandbox_id: sandbox_id_string.clone(),
        artifact_kind: ArtifactKind::Full,
        artifact_origin: ArtifactOrigin::Suspend,
        snapshot_name: None,
        local_path: format!("/var/lib/space/sandboxes/{sandbox_id_string}/suspend"),
    };
    let error = service
        .ensure_teardown_suspend_backup(
            &row,
            TeardownSuspendBackup {
                execution_generation: generation(GENERATION),
                artifact: &artifact,
                format: SuspendSnapshotFormat::Full,
                origin: super::TeardownBackupOrigin::Captured,
            },
        )
        .await
        .expect_err("a refused backup cannot make the suspend durable");
    RefusedTeardown {
        service,
        sandbox_id,
        staged,
        error,
        dispatched,
    }
}

type Dispatched = Arc<std::sync::Mutex<Vec<(String, bool)>>>;

/// A service whose cleanup dispatcher records each runtime teardown.
fn service_recording_cleanup(conn: crate::db::Connection) -> (SandboxManagerService, Dispatched) {
    let service = service_with_runtime_probe(conn, false);
    let dispatched = Dispatched::default();
    let observed = Arc::clone(&dispatched);
    *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |id, kill| {
        observed.lock().unwrap().push((id, kill));
        Box::pin(async { Ok(()) })
    }));
    (service, dispatched)
}

async fn poll_outcome(
    service: &SandboxManagerService,
    sandbox_id: SandboxId,
) -> TeardownSuspendOutcome {
    let response = service
        .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
            sandbox_id.to_string(),
            GENERATION,
            TeardownSuspendAction::BeginOrJoin,
        )))
        .await
        .unwrap()
        .into_inner();
    TeardownSuspendOutcome::try_from(response.outcome).unwrap()
}

pub(super) async fn drain(service: &SandboxManagerService) {
    service.shutdown_token.cancel();
    service.task_tracker.close();
    tokio::time::timeout(Duration::from_secs(5), service.task_tracker.wait())
        .await
        .expect("cleanup tasks must drain");
}

mod cluster_api_stub_tests;
mod registration_tests;
mod release_tests;
mod restart_tests;
mod retried_activation_tests;
mod verdict_tests;
