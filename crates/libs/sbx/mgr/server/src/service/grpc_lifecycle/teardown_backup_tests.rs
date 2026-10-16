use super::*;
use pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat;

use pplx_space_node_api_client::proto::{
    GetArtifactBackupLocationRequest, GetArtifactBackupLocationResponse,
};

#[derive(Clone)]
struct ReadyBackup;

impl tonic::server::NamedService for ReadyBackup {
    const NAME: &'static str = "space.node.api.v1.ClusterApi";
}

impl tonic::server::UnaryService<GetArtifactBackupLocationRequest> for ReadyBackup {
    type Response = GetArtifactBackupLocationResponse;
    type Future = std::future::Ready<Result<Response<Self::Response>, Status>>;

    fn call(&mut self, request: Request<GetArtifactBackupLocationRequest>) -> Self::Future {
        let request = request.into_inner();
        std::future::ready(Ok(Response::new(GetArtifactBackupLocationResponse {
            local_artifact_id: request.local_artifact_id,
            artifact_kind: request.artifact_kind,
            backup_attempt_id: uuid::Uuid::now_v7().to_string(),
            ..Default::default()
        })))
    }
}

impl<B> tower::Service<http::Request<B>> for ReadyBackup
where
    B: http_body::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = futures_util::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        assert!(request.uri().path().ends_with("/GetArtifactBackupLocation"));
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
            Ok(grpc.unary(ReadyBackup, request).await)
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn teardown_reuses_ready_backup_but_waits_for_active_local_upload() {
    use crate::service::tests::service_tests::insert_stranded_teardown_suspend_sandbox;
    use pplx_space_common_core::types::ExecutionGeneration;
    use std::ffi::OsStr;

    // The parent serves while the isolated child exercises environment-based
    // client wiring. A second runtime thread keeps the server making progress.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let stop = tokio_util::sync::CancellationToken::new();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ReadyBackup)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                stop.clone().cancelled_owned(),
            ),
    );
    let token = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(token.path(), "synthetic-backup-test-token").unwrap();
    let child = pplx_space_test_env::enter_test_process(&[
        ("SPACE_INTERNAL_RPC_INSECURE_DEV", Some(OsStr::new("1"))),
        (
            pplx_space_node_api_client::cluster::CLUSTER_API_ENDPOINT_ENV,
            Some(OsStr::new(&endpoint)),
        ),
        (
            pplx_space_common_svc::bearer_token::SPACE_CLUSTER_API_TOKEN_PATH_ENV,
            Some(token.path().as_os_str()),
        ),
    ]);
    if child {
        for (superseded, active_upload) in [(false, false), (true, false), (false, true)] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new();
            let generation = ExecutionGeneration::new(7).unwrap();
            let artifact_id =
                insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation);
            let row = crate::db::get_sandbox(&conn, &sandbox_id.to_string())
                .unwrap()
                .unwrap();
            let artifact = crate::db::get_local_artifact(&conn, artifact_id)
                .unwrap()
                .unwrap();
            if active_upload {
                pplx_space_common_jobs::db::insert_job(
                    conn.raw(),
                    pplx_space_common_jobs::db::InsertJob {
                        job_id: &JobId::new(),
                        kind: crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
                        payload: "{}",
                        subject_id: Some(&sandbox_id.to_string()),
                        dedup_key: Some(&artifact_id.to_string()),
                        max_attempts: 1,
                        traceparent: None,
                        priority: Default::default(),
                    },
                )
                .unwrap();
            }
            let service = SandboxManagerService::for_test(conn);
            let pending = service.ensure_teardown_suspend_backup(
                &row,
                super::TeardownSuspendBackup {
                    execution_generation: if superseded {
                        ExecutionGeneration::new(6).unwrap()
                    } else {
                        generation
                    },
                    artifact: &artifact,
                    format: SuspendSnapshotFormat::Full,
                },
            );
            if active_upload {
                // A real loopback RPC returns Ready promptly, but that must not
                // complete teardown while this locally persisted upload is active.
                tokio::time::timeout(std::time::Duration::from_millis(250), pending)
                    .await
                    .expect_err("teardown must wait for the active local upload");
                let db = service.db.lock().unwrap();
                assert!(
                    !crate::db::teardown_suspend_status(&db, &sandbox_id.to_string())
                        .unwrap()
                        .unwrap()
                        .backup_completed
                );
                continue;
            }
            let result = pending.await;
            if superseded {
                assert_eq!(result.unwrap_err().code(), tonic::Code::Aborted);
            } else {
                assert!(
                    result.is_ok(),
                    "{result:?}; lookup: {:?}",
                    service
                        .existing_ready_artifact_backup_response(&artifact)
                        .await
                );
            }
            let db = service.db.lock().unwrap();
            let status = crate::db::teardown_suspend_status(&db, &sandbox_id.to_string())
                .unwrap()
                .unwrap();
            assert!(status.backup_completed);
            assert!(
                pplx_space_common_jobs::db::list_jobs(
                    db.raw(),
                    Some(&sandbox_id.to_string()),
                    None,
                    0
                )
                .unwrap()
                .iter()
                .all(|job| job.kind != crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND)
            );
        }
    }
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn failed_teardown_defers_repeated_polls_without_holding_driver_capacity() {
    use crate::service::tests::service_tests::insert_stranded_teardown_suspend_sandbox;
    use pplx_space_common_core::types::ExecutionGeneration;
    let conn = crate::db::test_conn();
    let sandbox = SandboxId::new();
    let generation = ExecutionGeneration::new(7).unwrap();
    insert_stranded_teardown_suspend_sandbox(&conn, sandbox, generation);
    let service = SandboxManagerService::for_test(conn);
    service
        .drive_teardown_suspend(sandbox, generation, SandboxEventTrigger::System)
        .await
        .unwrap_err();
    let capacity = service.teardown_suspend_driver_sem.available_permits();
    for _ in 0..4 {
        service.spawn_teardown_suspend_driver(sandbox, generation, SandboxEventTrigger::System);
        assert!(
            service.teardown_suspend_drivers.is_empty(),
            "a failed operation must defer repeated polls"
        );
        assert_eq!(
            service.teardown_suspend_driver_sem.available_permits(),
            capacity
        );
    }
    {
        let db = service.db.lock().unwrap();
        db.execute("UPDATE jobs SET next_attempt_at=datetime('now','-1 second') WHERE subject_id=?1 AND kind='teardown_suspend'", [sandbox.to_string()]).unwrap();
    }
    service.spawn_teardown_suspend_driver(sandbox, generation, SandboxEventTrigger::System);
    assert_eq!(
        service.teardown_suspend_drivers.len(),
        1,
        "expired backoff must allow another attempt"
    );
    service.shutdown_token.cancel();
    service.task_tracker.close();
    service.task_tracker.wait().await;
}
