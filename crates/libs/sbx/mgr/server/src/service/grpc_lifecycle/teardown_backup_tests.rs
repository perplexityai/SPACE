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
                    origin: super::TeardownBackupOrigin::Captured,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn teardown_retry_seals_publishes_backs_up_and_completes_the_same_capture() {
    use crate::service::tests::node_manager_transport_tests::{
        record_encrypted_capture_in, sealing_node_manager_builder,
    };
    use crate::service::tests::service_tests::insert_stranded_teardown_suspend_sandbox;
    use pplx_space_common_core::types::ExecutionGeneration;
    use pplx_space_node_mgr_client::proto::{CleanupSandboxRequest, CleanupSandboxResponse};
    use std::ffi::OsStr;

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
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let id = sandbox_id.to_string();
        let generation = ExecutionGeneration::new(7).unwrap();
        insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation);
        // The generation's capture stopped the runtime but did not seal.
        conn.execute(
            "UPDATE sandboxes SET volume_encryption = 1, current_local_artifact_id = NULL,
                 current_local_artifact_kind = NULL, current_local_artifact_origin = NULL
             WHERE id = ?1",
            [&id],
        )
        .unwrap();
        let cache = tempfile::tempdir().unwrap();
        let (capture, _) =
            record_encrypted_capture_in(cache.path(), &id, std::time::Duration::ZERO, true);
        let node_manager = sealing_node_manager_builder(1)
            .unary("CleanupSandbox", |_: CleanupSandboxRequest| {
                Ok(CleanupSandboxResponse::default())
            })
            .serve();
        let service = SandboxManagerService::new_with_options(
            conn,
            SandboxManagerServiceOptions {
                node_manager: node_manager.connector(),
                ..SandboxManagerServiceOptions::for_test()
            },
        );
        let drive = || {
            service.drive_teardown_suspend_in(
                sandbox_id,
                generation,
                SandboxEventTrigger::System,
                cache.path(),
            )
        };

        // The holder is still there: the retry keeps the capture pending.
        let error = drive().await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().state,
            SandboxState::Suspending
        );

        // The next retry seals, publishes, backs up and completes teardown.
        drive().await.unwrap();
        let row = service.lookup_sandbox(&id).unwrap();
        assert_eq!(row.state, SandboxState::Suspended);
        assert_eq!(row.current_local_artifact_id, Some(capture));
        let db = service.db.lock().unwrap();
        assert!(
            crate::db::teardown_suspend_status(&db, &id)
                .unwrap()
                .unwrap()
                .is_durable_for(generation)
        );
    }
    stop.cancel();
    server.await.unwrap().unwrap();
}

/// A gRPC handler answering one request type, for the fakes below.
struct Answer<F, Resp>(std::sync::Arc<F>, std::marker::PhantomData<fn() -> Resp>);

impl<Req, Resp, F> tonic::server::UnaryService<Req> for Answer<F, Resp>
where
    F: Fn(Req) -> Result<Resp, Status>,
{
    type Response = Resp;
    type Future = std::future::Ready<Result<Response<Resp>, Status>>;

    fn call(&mut self, request: Request<Req>) -> Self::Future {
        std::future::ready((self.0)(request.into_inner()).map(Response::new))
    }
}

async fn answer<B, Req, Resp>(
    request: http::Request<B>,
    handler: impl Fn(Req) -> Result<Resp, Status>,
) -> http::Response<tonic::body::Body>
where
    B: http_body::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
    Req: prost::Message + Default + Send + 'static,
    Resp: prost::Message + Send + 'static,
{
    tonic::server::Grpc::new(tonic_prost::ProstCodec::<Resp, Req>::default())
        .unary(
            Answer(std::sync::Arc::new(handler), std::marker::PhantomData),
            request,
        )
        .await
}

/// The sandbox whose artifact the control plane refuses to register. The
/// stub serves from the parent test process, so its refusals are keyed by
/// identities both processes know up front.
const REFUSED_SANDBOX: uuid::Uuid =
    uuid::Uuid::from_u128(0x019f_8000_0000_7000_8000_0000_0000_0b0d);

/// The sandbox whose source node the control plane doesn't know until after
/// its first [`UNREGISTERED_NODE_BEGINS`] registrations.
const RECOVERING_SANDBOX: uuid::Uuid =
    uuid::Uuid::from_u128(0x019f_8000_0000_7000_8000_0000_0000_0b0e);

/// The node-local suspend's handoff and its worker each register once.
const UNREGISTERED_NODE_BEGINS: usize = 2;

/// A control plane that admits every artifact backup but [`REFUSED_SANDBOX`]'s
/// and [`RECOVERING_SANDBOX`]'s first ones, finalizes every attempt, and has no
/// ready backup of any artifact.
#[derive(Clone, Default)]
struct BackupControlPlane {
    recovering_begins: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl tonic::server::NamedService for BackupControlPlane {
    const NAME: &'static str = "space.node.api.v1.ClusterApi";
}

impl<B> tower::Service<http::Request<B>> for BackupControlPlane
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
        use pplx_space_node_api_client::proto as api;
        let method = request
            .uri()
            .path()
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_owned();
        let recovering_begins = std::sync::Arc::clone(&self.recovering_begins);
        Box::pin(async move {
            Ok(match method.as_str() {
                "BeginArtifactBackup" => {
                    answer(request, move |request: api::BeginArtifactBackupRequest| {
                        if request.sandbox_id == REFUSED_SANDBOX.to_string() {
                            return Err(Status::invalid_argument(
                                "the artifact's metadata contradicts its identity",
                            ));
                        }
                        if request.sandbox_id == RECOVERING_SANDBOX.to_string()
                            && recovering_begins.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                                < UNREGISTERED_NODE_BEGINS
                        {
                            return Err(Status::not_found("source node is not registered"));
                        }
                        Ok(api::BeginArtifactBackupResponse {
                            local_artifact_id: request.local_artifact_id,
                            backup_attempt_id: request.backup_attempt_id,
                            fence_verdict_only: request.fence_verdict_only,
                            generation_fence_admitted: false,
                        })
                    })
                    .await
                }
                "FinalizeArtifactBackup" => {
                    answer(request, |request: api::FinalizeArtifactBackupRequest| {
                        super::retired_teardown_tests::record_finalized_backup(&request);
                        Ok(api::FinalizeArtifactBackupResponse::default())
                    })
                    .await
                }
                "TouchArtifactBackup" => {
                    answer(request, |_: api::TouchArtifactBackupRequest| {
                        Ok(api::TouchArtifactBackupResponse::default())
                    })
                    .await
                }
                "GetSandboxSnapshotStorageUri" => {
                    answer(request, |_: api::GetSandboxSnapshotStorageUriRequest| {
                        Ok(api::GetSandboxSnapshotStorageUriResponse { storage_uri: None })
                    })
                    .await
                }
                "GetArtifactBackupLocation" => {
                    answer(
                        request,
                        |request: api::GetArtifactBackupLocationRequest| match request.readiness {
                            None => Err(Status::not_found("no ready backup of the artifact")),
                            Some(_) => Ok(api::GetArtifactBackupLocationResponse {
                                ready: Some(false),
                                ..Default::default()
                            }),
                        },
                    )
                    .await
                }
                _ => Status::unimplemented(method).into_http(),
            })
        })
    }
}

/// How the fake vol-mgr's transfers stand when the worker asks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Transfers {
    /// As vol-mgr refuses new pushes while it hands off.
    Refused,
    Running,
    Completed,
    /// As vol-mgr reports a transfer whose retries ran out during an
    /// object-store outage.
    Failed,
}

/// A vol-mgr on a test-owned socket that accepts every push and reports every
/// transfer as `Transfers` says.
struct FakeVolumeManager {
    socket: std::path::PathBuf,
    transfers: std::sync::Arc<std::sync::Mutex<Transfers>>,
    stop: tokio_util::sync::CancellationToken,
    _dir: tempfile::TempDir,
}

impl FakeVolumeManager {
    fn serve(transfers: Transfers) -> Self {
        use pplx_space_volmgr_client::proto as vol;
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("volume-manager.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let transfers = std::sync::Arc::new(std::sync::Mutex::new(transfers));
        let observed = std::sync::Arc::clone(&transfers);
        let rpc = tower::service_fn(move |request: http::Request<tonic::body::Body>| {
            let method = request
                .uri()
                .path()
                .rsplit('/')
                .next()
                .unwrap_or_default()
                .to_owned();
            let transfers = *observed.lock().unwrap();
            async move {
                Ok::<_, std::convert::Infallible>(match method.as_str() {
                    "SubmitPushFile" => {
                        answer(request, move |_: vol::PushFileRequest| {
                            if transfers == Transfers::Refused {
                                return Err(Status::unavailable("volume-manager is handing off"));
                            }
                            Ok(vol::SubmitTransferResponse {
                                job_id: JobId::new().to_string(),
                            })
                        })
                        .await
                    }
                    "GetTransferJob" => {
                        answer(request, move |request: vol::GetTransferJobRequest| {
                            let (status, error) = match transfers {
                                Transfers::Refused | Transfers::Running => {
                                    (vol::TransferJobStatus::Running, None)
                                }
                                Transfers::Completed => (vol::TransferJobStatus::Completed, None),
                                Transfers::Failed => (
                                    vol::TransferJobStatus::Failed,
                                    Some("object store unreachable".to_string()),
                                ),
                            };
                            Ok(vol::GetTransferJobResponse {
                                job: Some(vol::TransferJob {
                                    job_id: request.job_id,
                                    status: status as i32,
                                    bytes_transferred: 18,
                                    error,
                                    ..Default::default()
                                }),
                            })
                        })
                        .await
                    }
                    _ => Status::unimplemented(method).into_http(),
                })
            }
        });
        let stop = tokio_util::sync::CancellationToken::new();
        tokio::spawn(
            tonic::transport::Server::builder().serve_with_incoming_shutdown(
                rpc,
                tokio_stream::wrappers::UnixListenerStream::new(listener),
                stop.clone().cancelled_owned(),
            ),
        );
        Self {
            socket,
            transfers,
            stop,
            _dir: dir,
        }
    }

    fn report(&self, transfers: Transfers) {
        *self.transfers.lock().unwrap() = transfers;
    }
}

impl Drop for FakeVolumeManager {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

/// How a node-local suspend's upload stands when the pod's teardown suspend
/// of the suspended sandbox starts, and what the teardown meets after it.
#[derive(Clone, Copy, Debug)]
enum NodeLocalUpload {
    Running,
    /// vol-mgr gave up on its transfer; the teardown's new attempt completes.
    TransferFailed,
    /// vol-mgr gave up on its transfer, and on the teardown's new attempt too.
    TransferFailedTwice,
    /// vol-mgr gave up on its transfer, and on an attempt a backup requested
    /// by artifact id started before the teardown.
    TwoFailedAttempts,
    /// The control plane didn't know the node yet when the node-local suspend
    /// registered, and does by the teardown.
    RegistrationRecovers,
    /// The control plane refused the artifact's registration.
    RegistrationRefused,
}

/// A teardown suspend bound to a sandbox whose node-local suspend handed its
/// artifact to an upload, which the real worker runs against `volume_manager`.
struct NodeLocalSuspended {
    service: SandboxManagerService,
    sandbox_id: SandboxId,
    artifact_id: LocalArtifactId,
    node_local: JobId,
    cache: tempfile::TempDir,
}

const GENERATION: i64 = 7;

/// A node-local suspend of a paused Kubernetes sandbox, up to the result of
/// handing its published capture to an upload. The real worker runs that
/// upload against `volume_manager`.
struct NodeLocalHandoff {
    service: SandboxManagerService,
    artifact_id: LocalArtifactId,
    snapshot_dir: std::path::PathBuf,
    handoff: Result<SubmitSuspendSnapshotOutcome, Status>,
}

/// Suspend `sandbox_id` up to its backup handoff. The runtime probe reads the
/// scratch run directory, where nothing runs, unless `runtime_live`.
async fn node_local_handoff(
    sandbox_id: SandboxId,
    volume_manager: &FakeVolumeManager,
    scratch: &std::path::Path,
    runtime_live: bool,
) -> NodeLocalHandoff {
    use crate::service::tests::service_tests::{insert_test_sandbox, runtime_fields};
    use pplx_space_common_core::types::ExecutionGeneration;

    let conn = crate::db::test_conn();
    let id = sandbox_id.to_string();
    insert_test_sandbox(&conn, &id, SandboxState::Paused);
    let mut fields = runtime_fields(ExecutionGeneration::new(GENERATION).unwrap(), 7);
    fields.space_api_route_identity.substrate_kind =
        Some(pplx_space_common_core::types::NodeManagedBy::Kubernetes);
    crate::db::update_runtime_fields(&conn, sandbox_id, fields).unwrap();
    assert!(crate::db::begin_suspend(&conn, &id).unwrap());
    let mut options = SandboxManagerServiceOptions {
        volume_manager_socket_path: volume_manager.socket.clone(),
        ..SandboxManagerServiceOptions::for_test_in(scratch)
    };
    if runtime_live {
        options.sandbox_runtime_live = std::sync::Arc::new(|_| true);
    }
    let service = SandboxManagerService::new_with_options(conn, options);
    let artifact_id = LocalArtifactId::new();
    let snapshot_dir = scratch.join("suspend");
    crate::service::suspend_snapshot_publication::write_full_suspend_artifact(
        &snapshot_dir,
        &id,
        artifact_id,
        b"node-local-capture",
    );

    let sandbox = service.lookup_sandbox(&id).unwrap();
    let handoff = service
        .enqueue_node_local_suspend_backup(
            &sandbox,
            artifact_id,
            snapshot_dir.to_str().unwrap(),
            SuspendSnapshotFormat::Full,
        )
        .await;
    NodeLocalHandoff {
        service,
        artifact_id,
        snapshot_dir,
        handoff,
    }
}

async fn suspended_after_node_local_upload(
    upload: NodeLocalUpload,
    volume_manager: &FakeVolumeManager,
    scratch: &std::path::Path,
) -> NodeLocalSuspended {
    use pplx_space_common_core::types::ExecutionGeneration;

    let sandbox_id = match upload {
        NodeLocalUpload::RegistrationRefused => SandboxId::from_uuid(REFUSED_SANDBOX),
        NodeLocalUpload::RegistrationRecovers => SandboxId::from_uuid(RECOVERING_SANDBOX),
        _ => SandboxId::new(),
    };
    let id = sandbox_id.to_string();
    let generation = ExecutionGeneration::new(GENERATION).unwrap();
    let NodeLocalHandoff {
        service,
        artifact_id,
        snapshot_dir,
        handoff,
    } = node_local_handoff(sandbox_id, volume_manager, scratch, false).await;
    match upload {
        // The refusal would roll a real suspend back when the handoff meets
        // it; one only the worker meets leaves the suspend published, which
        // is what the rest of this setup records.
        NodeLocalUpload::RegistrationRefused | NodeLocalUpload::RegistrationRecovers => {
            handoff.expect_err("the control plane refuses the registration");
        }
        _ => {
            handoff.expect("the handoff stages the upload");
        }
    }
    let node_local = only_upload(&service, &id).job_id;
    {
        let db = service.db.lock().unwrap();
        crate::db::complete_row_intent_cleanup_for_job(
            &db,
            &id,
            CleanupCompletion::MarkSuspended,
            Some((artifact_id, snapshot_dir.to_str().unwrap())),
            None,
            None,
        )
        .unwrap();
        crate::db::bind_suspended_teardown_operation(&db, &id, generation).unwrap();
    }
    NodeLocalSuspended {
        service,
        sandbox_id,
        artifact_id,
        node_local,
        cache: tempfile::tempdir().unwrap(),
    }
}

fn uploads(service: &SandboxManagerService, sandbox_id: &str) -> Vec<pplx_space_common_jobs::Job> {
    let db = service.db.lock().unwrap();
    pplx_space_common_jobs::db::list_jobs(db.raw(), Some(sandbox_id), None, 0)
        .unwrap()
        .into_iter()
        .filter(|job| job.kind == crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND)
        .collect()
}

fn only_upload(service: &SandboxManagerService, sandbox_id: &str) -> pplx_space_common_jobs::Job {
    let mut uploads = uploads(service, sandbox_id);
    assert_eq!(uploads.len(), 1, "the artifact has one upload: {uploads:?}");
    uploads.remove(0)
}

/// Wait for the worker to end `upload` with `status`.
async fn ended(
    service: &SandboxManagerService,
    upload: &JobId,
    status: pplx_space_common_jobs::JobStatus,
) -> pplx_space_common_jobs::Job {
    let job = service
        .snapshot_upload_worker
        .wait_for_job(upload, std::time::Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(job.status, status.as_str(), "{job:?}");
    job
}

/// A pod torn down after a node-local suspend binds its generation to the
/// published artifact. The teardown and a backup requested by artifact id
/// share the node-local suspend's upload while it runs, and a joined driver
/// accepts the poll that completed the generation first. Once that upload has
/// failed, an attempt's failure, such as a transfer vol-mgr gave up on or a
/// registration refused for a node the control plane didn't know yet, gets
/// one new attempt unless two uploads of the artifact have already failed. A
/// registration refused for the artifact itself fails the teardown at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn teardown_after_node_local_suspend_shares_its_upload_or_settles_its_failure() {
    use super::retired_teardown_tests::with_cluster_api;

    with_cluster_api(BackupControlPlane::default(), async || {
        type Case = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;
        let cases = [
            NodeLocalUpload::Running,
            NodeLocalUpload::TransferFailed,
            NodeLocalUpload::TransferFailedTwice,
            NodeLocalUpload::TwoFailedAttempts,
            NodeLocalUpload::RegistrationRecovers,
            NodeLocalUpload::RegistrationRefused,
        ]
        // Each case's future is large; keep them off the test's stack.
        .map(|upload| Box::pin(teardown_after_node_local_upload(upload)) as Case);
        futures_util::future::join_all(
            cases
                .into_iter()
                .chain([Box::pin(captured_upload_failing_twice()) as Case]),
        )
        .await;
    })
    .await;
}

/// A teardown that captured its own artifact, whose upload vol-mgr gives up
/// on twice. The first failure gets a recovery attempt, so the `suspending`
/// row isn't marked as finished failing until that attempt fails too.
async fn captured_upload_failing_twice() {
    use crate::service::tests::service_tests::insert_stranded_teardown_suspend_sandbox;
    use pplx_space_common_core::types::ExecutionGeneration;
    use pplx_space_common_jobs::JobStatus;

    let generation = ExecutionGeneration::new(GENERATION).unwrap();
    let volume_manager = FakeVolumeManager::serve(Transfers::Failed);
    let scratch = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    let staged = insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation);
    let snapshot_dir = scratch.path().join("suspend");
    crate::service::suspend_snapshot_publication::write_full_suspend_artifact(
        &snapshot_dir,
        &id,
        staged,
        b"teardown-capture",
    );
    conn.execute(
        "UPDATE local_artifacts SET local_path = ?1 WHERE local_artifact_id = ?2",
        crate::db::fixture::params![snapshot_dir.to_str().unwrap(), staged.to_string()],
    )
    .unwrap();
    let service = SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            volume_manager_socket_path: volume_manager.socket.clone(),
            ..SandboxManagerServiceOptions::for_test_in(scratch.path())
        },
    );
    let cache = tempfile::tempdir().unwrap();
    let drive = || {
        service.drive_teardown_suspend_in(
            sandbox_id,
            generation,
            SandboxEventTrigger::System,
            cache.path(),
        )
    };
    let ends = std::time::Duration::from_secs(10);
    let marker = || {
        let db = service.db.lock().unwrap();
        crate::db::get_sandbox(&db, &id)
            .unwrap()
            .unwrap()
            .failure_reason
    };

    tokio::time::timeout(ends, drive())
        .await
        .unwrap()
        .expect_err("vol-mgr gives up on the transfer");
    let first = only_upload(&service, &id);
    assert_eq!(first.status, JobStatus::Failed.as_str());
    assert_eq!(marker(), None, "the recovery attempt is still to come");

    tokio::time::timeout(ends, drive())
        .await
        .unwrap()
        .expect_err("vol-mgr gives up on the recovery attempt too");
    let attempts = uploads(&service, &id);
    assert_eq!(attempts.len(), 2, "one recovery attempt");
    assert!(
        attempts
            .iter()
            .all(|job| job.status == JobStatus::Failed.as_str())
    );
    assert_eq!(
        marker().as_deref(),
        Some(pplx_space_sbx_mgr_client::TEARDOWN_SUSPEND_BACKUP_PERMANENTLY_FAILED_REASON)
    );
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxState::Suspending
    );
    super::retired_teardown_tests::drain(&service).await;
}

async fn teardown_after_node_local_upload(upload: NodeLocalUpload) {
    use crate::proto::enqueue_artifact_backup_request::Source;
    use crate::service::activation_support::internal_request;
    use crate::service::tests::service_tests::prepare_teardown_suspend_request;
    use pplx_space_common_core::types::ExecutionGeneration;
    use pplx_space_common_jobs::JobStatus;

    let generation = ExecutionGeneration::new(GENERATION).unwrap();
    let volume_manager = FakeVolumeManager::serve(match upload {
        NodeLocalUpload::Running
        | NodeLocalUpload::RegistrationRecovers
        | NodeLocalUpload::RegistrationRefused => Transfers::Running,
        NodeLocalUpload::TransferFailed
        | NodeLocalUpload::TransferFailedTwice
        | NodeLocalUpload::TwoFailedAttempts => Transfers::Failed,
    });
    let scratch = tempfile::tempdir().unwrap();
    let NodeLocalSuspended {
        service,
        sandbox_id,
        artifact_id,
        node_local,
        cache,
    } = suspended_after_node_local_upload(upload, &volume_manager, scratch.path()).await;
    let id = sandbox_id.to_string();
    let drive = || {
        service.drive_teardown_suspend_in(
            sandbox_id,
            generation,
            SandboxEventTrigger::System,
            cache.path(),
        )
    };
    let waiting = std::time::Duration::from_millis(250);
    let ends = std::time::Duration::from_secs(10);

    let durable_upload = match upload {
        NodeLocalUpload::Running => {
            // The driver holds the artifact lock while it waits, so the
            // request comes first.
            let response = service
                .enqueue_artifact_backup(internal_request(
                    crate::proto::EnqueueArtifactBackupRequest {
                        sandbox_id: id.clone(),
                        expected_artifact_kind: crate::proto::ArtifactKind::Full as i32,
                        source: Some(Source::LocalArtifactId(artifact_id.to_string())),
                    },
                ))
                .await
                .expect("a backup requested by artifact id joins the upload")
                .into_inner();
            let joined = only_upload(&service, &id);
            assert_eq!(joined.job_id, node_local);
            assert_eq!(
                response.backup_attempt_id,
                joined
                    .payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
                    .unwrap()
                    .snapshot_id
            );
            let mut driver = std::pin::pin!(drive());
            tokio::time::timeout(waiting, &mut driver)
                .await
                .expect_err("the driver waits for the node-local upload");
            assert_eq!(only_upload(&service, &id).job_id, node_local);
            // The worker stamps the artifact before its job completes, and a
            // shim poll in between completes the generation first.
            {
                let db = service.db.lock().unwrap();
                crate::db::mark_local_artifact_backup_completed(&db, &artifact_id.to_string())
                    .unwrap();
            }
            let response = service
                .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                    id.clone(),
                    generation.get(),
                    crate::proto::TeardownSuspendAction::BeginOrJoin,
                )))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                crate::proto::TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
                crate::proto::TeardownSuspendOutcome::Durable
            );
            volume_manager.report(Transfers::Completed);
            tokio::time::timeout(ends, driver)
                .await
                .unwrap()
                .unwrap_or_else(|error| panic!("the joined suspend is durable: {error}"));
            Some(node_local)
        }
        NodeLocalUpload::TransferFailed
        | NodeLocalUpload::TransferFailedTwice
        | NodeLocalUpload::RegistrationRecovers => {
            ended(&service, &node_local, JobStatus::Failed).await;
            let recovers = !matches!(upload, NodeLocalUpload::TransferFailedTwice);
            if recovers {
                volume_manager.report(Transfers::Running);
            }
            let mut driver = std::pin::pin!(drive());
            if recovers {
                tokio::time::timeout(waiting, &mut driver)
                    .await
                    .expect_err("the driver waits for its new attempt");
                volume_manager.report(Transfers::Completed);
            }
            let result = tokio::time::timeout(ends, driver).await.unwrap();
            let retry = uploads(&service, &id)
                .into_iter()
                .find(|job| job.job_id != node_local)
                .expect("a new attempt replaces the failed upload");
            assert_eq!(uploads(&service, &id).len(), 2, "exactly one new attempt");
            match upload {
                NodeLocalUpload::TransferFailed | NodeLocalUpload::RegistrationRecovers => {
                    result
                        .unwrap_or_else(|error| panic!("the retried suspend is durable: {error}"));
                    Some(retry.job_id)
                }
                _ => {
                    result.expect_err("the new attempt fails too");
                    // Two uploads of the artifact have failed now: the next
                    // drive reports the failure rather than uploading again.
                    let error = tokio::time::timeout(ends, drive())
                        .await
                        .expect("the driver must not wait on a dead upload")
                        .expect_err("no further attempt");
                    let retry = ended(&service, &retry.job_id, JobStatus::Failed).await;
                    assert_eq!(Some(error.message()), retry.error.as_deref());
                    assert_eq!(uploads(&service, &id).len(), 2, "no further attempt");
                    None
                }
            }
        }
        NodeLocalUpload::TwoFailedAttempts => {
            ended(&service, &node_local, JobStatus::Failed).await;
            let response = service
                .enqueue_artifact_backup(internal_request(
                    crate::proto::EnqueueArtifactBackupRequest {
                        sandbox_id: id.clone(),
                        expected_artifact_kind: crate::proto::ArtifactKind::Full as i32,
                        source: Some(Source::LocalArtifactId(artifact_id.to_string())),
                    },
                ))
                .await
                .expect("a backup requested by artifact id starts a new attempt")
                .into_inner();
            let requested = uploads(&service, &id)
                .into_iter()
                .find(|job| job.job_id != node_local)
                .expect("the request starts a new attempt");
            assert_eq!(
                response.backup_attempt_id,
                requested
                    .payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
                    .unwrap()
                    .snapshot_id
            );
            let requested = ended(&service, &requested.job_id, JobStatus::Failed).await;
            let error = tokio::time::timeout(ends, drive())
                .await
                .expect("the driver must not wait on a dead upload")
                .expect_err("no further attempt");
            assert_eq!(Some(error.message()), requested.error.as_deref());
            assert_eq!(uploads(&service, &id).len(), 2, "no further attempt");
            None
        }
        NodeLocalUpload::RegistrationRefused => {
            let refused = ended(&service, &node_local, JobStatus::Failed).await;
            let error = tokio::time::timeout(ends, drive())
                .await
                .expect("the driver must not wait on a dead upload")
                .expect_err("no new attempt can make the suspend durable");
            assert_eq!(Some(error.message()), refused.error.as_deref());
            assert_eq!(only_upload(&service, &id).job_id, node_local);
            None
        }
    };
    if let Some(durable_upload) = durable_upload {
        ended(&service, &durable_upload, JobStatus::Completed).await;
    }
    {
        let db = service.db.lock().unwrap();
        let status = crate::db::teardown_suspend_status(&db, &id)
            .unwrap()
            .unwrap();
        assert_eq!(
            status.is_durable_for(generation),
            durable_upload.is_some(),
            "{upload:?}"
        );
        if durable_upload.is_some() {
            assert_eq!(
                status.operation.unwrap().published_artifact_id,
                Some(artifact_id)
            );
        }
    }
    super::retired_teardown_tests::drain(&service).await;
}

/// The backup attempt an upload job carries.
fn backup_attempt(job: &pplx_space_common_jobs::Job) -> String {
    job.payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
        .unwrap()
        .snapshot_id
}

/// How a node-local suspend whose backup handoff vol-mgr refused stands when
/// the upload it persisted finalizes.
#[derive(Clone, Copy, Debug)]
enum AfterRefusedHandoff {
    /// Rolled back to `paused` with its runtime live, and still there.
    Paused,
    /// Rolled back to `paused`, then another suspend of it began.
    Resuspends,
    /// Quarantined, because no runtime was live to roll back to.
    Quarantined,
    /// Quarantined over its live runtime, as the failure is when rolling it
    /// back to `paused` fails.
    QuarantinedLive,
}

/// Refuse a node-local suspend's handoff, let `after` happen, and return the
/// backup attempt of the upload that then finalizes.
async fn upload_after_refused_handoff(after: AfterRefusedHandoff) -> String {
    use pplx_space_common_jobs::JobStatus;

    let volume_manager = FakeVolumeManager::serve(Transfers::Refused);
    let scratch = tempfile::tempdir().unwrap();
    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    let runtime_live = !matches!(after, AfterRefusedHandoff::Quarantined);
    let NodeLocalHandoff {
        service,
        artifact_id,
        handoff,
        ..
    } = node_local_handoff(sandbox_id, &volume_manager, scratch.path(), runtime_live).await;
    if matches!(after, AfterRefusedHandoff::QuarantinedLive) {
        // The quarantine the failure falls back to when the rollback's write
        // fails; the test can't fail that write.
        handoff.expect_err("vol-mgr refuses the handoff's pushes");
        let db = service.db.lock().unwrap();
        assert!(crate::db::mark_suspending_quarantined(&db, &id, "rollback failed").unwrap());
    } else {
        service
            .require_node_local_backup_handoff(&id, artifact_id, handoff)
            .expect_err("vol-mgr refuses the handoff's pushes");
    }
    let expected = match after {
        AfterRefusedHandoff::Paused => SandboxState::Paused,
        AfterRefusedHandoff::Resuspends => {
            let db = service.db.lock().unwrap();
            assert!(crate::db::begin_suspend(&db, &id).unwrap());
            SandboxState::Suspending
        }
        AfterRefusedHandoff::Quarantined | AfterRefusedHandoff::QuarantinedLive => {
            SandboxState::Quarantined
        }
    };

    // vol-mgr takes the pushes again; the upload's retry stages and
    // completes the capture.
    volume_manager.report(Transfers::Completed);
    let upload = only_upload(&service, &id);
    let completed = ended(&service, &upload.job_id, JobStatus::Completed).await;
    assert_eq!(service.lookup_sandbox(&id).unwrap().state, expected);
    super::retired_teardown_tests::drain(&service).await;
    backup_attempt(&completed)
}

/// A node-local suspend whose backup handoff meets a vol-mgr refusing pushes
/// rolls back to `paused` with its runtime resident, while the upload it
/// persisted goes on. When vol-mgr takes the pushes again, that upload
/// finalizes ready and reports the suspend uncommitted, so the control plane
/// doesn't project the row `suspended` over the runtime. It does so whether the
/// sandbox stayed paused or another suspend has begun since, and when the
/// suspend was quarantined instead, with its runtime live or not, since nothing
/// on the node settles a quarantined VM. An upload whose suspend committed
/// finalizes without that report.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_of_a_rolled_back_suspend_reports_it_uncommitted() {
    use super::retired_teardown_tests::{FinalizedBackup, finalized_backups, with_cluster_api};
    use pplx_space_common_jobs::JobStatus;

    with_cluster_api(BackupControlPlane::default(), async || {
        let committed = async {
            let scratch = tempfile::tempdir().unwrap();
            let volume_manager = FakeVolumeManager::serve(Transfers::Running);
            let suspended = suspended_after_node_local_upload(
                NodeLocalUpload::Running,
                &volume_manager,
                scratch.path(),
            )
            .await;
            volume_manager.report(Transfers::Completed);
            let completed = ended(
                &suspended.service,
                &suspended.node_local,
                JobStatus::Completed,
            )
            .await;
            super::retired_teardown_tests::drain(&suspended.service).await;
            backup_attempt(&completed)
        };
        // Each refused handoff's upload waits out a retry backoff; run the
        // cases side by side.
        let ((paused, resuspended), (quarantined, quarantined_live), committed) =
            futures_util::future::join3(
                futures_util::future::join(
                    Box::pin(upload_after_refused_handoff(AfterRefusedHandoff::Paused)),
                    Box::pin(upload_after_refused_handoff(
                        AfterRefusedHandoff::Resuspends,
                    )),
                ),
                futures_util::future::join(
                    Box::pin(upload_after_refused_handoff(
                        AfterRefusedHandoff::Quarantined,
                    )),
                    Box::pin(upload_after_refused_handoff(
                        AfterRefusedHandoff::QuarantinedLive,
                    )),
                ),
                Box::pin(committed),
            )
            .await;

        let mut finalized = finalized_backups();
        finalized.sort_by(|a, b| a.backup_attempt_id.cmp(&b.backup_attempt_id));
        let mut expected = [
            (paused, true),
            (resuspended, true),
            (quarantined, true),
            (quarantined_live, true),
            (committed, false),
        ]
        .map(|(backup_attempt_id, suspend_uncommitted)| FinalizedBackup {
            backup_attempt_id,
            suspend_uncommitted,
        });
        expected.sort_by(|a, b| a.backup_attempt_id.cmp(&b.backup_attempt_id));
        assert_eq!(finalized, expected);
    })
    .await;
}
