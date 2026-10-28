use super::*;
use pplx_space_spaced_host_client::{SpacedHostClientConfig, proto};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tonic::{Request, Response};

#[derive(Clone)]
struct Host {
    instance: ProcessRecoveryToken,
    activation: String,
    barrier: ProcessRecoveryToken,
    armed: Arc<AtomicBool>,
    observations: Arc<AtomicUsize>,
}
#[tonic::async_trait]
impl proto::meta_server::Meta for Host {
    async fn wait_ready(
        &self,
        _: Request<proto::WaitReadyRequest>,
    ) -> Result<Response<WaitReadyResponse>, Status> {
        self.observations.fetch_add(1, Ordering::SeqCst);
        let armed = self.armed.load(Ordering::SeqCst);
        Ok(Response::new(WaitReadyResponse {
            process_recovery_barrier_id: (!armed).then(|| self.barrier.to_string()),
            process_execution: Some(proto::ProcessExecutionInfo {
                instance_id: self.instance.to_string(),
                activation_id: armed.then(|| self.activation.clone()),
                capabilities: vec!["process_recovery_start".into()],
                ..Default::default()
            }),
            ..Default::default()
        }))
    }
    async fn resume_process_recovery(
        &self,
        request: Request<proto::ResumeProcessRecoveryRequest>,
    ) -> Result<Response<proto::ResumeProcessRecoveryResponse>, Status> {
        assert!(request.metadata().get("grpc-timeout").is_some());
        assert_eq!(
            request.into_inner().expected_barrier_id,
            self.barrier.to_string()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        self.armed.store(true, Ordering::SeqCst);
        Ok(Response::new(proto::ResumeProcessRecoveryResponse {}))
    }
    async fn ping(
        &self,
        _: Request<proto::PingRequest>,
    ) -> Result<Response<proto::PingResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn is_connected(
        &self,
        _: Request<proto::IsConnectedRequest>,
    ) -> Result<Response<proto::IsConnectedResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn ping_guest(
        &self,
        _: Request<proto::PingGuestRequest>,
    ) -> Result<Response<proto::PingGuestResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn get_status(
        &self,
        _: Request<proto::GetStatusRequest>,
    ) -> Result<Response<proto::GetStatusResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn configure_process_env(
        &self,
        _: Request<proto::ConfigureProcessEnvRequest>,
    ) -> Result<Response<proto::ConfigureProcessEnvResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn notify_resume(
        &self,
        _: Request<proto::NotifyResumeRequest>,
    ) -> Result<Response<proto::NotifyResumeResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn flush_guest_filesystems(
        &self,
        _: Request<proto::FlushGuestFilesystemsRequest>,
    ) -> Result<Response<proto::FlushGuestFilesystemsResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn prepare_suspend(
        &self,
        _: Request<proto::PrepareSuspendRequest>,
    ) -> Result<Response<proto::PrepareSuspendResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
}

#[tokio::test]
async fn legacy_rearm_reuses_observation_and_allows_real_rpc_latency() {
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    insert_test_sandbox_with_runtime(&conn, &id, SandboxState::Started);
    let service = SandboxManagerService::for_test(conn);
    let sandbox = service.lookup_sandbox(&id).unwrap();
    let host = Host {
        instance: ProcessRecoveryToken::new(),
        activation: sandbox.node_execution_id.unwrap().to_string(),
        barrier: ProcessRecoveryToken::new(),
        armed: Arc::new(AtomicBool::new(false)),
        observations: Arc::new(AtomicUsize::new(0)),
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("host.sock");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(proto::meta_server::MetaServer::new(host.clone()))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::UnixListenerStream::new(listener),
                shutdown.clone().cancelled_owned(),
            ),
    );
    let client =
        SpacedHostClient::new_with_config(SpacedHostClientConfig::default().with_socket_path(path))
            .await
            .unwrap();
    let observed = client.wait_ready(Duration::from_secs(3)).await.unwrap();
    let budget = legacy_rearm_budget(Duration::from_secs(3), Duration::from_millis(50));
    let response = best_effort_recovery_ready(
        observed.clone(),
        budget,
        service.rearm_observed(&sandbox, None, client.clone(), observed, budget),
    )
    .await;
    assert_eq!(
        response.process_execution.unwrap().activation_id.as_deref(),
        Some(host.activation.as_str())
    );
    assert_eq!(
        host.observations.load(Ordering::SeqCst),
        2,
        "one initial and one confirming observation"
    );
    let capture = service.auto_lifecycle.artifact_lock(&id).lock_owned().await;
    host.armed.store(false, Ordering::SeqCst);
    let observed = client.wait_ready(Duration::from_secs(1)).await.unwrap();
    let deadline =
        pplx_space_common_grpc::GrpcRetryContext::with_timeout(Duration::from_millis(30));
    let error = tokio::time::timeout(
        deadline.remaining().unwrap(),
        service.rearm_observed(&sandbox, None, client, observed, Duration::from_secs(1)),
    )
    .await;
    assert!(error.is_err(), "busy artifact lock respects caller budget");
    drop(capture);
    shutdown.cancel();
    server.await.unwrap().unwrap();
    service.shutdown_token.cancel();
}
