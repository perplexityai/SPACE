use super::*;
use crate::{BridgeGuestConnector, SpacedHostServiceConfig, proto::meta_server::Meta};
use bridge_guest::meta_server::{Meta as GuestMeta, MetaServer};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, Response};

#[derive(Clone)]
struct Guest {
    state: Arc<Mutex<(ProcessRecoveryToken, Option<String>)>>,
    instance: ProcessRecoveryToken,
    lose_reply: Arc<AtomicBool>,
    misconfirm: Arc<AtomicBool>,
    supports_recovery: bool,
}

impl Guest {
    fn info(&self) -> proto::ProcessExecutionInfo {
        proto::ProcessExecutionInfo {
            instance_id: self.instance.to_string(),
            activation_id: self.state.lock().unwrap().1.clone(),
            capabilities: vec![
                pplx_space_common_grpc::GuestCapability::ProcessRecoveryStart
                    .as_str()
                    .into(),
            ],
            ..Default::default()
        }
    }
}

#[tonic::async_trait]
impl GuestMeta for Guest {
    async fn ping(
        &self,
        _: Request<bridge_guest::PingRequest>,
    ) -> Result<Response<bridge_guest::PingResponse>, Status> {
        Ok(Response::new(Default::default()))
    }
    async fn wait_ready(
        &self,
        _: Request<bridge_guest::WaitReadyRequest>,
    ) -> Result<Response<bridge_guest::WaitReadyResponse>, Status> {
        Ok(Response::new(bridge_guest::WaitReadyResponse {
            ready: true,
            version: pplx_space_spaced_common::spaced_version().to_string(),
            compatibility_family: pplx_space_spaced_common::spaced_compatibility_family(),
            process_execution: Some(self.info()),
            process_recovery_barrier_id: self
                .supports_recovery
                .then(|| self.state.lock().unwrap().0.to_string()),
            ..Default::default()
        }))
    }
    async fn activate_process_recovery(
        &self,
        request: Request<bridge_guest::ActivateProcessRecoveryRequest>,
    ) -> Result<Response<bridge_guest::ActivateProcessRecoveryResponse>, Status> {
        if !self.supports_recovery {
            return Err(Status::unimplemented("older guest"));
        }
        let request = request.into_inner();
        {
            let mut state = self.state.lock().unwrap();
            if state.0.to_string() != request.expected_barrier_id {
                return Err(Status::failed_precondition("guest barrier changed"));
            }
            state.1 = Some(request.activation_id);
        }
        if self.lose_reply.swap(false, Ordering::SeqCst) {
            return Err(Status::unavailable("synthetic lost activation reply"));
        }
        Ok(Response::new(
            bridge_guest::ActivateProcessRecoveryResponse {
                process_execution: Some({
                    let mut info = self.info();
                    if self.misconfirm.swap(false, Ordering::SeqCst) {
                        info.activation_id = None;
                    }
                    info
                }),
            },
        ))
    }
    async fn configure_process_env(
        &self,
        _: Request<bridge_guest::ConfigureProcessEnvRequest>,
    ) -> Result<Response<bridge_guest::ConfigureProcessEnvResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn notify_resume(
        &self,
        _: Request<bridge_guest::NotifyResumeRequest>,
    ) -> Result<Response<bridge_guest::NotifyResumeResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn flush_filesystems(
        &self,
        _: Request<bridge_guest::FlushFilesystemsRequest>,
    ) -> Result<Response<bridge_guest::FlushFilesystemsResponse>, Status> {
        *self.state.lock().unwrap() = (ProcessRecoveryToken::new(), None);
        Ok(Response::new(bridge_guest::FlushFilesystemsResponse {}))
    }
}

async fn ready(host: &SpacedHostService) -> proto::WaitReadyResponse {
    Meta::wait_ready(
        host,
        Request::new(proto::WaitReadyRequest {
            timeout: Some(prost_types::Duration {
                seconds: 1,
                nanos: 0,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner()
}
async fn resume(host: &SpacedHostService, barrier: &str) -> Result<(), Status> {
    Meta::resume_process_recovery(
        host,
        Request::new(proto::ResumeProcessRecoveryRequest {
            expected_barrier_id: barrier.into(),
        }),
    )
    .await
    .map(|_| ())
}

#[tokio::test]
async fn activation_requires_current_host_barrier_and_launch_bound_epoch() {
    for supports_recovery in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("guest.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let guest = Guest {
            state: Arc::new(Mutex::new((ProcessRecoveryToken::new(), None))),
            instance: ProcessRecoveryToken::new(),
            lose_reply: Arc::new(AtomicBool::new(false)),
            misconfirm: Arc::new(AtomicBool::new(false)),
            supports_recovery,
        };
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let serving = guest.clone();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(MetaServer::new(serving))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::UnixListenerStream::new(listener),
                    stop.cancelled_owned(),
                )
                .await
                .unwrap();
        });
        let host = |activation| {
            SpacedHostService::new_with_config(
                shutdown.child_token(),
                SpacedHostServiceConfig {
                    process_recovery_activation: activation,
                    socket_path: directory.path().join("host.sock"),
                    bridge_host_socket_path: directory.path().join("bridge.sock"),
                    host_control_socket_path: directory.path().join("control.sock"),
                    bridge_guest_connector: BridgeGuestConnector::Uds(socket.clone()),
                    ..Default::default()
                },
            )
        };
        let disabled = host(None);
        assert!(ready(&disabled).await.process_recovery_barrier_id.is_none());
        assert_eq!(
            resume(&disabled, &ProcessRecoveryToken::new().to_string())
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        let epoch = ProcessRecoveryToken::new();
        let first = host(Some(epoch));
        let observation = ready(&first).await;
        assert!(
            observation
                .process_execution
                .unwrap()
                .capabilities
                .is_empty(),
            "do not advertise recovery before activation"
        );
        if !supports_recovery {
            assert!(observation.process_recovery_barrier_id.is_none());
            let barrier = first
                .inner
                .process_recovery
                .as_ref()
                .unwrap()
                .gate
                .lock()
                .unwrap()
                .barrier;
            assert_eq!(
                resume(&first, &barrier.to_string())
                    .await
                    .unwrap_err()
                    .code(),
                Code::FailedPrecondition
            );
        } else {
            let barrier = observation.process_recovery_barrier_id.unwrap();
            guest.misconfirm.store(true, Ordering::SeqCst);
            assert_eq!(
                resume(&first, &barrier).await.unwrap_err().code(),
                Code::FailedPrecondition
            );
            assert_eq!(
                ready(&first).await.process_recovery_barrier_id.as_deref(),
                Some(barrier.as_str())
            );
            guest.lose_reply.store(true, Ordering::SeqCst);
            assert_eq!(
                resume(&first, &barrier).await.unwrap_err().code(),
                Code::Unavailable
            );
            assert_eq!(guest.info().activation_id, Some(epoch.to_string()));
            assert_eq!(
                ready(&first).await.process_recovery_barrier_id.as_deref(),
                Some(barrier.as_str()),
                "lost reply keeps the host quiesced"
            );
            resume(&first, &barrier).await.unwrap();
            resume(&first, &barrier).await.unwrap();
            assert!(ready(&first).await.process_recovery_barrier_id.is_none());
            for memory_snapshot in [false, true] {
                let previous = first
                    .inner
                    .process_recovery
                    .as_ref()
                    .unwrap()
                    .gate
                    .lock()
                    .unwrap()
                    .barrier
                    .to_string();
                if memory_snapshot {
                    Meta::prepare_suspend(&first, Request::new(proto::PrepareSuspendRequest {}))
                        .await
                        .unwrap();
                } else {
                    Meta::flush_guest_filesystems(
                        &first,
                        Request::new(proto::FlushGuestFilesystemsRequest {}),
                    )
                    .await
                    .unwrap();
                }
                assert!(guest.info().activation_id.is_none());
                assert_eq!(
                    resume(&first, &previous).await.unwrap_err().code(),
                    Code::FailedPrecondition
                );
                let next = ready(&first).await.process_recovery_barrier_id.unwrap();
                assert_ne!(previous, next);
                resume(&first, &next).await.unwrap();
                assert_eq!(guest.info().activation_id, Some(epoch.to_string()));
            }
            // A sidecar restart preserves the resident VM's external identity,
            // but cannot accept an authorization issued to the previous host.
            let old = first
                .inner
                .process_recovery
                .as_ref()
                .unwrap()
                .gate
                .lock()
                .unwrap()
                .barrier
                .to_string();
            let replacement = host(Some(epoch));
            let next = ready(&replacement)
                .await
                .process_recovery_barrier_id
                .unwrap();
            assert_ne!(old, next);
            assert_eq!(
                resume(&replacement, &old).await.unwrap_err().code(),
                Code::FailedPrecondition
            );
            resume(&replacement, &next).await.unwrap();
            assert_eq!(guest.info().activation_id, Some(epoch.to_string()));
        }
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn recovery_start_does_not_replay_a_dispatched_request() {
    use proto::process_server::Process;
    use std::sync::atomic::AtomicUsize;

    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("guest.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let received = calls.clone();
    let rpc = tower::service_fn(
        move |request: tonic::codegen::http::Request<tonic::body::Body>| {
            assert_eq!(
                request.uri().path(),
                "/space.spaced.bridge.guest.v1.Process/Start"
            );
            received.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok::<_, std::convert::Infallible>(
                Status::unavailable("synthetic reply lost after dispatch")
                    .into_http::<tonic::body::Body>(),
            ))
        },
    );
    let server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        tonic::transport::Server::builder()
            .serve_with_incoming(
                rpc,
                tokio_stream::wrappers::UnixListenerStream::new(listener),
            )
            .await
            .unwrap();
    }));
    let epoch = ProcessRecoveryToken::new();
    let host = SpacedHostService::new_with_config(
        CancellationToken::new(),
        SpacedHostServiceConfig {
            process_recovery_activation: Some(epoch),
            socket_path: directory.path().join("host.sock"),
            bridge_host_socket_path: directory.path().join("bridge.sock"),
            host_control_socket_path: directory.path().join("control.sock"),
            bridge_guest_connector: BridgeGuestConnector::Uds(socket),
            ..Default::default()
        },
    );
    // Activation itself is covered above; isolate the dispatch/retry contract.
    host.inner
        .process_recovery
        .as_ref()
        .unwrap()
        .gate
        .lock()
        .unwrap()
        .phase = Phase::Active;
    let error = Process::start(
        &host,
        Request::new(proto::StartRequest {
            idempotency_key: Some("once".into()),
            recovery: Some(proto::RecoveryStart {
                contract_version: 1,
                expected_instance_id: ProcessRecoveryToken::new().to_string(),
                expected_activation_id: epoch.to_string(),
                command: vec!["/bin/true".into()],
            }),
            ..Default::default()
        }),
    )
    .await
    .err()
    .expect("lost response");
    assert_eq!(error.code(), Code::Unavailable);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}
