use super::*;
use pplx_space_spaced_common::proto::bridge_guest::{
    ConfigureProcessEnvResponse as BridgeGuestConfigureProcessEnvResponse,
    FlushFilesystemsResponse as BridgeGuestFlushFilesystemsResponse,
    NotifyResumeResponse as BridgeGuestNotifyResumeResponse,
    PingResponse as BridgeGuestPingResponse,
};

#[test]
fn historical_omission_and_explicit_platforms_have_distinct_admission() {
    for (version, os, admitted) in [
        ("0.1.0", None, true),
        ("0.1.4", None, true),
        ("0.8.0", None, true),
        ("0.8.0+build.1", None, true),
        ("0.8.1+build.1", None, false),
        ("0.8.1", None, false),
        ("0.8.1", Some(GuestOs::Linux), true),
        ("0.8.0", Some(GuestOs::Windows), false),
        ("0.8.0", Some(GuestOs::Unspecified), false),
    ] {
        let response = BridgeGuestWaitReadyResponse {
            version: version.into(),
            compatibility_family:
                pplx_space_spaced_common::spaced_compatibility_family_for_version(version).unwrap(),
            platform: os.map(|os| {
                pplx_space_spaced_common::proto::platform::GuestPlatformDescriptor {
                    os: os as i32,
                    operations: vec![],
                }
            }),
            ..Default::default()
        };
        let parsed = GuestPlatformObservation::parse(&response, QualifiedGuestAgent::LinuxV1);
        assert_eq!(parsed.is_ok(), admitted, "{version} {os:?}");
        if admitted {
            let platform = parsed.unwrap();
            if os.is_some() {
                platform.require(GuestOperation::Process, None).unwrap_err();
            } else {
                platform.require(GuestOperation::Process, None).unwrap();
                platform.require(GuestOperation::Files, None).unwrap();
                assert_eq!(
                    platform
                        .require(GuestOperation::InteractiveProcessControl, None)
                        .is_ok(),
                    version != "0.1.0",
                    "{version} interactive control"
                );
            }
        }
    }
}

#[test]
fn windows_operations_require_trusted_os_and_cannot_grant_memory_lifecycle() {
    for (os, accepted) in [
        (Some(GuestOs::Windows), true),
        (Some(GuestOs::Linux), false),
        (None, false),
    ] {
        let response = BridgeGuestWaitReadyResponse {
            version: "0.8.0".into(),
            compatibility_family: "0.8".into(),
            platform: os.map(|os| {
                pplx_space_spaced_common::proto::platform::GuestPlatformDescriptor {
                    os: os as i32,
                    operations: vec![GuestOperation::Process as i32, GuestOperation::Flush as i32],
                }
            }),
            ..Default::default()
        };
        assert!(
            GuestPlatformObservation::parse(&response, QualifiedGuestAgent::default()).is_err(),
            "an omitted policy must not authorize guest claims"
        );
        let result = GuestPlatformObservation::parse(&response, QualifiedGuestAgent::WindowsV1);
        assert_eq!(result.is_ok(), accepted);
        if let Ok(platform) = result {
            platform
                .require(GuestOperation::Process, None)
                .expect("qualified process operation");
            assert!(platform.require(GuestOperation::Flush, None).is_err());
        }
    }
}

#[derive(Clone)]
struct ChangingGuest {
    observations: Arc<std::sync::atomic::AtomicUsize>,
    configured: Arc<std::sync::atomic::AtomicUsize>,
}
#[tonic::async_trait]
impl pplx_space_spaced_common::proto::bridge_guest::meta_server::Meta for ChangingGuest {
    async fn wait_ready(
        &self,
        _: Request<BridgeGuestWaitReadyRequest>,
    ) -> Result<Response<BridgeGuestWaitReadyResponse>, Status> {
        use std::sync::atomic::Ordering;
        let first = self.observations.fetch_add(1, Ordering::SeqCst) == 0;
        Ok(Response::new(BridgeGuestWaitReadyResponse {
            ready: true,
            version: "0.8.0".into(),
            compatibility_family: "0.8".into(),
            platform: Some(
                pplx_space_spaced_common::proto::platform::GuestPlatformDescriptor {
                    os: GuestOs::Linux as i32,
                    operations: if first {
                        vec![GuestOperation::ProcessEnvironment as i32]
                    } else {
                        vec![GuestOperation::Process as i32]
                    },
                },
            ),
            ..Default::default()
        }))
    }
    async fn configure_process_env(
        &self,
        _: Request<BridgeGuestConfigureProcessEnvRequest>,
    ) -> Result<Response<BridgeGuestConfigureProcessEnvResponse>, Status> {
        self.configured
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Response::new(BridgeGuestConfigureProcessEnvResponse {}))
    }
    async fn ping(
        &self,
        _: Request<BridgeGuestPingRequest>,
    ) -> Result<Response<BridgeGuestPingResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn notify_resume(
        &self,
        _: Request<BridgeGuestNotifyResumeRequest>,
    ) -> Result<Response<BridgeGuestNotifyResumeResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn flush_filesystems(
        &self,
        _: Request<BridgeGuestFlushFilesystemsRequest>,
    ) -> Result<Response<BridgeGuestFlushFilesystemsResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
}

#[tokio::test]
async fn readiness_claim_cannot_authorize_a_different_dispatch_observation() {
    use crate::proto::meta_server::Meta;
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("guest.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let guest = ChangingGuest {
        observations: Arc::default(),
        configured: Arc::default(),
    };
    let configured = guest.configured.clone();
    let stop = CancellationToken::new();
    let server_stop = stop.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                pplx_space_spaced_common::proto::bridge_guest::meta_server::MetaServer::new(guest),
            )
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::UnixListenerStream::new(listener),
                server_stop.cancelled_owned(),
            )
            .await
            .unwrap();
    });
    let host = SpacedHostService::new_with_config(
        stop.clone(),
        SpacedHostServiceConfig {
            socket_path: root.path().join("spaced-host.sock"),
            bridge_host_socket_path: root.path().join("spaced-bridge-host.sock"),
            host_control_socket_path: root.path().join("sandbox-host-control.sock"),
            bridge_guest_connector: BridgeGuestConnector::Uds(socket),
            ..Default::default()
        },
    );
    host.wait_ready(Request::new(WaitReadyRequest {
        timeout: Some(prost_types::Duration {
            seconds: 2,
            nanos: 0,
        }),
    }))
    .await
    .unwrap();
    let error = host
        .configure_process_env(Request::new(ConfigureProcessEnvRequest {
            env: Default::default(),
        }))
        .await
        .unwrap_err();
    assert!(pplx_space_common_grpc::GuestCapabilityUnsupported::from_status(&error).is_some());
    assert_eq!(configured.load(std::sync::atomic::Ordering::SeqCst), 0);
    stop.cancel();
    server.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn negotiation_expiry_is_retryable_without_reclassifying_peer_cancellation() {
    let expired = negotiation_response(std::future::pending())
        .await
        .unwrap_err();
    assert_eq!(expired.code(), tonic::Code::DeadlineExceeded);
    assert!(pplx_space_spaced_common::status_is_retryable(&expired));
    let cancelled = negotiation_response(async { Err(Status::cancelled("peer cancelled")) })
        .await
        .unwrap_err();
    assert_eq!(cancelled.code(), tonic::Code::Cancelled);
    assert!(!pplx_space_spaced_common::status_is_retryable(&cancelled));
}

#[test]
fn historical_linux_capabilities_are_frozen_at_the_released_wire_values() {
    // 0.8.0 shipped operations 1 through 8. This release fixture must not grow
    // when current Linux guests advertise newly implemented operations.
    let mut operations = historical_linux_capabilities().operations;
    operations.sort_unstable();
    assert_eq!(operations, (1..=8).collect::<Vec<_>>());
}
