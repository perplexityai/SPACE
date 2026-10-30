use pplx_space_net_common::proto::{
    EgressDefaultAction as NetworkPolicyAction, NetworkPolicyV1 as NetworkPolicyProto,
    NetworkPolicyVersion,
};
use pplx_space_spaced_common::proto::bridge_host::meta_client::MetaClient as BridgeHostClient;
use pplx_space_spaced_common::proto::host_control::host_control_server::{
    HostControl, HostControlServer,
};
use pplx_space_spaced_common::proto::host_control::{
    Accepted as HostControlAccepted, ActiveForAccepted as HostControlActiveForAccepted,
    GetNetworkPolicyRequest as HostControlGetNetworkPolicyRequest,
    GetNetworkPolicyResponse as HostControlGetNetworkPolicyResponse,
    IssueGatewayIntermediateCertificateRequest, IssueGatewayIntermediateCertificateResponse,
    RequestActiveForRequest as HostControlRequestActiveForRequest,
    RequestActiveForResponse as HostControlRequestActiveForResponse,
    RequestShutdownRequest as HostControlShutdownRequest,
    RequestShutdownResponse as HostControlShutdownResponse,
    SetActivityStateRequest as HostControlSetActivityStateRequest,
    SetActivityStateResponse as HostControlSetActivityStateResponse,
    request_active_for_response::Outcome as HostControlActiveForOutcome,
    request_shutdown_response::Outcome as HostControlShutdownOutcome,
};
use pplx_space_spaced_host_server::{
    BridgeGuestConnector, SpacedHostPeerAuthorization, SpacedHostService, SpacedHostServiceConfig,
};
use std::time::Duration;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

#[derive(Debug, PartialEq)]
pub(crate) enum HostControlCall {
    Activity(HostControlSetActivityStateRequest),
    ActiveFor(HostControlRequestActiveForRequest),
    Shutdown(HostControlShutdownRequest),
}

struct MockHostControl {
    fail: bool,
    calls: std::sync::Arc<std::sync::Mutex<Vec<HostControlCall>>>,
}

#[tonic::async_trait]
impl HostControl for MockHostControl {
    async fn issue_gateway_intermediate_certificate(
        &self,
        _: Request<IssueGatewayIntermediateCertificateRequest>,
    ) -> Result<Response<IssueGatewayIntermediateCertificateResponse>, Status> {
        Err(Status::unimplemented("bridge fixture does not issue CAs"))
    }

    async fn set_activity_state(
        &self,
        request: Request<HostControlSetActivityStateRequest>,
    ) -> Result<Response<HostControlSetActivityStateResponse>, Status> {
        self.calls
            .lock()
            .unwrap()
            .push(HostControlCall::Activity(request.into_inner()));
        if self.fail {
            return Err(Status::internal("host control test failure"));
        }
        Ok(Response::new(HostControlSetActivityStateResponse {
            outcome: None,
        }))
    }

    async fn request_active_for(
        &self,
        request: Request<HostControlRequestActiveForRequest>,
    ) -> Result<Response<HostControlRequestActiveForResponse>, Status> {
        let request = request.into_inner();
        self.calls
            .lock()
            .unwrap()
            .push(HostControlCall::ActiveFor(request));
        if self.fail {
            return Err(Status::internal("host control test failure"));
        }
        Ok(Response::new(HostControlRequestActiveForResponse {
            outcome: Some(HostControlActiveForOutcome::Accepted(
                HostControlActiveForAccepted {
                    granted_for: request.hold_for,
                },
            )),
        }))
    }

    async fn request_shutdown(
        &self,
        request: Request<HostControlShutdownRequest>,
    ) -> Result<Response<HostControlShutdownResponse>, Status> {
        self.calls
            .lock()
            .unwrap()
            .push(HostControlCall::Shutdown(request.into_inner()));
        if self.fail {
            return Err(Status::internal("host control test failure"));
        }
        Ok(Response::new(HostControlShutdownResponse {
            outcome: Some(HostControlShutdownOutcome::Accepted(HostControlAccepted {})),
        }))
    }

    async fn get_network_policy(
        &self,
        _request: Request<HostControlGetNetworkPolicyRequest>,
    ) -> Result<Response<HostControlGetNetworkPolicyResponse>, Status> {
        if self.fail {
            return Err(Status::internal("host control test failure"));
        }
        Ok(Response::new(HostControlGetNetworkPolicyResponse {
            effective_policy: Some(NetworkPolicyProto {
                version: NetworkPolicyVersion::V1.into(),
                egress_default_action: NetworkPolicyAction::Allow.into(),
                ..Default::default()
            }),
            inherited: true,
            effective_policy_ref: "test:policy".to_string(),
            network_tx_bytes: 0,
            network_rx_bytes: 0,
            network_credentials: Vec::new(),
        }))
    }
}

pub(crate) async fn connect_bridge_host(
    socket_path: std::path::PathBuf,
) -> BridgeHostClient<tonic::transport::Channel> {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match pplx_space_spaced_common::connect_uds_channel("spaced bridge host", &socket_path)
                .await
            {
                Ok(channel) => return BridgeHostClient::new(channel),
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out connecting to {}", socket_path.display()))
}

pub(crate) struct BridgeHostFixture {
    _directory: tempfile::TempDir,
    pub(crate) calls: std::sync::Arc<std::sync::Mutex<Vec<HostControlCall>>>,
    pub(crate) bridge_host_socket_path: std::path::PathBuf,
    host_control_task: tokio::task::JoinHandle<()>,
    shutdown: CancellationToken,
    service_task: tokio::task::JoinHandle<
        Result<(), pplx_space_spaced_host_server::RunSpacedHostServerError>,
    >,
}

impl BridgeHostFixture {
    pub(crate) async fn start() -> Self {
        Self::start_with_host_control_failure(false, None).await
    }

    pub(crate) async fn start_with_failing_host_control() -> Self {
        Self::start_with_host_control_failure(true, None).await
    }

    pub(crate) async fn start_with_expected_peer(
        expected_peer: pplx_space_spaced_common::ExpectedUdsPeer,
    ) -> Self {
        Self::start_with_host_control_failure(false, Some(expected_peer)).await
    }

    async fn start_with_host_control_failure(
        fail: bool,
        expected_peer: Option<pplx_space_spaced_common::ExpectedUdsPeer>,
    ) -> Self {
        let temp_dir = tempfile::tempdir().expect("temp dir created");
        let temp_path = temp_dir.path();
        let socket_path = temp_path.join("spaced-host.sock");
        let bridge_host_socket_path = temp_path.join("spaced-bridge-host.sock");
        let bridge_guest_socket_path = temp_path.join("spaced-bridge-guest.sock");
        let host_control_socket_path = temp_path.join("sandbox-host-control.sock");
        let host_control_listener =
            UnixListener::bind(&host_control_socket_path).expect("host control socket bound");
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mock = MockHostControl {
            fail,
            calls: calls.clone(),
        };
        let host_control_task = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(HostControlServer::new(mock))
                .serve_with_incoming(UnixListenerStream::new(host_control_listener))
                .await;
        });
        let shutdown = CancellationToken::new();
        let bridge_guest_connector = expected_peer.map_or_else(
            || BridgeGuestConnector::Uds(bridge_guest_socket_path),
            |expected_peer| BridgeGuestConnector::FirecrackerVsock {
                vsock_uds_path: temp_path.join("v.sock"),
                port: pplx_space_spaced_common::BRIDGE_GUEST_VSOCK_PORT,
                expected_peer,
            },
        );
        let service = SpacedHostService::new_with_config(
            shutdown.clone(),
            SpacedHostServiceConfig {
                process_recovery_activation: None,
                socket_path,
                bridge_host_socket_path: bridge_host_socket_path.clone(),
                bridge_guest_connector,
                host_control_socket_path,
                host_peer_authorization: SpacedHostPeerAuthorization::Disabled,
            },
        );
        let service_task = tokio::spawn(service.run());
        Self {
            _directory: temp_dir,
            calls,
            bridge_host_socket_path,
            host_control_task,
            shutdown,
            service_task,
        }
    }

    pub(crate) async fn stop(mut self) {
        self.shutdown.cancel();
        self.service_task.abort();
        self.host_control_task.abort();
        match (&mut self.service_task).await {
            Ok(result) => result.expect("service stopped"),
            Err(error) => assert!(error.is_cancelled()),
        }
        assert!(
            (&mut self.host_control_task)
                .await
                .unwrap_err()
                .is_cancelled()
        );
    }
}

impl Drop for BridgeHostFixture {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.service_task.abort();
        self.host_control_task.abort();
    }
}
