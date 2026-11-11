use nix::unistd::{Uid, User};
use pplx_space_common_core::types::GuestUserName;
use pplx_space_spaced_common::proto::bridge_host::meta_server::{Meta, MetaServer};
use pplx_space_spaced_common::proto::bridge_host::{
    RequestActiveForRequest, RequestActiveForResponse,
    RequestShutdownRequest as BridgeHostShutdownRequest, RequestShutdownResponse,
    SetActivityStateRequest, SetActivityStateResponse,
};
use pplx_space_spaced_guest_server::{
    FilesystemExecutionMode, HostConnectorType, SpacedGuestService, SpacedGuestServiceConfig,
};
use std::time::Duration;
use tokio::net::{UnixListener, UnixStream};
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

#[derive(Debug, PartialEq)]
pub(crate) enum BridgeHostCall {
    Activity(SetActivityStateRequest),
    ActiveFor(RequestActiveForRequest),
    Shutdown(BridgeHostShutdownRequest),
}

type BridgeHostCalls = std::sync::Arc<std::sync::Mutex<Vec<BridgeHostCall>>>;
struct MockBridgeHost(BridgeHostCalls);

pub(crate) fn test_guest_uptime() -> prost_types::Duration {
    prost_types::Duration {
        seconds: 123,
        nanos: 456_000_000,
    }
}

fn test_guest_uptime_source() -> Option<prost_types::Duration> {
    Some(test_guest_uptime())
}

#[tonic::async_trait]
impl Meta for MockBridgeHost {
    async fn set_activity_state(
        &self,
        request: Request<SetActivityStateRequest>,
    ) -> Result<Response<SetActivityStateResponse>, Status> {
        self.0
            .lock()
            .unwrap()
            .push(BridgeHostCall::Activity(request.into_inner()));
        Ok(Response::new(SetActivityStateResponse { outcome: None }))
    }

    async fn request_active_for(
        &self,
        request: Request<RequestActiveForRequest>,
    ) -> Result<Response<RequestActiveForResponse>, Status> {
        self.0
            .lock()
            .unwrap()
            .push(BridgeHostCall::ActiveFor(request.into_inner()));
        Ok(Response::new(RequestActiveForResponse { outcome: None }))
    }

    async fn request_shutdown(
        &self,
        request: Request<BridgeHostShutdownRequest>,
    ) -> Result<Response<RequestShutdownResponse>, Status> {
        self.0
            .lock()
            .unwrap()
            .push(BridgeHostCall::Shutdown(request.into_inner()));
        Ok(Response::new(RequestShutdownResponse { outcome: None }))
    }
}

pub(crate) async fn start_mock_bridge_host(
    socket_path: std::path::PathBuf,
) -> (tokio::task::JoinHandle<()>, BridgeHostCalls) {
    let listener = match UnixListener::bind(&socket_path) {
        Ok(listener) => listener,
        Err(error) => panic!(
            "failed to bind mock bridge host socket {}: {error}",
            socket_path.display()
        ),
    };
    let calls = BridgeHostCalls::default();
    let mock = MockBridgeHost(calls.clone());
    let task = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(MetaServer::new(mock))
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await;
    });
    (task, calls)
}

pub(crate) struct GuestFixture {
    pub(crate) channel: tonic::transport::Channel,
    shutdown: CancellationToken,
    service: tokio::task::JoinHandle<()>,
    host: tokio::task::JoinHandle<()>,
}

impl GuestFixture {
    pub(crate) async fn start(directory: &std::path::Path) -> Self {
        let host_socket = directory.join("spaced-bridge-host.sock");
        let guest_socket = directory.join("spaced-bridge-guest.sock");
        let (host, _) = start_mock_bridge_host(host_socket.clone()).await;
        let (service, shutdown) =
            create_ready_guest_service(host_socket, None, Some(guest_socket.clone()));
        let service = tokio::spawn(async move { service.run().await.expect("service stopped") });
        Self {
            channel: connect_uds(guest_socket).await,
            shutdown,
            service,
            host,
        }
    }

    /// Cancels the service shutdown token without waiting for the service.
    pub(crate) fn begin_shutdown(&self) {
        self.shutdown.cancel();
    }

    pub(crate) async fn stop(mut self) {
        self.shutdown.cancel();
        (&mut self.service).await.expect("service joined");
        self.host.abort();
        assert!((&mut self.host).await.unwrap_err().is_cancelled());
    }
}

impl Drop for GuestFixture {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.service.abort();
        self.host.abort();
    }
}

pub(crate) fn create_guest_service(
    bridge_host_socket: std::path::PathBuf,
    local_socket: Option<std::path::PathBuf>,
    bridge_guest_socket: Option<std::path::PathBuf>,
) -> (SpacedGuestService, CancellationToken) {
    create_guest_service_with(
        bridge_host_socket,
        local_socket,
        bridge_guest_socket,
        |_| {},
    )
}

/// As [`create_guest_service`], letting a caller adjust the service
/// configuration before the service is built. Everything the adjustment does
/// not touch keeps its deployed default.
pub(crate) fn create_guest_service_with(
    bridge_host_socket: std::path::PathBuf,
    local_socket: Option<std::path::PathBuf>,
    bridge_guest_socket: Option<std::path::PathBuf>,
    adjust: impl FnOnce(&mut SpacedGuestServiceConfig),
) -> (SpacedGuestService, CancellationToken) {
    let current = User::from_uid(Uid::current())
        .expect("current user lookup works")
        .expect("current user exists");
    let shutdown = CancellationToken::new();
    let mut config = SpacedGuestServiceConfig {
        host_connector_type: HostConnectorType::Uds(bridge_host_socket),
        local_socket_path: local_socket,
        bridge_socket_path: bridge_guest_socket,
        guest_uptime_source: test_guest_uptime_source,
        default_operation_user: GuestUserName::parse(current.name)
            .expect("current test user is a valid guest user"),
        filesystem_execution: FilesystemExecutionMode::Direct,
        ..Default::default()
    };
    adjust(&mut config);
    let service = SpacedGuestService::with_config(shutdown.clone(), config).expect("guest service");
    (service, shutdown)
}

pub(crate) fn create_ready_guest_service(
    bridge_host_socket: std::path::PathBuf,
    local_socket: Option<std::path::PathBuf>,
    bridge_guest_socket: Option<std::path::PathBuf>,
) -> (SpacedGuestService, CancellationToken) {
    create_ready_guest_service_with(
        bridge_host_socket,
        local_socket,
        bridge_guest_socket,
        |_| {},
    )
}

/// As [`create_ready_guest_service`], with the configuration adjustment
/// [`create_guest_service_with`] takes.
pub(crate) fn create_ready_guest_service_with(
    bridge_host_socket: std::path::PathBuf,
    local_socket: Option<std::path::PathBuf>,
    bridge_guest_socket: Option<std::path::PathBuf>,
    adjust: impl FnOnce(&mut SpacedGuestServiceConfig),
) -> (SpacedGuestService, CancellationToken) {
    let (service, shutdown) = create_guest_service_with(
        bridge_host_socket,
        local_socket,
        bridge_guest_socket,
        adjust,
    );
    service.ready();
    (service, shutdown)
}

pub(crate) async fn connect_uds(socket_path: std::path::PathBuf) -> tonic::transport::Channel {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let path = socket_path.clone();
            let endpoint = tonic::transport::Endpoint::from_static("http://[::]:50051");
            match endpoint
                .connect_with_connector(tower::service_fn(move |_| {
                    let path = path.clone();
                    async move {
                        let stream = UnixStream::connect(path).await?;
                        Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                    }
                }))
                .await
            {
                Ok(channel) => return channel,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out connecting to {}", socket_path.display()))
}
