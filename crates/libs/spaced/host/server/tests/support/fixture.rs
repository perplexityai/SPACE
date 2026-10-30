use pplx_space_spaced_host_client::SpacedHostClientConfig;
use pplx_space_spaced_host_server::{
    BridgeGuestConnector, SpacedHostPeerAuthorization, SpacedHostService, SpacedHostServiceConfig,
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub(crate) struct HostFixture {
    _directory: tempfile::TempDir,
    _fallback_directory: Option<tempfile::TempDir>,
    pub(crate) socket_path: std::path::PathBuf,
    shutdown: CancellationToken,
    service_task: tokio::task::JoinHandle<
        Result<(), pplx_space_spaced_host_server::RunSpacedHostServerError>,
    >,
}

impl HostFixture {
    pub(crate) async fn start(bridge_guest_socket_path: Option<std::path::PathBuf>) -> Self {
        let fallback = bridge_guest_socket_path
            .is_none()
            .then(|| tempfile::tempdir().expect("fallback directory"));
        let bridge_guest_socket_path = bridge_guest_socket_path.unwrap_or_else(|| {
            fallback
                .as_ref()
                .expect("fallback")
                .path()
                .join("spaced-bridge-guest.sock")
        });
        let mut fixture =
            Self::start_with_connector(BridgeGuestConnector::Uds(bridge_guest_socket_path)).await;
        fixture._fallback_directory = fallback;
        fixture
    }

    pub(crate) async fn start_with_connector(bridge_guest_connector: BridgeGuestConnector) -> Self {
        let temp_dir = tempfile::tempdir().expect("temp dir created");
        let temp_path = temp_dir.path();
        let socket_path = temp_path.join("spaced-host.sock");
        let bridge_host_socket_path = temp_path.join("spaced-bridge-host.sock");
        let host_control_socket_path = temp_path.join("sandbox-host-control.sock");
        let shutdown = CancellationToken::new();
        let service = SpacedHostService::new_with_config(
            shutdown.clone(),
            SpacedHostServiceConfig {
                process_recovery_activation: None,
                socket_path: socket_path.clone(),
                bridge_host_socket_path: bridge_host_socket_path.clone(),
                bridge_guest_connector,
                host_control_socket_path,
                host_peer_authorization: SpacedHostPeerAuthorization::Disabled,
            },
        );
        let service_task = tokio::spawn(service.run());
        Self {
            _directory: temp_dir,
            _fallback_directory: None,
            socket_path,
            shutdown,
            service_task,
        }
    }

    pub(crate) async fn client(&self) -> pplx_space_spaced_host_client::SpacedHostClient {
        connect_client(self.socket_path.clone()).await
    }

    pub(crate) async fn stop(self) {
        self.shutdown.cancel();
        self.service_task.abort();
        let _cancelled = self.service_task.await;
    }

    pub(crate) async fn stop_gracefully(self) {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), self.service_task)
            .await
            .expect("host drained")
            .expect("host task")
            .expect("host stopped");
    }
}

async fn connect_client(
    socket_path: std::path::PathBuf,
) -> pplx_space_spaced_host_client::SpacedHostClient {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let config = SpacedHostClientConfig::new().with_socket_path(socket_path.clone());
            match pplx_space_spaced_host_client::SpacedHostClient::new_with_config(config).await {
                Ok(client) => return client,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out connecting to {}", socket_path.display()))
}
