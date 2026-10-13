use crate::guest_meta::{connect_bridge_guest, connect_local_guest, duration_from_millis};
use crate::guest_service::{
    connect_uds, create_guest_service, create_ready_guest_service, create_ready_guest_service_with,
    start_mock_bridge_host,
};
use pplx_space_spaced_common::proto::bridge_guest::filesystem_client::FilesystemClient as BridgeGuestFilesystemClient;
use pplx_space_spaced_common::proto::bridge_guest::meta_client::MetaClient as BridgeGuestClient;
use pplx_space_spaced_common::proto::bridge_guest::{
    PingRequest as BridgeGuestPingRequest, StatRequest,
    WaitReadyRequest as BridgeGuestWaitReadyRequest,
};
use pplx_space_spaced_guest_client::proto::{
    ActivityState, PingRequest as LocalPingRequest,
    RequestActiveForRequest as LocalRequestActiveForRequest,
    RequestShutdownRequest as LocalShutdownRequest,
    SetActivityStateRequest as LocalSetActivityStateRequest,
};
use std::os::unix::fs::PermissionsExt as _;

#[tokio::test]
async fn bridge_guest_wait_ready_returns_guest_readiness() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_host_socket = temp_dir.path().join("spaced-bridge-host.sock");
    let bridge_guest_socket = temp_dir.path().join("spaced-bridge-guest.sock");
    start_mock_bridge_host(bridge_host_socket.clone()).await;
    let (service, shutdown) =
        create_ready_guest_service(bridge_host_socket, None, Some(bridge_guest_socket.clone()));
    let service_task = tokio::spawn(service.run());
    let mut client = connect_bridge_guest(bridge_guest_socket).await;
    client
        .ping(BridgeGuestPingRequest::default())
        .await
        .expect("bridge guest ping response");

    let response = client
        .wait_ready(BridgeGuestWaitReadyRequest {
            timeout: Some(duration_from_millis(100)),
        })
        .await
        .expect("wait ready response")
        .into_inner();

    assert!(response.ready);
    assert_eq!(
        response.guest_uptime,
        Some(crate::guest_service::test_guest_uptime())
    );
    assert!(!response.version.is_empty());
    assert!(!response.compatibility_family.is_empty());

    shutdown.cancel();
    service_task
        .await
        .expect("service joined")
        .expect("service stopped");
}

#[tokio::test]
async fn local_request_shutdown_returns_opaque_when_bridge_host_ignores() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_host_socket = temp_dir.path().join("spaced-bridge-host.sock");
    let local_socket = temp_dir.path().join("spaced.sock");
    start_mock_bridge_host(bridge_host_socket.clone()).await;
    let (service, shutdown) =
        create_guest_service(bridge_host_socket, Some(local_socket.clone()), None);
    let service_task = tokio::spawn(service.run());
    let mut client = connect_local_guest(local_socket).await;

    let response = client
        .request_shutdown(LocalShutdownRequest {
            reason: Some("test".to_string()),
        })
        .await
        .expect("shutdown response")
        .into_inner();
    assert!(response.outcome.is_none());

    shutdown.cancel();
    service_task
        .await
        .expect("service joined")
        .expect("service stopped");
}

#[tokio::test]
async fn local_guest_socket_allows_guest_user_clients() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_host_socket = temp_dir.path().join("spaced-bridge-host.sock");
    let local_socket = temp_dir.path().join("spaced.sock");
    start_mock_bridge_host(bridge_host_socket.clone()).await;
    let (service, shutdown) =
        create_guest_service(bridge_host_socket, Some(local_socket.clone()), None);
    let service_task = tokio::spawn(service.run());
    let mut client = connect_local_guest(local_socket.clone()).await;

    client
        .ping(LocalPingRequest {})
        .await
        .expect("local guest ping response");
    let mode = std::fs::metadata(&local_socket)
        .expect("local guest socket metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o666);

    shutdown.cancel();
    service_task
        .await
        .expect("service joined")
        .expect("service stopped");
}

#[tokio::test]
async fn local_activity_calls_relay_requests_and_preserve_opaque_responses() {
    use crate::guest_service::BridgeHostCall;
    use pplx_space_spaced_common::proto::bridge_host;
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_host_socket = temp_dir.path().join("spaced-bridge-host.sock");
    let local_socket = temp_dir.path().join("spaced.sock");
    let (host, calls) = start_mock_bridge_host(bridge_host_socket.clone()).await;
    let (service, shutdown) =
        create_guest_service(bridge_host_socket, Some(local_socket.clone()), None);
    let service_task = tokio::spawn(service.run());
    let mut client = connect_local_guest(local_socket).await;
    let state = ActivityState::Idle as i32;
    let hold_for = Some(duration_from_millis(500));
    assert!(
        client
            .set_activity_state(LocalSetActivityStateRequest { state })
            .await
            .unwrap()
            .into_inner()
            .outcome
            .is_none()
    );
    assert!(
        client
            .request_active_for(LocalRequestActiveForRequest { hold_for })
            .await
            .unwrap()
            .into_inner()
            .outcome
            .is_none()
    );
    assert_eq!(
        *calls.lock().unwrap(),
        [
            BridgeHostCall::Activity(bridge_host::SetActivityStateRequest { state }),
            BridgeHostCall::ActiveFor(bridge_host::RequestActiveForRequest { hold_for }),
        ]
    );
    shutdown.cancel();
    service_task.await.unwrap().unwrap();
    host.abort();
    assert!(host.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn request_shutdown_rejects_empty_reason() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_host_socket = temp_dir.path().join("spaced-bridge-host.sock");
    let local_socket = temp_dir.path().join("spaced.sock");
    let bridge_guest_socket = temp_dir.path().join("spaced-bridge-guest.sock");
    start_mock_bridge_host(bridge_host_socket.clone()).await;
    let (service, shutdown) = create_guest_service(
        bridge_host_socket,
        Some(local_socket.clone()),
        Some(bridge_guest_socket.clone()),
    );
    let service_task = tokio::spawn(service.run());

    let mut local_client = connect_local_guest(local_socket).await;
    let local_error = local_client
        .request_shutdown(LocalShutdownRequest {
            reason: Some(String::new()),
        })
        .await
        .expect_err("empty local reason rejected");
    assert_eq!(local_error.code(), tonic::Code::InvalidArgument);

    shutdown.cancel();
    service_task
        .await
        .expect("service joined")
        .expect("service stopped");
}

#[tokio::test]
async fn bridge_guest_data_plane_accepts_after_bridge_listener_is_ready() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_host_socket = temp_dir.path().join("spaced-bridge-host.sock");
    let bridge_guest_socket = temp_dir.path().join("spaced-bridge-guest.sock");
    start_mock_bridge_host(bridge_host_socket.clone()).await;
    let (service, shutdown) =
        create_guest_service(bridge_host_socket, None, Some(bridge_guest_socket.clone()));
    let service_task = tokio::spawn(service.run());
    let channel = connect_uds(bridge_guest_socket).await;
    let mut client = BridgeGuestFilesystemClient::new(channel);

    let response = client
        .stat_info(StatRequest {
            path: temp_dir.path().display().to_string(),
            user: None,
        })
        .await
        .expect("data plane accepted after listener readiness")
        .into_inner()
        .info
        .expect("file info");
    assert_eq!(
        response.file_type(),
        pplx_space_spaced_common::proto::bridge_guest::FileType::Directory
    );

    shutdown.cancel();
    service_task
        .await
        .expect("service joined")
        .expect("service stopped");
}

#[tokio::test]
async fn bridge_guest_notify_resume_rejects_invalid_host_time() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_host_socket = temp_dir.path().join("spaced-bridge-host.sock");
    let bridge_guest_socket = temp_dir.path().join("spaced-bridge-guest.sock");
    start_mock_bridge_host(bridge_host_socket.clone()).await;
    let (service, shutdown) =
        create_ready_guest_service(bridge_host_socket, None, Some(bridge_guest_socket.clone()));
    let service_task = tokio::spawn(service.run());
    let channel = connect_uds(bridge_guest_socket).await;
    let mut client = BridgeGuestClient::new(channel);

    for host_time in [
        None,
        Some(prost_types::Timestamp {
            seconds: 0,
            nanos: -1,
        }),
        Some(prost_types::Timestamp {
            seconds: 0,
            nanos: 1_000_000_000,
        }),
    ] {
        let error = client
            .notify_resume(
                pplx_space_spaced_common::proto::bridge_guest::NotifyResumeRequest {
                    reason:
                        pplx_space_spaced_common::proto::bridge_guest::ResumeReason::SnapshotRestore
                            .into(),
                    host_time,
                },
            )
            .await
            .expect_err("invalid host time rejected");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    shutdown.cancel();
    service_task
        .await
        .expect("service joined")
        .expect("service stopped");
}

#[tokio::test]
async fn bridge_guest_recycles_incomplete_http2_connections() {
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    use tokio::net::UnixStream;

    // The whole point of this test is waiting out a preface deadline, so it
    // is the test's floor. Shorten it, and keep the window for filling
    // admission strictly inside it so no stalled peer can expire while the
    // fixture is still opening them.
    const PREFACE_TIMEOUT: Duration = Duration::from_secs(2);
    const FILL_BUDGET: Duration = Duration::from_secs(1);

    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let host_socket = temp_dir.path().join("host.sock");
    let guest_socket = temp_dir.path().join("guest.sock");
    start_mock_bridge_host(host_socket.clone()).await;
    let (service, shutdown) =
        create_ready_guest_service_with(host_socket, None, Some(guest_socket.clone()), |config| {
            config.http2_preface_timeout = PREFACE_TIMEOUT;
        });
    let service_task = tokio::spawn(service.run());
    let mut healthy = connect_bridge_guest(guest_socket.clone()).await;
    healthy
        .ping(BridgeGuestPingRequest::default())
        .await
        .expect("healthy ping");

    // One healthy connection plus 63 stalled peers fills the bridge's 64 slots.
    // Receiving the server SETTINGS header proves each peer was admitted.
    let stalled = tokio::time::timeout(FILL_BUDGET, async {
        let mut peers = Vec::new();
        for _ in 0..63 {
            let mut peer = UnixStream::connect(&guest_socket)
                .await
                .expect("raw connect");
            let mut settings_header = [0; 9];
            peer.read_exact(&mut settings_header)
                .await
                .expect("server SETTINGS");
            peers.push(peer);
        }
        peers
    })
    .await
    .expect("fill admission before preface deadlines expire");

    tokio::time::timeout(Duration::from_secs(10), async {
        let channel = tonic::transport::Endpoint::from_static("http://localhost")
            .connect_with_connector(tower::service_fn(move |_| {
                let path = guest_socket.clone();
                async move {
                    UnixStream::connect(path)
                        .await
                        .map(hyper_util::rt::TokioIo::new)
                }
            }))
            .await
            .expect("connection recycled while stalled peers remain open");
        BridgeGuestClient::new(channel)
            .ping(BridgeGuestPingRequest::default())
            .await
            .expect("new guest ping");
    })
    .await
    .expect("preface expiry releases admission capacity");
    healthy
        .ping(BridgeGuestPingRequest::default())
        .await
        .expect("healthy peer preserved");
    drop(stalled);
    drop(healthy);
    shutdown.cancel();
    service_task
        .await
        .expect("service joined")
        .expect("service stopped");
}

/// A guest restored from a snapshot forgets the diagnostics it was recorded
/// with: they fired in the sandbox the snapshot came from. A suspend-resume of
/// the same sandbox keeps them.
#[tokio::test]
async fn bridge_guest_snapshot_restore_discards_recorded_diagnostics() {
    use pplx_space_spaced_common::proto::bridge_guest::{NotifyResumeRequest, ResumeReason};
    use tracing_subscriber::layer::SubscriberExt as _;

    let _subscriber = tracing::subscriber::set_default(
        tracing_subscriber::registry()
            .with(pplx_space_observability::warning_tap::layer_for_test()),
    );
    pplx_space_observability::warning_tap::enable();
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_host_socket = temp_dir.path().join("spaced-bridge-host.sock");
    let bridge_guest_socket = temp_dir.path().join("spaced-bridge-guest.sock");
    start_mock_bridge_host(bridge_host_socket.clone()).await;
    let (service, shutdown) =
        create_ready_guest_service(bridge_host_socket, None, Some(bridge_guest_socket.clone()));
    let service_task = tokio::spawn(service.run());
    let mut client = BridgeGuestClient::new(connect_uds(bridge_guest_socket).await);

    for (reason, retained) in [
        (ResumeReason::SuspendResume, true),
        (ResumeReason::SnapshotRestore, false),
    ] {
        tracing::warn!("recorded before the guest resumed");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch");
        client
            .notify_resume(NotifyResumeRequest {
                reason: reason.into(),
                host_time: Some(prost_types::Timestamp {
                    seconds: i64::try_from(now.as_secs()).expect("seconds fit"),
                    nanos: 0,
                }),
            })
            .await
            .expect("notify resume");
        let drained = client
            .ping(BridgeGuestPingRequest {
                drain_diagnostics: true,
            })
            .await
            .expect("drain")
            .into_inner();
        // Only this test's callsite counts: an unprivileged clock sync warns
        // after the discard.
        let ours = drained
            .diagnostics
            .iter()
            .any(|callsite| callsite.file == file!());
        assert_eq!(ours, retained, "{reason:?}");
    }

    shutdown.cancel();
    service_task
        .await
        .expect("service joined")
        .expect("service stopped");
}
