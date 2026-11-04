use super::*;
use tokio_stream::wrappers::UnixListenerStream;

#[test]
fn sandbox_manager_capability_retry_delay_increases_and_caps() {
    let delays = (0..6)
        .map(sandbox_manager_capability_retry_delay)
        .collect::<Vec<_>>();

    assert_eq!(
        delays,
        [1, 2, 4, 8, 15, 15]
            .map(std::time::Duration::from_secs)
            .to_vec()
    );
}

async fn spawn_sandbox_manager_health(
    socket_path: &Path,
    advertise_output_cursor: bool,
    advertise_operation_receipt: bool,
) -> (HealthReporter, CancellationToken, JoinHandle<()>) {
    let listener = pplx_space_common_svc::bind_unix_socket(
        socket_path,
        pplx_space_common_svc::UnixSocketOptions::new(0o600),
    )
    .unwrap();
    let (reporter, health) = pplx_space_common_svc::grpc_health_service();
    reporter
        .set_service_status("", ReporterServingStatus::Serving)
        .await;
    if advertise_output_cursor {
        reporter
            .set_service_status(
                pplx_space_sbx_mgr_client::PROCESS_OUTPUT_CURSOR_HEALTH_SERVICE,
                ReporterServingStatus::Serving,
            )
            .await;
    }
    if advertise_operation_receipt {
        reporter
            .set_service_status(
                pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
                ReporterServingStatus::Serving,
            )
            .await;
    }
    let shutdown_token = CancellationToken::new();
    let server_shutdown = shutdown_token.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(health)
            .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async move {
                server_shutdown.cancelled().await
            })
            .await
            .unwrap();
    });
    (reporter, shutdown_token, server)
}

#[tokio::test]
async fn node_api_waits_only_for_required_sandbox_manager_capabilities() {
    let dir = tempfile::tempdir().unwrap();
    let sandbox_manager_socket = dir.path().join("sandbox-manager.sock");
    let (reporter, shutdown_token, server) =
        spawn_sandbox_manager_health(&sandbox_manager_socket, false, false).await;
    let health_socket = dir.path().join("node-api-health.sock");
    let node_api_shutdown_token = CancellationToken::new();
    let startup_health_socket = health_socket.clone();
    let startup_sandbox_manager_socket = sandbox_manager_socket.clone();
    let startup_shutdown_token = node_api_shutdown_token.clone();
    let mut startup = tokio::spawn(async move {
        start_local_health_and_await_capabilities(
            auth::NodeRole::Worker,
            Some(&startup_health_socket),
            &startup_sandbox_manager_socket,
            startup_shutdown_token,
        )
        .await
    });

    let early_result =
        tokio::time::timeout(std::time::Duration::from_millis(250), &mut startup).await;

    assert!(
        early_result.is_err(),
        "node-api capability gate exited while an older sandbox-manager was serving"
    );
    assert_eq!(
        local_health_status(&health_socket, LIVENESS_SERVICE).await,
        tonic_health::pb::health_check_response::ServingStatus::Serving
    );
    assert_eq!(
        local_health_status(&health_socket, READINESS_SERVICE).await,
        tonic_health::pb::health_check_response::ServingStatus::NotServing
    );

    reporter
        .set_service_status(
            pplx_space_sbx_mgr_client::PROCESS_OUTPUT_CURSOR_HEALTH_SERVICE,
            ReporterServingStatus::Serving,
        )
        .await;
    let startup = tokio::time::timeout(std::time::Duration::from_secs(10), startup)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let StartupPrelude::Started { health_server } = startup else {
        panic!("node-api startup should complete after the capability appears");
    };
    set_local_readiness(health_server.as_ref(), ReporterServingStatus::Serving).await;

    assert_eq!(
        local_health_status(&health_socket, READINESS_SERVICE).await,
        tonic_health::pb::health_check_response::ServingStatus::Serving
    );
    node_api_shutdown_token.cancel();
    stop_local_health(health_server).await;
    shutdown_token.cancel();
    server.await.unwrap();
}

#[tokio::test]
async fn shutdown_cancels_worker_capability_wait() {
    let dir = tempfile::tempdir().unwrap();
    let sandbox_manager_socket = dir.path().join("sandbox-manager.sock");
    let (_reporter, sandbox_manager_shutdown, sandbox_manager) =
        spawn_sandbox_manager_health(&sandbox_manager_socket, false, false).await;
    let health_socket = dir.path().join("node-api-health.sock");
    let node_api_shutdown = CancellationToken::new();
    let startup_health_socket = health_socket.clone();
    let startup_sandbox_manager_socket = sandbox_manager_socket.clone();
    let startup_shutdown = node_api_shutdown.clone();
    let startup = tokio::spawn(async move {
        start_local_health_and_await_capabilities(
            auth::NodeRole::Worker,
            Some(&startup_health_socket),
            &startup_sandbox_manager_socket,
            startup_shutdown,
        )
        .await
    });

    assert_eq!(
        wait_for_local_health_status(&health_socket, LIVENESS_SERVICE).await,
        tonic_health::pb::health_check_response::ServingStatus::Serving
    );
    node_api_shutdown.cancel();

    let result = tokio::time::timeout(std::time::Duration::from_secs(1), startup)
        .await
        .expect("shutdown should cancel the capability wait")
        .unwrap()
        .unwrap();
    assert!(matches!(result, StartupPrelude::Shutdown));

    sandbox_manager_shutdown.cancel();
    sandbox_manager.await.unwrap();
}

#[tokio::test]
async fn builder_startup_does_not_wait_for_worker_capability() {
    let dir = tempfile::tempdir().unwrap();
    let sandbox_manager_socket = dir.path().join("sandbox-manager.sock");
    let (_reporter, sandbox_manager_shutdown, sandbox_manager) =
        spawn_sandbox_manager_health(&sandbox_manager_socket, false, false).await;
    let health_socket = dir.path().join("node-api-health.sock");
    let node_api_shutdown = CancellationToken::new();

    let startup = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        start_local_health_and_await_capabilities(
            auth::NodeRole::Builder,
            Some(&health_socket),
            &sandbox_manager_socket,
            node_api_shutdown.clone(),
        ),
    )
    .await
    .expect("builder startup should not wait for the worker capability")
    .unwrap();
    let StartupPrelude::Started { health_server } = startup else {
        panic!("builder startup should complete without the worker capability");
    };

    node_api_shutdown.cancel();
    stop_local_health(health_server).await;
    sandbox_manager_shutdown.cancel();
    sandbox_manager.await.unwrap();
}

#[tokio::test]
async fn node_api_accepts_sandbox_manager_with_required_capabilities() {
    let dir = tempfile::tempdir().unwrap();
    let socket_path = dir.path().join("sandbox-manager.sock");
    let (_reporter, shutdown_token, server) =
        spawn_sandbox_manager_health(&socket_path, true, true).await;

    let result = check_sandbox_manager_capabilities(&socket_path).await;

    result.unwrap();
    shutdown_token.cancel();
    server.await.unwrap();
}

#[tokio::test]
async fn node_api_startup_accepts_sandbox_manager_without_operation_receipt_capability() {
    let dir = tempfile::tempdir().unwrap();
    let socket_path = dir.path().join("sandbox-manager.sock");
    let (_reporter, shutdown_token, server) =
        spawn_sandbox_manager_health(&socket_path, true, false).await;

    let result = check_sandbox_manager_capabilities(&socket_path).await;

    result.expect("optional receipt support must not withhold the worker listener");
    shutdown_token.cancel();
    server.await.unwrap();
}

#[tokio::test]
async fn node_api_receipt_capability_tracks_sandbox_manager_support() {
    let dir = tempfile::tempdir().unwrap();
    let sandbox_manager_socket = dir.path().join("sandbox-manager.sock");
    let (manager_reporter, manager_shutdown, manager_server) =
        spawn_sandbox_manager_health(&sandbox_manager_socket, true, false).await;
    let node_health_socket = dir.path().join("node-api-health.sock");
    let node_shutdown = CancellationToken::new();
    let node_health = spawn_local_health(Some(&node_health_socket), node_shutdown.clone())
        .await
        .unwrap();
    let node_reporter = node_health.as_ref().unwrap().0.clone();
    node_reporter
        .set_service_status(
            pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
            ReporterServingStatus::NotServing,
        )
        .await;
    let bridge = spawn_sandbox_manager_capability_bridge(
        pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
        node_reporter,
        sandbox_manager_socket,
        node_shutdown.clone(),
    );

    assert_eq!(
        local_health_status(
            &node_health_socket,
            pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
        )
        .await,
        tonic_health::pb::health_check_response::ServingStatus::NotServing
    );
    manager_reporter
        .set_service_status(
            pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
            ReporterServingStatus::Serving,
        )
        .await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if local_health_status(
                &node_health_socket,
                pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
            )
            .await
                == tonic_health::pb::health_check_response::ServingStatus::Serving
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("node-api must advertise receipt support after sandbox-manager does");

    node_shutdown.cancel();
    bridge.await.unwrap();
    stop_local_health(node_health).await;
    manager_shutdown.cancel();
    manager_server.await.unwrap();
}

#[test]
fn builder_does_not_require_sandbox_manager_capabilities() {
    assert!(!role_requires_sandbox_manager_capabilities(
        auth::NodeRole::Builder
    ));
}

#[test]
fn missing_node_id_file_generates_uuid_v7() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node-id");

    let (node_id, source) = resolve_node_id_with_env(&path, None).unwrap();
    let persisted = std::fs::read_to_string(&path).unwrap();

    assert_eq!(persisted.trim(), node_id);
    assert_eq!(source, path.display().to_string());
    assert_eq!(node_id.chars().nth(14), Some('7'));
}

#[test]
fn existing_node_id_file_is_reused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node-id");
    let expected = uuid::Uuid::now_v7();
    std::fs::write(&path, format!("  {expected}\n")).unwrap();

    let (node_id, source) = resolve_node_id_with_env(&path, None).unwrap();

    assert_eq!(node_id, expected.to_string());
    assert_eq!(source, path.display().to_string());
}

#[test]
fn empty_node_id_file_is_replaced() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node-id");
    std::fs::write(&path, "\n").unwrap();

    let (node_id, source) = resolve_node_id_with_env(&path, None).unwrap();
    let persisted = std::fs::read_to_string(&path).unwrap();

    assert_eq!(persisted.trim(), node_id);
    assert_eq!(source, path.display().to_string());
    assert_eq!(node_id.chars().nth(14), Some('7'));
}

#[test]
fn invalid_node_id_file_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node-id");
    std::fs::write(&path, "not-a-uuid\n").unwrap();

    let err = resolve_node_id_with_env(&path, None).unwrap_err();

    assert!(err.to_string().contains("invalid node-id"));
}

#[test]
fn env_node_id_takes_precedence_over_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node-id");
    std::fs::write(&path, "not-a-uuid\n").unwrap();

    let (node_id, source) =
        resolve_node_id_with_env(&path, Some("  ip-192-0-2-94.ec2.internal\n".to_string()))
            .unwrap();

    assert_eq!(node_id, "ip-192-0-2-94.ec2.internal");
    assert_eq!(source, SPACE_NODE_ID_ENV);
}

#[test]
fn builder_node_id_preserves_direct_node_uuid() {
    let expected = NodeId::new();

    let actual = builder_node_id(&expected.to_string()).unwrap();

    assert_eq!(actual, expected);
}

#[test]
fn builder_node_id_canonicalizes_kubernetes_node_name() {
    let node_name = "ip-192-0-2-94.ec2.internal";

    let actual = builder_node_id(node_name).unwrap();

    assert_eq!(
        actual,
        space_node_id(node_name, SpaceNodeRole::Builder).unwrap()
    );
}

#[test]
fn invalid_listen_address_fails() {
    let err = bind_node_api_listener("not-an-address").unwrap_err();

    assert!(err.to_string().contains("invalid node-api listen address"));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn linux_reuseport_allows_two_node_api_listeners_on_same_addr() {
    let first = bind_node_api_listener_addr("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = first.local_addr().unwrap();
    let second = bind_node_api_listener_addr(addr).unwrap();

    assert_eq!(second.local_addr().unwrap(), addr);
}

async fn local_health_status(
    socket_path: &Path,
    service: &str,
) -> tonic_health::pb::health_check_response::ServingStatus {
    let channel = pplx_space_common_grpc::connect_uds_channel(SERVICE_NAME, socket_path)
        .await
        .unwrap();
    let mut client = tonic_health::pb::health_client::HealthClient::new(channel);
    let response = client
        .check(tonic_health::pb::HealthCheckRequest {
            service: service.to_string(),
        })
        .await
        .unwrap();
    tonic_health::pb::health_check_response::ServingStatus::try_from(response.into_inner().status)
        .unwrap()
}

async fn wait_for_local_health_status(
    socket_path: &Path,
    service: &str,
) -> tonic_health::pb::health_check_response::ServingStatus {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if socket_path.exists() {
                return local_health_status(socket_path, service).await;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("local health server should start")
}

#[tokio::test]
async fn local_health_readiness_requires_explicit_activation() {
    let dir = tempfile::tempdir().unwrap();
    let health_socket = dir.path().join("probe.sock");
    let shutdown_token = CancellationToken::new();
    let health_server = spawn_local_health(Some(&health_socket), shutdown_token.clone())
        .await
        .unwrap();

    assert_eq!(
        local_health_status(&health_socket, "").await,
        tonic_health::pb::health_check_response::ServingStatus::NotServing
    );
    assert_eq!(
        local_health_status(&health_socket, READINESS_SERVICE).await,
        tonic_health::pb::health_check_response::ServingStatus::NotServing
    );
    assert_eq!(
        local_health_status(&health_socket, LIVENESS_SERVICE).await,
        tonic_health::pb::health_check_response::ServingStatus::Serving
    );

    set_local_readiness(health_server.as_ref(), ReporterServingStatus::Serving).await;

    assert_eq!(
        local_health_status(&health_socket, "").await,
        tonic_health::pb::health_check_response::ServingStatus::Serving
    );
    assert_eq!(
        local_health_status(&health_socket, READINESS_SERVICE).await,
        tonic_health::pb::health_check_response::ServingStatus::Serving
    );

    shutdown_token.cancel();
    stop_local_health(health_server).await;
}

fn bind_without_reuse_port(addr: SocketAddr) -> std::io::Result<socket2::Socket> {
    let domain = if addr.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let socket = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1)?;
    Ok(socket)
}

#[tokio::test]
async fn shutdown_releases_listener_while_accepted_connections_drain() {
    let listener = bind_node_api_listener_addr("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown_token = CancellationToken::new();
    let server_shutdown = shutdown_token.clone();
    let (_, health_service) = pplx_space_common_svc::grpc_health_service();
    struct PlaintextDev;
    impl pplx_space_common_core::env::Environment for PlaintextDev {
        fn var_os(&self, name: &str) -> Option<std::ffi::OsString> {
            (name == pplx_space_common_grpc::internal_transport::INSECURE_DEV_ENV)
                .then(|| "true".into())
        }
    }
    let mut builder =
        pplx_space_common_grpc::internal_transport::server_builder_from_environment(&PlaintextDev)
            .unwrap();
    // Use the same incoming stream as `run` so shutdown releases the listener before draining.
    let server = tokio::spawn(async move {
        builder
            .add_service(health_service)
            .serve_with_incoming_shutdown(
                pplx_space_common_grpc::internal_transport::Incoming::new(
                    TcpIncoming::from(listener).with_nodelay(Some(true)),
                    server_shutdown,
                ),
                std::future::pending(),
            )
            .await
            .unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = tonic_health::pb::health_client::HealthClient::new(channel);
    let mut watch = client
        .watch(tonic_health::pb::HealthCheckRequest {
            service: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    watch.message().await.unwrap().unwrap();

    shutdown_token.cancel();

    assert!(!server.is_finished());
    let rebound = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            match bind_without_reuse_port(addr) {
                Ok(listener) => break listener,
                Err(error) if error.kind() == ErrorKind::AddrInUse => {
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("failed to bind replacement listener: {error}"),
            }
        }
    })
    .await
    .expect("shutdown should release the listener before accepted connections drain");

    drop(watch);
    drop(client);
    drop(rebound);
    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}
