use super::*;

fn check_request(service: &str) -> Request<HealthCheckRequest> {
    Request::new(HealthCheckRequest {
        service: service.to_string(),
    })
}

async fn status(health: &LocalHealth, service: &str) -> ServingStatus {
    let response = health.check(check_request(service)).await.unwrap();
    ServingStatus::try_from(response.into_inner().status).unwrap()
}

#[tokio::test]
async fn passive_readiness_is_serving() {
    let (health, _activator) = local_health_pair("test");
    assert_eq!(status(&health, "").await, ServingStatus::Serving);
    assert_eq!(status(&health, "readiness").await, ServingStatus::Serving);
}

#[tokio::test]
async fn liveness_is_serving_in_every_state() {
    let (health, activator) = local_health_pair("test");
    assert_eq!(status(&health, "liveness").await, ServingStatus::Serving);
    activator.mark_starting();
    assert_eq!(status(&health, "liveness").await, ServingStatus::Serving);
    activator.activate(PathBuf::from("/nonexistent/socket"));
    // Active mode: liveness still Serving even though production probe
    // would fail, because liveness does not depend on production.
    assert_eq!(status(&health, "liveness").await, ServingStatus::Serving);
}

#[tokio::test]
async fn active_liveness_opt_in_probes_production() {
    let (health, activator) =
        local_health_pair_with_liveness("test", LocalHealthLiveness::ProductionWhenActive);

    assert_eq!(status(&health, "liveness").await, ServingStatus::Serving);
    activator.mark_starting();
    assert_eq!(status(&health, "liveness").await, ServingStatus::Serving);
    activator.activate(PathBuf::from("/nonexistent/socket-that-does-not-exist"));
    assert_eq!(status(&health, "liveness").await, ServingStatus::NotServing);
}

#[tokio::test]
async fn starting_readiness_is_not_serving() {
    let (health, activator) = local_health_pair("test");
    activator.mark_starting();
    assert_eq!(status(&health, "").await, ServingStatus::NotServing);
    assert_eq!(
        status(&health, "readiness").await,
        ServingStatus::NotServing
    );
}

#[tokio::test]
async fn active_readiness_probes_production() {
    // Production socket points at a nonexistent path; the probe should
    // fail and the handler should map that to NotServing rather than
    // bubbling an error.
    let (health, activator) = local_health_pair("test");
    activator.mark_starting();
    activator.activate(PathBuf::from("/nonexistent/socket-that-does-not-exist"));
    assert_eq!(status(&health, "").await, ServingStatus::NotServing);
}

#[tokio::test]
async fn unknown_service_is_not_found() {
    let (health, _activator) = local_health_pair("test");
    let err = health.check(check_request("nope")).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
}

async fn check_over_socket(socket_path: &Path, service: &str) -> ServingStatus {
    let channel = connect_uds_channel("local-health-test", socket_path)
        .await
        .unwrap();
    let mut client = HealthClient::new(channel);
    let response = client
        .check(HealthCheckRequest {
            service: service.to_string(),
        })
        .await
        .unwrap();
    ServingStatus::try_from(response.into_inner().status).unwrap()
}

// The bundled node-services DaemonSet points startup and readiness probes at
// this socket. A passive successor (service mutex still held by a
// predecessor) must answer Serving for the default service from
// `spawn_optional` onward — before `activate` — or the successor pod can
// never pass its probes while it waits for the mutex.
#[tokio::test]
async fn spawned_server_serves_default_and_liveness_while_passive() {
    let dir = tempfile::tempdir().unwrap();
    let socket_path = dir.path().join("probe.sock");
    let shutdown_token = CancellationToken::new();
    let server = LocalHealthServer::spawn_optional(
        Some(&socket_path),
        UnixSocketOptions::new(0o600),
        "local-health-test",
        shutdown_token.clone(),
    )
    .unwrap();

    assert_eq!(
        check_over_socket(&socket_path, "").await,
        ServingStatus::Serving
    );
    assert_eq!(
        check_over_socket(&socket_path, "readiness").await,
        ServingStatus::Serving
    );
    assert_eq!(
        check_over_socket(&socket_path, "liveness").await,
        ServingStatus::Serving
    );

    shutdown_token.cancel();
    server.join().await;
}

// Full lifecycle over the socket: passive → mark_starting (readiness drops,
// liveness holds) → activate against a Serving production socket (readiness
// forwards and returns Serving again).
#[tokio::test]
async fn spawned_server_lifecycle_over_socket() {
    for initially_waiting in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("probe.sock");
        let production_path = dir.path().join("production.sock");
        let shutdown_token = CancellationToken::new();

        let spawn = if initially_waiting {
            LocalHealthServer::spawn_optional_waiting
        } else {
            LocalHealthServer::spawn_optional
        };
        let server = spawn(
            Some(&socket_path),
            UnixSocketOptions::new(0o600),
            "local-health-test",
            shutdown_token.clone(),
        )
        .unwrap();

        if initially_waiting {
            assert_eq!(
                check_over_socket(&socket_path, "").await,
                ServingStatus::NotServing
            );
            assert_eq!(
                check_over_socket(&socket_path, "readiness").await,
                ServingStatus::NotServing
            );
            assert_eq!(
                check_over_socket(&socket_path, "liveness").await,
                ServingStatus::Serving
            );
            server.mark_takeover_ready();
        }

        let listener = bind_unix_socket(&production_path, UnixSocketOptions::new(0o600)).unwrap();
        let (reporter, health) = crate::bootstrap::grpc_health_service();
        reporter
            .set_service_status("", tonic_health::ServingStatus::Serving)
            .await;
        let production_shutdown = shutdown_token.clone();
        let production = tokio::spawn(async move {
            grpc_server_builder()
                .add_service(health)
                .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async move {
                    production_shutdown.cancelled().await
                })
                .await
                .unwrap();
        });

        assert_eq!(
            check_over_socket(&socket_path, "").await,
            ServingStatus::Serving
        );

        server.mark_starting();
        assert_eq!(
            check_over_socket(&socket_path, "").await,
            ServingStatus::NotServing
        );
        assert_eq!(
            check_over_socket(&socket_path, "liveness").await,
            ServingStatus::Serving
        );

        server.activate(production_path.clone());
        assert_eq!(
            check_over_socket(&socket_path, "").await,
            ServingStatus::Serving
        );
        assert_eq!(
            check_over_socket(&socket_path, "readiness").await,
            ServingStatus::Serving
        );
        assert_eq!(
            check_over_socket(&socket_path, "liveness").await,
            ServingStatus::Serving
        );

        shutdown_token.cancel();
        server.join().await;
        production.await.unwrap();
    }
}

#[tokio::test]
async fn disabled_server_lifecycle_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let socket_path = dir.path().join("probe.sock");
    let server = LocalHealthServer::disabled();

    server.mark_takeover_ready();
    server.mark_starting();
    server.activate(socket_path.clone());
    server.join().await;

    assert!(!socket_path.exists());
}
