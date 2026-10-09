use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

use tempfile::tempdir;

#[test]
fn storage_vend_role_is_a_startup_precondition_except_when_disabled() {
    assert!(matches!(
        resolve_storage_vend_role(
            ExchangeTokenMode::Legacy,
            Err(std::env::VarError::NotPresent)
        ),
        Err(RunCredmgrError::MissingStorageVendRole)
    ));
    assert!(matches!(
        resolve_storage_vend_role(
            ExchangeTokenMode::Mixed,
            Ok("arn:aws:iam::123456789012:role/storage\n".to_string()),
        ),
        Err(RunCredmgrError::InvalidStorageVendRole)
    ));
    assert!(
        resolve_storage_vend_role(
            ExchangeTokenMode::Enforce,
            Ok("arn:aws:iam::123456789012:role/storage".to_string()),
        )
        .unwrap()
        .is_some()
    );
    assert!(
        resolve_storage_vend_role(
            ExchangeTokenMode::Disabled,
            Err(std::env::VarError::NotPresent),
        )
        .unwrap()
        .is_none()
    );
}

/// A node-local run configuration with the binary's defaults, so each test
/// names only the field it is about.
fn node_local_config() -> CredmgrRunConfig {
    CredmgrRunConfig {
        role: CredmgrRole::Authority,
        socket_path: PathBuf::from(crate::CREDMGR_SOCKET),
        volume_socket_path: None,
        tcp_listen: None,
        tls_cert: None,
        tls_key: None,
        allow_plaintext: false,
        no_uds: false,
        health_socket_path: None,
        service_mutex_timeout: pplx_space_common_svc::DEFAULT_SERVICE_MUTEX_TIMEOUT,
    }
}

// `run` and `CredmgrRunConfig` are public, so the flag relationships clap
// declares for the binary have to hold for a caller that assembles the config
// in code: the four cases below used to be unrepresentable, and the first
// reached an `unreachable!`.
#[test]
fn no_uds_without_a_tcp_listener_is_a_typed_error() {
    let config = CredmgrRunConfig {
        no_uds: true,
        ..node_local_config()
    };

    assert!(matches!(
        validate_flag_shape(&config),
        Err(RunCredmgrError::NoUdsWithoutTcpListen)
    ));
}

#[test]
fn half_set_tls_material_is_rejected_even_with_allow_plaintext() {
    let base = CredmgrRunConfig {
        tcp_listen: Some("127.0.0.1:9810".parse().expect("socket addr")),
        // A certificate was supplied, so a plaintext listener is never the
        // answer, however permissive the other flags are.
        allow_plaintext: true,
        ..node_local_config()
    };

    for config in [
        CredmgrRunConfig {
            tls_cert: Some(PathBuf::from("/tls/server.crt")),
            ..base.clone()
        },
        CredmgrRunConfig {
            tls_key: Some(PathBuf::from("/tls/server.key")),
            ..base.clone()
        },
    ] {
        assert!(matches!(
            validate_flag_shape(&config),
            Err(RunCredmgrError::IncompleteTlsMaterial)
        ));
        assert!(matches!(
            TcpSecurity::from_config(&config),
            Err(RunCredmgrError::IncompleteTlsMaterial)
        ));
    }
}

#[test]
fn tls_material_without_a_tcp_listener_is_rejected() {
    let config = CredmgrRunConfig {
        tls_cert: Some(PathBuf::from("/tls/server.crt")),
        tls_key: Some(PathBuf::from("/tls/server.key")),
        ..node_local_config()
    };

    assert!(matches!(
        validate_flag_shape(&config),
        Err(RunCredmgrError::TlsWithoutTcpListen)
    ));
}

#[test]
fn the_deployed_flag_shapes_are_accepted() {
    let addr = "127.0.0.1:9810".parse().expect("socket addr");
    let node_local = node_local_config();
    let control_plane_tls = CredmgrRunConfig {
        no_uds: true,
        tcp_listen: Some(addr),
        tls_cert: Some(PathBuf::from("/tls/server.crt")),
        tls_key: Some(PathBuf::from("/tls/server.key")),
        ..node_local_config()
    };
    let local_dev_plaintext = CredmgrRunConfig {
        no_uds: true,
        tcp_listen: Some(addr),
        allow_plaintext: true,
        ..node_local_config()
    };

    for config in [&node_local, &control_plane_tls, &local_dev_plaintext] {
        validate_flag_shape(config).expect("a deployed flag shape must stay valid");
    }
    assert!(TcpSecurity::from_config(&node_local).unwrap().is_none());
    assert!(matches!(
        TcpSecurity::from_config(&control_plane_tls).unwrap(),
        Some((_, TcpSecurity::ServerTls { .. }))
    ));
    assert!(matches!(
        TcpSecurity::from_config(&local_dev_plaintext).unwrap(),
        Some((_, TcpSecurity::Plaintext))
    ));
}

// Without --allow-plaintext and without TLS material, a TCP listener is
// refused rather than silently unencrypted.
#[test]
fn a_bare_tcp_listener_is_refused() {
    let config = CredmgrRunConfig {
        tcp_listen: Some("127.0.0.1:9810".parse().expect("socket addr")),
        ..node_local_config()
    };

    validate_flag_shape(&config).expect("the shape is coherent; the refusal is about security");
    assert!(matches!(
        TcpSecurity::from_config(&config),
        Err(RunCredmgrError::PlaintextTcpRefused { .. })
    ));
}

// The bug this guard prevents, WHEN STRICT: a node-local credmgr left in the
// default `authority` role with no DATABASE_URL binds the Unix socket,
// passes health checks (Ready), and silently fails every crypto/injection
// RPC.
#[test]
fn keyless_node_authority_is_rejected_when_strict() {
    let err = validate_startup_config(
        CredmgrRole::Authority,
        /*no_uds=*/ false,
        /*has_db=*/ false,
        /*strict=*/ true,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("--role agent"),
        "error should point operators at agent mode: {err}"
    );
}

// Without strict mode, a keyless authority remains valid for deployments
// that have not enabled the stricter startup requirements.
#[test]
fn keyless_node_authority_is_allowed_when_not_strict() {
    validate_startup_config(CredmgrRole::Authority, false, false, false).unwrap();
}

#[test]
fn node_agent_needs_no_database() {
    // The node-local agent holds no keys or database; it forwards upstream.
    validate_startup_config(CredmgrRole::Agent, false, false, true).unwrap();
}

#[test]
fn node_authority_with_database_is_allowed() {
    validate_startup_config(CredmgrRole::Authority, false, true, true).unwrap();
}

#[test]
fn control_plane_authority_is_allowed() {
    // The control-plane authority serves TCP only (--no-uds); its database
    // requirement is enforced where DATABASE_URL is consumed, not by this
    // node-local-socket guard.
    validate_startup_config(CredmgrRole::Authority, true, true, true).unwrap();
    validate_startup_config(CredmgrRole::Authority, true, false, true).unwrap();
}

#[tokio::test]
async fn invalid_volume_socket_keeps_predecessor_serving() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let dir = tempdir().unwrap();
    let mutex_path = dir.path().join("service.mutex");
    let socket = dir.path().join("standard.sock");
    let health = dir.path().join("successor-health.sock");
    let predecessor = ServiceMutex::acquire(
        &mutex_path,
        CancellationToken::new(),
        Duration::from_millis(10),
        None,
    )
    .await
    .unwrap();
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let serving = tokio::spawn(async move {
        let (mut connection, _) = listener.accept().await.unwrap();
        connection.write_all(b"predecessor").await.unwrap();
    });
    let config = CredmgrRunConfig {
        socket_path: socket.clone(),
        volume_socket_path: Some(dir.path().join("unprovisioned/volume.sock")),
        health_socket_path: Some(health.clone()),
        ..node_local_config()
    };
    let result = tokio::time::timeout(
        Duration::from_millis(300),
        initialize_node_local(&config, &CancellationToken::new(), &mutex_path, async {
            Ok(())
        }),
    )
    .await;
    // A fresh connection must still reach the old listener, not just an
    // established connection whose pathname could already have been replaced.
    let mut reply = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::net::UnixStream::connect(&socket)
            .await
            .unwrap()
            .read_to_end(&mut reply)
            .await
            .unwrap();
    })
    .await
    .unwrap();
    serving.await.unwrap();
    drop(predecessor);
    assert_eq!(reply, b"predecessor");
    assert!(
        matches!(result, Ok(Err(_))),
        "bad provisioning must fail before waiting for the predecessor"
    );
    assert!(
        !health.exists(),
        "a failed successor must never advertise passive readiness"
    );
}

#[tokio::test]
async fn preflight_completes_before_passive_health_is_exposed() {
    let dir = tempdir().expect("tempdir");
    let mutex_path = dir.path().join("service.mutex");
    let health_socket = dir.path().join("health.sock");
    let config = CredmgrRunConfig {
        health_socket_path: Some(health_socket.clone()),
        service_mutex_timeout: Duration::from_secs(2),
        ..node_local_config()
    };
    let predecessor = ServiceMutex::acquire(
        &mutex_path,
        CancellationToken::new(),
        Duration::from_millis(10),
        None,
    )
    .await
    .expect("predecessor mutex");
    let shutdown_token = CancellationToken::new();
    let task_shutdown_token = shutdown_token.clone();
    let preflight_started = Arc::new(AtomicBool::new(false));
    let task_preflight_started = Arc::clone(&preflight_started);
    let task_mutex_path = mutex_path.clone();

    let startup = tokio::spawn(async move {
        initialize_node_local(
            &config,
            &task_shutdown_token,
            &task_mutex_path,
            async move {
                task_preflight_started.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await
    });

    let health_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while !health_socket.exists() && tokio::time::Instant::now() < health_deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let preflight_started_before_health = preflight_started.load(Ordering::SeqCst);
    shutdown_token.cancel();
    drop(predecessor);
    tokio::time::timeout(Duration::from_secs(1), startup)
        .await
        .expect("startup should observe shutdown")
        .expect("startup task should join")
        .expect("startup should exit cleanly while passive");

    assert!(
        health_socket.exists(),
        "passive health socket was not bound"
    );
    assert!(
        preflight_started_before_health,
        "passive health became ready before startup preflight began"
    );
}

#[tokio::test]
async fn early_server_exit_stops_parent_and_releases_mutex() {
    let dir = tempdir().expect("tempdir");
    let mutex_path = dir.path().join("service.mutex");
    let service_mutex = ServiceMutex::acquire(
        &mutex_path,
        CancellationToken::new(),
        Duration::from_millis(10),
        None,
    )
    .await
    .expect("service mutex");
    let node_local = NodeLocalActivation {
        health_server: LocalHealthServer::disabled(),
        service_mutex,
    };
    let shutdown_token = CancellationToken::new();
    let observed_shutdown_token = shutdown_token.clone();
    let mut servers = JoinSet::new();
    servers.spawn(async { (CredentialManagerServerKind::Uds, Ok(())) });

    let error = tokio::time::timeout(
        Duration::from_millis(300),
        supervise_node_local_servers(
            servers,
            node_local,
            shutdown_token,
            CancellationToken::new(),
        ),
    )
    .await
    .expect("parent ignored an early gRPC server exit")
    .expect_err("an early gRPC server exit should fail the process");

    assert!(error.to_string().contains("exited unexpectedly"));
    assert!(observed_shutdown_token.is_cancelled());
    let replacement = ServiceMutex::acquire(
        &mutex_path,
        CancellationToken::new(),
        Duration::from_millis(10),
        Some(Duration::from_millis(300)),
    )
    .await
    .expect("replacement should acquire the released mutex");
    drop(replacement);
}

#[test]
fn volume_socket_requires_a_distinct_uds_listener() {
    let mut config = node_local_config();
    config.volume_socket_path = Some(config.socket_path.clone());
    assert!(matches!(
        validate_flag_shape(&config),
        Err(RunCredmgrError::InvalidVolumeSocket)
    ));
    config.volume_socket_path = Some(PathBuf::from("/space/run/volume-credentials/credmgr.sock"));
    validate_flag_shape(&config).unwrap();
    config.no_uds = true;
    assert!(matches!(
        validate_flag_shape(&config),
        Err(RunCredmgrError::InvalidVolumeSocket)
    ));
}

#[cfg(target_os = "linux")]
mod volume_socket;
