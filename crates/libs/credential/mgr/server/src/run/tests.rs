use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

use tempfile::tempdir;

#[test]
fn storage_vend_role_is_optional_but_must_be_valid_when_set() {
    assert!(
        resolve_storage_vend_role(Err(std::env::VarError::NotPresent))
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        resolve_storage_vend_role(Ok("arn:aws:iam::123456789012:role/storage\n".to_string())),
        Err(RunCredmgrError::InvalidStorageVendRole)
    ));
    assert!(
        resolve_storage_vend_role(Ok("arn:aws:iam::123456789012:role/storage".to_string()))
            .unwrap()
            .is_some()
    );
}

/// Upgrading an authority that kept the retired `disabled` emergency stop must
/// not silently start issuing; every other retained or absent value is inert.
#[test]
fn retired_disabled_mode_with_a_vend_role_fails_startup() {
    assert!(matches!(
        check_retired_exchange_token_mode(Ok("disabled".to_string()), true),
        Err(RunCredmgrError::RetiredExchangeDisabled)
    ));
    check_retired_exchange_token_mode(Ok("disabled".to_string()), false).unwrap();
    for retained in ["legacy", "mixed", "enforce"] {
        check_retired_exchange_token_mode(Ok(retained.to_string()), true).unwrap();
    }
    check_retired_exchange_token_mode(Err(std::env::VarError::NotPresent), true).unwrap();
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

/// The longest backoff the standard connect policy waits between attempts,
/// which the agent's longer wait keeps.
fn longest_standard_backoff() -> Duration {
    let standard = pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY;
    standard.max_delay.mul_f64(1.0 + standard.jitter_factor)
}

/// The attempt cap the stand-in authority's attempts run under.
const STAND_IN_ATTEMPT_CAP: Duration = Duration::from_millis(9500);

/// What a stand-in authority's dial does before the authority is reachable.
#[derive(Clone, Copy)]
enum Unreachable {
    /// Fails at once, like a refused dial.
    Refuses,
    /// Never finishes, like a SYN no one answers, until the attempt cap.
    Stalls,
}

/// A stand-in for the agent's upstream authority whose attempts run through
/// the real connect retry loop, under the agent's policy, so the timing of
/// the wait does not depend on when the kernel completes a dial. Attempts
/// started from `reachable_after` on succeed at once.
struct StandInAuthority {
    started: tokio::time::Instant,
    reachable_after: Option<Duration>,
    unreachable: Unreachable,
    attempts: std::sync::Mutex<Vec<Duration>>,
}

impl StandInAuthority {
    fn new(reachable_after: Option<Duration>, unreachable: Unreachable) -> Self {
        Self {
            started: tokio::time::Instant::now(),
            reachable_after,
            unreachable,
            attempts: std::sync::Mutex::default(),
        }
    }

    /// When each attempt started, since the stand-in was created.
    fn attempt_starts(&self) -> Vec<Duration> {
        self.attempts.lock().unwrap().clone()
    }
}

impl AgentUpstream for StandInAuthority {
    type Client = Duration;

    /// Connects to "when the attempt that connected started".
    async fn connect(
        &self,
        retry_policy: pplx_space_common_grpc::GrpcRetryPolicy,
        on_retry: impl FnMut(&pplx_space_common_grpc::TcpConnectRetry<'_>),
    ) -> Result<Duration, pplx_space_credmgr_client::ConnectError> {
        pplx_space_common_grpc::test_support::retry_tcp_connect(
            "stand-in-authority",
            "stand-in:9810",
            retry_policy,
            STAND_IN_ATTEMPT_CAP,
            || {
                let started = self.started.elapsed();
                self.attempts.lock().unwrap().push(started);
                let reachable = self.reachable_after.is_some_and(|after| started >= after);
                let unreachable = self.unreachable;
                async move {
                    if reachable {
                        return Ok(started);
                    }
                    if let Unreachable::Stalls = unreachable {
                        std::future::pending::<()>().await;
                    }
                    Err(pplx_space_common_grpc::TcpConnectError::AttemptTimeout {
                        service_name: "stand-in-authority",
                        target: "stand-in:9810".to_owned(),
                        cap: Duration::ZERO,
                    })
                }
            },
            on_retry,
        )
        .await
        .map_err(pplx_space_credmgr_client::ConnectError::from)
    }
}

/// Every gap between two consecutive attempt starts, for attempts that
/// fail at once.
fn attempt_gaps(starts: &[Duration]) -> Vec<Duration> {
    starts.windows(2).map(|pair| pair[1] - pair[0]).collect()
}

// An authority that comes up well past the standard connect budget is still
// connected, by the first attempt after it comes up, and the agent redials
// at the standard policy's pace throughout, so it connects within one
// standard backoff of the authority becoming reachable.
#[tokio::test(start_paused = true)]
async fn agent_connects_to_an_authority_that_comes_up_late() {
    let reachable_after = Duration::from_secs(30);
    let authority = StandInAuthority::new(Some(reachable_after), Unreachable::Refuses);

    let connected = connect_agent_upstream(&authority, &CancellationToken::new(), |_| {})
        .await
        .expect("the agent must keep waiting for the authority")
        .expect("no shutdown was requested");

    assert!(
        connected >= reachable_after && connected - reachable_after <= longest_standard_backoff(),
        "connected by the attempt started at {connected:?}"
    );
    let gaps = attempt_gaps(&authority.attempt_starts());
    assert!(
        gaps.iter().all(|gap| *gap <= longest_standard_backoff()),
        "{gaps:?}"
    );
}

// A dial already in flight when the authority comes up has to run into its
// bound before the redial connects, so the agent connects within one attempt
// cap and one backoff of the authority becoming reachable.
#[tokio::test(start_paused = true)]
async fn agent_connects_one_attempt_cap_after_a_dial_in_flight() {
    let reachable_after = Duration::from_secs(30);
    let authority = StandInAuthority::new(Some(reachable_after), Unreachable::Stalls);

    let connected = connect_agent_upstream(&authority, &CancellationToken::new(), |_| {})
        .await
        .expect("the agent must keep waiting for the authority")
        .expect("no shutdown was requested");

    assert!(
        connected >= reachable_after
            && connected - reachable_after <= STAND_IN_ATTEMPT_CAP + longest_standard_backoff(),
        "connected by the attempt started at {connected:?}"
    );
}

// An authority that never comes up ends the agent's startup with a typed error
// that names the window. With attempts that fail at once, the last one starts
// inside the window's last backoff and the call ends with it.
#[tokio::test(start_paused = true)]
async fn agent_gives_up_on_an_unreachable_authority_after_the_window() {
    let authority = StandInAuthority::new(None, Unreachable::Refuses);

    let started = tokio::time::Instant::now();
    let error = connect_agent_upstream(&authority, &CancellationToken::new(), |_| {})
        .await
        .expect_err("an authority that never comes up must not connect");
    let elapsed = started.elapsed();

    assert!(
        matches!(
            error,
            RunCredmgrError::AgentUpstreamConnect { window, .. }
                if window == AGENT_UPSTREAM_CONNECT_WINDOW
        ),
        "{error:?}"
    );
    assert!(
        error
            .to_string()
            .contains(&format!("{AGENT_UPSTREAM_CONNECT_WINDOW:?}")),
        "{error}"
    );
    assert!(
        elapsed > AGENT_UPSTREAM_CONNECT_WINDOW - longest_standard_backoff()
            && elapsed <= AGENT_UPSTREAM_CONNECT_WINDOW,
        "gave up after {elapsed:?}"
    );
    let gaps = attempt_gaps(&authority.attempt_starts());
    assert!(
        gaps.iter().all(|gap| *gap <= longest_standard_backoff()),
        "{gaps:?}"
    );
}

// The window bounds when the last attempt starts, not when it ends: an
// attempt that stalls runs to its cap, so the call ends within the window
// plus one attempt cap.
#[tokio::test(start_paused = true)]
async fn the_last_attempt_starts_within_the_window_and_may_run_to_its_cap() {
    let authority = StandInAuthority::new(None, Unreachable::Stalls);

    let started = tokio::time::Instant::now();
    let error = connect_agent_upstream(&authority, &CancellationToken::new(), |_| {})
        .await
        .expect_err("an authority that never comes up must not connect");
    let elapsed = started.elapsed();

    assert!(
        matches!(error, RunCredmgrError::AgentUpstreamConnect { .. }),
        "{error:?}"
    );
    let last_start = *authority.attempt_starts().last().unwrap();
    assert!(last_start < AGENT_UPSTREAM_CONNECT_WINDOW, "{last_start:?}");
    assert_eq!(elapsed, last_start + STAND_IN_ATTEMPT_CAP);
}

// A connection that cannot be set up fails before any attempt is redialed
// and is reported as such, not as attempts that failed within the window.
#[tokio::test]
async fn an_unreadable_ca_is_a_setup_error_not_a_window_of_attempts() {
    let dir = tempdir().unwrap();
    let endpoint = crate::CredmgrEndpoint::Tls {
        uri: "https://127.0.0.1:9".parse().unwrap(),
        files: pplx_space_credmgr_client::TlsFiles::new(dir.path().join("missing-ca.pem")),
    };

    let error = connect_agent_upstream(&endpoint, &CancellationToken::new(), |_| {})
        .await
        .map(|_| ())
        .expect_err("an unreadable CA must fail");

    assert!(
        matches!(
            error,
            RunCredmgrError::AgentUpstreamSetup(
                pplx_space_credmgr_client::ConnectError::TlsRead { .. }
            )
        ),
        "{error:?}"
    );
}

// Shutdown ends the wait at once and cleanly: an agent that has not connected
// holds nothing to drain.
#[tokio::test(start_paused = true)]
async fn shutdown_ends_the_agents_wait_for_its_authority() {
    let authority = StandInAuthority::new(None, Unreachable::Refuses);
    let shutdown_token = CancellationToken::new();
    let requested_after = Duration::from_secs(30);
    let cancel = shutdown_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(requested_after).await;
        cancel.cancel();
    });

    let started = tokio::time::Instant::now();
    let connected = connect_agent_upstream(&authority, &shutdown_token, |_| {})
        .await
        .expect("shutdown is not a connect failure");

    assert!(connected.is_none());
    assert_eq!(started.elapsed(), requested_after);
}

// The wait is reported once per interval with the attempts made so far,
// however often attempts fail: here about once a second for the whole window.
#[tokio::test(start_paused = true)]
async fn still_waiting_reports_are_spaced_by_the_interval() {
    let authority = StandInAuthority::new(None, Unreachable::Refuses);
    let mut reports = Vec::new();

    let _ = connect_agent_upstream(&authority, &CancellationToken::new(), |waiting| {
        reports.push((waiting.elapsed, waiting.attempts, waiting.cause.clone()));
    })
    .await;

    let attempts = authority.attempt_starts().len();
    assert!(reports.len() > 1, "{reports:?}");
    assert!(
        reports.len() as u32
            <= AGENT_UPSTREAM_CONNECT_WINDOW.as_secs() as u32
                / AGENT_UPSTREAM_WAIT_WARNING_INTERVAL.as_secs() as u32,
        "{reports:?}"
    );
    assert!(
        reports[0].0 >= AGENT_UPSTREAM_WAIT_WARNING_INTERVAL,
        "{reports:?}"
    );
    assert!(
        reports.windows(2).all(|pair| pair[1].0 - pair[0].0
            >= AGENT_UPSTREAM_WAIT_WARNING_INTERVAL
            && pair[1].1 > pair[0].1),
        "{reports:?}"
    );
    assert!(
        reports
            .iter()
            .all(|(_, made, cause)| *made < attempts && cause.contains("stand-in:9810")),
        "{reports:?}"
    );
}

/// A port this test owns that refuses connections: bound, so nothing else can
/// take it, but not listening until the caller calls `listen`.
#[cfg(target_os = "linux")]
fn refusing_authority() -> (tokio::net::TcpSocket, crate::CredmgrEndpoint) {
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = socket.local_addr().unwrap();
    let endpoint = crate::CredmgrEndpoint::Tcp(format!("http://{address}").parse().unwrap());
    (socket, endpoint)
}

// Through a real endpoint and real dials: an authority that starts listening
// after the standard connect budget is connected, and the wait is reported
// with the cause a refused dial's transport error wraps, each layer's message
// once. A listening socket completes the TCP and HTTP/2 client handshakes
// from its accept queue, so it needs no server. How soon the dial completes
// is the kernel's, so nothing here bounds it.
#[cfg(target_os = "linux")]
#[tokio::test(start_paused = true)]
async fn agent_connects_to_a_real_authority_that_comes_up_late() {
    let (socket, endpoint) = refusing_authority();
    let reports = std::sync::Mutex::new(Vec::new());
    let shutdown_token = CancellationToken::new();
    let connecting = connect_agent_upstream(&endpoint, &shutdown_token, |waiting| {
        reports.lock().unwrap().push(waiting.cause.clone());
    });
    let listening = async {
        tokio::time::sleep(Duration::from_secs(30)).await;
        socket.listen(16).unwrap()
    };

    let (connected, _listener) = tokio::join!(connecting, listening);
    connected
        .expect("the agent must connect once the authority listens")
        .expect("no shutdown was requested");

    let refused =
        std::io::Error::from_raw_os_error(nix::errno::Errno::ECONNREFUSED as i32).to_string();
    let reports = reports.into_inner().unwrap();
    assert!(!reports.is_empty(), "the wait outlasted a report interval");
    for cause in &reports {
        let layers: Vec<&str> = cause.split(": ").collect();
        assert!(cause.ends_with(&refused), "{cause}");
        assert!(layers.len() > 1, "{cause}");
        assert!(layers.windows(2).all(|pair| pair[0] != pair[1]), "{cause}");
    }
}

use tonic_health::pb::health_check_response::ServingStatus;

/// An authority the test lets answer when it chooses.
#[derive(Default)]
struct GatedAuthority {
    open: tokio::sync::Notify,
    fails: bool,
}

impl AgentUpstream for GatedAuthority {
    type Client = ();

    async fn connect(
        &self,
        _retry_policy: pplx_space_common_grpc::GrpcRetryPolicy,
        _on_retry: impl FnMut(&pplx_space_common_grpc::TcpConnectRetry<'_>),
    ) -> Result<(), pplx_space_credmgr_client::ConnectError> {
        if self.fails {
            return Err(pplx_space_credmgr_client::ConnectError::Config(
                pplx_space_credmgr_client::EndpointConfigError::EmptyUdsPath,
            ));
        }
        self.open.notified().await;
        Ok(())
    }
}

async fn local_health(socket: &Path, service: &str) -> Result<ServingStatus, String> {
    let channel = pplx_space_common_grpc::connect_uds_channel("credmgr-health-test", socket)
        .await
        .map_err(|error| error.to_string())?;
    let response = tonic_health::pb::health_client::HealthClient::new(channel)
        .check(tonic_health::pb::HealthCheckRequest {
            service: service.to_owned(),
        })
        .await
        .map_err(|status| status.to_string())?;
    Ok(ServingStatus::try_from(response.into_inner().status).unwrap())
}

async fn wait_for_path(path: &Path) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while !path.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{} was never bound",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// While the agent waits for its authority, liveness answers and readiness is
// withheld, so a liveness probe does not end the wait and the pod is not
// Ready before it can serve. Readiness passes, as passive readiness, only
// after the authority answers. A predecessor holding the service mutex keeps
// the agent passive.
#[tokio::test]
async fn agent_withholds_readiness_but_answers_liveness_while_it_waits() {
    let dir = tempdir().unwrap();
    let mutex_path = dir.path().join("service.mutex");
    let health = dir.path().join("health.sock");
    let predecessor = ServiceMutex::acquire(
        &mutex_path,
        CancellationToken::new(),
        Duration::from_millis(10),
        None,
    )
    .await
    .unwrap();
    let config = CredmgrRunConfig {
        role: CredmgrRole::Agent,
        health_socket_path: Some(health.clone()),
        ..node_local_config()
    };
    let authority = Arc::new(GatedAuthority::default());
    let shutdown_token = CancellationToken::new();
    let starting = tokio::spawn({
        let authority = Arc::clone(&authority);
        let shutdown_token = shutdown_token.clone();
        async move {
            start_agent(&config, &shutdown_token, &mutex_path, &*authority, |_| {})
                .await
                .map(|started| started.is_some())
        }
    });

    wait_for_path(&health).await;
    assert_eq!(
        local_health(&health, "liveness").await,
        Ok(ServingStatus::Serving)
    );
    assert_eq!(
        local_health(&health, "readiness").await,
        Ok(ServingStatus::NotServing)
    );
    assert_eq!(
        local_health(&health, "").await,
        Ok(ServingStatus::NotServing)
    );

    authority.open.notify_one();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while local_health(&health, "readiness").await != Ok(ServingStatus::Serving) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "readiness never passed after the authority answered"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    shutdown_token.cancel();
    drop(predecessor);
    let activated = tokio::time::timeout(Duration::from_secs(2), starting)
        .await
        .expect("a passive agent must observe shutdown")
        .unwrap()
        .expect("a passive agent exits cleanly");
    assert!(!activated, "shutdown while passive does not activate");
}

// An agent that gives up stops answering on its health socket before it
// returns, so nothing reports it alive once its startup has failed.
#[tokio::test]
async fn an_agent_that_gives_up_stops_its_health_server() {
    let dir = tempdir().unwrap();
    let health = dir.path().join("health.sock");
    let config = CredmgrRunConfig {
        role: CredmgrRole::Agent,
        health_socket_path: Some(health.clone()),
        ..node_local_config()
    };
    let authority = GatedAuthority {
        fails: true,
        ..GatedAuthority::default()
    };
    let shutdown_token = CancellationToken::new();

    let result = start_agent(
        &config,
        &shutdown_token,
        &dir.path().join("service.mutex"),
        &authority,
        |_| {},
    )
    .await;

    assert!(
        matches!(result, Err(RunCredmgrError::AgentUpstreamSetup(_))),
        "{:?}",
        result.map(|started| started.is_some())
    );
    assert!(
        local_health(&health, "liveness").await.is_err(),
        "the health server must stop with a failed startup"
    );
    assert!(!shutdown_token.is_cancelled());
}

// A startup abandoned while the agent waits, its task aborted while the
// runtime keeps running, takes its health server with it, rather than
// leaving liveness answering for an agent that is gone.
#[tokio::test]
async fn dropping_a_waiting_agent_stops_its_health_server() {
    let dir = tempdir().unwrap();
    let health = dir.path().join("health.sock");
    let mutex_path = dir.path().join("service.mutex");
    let config = CredmgrRunConfig {
        role: CredmgrRole::Agent,
        health_socket_path: Some(health.clone()),
        ..node_local_config()
    };
    let starting = tokio::spawn(async move {
        let authority = GatedAuthority::default();
        let _ = start_agent(
            &config,
            &CancellationToken::new(),
            &mutex_path,
            &authority,
            |_| {},
        )
        .await;
    });
    wait_for_path(&health).await;
    assert_eq!(
        local_health(&health, "liveness").await,
        Ok(ServingStatus::Serving)
    );

    starting.abort();
    assert!(starting.await.unwrap_err().is_cancelled());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while local_health(&health, "liveness").await.is_ok() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the health server outlived its dropped startup"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// Activation takes the early health server over: it keeps answering after
// the startup future that bound it has returned.
#[tokio::test]
async fn an_activated_agent_keeps_its_health_server() {
    let dir = tempdir().unwrap();
    let health = dir.path().join("health.sock");
    let config = CredmgrRunConfig {
        role: CredmgrRole::Agent,
        health_socket_path: Some(health.clone()),
        ..node_local_config()
    };
    let authority = GatedAuthority::default();
    authority.open.notify_one();

    let (_, node_local) = start_agent(
        &config,
        &CancellationToken::new(),
        &dir.path().join("service.mutex"),
        &authority,
        |_| {},
    )
    .await
    .expect("the agent starts")
    .expect("nothing holds the service mutex");

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        local_health(&health, "liveness").await,
        Ok(ServingStatus::Serving)
    );
    drop(node_local);
}
