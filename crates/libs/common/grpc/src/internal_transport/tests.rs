use super::*;
use std::collections::HashMap;
use std::ffi::OsString;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic_health::pb::{HealthCheckRequest, health_client::HealthClient};

#[derive(Clone, Default)]
struct Env(HashMap<String, OsString>);
impl Environment for Env {
    fn var_os(&self, key: &str) -> Option<OsString> {
        self.0.get(key).cloned()
    }
}
impl Env {
    fn put(&mut self, key: &str, value: impl Into<OsString>) {
        self.0.insert(key.into(), value.into());
    }
}

#[tokio::test]
async fn plaintext_requires_explicit_development_opt_in_and_partial_tls_never_downgrades() {
    let endpoint = Endpoint::from_static("http://127.0.0.1:9800");
    for value in [None, Some("false"), Some("typo"), Some("true")] {
        let mut env = Env::default();
        if let Some(value) = value {
            env.put(INSECURE_DEV_ENV, value);
        }
        for service in [Service::ClusterApi, Service::NodeApi, Service::BuildManager] {
            let result = ClientTransport::from_environment(service, endpoint.uri(), &env).await;
            assert_eq!(result.is_ok(), value == Some("true"));
        }
        assert_eq!(
            server_builder_from_environment(&env).is_ok(),
            value == Some("true")
        );
        for name in [CERT_ENV, KEY_ENV] {
            for path in ["", "/missing/certificate.pem"] {
                let mut partial = env.clone();
                partial.put(name, path);
                server_builder_from_environment(&partial).unwrap_err();
            }
        }
    }
    let secure = Endpoint::from_static("https://worker.example:9800");
    let mut env = Env::default();
    env.put(INSECURE_DEV_ENV, "true");
    assert!(matches!(
        ClientTransport::from_environment(Service::NodeApi, secure.uri(), &env).await,
        Err(TransportError::Missing(CA_ENV))
    ));
}

#[tokio::test]
async fn authenticates_discovered_server_before_any_bearer_is_sent() {
    // Service bootstrap selects AWS-LC. This isolated fixture also needs an
    // explicit provider when the workspace unifies multiple rustls backends.
    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
    {
        // Another fixture may have initialized the process-wide provider first.
        rustls::crypto::CryptoProvider::get_default()
            .expect("a rejected provider installation leaves an installed provider");
    }
    let temp = tempfile::tempdir().unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let params =
        rcgen::CertificateParams::new(vec!["internal.example".into(), "::1".into()]).unwrap();
    let certificate = params.self_signed(&key).unwrap();
    let other = rcgen::generate_simple_self_signed(vec!["internal.example".into()]).unwrap();
    let cert_path = temp.path().join("server.pem");
    let key_path = temp.path().join("server.key");
    let wrong_ca = temp.path().join("wrong-ca.pem");
    std::fs::write(&cert_path, certificate.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    std::fs::write(&wrong_ca, other.cert.pem()).unwrap();
    let mut server_env = Env::default();
    server_env.put(CERT_ENV, cert_path.as_os_str());
    server_env.put(KEY_ENV, key_path.as_os_str());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (reporter, service) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let service = tonic::service::interceptor::InterceptedService::new(
        service,
        move |request: tonic::Request<()>| {
            assert_eq!(
                request.metadata().get("authorization").unwrap(),
                "Bearer regression-token"
            );
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(request)
        },
    );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        server_builder_from_environment(&server_env)
            .unwrap()
            .add_service(service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = stopped.await;
            }),
    );
    for original_host in ["internal.example", "[::1]"] {
        let original =
            Endpoint::from_shared(format!("https://{original_host}:{}", address.port())).unwrap();
        for service in [Service::ClusterApi, Service::NodeApi, Service::BuildManager] {
            for (ca, name, accepted) in [
                (&cert_path, None, true),
                (&wrong_ca, None, false),
                (&cert_path, Some("impostor.example"), false),
            ] {
                let before = calls.load(Ordering::SeqCst);
                let mut env = Env::default();
                env.put(CA_ENV, ca.as_os_str());
                if let Some(name) = name {
                    env.put(service.name_env(), name);
                }
                let transport = ClientTransport::from_environment(service, original.uri(), &env)
                    .await
                    .unwrap();
                // Discovery replaces the dial address, while trust remains attached
                // to the original service identity, not the pod IP.
                let dial = Endpoint::from_shared(format!("https://{address}")).unwrap();
                let channel = transport.endpoint(dial).unwrap().connect().await;
                if accepted {
                    let mut client = HealthClient::new(channel.unwrap());
                    let mut request = tonic::Request::new(HealthCheckRequest {
                        service: String::new(),
                    });
                    request
                        .metadata_mut()
                        .insert("authorization", "Bearer regression-token".parse().unwrap());
                    client.check(request).await.unwrap();
                    assert_eq!(calls.load(Ordering::SeqCst), before + 1);
                } else {
                    channel.unwrap_err();
                    assert_eq!(calls.load(Ordering::SeqCst), before);
                }
            }
        }
    }
    // Operator-provided identity is literal configuration, not URI syntax.
    // A malformed override must fail instead of being silently normalized.
    let mut env = Env::default();
    env.put(CA_ENV, cert_path.as_os_str());
    env.put(Service::NodeApi.name_env(), "[::1]");
    let endpoint = Endpoint::from_static("https://[::1]:9800");
    assert!(matches!(
        ClientTransport::from_environment(Service::NodeApi, endpoint.uri(), &env).await,
        Err(TransportError::Tonic(_))
    ));
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn rollout_accepts_both_protocols_without_downgrading_tls_or_blocking_on_idle_peers() {
    use tokio_util::sync::CancellationToken;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let temp = tempfile::tempdir().unwrap();
    let identity = rcgen::generate_simple_self_signed(vec!["internal.example".into()]).unwrap();
    let wrong = rcgen::generate_simple_self_signed(vec!["internal.example".into()]).unwrap();
    let cert = temp.path().join("server.pem");
    let key = temp.path().join("server.key");
    std::fs::write(&cert, identity.cert.pem()).unwrap();
    std::fs::write(&key, identity.signing_key.serialize_pem()).unwrap();
    for compatibility in [false, true] {
        let mut env = Env::default();
        env.put(CERT_ENV, cert.as_os_str());
        env.put(KEY_ENV, key.as_os_str());
        env.put(ALLOW_PLAINTEXT_ROLLOUT_ENV, compatibility.to_string());
        let transport = ServerTransport::from_environment(&env).unwrap();
        // Discovery is protocol-neutral while listeners admit both versions.
        // Strict clients choose HTTPS; prepared plaintext clients choose HTTP.
        assert_eq!(
            advertised_endpoint_prefix_from_environment(&env),
            if compatibility { "" } else { "https://" }
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let (reporter, health) = tonic_health::server::health_reporter();
        reporter
            .set_service_status("", tonic_health::ServingStatus::Serving)
            .await;
        let health = tonic::service::interceptor::InterceptedService::new(
            health,
            move |request: tonic::Request<()>| {
                assert_eq!(
                    request.metadata().get("authorization").unwrap(),
                    "Bearer synthetic-token"
                );
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(request)
            },
        );
        let shutdown = CancellationToken::new();
        let server = tokio::spawn(
            transport
                .server_builder()
                .add_service(health)
                .serve_with_incoming_shutdown(
                    transport.incoming(TcpListenerStream::new(listener), shutdown.clone()),
                    shutdown.clone().cancelled_owned(),
                ),
        );
        // This peer sends no preface; another peer must still be admitted promptly.
        let idle = tokio::net::TcpStream::connect(address).await.unwrap();
        for (tls, ca, name, accepted) in [
            (true, identity.cert.pem(), "internal.example", true),
            (
                false,
                identity.cert.pem(),
                "internal.example",
                compatibility,
            ),
            (true, wrong.cert.pem(), "internal.example", false),
            (true, identity.cert.pem(), "impostor.example", false),
        ] {
            let before = calls.load(Ordering::SeqCst);
            let endpoint = Endpoint::from_shared(format!(
                "{}://{address}",
                if tls { "https" } else { "http" }
            ))
            .unwrap();
            let endpoint = if tls {
                endpoint
                    .tls_config(
                        ClientTlsConfig::new()
                            .ca_certificate(Certificate::from_pem(ca))
                            .domain_name(name),
                    )
                    .unwrap()
            } else {
                endpoint
            };
            let result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                let channel = endpoint.connect().await?;
                let mut client = HealthClient::new(channel);
                let mut request = tonic::Request::new(HealthCheckRequest {
                    service: String::new(),
                });
                request
                    .metadata_mut()
                    .insert("authorization", "Bearer synthetic-token".parse().unwrap());
                client.check(request).await?;
                Ok::<_, Box<dyn std::error::Error>>(())
            })
            .await
            .expect("an idle peer must not block unrelated handshakes");
            assert_eq!(result.is_ok(), accepted);
            assert_eq!(calls.load(Ordering::SeqCst), before + usize::from(accepted));
        }
        shutdown.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(idle);
    }
    // Opting into compatibility still requires a complete valid identity.
    let mut env = Env::default();
    env.put(ALLOW_PLAINTEXT_ROLLOUT_ENV, "true");
    assert!(ServerTransport::from_environment(&env).is_err());
    env.put(CERT_ENV, cert.as_os_str());
    assert!(ServerTransport::from_environment(&env).is_err());
}

/// Binds like node-api so a replacement process can share the port during a rollout.
#[cfg(target_os = "linux")]
fn reuse_port_listener(addr: std::net::SocketAddr) -> tokio::net::TcpListener {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )
    .unwrap();
    socket.set_reuse_address(true).unwrap();
    socket.set_reuse_port(true).unwrap();
    socket.set_nonblocking(true).unwrap();
    socket.bind(&addr.into()).unwrap();
    socket.listen(1024).unwrap();
    tokio::net::TcpListener::from_std(socket.into()).unwrap()
}

/// Counts loopback IPv4 sockets in the kernel LISTEN state for `port`.
#[cfg(target_os = "linux")]
fn loopback_listeners(port: u16) -> usize {
    let local = format!("0100007F:{port:04X}");
    std::fs::read_to_string("/proc/net/tcp")
        .unwrap()
        .lines()
        .skip(1)
        .filter(|line| {
            let mut fields = line.split_whitespace().skip(1);
            fields.next() == Some(local.as_str()) && fields.nth(1) == Some("0A")
        })
        .count()
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_releases_reuse_port_listener_while_accepted_connections_drain() {
    use std::time::{Duration, Instant};
    use tokio_util::sync::CancellationToken;
    use tonic_health::pb::health_check_response::ServingStatus;

    let mut env = Env::default();
    env.put(INSECURE_DEV_ENV, "true");
    let transport = ServerTransport::from_environment(&env).unwrap();
    let serve = |listener: tokio::net::TcpListener, shutdown: CancellationToken| {
        let transport = transport.clone();
        async move {
            let (reporter, health) = tonic_health::server::health_reporter();
            reporter
                .set_service_status("", tonic_health::ServingStatus::Serving)
                .await;
            // Like node-api, only the end of `incoming` starts tonic's graceful drain.
            let server = tokio::spawn(
                transport
                    .server_builder()
                    .add_service(health)
                    .serve_with_incoming_shutdown(
                        transport.incoming(
                            tonic::transport::server::TcpIncoming::from(listener),
                            shutdown,
                        ),
                        std::future::pending(),
                    ),
            );
            (reporter, server)
        }
    };
    let watch_request = || HealthCheckRequest {
        service: String::new(),
    };

    let old_listener = reuse_port_listener("127.0.0.1:0".parse().unwrap());
    let addr = old_listener.local_addr().unwrap();
    let old_shutdown = CancellationToken::new();
    let (old_reporter, old_server) = serve(old_listener, old_shutdown.clone()).await;
    // Opened before the replacement binds, so this connection belongs to the old server.
    let draining = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut draining = HealthClient::new(draining);
    let mut watch = draining.watch(watch_request()).await.unwrap().into_inner();
    assert_eq!(
        watch.message().await.unwrap().unwrap().status(),
        ServingStatus::Serving
    );

    let new_shutdown = CancellationToken::new();
    let (_new_reporter, new_server) = serve(reuse_port_listener(addr), new_shutdown.clone()).await;
    assert_eq!(loopback_listeners(addr.port()), 2);

    old_shutdown.cancel();
    let deadline = Instant::now() + Duration::from_secs(1);
    while loopback_listeners(addr.port()) > 1 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let listeners = loopback_listeners(addr.port());

    // The kernel spreads fresh connections across every listener in the port group.
    const FRESH: usize = 64;
    let started = Instant::now();
    let outcomes = futures_util::future::join_all((0..FRESH).map(|_| async move {
        tokio::time::timeout(Duration::from_secs(2), async move {
            let channel = Endpoint::from_shared(format!("http://{addr}"))
                .unwrap()
                .connect()
                .await?;
            HealthClient::new(channel)
                .check(watch_request())
                .await
                .map_err(Box::new)?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await
    }))
    .await;
    let elapsed = started.elapsed();
    let hung = outcomes.iter().filter(|outcome| outcome.is_err()).count();
    let failed = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, Ok(Err(_))))
        .count();
    assert!(
        listeners == 1 && hung == 0 && failed == 0,
        "after old server shutdown: {listeners} LISTEN sockets on port {}; of {FRESH} fresh \
         connections {} answered, {hung} hung past the 2 s deadline, {failed} failed \
         (batch took {elapsed:?})",
        addr.port(),
        FRESH - hung - failed,
    );

    // The connection accepted before shutdown still drains on the old server.
    old_reporter
        .set_service_status("", tonic_health::ServingStatus::NotServing)
        .await;
    let update = tokio::time::timeout(Duration::from_secs(1), watch.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(update.status(), ServingStatus::NotServing);
    assert!(!old_server.is_finished());
    drop(watch);
    drop(draining);
    tokio::time::timeout(Duration::from_secs(1), old_server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    new_shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(1), new_server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
