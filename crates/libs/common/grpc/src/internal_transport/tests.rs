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
async fn tls_identity_rejects_plaintext_without_blocking_other_peers() {
    use tokio_util::sync::CancellationToken;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let temp = tempfile::tempdir().unwrap();
    let identity = rcgen::generate_simple_self_signed(vec!["internal.example".into()]).unwrap();
    let wrong = rcgen::generate_simple_self_signed(vec!["internal.example".into()]).unwrap();
    let cert = temp.path().join("server.pem");
    let key = temp.path().join("server.key");
    std::fs::write(&cert, identity.cert.pem()).unwrap();
    std::fs::write(&key, identity.signing_key.serialize_pem()).unwrap();
    for insecure_dev in [false, true] {
        let mut env = Env::default();
        env.put(CERT_ENV, cert.as_os_str());
        env.put(KEY_ENV, key.as_os_str());
        env.put(INSECURE_DEV_ENV, insecure_dev.to_string());
        let mut builder = server_builder_from_environment(&env).unwrap();
        // A configured TLS listener advertises HTTPS even when development
        // callers have opted into plaintext for other endpoints.
        assert_eq!(
            advertised_endpoint_prefix_from_environment(&env),
            "https://"
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
        let server = tokio::spawn(builder.add_service(health).serve_with_incoming_shutdown(
            Incoming::new(TcpListenerStream::new(listener), shutdown.clone()),
            shutdown.clone().cancelled_owned(),
        ));
        // This peer sends no preface; another peer must still be admitted promptly.
        let idle = tokio::net::TcpStream::connect(address).await.unwrap();
        for (tls, ca, name, accepted) in [
            (true, identity.cert.pem(), "internal.example", true),
            (false, identity.cert.pem(), "internal.example", false),
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
}

#[tokio::test]
async fn handshake_budget_releases_on_completion_failure_and_saturated_shutdown() {
    use std::time::Duration;
    use tokio_stream::StreamExt;
    use tokio_util::sync::CancellationToken;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let temp = tempfile::tempdir().unwrap();
    let identity = rcgen::generate_simple_self_signed(vec!["internal.example".into()]).unwrap();
    let cert = temp.path().join("server.pem");
    let key = temp.path().join("server.key");
    std::fs::write(&cert, identity.cert.pem()).unwrap();
    std::fs::write(&key, identity.signing_key.serialize_pem()).unwrap();
    let mut env = Env::default();
    env.put(CERT_ENV, cert.as_os_str());
    env.put(KEY_ENV, key.as_os_str());
    let mut builder = server_builder_from_environment(&env).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let observed = accepted.clone();
    let sockets = TcpListenerStream::new(listener).map(move |socket| {
        observed.fetch_add(1, Ordering::SeqCst);
        socket
    });
    let shutdown = CancellationToken::new();
    let incoming = incoming::Incoming::with_capacity(sockets, shutdown.clone(), 2);
    let (_, health) = tonic_health::server::health_reporter();
    let server = tokio::spawn(
        builder
            .add_service(health)
            .serve_with_incoming_shutdown(incoming, std::future::pending()),
    );
    let endpoint = Endpoint::from_shared(format!("https://{address}"))
        .unwrap()
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(identity.cert.pem()))
                .domain_name("internal.example"),
        )
        .unwrap();
    let wait_for_accepts = |count| {
        let accepted = accepted.clone();
        async move {
            tokio::time::timeout(Duration::from_secs(1), async {
                while accepted.load(Ordering::SeqCst) < count {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
    };
    let idle = tokio::net::TcpStream::connect(address).await.unwrap();
    let other_idle = tokio::net::TcpStream::connect(address).await.unwrap();
    wait_for_accepts(2).await;
    // This deadline is shorter than the ten-second TLS handshake deadline.
    // A third handshake must wait for capacity even though TCP can connect.
    tokio::time::timeout(Duration::from_millis(100), endpoint.connect())
        .await
        .unwrap_err();
    assert_eq!(accepted.load(Ordering::SeqCst), 2);
    drop(idle);
    // A failed handshake frees its slot; completed TLS channels also free slots
    // while they remain open, so neither channel below consumes the budget.
    let first = tokio::time::timeout(Duration::from_secs(1), endpoint.connect())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(1), endpoint.connect())
        .await
        .unwrap()
        .unwrap();
    let before = accepted.load(Ordering::SeqCst);
    let saturating = tokio::net::TcpStream::connect(address).await.unwrap();
    wait_for_accepts(before + 1).await;
    shutdown.cancel();
    let replacement = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match tokio::net::TcpListener::bind(address).await {
                Ok(listener) => break listener,
                Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                    tokio::task::yield_now().await
                }
                Err(error) => panic!("replacement bind failed: {error}"),
            }
        }
    })
    .await
    .expect("saturated handshakes must not retain the listening socket");
    drop((first, second, other_idle, saturating, replacement));
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
