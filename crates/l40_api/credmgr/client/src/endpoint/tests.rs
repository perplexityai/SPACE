use super::*;

fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
    move |var| {
        pairs
            .iter()
            .find(|(k, _)| *k == var)
            .map(|(_, v)| v.to_string())
    }
}

#[test]
fn crypto_provider_is_installed_for_tls() {
    // The TLS connect path builds a rustls ClientTlsConfig, which panics
    // at materialization if no process-global CryptoProvider is installed.
    // Consumer binaries do not install one, so the client must.
    ensure_crypto_provider();
    assert!(
        rustls::crypto::CryptoProvider::get_default().is_some(),
        "TLS connect path left no default CryptoProvider installed"
    );
}

#[test]
fn unset_defaults_to_uds_socket() {
    let endpoint = CredmgrEndpoint::from_env_with(env(&[])).unwrap();
    assert_eq!(
        endpoint,
        CredmgrEndpoint::Uds(PathBuf::from(crate::CREDMGR_SOCKET))
    );
}

#[test]
fn empty_value_defaults_to_uds_socket() {
    let endpoint = CredmgrEndpoint::from_env_with(env(&[(ENDPOINT_VAR, "")])).unwrap();
    assert_eq!(endpoint, CredmgrEndpoint::default());
}

#[test]
fn unix_scheme_selects_uds_path() {
    let endpoint = CredmgrEndpoint::from_env_with(env(&[(
        ENDPOINT_VAR,
        "unix:///space/run/credential-manager/credmgr.sock",
    )]))
    .unwrap();
    assert_eq!(
        endpoint,
        CredmgrEndpoint::Uds(PathBuf::from("/space/run/credential-manager/credmgr.sock"))
    );
}

#[test]
fn unix_scheme_with_empty_path_is_rejected() {
    let err = CredmgrEndpoint::from_env_with(env(&[(ENDPOINT_VAR, "unix://")])).unwrap_err();
    assert_eq!(err, EndpointConfigError::EmptyUdsPath);
}

#[test]
fn http_scheme_selects_plaintext_tcp() {
    let endpoint = CredmgrEndpoint::from_env_with(env(&[(
        ENDPOINT_VAR,
        "http://space-credential-manager:9810",
    )]))
    .unwrap();
    let CredmgrEndpoint::Tcp(uri) = endpoint else {
        panic!("expected Tcp, got {endpoint:?}");
    };
    assert_eq!(uri.host(), Some("space-credential-manager"));
    assert_eq!(uri.port_u16(), Some(9810));
}

#[test]
fn https_scheme_requires_ca_var() {
    let err = CredmgrEndpoint::from_env_with(env(&[(
        ENDPOINT_VAR,
        "https://space-credential-manager:9810",
    )]))
    .unwrap_err();
    assert_eq!(err, EndpointConfigError::MissingTlsVar { var: TLS_CA_VAR });
}

#[test]
fn https_scheme_selects_tls_with_ca_only() {
    let endpoint = CredmgrEndpoint::from_env_with(env(&[
        (ENDPOINT_VAR, "https://space-credential-manager:9810"),
        (TLS_CA_VAR, "/etc/space-tls/ca.crt"),
    ]))
    .unwrap();
    let CredmgrEndpoint::Tls { uri, files } = endpoint else {
        panic!("expected Tls, got {endpoint:?}");
    };
    assert_eq!(uri.host(), Some("space-credential-manager"));
    assert_eq!(files.ca(), std::path::Path::new("/etc/space-tls/ca.crt"));
}

#[test]
fn unsupported_scheme_is_rejected() {
    let err = CredmgrEndpoint::from_env_with(env(&[(
        ENDPOINT_VAR,
        "ftp://space-credential-manager:9810",
    )]))
    .unwrap_err();
    assert_eq!(
        err,
        EndpointConfigError::UnsupportedScheme {
            scheme: "ftp".to_string()
        }
    );
}

#[test]
fn host_is_required_for_tcp_schemes() {
    let err = CredmgrEndpoint::from_env_with(env(&[(ENDPOINT_VAR, "http://")])).unwrap_err();
    assert!(matches!(
        err,
        EndpointConfigError::InvalidUri { .. } | EndpointConfigError::MissingHost { .. }
    ));
}

// A dial whose SYN is never answered must not hold connect for the kernel's
// SYN retransmits: the dial hangs until its bound cuts it, and connect fails
// within the retry budget plus one dial. The error must carry the bound's
// `TimedOut`: under the paused clock the kernel cannot time a dial out first,
// and a dial that fails fast would carry its own error however long the
// policy kept redialing. Linux drops a SYN to a listener whose accept queue
// is full, which stands in for a SYN pinned to a stale conntrack entry.
#[cfg(target_os = "linux")]
#[tokio::test(start_paused = true)]
async fn an_unanswered_dial_fails_within_the_connect_budget() {
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(2).unwrap();
    let address = listener.local_addr().unwrap();
    // Linux counts the accept queue full only once it holds more than the
    // backlog, so three unaccepted connections fill it. Dialing them one at a
    // time keeps at most one handshake pending, so the SYN queue never counts
    // as full and the setup does not depend on SYN cookies.
    let mut queued = Vec::new();
    for _ in 0..3 {
        queued.push(tokio::net::TcpStream::connect(address).await.unwrap());
    }
    // The dial never completes, so the empty CA is never consulted.
    let dir = tempfile::tempdir().unwrap();
    let ca = dir.path().join("ca.pem");
    std::fs::write(&ca, "").unwrap();

    for endpoint in [
        CredmgrEndpoint::Tcp(format!("http://{address}").parse().unwrap()),
        CredmgrEndpoint::Tls {
            uri: format!("https://{address}").parse().unwrap(),
            files: TlsFiles::new(ca.clone()),
        },
    ] {
        let budget = pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY.max_elapsed;
        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(budget * 6, endpoint.connect())
            .await
            .unwrap_or_else(|_| panic!("{endpoint:?}: an unanswered dial must be bounded"));
        let elapsed = started.elapsed();
        let error = match result {
            Err(error @ ConnectError::Transport(_)) => error,
            other => panic!("{endpoint:?}: {:?}", other.map(|_| ())),
        };
        let timed_out = std::iter::successors(
            Some(&error as &(dyn std::error::Error + 'static)),
            |error| error.source(),
        )
        .any(|error| {
            error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::TimedOut)
        });
        assert!(
            timed_out,
            "{endpoint:?}: the dial bound did not fire: {error:?}"
        );
        assert!(
            elapsed <= budget + CONNECT_TIMEOUT,
            "{endpoint:?}: connect gave up after {elapsed:?}"
        );
    }
    drop(queued);
}
