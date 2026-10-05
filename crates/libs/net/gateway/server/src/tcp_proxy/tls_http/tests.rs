use super::credentials::{credential_headers, inject};
use super::forward::authorize_authority;
use super::*;
use crate::certificates::NetworkCredentialAccessError;
use http::HeaderMap;
use pplx_space_common_core::types::CredentialReferenceId;

#[test]
fn authority_binds_every_http_routing_field_to_the_tls_origin() {
    let host = "allowed.test".parse().unwrap();
    for (uri, headers, version, port, accepted) in [
        ("/", vec!["allowed.test"], Version::HTTP_11, 443, true),
        ("/", vec!["ALLOWED.test.:443"], Version::HTTP_11, 443, true),
        (
            "https://allowed.test/",
            vec!["allowed.test"],
            Version::HTTP_11,
            443,
            true,
        ),
        ("https://allowed.test/", vec![], Version::HTTP_2, 443, true),
        (
            "https://allowed.test:8443/",
            vec!["allowed.test:8443"],
            Version::HTTP_2,
            8443,
            true,
        ),
        ("/", vec!["forbidden.test"], Version::HTTP_11, 443, false),
        (
            "https://forbidden.test/",
            vec!["allowed.test"],
            Version::HTTP_11,
            443,
            false,
        ),
        (
            "https://allowed.test/",
            vec!["forbidden.test"],
            Version::HTTP_2,
            443,
            false,
        ),
        (
            "http://allowed.test/",
            vec!["allowed.test"],
            Version::HTTP_11,
            443,
            false,
        ),
        (
            "https://allowed.test:80/",
            vec![],
            Version::HTTP_2,
            443,
            false,
        ),
        ("/", vec!["allowed.test:8443"], Version::HTTP_11, 443, false),
        (
            "/",
            vec!["allowed.test:65536"],
            Version::HTTP_11,
            443,
            false,
        ),
        ("/", vec!["allowed.test:"], Version::HTTP_11, 443, false),
        ("/", vec!["user@allowed.test"], Version::HTTP_11, 443, false),
        (
            "/",
            vec!["allowed.test", "allowed.test"],
            Version::HTTP_11,
            443,
            false,
        ),
        ("/", vec![], Version::HTTP_11, 443, false),
        ("/", vec!["allowed.test"], Version::HTTP_2, 443, false),
        (
            "https://allowed.test/",
            vec![],
            Version::HTTP_11,
            443,
            false,
        ),
    ] {
        let mut request = Request::builder().uri(uri).version(version);
        for header in &headers {
            request = request.header(header::HOST, *header);
        }
        let request = request.body(()).unwrap();
        assert_eq!(
            authorize_authority(&request.try_into().unwrap(), &host, port).is_ok(),
            accepted,
            "{version:?} {uri} {headers:?} -> {port}"
        );
    }
}

const NETWORK_REFERENCE_ID: &str = "018f1e9d-7b7a-7d3a-8000-000000000001";
const SECOND_NETWORK_REFERENCE_ID: &str = "018f1e9d-7b7a-7d3a-8000-000000000002";
const NETWORK_SECRET: &str = "Bearer network-secret-test-value";

async fn credential_provider(
    include_material: bool,
) -> (
    crate::state::GatewayNetworkState,
    crate::certificates::CertificateProvider,
) {
    credential_provider_for_header(include_material, "authorization").await
}

async fn credential_provider_for_header(
    include_material: bool,
    header_name: &str,
) -> (
    crate::state::GatewayNetworkState,
    crate::certificates::CertificateProvider,
) {
    let bindings = [(header_name, NETWORK_REFERENCE_ID)];
    let material = include_material
        .then_some((NETWORK_REFERENCE_ID, NETWORK_SECRET))
        .into_iter()
        .collect::<Vec<_>>();
    credential_provider_with_material(&bindings, &material).await
}

async fn credential_provider_with_material(
    bindings: &[(&str, &str)],
    material: &[(&str, &str)],
) -> (
    crate::state::GatewayNetworkState,
    crate::certificates::CertificateProvider,
) {
    use pplx_space_credential_injection_core::proto::ResolvedNetworkCredential;
    use pplx_space_net_common::{
        CredentialHeaderBindingV1, NetworkCredentialDestinationV1, NetworkCredentialHttpMethodV1,
        NetworkCredentialRuleV1, TlsInterception,
    };

    let mut policy = pplx_space_net_common::NetworkPolicyV1::allow_all();
    policy.tls_interception = TlsInterception::On;
    policy.network_credential_rules = vec![NetworkCredentialRuleV1 {
        rule_id: "api".into(),
        destination: NetworkCredentialDestinationV1::new(
            "allowed.test".parse().unwrap(),
            None,
            Some("/v1".into()),
        )
        .unwrap(),
        credential_header_bindings: bindings
            .iter()
            .map(|(header_name, reference_id)| CredentialHeaderBindingV1 {
                header_name: (*header_name).into(),
                credential_reference_id: reference_id
                    .parse::<CredentialReferenceId>()
                    .unwrap()
                    .as_uuid(),
            })
            .collect(),
        supported_http_methods: vec![
            NetworkCredentialHttpMethodV1::Get,
            NetworkCredentialHttpMethodV1::Post,
        ],
    }];
    let credentials = material
        .iter()
        .map(|(reference_id, value)| ResolvedNetworkCredential {
            credential_reference_id: (*reference_id).into(),
            value: value.as_bytes().to_vec(),
        })
        .collect();
    let snapshot =
        crate::state::NetworkCredentialSnapshot::from_wire(credentials, &policy).unwrap();
    let state = crate::state::GatewayNetworkState::pending_policy();
    let (_, _, provider) =
        crate::certificates::local_provider_with_policy(state.clone(), policy, snapshot).await;
    (state, provider)
}

fn credential_request(method: http::Method, uri: &str) -> Request<()> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::HOST, "allowed.test")
        .body(())
        .unwrap()
}

#[tokio::test]
async fn network_credentials_replace_only_the_configured_header_and_are_sensitive() {
    let (_, provider) = credential_provider(true).await;
    let lease = provider
        .get("allowed.test".parse().unwrap(), 443)
        .await
        .unwrap();

    let mut request =
        SanitizedRequest::try_from(credential_request(http::Method::GET, "/v1/items")).unwrap();
    let headers = credential_headers(&lease, &request).unwrap();
    inject(&mut request, headers);
    assert_eq!(
        request.headers().get(header::AUTHORIZATION).unwrap(),
        NETWORK_SECRET
    );
    assert!(
        request
            .headers()
            .get(header::AUTHORIZATION)
            .unwrap()
            .is_sensitive()
    );

    let mut request = credential_request(http::Method::POST, "/v1/items?tenant=ignored");
    request.headers_mut().insert(
        header::AUTHORIZATION,
        http::HeaderValue::from_static("Bearer guest-value"),
    );
    let mut request = SanitizedRequest::try_from(request).unwrap();
    let headers = credential_headers(&lease, &request).unwrap();
    inject(&mut request, headers);
    assert_eq!(
        request.headers().get(header::AUTHORIZATION).unwrap(),
        NETWORK_SECRET
    );
    assert!(
        request
            .headers()
            .get(header::AUTHORIZATION)
            .unwrap()
            .is_sensitive()
    );

    let (_, custom_header_provider) = credential_provider_for_header(true, "x-api-key").await;
    let custom_header_lease = custom_header_provider
        .get("allowed.test".parse().unwrap(), 443)
        .await
        .unwrap();
    let mut request = credential_request(http::Method::GET, "/v1/items");
    request.headers_mut().insert(
        header::AUTHORIZATION,
        http::HeaderValue::from_static("Bearer guest-value"),
    );
    let mut request = SanitizedRequest::try_from(request).unwrap();
    let headers = credential_headers(&custom_header_lease, &request).unwrap();
    inject(&mut request, headers);
    assert_eq!(request.headers().get("x-api-key").unwrap(), NETWORK_SECRET);
    assert!(request.headers().get("x-api-key").unwrap().is_sensitive());
    assert_eq!(
        request.headers().get(header::AUTHORIZATION).unwrap(),
        "Bearer guest-value"
    );

    let (_, incomplete_provider) = credential_provider_with_material(
        &[
            ("authorization", NETWORK_REFERENCE_ID),
            ("x-api-key", SECOND_NETWORK_REFERENCE_ID),
        ],
        &[(NETWORK_REFERENCE_ID, NETWORK_SECRET)],
    )
    .await;
    let incomplete_lease = incomplete_provider
        .get("allowed.test".parse().unwrap(), 443)
        .await
        .unwrap();
    let mut request = credential_request(http::Method::GET, "/v1/items");
    request
        .headers_mut()
        .insert(header::AUTHORIZATION, "Bearer guest-value".parse().unwrap());
    request
        .headers_mut()
        .insert("x-api-key", "guest-api-key".parse().unwrap());
    let request = SanitizedRequest::try_from(request).unwrap();
    let error = credential_headers(&incomplete_lease, &request).unwrap_err();
    assert_eq!(error, NetworkCredentialAccessError::Missing);
    assert_eq!(
        request.headers().get(header::AUTHORIZATION).unwrap(),
        "Bearer guest-value"
    );
    assert_eq!(request.headers().get("x-api-key").unwrap(), "guest-api-key");
}

#[tokio::test]
async fn matching_policy_reports_service_unavailable_during_policy_refresh() {
    use pplx_space_net_common::{EffectiveNetworkPolicyRef, EffectiveNetworkPolicyRefScope};

    let (state, provider) = credential_provider(true).await;
    let lease = provider
        .get("allowed.test".parse().unwrap(), 443)
        .await
        .unwrap();
    let expected = EffectiveNetworkPolicyRef::local(
        EffectiveNetworkPolicyRefScope::Sandbox,
        "sandbox-1",
        "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    )
    .unwrap();
    let loader: crate::state::PolicyLoader = Arc::new(|| Box::pin(std::future::pending()));
    let refreshing = tokio::spawn({
        let state = state.clone();
        async move {
            state
                .refresh(
                    loader,
                    Some(expected),
                    crate::state::NetworkStateReason::ControlRefresh,
                )
                .await
        }
    });
    for _ in 0..100 {
        if state.snapshot().await.mode == crate::proto::NetworkState::PendingPolicy {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        state.snapshot().await.mode,
        crate::proto::NetworkState::PendingPolicy
    );
    let request =
        SanitizedRequest::try_from(credential_request(http::Method::GET, "/v1/items")).unwrap();
    let error = credential_headers(&lease, &request).unwrap_err();
    assert_eq!(error, NetworkCredentialAccessError::TemporarilyUnavailable);
    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
    refreshing.abort();
}

#[tokio::test]
async fn missing_policy_credential_never_connects_to_origin() {
    let (status, origin_connects) = credential_refusal_before_origin(false, false).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(origin_connects, 0);
}

#[tokio::test]
async fn credential_bearing_upgrade_never_connects_to_origin() {
    let (status, origin_connects) = credential_refusal_before_origin(true, true).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(origin_connects, 0);
}

async fn credential_refusal_before_origin(
    include_material: bool,
    websocket: bool,
) -> (StatusCode, usize) {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (state, provider) = credential_provider(include_material).await;
    let lease = provider
        .get("allowed.test".parse().unwrap(), 443)
        .await
        .unwrap();
    let binding = HostBinding::new(
        "test",
        "93.184.216.34:443".parse().unwrap(),
        state,
        Default::default(),
    );
    let origin_connects = Arc::new(AtomicUsize::new(0));
    let observed = origin_connects.clone();
    let (guest, guest_peer) = tokio::io::duplex(4096);
    let gateway = tokio::spawn(serve(guest, binding, lease, move |_| async move {
        observed.fetch_add(1, Ordering::SeqCst);
        Err::<(tokio::io::DuplexStream, HttpProtocol), _>(TlsTransportError::Upstream(
            std::io::Error::from(std::io::ErrorKind::ConnectionRefused),
        ))
    }));
    let (mut sender, connection) = http1::handshake(TokioIo::new(guest_peer)).await.unwrap();
    let client = tokio::spawn(connection);
    let mut request = credential_request(http::Method::GET, "/v1/items");
    if websocket {
        let headers = request.headers_mut();
        headers.insert(header::CONNECTION, "upgrade".parse().unwrap());
        headers.insert(header::UPGRADE, "websocket".parse().unwrap());
        headers.insert(header::SEC_WEBSOCKET_VERSION, "13".parse().unwrap());
        headers.insert(
            header::SEC_WEBSOCKET_KEY,
            "dGhlIHNhbXBsZSBub25jZQ==".parse().unwrap(),
        );
    }
    let request = request.map(|_| Empty::<Bytes>::new());
    let response = sender.send_request(request).await.unwrap();
    let result = (response.status(), origin_connects.load(Ordering::SeqCst));
    drop(sender);
    client.abort();
    gateway.abort();
    result
}

#[tokio::test]
async fn concurrent_http2_streams_keep_credential_decisions_request_local() {
    use http_body_util::Full;

    let (state, provider) = credential_provider(true).await;
    let lease = provider
        .get("allowed.test".parse().unwrap(), 443)
        .await
        .unwrap();
    let binding = HostBinding::new(
        "test",
        "93.184.216.34:443".parse().unwrap(),
        state,
        Default::default(),
    );
    let (origin, origin_peer) = tokio::io::duplex(4096);
    let (observed_tx, mut observed_rx) = tokio::sync::mpsc::channel(2);
    let origin_server = tokio::spawn(
        hyper::server::conn::http2::Builder::new(TokioExecutor::new()).serve_connection(
            TokioIo::new(origin_peer),
            service_fn(move |request: Request<Incoming>| {
                let observed_tx = observed_tx.clone();
                async move {
                    observed_tx
                        .send((
                            request.uri().path().to_owned(),
                            request
                                .headers()
                                .get(header::AUTHORIZATION)
                                .unwrap()
                                .to_str()
                                .unwrap()
                                .to_owned(),
                        ))
                        .await
                        .unwrap();
                    Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"ok"))))
                }
            }),
        ),
    );
    let (guest, guest_peer) = tokio::io::duplex(4096);
    let gateway = tokio::spawn(serve(guest, binding, lease, move |protocol| async move {
        assert_eq!(protocol, HttpProtocol::Http2);
        Ok((origin, HttpProtocol::Http2))
    }));
    let (sender, connection) = http2::handshake(TokioExecutor::new(), TokioIo::new(guest_peer))
        .await
        .unwrap();
    let client = tokio::spawn(connection);
    let request = |path, authorization| {
        Request::builder()
            .method(http::Method::GET)
            .uri(format!("https://allowed.test{path}"))
            .header(header::AUTHORIZATION, authorization)
            .body(Empty::<Bytes>::new())
            .unwrap()
    };
    let mut matching = sender.clone();
    let mut unmatched = sender;
    let (matching, unmatched) = tokio::join!(
        matching.send_request(request("/v1/items", "Bearer matching-guest")),
        unmatched.send_request(request("/public", "Bearer public-guest")),
    );
    assert_eq!(matching.unwrap().status(), StatusCode::OK);
    assert_eq!(unmatched.unwrap().status(), StatusCode::OK);

    let mut observed = vec![
        observed_rx.recv().await.unwrap(),
        observed_rx.recv().await.unwrap(),
    ];
    observed.sort();
    assert_eq!(
        observed,
        [
            ("/public".to_owned(), "Bearer public-guest".to_owned()),
            ("/v1/items".to_owned(), NETWORK_SECRET.to_owned()),
        ]
    );
    client.abort();
    gateway.abort();
    origin_server.abort();
}

#[tokio::test]
async fn policy_change_during_origin_setup_never_forwards_buffered_credentials() {
    use tokio::io::AsyncReadExt;

    let (state, provider) = credential_provider(true).await;
    let lease = provider
        .get("allowed.test".parse().unwrap(), 443)
        .await
        .unwrap();
    let binding = HostBinding::new(
        "test",
        "93.184.216.34:443".parse().unwrap(),
        state.clone(),
        Default::default(),
    );
    let (origin, mut origin_peer) = tokio::io::duplex(4096);
    let (connect_started_tx, connect_started_rx) = tokio::sync::oneshot::channel();
    let (connect_release_tx, connect_release_rx) = tokio::sync::oneshot::channel();
    let (guest, guest_peer) = tokio::io::duplex(4096);
    let gateway = tokio::spawn(serve(guest, binding, lease, move |_| async move {
        let _ = connect_started_tx.send(());
        let _ = connect_release_rx.await;
        Ok((origin, HttpProtocol::Http1))
    }));
    let (mut sender, connection) = http1::handshake(TokioIo::new(guest_peer)).await.unwrap();
    let client = tokio::spawn(connection);
    let request = credential_request(http::Method::GET, "/v1/items").map(|_| Empty::<Bytes>::new());
    let response = tokio::spawn(async move { sender.send_request(request).await.unwrap() });

    connect_started_rx.await.unwrap();
    state
        .set_no_identity(crate::state::NetworkStateReason::NoIdentity)
        .await;
    connect_release_tx.send(()).unwrap();

    assert_eq!(response.await.unwrap().status(), StatusCode::FORBIDDEN);
    let mut byte = [0];
    let observed = tokio::time::timeout(
        Duration::from_millis(100),
        origin_peer.read_exact(&mut byte),
    )
    .await;
    assert!(
        !matches!(observed, Ok(Ok(_))),
        "the invalidated request must not reach the origin"
    );
    client.abort();
    gateway.abort();
}

async fn fixture() -> (
    HostBinding,
    Arc<CertificateLease>,
    crate::tls_interception::TlsInterception,
) {
    let state = crate::state::GatewayNetworkState::pending_policy();
    let (root, _, provider) = crate::certificates::local_provider_with_state(state.clone()).await;
    let lease = provider
        .get("allowed.test".parse().unwrap(), 443)
        .await
        .unwrap();
    let binding = HostBinding::new(
        "test",
        "93.184.216.34:443".parse().unwrap(),
        state,
        Default::default(),
    );
    let mut roots = rustls::RootCertStore::empty();
    roots.add(root.der().clone()).unwrap();
    let tls = crate::tls_interception::TlsInterception::default().with_upstream_roots(roots);
    (binding, lease, tls)
}

#[tokio::test]
async fn inspected_tls_drops_fastly_routing_headers_before_forwarding() {
    use http_body_util::{BodyExt, StreamBody};
    use hyper::body::Frame;

    for (guest_protocol, origin_protocol, safe_trailers) in [
        (HttpProtocol::Http1, HttpProtocol::Http1, true),
        (HttpProtocol::Http2, HttpProtocol::Http1, true),
        (HttpProtocol::Http2, HttpProtocol::Http2, true),
        (HttpProtocol::Http1, HttpProtocol::Http2, true),
        // Removing every trailer must still terminate either wire protocol.
        (HttpProtocol::Http1, HttpProtocol::Http1, false),
        (HttpProtocol::Http2, HttpProtocol::Http2, false),
    ] {
        let (binding, lease, _) = fixture().await;
        let (origin, origin_peer) = tokio::io::duplex(4096);
        let (observed, mut received) = tokio::sync::mpsc::channel(1);
        let origin_task = tokio::spawn(async move {
            let service = service_fn(move |request: Request<Incoming>| {
                let observed = observed.clone();
                async move {
                    let (parts, body) = request.into_parts();
                    let body = body.collect().await.unwrap();
                    let trailers = body.trailers().cloned().unwrap_or_default();
                    observed
                        .send((parts, body.to_bytes(), trailers))
                        .await
                        .unwrap();
                    Ok::<_, Infallible>(Response::new(Empty::<Bytes>::new()))
                }
            });
            match origin_protocol {
                HttpProtocol::Http1 => hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(origin_peer), service)
                    .await
                    .unwrap(),
                HttpProtocol::Http2 => {
                    hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(origin_peer), service)
                        .await
                        .unwrap()
                }
            }
        });
        let (guest, guest_peer) = tokio::io::duplex(4096);
        let gateway = tokio::spawn(serve(guest, binding, lease, move |_| async move {
            Ok((origin, origin_protocol))
        }));
        let mut request = Request::get("https://allowed.test/")
            .header("fastly-host", "forbidden.test")
            .header("fastly-service", "forbidden-service")
            .header("fastly-backend", "forbidden-backend")
            .header("fastly-key", "fixture-token")
            .header(header::TRAILER, "Fastly-Host")
            .header(header::AUTHORIZATION, "Bearer fixture-token");
        let mut trailers = HeaderMap::new();
        trailers.insert("fastly-host", "forbidden.test".parse().unwrap());
        if safe_trailers {
            request = request.header(header::TRAILER, "Fastly-Key, x-checksum");
            trailers.insert("fastly-key", "trailer-token".parse().unwrap());
            trailers.insert("x-checksum", "checksum".parse().unwrap());
        }
        if guest_protocol == HttpProtocol::Http1 {
            request = request
                .header(header::HOST, "allowed.test")
                // Hyper needs explicit framing for an unknown-length GET body.
                .header(header::TRANSFER_ENCODING, "chunked")
                .header(header::CONNECTION, "X-Hop-One, TE")
                .header(header::CONNECTION, "x-HOP-Two, keep-alive")
                .header("x-hop-one", "first-hop")
                .header("x-hop-two", "second-hop")
                .header("keep-alive", "timeout=30")
                .header("proxy-connection", "keep-alive")
                .header(header::TE, "trailers")
                .header(header::TRAILER, "x-hop-one, x-hop-two");
            trailers.insert("x-hop-one", "first-trailer".parse().unwrap());
            trailers.insert("x-hop-two", "second-trailer".parse().unwrap());
        }
        let body = StreamBody::new(futures_util::stream::iter([
            Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"streamed body"))),
            Ok(Frame::trailers(trailers)),
        ]));
        let request = request.body(body).unwrap();
        let (response, client) = match guest_protocol {
            HttpProtocol::Http1 => {
                let (mut sender, connection) =
                    http1::handshake(TokioIo::new(guest_peer)).await.unwrap();
                let client = tokio::spawn(connection);
                (
                    tokio::time::timeout(Duration::from_secs(2), sender.send_request(request))
                        .await
                        .expect("HTTP/1 request completed")
                        .unwrap(),
                    client,
                )
            }
            HttpProtocol::Http2 => {
                let (mut sender, connection) =
                    http2::handshake(TokioExecutor::new(), TokioIo::new(guest_peer))
                        .await
                        .unwrap();
                let client = tokio::spawn(connection);
                (
                    tokio::time::timeout(Duration::from_secs(2), sender.send_request(request))
                        .await
                        .expect("HTTP/2 request completed")
                        .unwrap(),
                    client,
                )
            }
        };
        assert_eq!(response.status(), StatusCode::OK);
        let (forwarded, data, trailers) = received.recv().await.unwrap();
        assert_eq!(data, "streamed body");
        if safe_trailers {
            assert_eq!(trailers["fastly-key"], "trailer-token");
            assert_eq!(trailers["x-checksum"], "checksum");
        } else {
            assert!(trailers.is_empty());
            assert!(!forwarded.headers.contains_key(header::TRAILER));
        }
        assert!(!trailers.contains_key("fastly-host"));
        for name in ["keep-alive", "proxy-connection", "x-hop-one", "x-hop-two"] {
            assert!(
                !forwarded.headers.contains_key(name),
                "forwarded header {name}"
            );
            assert!(!trailers.contains_key(name), "forwarded trailer {name}");
        }
        assert!(!trailers.contains_key(header::CONNECTION));
        assert!(!trailers.contains_key(header::TE));
        if guest_protocol == HttpProtocol::Http1 {
            assert_eq!(forwarded.headers[header::TE], "trailers");
        } else {
            assert!(!forwarded.headers.contains_key(header::TE));
        }
        if guest_protocol == HttpProtocol::Http1 && origin_protocol == HttpProtocol::Http1 {
            assert!(
                forwarded.headers[header::CONNECTION]
                    .to_str()
                    .unwrap()
                    .eq_ignore_ascii_case("te")
            );
        } else {
            assert!(!forwarded.headers.contains_key(header::CONNECTION));
        }
        for declaration in forwarded.headers.get_all(header::TRAILER) {
            assert!(
                declaration
                    .to_str()
                    .unwrap()
                    .split(',')
                    .all(|name| matches!(
                        name.trim().to_ascii_lowercase().as_str(),
                        "fastly-key" | "x-checksum"
                    ))
            );
        }
        assert_eq!(
            forwarded
                .headers
                .keys()
                .filter(|name| name.as_str().starts_with("fastly-"))
                .map(|name| name.as_str())
                .collect::<Vec<_>>(),
            ["fastly-key"]
        );
        assert_eq!(forwarded.headers["fastly-key"], "fixture-token");
        assert_eq!(
            forwarded.headers[header::AUTHORIZATION],
            "Bearer fixture-token"
        );
        if origin_protocol == HttpProtocol::Http1 {
            assert_eq!(forwarded.headers[header::HOST], "allowed.test");
            assert_eq!(forwarded.uri, "/");
        } else {
            assert_eq!(forwarded.uri, "https://allowed.test/");
        }
        client.abort();
        gateway.abort();
        origin_task.abort();
    }
}

#[derive(Clone, Copy, Debug)]
enum UpstreamEnd {
    SetupFailed,
    SetupCancelled,
    Http1Close,
    H2Goaway,
}

#[tokio::test]
async fn upstream_failure_or_closure_never_leaves_a_poisoned_guest_connection() {
    use http_body_util::{BodyExt, Full};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut poisoned = Vec::new();
    for (ending, protocol) in [
        (UpstreamEnd::SetupFailed, HttpProtocol::Http2),
        (UpstreamEnd::SetupCancelled, HttpProtocol::Http2),
        (UpstreamEnd::Http1Close, HttpProtocol::Http1),
        (UpstreamEnd::Http1Close, HttpProtocol::Http2),
        (UpstreamEnd::H2Goaway, HttpProtocol::Http2),
    ] {
        let (binding, lease, _) = fixture().await;
        let started = Arc::new(tokio::sync::Notify::new());
        let completed = Arc::new(tokio::sync::Notify::new());
        let (origin, mut origin_peer) = tokio::io::duplex(4096);
        let origin_done = completed.clone();
        let origin_task = tokio::spawn(async move {
            match ending {
                UpstreamEnd::Http1Close => {
                    let mut header = Vec::new();
                    while !header.ends_with(b"\r\n\r\n") {
                        header.push(origin_peer.read_u8().await.unwrap());
                    }
                    origin_peer.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndone").await.unwrap();
                }
                UpstreamEnd::H2Goaway => {
                    let connection = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                        .serve_connection(
                            TokioIo::new(origin_peer),
                            service_fn(|_: Request<Incoming>| async {
                                Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(
                                    b"done",
                                ))))
                            }),
                        );
                    tokio::pin!(connection);
                    tokio::select! {
                        result = &mut connection => { result.unwrap(); return; }
                        () = origin_done.notified() => {}
                    }
                    connection.as_mut().graceful_shutdown();
                    // The final GOAWAY write can race the gateway's close.
                    let _ = connection.await;
                }
                _ => {}
            }
        });
        let (guest, guest_peer) = tokio::io::duplex(4096);
        let connect_started = started.clone();
        let gateway = tokio::spawn(serve(guest, binding, lease, move |_| async move {
            connect_started.notify_one();
            match ending {
                UpstreamEnd::SetupFailed => Err(TlsTransportError::Upstream(std::io::Error::from(
                    std::io::ErrorKind::ConnectionRefused,
                ))),
                UpstreamEnd::SetupCancelled => std::future::pending().await,
                UpstreamEnd::Http1Close => Ok((origin, HttpProtocol::Http1)),
                UpstreamEnd::H2Goaway => Ok((origin, HttpProtocol::Http2)),
            }
        }));
        let request = || {
            Request::get("https://allowed.test/")
                .header(header::HOST, "allowed.test")
                .body(Empty::<Bytes>::new())
                .unwrap()
        };
        let closed = if matches!(protocol, HttpProtocol::Http1) {
            let (mut sender, connection) =
                http1::handshake(TokioIo::new(guest_peer)).await.unwrap();
            let mut client = tokio::spawn(connection);
            let response = sender.send_request(request()).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                "done"
            );
            // Keep the sender alive: origin closure, rather than dropping the
            // client handle, must terminate the guest's keep-alive connection.
            let ended = tokio::time::timeout(Duration::from_secs(1), &mut client).await;
            client.abort();
            drop(sender);
            ended.is_ok_and(|joined| joined.is_ok_and(|result| result.is_ok()))
        } else {
            let (mut sender, connection) =
                http2::handshake(TokioExecutor::new(), TokioIo::new(guest_peer))
                    .await
                    .unwrap();
            let mut client = tokio::spawn(connection);
            let sending = tokio::spawn(sender.send_request(request()));
            started.notified().await;
            if matches!(ending, UpstreamEnd::SetupCancelled) {
                sending.abort();
            } else {
                let response = sending.await.unwrap().unwrap();
                assert_eq!(
                    response.status(),
                    if matches!(ending, UpstreamEnd::SetupFailed) {
                        StatusCode::BAD_GATEWAY
                    } else {
                        StatusCode::OK
                    }
                );
                response.into_body().collect().await.unwrap();
            }
            completed.notify_one();
            let ended = tokio::time::timeout(Duration::from_secs(1), &mut client).await;
            client.abort();
            drop(sender);
            ended.is_ok_and(|joined| joined.is_ok_and(|result| result.is_ok()))
        };
        if !closed {
            poisoned.push((ending, protocol));
        }
        gateway.abort();
        origin_task.abort();
    }
    assert!(
        poisoned.is_empty(),
        "poisoned guest connections: {poisoned:?}"
    );
}

#[test]
fn upgrades_require_a_valid_websocket_handshake() {
    // RFC 6455's published nonce/accept pair is an independent handshake oracle.
    let mut request = Request::builder()
        .header(header::UPGRADE, "websocket")
        .header(header::CONNECTION, "keep-alive, Upgrade")
        .header(header::SEC_WEBSOCKET_VERSION, "13")
        .header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
        .body(())
        .unwrap();
    let handshake = upgrade::Handshake::request(&request.clone().try_into().unwrap())
        .unwrap()
        .unwrap();
    for (status, accept, accepted) in [
        (101, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", true),
        (200, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", false),
        (101, "unrelated-handshake", false),
    ] {
        let response = Response::builder()
            .status(status)
            .header(header::UPGRADE, "websocket")
            .header(header::CONNECTION, "upgrade")
            .header(header::SEC_WEBSOCKET_ACCEPT, accept)
            .body(())
            .unwrap();
        assert_eq!(handshake.accepts(&response), accepted);
    }
    request
        .headers_mut()
        .append(header::SEC_WEBSOCKET_KEY, "another-key".parse().unwrap());
    assert!(upgrade::Handshake::request(&request.try_into().unwrap()).is_err());
}

#[tokio::test]
async fn connect_relays_private_ca_tls_without_inspecting_the_inner_request() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (binding, lease, _) = fixture().await;
    // A second issuer is trusted only by the guest's inner TLS client.
    let (_, inner, guest_trust) = fixture().await;
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_cert_resolver(Arc::new(rustls::sign::SingleCertAndKey::from(
        inner.certified_key().unwrap(),
    )));
    let payload = b"GET / HTTP/1.1\r\nHost: different.test\r\nFastly-Host: inner.test\r\n\r\n";
    let (origin, mut proxy) = tokio::io::duplex(128);
    let upstream = tokio::spawn(async move {
        let mut headers = Vec::new();
        while !headers.ends_with(b"\r\n\r\n") {
            headers.push(proxy.read_u8().await.unwrap());
        }
        let headers = String::from_utf8(headers).unwrap();
        assert!(headers.starts_with("CONNECT allowed.test:443 HTTP/1.1\r\n"));
        assert!(headers.contains("proxy-authorization: Basic Zml4dHVyZQ==\r\n"));
        assert!(!headers.contains("x-hop:"));
        assert!(!headers.contains("connection:"));
        assert_eq!(
            headers
                .lines()
                .filter(|line| line.starts_with("fastly-"))
                .collect::<Vec<_>>(),
            ["fastly-key: fixture-token"]
        );
        proxy
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await
            .unwrap();
        let mut stream = tokio_rustls::TlsAcceptor::from(Arc::new(config))
            .accept(proxy)
            .await
            .unwrap();
        let mut received = vec![0; payload.len()];
        stream.read_exact(&mut received).await.unwrap();
        assert_eq!(received, payload);
        stream.write_all(&received).await.unwrap();
        stream.flush().await.unwrap();
    });
    let (guest, guest_peer) = tokio::io::duplex(128);
    let gateway = tokio::spawn(serve(guest, binding, lease, move |protocol| async move {
        assert_eq!(protocol, HttpProtocol::Http1);
        Ok((origin, HttpProtocol::Http1))
    }));
    let (mut sender, connection) = http1::handshake(TokioIo::new(guest_peer)).await.unwrap();
    let client = tokio::spawn(connection.with_upgrades());
    let mut phase = "CONNECT response";
    let completed = tokio::time::timeout(Duration::from_secs(2), async {
        let request = Request::builder()
            .method("CONNECT")
            .uri("allowed.test:443")
            .header(header::HOST, "allowed.test:443")
            .header(header::PROXY_AUTHORIZATION, "Basic Zml4dHVyZQ==")
            .header(header::CONNECTION, "X-Hop")
            .header("x-hop", "proxy-hop")
            .header("fastly-host", "forbidden.test")
            .header("fastly-key", "fixture-token")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let mut response = sender.send_request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        phase = "upgrade";
        let stream = TokioIo::new(hyper::upgrade::on(&mut response).await.unwrap());
        phase = "inner TLS";
        let mut stream = tokio_rustls::TlsConnector::from(
            guest_trust.upstream_config(&[crate::tls_sni::HttpAlpn::Http1]),
        )
        .connect("allowed.test".try_into().unwrap(), stream)
        .await
        .unwrap();
        phase = "inner payload";
        stream.write_all(payload).await.unwrap();
        stream.flush().await.unwrap();
        let mut received = vec![0; payload.len()];
        stream.read_exact(&mut received).await.unwrap();
        assert_eq!(received, payload);
        upstream.await.unwrap();
    })
    .await;
    assert!(completed.is_ok(), "CONNECT stalled at {phase}");
    client.abort();
    gateway.abort();
}

#[tokio::test]
async fn upstream_upgrade_completion_preserves_the_guest_handshake_and_stream() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (binding, lease, _) = fixture().await;
    // Repeated handoffs exercise the race between the ready 101 response and
    // the HTTP driver's completion. Tiny buffers also force partial writes.
    for iteration in 0..16 {
        let (origin, mut origin_peer) = tokio::io::duplex(64);
        let origin_task = tokio::spawn(async move {
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(origin_peer.read_u8().await.unwrap());
            }
            let request = String::from_utf8(request).unwrap();
            assert!(!request.contains("x-hop:"));
            assert!(request.contains("connection: upgrade\r\n"));
            assert!(request.contains("upgrade: websocket\r\n"));
            assert_eq!(
                request
                    .lines()
                    .filter(|line| line.starts_with("fastly-"))
                    .collect::<Vec<_>>(),
                ["fastly-key: fixture-token"]
            );
            origin_peer.write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n").await.unwrap();
            let mut data = [0; 4];
            origin_peer.read_exact(&mut data).await.unwrap();
            origin_peer.write_all(&data).await.unwrap();
        });
        let (guest, guest_peer) = tokio::io::duplex(64);
        let gateway = tokio::spawn(serve(
            guest,
            binding.clone(),
            lease.clone(),
            move |_| async move { Ok((origin, HttpProtocol::Http1)) },
        ));
        let (mut sender, connection) = http1::handshake(TokioIo::new(guest_peer)).await.unwrap();
        let client = tokio::spawn(connection.with_upgrades());
        let request = Request::get("https://allowed.test/")
            .header(header::HOST, "allowed.test")
            .header(header::CONNECTION, "Upgrade, X-Hop")
            .header("x-hop", "websocket-hop")
            .header(header::UPGRADE, "websocket")
            .header(header::SEC_WEBSOCKET_VERSION, "13")
            .header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
            .header("fastly-host", "forbidden.test")
            .header("fastly-key", "fixture-token")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let mut phase = "response";
        let completed = tokio::time::timeout(Duration::from_secs(1), async {
            let mut response = sender.send_request(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
            assert_eq!(response.headers()[header::CONNECTION], "upgrade");
            phase = "upgrade";
            let mut upgraded = TokioIo::new(hyper::upgrade::on(&mut response).await.unwrap());
            phase = "echo";
            upgraded.write_all(b"echo").await.unwrap();
            let mut received = [0; 4];
            upgraded.read_exact(&mut received).await.unwrap();
            assert_eq!(&received, b"echo");
        })
        .await;
        assert!(completed.is_ok(), "handoff {iteration}: {phase}");
        origin_task.await.unwrap();
        client.abort();
        gateway.abort();
    }
}

/// Streams `total` bytes in `frame`-byte DATA frames to the guest-facing
/// HTTP/2 server while its handler leaves the body unread for `stall`, as when
/// the upstream applies backpressure. Returns whether the full body arrived.
async fn small_frame_upload(frame: usize, total: usize, stall: Duration) -> bool {
    use http_body_util::{BodyExt, Full};

    let (client_io, server_io) = tokio::io::duplex(1 << 20);
    let service = service_fn(move |request: Request<Incoming>| async move {
        tokio::time::sleep(stall).await;
        let read = request
            .into_body()
            .collect()
            .await
            .map_or(0, |body| body.to_bytes().len());
        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from(read.to_string()))))
    });
    tokio::spawn(async move {
        let _ = guest_http_builder()
            .serve_connection(TokioIo::new(server_io), service)
            .await;
    });

    let (mut client, connection) = h2::client::handshake(client_io).await.unwrap();
    tokio::spawn(connection);
    let request = Request::post("https://allowed.test/upload")
        .body(())
        .unwrap();
    let (response, mut body) = client.send_request(request, false).unwrap();
    let chunk = Bytes::from(vec![b'0'; frame]);
    let mut sent = 0;
    while sent < total {
        body.reserve_capacity(frame);
        match std::future::poll_fn(|cx| body.poll_capacity(cx)).await {
            Some(Ok(_)) => {}
            _ => return false,
        }
        if body.send_data(chunk.clone(), false).is_err() {
            return false;
        }
        sent += frame;
    }
    if body.send_data(Bytes::new(), true).is_err() {
        return false;
    }
    let Ok(Ok(response)) = tokio::time::timeout(Duration::from_secs(20), response).await else {
        return false;
    };
    let mut body = response.into_body();
    let mut read = Vec::new();
    while let Some(chunk) = body.data().await {
        read.extend_from_slice(&chunk.unwrap());
    }
    read == total.to_string().as_bytes()
}

#[tokio::test]
async fn guest_http2_accepts_backpressured_small_frame_uploads() {
    // git's pkt-lines are ~50 bytes, and curl sends each as its own frame.
    assert!(small_frame_upload(50, 5_000_000, Duration::from_secs(2)).await);
}

#[tokio::test]
async fn guest_http2_still_rejects_tiny_frame_floods() {
    assert!(!small_frame_upload(1, 1_000_000, Duration::from_secs(2)).await);
}
