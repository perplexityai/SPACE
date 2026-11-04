use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct ProbeBody(Arc<AtomicUsize>);

impl http_body::Body for ProbeBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(None)
    }
}

#[tokio::test]
async fn mounted_authorization_precedes_decoding_even_for_new_methods() {
    for disabled in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let body_polls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        // The handler has no authorization check; decoding must be unreachable
        // for missing credentials, including methods added in the future.
        let unchecked = tower::service_fn(move |request: http::Request<ProbeBody>| {
            observed.fetch_add(1, Ordering::SeqCst);
            async move {
                let mut body = request.into_body();
                std::future::poll_fn(|cx| http_body::Body::poll_frame(Pin::new(&mut body), cx))
                    .await;
                Ok::<_, std::convert::Infallible>(http::Response::new(Body::empty()))
            }
        });
        let host = SpacedHostService::new_with_config(
            tokio_util::sync::CancellationToken::new(),
            super::super::SpacedHostServiceConfig {
                host_peer_authorization: if disabled {
                    super::super::SpacedHostPeerAuthorization::Disabled
                } else {
                    super::super::SpacedHostPeerAuthorization::Enforce
                },
                ..Default::default()
            },
        );
        let mut service = HostAuthorizationLayer(host).layer(unchecked);
        for method in ["Ping", "FutureMethod"] {
            let request = http::Request::builder()
                .uri(format!("/space.spaced.host.v1.Meta/{method}"))
                .body(ProbeBody(body_polls.clone()))
                .expect("request");
            let response = service.call(request).await.expect("service response");
            if disabled {
                assert!(response.headers().get("grpc-status").is_none());
            } else {
                assert_eq!(response.headers()["grpc-status"], "16");
            }
        }
        assert_eq!(calls.load(Ordering::SeqCst), if disabled { 2 } else { 0 });
        assert_eq!(
            body_polls.load(Ordering::SeqCst),
            if disabled { 2 } else { 0 }
        );
    }
}

#[tokio::test]
async fn enforced_authorization_accepts_real_uds_provenance_through_the_host_listener() {
    use super::super::{BridgeGuestConnector, SPACE_SBXMGR_ACCOUNT, SpacedHostServiceConfig};
    use pplx_space_common_svc::{UdsPeerAccount, UdsPeerAuthorizer};
    use pplx_space_spaced_host_client::{SpacedHostClient, SpacedHostClientConfig};
    use std::time::Duration;

    let directory = tempfile::tempdir().expect("host sockets");
    let socket_path = directory.path().join("host.sock");
    let (peer, _connection) = tokio::net::UnixStream::pair().expect("local peer credentials");
    let uid = peer.peer_cred().expect("kernel peer credentials").uid();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let mut host = SpacedHostService::new_with_config(
        shutdown.clone(),
        SpacedHostServiceConfig {
            socket_path: socket_path.clone(),
            bridge_host_socket_path: directory.path().join("bridge.sock"),
            bridge_guest_connector: BridgeGuestConnector::Uds(directory.path().join("guest.sock")),
            host_control_socket_path: directory.path().join("control.sock"),
            ..Default::default()
        },
    );
    // Resolve the test's manager account to this process's real UID. Enforce
    // remains enabled; only the owning OS account differs from deployment.
    Arc::get_mut(&mut host.inner)
        .expect("unshared host")
        .host_peer_authorizer = UdsPeerAuthorizer::new(
        "test",
        [UdsPeerAccount::with_fallback_uid(SPACE_SBXMGR_ACCOUNT, uid)],
    );
    let server = tokio::spawn(host.run());
    let client = SpacedHostClient::new_with_config(
        SpacedHostClientConfig::default().with_socket_path(socket_path),
    )
    .await
    .expect("connect to enforced host listener");
    client
        .ping()
        .await
        .expect("real UDS manager identity is authorized");
    shutdown.cancel();
    drop(client);
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("host drains")
        .expect("host task")
        .expect("host shutdown");
}
