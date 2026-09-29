use super::*;
use std::convert::Infallible;
use std::future::ready;
use std::sync::Mutex;
use std::time::Duration;

use pplx_space_common_svc::PeerIdentity;
use pplx_space_common_svc::bootstrap::wrap_grpc_client_channel;
use pplx_space_credmgr_client::TlsFiles;
use pplx_space_credmgr_client::{CredentialManagerClient, CredmgrBearerInterceptor};
use std::path::PathBuf;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::service::interceptor::InterceptedService;

use pplx_space_credential_injection_core::proto::ResolvedNetworkCredential;

use crate::proto::{
    ResolveNetworkCredentialsRequest as ResolveRequest,
    ResolveNetworkCredentialsResponse as ResolveResponse,
};
use crate::tcp_auth::TcpCallerIdentity;

#[test]
fn uds_upstream_is_rejected_as_loopback() {
    let endpoint =
        CredmgrEndpoint::Uds(PathBuf::from("/space/run/credential-manager/credmgr.sock"));
    assert_eq!(
        validate_upstream(&endpoint),
        Err(AgentUpstreamError::Loopback)
    );
}

#[test]
fn plaintext_tcp_upstream_is_accepted() {
    let uri = "http://space-credential-manager:9810".parse().unwrap();
    validate_upstream(&CredmgrEndpoint::Tcp(uri)).unwrap();
}

#[test]
fn server_tls_upstream_is_accepted() {
    let uri = "https://space-credential-manager:9810".parse().unwrap();
    let endpoint = CredmgrEndpoint::Tls {
        uri,
        files: TlsFiles::new(PathBuf::from("/etc/space-tls/ca.crt")),
    };
    validate_upstream(&endpoint).unwrap();
}

#[test]
fn ok_call_classifies_as_ok() {
    assert_eq!(ForwardOutcome::from_code(None), ForwardOutcome::Ok);
    assert_eq!(ForwardOutcome::Ok.result_tag(), "ok");
}

#[test]
fn only_unavailable_is_a_reachability_failure() {
    // `Unavailable` is the one transport/TLS-level signal: the request
    // never got an answer from the authority.
    assert_eq!(
        ForwardOutcome::from_code(Some(Code::Unavailable)),
        ForwardOutcome::Unreachable
    );
    assert_eq!(ForwardOutcome::Unreachable.result_tag(), "unreachable");
}

#[test]
fn non_unavailable_codes_classify_as_upstream_response_not_unreachable() {
    // Regression: the proxy used to log *every* non-OK status as "failed
    // to reach the upstream authority". A status the authority produced
    // means it was reached — it must never be classified as unreachable.
    for code in [
        Code::FailedPrecondition,
        Code::NotFound,
        Code::PermissionDenied,
        Code::InvalidArgument,
        Code::AlreadyExists,
        Code::ResourceExhausted,
        Code::Aborted,
        Code::OutOfRange,
        Code::Unimplemented,
        Code::Internal,
        Code::DataLoss,
        Code::Unauthenticated,
        Code::Unknown,
        Code::Cancelled,
        Code::DeadlineExceeded,
    ] {
        let outcome = ForwardOutcome::from_code(Some(code));
        assert_eq!(
            outcome,
            ForwardOutcome::UpstreamError,
            "{code:?} is the authority's response, not a reachability failure"
        );
        assert_eq!(outcome.result_tag(), "upstream_error");
    }
}

type RecordedRequests = Arc<Mutex<Vec<ResolveRequest>>>;

async fn with_authority<F, Fut>(response: ResolveResponse, test: F)
where
    F: FnOnce(NodeCredentialManager, RecordedRequests) -> Fut,
    Fut: Future<Output = ()>,
{
    with_authority_reply(move || ready(Ok(Response::new(response.clone()))), test).await;
}

async fn with_authority_reply<Reply, ReplyFuture, F, Fut>(reply: Reply, test: F)
where
    Reply: Fn() -> ReplyFuture + Clone + Send + 'static,
    ReplyFuture: Future<Output = Result<Response<ResolveResponse>, Status>> + Send + 'static,
    F: FnOnce(NodeCredentialManager, RecordedRequests) -> Fut,
    Fut: Future<Output = ()>,
{
    let requests = RecordedRequests::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = tonic::transport::Endpoint::from_shared(format!(
        "http://{}",
        listener.local_addr().unwrap()
    ))
    .unwrap();
    let recorded = Arc::clone(&requests);
    let resolve = tower::service_fn(move |request: Request<ResolveRequest>| {
        recorded.lock().unwrap().push(request.into_inner());
        reply()
    });
    let service = tower::service_fn(move |request: http::Request<tonic::body::Body>| {
        // Any certificate-issuance RPC would fail this assertion too.
        assert_eq!(
            request.uri().path(),
            "/space.credential.manager.v1.CredentialManager/ResolveNetworkCredentials"
        );
        let resolve = resolve.clone();
        async move {
            let codec = tonic_prost::ProstCodec::default();
            Ok::<_, Infallible>(
                tonic::server::Grpc::new(codec)
                    .unary(resolve, request)
                    .await,
            )
        }
    });
    let server = tonic::transport::Server::builder()
        .serve_with_incoming(service, TcpListenerStream::new(listener));
    let exercise = async move {
        let channel = endpoint.connect().await.unwrap();
        let upstream = CredentialManagerClient::new(wrap_grpc_client_channel(
            InterceptedService::new(channel, CredmgrBearerInterceptor::None),
        ));
        let node_ca = Arc::new(NodeCa::new(upstream.clone()).unwrap());
        let node = NodeCredentialManager::new(
            upstream,
            node_ca,
            PeerProcessCheckMode::Off,
            tokio_util::sync::CancellationToken::new(),
        );
        test(node, requests).await;
    };
    // Poll the server only for this test's lifetime; no listener or task is
    // left running after the assertions complete or fail.
    tokio::select! {
        result = server => panic!("test authority stopped unexpectedly: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(5), exercise) => {
            result.expect("node resolution should complete promptly");
        }
    }
}

fn resolve_request() -> ResolveRequest {
    ResolveRequest {
        sandbox_id: uuid::Uuid::now_v7().to_string(),
        execution_generation: 23,
        effective_network_policy_ref: format!(
            "cluster:sandbox:{}:generation:7:sha256:{}",
            uuid::Uuid::now_v7(),
            "01".repeat(32)
        ),
    }
}

fn peer_request(uid: u32) -> Request<ResolveRequest> {
    let mut request = Request::new(resolve_request());
    request
        .extensions_mut()
        .insert(PeerIdentity::new(uid, uid, Some(1)));
    request
}

#[test]
fn exchange_forwarding_overwrites_untrusted_actor_claims() {
    for (uid, expected_actor) in [
        (users::SPACE_VOLMGR.uid, ClientService::VolumeManager),
        (users::SPACE_SBXMGR.uid, ClientService::VolumeManager),
        (users::SPACE_TMPLMGR.uid, ClientService::TemplateManager),
    ] {
        let caller = ResolvedCaller::UdsPeer(PeerIdentity::new(uid, uid, Some(1)));
        let mut request = ExchangeTokenRequest::default();
        request.actor_token = Some(ClientService::BuildManager.to_string());
        request.actor_token_type = Some("attacker-controlled".to_string());
        let forwarded = forwarded_exchange_request(&caller, request).unwrap();
        assert_eq!(
            forwarded.actor_token.as_deref(),
            Some(expected_actor.as_str())
        );
        assert_eq!(
            forwarded.actor_token_type.as_deref(),
            Some(SERVICE_ACTOR_TOKEN_TYPE)
        );
    }

    let root = ResolvedCaller::UdsPeer(PeerIdentity::new(0, 0, Some(1)));
    assert_eq!(
        forwarded_exchange_request(&root, ExchangeTokenRequest::default())
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
}

#[tokio::test]
async fn network_credential_resolution_forwards_only_uuid_keyed_material() {
    let expected = ResolveResponse {
        effective_network_policy_ref: format!(
            "cluster:sandbox:{}:generation:7:sha256:{}",
            uuid::Uuid::now_v7(),
            "02".repeat(32)
        ),
        credentials: vec![ResolvedNetworkCredential {
            credential_reference_id: uuid::Uuid::now_v7().to_string(),
            value: b"Bearer node-proxy-canary".to_vec(),
        }],
    };
    with_authority(expected.clone(), async move |node, received| {
        let request = peer_request(users::SPACE_SBXMGR.uid);
        let sent = request.get_ref().clone();
        let actual = node
            .resolve_network_credentials(request)
            .await
            .unwrap()
            .into_inner();
        assert_eq!(actual, expected);
        assert_eq!(*received.lock().unwrap(), [sent]);
    })
    .await;
}

#[tokio::test]
async fn gateway_signing_authorizes_sandbox_manager_before_acquiring_a_ca() {
    with_authority(ResolveResponse::default(), async |node, received| {
        // The signer tests own CSR/profile validation. An empty CSR proves
        // whether this caller reached the signer without issuing a real CA.
        for (uid, expected) in [
            (users::SPACE_SBXMGR.uid, Code::InvalidArgument),
            (users::SPACE_VOLMGR.uid, Code::PermissionDenied),
            (0, Code::PermissionDenied),
            (65534, Code::PermissionDenied),
        ] {
            let mut request = Request::new(IssueIntermediateCertificateRequest::default());
            request
                .extensions_mut()
                .insert(PeerIdentity::new(uid, uid, Some(1)));
            assert_eq!(
                node.issue_intermediate_certificate(request)
                    .await
                    .unwrap_err()
                    .code(),
                expected
            );
        }
        assert_eq!(
            node.issue_intermediate_certificate(Request::new(
                IssueIntermediateCertificateRequest::default()
            ))
            .await
            .unwrap_err()
            .code(),
            Code::Unauthenticated
        );
        let mut tcp = Request::new(IssueIntermediateCertificateRequest::default());
        tcp.extensions_mut().insert(TcpCallerIdentity::Plaintext);
        assert_eq!(
            node.issue_intermediate_certificate(tcp)
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert!(received.lock().unwrap().is_empty());
    })
    .await;
}

#[tokio::test]
async fn shutdown_cancels_pending_network_credential_forwarding_and_stops_new_reads() {
    let arrived = CancellationToken::new();
    let cancelled = CancellationToken::new();
    let authority_arrived = arrived.clone();
    let authority_cancelled = cancelled.clone();
    with_authority_reply(
        move || {
            let arrived = authority_arrived.clone();
            let cancelled = authority_cancelled.clone();
            async move {
                let _on_drop = cancelled.drop_guard();
                arrived.cancel();
                std::future::pending().await
            }
        },
        async |node, received| {
            // Use the existing manager peer fixture to include RPC-handler wiring.
            let pending = node.resolve_network_credentials(peer_request(users::SPACE_SBXMGR.uid));
            tokio::pin!(pending);
            tokio::select! {
                () = arrived.cancelled() => {},
                result = &mut pending => panic!("request finished before shutdown: {result:?}"),
            }
            node.shutdown_token.cancel();
            let status = tokio::time::timeout(Duration::from_secs(1), pending)
                .await
                .expect("shutdown must release the pending read")
                .unwrap_err();
            assert_eq!(status.code(), Code::Unavailable);
            tokio::time::timeout(Duration::from_secs(1), cancelled.cancelled())
                .await
                .expect("shutdown must cancel the authority RPC too");

            let status = node
                .resolve_network_credentials(peer_request(users::SPACE_SBXMGR.uid))
                .await
                .unwrap_err();
            assert_eq!(status.code(), Code::Unavailable);
            assert_eq!(received.lock().unwrap().len(), 1);
        },
    )
    .await;
}

#[tokio::test]
async fn resolution_rejects_unauthorized_callers_before_contacting_authority() {
    with_authority(ResolveResponse::default(), async |node, received| {
        let mut shared_token = Request::new(resolve_request());
        shared_token
            .extensions_mut()
            .insert(TcpCallerIdentity::Plaintext);
        // auth::tests owns peer classification; this RPC must apply its
        // allowlist and reject the otherwise-authorized shared-token fallback.
        for (request, expected_code) in [
            (Request::new(resolve_request()), Code::Unauthenticated),
            (peer_request(0), Code::PermissionDenied),
            (shared_token, Code::PermissionDenied),
        ] {
            let status = node.resolve_network_credentials(request).await.unwrap_err();
            assert_eq!(status.code(), expected_code);
        }
        assert!(received.lock().unwrap().is_empty());
    })
    .await;
}
