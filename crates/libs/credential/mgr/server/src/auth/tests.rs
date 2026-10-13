use pplx_space_common_svc::{PeerProcessCheckMode, UdsPeerAccount};

use super::*;

/// A resolver with no per-account process checks configured: the allow-list is
/// the whole policy, which is what the pre-existing cases below exercise.
fn plain_resolver() -> CallerResolver {
    CallerResolver::new(UdsPeerAuthorizer::new("space-credential-manager", []))
}

fn k8s_identity(service: ClientService) -> K8sIdentity {
    K8sIdentity::from_token_review(
        service,
        "credential-client",
        "credential-client-uid",
        "worker-node",
        "worker-node-uid",
    )
    .unwrap()
}

#[test]
fn client_service_round_trips_via_as_str() {
    for service in ClientService::ALL {
        assert_eq!(service.as_str().parse::<ClientService>(), Ok(service));
    }
}

#[test]
fn node_agent_wire_name_is_the_credmgr_service_account() {
    // The node credmgr DaemonSet runs as the `space-credential-manager`
    // ServiceAccount, so that is the name TokenReview reports for the agent's
    // projected token. If the variant's wire form drifts from it, the agent's
    // JWT resolves to no known identity and every forwarded RPC is denied.
    assert_eq!(
        ClientService::NodeCredentialAgent.as_str(),
        "space-credential-manager"
    );
    assert_eq!(
        "space-credential-manager".parse::<ClientService>(),
        Ok(ClientService::NodeCredentialAgent)
    );
}

#[test]
fn unknown_service_name_fails_parse() {
    let err = "space-unknown-manager"
        .parse::<ClientService>()
        .unwrap_err();
    assert_eq!(
        err,
        ParseClientServiceError("space-unknown-manager".to_string())
    );
}

#[test]
fn k8s_jwt_in_allow_list_is_authorized() {
    plain_resolver()
        .authorize_caller(
            &ResolvedCaller::K8sJwt(k8s_identity(ClientService::VolumeManager)),
            "IssueDek",
            &[],
            &[ClientService::SandboxManager, ClientService::VolumeManager],
        )
        .unwrap();
}

#[test]
fn k8s_jwt_outside_allow_list_is_rejected() {
    let err = plain_resolver()
        .authorize_caller(
            &ResolvedCaller::K8sJwt(k8s_identity(ClientService::ApiSvc)),
            "ResolveNetworkCredentials",
            &[],
            &[ClientService::SandboxManager],
        )
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert!(err.message().contains("space-api"));
    assert!(err.message().contains("ResolveNetworkCredentials"));
}

#[test]
fn plaintext_is_authorized_for_any_operation() {
    // Reachable only on direct-node plaintext profiles; by then the caller is
    // the shared internal principal.
    plain_resolver()
        .authorize_caller(&ResolvedCaller::Plaintext, "UnwrapDek", &[], &[])
        .unwrap();
}

// Each TCP resolver claims only its own injected identity, so CallerResolver
// routes a request to exactly one home.
#[test]
fn k8s_jwt_resolver_claims_only_the_jwt_identity() {
    let mut jwt = Request::new(());
    jwt.extensions_mut()
        .insert(TcpCallerIdentity::K8sJwt(k8s_identity(
            ClientService::ApiSvc,
        )));
    let Some(ResolvedCaller::K8sJwt(identity)) = K8sJwtResolver.resolve(&jwt) else {
        panic!("projected-token identity did not resolve");
    };
    assert_eq!(identity.service(), ClientService::ApiSvc);
    assert!(PlaintextResolver.resolve(&jwt).is_none());

    let mut plain = Request::new(());
    plain.extensions_mut().insert(TcpCallerIdentity::Plaintext);
    assert!(matches!(
        PlaintextResolver.resolve(&plain),
        Some(ResolvedCaller::Plaintext)
    ));
    assert!(K8sJwtResolver.resolve(&plain).is_none());
}

#[test]
fn uds_peer_in_allow_list_is_authorized() {
    plain_resolver()
        .authorize_caller(
            &ResolvedCaller::UdsPeer(PeerIdentity::new(0, 0, Some(1))),
            "IssueDek",
            &[UdsPeerPrincipal::Root],
            &[],
        )
        .unwrap();
}

#[test]
fn uds_peer_outside_allow_list_is_rejected() {
    let err = plain_resolver()
        .authorize_caller(
            &ResolvedCaller::UdsPeer(PeerIdentity::new(12_345, 12_345, Some(1))),
            "IssueDek",
            &[UdsPeerPrincipal::Root],
            &[],
        )
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

// --- per-account /proc peer-process checks -----------------------------
//
// The uid→principal table is the only thing that decides the *principal*, but
// an account can additionally demand that the peer's `/proc/<pid>/exe` sit in
// an allowlist. These tests pin that the per-RPC authorization path actually
// runs those checks rather than stopping at the allowlist.

/// Synthetic account name and uid. The uid is deliberately non-zero so
/// classification never short-circuits on the root branch, and deliberately
/// not the uid the test process runs as, so the peer can never satisfy a
/// `require_binary` allowlist.
const TEST_PEER: &str = "test-peer";
const TEST_PEER_UID: u32 = 4242;

/// A resolver whose `TEST_PEER` account requires a binary path no test
/// process can ever have, with process checks armed.
fn resolver_with_armed_binary_check() -> CallerResolver {
    CallerResolver::new(
        UdsPeerAuthorizer::new(
            "space-credential-manager",
            [UdsPeerAccount::with_fallback_uid(TEST_PEER, TEST_PEER_UID)
                .require_binary(&["/no/such/space-binary"])],
        )
        .with_process_check_mode(PeerProcessCheckMode::Enforce),
    )
}

/// A request carrying `TEST_PEER_UID` peer credentials, pointed at this test
/// process's own pid so the `/proc` read has a live target on Linux.
/// `peer_identity_from_tonic_request` reads a pre-inserted [`PeerIdentity`]
/// extension, which is how a test drives the real UDS resolution path without
/// a socket.
fn armed_test_peer_request() -> Request<()> {
    let mut request = Request::new(());
    request.extensions_mut().insert(PeerIdentity::new(
        TEST_PEER_UID,
        TEST_PEER_UID,
        Some(std::process::id() as i32),
    ));
    request
}

#[test]
fn uds_peer_process_checks_reject_when_enforcing() {
    let resolver = resolver_with_armed_binary_check();
    let caller = resolver
        .resolve(&armed_test_peer_request())
        .expect("peer credentials resolve to a UDS caller");
    assert!(matches!(caller, ResolvedCaller::UdsPeer(_)));

    // TEST_PEER_UID classifies to `Service(TEST_PEER)`, which IS in the
    // allow-list, so the only thing that can reject this call is the
    // account's process check. If authorization stops at the allow-list,
    // `require_binary` is dead code and this returns Ok.
    let err = resolver
        .authorize_caller(
            &caller,
            "IssueDek",
            &[UdsPeerPrincipal::Service(TEST_PEER)],
            &[],
        )
        .expect_err("enforce mode must reject a peer outside the binary allowlist");

    // On Linux the /proc read succeeds and the check fails closed with
    // PermissionDenied (the synthetic uid cannot match this process's
    // effective uid, so the pid-reuse guard fires first — either way the
    // layer ran and denied). Off-Linux there is no /proc at all, so the
    // snapshot read is Unsupported and enforce mode fails closed with
    // Unauthenticated. Both are rejections; only the code differs.
    #[cfg(target_os = "linux")]
    assert_eq!(err.code(), tonic::Code::PermissionDenied, "{err:?}");
    #[cfg(not(target_os = "linux"))]
    assert_eq!(err.code(), tonic::Code::Unauthenticated, "{err:?}");
}

/// Root classifies ahead of any account match, so a root-owned test process
/// can never reach a per-account check through the resolver. The
/// allowlist-reaching half runs in common-svc against the snapshot's own uid,
/// which root cannot divert. Classification reads only the presented uid, so
/// this needs no root.
#[test]
fn uds_peer_root_classifies_before_the_binary_check() {
    let resolver = resolver_with_armed_binary_check();
    let mut request = Request::new(());
    request
        .extensions_mut()
        .insert(PeerIdentity::new(0, 0, Some(std::process::id() as i32)));
    let caller = resolver.resolve(&request).expect("root peer resolves");
    resolver
        .authorize_caller(&caller, "IssueDek", &[UdsPeerPrincipal::Root], &[])
        .expect("root classifies before account matching; no account checks run");
}

#[test]
fn uds_peer_binary_check_is_reached_when_uids_match() {
    // The uid-drift guard fires before the binary comparison whenever the
    // presented uid differs from the process's effective uid (the enforce
    // test above exercises exactly that). To prove `require_binary` itself
    // executes end-to-end, present this process's own euid so the /proc
    // effective uid matches and the decision falls through to the allowlist.
    let euid = nix::unistd::geteuid().as_raw();
    let pid = Some(std::process::id() as i32);

    if euid == 0 {
        eprintln!(
            "SKIPPED: root classifies before any account match; \
             uds_peer_root_classifies_before_the_binary_check covers root"
        );
        return;
    }

    let resolver = CallerResolver::new(
        UdsPeerAuthorizer::new(
            "space-credential-manager",
            [UdsPeerAccount::with_fallback_uid(TEST_PEER, euid)
                .require_binary(&["/no/such/space-binary"])],
        )
        .with_process_check_mode(PeerProcessCheckMode::Enforce),
    );
    let mut request = Request::new(());
    request
        .extensions_mut()
        .insert(PeerIdentity::new(euid, euid, pid));
    let caller = resolver
        .resolve(&request)
        .expect("peer credentials resolve");

    let err = resolver
        .authorize_caller(
            &caller,
            "IssueDek",
            &[UdsPeerPrincipal::Service(TEST_PEER)],
            &[],
        )
        .expect_err("the test binary is not in the allowlist");

    // On Linux the /proc effective uid matches, so the decision reaches the
    // binary comparison and the denial names the allowlist. Off-Linux there
    // is no /proc; the read fails Unsupported and enforce fails closed
    // before any comparison.
    #[cfg(target_os = "linux")]
    {
        assert_eq!(err.code(), tonic::Code::PermissionDenied, "{err:?}");
        assert!(
            err.message().contains("not in the configured allowlist"),
            "{err:?}"
        );
    }
    #[cfg(not(target_os = "linux"))]
    assert_eq!(err.code(), tonic::Code::Unauthenticated, "{err:?}");
}

#[test]
fn uds_peer_process_checks_are_skipped_when_gate_is_off() {
    // The env gate defaults to off, which must leave the pre-existing
    // allow-list-only behavior exactly as it was.
    let resolver = CallerResolver::new(
        UdsPeerAuthorizer::new(
            "space-credential-manager",
            [UdsPeerAccount::with_fallback_uid(TEST_PEER, TEST_PEER_UID)
                .require_binary(&["/no/such/space-binary"])],
        )
        .with_process_check_mode(PeerProcessCheckMode::Off),
    );
    let caller = resolver
        .resolve(&armed_test_peer_request())
        .expect("peer credentials resolve to a UDS caller");

    resolver
        .authorize_caller(
            &caller,
            "IssueDek",
            &[UdsPeerPrincipal::Service(TEST_PEER)],
            &[],
        )
        .expect("off mode authorizes on the allow-list alone");
}

#[test]
fn uds_peer_forged_principal_does_not_bypass_classification() {
    // A resolved UDS caller carries only its kernel-attested identity; the
    // principal is derived from it at decision time, so a caller whose label
    // disagrees with its identity is unrepresentable. uid 9999 matches no
    // account and classifies to `Other`: the root-only allow-list must
    // reject it on that classification, with no principal slot through
    // which a `Root` label could pass the list or skip the account checks.
    let resolver = resolver_with_armed_binary_check();
    let err = resolver
        .authorize_caller(
            &ResolvedCaller::UdsPeer(PeerIdentity::new(
                9_999,
                9_999,
                Some(std::process::id() as i32),
            )),
            "IssueDek",
            &[UdsPeerPrincipal::Root],
            &[],
        )
        .expect_err("a peer whose uid classifies to `other` must not pass a root-only allow-list");
    assert_eq!(err.code(), tonic::Code::PermissionDenied, "{err:?}");
    assert!(err.message().contains("local peer other"), "{err:?}");
}

#[test]
fn uds_peer_process_checks_do_not_run_before_the_allow_list() {
    // A peer the allow-list rejects must be denied for that reason, without
    // the authorizer reaching for /proc first.
    let resolver = resolver_with_armed_binary_check();
    let caller = resolver
        .resolve(&armed_test_peer_request())
        .expect("peer credentials resolve to a UDS caller");

    let err = resolver
        .authorize_caller(&caller, "IssueDek", &[UdsPeerPrincipal::Root], &[])
        .expect_err("principal is not in the allow-list");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert!(err.message().contains("is not authorized for"), "{err:?}");
}

const CREDENTIAL_RPC_PREFIX: &str = "/space.credential.manager.v1.CredentialManager/";

#[tokio::test]
async fn volume_listener_limits_both_peer_and_methods_before_dispatch() {
    use tower::ServiceExt;

    for (method, allowed) in [
        ("ExchangeToken", true),
        ("IssueDek", true),
        ("UnwrapDek", true),
        ("IssueIntermediateCertificate", false),
        ("Encrypt", false),
        ("Decrypt", false),
        ("GetOrIssueSandboxVolumeDek", false),
        ("SandboxVolumeEncryptionEnabled", false),
        ("ResolveNetworkCredentials", false),
        ("FutureMethod", false),
    ] {
        for uid in [users::SPACE_VOLMGR.uid, users::SPACE_SBXMGR.uid, 0, 65534] {
            let dispatched = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let observed = dispatched.clone();
            let service = CredentialUdsAccess::Volume.layer(tower::service_fn(move |_| {
                observed.store(true, std::sync::atomic::Ordering::SeqCst);
                async {
                    Ok::<_, std::convert::Infallible>(http::Response::new(
                        tonic::body::Body::empty(),
                    ))
                }
            }));
            let mut request = http::Request::builder()
                .uri(format!("{CREDENTIAL_RPC_PREFIX}{method}"))
                .body(())
                .unwrap();
            request
                .extensions_mut()
                .insert(PeerIdentity::new(uid, 951, Some(1)));
            let result = service.oneshot(request).await;
            let expected = allowed && uid == users::SPACE_VOLMGR.uid;
            assert_eq!(
                dispatched.load(std::sync::atomic::Ordering::SeqCst),
                expected,
                "{method}: {uid}"
            );
            let response = result.expect("authorization returns a gRPC response");
            let status = Status::from_header_map(response.headers());
            if expected {
                assert!(
                    status.is_none(),
                    "the inner service answered {method}: {uid}"
                );
            } else {
                assert_eq!(status.unwrap().code(), tonic::Code::PermissionDenied);
            }
        }
    }
}

#[test]
fn standard_listener_cannot_be_used_to_bypass_volume_policy() {
    for uid in [
        users::SPACE_VOLMGR.uid,
        users::SPACE_SBXMGR.uid,
        users::SPACE_TMPLMGR.uid,
        0,
    ] {
        let mut request = Request::new(());
        request
            .extensions_mut()
            .insert(PeerIdentity::new(uid, 951, Some(1)));
        let result = CredentialUdsAccess::Standard.authorize(
            &format!("{CREDENTIAL_RPC_PREFIX}IssueIntermediateCertificate"),
            &request,
            &volume_peer_authorizer(),
        );
        if uid == users::SPACE_VOLMGR.uid {
            assert_eq!(result.unwrap_err().code(), tonic::Code::PermissionDenied);
        } else {
            result.unwrap(); // The existing handler still applies its own RPC policy.
        }
    }
    assert_eq!(
        CredentialUdsAccess::Volume
            .authorize(
                &format!("{CREDENTIAL_RPC_PREFIX}ExchangeToken"),
                &Request::new(()),
                &volume_peer_authorizer(),
            )
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
}
