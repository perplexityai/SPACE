use super::*;
use crate::service::activation_support::sandbox_peer_authorizer;
use pplx_space_common_core::host::linux::identities::users;
use pplx_space_common_svc::space_api_route::{
    SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY, SpaceApiBridgeCallerPolicy,
    set_space_api_bridge_caller_policy,
};

fn service() -> SandboxManagerService {
    SandboxManagerService::for_test(crate::db::Connection::open_in_memory())
}

fn node_api_peer() -> AuthorizedUdsPeer {
    sandbox_peer_authorizer()
        .authorize_identity(
            PeerIdentity::new(
                users::SPACE_NODEAPI.uid,
                users::SPACE_NODEAPI.primary_group.gid,
                None,
            ),
            SpacedRpc::WaitReady.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )
        .expect("node-api identity is authorized")
}

fn root_peer() -> AuthorizedUdsPeer {
    sandbox_peer_authorizer()
        .authorize_identity(
            PeerIdentity::new(0, 0, None),
            SpacedRpc::WaitReady.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )
        .expect("root identity is authorized")
}

fn operator_peer() -> AuthorizedUdsPeer {
    UdsPeerAuthorizer::new(
        "test-sandbox-manager",
        [UdsPeerAccount::role_with_fallback_uid(
            "deploy",
            LocalRole::Operator,
            2001,
        )],
    )
    .authorize_identity(
        PeerIdentity::new(2001, 2001, None),
        SpacedRpc::WaitReady.as_str(),
        ALLOW_SANDBOX_OPERATOR,
    )
    .expect("deploy identity is classified as an operator")
}

fn admin_peer() -> AuthorizedUdsPeer {
    UdsPeerAuthorizer::new(
        "test-sandbox-manager",
        [UdsPeerAccount::role_with_fallback_uid(
            "admin",
            LocalRole::Admin,
            2002,
        )],
    )
    .authorize_identity(
        PeerIdentity::new(2002, 2002, None),
        SpacedRpc::WaitReady.as_str(),
        ALLOW_SANDBOX_OPERATOR,
    )
    .expect("admin identity satisfies the operator requirement")
}

fn trusted_lifecycle_request() -> Request<()> {
    let mut request = Request::new(());
    set_space_api_bridge_caller_policy(
        request.metadata_mut(),
        SpaceApiBridgeCallerPolicy::TrustedLifecycle,
    );
    request
}

#[tokio::test]
async fn local_privileged_callers_are_exempt_without_route_token() {
    if !crate::service::tests::enter_route_test_process(&[]) {
        return;
    }

    for (caller, peer) in [
        ("admin", admin_peer()),
        ("operator", operator_peer()),
        ("root", root_peer()),
    ] {
        let route_auth = service()
            .prepare_classified_space_api_bridge(
                &Request::new(()),
                &SandboxId::new().to_string(),
                SpacedRpc::WaitReady,
                &peer,
            )
            .unwrap_or_else(|error| panic!("{caller} policy should be accepted: {error}"));

        assert!(route_auth.is_none(), "{caller} should be exempt");
    }
}

#[tokio::test]
async fn node_api_requires_an_authenticated_caller_policy() {
    if !crate::service::tests::enter_route_test_process(&[]) {
        return;
    }

    let error = service()
        .prepare_classified_space_api_bridge(
            &Request::new(()),
            &SandboxId::new().to_string(),
            SpacedRpc::WaitReady,
            &node_api_peer(),
        )
        .expect_err("node-api request without a policy must require a route token");

    assert_eq!(error.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn node_api_trusted_lifecycle_policy_is_exempt_without_route_token() {
    if !crate::service::tests::enter_route_test_process(&[]) {
        return;
    }

    let route_auth = service()
        .prepare_classified_space_api_bridge(
            &trusted_lifecycle_request(),
            &SandboxId::new().to_string(),
            SpacedRpc::WaitReady,
            &node_api_peer(),
        )
        .expect("trusted lifecycle policy is accepted");

    assert!(route_auth.is_none());
}

#[tokio::test]
async fn trusted_lifecycle_policy_verifies_a_supplied_token() {
    if !crate::service::tests::enter_route_test_process(&[]) {
        return;
    }
    let mut request = trusted_lifecycle_request();
    request.metadata_mut().insert(
        SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY,
        tonic::metadata::MetadataValue::from_static("bogus"),
    );

    let error = service()
        .prepare_classified_space_api_bridge(
            &request,
            &SandboxId::new().to_string(),
            SpacedRpc::WaitReady,
            &node_api_peer(),
        )
        .expect_err("a supplied capability must be verified even for a trusted caller");

    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    assert!(error.message().contains("invalid SPACE API route token"));
}

#[tokio::test]
async fn trusted_lifecycle_policy_does_not_ignore_an_unconfigured_scheme_token() {
    if !crate::service::tests::enter_route_test_process(&[]) {
        return;
    }
    let mut request = trusted_lifecycle_request();
    request.metadata_mut().insert(
        SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY,
        tonic::metadata::MetadataValue::from_static("disabled-hmac-token"),
    );

    let error = service()
        .prepare_classified_space_api_bridge(
            &request,
            &SandboxId::new().to_string(),
            SpacedRpc::WaitReady,
            &node_api_peer(),
        )
        .expect_err("a supplied unconfigured-scheme token must not take the exemption");

    assert_eq!(error.code(), tonic::Code::PermissionDenied);
}
