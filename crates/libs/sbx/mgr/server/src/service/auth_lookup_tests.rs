use super::{SandboxManagerService, SandboxManagerServiceOptions, SandboxState, SpacedRpc};
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::Ed25519KeyPair;
use pplx_space_common_core::types::{
    ClusterId, ExecutionGeneration, NodeId, NodeManagedBy, PodUid, RouteGeneration,
    SandboxActivationAttemptId, SandboxId, SandboxRouteIdentity, SpaceApiRouteIdentity,
};
use pplx_space_common_svc::space_api_route::{
    RouteIdentityRemediation, SpaceApiRouteIdentityDimension, route_identity_dimension_from_status,
    route_identity_remediation_from_status,
};
use pplx_space_common_svc::space_api_route_ed25519::{
    SpaceApiRouteEd25519Claims, SpaceApiRouteEd25519Issuer, SpaceApiRouteEd25519KeyId,
    SpaceApiRouteEd25519Signer, SpaceApiRouteEd25519VerificationBundle,
    SpaceApiRouteEd25519VerificationGeneration, insert_space_api_route_ed25519_token,
};

use std::sync::Arc;
use tonic::Code;

fn fixture() -> (
    SandboxManagerService,
    SandboxId,
    SandboxRouteIdentity,
    tonic::Request<()>,
) {
    fixture_with_claims(|_| {})
}

fn fixture_with_claims(
    mutate: impl FnOnce(&mut SpaceApiRouteEd25519Claims),
) -> (
    SandboxManagerService,
    SandboxId,
    SandboxRouteIdentity,
    tonic::Request<()>,
) {
    let sandbox_id = SandboxId::new();
    let identity = SandboxRouteIdentity::try_new(
        sandbox_id,
        RouteGeneration::new(42).unwrap(),
        NodeId::new(),
        NodeManagedBy::Kubernetes,
        ExecutionGeneration::new(7).unwrap(),
        SandboxActivationAttemptId::new(),
        Some(PodUid::parse("pod-uid-1").unwrap()),
    )
    .unwrap();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    let signer = SpaceApiRouteEd25519Signer::from_pkcs8(
        SpaceApiRouteEd25519KeyId::parse("unit-test-key").unwrap(),
        pkcs8.as_ref(),
    )
    .unwrap();
    let issuer = SpaceApiRouteEd25519Issuer::parse("unit-test-authority").unwrap();
    let cluster_id = ClusterId::parse("unit-test").unwrap();
    let mut claims = SpaceApiRouteEd25519Claims::space_api_bridge(
        issuer.clone(),
        cluster_id.clone(),
        identity.clone(),
    )
    .unwrap();
    mutate(&mut claims);
    let token = signer.mint(&claims).unwrap();
    let bundle = SpaceApiRouteEd25519VerificationBundle::new(
        SpaceApiRouteEd25519VerificationGeneration::new(1).unwrap(),
        issuer,
        cluster_id,
        vec![signer.public_key()],
    )
    .unwrap();
    let service = SandboxManagerService::new_with_options(
        crate::db::Connection::open_in_memory(),
        SandboxManagerServiceOptions {
            route_ed25519_verification_bundle: Some(Arc::new(bundle)),
            ..SandboxManagerServiceOptions::for_test()
        },
    );
    let mut request = tonic::Request::new(());
    insert_space_api_route_ed25519_token(request.metadata_mut(), &token).unwrap();
    (service, sandbox_id, identity, request)
}

#[tokio::test]
async fn ed25519_route_token_authorizes_matching_complete_identity() {
    if !crate::service::tests::enter_route_test_process(&[]) {
        return;
    }

    let (service, sandbox_id, identity, request) = fixture();

    let authorization = service
        .prepare_space_api_bridge(&request, &sandbox_id.to_string(), SpacedRpc::PingGuest)
        .expect("signed token verifies");
    authorization
        .authorize_local_identity(&identity.into(), SandboxState::Started)
        .unwrap();
}

#[tokio::test]
async fn ed25519_route_token_rejects_stale_activation_with_refresh_verdict() {
    if !crate::service::tests::enter_route_test_process(&[]) {
        return;
    }

    let (service, sandbox_id, identity, request) = fixture();
    let authorization = service
        .prepare_space_api_bridge(&request, &sandbox_id.to_string(), SpacedRpc::PingGuest)
        .unwrap();
    let mut local: SpaceApiRouteIdentity = identity.into();
    local.activation_attempt_id = Some(SandboxActivationAttemptId::new());

    let error = authorization
        .authorize_local_identity(&local, SandboxState::Started)
        .expect_err("stale activation rejects");

    assert_eq!(error.code(), Code::PermissionDenied);
    assert_eq!(
        route_identity_remediation_from_status(&error),
        Some(RouteIdentityRemediation::RefreshRoute)
    );
    assert_eq!(
        route_identity_dimension_from_status(&error),
        Some(SpaceApiRouteIdentityDimension::ActivationAttemptId)
    );
}

#[tokio::test]
async fn ed25519_route_token_rejects_incomplete_local_identity() {
    if !crate::service::tests::enter_route_test_process(&[]) {
        return;
    }

    let (service, sandbox_id, _identity, request) = fixture();
    let authorization = service
        .prepare_space_api_bridge(&request, &sandbox_id.to_string(), SpacedRpc::PingGuest)
        .unwrap();

    for (state, expected) in [
        (SandboxState::Started, Code::PermissionDenied),
        (SandboxState::Paused, Code::PermissionDenied),
        (SandboxState::Restoring, Code::PermissionDenied),
        (SandboxState::Suspended, Code::FailedPrecondition),
        (SandboxState::Stopped, Code::FailedPrecondition),
    ] {
        for mismatch in [false, true] {
            let error = authorization
                .authorize_local_identity(
                    &SpaceApiRouteIdentity {
                        sandbox_id: Some(if mismatch {
                            SandboxId::new()
                        } else {
                            sandbox_id
                        }),
                        ..Default::default()
                    },
                    state,
                )
                .expect_err("incomplete local identity never authorizes guest access");
            assert_eq!(
                error.code(),
                if mismatch {
                    Code::PermissionDenied
                } else {
                    expected
                },
                "state={state}, mismatch={mismatch}"
            );
        }
    }
}

#[tokio::test]
async fn missing_route_token_is_denied() {
    let (service, sandbox_id, _identity, _request) = fixture();

    let error = service
        .prepare_space_api_bridge(
            &tonic::Request::new(()),
            &sandbox_id.to_string(),
            SpacedRpc::PingGuest,
        )
        .expect_err("missing capability rejects");

    assert_eq!(error.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn expired_route_token_requests_refresh_only_after_verification() {
    for wrong_issuer in [false, true] {
        let (service, sandbox_id, _, request) = fixture_with_claims(|claims| {
            claims.issued_at_unix_secs -= 300;
            claims.not_before_unix_secs -= 300;
            claims.expires_at_unix_secs -= 300;
            if wrong_issuer {
                claims.issuer = SpaceApiRouteEd25519Issuer::parse("untrusted-authority").unwrap();
            }
        });
        let error = service
            .prepare_space_api_bridge(&request, &sandbox_id.to_string(), SpacedRpc::ConnectProcess)
            .expect_err("expired credentials must never authorize a backend operation");
        assert_eq!(error.code(), Code::PermissionDenied);
        assert_eq!(
            route_identity_remediation_from_status(&error),
            if wrong_issuer {
                None
            } else {
                Some(RouteIdentityRemediation::RefreshRoute)
            }
        );
    }
}
