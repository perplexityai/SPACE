use super::*;
use crate::auth::ClientService;

#[tokio::test]
async fn every_service_rechecks_transport_peer_on_cached_tokens() {
    use crate::k8s_identity::tests::{caller_reviewer, registered_node_db, test_identity};
    use std::sync::atomic::Ordering;
    use tonic::transport::server::TcpConnectInfo;
    use tower::ServiceExt;
    let (db, _) = registered_node_db().await.unwrap();
    for service in ClientService::ALL {
        let (reviewer, reviews) = caller_reviewer(service, db.pool.clone());
        let layer = CredmgrTcpAuthLayer {
            authenticator: Arc::new(Authenticator::ProjectedToken(Box::new(reviewer))),
        };
        let inner = tower::service_fn(move |req: http::Request<()>| async move {
            assert_eq!(
                req.extensions().get::<TcpCallerIdentity>(),
                Some(&TcpCallerIdentity::K8sJwt(test_identity(service)))
            );
            let body = http_body_util::Empty::<bytes::Bytes>::new();
            Ok::<_, std::convert::Infallible>(http::Response::new(body))
        });
        let authenticated = layer.layer(inner);
        for (peer, failure) in [
            (Some("10.0.0.2:1234"), None),
            (Some("[::ffff:10.0.0.2]:4567"), None),
            (Some("[2001:db8::2]:1234"), None),
            (Some("10.0.0.99:1234"), Some("peer_ip_mismatch")),
            (None, Some("peer_address_missing")),
        ] {
            let mut request = http::Request::builder()
                .uri(crate::intermediate_ca::ISSUE_INTERMEDIATE_METHOD_PATH)
                .header("authorization", "Bearer header.payload.signature")
                .header("x-forwarded-for", "10.0.0.2")
                .body(())
                .unwrap();
            request.extensions_mut().insert(TcpConnectInfo {
                local_addr: None,
                remote_addr: peer.map(|peer| peer.parse().unwrap()),
            });
            let result = authenticated.clone().oneshot(request).await;
            match failure {
                None => assert!(result.is_ok(), "{service}: {peer:?}"),
                Some(reason) => {
                    let error = Status::from_error(result.unwrap_err());
                    assert_eq!(error.code(), tonic::Code::PermissionDenied, "{service}");
                    assert_eq!(error.message(), reason, "{service}");
                }
            }
        }
        assert_eq!(
            reviews.load(Ordering::SeqCst),
            1,
            "{service}: one cached TokenReview"
        );
    }
}

#[test]
fn jwt_shape_requires_three_nonempty_parts() {
    assert!(is_jwt_shaped("aaa.bbb.ccc"));
    assert!(!is_jwt_shaped("aaa.bbb"));
    assert!(!is_jwt_shaped("aaa.bbb.ccc.ddd"));
    assert!(!is_jwt_shaped("aaa..ccc"));
    assert!(!is_jwt_shaped("not-a-jwt"));
}

#[test]
fn constant_time_eq_matches_only_equal_strings() {
    assert!(constant_time_eq("secret-token", "secret-token"));
    assert!(!constant_time_eq("secret-token", "secret-toker"));
    assert!(!constant_time_eq("secret", "secret-token"));
}

#[test]
fn health_and_reflection_are_exempt() {
    assert!(is_auth_exempt("/grpc.health.v1.Health/Check"));
    assert!(is_auth_exempt(
        "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"
    ));
    assert!(!is_auth_exempt(
        "/space.credential.manager.v1.CredentialManager/Encrypt"
    ));
}

#[tokio::test]
async fn static_token_resolves_to_internal_principal_in_static_only_mode() {
    let authn = Authenticator::StaticToken("the-secret".to_owned());
    assert_eq!(
        authn
            .authenticate(Some("Bearer the-secret"), &http::Extensions::new())
            .await
            .unwrap(),
        TcpCallerIdentity::Plaintext
    );
    let err = authn
        .authenticate(Some("Bearer not-the-secret"), &http::Extensions::new())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn missing_and_malformed_headers_are_unauthenticated() {
    let authn = Authenticator::StaticToken("the-secret".to_owned());
    assert_eq!(
        authn
            .authenticate(None, &http::Extensions::new())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        authn
            .authenticate(Some("Basic abc"), &http::Extensions::new())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
}
