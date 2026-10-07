use super::*;

#[tokio::test]
async fn cached_token_is_reauthorized_for_each_transport_peer() {
    use std::sync::atomic::Ordering;
    use tonic::transport::server::TcpConnectInfo;
    use tower::ServiceExt;
    let (reviewer, reviews) = crate::k8s_identity::tests::agent_reviewer();
    let layer = CredmgrTcpAuthLayer {
        authenticator: Arc::new(Authenticator::ProjectedToken {
            reviewer,
            agent: Box::new(super::agent::tests::authorizer()),
        }),
    };
    let inner = tower::service_fn(|req: http::Request<()>| async move {
        let caller = req.extensions().get::<TcpCallerIdentity>().unwrap();
        assert!(matches!(caller, TcpCallerIdentity::K8sJwt(identity)
            if identity.service() == ClientService::NodeCredentialAgent));
        Ok::<_, std::convert::Infallible>(http::Response::new(
            http_body_util::Empty::<bytes::Bytes>::new(),
        ))
    });
    let service = layer.layer(inner);
    // The same token, two source ports on its real pod, then a stolen-token
    // caller at another IP. A forwarding header cannot impersonate the peer.
    for (peer, accepted) in [
        ("10.0.0.2:1234", true),
        ("[::ffff:10.0.0.2]:4567", true),
        ("10.0.0.99:1234", false),
    ] {
        let mut request = http::Request::builder()
            .uri(crate::intermediate_ca::ISSUE_INTERMEDIATE_METHOD_PATH)
            .header("authorization", "Bearer header.payload.signature")
            .header("x-forwarded-for", "10.0.0.2")
            .body(())
            .unwrap();
        request.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(peer.parse().unwrap()),
        });
        let result = service.clone().oneshot(request).await;
        if accepted {
            assert!(result.is_ok());
        } else {
            let status = Status::from_error(result.unwrap_err());
            assert_eq!(status.code(), tonic::Code::PermissionDenied);
            assert_eq!(status.message(), "peer_ip_mismatch");
        }
    }
    assert_eq!(
        reviews.load(Ordering::SeqCst),
        1,
        "all requests share one cached TokenReview"
    );
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
            .authenticate(
                Some("Bearer the-secret"),
                &http::Extensions::new(),
                "unknown"
            )
            .await
            .unwrap(),
        TcpCallerIdentity::Plaintext
    );
    let err = authn
        .authenticate(
            Some("Bearer not-the-secret"),
            &http::Extensions::new(),
            "unknown",
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn missing_and_malformed_headers_are_unauthenticated() {
    let authn = Authenticator::StaticToken("the-secret".to_owned());
    assert_eq!(
        authn
            .authenticate(None, &http::Extensions::new(), "unknown")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        authn
            .authenticate(Some("Basic abc"), &http::Extensions::new(), "unknown")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
}
