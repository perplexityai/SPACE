use super::*;
use tower::ServiceExt;

#[tokio::test]
async fn listener_authenticates_build_rpcs_without_blocking_health() {
    for (path, bearer, expected) in [
        (
            "/build.manager.v1.BuildManager/StartBuild",
            Some("Bearer secret"),
            None,
        ),
        (
            "/build.manager.v1.BuildManager/UploadBlob",
            Some("Bearer wrong"),
            Some(tonic::Code::Unauthenticated),
        ),
        (
            "/build.manager.v1.BuildManager/GetBuildStatus",
            None,
            Some(tonic::Code::Unauthenticated),
        ),
        ("/grpc.health.v1.Health/Check", None, None),
    ] {
        let layer = BuildAuthLayer {
            authenticator: Arc::new(BuildAuthenticator {
                surface: BuildSurface::Manager,
                static_token: "secret".to_string(),
                static_fallback_enabled: true,
                direct_publication_token: None,
                #[cfg(feature = "k8s")]
                reviewer: None,
            }),
        };
        let inner = tower::service_fn(|request: HttpRequest<()>| async move {
            assert_eq!(
                request
                    .extensions()
                    .get::<BuildAuthenticatedCaller>()
                    .is_some(),
                request
                    .uri()
                    .path()
                    .starts_with("/build.manager.v1.BuildManager/")
            );
            Ok::<_, BoxError>(Response::new(tonic::body::Body::empty()))
        });
        let mut request = HttpRequest::builder().uri(path);
        if let Some(bearer) = bearer {
            request = request.header("authorization", bearer);
        }

        let result = layer.layer(inner).oneshot(request.body(()).unwrap()).await;

        assert_eq!(
            result
                .err()
                .map(|error| error.downcast_ref::<Status>().unwrap().code()),
            expected
        );
    }
}

#[tokio::test]
async fn publication_never_accepts_the_shared_static_credential() {
    use std::ffi::OsStr;

    if !pplx_space_test_env::enter_test_process(&[
        ("SPACE_K8S_TOKEN_AUTH_ENABLED", Some(OsStr::new("false"))),
        (
            "SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED",
            Some(OsStr::new("true")),
        ),
    ]) {
        return;
    }
    let layer = BuildAuthLayer::for_template_publication("secret".into())
        .await
        .unwrap();

    for method in [
        "RegisterTemplate",
        "RegisterTemplateRelease",
        "CreateSystemTemplatePublicationGrant",
    ] {
        let result = layer
            .authenticator
            .authenticate(
                &format!("/space.node.api.v1.ClusterApi/{method}"),
                Some("Bearer secret"),
            )
            .await;

        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }
}

#[tokio::test]
async fn direct_publication_configuration_fails_closed() {
    use std::ffi::OsStr;
    if !pplx_space_test_env::enter_test_process(&[
        ("SPACE_K8S_TOKEN_AUTH_ENABLED", Some(OsStr::new("false"))),
        ("SPACE_TEMPLATE_PUBLICATION_TOKEN_PATH", None),
    ]) {
        return;
    }
    assert!(publication_request(()).unwrap().metadata().is_empty());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("publisher.token");
    // SAFETY: enter_test_process isolates this test from concurrent env readers.
    unsafe { std::env::set_var("SPACE_TEMPLATE_PUBLICATION_TOKEN_PATH", &path) };
    assert!(matches!(
        publication_request(()),
        Err(DirectPublicationError::Read { .. })
    ));
    for contents in [
        b"".as_slice(),
        b"too-short".as_slice(),
        b"has spaces and is not a single credential".as_slice(),
        &[b'x'; 4097],
    ] {
        std::fs::write(&path, contents).unwrap();
        let result = BuildAuthLayer::for_template_publication("shared".into()).await;
        assert!(matches!(
            result,
            Err(BuildAuthError::DirectPublication(
                DirectPublicationError::Invalid { .. }
            ))
        ));
    }
    let reused = "shared-credential-that-must-not-publish";
    std::fs::write(&path, reused).unwrap();
    assert!(matches!(
        BuildAuthLayer::for_template_publication(reused.into()).await,
        Err(BuildAuthError::DirectPublication(
            DirectPublicationError::SharedCredential
        ))
    ));
    // SAFETY: enter_test_process isolates this test from concurrent env readers.
    unsafe { std::env::set_var("SPACE_K8S_TOKEN_AUTH_ENABLED", "true") };
    assert!(matches!(
        BuildAuthLayer::for_template_publication("shared".into()).await,
        Err(BuildAuthError::DirectPublication(
            DirectPublicationError::KubernetesAuthentication
        ))
    ));
}

#[tokio::test]
async fn direct_publication_requires_both_credentials_and_an_independent_build_grant() {
    use std::ffi::OsStr;
    const PUBLISHER: &str = "dedicated-publisher-credential-for-tests";
    if !pplx_space_test_env::enter_test_process(&[
        ("SPACE_K8S_TOKEN_AUTH_ENABLED", Some(OsStr::new("false"))),
        (
            "SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED",
            Some(OsStr::new("true")),
        ),
    ]) {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let token = directory.path().join("publisher.token");
    std::fs::write(&token, PUBLISHER).unwrap();
    // This is an isolated subprocess; no concurrent test observes the setting.
    // SAFETY: enter_test_process isolates this test from concurrent env readers.
    unsafe { std::env::set_var("SPACE_TEMPLATE_PUBLICATION_TOKEN_PATH", &token) };
    let publisher = BuildAuthLayer::for_template_publication("shared-node-token".into())
        .await
        .unwrap();
    // Dedicated proof never lends build authority to unrelated RPCs.
    for path in [
        "/space.node.api.v1.ClusterApi/ListNodes",
        "/build.manager.v1.BuildManager/StartBuild",
        "/grpc.health.v1.Health/Check",
    ] {
        assert!(
            publisher
                .authenticator
                .authenticate_direct_publication(
                    path,
                    Some("Bearer shared-node-token"),
                    Some(PUBLISHER),
                )
                .unwrap()
                .is_none()
        );
    }
    let node = super::super::NodeApiAuthLayer::from_config(
        super::super::NodeApiAuthConfig::from_env(
            super::super::NodeRole::Cluster,
            "shared-node-token".into(),
            None,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    for (method, bearer, credential, allowed) in [
        (
            "RegisterTemplate",
            Some("shared-node-token"),
            Some(PUBLISHER),
            true,
        ),
        (
            "RegisterTemplateRelease",
            Some("shared-node-token"),
            Some(PUBLISHER),
            true,
        ),
        ("RegisterTemplate", Some("shared-node-token"), None, false),
        (
            "RegisterTemplate",
            Some("shared-node-token"),
            Some("wrong"),
            false,
        ),
        ("RegisterTemplate", None, Some(PUBLISHER), false),
        ("RegisterTemplate", Some("wrong"), Some(PUBLISHER), false),
    ] {
        let inner = tower::service_fn(|request: HttpRequest<()>| async move {
            let caller = request
                .extensions()
                .get::<BuildAuthenticatedCaller>()
                .unwrap()
                .clone();
            let mut request = tonic::Request::new(());
            request.extensions_mut().insert(caller);
            authorize_template_build(&request, None).unwrap();
            authorize_template_build(&request, Some("example-org")).unwrap();
            assert!(system_publication_requires_grant(&request));
            assert!(tenant_publication_requires_grant(&request));
            assert_eq!(
                authorize_system_publication_grant(&request)
                    .unwrap_err()
                    .code(),
                tonic::Code::PermissionDenied
            );
            Ok::<_, BoxError>(Response::new(tonic::body::Body::empty()))
        });
        let mut request =
            HttpRequest::builder().uri(format!("/space.node.api.v1.ClusterApi/{method}"));
        if let Some(bearer) = bearer {
            request = request.header("authorization", format!("Bearer {bearer}"));
        }
        if let Some(credential) = credential {
            let value = if credential == PUBLISHER {
                let decorated = publication_request(()).unwrap();
                let value = decorated
                    .metadata()
                    .get("x-space-template-publication-token")
                    .unwrap();
                assert!(value.is_sensitive());
                value.to_str().unwrap().to_owned()
            } else {
                credential.to_owned()
            };
            request = request.header("x-space-template-publication-token", value);
        }
        let result = node
            .layer(publisher.layer(inner))
            .oneshot(request.body(()).unwrap())
            .await;
        assert_eq!(
            result.is_ok(),
            allowed,
            "{method}, bearer={bearer:?}, has_publisher={}",
            credential.is_some()
        );
    }
}
