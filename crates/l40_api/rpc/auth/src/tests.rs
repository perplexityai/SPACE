use super::*;

fn make_request(auth_value: Option<&str>) -> Request<()> {
    let mut req = Request::new(());
    if let Some(v) = auth_value {
        req.metadata_mut()
            .insert("authorization", v.parse().unwrap());
    }
    req
}

#[test]
fn bearer_token_extracts_valid_value() {
    assert_eq!(bearer_token(Some("Bearer secret")).unwrap(), "secret");
}

#[test]
fn bearer_token_accepts_case_insensitive_scheme() {
    assert_eq!(bearer_token(Some("bearer secret")).unwrap(), "secret");
}

#[test]
fn bearer_token_rejects_empty_value() {
    let err = bearer_token(Some("Bearer   ")).unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    assert_eq!(err.message(), "expected Bearer scheme");
}

#[test]
fn bearer_token_rejects_wrong_scheme() {
    let err = bearer_token(Some("Basic dXNlcjpwYXNz")).unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    assert_eq!(err.message(), "expected Bearer scheme");
}

#[test]
fn bearer_token_rejects_missing_header() {
    let err = bearer_token(None).unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    assert_eq!(err.message(), "missing authorization header");
}

#[test]
fn auth_surface_allows_health_and_reflection() {
    assert_eq!(auth_surface("/grpc.health.v1.Health/Check"), None);
    assert_eq!(
        auth_surface("/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"),
        None
    );
    assert_eq!(
        auth_surface("/grpc.reflection.bad/ServerReflectionInfo"),
        Some(AuthSurface::Normal)
    );
}

#[test]
fn auth_surface_classifies_worker_internal() {
    assert_eq!(
        auth_surface("/space.node.api.v1.WorkerApiInternal/WakeSandbox"),
        Some(AuthSurface::WorkerInternal)
    );
}

#[test]
fn auth_surface_defaults_to_normal() {
    assert_eq!(
        auth_surface("/space.node.api.v1.WorkerApi/CreateSandbox"),
        Some(AuthSurface::Normal)
    );
}

#[test]
fn node_api_auth_boundary_strips_caller_supplied_bridge_policy() {
    let mut request = HttpRequest::builder()
        .header(
            SPACE_API_BRIDGE_CALLER_POLICY_METADATA_KEY,
            "trusted_lifecycle",
        )
        .body(())
        .unwrap();

    strip_untrusted_bridge_policy(&mut request);

    assert!(
        request
            .headers()
            .get(SPACE_API_BRIDGE_CALLER_POLICY_METADATA_KEY)
            .is_none()
    );
}

#[tokio::test]
async fn static_authentication_returns_caller_identity() {
    let authenticator = NodeApiAuthenticator {
        #[cfg(feature = "k8s")]
        role: NodeRole::Worker,
        static_token: "secret".to_string(),
        static_control_token: None,
        static_fallback_enabled: true,
        #[cfg(feature = "k8s")]
        k8s: None,
    };

    let caller = authenticator
        .authenticate(
            "/space.node.api.v1.WorkerApi/CreateSandbox",
            Some("Bearer secret"),
        )
        .await
        .unwrap();

    let caller = caller.expect("static authentication returns a caller policy");
    assert_eq!(
        caller.wait_ready_lifecycle_authority(),
        WaitReadyLifecycleAuthority::Granted
    );
    // Static-only deployment (no k8s auth): the shared token is the trust
    // boundary, so it retains policy-mutation authority.
    assert_eq!(
        caller.network_policy_authority(),
        NetworkPolicyMutationAuthority::Granted
    );
    assert_eq!(
        caller.data_plane_session_refresh_authority(),
        DataPlaneSessionRefreshAuthority::Granted
    );
    assert_eq!(
        caller.template_release_read_authority(),
        TemplateReleaseReadAuthority::Unscoped
    );
}

/// Once TokenReview is configured the static token is a lower-trust fallback
/// that a worker also holds, so it must not read the template catalog
/// unscoped; a TokenReview identity never does.
#[cfg(feature = "k8s")]
#[test]
fn template_catalog_reads_are_node_scoped_unless_static_only() {
    assert_eq!(
        NodeApiAuthenticatedCaller::static_token(KubernetesAuthenticationMode::Configured)
            .template_release_read_authority(),
        TemplateReleaseReadAuthority::NodeScoped
    );
    assert_eq!(
        NodeApiAuthenticatedCaller::kubernetes_service_account(
            WaitReadyLifecycleAuthority::Granted,
            NetworkPolicyMutationAuthority::Granted,
            DataPlaneSessionRefreshAuthority::Granted,
        )
        .template_release_read_authority(),
        TemplateReleaseReadAuthority::NodeScoped
    );
}

#[tokio::test]
async fn worker_internal_surface_requires_the_internal_static_token() {
    let authenticator = NodeApiAuthenticator {
        #[cfg(feature = "k8s")]
        role: NodeRole::Worker,
        static_token: "normal-token".to_string(),
        static_control_token: Some("control-token".to_string()),
        static_fallback_enabled: true,
        #[cfg(feature = "k8s")]
        k8s: None,
    };
    let path = "/space.node.api.v1.WorkerApiInternal/WakeSandbox";

    let denied = authenticator
        .authenticate(path, Some("Bearer normal-token"))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::Unauthenticated);

    assert!(
        authenticator
            .authenticate(path, Some("Bearer control-token"))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn static_fallback_disabled_denies_static_token_without_k8s() {
    let authenticator = NodeApiAuthenticator {
        #[cfg(feature = "k8s")]
        role: NodeRole::Worker,
        static_token: "secret".to_string(),
        static_control_token: None,
        static_fallback_enabled: false,
        #[cfg(feature = "k8s")]
        k8s: None,
    };

    let err = authenticator
        .authenticate(
            "/space.node.api.v1.WorkerApi/CreateSandbox",
            Some("Bearer secret"),
        )
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    assert_eq!(err.message(), "invalid token");
}

#[test]
fn required_token_trims_valid_value() {
    assert_eq!(
        validate_required_bearer_token("TOKEN", " secret \n".to_string()).unwrap(),
        "secret"
    );
}

#[test]
fn required_token_rejects_empty_value() {
    let err = validate_required_bearer_token("TOKEN", " \n".to_string()).unwrap_err();
    assert_eq!(err.code(), tonic::Code::Internal);
    assert_eq!(err.message(), "TOKEN is empty");
}

#[test]
fn client_interceptor_inserts_bearer() {
    let mut interceptor = bearer_client_interceptor("secret".into());
    let req = interceptor(make_request(None)).unwrap();
    assert_eq!(
        req.metadata()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap(),
        "Bearer secret"
    );
}
