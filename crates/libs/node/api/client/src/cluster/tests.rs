use super::*;

#[test]
fn endpoint_discovery_accepts_only_off_and_kubernetes() {
    for value in ["off", " off ", "OFF"] {
        assert_eq!(value.parse(), Ok(EndpointDiscovery::Off));
    }
    for value in ["kubernetes", " Kubernetes "] {
        assert_eq!(value.parse(), Ok(EndpointDiscovery::Kubernetes));
    }
    assert_eq!(EndpointDiscovery::Off.as_str(), "off");
    assert_eq!(EndpointDiscovery::Kubernetes.to_string(), "kubernetes");
    assert_eq!(
        "dns".parse::<EndpointDiscovery>(),
        Err(ParseEndpointDiscoveryError("dns".to_owned()))
    );
}

#[cfg(feature = "kubernetes")]
#[test]
fn discovery_service_comes_from_the_cluster_api_endpoint() {
    let uri: Uri = "http://space-internal-api.space.svc:9800".parse().unwrap();
    assert_eq!(kubernetes_service_name(&uri), Some("space-internal-api"));
}

#[test]
fn unconfigured_token_errors_classify_as_missing() {
    for error in [
        BearerTokenError::Missing {
            path_env: "SPACE_CLUSTER_API_TOKEN_PATH",
            default_path: "/var/run/space-auth/cluster-api.token".into(),
            fallback_env: "SPACE_INTERNAL_TOKEN",
        },
        BearerTokenError::EmptyEnv {
            env_var: "SPACE_INTERNAL_TOKEN",
        },
    ] {
        assert!(matches!(
            classify_token_error(error),
            ConnectClusterApiError::MissingToken(_)
        ));
    }
}

#[test]
fn broken_token_source_errors_classify_as_token_source() {
    for error in [
        BearerTokenError::MissingFile {
            path: "/var/run/space-auth/cluster-api.token".into(),
        },
        BearerTokenError::EmptyFile {
            path: "/var/run/space-auth/cluster-api.token".into(),
        },
        BearerTokenError::ReadFile {
            path: "/var/run/space-auth/cluster-api.token".into(),
            source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        },
    ] {
        assert!(matches!(
            classify_token_error(error),
            ConnectClusterApiError::TokenSource(_)
        ));
    }
}

#[test]
fn endpoint_resolution_accepts_the_legacy_env_name() {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set(
        CLUSTER_API_ENDPOINT_ENV_LEGACY,
        "http://legacy.example:9800",
    );
    assert_eq!(
        cluster_api_endpoint_from_environment(&env).as_deref(),
        Some("http://legacy.example:9800"),
    );
}

#[test]
fn endpoint_resolution_prefers_the_primary_env_name() {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set(CLUSTER_API_ENDPOINT_ENV, "http://primary.example:9800");
    env.set(
        CLUSTER_API_ENDPOINT_ENV_LEGACY,
        "http://legacy.example:9800",
    );
    assert_eq!(
        cluster_api_endpoint_from_environment(&env).as_deref(),
        Some("http://primary.example:9800"),
    );
}

#[test]
fn endpoint_resolution_treats_empty_values_as_unset() {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set(CLUSTER_API_ENDPOINT_ENV, "");
    assert_eq!(cluster_api_endpoint_from_environment(&env), None);
    env.set(
        CLUSTER_API_ENDPOINT_ENV_LEGACY,
        "http://legacy.example:9800",
    );
    assert_eq!(
        cluster_api_endpoint_from_environment(&env).as_deref(),
        Some("http://legacy.example:9800"),
    );
}
