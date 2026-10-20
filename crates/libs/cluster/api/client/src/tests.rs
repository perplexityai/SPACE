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
fn backup_execution_superseded_marker_round_trips_only_on_failed_precondition() {
    let generation = ExecutionGeneration::new(7).unwrap();
    let status = backup_execution_superseded_status("retired", generation);
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        backup_execution_superseded_generation(&status),
        Some(generation)
    );

    let mut other_code = tonic::Status::not_found("gone");
    *other_code.metadata_mut() = status.metadata().clone();
    assert_eq!(backup_execution_superseded_generation(&other_code), None);

    let unmarked = tonic::Status::failed_precondition("no source node");
    assert_eq!(backup_execution_superseded_generation(&unmarked), None);

    let mut malformed = tonic::Status::failed_precondition("retired");
    malformed.metadata_mut().insert(
        BACKUP_EXECUTION_SUPERSEDED_METADATA,
        MetadataValue::from_static("0"),
    );
    assert_eq!(backup_execution_superseded_generation(&malformed), None);
}
