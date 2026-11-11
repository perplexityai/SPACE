use super::*;

const NODE_API_TOKEN_ENV_VARS: &[&str] = &["SPACE_NODE_API_TOKEN_PATH", "SPACE_INTERNAL_TOKEN"];

#[test]
fn node_request_attaches_static_fallback_bearer() {
    {
        let mut vars: Vec<(&str, Option<&std::ffi::OsStr>)> = (NODE_API_TOKEN_ENV_VARS)
            .iter()
            .map(|key| (*key, None))
            .collect();
        vars.extend([(
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("static-tok")),
        )]);
        if !pplx_space_test_env::enter_test_process(&vars) {
            return;
        }

        let request = node_request_with_metadata(
            (),
            tonic::metadata::MetadataMap::new(),
            SpaceBearerToken::NodeApi,
        )
        .unwrap();
        let header = request
            .metadata()
            .get("authorization")
            .map(|value| value.to_str().unwrap().to_string());
        assert_eq!(header.as_deref(), Some("Bearer static-tok"));
    };
}

#[test]
fn node_request_preserves_caller_metadata() {
    {
        let mut vars: Vec<(&str, Option<&std::ffi::OsStr>)> = (NODE_API_TOKEN_ENV_VARS)
            .iter()
            .map(|key| (*key, None))
            .collect();
        vars.extend([(
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("static-tok")),
        )]);
        if !pplx_space_test_env::enter_test_process(&vars) {
            return;
        }

        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            "x-space-api-route-token-ed25519",
            tonic::metadata::MetadataValue::from_static("route-token"),
        );

        let request = node_request_with_metadata((), metadata, SpaceBearerToken::NodeApi).unwrap();

        assert_eq!(
            request
                .metadata()
                .get("x-space-api-route-token-ed25519")
                .and_then(|value| value.to_str().ok()),
            Some("route-token")
        );
        assert_eq!(
            request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer static-tok")
        );
    };
}

#[test]
fn node_request_fails_loud_without_token() {
    {
        let vars: Vec<(&str, Option<&std::ffi::OsStr>)> = (NODE_API_TOKEN_ENV_VARS)
            .iter()
            .map(|key| (*key, None))
            .collect();

        if !pplx_space_test_env::enter_test_process(&vars) {
            return;
        }

        let status = node_request_with_metadata(
            (),
            tonic::metadata::MetadataMap::new(),
            SpaceBearerToken::NodeApi,
        )
        .unwrap_err();
        // No projected token and no static fallback: deny with a retryable
        // UNAVAILABLE (mirrors SpaceBearerInterceptor) rather than sending a
        // header-less request the node would reject as UNAUTHENTICATED.
        assert_eq!(status.code(), tonic::Code::Unavailable);
    };
}
