use super::*;

#[test]
fn interceptor_reports_token_source_failure_as_unavailable() {
    use crate::SpaceBearerInterceptor;

    let mut env = pplx_space_test_env::TestEnv::default();
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("nonexistent.token");
    env.set(SPACE_CLUSTER_API_TOKEN_PATH_ENV, &missing);

    // An explicitly-configured-but-unreadable token must fail the call with
    // UNAVAILABLE (a transient, local infra failure), never INTERNAL — which
    // the command-ready probe would misread as remote guest-wedge evidence.
    let mut interceptor = SpaceBearerInterceptor::new(SpaceBearerToken::ClusterApi);
    let err = interceptor
        .call_with_environment(tonic::Request::new(()), &env)
        .expect_err("missing token file must fail the call");
    assert_eq!(err.code(), tonic::Code::Unavailable);
}

#[test]
fn required_space_bearer_token_reports_source_failure_as_unavailable() {
    use crate::required_space_bearer_token_from_environment;

    let mut env = pplx_space_test_env::TestEnv::default();
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("nonexistent.token");
    env.set(SPACE_NODE_API_TOKEN_PATH_ENV, &missing);

    // The template transfer client path (PullTemplate) reads its token
    // through this helper. A configured-but-unreadable projected token must
    // surface as retryable UNAVAILABLE so the shared gRPC retry policy
    // retries a mid-rotation read, never INTERNAL — which the retry policy
    // treats as permanent.
    let err = required_space_bearer_token_from_environment(SpaceBearerToken::NodeApi, &env)
        .expect_err("missing token file must fail");
    assert_eq!(err.code(), tonic::Code::Unavailable);
}

#[test]
fn falls_back_to_env_when_default_file_is_missing() {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set(SPACE_INTERNAL_TOKEN_ENV, " env-token ");

    assert_eq!(
        space_bearer_token_from_environment(SpaceBearerToken::NodeApi, &env).unwrap(),
        "env-token"
    );
}

#[test]
fn explicit_file_path_wins_over_env() {
    let mut env = pplx_space_test_env::TestEnv::default();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    std::fs::write(&path, "file-token\n").unwrap();

    env.set(SPACE_NODE_API_TOKEN_PATH_ENV, &path);
    env.set(SPACE_INTERNAL_TOKEN_ENV, "env-token");

    assert_eq!(
        space_bearer_token_from_environment(SpaceBearerToken::NodeApi, &env).unwrap(),
        "file-token"
    );
}

#[test]
fn explicit_missing_file_is_an_error() {
    let mut env = pplx_space_test_env::TestEnv::default();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing");
    env.set(SPACE_CLUSTER_API_TOKEN_PATH_ENV, &path);
    env.set(SPACE_INTERNAL_TOKEN_ENV, "env-token");

    let error =
        space_bearer_token_from_environment(SpaceBearerToken::ClusterApi, &env).unwrap_err();
    assert!(matches!(error, BearerTokenError::MissingFile { .. }));
}

#[test]
fn explicit_empty_file_is_an_error() {
    let mut env = pplx_space_test_env::TestEnv::default();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    std::fs::write(&path, " \n").unwrap();
    env.set(SPACE_NODE_API_TOKEN_PATH_ENV, &path);
    env.set(SPACE_INTERNAL_TOKEN_ENV, "env-token");

    let error = space_bearer_token_from_environment(SpaceBearerToken::NodeApi, &env).unwrap_err();
    assert!(matches!(error, BearerTokenError::EmptyFile { .. }));
}

#[test]
fn token_file_cache_is_keyed_by_mtime_and_short_ttl() {
    let path = PathBuf::from(format!(
        "/tmp/space-token-cache-test-{}",
        uuid::Uuid::now_v7()
    ));
    let modified = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    let now = Instant::now();

    cache_token_file(&path, modified, now, "cached-token");

    assert_eq!(
        cached_token_file(&path, modified, now + Duration::from_millis(500)),
        Some("cached-token".to_string())
    );
    assert_eq!(
        cached_token_file(
            &path,
            modified + Duration::from_secs(1),
            now + Duration::from_millis(500)
        ),
        None
    );
    assert_eq!(
        cached_token_file(&path, modified, now + Duration::from_secs(2)),
        None
    );
}

#[test]
fn token_file_cache_refreshes_after_short_ttl() {
    let mut env = pplx_space_test_env::TestEnv::default();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    std::fs::write(&path, "first\n").unwrap();
    env.set(SPACE_NODE_API_TOKEN_PATH_ENV, &path);
    env.set(SPACE_INTERNAL_TOKEN_ENV, "env-token");

    assert_eq!(
        space_bearer_token_from_environment(SpaceBearerToken::NodeApi, &env).unwrap(),
        "first"
    );
    std::thread::sleep(TOKEN_FILE_CACHE_TTL + Duration::from_millis(50));
    std::fs::write(&path, "second\n").unwrap();

    assert_eq!(
        space_bearer_token_from_environment(SpaceBearerToken::NodeApi, &env).unwrap(),
        "second"
    );
}
