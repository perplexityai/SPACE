use super::*;

#[test]
fn falls_back_to_env_when_default_file_is_missing() {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set(SPACE_INTERNAL_TOKEN_ENV, " env-token ");
    assert_eq!(bearer_token_from_environment(&env).unwrap(), "env-token");
}

#[test]
fn explicit_file_path_wins_over_env() {
    let mut env = pplx_space_test_env::TestEnv::default();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    std::fs::write(&path, "file-token\n").unwrap();
    env.set(SPACE_CREDMGR_TOKEN_PATH_ENV, &path);
    env.set(SPACE_INTERNAL_TOKEN_ENV, "env-token");
    assert_eq!(bearer_token_from_environment(&env).unwrap(), "file-token");
}

#[test]
fn explicit_missing_file_is_an_error_not_a_fallback() {
    let mut env = pplx_space_test_env::TestEnv::default();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing");
    env.set(SPACE_CREDMGR_TOKEN_PATH_ENV, &path);
    env.set(SPACE_INTERNAL_TOKEN_ENV, "env-token");
    let err = bearer_token_from_environment(&env).unwrap_err();
    assert!(matches!(err, BearerTokenError::MissingFile { .. }));
}

#[test]
fn token_file_cache_is_keyed_by_mtime_and_short_ttl() {
    let path = PathBuf::from(format!("/tmp/credmgr-token-cache-{}", uuid::Uuid::now_v7()));
    let modified = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    let now = Instant::now();
    cache_token_file(&path, modified, now, "cached-token");

    assert_eq!(
        cached_token_file(&path, modified, now + Duration::from_millis(500)),
        Some("cached-token".to_string())
    );
    // A new mtime invalidates the cache (token rotated on disk).
    assert_eq!(
        cached_token_file(&path, modified + Duration::from_secs(1), now),
        None
    );
    // So does crossing the short TTL.
    assert_eq!(
        cached_token_file(&path, modified, now + Duration::from_secs(2)),
        None
    );
}
