use super::*;

#[test]
fn internal_token_validation_requires_matching_nonempty_token() {
    assert!(!valid_internal_token_value("", Some("token")));
    assert!(!valid_internal_token_value("token", None));
    assert!(!valid_internal_token_value("token", Some("wrong")));
    assert!(valid_internal_token_value("token", Some("token")));
}

#[tokio::test(flavor = "current_thread")]
async fn epoch_advance_answers_a_lost_lock_race_as_retryable() -> pplx_space_test_env::TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_raw_pg_test_db().await? else {
        return Ok(());
    };

    for sqlstate in ["55P03", "40001", "40P01"] {
        let error = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, sqlstate).await;
        let response = db_error(&error, SandboxId::new());
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .expect("a retryable 503 must pace the retry"),
            "1"
        );
    }
    Ok(())
}
