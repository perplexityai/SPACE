use super::*;

#[test]
fn template_build_rejects_only_unimplemented_spec_values() {
    validate_template_build_request(&TemplateBuildRequestV3 {
        min_free_disk_mb: Some(0),
        ..Default::default()
    })
    .expect("zero does not require additional free disk");

    let disk_error = validate_template_build_request(&TemplateBuildRequestV3 {
        min_free_disk_mb: Some(1),
        ..Default::default()
    })
    .unwrap_err();
    assert_eq!(
        disk_error.message,
        "unsupported template build parameter: minFreeDiskMb greater than zero"
    );

    let team_error = validate_template_build_request(&TemplateBuildRequestV3 {
        team_id: Some("sdk-team".to_string()),
        ..Default::default()
    })
    .unwrap_err();
    assert_eq!(
        team_error.message,
        "unsupported template build parameter: teamID"
    );
}

#[test]
fn template_build_status_error_preserves_controller_status() {
    let err = template_build_status_error(TemplateBuildStatusPollError::Failed(
        ControllerError::new(StatusCode::GATEWAY_TIMEOUT, "deadline exceeded"),
    ));

    assert_eq!(err.status, StatusCode::GATEWAY_TIMEOUT);
    assert!(err.message.contains("BuilderApi/GetBuildTemplateStatus"));
    assert!(err.message.contains("deadline exceeded"));
}

#[test]
fn is_e2b_identifier_matches_lowercase_alnum_dash_underscore() {
    assert!(is_e2b_identifier("seed"));
    assert!(is_e2b_identifier("code-runtime"));
    assert!(is_e2b_identifier("my_template-1"));
    assert!(!is_e2b_identifier(""));
    assert!(!is_e2b_identifier("MyTemplate")); // caller must lowercase first
    assert!(!is_e2b_identifier("my template!"));
    assert!(!is_e2b_identifier("with.dot"));
    assert!(!is_e2b_identifier("trailing-:"));
}

#[test]
fn is_e2b_tag_matches_lowercase_alnum_dash_underscore_dot() {
    assert!(is_e2b_tag("v1"));
    assert!(is_e2b_tag("release-2026.05"));
    assert!(is_e2b_tag("rc1.2-beta"));
    // e2b's tag regex actually allows leading `-` and `.` — verify
    // we accept them where SPACE's TemplateTag::parse would reject.
    assert!(is_e2b_tag("-leading-dash-ok"));
    assert!(is_e2b_tag(".leading-dot-ok"));
    assert!(!is_e2b_tag(""));
    assert!(!is_e2b_tag("UPPER"));
    assert!(!is_e2b_tag("with space"));
}

#[test]
fn normalize_body_tag_trims_lowercases_and_accepts_default() {
    assert_eq!(normalize_body_tag("  Latest  ").unwrap(), "latest");
    // `tags=["default"]` IS valid in body per vendored e2b — only
    // the embedded `:default` suffix is dropped.
    assert_eq!(normalize_body_tag("default").unwrap(), "default");
}

#[test]
fn normalize_body_tag_accepts_git_commit_sha() {
    assert_eq!(
        normalize_body_tag("62f56c72e6af1173d3e6445c778be4a392d9e442").unwrap(),
        "62f56c72e6af1173d3e6445c778be4a392d9e442"
    );
}

#[test]
fn normalize_body_tag_rejects_empty_and_malformed() {
    normalize_body_tag("").unwrap_err();
    normalize_body_tag("   ").unwrap_err();
    normalize_body_tag("with space").unwrap_err();
    let uuid = "550e8400-e29b-41d4-a716-446655440000";
    let err = normalize_body_tag(uuid).unwrap_err();
    assert!(format!("{err:?}").to_lowercase().contains("uuid"));
}

#[test]
fn normalize_embedded_tag_drops_default_and_empty() {
    assert!(normalize_embedded_tag("").unwrap().is_none());
    assert!(normalize_embedded_tag("default").unwrap().is_none());
    assert!(normalize_embedded_tag("DEFAULT").unwrap().is_none());
    assert_eq!(
        normalize_embedded_tag("  v1  ").unwrap().as_deref(),
        Some("v1"),
    );
}

#[test]
fn e2b_builds_default_to_latest_only_without_explicit_tags() {
    assert_eq!(publish_tags(vec![]), vec!["latest"]);
    assert_eq!(publish_tags(vec!["v1".to_string()]), vec!["v1"]);
    assert_eq!(
        publish_tags(vec!["latest".to_string(), "v1".to_string()]),
        vec!["latest", "v1"]
    );
}

#[test]
fn map_state_translates_buildmgr_states() {
    let (s, r) = map_state(BuildState::Running as i32, String::new());
    assert_eq!(s, "building");
    assert!(r.is_none());

    let (s, r) = map_state(BuildState::Succeeded as i32, String::new());
    assert_eq!(s, "ready");
    assert!(r.is_none());

    let (s, r) = map_state(BuildState::Failed as i32, "boom".into());
    assert_eq!(s, "error");
    assert_eq!(r.unwrap().message, "boom");

    let (s, r) = map_state(BuildState::Failed as i32, String::new());
    assert_eq!(s, "error");
    assert_eq!(r.unwrap().message, "build failed");
}

#[test]
fn file_upload_path_keeps_ecw_adapter_root_for_shared_requests() {
    assert_eq!(
        file_upload_path(
            "code-runtime",
            "abc123",
            "/e2b/api/templates/code-runtime/files/abc123"
        ),
        "/e2b/api/v1/internal/e2b-files/code-runtime/abc123"
    );
}

#[test]
fn file_upload_path_uses_root_for_dedicated_ecw_requests() {
    assert_eq!(
        file_upload_path(
            "code-runtime",
            "abc123",
            "/templates/code-runtime/files/abc123"
        ),
        "/v1/internal/e2b-files/code-runtime/abc123"
    );
}

#[test]
fn request_origin_uses_forwarded_scheme_and_host() {
    let mut headers = HeaderMap::new();
    headers.insert(header::HOST, "api.e2b.example.test".parse().unwrap());
    headers.insert("x-forwarded-proto", "https".parse().unwrap());

    assert_eq!(
        request_origin(&headers).unwrap(),
        "https://api.e2b.example.test",
    );
}

#[test]
fn request_origin_defaults_direct_connections_to_http() {
    let mut headers = HeaderMap::new();
    headers.insert(header::HOST, "127.0.0.1:8080".parse().unwrap());

    assert_eq!(request_origin(&headers).unwrap(), "http://127.0.0.1:8080");
}

/// The template-build store boundary, at both of its shapes.
///
/// Two properties, and the second is the one no status assertion would catch:
/// a contention abort is retryable, *and* no store error's text reaches the
/// client. `E2bApiError` puts `message` in the response body at every status,
/// and these call sites used to interpolate the `sqlx::Error` into it — so the
/// SQLSTATE, the constraint name, and the failing statement's detail line went
/// to whoever called `POST /v3/templates`.
#[tokio::test(flavor = "current_thread")]
async fn template_build_store_errors_are_classified_and_never_echoed()
-> pplx_space_test_env::TestResult {
    use crate::template_build_jobs_store::TemplateBuildJobsStoreError;

    let Some(db) = pplx_space_db_test_fixtures::try_raw_pg_test_db().await? else {
        return Ok(());
    };

    for sqlstate in ["55P03", "40001", "40P01"] {
        let raw = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, sqlstate).await;
        // The bare-sqlx wrapper borrows, so it goes first and the same raised
        // error then moves into the store variant — one round trip, not two.
        let via_sql = super::super::template_build_sql_error("record template build blob", &raw);
        assert_eq!(
            via_sql.status,
            StatusCode::SERVICE_UNAVAILABLE,
            "{sqlstate} through the bare-sqlx wrapper"
        );

        let via_store = super::super::template_build_store_error(
            "record template build spec",
            TemplateBuildJobsStoreError::Storage(raw),
        );
        assert_eq!(
            via_store.status,
            StatusCode::SERVICE_UNAVAILABLE,
            "{sqlstate} through the store wrapper"
        );
    }

    // A real failure keeps the 500, and neither disposition puts the store's
    // message in the response.
    let raw = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "23505").await;
    let rendered = raw.to_string();
    let failure = super::super::template_build_store_error(
        "record template build spec",
        TemplateBuildJobsStoreError::Storage(raw),
    );
    assert_eq!(failure.status_for_test(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = failure.to_string();
    assert!(
        !body.contains("23505") && !body.contains(&rendered),
        "the store error leaked into the response: {body}"
    );
    Ok(())
}
