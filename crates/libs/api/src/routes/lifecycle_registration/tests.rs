use super::*;

#[test]
fn cursor_rejects_other_versions_and_malformed_keys() {
    let id = SandboxId::new();
    assert_eq!(decode_cursor(&encode_cursor(id)), Ok(id));
    for value in [
        "",
        "!",
        &URL_SAFE_NO_PAD.encode("lifecycle-v2:bad"),
        &URL_SAFE_NO_PAD.encode("lifecycle-v1:bad"),
    ] {
        assert!(decode_cursor(value).is_err());
    }
}

#[test]
fn binding_rejects_user_text_and_invalid_users() {
    for (user_id, session_id) in [(0, "session"), (42, ""), (42, "a title"), (42, "../x")] {
        assert!(
            PolicyBinding {
                user_id,
                session_id: session_id.into(),
                organization_uuid: None,
                context_uuid: None
            }
            .validate()
            .is_err()
        );
    }
}

#[tokio::test]
async fn both_internal_routes_reject_untrusted_requests_before_database_access()
-> pplx_space_test_env::TestResult {
    use axum::{body::Body, http::Request};
    use std::{ffi::OsStr, time::Duration};
    use tower::ServiceExt;
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_TOKEN",
        Some(OsStr::new("trusted-test-token")),
    )]) {
        return Ok(());
    }
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy("postgresql://127.0.0.1:1/unused")?;
    let state = AppState::from_pool(pool).await?;
    let app = crate::routes::internal_router().with_state(state);
    for token in [None, Some("wrong-token")] {
        for (method, path, body) in [
            (
                "GET",
                "/internal/v1/lifecycle/candidates".to_owned(),
                String::new(),
            ),
            (
                "PUT",
                format!(
                    "/internal/v1/sandboxes/{}/lifecycle-registration",
                    SandboxId::new()
                ),
                serde_json::json!({"enabled": true, "expected_revision": 0,
                 "binding": {"user_id":42,"session_id":"test-session",
                             "organization_uuid":null,"context_uuid":null}})
                .to_string(),
            ),
        ] {
            let mut request = Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json");
            if let Some(token) = token {
                request = request.header("x-internal-token", token);
            }
            let response = app.clone().oneshot(request.body(Body::from(body))?).await?;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
    }
    Ok(())
}

#[test]
fn candidate_page_wire_shape_uses_native_lifecycle_and_rfc3339_activity()
-> pplx_space_test_env::TestResult {
    let page = CandidatePage {
        items: vec![Candidate {
            sandbox_id: "00000000-0000-0000-0000-000000000001".parse()?,
            binding: PolicyBinding {
                user_id: 42,
                organization_uuid: None,
                session_id: "session_test".into(),
                context_uuid: None,
            },
            registration_revision: 2,
            lifecycle: SandboxLifecycle::Started,
            last_access_at: Some(time::OffsetDateTime::parse(
                "2026-01-01T00:00:00Z",
                &time::format_description::well_known::Rfc3339,
            )?),
        }],
        next_cursor: None,
    };
    let actual = serde_json::to_value(page)?;
    let expected: serde_json::Value = serde_json::from_str(include_str!("candidate-page.json"))?;
    assert_eq!(actual, expected);
    Ok(())
}

#[tokio::test]
async fn trusted_registration_is_visible_in_advisory_inventory() -> pplx_space_test_env::TestResult
{
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use std::ffi::OsStr;
    use tower::ServiceExt;
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_TOKEN",
        Some(OsStr::new("trusted-test-token")),
    )]) {
        return Ok(());
    }
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let id = SandboxId::new();
    sqlx::query(pplx_space_common_core::sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, '00000000-0000-0000-0000-000000000010',
                 '00000000-0000-0000-0000-000000000001', 1)"
    ))
    .bind(id)
    .execute(&db.pool)
    .await?;
    let state = AppState::from_pool(db.pool.clone()).await?;
    let app = crate::routes::internal_router().with_state(state);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!(
                    "/internal/v1/sandboxes/{id}/lifecycle-registration"
                ))
                .header("content-type", "application/json")
                .header("x-internal-token", "trusted-test-token")
                .body(Body::from(
                    serde_json::json!({"enabled":true,"expected_revision":0,
            "binding":{"user_id":42,"session_id":"test-session",
                "organization_uuid":null,"context_uuid":null}})
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/internal/v1/lifecycle/candidates?limit=1")
                .header("x-internal-token", "trusted-test-token")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 8192).await?)?;
    assert_eq!(body["items"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["items"][0]["sandbox_id"], id.to_string());
    assert_eq!(body["items"][0]["registration_revision"], 1);
    assert_eq!(body["items"][0]["lifecycle"], "creating");
    assert_eq!(body["items"][0]["last_access_at"], serde_json::Value::Null);
    assert_eq!(body["next_cursor"], serde_json::Value::Null);
    Ok(())
}
