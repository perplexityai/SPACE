//! Every Native route that reads a JSON body names the field a body that does
//! not match its schema got wrong, as the routes' own field checks do. Routes
//! hidden by default (protocol and file-watch sessions) are not mounted here;
//! they read their bodies with the same extractor. A credential reference is
//! an internally tagged enum, inside which serde reports no field path, so
//! its refusal names none.

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode},
};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, TestDbResult, pg_test_db};
use pplx_space_public_api::PublicSandboxId;
use pplx_space_sql_locator::sql;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

use crate::routes::AppState;

const KEY: &str = "native-body-validation-key";

/// A Native router over a fresh database, with an API key for `KEY` and a
/// stopped sandbox for the routes under it.
async fn native_app() -> TestDbResult<(pplx_space_db_test_fixtures::TestDb, Router, PublicSandboxId)>
{
    let db = pg_test_db().await?;
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces)
         VALUES ($1, $2, 'native-body-validation', $3, ARRAY['native'])"
    ))
    .bind(Uuid::now_v7())
    .bind(SEED_ORG_ID)
    .bind(crate::auth::hash_token(KEY))
    .execute(&db.pool)
    .await?;
    let sandbox = PublicSandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, network_policy_generation)
         VALUES ($1, $2, $3, 'stopped', 1)"
    ))
    .bind(sandbox.as_uuid())
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    let state = AppState::from_pool(db.pool.clone()).await?;
    let app = Router::new()
        .nest("/v1", crate::routes::public::v1_router(state.clone()))
        .with_state(state);
    Ok((db, app, sandbox))
}

/// Sends `body` as JSON to `path` and returns the status and answer.
async fn send(
    app: &Router,
    method: Method,
    path: &str,
    body: &Value,
) -> TestDbResult<(StatusCode, Value)> {
    send_text(app, method, path, &body.to_string()).await
}

/// [`send`], with the body's exact text. serde_json writes an object's keys
/// in an order a feature can change, so a test that reads a position in the
/// body sends its text.
async fn send_text(
    app: &Router,
    method: Method,
    path: &str,
    body: &str,
) -> TestDbResult<(StatusCode, Value)> {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {KEY}"))
        .header("content-type", "application/json")
        .header(
            "idempotency-key",
            format!("body-validation-{}", Uuid::now_v7()),
        )
        .body(Body::from(body.to_owned()))?;
    let response = app.clone().oneshot(request).await?;
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024).await?;
    Ok((status, serde_json::from_slice(&bytes)?))
}

#[tokio::test]
async fn a_body_that_does_not_match_the_schema_names_the_field() -> TestDbResult<()> {
    let (_db, app, sandbox) = native_app().await?;

    let files = |action: &str| format!("/v1/sandboxes/{sandbox}/files:{action}");
    let cases = [
        (
            Method::POST,
            "/v1/sandboxes".to_string(),
            json!({"template": 5}),
            "template",
        ),
        (
            Method::PATCH,
            format!("/v1/sandboxes/{sandbox}"),
            json!({"resources": {"vcpu": "two"}}),
            "resources.vcpu",
        ),
        (
            Method::POST,
            format!("/v1/sandboxes/{sandbox}/commands"),
            json!({"command": "ls"}),
            "command",
        ),
        (
            Method::POST,
            format!("/v1/sandboxes/{sandbox}/terminal-sessions"),
            json!({"env": {"TERM": 1}}),
            "env.TERM",
        ),
        (
            Method::POST,
            format!("/v1/sandboxes/{sandbox}/process-sessions"),
            json!({"command": ["sh", 1]}),
            "command[1]",
        ),
        (
            Method::POST,
            format!("/v1/sandboxes/{sandbox}/port-sessions"),
            json!({"port": "http", "protocol": "http"}),
            "port",
        ),
        (Method::POST, files("stat"), json!({"path": 5}), "path"),
        (
            Method::POST,
            files("list"),
            json!({"path": "/", "depth": "deep"}),
            "depth",
        ),
        (
            Method::POST,
            files("read"),
            json!({"path": "/a", "length": "all"}),
            "length",
        ),
        (
            Method::POST,
            files("write"),
            json!({"path": "/a", "data_base64": 5}),
            "data_base64",
        ),
        (
            Method::POST,
            files("write_batch"),
            json!({"entries": [{"path": 5, "data_base64": ""}]}),
            "entries[0].path",
        ),
        (Method::POST, files("mkdir"), json!({"path": 5}), "path"),
        (
            Method::POST,
            files("remove"),
            json!({"path": "/a", "recursive": "yes"}),
            "recursive",
        ),
        (
            Method::POST,
            files("rename"),
            json!({"old_path": "/a", "new_path": 5}),
            "new_path",
        ),
        (
            Method::POST,
            files("modified"),
            json!({"window_seconds": "1h"}),
            "window_seconds",
        ),
        (
            Method::POST,
            files("changes_enable"),
            json!({"roots": "/"}),
            "roots",
        ),
        (
            Method::POST,
            files("changes_disable"),
            json!({"roots": "/"}),
            "roots",
        ),
    ];
    for (method, path, body, field) in cases {
        let (status, answer) = send(&app, method.clone(), &path, &body).await?;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{method} {path}: {answer}"
        );
        assert_eq!(
            answer["code"], "invalid_request",
            "{method} {path}: {answer}"
        );
        assert_eq!(
            answer["details"],
            json!([{"key": "field", "value": field}]),
            "{method} {path}: {answer}"
        );
    }
    Ok(())
}

/// A value the schema refuses is not answered back, whether serde refused it
/// (a closed wire enum) or the field's own parser did (a duration). Either
/// could be a secret sent to the wrong field. The answer names the field,
/// and for the enum the values it expected.
#[tokio::test]
async fn a_refused_value_is_not_answered_back() -> TestDbResult<()> {
    const SENT: &str = "SECRET-VALUE-1234";
    let (_db, app, sandbox) = native_app().await?;

    let cases = [
        (
            format!("/v1/sandboxes/{sandbox}/port-sessions"),
            format!(r#"{{"port":8080,"protocol":"{SENT}"}}"#),
            "invalid request body: protocol: unknown variant, expected `http` or `tcp` at line 1 column 44",
            "protocol",
        ),
        (
            "/v1/sandboxes".to_string(),
            format!(r#"{{"template":"seed","idle_timeout":"{SENT}"}}"#),
            "invalid request body: idle_timeout: invalid value at line 1 column 54",
            "idle_timeout",
        ),
    ];
    for (path, body, message, field) in cases {
        let (status, answer) = send_text(&app, Method::POST, &path, &body).await?;
        assert!(!answer.to_string().contains(SENT), "{path}: {answer}");
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{path}: {answer}");
        assert_eq!(answer["code"], "invalid_request", "{path}: {answer}");
        assert_eq!(answer["message"], message, "{path}: {answer}");
        assert_eq!(
            answer["details"],
            json!([{"key": "field", "value": field}]),
            "{path}: {answer}"
        );
    }
    Ok(())
}
