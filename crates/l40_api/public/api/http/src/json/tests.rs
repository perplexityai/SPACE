use std::collections::BTreeMap;

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
    routing::post,
};
use http_body_util::BodyExt;
use pplx_space_public_api::{PublicErrorCode, PublicErrorDetail, PublicErrorEnvelope};
use serde::Deserialize;
use tower::ServiceExt;

use super::PublicJson;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[expect(dead_code, reason = "only deserialized")]
struct Resources {
    vcpu: Option<u32>,
}

/// A value whose own `Deserialize` quotes what it refused, as a duration or
/// id parser does.
#[derive(Debug)]
struct Timeout;

impl<'de> Deserialize<'de> for Timeout {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Err(serde::de::Error::custom(format!(
            "invalid duration '{value}'"
        )))
    }
}

#[derive(Debug, Deserialize)]
#[expect(dead_code, reason = "only deserialized")]
struct CreateRequest {
    template: String,
    #[serde(default)]
    command: Vec<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    resources: Option<Resources>,
    visibility: Option<Visibility>,
    separator: Option<char>,
    timeout: Option<Timeout>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Visibility {
    Public,
    Private,
}

async fn create(PublicJson(_): PublicJson<CreateRequest>) -> StatusCode {
    StatusCode::CREATED
}

async fn track(body: Option<PublicJson<CreateRequest>>) -> StatusCode {
    match body {
        Some(_) => StatusCode::CREATED,
        None => StatusCode::NO_CONTENT,
    }
}

const BODY_LIMIT: usize = 2048;

async fn send(
    path: &str,
    content_type: Option<&str>,
    body: &str,
) -> (StatusCode, PublicErrorEnvelope) {
    let app = Router::new()
        .route("/create", post(create))
        .route("/track", post(track))
        .layer(axum::extract::DefaultBodyLimit::max(BODY_LIMIT));
    let mut request = Request::post(path);
    if let Some(content_type) = content_type {
        request = request.header(header::CONTENT_TYPE, content_type);
    }
    let response = app
        .oneshot(request.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let envelope = serde_json::from_slice(&body)
        .unwrap_or_else(|error| panic!("{path}: {error}: {}", String::from_utf8_lossy(&body)));
    (status, envelope)
}

fn field(value: &str) -> Vec<PublicErrorDetail> {
    vec![PublicErrorDetail {
        key: "field".to_string(),
        value: value.to_string(),
    }]
}

/// Every body the extractor refuses answers `invalid_request`, not
/// retryable, with the framework's status, a message saying what is wrong,
/// and, for a value that does not match the schema, the field it is in.
#[tokio::test]
async fn a_refused_body_says_what_is_wrong_and_where() {
    const JSON: Option<&str> = Some("application/json");
    let long_key = "K".repeat(600);
    let long_key_body = format!(r#"{{"template":"seed","env":{{"{long_key}":1}}}}"#);
    let long_key_message = format!("invalid request body: env.{}…", "K".repeat(483));
    let too_large = format!(
        r#"{{"template":"seed","env":{{"K":"{}"}}}}"#,
        "0".repeat(BODY_LIMIT)
    );
    for (label, path, content_type, body, status, message, details) in [
        (
            "wrong type in a nested field",
            "/create",
            JSON,
            r#"{"template":"seed","resources":{"vcpu":"two"}}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid request body: resources.vcpu: invalid type: string, expected u32 at line 1 column 44",
            field("resources.vcpu"),
        ),
        (
            "a string of the wrong type is not answered back",
            "/create",
            JSON,
            r#"{"template":"seed","env":"SECRET=\"x\\"}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid request body: env: invalid type: string, expected a map at line 1 column 39",
            field("env"),
        ),
        (
            "a string of the wrong value is not answered back",
            "/create",
            JSON,
            r#"{"template":"seed","separator":"SECRET"}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid request body: separator: invalid value: string, expected a character at line 1 column 39",
            field("separator"),
        ),
        (
            "an unknown variant is not answered back",
            "/create",
            JSON,
            r#"{"template":"seed","visibility":"SECRET`, there are no variants`, expected x"}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid request body: visibility: unknown variant, expected `public` or `private` at line 1 column 77",
            field("visibility"),
        ),
        (
            "a type's own error is not answered back",
            "/create",
            JSON,
            r#"{"template":"seed","timeout":"SECRET"}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid request body: timeout: invalid value at line 1 column 38",
            field("timeout"),
        ),
        (
            "an unknown field names the key",
            "/create",
            JSON,
            r#"{"template":"seed","resources":{"gpu":1}}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid request body: resources.gpu: unknown field `gpu`, expected `vcpu` at line 1 column 37",
            field("resources.gpu"),
        ),
        (
            "a long map key is cut short",
            "/create",
            JSON,
            &long_key_body,
            StatusCode::UNPROCESSABLE_ENTITY,
            &long_key_message,
            field(&format!("env.{}…", "K".repeat(505))),
        ),
        (
            "wrong type in a sequence element",
            "/create",
            JSON,
            r#"{"template":"seed","command":["ls",1]}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid request body: command[1]: invalid type: integer `1`, expected a string at line 1 column 36",
            field("command[1]"),
        ),
        (
            "wrong type in a map value",
            "/create",
            JSON,
            r#"{"template":"seed","env":{"A":1}}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid request body: env.A: invalid type: integer `1`, expected a string at line 1 column 31",
            field("env.A"),
        ),
        (
            "missing field",
            "/create",
            JSON,
            "{}",
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid request body: missing field `template` at line 1 column 2",
            Vec::new(),
        ),
        (
            "not JSON",
            "/create",
            JSON,
            "{nope",
            StatusCode::BAD_REQUEST,
            "request body is not valid JSON: key must be a string at line 1 column 2",
            Vec::new(),
        ),
        (
            "wrong content type",
            "/create",
            Some("text/plain"),
            r#"{"template":"seed"}"#,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/json",
            Vec::new(),
        ),
        (
            "too large",
            "/create",
            JSON,
            &too_large,
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body is too large",
            Vec::new(),
        ),
        (
            "optional body with a wrong type",
            "/track",
            JSON,
            r#"{"template":5}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid request body: template: invalid type: integer `5`, expected a string at line 1 column 13",
            field("template"),
        ),
    ] {
        let (actual, envelope) = send(path, content_type, body).await;
        assert_eq!(actual, status, "{label}");
        assert_eq!(envelope.code, PublicErrorCode::InvalidRequest, "{label}");
        assert!(!envelope.retryable, "{label}");
        assert_eq!(envelope.message, message, "{label}");
        assert_eq!(envelope.details, details, "{label}");
    }
}
