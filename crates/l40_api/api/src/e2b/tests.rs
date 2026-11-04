use serde_json::json;

use super::{
    errors::{E2bApiError, E2bErrorResponse},
    schema::*,
};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use pplx_space_metrics::response::{ResponseKind, ResponseOrigin};

#[test]
fn fork_request_accepts_the_sdk_defaults_shape() {
    let omitted: SandboxForkRequestE2b = serde_json::from_value(json!({})).unwrap();
    assert_eq!(omitted, SandboxForkRequestE2b::default());

    let explicit: SandboxForkRequestE2b = serde_json::from_value(json!({
        "timeout": 300,
        "count": 2,
    }))
    .unwrap();
    assert_eq!(explicit.timeout, Some(300));
    assert_eq!(explicit.count, Some(2));
}

#[test]
fn fork_results_serialize_exactly_one_outcome() {
    let sandbox = SandboxForkResultE2b::Sandbox(SandboxResponseE2b {
        sandbox_id: "019f8b57-c1a5-7923-ae02-72019d0f65ca".to_string(),
        template_id: "code-runtime".to_string(),
        client_id: String::new(),
        alias: None,
        envd_version: "0.5.7".to_string(),
        envd_access_token: Some("envd-token".to_string()),
        traffic_access_token: Some("traffic-token".to_string()),
        domain: Some("e2b.example.com".to_string()),
    });
    assert_eq!(
        serde_json::to_value(sandbox).unwrap(),
        json!({
            "sandboxID": "019f8b57-c1a5-7923-ae02-72019d0f65ca",
            "templateID": "code-runtime",
            "clientID": "",
            "envdVersion": "0.5.7",
            "envdAccessToken": "envd-token",
            "trafficAccessToken": "traffic-token",
            "domain": "e2b.example.com",
        })
    );

    let error = SandboxForkResultE2b::Error {
        error: SandboxForkErrorE2b {
            code: 500,
            message: "target activation failed".to_string(),
        },
    };
    assert_eq!(
        serde_json::to_value(error).unwrap(),
        json!({
            "error": {
                "code": 500,
                "message": "target activation failed",
            }
        })
    );
}

#[tokio::test]
async fn per_client_limit_error_renders_a_distinct_retryable_429_response() {
    // Assert on the actual HTTP response `into_response` produces, not the
    // raw struct fields, so the Retry-After header insertion and JSON body
    // serialization are exercised.
    let response = E2bApiError::too_many_client_operations()
        .with_retry_after(std::time::Duration::from_secs(1))
        .into_response();

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        response
            .headers()
            .get(header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok()),
        Some("1"),
        "retryable 429 must carry Retry-After: 1"
    );

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should collect");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("body should be JSON");
    assert_eq!(body["code"], 429);
    // The message must attribute the throttle to the client's own limit so it
    // reads distinctly from the server-side capacity throttle.
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|m| m.starts_with("Per-client rate limit:")),
        "unexpected message: {}",
        body["message"]
    );
}

#[test]
fn e2b_api_error_defaults_to_bounded_response_classification() {
    let bad_request = E2bApiError::bad_request("invalid");
    assert_eq!(bad_request.origin, ResponseOrigin::Client);
    assert_eq!(bad_request.kind, ResponseKind::Validation);

    let unauthorized = E2bApiError::unauthorized();
    assert_eq!(unauthorized.origin, ResponseOrigin::Client);
    assert_eq!(unauthorized.kind, ResponseKind::Auth);

    let route = E2bApiError::sandbox_waking();
    assert_eq!(route.origin, ResponseOrigin::Cluster);
    assert_eq!(route.kind, ResponseKind::RouteNotReady);

    let internal = E2bApiError::internal("failed");
    assert_eq!(internal.origin, ResponseOrigin::Api);
    assert_eq!(internal.kind, ResponseKind::Internal);
}

#[test]
fn e2b_api_error_classification_can_be_overridden() {
    let error = E2bApiError::upstream("backend failed")
        .with_classification(ResponseOrigin::Spaced, ResponseKind::BackendStatus);

    assert_eq!(error.origin, ResponseOrigin::Spaced);
    assert_eq!(error.kind, ResponseKind::BackendStatus);
}

/// End-to-end through the real trace layer: the `http.server` span must
/// declare `otel.status_description` (`tracing` silently drops `record()`
/// calls for undeclared fields) and be the current span when a handler
/// error converts to a response, for both error types that record it.
/// Without this, the Datadog exporter falls back to the bare HTTP status
/// code as `error.message`.
#[tokio::test]
async fn error_messages_land_on_the_http_server_span() {
    use std::sync::{Arc, Mutex};

    use axum::body::Body;
    use axum::http::Request;
    use pplx_space_common_svc::{HttpServerMakeSpan, http_trace_layer};
    use tower::util::ServiceExt;
    use tracing_subscriber::prelude::*;

    #[derive(Clone, Default)]
    struct Descriptions(Arc<Mutex<Vec<String>>>);

    impl tracing::field::Visit for Descriptions {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "otel.status_description" {
                self.0.lock().unwrap().push(value.to_string());
            }
        }
        fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Descriptions {
        fn on_record(
            &self,
            _span: &tracing::Id,
            values: &tracing::span::Record<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            values.record(&mut self.clone());
        }
    }

    let descriptions = Descriptions::default();
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(descriptions.clone()));

    let app = axum::Router::new()
        .route(
            "/e2b",
            axum::routing::post(|| async { E2bApiError::failed_to_place_sandbox() }),
        )
        .route(
            "/native",
            axum::routing::post(|| async {
                crate::controllers::ControllerError::internal("db exploded")
            }),
        )
        .route(
            "/client",
            axum::routing::post(|| async {
                E2bApiError::bad_request("invalid caller value: do-not-record")
            }),
        )
        .layer(http_trace_layer(HttpServerMakeSpan::default()));

    for path in ["/e2b", "/native"] {
        let request = Request::post(path).body(Body::empty()).unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
    let response = app
        .oneshot(Request::post("/client").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    assert_eq!(
        *descriptions.0.lock().unwrap(),
        ["Failed to place sandbox", "db exploded", "validation"],
    );
}

#[test]
fn deserializes_create_request_sdk_template_key() {
    let raw = r#"{
        "templateID": "app-runtime",
        "metadata": {"name": "session-123"},
        "envVars": {"FOO": "bar"},
        "autoPause": true,
        "allow_internet_access": false,
        "timeout": 300
    }"#;

    let e2b: CreateSandboxRequestE2b = serde_json::from_str(raw).unwrap();
    assert_eq!(e2b.template_id.as_deref(), Some("app-runtime"));
    assert_eq!(
        e2b.metadata.get("name").map(String::as_str),
        Some("session-123")
    );
    assert_eq!(e2b.env_vars.get("FOO").map(String::as_str), Some("bar"));
    assert_eq!(e2b.auto_pause, Some(true));
    assert_eq!(e2b.allow_internet_access, Some(false));
    assert_eq!(e2b.timeout, Some(300));
}

#[test]
fn ignores_non_e2b_template_key() {
    let raw = r#"{"templateId": "not-e2b-schema"}"#;

    let e2b: CreateSandboxRequestE2b = serde_json::from_str(raw).unwrap();
    assert_eq!(e2b.template_id, None);
}

#[test]
fn serializes_sandbox_response_with_e2b_id_keys() {
    let e2b = SandboxResponseE2b {
        sandbox_id: "sbox_123".to_string(),
        template_id: "tmpl_123".to_string(),
        client_id: "".to_string(),
        alias: None,
        envd_version: "0.0.0".to_string(),
        envd_access_token: Some("envd-token".to_string()),
        traffic_access_token: Some("traffic-token".to_string()),
        domain: Some("example.com".to_string()),
    };

    let value = serde_json::to_value(e2b).unwrap();
    assert_eq!(value["sandboxID"], json!("sbox_123"));
    assert_eq!(value["templateID"], json!("tmpl_123"));
    assert_eq!(value["clientID"], json!(""));
    assert_eq!(value["envdVersion"], json!("0.0.0"));
    assert_eq!(value["envdAccessToken"], json!("envd-token"));
    assert_eq!(value["trafficAccessToken"], json!("traffic-token"));
}

#[test]
fn serializes_list_response_as_array() {
    let listed = ListedSandboxesResponseE2b(vec![ListedSandboxE2b {
        template_id: "tmpl_123".to_string(),
        alias: None,
        sandbox_id: "sbox_123".to_string(),
        client_id: "client_123".to_string(),
        started_at: "2026-04-08T20:00:00Z".to_string(),
        end_at: "2026-04-08T21:00:00Z".to_string(),
        cpu_count: 2,
        memory_mb: 1024,
        disk_size_mb: 4096,
        metadata: StringMap::from([(String::from("name"), String::from("session-123"))]),
        state: SandboxStateE2b::Running,
        envd_version: "0.0.0".to_string(),
        volume_mounts: Vec::new(),
    }]);

    let value = serde_json::to_value(listed).unwrap();
    assert!(value.is_array());
    assert_eq!(value[0]["sandboxID"], json!("sbox_123"));
}

#[test]
fn serializes_error_body_without_wrapper() {
    let value = serde_json::to_value(E2bErrorResponse {
        code: 404,
        message: "sandbox not found".to_string(),
    })
    .unwrap();

    assert_eq!(value["code"], json!(404));
    assert_eq!(value["message"], json!("sandbox not found"));
}

#[test]
fn serializes_file_and_process_fields_with_explicit_wire_names() {
    let file_info = FileInfoE2b {
        name: "main.py".to_string(),
        path: "/workspace/main.py".to_string(),
        file_type: FileTypeE2b::File,
        size: 12,
        mode: 0o644,
        mod_time: Some("2026-04-08T20:00:00Z".to_string()),
    };
    let process = ProcessStartResponseE2b {
        process_id: "proc-123".to_string(),
        exit_code: Some(0),
        stdout: "ok".to_string(),
        stderr: String::new(),
    };

    let file_value = serde_json::to_value(file_info).unwrap();
    let process_value = serde_json::to_value(process).unwrap();

    assert_eq!(file_value["modTime"], json!("2026-04-08T20:00:00Z"));
    assert_eq!(process_value["processID"], json!("proc-123"));
    assert_eq!(process_value["exitCode"], json!(0));
}
