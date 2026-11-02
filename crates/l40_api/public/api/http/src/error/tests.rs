use axum::{
    Json, Router,
    body::Body,
    http::{Request, StatusCode, header},
    middleware,
    routing::{get, post},
};
use http_body_util::BodyExt;
use pplx_space_public_api::{PublicErrorCode, PublicErrorEnvelope};
use serde::Deserialize;
use tower::ServiceExt;

use super::{
    PublicApiError, ResponseKind, ResponseOrigin, normalize_public_error_response,
    public_route_not_found,
};
use crate::request_id::{scope_request_id, with_public_request_id};

const REQUEST_ID: &str = "req-native-error-test";
const SPECIFIC_CONFLICT: &str = "specific conflict";

#[derive(Deserialize)]
struct TestRequest {
    _value: String,
}

#[tokio::test]
async fn public_error_envelope_includes_current_request_id() {
    let envelope = scope_request_id("req-123".to_string(), async {
        PublicApiError::bad_request("invalid input").into_envelope()
    })
    .await;

    assert_eq!(envelope.request_id.as_deref(), Some("req-123"));
}

#[test]
fn public_error_defaults_carry_response_classification() {
    let client_error = PublicApiError::bad_request("invalid input");
    assert_eq!(client_error.origin, ResponseOrigin::Client);
    assert_eq!(client_error.kind, ResponseKind::Validation);

    let api_error = PublicApiError::internal();
    assert_eq!(api_error.origin, ResponseOrigin::Api);
    assert_eq!(api_error.kind, ResponseKind::Internal);
}

#[test]
fn advertised_public_error_codes_are_constructible() {
    assert_eq!(
        PublicApiError::unauthorized("missing auth")
            .into_envelope()
            .code,
        pplx_space_public_api::PublicErrorCode::Unauthorized
    );
    assert_eq!(
        PublicApiError::forbidden("forbidden").into_envelope().code,
        pplx_space_public_api::PublicErrorCode::Forbidden
    );
    assert_eq!(
        PublicApiError::capacity_unavailable("no capacity")
            .into_envelope()
            .code,
        pplx_space_public_api::PublicErrorCode::CapacityUnavailable
    );
    assert_eq!(
        PublicApiError::rate_limited("slow down")
            .into_envelope()
            .code,
        pplx_space_public_api::PublicErrorCode::RateLimited
    );
}

#[tokio::test]
async fn public_router_normalizes_unhandled_http_errors() {
    async fn parse_json(Json(_request): Json<TestRequest>) -> StatusCode {
        StatusCode::NO_CONTENT
    }

    let v1 = Router::new()
        .route("/items", post(parse_json))
        .fallback(public_route_not_found)
        .layer(middleware::from_fn(normalize_public_error_response))
        .layer(middleware::from_fn(with_public_request_id));
    let app = Router::new().nest("/v1", v1);

    let cases = [
        (
            Request::post("/v1/items")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-request-id", REQUEST_ID)
                .body(Body::from("{"))
                .unwrap(),
            StatusCode::BAD_REQUEST,
            PublicErrorCode::InvalidRequest,
        ),
        (
            Request::get("/v1/items")
                .header("x-request-id", REQUEST_ID)
                .body(Body::empty())
                .unwrap(),
            StatusCode::METHOD_NOT_ALLOWED,
            PublicErrorCode::InvalidRequest,
        ),
        // The documented contract: extractor rejections keep their own status
        // (415, 422) but carry `invalid_request`, not retryable.
        (
            Request::post("/v1/items")
                .header(header::CONTENT_TYPE, "text/plain")
                .header("x-request-id", REQUEST_ID)
                .body(Body::from("{\"_value\":\"x\"}"))
                .unwrap(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            PublicErrorCode::InvalidRequest,
        ),
        (
            Request::post("/v1/items")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-request-id", REQUEST_ID)
                .body(Body::from("{}"))
                .unwrap(),
            StatusCode::UNPROCESSABLE_ENTITY,
            PublicErrorCode::InvalidRequest,
        ),
        (
            Request::get("/v1/missing")
                .header("x-request-id", REQUEST_ID)
                .body(Body::empty())
                .unwrap(),
            StatusCode::NOT_FOUND,
            PublicErrorCode::NotFound,
        ),
    ];

    for (request, expected_status, expected_code) in cases {
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), expected_status);
        assert_eq!(response.headers().get("x-request-id").unwrap(), REQUEST_ID);
        let envelope: PublicErrorEnvelope =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(envelope.code, expected_code);
        assert_eq!(envelope.request_id.as_deref(), Some(REQUEST_ID));
        assert!(!envelope.message.is_empty());
        assert!(!envelope.retryable);
    }
}

#[tokio::test]
async fn public_router_preserves_handler_error_envelopes() {
    async fn handler_error() -> PublicApiError {
        PublicApiError::conflict(SPECIFIC_CONFLICT)
    }

    let app = Router::new()
        .route("/conflict", get(handler_error))
        .layer(middleware::from_fn(normalize_public_error_response))
        .layer(middleware::from_fn(with_public_request_id));
    let response = app
        .oneshot(
            Request::get("/conflict")
                .header("x-request-id", REQUEST_ID)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let envelope: PublicErrorEnvelope =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(envelope.code, PublicErrorCode::Conflict);
    assert_eq!(envelope.message, SPECIFIC_CONFLICT);
}

/// `error_contract` is the single source of truth: every constructor's status
/// and `retryable` must come from it, non-retryable errors never advertise a
/// delay, and `retryable` is only ever claimed for 429 and 5xx statuses.
#[test]
fn every_constructor_matches_the_error_contract() {
    use super::error_contract;

    let constructed = [
        PublicApiError::bad_request("x"),
        PublicApiError::unauthorized("x"),
        PublicApiError::forbidden("x"),
        PublicApiError::not_found("x"),
        PublicApiError::conflict("x"),
        PublicApiError::idempotency_conflict("x"),
        PublicApiError::failed_precondition("x"),
        PublicApiError::rate_limited("x"),
        PublicApiError::sandbox_quota_exceeded("x"),
        PublicApiError::capacity_unavailable("x"),
        PublicApiError::service_unavailable("x"),
        PublicApiError::lock_contention(),
        PublicApiError::internal(),
    ];
    for error in constructed {
        let (status, retryable) = error_contract(&error.code);
        assert_eq!(error.status, status, "{}", error.code);
        assert_eq!(error.retryable, retryable, "{}", error.code);
        if !retryable {
            assert!(
                error.retry_after.is_none(),
                "{} advertised a delay",
                error.code
            );
        }
    }
    for code in PublicErrorCode::ALL {
        let (status, retryable) = error_contract(code);
        if retryable {
            assert!(
                status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error(),
                "{code} is retryable but answers {status}"
            );
        }
    }
    assert_eq!(
        error_contract(&PublicErrorCode::Other("future_code".to_string())),
        (StatusCode::INTERNAL_SERVER_ERROR, false)
    );
}

#[test]
fn not_retryable_clears_the_retry_hint() {
    use axum::response::IntoResponse;

    let error = PublicApiError::lock_contention().not_retryable();
    assert!(!error.retryable);
    assert!(error.retry_after.is_none());
    let response = error.into_response();
    assert!(response.headers().get(header::RETRY_AFTER).is_none());
}

/// `Retry-After` is whole seconds, rounded up, never zero: a client that
/// honours it literally must not spin.
#[test]
fn retry_after_is_rendered_in_whole_seconds_rounded_up() {
    use axum::response::IntoResponse;
    use std::time::Duration;

    for (retry_after, expected) in [
        (Duration::ZERO, "1"),
        (Duration::from_millis(1500), "2"),
        (Duration::from_secs(5), "5"),
    ] {
        let response = PublicApiError::rate_limited("slow down")
            .with_retry_after(retry_after)
            .into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response.headers().get(header::RETRY_AFTER).unwrap(),
            expected,
            "{retry_after:?}"
        );
    }
}

/// A 429 that bypassed the handlers (an Axum layer answered) is still
/// normalized into the envelope without losing the delay it advertised.
#[tokio::test]
async fn unhandled_429_keeps_its_retry_after_when_normalized() {
    async fn throttled() -> (StatusCode, [(header::HeaderName, &'static str); 1]) {
        (StatusCode::TOO_MANY_REQUESTS, [(header::RETRY_AFTER, "7")])
    }

    let v1 = Router::new()
        .route("/throttled", get(throttled))
        .layer(middleware::from_fn(normalize_public_error_response))
        .layer(middleware::from_fn(with_public_request_id));
    let app = Router::new().nest("/v1", v1);
    let response = app
        .oneshot(
            Request::get("/v1/throttled")
                .header("x-request-id", REQUEST_ID)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers().get(header::RETRY_AFTER).unwrap(), "7");
    let envelope: PublicErrorEnvelope =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(envelope.code, PublicErrorCode::RateLimited);
    assert!(envelope.retryable);
    assert_eq!(envelope.request_id.as_deref(), Some(REQUEST_ID));
}
