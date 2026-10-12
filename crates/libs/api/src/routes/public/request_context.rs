use axum::{
    extract::Request,
    http::{HeaderName, HeaderValue},
    middleware::Next,
    response::Response,
};
use uuid::Uuid;

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

tokio::task_local! {
    static PUBLIC_REQUEST_ID: String;
}

/// Echo a caller's valid request id, or issue a fresh `req_<uuid>`. The shared
/// parser rejects an oversized or malformed id, which is replaced rather than
/// echoed onto the response, the span and every log line.
pub(super) async fn with_public_request_id(request: Request, next: Next) -> Response {
    let request_id = pplx_space_common_svc::request_id_from_headers(request.headers())
        .unwrap_or_else(generate_request_id);
    // The shared trace layer's `http.server` span is current here, and has
    // recorded only a caller-supplied id; record the one this API returns.
    tracing::Span::current().record("request_id", request_id.as_str());
    let mut response = PUBLIC_REQUEST_ID
        .scope(request_id.clone(), async move { next.run(request).await })
        .await;
    if let Ok(header_value) = HeaderValue::from_str(&request_id) {
        response
            .headers_mut()
            .insert(REQUEST_ID_HEADER, header_value);
    }
    response
}

pub(crate) fn current_request_id() -> Option<String> {
    PUBLIC_REQUEST_ID.try_with(Clone::clone).ok()
}

fn generate_request_id() -> String {
    format!("req_{}", Uuid::now_v7())
}

#[cfg(test)]
pub(super) async fn scope_request_id_for_test<F>(request_id: String, future: F) -> F::Output
where
    F: std::future::Future,
{
    PUBLIC_REQUEST_ID.scope(request_id, future).await
}
