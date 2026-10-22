//! TokenReview fixtures shared by in-memory policy tests and HTTP-backed RPC tests.

use axum::{Json, Router, routing::post};
use http::StatusCode;
use k8s_openapi::api::authentication::v1::{TokenReview, TokenReviewSpec, UserInfo};

pub use k8s_openapi::api::authentication::v1::TokenReviewStatus;

pub fn token_review_service(
    review: impl Fn(TokenReviewSpec) -> TokenReviewStatus + Clone + Send + Sync + 'static,
) -> Router {
    Router::new().route(
        "/apis/authentication.k8s.io/v1/tokenreviews",
        post(move |Json(request): Json<TokenReview>| {
            let status = review(request.spec);
            async move {
                (
                    StatusCode::CREATED,
                    Json(TokenReview {
                        status: Some(status),
                        ..Default::default()
                    }),
                )
            }
        }),
    )
}

pub fn service_account_status(audience: &str, username: &str) -> TokenReviewStatus {
    TokenReviewStatus {
        authenticated: Some(true),
        audiences: Some(vec![audience.to_string()]),
        user: Some(UserInfo {
            username: Some(username.to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn pod_bound_service_account_status(
    audience: &str,
    username: &str,
    node_name: &str,
    node_uid: &str,
) -> TokenReviewStatus {
    let mut status = service_account_status(audience, username);
    status
        .user
        .as_mut()
        .expect("service_account_status supplies UserInfo")
        .extra = Some(
        [
            ("pod-name", "test-pod"),
            ("pod-uid", "test-pod-uid"),
            ("node-name", node_name),
            ("node-uid", node_uid),
        ]
        .map(|(key, value)| {
            (
                format!("authentication.kubernetes.io/{key}"),
                vec![value.into()],
            )
        })
        .into(),
    );
    status
}

pub fn projected_token(service_account: &str) -> String {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let expiry = jiff::Timestamp::now().as_second() + 600;
    format!(
        "{}.{}.signature",
        URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256"}"#),
        URL_SAFE_NO_PAD
            .encode(serde_json::json!({"sub": service_account, "exp": expiry}).to_string())
    )
}

pub fn kubeconfig_for_endpoint(endpoint: std::net::SocketAddr) -> String {
    format!(
        "apiVersion: v1\nkind: Config\nclusters:\n- name: test\n  cluster:\n    server: http://{endpoint}\ncontexts:\n- name: test\n  context:\n    cluster: test\n    user: test\ncurrent-context: test\nusers:\n- name: test\n  user: {{}}\n"
    )
}

pub async fn serve_token_reviews(
    listener: tokio::net::TcpListener,
    review: impl Fn(TokenReviewSpec) -> TokenReviewStatus + Clone + Send + Sync + 'static,
    shutdown: tokio_util::sync::CancellationToken,
) -> std::io::Result<()> {
    axum::serve(listener, token_review_service(review))
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await
}
