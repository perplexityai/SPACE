use super::*;
use axum::{
    Json, Router,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[tokio::test]
async fn login_requires_a_session_and_admin_access_without_following_redirects() {
    for (login_code, token, admin_code, succeeds) in [
        (200, "synthetic-session", 200, true),
        (401, "synthetic-session", 200, false),
        (503, "synthetic-session", 200, false),
        (302, "synthetic-session", 200, false),
        (200, "", 200, false),
        (200, "synthetic-session", 403, false),
        (200, "synthetic-session", 401, false),
    ] {
        let redirects = Arc::new(AtomicUsize::new(0));
        let received = redirects.clone();
        let router = Router::new()
            .route(
                "/auth/v1/sessions",
                post(move |Json(body): Json<serde_json::Value>| async move {
                    assert_eq!(
                        body,
                        json!({"email":"operator@example.com","password":"synthetic-password"})
                    );
                    (
                        StatusCode::from_u16(login_code).unwrap(),
                        [("location", "/unexpected")],
                        Json(json!({"token":token})),
                    )
                }),
            )
            .route(
                "/admin/api/v1/templates",
                get(move |headers: HeaderMap| async move {
                    assert_eq!(headers[header::AUTHORIZATION], "Bearer synthetic-session");
                    StatusCode::from_u16(admin_code).unwrap()
                }),
            )
            .route(
                "/unexpected",
                get(move || {
                    let received = received.clone();
                    async move {
                        received.fetch_add(1, Ordering::SeqCst);
                        StatusCode::OK
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = origin(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        let result = authenticate(
            url.clone(),
            url,
            "operator@example.com",
            "synthetic-password",
        )
        .await;

        assert_eq!(
            result.is_ok(),
            succeeds,
            "login={login_code}, admin={admin_code}"
        );
        match result {
            Ok(token) => assert_eq!(token, "synthetic-session"),
            Err(error) => {
                assert!(!error.to_string().contains("synthetic-session"));
                assert!(!error.to_string().contains("synthetic-password"));
                if login_code == 503 {
                    assert!(error.to_string().contains("retry"));
                }
            }
        }
        assert_eq!(redirects.load(Ordering::SeqCst), 0);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }
}

#[test]
fn credentials_require_a_secure_origin() {
    for value in [
        "https://api.example.com/path",
        "https://user:password@api.example.com",
        "http://api.example.com",
        "https://api.example.com?token=secret",
        "https://api.example.com#fragment",
    ] {
        assert!(origin(value).is_err(), "{value}");
    }
    for value in [
        "https://api.example.com",
        "http://127.0.0.1:8081",
        "http://[::1]:8081",
    ] {
        assert!(origin(value).is_ok(), "{value}");
    }
}
