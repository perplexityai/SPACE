use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

use super::*;

#[tokio::test(start_paused = true)]
async fn stream_load_counts_every_attached_stream_whatever_its_age() {
    let limiter = Arc::new(AttachedStreamLimiter::new(10, 10, 10));

    let long_lived = limiter.try_acquire_label("ecw_service").expect("permit");
    tokio::time::advance(Duration::from_secs(24 * 60 * 60)).await;
    let fresh = limiter.try_acquire_label("ecw_service").expect("permit");

    assert_eq!(
        stream_load(&limiter),
        DataPlaneStreamLoad { active_streams: 2 },
        "a day-old stream is cut by a scale-in just like a new one"
    );

    drop(fresh);
    assert_eq!(stream_load(&limiter).active_streams, 1);
    drop(long_lived);
    assert_eq!(stream_load(&limiter).active_streams, 0);
}

#[tokio::test]
async fn internal_router_serves_the_stream_load_report() {
    let limiter = Arc::new(AttachedStreamLimiter::new(10, 10, 10));
    let _permit = limiter.try_acquire_label("ecw_service").expect("permit");

    let response = internal_router(limiter)
        .oneshot(
            Request::get(DATA_PLANE_STREAM_LOAD_PATH)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let load: DataPlaneStreamLoad = serde_json::from_slice(&body).expect("json");
    assert_eq!(load, DataPlaneStreamLoad { active_streams: 1 });
}

#[tokio::test]
async fn internal_router_serves_nothing_else() {
    let limiter = Arc::new(AttachedStreamLimiter::new(10, 10, 10));
    for path in ["/", "/healthz", "/files", "/internal/v1"] {
        let response = internal_router(limiter.clone())
            .oneshot(Request::get(path).body(Body::empty()).expect("request"))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }
}

#[tokio::test]
async fn internal_listener_serves_over_tcp_and_stops_cleanly_on_shutdown() {
    let limiter = Arc::new(AttachedStreamLimiter::new(10, 10, 10));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let shutdown = CancellationToken::new();
    let task = spawn_internal(listener, limiter, shutdown.clone());

    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    tokio::io::AsyncWriteExt::write_all(
        &mut stream,
        format!(
            "GET {DATA_PLANE_STREAM_LOAD_PATH} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"
        )
        .as_bytes(),
    )
    .await
    .expect("write");
    let mut response = String::new();
    tokio::io::AsyncReadExt::read_to_string(&mut stream, &mut response)
        .await
        .expect("read");
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("\"active_streams\":0"), "{response}");

    shutdown.cancel();
    assert_eq!(
        task.await.expect("internal listener task"),
        InternalListenerEnd::Shutdown
    );
}

#[tokio::test]
async fn a_panicking_serve_task_is_logged_and_leaves_the_replica_serving() {
    let shutdown = CancellationToken::new();
    let serve = tokio::spawn(async { panic!("serve task panicked") });

    assert_eq!(
        supervise_internal(serve, shutdown.clone()).await,
        InternalListenerEnd::Early
    );
    assert!(
        !shutdown.is_cancelled(),
        "shutting the replica down would cut every stream it holds"
    );
}

#[tokio::test]
async fn a_serve_task_that_returns_before_shutdown_is_an_early_end() {
    let shutdown = CancellationToken::new();
    let serve = tokio::spawn(async { Ok(()) });

    assert_eq!(
        supervise_internal(serve, shutdown.clone()).await,
        InternalListenerEnd::Early
    );
    assert!(!shutdown.is_cancelled());
}

#[tokio::test]
async fn a_serve_task_that_stops_after_shutdown_is_clean() {
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let serve = tokio::spawn(async { Ok(()) });

    assert_eq!(
        supervise_internal(serve, shutdown).await,
        InternalListenerEnd::Shutdown
    );
}
