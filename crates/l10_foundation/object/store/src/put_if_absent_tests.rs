//! [`S3ObjectStore::put_bytes_if_absent`] against an S3 stand-in that
//! enforces `If-None-Match: *` the way S3 does.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;

use crate::test_support::{credentials, serve};
use crate::{ObjectStore, ObjectTarget, PutIfAbsent, S3ObjectStore};

/// Objects by request path, plus whether each PutObject asked for a
/// conditional create.
#[derive(Clone, Default)]
struct ConditionalS3 {
    objects: Arc<Mutex<HashMap<String, Bytes>>>,
    conditional_puts: Arc<Mutex<Vec<bool>>>,
}

async fn handle(
    State(s3): State<ConditionalS3>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path().to_string();
    match method {
        Method::PUT => {
            let conditional = headers
                .get("if-none-match")
                .is_some_and(|value| value == "*");
            s3.conditional_puts.lock().unwrap().push(conditional);
            let mut objects = s3.objects.lock().unwrap();
            if conditional && objects.contains_key(&path) {
                return (
                    StatusCode::PRECONDITION_FAILED,
                    [("content-type", "application/xml")],
                    "<Error><Code>PreconditionFailed</Code><Message>At least one of \
                     the pre-conditions you specified did not hold</Message></Error>",
                )
                    .into_response();
            }
            objects.insert(path, body);
            (StatusCode::OK, [("etag", "\"stand-in\"")]).into_response()
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

fn target(endpoint: &str) -> ObjectTarget<'_> {
    ObjectTarget {
        bucket: "b",
        key: "templates/seed/sha256:ab/manifest.json",
        region: "us-east-1",
        endpoint: Some(endpoint),
        path_style: true,
    }
}

#[tokio::test]
async fn a_conditional_put_creates_once_and_never_replaces() {
    let s3 = ConditionalS3::default();
    let endpoint = serve(Router::new().fallback(handle).with_state(s3.clone())).await;
    let store = S3ObjectStore::new();

    let first = store
        .put_bytes_if_absent(
            &credentials(),
            target(&endpoint),
            Bytes::from_static(b"original"),
        )
        .await
        .expect("first put");
    let second = store
        .put_bytes_if_absent(
            &credentials(),
            target(&endpoint),
            Bytes::from_static(b"twin"),
        )
        .await
        .expect("second put");

    assert_eq!(first, PutIfAbsent::Created);
    assert_eq!(second, PutIfAbsent::AlreadyExists);
    assert_eq!(*s3.conditional_puts.lock().unwrap(), [true, true]);
    let objects = s3.objects.lock().unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(
        objects.values().next().map(Bytes::as_ref),
        Some(b"original".as_slice())
    );
}
