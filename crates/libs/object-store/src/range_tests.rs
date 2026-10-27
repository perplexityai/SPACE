use super::*;
use crate::test_support::{Recorder, credentials, every_class, serve, store, target};
use axum::{
    Router,
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode, Uri},
    response::Response,
};
use pplx_space_common_core::types::ObjectByteRange;

#[derive(Clone, Copy)]
enum Reply {
    Exact,
    NoRange,
    WrongOffset,
    WrongEof,
    WrongVersion,
    Short,
}

async fn respond(State(reply): State<Reply>, headers: HeaderMap, uri: Uri) -> Response {
    assert_eq!(headers.get("range").unwrap(), "bytes=3-6");
    assert!(uri.query().unwrap().contains("versionId=pinned"));
    let range = match reply {
        Reply::NoRange => None,
        Reply::WrongOffset => Some("bytes 4-7/10"),
        Reply::WrongEof => Some("bytes 3-6/11"),
        _ => Some("bytes 3-6/10"),
    };
    let mut builder = Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(
            "x-amz-version-id",
            if matches!(reply, Reply::WrongVersion) {
                "replaced"
            } else {
                "pinned"
            },
        );
    if let Some(range) = range {
        builder = builder.header("content-range", range);
    }
    let body: &'static [u8] = if matches!(reply, Reply::Short) {
        b"def"
    } else {
        b"defg"
    };
    builder
        .header("content-length", body.len())
        .body(Body::from(body))
        .unwrap()
}

#[tokio::test]
async fn exact_object_range_validates_headers_version_and_length_without_full_get() {
    let range = ObjectByteRange::new(3, 4, 10, Some("pinned".into())).unwrap();
    let directory = tempfile::tempdir().unwrap();
    for reply in [
        Reply::Exact,
        Reply::NoRange,
        Reply::WrongOffset,
        Reply::WrongEof,
        Reply::WrongVersion,
        Reply::Short,
    ] {
        let endpoint = serve(Router::new().fallback(respond).with_state(reply)).await;
        let backend = store(
            every_class(Duration::from_secs(2), Duration::from_secs(5)),
            Duration::from_secs(1),
            &Arc::new(Recorder::default()),
        );
        let destination = directory.path().join("range");
        let result = backend
            .get_file_range(&credentials(), target(&endpoint), &destination, &range)
            .await;
        if matches!(reply, Reply::Exact) {
            assert_eq!(result.unwrap(), 4);
            assert_eq!(std::fs::read(&destination).unwrap(), b"defg");
        } else {
            result.unwrap_err();
        }
    }
}
